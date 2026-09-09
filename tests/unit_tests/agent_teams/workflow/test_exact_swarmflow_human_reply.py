# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Exact SDK delivery resolves the real avatar-owned human-input Future.

The pool, controller, worker backend, avatar wait and Runner delegates are real.
No LLM, Tool or business completion is claimed by these receipt tests.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from openjiuwen.agent_teams.harness.async_tools import AsyncToolRecord, AsyncToolRuntime
from openjiuwen.agent_teams.harness.team_harness import TeamHarness
from openjiuwen.agent_teams.runtime.background_task_controller import BackgroundTaskController, SwarmflowRunHandle
from openjiuwen.agent_teams.runtime.manager import TeamRuntimeActivation, TeamRuntimeManager
from openjiuwen.agent_teams.runtime.dispatch import RunAction, RunActionKind
from openjiuwen.agent_teams.runtime.pool import ActiveTeam, RuntimeState
from openjiuwen.agent_teams.schema.events import EventMessage, TeamEvent
from openjiuwen.agent_teams.schema.team import TeamRole
from openjiuwen.agent_teams.workflow.backends.team_worker_backend import TeamWorkerBackend
from openjiuwen.agent_teams.workflow.backends.avatar_session_backend import AvatarSessionManager, _SessionState
from openjiuwen.agent_teams.workflow.backends.budget_rail import SwarmflowBudgetRail
from openjiuwen.core.runner import Runner
from openjiuwen.core.runner import team_runner


@asynccontextmanager
async def pending_runtime(monkeypatch, *, timeout=600):
    ready = asyncio.Event()
    backend = TeamWorkerBackend(
        model=None, team_name="team", session_id="session", run_id="run",
        on_human_prompt=lambda *_: ready.set(),
    )
    avatar = backend._sessions()
    avatar._human_timeout = timeout
    state = _SessionState("human", None, None, "human", SwarmflowBudgetRail(avatar._budget))
    avatar._sessions["human"] = state
    # Run the production rendezvous itself; no harness/model is needed until
    # its accepted raw input proceeds to avatar formatting in _human_turn.
    # Keep the acceptance boundary at raw input, before any model formatting.
    async def raw_turn(state, prompt, opts, schema_json, correlation_id):
        return await avatar._await_human_reply(state, prompt, opts, correlation_id)
    monkeypatch.setattr(avatar, "_human_turn", raw_turn)
    wait = asyncio.create_task(avatar.send_turn("human", "approve?", {}, None, correlation_id="phase:human:0"))
    await asyncio.wait_for(ready.wait(), 1)
    record = AsyncToolRecord(task_id="task", tool_name="swarmflow", description="human wait")
    native = SimpleNamespace(async_tool_runtime=SimpleNamespace(get=lambda _: record))
    controller = BackgroundTaskController()
    handle = SwarmflowRunHandle("task", asyncio.Event(), backend, native, lambda: pytest.fail("relaunch"))
    controller.register(handle)
    harness = TeamHarness(None, None, native, role=TeamRole.LEADER, member_name="leader")
    harness.set_background_task_controller(controller)
    entry = ActiveTeam("team", SimpleNamespace(harness=harness), "session")
    manager = TeamRuntimeManager()
    await manager.pool.add(entry)
    impl = team_runner._TeamRunnerMixin()
    impl._team_runtime_manager = manager
    monkeypatch.setattr(team_runner, "_global_runner", lambda: impl)

    async def reply(**overrides):
        args = dict(session_id="session", team_name="team", run_id="run",
                    correlation_id="phase:human:0", answer=" accepted verbatim ")
        args.update(overrides)
        return await Runner.reply_swarmflow_human(**args)

    context = SimpleNamespace(
        backend=backend, avatar=avatar, wait=wait, record=record, handle=handle,
        controller=controller, entry=entry, manager=manager, reply=reply, state=state,
    )
    try:
        yield context
    finally:
        wait.cancel()
        await asyncio.gather(wait, return_exceptions=True)
        await backend.aclose()


@pytest.mark.asyncio
async def test_exact_runner_receipt_means_input_future_received(monkeypatch):
    async with pending_runtime(monkeypatch) as p:
        effects = []
        future = p.avatar._pending_human["phase:human:0"]

        def before_effect():
            assert not future.done()
            effects.append("authorize")

        result = await p.reply(before_effect=before_effect)
        assert result.ok and result.message_id is None
        assert future.done() and future.result() == " accepted verbatim "
        assert await p.wait == " accepted verbatim "
        assert effects == ["authorize"]
        assert not (await p.reply(before_effect=lambda: effects.append("duplicate"))).ok
        assert effects == ["authorize"]


@pytest.mark.asyncio
@pytest.mark.parametrize("overrides", [
    {"session_id": "other"}, {"team_name": "other"}, {"run_id": "other"},
    {"correlation_id": "other"}, {"session_id": ""}, {"team_name": " "},
    {"run_id": None}, {"correlation_id": []}, {"answer": {}},
])
async def test_invalid_or_wrong_exact_identity_has_no_effect(monkeypatch, overrides):
    async with pending_runtime(monkeypatch) as p:
        effects = []
        result = await p.reply(**overrides, before_effect=lambda: effects.append("forbidden"))
        assert not result.ok
        assert not p.wait.done() and effects == []
        assert not p.avatar._pending_human["phase:human:0"].done()
        assert await p.manager.pool.get("team") is p.entry
        assert p.backend._session_mgr is p.avatar


@pytest.mark.asyncio
async def test_callback_failure_propagates_leaving_same_future_retryable(monkeypatch):
    async with pending_runtime(monkeypatch) as p:
        failure = RuntimeError("application revoked authority")
        future = p.avatar._pending_human["phase:human:0"]

        def reject():
            raise failure

        with pytest.raises(RuntimeError) as raised:
            await p.reply(before_effect=reject)
        assert raised.value is failure
        assert not future.done() and not p.wait.done()
        assert (await p.reply()).ok
        assert await p.wait == " accepted verbatim "


@pytest.mark.asyncio
async def test_simultaneous_exact_replies_consume_only_once(monkeypatch):
    async with pending_runtime(monkeypatch) as p:
        effects = []
        results = await asyncio.gather(*[
            p.reply(answer=str(n), before_effect=lambda n=n: effects.append(n)) for n in range(8)
        ])
        assert sum(result.ok for result in results) == 1
        assert len(effects) == 1
        assert await p.wait == str(effects[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_first", [True, False])
async def test_legacy_event_and_exact_reply_share_consumption(monkeypatch, legacy_first):
    async with pending_runtime(monkeypatch) as p:
        effects = []
        legacy = EventMessage(event_type=TeamEvent.WORKFLOW_HUMAN_REPLY,
                              payload={"correlation_id": "phase:human:0", "answer": "legacy"}, sender_id="user")
        if legacy_first:
            await p.avatar._on_reply_event(legacy)
        result = await p.reply(before_effect=lambda: effects.append("exact"))
        if not legacy_first:
            await p.avatar._on_reply_event(legacy)
        assert result.ok is not legacy_first
        assert effects == ([] if legacy_first else ["exact"])
        assert await p.wait == ("legacy" if legacy_first else " accepted verbatim ")


@pytest.mark.asyncio
async def test_reentrant_legacy_consumer_cannot_steal_reserved_future(monkeypatch):
    async with pending_runtime(monkeypatch) as p:
        nested = []
        result = await p.reply(before_effect=lambda: nested.append(
            p.avatar.submit_human_reply("phase:human:0", "nested legacy")))
        assert result.ok and nested == [False]
        assert await p.wait == " accepted verbatim "


@pytest.mark.asyncio
@pytest.mark.parametrize("closed", ["entry", "gate", "paused", "aborted", "settled", "cancelled", "unregistered"])
async def test_closed_runtime_or_run_does_not_receive_or_restore(monkeypatch, closed):
    async with pending_runtime(monkeypatch) as p:
        if closed == "entry":
            p.entry.closing = True
        elif closed == "gate":
            await p.entry.interact_gate.close_and_drain()
        elif closed == "paused":
            p.entry.state = RuntimeState.PAUSED
        elif closed == "aborted":
            p.handle.abort_event.set()
        elif closed == "settled":
            p.record.execution_settled = True
        elif closed == "cancelled":
            p.record.cancellation_requested = True
        else:
            p.controller.deregister("task")
        effects = []
        assert not (await p.reply(before_effect=lambda: effects.append("forbidden"))).ok
        assert effects == [] and not p.wait.done()
        assert not p.avatar._pending_human["phase:human:0"].done()


@pytest.mark.asyncio
async def test_missing_avatar_owner_is_not_lazily_created(monkeypatch):
    async with pending_runtime(monkeypatch) as p:
        p.backend._session_mgr = None
        try:
            assert not (await p.reply(before_effect=lambda: pytest.fail("callback"))).ok
            assert p.backend._session_mgr is None
            assert not p.wait.done()
        finally:
            p.backend._session_mgr = p.avatar


@pytest.mark.asyncio
async def test_aborted_avatar_rejects_stale_reply(monkeypatch):
    async with pending_runtime(monkeypatch) as p:
        await p.avatar.abort_all()
        result = await p.reply(before_effect=lambda: pytest.fail("callback"))
        assert not result.ok and p.avatar._pending_human == {}
        assert not p.avatar.submit_human_reply("phase:human:0", "legacy")


@pytest.mark.asyncio
async def test_session_close_fences_input_before_waiting_for_turn_lock(monkeypatch):
    async with pending_runtime(monkeypatch) as p:
        assert p.state.lock.locked()
        close = asyncio.create_task(p.avatar.close_session("human"))
        try:
            await asyncio.sleep(0)
            assert not (await p.reply(before_effect=lambda: pytest.fail("callback during close"))).ok
            await asyncio.wait_for(close, 1)
            assert "human" not in p.avatar._sessions
            assert p.wait.cancelled()
        finally:
            close.cancel()
            await asyncio.gather(close, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["pool", "controller", "record", "replaced_record", "avatar_scope"])
async def test_missing_or_replaced_owner_rejects_without_reconstruction(monkeypatch, missing):
    async with pending_runtime(monkeypatch) as p:
        if missing == "pool":
            await p.manager.pool.remove("team")
        elif missing == "controller":
            p.entry.agent.harness = None
        elif missing in {"record", "replaced_record"}:
            replacement = None if missing == "record" else AsyncToolRecord("task", "swarmflow", "replacement")
            p.handle.native.async_tool_runtime.get = lambda _: replacement
        else:
            p.avatar._run_id = "other"
        assert not (await p.reply(before_effect=lambda: pytest.fail("forbidden callback"))).ok
        assert not p.wait.done() and p.backend._session_mgr is p.avatar


@pytest.mark.asyncio
async def test_ambiguous_run_rejects_without_picking_a_handle(monkeypatch):
    async with pending_runtime(monkeypatch) as p:
        p.controller.register(SwarmflowRunHandle(
            "duplicate", asyncio.Event(), p.backend, p.handle.native, lambda: pytest.fail("relaunch")))
        result = await p.reply(before_effect=lambda: pytest.fail("forbidden callback"))
        assert not result.ok and result.reason == "ambiguous_run"
        assert not p.wait.done()


@pytest.mark.asyncio
async def test_awaitable_callback_is_rejected_without_consuming(monkeypatch):
    async with pending_runtime(monkeypatch) as p:
        effects = []

        async def callback():
            effects.append("must not execute")

        with pytest.raises(TypeError, match="synchronous"):
            await p.reply(before_effect=callback)
        assert effects == [] and not p.wait.done()
        assert (await p.reply()).ok


@pytest.mark.asyncio
async def test_timeout_rejects_late_receipt_and_legacy_input(monkeypatch):
    async with pending_runtime(monkeypatch, timeout=0.01) as p:
        assert await p.wait is None
        assert not (await p.reply(before_effect=lambda: pytest.fail("late callback"))).ok
        assert not p.avatar.submit_human_reply("phase:human:0", "late legacy")
        assert p.avatar._pending_human == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["pause", "stop_team", "finalize"])
async def test_team_lifecycle_fences_before_awaiting_teardown(monkeypatch, operation):
    async with pending_runtime(monkeypatch) as p:
        entered, release = asyncio.Event(), asyncio.Event()

        async def teardown():
            entered.set()
            await release.wait()
            return False

        p.entry.agent.pause_coordination = teardown
        p.entry.agent.stop_coordination = teardown
        p.entry.agent.is_shutdown_requested = teardown
        p.entry.agent.lifecycle = "persistent"
        task = asyncio.create_task(getattr(p.manager, operation)(team_name="team", session_id="session"))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            assert p.entry.closing
            assert not (await p.reply(before_effect=lambda: pytest.fail("closing callback"))).ok
            # A legacy event already published before the lifecycle request can
            # arrive after its early fence. The original consumer must reject it.
            await p.avatar._on_reply_event(EventMessage(
                event_type=TeamEvent.WORKFLOW_HUMAN_REPLY,
                payload={"correlation_id": "phase:human:0", "answer": "in-flight legacy"}, sender_id="user",
            ))
            assert not p.avatar._pending_human["phase:human:0"].done()
            assert not p.wait.done()
        finally:
            release.set()
            await task


@pytest.mark.asyncio
async def test_two_live_runs_with_same_correlation_receive_only_their_own_answer(monkeypatch):
    async with pending_runtime(monkeypatch) as p:
        ready = asyncio.Event()
        other = TeamWorkerBackend(model=None, team_name="team", session_id="session", run_id="run2",
                                  on_human_prompt=lambda *_: ready.set())
        avatar = other._sessions()
        state = _SessionState("human", None, None, "other-human", SwarmflowBudgetRail(avatar._budget))
        avatar._sessions["other-human"] = state
        wait = asyncio.create_task(avatar._await_human_reply(state, "other?", {}, "phase:human:0"))
        record = AsyncToolRecord("other-task", "swarmflow", "other")
        native = SimpleNamespace(async_tool_runtime=SimpleNamespace(get=lambda _: record))
        p.controller.register(SwarmflowRunHandle("other-task", asyncio.Event(), other, native, lambda: None))
        try:
            await asyncio.wait_for(ready.wait(), 1)
            assert (await p.reply(answer="first")).ok
            assert not avatar._pending_human["phase:human:0"].done()
            assert (await p.reply(run_id="run2", answer="second")).ok
            assert await p.wait == "first" and await wait == "second"
        finally:
            wait.cancel()
            await asyncio.gather(wait, return_exceptions=True)
            await other.aclose()


@pytest.mark.asyncio
async def test_callback_exception_after_reentrant_legacy_rejection_preserves_retry(monkeypatch):
    async with pending_runtime(monkeypatch) as p:
        def reject():
            assert not p.avatar.submit_human_reply("phase:human:0", "stolen")
            raise ValueError("revoke")

        with pytest.raises(ValueError, match="revoke"):
            await p.reply(before_effect=reject)
        assert not p.wait.done() and not p.avatar._pending_human["phase:human:0"].done()
        assert (await p.reply()).ok


@pytest.mark.asyncio
async def test_runner_with_no_runtime_manager_does_not_create_one(monkeypatch):
    impl = team_runner._TeamRunnerMixin()
    impl._team_runtime_manager = None
    monkeypatch.setattr(team_runner, "_global_runner", lambda: impl)
    result = await Runner.reply_swarmflow_human(
        session_id="s", team_name="t", run_id="r", correlation_id="c", answer="yes",
        before_effect=lambda: pytest.fail("callback"),
    )
    assert not result.ok and result.reason == "not_active"
    assert impl._team_runtime_manager is None


@pytest.mark.asyncio
async def test_consumer_guard_survives_pause_resume_but_never_owner_replacement(monkeypatch):
    async with pending_runtime(monkeypatch) as p:
        p.manager.bind_swarmflow_human_reply_admission(p.entry.agent)
        p.entry.closing = True
        p.entry.state = RuntimeState.PAUSED
        assert not p.avatar.submit_human_reply("phase:human:0", "paused")
        monkeypatch.setattr(p.manager, "_pre_run_with_inputs", AsyncMock())
        await p.manager._apply_action(
            RunAction(RunActionKind.RESUME_FROM_PAUSE, False), spec=SimpleNamespace(team_name="team"),
            team_session=SimpleNamespace(get_session_id=lambda: "session"), pool_entry=p.entry, inputs=None,
        )
        assert (await p.reply()).ok
        assert await p.wait == " accepted verbatim "

    async with pending_runtime(monkeypatch) as p:
        p.manager.bind_swarmflow_human_reply_admission(p.entry.agent)
        replacement = ActiveTeam("team", p.entry.agent, "session")
        await p.manager.pool.add(replacement)
        # Even re-binding the controller cannot lend the old backend a new owner.
        p.manager.bind_swarmflow_human_reply_admission(replacement.agent)
        assert not p.avatar.submit_human_reply("phase:human:0", "orphan")
        assert not (await p.reply()).ok
        assert not p.avatar._pending_human["phase:human:0"].done()


@pytest.mark.asyncio
async def test_shared_controller_fences_only_original_team_scope_and_late_runs(monkeypatch):
    async with pending_runtime(monkeypatch) as p:
        p.manager.bind_swarmflow_human_reply_admission(p.entry.agent)
        other_backend = TeamWorkerBackend(model=None, team_name="other", session_id="session", run_id="run")
        other_entry = ActiveTeam("other", p.entry.agent, "session")
        await p.manager.pool.add(other_entry)
        record = AsyncToolRecord("other", "swarmflow", "other")
        native = SimpleNamespace(async_tool_runtime=SimpleNamespace(get=lambda _: record))
        p.controller.register(SwarmflowRunHandle("other", asyncio.Event(), other_backend, native, lambda: None))
        p.entry.closing = True
        # A run registered after the fence also reads the existing pool state.
        late = TeamWorkerBackend(model=None, team_name="team", session_id="session", run_id="late")
        p.controller.register(SwarmflowRunHandle("late", asyncio.Event(), late, native, lambda: None))
        assert not late._human_reply_admission()
        assert other_backend._human_reply_admission()
        assert not p.avatar.submit_human_reply("phase:human:0", "closed")


@pytest.mark.parametrize("role", list(TeamRole))
def test_only_leader_gets_a_default_controller(role):
    native = SimpleNamespace()
    harness = TeamHarness(None, None, native, role=role, member_name="member")
    if role is TeamRole.LEADER:
        assert isinstance(harness.background_task_controller, BackgroundTaskController)
        assert native.background_task_controller is harness.background_task_controller
    else:
        assert harness.background_task_controller is None
        assert not hasattr(native, "background_task_controller")


@pytest.mark.asyncio
@pytest.mark.parametrize("paused", [False, True])
async def test_explicit_controller_can_replace_empty_default_but_not_owned_runs(paused):
    native = SimpleNamespace(async_tool_runtime=SimpleNamespace(
        get=lambda _: AsyncToolRecord("task", "swarmflow", "test"), cancel=AsyncMock()))
    harness = TeamHarness(None, None, native, role=TeamRole.LEADER, member_name="leader")
    explicit = BackgroundTaskController()
    harness.set_background_task_controller(explicit)
    assert harness.background_task_controller is explicit and native.background_task_controller is explicit
    explicit.register(SwarmflowRunHandle("task", asyncio.Event(), SimpleNamespace(abort_sessions=AsyncMock()),
                                        native, lambda: None))
    if paused:
        await explicit.pause()
    harness.set_background_task_controller(explicit)  # Same owner is idempotent.
    with pytest.raises(ValueError, match="owns runs"):
        harness.set_background_task_controller(BackgroundTaskController())
    assert harness.background_task_controller is explicit and native.background_task_controller is explicit


@pytest.mark.asyncio
async def test_native_rebuild_keeps_original_default_controller(monkeypatch):
    from openjiuwen.agent_teams.harness import team_harness
    from openjiuwen.agent_teams.harness.state import HarnessState

    old = SimpleNamespace(state=HarnessState.TERMINATED)
    harness = TeamHarness(None, None, old, role=TeamRole.LEADER, member_name="leader")
    controller = harness.background_task_controller
    rebuilt = SimpleNamespace(state=HarnessState.IDLE, start=AsyncMock())
    monkeypatch.setattr(team_harness, "NativeHarness", lambda *_: rebuilt)
    child = SimpleNamespace(pre_run=AsyncMock())
    monkeypatch.setattr(harness, "_make_child_session", lambda _: child)
    await harness.start(team_session=SimpleNamespace(get_session_id=lambda: "session"))
    assert harness.background_task_controller is controller
    assert rebuilt.background_task_controller is controller
    rebuilt.start.assert_awaited_once_with(session=child)


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit_controller", [False, True])
async def test_default_runner_stream_registers_real_swarmflow_and_receives_human_input(
    monkeypatch, tmp_path, explicit_controller,
):
    """Real Runner/Tool/engine/backend/Future chain; only activation and LLM are stubbed."""
    from openjiuwen.agent_teams.context import reset_session_id, set_session_id
    from openjiuwen.agent_teams.schema.deep_agent_spec import DeepAgentSpec
    from openjiuwen.agent_teams.workflow import runner as workflow_runner
    from openjiuwen.agent_teams.workflow.concurrency import ConcurrencyGovernor, ConcurrencyLimits
    from openjiuwen.agent_teams.workflow.engine.backends.base import AgentResult
    from openjiuwen.agent_teams.workflow.tool_swarmflow import SwarmflowTool

    script = tmp_path / "human.py"
    script.write_text(
        "from openjiuwen.agent_teams.workflow.engine.facade import human\n"
        "META = {'name': 'default-human'}\n"
        "async def run(args):\n    return await human('approve?', label='person')\n", encoding="utf-8")
    monkeypatch.setattr(workflow_runner, "_resolve_journal_path", lambda *_: str(tmp_path / "journal.jsonl"))
    ready, captured = asyncio.Event(), {}

    async def start_avatar(manager, state, opts):
        original = manager._on_human_prompt

        def prompt(*args):
            original(*args)
            captured.update(avatar=manager, correlation=args[1])
            ready.set()

        manager._on_human_prompt = prompt

    monkeypatch.setattr(AvatarSessionManager, "_start_avatar", start_avatar)
    monkeypatch.setattr(AvatarSessionManager, "_agent_turn", AsyncMock(return_value=AgentResult(text="formatted")))
    runtime = AsyncToolRuntime(inject=AsyncMock())
    native = SimpleNamespace(model=None, async_tool_runtime=runtime, launch_async_tool=runtime.launch)
    harness = TeamHarness(None, None, native, role=TeamRole.LEADER, member_name="leader")
    controller = BackgroundTaskController() if explicit_controller else harness.background_task_controller
    tool = SwarmflowTool(
        parent_agent=native, messager=None, team_name="team", model_resolver=lambda _: None,
        human_base_spec=DeepAgentSpec(tools=[]),
        concurrency_governor=ConcurrencyGovernor(ConcurrencyLimits(max_workflows=1, max_agents_total=1),
                                                agents_per_run_cap=1),
    )

    async def stream(inputs, *, session):
        token = set_session_id("session")
        try:
            launched = await tool.invoke({"script_path": str(script)})
            assert launched.success, launched.error
            captured.update(launched.data)
            await asyncio.wait_for(ready.wait(), 2)
            yield launched.data
            await runtime.wait(launched.data["task_id"], 2)
        finally:
            reset_session_id(token)

    agent = SimpleNamespace(harness=harness, stream=stream, lifecycle="persistent",
                            is_shutdown_requested=AsyncMock(return_value=False), pause_coordination=AsyncMock(),
                            set_background_task_controller=harness.set_background_task_controller)
    manager = TeamRuntimeManager()
    await manager.pool.add(ActiveTeam("team", agent, "session"))
    session = SimpleNamespace(get_session_id=lambda: "session", post_run=AsyncMock())
    monkeypatch.setattr(manager, "activate", AsyncMock(return_value=TeamRuntimeActivation(
        agent=agent, session=session, action=RunAction(RunActionKind.CREATE, True))))
    impl = team_runner._TeamRunnerMixin()
    impl._team_runtime_manager = manager
    impl._resolve_team_agent_spec = AsyncMock(return_value=SimpleNamespace(team_name="team"))
    impl._enter_root_task_group_context = lambda: None
    impl._exit_root_task_group_context = lambda _: None
    impl._maybe_attach_observability = lambda _: None
    impl._maybe_finalize_trace = lambda _: None
    monkeypatch.setattr(team_runner, "_global_runner", lambda: impl)
    # The default case deliberately omits the controller, as production does.
    controller_args = {"background_task_controller": controller} if explicit_controller else {}
    output = Runner.run_agent_team_streaming(agent_team="team", inputs="begin", session="session", **controller_args)
    try:
        await anext(output)  # Runtime-ready marker.
        await anext(output)  # Real Tool launch and engine human prompt.
        assert harness.background_task_controller is controller
        assert controller.has_owned_runs()
        handle = controller._active[captured["task_id"]]
        assert handle.backend._session_mgr is captured["avatar"]
        future = captured["avatar"]._pending_human[captured["correlation"]]
        receipt = await Runner.reply_swarmflow_human(
            session_id="session", team_name="team", run_id=captured["run_id"],
            correlation_id=captured["correlation"], answer="actual input",
        )
        assert receipt.ok and future.result() == "actual input"
        with pytest.raises(StopAsyncIteration):
            await anext(output)
        assert not controller.has_owned_runs()
    finally:
        await output.aclose()
        for task_id in tuple(runtime.registry):
            await runtime.cancel(task_id)
