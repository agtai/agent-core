"""Real Goal store, scheduler and output lease admission through DeepAgent."""

import asyncio
from copy import deepcopy
from unittest.mock import AsyncMock, Mock

import pytest

from openjiuwen.core.single_agent.schema.agent_card import AgentCard
from openjiuwen.harness.deep_agent import DeepAgent
from openjiuwen.harness.goal.manager import GoalManager
from openjiuwen.harness.goal.schema import GoalOperationError, GoalStatus
from openjiuwen.harness.goal.store import SESSION_GOAL_RECORD_KEY, SessionGoalStore
from openjiuwen.harness.schema.interaction import InteractionPhase, RoundWorkItem


@pytest.fixture
def owner():
    agent = DeepAgent(AgentCard(name="goal-owner", description="test"))
    agent._interaction_started = True
    state = {}
    session = Mock()
    session.get_session_id.return_value = "session"
    session.get_state.side_effect = state.get
    session.update_state.side_effect = state.update
    session.commit = AsyncMock()
    agent._cancel_active_round = AsyncMock()
    agent._notify_work = Mock()
    agent._emit_interaction_event = Mock()
    agent.goal_manager = GoalManager(
        store=SessionGoalStore(session), event_manager=agent._event_manager,
        control_lock=agent._interaction_control_lock, has_output_stream=agent.has_output_stream,
        cancel_active_round=agent._cancel_active_round, notify_work=agent._notify_work,
        emit_event=agent._emit_interaction_event,
    )
    return agent, session, state


def target(record):
    return {"expected_goal_id": record.goal_id, "expected_control_revision": record.control_revision}


def effects(owner):
    agent, session, state = owner
    return (deepcopy(state), session.update_state.call_count, session.commit.await_count,
            agent._cancel_active_round.await_count, agent._notify_work.call_count,
            agent._emit_interaction_event.call_count, agent._interaction_output.current_token())


@pytest.mark.asyncio
async def test_output_ready_observes_exact_borrowed_lease_before_goal_effects(owner):
    agent, _, _ = owner
    notices = []
    def ready(token, acquired):
        assert agent._interaction_control_lock.locked()
        assert agent.goal_manager.peek() is None
        assert not agent._event_manager.has_pending_work()
        notices.append((token, acquired))
    stream = await agent.attach_output(on_output_ready=ready)
    record, borrowed = await agent.set_goal("Retained objective", on_output_ready=ready)
    assert borrowed is None and record.objective == "Retained objective"
    assert notices == [(agent._interaction_output.current_token(), True),
                       (agent._interaction_output.current_token(), False)]
    await stream.close(abort_active_round=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("attached", [False, True])
async def test_output_ready_rejection_preserves_goal_queue_and_old_reader(owner, attached):
    agent, _, _ = owner
    stream = await agent.attach_output() if attached else None
    user = RoundWorkItem.user(request_id="user", inputs={"query": "Keep this work"})
    agent._event_manager.push_user(user)
    before = effects(owner)
    with pytest.raises(PermissionError, match="owner disconnected"):
        await agent.set_goal("Must not start", on_output_ready=Mock(side_effect=PermissionError("owner disconnected")))
    assert effects(owner) == before
    assert agent._event_manager.next_work() is user
    if stream is not None:
        await agent._interaction_output.emit("preserved")
        assert await anext(stream) == "preserved"
        await stream.close(abort_active_round=False)


@pytest.mark.asyncio
async def test_stale_goal_never_notifies_output_owner(owner):
    agent, _, _ = owner
    record, stream = await agent.set_goal("Original")
    ready = Mock()
    before = effects(owner)
    with pytest.raises(GoalOperationError):
        await agent.resume_goal(expected_goal_id=record.goal_id,
            expected_control_revision=record.control_revision + 1, on_output_ready=ready)
    ready.assert_not_called()
    assert effects(owner) == before
    await stream.close(abort_active_round=False)


@pytest.mark.asyncio
async def test_attach_output_ready_failure_does_not_discard_work_or_keep_new_lease(owner):
    agent, _, _ = owner
    user = RoundWorkItem.user(request_id="user", inputs={"query": "Keep this work"})
    agent._event_manager.push_user(user)
    before = effects(owner)
    with pytest.raises(PermissionError):
        await agent.attach_output(on_output_ready=Mock(side_effect=PermissionError("closed")))
    assert effects(owner) == before
    assert agent._event_manager.next_work() is user


@pytest.mark.asyncio
async def test_finishing_attach_cannot_ensure_goal_work_without_host_admission(owner):
    agent, _, _ = owner
    record, stream = await agent.set_goal("Active goal")
    assert agent._event_manager.next_work().context["goal_id"] == record.goal_id
    await agent._interaction_output.finish_current()
    before = effects(owner)
    ready = Mock(side_effect=PermissionError("owner closing"))
    with pytest.raises(RuntimeError, match="output_unavailable"):
        await agent.attach_output(on_output_ready=ready)
    assert effects(owner) == before
    assert not agent._event_manager.has_pending_work()
    ready.assert_not_called()
    with pytest.raises(StopAsyncIteration):
        await anext(stream)


@pytest.mark.asyncio
async def test_set_consumes_real_output_and_replacement_keeps_original_consumer(owner):
    agent, _, _ = owner
    created, stream = await agent.set_goal("First objective")
    assert stream is not None
    first_token = agent._interaction_output.current_token()
    replaced, other_stream = await agent.set_goal("Replacement", overwrite_confirmed=True, **target(created))
    assert other_stream is None
    assert agent._interaction_output.current_token() == first_token
    assert replaced.goal_id != created.goal_id
    work = agent._event_manager.next_work()
    assert work.context["goal_id"] == replaced.goal_id
    assert agent._event_manager.next_work() is None
    # Exercise the actual reader transport; no Provider/model is credited here.
    await agent._interaction_output.emit({"goal_id": replaced.goal_id})
    assert await anext(stream) == {"goal_id": replaced.goal_id}
    agent._cancel_active_round.assert_awaited_once_with(
        expected_run_kind="goal", expected_goal_id=created.goal_id, reason="goal_overwrite")
    await stream.close(abort_active_round=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["wrong_id", "stale_revision", "invalid_target", "already_exists", "revoked"])
@pytest.mark.parametrize("attached", [False, True])
async def test_rejected_set_preserves_existing_goal_user_queue_and_reader(owner, failure, attached):
    agent, _, _ = owner
    existing = await agent.goal_manager.set("Original")
    stream = await agent.attach_output() if attached else None
    user_work = RoundWorkItem.user(request_id="user", inputs={"query": "User task"})
    agent._event_manager.push_user(user_work)
    controls = {"overwrite_confirmed": True, **target(existing)}
    if failure == "wrong_id":
        controls["expected_goal_id"] = "other"
    elif failure == "stale_revision":
        controls["expected_control_revision"] += 1
    elif failure == "invalid_target":
        controls["expected_control_revision"] = True
    elif failure == "already_exists":
        controls = {}
    else:
        controls["before_effect"] = Mock(side_effect=PermissionError("revoked"))
    before = effects(owner)
    with pytest.raises((GoalOperationError, PermissionError)):
        await agent.set_goal("Forbidden replacement", **controls)
    assert effects(owner) == before
    assert agent._event_manager.next_work() is user_work
    goal_work = agent._event_manager.next_work()
    assert (goal_work is not None) is attached
    if goal_work is not None:
        assert goal_work.context["goal_id"] == existing.goal_id
    if stream is not None:
        await stream.close(abort_active_round=False)


@pytest.mark.asyncio
async def test_resume_reuses_attempt_and_does_not_queue_duplicate_while_finishing(owner):
    agent, _, _ = owner
    created, stream = await agent.set_goal("Finish report")
    work = agent._event_manager.next_work()
    agent._event_manager.mark_started(work)
    await agent.goal_manager.begin_attempt(goal_id=created.goal_id, revision=created.revision)
    paused = await agent.goal_manager.pause(**target(created))
    resumed, other_stream = await agent.resume_goal(**target(paused))
    assert resumed.status is GoalStatus.ACTIVE
    assert resumed.revision == created.revision
    assert other_stream is None
    assert agent._event_manager.active_work is work
    assert agent._event_manager.next_work() is None
    agent._cancel_active_round.assert_not_awaited()
    await stream.close(abort_active_round=False)


@pytest.mark.asyncio
async def test_resume_acquires_reader_for_idle_paused_goal(owner):
    agent, _, _ = owner
    created = await agent.goal_manager.set("Resume report")
    paused = await agent.goal_manager.pause(**target(created))
    resumed, stream = await agent.resume_goal(**target(paused))
    assert stream is not None
    assert resumed.status is GoalStatus.ACTIVE
    assert agent._event_manager.next_work().context == {
        "goal_id": resumed.goal_id, "revision": resumed.revision, "session_id": "session", "reset_loop": True}
    assert agent._event_manager.next_work() is None
    await stream.close(abort_active_round=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["target", "revoked", "terminated"])
async def test_admission_rechecks_after_waiting_for_goal_owner_lock(owner, failure):
    agent, _, _ = owner
    current = await agent.goal_manager.set("Current")
    allowed = True

    def admit():
        if not allowed:
            raise PermissionError("revoked")

    async with agent._interaction_control_lock:
        pending = asyncio.create_task(agent.resume_goal(**target(current), before_effect=admit))
        await asyncio.sleep(0)
        if failure == "target":
            current.control_revision += 1
            agent.goal_manager._store.save(current)
        elif failure == "revoked":
            allowed = False
        else:
            agent._interaction_phase = InteractionPhase.TERMINATED
        before = effects(owner)
    with pytest.raises((GoalOperationError, PermissionError, RuntimeError)):
        await pending
    assert effects(owner) == before
    assert not agent._event_manager.has_pending_work()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["persist", "cancel"])
async def test_post_admission_failure_releases_only_new_lease_and_keeps_user_work(owner, failure):
    agent, session, _ = owner
    user_work = RoundWorkItem.user(request_id="user", inputs={"query": "Unrelated user task"})
    agent._event_manager.push_user(user_work)
    error = OSError("commit failed") if failure == "persist" else asyncio.CancelledError()
    session.commit.side_effect = error
    with pytest.raises(type(error)):
        await agent.set_goal("Admitted goal")
    assert not agent.has_output_stream()
    assert agent._event_manager.next_work() is user_work
    assert agent._event_manager.next_work() is None
    # Save precedes the failing commit. The exception is an uncertain outcome,
    # not a claim that the Goal never existed or that its execution completed.
    assert agent.goal_manager.peek().objective == "Admitted goal"
    agent._cancel_active_round.assert_not_awaited()
    agent._notify_work.assert_not_called()
    agent._emit_interaction_event.assert_not_called()


@pytest.mark.asyncio
async def test_corrupt_state_is_not_repaired_by_new_execution_entry(owner):
    agent, _, state = owner
    state[SESSION_GOAL_RECORD_KEY] = "corrupt"
    before = effects(owner)
    with pytest.raises(GoalOperationError, match="Stored Goal"):
        await agent.set_goal("Must not replace corrupt state")
    assert effects(owner) == before
    assert not agent._event_manager.has_pending_work()


@pytest.mark.asyncio
async def test_new_goal_waits_for_finishing_reader_without_losing_either_stream(owner):
    agent, _, _ = owner
    old = await agent.attach_output()
    await agent._interaction_output.emit({"text": "last ordinary response"})
    await agent._interaction_output.finish_current()
    before = effects(owner)
    ready = Mock()
    pending = asyncio.create_task(agent.set_goal("New goal", on_output_ready=ready))
    await asyncio.sleep(0)
    assert not pending.done()
    assert effects(owner) == before
    ready.assert_not_called()
    assert not agent._event_manager.has_pending_work()
    assert await anext(old) == {"text": "last ordinary response"}
    with pytest.raises(StopAsyncIteration):
        await anext(old)
    created, stream = await asyncio.wait_for(pending, 1)
    assert stream is not None
    ready.assert_called_once_with(agent._interaction_output.current_token(), True)
    assert agent._event_manager.next_work().context["goal_id"] == created.goal_id
    await agent._interaction_output.emit({"text": "new goal response"})
    assert await anext(stream) == {"text": "new goal response"}
    await old.close()  # A stale handle cannot cancel the new Goal.
    agent._cancel_active_round.assert_not_awaited()
    await stream.close(abort_active_round=False)


@pytest.mark.asyncio
async def test_waiting_for_finishing_reader_does_not_freeze_goal_controls_or_retarget(owner):
    agent, _, _ = owner
    original = await agent.goal_manager.set("Original")
    paused = await agent.goal_manager.pause(**target(original))
    old = await agent.attach_output()
    await agent._interaction_output.finish_current()
    pending = asyncio.create_task(agent.resume_goal(**target(paused)))
    await asyncio.sleep(0)
    await asyncio.wait_for(agent.goal_manager.clear(**target(paused)), 1)
    before = effects(owner)
    with pytest.raises(StopAsyncIteration):
        await anext(old)
    with pytest.raises(GoalOperationError, match="target has changed"):
        await asyncio.wait_for(pending, 1)
    assert effects(owner)[:-1] == before[:-1]
    assert not agent.has_output_stream()
    assert not agent._event_manager.has_pending_work()


@pytest.mark.asyncio
async def test_repeated_cancellation_waits_for_actual_lease_release(owner):
    agent, session, _ = owner
    entered = asyncio.Event()
    locked = asyncio.Event()
    release = asyncio.Event()

    async def committing():
        entered.set()
        await asyncio.Event().wait()

    async def competing_control():
        async with agent._interaction_control_lock:
            locked.set()
            await release.wait()

    session.commit.side_effect = committing
    pending = asyncio.create_task(agent.set_goal("Admitted"))
    await entered.wait()
    competitor = asyncio.create_task(competing_control())
    await asyncio.sleep(0)
    pending.cancel()
    await locked.wait()
    pending.cancel()
    await asyncio.sleep(0)
    assert not pending.done(), "caller retains ownership until its new lease is released"
    release.set()
    await competitor
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(pending, 1)
    assert not agent.has_output_stream()
    session.commit.side_effect = None
    resumed, stream = await agent.resume_goal(**target(agent.goal_manager.peek()))
    assert stream is not None
    assert agent._event_manager.next_work().context["goal_id"] == resumed.goal_id
    await stream.close(abort_active_round=False)
