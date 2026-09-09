# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Strict continuation through the real Runner, workflow, graph and checkpoint owners."""

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio

from openjiuwen.core.runner import Runner
from openjiuwen.core.runner.callback import AsyncCallbackFramework
from openjiuwen.core.session import InteractiveInput
from openjiuwen.core.session.checkpointer import CheckpointerFactory
from openjiuwen.core.session.checkpointer.base import (
    Checkpointer, build_key_with_namespace, SESSION_NAMESPACE_WORKFLOW,
)
from openjiuwen.core.session.checkpointer.inmemory import InMemoryCheckpointer
from openjiuwen.core.session.checkpointer.persistence import PersistenceCheckpointerProvider
from openjiuwen.core.workflow import (
    Workflow, WorkflowCard, WorkflowComponent, Start, End, WorkflowExecutionState,
    WorkflowResumeGuard, WorkflowResumeError,
)


class CountStart(Start):
    def __init__(self, counts):
        super().__init__()
        self.counts = counts

    async def invoke(self, inputs, session, context):
        self.counts["start"] += 1
        return inputs


class Ask(WorkflowComponent):
    def __init__(self, counts, rounds=1):
        super().__init__()
        self.counts = counts
        self.rounds = rounds

    async def invoke(self, inputs, session, context):
        self.counts["ask"] += 1
        answers = [await session.interact(f"Answer question {index}") for index in range(self.rounds)]
        self.counts["answered"] += 1
        return {"answer": answers[-1]}


@pytest_asyncio.fixture(params=["memory", "sqlite"])
async def pending(request, monkeypatch, tmp_path):
    monkeypatch.setattr(Runner, "callback_framework", AsyncCallbackFramework())
    provider = InMemoryCheckpointer() if request.param == "memory" else await PersistenceCheckpointerProvider().create(
        {"db_type": "sqlite", "db_path": str(tmp_path / "checkpoint.db")})
    monkeypatch.setattr(CheckpointerFactory, "_default_checkpointer", provider)
    counts = {"start": 0, "ask": 0, "answered": 0}
    workflow = Workflow(WorkflowCard(id="review-workflow", name="Strict resume fixture"))
    workflow.set_start_comp("start", CountStart(counts))
    ask = Ask(counts)
    workflow.add_workflow_comp("ask", ask)
    workflow.set_end_comp("end", End(), inputs_schema={"answer": "${ask.answer}"})
    workflow.add_connection("start", "ask")
    workflow.add_connection("ask", "end")
    sid = uuid4().hex
    output = await Runner.run_workflow(workflow, {"query": "test"}, session=sid)
    assert output.state is WorkflowExecutionState.INPUT_REQUIRED
    assert output.result[0].payload.id == "ask"
    assert counts == {"start": 1, "ask": 1, "answered": 0}
    value = SimpleNamespace(workflow=workflow, provider=provider, sid=sid, counts=counts,
                            kind=request.param, ask=ask)
    yield value
    await provider.release(sid)
    if request.param == "sqlite":
        await provider._kv_store.engine.dispose()


async def resume(pending, guard=lambda: None, *, sid=None, inputs=None):
    if inputs is None:
        inputs = InteractiveInput()
        inputs.update("ask", "accepted answer")
    return await Runner.run_workflow(pending.workflow, inputs, session=sid or pending.sid,
                                     resume_guard=WorkflowResumeGuard(before_effect=guard))


@pytest.mark.asyncio
async def test_strict_resume_continues_actual_pending_node_once(pending):
    seen = []
    result = await resume(pending, lambda: seen.append("admitted"))
    assert result.state is WorkflowExecutionState.COMPLETED
    assert pending.counts == {"start": 1, "ask": 2, "answered": 1}
    assert seen == ["admitted"]


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["state", "graph", "wrong_session", "wrong_workflow"])
async def test_missing_proof_never_restarts_or_injects_input(pending, monkeypatch, missing):
    b = pending
    if missing == "graph":
        await b.provider.graph_store().delete(b.sid, "review-workflow")
    if missing == "state":
        if b.kind == "memory":
            await b.provider._workflow_stores[b.sid].clear("review-workflow")
        else:
            await b.provider._workflow_storage.clear("review-workflow", b.sid)
    if missing == "wrong_workflow":
        monkeypatch.setattr(b.workflow.card, "id", "wrong-workflow")
    writes = watch_restoration(monkeypatch)
    admitted = []
    before = dict(b.counts)
    error = None
    try:
        await resume(b, lambda: admitted.append(True),
                     sid="wrong-session" if missing == "wrong_session" else None)
    except Exception as caught:
        error = caught
    assert b.counts == before
    assert writes == [] and admitted == []
    assert error is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [PermissionError, TimeoutError, RecursionError])
async def test_guard_rejection_has_zero_node_effect(pending, monkeypatch, error_type):
    error = error_type("revoked while loading checkpoint")
    writes = watch_restoration(monkeypatch)

    def reject():
        raise error

    before = dict(pending.counts)
    caught = None
    try:
        await resume(pending, reject)
    except Exception as exception:
        caught = exception
    assert pending.counts == before
    assert writes == []
    assert caught is error


def watch_restoration(monkeypatch):
    from openjiuwen.core.session.state.workflow_state import InMemoryState

    writes = []
    for name in ("set_state", "set_updates"):
        original = getattr(InMemoryState, name)

        def record(self, value, _name=name, _original=original):
            writes.append(_name)
            return _original(self, value)

        monkeypatch.setattr(InMemoryState, name, record)
    return writes


def delay_snapshot(pending, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    if pending.kind == "memory":
        graph = pending.provider.graph_store()
        original = graph.get

        async def get(*args):
            result = await original(*args)
            entered.set()
            await release.wait()
            return result

        monkeypatch.setattr(graph, "get", get)
    else:
        kv = pending.provider._kv_store
        original = kv.pipeline

        def pipeline():
            value = original()
            execute = value.execute

            async def wait_execute():
                result = await execute()
                entered.set()
                await release.wait()
                return result

            value.execute = wait_execute
            return value

        monkeypatch.setattr(kv, "pipeline", pipeline)
    return entered, release


@pytest.mark.asyncio
async def test_permission_revoked_during_actual_snapshot_read(pending, monkeypatch):
    writes = watch_restoration(monkeypatch)
    entered, release = delay_snapshot(pending, monkeypatch)
    admitted = []
    revoked = False

    def guard():
        admitted.append("checked")
        if revoked:
            raise PermissionError("revoked")

    before = dict(pending.counts)
    task = asyncio.create_task(resume(pending, guard))
    await asyncio.wait_for(entered.wait(), 5)
    assert admitted == [] and writes == []
    revoked = True
    release.set()
    with pytest.raises(PermissionError, match="revoked"):
        await task
    assert admitted == ["checked"] and writes == [] and pending.counts == before


@pytest.mark.asyncio
async def test_cancel_during_proof_preserves_pending_checkpoint(pending, monkeypatch):
    writes = watch_restoration(monkeypatch)
    with monkeypatch.context() as gate_patch:
        entered, release = delay_snapshot(pending, gate_patch)
        task = asyncio.create_task(resume(pending))
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        # Cleanup must not need another checkpoint operation while proof is pending.
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert writes == []
    assert pending.counts == {"start": 1, "ask": 1, "answered": 0}
    assert (await resume(pending)).state is WorkflowExecutionState.COMPLETED


async def remove_pairing_proof(pending):
    if pending.kind == "memory":
        pending.provider._workflow_stores[pending.sid].resume_proofs.clear()
    else:
        storage = pending.provider._workflow_storage
        key = build_key_with_namespace(pending.sid, SESSION_NAMESPACE_WORKFLOW,
                                       "review-workflow", storage._RESUME_PROOF)
        await pending.provider._kv_store.delete(key)


@pytest.mark.asyncio
async def test_old_checkpoint_is_legacy_only(pending, monkeypatch):
    await remove_pairing_proof(pending)
    writes = watch_restoration(monkeypatch)
    with pytest.raises(WorkflowResumeError, match="checkpoint_unproven"):
        await resume(pending)
    assert writes == []
    inputs = InteractiveInput()
    inputs.update("ask", "legacy answer")
    result = await Runner.run_workflow(pending.workflow, inputs, session=pending.sid)
    assert result.state is WorkflowExecutionState.COMPLETED
    assert pending.counts == {"start": 1, "ask": 2, "answered": 1}


@pytest.mark.asyncio
@pytest.mark.parametrize("part", ["state", "updates", "graph", "mixed_graph"])
async def test_damaged_or_mixed_parts_never_restore(pending, monkeypatch, part):
    from openjiuwen.core.graph.store import create_serializer

    b = pending
    if part in ("graph", "mixed_graph"):
        graph = await b.provider.graph_store().get(b.sid, "review-workflow")
        graph.step += 1
        if part == "graph":
            graph.pending_node.clear()
        await b.provider.graph_store().save(b.sid, "review-workflow", graph)
    elif b.kind == "memory":
        storage = b.provider._workflow_stores[b.sid]
        blobs = storage.state_blobs if part == "state" else storage.state_updates_blobs
        blobs["review-workflow"] = create_serializer("pickle").dumps_typed({"unexpected": "damaged"})
    else:
        storage = b.provider._workflow_storage
        suffix = storage._STATE_BLOBS if part == "state" else storage._UPDATE_BLOBS
        await b.provider._kv_store.set(build_key_with_namespace(
            b.sid, SESSION_NAMESPACE_WORKFLOW, "review-workflow", suffix), b"corrupt checkpoint")
    writes = watch_restoration(monkeypatch)
    before = dict(b.counts)
    admitted = []
    with pytest.raises(WorkflowResumeError):
        await resume(b, lambda: admitted.append(True))
    assert writes == [] and admitted == [] and b.counts == before


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["raw", "empty", "wrong_node", "nested", "extra_node"])
async def test_unsupported_input_scope_has_no_effect(pending, monkeypatch, mode):
    inputs = InteractiveInput("unbound answer") if mode == "raw" else InteractiveInput()
    if mode in ("wrong_node", "nested"):
        inputs.update("other" if mode == "wrong_node" else "ask.inner", "answer")
    if mode == "extra_node":
        inputs.update("ask", "answer")
        inputs.update("other", "answer")
    writes = watch_restoration(monkeypatch)
    with pytest.raises(WorkflowResumeError, match="inputs_unsupported"):
        await resume(pending, inputs=inputs)
    assert writes == [] and pending.counts == {"start": 1, "ask": 1, "answered": 0}


@pytest.mark.asyncio
async def test_prepared_graph_is_consumed_without_second_storage_read(pending, monkeypatch):
    from openjiuwen.core.graph.store import GraphStore

    async def second_read(*args, **kwargs):
        raise AssertionError("Pregel must consume the prepared snapshot")

    monkeypatch.setattr(GraphStore, "get", second_read)
    assert (await resume(pending)).state is WorkflowExecutionState.COMPLETED
    assert pending.counts == {"start": 1, "ask": 2, "answered": 1}


@pytest.mark.asyncio
async def test_unknown_provider_default_is_unsupported(pending, monkeypatch):
    monkeypatch.setattr(pending.provider, "prepare_workflow_resume",
                        Checkpointer.prepare_workflow_resume.__get__(pending.provider))
    writes = watch_restoration(monkeypatch)
    with pytest.raises(WorkflowResumeError, match="strict_resume_unsupported"):
        await resume(pending)
    assert writes == [] and pending.counts == {"start": 1, "ask": 1, "answered": 0}


@pytest.mark.asyncio
async def test_replaced_provider_cannot_restore_cached_graph(pending, monkeypatch):
    monkeypatch.setattr(CheckpointerFactory, "_default_checkpointer", InMemoryCheckpointer())
    writes = watch_restoration(monkeypatch)
    with pytest.raises(WorkflowResumeError, match="owner_mismatch"):
        await resume(pending)
    assert writes == [] and pending.counts == {"start": 1, "ask": 1, "answered": 0}


@pytest.mark.asyncio
async def test_async_guard_is_rejected_before_restoration(pending, monkeypatch):
    writes = watch_restoration(monkeypatch)

    async def unsupported():
        raise AssertionError("must not await this guard")

    with pytest.raises(WorkflowResumeError, match="async_guard_unsupported"):
        await resume(pending, unsupported)
    assert writes == [] and pending.counts == {"start": 1, "ask": 1, "answered": 0}


@pytest.mark.asyncio
@pytest.mark.parametrize("guard_result", [False, True, object()], ids=["false", "true", "object"])
async def test_non_none_guard_result_is_rejected_before_restoration(pending, monkeypatch, guard_result):
    from openjiuwen.core.session.checkpointer.workflow_resume import PreparedWorkflowResume

    applied = []
    original_apply = PreparedWorkflowResume.apply

    def apply(self, session):
        applied.append(session)
        return original_apply(self, session)

    monkeypatch.setattr(PreparedWorkflowResume, "apply", apply)
    writes = watch_restoration(monkeypatch)
    with pytest.raises(WorkflowResumeError) as caught:
        await resume(pending, lambda: guard_result)
    assert caught.value.code == "strict_resume_guard_result_invalid"
    assert applied == [] and writes == []
    assert pending.counts == {"start": 1, "ask": 1, "answered": 0}


@pytest.mark.asyncio
async def test_strict_requires_interactive_input_before_any_start(pending, monkeypatch):
    writes = watch_restoration(monkeypatch)
    with pytest.raises(WorkflowResumeError, match="input_required"):
        await Runner.run_workflow(pending.workflow, {"query": "new work"}, session="fresh",
                                  resume_guard=WorkflowResumeGuard(before_effect=lambda: None))
    assert writes == [] and pending.counts == {"start": 1, "ask": 1, "answered": 0}


@pytest.mark.asyncio
@pytest.mark.parametrize("answer_id", ["outer.ask", "outer"])
async def test_real_nested_interruption_is_unsupported(pending, monkeypatch, answer_id):
    from openjiuwen.core.workflow.components.flow.workflow_comp import SubWorkflowComponent

    counts = {"start": 0, "ask": 0, "answered": 0}
    inner = Workflow(WorkflowCard(id="inner"))
    inner.set_start_comp("start", CountStart(counts))
    inner.add_workflow_comp("ask", Ask(counts))
    inner.set_end_comp("end", End())
    inner.add_connection("start", "ask")
    inner.add_connection("ask", "end")
    outer = Workflow(WorkflowCard(id="nested-root"))
    outer.set_start_comp("start", Start())
    outer.add_workflow_comp("outer", SubWorkflowComponent(inner))
    outer.set_end_comp("end", End())
    outer.add_connection("start", "outer")
    outer.add_connection("outer", "end")
    sid = uuid4().hex
    try:
        result = await Runner.run_workflow(outer, {"query": "nested"}, session=sid)
        assert result.state is WorkflowExecutionState.INPUT_REQUIRED
        assert result.result[0].payload.id == "outer.ask"
        writes = watch_restoration(monkeypatch)
        inputs = InteractiveInput()
        inputs.update(answer_id, "answer")
        with pytest.raises(WorkflowResumeError, match="inputs_unsupported"):
            await Runner.run_workflow(outer, inputs, session=sid,
                                      resume_guard=WorkflowResumeGuard(before_effect=lambda: None))
        assert counts == {"start": 1, "ask": 1, "answered": 0} and writes == []
    finally:
        await pending.provider.release(sid)


@pytest.mark.asyncio
async def test_same_node_second_interruption_uses_new_paired_snapshot(pending):
    pending.ask.rounds = 2
    first = await resume(pending)
    assert first.state is WorkflowExecutionState.INPUT_REQUIRED
    assert first.result[0].payload.id == "ask"
    assert pending.counts == {"start": 1, "ask": 2, "answered": 0}
    second = await resume(pending)
    assert second.state is WorkflowExecutionState.COMPLETED
    assert pending.counts == {"start": 1, "ask": 3, "answered": 1}


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["inputs", "session", "subgraph"])
async def test_workflow_input_callback_cannot_change_strict_target(pending, monkeypatch, change):
    from openjiuwen.core.runner.callback.events import WorkflowEvents
    from openjiuwen.core.workflow import create_workflow_session

    async def transform(*args, **kwargs):
        if change == "inputs":
            inputs = InteractiveInput()
            inputs.update("ask", "replacement answer")
            return (inputs, *args[1:]), kwargs
        if change == "session":
            return args, {**kwargs, "session": create_workflow_session(session_id=pending.sid)}
        return args, {**kwargs, "is_sub": True}

    await Runner.callback_framework.register(WorkflowEvents.WORKFLOW_INVOKE_INPUT,
                                              transform, callback_type="transform")
    writes = watch_restoration(monkeypatch)
    with pytest.raises(WorkflowResumeError, match="owner_mismatch"):
        await resume(pending)
    assert writes == [] and pending.counts == {"start": 1, "ask": 1, "answered": 0}


@pytest.mark.asyncio
async def test_caller_answer_mutation_during_proof_cannot_rebind(pending, monkeypatch):
    inputs = InteractiveInput()
    inputs.update("ask", "original answer")
    entered, release = delay_snapshot(pending, monkeypatch)
    task = asyncio.create_task(resume(pending, inputs=inputs))
    await asyncio.wait_for(entered.wait(), 5)
    inputs.update("ask", "changed answer")
    release.set()
    result = await task
    assert result.result == {"output": {"answer": "original answer"}}


@pytest.mark.asyncio
@pytest.mark.parametrize("pending", ["memory"], indirect=True)
async def test_legacy_inmemory_graph_without_serializable_proof_still_recovers(pending):
    from openjiuwen.core.graph.pregel.constants import TASK_STATUS_INTERRUPT
    from openjiuwen.core.session.internal.workflow import WorkflowSession

    graph = pending.provider._graph_store.store_ck[pending.sid]["review-workflow"]
    graph.pending_node["ask"].exception[0].value[0].value.payload.value = lambda: "question"
    storage = pending.provider._workflow_stores[pending.sid]
    session = WorkflowSession(workflow_id="review-workflow", session_id=pending.sid)
    session.state().set_state(storage.serde.loads_typed(storage.state_blobs["review-workflow"]))
    session.state().set_updates(storage.serde.loads_typed(storage.state_updates_blobs["review-workflow"]))
    await pending.provider.post_workflow_execute(session, {TASK_STATUS_INTERRUPT: True}, None)
    assert "review-workflow" not in storage.resume_proofs
    inputs = InteractiveInput()
    inputs.update("ask", "legacy answer")
    result = await Runner.run_workflow(pending.workflow, inputs, session=pending.sid)
    assert result.state is WorkflowExecutionState.COMPLETED
    assert pending.counts == {"start": 1, "ask": 2, "answered": 1}
