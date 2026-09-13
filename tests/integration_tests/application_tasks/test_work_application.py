"""Real SDK Work storage/ownership without a Voice or Host input journal."""

import asyncio
import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest

from openjiuwen.core.application.tasks import SqliteTaskStore
from openjiuwen.core.application.tasks.contracts import Assurance, ScopeRef
from openjiuwen.core.application.tasks.execution_control import CURRENT_INTERACTION_CONTROL
from openjiuwen.core.application.tasks.work_runtime import WorkRuntime, WorkState, WorkViolation, context_identity
from openjiuwen.core.application.tasks.work_store import SqliteWorkStore


@pytest.mark.asyncio
async def test_sdk_work_has_one_durable_admission_exact_scope_and_no_restart_replay(tmp_path):
    database = tmp_path / "application.db"
    tasks = SqliteTaskStore(database)
    store = SqliteWorkStore(database)
    scope = ScopeRef("user", "project", "session", Assurance.AUTHENTICATED)
    calls = []
    completed = asyncio.Event()

    async def run(control):
        assert CURRENT_INTERACTION_CONTROL.get() is None
        calls.append(control.snapshot.work_id)
        completed.set()
        return "verified result"

    runtime = WorkRuntime(save=store.save, restored=store.restore())
    fields = dict(
        scope=scope,
        request_id="request",
        input_id="input",
        instruction="Read context",
        model_identity="configured",
        model_config_version="v1",
        foreground=False,
        context_id=context_identity(SimpleNamespace(scope=scope, entries=())),
        runner=run,
    )
    token = CURRENT_INTERACTION_CONTROL.set(object())
    try:
        accepted = await runtime.start(**fields)
    finally:
        CURRENT_INTERACTION_CONTROL.reset(token)
    assert calls == []
    assert store.restore()[0].state is WorkState.ACCEPTED
    with pytest.raises(WorkViolation):
        runtime.query(scope=replace(scope, session_id="other"), work_id=accepted.work_id)
    assert calls == []
    assert (await runtime.start(**fields)).work_id == accepted.work_id
    await asyncio.wait_for(completed.wait(), 2)
    await asyncio.gather(*(asyncio.shield(record.operation) for record in runtime._records.values()))
    assert runtime.query(scope=scope, work_id=accepted.work_id).state is WorkState.COMPLETED
    restored = WorkRuntime(save=store.save, restored=store.restore())
    assert (await restored.start(**fields)).result_text == "verified result"
    assert len(calls) == 1
    assert tasks.list_tasks(scope) == ()  # Work does not manufacture a project Task/UI card.
    # Existing serialized snapshot identity is immutable and cannot be retargeted.
    snapshot = store.restore()[0]
    with pytest.raises(WorkViolation):
        store.save(replace(snapshot, input_id="foreign", sequence=snapshot.sequence + 1))
    assert store.restore()[0] == snapshot
    await restored.close()
    await runtime.close()


@pytest.mark.asyncio
async def test_sdk_observation_wait_has_no_host_dependency_and_preserves_scope():
    runtime = WorkRuntime()
    scope = ScopeRef("user", "project", "session", Assurance.AUTHENTICATED)
    cursor = runtime.observation_cursor(scope)
    await runtime.wait_for_observation(scope=scope, after=cursor, wait_ms=0)
    waiter = asyncio.create_task(runtime.wait_for_observation(scope=scope, after=cursor, wait_ms=1000))
    await asyncio.sleep(0)
    runtime.wake_observers(replace(scope, session_id="other"))
    await asyncio.sleep(0)
    assert not waiter.done()
    runtime.wake_observers(scope)
    await asyncio.wait_for(waiter, 1)
    assert runtime.observation_cursor(scope)["sequence"] == cursor["sequence"] + 1
    with pytest.raises(ValueError):
        await runtime.wait_for_observation(scope=scope, after={**cursor, "sequence": True}, wait_ms=0)
    with pytest.raises(ValueError):
        await runtime.wait_for_observation(scope=scope, after=cursor, wait_ms=1001)
    await runtime.close()


def test_sdk_work_restores_lost_execution_as_unknown_without_forged_completion(tmp_path):
    from openjiuwen.core.application.tasks.work_runtime import WorkSnapshot

    database = tmp_path / "restore.db"
    sqlite3.connect(database).close()
    store = SqliteWorkStore(database)
    snapshot = WorkSnapshot(
        scope=ScopeRef("user", "project", "session", Assurance.AUTHENTICATED),
        work_id="work",
        revision=1,
        sequence=1,
        request_id="request",
        input_id="input",
        instruction="read",
        model_identity="model",
        model_config_version="v1",
        context_id="a" * 64,
        foreground=False,
        state=WorkState.ACCEPTED,
        accepted_at="2026-09-13T10:00:00Z",
        updated_at="2026-09-13T10:00:00Z",
    )
    store.save(snapshot)
    runtime = WorkRuntime(save=store.save, restored=store.restore())
    recovered = store.restore()[0]
    assert recovered.state is WorkState.UNKNOWN
    assert recovered.reason == "PROCESS_OWNERSHIP_LOST" and recovered.result_text is None
    assert runtime.list(scope=snapshot.scope) == (recovered,)
