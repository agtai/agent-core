# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Characterize native ownership against durable Work; no proposed adapter is mocked."""

import asyncio
import sqlite3
from contextlib import asynccontextmanager, closing

import anyio
import pytest

from openjiuwen.core.application.tasks.contracts import Assurance, ScopeRef
from openjiuwen.core.application.tasks.work_runtime import WorkRuntime, WorkState, WorkViolation
from openjiuwen.core.application.tasks.work_store import SqliteWorkStore
from openjiuwen.core.common.background_tasks import create_background_task
from openjiuwen.core.common.task_manager.context import reset_task_group, set_task_group
from openjiuwen.core.common.task_manager.manager import get_task_manager
from openjiuwen.core.runner.runner import Runner, _RunnerImpl

SCOPE = ScopeRef("owner-probe", "project", "session", Assurance.AUTHENTICATED)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "failure", "cancel", "cleanup_failure", "cleanup_cancel"])
async def test_independent_cleanup_keeps_sqlite_unsettled_and_capacity_reserved(tmp_path, outcome):
    owner, store = _work(tmp_path)
    started, release_runner, release_cleanup = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def cleanup():
        await release_cleanup.wait()
        if outcome == "cleanup_failure":
            raise RuntimeError("cleanup failed")

    async def run(control):
        control.settlement = asyncio.create_task(cleanup())
        started.set()
        await release_runner.wait()
        if outcome == "failure":
            raise RuntimeError("execution failed")
        return "physical result"

    work = await owner.start(**_inputs(), runner=run)
    record = owner._records[(SCOPE, work.work_id, 1)]
    try:
        await asyncio.wait_for(started.wait(), 2)
        if outcome == "cancel":
            await owner.cancel(scope=SCOPE, work_id=work.work_id, revision=1)
            for _ in range(500):
                if record.snapshot.state is WorkState.UNKNOWN:
                    break
                await asyncio.sleep(0.001)
            assert record.snapshot.state is WorkState.UNKNOWN
        release_runner.set()
        await asyncio.wait_for(asyncio.gather(record.runner, return_exceptions=True), 2)
        await asyncio.sleep(0.01)
        if outcome == "cleanup_cancel":
            await owner.cancel(scope=SCOPE, work_id=work.work_id, revision=1)
            for _ in range(500):
                if record.snapshot.state is WorkState.UNKNOWN:
                    break
                await asyncio.sleep(0.001)
            assert record.snapshot.state is WorkState.UNKNOWN
        retained = owner.query(scope=SCOPE, work_id=work.work_id)
        assert not retained.execution_settled
        assert retained.result_text is None
        assert store.restore() == (retained,)
        with pytest.raises(WorkViolation) as full:
            await owner.start(**_inputs("forbidden-replacement"), runner=run)
        assert full.value.reason == "NATIVE_WORK_CAPACITY_FULL"
        assert store.restore() == (retained,)
        release_cleanup.set()
        await asyncio.wait_for(record.operation, 2)
        final = owner.query(scope=SCOPE, work_id=work.work_id)
        assert final.execution_settled
        assert final.state is {"success": WorkState.COMPLETED, "failure": WorkState.FAILED,
                               "cancel": WorkState.UNKNOWN, "cleanup_failure": WorkState.UNKNOWN,
                               "cleanup_cancel": WorkState.UNKNOWN}[outcome]
        assert final.result_text == ("physical result" if outcome == "success" else None)
        assert SqliteWorkStore(store.database_path).restore() == (final,)
    finally:
        release_runner.set()
        release_cleanup.set()
        await asyncio.gather(record.operation, return_exceptions=True)
        await owner.close()


def _inputs(request="request"):
    return dict(scope=SCOPE, request_id=request, input_id="input", instruction="Analyze",
                model_identity="model", model_config_version="config", context_id="a" * 64, foreground=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("closed", [False, True])
async def test_native_owner_lost_before_execution_never_starts_producer_or_replays(tmp_path, closed):
    calls = []

    async def run(_control):
        calls.append("forbidden")
        return "forbidden"

    owner, store = _work(tmp_path, task_group_provider=lambda: root)
    try:
        async with _runner_root() as root:
            if not closed:
                root.cancel_scope.cancel()
                work = await owner.start(**_inputs(), runner=run)
                await asyncio.wait_for(owner._records[(SCOPE, work.work_id, 1)].operation, 2)
        if closed:
            work = await owner.start(**_inputs(), runner=run)
        final = owner.query(scope=SCOPE, work_id=work.work_id)
        assert calls == []
        assert final.state is WorkState.UNKNOWN and final.execution_settled
        assert final.result_text is None
        assert await owner.start(**_inputs(), runner=run) == final
        assert SqliteWorkStore(store.database_path).restore() == (final,)
    finally:
        await owner.close()


def _work(tmp_path, **kwargs):
    path = tmp_path / "work.sqlite"
    with closing(sqlite3.connect(path)):
        pass
    store = SqliteWorkStore(path)
    owner = WorkRuntime(save=store.save, max_active=2, reserved_foreground=1,
                        cancel_settlement_seconds=0.01, **kwargs)
    return owner, store


@asynccontextmanager
async def _runner_root():
    # Exercise the actual native root owner, without starting network/Agent services.
    runner = _RunnerImpl(runner_id="work-owner-probe", config=Runner.get_config())
    await runner._ensure_root_task_group()
    try:
        yield runner.get_root_task_group()
    finally:
        # Only this isolated root is stopped; do not call global cancel_all.
        runner._root_task_group_stop.set()
        await asyncio.wait_for(runner._root_task_group_owner, 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit_root", [False, True])
async def test_native_root_can_outlive_caller_group_like_durable_work(tmp_path, explicit_root):
    owner, store = _work(tmp_path, task_group_provider=lambda: root)
    native_started, work_started, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def native_run():
        native_started.set()
        await release.wait()
        return "native-result"

    async def work_run(control):
        work_started.set()
        await control.read_only(release.wait())
        return "durable-result"

    work = None
    try:
        async with _runner_root() as root:
            async with get_task_manager().task_group() as caller:
                token = set_task_group(root) if explicit_root else None
                try:
                    handle = await create_background_task(
                        native_run(), name="owner-probe", group="owner-probe", fallback_to_asyncio=False,
                    )
                finally:
                    if token is not None:
                        reset_task_group(token)
                assert handle._manager_task is not None  # native manager, never fallback
                work = await owner.start(**_inputs(), runner=work_run)
                await asyncio.wait_for(native_started.wait(), 2)
                await asyncio.wait_for(work_started.wait(), 2)
                caller.cancel_scope.cancel()
            assert handle.done() is (not explicit_root)
            retained = owner.query(scope=SCOPE, work_id=work.work_id)
            assert retained.state is WorkState.RUNNING
            assert store.restore() == (retained,)
            release.set()
            if explicit_root:
                assert await asyncio.wait_for(handle.wait(), 2) == "native-result"
            await asyncio.wait_for(owner._records[(SCOPE, work.work_id, 1)].operation, 2)
            completed = owner.query(scope=SCOPE, work_id=work.work_id)
            assert completed.result_text == "durable-result"
            assert completed.state is WorkState.COMPLETED and completed.execution_settled
            assert SqliteWorkStore(store.database_path).restore() == (completed,)
    finally:
        release.set()
        await owner.close()
        

@pytest.mark.asyncio
async def test_native_handle_retains_cleanup_but_durable_unknown_is_separate(tmp_path):
    owner, store = _work(tmp_path)
    native_started, native_cleanup, work_started = asyncio.Event(), asyncio.Event(), asyncio.Event()
    release = asyncio.Event()

    async def native_run():
        native_started.set()
        try:
            await anyio.sleep_forever()
        finally:
            with anyio.CancelScope(shield=True):
                native_cleanup.set()
                await release.wait()

    async def work_run(_control):
        work_started.set()
        await release.wait()  # physical producer deliberately does not settle on cancel
        return "late-result"

    try:
        async with _runner_root() as root:
            token = set_task_group(root)
            try:
                handle = await create_background_task(
                    native_run(), name="cleanup-probe", group="owner-probe", fallback_to_asyncio=False,
                )
            finally:
                reset_task_group(token)
            work = await owner.start(**_inputs(), runner=work_run)
            await asyncio.wait_for(native_started.wait(), 2)
            await asyncio.wait_for(work_started.wait(), 2)
            await handle.cancel(timeout=0.01)
            assert native_cleanup.is_set() and not handle.done()
            await owner.cancel(scope=SCOPE, work_id=work.work_id, revision=1)
            for _ in range(500):
                retained = owner.query(scope=SCOPE, work_id=work.work_id)
                if retained.state is WorkState.UNKNOWN:
                    break
                await asyncio.sleep(0.001)
            assert retained.state is WorkState.UNKNOWN and not retained.execution_settled
            before = store.restore()
            assert before == (retained,)
            with pytest.raises(WorkViolation) as full:
                await owner.start(**_inputs("forbidden-replacement"), runner=work_run)
            assert full.value.reason == "NATIVE_WORK_CAPACITY_FULL"
            assert store.restore() == before
            release.set()
            await asyncio.wait_for(handle.wait(), 2)
            await asyncio.wait_for(owner._records[(SCOPE, work.work_id, 1)].operation, 2)
            assert handle.done()
            final = owner.query(scope=SCOPE, work_id=work.work_id)
            assert final.state is WorkState.UNKNOWN and final.execution_settled
            assert final.result_text is None
            assert SqliteWorkStore(store.database_path).restore() == (final,)
    finally:
        release.set()
        await owner.close()


@pytest.mark.asyncio
async def test_work_orchestration_native_cancel_keeps_physical_reservation(tmp_path):
    owner, store = _work(tmp_path, task_group_provider=lambda: root)
    started, release = asyncio.Event(), asyncio.Event()

    async def run(_control):
        started.set()
        await release.wait()
        return "late physical result"

    work = None
    try:
        async with _runner_root() as root:
            work = await owner.start(**_inputs(), runner=run)
            await asyncio.wait_for(started.wait(), 2)
            try:
                root.cancel_scope.cancel()
                for _ in range(100):
                    if owner.query(scope=SCOPE, work_id=work.work_id).state is WorkState.UNKNOWN:
                        break
                    await asyncio.sleep(0.001)
                retained = owner.query(scope=SCOPE, work_id=work.work_id)
                assert retained.state is WorkState.UNKNOWN and not retained.execution_settled
                record = owner._records[(SCOPE, work.work_id, 1)]
                assert not record.operation.done()
                assert not record.runner.done()
                before = store.restore()
                with pytest.raises(WorkViolation) as full:
                    await owner.start(**_inputs("forbidden-replacement"), runner=run)
                assert full.value.reason == "NATIVE_WORK_CAPACITY_FULL"
                assert store.restore() == before
                release.set()
                await asyncio.wait_for(record.operation, 2)
                final = owner.query(scope=SCOPE, work_id=work.work_id)
                assert final.state is WorkState.UNKNOWN and final.execution_settled
                assert final.result_text is None
                assert store.restore() == (final,)
            finally:
                release.set()
    finally:
        release.set()
        if work is not None:
            record = owner._records[(SCOPE, work.work_id, 1)]
            await asyncio.gather(record.operation, record.runner, return_exceptions=True)
        await owner.close()
