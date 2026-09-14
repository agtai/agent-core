# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""The native manager must execute the actual project attempt, not a mirror."""

import asyncio
from dataclasses import replace
from threading import Event

import anyio
import pytest

from openjiuwen.core.application.tasks import project_executor as project_module
from openjiuwen.core.application.tasks.formal_task_models import FormalTaskViolation, OutboxKind
from openjiuwen.core.application.tasks.project_executor import DirectProjectCodeExecutorAdapter
from openjiuwen.core.common.task_manager.context import _current_task_id
from openjiuwen.core.common.task_manager.manager import TaskManager
from openjiuwen.core.runner.callback.errors import AbortError
from openjiuwen.core.runner.callback.framework import AsyncCallbackFramework
from tests.integration_tests.application_tasks.test_project_executor_application import (
    Application,
    _direct_binding,
    _DirectProjectExecutor,
    _git_project,
    _item,
)


@pytest.mark.asyncio
async def test_project_attempt_runs_in_native_task_with_real_git_and_detached_voice_parent(tmp_path, monkeypatch):
    monkeypatch.setattr(TaskManager, "_instance", None)
    manager = TaskManager()
    manager._callback_framework = AsyncCallbackFramework()
    scheduled = []

    async def created(task, **_kwargs):
        scheduled.append(task)

    await manager.on_created(created)
    project = tmp_path / "project"
    _git_project(project)
    original = (project / "README.md").read_bytes()
    agent = _DirectProjectExecutor()
    releases = []

    class Resolver:
        async def resolve(self, spec, *, for_dispatch):
            return _direct_binding(project, agent, releases=releases)

    async with manager.task_group() as group:
        async def owner():
            return group

        executor = DirectProjectCodeExecutorAdapter(
            Resolver(), tmp_path / "attempts.db", application=Application(),
            clock=lambda: "2026-08-05T12:00:00Z", task_group_provider=owner,
        )
        token = _current_task_id.set("voice-parent")
        try:
            receipt = await executor.dispatch(_item(project))
        finally:
            _current_task_id.reset(token)
        try:
            assert receipt.executor_ref == "d0-project:attempt-1"
            assert len(scheduled) == 1
            native = scheduled[0]
            assert native.parent_task_id is None
            assert native.group == "application-project"
            await asyncio.wait_for(native.wait(), timeout=30)
            assert native.is_settled
            record = executor._journal.get("attempt-1")
            assert record.outcome.value == "completed"
            assert (project / "result.txt").read_text(encoding="utf-8") == "sdk result"
            assert (project / "README.md").read_bytes() == original
            assert len(agent.requests) == 1
            assert releases == ["released"]
        finally:
            await executor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["running", "running_release_failure", "created", "completed_created"])
async def test_native_observer_failure_does_not_orphan_resources_or_replay(tmp_path, monkeypatch, phase):
    monkeypatch.setattr(TaskManager, "_instance", None)
    manager = TaskManager()
    manager._callback_framework = AsyncCallbackFramework()
    scheduled, releases = [], []
    project = tmp_path / "project"
    _git_project(project)
    agent = _DirectProjectExecutor()

    async def reject(task, **_kwargs):
        scheduled.append(task)
        if phase == "completed_created":
            await task.wait()
        raise AbortError("controlled native observer failure")

    await (manager.on_running(reject) if phase.startswith("running") else manager.on_created(reject))

    def failed_release():
        releases.append("released")
        raise RuntimeError("controlled context release failure")

    class Resolver:
        async def resolve(self, spec, *, for_dispatch):
            binding = _direct_binding(project, agent, releases=releases)
            return replace(binding, context_release=failed_release) if phase == "running_release_failure" else binding

    async with manager.task_group() as group:
        async def owner():
            return group

        executor = DirectProjectCodeExecutorAdapter(
            Resolver(), tmp_path / "attempts.db", application=Application(),
            clock=lambda: "2026-08-05T12:00:00Z", task_group_provider=owner,
        )
        try:
            if phase.startswith("running"):
                await asyncio.wait_for(executor.dispatch(_item(project)), 30)
            else:
                with pytest.raises(AbortError):
                    await asyncio.wait_for(executor.dispatch(_item(project)), 30)
            native = scheduled[0]
            await asyncio.wait_for(executor._wait_workers({native}, timeout=30), 31)
            assert native.is_settled
            if phase == "running_release_failure":
                assert str(native.exception) == "controlled context release failure"
            assert not executor.has_live_workers
            assert releases == ["released"]
            expected_calls = 1 if phase == "completed_created" else 0
            assert len(agent.requests) == expected_calls
            record = executor._journal.get("attempt-1")
            assert record.outcome.value == ("completed" if expected_calls else "interrupted")
            await executor.dispatch(_item(project))
            assert len(agent.requests) == expected_calls
            assert not group.cancel_scope.cancel_called
            assert executor.retained_cleanup_attempt_ids() == ()
        finally:
            await executor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["checkout", "prepare", "apply", "cleanup"])
@pytest.mark.parametrize("cancel_kind", ["native", "root"])
async def test_native_cancel_retains_thread_owner_until_real_git_operation_finishes(
    tmp_path, monkeypatch, stage, cancel_kind,
):
    monkeypatch.setattr(TaskManager, "_instance", None)
    manager = TaskManager()
    manager._callback_framework = AsyncCallbackFramework()
    scheduled, releases = [], []
    entered, release_thread = Event(), Event()
    project = tmp_path / "project"
    _git_project(project)
    original = (project / "README.md").read_bytes()
    agent = _DirectProjectExecutor()
    function = {
        "checkout": "_create_attempt_worktree", "prepare": "_apply_attempt_patch",
        "apply": "_apply_attempt_patch", "cleanup": "_remove_attempt_worktree",
    }[stage]
    original_function = getattr(project_module, function)

    def blocked(*args, **kwargs):
        entered.set()
        assert release_thread.wait(30), "test barrier was not released"
        return original_function(*args, **kwargs)

    if stage == "prepare":
        original_prepare = DirectProjectCodeExecutorAdapter._prepare_d2_project_effect

        async def blocked_prepare(self, **kwargs):
            entered.set()
            assert await asyncio.to_thread(release_thread.wait, 30)
            return await original_prepare(self, **kwargs)

        monkeypatch.setattr(DirectProjectCodeExecutorAdapter, "_prepare_d2_project_effect", blocked_prepare)
    else:
        monkeypatch.setattr(project_module, function, blocked)

    async def created(task, **_kwargs):
        scheduled.append(task)

    await manager.on_created(created)

    class Resolver:
        async def resolve(self, spec, *, for_dispatch):
            return _direct_binding(project, agent, releases=releases)

    async with manager.task_group() as group:
        async def owner():
            return group

        executor = DirectProjectCodeExecutorAdapter(
            Resolver(), tmp_path / "attempts.db", application=Application(),
            clock=lambda: "2026-08-05T12:00:00Z", task_group_provider=owner,
            close_timeout=0.05,
        )
        dispatch = asyncio.create_task(executor.dispatch(_item(project)))
        try:
            assert await asyncio.to_thread(entered.wait, 20)
            native = scheduled[0]
            if cancel_kind == "root":
                group.cancel_scope.cancel()
            else:
                assert native.abort(reason="test_native_cancel")
            with anyio.CancelScope(shield=True):
                assert not native.is_settled
                assert executor.has_live_workers
                assert releases == []
                if stage == "apply":
                    with pytest.raises(FormalTaskViolation) as pending:
                        await executor.close()
                    assert pending.value.reason == "EXECUTOR_CLOSE_CLEANUP_PENDING"
                    assert not (project / "result.txt").exists()
                elif stage == "cleanup":
                    with pytest.raises(RuntimeError, match="PROJECT_WORKTREE_CLEANUP_PENDING"):
                        await executor.close()
                    assert (project / "result.txt").exists()
                release_thread.set()
                await asyncio.wait_for(executor._wait_workers({native}, timeout=30), 31)
                assert native.is_settled
                await asyncio.wait_for(dispatch, 30)
                await executor.close()

                assert releases == ["released"]
                assert not executor.has_live_workers
                assert executor.retained_cleanup_attempt_ids() == ()
                assert (project / "README.md").read_bytes() == original
                record = executor._journal.get("attempt-1")
                if stage in {"apply", "cleanup"}:
                    assert record.outcome.value == "completed"
                    assert (project / "result.txt").read_text(encoding="utf-8") == "sdk result"
                else:
                    assert record.outcome.value == "interrupted"
                    assert len(agent.requests) == (1 if stage == "prepare" else 0)
                    assert not (project / "result.txt").exists()
        finally:
            release_thread.set()
            with anyio.CancelScope(shield=True):
                await asyncio.gather(dispatch, return_exceptions=True)
                if executor._running:
                    await executor._wait_workers(set(executor._running.values()), timeout=30)
                await executor.close()


@pytest.mark.asyncio
async def test_native_running_cancel_is_exact_and_restart_cannot_replay(tmp_path, monkeypatch):
    monkeypatch.setattr(TaskManager, "_instance", None)
    manager = TaskManager()
    manager._callback_framework = AsyncCallbackFramework()
    project = tmp_path / "project"
    _git_project(project)
    (project / "draft.txt").write_bytes(b"uncommitted user bytes\r\n")
    entered = asyncio.Event()
    calls, releases = [], []

    class Agent:
        async def process_background_code_task_stream(self, request):
            calls.append(request)
            entered.set()
            await asyncio.Event().wait()
            yield None

    class Resolver:
        async def resolve(self, spec, *, for_dispatch):
            return _direct_binding(project, Agent(), releases=releases)

    database = tmp_path / "attempts.db"
    async with manager.task_group() as group:
        async def owner():
            return group

        executor = DirectProjectCodeExecutorAdapter(
            Resolver(), database, application=Application(),
            clock=lambda: "2026-08-05T12:00:00Z", task_group_provider=owner,
        )
        try:
            receipt = await executor.dispatch(_item(project))
            await asyncio.wait_for(entered.wait(), 20)
            cancel = replace(_item(project, kind=OutboxKind.ATTEMPT_CANCEL), executor_ref=receipt.executor_ref)
            before = executor._journal.get("attempt-1")
            foreign = replace(cancel.scope, session_id="foreign")
            with pytest.raises(FormalTaskViolation):
                await executor.cancel(replace(
                    cancel, scope=foreign,
                    spec=replace(cancel.spec, context=replace(cancel.spec.context, scope=foreign)),
                ))
            assert executor._journal.get("attempt-1") == before
            native = executor._running["attempt-1"]
            await executor.cancel(cancel)
            await executor._wait_workers({native}, timeout=30)
            assert native.is_settled
            assert executor._journal.get("attempt-1").outcome.value == "cancelled"
            assert not (project / "result.txt").exists()
            assert (project / "draft.txt").read_bytes() == b"uncommitted user bytes\r\n"
        finally:
            await executor.close()
    restored = DirectProjectCodeExecutorAdapter(
        Resolver(), database, application=Application(), clock=lambda: "2026-08-05T12:00:01Z",
    )
    try:
        await restored.prepare_startup()
        await restored.dispatch(_item(project))
        assert restored._journal.get("attempt-1").outcome.value == "cancelled"
        assert len(calls) == 1 and releases == ["released"]
    finally:
        await restored.close()


@pytest.mark.asyncio
async def test_configured_native_owner_failure_never_falls_back_or_acquires_agent(tmp_path):
    project = tmp_path / "project"
    _git_project(project)
    calls = []

    class Resolver:
        async def resolve(self, spec, *, for_dispatch):
            calls.append(spec)
            raise AssertionError("forbidden Agent acquisition")

    async def unavailable():
        raise RuntimeError("Host owner unavailable")

    executor = DirectProjectCodeExecutorAdapter(
        Resolver(), tmp_path / "attempts.db", application=Application(), task_group_provider=unavailable,
    )
    try:
        with pytest.raises(RuntimeError, match="Host owner unavailable"):
            await executor.dispatch(_item(project))
        assert calls == []
        assert executor._journal.get("attempt-1") is None
        assert not executor.has_live_workers
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_cancelled_native_acquisition_retains_checkout_and_bounds_close(tmp_path, monkeypatch):
    from pathlib import Path

    from openjiuwen.core.application.tasks.project_executor import AttemptProjectExecutorLease

    monkeypatch.setattr(TaskManager, "_instance", None)
    manager = TaskManager()
    manager._callback_framework = AsyncCallbackFramework()
    project = tmp_path / "project"
    _git_project(project)
    entered, release_acquire = asyncio.Event(), asyncio.Event()
    roots, releases = [], []
    agent = _DirectProjectExecutor()

    async def acquire(root):
        roots.append(Path(root))
        entered.set()
        await release_acquire.wait()

        async def release():
            assert Path(root).exists()  # noqa: ASYNC240 -- real retained-checkout assertion
            releases.append("agent")

        return AttemptProjectExecutorLease(agent, root, release)

    class Resolver:
        async def resolve(self, spec, *, for_dispatch):
            return replace(_direct_binding(project, agent), attempt_executor_factory=acquire)

    async with manager.task_group() as group:
        async def owner():
            return group

        executor = DirectProjectCodeExecutorAdapter(
            Resolver(), tmp_path / "attempts.db", application=Application(),
            task_group_provider=owner, close_timeout=0.05,
        )
        dispatch = asyncio.create_task(executor.dispatch(_item(project)))
        try:
            await asyncio.wait_for(entered.wait(), 20)
            native = executor._running["attempt-1"]
            assert native.abort(reason="cancel_while_acquiring")
            await asyncio.wait_for(dispatch, 20)
            with pytest.raises(RuntimeError, match="PROJECT_WORKTREE_CLEANUP_PENDING"):
                await executor.close()
            assert not native.is_settled
            assert roots[0].exists() and releases == []
            assert executor.retained_cleanup_attempt_ids() == ("attempt-1",)
            release_acquire.set()
            await executor._wait_workers({native}, timeout=30)
            assert native.is_settled
            await executor.close()
            assert not roots[0].exists()
            assert releases == ["agent"] and agent.requests == []
            assert not (project / "result.txt").exists()
        finally:
            release_acquire.set()
            await asyncio.gather(dispatch, return_exceptions=True)
            if executor._running:
                await executor._wait_workers(set(executor._running.values()), timeout=30)
            await executor.close()
