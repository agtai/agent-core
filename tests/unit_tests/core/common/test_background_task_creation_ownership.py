# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Native startup errors must not strand an already returned background handle."""

import asyncio
import inspect

import anyio
import pytest

from openjiuwen.core.common.background_tasks import start_background_task
from openjiuwen.core.common.task_manager.manager import TaskManager
from openjiuwen.core.runner.callback.errors import AbortError
from openjiuwen.core.runner.callback.framework import AsyncCallbackFramework


@pytest.fixture
def manager(monkeypatch):
    monkeypatch.setattr(TaskManager, "_instance", None)
    value = TaskManager()
    value._callback_framework = AsyncCallbackFramework()
    return value


@pytest.mark.asyncio
async def test_created_callback_error_retains_actual_completed_task(manager):
    started = asyncio.Event()

    async def body():
        started.set()
        return "actual result"

    async def created(task, **_kwargs):
        await started.wait()
        await task.wait()
        raise AbortError("creation observer aborted")

    await manager.on_created(created)
    with pytest.raises(ExceptionGroup) as failure:
        async with manager.task_group():
            handle = start_background_task(body(), name="creation-probe", group="probe", fallback_to_asyncio=False)
            await anyio.sleep_forever()
    assert any(isinstance(exc, AbortError) for exc in failure.value.exceptions)
    assert handle._ready.is_set()
    assert handle.done()
    assert await asyncio.wait_for(handle.wait(), 2) == "actual result"


@pytest.mark.asyncio
async def test_cancel_before_registration_closes_body_and_completes_handle(manager):
    calls = []

    async def body():
        calls.append("forbidden")

    coro = body()
    await manager._lock.acquire()
    try:
        async with manager.task_group() as group:
            handle = start_background_task(coro, name="blocked-creation", group="probe", fallback_to_asyncio=False)
            await asyncio.sleep(0)
            group.cancel_scope.cancel()
        assert handle._ready.is_set()
        assert handle.done()
        with pytest.raises(asyncio.CancelledError):
            await handle.wait()
        assert inspect.getcoroutinestate(coro) == inspect.CORO_CLOSED
        assert calls == []
    finally:
        manager._lock.release()
        coro.close()
