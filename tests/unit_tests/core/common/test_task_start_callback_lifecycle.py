# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""A failed native start callback must not strand ownership or run effects."""

import asyncio
import inspect

import pytest

from openjiuwen.core.common.task_manager.context import _current_task_id, get_current_task_id
from openjiuwen.core.common.task_manager.task import Task
from openjiuwen.core.common.task_manager.types import TaskStatus


@pytest.mark.asyncio
@pytest.mark.parametrize("catch_exceptions", [False, True])
async def test_failed_start_callback_settles_without_starting_body(catch_exceptions):
    calls = []
    task = Task(task_id="start-callback-probe")
    failure = RuntimeError("start callback failed")

    async def body():
        calls.append("forbidden")

    async def callback(_task, status):
        if status == "running":
            raise failure

    coro = body()
    token = _current_task_id.set("parent")
    try:
        if catch_exceptions:
            await task.execute(coro, callback, catch_exceptions=True)
        else:
            with pytest.raises(RuntimeError, match="start callback failed"):
                await task.execute(coro, callback)
        assert task.status is TaskStatus.FAILED
        assert task.exception is failure
        assert task._done_event.is_set()
        assert task.get_cancel_scope() is None
        assert get_current_task_id() == "parent"
        assert inspect.getcoroutinestate(coro) == inspect.CORO_CLOSED
        assert calls == []
    finally:
        coro.close()
        _current_task_id.reset(token)


@pytest.mark.asyncio
async def test_cancel_during_start_callback_settles_without_body_effects():
    started = asyncio.Event()
    calls, restored_context = [], []
    task = Task(task_id="cancel-start-probe")

    async def body():
        calls.append("forbidden")

    async def callback(_task, status):
        if status == "running":
            started.set()
            await asyncio.Event().wait()

    coro = body()

    async def execute():
        token = _current_task_id.set("parent")
        try:
            await task.execute(coro, callback)
        finally:
            restored_context.append(get_current_task_id())
            _current_task_id.reset(token)

    execution = asyncio.create_task(execute())
    try:
        await asyncio.wait_for(started.wait(), 2)
        execution.cancel()
        with pytest.raises(asyncio.CancelledError):
            await execution
        assert task.status is TaskStatus.CANCELLED
        assert task._done_event.is_set()
        assert task.get_cancel_scope() is None
        assert restored_context == ["parent"]
        assert inspect.getcoroutinestate(coro) == inspect.CORO_CLOSED
        assert calls == []
    finally:
        execution.cancel()
        await asyncio.gather(execution, return_exceptions=True)
        coro.close()


@pytest.mark.asyncio
async def test_native_cancel_handle_works_while_start_callback_is_pending():
    started = asyncio.Event()
    task = Task(task_id="native-cancel-start-probe")
    calls = []

    async def body():
        calls.append("forbidden")

    async def callback(_task, status):
        if status == "running":
            started.set()
            await asyncio.Event().wait()

    coro = body()
    execution = asyncio.create_task(task.execute(coro, callback))
    try:
        await asyncio.wait_for(started.wait(), 2)
        assert await task.cancel(cascade=False, reason="startup_cancel")
        await asyncio.wait_for(execution, 2)
        assert task.status is TaskStatus.CANCELLED
        assert task.cancel_reason == "startup_cancel"
        assert task._done_event.is_set()
        assert inspect.getcoroutinestate(coro) == inspect.CORO_CLOSED
        assert calls == []
    finally:
        execution.cancel()
        await asyncio.gather(execution, return_exceptions=True)
        coro.close()
