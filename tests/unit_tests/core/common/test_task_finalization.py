# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Ownership handed over at scheduling survives failure before body entry."""

import asyncio
import inspect

import anyio
import pytest

from openjiuwen.core.common.task_manager.task import Task


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["success", "startup_failure", "root_cancel"])
async def test_finalization_precedes_settlement_even_when_body_never_enters(mode):
    calls = []
    cleanup_entered, cleanup_release = asyncio.Event(), asyncio.Event()
    callback_entered = asyncio.Event()
    task = Task(task_id="retained-resource")

    async def body():
        calls.append("body")

    async def callback(_task, status):
        if status == "running":
            callback_entered.set()
            if mode == "startup_failure":
                raise ValueError("startup rejected")
            if mode == "root_cancel":
                await asyncio.Event().wait()

    async def finalize(owner):
        assert owner is task
        cleanup_entered.set()
        await cleanup_release.wait()
        calls.append("released")

    coro = body()
    async with anyio.create_task_group() as group:
        group.start_soon(task.execute, coro, callback, True, finalize)
        await asyncio.wait_for(callback_entered.wait(), 2)
        if mode == "root_cancel":
            group.cancel_scope.cancel()
        with anyio.CancelScope(shield=True):
            try:
                await asyncio.wait_for(cleanup_entered.wait(), 2)
                assert not task.is_settled
                assert calls == (["body"] if mode == "success" else [])
            finally:
                cleanup_release.set()
    assert task.is_settled
    assert calls == (["body", "released"] if mode == "success" else ["released"])
    assert inspect.getcoroutinestate(coro) == inspect.CORO_CLOSED


@pytest.mark.asyncio
async def test_finalizer_failure_is_observable_and_does_not_strand_waiters():
    task = Task(task_id="cleanup-failure")

    async def body():
        return "finished"

    async def finalize(_task):
        raise RuntimeError("cleanup failed")

    await task.execute(body(), catch_exceptions=True, finalizer=finalize)
    assert task.is_settled
    with pytest.raises(RuntimeError, match="cleanup failed"):
        await task.wait()
