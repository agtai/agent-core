# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Native lifecycle status must not be mistaken for release of its owner."""

import asyncio

import pytest

from openjiuwen.core.common.background_tasks import BackgroundTask
from openjiuwen.core.common.task_manager.task import Task


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [False, True])
async def test_physical_settlement_waits_for_terminal_observer_without_changing_done(failure):
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []
    native = Task(task_id="physical-settlement")
    handle = BackgroundTask(group="physical-settlement")
    handle.set_manager_task(native)

    async def body():
        calls.append("body")
        if failure:
            raise ValueError("controlled body failure")
        return "result"

    async def callback(_task, status):
        if status == ("failed" if failure else "completed"):
            entered.set()
            await release.wait()

    execution = asyncio.create_task(native.execute(body(), callback, catch_exceptions=True))
    waiter = asyncio.create_task(handle.wait())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert native.is_terminal and handle.done()  # Existing status contract.
        assert not native.is_settled
        assert not handle.is_settled
        assert not waiter.done()
        waiter.cancel()  # Stopping an observer must not settle the real owner.
        await asyncio.gather(waiter, return_exceptions=True)
        assert not execution.done()
    finally:
        release.set()
        await asyncio.wait_for(execution, 2)
        await asyncio.gather(waiter, return_exceptions=True)
    assert native.is_settled and handle.is_settled
    assert calls == ["body"]
    if failure:
        with pytest.raises(ValueError, match="controlled body failure"):
            await handle.wait()
    else:
        assert await handle.wait() == "result"
