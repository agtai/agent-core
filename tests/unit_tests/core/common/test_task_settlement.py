import asyncio

import pytest

from openjiuwen.core.common import wait_for_task_settlement


@pytest.mark.asyncio
async def test_success_and_failure_remain_on_original_task():
    for fails in (False, True):
        async def run():
            if fails:
                raise ValueError("real failure")
            return 42
        task = asyncio.create_task(run())
        stop = asyncio.Event()
        result = await wait_for_task_settlement(task, cancelled=stop)
        assert result.settled and not result.timed_out and not result.cancellation_requested
        if fails:
            with pytest.raises(ValueError, match="real failure"):
                task.result()
        else:
            assert task.result() == 42


@pytest.mark.asyncio
async def test_timeout_requests_cooperative_stop_but_keeps_unsettled_task():
    entered, release, stop = asyncio.Event(), asyncio.Event(), asyncio.Event()
    async def run():
        entered.set()
        await release.wait()
        return "late"
    task = asyncio.create_task(run())
    await entered.wait()
    try:
        result = await wait_for_task_settlement(task, cancelled=stop, timeout=0, settlement_timeout=0)
        assert result.timed_out and result.cancellation_requested and not result.settled
        assert not task.done() and task.cancelling() == 0 and stop.is_set()
    finally:
        release.set()
        assert await task == "late"


@pytest.mark.asyncio
async def test_cancel_observer_does_not_cancel_execution_or_leak_stop_waiter():
    before = set(asyncio.all_tasks())
    release, stop = asyncio.Event(), asyncio.Event()
    task = asyncio.create_task(release.wait())
    observer = asyncio.create_task(wait_for_task_settlement(task, cancelled=stop))
    await asyncio.sleep(0)
    observer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await observer
    assert not stop.is_set() and task.cancelling() == 0
    release.set()
    await task
    assert set(asyncio.all_tasks()) == before


@pytest.mark.asyncio
async def test_cancel_requests_exact_task_once_and_waits_for_its_finally():
    entered, cleaning, release, stop = (asyncio.Event() for _ in range(4))
    async def run():
        entered.set()
        try:
            await asyncio.Future()
        finally:
            cleaning.set()
            await release.wait()
    task = asyncio.create_task(run())
    await entered.wait()
    stop.set()
    calls = []
    def cancel():
        calls.append(task)
        task.cancel()
    observer = asyncio.create_task(wait_for_task_settlement(
        task, cancelled=stop, request_cancel=cancel, settlement_timeout=1))
    await cleaning.wait()
    assert not observer.done() and calls == [task]
    release.set()
    result = await observer
    assert result.settled and not result.timed_out and result.cancellation_requested
    assert task.cancelled()


@pytest.mark.asyncio
@pytest.mark.parametrize("bound", [-1, True, float("nan"), float("inf")])
async def test_invalid_bound_has_zero_task_effects(bound):
    task = asyncio.create_task(asyncio.sleep(0))
    stop = asyncio.Event()
    with pytest.raises(ValueError):
        await wait_for_task_settlement(task, cancelled=stop, settlement_timeout=bound)
    assert not stop.is_set() and task.cancelling() == 0
    await task
