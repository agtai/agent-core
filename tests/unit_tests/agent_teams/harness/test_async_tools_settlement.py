import asyncio
import threading

import pytest

from openjiuwen.agent_teams.harness.async_tools import AsyncToolRuntime
from openjiuwen.agent_teams.runtime.background_task_controller import BackgroundTaskController, SwarmflowRunHandle
from types import SimpleNamespace


async def async_noop():
    pass


async def until(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(0)
    await asyncio.wait_for(wait(), 2)


def runtime_with_output(**kwargs):
    output = []
    async def inject(text):
        output.append(text)
    return AsyncToolRuntime(inject=inject, **kwargs), output


@pytest.mark.asyncio
async def test_duplicate_live_id_cannot_replace_owner_and_terminal_id_can_relaunch():
    runtime, output = runtime_with_output()
    started, release = asyncio.Event(), asyncio.Event()
    async def run():
        started.set()
        await release.wait()
        return "first"
    runtime.launch("same", run, tool_name="tool", description="first")
    await started.wait()
    original = runtime.get("same")
    called = []
    async def replacement():
        called.append(True)
        return "second"
    try:
        with pytest.raises(ValueError, match="still owned"):
            runtime.launch("same", replacement, tool_name="tool", description="second")
        assert runtime.get("same") is original and called == [] and output == []
    finally:
        release.set()
        await runtime.wait("same", 2)
    assert original.execution_settled
    runtime.launch("same", replacement, tool_name="tool", description="second")
    await runtime.wait("same", 2)
    assert runtime.get("same").result == "second" and called == [True]
    assert len(output) == 2
    assert await runtime.cancel("same") is True
    assert runtime.get("same").status == "completed"


@pytest.mark.asyncio
async def test_unsettled_cancel_keeps_id_and_never_publishes_swallowed_cancel_result():
    runtime, output = runtime_with_output(cancel_settlement_seconds=0.01)
    started, cleaning, release = (asyncio.Event() for _ in range(3))
    async def run():
        started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cleaning.set()
            await release.wait()
            return "forbidden late result"
    runtime.launch("slow", run, tool_name="tool", description="d")
    await started.wait()
    try:
        assert await runtime.cancel("slow")
        await asyncio.wait_for(cleaning.wait(), 2)
        await until(lambda: runtime.get("slow").status == "unknown")
        row = await runtime.wait("slow", 0)
        assert row.cancellation_requested and not row.execution_settled
        assert runtime.has_running("tool") and output == []
        with pytest.raises(ValueError, match="still owned"):
            runtime.launch("slow", run, tool_name="tool", description="d")
        assert await runtime.cancel("slow")  # must not cancel the cleanup twice
        assert not release.is_set() and not row.execution_settled
    finally:
        release.set()
        await runtime.wait("slow", 2)
    assert row.execution_settled and row.status == "unknown"
    assert row.result is None and output == [] and not runtime.has_running("tool")


@pytest.mark.asyncio
async def test_cancel_before_start_never_calls_factory():
    runtime, output = runtime_with_output()
    calls = []
    async def run():
        calls.append(True)
    runtime.launch("early", run, tool_name="tool", description="d")
    assert await runtime.cancel("early")
    assert calls == [] and output == []
    assert runtime.get("early").execution_settled
    assert runtime.get("early").status == "error"


@pytest.mark.asyncio
async def test_cancel_during_spill_keeps_thread_owned_and_suppresses_injection(monkeypatch, tmp_path):
    from openjiuwen.agent_teams.harness import async_tools
    started, release, finished = (threading.Event() for _ in range(3))
    real_write = async_tools._write_output_file
    def write(path, text):
        started.set()
        if not release.wait(3):
            raise RuntimeError("test spill release missing")
        real_write(path, text)
        finished.set()
    monkeypatch.setattr(async_tools, "_write_output_file", write)
    runtime, output = runtime_with_output(
        output_dir_resolver=lambda: tmp_path, spill_threshold=1, cancel_settlement_seconds=0.01)
    async def run():
        return "real file operation"
    runtime.launch("spill", run, tool_name="tool", description="d")
    assert await asyncio.to_thread(started.wait, 2)
    try:
        assert await runtime.cancel("spill")
        row = runtime.get("spill")
        assert not finished.is_set() and not row.execution_settled and output == []
        assert runtime.has_running("tool")
    finally:
        release.set()
        await runtime.wait("spill", 2)
    assert finished.is_set() and row.execution_settled and output == []
    assert row.status in {"error", "unknown"} and row.result is None


@pytest.mark.asyncio
async def test_teardown_retains_execution_until_injection_cleanup_exits():
    entered, cleanup, release = (asyncio.Event() for _ in range(3))
    async def inject(text):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            cleanup.set()
            await release.wait()
    runtime = AsyncToolRuntime(inject=inject, cancel_settlement_seconds=0.01)
    async def run():
        return "result"
    runtime.launch("notification", run, tool_name="tool", description="d")
    await entered.wait()
    runtime.cancel_all()
    await cleanup.wait()
    try:
        row = await runtime.wait("notification", 0.01)
        assert not row.execution_settled and runtime.has_running("tool")
    finally:
        release.set()
        await runtime.wait("notification", 2)
    assert row.execution_settled


@pytest.mark.asyncio
async def test_swarmflow_resume_retries_same_intent_after_old_execution_settles():
    runtime, output = runtime_with_output(cancel_settlement_seconds=0.01)
    started, cleaning, release = (asyncio.Event() for _ in range(3))
    async def old():
        started.set()
        try:
            await asyncio.Future()
        finally:
            cleaning.set()
            await release.wait()
    async def fresh(task_id, inputs):
        from openjiuwen.agent_teams.context import get_session_id
        assert task_id != "flow" and inputs == {"script_path": "verified-flow.py"}
        assert get_session_id() == "shared-session"
        return "resumed"
    async def abort_sessions():
        pass
    ctl = BackgroundTaskController()
    def relaunch():
        from openjiuwen.agent_teams.workflow.tool_swarmflow import SwarmflowTool
        host = SimpleNamespace(card=SimpleNamespace(name="swarmflow"),
            _parent_agent=SimpleNamespace(launch_async_tool=runtime.launch),
            run_background=fresh, launched_description=lambda inputs: "resume")
        SwarmflowTool._relaunch(host, {"script_path": "verified-flow.py"}, "shared-session")
    runtime.launch("flow", old, tool_name="swarmflow", description="old")
    ctl.register(SwarmflowRunHandle("flow", asyncio.Event(),
        SimpleNamespace(abort_sessions=abort_sessions, aclose=async_noop), SimpleNamespace(async_tool_runtime=runtime), relaunch))
    await started.wait()
    try:
        assert await ctl.pause()
        assert await ctl.resume() is False
        assert ctl.is_paused() and output == []
    finally:
        release.set()
        await runtime.wait("flow", 2)
    assert await ctl.resume()
    fresh_record, = [row for row in runtime.list_all() if row.task_id != "flow"]
    await runtime.wait(fresh_record.task_id, 2)
    assert not ctl.is_paused() and fresh_record.result == "resumed"
    assert len(output) == 1


@pytest.mark.asyncio
async def test_tool_initiated_abort_is_terminal_without_completion_injection():
    runtime, output = runtime_with_output()
    async def aborted():
        raise asyncio.CancelledError("workflow abort event")
    runtime.launch("aborted", aborted, tool_name="swarmflow", description="d")
    record = await runtime.wait("aborted", 2)
    assert record.execution_settled and record.status == "error" and record.error == "cancelled"
    assert not record.cancellation_requested and not runtime.has_running("swarmflow")
    assert output == []


@pytest.mark.asyncio
async def test_cancel_tool_reports_unsettled_request_and_preserves_known_outcome():
    from openjiuwen.agent_teams.tools.locales import make_translator
    from openjiuwen.agent_teams.tools.tool_async import AsyncTaskCancelTool, AsyncTaskOutputTool
    runtime, output = runtime_with_output(cancel_settlement_seconds=0.01)
    started, release = asyncio.Event(), asyncio.Event()
    async def run():
        started.set()
        try:
            await asyncio.Future()
        finally:
            await release.wait()
    runtime.launch("tool", run, tool_name="tool", description="d")
    await started.wait()
    harness = SimpleNamespace(async_tool_runtime=runtime)
    tool = AsyncTaskCancelTool(harness, make_translator("cn"))
    try:
        receipt = await tool.invoke({"task_id": "tool"})
        assert receipt.success and receipt.data["cancellation_requested"]
        assert receipt.data["status"] in {"cancelling", "unknown"}
        assert not receipt.data["execution_settled"]
        assert "cancelled." not in tool.map_result(receipt)
        query = AsyncTaskOutputTool(harness, make_translator("cn"))
        observed = await query.invoke({"task_id": "tool"})
        assert not observed.data["execution_settled"] and observed.data["result"] == ""
        assert output == []
    finally:
        release.set()
        await runtime.wait("tool", 2)


@pytest.mark.asyncio
async def test_failed_avatar_abort_blocks_resume_until_actual_abort_succeeds():
    runtime, output = runtime_with_output()
    async def run():
        return "ready"
    runtime.launch("old", run, tool_name="swarmflow", description="d")
    await runtime.wait("old", 2)
    failures = [True]
    async def abort_sessions():
        if failures:
            raise OSError("avatar still owned")
    launched = []
    ctl = BackgroundTaskController()
    ctl.register(SwarmflowRunHandle("old", asyncio.Event(),
        SimpleNamespace(abort_sessions=abort_sessions, aclose=async_noop), SimpleNamespace(async_tool_runtime=runtime),
        lambda: launched.append(True)))
    assert await ctl.pause()
    assert not await ctl.resume()
    assert ctl.is_paused() and launched == []
    failures.clear()
    assert await ctl.resume()
    assert launched == [True] and not ctl.is_paused()


@pytest.mark.asyncio
async def test_relaunch_before_old_done_callback_settles_only_captured_row_and_event():
    runtime, output = runtime_with_output()
    release = asyncio.Event()
    async def first():
        return "first"
    async def second():
        await release.wait()
        return "second"
    runtime.launch("reuse", first, tool_name="tool", description="first")
    old = runtime.get("reuse")
    old_owner = runtime._tasks["reuse"]
    old_event = runtime._events["reuse"]
    waiting = asyncio.create_task(runtime.wait("reuse", 2))
    # Resume in the same ready-queue cycle before the task's done callbacks.
    while not old_owner.done():
        await asyncio.sleep(0)
    assert not old_event.is_set()
    runtime.launch("reuse", second, tool_name="tool", description="second")
    new = runtime.get("reuse")
    try:
        assert await waiting is old
        assert old.execution_settled and old_event.is_set()
        assert not new.execution_settled and not runtime._events["reuse"].is_set()
        assert runtime._tasks["reuse"] is not old_owner
    finally:
        release.set()
        await runtime.wait("reuse", 2)
    assert new.result == "second" and len(output) == 2


@pytest.mark.asyncio
async def test_cancel_receipt_does_not_observe_replacement_with_same_id():
    from openjiuwen.agent_teams.tools.locales import make_translator
    from openjiuwen.agent_teams.tools.tool_async import AsyncTaskCancelTool
    runtime, output = runtime_with_output()
    entered, release = asyncio.Event(), asyncio.Event()
    async def first():
        entered.set()
        await asyncio.Future()
    async def second():
        await release.wait()
        return "replacement"
    runtime.launch("reuse", first, tool_name="tool", description="first")
    await entered.wait()
    old = runtime.get("reuse")
    runtime._tasks["reuse"].add_done_callback(lambda _: runtime.launch(
        "reuse", second, tool_name="tool", description="second"))
    tool = AsyncTaskCancelTool(SimpleNamespace(async_tool_runtime=runtime), make_translator("cn"))
    try:
        receipt = await tool.invoke({"task_id": "reuse"})
        assert receipt.data["status"] == "error" and receipt.data["execution_settled"]
        assert receipt.data["cancellation_requested"] and old.execution_settled
        new = runtime.get("reuse")
        assert new is not old and not new.cancellation_requested and not new.execution_settled
        assert output == []
    finally:
        release.set()
        await runtime.wait("reuse", 2)
    assert new.result == "replacement" and len(output) == 1
