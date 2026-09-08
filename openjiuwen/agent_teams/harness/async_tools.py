# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Async background-tool framework, scoped to ``NativeHarness``.

A two-phase model mirroring Claude Code's background tools: a tool's
``invoke`` launches work and returns *immediately* with a ``launched`` result
so the ``tool_use``/``ToolMessage`` pair closes at once (the round is not
blocked); the real result is fed back *later* as an injected message — it never
rides back on the original ``tool_use`` (the LLM protocol forbids a suspended
``tool_result``, exactly as the Anthropic API does).

The framework lives entirely inside ``NativeHarness`` (a ``DeepAgent``
subclass) — it never touches ``TeamAgent``. Completion is injected through the
harness's own ``send(..., immediate=False)`` entry: IDLE starts a fresh round
to report, RUNNING queues a follow-up so an in-flight user turn is not
interrupted. A concrete async tool subclasses :class:`AsyncTool` and holds a
``parent_agent`` (the ``NativeHarness``) reference — the same shape
``sessions_spawn`` uses to reach the scheduler.

The runtime also backs the generic control tools (``async_tasks_list`` /
``async_task_output`` / ``async_task_cancel``) via ``list_all`` / ``get`` /
``wait`` / ``cancel``, and spills oversized results to disk so a large report
does not blow the leader's context — small results still inline in full.
"""
from __future__ import annotations

import asyncio
import json
import math
from abc import abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable

from openjiuwen.agent_teams.i18n import t
from openjiuwen.agent_teams.id_generator import generate_id
from openjiuwen.agent_teams.tools.tool_base import TeamTool
from openjiuwen.core.common.logging import team_logger
from openjiuwen.core.common.background_tasks import wait_for_task_settlement
from openjiuwen.harness.tools.base_tool import ToolOutput

# A completion-injection callback: hand the harness the model-facing text. Wired
# to ``NativeHarness.send(text, immediate=False)`` by the harness itself.
InjectCallback = Callable[[str], Awaitable[Any]]
CompletionFormatter = Callable[[Any], str | None]
FailureFormatter = Callable[[str], str | None]

# Result-spill summary length: how many leading characters of an oversized
# result stay inline (as a preview) next to the retrieval pointer.
_SPILL_SUMMARY_CHARS = 1024


def render_result_text(result: Any) -> str:
    """Render an async tool's return value to model-facing text, in full.

    No truncation — the leader receives the complete result. ``str`` passes
    through verbatim (a tool that already composed its own text); ``dict`` /
    ``list`` are pretty JSON; everything else falls back to ``str()``.

    Args:
        result: Whatever the tool's ``run_background`` returned.

    Returns:
        The full textual rendering (empty string for ``None``).
    """
    if result is None:
        return ""
    if isinstance(result, str):
        return result
    if isinstance(result, (dict, list)):
        return json.dumps(result, ensure_ascii=False, indent=2)
    return str(result)


def _write_output_file(path: Path, text: str) -> None:
    """Write spilled output to disk, creating parent dirs (runs off-loop)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


@dataclass
class AsyncToolRecord:
    """Registry row for one launched async-tool task (business view)."""

    task_id: str
    tool_name: str
    description: str
    status: str = "running"  # running | cancelling | unknown | completed | error
    result: Any = None
    error: str = ""
    output_file: str | None = None  # set when an oversized result spills to disk
    format_completed: CompletionFormatter | None = None
    format_failed: FailureFormatter | None = None
    execution_settled: bool = False
    cancellation_requested: bool = False


@dataclass
class AsyncToolRuntime:
    """Per-harness async background-task registry and completion injector.

    Owns the in-flight task map (so tasks are not garbage-collected mid-run and
    can be cancelled on teardown or by id) and a registry of
    :class:`AsyncToolRecord`. On completion it renders the result and injects it
    via ``inject`` — the harness's ``send(..., immediate=False)`` entry. Zero
    ``TeamAgent`` coupling.
    """

    inject: InjectCallback
    registry: dict[str, AsyncToolRecord] = field(default_factory=dict)
    # Oversized-result spill. ``output_dir_resolver`` is injected by the host
    # (TeamToolRail) and resolved lazily at completion time, since the session
    # id it needs is only available once a round runs; None keeps the original
    # full-inline behavior. A result whose text exceeds ``spill_threshold`` is
    # written to disk and the injected text becomes a summary + retrieval hint.
    output_dir_resolver: "Callable[[], Path | None] | None" = None
    spill_threshold: int = 32768
    # task_id -> Task / completion Event. The id-keyed task map (not a bare set)
    # lets ``cancel`` / ``wait`` address one task; the per-task Event wakes a
    # blocking ``wait`` the moment the record reaches a terminal state.
    _tasks: "dict[str, asyncio.Task]" = field(default_factory=dict)
    _events: "dict[str, asyncio.Event]" = field(default_factory=dict)
    _cancellations: dict[str, asyncio.Event] = field(default_factory=dict)
    cancel_settlement_seconds: float = 1.0

    def __post_init__(self) -> None:
        bound = self.cancel_settlement_seconds
        if isinstance(bound, bool) or not isinstance(bound, (int, float)) or not math.isfinite(bound) or bound <= 0:
            raise ValueError("cancel_settlement_seconds must be positive and finite")

    def has_running(self, tool_name: str) -> bool:
        """Return whether a task for ``tool_name`` is currently running."""
        return any(
            not record.execution_settled and record.tool_name == tool_name
            for record in self.registry.values()
        )

    def launch(
        self,
        task_id: str,
        coro_factory: Callable[[], Awaitable[Any]],
        *,
        tool_name: str,
        description: str,
        format_completed: CompletionFormatter | None = None,
        format_failed: FailureFormatter | None = None,
    ) -> None:
        """Schedule a background task and track it until completion.

        Args:
            task_id: Caller-generated unique id for this run.
            coro_factory: Zero-arg factory returning the coroutine to run. A
                factory (not a coroutine) keeps construction lazy and lets the
                task be created on the running loop.
            tool_name: The launching tool's name (for the completion message).
            description: Human-readable task description (for the registry).
            format_completed: Optional callback to render completion text.
            format_failed: Optional callback to render failure text.
        """
        previous = self._tasks.get(task_id)
        if previous is not None and not previous.done():
            raise ValueError(f"Async tool task {task_id!r} is still owned")
        record = self.registry[task_id] = AsyncToolRecord(
            task_id=task_id,
            tool_name=tool_name,
            description=description,
            format_completed=format_completed,
            format_failed=format_failed,
        )
        completion = self._events[task_id] = asyncio.Event()
        self._cancellations[task_id] = asyncio.Event()
        task = asyncio.create_task(self._run(task_id, coro_factory, tool_name))
        self._tasks[task_id] = task

        def finished(completed: asyncio.Task) -> None:
            # An already-done ID may have been relaunched before this callback.
            # Always settle its captured row/event, never the replacement's.
            record.execution_settled = True
            if completed.cancelled() and record.status in {"running", "cancelling"}:
                record.status = "error"
                record.error = "cancelled"
            completion.set()
            if self._tasks.get(task_id) is completed:
                self._tasks.pop(task_id, None)
                self._cancellations.pop(task_id, None)
            if not completed.cancelled():
                completed.exception()

        task.add_done_callback(finished)

    async def _run(
        self,
        task_id: str,
        coro_factory: Callable[[], Awaitable[Any]],
        tool_name: str,
    ) -> None:
        """Own execution until its coroutine and completion delivery exit."""
        record = self.registry[task_id]
        cancelled = self._cancellations[task_id]
        worker = asyncio.create_task(self._execute(task_id, record, cancelled, coro_factory, tool_name))
        try:
            settlement = await wait_for_task_settlement(
                worker, cancelled=cancelled,
                settlement_timeout=self.cancel_settlement_seconds,
                request_cancel=worker.cancel,
            )
            if not settlement.settled:
                record.status = "unknown"
                record.error = "cancellation_outcome_unknown"
            # Retain the exact ID and physical reservation even after timeout.
            await asyncio.shield(asyncio.gather(worker, return_exceptions=True))
        except asyncio.CancelledError:
            cancelled.set()
            record.cancellation_requested = True
            if not worker.done():
                worker.cancel()
            await asyncio.shield(asyncio.gather(worker, return_exceptions=True))
            raise
        finally:
            if (cancelled.is_set() or worker.cancelled()) and record.status not in {"completed", "error", "unknown"}:
                record.status = "error"
                record.error = "cancelled"

    async def _execute(
        self, task_id: str, record: AsyncToolRecord, cancelled: asyncio.Event,
        coro_factory: Callable[[], Awaitable[Any]], tool_name: str,
    ) -> None:
        """Two-phase protocol; stop fencing precedes every new completion effect."""
        if cancelled.is_set():
            return
        try:
            result = await coro_factory()
            if cancelled.is_set():
                return
            result_text = await self._maybe_spill(task_id, record, render_result_text(result))
            if cancelled.is_set():
                return
            completion_text = record.format_completed(result) if record.format_completed else None
            if completion_text is None:
                completion_text = t("async_tool.completed", tool=tool_name, result=result_text)
            if cancelled.is_set():
                return
            record.status = "completed"
            record.result = result
            await self._inject(completion_text)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - report any tool failure back
            if cancelled.is_set():
                return
            team_logger.error(
                "[AsyncToolRuntime] task %s (%s) failed: %s",
                task_id,
                tool_name,
                exc,
                exc_info=True,
            )
            record.status = "error"
            record.error = str(exc)
            failure_text = None
            if record.format_failed is not None:
                failure_text = record.format_failed(str(exc))
            if failure_text is None:
                failure_text = t("async_tool.failed", tool=tool_name, error=str(exc))
            if cancelled.is_set():
                return
            await self._inject(failure_text)

    async def _maybe_spill(
        self,
        task_id: str,
        record: AsyncToolRecord,
        text: str,
    ) -> str:
        """Spill an oversized result to disk; return the model-facing text.

        Small results (``len <= spill_threshold``) inline in full, preserving
        the complete-feedback behavior. Oversized results are written to the
        task's output file off the event loop, and the returned text becomes a
        leading summary plus a retrieval pointer (``async_task_output``). A
        resolver that is unset or yields no directory falls back to full inline,
        and a write failure degrades to inline rather than dropping the result.

        Args:
            task_id: The completed task's id (names the output file).
            record: The task record; ``output_file`` is set on a successful spill.
            text: The fully rendered result text.

        Returns:
            The text to inject — either the full result or a summary + pointer.
        """
        if len(text) <= self.spill_threshold or self.output_dir_resolver is None:
            return text
        out_dir = self.output_dir_resolver()
        if out_dir is None:
            return text
        path = out_dir / f"{task_id}.output"
        try:
            writing = asyncio.create_task(asyncio.to_thread(_write_output_file, path, text))
            try:
                await asyncio.shield(writing)
            except asyncio.CancelledError:
                # Cancelling an asyncio wrapper cannot stop its OS thread.
                # Keep ownership until that actual file operation exits.
                await asyncio.shield(asyncio.gather(writing, return_exceptions=True))
                raise
        except Exception:  # noqa: BLE001 - a spill failure degrades to inline
            team_logger.warning(
                "[AsyncToolRuntime] spill failed for %s; inlining full result",
                task_id,
                exc_info=True,
            )
            return text
        record.output_file = str(path)
        summary = text[:_SPILL_SUMMARY_CHARS]
        notice = t("async_tool.spilled_notice", path=str(path), task_id=task_id)
        return f"{summary}\n\n{notice}"

    def get(self, task_id: str) -> "AsyncToolRecord | None":
        """Return the record for ``task_id``, or None if unknown."""
        return self.registry.get(task_id)

    def list_all(self) -> "list[AsyncToolRecord]":
        """Return all task records in launch order."""
        return list(self.registry.values())

    async def cancel(self, task_id: str) -> bool:
        """Request exact cancellation and wait a bounded time for settlement.

        True means the target is known, not that cancellation has completed.
        Inspect ``status`` and ``execution_settled`` on the returned registry row.
        """
        record = self.registry.get(task_id)
        if record is None:
            return False
        if record.status in {"completed", "error"}:
            return True
        self._request_cancel(task_id)
        await self.wait(task_id, self.cancel_settlement_seconds)
        return True

    def _request_cancel(self, task_id: str) -> None:
        event = self._cancellations.get(task_id)
        if event is not None:
            record = self.registry[task_id]
            record.cancellation_requested = True
            if record.status == "running":
                record.status = "cancelling"
            event.set()

    async def wait(self, task_id: str, timeout: float) -> "AsyncToolRecord | None":
        """Block until ``task_id`` is terminal or ``timeout`` seconds elapse.

        Returns the record immediately if it is already terminal, or None if the
        task is unknown. On timeout it returns the still-running record rather
        than raising, so a caller can report "still running" without
        special-casing the exception.

        Args:
            task_id: The task to wait on.
            timeout: Maximum wait in seconds.

        Returns:
            The (possibly still-running) record, or None for an unknown id.
        """
        record = self.registry.get(task_id)
        if record is None:
            return None
        if record.execution_settled:
            return record
        event = self._events.get(task_id)
        if event is None:
            return record
        try:
            await asyncio.wait_for(event.wait(), timeout)
        except asyncio.TimeoutError:
            pass
        return record

    async def _inject(self, text: str) -> None:
        """Best-effort completion injection; a stopped harness must not raise."""
        try:
            await self.inject(text)
        except Exception:  # noqa: BLE001 - teardown races must not surface here
            team_logger.debug("[AsyncToolRuntime] completion injection skipped", exc_info=True)

    def cancel_all(self) -> None:
        """Cancel all in-flight tasks (teardown)."""
        for task_id in list(self._tasks):
            self._request_cancel(task_id)


class AsyncTool(TeamTool):
    """Base class for two-phase async background tools.

    ``invoke`` launches ``run_background`` on the harness's runtime and returns
    a ``launched`` result immediately. The real result is injected later by the
    runtime. Subclasses implement ``run_background`` (and optionally override
    ``launched_description``); they hold ``parent_agent`` — the ``NativeHarness``
    exposing ``launch_async_tool`` — wired in at rail-init time.
    """

    def __init__(self, card: Any, parent_agent: Any, language: str = "cn") -> None:
        """Initialize the async tool.

        Args:
            card: The tool's ``ToolCard``.
            parent_agent: The owning ``NativeHarness`` (exposes
                ``launch_async_tool``). Held as a reference; only used at invoke
                time, by which point the harness has started.
            language: Language code for model-facing text.
        """
        super().__init__(card)
        self._parent_agent = parent_agent
        self._language = language

    @abstractmethod
    async def run_background(self, task_id: str, inputs: dict[str, Any]) -> Any:
        """Run the actual work in the background and return the full result.

        Args:
            task_id: The launched task's unique id.
            inputs: The tool-call arguments.

        Returns:
            The complete result (rendered to text by the runtime).
        """
        ...

    def launched_description(self, inputs: dict[str, Any]) -> str:
        """Return a short task description for the registry (override me)."""
        return self.card.name

    async def invoke(self, inputs: dict[str, Any], **kwargs: Any) -> ToolOutput:
        """Launch the background task and return immediately with ``launched``."""
        task_id = generate_id(self.card.name)
        try:
            self._parent_agent.launch_async_tool(
                task_id,
                lambda: self.run_background(task_id, inputs),
                tool_name=self.card.name,
                description=self.launched_description(inputs),
            )
        except Exception as exc:  # noqa: BLE001 - never escape as an exception
            return ToolOutput(success=False, error=f"Internal error: {exc}")
        return ToolOutput(success=True, data={"status": "launched", "task_id": task_id})

    def map_result(self, output: ToolOutput) -> str:
        if not output.success:
            return output.error or "Failed to launch async tool"
        return t("async_tool.launched", tool=self.card.name, task_id=output.data["task_id"])


__all__ = [
    "AsyncToolRecord",
    "AsyncToolRuntime",
    "AsyncTool",
    "CompletionFormatter",
    "FailureFormatter",
    "InjectCallback",
    "render_result_text",
]
