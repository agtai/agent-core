# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Background task controller: external pause/resume for leader background work.

Owned by the leader harness by default (or explicitly supplied through Runner),
this is the control surface for long-running background tools (today: the
leader's swarmflow run). A single object instead of a growing
set of Runner facade methods, so new controls / callbacks extend the object, not
the SDK surface.

The controller is a registry + control plane: each live swarmflow run registers
a :class:`SwarmflowRunHandle` at launch (carrying the engine abort signal, the
worker backend, the owning harness, and a relaunch closure) and deregisters on
completion. ``pause`` / ``resume`` operate on the registered handles.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

from openjiuwen.core.common.logging import team_logger

if TYPE_CHECKING:
    from openjiuwen.agent_teams.interaction.payload import DeliverResult


@dataclass
class SwarmflowRunHandle:
    """Control handles for one live swarmflow run (registered at launch)."""

    task_id: str
    abort_event: asyncio.Event  # engine Runtime.abort_event for THIS run
    backend: Any  # TeamWorkerBackend → abort_sessions()
    native: Any  # leader NativeHarness → async_tool_runtime.cancel
    relaunch: Callable[[], None]  # re-launch run_background with the SAME inputs
    _record: Any = field(default=None, init=False, repr=False)


@dataclass
class _PausedRun:
    handle: SwarmflowRunHandle
    record: Any
    sessions_aborted: bool


class BackgroundTaskController:
    """Unified pause/resume control surface threaded through streaming.

    Lifecycle-neutral: owned by the leader harness, optionally supplied externally,
    and self-populated by ``SwarmflowTool`` as runs launch. Pausing / resuming
    with no matching run is a no-op (returns ``False``).
    """

    def __init__(self) -> None:
        self._active: dict[str, SwarmflowRunHandle] = {}
        self._paused: dict[str, _PausedRun] = {}
        self._lock = asyncio.Lock()
        self._human_reply_admission_factory: Callable[[str | None, str], Callable[[], bool]] | None = None

    # ------------------------------------------------------------------
    # Registration seam (SwarmflowTool self-registers at launch)
    # ------------------------------------------------------------------

    def register(self, handle: SwarmflowRunHandle) -> None:
        """Register a live run's control handles (called at launch)."""
        handle._record = handle.native.async_tool_runtime.get(handle.task_id)
        self._active[handle.task_id] = handle
        self._bind_human_reply_owner(handle)

    def deregister(self, task_id: str) -> None:
        """Drop a run's handles (called in the launcher's finally; idempotent)."""
        self._active.pop(task_id, None)

    # ------------------------------------------------------------------
    # Control surface (embedder)
    # ------------------------------------------------------------------

    def has_owned_runs(self) -> bool:
        """Whether replacing this controller would abandon active/paused runs."""
        return bool(self._active or self._paused)

    def bind_human_reply_admission(
        self, factory: Callable[[str | None, str], Callable[[], bool]],
    ) -> None:
        """Bind run consumers to the original pool's live lifecycle authority."""
        self._human_reply_admission_factory = factory
        for handle in self._active.values():
            self._bind_human_reply_owner(handle)

    def _bind_human_reply_owner(self, handle: SwarmflowRunHandle) -> None:
        factory = self._human_reply_admission_factory
        bind = getattr(handle.backend, "bind_human_reply_admission", None)
        if factory is None or bind is None:
            return
        session_id, team_name, _ = handle.backend.human_reply_scope
        pool_admitted = factory(session_id, team_name)
        # Installed once on the backend; re-binding a reused controller never
        # lends an old backend the identity of a replacement pool entry.
        bind(lambda: pool_admitted() and self._run_receives_human_input(handle))

    def _run_receives_human_input(self, handle: SwarmflowRunHandle) -> bool:
        record = handle.native.async_tool_runtime.get(handle.task_id)
        return (
            self._active.get(handle.task_id) is handle and not handle.abort_event.is_set()
            and record is not None and record is handle._record and not record.execution_settled
            and not record.cancellation_requested and record.status == "running"
        )

    def reply_swarmflow_human(
        self,
        *,
        session_id: str,
        team_name: str,
        run_id: str,
        correlation_id: str,
        answer: str,
        before_effect: Callable[[], None] | None = None,
    ) -> "DeliverResult":
        """Forward to one exact live run's original human-input owner.

        This synchronous path shares the event loop with register/pause and the
        avatar's bus handler. It cannot yield between scope validation and the
        original Future's consume operation.
        """
        from openjiuwen.agent_teams.interaction.payload import DeliverResult

        scope = (session_id, team_name, run_id)
        matches = [handle for handle in self._active.values()
                   if getattr(handle.backend, "human_reply_scope", None) == scope]
        if not matches:
            return DeliverResult.failure("unknown_run")
        if len(matches) != 1:
            return DeliverResult.failure("ambiguous_run")
        handle = matches[0]
        if not self._run_receives_human_input(handle):
            return DeliverResult.failure("run_closed")
        return handle.backend.reply_swarmflow_human(
            session_id=session_id, team_name=team_name, run_id=run_id,
            correlation_id=correlation_id, answer=answer, before_effect=before_effect,
        )

    async def pause(self) -> bool:
        """Pause every active background run. Returns ``False`` when none active.

        Three steps per run, in this order (correctness-critical):

        1. set the engine ``abort_event`` — queued ``agent()`` / session turns are
           gated, and an in-flight call reaching the pre-journal guard does NOT
           persist to the WAL;
        2. abort live avatar sessions — their supervisor is a separate asyncio
           task the top-level cancel cannot reach, so abort them here in the
           controller coroutine where it runs to completion (else the supervisor
           leaks);
        3. cancel the top-level swarmflow task — stops the in-flight ``run_once``
           worker (not abortable) and unwinds the engine; the WAL is preserved
           (``finalize`` is skipped on the cancel path) for resume.
        """
        async with self._lock:
            if not self._active:
                return False
            for task_id, handle in list(self._active.items()):
                record = handle.native.async_tool_runtime.get(task_id)
                handle.abort_event.set()
                sessions_aborted = False
                try:
                    await handle.backend.abort_sessions()
                    sessions_aborted = True
                except Exception:  # noqa: BLE001 - best effort; cancel still stops the run
                    team_logger.debug("[bg-ctl] abort_sessions failed for %s", task_id, exc_info=True)
                try:
                    await handle.native.async_tool_runtime.cancel(task_id)
                except Exception:  # noqa: BLE001 - cancel is best-effort
                    team_logger.debug("[bg-ctl] cancel failed for %s", task_id, exc_info=True)
                self._paused[task_id] = _PausedRun(handle, record, sessions_aborted)
                self._active.pop(task_id, None)
            return True

    async def resume(self) -> bool:
        """Resume every paused run by relaunching it. Returns ``False`` when none.

        The relaunch closure re-invokes ``run_background`` with the SAME inputs;
        the journal path is unchanged, so the completed prefix is a cache hit and
        only the interrupted call reruns live.
        """
        async with self._lock:
            if not self._paused:
                return False
            resumed = False
            for task_id, paused in list(self._paused.items()):
                handle = paused.handle
                # Swarmflow relaunch uses a fresh task ID. Fence the original
                # execution explicitly; duplicate-ID rejection alone cannot do it.
                record = handle.native.async_tool_runtime.get(task_id)
                if record is None or record is not paused.record or not record.execution_settled:
                    continue
                try:
                    if not paused.sessions_aborted:
                        await handle.backend.abort_sessions()
                        paused.sessions_aborted = True
                    # The engine's finally is best effort. Retry retained avatar
                    # disposal before starting another run against the same WAL.
                    await handle.backend.aclose()
                    handle.relaunch()
                except Exception:  # noqa: BLE001 - a failed relaunch must not strand the rest
                    team_logger.debug("[bg-ctl] relaunch failed for %s", task_id, exc_info=True)
                    # The old execution may still be settling. Preserve this
                    # exact relaunch intent for retry; a failure is not a resume.
                    continue
                self._paused.pop(task_id, None)
                resumed = True
            return resumed

    def is_paused(self) -> bool:
        """Whether any run is currently paused (awaiting resume)."""
        return bool(self._paused)


__all__ = ["BackgroundTaskController", "SwarmflowRunHandle"]
