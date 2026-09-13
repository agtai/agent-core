# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Authorized delivery of canonical formal TaskEvents.

The default reader deliberately starts at the Store's current event head and is
live-only.  The authority replay mode instead consumes one Store-owned atomic
prefix/cursor snapshot before reading its durable suffix.  Neither mode exposes
a caller-selected cursor or lets a consumer fabricate lifecycle history.
"""

from __future__ import annotations

import asyncio
import math
import threading
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from openjiuwen.core.application.tasks.contracts import (
    Assurance,
    ContractViolation,
    ErrorCode,
    LifecycleKind,
    ScopeRef,
    TerminalOutcome,
    canonical_json_bytes,
    validate_transition,
)
from openjiuwen.core.application.tasks.formal_task_models import (
    AdmissionPriority,
    FormalAttemptState,
    FormalTaskSpec,
    FormalTaskState,
    FormalTaskViolation,
    PersistentAttemptRecord,
    PersistentTaskEvent,
    PersistentTaskRecord,
    TaskAuthorizationGrant,
    TaskEventAuthoritySnapshot,
    utc_now,
)

_EVENTS_CAPABILITY = frozenset({"task.events"})
_TASK_LIFECYCLE_EVENT_STATES = {
    "task.accepted": FormalTaskState.ACCEPTED,
    "task.retry_accepted": FormalTaskState.ACCEPTED,
    "task.recovery_accepted": FormalTaskState.ACCEPTED,
    "task.running": FormalTaskState.RUNNING,
    "task.blocked": FormalTaskState.BLOCKED,
    "task.decision_required": FormalTaskState.DECISION_REQUIRED,
    "task.terminal": FormalTaskState.TERMINAL,
}
_ATTEMPT_LIFECYCLE_EVENT_STATES = {
    "attempt.accepted": FormalAttemptState.ACCEPTED,
    "attempt.running": FormalAttemptState.RUNNING,
    "attempt.terminal": FormalAttemptState.TERMINAL,
}
_ATTEMPT_TRANSITIONS = {
    FormalAttemptState.ACCEPTED: frozenset(
        {
            FormalAttemptState.ACCEPTED,
            FormalAttemptState.RUNNING,
            FormalAttemptState.TERMINAL,
        }
    ),
    FormalAttemptState.RUNNING: frozenset({FormalAttemptState.TERMINAL}),
    FormalAttemptState.TERMINAL: frozenset(),
}
_INTERNAL_ATTEMPT_TERMINAL_PRODUCERS = frozenset({"task_core.delivery", "task_core.reconciliation"})
_TASK_EVENT_PRODUCERS = {
    "task.accepted": frozenset({"task_core"}),
    "task.retry_accepted": frozenset({"task_core"}),
    "task.recovery_accepted": frozenset({"task_core"}),
    "task.running": frozenset({"task_core"}),
    "task.blocked": frozenset({"task_core"}),
    "task.decision_required": frozenset({"task_core"}),
    "task.terminal": frozenset({"task_core", "task_core.delivery", "task_core.reconciliation"}),
    "task.cancel_requested": frozenset({"task_core.control"}),
    "task.adjust_requested": frozenset({"task_core.control"}),
    "task.adjust_applied": frozenset({"task_core.control"}),
    "task.adjust_rejected": frozenset({"task_core.control"}),
    "task.update_requested": frozenset({"task_core.control"}),
    "task.update_applied": frozenset({"task_core.control"}),
    "task.reprioritize_requested": frozenset({"task_core.control"}),
    "task.reprioritize_applied": frozenset({"task_core.control"}),
}
_CANONICAL_EVENT_TYPES = frozenset(_TASK_EVENT_PRODUCERS) | frozenset(_ATTEMPT_LIFECYCLE_EVENT_STATES)


class TaskEventSource(Protocol):
    """Read-only surface implemented directly by ``SqliteTaskStore``."""

    def get_task(self, task_id: str, scope: ScopeRef) -> PersistentTaskRecord: ...

    def get_attempt(self, attempt_id: str) -> PersistentAttemptRecord: ...

    def events(
        self,
        task_id: str,
        scope: ScopeRef,
        *,
        after_seq: int = -1,
        attempt_id: str | None = None,
    ) -> tuple[PersistentTaskEvent, ...]: ...


class TaskEventAuthoritySource(TaskEventSource, Protocol):
    """Store authority surface for an atomic prefix/cursor handoff."""

    def event_authority_snapshot(
        self, task_id: str, scope: ScopeRef, *, max_events: int
    ) -> TaskEventAuthoritySnapshot: ...


class TaskEventSubscriptionState(StrEnum):
    NEW = "new"
    DISABLED = "disabled"
    ACTIVE = "active"
    DETACHING = "detaching"
    TERMINAL_PENDING = "terminal_pending"
    CLOSED = "closed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class TaskEventSubscriptionSnapshot:
    enabled: bool
    state: TaskEventSubscriptionState
    task_id: str
    start_head_seq: int | None
    last_seq: int | None
    queue_capacity: int
    queue_allocated: bool
    queued_events: int
    validation_capacity: int
    tracked_events: int
    worker_pending: bool
    source_reads: int
    live_only: bool
    cursor_replay_supported: bool
    terminal_event_seen: bool
    terminal_event_delivered: bool
    close_reason: str | None
    failure_reason: str | None
    failure_code: ErrorCode | None
    discarded_events: int
    segment_start_seq: int | None
    attempt_id: str | None
    attempt_number: int | None


def _violation(reason: str, message: str, code: ErrorCode) -> FormalTaskViolation:
    return FormalTaskViolation(reason, message, code)


class TaskEventSubscription:
    """One bounded, exact-task feed over the formal SQLite Task Store.

    Authorization and the initial Store head read happen before allocation of the
    queue or polling worker. Authority replay additionally validates the complete
    prefix against its snapshot before exposing any event. Closing only detaches
    this reader; it never issues a task command or mutates Store/outbox state.
    """

    def __init__(
        self,
        *,
        source: TaskEventSource,
        authorization: TaskAuthorizationGrant | None,
        scope: ScopeRef,
        task_id: str,
        enabled: bool = False,
        queue_capacity: int = 32,
        validation_capacity: int = 4096,
        poll_interval: float = 0.05,
        authority_atomic_replay: bool = False,
        consumer_scope: bool = False,
        clock: Callable[[], str] = utc_now,
    ) -> None:
        if type(task_id) is not str or not task_id.strip():
            raise _violation(
                "INVALID_TASK_EVENT_SUBSCRIPTION_TARGET",
                "task event subscription requires a non-empty task id",
                ErrorCode.INVALID_ARGUMENT,
            )
        if not isinstance(scope, ScopeRef):
            raise _violation(
                "INVALID_TASK_EVENT_SUBSCRIPTION_SCOPE",
                "task event subscription requires a ScopeRef",
                ErrorCode.INVALID_ARGUMENT,
            )
        if type(enabled) is not bool:
            raise _violation(
                "INVALID_TASK_EVENT_SUBSCRIPTION_FLAG",
                "task event subscription flag must be boolean",
                ErrorCode.INVALID_ARGUMENT,
            )
        if type(authority_atomic_replay) is not bool:
            raise _violation(
                "INVALID_TASK_EVENT_AUTHORITY_MODE",
                "TaskEvent authority replay mode must be boolean",
                ErrorCode.INVALID_ARGUMENT,
            )
        if type(consumer_scope) is not bool:
            raise _violation(
                "INVALID_TASK_EVENT_AUTHORITY_MODE",
                "TaskEvent consumer scope mode must be boolean",
                ErrorCode.INVALID_ARGUMENT,
            )
        if type(queue_capacity) is not int or queue_capacity <= 0:
            raise _violation(
                "INVALID_TASK_EVENT_SUBSCRIPTION_CAPACITY",
                "task event subscription queue capacity must be positive",
                ErrorCode.INVALID_ARGUMENT,
            )
        if type(validation_capacity) is not int or validation_capacity <= 0:
            raise _violation(
                "INVALID_TASK_EVENT_VALIDATION_CAPACITY",
                "TaskEvent validation capacity must be positive",
                ErrorCode.INVALID_ARGUMENT,
            )
        if (
            isinstance(poll_interval, bool)
            or not isinstance(poll_interval, (int, float))
            or not math.isfinite(poll_interval)
            or poll_interval <= 0
        ):
            raise _violation(
                "INVALID_TASK_EVENT_SUBSCRIPTION_INTERVAL",
                "task event subscription poll interval must be positive and finite",
                ErrorCode.INVALID_ARGUMENT,
            )
        self._source = source
        self._authorization = authorization
        self._scope = scope
        self._task_id = task_id
        self._enabled = enabled
        self._queue_capacity = queue_capacity
        self._validation_capacity = validation_capacity
        self._poll_interval = float(poll_interval)
        self._authority_atomic_replay = authority_atomic_replay
        self._consumer_scope = consumer_scope
        self._clock = clock

        # Locks contain no background work.  Queue, Events and worker are created
        # only after authorization and the initial exact-object read succeed.
        self._lifecycle_lock = asyncio.Lock()
        self._consumer_lock = asyncio.Lock()
        self._close_intent_lock = threading.Lock()
        self._close_requested = False
        self._close_request_reason: str | None = None
        self._state = TaskEventSubscriptionState.NEW
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._queue: asyncio.Queue[PersistentTaskEvent] | None = None
        self._changed: asyncio.Event | None = None
        self._detach: asyncio.Event | None = None
        self._worker: asyncio.Task[None] | None = None
        self._start_head_seq: int | None = None
        self._last_seq: int | None = None
        self._attempt_id: str | None = None
        self._previous_attempt_id: str | None = None
        self._attempt_number: int | None = None
        self._segment_start_seq: int | None = None
        self._attempt_executor_id: str | None = None
        self._correlation_id: str | None = None
        self._task_state: FormalTaskState | None = None
        self._task_outcome: str | None = None
        self._attempt_state: FormalAttemptState | None = None
        self._attempt_outcome: str | None = None
        self._seen_event_ids: dict[str, bytes] = {}
        self._seen_sequences: dict[int, bytes] = {}
        self._source_reads = 0
        self._terminal_event_seen = False
        self._terminal_event_delivered = False
        self._close_reason: str | None = None
        self._failure: FormalTaskViolation | None = None
        self._discarded_events = 0

    async def start(self) -> bool:
        """Start at the current Store head; return false only for off/idempotent start."""

        async with self._lifecycle_lock:
            if not self._enabled:
                if self._state is TaskEventSubscriptionState.NEW:
                    self._state = TaskEventSubscriptionState.DISABLED
                    self._close_reason = "feature_off"
                return False
            if self._state in {
                TaskEventSubscriptionState.ACTIVE,
                TaskEventSubscriptionState.TERMINAL_PENDING,
            }:
                self._require_owner_loop()
                return False
            if self._state is not TaskEventSubscriptionState.NEW:
                raise _violation(
                    "TASK_EVENT_SUBSCRIPTION_CLOSED",
                    "a settled task event subscription cannot restart",
                    ErrorCode.CONFLICT,
                )

            if self._settle_start_close_intent():
                return False
            self._authorize_current_read()
            if self._settle_start_close_intent():
                return False
            current_loop = asyncio.get_running_loop()
            if self._owner_loop is None:
                # Authorization failure must not bind this otherwise NEW reader
                # to a loop or constrain a later close.
                self._owner_loop = current_loop
            elif self._owner_loop is not current_loop:
                raise _violation(
                    "TASK_EVENT_SUBSCRIPTION_LOOP_MISMATCH",
                    "task event subscription belongs to another event loop",
                    ErrorCode.CONFLICT,
                )

            try:
                if self._authority_atomic_replay:
                    return await self._start_authority_atomic_replay()
                return await self._start_authorized_baseline()
            except BaseException:
                # Preserve the original error/cancellation, but do not strand a
                # resource-free NEW reader on the loop used by a failed start.
                self._cleanup_preallocation_start_failure(current_loop)
                raise

    async def _start_authority_atomic_replay(self) -> bool:
        snapshot_reader = getattr(self._source, "event_authority_snapshot", None)
        if not callable(snapshot_reader):
            raise _violation(
                "TASK_EVENT_AUTHORITY_HANDOFF_UNAVAILABLE",
                "TaskEvent source does not own an atomic prefix/cursor read",
                ErrorCode.UNAVAILABLE,
            )
        try:
            snapshot = await asyncio.to_thread(
                snapshot_reader,
                self._task_id,
                self._scope,
                max_events=min(self._queue_capacity, self._validation_capacity),
            )
            self._source_reads += 1
        except FormalTaskViolation:
            raise
        except Exception as error:
            raise _violation(
                "TASK_EVENT_SOURCE_FAILURE",
                "formal TaskEvent authority snapshot failed",
                ErrorCode.UNAVAILABLE,
            ) from error
        if self._settle_start_close_intent():
            return False
        self._authorize_current_read()
        if self._settle_start_close_intent():
            return False
        if not isinstance(snapshot, TaskEventAuthoritySnapshot):
            raise _violation(
                "TASK_EVENT_SOURCE_PROTOCOL_VIOLATION",
                "TaskEvent authority returned an invalid snapshot",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        task = snapshot.task
        attempt = snapshot.attempt
        self._validate_start_snapshot(task)
        self._validate_attempt_snapshot(task, attempt)
        events = snapshot.events
        if snapshot.cursor != task.event_head or not events or events[-1].seq != snapshot.cursor:
            raise _violation(
                "TASK_EVENT_AUTHORITY_SNAPSHOT_CURSOR_MISMATCH",
                "TaskEvent authority cursor does not close its exact prefix",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if len(events) > min(self._queue_capacity, self._validation_capacity):
            raise _violation(
                "TASK_EVENT_AUTHORITY_PREFIX_CAPACITY",
                "TaskEvent authority prefix exceeds the declared bounded reader capacity",
                ErrorCode.UNAVAILABLE,
            )

        genesis = events[0]
        successor_boundary = genesis.event_type if attempt.attempt_number > 1 else None
        recovery_boundary = successor_boundary == "task.recovery_accepted"
        boundary_type_valid = (
            genesis.event_type == "task.accepted"
            if attempt.attempt_number == 1
            else successor_boundary in {"task.retry_accepted", "task.recovery_accepted"}
        )
        expected_details = {"command_id": genesis.causation_id} if attempt.attempt_number == 1 else None
        if (
            genesis.seq != snapshot.start_seq
            or not boundary_type_valid
            or genesis.state != FormalTaskState.ACCEPTED.value
            or genesis.outcome is not None
            or genesis.producer != "task_core"
            or genesis.source_event_id is not None
            or genesis.attempt_id != attempt.attempt_id
            or (attempt.attempt_number == 1 and (snapshot.start_seq != 0 or dict(genesis.details) != expected_details))
            or (
                attempt.attempt_number > 1
                and (
                    snapshot.start_seq <= 0
                    or set(genesis.details)
                    != (
                        {
                            "recovery_id",
                            "producer_attempt_id",
                            "producer_outcome",
                            "recovery_generation",
                            "recovery_budget_remaining",
                            "attempt_number",
                        }
                        if recovery_boundary
                        else {
                            "command_id",
                            "retry_of_attempt_id",
                            "previous_outcome",
                            "attempt_number",
                        }
                    )
                    or genesis.details.get("attempt_number") != attempt.attempt_number
                    or (
                        recovery_boundary
                        and (
                            genesis.details.get("recovery_id") != genesis.causation_id
                            or type(genesis.details.get("producer_attempt_id")) is not str
                            or not str(genesis.details.get("producer_attempt_id")).strip()
                            or genesis.details.get("producer_attempt_id") == attempt.attempt_id
                            or genesis.details.get("producer_outcome") != TerminalOutcome.INTERRUPTED.value
                            or type(genesis.details.get("recovery_generation")) is not int
                            or int(genesis.details.get("recovery_generation", 0)) <= 0
                            or type(genesis.details.get("recovery_budget_remaining")) is not int
                            or int(genesis.details.get("recovery_budget_remaining", -1)) < 0
                        )
                    )
                    or (
                        not recovery_boundary
                        and (
                            genesis.details.get("command_id") != genesis.causation_id
                            or type(genesis.details.get("retry_of_attempt_id")) is not str
                            or not str(genesis.details.get("retry_of_attempt_id")).strip()
                            or genesis.details.get("retry_of_attempt_id") == attempt.attempt_id
                            or genesis.details.get("previous_outcome")
                            not in {
                                TerminalOutcome.CANCELLED.value,
                                TerminalOutcome.COMPLETED.value,
                            }
                        )
                    )
                )
            )
        ):
            raise _violation(
                "TASK_EVENT_AUTHORITY_SEGMENT_BOUNDARY_INVALID",
                "TaskEvent authority segment lacks its canonical attempt boundary",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        try:
            genesis_bytes = canonical_json_bytes(genesis.to_dict())
        except (TypeError, ValueError) as error:
            raise _violation(
                "TASK_EVENT_SOURCE_PROTOCOL_VIOLATION",
                "TaskEvent authority genesis cannot be canonically validated",
                ErrorCode.PROTOCOL_VIOLATION,
            ) from error

        # No await occurs while this lock is held. A cross-thread close either
        # wins before allocation or observes a fully initialized retained feed.
        with self._close_intent_lock:
            if self._close_requested:
                self._state = TaskEventSubscriptionState.CLOSED
                self._close_reason = self._close_request_reason or "consumer_detached"
                return False
            self._queue = asyncio.Queue(maxsize=self._queue_capacity)
            self._changed = asyncio.Event()
            self._detach = asyncio.Event()
            self._state = TaskEventSubscriptionState.ACTIVE
            self._start_head_seq = snapshot.cursor
            self._last_seq = snapshot.start_seq
            self._attempt_id = task.attempt_id
            self._previous_attempt_id = (
                None if attempt.attempt_number == 1 else str(genesis.details["retry_of_attempt_id"])
            )
            self._attempt_number = attempt.attempt_number
            self._segment_start_seq = snapshot.start_seq
            self._attempt_executor_id = task.spec.executor_id
            self._correlation_id = task.correlation_id
            self._task_state = FormalTaskState.ACCEPTED
            self._task_outcome = None
            self._attempt_state = FormalAttemptState.ACCEPTED
            self._attempt_outcome = None
            self._seen_event_ids = {genesis.event_id: genesis_bytes}
            self._seen_sequences = {snapshot.start_seq: genesis_bytes}
            self._queue.put_nowait(genesis)
            if len(events) > 1 and not self._accept_batch(events[1:]):
                assert self._failure is not None
                raise self._failure

            expected_task_outcome = None if task.outcome is None else task.outcome.value
            expected_attempt_outcome = None if attempt.outcome is None else attempt.outcome.value
            if (
                self._last_seq != snapshot.cursor
                or self._task_state is not task.state
                or self._task_outcome != expected_task_outcome
                or self._attempt_state is not attempt.state
                or self._attempt_outcome != expected_attempt_outcome
            ):
                error = _violation(
                    "TASK_EVENT_AUTHORITY_SNAPSHOT_STATE_MISMATCH",
                    "TaskEvent authority prefix does not reduce to its task snapshot",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
                self._fail(error)
                raise error
        return True

    def _start_authority_tail_if_ready(self) -> None:
        if (
            not self._authority_atomic_replay
            or self._state is not TaskEventSubscriptionState.ACTIVE
            or self._worker is not None
            or self._queue is None
            or not self._queue.empty()
        ):
            return
        assert self._owner_loop is not None
        self._worker = self._owner_loop.create_task(
            self._poll_loop(),
            name=f"live-voice-task-events-authority:{self._task_id}",
        )

    async def _start_authorized_baseline(self) -> bool:
        try:
            task = await asyncio.to_thread(self._source.get_task, self._task_id, self._scope)
        except FormalTaskViolation:
            raise
        except Exception as error:
            raise _violation(
                "TASK_EVENT_SOURCE_FAILURE",
                "formal TaskEvent source failed during subscription start",
                ErrorCode.UNAVAILABLE,
            ) from error
        if self._settle_start_close_intent():
            return False
        self._authorize_current_read()
        if self._settle_start_close_intent():
            return False
        self._validate_start_snapshot(task)

        if self._settle_start_close_intent():
            return False
        self._authorize_current_read()
        if self._settle_start_close_intent():
            return False
        try:
            attempt = await asyncio.to_thread(self._source.get_attempt, task.attempt_id)
        except FormalTaskViolation:
            raise
        except Exception as error:
            raise _violation(
                "TASK_EVENT_SOURCE_FAILURE",
                "formal attempt source failed during subscription start",
                ErrorCode.UNAVAILABLE,
            ) from error
        if self._settle_start_close_intent():
            return False
        self._authorize_current_read()
        if self._settle_start_close_intent():
            return False
        attempt_state = self._validate_attempt_snapshot(task, attempt)

        # The Store currently exposes task and attempt reads separately. Bracket
        # the attempt read and reject change instead of mixing two revisions.
        if self._settle_start_close_intent():
            return False
        self._authorize_current_read()
        if self._settle_start_close_intent():
            return False
        try:
            confirmed_task = await asyncio.to_thread(self._source.get_task, self._task_id, self._scope)
        except FormalTaskViolation:
            raise
        except Exception as error:
            raise _violation(
                "TASK_EVENT_SOURCE_FAILURE",
                "formal TaskEvent source failed during subscription start",
                ErrorCode.UNAVAILABLE,
            ) from error
        if self._settle_start_close_intent():
            return False
        self._authorize_current_read()
        if self._settle_start_close_intent():
            return False
        self._validate_start_snapshot(confirmed_task)
        if confirmed_task != task:
            raise _violation(
                "TASK_EVENT_START_SNAPSHOT_CHANGED",
                "formal task changed while the live feed baseline was read",
                ErrorCode.CONFLICT,
            )
        self._validate_attempt_snapshot(confirmed_task, attempt)

        self._start_head_seq = task.event_head
        self._last_seq = task.event_head
        self._attempt_id = task.attempt_id
        self._previous_attempt_id = None
        self._attempt_number = attempt.attempt_number
        self._segment_start_seq = None
        self._attempt_executor_id = task.spec.executor_id
        self._correlation_id = task.correlation_id
        self._task_state = task.state
        self._task_outcome = None if task.outcome is None else task.outcome.value
        self._attempt_state = attempt_state
        self._attempt_outcome = None if attempt.outcome is None else attempt.outcome.value
        if task.state is FormalTaskState.TERMINAL:
            self._state = TaskEventSubscriptionState.CLOSED
            self._close_reason = "already_terminal_at_start_head"
            return True

        # Linearize allocation against close intent arriving from another
        # thread/loop. Same-loop close cannot run between these statements.
        with self._close_intent_lock:
            if self._close_requested:
                close_reason = self._close_request_reason or "consumer_detached"
            else:
                close_reason = None
                self._queue = asyncio.Queue(maxsize=self._queue_capacity)
                self._changed = asyncio.Event()
                self._detach = asyncio.Event()
                self._state = TaskEventSubscriptionState.ACTIVE
                assert self._owner_loop is not None
                self._worker = self._owner_loop.create_task(
                    self._poll_loop(),
                    name=f"live-voice-task-events:{self._task_id}",
                )
        if close_reason is not None:
            self._state = TaskEventSubscriptionState.CLOSED
            self._close_reason = close_reason
            return False
        return True

    async def next_event(self) -> PersistentTaskEvent:
        """Return the next validated event; consumer cancellation detaches the feed."""

        async with self._consumer_lock:
            self._require_owner_loop()
            try:
                while True:
                    if self._failure is not None:
                        raise self._failure
                    queue = self._queue
                    if queue is not None and not queue.empty():
                        try:
                            self._authorize_current_read()
                        except FormalTaskViolation as error:
                            self._fail(error)
                            raise
                        event = queue.get_nowait()
                        if event.event_type == "task.terminal":
                            self._terminal_event_delivered = True
                            self._state = TaskEventSubscriptionState.CLOSED
                            self._close_reason = "terminal_event_delivered"
                            self._signal_changed()
                        else:
                            self._start_authority_tail_if_ready()
                        return event
                    if self._state in {
                        TaskEventSubscriptionState.DISABLED,
                        TaskEventSubscriptionState.CLOSED,
                    }:
                        raise StopAsyncIteration
                    changed = self._changed
                    if changed is None:
                        raise _violation(
                            "TASK_EVENT_SUBSCRIPTION_NOT_STARTED",
                            "task event subscription has not started",
                            ErrorCode.CONFLICT,
                        )
                    changed.clear()
                    # Recheck after clearing to avoid losing a producer signal.
                    if self._failure is not None or (self._queue is not None and not self._queue.empty()):
                        continue
                    if self._state in {
                        TaskEventSubscriptionState.DISABLED,
                        TaskEventSubscriptionState.CLOSED,
                    }:
                        continue
                    await changed.wait()
            except asyncio.CancelledError:
                self._remember_close_intent("consumer_cancelled")
                self._request_detach(self._close_intent_reason())
                raise

    def __aiter__(self) -> TaskEventSubscription:
        return self

    async def __anext__(self) -> PersistentTaskEvent:
        return await self.next_event()

    async def close(self) -> None:
        """Detach without cancelling business work; caller cancellation is honest."""

        if self._state in {
            TaskEventSubscriptionState.DISABLED,
            TaskEventSubscriptionState.CLOSED,
        }:
            return
        # This is deliberately before the first cancellable await.  If this
        # caller times out waiting for start's lifecycle lock, start still sees
        # and honors the durable reader-close intent.
        self._remember_close_intent("consumer_detached")
        current_loop = asyncio.get_running_loop()
        owner_loop = self._owner_loop
        if owner_loop is not None and current_loop is not owner_loop:
            if not owner_loop.is_closed():
                owner_loop.call_soon_threadsafe(self._apply_close_intent_on_owner)
            raise _violation(
                "TASK_EVENT_SUBSCRIPTION_LOOP_MISMATCH",
                "task event subscription belongs to another event loop",
                ErrorCode.CONFLICT,
            )

        async with self._lifecycle_lock:
            if self._state in {
                TaskEventSubscriptionState.DISABLED,
                TaskEventSubscriptionState.CLOSED,
            }:
                return
            if self._state is TaskEventSubscriptionState.FAILED:
                worker = self._worker
            elif self._state is TaskEventSubscriptionState.NEW:
                self._state = TaskEventSubscriptionState.CLOSED
                self._close_reason = "detached_before_start"
                return
            else:
                self._require_owner_loop()
                self._request_detach(self._close_intent_reason())
                worker = self._worker
        if worker is not None:
            # A timeout/cancel of this caller does not cancel the retained reader.
            await asyncio.shield(worker)

    def snapshot(self) -> TaskEventSubscriptionSnapshot:
        queue = self._queue
        worker = self._worker
        failure = self._failure
        return TaskEventSubscriptionSnapshot(
            enabled=self._enabled,
            state=self._state,
            task_id=self._task_id,
            start_head_seq=self._start_head_seq,
            last_seq=self._last_seq,
            queue_capacity=self._queue_capacity,
            queue_allocated=queue is not None,
            queued_events=0 if queue is None else queue.qsize(),
            validation_capacity=self._validation_capacity,
            tracked_events=len(self._seen_event_ids),
            worker_pending=worker is not None and not worker.done(),
            source_reads=self._source_reads,
            live_only=not self._authority_atomic_replay,
            cursor_replay_supported=self._authority_atomic_replay,
            terminal_event_seen=self._terminal_event_seen,
            terminal_event_delivered=self._terminal_event_delivered,
            close_reason=self._close_reason,
            failure_reason=None if failure is None else failure.reason,
            failure_code=None if failure is None else failure.code,
            discarded_events=self._discarded_events,
            segment_start_seq=self._segment_start_seq,
            attempt_id=self._attempt_id,
            attempt_number=self._attempt_number,
        )

    def _validate_start_snapshot(self, task: PersistentTaskRecord) -> None:
        if not isinstance(task, PersistentTaskRecord):
            raise _violation(
                "TASK_EVENT_SOURCE_PROTOCOL_VIOLATION",
                "TaskEvent source returned an invalid task snapshot",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if task.task_id != self._task_id or not self._scope_matches(task.scope):
            raise _violation(
                "TASK_EVENT_SUBSCRIPTION_SCOPE_MISMATCH",
                "TaskEvent source returned a foreign task snapshot",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if type(task.event_head) is not int or task.event_head < 0:
            raise _violation(
                "TASK_EVENT_SOURCE_PROTOCOL_VIOLATION",
                "TaskEvent source returned an invalid event head",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if (
            not isinstance(task.spec, FormalTaskSpec)
            or not isinstance(task.state, FormalTaskState)
            or type(task.attempt_id) is not str
            or not task.attempt_id.strip()
            or type(task.correlation_id) is not str
            or not task.correlation_id.strip()
            or (task.state is FormalTaskState.TERMINAL) != (task.outcome is not None)
            or (task.outcome is not None and not isinstance(task.outcome, TerminalOutcome))
        ):
            raise _violation(
                "TASK_EVENT_SOURCE_PROTOCOL_VIOLATION",
                "TaskEvent source returned an invalid lifecycle snapshot",
                ErrorCode.PROTOCOL_VIOLATION,
            )

    def _validate_attempt_snapshot(
        self,
        task: PersistentTaskRecord,
        attempt: PersistentAttemptRecord,
    ) -> FormalAttemptState:
        if not isinstance(attempt, PersistentAttemptRecord):
            raise _violation(
                "TASK_EVENT_SOURCE_PROTOCOL_VIOLATION",
                "TaskEvent source returned an invalid attempt snapshot",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if (
            attempt.attempt_id != task.attempt_id
            or attempt.task_id != task.task_id
            or attempt.executor_id != task.spec.executor_id
        ):
            raise _violation(
                "TASK_EVENT_ATTEMPT_BASELINE_MISMATCH",
                "attempt baseline does not bind the exact formal task attempt",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if (
            not isinstance(attempt.state, FormalAttemptState)
            or (
                attempt.executor_ref is not None
                and (type(attempt.executor_ref) is not str or not attempt.executor_ref.strip())
            )
            or type(attempt.source_seq) is not int
            or attempt.source_seq < -1
            or (attempt.state is FormalAttemptState.TERMINAL) != (attempt.outcome is not None)
            or (attempt.outcome is not None and not isinstance(attempt.outcome, TerminalOutcome))
            or (
                attempt.source_seq == -1
                and attempt.state not in {FormalAttemptState.ACCEPTED, FormalAttemptState.TERMINAL}
            )
            or (attempt.source_seq >= 0 and attempt.executor_ref is None)
            or (attempt.source_seq >= 0 and task.event_head <= attempt.source_seq)
        ):
            raise _violation(
                "TASK_EVENT_SOURCE_PROTOCOL_VIOLATION",
                "TaskEvent source returned an invalid attempt lifecycle baseline",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        expected_attempt_state = {
            FormalTaskState.ACCEPTED: FormalAttemptState.ACCEPTED,
            FormalTaskState.RUNNING: FormalAttemptState.RUNNING,
            FormalTaskState.BLOCKED: FormalAttemptState.RUNNING,
            FormalTaskState.DECISION_REQUIRED: FormalAttemptState.RUNNING,
            FormalTaskState.TERMINAL: FormalAttemptState.TERMINAL,
        }[task.state]
        if attempt.state is not expected_attempt_state or (
            task.state is FormalTaskState.TERMINAL and attempt.outcome is not task.outcome
        ):
            raise _violation(
                "TASK_EVENT_ATTEMPT_BASELINE_CONFLICT",
                "task and attempt lifecycle baselines disagree",
                ErrorCode.PROTOCOL_VIOLATION,
            )

        # source_seq=-1 means no Executor lifecycle observation was consumed. It
        # does not erase the authoritative current state: Store permits distinct
        # ACCEPTED source events while still ACCEPTED, or a direct transition to
        # RUNNING; TERMINAL also represents Task-Core-owned pre-dispatch truth.
        return attempt.state

    def _require_owner_loop(self) -> None:
        if self._owner_loop is None:
            if self._state in {
                TaskEventSubscriptionState.DISABLED,
                TaskEventSubscriptionState.CLOSED,
            }:
                return
            raise _violation(
                "TASK_EVENT_SUBSCRIPTION_NOT_STARTED",
                "task event subscription has not started",
                ErrorCode.CONFLICT,
            )
        if asyncio.get_running_loop() is not self._owner_loop:
            raise _violation(
                "TASK_EVENT_SUBSCRIPTION_LOOP_MISMATCH",
                "task event subscription belongs to another event loop",
                ErrorCode.CONFLICT,
            )

    def _authorize_current_read(self) -> None:
        if self._authorization is None:
            raise _violation(
                "FORMAL_TASK_AUTHORIZATION_REQUIRED",
                "formal task event subscription requires trusted authorization",
                ErrorCode.UNAUTHENTICATED,
            )
        self._authorization.authorize(
            scope=self._scope,
            operation="task.events",
            command_id=None,
            target_task_id=self._task_id,
            required_capabilities=_EVENTS_CAPABILITY,
            destructive=False,
            now=self._clock(),
        )

    def _scope_matches(self, stored: ScopeRef) -> bool:
        if not self._consumer_scope:
            return stored == self._scope
        return (
            isinstance(stored, ScopeRef)
            and stored.assurance is Assurance.AUTHENTICATED
            and self._scope.assurance is Assurance.AUTHENTICATED
            and (stored.subject_id, stored.project_id) == (self._scope.subject_id, self._scope.project_id)
        )

    async def _poll_loop(self) -> None:
        detach = self._detach
        assert detach is not None
        try:
            while not detach.is_set():
                if self._close_was_requested():
                    self._request_detach(self._close_intent_reason())
                    break
                assert self._last_seq is not None
                try:
                    # A long-lived feed never extends a grant past its expiry.
                    # Reauthorize before each object-content read.
                    self._authorize_current_read()
                    batch = await asyncio.to_thread(
                        self._source.events,
                        self._task_id,
                        self._scope,
                        after_seq=self._last_seq,
                        attempt_id=self._attempt_id,
                    )
                    self._source_reads += 1
                except asyncio.CancelledError:
                    self._fail(
                        _violation(
                            "TASK_EVENT_SUBSCRIPTION_WORKER_CANCELLED",
                            "TaskEvent subscription worker was cancelled",
                            ErrorCode.CANCELLED,
                        )
                    )
                    raise
                except FormalTaskViolation as error:
                    self._fail(error)
                    return
                except Exception:
                    self._fail(
                        _violation(
                            "TASK_EVENT_SOURCE_FAILURE",
                            "formal TaskEvent source read failed",
                            ErrorCode.UNAVAILABLE,
                        )
                    )
                    return
                if self._close_was_requested():
                    self._request_detach(self._close_intent_reason())
                    break
                if detach.is_set():
                    break
                try:
                    # A read authorized when it began cannot disclose content
                    # after the exact grant has expired while it was blocked.
                    self._authorize_current_read()
                except FormalTaskViolation as error:
                    self._fail(error)
                    return
                if type(batch) is not tuple or any(not isinstance(event, PersistentTaskEvent) for event in batch):
                    self._fail(
                        _violation(
                            "TASK_EVENT_SOURCE_PROTOCOL_VIOLATION",
                            "TaskEvent source returned an invalid event batch",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    )
                    return
                advanced = False
                if batch:
                    previous_last_seq = self._last_seq
                    if not self._accept_batch(batch):
                        return
                    advanced = self._last_seq != previous_last_seq
                if self._state is TaskEventSubscriptionState.TERMINAL_PENDING:
                    return
                if advanced:
                    continue
                try:
                    await asyncio.wait_for(detach.wait(), timeout=self._poll_interval)
                except TimeoutError:
                    pass
        finally:
            if self._state is TaskEventSubscriptionState.DETACHING:
                self._state = TaskEventSubscriptionState.CLOSED
                self._signal_changed()

    def _accept_batch(self, batch: tuple[PersistentTaskEvent, ...]) -> bool:
        assert self._last_seq is not None
        assert self._attempt_id is not None
        assert self._attempt_executor_id is not None
        assert self._correlation_id is not None
        assert self._task_state is not None
        assert self._attempt_state is not None
        queue = self._queue
        assert queue is not None

        ids = dict(self._seen_event_ids)
        sequences = dict(self._seen_sequences)
        last_seq = self._last_seq
        task_state = self._task_state
        task_outcome = self._task_outcome
        attempt_state = self._attempt_state
        attempt_outcome = self._attempt_outcome
        terminal_seen = self._terminal_event_seen
        expected_task_type: str | None = None
        expected_task_producers = frozenset[str]()
        expected_task_source: str | None = None
        expected_task_cause: str | None = None
        expected_task_outcome: str | None = None
        accepted: list[tuple[PersistentTaskEvent, bytes]] = []
        try:
            for event in batch:
                canonical = canonical_json_bytes(event.to_dict())
                known_id = ids.get(event.event_id)
                if known_id is not None:
                    if known_id != canonical:
                        raise _violation(
                            "TASK_EVENT_ID_CONFLICT",
                            "TaskEvent id was reused with different canonical bytes",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    continue
                known_sequence = sequences.get(event.seq)
                if known_sequence is not None:
                    raise _violation(
                        "TASK_EVENT_SEQUENCE_CONFLICT",
                        "TaskEvent sequence was reused with different content",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
                expected = last_seq + 1
                if event.seq > expected:
                    raise _violation(
                        "TASK_EVENT_SEQUENCE_GAP",
                        "TaskEvent live feed contains a sequence gap",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
                if event.seq < expected:
                    raise _violation(
                        "TASK_EVENT_SEQUENCE_REORDERED",
                        "TaskEvent live feed moved behind its accepted sequence",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
                if len(ids) >= self._validation_capacity:
                    raise _violation(
                        "TASK_EVENT_SUBSCRIPTION_VALIDATION_LIMIT",
                        "TaskEvent subscription reached its declared validation limit",
                        ErrorCode.UNAVAILABLE,
                    )
                if event.task_id != self._task_id or not self._scope_matches(event.scope):
                    raise _violation(
                        "TASK_EVENT_SCOPE_MISMATCH",
                        "TaskEvent does not belong to the subscribed task scope",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
                if event.attempt_id != self._attempt_id:
                    stale = event.attempt_id == self._previous_attempt_id
                    raise _violation(
                        ("TASK_EVENT_ATTEMPT_STALE" if stale else "TASK_EVENT_ATTEMPT_MISMATCH"),
                        (
                            "TaskEvent belongs to the predecessor attempt epoch"
                            if stale
                            else "TaskEvent does not belong to the subscribed task attempt"
                        ),
                        ErrorCode.STALE if stale else ErrorCode.PROTOCOL_VIOLATION,
                    )
                if event.correlation_id != self._correlation_id:
                    raise _violation(
                        "TASK_EVENT_CORRELATION_MISMATCH",
                        "TaskEvent does not belong to the subscribed correlation",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
                if terminal_seen:
                    raise _violation(
                        "TASK_EVENT_AFTER_TERMINAL",
                        "TaskEvent appeared after canonical task termination",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
                if event.event_type not in _CANONICAL_EVENT_TYPES:
                    raise _violation(
                        "TASK_EVENT_TYPE_UNKNOWN",
                        "TaskEvent type is not emitted by the formal Task Store",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
                if expected_task_type is not None and event.event_type != expected_task_type:
                    raise _violation(
                        "TASK_EVENT_ATTEMPT_TASK_COUPLING_MISSING",
                        "attempt lifecycle event lacks its consecutive Task-Core event",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
                lifecycle_state = _TASK_LIFECYCLE_EVENT_STATES.get(event.event_type)
                if lifecycle_state is not None:
                    allowed_producers = _TASK_EVENT_PRODUCERS[event.event_type]
                    if event.producer not in allowed_producers:
                        raise _violation(
                            "TASK_EVENT_PRODUCER_MISMATCH",
                            "task lifecycle event has no canonical Task-Core authority",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    if event.state != lifecycle_state.value:
                        raise _violation(
                            "TASK_EVENT_LIFECYCLE_MISMATCH",
                            "TaskEvent type and lifecycle state disagree",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    try:
                        validate_transition(
                            LifecycleKind.TASK,
                            task_state.value,
                            lifecycle_state.value,
                            outcome=event.outcome,
                        )
                    except ContractViolation as error:
                        raise _violation(
                            "TASK_EVENT_LIFECYCLE_CONFLICT",
                            "TaskEvent contains a backward or repeated task transition",
                            ErrorCode.PROTOCOL_VIOLATION,
                        ) from error
                    coupled_to_attempt = expected_task_type is not None
                    if coupled_to_attempt and (
                        event.producer not in expected_task_producers
                        or event.source_event_id != expected_task_source
                        or event.causation_id != expected_task_cause
                        or event.outcome != expected_task_outcome
                    ):
                        raise _violation(
                            "TASK_EVENT_ATTEMPT_TASK_COUPLING_MISMATCH",
                            "Task-Core lifecycle event disagrees with its attempt cause",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    if (
                        lifecycle_state
                        in {
                            FormalTaskState.RUNNING,
                            FormalTaskState.BLOCKED,
                            FormalTaskState.DECISION_REQUIRED,
                        }
                        and attempt_state is not FormalAttemptState.RUNNING
                    ):
                        raise _violation(
                            "TASK_EVENT_ATTEMPT_STATE_CONFLICT",
                            "nonterminal task progress requires a running attempt",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    if lifecycle_state is FormalTaskState.TERMINAL:
                        if attempt_state is not FormalAttemptState.TERMINAL or event.outcome != attempt_outcome:
                            raise _violation(
                                "TASK_EVENT_ATTEMPT_OUTCOME_CONFLICT",
                                "task terminal outcome disagrees with its terminal attempt",
                                ErrorCode.PROTOCOL_VIOLATION,
                            )
                        if not coupled_to_attempt:
                            raise _violation(
                                "TASK_EVENT_ATTEMPT_TASK_COUPLING_MISSING",
                                "task.terminal requires its consecutive attempt.terminal event",
                                ErrorCode.PROTOCOL_VIOLATION,
                            )
                    task_state = lifecycle_state
                    task_outcome = event.outcome
                    terminal_seen = lifecycle_state is FormalTaskState.TERMINAL
                    expected_task_type = None
                    expected_task_producers = frozenset()
                    expected_task_source = None
                    expected_task_cause = None
                    expected_task_outcome = None
                elif event.event_type in {
                    "task.cancel_requested",
                    "task.adjust_requested",
                    "task.adjust_applied",
                    "task.adjust_rejected",
                    "task.update_requested",
                    "task.update_applied",
                    "task.reprioritize_requested",
                    "task.reprioritize_applied",
                }:
                    if event.producer not in _TASK_EVENT_PRODUCERS[event.event_type]:
                        raise _violation(
                            "TASK_EVENT_PRODUCER_MISMATCH",
                            "task control event has no canonical Task-Core authority",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    if event.source_event_id is not None:
                        raise _violation(
                            "TASK_EVENT_SOURCE_EVIDENCE_MISMATCH",
                            "task control event cannot claim Executor source evidence",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    if event.state != task_state.value or event.outcome != task_outcome:
                        raise _violation(
                            "TASK_EVENT_CONTROL_STATE_CONFLICT",
                            "Task control event disagrees with the canonical task state",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    if event.event_type.startswith("task.adjust_"):
                        command_id = event.details.get("command_id")
                        expected_details = (
                            {"command_id", "reason"} if event.event_type == "task.adjust_rejected" else {"command_id"}
                        )
                        reason = event.details.get("reason")
                        if (
                            set(event.details) != expected_details
                            or type(command_id) is not str
                            or not command_id.strip()
                            or command_id != event.causation_id
                            or (
                                event.event_type == "task.adjust_rejected"
                                and (
                                    type(reason) is not str
                                    or not reason
                                    or len(reason) > 128
                                    or any(
                                        character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for character in reason
                                    )
                                )
                            )
                        ):
                            raise _violation(
                                "TASK_EVENT_ADJUSTMENT_EVIDENCE_MISMATCH",
                                "task adjustment event has invalid authority evidence",
                                ErrorCode.PROTOCOL_VIOLATION,
                            )
                    elif event.event_type.startswith("task.update_"):
                        if (
                            set(event.details) != {"command_id"}
                            or event.details.get("command_id") != event.causation_id
                        ):
                            raise _violation(
                                "TASK_EVENT_UPDATE_EVIDENCE_MISMATCH",
                                "task update event has invalid authority evidence",
                                ErrorCode.PROTOCOL_VIOLATION,
                            )
                    elif event.event_type.startswith("task.reprioritize_"):
                        if (
                            set(event.details) != {"command_id", "priority"}
                            or event.details.get("command_id") != event.causation_id
                            or event.details.get("priority") not in {item.value for item in AdmissionPriority}
                        ):
                            raise _violation(
                                "TASK_EVENT_REPRIORITIZE_EVIDENCE_MISMATCH",
                                "task reprioritize event has invalid authority evidence",
                                ErrorCode.PROTOCOL_VIOLATION,
                            )
                attempt_lifecycle_state = _ATTEMPT_LIFECYCLE_EVENT_STATES.get(event.event_type)
                if attempt_lifecycle_state is not None:
                    if event.state != attempt_lifecycle_state.value:
                        raise _violation(
                            "TASK_EVENT_ATTEMPT_LIFECYCLE_MISMATCH",
                            "TaskEvent attempt type and lifecycle state disagree",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    if (
                        attempt_lifecycle_state
                        in {
                            FormalAttemptState.ACCEPTED,
                            FormalAttemptState.RUNNING,
                        }
                        and event.producer != self._attempt_executor_id
                    ):
                        raise _violation(
                            "TASK_EVENT_ATTEMPT_PRODUCER_MISMATCH",
                            "attempt lifecycle event does not belong to its Executor",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    producer_is_executor = event.producer == self._attempt_executor_id
                    if producer_is_executor:
                        if event.source_event_id is None or event.causation_id != event.source_event_id:
                            raise _violation(
                                "TASK_EVENT_ATTEMPT_SOURCE_EVIDENCE_MISMATCH",
                                "Executor attempt event requires exact source and causation",
                                ErrorCode.PROTOCOL_VIOLATION,
                            )
                    elif event.source_event_id is not None:
                        raise _violation(
                            "TASK_EVENT_ATTEMPT_SOURCE_EVIDENCE_MISMATCH",
                            "internal attempt terminal event cannot claim Executor source",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    if attempt_lifecycle_state is FormalAttemptState.TERMINAL:
                        producer_is_internal = event.producer in _INTERNAL_ATTEMPT_TERMINAL_PRODUCERS
                        if not producer_is_executor and not producer_is_internal:
                            raise _violation(
                                "TASK_EVENT_ATTEMPT_PRODUCER_MISMATCH",
                                "attempt terminal event has no canonical producer authority",
                                ErrorCode.PROTOCOL_VIOLATION,
                            )
                        if attempt_state is FormalAttemptState.ACCEPTED and producer_is_executor:
                            raise _violation(
                                "TASK_EVENT_ATTEMPT_LIFECYCLE_CONFLICT",
                                "Executor attempt cannot skip running before termination",
                                ErrorCode.PROTOCOL_VIOLATION,
                            )
                    if attempt_lifecycle_state not in _ATTEMPT_TRANSITIONS[attempt_state]:
                        raise _violation(
                            "TASK_EVENT_ATTEMPT_LIFECYCLE_CONFLICT",
                            "TaskEvent contains a missing, repeated, or backward attempt transition",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    attempt_state = attempt_lifecycle_state
                    attempt_outcome = event.outcome
                    if attempt_lifecycle_state is FormalAttemptState.RUNNING:
                        expected_task_type = "task.running"
                        expected_task_producers = frozenset({"task_core"})
                        expected_task_source = event.source_event_id
                        expected_task_cause = event.causation_id
                        expected_task_outcome = None
                    elif attempt_lifecycle_state is FormalAttemptState.TERMINAL:
                        expected_task_type = "task.terminal"
                        if producer_is_executor:
                            expected_task_producers = frozenset({"task_core"})
                        elif event.producer == "task_core.delivery":
                            expected_task_producers = frozenset({"task_core.delivery"})
                        else:
                            expected_task_producers = frozenset({"task_core", "task_core.reconciliation"})
                        expected_task_source = event.source_event_id
                        expected_task_cause = event.causation_id
                        expected_task_outcome = event.outcome
                ids[event.event_id] = canonical
                sequences[event.seq] = canonical
                last_seq = event.seq
                accepted.append((event, canonical))
            if expected_task_type is not None:
                raise _violation(
                    "TASK_EVENT_ATTEMPT_TASK_COUPLING_MISSING",
                    "attempt lifecycle event lacks its Task-Core lifecycle event",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
        except FormalTaskViolation as error:
            self._fail(error)
            return False
        except Exception:
            self._fail(
                _violation(
                    "TASK_EVENT_SOURCE_PROTOCOL_VIOLATION",
                    "TaskEvent batch cannot be canonically validated",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
            )
            return False

        if len(accepted) > queue.maxsize - queue.qsize():
            self._fail(
                _violation(
                    "TASK_EVENT_SUBSCRIPTION_OVERFLOW",
                    "TaskEvent subscription queue is full; no event was silently dropped",
                    ErrorCode.UNAVAILABLE,
                )
            )
            return False
        if self._state is not TaskEventSubscriptionState.ACTIVE:
            return False
        self._seen_event_ids = ids
        self._seen_sequences = sequences
        self._last_seq = last_seq
        self._task_state = task_state
        self._task_outcome = task_outcome
        self._attempt_state = attempt_state
        self._attempt_outcome = attempt_outcome
        self._terminal_event_seen = terminal_seen
        for event, _canonical in accepted:
            queue.put_nowait(event)
        if terminal_seen:
            self._state = TaskEventSubscriptionState.TERMINAL_PENDING
        self._signal_changed()
        return True

    def _request_detach(self, reason: str) -> None:
        if self._state not in {
            TaskEventSubscriptionState.ACTIVE,
            TaskEventSubscriptionState.TERMINAL_PENDING,
            TaskEventSubscriptionState.DETACHING,
        }:
            return
        already_detaching = self._state is TaskEventSubscriptionState.DETACHING
        self._state = TaskEventSubscriptionState.DETACHING
        if not already_detaching:
            self._close_reason = reason
        self._discard_queued_events()
        if self._detach is not None:
            self._detach.set()
        if self._worker is None or self._worker.done():
            self._state = TaskEventSubscriptionState.CLOSED
        self._signal_changed()

    def _remember_close_intent(self, reason: str) -> None:
        with self._close_intent_lock:
            if not self._close_requested:
                self._close_requested = True
                self._close_request_reason = reason

    def _close_was_requested(self) -> bool:
        with self._close_intent_lock:
            return self._close_requested

    def _close_intent_reason(self) -> str:
        with self._close_intent_lock:
            return self._close_request_reason or "consumer_detached"

    def _settle_start_close_intent(self) -> bool:
        with self._close_intent_lock:
            if not self._close_requested:
                return False
            reason = self._close_request_reason or "consumer_detached"
        self._state = TaskEventSubscriptionState.CLOSED
        self._close_reason = reason
        self._signal_changed()
        return True

    def _cleanup_preallocation_start_failure(self, owner_loop: asyncio.AbstractEventLoop) -> None:
        if self._state is not TaskEventSubscriptionState.NEW or self._queue is not None or self._worker is not None:
            return
        if self._close_was_requested():
            self._settle_start_close_intent()
        elif self._owner_loop is owner_loop:
            self._owner_loop = None

    def _apply_close_intent_on_owner(self) -> None:
        if self._state is TaskEventSubscriptionState.NEW:
            # start() owns the lifecycle lock and will settle immediately after
            # its current authority read; no resource exists to detach yet.
            return
        self._request_detach(self._close_intent_reason())

    def _fail(self, error: FormalTaskViolation) -> None:
        if self._state in {
            TaskEventSubscriptionState.CLOSED,
            TaskEventSubscriptionState.FAILED,
        }:
            return
        self._failure = error
        self._state = TaskEventSubscriptionState.FAILED
        self._close_reason = "failed_closed"
        self._discard_queued_events()
        if self._detach is not None:
            self._detach.set()
        self._signal_changed()

    def _discard_queued_events(self) -> None:
        queue = self._queue
        if queue is None:
            return
        while not queue.empty():
            queue.get_nowait()
            self._discarded_events += 1

    def _signal_changed(self) -> None:
        if self._changed is not None:
            self._changed.set()


__all__ = [
    "TaskEventAuthoritySource",
    "TaskEventSource",
    "TaskEventSubscription",
    "TaskEventSubscriptionSnapshot",
    "TaskEventSubscriptionState",
]
