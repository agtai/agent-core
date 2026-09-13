# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from __future__ import annotations

import asyncio
import threading
from collections import deque
from dataclasses import replace
from pathlib import Path

import pytest

from openjiuwen.core.application.tasks.contracts import (
    CONTRACT_VERSION,
    Assurance,
    CommandEnvelope,
    ErrorCode,
    OriginRef,
    ScopeRef,
    TerminalOutcome,
)
from openjiuwen.core.application.tasks.formal_task_models import (
    ExecutorObservation,
    ExecutorResolution,
    FormalAttemptState,
    FormalTaskSpec,
    FormalTaskState,
    FormalTaskViolation,
    PersistentAttemptRecord,
    PersistentTaskEvent,
    PersistentTaskRecord,
    ResolvedTaskContext,
    TaskAuthorizationGrant,
    TaskRetryAuthoritySnapshot,
    TaskRetryProductRequestFingerprint,
)
from openjiuwen.core.application.tasks.task_event_subscription import (
    TaskEventSubscription,
    TaskEventSubscriptionState,
)
from openjiuwen.core.application.tasks.task_store import SqliteTaskStore

NOW = "2026-08-06T10:00:00Z"
EXPIRY = "2026-08-06T11:00:00Z"


def _scope(subject: str = "user-1") -> ScopeRef:
    return ScopeRef(subject, "project-1", "session-1", Assurance.AUTHENTICATED)


def _grant(
    task_id: str,
    *,
    scope: ScopeRef | None = None,
    operation: str = "task.events",
    capabilities: frozenset[str] = frozenset({"task.events"}),
    expires_at: str = EXPIRY,
) -> TaskAuthorizationGrant:
    granted_scope = scope or _scope()
    return TaskAuthorizationGrant(
        principal_id=granted_scope.subject_id,
        scope=granted_scope,
        operation=operation,
        command_id=None,
        target_task_id=task_id,
        allowed_capabilities=capabilities,
        confirmation_id=None,
        confirmed=False,
        expires_at=expires_at,
    )


def _context(tmp_path: Path, scope: ScopeRef | None = None) -> ResolvedTaskContext:
    current_scope = scope or _scope()
    return ResolvedTaskContext(
        source="test.project_registry",
        stable_id=current_scope.project_id or "project-1",
        uri=tmp_path.resolve().as_uri(),
        revision_kind="version",
        revision_value="revision-1",
        scope=current_scope,
        permissions=("task.execute", "project.write"),
        expires_at=EXPIRY,
        redaction_policy_id="test-redaction-v1",
    )


def _spec(tmp_path: Path, *, suffix: str = "") -> FormalTaskSpec:
    return FormalTaskSpec(
        name=f"task{suffix}",
        instruction="Perform one bounded test task.",
        origin=OriginRef("structured", None, None),
        context=_context(tmp_path),
        executor_id="executor-1",
        required_capabilities=("task.create",),
        side_effect_class="project_mutation",
        attributes=(),
    )


def _create_task(store: SqliteTaskStore, tmp_path: Path, *, suffix: str = "") -> PersistentTaskRecord:
    command_id = f"command-{suffix or 'one'}"
    observed_at = "2026-08-06T10:00:01Z" if suffix == "two" else NOW
    command = CommandEnvelope.from_dict(
        {
            "contract_version": CONTRACT_VERSION,
            "request_id": f"request-{suffix or 'one'}",
            "command_id": command_id,
            "command_type": "task.create",
            "issued_at": observed_at,
            "scope": _scope().to_dict(),
            "correlation_id": f"correlation-{suffix or 'one'}",
            "causation_id": None,
            "origin": {
                "kind": "structured",
                "turn_id": None,
                "commit_id": None,
            },
            "target_ref": {"kind": "task", "id": f"create:{command_id}"},
            "context_refs": [],
            "required_capabilities": ["task.create"],
            "payload": {},
            "extensions": {},
        }
    )
    result = store.create(command, _spec(tmp_path, suffix=suffix), observed_at=observed_at)
    assert result.ok and result.result is not None
    return store.get_task(str(result.result["task_id"]), _scope())


def _observation(
    task: PersistentTaskRecord,
    *,
    source_seq: int,
    state: FormalAttemptState,
    outcome: TerminalOutcome | None = None,
) -> ExecutorObservation:
    return ExecutorObservation(
        resolution=ExecutorResolution.KNOWN,
        executor_id=task.spec.executor_id,
        executor_ref=f"executor-ref:{task.attempt_id}",
        task_id=task.task_id,
        attempt_id=task.attempt_id,
        source_event_id=f"source:{task.attempt_id}:{source_seq}",
        source_seq=source_seq,
        attempt_state=state,
        attempt_outcome=outcome,
        occurred_at=NOW,
        raw_status=state.value,
    )


def _advance_running(store: SqliteTaskStore, task: PersistentTaskRecord) -> None:
    item = store.claim_outbox(f"worker:{task.task_id}")
    assert item is not None and item.task_id == task.task_id
    store.complete_outbox(
        item,
        executor_ref=f"executor-ref:{task.attempt_id}",
        observations=(
            _observation(task, source_seq=0, state=FormalAttemptState.ACCEPTED),
            _observation(task, source_seq=1, state=FormalAttemptState.RUNNING),
        ),
    )


def _advance_running_direct(store: SqliteTaskStore, task: PersistentTaskRecord) -> None:
    item = store.claim_outbox(f"worker-direct:{task.task_id}")
    assert item is not None and item.task_id == task.task_id
    store.complete_outbox(
        item,
        executor_ref=f"executor-ref:{task.attempt_id}",
        observations=(_observation(task, source_seq=0, state=FormalAttemptState.RUNNING),),
    )


def _advance_terminal(
    store: SqliteTaskStore,
    task: PersistentTaskRecord,
    *,
    outcome: TerminalOutcome = TerminalOutcome.COMPLETED,
) -> None:
    store.apply_observations(
        (
            _observation(
                task,
                source_seq=2,
                state=FormalAttemptState.TERMINAL,
                outcome=outcome,
            ),
        )
    )


def _retry_task(
    store: SqliteTaskStore,
    task: PersistentTaskRecord,
    *,
    attempt_number: int,
) -> PersistentTaskRecord:
    assert task.outcome is not None
    command_id = f"command-retry-{attempt_number}"
    command = CommandEnvelope.from_dict(
        {
            "contract_version": CONTRACT_VERSION,
            "request_id": f"request-retry-{attempt_number}",
            "command_id": command_id,
            "command_type": "task.retry",
            "issued_at": NOW,
            "scope": task.scope.to_dict(),
            "correlation_id": task.correlation_id,
            "causation_id": None,
            "origin": {"kind": "structured", "turn_id": None, "commit_id": None},
            "target_ref": {"kind": "task", "id": task.task_id},
            "context_refs": [],
            "required_capabilities": ["task.retry"],
            "payload": {
                "previous_attempt_id": task.attempt_id,
                "previous_outcome": task.outcome.value,
                "attempt_number": attempt_number,
            },
            "extensions": TaskRetryProductRequestFingerprint("a" * 64).to_extensions(),
        }
    )
    spec = replace(
        task.spec,
        context=replace(
            task.spec.context,
            revision_value=f"revision-{attempt_number}",
        ),
    )
    authority = store.read_retry_authority(command)
    assert isinstance(authority, TaskRetryAuthoritySnapshot)
    result = store.retry(command, spec, authority, observed_at=NOW)
    assert result.ok
    return store.get_task(task.task_id, task.scope)


def _event(
    *,
    seq: int,
    event_id: str | None = None,
    task_id: str = "task-1",
    attempt_id: str = "attempt-1",
    scope: ScopeRef | None = None,
    event_type: str = "task.running",
    state: str = "running",
    outcome: str | None = None,
    correlation_id: str = "correlation-1",
    producer: str | None = None,
    source_event_id: str | None = None,
    causation_id: str | None = None,
    details: dict[str, object] | None = None,
) -> PersistentTaskEvent:
    resolved_producer = (
        producer if producer is not None else ("executor-1" if event_type.startswith("attempt.") else "task_core")
    )
    resolved_source = source_event_id
    if resolved_source is None and resolved_producer == "executor-1":
        resolved_source = f"source-{seq}"
    return PersistentTaskEvent(
        event_id=event_id or f"event-{seq}",
        task_id=task_id,
        attempt_id=attempt_id,
        scope=scope or _scope(),
        seq=seq,
        event_type=event_type,
        state=state,
        outcome=outcome,
        producer=resolved_producer,
        source_event_id=resolved_source,
        causation_id=causation_id or resolved_source or f"cause-{seq}",
        correlation_id=correlation_id,
        occurred_at=NOW,
        details=details or {},
    )


def _record(
    tmp_path: Path,
    *,
    task_id: str = "task-1",
    scope: ScopeRef | None = None,
    state: FormalTaskState = FormalTaskState.ACCEPTED,
    outcome: TerminalOutcome | None = None,
    event_head: int = 0,
) -> PersistentTaskRecord:
    current_scope = scope or _scope()
    return PersistentTaskRecord(
        task_id=task_id,
        scope=current_scope,
        spec=_spec(tmp_path),
        state=state,
        attempt_id="attempt-1",
        correlation_id="correlation-1",
        cancel_requested=False,
        dispatch_fenced=False,
        outcome=outcome,
        reconciliation_state=None,
        reconciliation_reason=None,
        create_command_id="command-create-1",
        predecessor_task_id=None,
        revision_number=1,
        event_head=event_head,
    )


def _cancel_task(store: SqliteTaskStore, task: PersistentTaskRecord) -> None:
    command = CommandEnvelope.from_dict(
        {
            "contract_version": CONTRACT_VERSION,
            "request_id": f"request-cancel:{task.task_id}",
            "command_id": f"command-cancel:{task.task_id}",
            "command_type": "task.cancel",
            "issued_at": NOW,
            "scope": task.scope.to_dict(),
            "correlation_id": task.correlation_id,
            "causation_id": None,
            "origin": {
                "kind": "structured",
                "turn_id": None,
                "commit_id": None,
            },
            "target_ref": {"kind": "task", "id": task.task_id},
            "context_refs": [],
            "required_capabilities": ["task.cancel"],
            "payload": {},
            "extensions": {},
        }
    )
    result = store.cancel(command, observed_at=NOW)
    assert result.ok


def _attempt(
    task: PersistentTaskRecord,
    *,
    attempt_id: str | None = None,
    task_id: str | None = None,
    executor_id: str | None = None,
    executor_ref: str | None = None,
    state: FormalAttemptState | None = None,
    outcome: TerminalOutcome | None = None,
    source_seq: int | None = None,
) -> PersistentAttemptRecord:
    if state is None:
        state = (
            FormalAttemptState.TERMINAL
            if task.state is FormalTaskState.TERMINAL
            else (FormalAttemptState.ACCEPTED if task.state is FormalTaskState.ACCEPTED else FormalAttemptState.RUNNING)
        )
    if outcome is None and state is FormalAttemptState.TERMINAL:
        outcome = task.outcome
    if source_seq is None:
        source_seq = -1 if state is FormalAttemptState.ACCEPTED else 0
    if executor_ref is None and source_seq >= 0:
        executor_ref = f"executor-ref:{task.attempt_id}"
    return PersistentAttemptRecord(
        attempt_id=attempt_id or task.attempt_id,
        task_id=task_id or task.task_id,
        executor_id=executor_id or task.spec.executor_id,
        executor_ref=executor_ref,
        state=state,
        outcome=outcome,
        source_seq=source_seq,
    )


class _ScriptedSource:
    def __init__(
        self,
        task: PersistentTaskRecord,
        batches: tuple[tuple[PersistentTaskEvent, ...] | Exception, ...] = (),
        *,
        attempt: PersistentAttemptRecord | None = None,
        task_reads: tuple[PersistentTaskRecord, ...] | None = None,
    ) -> None:
        self.task = task
        self.attempt = attempt or _attempt(task)
        self.task_reads = deque(task_reads or (task,))
        self.batches = deque(batches)
        self.get_calls = 0
        self.attempt_calls = 0
        self.event_calls = 0
        self.mutations = 0
        self._lock = threading.Lock()

    def get_task(self, task_id: str, scope: ScopeRef) -> PersistentTaskRecord:
        with self._lock:
            self.get_calls += 1
            if len(self.task_reads) > 1:
                return self.task_reads.popleft()
            return self.task_reads[0]

    def get_attempt(self, attempt_id: str) -> PersistentAttemptRecord:
        self.attempt_calls += 1
        return self.attempt

    def events(
        self,
        task_id: str,
        scope: ScopeRef,
        *,
        after_seq: int = -1,
        attempt_id: str | None = None,
    ) -> tuple[PersistentTaskEvent, ...]:
        with self._lock:
            self.event_calls += 1
            if not self.batches:
                return ()
            current = self.batches.popleft()
        if isinstance(current, Exception):
            raise current
        return current


class _BlockingSource(_ScriptedSource):
    def __init__(
        self,
        task: PersistentTaskRecord,
        *,
        batch: tuple[PersistentTaskEvent, ...] = (),
    ) -> None:
        super().__init__(task)
        self.batch = batch
        self.read_started = threading.Event()
        self.release_read = threading.Event()

    def events(
        self,
        task_id: str,
        scope: ScopeRef,
        *,
        after_seq: int = -1,
        attempt_id: str | None = None,
    ) -> tuple[PersistentTaskEvent, ...]:
        self.event_calls += 1
        self.read_started.set()
        self.release_read.wait(timeout=2)
        return self.batch


class _BlockingStartSource(_ScriptedSource):
    def __init__(self, task: PersistentTaskRecord, *, block_read: str) -> None:
        super().__init__(task)
        self.block_read = block_read
        self.read_started = threading.Event()
        self.release_read = threading.Event()

    def _block(self) -> None:
        self.read_started.set()
        self.release_read.wait(timeout=2)

    def get_task(self, task_id: str, scope: ScopeRef) -> PersistentTaskRecord:
        self.get_calls += 1
        if (self.block_read == "task_first" and self.get_calls == 1) or (
            self.block_read == "task_confirm" and self.get_calls == 2
        ):
            self._block()
        return self.task

    def get_attempt(self, attempt_id: str) -> PersistentAttemptRecord:
        self.attempt_calls += 1
        if self.block_read == "attempt":
            self._block()
        return self.attempt


async def _wait_until(predicate, *, attempts: int = 300) -> None:
    for _ in range(attempts):
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("condition was not reached")


@pytest.mark.asyncio
async def test_authority_snapshot_replays_prefix_then_concurrent_durable_suffix(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "atomic-prefix.sqlite")
    task = _create_task(store, tmp_path)
    _advance_running(store, task)
    subscription = TaskEventSubscription(
        source=store,
        authorization=_grant(task.task_id),
        scope=task.scope,
        task_id=task.task_id,
        enabled=True,
        authority_atomic_replay=True,
        poll_interval=0.001,
        clock=lambda: NOW,
    )

    assert await subscription.start() is True
    assert subscription.snapshot().start_head_seq == 3
    assert subscription.snapshot().worker_pending is False

    # Append while the authoritative prefix is still queued. The tail reader
    # starts only after that prefix is consumed, so queue pressure can neither
    # drop nor race the durable suffix.
    _advance_terminal(store, task)
    replay = [await asyncio.wait_for(subscription.next_event(), 1) for _ in range(6)]
    assert [event.seq for event in replay] == [0, 1, 2, 3, 4, 5]
    assert [event.event_type for event in replay] == [
        "task.accepted",
        "attempt.accepted",
        "attempt.running",
        "task.running",
        "attempt.terminal",
        "task.terminal",
    ]
    snapshot = subscription.snapshot()
    assert snapshot.start_head_seq == 3
    assert snapshot.last_seq == 5
    assert snapshot.live_only is False
    assert snapshot.cursor_replay_supported is True
    await subscription.close()


@pytest.mark.asyncio
async def test_authority_restart_replays_terminal_prefix_without_worker(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "atomic-terminal.sqlite")
    task = _create_task(store, tmp_path)
    _advance_running(store, task)
    _advance_terminal(store, task)
    terminal = store.get_task(task.task_id, task.scope)
    subscription = TaskEventSubscription(
        source=store,
        authorization=_grant(task.task_id),
        scope=task.scope,
        task_id=task.task_id,
        enabled=True,
        authority_atomic_replay=True,
        queue_capacity=32,
        clock=lambda: NOW,
    )

    assert await subscription.start() is True
    replay = [await subscription.next_event() for _ in range(terminal.event_head + 1)]
    assert [event.seq for event in replay] == list(range(terminal.event_head + 1))
    assert replay[-1].event_type == "task.terminal"
    assert replay[-1].outcome == "completed"
    with pytest.raises(StopAsyncIteration):
        await subscription.next_event()
    snapshot = subscription.snapshot()
    assert snapshot.start_head_seq == terminal.event_head
    assert snapshot.worker_pending is False
    assert snapshot.terminal_event_delivered is True


@pytest.mark.asyncio
async def test_authority_replay_expiry_and_capacity_fail_before_allocation(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "atomic-bounds.sqlite")
    task = _create_task(store, tmp_path)
    _advance_running(store, task)

    expired = TaskEventSubscription(
        source=store,
        authorization=_grant(task.task_id, expires_at="2026-08-06T09:59:59Z"),
        scope=task.scope,
        task_id=task.task_id,
        enabled=True,
        authority_atomic_replay=True,
        clock=lambda: NOW,
    )
    with pytest.raises(FormalTaskViolation):
        await expired.start()
    assert expired.snapshot().source_reads == 0
    assert expired.snapshot().queue_allocated is False

    bounded = TaskEventSubscription(
        source=store,
        authorization=_grant(task.task_id),
        scope=task.scope,
        task_id=task.task_id,
        enabled=True,
        authority_atomic_replay=True,
        queue_capacity=3,
        clock=lambda: NOW,
    )
    with pytest.raises(FormalTaskViolation) as raised:
        await bounded.start()
    assert raised.value.reason == "TASK_EVENT_AUTHORITY_PREFIX_CAPACITY"
    assert raised.value.code is ErrorCode.UNAVAILABLE
    assert bounded.snapshot().source_reads == 0
    assert bounded.snapshot().queue_allocated is False
    assert bounded.snapshot().worker_pending is False


@pytest.mark.asyncio
async def test_authority_close_before_prefix_delivery_has_zero_task_effect(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "atomic-close.sqlite")
    task = _create_task(store, tmp_path)
    _advance_running(store, task)
    before = store.counts()
    subscription = TaskEventSubscription(
        source=store,
        authorization=_grant(task.task_id),
        scope=task.scope,
        task_id=task.task_id,
        enabled=True,
        authority_atomic_replay=True,
        clock=lambda: NOW,
    )

    assert await subscription.start() is True
    assert subscription.snapshot().worker_pending is False
    await subscription.close()
    snapshot = subscription.snapshot()
    assert snapshot.state is TaskEventSubscriptionState.CLOSED
    assert snapshot.queued_events == 0
    assert snapshot.worker_pending is False
    assert store.counts() == before


@pytest.mark.asyncio
async def test_old_epoch_feed_closes_on_its_terminal_and_never_consumes_retry(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "epoch-race.sqlite")
    task_a = _create_task(store, tmp_path)
    _advance_running(store, task_a)
    _advance_terminal(store, task_a, outcome=TerminalOutcome.CANCELLED)
    task_b = _retry_task(
        store,
        store.get_task(task_a.task_id, task_a.scope),
        attempt_number=2,
    )
    _advance_running(store, task_b)
    task_b = store.get_task(task_b.task_id, task_b.scope)
    subscription = TaskEventSubscription(
        source=store,
        authorization=_grant(task_b.task_id),
        scope=task_b.scope,
        task_id=task_b.task_id,
        enabled=True,
        authority_atomic_replay=True,
        queue_capacity=32,
        poll_interval=0.001,
        clock=lambda: NOW,
    )
    assert await subscription.start() is True
    segment_start = subscription.snapshot().segment_start_seq
    assert segment_start is not None and segment_start > 0

    # Keep the prefix queued while both durable facts land. The tail SELECT sees
    # one SQLite state containing B terminal and C retry_accepted, but its exact
    # attempt filter must expose only B's terminal pair to this retained feed.
    _advance_terminal(store, task_b, outcome=TerminalOutcome.CANCELLED)
    task_c = _retry_task(
        store,
        store.get_task(task_b.task_id, task_b.scope),
        attempt_number=3,
    )
    b_events = store.events(
        task_b.task_id,
        task_b.scope,
        after_seq=segment_start - 1,
        attempt_id=task_b.attempt_id,
    )
    delivered = [await asyncio.wait_for(subscription.next_event(), 1) for _ in range(len(b_events))]

    assert delivered == list(b_events)
    assert delivered[-1].event_type == "task.terminal"
    assert all(event.attempt_id == task_b.attempt_id for event in delivered)
    settled = subscription.snapshot()
    assert settled.state is TaskEventSubscriptionState.CLOSED
    assert settled.failure_reason is None
    assert task_c.attempt_id not in {event.attempt_id for event in delivered}


@pytest.mark.asyncio
async def test_sqlite_live_feed_starts_at_head_and_delivers_terminal_before_close(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.sqlite3")
    task = _create_task(store, tmp_path)
    counts_at_start = store.counts()
    subscription = TaskEventSubscription(
        source=store,
        authorization=_grant(task.task_id),
        scope=_scope(),
        task_id=task.task_id,
        enabled=True,
        queue_capacity=8,
        poll_interval=0.005,
        clock=lambda: NOW,
    )

    assert await subscription.start() is True
    assert subscription.snapshot().start_head_seq == 0
    _advance_running(store, task)
    running = [await asyncio.wait_for(subscription.next_event(), timeout=1) for _ in range(3)]
    assert [event.seq for event in running] == [1, 2, 3]
    assert [event.event_type for event in running] == [
        "attempt.accepted",
        "attempt.running",
        "task.running",
    ]

    _advance_terminal(store, task)
    terminal = [await asyncio.wait_for(subscription.next_event(), timeout=1) for _ in range(2)]
    assert [event.seq for event in terminal] == [4, 5]
    assert terminal[-1].event_type == "task.terminal"
    assert terminal[-1].outcome == TerminalOutcome.COMPLETED.value
    with pytest.raises(StopAsyncIteration):
        await subscription.next_event()

    final = subscription.snapshot()
    assert final.state is TaskEventSubscriptionState.CLOSED
    assert final.terminal_event_seen is True
    assert final.terminal_event_delivered is True
    assert final.cursor_replay_supported is False
    assert final.live_only is True
    counts_after_external_appends = store.counts()
    await subscription.close()
    assert store.counts() == counts_after_external_appends
    assert counts_after_external_appends["tasks"] == counts_at_start["tasks"]
    assert store.get_task(task.task_id, _scope()).outcome is TerminalOutcome.COMPLETED


@pytest.mark.asyncio
async def test_sqlite_feed_accepts_direct_first_running_observation(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.sqlite3")
    task = _create_task(store, tmp_path)
    subscription = TaskEventSubscription(
        source=store,
        authorization=_grant(task.task_id),
        scope=_scope(),
        task_id=task.task_id,
        enabled=True,
        poll_interval=0.005,
        clock=lambda: NOW,
    )
    assert await subscription.start() is True
    _advance_running_direct(store, task)

    delivered = [await asyncio.wait_for(subscription.next_event(), timeout=1) for _ in range(2)]
    assert [event.seq for event in delivered] == [1, 2]
    assert [event.event_type for event in delivered] == [
        "attempt.running",
        "task.running",
    ]
    await subscription.close()


@pytest.mark.asyncio
async def test_sqlite_feed_accepts_distinct_repeated_accepted_observations(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.sqlite3")
    task = _create_task(store, tmp_path)
    subscription = TaskEventSubscription(
        source=store,
        authorization=_grant(task.task_id),
        scope=_scope(),
        task_id=task.task_id,
        enabled=True,
        poll_interval=0.005,
        clock=lambda: NOW,
    )
    assert await subscription.start() is True
    item = store.claim_outbox(f"worker-repeated-accepted:{task.task_id}")
    assert item is not None
    store.complete_outbox(
        item,
        executor_ref=f"executor-ref:{task.attempt_id}",
        observations=(
            _observation(task, source_seq=0, state=FormalAttemptState.ACCEPTED),
            _observation(task, source_seq=1, state=FormalAttemptState.ACCEPTED),
            _observation(task, source_seq=2, state=FormalAttemptState.RUNNING),
        ),
    )

    delivered = [await asyncio.wait_for(subscription.next_event(), timeout=1) for _ in range(4)]
    assert [event.event_type for event in delivered] == [
        "attempt.accepted",
        "attempt.accepted",
        "attempt.running",
        "task.running",
    ]
    assert [event.seq for event in delivered] == [1, 2, 3, 4]
    assert store.get_attempt(task.attempt_id).source_seq == 2
    await subscription.close()


@pytest.mark.asyncio
async def test_sqlite_feed_accepts_task_core_first_terminal_control_path(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.sqlite3")
    task = _create_task(store, tmp_path)
    subscription = TaskEventSubscription(
        source=store,
        authorization=_grant(task.task_id),
        scope=_scope(),
        task_id=task.task_id,
        enabled=True,
        poll_interval=0.005,
        clock=lambda: NOW,
    )
    assert await subscription.start() is True
    _cancel_task(store, task)

    delivered = [await asyncio.wait_for(subscription.next_event(), timeout=1) for _ in range(3)]
    assert [event.event_type for event in delivered] == [
        "task.cancel_requested",
        "attempt.terminal",
        "task.terminal",
    ]
    assert delivered[1].producer == "task_core.reconciliation"
    assert delivered[1].outcome == TerminalOutcome.CANCELLED.value
    with pytest.raises(StopAsyncIteration):
        await subscription.next_event()


@pytest.mark.asyncio
async def test_sqlite_terminal_sentinel_starts_closed_without_history_worker(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.sqlite3")
    created = _create_task(store, tmp_path)
    _cancel_task(store, created)
    task = store.get_task(created.task_id, _scope())
    attempt = store.get_attempt(created.attempt_id)
    assert task.state is FormalTaskState.TERMINAL
    assert task.outcome is TerminalOutcome.CANCELLED
    assert attempt.state is FormalAttemptState.TERMINAL
    assert attempt.outcome is TerminalOutcome.CANCELLED
    assert attempt.source_seq == -1
    assert attempt.executor_ref is None
    counts = store.counts()
    subscription = TaskEventSubscription(
        source=store,
        authorization=_grant(task.task_id),
        scope=_scope(),
        task_id=task.task_id,
        enabled=True,
        clock=lambda: NOW,
    )

    assert await subscription.start() is True
    snapshot = subscription.snapshot()
    assert snapshot.state is TaskEventSubscriptionState.CLOSED
    assert snapshot.start_head_seq == snapshot.last_seq == task.event_head
    assert snapshot.close_reason == "already_terminal_at_start_head"
    assert snapshot.queue_allocated is False
    assert snapshot.worker_pending is False
    assert snapshot.source_reads == 0
    assert store.counts() == counts
    with pytest.raises(StopAsyncIteration):
        await subscription.next_event()


@pytest.mark.asyncio
async def test_sqlite_nonzero_head_feed_is_live_only_and_uses_attempt_baseline(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.sqlite3")
    task = _create_task(store, tmp_path)
    _advance_running(store, task)
    running_snapshot = store.get_task(task.task_id, _scope())
    running_attempt = store.get_attempt(task.attempt_id)
    assert running_snapshot.event_head == 3
    assert running_snapshot.state is FormalTaskState.RUNNING
    assert running_attempt.state is FormalAttemptState.RUNNING
    assert running_attempt.source_seq == 1
    subscription = TaskEventSubscription(
        source=store,
        authorization=_grant(task.task_id),
        scope=_scope(),
        task_id=task.task_id,
        enabled=True,
        poll_interval=0.005,
        clock=lambda: NOW,
    )

    assert await subscription.start() is True
    assert subscription.snapshot().start_head_seq == 3
    assert subscription.snapshot().queued_events == 0
    _advance_terminal(store, task)

    delivered = [await asyncio.wait_for(subscription.next_event(), timeout=1) for _ in range(2)]
    assert [event.seq for event in delivered] == [4, 5]
    assert [event.event_type for event in delivered] == [
        "attempt.terminal",
        "task.terminal",
    ]
    with pytest.raises(StopAsyncIteration):
        await subscription.next_event()


@pytest.mark.asyncio
async def test_two_sqlite_task_feeds_are_isolated_and_detach_has_zero_task_effects(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "tasks.sqlite3")
    first = _create_task(store, tmp_path, suffix="one")
    second = _create_task(store, tmp_path, suffix="two")
    subscriptions = [
        TaskEventSubscription(
            source=store,
            authorization=_grant(task.task_id),
            scope=_scope(),
            task_id=task.task_id,
            enabled=True,
            poll_interval=0.005,
            clock=lambda: NOW,
        )
        for task in (first, second)
    ]
    await asyncio.gather(*(subscription.start() for subscription in subscriptions))

    _advance_running(store, first)
    await _wait_until(lambda: subscriptions[0].snapshot().queued_events == 3)
    await asyncio.sleep(0.02)
    assert subscriptions[1].snapshot().queued_events == 0
    before_detach = store.counts()
    first_snapshot = store.get_task(first.task_id, _scope())
    second_snapshot = store.get_task(second.task_id, _scope())

    await subscriptions[0].close()

    assert store.counts() == before_detach
    assert store.get_task(first.task_id, _scope()) == first_snapshot
    assert store.get_task(second.task_id, _scope()) == second_snapshot
    assert subscriptions[0].snapshot().discarded_events == 3
    assert subscriptions[1].snapshot().state is TaskEventSubscriptionState.ACTIVE

    _advance_running(store, second)
    received = [await asyncio.wait_for(subscriptions[1].next_event(), timeout=1) for _ in range(3)]
    assert {event.task_id for event in received} == {second.task_id}
    await subscriptions[1].close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "authorization",
    [
        None,
        _grant("task-1", scope=_scope("user-2")),
        _grant("task-other"),
        _grant("task-1", operation="task.get"),
        _grant("task-1", capabilities=frozenset()),
        _grant("task-1", expires_at=NOW),
        _grant(
            "task-1",
            scope=ScopeRef(
                "user-1",
                "project-1",
                "session-1",
                Assurance.REQUEST_ASSERTED,
            ),
        ),
    ],
)
async def test_authorization_rejects_before_object_read_queue_or_worker(
    tmp_path: Path, authorization: TaskAuthorizationGrant | None
) -> None:
    source = _ScriptedSource(_record(tmp_path))
    subscription = TaskEventSubscription(
        source=source,
        authorization=authorization,
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        clock=lambda: NOW,
    )

    with pytest.raises(FormalTaskViolation):
        await subscription.start()

    snapshot = subscription.snapshot()
    assert source.get_calls == 0
    assert source.event_calls == 0
    assert snapshot.state is TaskEventSubscriptionState.NEW
    assert snapshot.queue_allocated is False
    assert snapshot.queued_events == 0
    assert snapshot.worker_pending is False


def test_unauthorized_start_does_not_bind_loop_or_prevent_later_close(
    tmp_path: Path,
) -> None:
    source = _ScriptedSource(_record(tmp_path))
    subscription = TaskEventSubscription(
        source=source,
        authorization=None,
        scope=_scope(),
        task_id="task-1",
        enabled=True,
    )

    async def reject_on_first_loop() -> None:
        with pytest.raises(FormalTaskViolation) as raised:
            await subscription.start()
        assert raised.value.reason == "FORMAL_TASK_AUTHORIZATION_REQUIRED"

    asyncio.run(reject_on_first_loop())
    assert source.get_calls == 0
    assert source.attempt_calls == 0
    assert source.event_calls == 0
    assert subscription.snapshot().state is TaskEventSubscriptionState.NEW
    assert subscription.snapshot().queue_allocated is False
    assert subscription.snapshot().worker_pending is False

    # A fresh loop can still close the untouched NEW reader.
    asyncio.run(subscription.close())
    snapshot = subscription.snapshot()
    assert snapshot.state is TaskEventSubscriptionState.CLOSED
    assert snapshot.close_reason == "detached_before_start"
    assert snapshot.queue_allocated is False
    assert snapshot.worker_pending is False


@pytest.mark.asyncio
async def test_feature_off_creates_no_reader_queue_timer_or_worker(
    tmp_path: Path,
) -> None:
    source = _ScriptedSource(_record(tmp_path))
    subscription = TaskEventSubscription(
        source=source,
        authorization=None,
        scope=_scope(),
        task_id="task-1",
        enabled=False,
    )

    assert await subscription.start() is False

    snapshot = subscription.snapshot()
    assert snapshot.state is TaskEventSubscriptionState.DISABLED
    assert snapshot.close_reason == "feature_off"
    assert snapshot.queue_allocated is False
    assert snapshot.queued_events == 0
    assert snapshot.worker_pending is False
    assert source.get_calls == 0
    assert source.event_calls == 0


@pytest.mark.asyncio
async def test_not_found_is_generic_and_allocates_no_feed_resources(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "empty.sqlite3")
    subscription = TaskEventSubscription(
        source=store,
        authorization=_grant("secret-task"),
        scope=_scope(),
        task_id="secret-task",
        enabled=True,
        clock=lambda: NOW,
    )

    with pytest.raises(FormalTaskViolation) as raised:
        await subscription.start()

    assert raised.value.reason == "TASK_NOT_FOUND"
    assert "secret-task" not in str(raised.value)
    assert subscription.snapshot().worker_pending is False
    assert subscription.snapshot().queue_allocated is False
    assert subscription.snapshot().queued_events == 0


def test_not_found_start_releases_loop_binding_for_cross_loop_close(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "empty-cross-loop.sqlite3")
    counts = store.counts()
    subscription = TaskEventSubscription(
        source=store,
        authorization=_grant("secret-task"),
        scope=_scope(),
        task_id="secret-task",
        enabled=True,
        clock=lambda: NOW,
    )

    async def fail_on_first_loop() -> None:
        with pytest.raises(FormalTaskViolation) as raised:
            await subscription.start()
        assert raised.value.reason == "TASK_NOT_FOUND"

    asyncio.run(fail_on_first_loop())
    failed = subscription.snapshot()
    assert failed.state is TaskEventSubscriptionState.NEW
    assert failed.queue_allocated is False
    assert failed.worker_pending is False
    assert store.counts() == counts

    asyncio.run(subscription.close())
    closed = subscription.snapshot()
    assert closed.state is TaskEventSubscriptionState.CLOSED
    assert closed.close_reason == "detached_before_start"
    assert closed.queue_allocated is False
    assert closed.worker_pending is False
    assert store.counts() == counts


def test_protocol_failed_start_releases_loop_binding_for_cross_loop_close(
    tmp_path: Path,
) -> None:
    source = _ScriptedSource(replace(_record(tmp_path), correlation_id=" "))
    subscription = TaskEventSubscription(
        source=source,
        authorization=_grant("task-1"),
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        clock=lambda: NOW,
    )

    async def fail_on_first_loop() -> None:
        with pytest.raises(FormalTaskViolation) as raised:
            await subscription.start()
        assert raised.value.reason == "TASK_EVENT_SOURCE_PROTOCOL_VIOLATION"

    asyncio.run(fail_on_first_loop())
    assert subscription.snapshot().state is TaskEventSubscriptionState.NEW
    assert source.get_calls == 1
    assert source.attempt_calls == 0
    assert source.event_calls == 0
    assert source.mutations == 0

    asyncio.run(subscription.close())
    closed = subscription.snapshot()
    assert closed.state is TaskEventSubscriptionState.CLOSED
    assert closed.queue_allocated is False
    assert closed.worker_pending is False
    assert source.mutations == 0


def test_malformed_task_spec_is_protocol_failure_and_releases_loop_binding(
    tmp_path: Path,
) -> None:
    valid_task = _record(tmp_path)
    malformed_task = replace(valid_task, spec=None)
    source = _ScriptedSource(malformed_task, attempt=_attempt(valid_task))
    subscription = TaskEventSubscription(
        source=source,
        authorization=_grant("task-1"),
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        clock=lambda: NOW,
    )

    async def fail_on_first_loop() -> None:
        with pytest.raises(FormalTaskViolation) as raised:
            await subscription.start()
        assert raised.value.reason == "TASK_EVENT_SOURCE_PROTOCOL_VIOLATION"
        assert raised.value.code is ErrorCode.PROTOCOL_VIOLATION

    asyncio.run(fail_on_first_loop())
    failed = subscription.snapshot()
    assert failed.state is TaskEventSubscriptionState.NEW
    assert failed.queue_allocated is False
    assert failed.queued_events == 0
    assert failed.worker_pending is False
    assert source.get_calls == 1
    assert source.attempt_calls == 0
    assert source.event_calls == 0
    assert source.mutations == 0

    asyncio.run(subscription.close())
    closed = subscription.snapshot()
    assert closed.state is TaskEventSubscriptionState.CLOSED
    assert closed.close_reason == "detached_before_start"
    assert closed.queue_allocated is False
    assert closed.worker_pending is False
    assert source.mutations == 0


@pytest.mark.asyncio
async def test_start_snapshot_requires_nonempty_correlation_binding(
    tmp_path: Path,
) -> None:
    source = _ScriptedSource(replace(_record(tmp_path), correlation_id=" "))
    subscription = TaskEventSubscription(
        source=source,
        authorization=_grant("task-1"),
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        clock=lambda: NOW,
    )

    with pytest.raises(FormalTaskViolation) as raised:
        await subscription.start()

    assert raised.value.reason == "TASK_EVENT_SOURCE_PROTOCOL_VIOLATION"
    assert source.get_calls == 1
    assert source.attempt_calls == 0
    assert source.event_calls == 0
    assert subscription.snapshot().queue_allocated is False
    assert subscription.snapshot().worker_pending is False


@pytest.mark.asyncio
async def test_start_rejects_inconsistent_task_attempt_task_snapshot(
    tmp_path: Path,
) -> None:
    first = _record(tmp_path)
    changed = replace(first, event_head=1, cancel_requested=True)
    source = _ScriptedSource(first, task_reads=(first, changed))
    subscription = TaskEventSubscription(
        source=source,
        authorization=_grant("task-1"),
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        clock=lambda: NOW,
    )

    with pytest.raises(FormalTaskViolation) as raised:
        await subscription.start()

    assert raised.value.reason == "TASK_EVENT_START_SNAPSHOT_CHANGED"
    assert source.get_calls == 2
    assert source.attempt_calls == 1
    assert source.event_calls == 0
    assert subscription.snapshot().queue_allocated is False
    assert subscription.snapshot().worker_pending is False


@pytest.mark.asyncio
async def test_attempt_baseline_requires_exact_task_executor_and_source_binding(
    tmp_path: Path,
) -> None:
    task = _record(tmp_path, state=FormalTaskState.RUNNING, event_head=3)
    valid = _attempt(
        task,
        state=FormalAttemptState.RUNNING,
        source_seq=1,
    )
    cases = [
        (
            replace(valid, attempt_id="attempt-other"),
            "TASK_EVENT_ATTEMPT_BASELINE_MISMATCH",
        ),
        (replace(valid, task_id="task-other"), "TASK_EVENT_ATTEMPT_BASELINE_MISMATCH"),
        (
            replace(valid, executor_id="executor-other"),
            "TASK_EVENT_ATTEMPT_BASELINE_MISMATCH",
        ),
        (replace(valid, source_seq=-2), "TASK_EVENT_SOURCE_PROTOCOL_VIOLATION"),
        (replace(valid, executor_ref=None), "TASK_EVENT_SOURCE_PROTOCOL_VIOLATION"),
        (
            replace(
                valid,
                state=FormalAttemptState.ACCEPTED,
                source_seq=-1,
                executor_ref=None,
            ),
            "TASK_EVENT_ATTEMPT_BASELINE_CONFLICT",
        ),
    ]

    for attempt, reason in cases:
        source = _ScriptedSource(task, attempt=attempt)
        subscription = TaskEventSubscription(
            source=source,
            authorization=_grant("task-1"),
            scope=_scope(),
            task_id="task-1",
            enabled=True,
            clock=lambda: NOW,
        )

        with pytest.raises(FormalTaskViolation) as raised:
            await subscription.start()

        assert raised.value.reason == reason
        assert source.get_calls == 1
        assert source.attempt_calls == 1
        assert source.event_calls == 0
        assert subscription.snapshot().queue_allocated is False


@pytest.mark.asyncio
async def test_terminal_attempt_sentinel_rejects_mixed_state_or_outcome(
    tmp_path: Path,
) -> None:
    task = _record(
        tmp_path,
        state=FormalTaskState.TERMINAL,
        outcome=TerminalOutcome.CANCELLED,
        event_head=3,
    )
    valid = _attempt(
        task,
        state=FormalAttemptState.TERMINAL,
        outcome=TerminalOutcome.CANCELLED,
        source_seq=-1,
    )
    cases = [
        replace(valid, state=FormalAttemptState.ACCEPTED, outcome=None),
        replace(valid, outcome=TerminalOutcome.COMPLETED),
        replace(valid, state=FormalAttemptState.RUNNING),
    ]

    for attempt in cases:
        source = _ScriptedSource(task, attempt=attempt)
        subscription = TaskEventSubscription(
            source=source,
            authorization=_grant("task-1"),
            scope=_scope(),
            task_id="task-1",
            enabled=True,
            clock=lambda: NOW,
        )
        with pytest.raises(FormalTaskViolation):
            await subscription.start()
        assert source.event_calls == 0
        assert subscription.snapshot().queue_allocated is False
        assert subscription.snapshot().worker_pending is False


@pytest.mark.asyncio
@pytest.mark.parametrize("close_mode", ["timeout", "cancel"])
@pytest.mark.parametrize(
    ("block_read", "expected_task_reads", "expected_attempt_reads"),
    [
        ("task_first", 1, 0),
        ("attempt", 1, 1),
        ("task_confirm", 2, 1),
    ],
)
async def test_cancelled_close_during_blocking_start_prevents_reader_activation(
    tmp_path: Path,
    close_mode: str,
    block_read: str,
    expected_task_reads: int,
    expected_attempt_reads: int,
) -> None:
    source = _BlockingStartSource(_record(tmp_path), block_read=block_read)
    subscription = TaskEventSubscription(
        source=source,
        authorization=_grant("task-1"),
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        clock=lambda: NOW,
    )
    starting = asyncio.create_task(subscription.start())
    await _wait_until(source.read_started.is_set)
    closing = asyncio.create_task(subscription.close())
    await asyncio.sleep(0)

    if close_mode == "timeout":
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(closing, timeout=0.01)
    else:
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing

    source.release_read.set()
    assert await asyncio.wait_for(starting, timeout=1) is False

    snapshot = subscription.snapshot()
    assert snapshot.state is TaskEventSubscriptionState.CLOSED
    assert snapshot.close_reason == "consumer_detached"
    assert snapshot.queue_allocated is False
    assert snapshot.worker_pending is False
    assert source.get_calls == expected_task_reads
    assert source.attempt_calls == expected_attempt_reads
    assert source.event_calls == 0
    assert source.mutations == 0


@pytest.mark.asyncio
async def test_live_feed_reauthorizes_expiry_before_each_object_read(
    tmp_path: Path,
) -> None:
    source = _ScriptedSource(_record(tmp_path))
    current_time = [NOW]
    expires_at = "2026-08-06T10:00:01Z"
    subscription = TaskEventSubscription(
        source=source,
        authorization=_grant("task-1", expires_at=expires_at),
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        poll_interval=0.005,
        clock=lambda: current_time[0],
    )
    await subscription.start()
    await _wait_until(lambda: source.event_calls >= 1)
    reads_before_expiry = source.event_calls

    current_time[0] = expires_at
    await _wait_until(lambda: subscription.snapshot().state is TaskEventSubscriptionState.FAILED)
    await asyncio.sleep(0.01)

    snapshot = subscription.snapshot()
    assert snapshot.failure_reason == "FORMAL_TASK_AUTHORIZATION_EXPIRED"
    assert snapshot.failure_code is ErrorCode.PERMISSION_DENIED
    assert source.event_calls == reads_before_expiry


@pytest.mark.asyncio
async def test_blocking_event_read_cannot_disclose_content_after_grant_expiry(
    tmp_path: Path,
) -> None:
    terminal_attempt = _event(
        seq=1,
        event_type="attempt.terminal",
        state="terminal",
        outcome=TerminalOutcome.COMPLETED.value,
        producer="task_core.reconciliation",
    )
    terminal_task = _event(
        seq=2,
        event_type="task.terminal",
        state="terminal",
        outcome=TerminalOutcome.COMPLETED.value,
        producer="task_core.reconciliation",
        causation_id=terminal_attempt.causation_id,
    )
    source = _BlockingSource(
        _record(tmp_path),
        batch=(terminal_attempt, terminal_task),
    )
    current_time = [NOW]
    expires_at = "2026-08-06T10:00:01Z"
    subscription = TaskEventSubscription(
        source=source,
        authorization=_grant("task-1", expires_at=expires_at),
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        poll_interval=60,
        clock=lambda: current_time[0],
    )
    assert await subscription.start() is True
    await _wait_until(source.read_started.is_set)

    current_time[0] = expires_at
    source.release_read.set()
    await _wait_until(lambda: subscription.snapshot().state is TaskEventSubscriptionState.FAILED)

    with pytest.raises(FormalTaskViolation) as raised:
        await subscription.next_event()
    snapshot = subscription.snapshot()
    assert raised.value.reason == "FORMAL_TASK_AUTHORIZATION_EXPIRED"
    assert raised.value.code is ErrorCode.PERMISSION_DENIED
    assert snapshot.failure_reason == "FORMAL_TASK_AUTHORIZATION_EXPIRED"
    assert snapshot.queued_events == 0
    assert snapshot.last_seq == snapshot.start_head_seq == 0
    assert snapshot.source_reads == 1
    assert snapshot.worker_pending is False
    assert snapshot.terminal_event_seen is False
    assert snapshot.terminal_event_delivered is False
    assert source.event_calls == 1
    assert source.mutations == 0


@pytest.mark.asyncio
async def test_queued_content_is_discarded_if_grant_expires_before_delivery(
    tmp_path: Path,
) -> None:
    terminal_attempt = _event(
        seq=1,
        event_type="attempt.terminal",
        state="terminal",
        outcome=TerminalOutcome.COMPLETED.value,
        producer="task_core.reconciliation",
    )
    terminal_task = _event(
        seq=2,
        event_type="task.terminal",
        state="terminal",
        outcome=TerminalOutcome.COMPLETED.value,
        producer="task_core.reconciliation",
        causation_id=terminal_attempt.causation_id,
    )
    source = _ScriptedSource(
        _record(tmp_path),
        ((terminal_attempt, terminal_task),),
    )
    current_time = [NOW]
    expires_at = "2026-08-06T10:00:01Z"
    subscription = TaskEventSubscription(
        source=source,
        authorization=_grant("task-1", expires_at=expires_at),
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        poll_interval=0.005,
        clock=lambda: current_time[0],
    )
    assert await subscription.start() is True
    await _wait_until(lambda: subscription.snapshot().state is TaskEventSubscriptionState.TERMINAL_PENDING)
    assert subscription.snapshot().queued_events == 2

    current_time[0] = expires_at
    with pytest.raises(FormalTaskViolation) as raised:
        await subscription.next_event()

    snapshot = subscription.snapshot()
    assert raised.value.reason == "FORMAL_TASK_AUTHORIZATION_EXPIRED"
    assert raised.value.code is ErrorCode.PERMISSION_DENIED
    assert snapshot.state is TaskEventSubscriptionState.FAILED
    assert snapshot.failure_reason == "FORMAL_TASK_AUTHORIZATION_EXPIRED"
    assert snapshot.queued_events == 0
    assert snapshot.discarded_events == 2
    assert snapshot.terminal_event_delivered is False
    assert snapshot.worker_pending is False
    assert source.mutations == 0


def test_final_start_read_expiry_preserves_error_and_releases_loop_binding(
    tmp_path: Path,
) -> None:
    source = _BlockingStartSource(_record(tmp_path), block_read="task_confirm")
    current_time = [NOW]
    expires_at = "2026-08-06T10:00:01Z"
    subscription = TaskEventSubscription(
        source=source,
        authorization=_grant("task-1", expires_at=expires_at),
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        clock=lambda: current_time[0],
    )

    async def fail_on_first_loop() -> None:
        starting = asyncio.create_task(subscription.start())
        await _wait_until(source.read_started.is_set)
        current_time[0] = expires_at
        source.release_read.set()
        with pytest.raises(FormalTaskViolation) as raised:
            await starting
        assert raised.value.reason == "FORMAL_TASK_AUTHORIZATION_EXPIRED"
        assert raised.value.code is ErrorCode.PERMISSION_DENIED

    asyncio.run(fail_on_first_loop())
    failed = subscription.snapshot()
    assert failed.state is TaskEventSubscriptionState.NEW
    assert failed.queue_allocated is False
    assert failed.queued_events == 0
    assert failed.worker_pending is False
    assert source.get_calls == 2
    assert source.attempt_calls == 1
    assert source.event_calls == 0
    assert source.mutations == 0

    asyncio.run(subscription.close())
    closed = subscription.snapshot()
    assert closed.state is TaskEventSubscriptionState.CLOSED
    assert closed.close_reason == "detached_before_start"
    assert closed.queue_allocated is False
    assert closed.worker_pending is False
    assert source.mutations == 0


@pytest.mark.asyncio
async def test_identical_duplicate_is_idempotent_and_terminal_is_delivered_once(
    tmp_path: Path,
) -> None:
    accepted = _event(seq=1, event_type="attempt.accepted", state="accepted")
    running = _event(seq=2, event_type="attempt.running", state="running")
    task_running = _event(
        seq=3,
        source_event_id=running.source_event_id,
        causation_id=running.causation_id,
    )
    attempt_terminal = _event(
        seq=4,
        event_type="attempt.terminal",
        state="terminal",
        outcome="completed",
        producer="task_core.reconciliation",
    )
    task_terminal = _event(
        seq=5,
        event_type="task.terminal",
        state="terminal",
        outcome="completed",
        producer="task_core.reconciliation",
        causation_id=attempt_terminal.causation_id,
    )
    source = _ScriptedSource(
        _record(tmp_path),
        (
            (
                accepted,
                accepted,
                running,
                task_running,
                attempt_terminal,
                task_terminal,
            ),
        ),
    )
    subscription = TaskEventSubscription(
        source=source,
        authorization=_grant("task-1"),
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        poll_interval=0.005,
        clock=lambda: NOW,
    )
    await subscription.start()

    delivered = [await asyncio.wait_for(subscription.next_event(), timeout=1) for _ in range(5)]

    assert delivered == [
        accepted,
        running,
        task_running,
        attempt_terminal,
        task_terminal,
    ]
    assert subscription.snapshot().state is TaskEventSubscriptionState.CLOSED
    with pytest.raises(StopAsyncIteration):
        await subscription.next_event()


def _fault_cases() -> list[tuple[str, tuple[PersistentTaskEvent, ...], str]]:
    accepted = _event(seq=1, event_type="attempt.accepted", state="accepted")
    return [
        (
            "event-id-conflict",
            (accepted, replace(accepted, seq=2, details={"changed": True})),
            "TASK_EVENT_ID_CONFLICT",
        ),
        (
            "sequence-conflict",
            (accepted, replace(accepted, event_id="event-other")),
            "TASK_EVENT_SEQUENCE_CONFLICT",
        ),
        ("gap", (_event(seq=2),), "TASK_EVENT_SEQUENCE_GAP"),
        ("reorder", (_event(seq=0),), "TASK_EVENT_SEQUENCE_REORDERED"),
        (
            "foreign-task",
            (_event(seq=1, task_id="task-other"),),
            "TASK_EVENT_SCOPE_MISMATCH",
        ),
        (
            "foreign-scope",
            (_event(seq=1, scope=_scope("user-2")),),
            "TASK_EVENT_SCOPE_MISMATCH",
        ),
        (
            "foreign-attempt",
            (_event(seq=1, attempt_id="attempt-other"),),
            "TASK_EVENT_ATTEMPT_MISMATCH",
        ),
        (
            "foreign-correlation",
            (_event(seq=1, correlation_id="correlation-other"),),
            "TASK_EVENT_CORRELATION_MISMATCH",
        ),
        (
            "attempt-terminal-missing-task-terminal",
            (
                _event(
                    seq=1,
                    event_type="attempt.terminal",
                    state="terminal",
                    outcome="completed",
                    producer="task_core.reconciliation",
                ),
                _event(seq=2, event_type="attempt.running", state="running"),
            ),
            "TASK_EVENT_ATTEMPT_TASK_COUPLING_MISSING",
        ),
        (
            "attempt-type-state-mismatch",
            (_event(seq=1, event_type="attempt.running", state="accepted"),),
            "TASK_EVENT_ATTEMPT_LIFECYCLE_MISMATCH",
        ),
        (
            "task-type-state-mismatch",
            (_event(seq=1, event_type="task.running", state="accepted"),),
            "TASK_EVENT_LIFECYCLE_MISMATCH",
        ),
        (
            "task-transition-repeated",
            (_event(seq=1, event_type="task.accepted", state="accepted"),),
            "TASK_EVENT_LIFECYCLE_CONFLICT",
        ),
        (
            "task-transition-backward",
            (
                _event(seq=1, event_type="attempt.running", state="running"),
                _event(
                    seq=2,
                    event_type="task.running",
                    state="running",
                    source_event_id="source-1",
                    causation_id="source-1",
                ),
                _event(seq=3, event_type="task.accepted", state="accepted"),
            ),
            "TASK_EVENT_LIFECYCLE_CONFLICT",
        ),
        (
            "task-control-state-conflict",
            (
                _event(
                    seq=1,
                    event_type="task.cancel_requested",
                    state="running",
                    producer="task_core.control",
                ),
            ),
            "TASK_EVENT_CONTROL_STATE_CONFLICT",
        ),
        (
            "after-terminal",
            (
                _event(
                    seq=1,
                    event_type="attempt.terminal",
                    state="terminal",
                    outcome="completed",
                    producer="task_core.reconciliation",
                ),
                _event(
                    seq=2,
                    event_type="task.terminal",
                    state="terminal",
                    outcome="completed",
                    producer="task_core.reconciliation",
                    causation_id="cause-1",
                ),
                _event(
                    seq=3,
                    event_type="attempt.running",
                    state="running",
                ),
            ),
            "TASK_EVENT_AFTER_TERMINAL",
        ),
        (
            "wrong-task-lifecycle-producer",
            (
                _event(
                    seq=1,
                    event_type="task.running",
                    state="running",
                    producer="executor-1",
                ),
            ),
            "TASK_EVENT_PRODUCER_MISMATCH",
        ),
        (
            "task-running-missing-attempt-running",
            (_event(seq=1, event_type="task.running", state="running"),),
            "TASK_EVENT_ATTEMPT_STATE_CONFLICT",
        ),
        (
            "terminal-outcome-mismatch",
            (
                _event(
                    seq=1,
                    event_type="attempt.terminal",
                    state="terminal",
                    outcome="completed",
                    producer="task_core.reconciliation",
                ),
                _event(
                    seq=2,
                    event_type="task.terminal",
                    state="terminal",
                    outcome="failed",
                    producer="task_core.reconciliation",
                    causation_id="cause-1",
                ),
            ),
            "TASK_EVENT_ATTEMPT_TASK_COUPLING_MISMATCH",
        ),
        (
            "wrong-control-producer",
            (
                _event(
                    seq=1,
                    event_type="task.cancel_requested",
                    state="accepted",
                    producer="task_core",
                ),
            ),
            "TASK_EVENT_PRODUCER_MISMATCH",
        ),
        (
            "unknown-event-type",
            (_event(seq=1, event_type="task.unknown", state="accepted"),),
            "TASK_EVENT_TYPE_UNKNOWN",
        ),
        (
            "executor-source-evidence-missing",
            (
                replace(
                    _event(seq=1, event_type="attempt.running", state="running"),
                    source_event_id=None,
                ),
            ),
            "TASK_EVENT_ATTEMPT_SOURCE_EVIDENCE_MISMATCH",
        ),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(("_name", "batch", "reason"), _fault_cases())
async def test_protocol_faults_fail_closed_without_speculative_delivery(
    tmp_path: Path,
    _name: str,
    batch: tuple[PersistentTaskEvent, ...],
    reason: str,
) -> None:
    source = _ScriptedSource(_record(tmp_path), (batch,))
    subscription = TaskEventSubscription(
        source=source,
        authorization=_grant("task-1"),
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        poll_interval=0.005,
        clock=lambda: NOW,
    )
    await subscription.start()
    await _wait_until(lambda: subscription.snapshot().state is TaskEventSubscriptionState.FAILED)

    with pytest.raises(FormalTaskViolation) as raised:
        await subscription.next_event()

    snapshot = subscription.snapshot()
    assert raised.value.reason == reason
    assert snapshot.failure_reason == reason
    assert snapshot.failure_code is ErrorCode.PROTOCOL_VIOLATION
    assert snapshot.queued_events == 0
    assert snapshot.last_seq == snapshot.start_head_seq == 0
    assert snapshot.worker_pending is False
    assert source.mutations == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("event_type", "state"),
    [
        ("attempt.accepted", "accepted"),
        ("attempt.running", "running"),
    ],
)
async def test_nonzero_head_attempt_baseline_rejects_backward_or_repeated_events(
    tmp_path: Path,
    event_type: str,
    state: str,
) -> None:
    task = _record(tmp_path, state=FormalTaskState.RUNNING, event_head=3)
    source = _ScriptedSource(
        task,
        ((_event(seq=4, event_type=event_type, state=state),),),
        attempt=_attempt(
            task,
            state=FormalAttemptState.RUNNING,
            source_seq=1,
        ),
    )
    subscription = TaskEventSubscription(
        source=source,
        authorization=_grant("task-1"),
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        poll_interval=0.005,
        clock=lambda: NOW,
    )
    assert await subscription.start() is True
    await _wait_until(lambda: subscription.snapshot().state is TaskEventSubscriptionState.FAILED)

    snapshot = subscription.snapshot()
    assert snapshot.failure_reason == "TASK_EVENT_ATTEMPT_LIFECYCLE_CONFLICT"
    assert snapshot.failure_code is ErrorCode.PROTOCOL_VIOLATION
    assert snapshot.last_seq == snapshot.start_head_seq == 3
    assert snapshot.queued_events == 0
    assert source.mutations == 0


@pytest.mark.asyncio
async def test_executor_cannot_skip_running_with_first_terminal_observation(
    tmp_path: Path,
) -> None:
    source = _ScriptedSource(
        _record(tmp_path),
        (
            (
                _event(
                    seq=1,
                    event_type="attempt.terminal",
                    state="terminal",
                    outcome=TerminalOutcome.COMPLETED.value,
                    producer="executor-1",
                ),
            ),
        ),
    )
    subscription = TaskEventSubscription(
        source=source,
        authorization=_grant("task-1"),
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        poll_interval=0.005,
        clock=lambda: NOW,
    )
    assert await subscription.start() is True
    await _wait_until(lambda: subscription.snapshot().state is TaskEventSubscriptionState.FAILED)

    with pytest.raises(FormalTaskViolation) as raised:
        await subscription.next_event()

    snapshot = subscription.snapshot()
    assert raised.value.reason == "TASK_EVENT_ATTEMPT_LIFECYCLE_CONFLICT"
    assert snapshot.failure_reason == "TASK_EVENT_ATTEMPT_LIFECYCLE_CONFLICT"
    assert snapshot.failure_code is ErrorCode.PROTOCOL_VIOLATION
    assert snapshot.last_seq == snapshot.start_head_seq == 0
    assert snapshot.queued_events == 0
    assert snapshot.worker_pending is False
    assert source.mutations == 0


@pytest.mark.asyncio
async def test_queue_overflow_and_reader_failure_are_explicit_failures(
    tmp_path: Path,
) -> None:
    overflow_source = _ScriptedSource(
        _record(tmp_path),
        (
            (
                _event(seq=1, event_type="attempt.accepted", state="accepted"),
                _event(seq=2, event_type="attempt.accepted", state="accepted"),
            ),
        ),
    )
    overflow = TaskEventSubscription(
        source=overflow_source,
        authorization=_grant("task-1"),
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        queue_capacity=1,
        poll_interval=0.005,
        clock=lambda: NOW,
    )
    await overflow.start()
    await _wait_until(lambda: overflow.snapshot().state is TaskEventSubscriptionState.FAILED)
    assert overflow.snapshot().failure_reason == "TASK_EVENT_SUBSCRIPTION_OVERFLOW"
    assert overflow.snapshot().last_seq == 0
    assert overflow.snapshot().queued_events == 0

    limit_source = _ScriptedSource(
        _record(tmp_path),
        (
            (
                _event(seq=1, event_type="attempt.accepted", state="accepted"),
                _event(seq=2, event_type="attempt.accepted", state="accepted"),
            ),
        ),
    )
    limited = TaskEventSubscription(
        source=limit_source,
        authorization=_grant("task-1"),
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        queue_capacity=4,
        validation_capacity=1,
        poll_interval=0.005,
        clock=lambda: NOW,
    )
    await limited.start()
    await _wait_until(lambda: limited.snapshot().state is TaskEventSubscriptionState.FAILED)
    assert limited.snapshot().failure_reason == ("TASK_EVENT_SUBSCRIPTION_VALIDATION_LIMIT")
    assert limited.snapshot().tracked_events == 0

    reader_source = _ScriptedSource(_record(tmp_path), (RuntimeError("read"),))
    reader = TaskEventSubscription(
        source=reader_source,
        authorization=_grant("task-1"),
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        poll_interval=0.005,
        clock=lambda: NOW,
    )
    await reader.start()
    await _wait_until(lambda: reader.snapshot().state is TaskEventSubscriptionState.FAILED)
    assert reader.snapshot().failure_reason == "TASK_EVENT_SOURCE_FAILURE"
    assert reader.snapshot().failure_code is ErrorCode.UNAVAILABLE


@pytest.mark.asyncio
async def test_cancelled_close_retains_reader_and_never_claims_closed_early(
    tmp_path: Path,
) -> None:
    source = _BlockingSource(_record(tmp_path))
    subscription = TaskEventSubscription(
        source=source,
        authorization=_grant("task-1"),
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        poll_interval=60,
        clock=lambda: NOW,
    )
    await subscription.start()
    await _wait_until(source.read_started.is_set)

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(subscription.close(), timeout=0.01)

    pending = subscription.snapshot()
    assert pending.state is TaskEventSubscriptionState.DETACHING
    assert pending.worker_pending is True
    assert pending.close_reason == "consumer_detached"
    assert source.mutations == 0

    source.release_read.set()
    await subscription.close()
    settled = subscription.snapshot()
    assert settled.state is TaskEventSubscriptionState.CLOSED
    assert settled.worker_pending is False
    assert source.mutations == 0


@pytest.mark.asyncio
async def test_cancelled_consumer_only_detaches_subscription(tmp_path: Path) -> None:
    source = _ScriptedSource(_record(tmp_path))
    subscription = TaskEventSubscription(
        source=source,
        authorization=_grant("task-1"),
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        poll_interval=0.005,
        clock=lambda: NOW,
    )
    await subscription.start()
    consumer = asyncio.create_task(subscription.next_event())
    await asyncio.sleep(0)
    consumer.cancel()

    with pytest.raises(asyncio.CancelledError):
        await consumer
    await subscription.close()

    assert subscription.snapshot().state is TaskEventSubscriptionState.CLOSED
    assert subscription.snapshot().close_reason == "consumer_cancelled"
    assert source.mutations == 0


@pytest.mark.asyncio
async def test_already_terminal_start_is_honest_live_only_without_history_read(
    tmp_path: Path,
) -> None:
    source = _ScriptedSource(
        _record(
            tmp_path,
            state=FormalTaskState.TERMINAL,
            outcome=TerminalOutcome.COMPLETED,
            event_head=7,
        )
    )
    subscription = TaskEventSubscription(
        source=source,
        authorization=_grant("task-1"),
        scope=_scope(),
        task_id="task-1",
        enabled=True,
        clock=lambda: NOW,
    )

    assert await subscription.start() is True
    snapshot = subscription.snapshot()
    assert snapshot.state is TaskEventSubscriptionState.CLOSED
    assert snapshot.start_head_seq == snapshot.last_seq == 7
    assert snapshot.close_reason == "already_terminal_at_start_head"
    assert snapshot.terminal_event_seen is False
    assert snapshot.terminal_event_delivered is False
    assert source.get_calls == 2
    assert source.attempt_calls == 1
    assert source.event_calls == 0
    with pytest.raises(StopAsyncIteration):
        await subscription.next_event()
