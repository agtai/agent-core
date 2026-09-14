# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Bounded, service-owned Native analysis; speech responses own no work lifetime.

The injected journal owns durable storage. Admission is saved before scheduling
an Agent. Recovery restores facts only: interrupted process ownership is UNKNOWN
and never replays tools. This is not the durable Task executor.
"""

from __future__ import annotations

import asyncio
import hashlib
import math
import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol, TypeVar

import anyio

from .contracts import ContextRef, ErrorCode, ScopeRef, canonical_json_bytes
from .execution_control import CURRENT_INTERACTION_CONTROL, read_only_operation

T = TypeVar("T")


class WorkViolation(ValueError):
    def __init__(self, reason: str, message: str, code: ErrorCode = ErrorCode.INVALID_ARGUMENT):
        super().__init__(message)
        self.reason = reason
        self.code = code


class WorkCancelled(WorkViolation):
    def __init__(self) -> None:
        super().__init__(
            "NATIVE_WORK_CANCELLED",
            "Native work cancellation requested",
            ErrorCode.CANCELLED,
        )


class WorkState(StrEnum):
    ACCEPTED = "accepted"
    RUNNING = "running"
    CANCELLING = "cancelling"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    SUPERSEDED = "superseded"
    FAILED = "failed"
    UNKNOWN = "unknown"


_TERMINAL = frozenset(
    {
        WorkState.COMPLETED,
        WorkState.CANCELLED,
        WorkState.SUPERSEDED,
        WorkState.FAILED,
        WorkState.UNKNOWN,
    }
)


class WorkContextEntry(Protocol):
    ref: ContextRef
    content: str


class WorkContext(Protocol):
    """Canonical immutable input facts supplied by an application's context adapter."""

    scope: ScopeRef
    entries: tuple[WorkContextEntry, ...]


def context_identity(context: WorkContext) -> str:
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "scope": context.scope.to_dict(),
                "entries": [{"ref": entry.ref.to_dict(), "content": entry.content} for entry in context.entries],
            }
        )
    ).hexdigest()


def _text(value: str, name: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WorkViolation("INVALID_NATIVE_WORK_INPUT", f"{name} must be nonempty")
    try:
        if len(value.encode("utf-8")) > maximum:
            raise WorkViolation("INVALID_NATIVE_WORK_INPUT", f"{name} exceeds its bound")
    except UnicodeEncodeError as error:
        raise WorkViolation("INVALID_NATIVE_WORK_INPUT", f"{name} must be valid UTF-8") from error
    return value


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class WorkSnapshot:
    scope: ScopeRef
    work_id: str
    revision: int
    sequence: int
    request_id: str
    input_id: str
    instruction: str
    model_identity: str
    model_config_version: str
    context_id: str
    foreground: bool
    state: WorkState
    accepted_at: str
    updated_at: str
    result_text: str | None = None
    reason: str | None = None
    supersedes_revision: int | None = None
    execution_settled: bool = False

    def __post_init__(self) -> None:
        if (
            not isinstance(self.scope, ScopeRef)
            or not isinstance(self.state, WorkState)
            or type(self.revision) is not int
            or self.revision < 1
            or type(self.sequence) is not int
            or self.sequence < 1
            or type(self.foreground) is not bool
            or type(self.execution_settled) is not bool
            or self.supersedes_revision != (None if self.revision == 1 else self.revision - 1)
        ):
            raise WorkViolation(
                "INVALID_NATIVE_WORK_SNAPSHOT",
                "invalid retained work identity or state",
            )
        for name in (
            "work_id",
            "request_id",
            "input_id",
            "model_identity",
            "model_config_version",
        ):
            _text(getattr(self, name), name)
        _text(self.instruction, "instruction", 4096)
        if (
            not isinstance(self.context_id, str)
            or len(self.context_id) != 64
            or any(c not in "0123456789abcdef" for c in self.context_id)
        ):
            raise WorkViolation("INVALID_NATIVE_WORK_SNAPSHOT", "invalid context identity")
        for timestamp in (self.accepted_at, self.updated_at):
            try:
                if datetime.fromisoformat(timestamp.replace("Z", "+00:00")).tzinfo is None:
                    raise ValueError("timezone required")
            except (AttributeError, ValueError) as error:
                raise WorkViolation("INVALID_NATIVE_WORK_SNAPSHOT", "invalid work timestamp") from error
        if self.result_text is not None:
            _text(self.result_text, "result_text", 131072)
            if self.state not in {
                WorkState.COMPLETED,
                WorkState.SUPERSEDED,
            }:
                raise WorkViolation(
                    "INVALID_NATIVE_WORK_SNAPSHOT",
                    "uncompleted work cannot claim a result",
                )
        if self.state is WorkState.COMPLETED and (not self.execution_settled or self.result_text is None):
            raise WorkViolation(
                "INVALID_NATIVE_WORK_SNAPSHOT",
                "completed work requires a settled result",
            )
        if self.reason is not None:
            _text(self.reason, "reason")

    def to_dict(self) -> dict[str, object]:
        from dataclasses import fields

        result = {item.name: getattr(self, item.name) for item in fields(self)}
        result["scope"] = self.scope.to_dict()
        result["state"] = self.state.value
        return result

    @classmethod
    def from_dict(cls, value: dict[str, object]) -> WorkSnapshot:
        copied = dict(value)
        copied["scope"] = ScopeRef.from_dict(copied["scope"])
        copied["state"] = WorkState(copied["state"])
        return cls(**copied)


@dataclass(slots=True)
class WorkControl:
    """Exact admitted work capability handed only to its owned runner."""

    snapshot: WorkSnapshot
    cancelled: asyncio.Event
    settlement: asyncio.Future | None = None
    observer: Callable[..., None] | None = None

    def observe(self, stage: str, **fields: object) -> None:
        if self.observer is not None:
            try:
                self.observer(
                    "native_work",
                    work_id=self.snapshot.work_id,
                    request_id=self.snapshot.request_id,
                    milestone=stage,
                    **fields,
                )
            except Exception:
                pass  # Observability cannot alter execution authority.

    def check(self) -> None:
        if self.cancelled.is_set():
            raise WorkCancelled()

    async def read_only(self, operation: Awaitable[T], *, timeout: float | None = None) -> T:  # noqa: ASYNC109
        return await read_only_operation(self, self.cancelled, operation, timeout=timeout)


WorkRunner = Callable[[WorkControl], Awaitable[str]]


@dataclass(slots=True)
class _Record:
    snapshot: WorkSnapshot
    control: WorkControl
    operation: asyncio.Future[None] | None = None
    runner: asyncio.Task[str] | None = None


class WorkRuntime:
    def __init__(
        self,
        *,
        save: Callable[[WorkSnapshot], None] | None = None,
        restored: tuple[WorkSnapshot, ...] = (),
        max_active: int = 4,
        reserved_foreground: int = 1,
        max_records: int = 128,
        timeout_seconds: float = 120.0,
        cancel_settlement_seconds: float = 1.0,
        observer: Callable[..., None] | None = None,
        task_group_provider: Callable[[], anyio.abc.TaskGroup | None] | None = None,
    ) -> None:
        if (
            type(max_active) is not int
            or type(reserved_foreground) is not int
            or not 0 < reserved_foreground < max_active
            or type(max_records) is not int
            or max_records < max_active
        ):
            raise WorkViolation("INVALID_NATIVE_WORK_BOUNDS", "invalid Native work capacity")
        for bound in (timeout_seconds, cancel_settlement_seconds):
            if isinstance(bound, bool) or not isinstance(bound, (int, float)) or not math.isfinite(bound) or bound <= 0:
                raise WorkViolation(
                    "INVALID_NATIVE_WORK_BOUNDS",
                    "deadlines must be positive and finite",
                )
        self._observer = observer
        self._task_group_provider = task_group_provider
        self._save = save
        self._max_active = max_active
        self._background_limit = max_active - reserved_foreground
        self._max_records = max_records
        self._timeout = timeout_seconds
        self._cancel_timeout = cancel_settlement_seconds
        self._records: dict[tuple[ScopeRef, str, int], _Record] = {}
        self._requests: dict[tuple[ScopeRef, str], _Record] = {}
        self._latest: dict[tuple[ScopeRef, str], int] = {}
        self._closed = False
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._observation_epoch = secrets.token_hex(16)
        self._observation_sequence: dict[ScopeRef, int] = {}
        self._observation_events: dict[ScopeRef, asyncio.Event] = {}
        self._observation_waiters = 0
        self._observation_event_waiters: dict[asyncio.Event, int] = {}
        if len(restored) > max_records:
            raise WorkViolation("NATIVE_WORK_LEDGER_FULL", "restored work exceeds configured capacity")
        for original in restored:
            if not isinstance(original, WorkSnapshot):
                raise WorkViolation(
                    "INVALID_NATIVE_WORK_RESTORE",
                    "restore requires canonical snapshots",
                )
            self._validate_identity(
                original.scope,
                original.request_id,
                original.input_id,
                original.instruction,
                original.model_identity,
                original.model_config_version,
                original.context_id,
                original.foreground,
            )
            if original.revision < 1 or original.sequence < 1:
                raise WorkViolation("INVALID_NATIVE_WORK_RESTORE", "invalid restored version")
            snapshot = original
            if original.state not in _TERMINAL or not original.execution_settled:
                snapshot = replace(
                    original,
                    state=WorkState.UNKNOWN,
                    sequence=original.sequence + 1,
                    reason="PROCESS_OWNERSHIP_LOST",
                    execution_settled=True,
                    updated_at=_now(),
                )
                self._persist(snapshot)
            self._insert(_Record(snapshot, WorkControl(snapshot, asyncio.Event(), observer=self._observer)))

    def _require_owner(self) -> None:
        running = asyncio.get_running_loop()
        if self._owner_loop is None:
            self._owner_loop = running
        if self._owner_loop is not running:
            raise WorkViolation("NATIVE_WORK_OWNER_MISMATCH", "work owner cannot cross event loops")

    @staticmethod
    def _validate_identity(
        scope,
        request_id,
        input_id,
        instruction,
        model_identity,
        model_config_version,
        context_id,
        foreground,
    ):
        if not isinstance(scope, ScopeRef) or type(foreground) is not bool:
            raise WorkViolation("INVALID_NATIVE_WORK_INPUT", "canonical scope and priority are required")
        for name, value in (
            ("request_id", request_id),
            ("input_id", input_id),
            ("model_identity", model_identity),
            ("model_config_version", model_config_version),
        ):
            _text(value, name)
        _text(instruction, "instruction", 4096)
        if (
            not isinstance(context_id, str)
            or len(context_id) != 64
            or any(c not in "0123456789abcdef" for c in context_id)
        ):
            raise WorkViolation("INVALID_NATIVE_WORK_INPUT", "context_id must be a SHA256 identity")

    def _persist(self, snapshot: WorkSnapshot) -> None:
        if self._save is not None:
            try:
                self._save(snapshot)
            except Exception as error:
                raise WorkViolation(
                    "NATIVE_WORK_PERSISTENCE_FAILED",
                    "work checkpoint could not be saved",
                    ErrorCode.UNAVAILABLE,
                ) from error

    def _insert(self, record: _Record) -> None:
        s = record.snapshot
        key = (s.scope, s.work_id, s.revision)
        request_key = (s.scope, s.request_id)
        if key in self._records or request_key in self._requests:
            raise WorkViolation(
                "NATIVE_WORK_ID_CONFLICT",
                "retained work identity conflicts",
                ErrorCode.CONFLICT,
            )
        self._records[key] = record
        self._requests[request_key] = record
        self._latest[(s.scope, s.work_id)] = max(s.revision, self._latest.get((s.scope, s.work_id), 0))
        self._signal_observation(s.scope)

    def observation_cursor(self, scope: ScopeRef) -> dict[str, object]:
        return {
            "epoch": self._observation_epoch,
            "sequence": self._observation_sequence.get(scope, 0),
            "read_sequence": 0,
        }

    def _signal_observation(self, scope: ScopeRef) -> None:
        if scope not in self._observation_sequence and len(self._observation_sequence) >= self._max_records:
            # Retired activations can introduce scopes without any Work record.
            # A bounded observation cache must not retain those scopes forever.
            # Epoch replacement makes every older cursor request full facts;
            # waking the exact old events preserves installed waiter ownership.
            self._observation_epoch = secrets.token_hex(16)
            self._observation_sequence.clear()
            for event in self._observation_events.values():
                event.set()
            self._observation_events.clear()
        self._observation_sequence[scope] = self._observation_sequence.get(scope, 0) + 1
        event = self._observation_events.pop(scope, None)
        if event is not None:
            event.set()

    def wake_observers(self, scope: ScopeRef) -> None:
        self._signal_observation(scope)

    async def wait_for_observation(self, *, scope: ScopeRef, after, wait_ms: int) -> None:
        from .observation import MAX_OBSERVATION_WAIT_MS, observation_cursor

        self._require_owner()
        after = observation_cursor(after)
        if type(wait_ms) is not int or not 0 <= wait_ms <= MAX_OBSERVATION_WAIT_MS:
            raise ValueError("invalid Native observation wait")
        current = self.observation_cursor(scope)
        if (
            self._closed
            or after is None
            or not wait_ms
            or (after["epoch"], after["sequence"]) != (current["epoch"], current["sequence"])
        ):
            return
        if self._observation_waiters >= 128:
            raise WorkViolation(
                "NATIVE_OBSERVATION_CAPACITY", "bounded observation capacity is full", ErrorCode.UNAVAILABLE
            )
        # No await separates the cursor check and event installation. Work
        # transitions use this same event-loop owner and set this exact event.
        event = self._observation_events.setdefault(scope, asyncio.Event())
        self._observation_waiters += 1
        self._observation_event_waiters[event] = self._observation_event_waiters.get(event, 0) + 1
        try:
            try:
                await asyncio.wait_for(event.wait(), wait_ms / 1000)
            except TimeoutError:
                pass
        finally:
            self._observation_waiters -= 1
            remaining = self._observation_event_waiters[event] - 1
            if remaining:
                self._observation_event_waiters[event] = remaining
            else:
                self._observation_event_waiters.pop(event, None)
                if self._observation_events.get(scope) is event:
                    self._observation_events.pop(scope, None)

    def _transition(self, record: _Record, state: WorkState, **fields) -> bool:
        if fields.get("execution_settled") and (
            (record.runner is not None and not record.runner.done())
            or (record.control.settlement is not None and not record.control.settlement.done())
        ):
            fields["execution_settled"] = False
        if state not in {WorkState.COMPLETED, WorkState.SUPERSEDED}:
            fields.setdefault("result_text", None)
        updated = replace(
            record.snapshot,
            sequence=record.snapshot.sequence + 1,
            state=state,
            updated_at=_now(),
            **fields,
        )
        try:
            self._persist(updated)
        except WorkViolation:
            # Never publish success when its checkpoint failed. The previous
            # durable nonterminal record restores as UNKNOWN, without replay.
            record.snapshot = replace(
                updated,
                state=WorkState.UNKNOWN,
                reason="NATIVE_WORK_PERSISTENCE_FAILED",
                result_text=None,
            )
            record.control.cancelled.set()
            self._signal_observation(record.snapshot.scope)
            return False
        record.snapshot = updated
        self._signal_observation(updated.scope)
        return True

    def _record(
        self,
        scope: ScopeRef,
        work_id: str,
        revision: int | None = None,
        *,
        current: bool = False,
    ) -> _Record:
        latest = self._latest.get((scope, work_id))
        if latest is None:
            raise WorkViolation(
                "NATIVE_WORK_NOT_FOUND",
                "no work in the exact scope",
                ErrorCode.PERMISSION_DENIED,
            )
        if revision is not None and (type(revision) is not int or revision < 1):
            raise WorkViolation("INVALID_NATIVE_WORK_REVISION", "revision must be a positive integer")
        if current and revision != latest:
            raise WorkViolation(
                "NATIVE_WORK_REVISION_STALE",
                "work revision has been superseded",
                ErrorCode.STALE,
            )
        record = self._records.get((scope, work_id, latest if revision is None else revision))
        if record is None:
            raise WorkViolation("NATIVE_WORK_NOT_FOUND", "work revision is unavailable", ErrorCode.STALE)
        return record

    def query(self, *, scope: ScopeRef, work_id: str, revision: int | None = None) -> WorkSnapshot:
        return self._record(scope, work_id, revision).snapshot

    def list(self, *, scope: ScopeRef) -> tuple[WorkSnapshot, ...]:
        return tuple(
            self._records[(item_scope, work_id, revision)].snapshot
            for (item_scope, work_id), revision in self._latest.items()
            if item_scope == scope
        )

    def _admit(
        self,
        *,
        scope,
        request_id,
        input_id,
        instruction,
        model_identity,
        model_config_version,
        context_id,
        runner,
        foreground,
        predecessor: _Record | None = None,
    ) -> tuple[_Record, bool]:
        self._require_owner()
        self._validate_identity(
            scope,
            request_id,
            input_id,
            instruction,
            model_identity,
            model_config_version,
            context_id,
            foreground,
        )
        if not callable(runner):
            raise WorkViolation("INVALID_NATIVE_WORK_RUNNER", "an owned async runner is required")
        existing = self._requests.get((scope, request_id))
        if existing is not None:
            s = existing.snapshot
            if (
                s.input_id,
                s.instruction,
                s.model_identity,
                s.model_config_version,
                s.context_id,
                s.foreground,
                s.supersedes_revision,
                s.work_id if predecessor else None,
            ) != (
                input_id,
                instruction,
                model_identity,
                model_config_version,
                context_id,
                foreground,
                predecessor.snapshot.revision if predecessor else None,
                predecessor.snapshot.work_id if predecessor else None,
            ):
                raise WorkViolation(
                    "NATIVE_WORK_REQUEST_CONFLICT",
                    "request cannot change its admitted binding",
                    ErrorCode.CONFLICT,
                )
            return existing, False
        if self._closed:
            raise WorkViolation(
                "NATIVE_WORK_CLOSED",
                "service work owner is closed",
                ErrorCode.UNAVAILABLE,
            )
        if len(self._records) >= self._max_records:
            raise WorkViolation(
                "NATIVE_WORK_LEDGER_FULL",
                "bounded work ledger is full",
                ErrorCode.UNAVAILABLE,
            )
        occupied_by_work = {
            (r.snapshot.scope, r.snapshot.work_id): r
            for r in self._records.values()
            if r.operation is not None and not r.snapshot.execution_settled
        }
        occupied = list(occupied_by_work.values())
        # A replacement shares its predecessor's reservation until settlement.
        occupied = [
            r
            for r in occupied
            if predecessor is None or r.snapshot.work_id != predecessor.snapshot.work_id or r.snapshot.scope != scope
        ]
        if len(occupied) >= self._max_active or (
            not foreground and sum(not r.snapshot.foreground for r in occupied) >= self._background_limit
        ):
            raise WorkViolation(
                "NATIVE_WORK_CAPACITY_FULL",
                "bounded work capacity is full; foreground capacity is reserved",
                ErrorCode.UNAVAILABLE,
            )
        now = _now()
        snapshot = WorkSnapshot(
            scope=scope,
            work_id=predecessor.snapshot.work_id if predecessor else "native-work-" + secrets.token_hex(16),
            revision=predecessor.snapshot.revision + 1 if predecessor else 1,
            sequence=1,
            request_id=request_id,
            input_id=input_id,
            instruction=instruction,
            model_identity=model_identity,
            model_config_version=model_config_version,
            context_id=context_id,
            foreground=foreground,
            state=WorkState.ACCEPTED,
            accepted_at=now,
            updated_at=now,
            supersedes_revision=predecessor.snapshot.revision if predecessor else None,
        )
        task_group = self._task_group_provider() if self._task_group_provider is not None else None
        self._persist(snapshot)
        record = _Record(snapshot, WorkControl(snapshot, asyncio.Event(), observer=self._observer))
        self._insert(record)
        if predecessor is not None:
            self._transition(
                predecessor,
                WorkState.SUPERSEDED,
                reason="SUPERSEDED_BY_NEW_REVISION",
            )
            predecessor.control.cancelled.set()
        name = f"native-work:{snapshot.work_id}:{snapshot.revision}"
        if task_group is None:
            record.operation = asyncio.create_task(self._run(record, runner, predecessor), name=name)
        else:
            record.operation = asyncio.get_running_loop().create_future()

            async def run_owned() -> None:
                from openjiuwen.core.common.task_manager.context import (
                    _current_task_id,
                    reset_task_group,
                    set_task_group,
                )
                from openjiuwen.core.common.task_manager.manager import get_task_manager

                native = None
                operation = self._run(record, runner, predecessor)

                def retain(task) -> None:
                    nonlocal native
                    native = task

                group_token = set_task_group(task_group)
                parent_token = _current_task_id.set(None)
                try:
                    await get_task_manager().create_task(
                        operation, name=name, group="application-work",
                        catch_exceptions=True, on_scheduled=retain,
                    )
                except BaseException:
                    record.control.cancelled.set()
                    if record.snapshot.state not in _TERMINAL:
                        self._transition(record, WorkState.UNKNOWN, reason="SERVICE_OWNERSHIP_LOST")
                finally:
                    _current_task_id.reset(parent_token)
                    reset_task_group(group_token)
                    with anyio.CancelScope(shield=True):
                        if native is None:
                            operation.close()
                        else:
                            try:
                                await native.wait()
                            except BaseException:
                                if record.snapshot.state not in _TERMINAL:
                                    self._transition(record, WorkState.UNKNOWN, reason="SERVICE_OWNERSHIP_LOST")
                        if record.snapshot.state not in _TERMINAL:
                            self._transition(record, WorkState.UNKNOWN, reason="SERVICE_OWNERSHIP_LOST")
                        if not record.snapshot.execution_settled:
                            self._transition(record, record.snapshot.state, execution_settled=True)
                        if not record.operation.done():
                            record.operation.set_result(None)

            try:
                task_group.start_soon(run_owned, name=name)
            except RuntimeError:
                self._transition(
                    record, WorkState.UNKNOWN, reason="SERVICE_OWNERSHIP_LOST", execution_settled=True
                )
                record.operation.set_result(None)
        return record, True

    async def start(
        self,
        *,
        scope: ScopeRef,
        request_id: str,
        input_id: str,
        instruction: str,
        model_identity: str,
        model_config_version: str,
        context_id: str,
        runner: WorkRunner,
        foreground: bool = False,
    ) -> WorkSnapshot:
        record, _ = self._admit(
            scope=scope,
            request_id=request_id,
            input_id=input_id,
            instruction=instruction,
            model_identity=model_identity,
            model_config_version=model_config_version,
            context_id=context_id,
            runner=runner,
            foreground=foreground,
        )
        return record.snapshot

    async def update(
        self,
        *,
        scope: ScopeRef,
        work_id: str,
        revision: int,
        request_id: str,
        input_id: str,
        instruction: str,
        model_identity: str,
        model_config_version: str,
        context_id: str,
        runner: WorkRunner,
    ) -> WorkSnapshot:
        predecessor = self._record(scope, work_id, revision)
        replay = self._requests.get((scope, request_id))
        if replay is None:
            self._record(scope, work_id, revision, current=True)
            if predecessor.snapshot.state in {
                WorkState.UNKNOWN,
                WorkState.CANCELLING,
                WorkState.CANCELLED,
                WorkState.SUPERSEDED,
            }:
                raise WorkViolation(
                    "NATIVE_WORK_NOT_UPDATABLE",
                    "work outcome does not permit an update",
                    ErrorCode.CONFLICT,
                )
        record, _ = self._admit(
            scope=scope,
            request_id=request_id,
            input_id=input_id,
            instruction=instruction,
            model_identity=model_identity,
            model_config_version=model_config_version,
            context_id=context_id,
            runner=runner,
            foreground=predecessor.snapshot.foreground,
            predecessor=predecessor,
        )
        return record.snapshot

    async def cancel(self, *, scope: ScopeRef, work_id: str, revision: int) -> WorkSnapshot:
        self._require_owner()
        record = self._record(scope, work_id, revision, current=True)
        if record.snapshot.state not in _TERMINAL and record.snapshot.state is not WorkState.CANCELLING:
            self._transition(record, WorkState.CANCELLING, reason="EXPLICIT_CANCELLATION")
            record.control.cancelled.set()
        return record.snapshot

    async def _run(self, record: _Record, runner: WorkRunner, predecessor: _Record | None) -> None:
        token = CURRENT_INTERACTION_CONTROL.set(None)
        try:
            await anyio.lowlevel.checkpoint_if_cancelled()
            if predecessor is not None and predecessor.operation is not None:
                try:
                    await asyncio.wait_for(asyncio.shield(predecessor.operation), timeout=self._timeout)
                except TimeoutError:
                    self._transition(
                        record,
                        WorkState.UNKNOWN,
                        reason="PREDECESSOR_OUTCOME_UNKNOWN",
                        execution_settled=True,
                    )
                    return
                if not predecessor.snapshot.execution_settled or predecessor.snapshot.state is WorkState.UNKNOWN:
                    self._transition(
                        record,
                        WorkState.UNKNOWN,
                        reason="PREDECESSOR_OUTCOME_UNKNOWN",
                        execution_settled=True,
                    )
                    return
            if record.control.cancelled.is_set():
                if record.snapshot.state is not WorkState.UNKNOWN:
                    state = (
                        WorkState.SUPERSEDED if record.snapshot.state is WorkState.SUPERSEDED else WorkState.CANCELLED
                    )
                    self._transition(record, state, execution_settled=True)
                else:
                    self._transition(record, WorkState.UNKNOWN, execution_settled=True)
                return
            if not self._transition(record, WorkState.RUNNING):
                self._transition(record, WorkState.UNKNOWN, execution_settled=True)
                return
            record.runner = asyncio.create_task(runner(record.control))
            deadline = asyncio.get_running_loop().time() + self._timeout
            stop = asyncio.create_task(record.control.cancelled.wait())
            try:
                done, _ = await asyncio.wait(
                    {record.runner, stop},
                    timeout=self._timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                timed_out = not done
                if record.runner not in done:
                    record.control.cancelled.set()
                    settled, _ = await asyncio.wait({record.runner}, timeout=self._cancel_timeout)
                    if not settled:
                        self._transition(
                            record,
                            WorkState.UNKNOWN,
                            reason="CANCELLATION_OUTCOME_UNKNOWN",
                            execution_settled=False,
                        )
                        # Keep the physical slot occupied until the real runner
                        # settles. A timeout is never permission for more work.
                        await asyncio.gather(record.runner, return_exceptions=True)
                        return
                if record.control.settlement is not None:
                    cleanup_done, _ = await asyncio.wait(
                        {record.control.settlement, stop},
                        timeout=max(0, deadline - asyncio.get_running_loop().time()),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if record.control.settlement not in cleanup_done:
                        timed_out = timed_out or not cleanup_done
                        record.control.cancelled.set()
                        cleanup_done, _ = await asyncio.wait(
                            {record.control.settlement}, timeout=self._cancel_timeout
                        )
                        if not cleanup_done:
                            self._transition(
                                record, WorkState.UNKNOWN,
                                reason="CANCELLATION_OUTCOME_UNKNOWN", execution_settled=False,
                            )
                            return
                    cleanup = await asyncio.gather(asyncio.shield(record.control.settlement), return_exceptions=True)
                    if isinstance(cleanup[0], BaseException):
                        raise WorkViolation(
                            "EXECUTION_SETTLEMENT_UNCONFIRMED", "physical cleanup failed", ErrorCode.RESULT_UNKNOWN
                        )
                if record.snapshot.state is WorkState.UNKNOWN:
                    await asyncio.gather(record.runner, return_exceptions=True)
                    self._transition(
                        record,
                        WorkState.UNKNOWN,
                        execution_settled=record.control.settlement is None or record.control.settlement.done(),
                    )
                    return
                if record.control.cancelled.is_set():
                    error = record.runner.exception() if not record.runner.cancelled() else None
                    unknown = getattr(error, "code", None) is ErrorCode.RESULT_UNKNOWN
                    state = (
                        WorkState.UNKNOWN
                        if unknown
                        else (
                            WorkState.SUPERSEDED
                            if record.snapshot.state is WorkState.SUPERSEDED
                            else WorkState.CANCELLED
                        )
                    )
                    self._transition(
                        record,
                        state,
                        reason="WORK_DEADLINE_EXCEEDED"
                        if timed_out
                        else (getattr(error, "reason", None) or record.snapshot.reason),
                        execution_settled=not unknown,
                    )
                    return
                result = record.runner.result()
                _text(result, "result_text", 131072)
                self._transition(
                    record,
                    WorkState.COMPLETED,
                    result_text=result,
                    execution_settled=True,
                )
            finally:
                stop.cancel()
                await asyncio.gather(stop, return_exceptions=True)
        except asyncio.CancelledError:
            record.control.cancelled.set()
            self._transition(
                record,
                WorkState.UNKNOWN,
                reason="SERVICE_OWNERSHIP_LOST",
                execution_settled=False,
            )
            raise
        except Exception as error:
            unknown = getattr(error, "code", None) in {
                ErrorCode.RESULT_UNKNOWN,
                ErrorCode.TIMEOUT,
            }
            self._transition(
                record,
                WorkState.UNKNOWN if unknown else WorkState.FAILED,
                reason=getattr(error, "reason", "NATIVE_WORK_EXECUTION_FAILED"),
                execution_settled=not unknown,
            )
        finally:
            try:
                # Native task cancellation must not end the reservation before
                # the separately owned producer and its physical cleanup settle.
                with anyio.CancelScope(shield=True):
                    if record.runner is not None and not record.runner.done():
                        await asyncio.gather(asyncio.shield(record.runner), return_exceptions=True)
                    if record.control.settlement is not None:
                        settled = await asyncio.gather(
                            asyncio.shield(record.control.settlement), return_exceptions=True
                        )
                        if isinstance(settled[0], BaseException) and record.snapshot.state is not WorkState.UNKNOWN:
                            self._transition(
                                record,
                                WorkState.UNKNOWN,
                                reason="EXECUTION_SETTLEMENT_UNCONFIRMED",
                                execution_settled=True,
                            )
                    if record.snapshot.state in _TERMINAL and not record.snapshot.execution_settled:
                        self._transition(record, record.snapshot.state, execution_settled=True)
            finally:
                CURRENT_INTERACTION_CONTROL.reset(token)

    async def close(self) -> tuple[WorkSnapshot, ...]:
        self._require_owner()
        self._closed = True
        for scope in tuple(self._observation_events):
            self._signal_observation(scope)
        pending = []
        for record in self._records.values():
            if record.operation is not None and not record.operation.done():
                if record.snapshot.state not in _TERMINAL:
                    self._transition(record, WorkState.CANCELLING, reason="SERVICE_SHUTDOWN")
                record.control.cancelled.set()
                pending.append(record.operation)
        if pending:
            _, unsettled = await asyncio.wait(pending, timeout=self._cancel_timeout + 0.1)
            for record in self._records.values():
                if record.operation in unsettled and record.snapshot.state is not WorkState.UNKNOWN:
                    self._transition(
                        record,
                        WorkState.UNKNOWN,
                        reason="SERVICE_SHUTDOWN_UNSETTLED",
                        execution_settled=False,
                    )
        return tuple(record.snapshot for record in self._records.values())
