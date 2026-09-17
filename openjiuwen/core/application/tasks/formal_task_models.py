# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Formal P3-alpha task records shared by the persistent Core and Executor.

These records are deliberately independent from the legacy schedule JSON rows.
They contain only stable, non-secret facts that the formal Task Core can persist.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import Any
from urllib.parse import unquote, urlparse

from openjiuwen.core.application.tasks.contracts import (
    MAX_SAFE_INTEGER,
    Assurance,
    CommandEnvelope,
    ContractViolation,
    ErrorCode,
    OriginRef,
    ResultEnvelope,
    ScopeRef,
    TerminalOutcome,
    canonical_json_bytes,
)
from openjiuwen.core.application.tasks.source import TaskSourceEvidence, source_extension, source_from_payload


class FormalTaskViolation(ValueError):
    def __init__(self, reason: str, message: str, code: ErrorCode) -> None:
        super().__init__(message)
        self.reason = reason
        self.code = code


class FormalTaskState(StrEnum):
    ACCEPTED = "accepted"
    RUNNING = "running"
    BLOCKED = "blocked"
    DECISION_REQUIRED = "decision_required"
    TERMINAL = "terminal"


class FormalAttemptState(StrEnum):
    ACCEPTED = "accepted"
    RUNNING = "running"
    TERMINAL = "terminal"


class OutboxKind(StrEnum):
    ATTEMPT_DISPATCH = "attempt.dispatch"
    ATTEMPT_CANCEL = "attempt.cancel"
    ATTEMPT_ADJUST = "attempt.adjust"


class OutboxState(StrEnum):
    PENDING = "pending"
    CLAIMED = "claimed"
    DELIVERED = "delivered"
    SUPPRESSED = "suppressed"


class AdmissionPriority(StrEnum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


class AdmissionDisposition(StrEnum):
    DEFERRED = "deferred"
    TIMED_OUT = "timed_out"
    RECONCILIATION_REQUIRED = "reconciliation_required"


class ReconciliationState(StrEnum):
    REQUIRED = "required"
    IN_PROGRESS = "in_progress"
    PENDING = "pending"
    RESOLVED = "resolved"


class TaskMutationDisposition(StrEnum):
    APPLIED = "applied"
    NOOP = "noop"
    SUPERSEDED = "superseded"


class ExecutorResolution(StrEnum):
    KNOWN = "known"
    UNAVAILABLE = "unavailable"
    LOST = "lost"


class TaskResultAvailability(StrEnum):
    AVAILABLE = "available"
    NOT_READY = "not_ready"
    UNAVAILABLE = "unavailable"


class TaskAdjustmentState(StrEnum):
    PENDING = "pending"
    APPLIED = "applied"
    REJECTED = "rejected"


class TaskCommandDisposition(StrEnum):
    ACCEPTED = "accepted"
    APPLIED = "applied"
    REJECTED = "rejected"
    UNSUPPORTED = "unsupported"
    CONFLICT = "conflict"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


_MAX_TASK_ADJUSTMENT_BYTES = 4096
_TASK_ADJUSTMENT_REASON_ALPHABET = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")


def canonical_task_adjustment_rejection_reason(value: object) -> str:
    if (
        type(value) is str
        and 0 < len(value) <= 128
        and all(character in _TASK_ADJUSTMENT_REASON_ALPHABET for character in value)
    ):
        return value
    return "TASK_ADJUSTMENT_RESULT_INVALID"


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _require_text(value: object, field_name: str) -> str:
    if type(value) is not str or not value.strip():
        raise FormalTaskViolation(
            "INVALID_FORMAL_TASK_FIELD",
            f"{field_name} must be a non-empty string",
            ErrorCode.INVALID_ARGUMENT,
        )
    return value


def _utf8_size(value: str, field_name: str) -> int:
    try:
        return len(value.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise FormalTaskViolation(
            "INVALID_FORMAL_TASK_FIELD",
            f"{field_name} must contain valid Unicode scalar values",
            ErrorCode.INVALID_ARGUMENT,
        ) from exc


def _parse_utc(value: object, field_name: str) -> datetime:
    if type(value) is not str:
        raise FormalTaskViolation(
            "INVALID_FORMAL_TASK_TIMESTAMP",
            f"{field_name} must be an RFC3339 timestamp",
            ErrorCode.INVALID_ARGUMENT,
        )
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise FormalTaskViolation(
            "INVALID_FORMAL_TASK_TIMESTAMP",
            f"{field_name} must be an RFC3339 timestamp",
            ErrorCode.INVALID_ARGUMENT,
        ) from exc
    if parsed.tzinfo is None:
        raise FormalTaskViolation(
            "INVALID_FORMAL_TASK_TIMESTAMP",
            f"{field_name} must include a timezone",
            ErrorCode.INVALID_ARGUMENT,
        )
    return parsed.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class AdmissionPolicy:
    """Runtime admission bounds; only derived absolute facts are persisted."""

    deadline_seconds: float = 3_600
    initial_backoff_seconds: float = 1
    max_backoff_seconds: float = 60
    max_attempts: int = 120

    def __post_init__(self) -> None:
        for field_name, value in (
            ("deadline_seconds", self.deadline_seconds),
            ("initial_backoff_seconds", self.initial_backoff_seconds),
            ("max_backoff_seconds", self.max_backoff_seconds),
        ):
            if type(value) not in {int, float} or value <= 0:
                raise FormalTaskViolation(
                    "INVALID_ADMISSION_POLICY",
                    f"admission policy {field_name} must be positive",
                    ErrorCode.INVALID_ARGUMENT,
                )
            try:
                if not math.isfinite(value):
                    raise ValueError("admission policy bound must be finite")
                duration = timedelta(seconds=value)
            except (OverflowError, TypeError, ValueError) as error:
                raise FormalTaskViolation(
                    "INVALID_ADMISSION_POLICY",
                    f"admission policy {field_name} is not representable",
                    ErrorCode.INVALID_ARGUMENT,
                ) from error
            if duration <= timedelta(0):
                raise FormalTaskViolation(
                    "INVALID_ADMISSION_POLICY",
                    f"admission policy {field_name} is below timestamp precision",
                    ErrorCode.INVALID_ARGUMENT,
                )
        if self.max_backoff_seconds < self.initial_backoff_seconds:
            raise FormalTaskViolation(
                "INVALID_ADMISSION_POLICY",
                "admission policy maximum backoff cannot be below its initial value",
                ErrorCode.INVALID_ARGUMENT,
            )
        if type(self.max_attempts) is not int or self.max_attempts <= 0:
            raise FormalTaskViolation(
                "INVALID_ADMISSION_POLICY",
                "admission policy max_attempts must be a positive integer",
                ErrorCode.INVALID_ARGUMENT,
            )


@dataclass(frozen=True, slots=True)
class PersistedExecutorSelection:
    """Executor-independent canonical selection facts owned by Core/Store."""

    adapter_id: str
    capability_profile_json: bytes
    capability_profile_digest: str
    execution_requirements_json: bytes
    admission_priority: AdmissionPriority = AdmissionPriority.NORMAL

    def __post_init__(self) -> None:
        adapter_id = _require_text(self.adapter_id, "executor_selection.adapter_id")
        if (
            "\x00" in adapter_id
            or len(adapter_id) > 256
            or _utf8_size(adapter_id, "executor_selection.adapter_id") > 1_024
        ):
            raise FormalTaskViolation(
                "INVALID_EXECUTOR_SELECTION",
                "executor selection adapter identity exceeds its closed bound",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        try:
            priority = AdmissionPriority(self.admission_priority)
        except (TypeError, ValueError) as error:
            raise FormalTaskViolation(
                "INVALID_ADMISSION_PRIORITY",
                "executor selection priority must be low, normal, high, or urgent",
                ErrorCode.INVALID_ARGUMENT,
            ) from error
        object.__setattr__(self, "admission_priority", priority)
        for field_name, encoded in (
            ("capability_profile_json", self.capability_profile_json),
            ("execution_requirements_json", self.execution_requirements_json),
        ):
            if type(encoded) is not bytes or not encoded or len(encoded) > 262_144:
                raise FormalTaskViolation(
                    "INVALID_EXECUTOR_SELECTION",
                    f"executor selection {field_name} must be bounded UTF-8 bytes",
                    ErrorCode.INVALID_ARGUMENT,
                )
            try:
                decoded = encoded.decode("utf-8")
                value = json.loads(decoded)
                canonical = canonical_json_bytes(value)
            except (
                UnicodeDecodeError,
                json.JSONDecodeError,
                ContractViolation,
            ) as error:
                raise FormalTaskViolation(
                    "INVALID_EXECUTOR_SELECTION",
                    f"executor selection {field_name} is not canonical JSON",
                    ErrorCode.INVALID_ARGUMENT,
                ) from error
            if type(value) is not dict or canonical != encoded:
                raise FormalTaskViolation(
                    "EXECUTOR_SELECTION_JSON_NOT_CANONICAL",
                    f"executor selection {field_name} must use exact canonical JSON",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
        digest = self.capability_profile_digest
        if (
            type(digest) is not str
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
            or hashlib.sha256(self.capability_profile_json).hexdigest() != digest
        ):
            raise FormalTaskViolation(
                "EXECUTOR_SELECTION_DIGEST_MISMATCH",
                "executor selection digest does not match its canonical profile",
                ErrorCode.PROTOCOL_VIOLATION,
            )

    @classmethod
    def from_values(
        cls,
        *,
        adapter_id: str,
        capability_profile: Mapping[str, object],
        execution_requirements: Mapping[str, object],
        admission_priority: AdmissionPriority | str = AdmissionPriority.NORMAL,
    ) -> PersistedExecutorSelection:
        profile = canonical_json_bytes(dict(capability_profile))
        requirements = canonical_json_bytes(dict(execution_requirements))
        return cls(
            adapter_id=adapter_id,
            capability_profile_json=profile,
            capability_profile_digest=hashlib.sha256(profile).hexdigest(),
            execution_requirements_json=requirements,
            admission_priority=admission_priority,
        )


@dataclass(frozen=True, slots=True)
class PersistentAdmissionRecord:
    task_id: str
    attempt_id: str
    priority: AdmissionPriority
    reason: str | None
    attempt_count: int
    next_eligible_at: str
    deadline_at: str
    enqueued_at: str
    queued: bool
    reconciliation_required: bool = False
    reconciliation_reason: str | None = None
    manual_action: str | None = None

    def __post_init__(self) -> None:
        _require_text(self.task_id, "admission.task_id")
        _require_text(self.attempt_id, "admission.attempt_id")
        if not isinstance(self.priority, AdmissionPriority):
            raise FormalTaskViolation(
                "INVALID_ADMISSION_PRIORITY",
                "persisted admission priority is not canonical",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if self.reason not in {
            None,
            "EXECUTOR_PROJECT_BUSY",
            "EXECUTOR_CAPACITY_EXHAUSTED",
        }:
            raise FormalTaskViolation(
                "INVALID_ADMISSION_REASON",
                "persisted admission reason is not a closed pre-effect defer reason",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if type(self.attempt_count) is not int or self.attempt_count < 0:
            raise FormalTaskViolation(
                "INVALID_ADMISSION_ATTEMPT_COUNT",
                "persisted admission attempt count must be non-negative",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if (self.attempt_count == 0) != (self.reason is None):
            raise FormalTaskViolation(
                "INVALID_ADMISSION_HISTORY",
                "persisted admission reason must exactly prove deferred deliveries",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        next_eligible = _parse_utc(self.next_eligible_at, "admission.next_eligible_at")
        deadline = _parse_utc(self.deadline_at, "admission.deadline_at")
        enqueued = _parse_utc(self.enqueued_at, "admission.enqueued_at")
        if next_eligible < enqueued or deadline < enqueued:
            raise FormalTaskViolation(
                "INVALID_ADMISSION_TIMELINE",
                "persisted admission times precede immutable enqueue time",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if type(self.queued) is not bool:
            raise FormalTaskViolation(
                "INVALID_ADMISSION_PROJECTION",
                "admission queued projection must be boolean",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if type(self.reconciliation_required) is not bool or (
            self.reconciliation_required != (self.reconciliation_reason is not None)
        ):
            raise FormalTaskViolation(
                "INVALID_ADMISSION_PROJECTION",
                "admission reconciliation projection is incomplete",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if self.reconciliation_required:
            _require_text(
                self.reconciliation_reason,
                "admission.reconciliation_reason",
            )
            if len(self.reconciliation_reason) > 1_000 or self.manual_action != "verify_external_ownership_and_settle":
                raise FormalTaskViolation(
                    "INVALID_ADMISSION_PROJECTION",
                    "admission reconciliation requires one bounded manual action",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
        elif self.manual_action is not None:
            raise FormalTaskViolation(
                "INVALID_ADMISSION_PROJECTION",
                "healthy admission cannot request manual settlement",
                ErrorCode.PROTOCOL_VIOLATION,
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "task_id": self.task_id,
            "attempt_id": self.attempt_id,
            "queued": self.queued,
            "priority": self.priority.value,
            "reason": self.reason,
            "attempt_count": self.attempt_count,
            "next_eligible_at": self.next_eligible_at,
            "deadline_at": self.deadline_at,
            "enqueued_at": self.enqueued_at,
            "reconciliation_required": self.reconciliation_required,
            "reconciliation_reason": self.reconciliation_reason,
            "manual_action": self.manual_action,
        }


def command_result_extensions(
    disposition: TaskCommandDisposition,
    *,
    admission_event_id: str | None = None,
    settlement_event_id: str | None = None,
) -> dict[str, object]:
    """Return the closed command result carrier without command content."""

    if not isinstance(disposition, TaskCommandDisposition):
        raise FormalTaskViolation(
            "INVALID_TASK_COMMAND_DISPOSITION",
            "task command disposition is not canonical",
            ErrorCode.INVALID_ARGUMENT,
        )
    for field_name, event_id in (
        ("admission_event_id", admission_event_id),
        ("settlement_event_id", settlement_event_id),
    ):
        if event_id is not None:
            _require_text(event_id, f"command_result.{field_name}")
    return {
        "live_voice.command": {
            "disposition": disposition.value,
            "admission_event_id": admission_event_id,
            "settlement_event_id": settlement_event_id,
        }
    }


@dataclass(frozen=True, slots=True)
class ResolvedTaskContext:
    """Server-resolved execution resource; it is not an authorization grant."""

    source: str
    stable_id: str
    uri: str
    revision_kind: str
    revision_value: str | None
    scope: ScopeRef
    permissions: tuple[str, ...]
    expires_at: str | None
    redaction_policy_id: str
    redacted: bool = False
    redacted_fields: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_text(self.source, "context.source")
        _require_text(self.stable_id, "context.stable_id")
        uri = _require_text(self.uri, "context.uri")
        parsed = urlparse(uri)
        if not parsed.scheme:
            raise FormalTaskViolation(
                "INVALID_TASK_CONTEXT_URI",
                "context.uri must be absolute",
                ErrorCode.INVALID_ARGUMENT,
            )
        if not isinstance(self.scope, ScopeRef):
            raise FormalTaskViolation(
                "INVALID_TASK_CONTEXT_SCOPE",
                "context scope must be a ScopeRef",
                ErrorCode.INVALID_ARGUMENT,
            )
        if self.revision_kind not in {"version", "snapshot", "unversioned"}:
            raise FormalTaskViolation(
                "INVALID_TASK_CONTEXT_REVISION",
                "context revision kind is unsupported",
                ErrorCode.INVALID_ARGUMENT,
            )
        if self.revision_kind == "unversioned":
            if self.revision_value is not None:
                raise FormalTaskViolation(
                    "INVALID_TASK_CONTEXT_REVISION",
                    "unversioned context forbids a revision value",
                    ErrorCode.INVALID_ARGUMENT,
                )
        else:
            _require_text(self.revision_value, "context.revision_value")
        if self.scope.assurance is not Assurance.AUTHENTICATED:
            raise FormalTaskViolation(
                "TASK_CONTEXT_NOT_AUTHENTICATED",
                "formal task context requires authenticated scope",
                ErrorCode.UNAUTHENTICATED,
            )
        if (
            type(self.permissions) is not tuple
            or len(set(self.permissions)) != len(self.permissions)
            or any(type(item) is not str or not item.strip() for item in self.permissions)
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_CONTEXT_PERMISSIONS",
                "context permissions must be unique non-empty strings",
                ErrorCode.INVALID_ARGUMENT,
            )
        object.__setattr__(self, "permissions", tuple(sorted(self.permissions)))
        if self.expires_at is not None and type(self.expires_at) is not str:
            raise FormalTaskViolation(
                "INVALID_FORMAL_TASK_TIMESTAMP",
                "context.expires_at must be a timestamp or null",
                ErrorCode.INVALID_ARGUMENT,
            )
        if self.expires_at is not None:
            _parse_utc(self.expires_at, "context.expires_at")
        _require_text(self.redaction_policy_id, "context.redaction_policy_id")
        if type(self.redacted) is not bool:
            raise FormalTaskViolation(
                "INVALID_TASK_CONTEXT_REDACTION",
                "context redacted flag must be boolean",
                ErrorCode.INVALID_ARGUMENT,
            )
        if (
            type(self.redacted_fields) is not tuple
            or len(set(self.redacted_fields)) != len(self.redacted_fields)
            or any(type(item) is not str or not item.strip() for item in self.redacted_fields)
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_CONTEXT_REDACTION",
                "redacted field names must be unique non-empty strings",
                ErrorCode.INVALID_ARGUMENT,
            )
        object.__setattr__(self, "redacted_fields", tuple(sorted(self.redacted_fields)))

    @property
    def file_path(self) -> str | None:
        parsed = urlparse(self.uri)
        if parsed.scheme != "file":
            return None
        path = unquote(parsed.path)
        if parsed.netloc:
            return f"//{parsed.netloc}{path}"
        if len(path) >= 3 and path[0] == "/" and path[2] == ":":
            return path[1:]
        return path

    def require_usable(
        self,
        *,
        scope: ScopeRef,
        required_permissions: frozenset[str],
        destructive: bool,
        now: str,
    ) -> None:
        if self.scope != scope:
            raise FormalTaskViolation(
                "TASK_CONTEXT_SCOPE_MISMATCH",
                "resolved context does not match the exact task scope",
                ErrorCode.PERMISSION_DENIED,
            )
        if self.expires_at is not None and _parse_utc(self.expires_at, "context.expires_at") <= _parse_utc(now, "now"):
            raise FormalTaskViolation(
                "TASK_CONTEXT_EXPIRED",
                "resolved task context has expired",
                ErrorCode.PERMISSION_DENIED,
            )
        if self.redacted or self.redacted_fields:
            raise FormalTaskViolation(
                "TASK_CONTEXT_REDACTED",
                "redacted context cannot authorize task execution",
                ErrorCode.PERMISSION_DENIED,
            )
        if destructive and self.revision_kind == "unversioned":
            raise FormalTaskViolation(
                "UNVERSIONED_DESTRUCTIVE_CONTEXT",
                "destructive task execution requires a versioned context",
                ErrorCode.PERMISSION_DENIED,
            )
        if not required_permissions.issubset(self.permissions):
            raise FormalTaskViolation(
                "TASK_CONTEXT_PERMISSION_MISSING",
                "resolved context lacks a required permission",
                ErrorCode.PERMISSION_DENIED,
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "stable_id": self.stable_id,
            "uri": self.uri,
            "revision": {
                "kind": self.revision_kind,
                "value": self.revision_value,
            },
            "scope": self.scope.to_dict(),
            "permissions": list(self.permissions),
            "expires_at": self.expires_at,
            "redaction": {
                "policy_id": self.redaction_policy_id,
                "redacted": self.redacted,
                "fields": list(self.redacted_fields),
            },
        }

    @classmethod
    def from_dict(cls, payload: object) -> ResolvedTaskContext:
        if type(payload) is not dict:
            raise FormalTaskViolation(
                "INVALID_TASK_CONTEXT",
                "resolved context must be an object",
                ErrorCode.INVALID_ARGUMENT,
            )
        expected = {
            "source",
            "stable_id",
            "uri",
            "revision",
            "scope",
            "permissions",
            "expires_at",
            "redaction",
        }
        if set(payload) != expected:
            raise FormalTaskViolation(
                "INVALID_TASK_CONTEXT",
                "resolved context fields are incomplete or unknown",
                ErrorCode.INVALID_ARGUMENT,
            )
        revision = payload["revision"]
        redaction = payload["redaction"]
        if type(revision) is not dict or set(revision) != {"kind", "value"}:
            raise FormalTaskViolation(
                "INVALID_TASK_CONTEXT_REVISION",
                "context revision is malformed",
                ErrorCode.INVALID_ARGUMENT,
            )
        if type(redaction) is not dict or set(redaction) != {
            "policy_id",
            "redacted",
            "fields",
        }:
            raise FormalTaskViolation(
                "INVALID_TASK_CONTEXT_REDACTION",
                "context redaction is malformed",
                ErrorCode.INVALID_ARGUMENT,
            )
        permissions = payload["permissions"]
        redacted_fields = redaction["fields"]
        if type(permissions) is not list or type(redacted_fields) is not list:
            raise FormalTaskViolation(
                "INVALID_TASK_CONTEXT",
                "context permissions and redacted fields must be arrays",
                ErrorCode.INVALID_ARGUMENT,
            )
        return cls(
            source=_require_text(payload["source"], "context.source"),
            stable_id=_require_text(payload["stable_id"], "context.stable_id"),
            uri=_require_text(payload["uri"], "context.uri"),
            revision_kind=_require_text(revision["kind"], "context.revision.kind"),
            revision_value=revision["value"],
            scope=ScopeRef.from_dict(payload["scope"]),
            permissions=tuple(permissions),
            expires_at=payload["expires_at"],
            redaction_policy_id=_require_text(redaction["policy_id"], "context.redaction.policy_id"),
            redacted=redaction["redacted"],
            redacted_fields=tuple(redacted_fields),
        )


@dataclass(frozen=True, slots=True)
class TaskAuthorizationGrant:
    """Trusted, out-of-band decision bound to one exact Core invocation."""

    principal_id: str
    scope: ScopeRef
    operation: str
    command_id: str | None
    target_task_id: str | None
    allowed_capabilities: frozenset[str]
    confirmation_id: str | None
    confirmed: bool
    expires_at: str
    policy_bypass: str | None = None

    def __post_init__(self) -> None:
        _require_text(self.principal_id, "authorization.principal_id")
        if not isinstance(self.scope, ScopeRef):
            raise FormalTaskViolation(
                "INVALID_FORMAL_TASK_AUTHORIZATION",
                "authorization scope must be a ScopeRef",
                ErrorCode.INVALID_ARGUMENT,
            )
        _require_text(self.operation, "authorization.operation")
        for field_name, value in (
            ("authorization.command_id", self.command_id),
            ("authorization.target_task_id", self.target_task_id),
            ("authorization.confirmation_id", self.confirmation_id),
            ("authorization.policy_bypass", self.policy_bypass),
        ):
            if value is not None:
                _require_text(value, field_name)
        if type(self.confirmed) is not bool:
            raise FormalTaskViolation(
                "INVALID_FORMAL_TASK_AUTHORIZATION",
                "authorization confirmed flag must be boolean",
                ErrorCode.INVALID_ARGUMENT,
            )
        if self.policy_bypass not in {
            None,
            "server_task_presentation_v1",
        }:
            raise FormalTaskViolation(
                "INVALID_FORMAL_TASK_AUTHORIZATION",
                "authorization policy bypass is unsupported",
                ErrorCode.INVALID_ARGUMENT,
            )
        if self.policy_bypass is not None and (self.confirmed or self.confirmation_id is not None):
            raise FormalTaskViolation(
                "INVALID_FORMAL_TASK_AUTHORIZATION",
                "policy bypass cannot impersonate user confirmation",
                ErrorCode.INVALID_ARGUMENT,
            )
        if self.policy_bypass == "server_task_presentation_v1" and self.operation != "task.ack_events":
            raise FormalTaskViolation(
                "INVALID_FORMAL_TASK_AUTHORIZATION",
                "server presentation authority is limited to task.ack_events",
                ErrorCode.INVALID_ARGUMENT,
            )
        if type(self.allowed_capabilities) is not frozenset or any(
            type(capability) is not str or not capability.strip() for capability in self.allowed_capabilities
        ):
            raise FormalTaskViolation(
                "INVALID_FORMAL_TASK_AUTHORIZATION",
                "authorization capabilities must be non-empty strings",
                ErrorCode.INVALID_ARGUMENT,
            )
        _parse_utc(self.expires_at, "authorization.expires_at")

    def authorize(
        self,
        *,
        scope: ScopeRef,
        operation: str,
        command_id: str | None,
        target_task_id: str | None,
        required_capabilities: frozenset[str],
        destructive: bool,
        now: str,
    ) -> None:
        if self.scope.assurance is not Assurance.AUTHENTICATED or scope.assurance is not Assurance.AUTHENTICATED:
            raise FormalTaskViolation(
                "FORMAL_TASK_AUTHENTICATION_REQUIRED",
                "formal task mutations require authenticated scope",
                ErrorCode.UNAUTHENTICATED,
            )
        if (
            not self.principal_id.strip()
            or self.principal_id != scope.subject_id
            or self.scope != scope
            or self.operation != operation
            or self.command_id != command_id
            or self.target_task_id != target_task_id
        ):
            raise FormalTaskViolation(
                "FORMAL_TASK_AUTHORIZATION_DENIED",
                "authorization does not bind the exact task invocation",
                ErrorCode.PERMISSION_DENIED,
            )
        if not required_capabilities.issubset(self.allowed_capabilities):
            raise FormalTaskViolation(
                "FORMAL_TASK_CAPABILITY_DENIED",
                "authorization does not grant every required capability",
                ErrorCode.PERMISSION_DENIED,
            )
        if _parse_utc(self.expires_at, "authorization.expires_at") <= _parse_utc(now, "now"):
            raise FormalTaskViolation(
                "FORMAL_TASK_AUTHORIZATION_EXPIRED",
                "authorization has expired",
                ErrorCode.PERMISSION_DENIED,
            )
        confirmed_boundary = self.confirmed and self.confirmation_id is not None
        bypass_boundary = (
            not self.confirmed
            and self.confirmation_id is None
            and self.policy_bypass == "server_task_presentation_v1"
            and operation == "task.ack_events"
        )
        if destructive and not (confirmed_boundary or bypass_boundary):
            raise FormalTaskViolation(
                "FORMAL_TASK_CONFIRMATION_REQUIRED",
                "destructive task operation requires confirmation or trusted policy",
                ErrorCode.PERMISSION_DENIED,
            )


@dataclass(frozen=True, slots=True)
class FormalTaskSpec:
    name: str
    instruction: str
    origin: OriginRef
    context: ResolvedTaskContext
    executor_id: str
    required_capabilities: tuple[str, ...]
    side_effect_class: str
    constraints: tuple[str, ...] = ()
    attributes: tuple[tuple[str, str], ...] = ()
    native_source: TaskSourceEvidence | None = None

    def __post_init__(self) -> None:
        _require_text(self.name, "task.name")
        _require_text(self.instruction, "task.instruction")
        _require_text(self.executor_id, "task.executor_id")
        if not isinstance(self.origin, OriginRef):
            raise FormalTaskViolation(
                "INVALID_FORMAL_TASK_ORIGIN",
                "task origin must be a committed-turn or structured OriginRef",
                ErrorCode.INVALID_ARGUMENT,
            )
        try:
            normalized_origin = OriginRef.from_dict(self.origin.to_dict())
        except ContractViolation as error:
            raise FormalTaskViolation(
                "INVALID_FORMAL_TASK_ORIGIN",
                str(error),
                error.code,
            ) from error
        object.__setattr__(self, "origin", normalized_origin)
        if not isinstance(self.context, ResolvedTaskContext):
            raise FormalTaskViolation(
                "INVALID_FORMAL_TASK_CONTEXT",
                "task context must be server-resolved",
                ErrorCode.INVALID_ARGUMENT,
            )
        if self.side_effect_class not in {"read_only", "project_mutation"}:
            raise FormalTaskViolation(
                "INVALID_TASK_SIDE_EFFECT_CLASS",
                "task side-effect class is unsupported",
                ErrorCode.INVALID_ARGUMENT,
            )
        if (
            type(self.required_capabilities) is not tuple
            or len(set(self.required_capabilities)) != len(self.required_capabilities)
            or any(type(capability) is not str or not capability.strip() for capability in self.required_capabilities)
        ):
            raise FormalTaskViolation(
                "DUPLICATE_TASK_CAPABILITY",
                "required task capabilities must be unique non-empty strings",
                ErrorCode.INVALID_ARGUMENT,
            )
        object.__setattr__(
            self,
            "required_capabilities",
            tuple(sorted(self.required_capabilities)),
        )
        if type(self.constraints) is not tuple or any(
            type(constraint) is not str or not constraint.strip() for constraint in self.constraints
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_CONSTRAINTS",
                "task constraints must be unique non-empty strings",
                ErrorCode.INVALID_ARGUMENT,
            )
        if len(set(self.constraints)) != len(self.constraints):
            raise FormalTaskViolation(
                "INVALID_TASK_CONSTRAINTS",
                "task constraints must be unique non-empty strings",
                ErrorCode.INVALID_ARGUMENT,
            )
        constraint_sizes = tuple(_utf8_size(constraint, "task.constraint") for constraint in self.constraints)
        if (
            len(self.constraints) > 16
            or any(
                "\x00" in constraint or size > 1_024
                for constraint, size in zip(self.constraints, constraint_sizes, strict=True)
            )
            or sum(constraint_sizes) > 4_096
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_CONSTRAINTS",
                "task constraints exceed their closed UTF-8 bounds",
                ErrorCode.INVALID_ARGUMENT,
            )
        if type(self.attributes) is not tuple:
            raise FormalTaskViolation(
                "INVALID_TASK_ATTRIBUTES",
                "task attributes must be an immutable tuple",
                ErrorCode.INVALID_ARGUMENT,
            )
        if any(type(item) is not tuple or len(item) != 2 for item in self.attributes):
            raise FormalTaskViolation(
                "INVALID_TASK_ATTRIBUTES",
                "task attributes must contain key/value pairs",
                ErrorCode.INVALID_ARGUMENT,
            )
        keys = [item[0] for item in self.attributes]
        if len(set(keys)) != len(keys) or any(
            type(key) is not str or not key.strip() or type(value) is not str or not value.strip()
            for key, value in self.attributes
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_ATTRIBUTES",
                "task attributes must have unique non-empty string keys and values",
                ErrorCode.INVALID_ARGUMENT,
            )
        object.__setattr__(self, "attributes", tuple(sorted(self.attributes)))
        if self.native_source is not None:
            if not isinstance(self.native_source, TaskSourceEvidence) or self.native_source.operation not in {
                "task.create",
                "task.create_successor",
            }:
                raise ValueError("NATIVE_TASK_SOURCE_SPEC_INVALID")
            if self.native_source.scope != self.context.scope:
                raise ValueError("NATIVE_TASK_SOURCE_SPEC_SCOPE_MISMATCH")

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "instruction": self.instruction,
            "origin": self.origin.to_dict(),
            "context": self.context.to_dict(),
            "executor_id": self.executor_id,
            "required_capabilities": list(self.required_capabilities),
            "side_effect_class": self.side_effect_class,
            "constraints": list(self.constraints),
            "attributes": dict(self.attributes),
            **source_extension(self.native_source),
        }

    @classmethod
    def from_dict(cls, payload: object) -> FormalTaskSpec:
        required_fields = {
            "name",
            "instruction",
            "origin",
            "context",
            "executor_id",
            "required_capabilities",
            "side_effect_class",
            "attributes",
        }
        payload_fields = set(payload) if type(payload) is dict else set()
        if type(payload) is not dict or payload_fields not in (
            required_fields,
            required_fields | {"constraints"},
            required_fields | {"native_source"},
            required_fields | {"constraints", "native_source"},
        ):
            raise FormalTaskViolation(
                "INVALID_FORMAL_TASK_SPEC",
                "formal task spec fields are incomplete or unknown",
                ErrorCode.INVALID_ARGUMENT,
            )
        capabilities = payload["required_capabilities"]
        attributes = payload["attributes"]
        constraints = payload.get("constraints", [])
        if type(capabilities) is not list or type(constraints) is not list or type(attributes) is not dict:
            raise FormalTaskViolation(
                "INVALID_FORMAL_TASK_SPEC",
                "task capabilities and attributes have invalid types",
                ErrorCode.INVALID_ARGUMENT,
            )
        try:
            origin = OriginRef.from_dict(payload["origin"])
        except ContractViolation as error:
            raise FormalTaskViolation(
                "INVALID_FORMAL_TASK_ORIGIN",
                str(error),
                error.code,
            ) from error
        return cls(
            name=_require_text(payload["name"], "task.name"),
            instruction=_require_text(payload["instruction"], "task.instruction"),
            origin=origin,
            context=ResolvedTaskContext.from_dict(payload["context"]),
            executor_id=_require_text(payload["executor_id"], "task.executor_id"),
            required_capabilities=tuple(capabilities),
            side_effect_class=_require_text(payload["side_effect_class"], "task.side_effect_class"),
            constraints=tuple(constraints),
            attributes=tuple(sorted(attributes.items())),
            native_source=source_from_payload(payload),
        )

    def fingerprint_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())


@dataclass(frozen=True, slots=True)
class PersistentTaskRecord:
    task_id: str
    scope: ScopeRef
    spec: FormalTaskSpec
    state: FormalTaskState
    attempt_id: str
    correlation_id: str
    cancel_requested: bool
    dispatch_fenced: bool
    outcome: TerminalOutcome | None
    reconciliation_state: ReconciliationState | None
    reconciliation_reason: str | None
    create_command_id: str
    predecessor_task_id: str | None
    revision_number: int
    event_head: int = 0

    def __post_init__(self) -> None:
        for field_name, value in (
            ("task.task_id", self.task_id),
            ("task.create_command_id", self.create_command_id),
        ):
            identity = _require_text(value, field_name)
            if "\x00" in identity or len(identity) > 256 or _utf8_size(identity, field_name) > 1_024:
                raise FormalTaskViolation(
                    "INVALID_FORMAL_TASK_IDENTITY",
                    "formal Task identity exceeds its closed bound",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
        if self.predecessor_task_id is not None:
            predecessor = _require_text(self.predecessor_task_id, "task.predecessor_task_id")
            if (
                predecessor == self.task_id
                or "\x00" in predecessor
                or len(predecessor) > 256
                or _utf8_size(predecessor, "task.predecessor_task_id") > 1_024
            ):
                raise FormalTaskViolation(
                    "INVALID_TASK_REVISION_LINEAGE",
                    "formal Task predecessor identity is invalid",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
        if (
            type(self.revision_number) is not int
            or self.revision_number < 1
            or self.revision_number > 1_000_000
            or (self.revision_number == 1) != (self.predecessor_task_id is None)
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_REVISION_LINEAGE",
                "formal Task revision must bind one canonical predecessor chain",
                ErrorCode.PROTOCOL_VIOLATION,
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "task_id": self.task_id,
            "scope": self.scope.to_dict(),
            "spec": self.spec.to_dict(),
            "state": self.state.value,
            "attempt_id": self.attempt_id,
            "correlation_id": self.correlation_id,
            "cancel_requested": self.cancel_requested,
            "dispatch_fenced": self.dispatch_fenced,
            "outcome": None if self.outcome is None else self.outcome.value,
            "reconciliation": (
                None
                if self.reconciliation_state is None
                else {
                    "state": self.reconciliation_state.value,
                    "reason": self.reconciliation_reason,
                }
            ),
            "revision": {
                "number": self.revision_number,
                "predecessor_task_id": self.predecessor_task_id,
                "create_command_id": self.create_command_id,
            },
            "event_head": self.event_head,
        }


@dataclass(frozen=True, slots=True)
class PersistentAttemptRecord:
    attempt_id: str
    task_id: str
    executor_id: str
    executor_ref: str | None
    state: FormalAttemptState
    outcome: TerminalOutcome | None
    source_seq: int
    attempt_number: int = 1
    selection: PersistedExecutorSelection | None = None

    def __post_init__(self) -> None:
        if type(self.attempt_number) is not int or not 1 <= self.attempt_number <= 3:
            raise FormalTaskViolation(
                "INVALID_TASK_ATTEMPT_NUMBER",
                "formal task attempt_number must be between 1 and 3",
                ErrorCode.PROTOCOL_VIOLATION,
            )

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "attempt_id": self.attempt_id,
            "task_id": self.task_id,
            "executor_id": self.executor_id,
            "executor_ref": self.executor_ref,
            "state": self.state.value,
            "outcome": None if self.outcome is None else self.outcome.value,
            "source_seq": self.source_seq,
            "attempt_number": self.attempt_number,
        }
        if self.selection is not None:
            payload["executor_selection"] = {
                "adapter_id": self.selection.adapter_id,
                "capability_profile": json.loads(self.selection.capability_profile_json),
                "capability_profile_digest": (self.selection.capability_profile_digest),
                "execution_requirements": json.loads(self.selection.execution_requirements_json),
                "admission_priority": self.selection.admission_priority.value,
            }
        return payload


@dataclass(frozen=True, slots=True)
class TaskResultArtifact:
    relative_path: str
    sha256: str

    def __post_init__(self) -> None:
        path = _require_text(self.relative_path, "task_result.relative_path")
        if (
            len(path) > 512
            or _utf8_size(path, "task_result.relative_path") > 2_048
            or "\\" in path
            or path.startswith("/")
            or path.startswith("./")
            or any(part in {"", ".", ".."} for part in path.split("/"))
            or ":" in path.split("/", 1)[0]
            or "\x00" in path
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_RESULT_ARTIFACT",
                "task result artifact must be one normalized relative path",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if (
            type(self.sha256) is not str
            or len(self.sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.sha256)
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_RESULT_ARTIFACT",
                "task result artifact requires canonical SHA-256",
                ErrorCode.PROTOCOL_VIOLATION,
            )

    def to_dict(self) -> dict[str, str]:
        return {"relative_path": self.relative_path, "sha256": self.sha256}


@dataclass(frozen=True, slots=True)
class TaskResultRecord:
    task_id: str
    attempt_id: str
    source_event_id: str
    result_text: str
    artifacts: tuple[TaskResultArtifact, ...]
    completed_at: str

    def __post_init__(self) -> None:
        for field_name, value in (
            ("task_result.task_id", self.task_id),
            ("task_result.attempt_id", self.attempt_id),
            ("task_result.source_event_id", self.source_event_id),
        ):
            identity = _require_text(value, field_name)
            if "\x00" in identity or len(identity) > 256 or _utf8_size(identity, field_name) > 1_024:
                raise FormalTaskViolation(
                    "INVALID_TASK_RESULT_IDENTITY",
                    "task result identity exceeds its closed bound",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
        text = _require_text(self.result_text, "task_result.result_text")
        if "\x00" in text or len(text) > 32_768 or _utf8_size(text, "task_result.result_text") > 131_072:
            raise FormalTaskViolation(
                ("INVALID_TASK_RESULT_TEXT" if "\x00" in text else "TASK_RESULT_TOO_LARGE"),
                "task result text is invalid or exceeds its closed bound",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if (
            type(self.artifacts) is not tuple
            or not self.artifacts
            or len(self.artifacts) > 32
            or any(not isinstance(item, TaskResultArtifact) for item in self.artifacts)
            or len({item.relative_path for item in self.artifacts}) != len(self.artifacts)
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_RESULT_ARTIFACT",
                "task result artifacts are invalid or duplicated",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        completed = _parse_utc(self.completed_at, "task_result.completed_at")
        object.__setattr__(
            self,
            "completed_at",
            completed.isoformat().replace("+00:00", "Z"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "task_id": self.task_id,
            "attempt_id": self.attempt_id,
            "source_event_id": self.source_event_id,
            "result_text": self.result_text,
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "completed_at": self.completed_at,
        }


TASK_RETRY_PRODUCT_REQUEST_EXTENSION = "jiuwenswarm.task_retry_product_request"


@dataclass(frozen=True, slots=True)
class TaskRetryProductRequestFingerprint:
    """Opaque product-owned retry request identity persisted with the command.

    The product computes this SHA-256 from its immutable request/confirmation
    facts.  It must exclude the transport ``request_id`` and every server-derived
    predecessor, context, readiness, or checkout fact.  Core deliberately does
    not interpret those product facts; it only retains and compares their exact
    canonical digest during durable replay.
    """

    sha256: str

    def __post_init__(self) -> None:
        if (
            type(self.sha256) is not str
            or len(self.sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.sha256)
        ):
            raise FormalTaskViolation(
                "TASK_RETRY_PRODUCT_REQUEST_FINGERPRINT_INVALID",
                "task.retry product request fingerprint must be canonical SHA-256",
                ErrorCode.INVALID_ARGUMENT,
            )

    def to_extensions(self) -> dict[str, object]:
        return {
            TASK_RETRY_PRODUCT_REQUEST_EXTENSION: {"sha256": self.sha256},
        }

    @classmethod
    def from_extensions(cls, extensions: Mapping[str, object]) -> TaskRetryProductRequestFingerprint:
        binding = extensions.get(TASK_RETRY_PRODUCT_REQUEST_EXTENSION)
        if set(extensions) != {TASK_RETRY_PRODUCT_REQUEST_EXTENSION} or not isinstance(binding, Mapping):
            raise FormalTaskViolation(
                "TASK_RETRY_PRODUCT_REQUEST_FINGERPRINT_REQUIRED",
                "task.retry requires one product-owned request fingerprint",
                ErrorCode.INVALID_ARGUMENT,
            )
        if set(binding) != {"sha256"}:
            raise FormalTaskViolation(
                "TASK_RETRY_PRODUCT_REQUEST_FINGERPRINT_INVALID",
                "task.retry product request fingerprint extension is not canonical",
                ErrorCode.INVALID_ARGUMENT,
            )
        return cls(binding["sha256"])  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class TaskRetryPrecondition:
    """Exact server-verified predecessor facts carried by ``task.retry``."""

    previous_attempt_id: str
    previous_outcome: TerminalOutcome
    attempt_number: int

    def __post_init__(self) -> None:
        _require_text(self.previous_attempt_id, "task.retry.previous_attempt_id")
        if self.previous_outcome not in {
            TerminalOutcome.CANCELLED,
            TerminalOutcome.COMPLETED,
        }:
            raise FormalTaskViolation(
                "TASK_RETRY_OUTCOME_NOT_ELIGIBLE",
                "only cancelled or completed attempts can be retried",
                ErrorCode.CONFLICT,
            )
        if type(self.attempt_number) is not int or self.attempt_number not in {2, 3}:
            raise FormalTaskViolation(
                "TASK_RETRY_ATTEMPT_NUMBER_INVALID",
                "task.retry attempt_number must be 2 or 3",
                ErrorCode.INVALID_ARGUMENT,
            )

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> TaskRetryPrecondition:
        if set(payload) != {
            "previous_attempt_id",
            "previous_outcome",
            "attempt_number",
        }:
            raise FormalTaskViolation(
                "TASK_RETRY_PRECONDITION_INVALID",
                "task.retry requires one exact predecessor lineage",
                ErrorCode.INVALID_ARGUMENT,
            )
        try:
            outcome = TerminalOutcome(payload["previous_outcome"])
        except (TypeError, ValueError) as error:
            raise FormalTaskViolation(
                "TASK_RETRY_PRECONDITION_INVALID",
                "task.retry previous_outcome is invalid",
                ErrorCode.INVALID_ARGUMENT,
            ) from error
        return cls(
            previous_attempt_id=_require_text(payload["previous_attempt_id"], "task.retry.previous_attempt_id"),
            previous_outcome=outcome,
            attempt_number=payload["attempt_number"],  # type: ignore[arg-type]
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "previous_attempt_id": self.previous_attempt_id,
            "previous_outcome": self.previous_outcome.value,
            "attempt_number": self.attempt_number,
        }


@dataclass(frozen=True, slots=True)
class TaskMutationPrecondition:
    """Trusted exact Task/Attempt/head facts for one targeted mutation.

    This value is deliberately out-of-band from the client command payload.
    Product composition derives it from freshly reread Store authority, while
    the Store compares it inside the same transaction that would append the
    mutation event or outbox item.
    """

    task_id: str
    attempt_id: str
    expected_event_head: int

    def __post_init__(self) -> None:
        for field_name, value in (
            ("task.mutation_precondition.task_id", self.task_id),
            ("task.mutation_precondition.attempt_id", self.attempt_id),
        ):
            identity = _require_text(value, field_name)
            if "\x00" in identity or len(identity) > 256 or _utf8_size(identity, field_name) > 1_024:
                raise FormalTaskViolation(
                    "TASK_MUTATION_PRECONDITION_INVALID",
                    "task mutation precondition identity exceeds its closed bound",
                    ErrorCode.INVALID_ARGUMENT,
                )
        if (
            type(self.expected_event_head) is not int
            or self.expected_event_head < 0
            or self.expected_event_head > MAX_SAFE_INTEGER
        ):
            raise FormalTaskViolation(
                "TASK_MUTATION_PRECONDITION_INVALID",
                "task mutation precondition event head is outside its closed bound",
                ErrorCode.INVALID_ARGUMENT,
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "task_id": self.task_id,
            "attempt_id": self.attempt_id,
            "expected_event_head": self.expected_event_head,
        }

    @classmethod
    def from_dict(cls, payload: object) -> TaskMutationPrecondition:
        if type(payload) is not dict or set(payload) != {
            "task_id",
            "attempt_id",
            "expected_event_head",
        }:
            raise FormalTaskViolation(
                "TASK_MUTATION_PRECONDITION_INVALID",
                "task mutation precondition is not canonical",
                ErrorCode.INVALID_ARGUMENT,
            )
        return cls(
            task_id=payload["task_id"],  # type: ignore[arg-type]
            attempt_id=payload["attempt_id"],  # type: ignore[arg-type]
            expected_event_head=payload["expected_event_head"],  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class TaskRetryAuthoritySnapshot:
    """Read-only retry admission snapshot; Store must re-check it when applying."""

    task: PersistentTaskRecord
    attempt: PersistentAttemptRecord
    precondition: TaskRetryPrecondition

    def __post_init__(self) -> None:
        if (
            self.task.attempt_id != self.attempt.attempt_id
            or self.task.task_id != self.attempt.task_id
            or self.task.state is not FormalTaskState.TERMINAL
            or self.task.outcome != self.attempt.outcome
            or self.attempt.state is not FormalAttemptState.TERMINAL
            or self.precondition.previous_attempt_id != self.attempt.attempt_id
            or self.precondition.previous_outcome != self.attempt.outcome
            or self.precondition.attempt_number != self.attempt.attempt_number + 1
        ):
            raise FormalTaskViolation(
                "TASK_RETRY_PRECONDITION_STALE",
                "retry snapshot does not bind the exact current terminal attempt",
                ErrorCode.STALE,
            )


@dataclass(frozen=True, slots=True)
class DurableRecoveryAuthoritySnapshot:
    """Store-read authority facts for D1/D2; never a user retry request."""

    task: PersistentTaskRecord
    producer_attempt: PersistentAttemptRecord
    recovery_generation: int
    recovery_budget_remaining: int

    def __post_init__(self) -> None:
        if (
            self.task.task_id != self.producer_attempt.task_id
            or self.task.attempt_id != self.producer_attempt.attempt_id
            or self.task.state is not FormalTaskState.TERMINAL
            or self.producer_attempt.state is not FormalAttemptState.TERMINAL
            or self.task.outcome is not TerminalOutcome.INTERRUPTED
            or self.producer_attempt.outcome is not TerminalOutcome.INTERRUPTED
            or self.recovery_generation != self.producer_attempt.attempt_number
            or self.recovery_budget_remaining != 3 - self.producer_attempt.attempt_number
            or self.recovery_budget_remaining <= 0
        ):
            raise FormalTaskViolation(
                "TASK_RECOVERY_PRECONDITION_STALE",
                "recovery snapshot does not bind one interrupted producer and budget",
                ErrorCode.STALE,
            )


@dataclass(frozen=True, slots=True)
class AppliedTaskRetryReplay:
    """Read-only durable proof of one previously applied ``task.retry``."""

    original_command: CommandEnvelope
    original_result: ResultEnvelope
    precondition: TaskRetryPrecondition
    resulting_spec: FormalTaskSpec

    def __post_init__(self) -> None:
        result = self.original_result.result
        if (
            self.original_command.command_type != "task.retry"
            or not self.original_result.ok
            or self.original_result.command_id != self.original_command.command_id
            or result is None
            or result.get("task_id") != self.original_command.target_ref.id
            or result.get("previous_attempt_id") != self.precondition.previous_attempt_id
            or result.get("attempt_number") != self.precondition.attempt_number
            or result.get("state") != FormalTaskState.ACCEPTED.value
            or result.get("applied") is not True
            or self.resulting_spec.context.scope != self.original_command.scope
        ):
            raise FormalTaskViolation(
                "TASK_RETRY_REPLAY_BINDING_MISMATCH",
                "applied retry replay does not bind one durable successor",
                ErrorCode.PROTOCOL_VIOLATION,
            )


@dataclass(frozen=True, slots=True)
class ExecutorRetryReadiness:
    """Executor-owned proof that predecessor cleanup reached a retry-safe state."""

    task_id: str
    previous_attempt_id: str
    previous_outcome: TerminalOutcome
    previous_attempt_number: int
    ready: bool
    reason: str

    def __post_init__(self) -> None:
        _require_text(self.task_id, "retry_readiness.task_id")
        _require_text(self.previous_attempt_id, "retry_readiness.previous_attempt_id")
        if not isinstance(self.previous_outcome, TerminalOutcome):
            raise FormalTaskViolation(
                "INVALID_RETRY_READINESS",
                "retry readiness requires a canonical predecessor outcome",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if (
            type(self.previous_attempt_number) is not int
            or not 1 <= self.previous_attempt_number <= 2
            or type(self.ready) is not bool
        ):
            raise FormalTaskViolation(
                "INVALID_RETRY_READINESS",
                "retry readiness facts are invalid",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        _require_text(self.reason, "retry_readiness.reason")


@dataclass(frozen=True, slots=True)
class PersistentTaskEvent:
    event_id: str
    task_id: str
    attempt_id: str
    scope: ScopeRef
    seq: int
    event_type: str
    state: str
    outcome: str | None
    producer: str
    source_event_id: str | None
    causation_id: str
    correlation_id: str
    occurred_at: str
    details: Mapping[str, object]

    def __post_init__(self) -> None:
        for field_name, value in (
            ("event.event_id", self.event_id),
            ("event.task_id", self.task_id),
            ("event.attempt_id", self.attempt_id),
            ("event.event_type", self.event_type),
            ("event.state", self.state),
            ("event.producer", self.producer),
            ("event.causation_id", self.causation_id),
            ("event.correlation_id", self.correlation_id),
        ):
            _require_text(value, field_name)
        if not isinstance(self.scope, ScopeRef):
            raise FormalTaskViolation(
                "INVALID_TASK_EVENT_SCOPE",
                "task event scope must be an exact ScopeRef",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        try:
            normalized_scope = ScopeRef.from_dict(self.scope.to_dict())
        except ContractViolation as error:
            raise FormalTaskViolation(
                "INVALID_TASK_EVENT_SCOPE",
                str(error),
                error.code,
            ) from error
        if normalized_scope.assurance is not Assurance.AUTHENTICATED:
            raise FormalTaskViolation(
                "INVALID_TASK_EVENT_SCOPE",
                "formal task events require authenticated scope",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        object.__setattr__(self, "scope", normalized_scope)
        if type(self.seq) is not int or self.seq < 0:
            raise FormalTaskViolation(
                "INVALID_TASK_EVENT_SEQUENCE",
                "task event sequence must be a non-negative integer",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        _parse_utc(self.occurred_at, "event.occurred_at")
        allowed_states = {state.value for state in FormalTaskState}
        if self.state not in allowed_states:
            raise FormalTaskViolation(
                "INVALID_TASK_EVENT_STATE",
                "task event state is outside the canonical lifecycle vocabulary",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if self.outcome is not None:
            _require_text(self.outcome, "event.outcome")
            try:
                TerminalOutcome(self.outcome)
            except ValueError as error:
                raise FormalTaskViolation(
                    "INVALID_TASK_EVENT_OUTCOME",
                    "task event outcome is outside the canonical terminal vocabulary",
                    ErrorCode.PROTOCOL_VIOLATION,
                ) from error
        if (self.state == FormalTaskState.TERMINAL.value) != (self.outcome is not None):
            raise FormalTaskViolation(
                "INVALID_TASK_EVENT_OUTCOME",
                "terminal task events require an outcome and nonterminal events forbid it",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if self.source_event_id is not None:
            _require_text(self.source_event_id, "event.source_event_id")
        if not isinstance(self.details, Mapping):
            raise FormalTaskViolation(
                "INVALID_TASK_EVENT_DETAILS",
                "task event details must be an object",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if any(
            type(key) is not str or not key.strip() or type(value) not in {str, int, bool, type(None)}
            for key, value in self.details.items()
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_EVENT_DETAILS",
                "task event details must contain only scalar JSON facts",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        object.__setattr__(self, "details", MappingProxyType(dict(self.details)))

    def to_dict(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "task_id": self.task_id,
            "attempt_id": self.attempt_id,
            "scope": self.scope.to_dict(),
            "seq": self.seq,
            "event_type": self.event_type,
            "state": self.state,
            "outcome": self.outcome,
            "producer": self.producer,
            "source_event_id": self.source_event_id,
            "causation_id": self.causation_id,
            "correlation_id": self.correlation_id,
            "occurred_at": self.occurred_at,
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class TaskUnreadPage:
    """One prefix-bounded unread page against a frozen consumer snapshot."""

    task_id: str
    presentation_class: str
    watermark: int
    acked_event_id: str | None
    head_seq: int
    events: tuple[PersistentTaskEvent, ...]
    next_after_seq: int | None
    has_more: bool

    def __post_init__(self) -> None:
        _require_text(self.task_id, "task_unread.task_id")
        if type(self.presentation_class) is not str or self.presentation_class not in {
            "text",
            "voice",
        }:
            raise FormalTaskViolation(
                "INVALID_PRESENTATION_CLASS",
                "task unread presentation class must be text or voice",
                ErrorCode.INVALID_ARGUMENT,
            )
        if (
            type(self.watermark) is not int
            or self.watermark < -1
            or type(self.head_seq) is not int
            or self.head_seq < 0
            or self.watermark > self.head_seq
            or type(self.events) is not tuple
            or len(self.events) > 500
            or any(not isinstance(event, PersistentTaskEvent) for event in self.events)
            or type(self.has_more) is not bool
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_UNREAD_PAGE",
                "task unread page bounds are not canonical",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if self.watermark == -1:
            if self.acked_event_id is not None:
                raise FormalTaskViolation(
                    "INVALID_TASK_UNREAD_PAGE",
                    "logical initial watermark cannot bind an event",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
        else:
            _require_text(self.acked_event_id, "task_unread.acked_event_id")
        for expected_seq, event in enumerate(self.events, self.watermark + 1):
            if event.task_id != self.task_id or event.seq != expected_seq:
                raise FormalTaskViolation(
                    "INVALID_TASK_UNREAD_PAGE",
                    "task unread page is not one contiguous Task prefix",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
        if self.has_more:
            if not self.events or self.next_after_seq != self.events[-1].seq or self.events[-1].seq >= self.head_seq:
                raise FormalTaskViolation(
                    "INVALID_TASK_UNREAD_PAGE",
                    "truncated task unread page lacks its next prefix position",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
        elif (
            self.next_after_seq is not None
            or (self.events and self.events[-1].seq != self.head_seq)
            or (not self.events and self.watermark != self.head_seq)
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_UNREAD_PAGE",
                "complete task unread page does not reach its frozen head",
                ErrorCode.PROTOCOL_VIOLATION,
            )

    @property
    def acked_through_seq(self) -> int:
        return self.watermark

    def to_dict(self) -> dict[str, object]:
        return {
            "task_id": self.task_id,
            "presentation_class": self.presentation_class,
            "watermark": self.watermark,
            "acked_event_id": self.acked_event_id,
            "head_seq": self.head_seq,
            "events": [event.to_dict() for event in self.events],
            "next_after_seq": self.next_after_seq,
            "has_more": self.has_more,
        }


@dataclass(frozen=True, slots=True)
class TaskEventConsumerCursorBaseline:
    """Store-owned lifecycle/Attempt facts at one durable consumer watermark."""

    task_id: str
    scope: ScopeRef
    correlation_id: str
    watermark: int
    acked_event_id: str | None
    lifecycle_event: PersistentTaskEvent | None
    attempt_boundary: PersistentTaskEvent | None

    def __post_init__(self) -> None:
        _require_text(self.task_id, "task_event_consumer_cursor.task_id")
        if (
            not isinstance(self.scope, ScopeRef)
            or self.scope.assurance is not Assurance.AUTHENTICATED
            or self.scope.project_id is None
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_EVENT_CONSUMER_CURSOR_BASELINE",
                "consumer cursor requires one authenticated project scope",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        _require_text(
            self.correlation_id,
            "task_event_consumer_cursor.correlation_id",
        )
        if type(self.watermark) is not int or self.watermark < -1:
            raise FormalTaskViolation(
                "INVALID_TASK_EVENT_CONSUMER_CURSOR_BASELINE",
                "consumer cursor watermark is not canonical",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if self.watermark == -1:
            if self.acked_event_id is not None or self.lifecycle_event is not None or self.attempt_boundary is not None:
                raise FormalTaskViolation(
                    "INVALID_TASK_EVENT_CONSUMER_CURSOR_BASELINE",
                    "initial consumer cursor cannot carry prior lifecycle facts",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
            return
        _require_text(
            self.acked_event_id,
            "task_event_consumer_cursor.acked_event_id",
        )
        lifecycle = self.lifecycle_event
        boundary = self.attempt_boundary
        if (
            not isinstance(lifecycle, PersistentTaskEvent)
            or not isinstance(boundary, PersistentTaskEvent)
            or not 0 <= boundary.seq <= lifecycle.seq <= self.watermark
            or lifecycle.task_id != boundary.task_id
            or lifecycle.task_id != self.task_id
            or lifecycle.attempt_id != boundary.attempt_id
            or lifecycle.scope != boundary.scope
            or lifecycle.scope != self.scope
            or lifecycle.correlation_id != boundary.correlation_id
            or lifecycle.correlation_id != self.correlation_id
            or lifecycle.event_type
            not in {
                "task.accepted",
                "task.retry_accepted",
                "task.recovery_accepted",
                "task.running",
                "task.blocked",
                "task.decision_required",
                "task.terminal",
            }
            or boundary.event_type
            not in {
                "task.accepted",
                "task.retry_accepted",
                "task.recovery_accepted",
            }
            or boundary.state != FormalTaskState.ACCEPTED.value
            or boundary.outcome is not None
            or boundary.producer != "task_core"
            or boundary.source_event_id is not None
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_EVENT_CONSUMER_CURSOR_BASELINE",
                "consumer cursor lacks one exact lifecycle and Attempt boundary",
                ErrorCode.PROTOCOL_VIOLATION,
            )


@dataclass(frozen=True, slots=True)
class TaskEventConsumerAuthorityPage:
    """One Store-verified consumer cursor and bounded Task-wide suffix."""

    task: PersistentTaskRecord
    attempt: PersistentAttemptRecord
    presentation_class: str
    watermark: int
    acked_event_id: str | None
    cursor_baseline: TaskEventConsumerCursorBaseline
    segment_start_seq: int
    page_after_seq: int
    start_seq: int
    head_seq: int
    events: tuple[PersistentTaskEvent, ...]
    next_after_seq: int | None
    has_more: bool
    terminal_head_event: PersistentTaskEvent | None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.task, PersistentTaskRecord)
            or not isinstance(self.attempt, PersistentAttemptRecord)
            or self.attempt.task_id != self.task.task_id
            or self.attempt.attempt_id != self.task.attempt_id
            or self.presentation_class not in {"text", "voice"}
            or not isinstance(self.cursor_baseline, TaskEventConsumerCursorBaseline)
            or self.cursor_baseline.watermark != self.watermark
            or self.cursor_baseline.acked_event_id != self.acked_event_id
            or self.cursor_baseline.task_id != self.task.task_id
            or self.cursor_baseline.scope != self.task.scope
            or self.cursor_baseline.correlation_id != self.task.correlation_id
            or type(self.watermark) is not int
            or self.watermark < -1
            or type(self.segment_start_seq) is not int
            or type(self.page_after_seq) is not int
            or type(self.start_seq) is not int
            or type(self.head_seq) is not int
            or not 0 <= self.segment_start_seq <= self.head_seq + 1
            or self.watermark > self.head_seq
            or not max(self.segment_start_seq - 1, self.watermark) <= self.page_after_seq <= self.head_seq
            or self.start_seq != self.page_after_seq + 1
            or not self.segment_start_seq <= self.start_seq <= self.head_seq + 1
            or self.task.event_head < self.head_seq
            or type(self.events) is not tuple
            or len(self.events) > 500
            or any(not isinstance(event, PersistentTaskEvent) for event in self.events)
            or type(self.has_more) is not bool
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_EVENT_CONSUMER_AUTHORITY_PAGE",
                "consumer TaskEvent authority page is not canonical",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        cursor_lifecycle = self.cursor_baseline.lifecycle_event
        cursor_boundary = self.cursor_baseline.attempt_boundary
        if self.watermark >= 0 and (
            cursor_lifecycle is None
            or cursor_boundary is None
            or cursor_lifecycle.task_id != self.task.task_id
            or cursor_lifecycle.scope != self.task.scope
            or cursor_lifecycle.correlation_id != self.task.correlation_id
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_EVENT_CONSUMER_AUTHORITY_PAGE",
                "consumer cursor baseline crosses its Task authority",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        terminal = self.terminal_head_event
        if self.task.state is FormalTaskState.TERMINAL:
            if (
                not isinstance(terminal, PersistentTaskEvent)
                or terminal.event_type != "task.terminal"
                or terminal.seq != self.task.event_head
                or terminal.task_id != self.task.task_id
                or terminal.attempt_id != self.task.attempt_id
                or terminal.scope != self.task.scope
                or terminal.correlation_id != self.task.correlation_id
                or terminal.state != FormalTaskState.TERMINAL.value
                or terminal.outcome != (None if self.task.outcome is None else self.task.outcome.value)
            ):
                raise FormalTaskViolation(
                    "INVALID_TASK_EVENT_CONSUMER_AUTHORITY_PAGE",
                    "terminal Task page lacks its canonical head event",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
        elif terminal is not None:
            raise FormalTaskViolation(
                "INVALID_TASK_EVENT_CONSUMER_AUTHORITY_PAGE",
                "nonterminal Task page cannot carry terminal-head authority",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if self.watermark == -1:
            if self.acked_event_id is not None:
                raise FormalTaskViolation(
                    "INVALID_TASK_EVENT_CONSUMER_AUTHORITY_PAGE",
                    "initial consumer cursor cannot bind an acknowledged event",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
        else:
            _require_text(
                self.acked_event_id,
                "task_event_consumer_authority.acked_event_id",
            )
        for expected_seq, event in enumerate(self.events, self.start_seq):
            if (
                event.seq != expected_seq
                or event.task_id != self.task.task_id
                or event.scope != self.task.scope
                or event.correlation_id != self.task.correlation_id
            ):
                raise FormalTaskViolation(
                    "INVALID_TASK_EVENT_CONSUMER_AUTHORITY_PAGE",
                    "consumer TaskEvent page is not one exact Task suffix",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
        if self.has_more:
            if not self.events or self.next_after_seq != self.events[-1].seq or self.events[-1].seq >= self.head_seq:
                raise FormalTaskViolation(
                    "INVALID_TASK_EVENT_CONSUMER_AUTHORITY_PAGE",
                    "truncated consumer TaskEvent page lacks its exact cursor",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
        elif (
            self.next_after_seq is not None
            or (self.events and self.events[-1].seq != self.head_seq)
            or (not self.events and self.start_seq != self.head_seq + 1)
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_EVENT_CONSUMER_AUTHORITY_PAGE",
                "complete consumer TaskEvent page does not reach its frozen head",
                ErrorCode.PROTOCOL_VIOLATION,
            )


@dataclass(frozen=True, slots=True)
class TaskMutationResult:
    """Atomic mutation receipt pinned to one durable attempt epoch."""

    disposition: TaskMutationDisposition
    task_id: str
    attempt: PersistentAttemptRecord
    through_seq: int
    events: tuple[PersistentTaskEvent, ...] = ()

    def __post_init__(self) -> None:
        _require_text(self.task_id, "task_mutation.task_id")
        if (
            not isinstance(self.disposition, TaskMutationDisposition)
            or not isinstance(self.attempt, PersistentAttemptRecord)
            or self.attempt.task_id != self.task_id
            or type(self.through_seq) is not int
            or self.through_seq < 0
            or type(self.events) is not tuple
            or any(not isinstance(event, PersistentTaskEvent) for event in self.events)
            or any(
                event.task_id != self.task_id
                or event.attempt_id != self.attempt.attempt_id
                or event.seq > self.through_seq
                for event in self.events
            )
            or (self.disposition is not TaskMutationDisposition.APPLIED and bool(self.events))
        ):
            raise FormalTaskViolation(
                "TASK_MUTATION_RECEIPT_INVALID",
                "task mutation receipt does not bind one durable attempt epoch",
                ErrorCode.PROTOCOL_VIOLATION,
            )


@dataclass(frozen=True, slots=True)
class TaskEventAuthoritySnapshot:
    """One exact durable TaskEvent prefix and its authority-owned cursor."""

    task: PersistentTaskRecord
    attempt: PersistentAttemptRecord
    events: tuple[PersistentTaskEvent, ...]
    cursor: int
    start_seq: int = 0

    def __post_init__(self) -> None:
        if (
            not isinstance(self.task, PersistentTaskRecord)
            or not isinstance(self.attempt, PersistentAttemptRecord)
            or type(self.events) is not tuple
            or any(not isinstance(event, PersistentTaskEvent) for event in self.events)
            or type(self.cursor) is not int
            or self.cursor < 0
            or type(self.start_seq) is not int
            or self.start_seq < 0
            or self.start_seq > self.cursor
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_EVENT_AUTHORITY_SNAPSHOT",
                "TaskEvent authority snapshot is not canonical",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if (
            self.cursor != self.task.event_head
            or self.attempt.task_id != self.task.task_id
            or self.attempt.attempt_id != self.task.attempt_id
            or self.attempt.executor_id != self.task.spec.executor_id
            or len(self.events) != self.cursor - self.start_seq + 1
        ):
            raise FormalTaskViolation(
                "TASK_EVENT_AUTHORITY_SNAPSHOT_BINDING_MISMATCH",
                "TaskEvent authority snapshot does not bind one exact task revision",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        for expected_seq, event in enumerate(self.events, self.start_seq):
            if (
                event.seq != expected_seq
                or event.task_id != self.task.task_id
                or event.attempt_id != self.task.attempt_id
                or event.scope != self.task.scope
                or event.correlation_id != self.task.correlation_id
            ):
                raise FormalTaskViolation(
                    "TASK_EVENT_AUTHORITY_SNAPSHOT_SEQUENCE_MISMATCH",
                    "TaskEvent authority snapshot is not one contiguous canonical prefix",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
        boundary = self.events[0]
        expected_boundaries = (
            {"task.accepted"} if self.attempt.attempt_number == 1 else {"task.retry_accepted", "task.recovery_accepted"}
        )
        if boundary.event_type not in expected_boundaries:
            raise FormalTaskViolation(
                "TASK_EVENT_AUTHORITY_SEGMENT_BOUNDARY_MISMATCH",
                "TaskEvent authority segment lacks its canonical attempt boundary",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if self.attempt.attempt_number == 1:
            if self.start_seq != 0:
                raise FormalTaskViolation(
                    "TASK_EVENT_AUTHORITY_SEGMENT_BOUNDARY_MISMATCH",
                    "initial attempt segment must begin at global sequence zero",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
        else:
            details = boundary.details
            is_recovery = boundary.event_type == "task.recovery_accepted"
            predecessor_key = "producer_attempt_id" if is_recovery else "retry_of_attempt_id"
            outcome_key = "producer_outcome" if is_recovery else "previous_outcome"
            authority_key = "recovery_id" if is_recovery else "command_id"
            predecessor_attempt_id = details.get(predecessor_key)
            expected_keys = {
                authority_key,
                predecessor_key,
                outcome_key,
                "attempt_number",
            }
            if is_recovery:
                expected_keys.update({"recovery_generation", "recovery_budget_remaining"})
            if (
                set(details) != expected_keys
                or details.get(authority_key) != boundary.causation_id
                or details.get("attempt_number") != self.attempt.attempt_number
                or type(predecessor_attempt_id) is not str
                or not predecessor_attempt_id.strip()
                or predecessor_attempt_id == self.attempt.attempt_id
                or details.get(outcome_key)
                not in (
                    {TerminalOutcome.INTERRUPTED.value}
                    if is_recovery
                    else {
                        TerminalOutcome.CANCELLED.value,
                        TerminalOutcome.COMPLETED.value,
                    }
                )
                or (
                    is_recovery
                    and (
                        details.get("recovery_generation") != self.attempt.attempt_number - 1
                        or details.get("recovery_budget_remaining") != 3 - self.attempt.attempt_number
                    )
                )
            ):
                raise FormalTaskViolation(
                    "TASK_EVENT_AUTHORITY_SEGMENT_BOUNDARY_MISMATCH",
                    "successor segment boundary does not bind exact predecessor lineage",
                    ErrorCode.PROTOCOL_VIOLATION,
                )


@dataclass(frozen=True, slots=True)
class TaskAdjustmentRequest:
    """One bounded untrusted adjustment bound to its durable request event."""

    adjustment_id: str
    adjustment: str
    requested_seq: int
    native_source: TaskSourceEvidence | None = None

    def __post_init__(self) -> None:
        _require_text(self.adjustment_id, "task_adjustment.adjustment_id")
        _require_text(self.adjustment, "task_adjustment.adjustment")
        if self.native_source is not None:
            if not isinstance(self.native_source, TaskSourceEvidence):
                raise ValueError("NATIVE_TASK_SOURCE_ADJUSTMENT_INVALID")
            self.native_source.require_request(
                scope=self.native_source.scope, operation="task.adjust", instruction=self.adjustment
            )
        if (
            "\x00" in self.adjustment
            or _utf8_size(self.adjustment, "task_adjustment.adjustment") > _MAX_TASK_ADJUSTMENT_BYTES
        ):
            raise FormalTaskViolation(
                "TASK_ADJUSTMENT_INVALID",
                "task adjustment must be bounded text without NUL characters",
                ErrorCode.INVALID_ARGUMENT,
            )
        if type(self.requested_seq) is not int or self.requested_seq < 1:
            raise FormalTaskViolation(
                "TASK_ADJUSTMENT_SEQUENCE_INVALID",
                "task adjustment must bind a positive durable event sequence",
                ErrorCode.PROTOCOL_VIOLATION,
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "adjustment_id": self.adjustment_id,
            "adjustment": self.adjustment,
            "requested_seq": self.requested_seq,
            **source_extension(self.native_source),
        }

    @classmethod
    def from_dict(cls, payload: object) -> TaskAdjustmentRequest:
        if type(payload) is not dict or set(payload) - {"native_source"} != {
            "adjustment_id",
            "adjustment",
            "requested_seq",
        }:
            raise FormalTaskViolation(
                "TASK_ADJUSTMENT_INVALID",
                "task adjustment carrier is not canonical",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        return cls(
            adjustment_id=_require_text(payload["adjustment_id"], "task_adjustment.adjustment_id"),
            adjustment=_require_text(payload["adjustment"], "task_adjustment.adjustment"),
            requested_seq=payload["requested_seq"],
            native_source=source_from_payload(payload),
        )


@dataclass(frozen=True, slots=True)
class TaskAdjustmentDeliveryResult:
    """Executor checkpoint result; content never crosses this acknowledgement."""

    executor_ref: str
    adjustment_id: str
    state: TaskAdjustmentState
    reason: str | None = None

    def __post_init__(self) -> None:
        _require_text(self.executor_ref, "task_adjustment_result.executor_ref")
        _require_text(self.adjustment_id, "task_adjustment_result.adjustment_id")
        if self.state not in {
            TaskAdjustmentState.APPLIED,
            TaskAdjustmentState.REJECTED,
        }:
            raise FormalTaskViolation(
                "TASK_ADJUSTMENT_RESULT_INVALID",
                "Executor adjustment result must be applied or rejected",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if self.state is TaskAdjustmentState.APPLIED:
            if self.reason is not None:
                raise FormalTaskViolation(
                    "TASK_ADJUSTMENT_RESULT_INVALID",
                    "applied adjustment result cannot carry a rejection reason",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
        else:
            if canonical_task_adjustment_rejection_reason(self.reason) != self.reason:
                raise FormalTaskViolation(
                    "TASK_ADJUSTMENT_RESULT_INVALID",
                    "rejected adjustment result must carry a canonical reason",
                    ErrorCode.PROTOCOL_VIOLATION,
                )


@dataclass(frozen=True, slots=True)
class TaskAdjustmentSettlement:
    """Store-owned final state and terminal-fence continuation fact."""

    state: TaskAdjustmentState
    has_more: bool

    def __post_init__(self) -> None:
        if (
            self.state
            not in {
                TaskAdjustmentState.APPLIED,
                TaskAdjustmentState.REJECTED,
            }
            or type(self.has_more) is not bool
        ):
            raise FormalTaskViolation(
                "TASK_ADJUSTMENT_SETTLEMENT_INVALID",
                "task adjustment settlement is not canonical",
                ErrorCode.PROTOCOL_VIOLATION,
            )


@dataclass(frozen=True, slots=True)
class PersistentOutboxItem:
    outbox_id: str
    kind: OutboxKind
    task_id: str
    attempt_id: str
    command_id: str
    scope: ScopeRef
    spec: FormalTaskSpec
    executor_ref: str | None
    source_seq: int
    state: OutboxState
    delivery_count: int
    claim_token: str | None = None
    adjustment: TaskAdjustmentRequest | None = None
    selection: PersistedExecutorSelection | None = None
    admission: PersistentAdmissionRecord | None = None

    def __post_init__(self) -> None:
        if (self.kind is OutboxKind.ATTEMPT_ADJUST) != (self.adjustment is not None):
            raise FormalTaskViolation(
                "OUTBOX_ADJUSTMENT_BINDING_MISMATCH",
                "adjustment outbox kind and carrier must agree",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if self.adjustment is not None and self.adjustment.adjustment_id != self.command_id:
            raise FormalTaskViolation(
                "OUTBOX_ADJUSTMENT_BINDING_MISMATCH",
                "adjustment identity must equal the durable command identity",
                ErrorCode.PROTOCOL_VIOLATION,
            )


@dataclass(frozen=True, slots=True)
class ExecutorObservation:
    resolution: ExecutorResolution
    executor_id: str
    executor_ref: str | None
    task_id: str
    attempt_id: str
    source_event_id: str | None
    source_seq: int | None
    attempt_state: FormalAttemptState | None
    attempt_outcome: TerminalOutcome | None
    occurred_at: str
    raw_status: str | None
    summary: str | None = None
    error: str | None = None
    result_text: str | None = None
    result_artifacts: tuple[TaskResultArtifact, ...] = ()
    adapter_id: str | None = None
    capability_profile_digest: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.resolution, ExecutorResolution):
            raise FormalTaskViolation(
                "INVALID_EXECUTOR_RESOLUTION",
                "Executor resolution is unsupported",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        _require_text(self.executor_id, "executor_observation.executor_id")
        _require_text(self.task_id, "executor_observation.task_id")
        _require_text(self.attempt_id, "executor_observation.attempt_id")
        _parse_utc(self.occurred_at, "executor_observation.occurred_at")
        for field_name, value in (
            ("executor_observation.executor_ref", self.executor_ref),
            ("executor_observation.source_event_id", self.source_event_id),
            ("executor_observation.raw_status", self.raw_status),
            ("executor_observation.summary", self.summary),
            ("executor_observation.error", self.error),
            ("executor_observation.result_text", self.result_text),
            ("executor_observation.adapter_id", self.adapter_id),
            (
                "executor_observation.capability_profile_digest",
                self.capability_profile_digest,
            ),
        ):
            if value is not None:
                _require_text(value, field_name)
        if (self.adapter_id is None) != (self.capability_profile_digest is None):
            raise FormalTaskViolation(
                "EXECUTOR_SELECTION_BINDING_INCOMPLETE",
                "Executor callback selection binding must be all-null or complete",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if self.capability_profile_digest is not None and (
            len(self.capability_profile_digest) != 64
            or any(character not in "0123456789abcdef" for character in self.capability_profile_digest)
        ):
            raise FormalTaskViolation(
                "EXECUTOR_SELECTION_BINDING_INVALID",
                "Executor callback profile digest must be lowercase SHA-256",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if self.result_text is not None and (
            "\x00" in self.result_text
            or len(self.result_text) > 32_768
            or _utf8_size(self.result_text, "executor_observation.result_text") > 131_072
        ):
            raise FormalTaskViolation(
                ("INVALID_TASK_RESULT_TEXT" if "\x00" in self.result_text else "TASK_RESULT_TOO_LARGE"),
                "Executor result text is invalid or exceeds its closed bound",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if (
            type(self.result_artifacts) is not tuple
            or len(self.result_artifacts) > 32
            or any(not isinstance(artifact, TaskResultArtifact) for artifact in self.result_artifacts)
            or len({artifact.relative_path for artifact in self.result_artifacts}) != len(self.result_artifacts)
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_RESULT_ARTIFACT",
                "Executor result artifacts are invalid or duplicated",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        publishes_result = self.result_text is not None or bool(self.result_artifacts)
        if (self.result_text is None) != (not self.result_artifacts):
            raise FormalTaskViolation(
                "INVALID_TASK_RESULT_STATE",
                "Executor result text requires applied artifact evidence",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if publishes_result and not (
            self.resolution is ExecutorResolution.KNOWN
            and self.attempt_state is FormalAttemptState.TERMINAL
            and self.attempt_outcome is TerminalOutcome.COMPLETED
            and self.result_text is not None
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_RESULT_STATE",
                "only one completed Executor observation may publish a result",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if self.resolution is ExecutorResolution.KNOWN:
            if (
                self.executor_ref is None
                or self.source_event_id is None
                or type(self.source_seq) is not int
                or self.source_seq < 0
                or not isinstance(self.attempt_state, FormalAttemptState)
            ):
                raise FormalTaskViolation(
                    "EXECUTOR_EVENT_INCOMPLETE",
                    "known Executor observation lacks exact lifecycle evidence",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
            if self.attempt_state is FormalAttemptState.TERMINAL:
                if not isinstance(self.attempt_outcome, TerminalOutcome):
                    raise FormalTaskViolation(
                        "TERMINAL_OUTCOME_REQUIRED",
                        "terminal Executor observation requires an outcome",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
            elif self.attempt_outcome is not None:
                raise FormalTaskViolation(
                    "NONTERMINAL_OUTCOME_FORBIDDEN",
                    "nonterminal Executor observation cannot carry an outcome",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
        elif any(
            value is not None
            for value in (
                self.source_event_id,
                self.source_seq,
                self.attempt_state,
                self.attempt_outcome,
            )
        ):
            raise FormalTaskViolation(
                "INVALID_EXECUTOR_RESOLUTION",
                "unknown Executor resolution cannot carry lifecycle evidence",
                ErrorCode.PROTOCOL_VIOLATION,
            )

    def canonical_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "resolution": self.resolution.value,
            "executor_id": self.executor_id,
            "executor_ref": self.executor_ref,
            "task_id": self.task_id,
            "attempt_id": self.attempt_id,
            "source_event_id": self.source_event_id,
            "source_seq": self.source_seq,
            "attempt_state": (None if self.attempt_state is None else self.attempt_state.value),
            "attempt_outcome": (None if self.attempt_outcome is None else self.attempt_outcome.value),
            "occurred_at": self.occurred_at,
            "raw_status": self.raw_status,
            "summary": self.summary,
            "error": self.error,
            "result_text": self.result_text,
            "result_artifacts": [artifact.to_dict() for artifact in self.result_artifacts],
        }
        if self.adapter_id is not None:
            payload["adapter_id"] = self.adapter_id
            payload["capability_profile_digest"] = self.capability_profile_digest
        return payload


@dataclass(frozen=True, slots=True)
class ExecutorDeliveryResult:
    executor_ref: str
    observations: tuple[ExecutorObservation, ...]

    def __post_init__(self) -> None:
        _require_text(self.executor_ref, "executor_delivery.executor_ref")
        if type(self.observations) is not tuple or any(
            not isinstance(observation, ExecutorObservation)
            or observation.resolution is not ExecutorResolution.KNOWN
            or observation.executor_ref != self.executor_ref
            for observation in self.observations
        ):
            raise FormalTaskViolation(
                "EXECUTOR_DELIVERY_BINDING_MISMATCH",
                "delivery observations must be known facts for one executor reference",
                ErrorCode.PROTOCOL_VIOLATION,
            )


def require_exact_payload(payload: dict[str, object], expected: AbstractSet[str], *, field_name: str) -> None:
    if set(payload) != expected:
        missing = sorted(expected - set(payload))
        unknown = sorted(set(payload) - expected)
        detail = []
        if missing:
            detail.append("missing " + ", ".join(missing))
        if unknown:
            detail.append("unknown " + ", ".join(unknown))
        raise FormalTaskViolation(
            "INVALID_FORMAL_TASK_PAYLOAD",
            f"{field_name} has " + "; ".join(detail),
            ErrorCode.INVALID_ARGUMENT,
        )


def safe_json_value(value: Any) -> Any:
    """Return a JSON-only copy and reject custom values before persistence."""

    canonical_json_bytes(value)
    if type(value) is dict:
        return {key: safe_json_value(item) for key, item in value.items()}
    if type(value) is list:
        return [safe_json_value(item) for item in value]
    return value


__all__ = [
    "AdmissionDisposition",
    "AdmissionPolicy",
    "AdmissionPriority",
    "AppliedTaskRetryReplay",
    "DurableRecoveryAuthoritySnapshot",
    "ExecutorDeliveryResult",
    "ExecutorObservation",
    "ExecutorResolution",
    "ExecutorRetryReadiness",
    "FormalAttemptState",
    "FormalTaskSpec",
    "FormalTaskState",
    "FormalTaskViolation",
    "OutboxKind",
    "OutboxState",
    "PersistedExecutorSelection",
    "PersistentAdmissionRecord",
    "PersistentAttemptRecord",
    "PersistentOutboxItem",
    "PersistentTaskEvent",
    "PersistentTaskRecord",
    "ReconciliationState",
    "ResolvedTaskContext",
    "TaskAuthorizationGrant",
    "TaskAdjustmentDeliveryResult",
    "TaskAdjustmentRequest",
    "TaskAdjustmentSettlement",
    "TaskAdjustmentState",
    "TaskCommandDisposition",
    "TaskEventAuthoritySnapshot",
    "TaskEventConsumerAuthorityPage",
    "TaskEventConsumerCursorBaseline",
    "TaskMutationDisposition",
    "TaskMutationPrecondition",
    "TaskMutationResult",
    "TaskResultArtifact",
    "TaskResultAvailability",
    "TaskResultRecord",
    "TaskUnreadPage",
    "TaskRetryAuthoritySnapshot",
    "TaskRetryPrecondition",
    "TaskRetryProductRequestFingerprint",
    "TASK_RETRY_PRODUCT_REQUEST_EXTENSION",
    "canonical_task_adjustment_rejection_reason",
    "command_result_extensions",
    "require_exact_payload",
    "safe_json_value",
    "utc_now",
]
