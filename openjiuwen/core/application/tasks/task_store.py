# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""SQLite authority for formal P3 command/task/event/attempt state."""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TypeVar

from openjiuwen.core.application.tasks.contracts import (
    MAX_SAFE_INTEGER,
    Assurance,
    CommandEnvelope,
    ContractViolation,
    ErrorCode,
    LifecycleKind,
    ResultEnvelope,
    ScopeRef,
    TerminalOutcome,
    canonical_json_bytes,
    validate_transition,
)
from openjiuwen.core.application.tasks.durability.durability_authority import (
    DurabilityMutationAuthorization,
    _durability_authorization_payload_digest,
)
from openjiuwen.core.application.tasks.durability.durability_checkpoint import D1Checkpoint
from openjiuwen.core.application.tasks.durability.durability_effects import (
    EffectContinuationAuthorization,
    EffectFact,
    ExternalEffectDispatch,
    ExternalEffectObservation,
    ExternalEffectSettlement,
    effect_fact_bytes,
)
from openjiuwen.core.application.tasks.durability.durability_identity import DurabilityProfileBinding
from openjiuwen.core.application.tasks.durability.durability_readers import (
    CheckpointPrefixRow,
    DurabilityPrefixViolation,
    DurabilityReadBinding,
    EffectPrefixRow,
    VerifiedCheckpointPrefix,
    VerifiedEffectPrefix,
    verify_checkpoint_prefix,
    verify_effect_prefix,
)
from openjiuwen.core.application.tasks.durability.durability_recovery_facts import ExecutorRecoveryFacts
from openjiuwen.core.application.tasks.executor_capabilities import ExecutorCapabilityProfile
from openjiuwen.core.application.tasks.formal_task_models import (
    AdmissionDisposition,
    AdmissionPolicy,
    AdmissionPriority,
    AppliedTaskRetryReplay,
    DurableRecoveryAuthoritySnapshot,
    ExecutorObservation,
    ExecutorResolution,
    FormalAttemptState,
    FormalTaskSpec,
    FormalTaskState,
    FormalTaskViolation,
    OutboxKind,
    OutboxState,
    PersistedExecutorSelection,
    PersistentAdmissionRecord,
    PersistentAttemptRecord,
    PersistentOutboxItem,
    PersistentTaskEvent,
    PersistentTaskRecord,
    ReconciliationState,
    TaskAdjustmentDeliveryResult,
    TaskAdjustmentRequest,
    TaskAdjustmentSettlement,
    TaskAdjustmentState,
    TaskCommandDisposition,
    TaskEventAuthoritySnapshot,
    TaskEventConsumerAuthorityPage,
    TaskEventConsumerCursorBaseline,
    TaskMutationDisposition,
    TaskMutationPrecondition,
    TaskMutationResult,
    TaskResultArtifact,
    TaskResultAvailability,
    TaskResultRecord,
    TaskRetryAuthoritySnapshot,
    TaskRetryPrecondition,
    TaskRetryProductRequestFingerprint,
    TaskUnreadPage,
    command_result_extensions,
    utc_now,
)
from openjiuwen.core.application.tasks.source import require_payload_source, source_extension

_SCHEMA_VERSION = 6
_DEFAULT_TASK_PAGE_LIMIT = 50
_MAX_TASK_PAGE_LIMIT = 100
_DEFAULT_EVENT_PAGE_LIMIT = 100
_MAX_EVENT_PAGE_LIMIT = 500
_JOURNAL_MODE_RETRY_SECONDS = 10.0
_JOURNAL_MODE_RETRY_INTERVAL_SECONDS = 0.01
_DECISION_BINDING_TYPE = "live_voice.task_business_decision"
_DECISION_BINDING_VERSION = 1
_LEGACY_CONSUMPTION_SEED_TYPE = "legacy_seed_v1"
_RUNTIME_CONSUMPTION_ORIGIN = "runtime_v1"
_ACK_HISTORY_BINDING_TYPE = "live_voice.task_ack_consumption_history"
_ACK_HISTORY_VERSION = 1
_CANONICAL_UTC_RE = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})"
    r"T(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?Z$"
)


@dataclass(frozen=True, slots=True)
class TaskOutboxDiagnosticFact:
    """Content-free current state for one Store-owned outbox record."""

    outbox_id: str
    kind: OutboxKind
    task_id: str
    attempt_id: str
    command_id: str
    state: OutboxState
    delivery_count: int


@dataclass(frozen=True, slots=True)
class TaskDurabilityDiagnosticSnapshot:
    """Authority-free identities for the current durability diagnostic seam."""

    task_id: str
    attempt_id: str
    executor_id: str
    event_head: int
    event_head_id: str
    checkpoint_id: str | None
    checkpoint_attempt_id: str | None
    effect_id: str | None
    effect_attempt_id: str | None
    recovery_id: str | None
    reconciliation_state: ReconciliationState | None
    outbox: tuple[TaskOutboxDiagnosticFact, ...]


_RETRY_BUSINESS_DECISIONS = {
    "TASK_RETRY_LIMIT_EXCEEDED": (
        TaskCommandDisposition.CONFLICT,
        frozenset({ErrorCode.CONFLICT}),
    ),
    "TASK_RETRY_REQUIRES_TERMINAL": (
        TaskCommandDisposition.CONFLICT,
        frozenset({ErrorCode.CONFLICT}),
    ),
    "TASK_RETRY_OUTCOME_NOT_ELIGIBLE": (
        TaskCommandDisposition.CONFLICT,
        frozenset({ErrorCode.CONFLICT}),
    ),
    "TASK_RETRY_PRECONDITION_STALE": (
        TaskCommandDisposition.CONFLICT,
        frozenset({ErrorCode.STALE}),
    ),
}
_UPDATE_BUSINESS_DECISIONS = {
    "TASK_UPDATE_PRECONDITION_STALE": (
        TaskCommandDisposition.CONFLICT,
        frozenset({ErrorCode.STALE}),
    )
}
_SUCCESSOR_BUSINESS_DECISIONS = {
    "TASK_SUCCESSOR_PRECONDITION_CONFLICT": (
        TaskCommandDisposition.CONFLICT,
        frozenset({ErrorCode.CONFLICT}),
    ),
    "TASK_SUCCESSOR_RESULT_CONFLICT": (
        TaskCommandDisposition.CONFLICT,
        frozenset({ErrorCode.CONFLICT}),
    ),
}
_CONTROL_BUSINESS_DECISIONS = {
    "TASK_CONTROL_UNSUPPORTED": (
        TaskCommandDisposition.UNSUPPORTED,
        frozenset({ErrorCode.CAPABILITY_UNAVAILABLE}),
    ),
    "TASK_CONTROL_STATE_CONFLICT": (
        TaskCommandDisposition.CONFLICT,
        frozenset({ErrorCode.CONFLICT}),
    ),
    "TASK_CONTROL_PRECONDITION_STALE": (
        TaskCommandDisposition.CONFLICT,
        frozenset({ErrorCode.STALE}),
    ),
}
_CANCEL_BUSINESS_DECISIONS = {
    "TASK_ALREADY_TERMINAL": (
        TaskCommandDisposition.CONFLICT,
        frozenset({ErrorCode.CONFLICT}),
    ),
    "TASK_MUTATION_PRECONDITION_STALE": (
        TaskCommandDisposition.CONFLICT,
        frozenset({ErrorCode.STALE}),
    ),
    "TASK_CANCEL_ALREADY_REQUESTED": (
        TaskCommandDisposition.CONFLICT,
        frozenset({ErrorCode.CONFLICT}),
    ),
}
_ADJUST_BUSINESS_DECISIONS = {
    "TASK_ADJUSTMENT_STATE_CONFLICT": (
        TaskCommandDisposition.CONFLICT,
        frozenset({ErrorCode.CONFLICT}),
    ),
    "TASK_MUTATION_PRECONDITION_STALE": (
        TaskCommandDisposition.CONFLICT,
        frozenset({ErrorCode.STALE}),
    ),
}
_ACK_BUSINESS_DECISIONS = {
    "TASK_ACK_PRECONDITION_STALE": (
        TaskCommandDisposition.CONFLICT,
        frozenset({ErrorCode.STALE}),
    ),
    "TASK_ACK_EVENT_MISMATCH": (
        TaskCommandDisposition.CONFLICT,
        frozenset({ErrorCode.CONFLICT}),
    ),
}
_TASK_STORE_TABLES_V2 = frozenset(
    {
        "metadata",
        "commands",
        "tasks",
        "attempts",
        "task_events",
        "executor_events",
        "outbox",
    }
)
_TASK_STORE_TABLES_V4 = _TASK_STORE_TABLES_V2 | frozenset({"current_background_tasks", "task_results"})
_TASK_STORE_TABLES_V5 = _TASK_STORE_TABLES_V4 | frozenset({"task_event_consumption"})
_TASK_STORE_TABLES = _TASK_STORE_TABLES_V5 | frozenset(
    {
        "durability_checkpoints",
        "durability_effect_facts",
        "durability_recoveries",
        "durability_mutator_leases",
        "durability_recovery_fences",
    }
)
_TASK_STORE_COLUMNS = {
    "metadata": ("key", "value"),
    "commands": (
        "command_id",
        "fingerprint",
        "command_type",
        "scope_key",
        "result_json",
        "created_at",
    ),
    "tasks": (
        "task_id",
        "scope_key",
        "scope_json",
        "spec_json",
        "state",
        "outcome",
        "attempt_id",
        "correlation_id",
        "cancel_requested",
        "dispatch_fenced",
        "event_head",
        "reconciliation_state",
        "reconciliation_reason",
        "created_at",
        "updated_at",
        "create_command_id",
        "predecessor_task_id",
        "revision_number",
    ),
    "attempts": (
        "attempt_id",
        "task_id",
        "attempt_number",
        "executor_id",
        "executor_ref",
        "state",
        "outcome",
        "source_seq",
        "updated_at",
        "adapter_id",
        "capability_profile_json",
        "capability_profile_digest",
        "execution_requirements_json",
        "admission_priority",
        "admission_reason",
        "admission_attempt_count",
        "admission_next_eligible_at",
        "admission_deadline_at",
        "admission_enqueued_at",
    ),
    "task_events": (
        "task_id",
        "seq",
        "event_id",
        "attempt_id",
        "scope_json",
        "event_type",
        "state",
        "outcome",
        "producer",
        "source_event_id",
        "causation_id",
        "correlation_id",
        "occurred_at",
        "details_json",
    ),
    "executor_events": (
        "source_event_id",
        "attempt_id",
        "source_seq",
        "canonical",
    ),
    "outbox": (
        "outbox_id",
        "kind",
        "task_id",
        "attempt_id",
        "command_id",
        "payload_json",
        "state",
        "delivery_count",
        "claimed_by",
        "claimed_at",
        "claim_token",
        "last_error",
        "created_at",
        "updated_at",
    ),
    "current_background_tasks": (
        "scope_key",
        "session_id",
        "task_id",
        "updated_at",
    ),
    "task_results": (
        "task_id",
        "attempt_id",
        "source_event_id",
        "result_text",
        "artifacts_json",
        "completed_at",
    ),
    "task_event_consumption": (
        "subject_id",
        "project_id",
        "task_id",
        "presentation_class",
        "acked_through_seq",
        "acked_event_id",
        "updated_at",
    ),
    "durability_checkpoints": (
        "task_id",
        "producer_attempt_id",
        "row_sequence",
        "canonical",
        "payload_digest",
        "created_at",
    ),
    "durability_effect_facts": (
        "task_id",
        "origin_attempt_id",
        "row_sequence",
        "canonical",
        "payload_digest",
        "created_at",
    ),
    "durability_recoveries": (
        "recovery_id",
        "task_id",
        "producer_attempt_id",
        "recovery_attempt_id",
        "recovery_generation",
        "profile_json",
        "checkpoint_head",
        "checkpoint_prefix_digest",
        "effect_head",
        "effect_prefix_digest",
        "recovery_facts",
        "created_at",
    ),
    "durability_mutator_leases": (
        "task_id",
        "owner_id",
        "claim_token",
        "claim_generation",
        "claimed_at",
        "expires_at",
    ),
    "durability_recovery_fences": (
        "task_id",
        "producer_attempt_id",
        "cancel_command_id",
        "created_at",
    ),
}
_TASK_STORE_NOT_NULL = {
    "metadata": frozenset({"value"}),
    "commands": frozenset(_TASK_STORE_COLUMNS["commands"]),
    "tasks": frozenset(
        {
            "scope_key",
            "scope_json",
            "spec_json",
            "state",
            "attempt_id",
            "correlation_id",
            "cancel_requested",
            "dispatch_fenced",
            "event_head",
            "created_at",
            "updated_at",
            "create_command_id",
            "revision_number",
        }
    ),
    "attempts": frozenset(
        {
            "task_id",
            "attempt_number",
            "executor_id",
            "state",
            "source_seq",
            "updated_at",
        }
    ),
    "task_events": frozenset(
        {
            "task_id",
            "seq",
            "event_id",
            "attempt_id",
            "scope_json",
            "event_type",
            "state",
            "producer",
            "causation_id",
            "correlation_id",
            "occurred_at",
            "details_json",
        }
    ),
    "executor_events": frozenset({"attempt_id", "source_seq", "canonical"}),
    "outbox": frozenset(
        {
            "kind",
            "task_id",
            "attempt_id",
            "command_id",
            "payload_json",
            "state",
            "delivery_count",
            "created_at",
            "updated_at",
        }
    ),
    "current_background_tasks": frozenset({"scope_key", "session_id", "task_id", "updated_at"}),
    "task_results": frozenset(
        {
            "task_id",
            "attempt_id",
            "source_event_id",
            "result_text",
            "artifacts_json",
            "completed_at",
        }
    ),
    "task_event_consumption": frozenset(_TASK_STORE_COLUMNS["task_event_consumption"]),
    "durability_checkpoints": frozenset(_TASK_STORE_COLUMNS["durability_checkpoints"]),
    "durability_effect_facts": frozenset(_TASK_STORE_COLUMNS["durability_effect_facts"]),
    "durability_recoveries": frozenset(_TASK_STORE_COLUMNS["durability_recoveries"]),
    "durability_mutator_leases": frozenset(_TASK_STORE_COLUMNS["durability_mutator_leases"]),
    "durability_recovery_fences": frozenset(_TASK_STORE_COLUMNS["durability_recovery_fences"]),
}
_TASK_STORE_PRIMARY_KEYS = {
    "metadata": ("key",),
    "commands": ("scope_key", "command_id"),
    "tasks": ("task_id",),
    "attempts": ("attempt_id",),
    "task_events": ("task_id", "seq"),
    "executor_events": ("source_event_id",),
    "outbox": ("outbox_id",),
    "current_background_tasks": ("scope_key", "session_id"),
    "task_results": ("task_id", "attempt_id", "source_event_id"),
    "task_event_consumption": (
        "subject_id",
        "project_id",
        "task_id",
        "presentation_class",
    ),
    "durability_checkpoints": (
        "task_id",
        "producer_attempt_id",
        "row_sequence",
    ),
    "durability_effect_facts": (
        "task_id",
        "origin_attempt_id",
        "row_sequence",
    ),
    "durability_recoveries": ("recovery_id",),
    "durability_mutator_leases": ("task_id",),
    "durability_recovery_fences": ("task_id",),
}
_TASK_STORE_INTEGER_COLUMNS = frozenset(
    {
        ("tasks", "cancel_requested"),
        ("tasks", "dispatch_fenced"),
        ("tasks", "event_head"),
        ("tasks", "revision_number"),
        ("attempts", "attempt_number"),
        ("attempts", "source_seq"),
        ("attempts", "admission_attempt_count"),
        ("task_events", "seq"),
        ("executor_events", "source_seq"),
        ("outbox", "delivery_count"),
        ("task_event_consumption", "acked_through_seq"),
        ("durability_checkpoints", "row_sequence"),
        ("durability_effect_facts", "row_sequence"),
        ("durability_recoveries", "recovery_generation"),
        ("durability_recoveries", "checkpoint_head"),
        ("durability_recoveries", "effect_head"),
        ("durability_mutator_leases", "claim_generation"),
    }
)
_TASK_STORE_BLOB_COLUMNS = frozenset(
    {
        ("commands", "fingerprint"),
        ("executor_events", "canonical"),
        ("durability_checkpoints", "canonical"),
        ("durability_effect_facts", "canonical"),
        ("durability_recoveries", "recovery_facts"),
    }
)
_TASK_STORE_DEFAULTS = {
    ("tasks", "cancel_requested"): "0",
    ("tasks", "dispatch_fenced"): "0",
    ("tasks", "create_command_id"): "''",
    ("tasks", "revision_number"): "1",
    ("attempts", "source_seq"): "-1",
    ("outbox", "delivery_count"): "0",
}
_TASK_STORE_NAMED_INDEXES_V3 = {
    "idx_tasks_scope": ("tasks", ("scope_key", "task_id")),
    "idx_tasks_state": ("tasks", ("state", "task_id")),
    "idx_outbox_pending": ("outbox", ("state", "created_at", "outbox_id")),
    "idx_task_results_task": (
        "task_results",
        ("task_id", "attempt_id", "completed_at"),
    ),
}
_TASK_STORE_NAMED_INDEXES_V4 = {
    **_TASK_STORE_NAMED_INDEXES_V3,
    "idx_tasks_scope_page": ("tasks", ("scope_key", "created_at", "task_id")),
}
_TASK_STORE_NAMED_INDEXES_V5 = {
    **_TASK_STORE_NAMED_INDEXES_V4,
    "idx_attempts_admission": (
        "attempts",
        (
            "state",
            "admission_next_eligible_at",
            "admission_deadline_at",
            "admission_priority",
            "admission_enqueued_at",
            "attempt_id",
        ),
    ),
    "idx_task_event_consumption_event": (
        "task_event_consumption",
        ("task_id", "acked_through_seq", "acked_event_id"),
    ),
}
_TASK_STORE_NAMED_INDEXES_V6 = {
    **_TASK_STORE_NAMED_INDEXES_V5,
    "idx_durability_recoveries_producer": (
        "durability_recoveries",
        ("task_id", "producer_attempt_id", "recovery_generation"),
    ),
    "idx_durability_effect_facts_origin": (
        "durability_effect_facts",
        ("task_id", "origin_attempt_id", "row_sequence"),
    ),
}
_TASK_STORE_UNIQUE_KEYS_V3 = {
    "metadata": frozenset({("key",)}),
    "commands": frozenset({("scope_key", "command_id")}),
    "tasks": frozenset({("task_id",), ("attempt_id",)}),
    "attempts": frozenset({("attempt_id",), ("task_id", "attempt_number")}),
    "task_events": frozenset({("task_id", "seq"), ("event_id",)}),
    "executor_events": frozenset({("source_event_id",), ("attempt_id", "source_seq")}),
    "outbox": frozenset({("outbox_id",)}),
    "current_background_tasks": frozenset({("scope_key", "session_id")}),
    "task_results": frozenset({("task_id", "attempt_id", "source_event_id")}),
}
_TASK_STORE_UNIQUE_KEYS_V4 = {
    **_TASK_STORE_UNIQUE_KEYS_V3,
    "tasks": _TASK_STORE_UNIQUE_KEYS_V3["tasks"] | frozenset({("predecessor_task_id",)}),
}
_TASK_STORE_UNIQUE_KEYS_V5 = {
    **_TASK_STORE_UNIQUE_KEYS_V4,
    "task_events": _TASK_STORE_UNIQUE_KEYS_V4["task_events"] | frozenset({("task_id", "seq", "event_id")}),
    "task_event_consumption": frozenset(
        {
            (
                "subject_id",
                "project_id",
                "task_id",
                "presentation_class",
            )
        }
    ),
}
_TASK_STORE_UNIQUE_KEYS_V6 = {
    **_TASK_STORE_UNIQUE_KEYS_V5,
    "durability_checkpoints": frozenset({("task_id", "producer_attempt_id", "row_sequence")}),
    "durability_effect_facts": frozenset({("task_id", "origin_attempt_id", "row_sequence")}),
    "durability_recoveries": frozenset({("recovery_id",), ("recovery_attempt_id",)}),
    "durability_mutator_leases": frozenset({("task_id",)}),
    "durability_recovery_fences": frozenset({("task_id",)}),
}
_LEGACY_CANDIDATE_V6_RECOVERY_FENCE_UNIQUE_KEYS = frozenset({("task_id",), ("cancel_command_id",)})
_LEGACY_CANDIDATE_V6_RECOVERY_FENCE_SQL = (
    "CREATETABLEDURABILITY_RECOVERY_FENCES("
    "TASK_IDTEXTNOTNULLPRIMARYKEYREFERENCESTASKS(TASK_ID)ONDELETECASCADE,"
    "PRODUCER_ATTEMPT_IDTEXTNOTNULLREFERENCESATTEMPTS(ATTEMPT_ID)ONDELETECASCADE,"
    "CANCEL_COMMAND_IDTEXTNOTNULLUNIQUE,CREATED_ATTEXTNOTNULL)"
)
_LEGACY_CANDIDATE_V6_RECOVERY_FENCE_INDEXES = frozenset(
    {
        ("pk", True, False, ("task_id",)),
        ("u", True, False, ("cancel_command_id",)),
    }
)
_TASK_STORE_FOREIGN_KEYS_V3 = {
    "metadata": frozenset(),
    "commands": frozenset(),
    "tasks": frozenset(),
    "attempts": frozenset({("task_id", "tasks", "task_id", "CASCADE")}),
    "task_events": frozenset({("task_id", "tasks", "task_id", "CASCADE")}),
    "executor_events": frozenset({("attempt_id", "attempts", "attempt_id", "CASCADE")}),
    "outbox": frozenset(
        {
            ("task_id", "tasks", "task_id", "CASCADE"),
            ("attempt_id", "attempts", "attempt_id", "CASCADE"),
        }
    ),
    "current_background_tasks": frozenset({("task_id", "tasks", "task_id", "RESTRICT")}),
    "task_results": frozenset(
        {
            ("task_id", "tasks", "task_id", "CASCADE"),
            ("attempt_id", "attempts", "attempt_id", "CASCADE"),
            (
                "source_event_id",
                "executor_events",
                "source_event_id",
                "CASCADE",
            ),
        }
    ),
}
_TASK_STORE_FOREIGN_KEYS_V4 = {
    **_TASK_STORE_FOREIGN_KEYS_V3,
    "tasks": frozenset({("predecessor_task_id", "tasks", "task_id", "RESTRICT")}),
}
_TASK_STORE_FOREIGN_KEYS_V6 = {
    **_TASK_STORE_FOREIGN_KEYS_V4,
    "durability_checkpoints": frozenset(
        {
            ("task_id", "tasks", "task_id", "CASCADE"),
            ("producer_attempt_id", "attempts", "attempt_id", "CASCADE"),
        }
    ),
    "durability_effect_facts": frozenset(
        {
            ("task_id", "tasks", "task_id", "CASCADE"),
            ("origin_attempt_id", "attempts", "attempt_id", "CASCADE"),
        }
    ),
    "durability_recoveries": frozenset(
        {
            ("task_id", "tasks", "task_id", "CASCADE"),
            ("producer_attempt_id", "attempts", "attempt_id", "CASCADE"),
            ("recovery_attempt_id", "attempts", "attempt_id", "CASCADE"),
        }
    ),
    "durability_mutator_leases": frozenset({("task_id", "tasks", "task_id", "CASCADE")}),
    "durability_recovery_fences": frozenset(
        {
            ("task_id", "tasks", "task_id", "CASCADE"),
            ("producer_attempt_id", "attempts", "attempt_id", "CASCADE"),
        }
    ),
}
_TASK_EVENT_CONSUMPTION_FOREIGN_KEYS = frozenset(
    {
        (
            ("task_id", "acked_through_seq", "acked_event_id"),
            "task_events",
            ("task_id", "seq", "event_id"),
            "CASCADE",
        )
    }
)
_StoredRecordT = TypeVar("_StoredRecordT")
_TaskReadSnapshot = tuple[
    PersistentTaskRecord,
    PersistentAttemptRecord,
    PersistentAdmissionRecord | None,
]


@dataclass(frozen=True, slots=True)
class TaskQueueControl:
    """As-of queue ownership, never a permit to bypass a write transaction."""

    dispatch_control: str
    can_update: bool
    can_reprioritize: bool

    @property
    def operations(self) -> frozenset[str]:
        if self.dispatch_control != "unclaimed":
            return frozenset()
        return frozenset(operation for operation, allowed in (
            ("task.update", self.can_update and self.can_reprioritize),
            ("task.reprioritize", self.can_reprioritize),
        ) if allowed)


@dataclass(frozen=True, slots=True)
class TaskAuthorityReadSnapshot:
    """One immutable database view; constituent models retain their own validation."""

    task: PersistentTaskRecord
    attempt: PersistentAttemptRecord
    admission: PersistentAdmissionRecord | None
    event_head: PersistentTaskEvent
    result_availability: TaskResultAvailability
    result: TaskResultRecord | None
    result_reason: str
    queue_control: TaskQueueControl


_OUTBOX_BINDING_SELECT = """
    SELECT o.*, a.attempt_id AS canonical_attempt_id,
           a.task_id AS attempt_task_id,
           a.executor_ref AS bound_executor_ref,
           a.attempt_number AS bound_attempt_number,
           a.source_seq AS bound_source_seq,
           a.executor_id AS bound_executor_id,
           a.state AS bound_attempt_state,
           a.outcome AS bound_attempt_outcome,
           a.adapter_id AS bound_adapter_id,
           a.capability_profile_json AS bound_capability_profile_json,
           a.capability_profile_digest AS bound_capability_profile_digest,
           a.execution_requirements_json AS bound_execution_requirements_json,
           a.admission_priority AS bound_admission_priority,
           a.admission_reason AS bound_admission_reason,
           a.admission_attempt_count AS bound_admission_attempt_count,
           a.admission_next_eligible_at AS bound_admission_next_eligible_at,
           a.admission_deadline_at AS bound_admission_deadline_at,
           a.admission_enqueued_at AS bound_admission_enqueued_at,
           pa.attempt_id AS predecessor_attempt_id,
           pa.task_id AS predecessor_task_id,
           pa.attempt_number AS predecessor_attempt_number,
           pa.state AS predecessor_attempt_state,
           pa.outcome AS predecessor_attempt_outcome,
           t.task_id AS canonical_task_id,
           t.attempt_id AS task_attempt_id,
           t.scope_key AS task_scope_key,
           t.scope_json AS task_scope_json,
           t.spec_json AS task_spec_json,
           t.state AS bound_task_state,
           t.outcome AS bound_task_outcome,
           t.correlation_id AS task_correlation_id,
           t.event_head AS task_event_head,
           t.cancel_requested AS task_cancel_requested,
           t.dispatch_fenced AS task_dispatch_fenced,
           c.command_id AS canonical_command_id,
           c.command_type AS bound_command_type,
           c.scope_key AS command_scope_key,
           c.fingerprint AS command_fingerprint,
           c.result_json AS command_result_json,
           ce.event_id AS cancel_event_id,
           ce.task_id AS cancel_event_task_id,
           ce.attempt_id AS cancel_event_attempt_id,
           ce.scope_json AS cancel_event_scope_json,
           ce.event_type AS cancel_event_type,
           ce.state AS cancel_event_state,
           ce.outcome AS cancel_event_outcome,
           ce.producer AS cancel_event_producer,
           ce.source_event_id AS cancel_event_source_event_id,
           ce.causation_id AS cancel_event_causation_id,
           ce.correlation_id AS cancel_event_correlation_id,
           ce.occurred_at AS cancel_event_occurred_at,
           ce.details_json AS cancel_event_details_json,
           ce.seq AS cancel_event_seq,
           re.event_id AS retry_event_id,
           re.task_id AS retry_event_task_id,
           re.attempt_id AS retry_event_attempt_id,
           re.scope_json AS retry_event_scope_json,
           re.event_type AS retry_event_type,
           re.state AS retry_event_state,
           re.outcome AS retry_event_outcome,
           re.producer AS retry_event_producer,
           re.source_event_id AS retry_event_source_event_id,
           re.causation_id AS retry_event_causation_id,
           re.correlation_id AS retry_event_correlation_id,
           re.occurred_at AS retry_event_occurred_at,
           re.details_json AS retry_event_details_json,
           re.seq AS retry_event_seq,
           ae.event_id AS adjust_event_id,
           ae.task_id AS adjust_event_task_id,
           ae.attempt_id AS adjust_event_attempt_id,
           ae.scope_json AS adjust_event_scope_json,
           ae.event_type AS adjust_event_type,
           ae.state AS adjust_event_state,
           ae.outcome AS adjust_event_outcome,
           ae.producer AS adjust_event_producer,
           ae.source_event_id AS adjust_event_source_event_id,
           ae.causation_id AS adjust_event_causation_id,
           ae.correlation_id AS adjust_event_correlation_id,
           ae.occurred_at AS adjust_event_occurred_at,
           ae.details_json AS adjust_event_details_json,
           ae.seq AS adjust_event_seq,
           (
             SELECT COUNT(*) FROM task_events AS ce_count
             WHERE ce_count.task_id=o.task_id
               AND ce_count.attempt_id=o.attempt_id
               AND ce_count.event_type='task.cancel_requested'
               AND ce_count.causation_id=o.command_id
           ) AS cancel_event_count
           ,(
             SELECT COUNT(*) FROM task_events AS re_count
             WHERE re_count.task_id=o.task_id
               AND re_count.attempt_id=o.attempt_id
               AND re_count.event_type='task.retry_accepted'
               AND re_count.causation_id=o.command_id
           ) AS retry_event_count
           ,(
             SELECT COUNT(*) FROM task_events AS ae_count
             WHERE ae_count.task_id=o.task_id
               AND ae_count.attempt_id=o.attempt_id
               AND ae_count.event_type='task.adjust_requested'
               AND ae_count.causation_id=o.command_id
           ) AS adjust_event_count
           ,(
             SELECT MIN(re_start.seq) FROM task_events AS re_start
             WHERE re_start.task_id=o.task_id
               AND re_start.attempt_id=o.attempt_id
           ) AS retry_segment_start_seq
    FROM outbox AS o
    LEFT JOIN attempts AS a ON a.attempt_id=o.attempt_id
    LEFT JOIN attempts AS pa
      ON pa.task_id=o.task_id AND pa.attempt_number=a.attempt_number-1
    LEFT JOIN tasks AS t ON t.task_id=o.task_id
    LEFT JOIN commands AS c
      ON c.command_id=o.command_id AND c.scope_key=t.scope_key
    LEFT JOIN task_events AS ce
      ON ce.task_id=o.task_id
     AND ce.attempt_id=o.attempt_id
     AND ce.event_type='task.cancel_requested'
     AND ce.causation_id=o.command_id
    LEFT JOIN task_events AS re
      ON re.task_id=o.task_id
     AND re.attempt_id=o.attempt_id
     AND re.event_type='task.retry_accepted'
     AND re.causation_id=o.command_id
    LEFT JOIN task_events AS ae
      ON ae.task_id=o.task_id
     AND ae.attempt_id=o.attempt_id
     AND ae.event_type='task.adjust_requested'
     AND ae.causation_id=o.command_id
"""


def _json_dump(value: object) -> str:
    canonical_json_bytes(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_load(value: str | bytes) -> object:
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError) as error:
        raise FormalTaskViolation(
            "TASK_STORE_CORRUPT",
            "formal Task Store contains malformed JSON",
            ErrorCode.INTERNAL,
        ) from error


def _scope_key(scope: ScopeRef) -> str:
    return _json_dump(scope.to_dict())


def _selection_fingerprint_payload(
    selection: PersistedExecutorSelection,
) -> dict[str, object]:
    return {
        "adapter_id": selection.adapter_id,
        "capability_profile": json.loads(selection.capability_profile_json),
        "capability_profile_digest": selection.capability_profile_digest,
        "execution_requirements": json.loads(selection.execution_requirements_json),
        "admission_priority": selection.admission_priority.value,
    }


def _selection_from_fingerprint_payload(
    value: object,
) -> PersistedExecutorSelection:
    if type(value) is not dict or set(value) != {
        "adapter_id",
        "capability_profile",
        "capability_profile_digest",
        "execution_requirements",
        "admission_priority",
    }:
        raise FormalTaskViolation(
            "TASK_STORE_CORRUPT",
            "executor selection fingerprint is not canonical",
            ErrorCode.INTERNAL,
        )
    return PersistedExecutorSelection(
        adapter_id=value["adapter_id"],
        capability_profile_json=canonical_json_bytes(value["capability_profile"]),
        capability_profile_digest=value["capability_profile_digest"],
        execution_requirements_json=canonical_json_bytes(value["execution_requirements"]),
        admission_priority=AdmissionPriority(value["admission_priority"]),
    )


def _utc_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise FormalTaskViolation(
            "INVALID_FORMAL_TASK_TIMESTAMP",
            "admission creation requires an RFC3339 timestamp",
            ErrorCode.INVALID_ARGUMENT,
        ) from error
    if parsed.tzinfo is None:
        raise FormalTaskViolation(
            "INVALID_FORMAL_TASK_TIMESTAMP",
            "admission creation timestamp must include a timezone",
            ErrorCode.INVALID_ARGUMENT,
        )
    return parsed.astimezone(UTC)


def _canonical_utc_order_key(
    value: object,
) -> tuple[datetime, int] | None:
    """Parse canonical UTC seconds plus the contract's full nanoseconds."""

    if type(value) is not str:
        return None
    match = _CANONICAL_UTC_RE.fullmatch(value)
    if match is None:
        return None
    try:
        second = datetime(
            int(match["year"]),
            int(match["month"]),
            int(match["day"]),
            int(match["hour"]),
            int(match["minute"]),
            int(match["second"]),
            tzinfo=UTC,
        )
    except ValueError:
        return None
    fraction = match["fraction"] or ""
    nanosecond = int(fraction.ljust(9, "0")) if fraction else 0
    return second, nanosecond


def _utc_plus_seconds(value: str, seconds: float) -> str:
    base = _utc_datetime(value)
    try:
        absolute = base + timedelta(seconds=seconds)
    except (OverflowError, ValueError) as error:
        raise FormalTaskViolation(
            "INVALID_ADMISSION_POLICY",
            "admission policy exceeds the representable UTC timestamp range",
            ErrorCode.INVALID_ARGUMENT,
        ) from error
    return absolute.isoformat().replace("+00:00", "Z")


def _selection_from_attempt_row(
    row: sqlite3.Row,
) -> PersistedExecutorSelection | None:
    selection_columns = _TASK_STORE_COLUMNS["attempts"][9:]
    row_keys = frozenset(row.keys())
    present_columns = row_keys.intersection(selection_columns)
    if not present_columns:
        return None
    if present_columns != frozenset(selection_columns):
        raise FormalTaskViolation(
            "INVALID_EXECUTOR_SELECTION",
            "persisted Attempt has a partial executor selection schema",
            ErrorCode.PROTOCOL_VIOLATION,
        )
    values = tuple(row[column] for column in selection_columns)
    if all(value is None for value in values):
        return None
    required_columns = tuple(column for column in selection_columns if column != "admission_reason")
    if any(row[column] is None for column in required_columns):
        raise FormalTaskViolation(
            "INVALID_EXECUTOR_SELECTION",
            "persisted Attempt has partial executor admission facts",
            ErrorCode.PROTOCOL_VIOLATION,
        )
    text_columns = (
        "adapter_id",
        "capability_profile_json",
        "capability_profile_digest",
        "execution_requirements_json",
        "admission_priority",
        "admission_next_eligible_at",
        "admission_deadline_at",
        "admission_enqueued_at",
    )
    if (
        any(type(row[column]) is not str for column in text_columns)
        or type(row["admission_attempt_count"]) is not int
        or (row["admission_reason"] is not None and type(row["admission_reason"]) is not str)
    ):
        raise FormalTaskViolation(
            "INVALID_EXECUTOR_SELECTION",
            "persisted Attempt executor admission facts have invalid storage types",
            ErrorCode.PROTOCOL_VIOLATION,
        )
    return PersistedExecutorSelection(
        adapter_id=row["adapter_id"],
        capability_profile_json=row["capability_profile_json"].encode("utf-8"),
        capability_profile_digest=row["capability_profile_digest"],
        execution_requirements_json=row["execution_requirements_json"].encode("utf-8"),
        admission_priority=AdmissionPriority(row["admission_priority"]),
    )


def _stored_record(record_kind: str, loader: Callable[[], _StoredRecordT]) -> _StoredRecordT:
    try:
        return loader()
    except FormalTaskViolation as error:
        if error.reason == "TASK_STORE_CORRUPT":
            raise
        raise FormalTaskViolation(
            "TASK_STORE_CORRUPT",
            f"formal Task Store contains an invalid {record_kind} record",
            ErrorCode.INTERNAL,
        ) from error
    except (ContractViolation, KeyError, OverflowError, TypeError, ValueError) as error:
        raise FormalTaskViolation(
            "TASK_STORE_CORRUPT",
            f"formal Task Store contains an invalid {record_kind} record",
            ErrorCode.INTERNAL,
        ) from error


def _task_binding_from_row(row: sqlite3.Row) -> tuple[ScopeRef, FormalTaskSpec]:
    def load() -> tuple[ScopeRef, FormalTaskSpec]:
        scope = ScopeRef.from_dict(_json_load(row["scope_json"]))
        spec = FormalTaskSpec.from_dict(_json_load(row["spec_json"]))
        if row["scope_key"] != _scope_key(scope) or spec.context.scope != scope:
            raise FormalTaskViolation(
                "TASK_SCOPE_BINDING_MISMATCH",
                "task scope key or context does not match its canonical scope",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        return scope, spec

    return _stored_record("task", load)


class SqliteTaskStore:
    """Cross-process transactional Store; the legacy schedule JSON is not read."""

    def __init__(
        self,
        database_path: str | Path,
        *,
        failpoint: Callable[[str], None] | None = None,
    ) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._failpoint = failpoint
        self._initialize()
        from openjiuwen.core.application.tasks.task_adjustment_queue import TaskAdjustmentQueue

        self.adjustment_queue = TaskAdjustmentQueue(self)

    def _connect(self, *, foreign_keys: bool = True) -> sqlite3.Connection:
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(
                self.database_path,
                timeout=10,
                isolation_level=None,
            )
            connection.row_factory = sqlite3.Row
            connection.execute(f"PRAGMA foreign_keys = {'ON' if foreign_keys else 'OFF'}")
            connection.execute("PRAGMA busy_timeout = 10000")
            return connection
        except sqlite3.Error as exc:
            if connection is not None:
                connection.close()
            raise FormalTaskViolation(
                "TASK_STORE_UNAVAILABLE",
                "formal Task Store is unavailable",
                ErrorCode.UNAVAILABLE,
            ) from exc

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except sqlite3.Error as exc:
            try:
                connection.rollback()
            finally:
                raise FormalTaskViolation(
                    "TASK_STORE_UNAVAILABLE",
                    "formal Task Store transaction is unavailable",
                    ErrorCode.UNAVAILABLE,
                ) from exc
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def _reader(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
        except sqlite3.Error as exc:
            raise FormalTaskViolation(
                "TASK_STORE_UNAVAILABLE",
                "formal Task Store read is unavailable",
                ErrorCode.UNAVAILABLE,
            ) from exc
        finally:
            connection.close()

    @contextmanager
    def _snapshot_reader(self) -> Iterator[sqlite3.Connection]:
        """Hold one explicit SQLite read snapshot across related projections."""

        connection = self._connect()
        try:
            connection.execute("BEGIN")
            yield connection
            connection.rollback()
        except sqlite3.Error as exc:
            try:
                connection.rollback()
            finally:
                raise FormalTaskViolation(
                    "TASK_STORE_UNAVAILABLE",
                    "formal Task Store snapshot read is unavailable",
                    ErrorCode.UNAVAILABLE,
                ) from exc
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _profile_binding_from_selection(
        *,
        executor_id: str,
        selection: PersistedExecutorSelection | None,
    ) -> DurabilityProfileBinding:
        if selection is None:
            raise FormalTaskViolation(
                "DURABILITY_PROFILE_UNAVAILABLE",
                "persisted Executor selection is required for durability",
                ErrorCode.CAPABILITY_UNAVAILABLE,
            )
        try:
            profile = ExecutorCapabilityProfile.from_dict(json.loads(selection.capability_profile_json))
            binding = DurabilityProfileBinding(
                executor_id=profile.executor_id,
                adapter_id=profile.adapter_id,
                profile_id=profile.profile_id,
                profile_version=profile.adapter_protocol_version,
                profile_digest=selection.capability_profile_digest,
                durability_level=profile.durability_level,
                durability_capability_version=profile.durability_version,
            )
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            raise FormalTaskViolation(
                "TASK_STORE_CORRUPT",
                "persisted durability profile is invalid",
                ErrorCode.INTERNAL,
            ) from error
        if (
            selection.adapter_id != profile.adapter_id
            or executor_id != profile.executor_id
            or profile.digest_sha256() != selection.capability_profile_digest
        ):
            raise FormalTaskViolation(
                "TASK_STORE_CORRUPT",
                "persisted durability profile binding is inconsistent",
                ErrorCode.INTERNAL,
            )
        return binding

    @classmethod
    def _durability_binding_from_connection(
        cls,
        connection: sqlite3.Connection,
        *,
        scope: ScopeRef,
        task_id: str,
        origin_attempt_id: str,
    ) -> DurabilityReadBinding:
        task_row = cls._require_task_row(connection, task_id, scope)
        attempt_row = connection.execute(
            "SELECT * FROM attempts WHERE task_id=? AND attempt_id=?",
            (task_id, origin_attempt_id),
        ).fetchone()
        if attempt_row is None:
            raise FormalTaskViolation(
                "DURABILITY_BINDING_MISMATCH",
                "durability origin Attempt does not belong to the Task",
                ErrorCode.CONFLICT,
            )
        profile = cls._profile_binding_from_selection(
            executor_id=attempt_row["executor_id"],
            selection=_selection_from_attempt_row(attempt_row),
        )
        logical_origin_attempt_id = origin_attempt_id
        visited: set[str] = set()
        while True:
            if logical_origin_attempt_id in visited:
                raise cls._corrupt("durability recovery lineage contains a cycle")
            visited.add(logical_origin_attempt_id)
            recovery_row = connection.execute(
                """SELECT producer_attempt_id FROM durability_recoveries
                   WHERE task_id=? AND recovery_attempt_id=?""",
                (task_id, logical_origin_attempt_id),
            ).fetchone()
            if recovery_row is None:
                break
            logical_origin_attempt_id = recovery_row["producer_attempt_id"]
        return DurabilityReadBinding(
            scope=scope,
            task_id=task_row["task_id"],
            origin_attempt_id=origin_attempt_id,
            profile=profile,
            logical_origin_attempt_id=logical_origin_attempt_id,
        )

    @staticmethod
    def _checkpoint_rows(
        connection: sqlite3.Connection,
        binding: DurabilityReadBinding,
    ) -> tuple[CheckpointPrefixRow, ...]:
        rows = connection.execute(
            """SELECT row_sequence, canonical, payload_digest
               FROM durability_checkpoints
               WHERE task_id=? AND producer_attempt_id=?
               ORDER BY row_sequence""",
            (binding.task_id, binding.origin_attempt_id),
        ).fetchall()
        return tuple(
            CheckpointPrefixRow(
                row_sequence=row["row_sequence"],
                binding=binding,
                canonical_bytes=bytes(row["canonical"]),
                payload_digest=row["payload_digest"],
            )
            for row in rows
        )

    @staticmethod
    def _effect_rows(
        connection: sqlite3.Connection,
        binding: DurabilityReadBinding,
    ) -> tuple[EffectPrefixRow, ...]:
        rows = connection.execute(
            """SELECT row_sequence, canonical, payload_digest
               FROM durability_effect_facts
               WHERE task_id=? AND origin_attempt_id=?
               ORDER BY row_sequence""",
            (binding.task_id, binding.origin_attempt_id),
        ).fetchall()
        return tuple(
            EffectPrefixRow(
                row_sequence=row["row_sequence"],
                binding=binding,
                canonical_bytes=bytes(row["canonical"]),
                payload_digest=row["payload_digest"],
            )
            for row in rows
        )

    @classmethod
    def _verified_checkpoint_prefix(
        cls,
        connection: sqlite3.Connection,
        binding: DurabilityReadBinding,
        *,
        expected_head: int | None = None,
        expected_prefix_digest: str | None = None,
    ) -> VerifiedCheckpointPrefix:
        rows = cls._checkpoint_rows(connection, binding)
        if expected_head is not None:
            rows = tuple(row for row in rows if row.row_sequence <= expected_head)
        return verify_checkpoint_prefix(
            rows,
            expected_binding=binding,
            expected_head=len(rows) if expected_head is None else expected_head,
            expected_prefix_digest=expected_prefix_digest,
        )

    @classmethod
    def _verified_effect_prefix(
        cls,
        connection: sqlite3.Connection,
        binding: DurabilityReadBinding,
        *,
        expected_head: int | None = None,
        expected_prefix_digest: str | None = None,
    ) -> VerifiedEffectPrefix:
        rows = cls._effect_rows(connection, binding)
        if expected_head is not None:
            rows = tuple(row for row in rows if row.row_sequence <= expected_head)
        return verify_effect_prefix(
            rows,
            expected_binding=binding,
            expected_head=len(rows) if expected_head is None else expected_head,
            expected_prefix_digest=expected_prefix_digest,
        )

    def _consume_durability_authorization(
        self,
        connection: sqlite3.Connection,
        authorization: DurabilityMutationAuthorization | None,
        *,
        operation: str,
        binding: DurabilityReadBinding,
        candidate_attempt_id: str | None,
        payload_digest: str,
        observed_at: str,
    ) -> tuple[VerifiedCheckpointPrefix, VerifiedEffectPrefix]:
        if type(authorization) is not DurabilityMutationAuthorization:
            raise FormalTaskViolation(
                "DURABILITY_MUTATION_AUTHORIZATION_REQUIRED",
                "durability mutation requires one opaque Direct authorization",
                ErrorCode.PERMISSION_DENIED,
            )
        if not authorization.is_authentic_for_store(self):
            raise FormalTaskViolation(
                "DURABILITY_MUTATION_AUTHORIZATION_INVALID",
                "durability mutation authorization has no valid issuer binding",
                ErrorCode.PERMISSION_DENIED,
            )
        checkpoints = self._verified_checkpoint_prefix(connection, binding)
        effects = self._verified_effect_prefix(connection, binding)
        lease = connection.execute(
            "SELECT * FROM durability_mutator_leases WHERE task_id=?",
            (binding.task_id,),
        ).fetchone()
        if (
            authorization.operation != operation
            or authorization.scope != binding.scope
            or authorization.task_id != binding.task_id
            or authorization.producer_attempt_id != binding.origin_attempt_id
            or authorization.candidate_attempt_id != candidate_attempt_id
            or authorization.profile != binding.profile
            or authorization.checkpoint_head != checkpoints.head
            or authorization.checkpoint_prefix_digest != checkpoints.prefix_digest
            or authorization.effect_head != effects.head
            or authorization.effect_prefix_digest != effects.prefix_digest
            or authorization.payload_digest != payload_digest
            or lease is None
            or lease["owner_id"] != authorization.claim_owner_id
            or lease["claim_token"] != authorization.claim_token
            or lease["claim_generation"] != authorization.claim_generation
            or _utc_datetime(lease["expires_at"]) <= _utc_datetime(observed_at)
        ):
            raise FormalTaskViolation(
                "DURABILITY_MUTATION_AUTHORIZATION_STALE",
                "durability authorization does not bind current Store truth",
                ErrorCode.STALE,
            )
        consumed = connection.execute(
            """DELETE FROM durability_mutator_leases
               WHERE task_id=? AND owner_id=? AND claim_token=?
                 AND claim_generation=?""",
            (
                binding.task_id,
                authorization.claim_owner_id,
                authorization.claim_token,
                authorization.claim_generation,
            ),
        ).rowcount
        if consumed != 1:
            raise FormalTaskViolation(
                "DURABILITY_MUTATION_AUTHORIZATION_STALE",
                "durability authorization was already consumed",
                ErrorCode.STALE,
            )
        return checkpoints, effects

    def append_durability_checkpoint(
        self,
        checkpoint: D1Checkpoint,
        *,
        observed_at: str,
        authorization: DurabilityMutationAuthorization | None = None,
    ) -> VerifiedCheckpointPrefix:
        """Atomically append one canonical immutable checkpoint fact."""

        if type(checkpoint) is not D1Checkpoint:
            raise FormalTaskViolation(
                "INVALID_DURABILITY_CHECKPOINT",
                "durability checkpoint must use the exact contract",
                ErrorCode.INVALID_ARGUMENT,
            )
        canonical = checkpoint.canonical_bytes()
        digest = hashlib.sha256(canonical).hexdigest()
        with self._transaction() as connection:
            binding = self._durability_binding_from_connection(
                connection,
                scope=checkpoint.scope,
                task_id=checkpoint.task_id,
                origin_attempt_id=checkpoint.producer_attempt_id,
            )
            if checkpoint.profile != binding.profile:
                raise FormalTaskViolation(
                    "DURABILITY_BINDING_MISMATCH",
                    "checkpoint profile does not match persisted selection",
                    ErrorCode.CONFLICT,
                )
            rows = self._checkpoint_rows(connection, binding)
            row_sequence = checkpoint.checkpoint_sequence
            if row_sequence <= 0:
                raise FormalTaskViolation(
                    "DURABILITY_PREFIX_CONFLICT",
                    "persisted checkpoint sequence must start at one",
                    ErrorCode.CONFLICT,
                )
            self._consume_durability_authorization(
                connection,
                authorization,
                operation="checkpoint.append",
                binding=binding,
                candidate_attempt_id=None,
                payload_digest=digest,
                observed_at=observed_at,
            )
            existing = next((row for row in rows if row.row_sequence == row_sequence), None)
            if existing is not None:
                if existing.canonical_bytes != canonical:
                    raise FormalTaskViolation(
                        "DURABILITY_PREFIX_CONFLICT",
                        "changed checkpoint reuses an immutable sequence",
                        ErrorCode.CONFLICT,
                    )
                return self._verified_checkpoint_prefix(connection, binding)
            if row_sequence != len(rows) + 1:
                raise FormalTaskViolation(
                    "DURABILITY_PREFIX_PARTIAL",
                    "checkpoint append would create a partial prefix",
                    ErrorCode.STALE,
                )
            self._hit("durability.checkpoint.before_insert")
            connection.execute(
                """INSERT INTO durability_checkpoints(
                       task_id, producer_attempt_id, row_sequence, canonical,
                       payload_digest, created_at)
                   VALUES(?, ?, ?, ?, ?, ?)""",
                (
                    binding.task_id,
                    binding.origin_attempt_id,
                    row_sequence,
                    canonical,
                    digest,
                    observed_at,
                ),
            )
            self._hit("durability.checkpoint.after_insert")
            return self._verified_checkpoint_prefix(connection, binding)

    def read_durability_binding(
        self,
        *,
        scope: ScopeRef,
        task_id: str,
        origin_attempt_id: str,
    ) -> DurabilityReadBinding:
        """Return the exact persisted origin/profile binding for a durability read."""

        with self._snapshot_reader() as connection:
            return self._durability_binding_from_connection(
                connection,
                scope=scope,
                task_id=task_id,
                origin_attempt_id=origin_attempt_id,
            )

    def read_durability_checkpoints(
        self,
        binding: DurabilityReadBinding,
        *,
        expected_head: int | None = None,
        expected_prefix_digest: str | None = None,
    ) -> VerifiedCheckpointPrefix:
        with self._snapshot_reader() as connection:
            stored = self._durability_binding_from_connection(
                connection,
                scope=binding.scope,
                task_id=binding.task_id,
                origin_attempt_id=binding.origin_attempt_id,
            )
            if stored != binding:
                raise FormalTaskViolation(
                    "DURABILITY_BINDING_MISMATCH",
                    "checkpoint reader binding does not match persisted selection",
                    ErrorCode.CONFLICT,
                )
            return self._verified_checkpoint_prefix(
                connection,
                binding,
                expected_head=expected_head,
                expected_prefix_digest=expected_prefix_digest,
            )

    @staticmethod
    def _effect_actor(fact: EffectFact) -> tuple[str, int]:
        if hasattr(fact, "actor_attempt_id"):
            return fact.actor_attempt_id, fact.recovery_generation
        return fact.binding.origin_attempt_id, 0

    @classmethod
    def _require_effect_continuation(
        cls,
        connection: sqlite3.Connection,
        *,
        binding: DurabilityReadBinding,
        fact: EffectFact,
        current_effect_head: int,
        current_effect_digest: str,
    ) -> None:
        actor_attempt_id, generation = cls._effect_actor(fact)
        if actor_attempt_id == binding.logical_origin_attempt_id:
            if generation != 0:
                raise FormalTaskViolation(
                    "DURABILITY_BINDING_MISMATCH",
                    "origin effect actor cannot claim a recovery generation",
                    ErrorCode.CONFLICT,
                )
            return
        row = connection.execute(
            """SELECT * FROM durability_recoveries
               WHERE task_id=? AND recovery_attempt_id=?
                 AND recovery_generation=?""",
            (
                binding.task_id,
                actor_attempt_id,
                generation,
            ),
        ).fetchone()
        if row is None or row["profile_json"] != _json_dump(binding.profile.to_dict()):
            raise FormalTaskViolation(
                "DURABILITY_CONTINUATION_UNAUTHORIZED",
                "linked effect actor lacks Store continuation authorization",
                ErrorCode.PERMISSION_DENIED,
            )
        if type(fact) is EffectContinuationAuthorization and (
            fact.checkpoint_head != row["checkpoint_head"]
            or fact.checkpoint_prefix_digest != row["checkpoint_prefix_digest"]
            or fact.effect_head != current_effect_head
            or fact.effect_prefix_digest != current_effect_digest
        ):
            raise FormalTaskViolation(
                "DURABILITY_CONTINUATION_STALE",
                "effect continuation does not bind the authorized prefixes",
                ErrorCode.STALE,
            )

    def append_durability_effect_fact(
        self,
        fact: EffectFact,
        *,
        row_sequence: int,
        observed_at: str,
        authorization: DurabilityMutationAuthorization | None = None,
    ) -> VerifiedEffectPrefix:
        """Commit intent/facts before any caller-owned external dispatch."""

        canonical = effect_fact_bytes(fact)
        digest = hashlib.sha256(canonical).hexdigest()
        lineage_attempt_id = (
            authorization.producer_attempt_id
            if type(authorization) is DurabilityMutationAuthorization
            else fact.binding.origin_attempt_id
        )
        with self._transaction() as connection:
            stored = self._durability_binding_from_connection(
                connection,
                scope=fact.binding.scope,
                task_id=fact.binding.task_id,
                origin_attempt_id=lineage_attempt_id,
            )
            binding = stored
            if (
                fact.binding.scope != binding.scope
                or fact.binding.task_id != binding.task_id
                or fact.binding.origin_attempt_id != binding.logical_origin_attempt_id
                or fact.binding.profile != binding.profile
                or binding.profile.durability_level != "D2"
            ):
                raise FormalTaskViolation(
                    "DURABILITY_BINDING_MISMATCH",
                    "effect fact does not match one persisted D2 selection",
                    ErrorCode.CONFLICT,
                )
            rows = self._effect_rows(connection, binding)
            self._consume_durability_authorization(
                connection,
                authorization,
                operation="effect.append",
                binding=binding,
                candidate_attempt_id=None,
                payload_digest=digest,
                observed_at=observed_at,
            )
            existing = next((row for row in rows if row.row_sequence == row_sequence), None)
            if existing is not None:
                if existing.canonical_bytes != canonical:
                    raise FormalTaskViolation(
                        "DURABILITY_PREFIX_CONFLICT",
                        "changed effect fact reuses an immutable sequence",
                        ErrorCode.CONFLICT,
                    )
                return self._verified_effect_prefix(connection, binding)
            if type(row_sequence) is not int or row_sequence != len(rows) + 1:
                raise FormalTaskViolation(
                    "DURABILITY_PREFIX_PARTIAL",
                    "effect append would create a partial prefix",
                    ErrorCode.STALE,
                )
            current = self._verified_effect_prefix(connection, binding)
            self._require_effect_continuation(
                connection,
                binding=binding,
                fact=fact,
                current_effect_head=current.head,
                current_effect_digest=current.prefix_digest,
            )
            candidate = rows + (
                EffectPrefixRow(
                    row_sequence=row_sequence,
                    binding=binding,
                    canonical_bytes=canonical,
                    payload_digest=digest,
                ),
            )
            verified = verify_effect_prefix(
                candidate,
                expected_binding=binding,
                expected_head=row_sequence,
            )
            self._hit("durability.effect.before_insert")
            connection.execute(
                """INSERT INTO durability_effect_facts(
                       task_id, origin_attempt_id, row_sequence, canonical,
                       payload_digest, created_at)
                   VALUES(?, ?, ?, ?, ?, ?)""",
                (
                    binding.task_id,
                    binding.origin_attempt_id,
                    row_sequence,
                    canonical,
                    digest,
                    observed_at,
                ),
            )
            self._hit("durability.effect.after_insert")
            return verified

    def read_durability_effects(
        self,
        binding: DurabilityReadBinding,
        *,
        expected_head: int | None = None,
        expected_prefix_digest: str | None = None,
    ) -> VerifiedEffectPrefix:
        with self._snapshot_reader() as connection:
            stored = self._durability_binding_from_connection(
                connection,
                scope=binding.scope,
                task_id=binding.task_id,
                origin_attempt_id=binding.origin_attempt_id,
            )
            if stored != binding:
                raise FormalTaskViolation(
                    "DURABILITY_BINDING_MISMATCH",
                    "effect reader binding does not match persisted selection",
                    ErrorCode.CONFLICT,
                )
            return self._verified_effect_prefix(
                connection,
                binding,
                expected_head=expected_head,
                expected_prefix_digest=expected_prefix_digest,
            )

    def claim_durability_mutator(
        self,
        *,
        scope: ScopeRef,
        task_id: str,
        owner_id: str,
        observed_at: str,
        expires_at: str,
    ) -> tuple[str, int] | None:
        """Claim the Store fence; Direct runtime and OS fences remain separate."""

        if (
            type(owner_id) is not str
            or not owner_id.strip()
            or len(owner_id.encode("utf-8")) > 512
            or _utc_datetime(expires_at) <= _utc_datetime(observed_at)
        ):
            raise FormalTaskViolation(
                "INVALID_DURABILITY_LEASE",
                "durability lease facts are invalid",
                ErrorCode.INVALID_ARGUMENT,
            )
        with self._transaction() as connection:
            self._require_task_row(connection, task_id, scope)
            row = connection.execute(
                "SELECT * FROM durability_mutator_leases WHERE task_id=?",
                (task_id,),
            ).fetchone()
            if row is not None and _utc_datetime(row["expires_at"]) > _utc_datetime(observed_at):
                return None
            generation = 1 if row is None else row["claim_generation"] + 1
            token = f"durability-claim-{uuid.uuid4().hex}"
            self._hit("durability.lease.before_claim")
            connection.execute(
                """INSERT INTO durability_mutator_leases(
                       task_id, owner_id, claim_token, claim_generation,
                       claimed_at, expires_at)
                   VALUES(?, ?, ?, ?, ?, ?)
                   ON CONFLICT(task_id) DO UPDATE SET
                       owner_id=excluded.owner_id,
                       claim_token=excluded.claim_token,
                       claim_generation=excluded.claim_generation,
                       claimed_at=excluded.claimed_at,
                       expires_at=excluded.expires_at""",
                (task_id, owner_id, token, generation, observed_at, expires_at),
            )
            self._hit("durability.lease.after_claim")
            return token, generation

    def release_durability_mutator(
        self,
        *,
        scope: ScopeRef,
        task_id: str,
        owner_id: str,
        claim_token: str,
        claim_generation: int,
    ) -> bool:
        with self._transaction() as connection:
            self._require_task_row(connection, task_id, scope)
            changed = connection.execute(
                """DELETE FROM durability_mutator_leases
                   WHERE task_id=? AND owner_id=? AND claim_token=?
                     AND claim_generation=?""",
                (task_id, owner_id, claim_token, claim_generation),
            ).rowcount
            return changed == 1

    def _initialize(self) -> None:
        connection = self._connect(foreign_keys=False)
        try:
            connection.execute("BEGIN EXCLUSIVE")
            tables = {
                row["name"]
                for row in connection.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name NOT LIKE 'sqlite_%'
                    """
                ).fetchall()
            }
            task_store_tables = tables & _TASK_STORE_TABLES
            if not task_store_tables:
                self._create_schema_v6(connection)
                self._verify_schema_structure(connection, version=6)
                self._verify_database(connection)
                self._verify_v4_lineage(connection)
                self._verify_v4_semantics(connection)
                self._verify_v5_semantics(connection)
                self._verify_v6_semantics(connection)
                self._hit("initialize.bootstrap.before_metadata")
                connection.execute(
                    "INSERT INTO metadata(key, value) VALUES('schema_version', ?)",
                    (str(_SCHEMA_VERSION),),
                )
            else:
                if "metadata" not in task_store_tables:
                    raise FormalTaskViolation(
                        "TASK_STORE_SCHEMA_UNSUPPORTED",
                        "formal task Store has tables but no schema authority",
                        ErrorCode.UNSUPPORTED,
                    )
                self._verify_metadata_schema(connection)
                row = connection.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()
                if row is None:
                    raise FormalTaskViolation(
                        "TASK_STORE_SCHEMA_UNSUPPORTED",
                        "formal task Store schema version is unavailable",
                        ErrorCode.UNSUPPORTED,
                    )
                try:
                    version = int(row["value"])
                except (TypeError, ValueError) as error:
                    raise FormalTaskViolation(
                        "TASK_STORE_SCHEMA_UNSUPPORTED",
                        "formal task Store schema version is unsupported",
                        ErrorCode.UNSUPPORTED,
                    ) from error
                if version == 1:
                    self._verify_schema_structure(connection, version=1)
                    self._migrate_v1_to_v2(connection)
                    self._migrate_v2_to_v3(connection)
                    self._migrate_v3_to_v4(connection)
                    self._migrate_v4_to_v5(connection)
                    self._migrate_v5_to_v6(connection)
                elif version == 2:
                    self._verify_schema_structure(connection, version=2)
                    self._verify_database(connection)
                    self._migrate_v2_to_v3(connection)
                    self._migrate_v3_to_v4(connection)
                    self._migrate_v4_to_v5(connection)
                    self._migrate_v5_to_v6(connection)
                elif version == 3:
                    self._verify_schema_structure(connection, version=3)
                    self._verify_database(connection)
                    self._migrate_v3_to_v4(connection)
                    self._migrate_v4_to_v5(connection)
                    self._migrate_v5_to_v6(connection)
                elif version == 4:
                    self._verify_schema_structure(connection, version=4)
                    self._verify_database(connection)
                    self._verify_v4_lineage(connection)
                    self._verify_v4_semantics(connection)
                    self._migrate_v4_to_v5(connection)
                    self._migrate_v5_to_v6(connection)
                elif version == 5:
                    self._verify_schema_structure(connection, version=5)
                    self._verify_database(connection)
                    self._verify_v4_lineage(connection)
                    self._verify_v4_semantics(connection)
                    self._verify_v5_semantics(connection)
                    self._migrate_v5_to_v6(connection)
                elif version == _SCHEMA_VERSION:
                    try:
                        self._verify_schema_structure(connection, version=6)
                    except FormalTaskViolation:
                        self._verify_schema_structure(
                            connection,
                            version=6,
                            legacy_candidate_v6=True,
                        )
                        self._verify_database(connection)
                        self._verify_v4_lineage(connection)
                        self._verify_v4_semantics(connection)
                        self._verify_v5_semantics(connection)
                        self._verify_v6_semantics(connection)
                        self._normalize_legacy_candidate_v6(connection)
                        self._verify_schema_structure(connection, version=6)
                    self._verify_database(connection)
                    self._verify_v4_lineage(connection)
                    self._verify_v4_semantics(connection)
                    self._verify_v5_semantics(connection)
                    self._verify_v6_semantics(connection)
                else:
                    raise FormalTaskViolation(
                        "TASK_STORE_SCHEMA_UNSUPPORTED",
                        "formal task Store schema version is unsupported",
                        ErrorCode.UNSUPPORTED,
                    )
            connection.commit()
        except sqlite3.Error as exc:
            try:
                connection.rollback()
            except BaseException:  # noqa: BLE001 -- preserve the stable primary error
                pass
            raise FormalTaskViolation(
                "TASK_STORE_UNAVAILABLE",
                "formal Task Store schema cannot be initialized",
                ErrorCode.UNAVAILABLE,
            ) from exc
        except BaseException:
            try:
                connection.rollback()
            except BaseException:  # noqa: BLE001 -- rollback cannot replace authority truth
                pass
            raise
        finally:
            connection.close()

        journal_connection = self._connect()
        try:
            self._enable_wal_journal_mode(journal_connection)
        except sqlite3.Error as exc:
            raise FormalTaskViolation(
                "TASK_STORE_UNAVAILABLE",
                "formal Task Store journal cannot be initialized",
                ErrorCode.UNAVAILABLE,
            ) from exc
        finally:
            journal_connection.close()

    @staticmethod
    def _enable_wal_journal_mode(connection: sqlite3.Connection) -> None:
        """Converge concurrent initializers on WAL without accepting lock races."""
        deadline = time.monotonic() + _JOURNAL_MODE_RETRY_SECONDS
        while True:
            try:
                row = connection.execute("PRAGMA journal_mode = WAL").fetchone()
                if row is None or str(row[0]).lower() != "wal":
                    raise sqlite3.OperationalError("formal Task Store journal mode did not converge on WAL")
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or time.monotonic() >= deadline:
                    raise
                time.sleep(_JOURNAL_MODE_RETRY_INTERVAL_SECONDS)

    @staticmethod
    def _schema_unsupported(message: str) -> FormalTaskViolation:
        return FormalTaskViolation(
            "TASK_STORE_SCHEMA_UNSUPPORTED",
            message,
            ErrorCode.UNSUPPORTED,
        )

    @classmethod
    def _verify_metadata_schema(cls, connection: sqlite3.Connection) -> None:
        rows = connection.execute("PRAGMA table_info(metadata)").fetchall()
        if tuple(row["name"] for row in rows) != _TASK_STORE_COLUMNS["metadata"]:
            raise cls._schema_unsupported("formal task Store metadata schema is unsupported")
        if tuple(row["type"].upper() for row in rows) != ("TEXT", "TEXT"):
            raise cls._schema_unsupported("formal task Store metadata schema is unsupported")
        if {row["name"] for row in rows if bool(row["notnull"])} != (_TASK_STORE_NOT_NULL["metadata"]):
            raise cls._schema_unsupported("formal task Store metadata schema is unsupported")
        primary_key = tuple(row["name"] for row in sorted(rows, key=lambda item: item["pk"]) if row["pk"])
        if primary_key != _TASK_STORE_PRIMARY_KEYS["metadata"]:
            raise cls._schema_unsupported("formal task Store metadata schema is unsupported")

    @classmethod
    def _verify_schema_structure(
        cls,
        connection: sqlite3.Connection,
        *,
        version: int,
        legacy_candidate_v6: bool = False,
    ) -> None:
        tables = {
            row["name"]
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                """
            ).fetchall()
        }
        expected_tables = (
            _TASK_STORE_TABLES
            if version >= 6
            else _TASK_STORE_TABLES_V5
            if version >= 5
            else _TASK_STORE_TABLES_V4
            if version >= 3
            else _TASK_STORE_TABLES_V2
        )
        if tables & _TASK_STORE_TABLES != expected_tables:
            raise cls._schema_unsupported("formal task Store schema is missing required tables")

        unique_keys = dict(
            _TASK_STORE_UNIQUE_KEYS_V6
            if version >= 6
            else _TASK_STORE_UNIQUE_KEYS_V5
            if version >= 5
            else _TASK_STORE_UNIQUE_KEYS_V4
            if version >= 4
            else _TASK_STORE_UNIQUE_KEYS_V3
        )
        foreign_keys = (
            _TASK_STORE_FOREIGN_KEYS_V6
            if version >= 6
            else _TASK_STORE_FOREIGN_KEYS_V4
            if version >= 4
            else _TASK_STORE_FOREIGN_KEYS_V3
        )
        named_indexes = (
            _TASK_STORE_NAMED_INDEXES_V6
            if version >= 6
            else _TASK_STORE_NAMED_INDEXES_V5
            if version >= 5
            else _TASK_STORE_NAMED_INDEXES_V4
            if version >= 4
            else _TASK_STORE_NAMED_INDEXES_V3
        )
        if version == 1:
            unique_keys["attempts"] = frozenset({("attempt_id",), ("task_id",)})
        if legacy_candidate_v6:
            if version != 6:
                raise cls._schema_unsupported("legacy candidate normalization requires schema v6")
            unique_keys["durability_recovery_fences"] = _LEGACY_CANDIDATE_V6_RECOVERY_FENCE_UNIQUE_KEYS
        for table in sorted(expected_tables):
            rows = connection.execute(f"PRAGMA table_info({table})").fetchall()
            expected_columns = _TASK_STORE_COLUMNS[table]
            if version < 5 and table == "attempts":
                expected_columns = expected_columns[:9]
            if version < 4 and table == "tasks":
                expected_columns = tuple(
                    column
                    for column in expected_columns
                    if column
                    not in {
                        "create_command_id",
                        "predecessor_task_id",
                        "revision_number",
                    }
                )
            if version == 1 and table == "attempts":
                expected_columns = tuple(column for column in expected_columns if column != "attempt_number")
            if tuple(row["name"] for row in rows) != expected_columns:
                raise cls._schema_unsupported(f"formal task Store {table} columns are unsupported")
            for row in rows:
                column = row["name"]
                expected_type = (
                    "INTEGER"
                    if (table, column) in _TASK_STORE_INTEGER_COLUMNS
                    else "BLOB"
                    if (table, column) in _TASK_STORE_BLOB_COLUMNS
                    else "TEXT"
                )
                if row["type"].upper() != expected_type:
                    raise cls._schema_unsupported(f"formal task Store {table} column types are unsupported")
                expected_default = _TASK_STORE_DEFAULTS.get((table, column))
                actual_default = row["dflt_value"]
                if actual_default != expected_default:
                    raise cls._schema_unsupported(f"formal task Store {table} defaults are unsupported")
            expected_not_null = _TASK_STORE_NOT_NULL[table]
            if version < 4 and table == "tasks":
                expected_not_null = expected_not_null - {
                    "create_command_id",
                    "revision_number",
                }
            if version == 1 and table == "attempts":
                expected_not_null = expected_not_null - {"attempt_number"}
            if {row["name"] for row in rows if bool(row["notnull"])} != expected_not_null:
                raise cls._schema_unsupported(f"formal task Store {table} nullability is unsupported")
            primary_key = tuple(row["name"] for row in sorted(rows, key=lambda item: item["pk"]) if row["pk"])
            if primary_key != _TASK_STORE_PRIMARY_KEYS[table]:
                raise cls._schema_unsupported(f"formal task Store {table} primary key is unsupported")

            actual_unique_keys: set[tuple[str, ...]] = set()
            for index in connection.execute(f"PRAGMA index_list({table})").fetchall():
                if not bool(index["unique"]):
                    continue
                columns = tuple(
                    row["name"] for row in connection.execute(f"PRAGMA index_info({index['name']})").fetchall()
                )
                actual_unique_keys.add(columns)
            if actual_unique_keys != unique_keys[table]:
                raise cls._schema_unsupported(f"formal task Store {table} uniqueness is unsupported")

            foreign_key_rows = connection.execute(f"PRAGMA foreign_key_list({table})").fetchall()
            if table == "task_event_consumption":
                grouped: dict[int, list[sqlite3.Row]] = {}
                for row in foreign_key_rows:
                    grouped.setdefault(int(row["id"]), []).append(row)
                actual_foreign_keys = frozenset(
                    (
                        tuple(item["from"] for item in sorted(rows, key=lambda item: item["seq"])),
                        rows[0]["table"],
                        tuple(item["to"] for item in sorted(rows, key=lambda item: item["seq"])),
                        rows[0]["on_delete"].upper(),
                    )
                    for rows in grouped.values()
                )
                expected_foreign_keys = _TASK_EVENT_CONSUMPTION_FOREIGN_KEYS
            else:
                actual_foreign_keys = frozenset(
                    (
                        row["from"],
                        row["table"],
                        row["to"],
                        row["on_delete"].upper(),
                    )
                    for row in foreign_key_rows
                )
                expected_foreign_keys = foreign_keys[table]
            if actual_foreign_keys != expected_foreign_keys:
                raise cls._schema_unsupported(f"formal task Store {table} foreign keys are unsupported")

        for index_name, (table, expected_columns) in named_indexes.items():
            if table not in expected_tables:
                continue
            indexes = {row["name"]: row for row in connection.execute(f"PRAGMA index_list({table})").fetchall()}
            index = indexes.get(index_name)
            if index is None or bool(index["unique"]) or bool(index["partial"]):
                raise cls._schema_unsupported(f"formal task Store index {index_name} is unsupported")
            columns = tuple(row["name"] for row in connection.execute(f"PRAGMA index_info({index_name})").fetchall())
            if columns != expected_columns:
                raise cls._schema_unsupported(f"formal task Store index {index_name} is unsupported")

        if version >= 2:
            attempt_sql_row = connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='attempts'"
            ).fetchone()
            attempt_sql = "" if attempt_sql_row is None else attempt_sql_row["sql"]
            normalized = "".join(str(attempt_sql).upper().split())
            if "CHECK(ATTEMPT_NUMBERBETWEEN1AND3)" not in normalized:
                raise cls._schema_unsupported("formal task Store attempt bounds are unsupported")
            if version >= 5 and "CHECK(ADMISSION_ATTEMPT_COUNT>=0)" not in normalized:
                raise cls._schema_unsupported("formal task Store admission bounds are unsupported")

        if version >= 4:
            task_sql_row = connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='tasks'"
            ).fetchone()
            task_sql = "" if task_sql_row is None else task_sql_row["sql"]
            normalized = "".join(str(task_sql).upper().split())
            if "CHECK(REVISION_NUMBERBETWEEN1AND1000000)" not in normalized:
                raise cls._schema_unsupported("formal task Store revision bounds are unsupported")

        if version >= 5:
            consumption_sql_row = connection.execute(
                """SELECT sql FROM sqlite_master
                   WHERE type='table' AND name='task_event_consumption'"""
            ).fetchone()
            consumption_sql = "" if consumption_sql_row is None else consumption_sql_row["sql"]
            normalized = "".join(str(consumption_sql).upper().split())
            required_checks = {
                "CHECK(PRESENTATION_CLASSIN('TEXT','VOICE'))",
                "CHECK(ACKED_THROUGH_SEQ>=0)",
            }
            if any(fragment not in normalized for fragment in required_checks):
                raise cls._schema_unsupported("formal task Store consumer bounds are unsupported")

        if legacy_candidate_v6:
            cls._verify_legacy_candidate_v6_recovery_fence(connection)

    @classmethod
    def _verify_legacy_candidate_v6_recovery_fence(
        cls,
        connection: sqlite3.Connection,
    ) -> None:
        row = connection.execute(
            """SELECT sql FROM sqlite_master
               WHERE type='table' AND name='durability_recovery_fences'"""
        ).fetchone()
        sql = "" if row is None else str(row["sql"])
        normalized_sql = "".join(sql.upper().split()).replace(
            'CREATETABLE"DURABILITY_RECOVERY_FENCES"',
            "CREATETABLEDURABILITY_RECOVERY_FENCES",
            1,
        )
        if normalized_sql != _LEGACY_CANDIDATE_V6_RECOVERY_FENCE_SQL:
            raise cls._schema_unsupported("legacy candidate v6 recovery fence definition is unsupported")

        index_rows = connection.execute("PRAGMA index_list(durability_recovery_fences)").fetchall()
        indexes = frozenset(
            (
                str(index["origin"]),
                bool(index["unique"]),
                bool(index["partial"]),
                tuple(
                    column["name"] for column in connection.execute(f"PRAGMA index_info({index['name']})").fetchall()
                ),
            )
            for index in index_rows
        )
        if (
            len(index_rows) != len(_LEGACY_CANDIDATE_V6_RECOVERY_FENCE_INDEXES)
            or indexes != _LEGACY_CANDIDATE_V6_RECOVERY_FENCE_INDEXES
        ):
            raise cls._schema_unsupported("legacy candidate v6 recovery fence indexes are unsupported")

    @staticmethod
    def _create_schema_v6(connection: sqlite3.Connection) -> None:
        statements = (
            "CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
            """CREATE TABLE commands (
                command_id TEXT NOT NULL, fingerprint BLOB NOT NULL,
                command_type TEXT NOT NULL, scope_key TEXT NOT NULL,
                result_json TEXT NOT NULL, created_at TEXT NOT NULL,
                PRIMARY KEY(scope_key, command_id))""",
            """CREATE TABLE tasks (
                task_id TEXT PRIMARY KEY, scope_key TEXT NOT NULL,
                scope_json TEXT NOT NULL, spec_json TEXT NOT NULL,
                state TEXT NOT NULL, outcome TEXT,
                attempt_id TEXT NOT NULL UNIQUE, correlation_id TEXT NOT NULL,
                cancel_requested INTEGER NOT NULL DEFAULT 0,
                dispatch_fenced INTEGER NOT NULL DEFAULT 0,
                event_head INTEGER NOT NULL, reconciliation_state TEXT,
                reconciliation_reason TEXT, created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                create_command_id TEXT NOT NULL DEFAULT '',
                predecessor_task_id TEXT UNIQUE
                    REFERENCES tasks(task_id) ON DELETE RESTRICT,
                revision_number INTEGER NOT NULL DEFAULT 1
                    CHECK(revision_number BETWEEN 1 AND 1000000))""",
            "CREATE INDEX idx_tasks_scope ON tasks(scope_key, task_id)",
            "CREATE INDEX idx_tasks_state ON tasks(state, task_id)",
            """CREATE INDEX idx_tasks_scope_page
                ON tasks(scope_key, created_at, task_id)""",
            """CREATE TABLE attempts (
                attempt_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                attempt_number INTEGER NOT NULL CHECK(attempt_number BETWEEN 1 AND 3),
                executor_id TEXT NOT NULL, executor_ref TEXT,
                state TEXT NOT NULL, outcome TEXT,
                source_seq INTEGER NOT NULL DEFAULT -1, updated_at TEXT NOT NULL,
                adapter_id TEXT, capability_profile_json TEXT,
                capability_profile_digest TEXT,
                execution_requirements_json TEXT, admission_priority TEXT,
                admission_reason TEXT,
                admission_attempt_count INTEGER
                    CHECK(admission_attempt_count >= 0),
                admission_next_eligible_at TEXT, admission_deadline_at TEXT,
                admission_enqueued_at TEXT,
                UNIQUE(task_id, attempt_number))""",
            """CREATE TABLE task_events (
                task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                seq INTEGER NOT NULL, event_id TEXT NOT NULL UNIQUE,
                attempt_id TEXT NOT NULL, scope_json TEXT NOT NULL,
                event_type TEXT NOT NULL, state TEXT NOT NULL, outcome TEXT,
                producer TEXT NOT NULL, source_event_id TEXT,
                causation_id TEXT NOT NULL, correlation_id TEXT NOT NULL,
                occurred_at TEXT NOT NULL, details_json TEXT NOT NULL,
                PRIMARY KEY(task_id, seq))""",
            """CREATE UNIQUE INDEX uq_task_events_exact
                ON task_events(task_id, seq, event_id)""",
            """CREATE TABLE executor_events (
                source_event_id TEXT PRIMARY KEY,
                attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                source_seq INTEGER NOT NULL, canonical BLOB NOT NULL,
                UNIQUE(attempt_id, source_seq))""",
            """CREATE TABLE outbox (
                outbox_id TEXT PRIMARY KEY,
                kind TEXT NOT NULL, task_id TEXT NOT NULL
                    REFERENCES tasks(task_id) ON DELETE CASCADE,
                attempt_id TEXT NOT NULL
                    REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                command_id TEXT NOT NULL, payload_json TEXT NOT NULL,
                state TEXT NOT NULL, delivery_count INTEGER NOT NULL DEFAULT 0,
                claimed_by TEXT, claimed_at TEXT, claim_token TEXT,
                last_error TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""",
            "CREATE INDEX idx_outbox_pending ON outbox(state, created_at, outbox_id)",
            """CREATE TABLE current_background_tasks (
                scope_key TEXT NOT NULL, session_id TEXT NOT NULL,
                task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE RESTRICT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(scope_key, session_id))""",
            """CREATE TABLE task_results (
                task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                attempt_id TEXT NOT NULL
                    REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                source_event_id TEXT NOT NULL
                    REFERENCES executor_events(source_event_id) ON DELETE CASCADE,
                result_text TEXT NOT NULL, artifacts_json TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                PRIMARY KEY(task_id, attempt_id, source_event_id))""",
            """CREATE INDEX idx_task_results_task
                ON task_results(task_id, attempt_id, completed_at)""",
            """CREATE TABLE task_event_consumption (
                subject_id TEXT NOT NULL, project_id TEXT NOT NULL,
                task_id TEXT NOT NULL, presentation_class TEXT NOT NULL
                    CHECK(presentation_class IN ('text', 'voice')),
                acked_through_seq INTEGER NOT NULL CHECK(acked_through_seq >= 0),
                acked_event_id TEXT NOT NULL, updated_at TEXT NOT NULL,
                PRIMARY KEY(subject_id, project_id, task_id, presentation_class),
                FOREIGN KEY(task_id, acked_through_seq, acked_event_id)
                    REFERENCES task_events(task_id, seq, event_id)
                    ON DELETE CASCADE)""",
            """CREATE INDEX idx_attempts_admission ON attempts(
                state, admission_next_eligible_at, admission_deadline_at,
                admission_priority, admission_enqueued_at, attempt_id)""",
            """CREATE INDEX idx_task_event_consumption_event
                ON task_event_consumption(
                    task_id, acked_through_seq, acked_event_id)""",
            """CREATE TABLE durability_checkpoints (
                task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                producer_attempt_id TEXT NOT NULL
                    REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                row_sequence INTEGER NOT NULL CHECK(row_sequence > 0),
                canonical BLOB NOT NULL, payload_digest TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(task_id, producer_attempt_id, row_sequence))""",
            """CREATE TABLE durability_effect_facts (
                task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                origin_attempt_id TEXT NOT NULL
                    REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                row_sequence INTEGER NOT NULL CHECK(row_sequence > 0),
                canonical BLOB NOT NULL, payload_digest TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(task_id, origin_attempt_id, row_sequence))""",
            """CREATE TABLE durability_recoveries (
                recovery_id TEXT NOT NULL PRIMARY KEY,
                task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                producer_attempt_id TEXT NOT NULL
                    REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                recovery_attempt_id TEXT NOT NULL UNIQUE
                    REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                recovery_generation INTEGER NOT NULL
                    CHECK(recovery_generation > 0),
                profile_json TEXT NOT NULL,
                checkpoint_head INTEGER NOT NULL CHECK(checkpoint_head > 0),
                checkpoint_prefix_digest TEXT NOT NULL,
                effect_head INTEGER NOT NULL CHECK(effect_head >= 0),
                effect_prefix_digest TEXT NOT NULL,
                recovery_facts BLOB NOT NULL, created_at TEXT NOT NULL)""",
            """CREATE INDEX idx_durability_recoveries_producer
                ON durability_recoveries(
                    task_id, producer_attempt_id, recovery_generation)""",
            """CREATE INDEX idx_durability_effect_facts_origin
                ON durability_effect_facts(
                    task_id, origin_attempt_id, row_sequence)""",
            """CREATE TABLE durability_mutator_leases (
                task_id TEXT NOT NULL PRIMARY KEY
                    REFERENCES tasks(task_id) ON DELETE CASCADE,
                owner_id TEXT NOT NULL, claim_token TEXT NOT NULL,
                claim_generation INTEGER NOT NULL CHECK(claim_generation > 0),
                claimed_at TEXT NOT NULL, expires_at TEXT NOT NULL)""",
            """CREATE TABLE durability_recovery_fences (
                task_id TEXT NOT NULL PRIMARY KEY
                    REFERENCES tasks(task_id) ON DELETE CASCADE,
                producer_attempt_id TEXT NOT NULL
                    REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                cancel_command_id TEXT NOT NULL,
                created_at TEXT NOT NULL)""",
        )
        for statement in statements:
            connection.execute(statement)

    def _migrate_v1_to_v2(self, connection: sqlite3.Connection) -> None:
        self._hit("migration.v1_to_v2.before_create")
        connection.execute(
            """CREATE TABLE attempts_v2 (
                attempt_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                attempt_number INTEGER NOT NULL CHECK(attempt_number BETWEEN 1 AND 3),
                executor_id TEXT NOT NULL, executor_ref TEXT,
                state TEXT NOT NULL, outcome TEXT,
                source_seq INTEGER NOT NULL DEFAULT -1, updated_at TEXT NOT NULL,
                UNIQUE(task_id, attempt_number))"""
        )
        self._hit("migration.v1_to_v2.after_create")
        connection.execute(
            """INSERT INTO attempts_v2(
                attempt_id, task_id, attempt_number, executor_id, executor_ref,
                state, outcome, source_seq, updated_at)
                SELECT attempt_id, task_id, 1, executor_id, executor_ref,
                       state, outcome, source_seq, updated_at FROM attempts"""
        )
        self._hit("migration.v1_to_v2.after_copy")
        connection.execute("DROP TABLE attempts")
        self._hit("migration.v1_to_v2.after_drop")
        connection.execute("ALTER TABLE attempts_v2 RENAME TO attempts")
        self._hit("migration.v1_to_v2.after_rename")
        self._verify_schema_structure(connection, version=2)
        self._verify_database(connection)
        self._hit("migration.v1_to_v2.before_metadata")
        changed = connection.execute(
            "UPDATE metadata SET value=? WHERE key='schema_version' AND value='1'",
            ("2",),
        ).rowcount
        if changed != 1:
            raise FormalTaskViolation(
                "TASK_STORE_SCHEMA_UNSUPPORTED",
                "formal task Store schema changed during migration",
                ErrorCode.UNSUPPORTED,
            )

    def _migrate_v2_to_v3(self, connection: sqlite3.Connection) -> None:
        self._hit("migration.v2_to_v3.before_create")
        connection.execute(
            """CREATE TABLE current_background_tasks (
                scope_key TEXT NOT NULL, session_id TEXT NOT NULL,
                task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE RESTRICT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(scope_key, session_id))"""
        )
        connection.execute(
            """CREATE TABLE task_results (
                task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                attempt_id TEXT NOT NULL
                    REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                source_event_id TEXT NOT NULL
                    REFERENCES executor_events(source_event_id) ON DELETE CASCADE,
                result_text TEXT NOT NULL, artifacts_json TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                PRIMARY KEY(task_id, attempt_id, source_event_id))"""
        )
        connection.execute(
            """CREATE INDEX idx_task_results_task
                ON task_results(task_id, attempt_id, completed_at)"""
        )
        self._hit("migration.v2_to_v3.after_create")
        self._verify_schema_structure(connection, version=3)
        self._verify_database(connection)
        changed = connection.execute("UPDATE metadata SET value='3' WHERE key='schema_version' AND value='2'").rowcount
        if changed != 1:
            raise FormalTaskViolation(
                "TASK_STORE_SCHEMA_UNSUPPORTED",
                "formal task Store changed during v3 migration",
                ErrorCode.UNSUPPORTED,
            )

    def _migrate_v3_to_v4(self, connection: sqlite3.Connection) -> None:
        """Add explicit revision/create lineage without rewriting lifecycle truth."""

        self._hit("migration.v3_to_v4.before_columns")
        connection.execute("ALTER TABLE tasks ADD COLUMN create_command_id TEXT NOT NULL DEFAULT ''")
        connection.execute(
            """ALTER TABLE tasks ADD COLUMN predecessor_task_id TEXT
               REFERENCES tasks(task_id) ON DELETE RESTRICT"""
        )
        connection.execute(
            """ALTER TABLE tasks ADD COLUMN revision_number INTEGER NOT NULL DEFAULT 1
               CHECK(revision_number BETWEEN 1 AND 1000000)"""
        )
        self._hit("migration.v3_to_v4.after_columns")

        lineage_rows = connection.execute(
            """
            SELECT t.task_id, t.scope_key, e.causation_id AS create_command_id,
                   e.event_type, e.state AS accepted_state,
                   e.outcome AS accepted_outcome, e.producer,
                   c.command_type, c.result_json
            FROM tasks AS t
            LEFT JOIN task_events AS e
              ON e.task_id=t.task_id AND e.seq=0
            LEFT JOIN commands AS c
              ON c.scope_key=t.scope_key AND c.command_id=e.causation_id
            ORDER BY t.task_id
            """
        ).fetchall()
        for row in lineage_rows:
            try:
                command_result = _json_load(row["result_json"])
            except (FormalTaskViolation, TypeError):
                command_result = None
            if (
                type(row["create_command_id"]) is not str
                or not row["create_command_id"]
                or row["event_type"] != "task.accepted"
                or row["accepted_state"] != FormalTaskState.ACCEPTED.value
                or row["accepted_outcome"] is not None
                or row["producer"] != "task_core"
                or row["command_type"] != "task.create"
                or type(command_result) is not dict
                or command_result.get("ok") is not True
                or type(command_result.get("result")) is not dict
                or command_result["result"].get("task_id") != row["task_id"]
            ):
                raise self._schema_unsupported("formal task Store v3 create lineage is corrupt")

        self._hit("migration.v3_to_v4.before_backfill")
        connection.execute(
            """
            UPDATE tasks
            SET create_command_id=(
                SELECT e.causation_id FROM task_events AS e
                WHERE e.task_id=tasks.task_id AND e.seq=0
            )
            """
        )
        self._hit("migration.v3_to_v4.after_backfill")
        connection.execute(
            """CREATE UNIQUE INDEX uq_tasks_predecessor
               ON tasks(predecessor_task_id)"""
        )
        connection.execute(
            """CREATE INDEX idx_tasks_scope_page
               ON tasks(scope_key, created_at, task_id)"""
        )
        self._hit("migration.v3_to_v4.after_indexes")
        self._verify_schema_structure(connection, version=4)
        self._verify_database(connection)
        self._verify_v4_lineage(connection)
        self._verify_v4_semantics(connection)
        self._hit("migration.v3_to_v4.before_metadata")
        changed = connection.execute("UPDATE metadata SET value='4' WHERE key='schema_version' AND value='3'").rowcount
        if changed != 1:
            raise FormalTaskViolation(
                "TASK_STORE_SCHEMA_UNSUPPORTED",
                "formal task Store changed during v4 migration",
                ErrorCode.UNSUPPORTED,
            )

    def _migrate_v4_to_v5(self, connection: sqlite3.Connection) -> None:
        """Add historical-nullable admission facts and the sole consumer table."""

        self._hit("migration.v4_to_v5.before_columns")
        column_definitions = (
            "adapter_id TEXT",
            "capability_profile_json TEXT",
            "capability_profile_digest TEXT",
            "execution_requirements_json TEXT",
            "admission_priority TEXT",
            "admission_reason TEXT",
            "admission_attempt_count INTEGER CHECK(admission_attempt_count >= 0)",
            "admission_next_eligible_at TEXT",
            "admission_deadline_at TEXT",
            "admission_enqueued_at TEXT",
        )
        for definition in column_definitions:
            connection.execute(f"ALTER TABLE attempts ADD COLUMN {definition}")
        self._hit("migration.v4_to_v5.after_columns")

        connection.execute(
            """CREATE UNIQUE INDEX uq_task_events_exact
               ON task_events(task_id, seq, event_id)"""
        )
        connection.execute(
            """CREATE TABLE task_event_consumption (
                subject_id TEXT NOT NULL, project_id TEXT NOT NULL,
                task_id TEXT NOT NULL, presentation_class TEXT NOT NULL
                    CHECK(presentation_class IN ('text', 'voice')),
                acked_through_seq INTEGER NOT NULL CHECK(acked_through_seq >= 0),
                acked_event_id TEXT NOT NULL, updated_at TEXT NOT NULL,
                PRIMARY KEY(subject_id, project_id, task_id, presentation_class),
                FOREIGN KEY(task_id, acked_through_seq, acked_event_id)
                    REFERENCES task_events(task_id, seq, event_id)
                    ON DELETE CASCADE)"""
        )
        self._hit("migration.v4_to_v5.after_consumption")
        connection.execute(
            """CREATE INDEX idx_attempts_admission ON attempts(
                state, admission_next_eligible_at, admission_deadline_at,
                admission_priority, admission_enqueued_at, attempt_id)"""
        )
        connection.execute(
            """CREATE INDEX idx_task_event_consumption_event
               ON task_event_consumption(
                   task_id, acked_through_seq, acked_event_id)"""
        )
        self._hit("migration.v4_to_v5.after_indexes")

        self._verify_schema_structure(connection, version=5)
        self._verify_database(connection)
        self._verify_v4_lineage(connection)
        self._verify_v4_semantics(connection)
        self._verify_v5_semantics(connection)
        self._hit("migration.v4_to_v5.before_metadata")
        changed = connection.execute("UPDATE metadata SET value='5' WHERE key='schema_version' AND value='4'").rowcount
        if changed != 1:
            raise FormalTaskViolation(
                "TASK_STORE_SCHEMA_UNSUPPORTED",
                "formal task Store changed during v5 migration",
                ErrorCode.UNSUPPORTED,
            )

    def _migrate_v5_to_v6(self, connection: sqlite3.Connection) -> None:
        """Add the sole Store-owned durability ledger and recovery fence."""

        self._hit("migration.v5_to_v6.before_create")
        statements = (
            """CREATE TABLE durability_checkpoints (
                task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                producer_attempt_id TEXT NOT NULL
                    REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                row_sequence INTEGER NOT NULL CHECK(row_sequence > 0),
                canonical BLOB NOT NULL, payload_digest TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(task_id, producer_attempt_id, row_sequence))""",
            """CREATE TABLE durability_effect_facts (
                task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                origin_attempt_id TEXT NOT NULL
                    REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                row_sequence INTEGER NOT NULL CHECK(row_sequence > 0),
                canonical BLOB NOT NULL, payload_digest TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(task_id, origin_attempt_id, row_sequence))""",
            """CREATE TABLE durability_recoveries (
                recovery_id TEXT NOT NULL PRIMARY KEY,
                task_id TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
                producer_attempt_id TEXT NOT NULL
                    REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                recovery_attempt_id TEXT NOT NULL UNIQUE
                    REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                recovery_generation INTEGER NOT NULL
                    CHECK(recovery_generation > 0),
                profile_json TEXT NOT NULL,
                checkpoint_head INTEGER NOT NULL CHECK(checkpoint_head > 0),
                checkpoint_prefix_digest TEXT NOT NULL,
                effect_head INTEGER NOT NULL CHECK(effect_head >= 0),
                effect_prefix_digest TEXT NOT NULL,
                recovery_facts BLOB NOT NULL, created_at TEXT NOT NULL)""",
            """CREATE TABLE durability_mutator_leases (
                task_id TEXT NOT NULL PRIMARY KEY
                    REFERENCES tasks(task_id) ON DELETE CASCADE,
                owner_id TEXT NOT NULL, claim_token TEXT NOT NULL,
                claim_generation INTEGER NOT NULL CHECK(claim_generation > 0),
                claimed_at TEXT NOT NULL, expires_at TEXT NOT NULL)""",
            """CREATE TABLE durability_recovery_fences (
                task_id TEXT NOT NULL PRIMARY KEY
                    REFERENCES tasks(task_id) ON DELETE CASCADE,
                producer_attempt_id TEXT NOT NULL
                    REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                cancel_command_id TEXT NOT NULL,
                created_at TEXT NOT NULL)""",
            """CREATE INDEX idx_durability_recoveries_producer
                ON durability_recoveries(
                    task_id, producer_attempt_id, recovery_generation)""",
            """CREATE INDEX idx_durability_effect_facts_origin
                ON durability_effect_facts(
                    task_id, origin_attempt_id, row_sequence)""",
        )
        for statement in statements:
            connection.execute(statement)
        self._hit("migration.v5_to_v6.after_create")
        self._verify_schema_structure(connection, version=6)
        self._verify_database(connection)
        self._verify_v4_lineage(connection)
        self._verify_v4_semantics(connection)
        self._verify_v5_semantics(connection)
        self._verify_v6_semantics(connection)
        self._hit("migration.v5_to_v6.before_metadata")
        changed = connection.execute("UPDATE metadata SET value='6' WHERE key='schema_version' AND value='5'").rowcount
        if changed != 1:
            raise FormalTaskViolation(
                "TASK_STORE_SCHEMA_UNSUPPORTED",
                "formal task Store changed during v6 migration",
                ErrorCode.UNSUPPORTED,
            )

    def _normalize_legacy_candidate_v6(self, connection: sqlite3.Connection) -> None:
        """Remove the released candidate's cross-scope cancel uniqueness only."""

        replacement = "durability_recovery_fences_v6_final"
        self._hit("migration.legacy_v6_to_final_v6.before_replace")
        connection.execute(
            f"""CREATE TABLE {replacement} (
                task_id TEXT NOT NULL PRIMARY KEY
                    REFERENCES tasks(task_id) ON DELETE CASCADE,
                producer_attempt_id TEXT NOT NULL
                    REFERENCES attempts(attempt_id) ON DELETE CASCADE,
                cancel_command_id TEXT NOT NULL,
                created_at TEXT NOT NULL)"""
        )
        connection.execute(
            f"""INSERT INTO {replacement}(
                       task_id, producer_attempt_id, cancel_command_id, created_at)
                   SELECT task_id, producer_attempt_id, cancel_command_id, created_at
                   FROM durability_recovery_fences"""
        )
        connection.execute("DROP TABLE durability_recovery_fences")
        connection.execute(f"ALTER TABLE {replacement} RENAME TO durability_recovery_fences")
        self._hit("migration.legacy_v6_to_final_v6.after_replace")

    @staticmethod
    def _verify_database(connection: sqlite3.Connection) -> None:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        if integrity is None or integrity[0] != "ok":
            raise FormalTaskViolation(
                "TASK_STORE_CORRUPT",
                "formal Task Store failed its integrity check",
                ErrorCode.INTERNAL,
            )
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise FormalTaskViolation(
                "TASK_STORE_CORRUPT",
                "formal Task Store failed its foreign-key check",
                ErrorCode.INTERNAL,
            )

    @classmethod
    def _verify_v4_lineage(cls, connection: sqlite3.Connection) -> None:
        """Fail closed when persisted revision/create relationships are ambiguous."""

        rows = connection.execute(
            """
            SELECT t.task_id, t.scope_key, t.state, t.create_command_id,
                   t.predecessor_task_id, t.revision_number,
                   e.causation_id AS accepted_command_id,
                   e.state AS accepted_state,
                   e.outcome AS accepted_outcome,
                   e.producer AS accepted_producer,
                   c.command_type, c.result_json,
                   p.scope_key AS predecessor_scope_key,
                   p.state AS predecessor_state,
                   p.revision_number AS predecessor_revision_number
            FROM tasks AS t
            LEFT JOIN task_events AS e
              ON e.task_id=t.task_id AND e.seq=0 AND e.event_type='task.accepted'
            LEFT JOIN commands AS c
              ON c.scope_key=t.scope_key AND c.command_id=t.create_command_id
            LEFT JOIN tasks AS p ON p.task_id=t.predecessor_task_id
            ORDER BY t.task_id
            """
        ).fetchall()
        for row in rows:
            try:
                command_result = _json_load(row["result_json"])
            except (FormalTaskViolation, TypeError):
                command_result = None
            predecessor_id = row["predecessor_task_id"]
            revision_number = row["revision_number"]
            base_revision = predecessor_id is None and revision_number == 1
            successor_revision = (
                type(predecessor_id) is str
                and bool(predecessor_id)
                and predecessor_id != row["task_id"]
                and type(revision_number) is int
                and revision_number > 1
                and row["predecessor_scope_key"] == row["scope_key"]
                and row["predecessor_state"] == FormalTaskState.TERMINAL.value
                and row["predecessor_revision_number"] == revision_number - 1
            )
            if (
                type(row["create_command_id"]) is not str
                or not row["create_command_id"]
                or "\x00" in row["create_command_id"]
                or len(row["create_command_id"]) > 256
                or row["accepted_command_id"] != row["create_command_id"]
                or row["accepted_state"] != FormalTaskState.ACCEPTED.value
                or row["accepted_outcome"] is not None
                or row["accepted_producer"] != "task_core"
                or (base_revision and row["command_type"] != "task.create")
                or (successor_revision and row["command_type"] != "task.create_successor")
                or type(command_result) is not dict
                or command_result.get("ok") is not True
                or type(command_result.get("result")) is not dict
                or command_result["result"].get("task_id") != row["task_id"]
                or not (base_revision or successor_revision)
            ):
                raise cls._schema_unsupported("formal task Store revision lineage is corrupt")

    @classmethod
    def _verify_v4_semantics(cls, connection: sqlite3.Connection) -> None:
        """Reconstruct every durable Task before accepting v4 authority."""

        task_rows = connection.execute("SELECT * FROM tasks ORDER BY task_id").fetchall()
        for task_row in task_rows:
            cls._verify_durable_lineage(connection, task_row)
            if task_row["cancel_requested"] not in {0, 1} or task_row["dispatch_fenced"] not in {0, 1}:
                raise cls._corrupt("formal Task control flags are not canonical booleans")
            if bool(task_row["cancel_requested"]) != bool(task_row["dispatch_fenced"]):
                raise cls._corrupt("formal Task cancellation flags disagree")

        command_rows = connection.execute("SELECT * FROM commands ORDER BY scope_key, command_id").fetchall()
        for command_row in command_rows:
            command_type = command_row["command_type"]
            command_id = command_row["command_id"]
            scope_key = command_row["scope_key"]
            if command_type == "task.create":
                command, _result, _spec = cls._command_ledger_from_row(command_row)
                owner_count = connection.execute(
                    """SELECT COUNT(*) FROM tasks
                       WHERE scope_key=? AND create_command_id=?""",
                    (scope_key, command_id),
                ).fetchone()[0]
                if owner_count != 1 or command.target_ref.id != f"create:{command_id}":
                    raise cls._corrupt("task.create command lacks one exact durable Task")
                continue
            if command_type == "task.create_successor":
                try:
                    stored_result = ResultEnvelope.from_dict(_json_load(command_row["result_json"]))
                except (ContractViolation, FormalTaskViolation) as error:
                    raise cls._corrupt("task.create_successor command result is not canonical") from error
                if not stored_result.ok:
                    cls._verify_business_decision(
                        connection,
                        command_row,
                        expected=_SUCCESSOR_BUSINESS_DECISIONS,
                    )
                    continue
                command, result, resolved_spec = cls._command_ledger_from_row(command_row)
                owners = connection.execute(
                    """SELECT * FROM tasks
                       WHERE scope_key=? AND create_command_id=?""",
                    (scope_key, command_id),
                ).fetchall()
                value = result.result
                if (
                    len(owners) != 1
                    or resolved_spec is None
                    or command.target_ref.kind.value != "task"
                    or owners[0]["predecessor_task_id"] != command.target_ref.id
                    or type(value) is not dict
                    or value.get("task_id") != owners[0]["task_id"]
                ):
                    raise cls._corrupt("task.create_successor command lacks one exact revision")
                continue
            if command_type == "task.retry":
                try:
                    stored_result = ResultEnvelope.from_dict(_json_load(command_row["result_json"]))
                except (ContractViolation, FormalTaskViolation) as error:
                    raise cls._corrupt("task.retry command result is not canonical") from error
                if not stored_result.ok:
                    cls._verify_business_decision(
                        connection,
                        command_row,
                        expected=_RETRY_BUSINESS_DECISIONS,
                    )
                    continue
                command, _result, _spec = cls._command_ledger_from_row(command_row)
                boundary_count = connection.execute(
                    """SELECT COUNT(*) FROM task_events AS e
                       JOIN tasks AS t ON t.task_id=e.task_id
                       WHERE t.scope_key=? AND e.event_type='task.retry_accepted'
                         AND e.causation_id=?""",
                    (scope_key, command_id),
                ).fetchone()[0]
                if boundary_count != 1 or command.target_ref.kind != "task":
                    raise cls._corrupt("task.retry command lacks one exact durable boundary")
                continue
            if command_type == "task.update":
                try:
                    stored_result = ResultEnvelope.from_dict(_json_load(command_row["result_json"]))
                except (ContractViolation, FormalTaskViolation) as error:
                    raise cls._corrupt("task.update command result is not canonical") from error
                if not stored_result.ok:
                    cls._verify_business_decision(
                        connection,
                        command_row,
                        expected=_UPDATE_BUSINESS_DECISIONS,
                    )
                    continue
                command, result = cls._control_command_from_row(command_row)
                event_rows = connection.execute(
                    """SELECT e.* FROM task_events AS e
                       JOIN tasks AS t ON t.task_id=e.task_id
                       WHERE t.scope_key=? AND e.causation_id=?
                         AND e.event_type IN (
                           'task.update_requested', 'task.update_applied'
                         ) ORDER BY e.seq""",
                    (scope_key, command_id),
                ).fetchall()
                value = result.result
                if (
                    len(event_rows) != 2
                    or event_rows[0]["event_type"] != "task.update_requested"
                    or event_rows[1]["event_type"] != "task.update_applied"
                    or event_rows[1]["seq"] != event_rows[0]["seq"] + 1
                    or command.target_ref.kind.value != "task"
                    or type(value) is not dict
                    or value.get("task_id") != command.target_ref.id
                ):
                    raise cls._corrupt("task.update command lacks one exact durable settlement")
                continue
            if command_type == "task.reprioritize":
                try:
                    stored_result = ResultEnvelope.from_dict(_json_load(command_row["result_json"]))
                except (ContractViolation, FormalTaskViolation) as error:
                    raise cls._corrupt("task.reprioritize command result is not canonical") from error
                if stored_result.ok:
                    command, result = cls._control_command_from_row(command_row)
                    event_rows = connection.execute(
                        """SELECT e.* FROM task_events AS e
                           JOIN tasks AS t ON t.task_id=e.task_id
                           WHERE t.scope_key=? AND e.causation_id=?
                             AND e.event_type IN (
                               'task.reprioritize_requested',
                               'task.reprioritize_applied'
                             ) ORDER BY e.seq""",
                        (scope_key, command_id),
                    ).fetchall()
                    value = result.result
                    if (
                        len(event_rows) != 2
                        or event_rows[0]["event_type"] != "task.reprioritize_requested"
                        or event_rows[1]["event_type"] != "task.reprioritize_applied"
                        or event_rows[1]["seq"] != event_rows[0]["seq"] + 1
                        or command.target_ref.kind.value != "task"
                        or type(value) is not dict
                        or value.get("task_id") != command.target_ref.id
                    ):
                        raise cls._corrupt("task.reprioritize command lacks one exact durable settlement")
                    continue
            if command_type in {
                "task.provide_input",
                "task.pause",
                "task.resume",
                "task.reprioritize",
            }:
                binding, result = cls._verify_business_decision(
                    connection,
                    command_row,
                    expected=_CONTROL_BUSINESS_DECISIONS,
                )
                value = result.error
                authority = binding["authority"]
                payload_authority = authority["payload"]
                attempt_row = connection.execute(
                    "SELECT task_id FROM attempts WHERE attempt_id=?",
                    (payload_authority.get("attempt_id"),),
                ).fetchone()
                if value is None or (
                    value.reason != "TASK_CONTROL_PRECONDITION_STALE"
                    and (attempt_row is None or attempt_row["task_id"] != binding["target_task_id"])
                ):
                    raise cls._corrupt("formal Task unsupported-control decision is not canonical")
                continue
            if command_type == "task.ack_events":
                # Consumption authority exists only in schema v5 and is rebuilt
                # with its consumer rows by _verify_v5_semantics.
                continue
            if command_type not in {"task.cancel", "task.adjust"}:
                raise cls._corrupt("formal Task command ledger contains an unsupported operation")
            try:
                stored_result = ResultEnvelope.from_dict(_json_load(command_row["result_json"]))
            except (ContractViolation, FormalTaskViolation) as error:
                raise cls._corrupt("formal Task control command result is not canonical") from error
            expected_decisions = (
                _CANCEL_BUSINESS_DECISIONS if command_type == "task.cancel" else _ADJUST_BUSINESS_DECISIONS
            )
            if (
                not stored_result.ok
                and stored_result.error is not None
                and stored_result.error.reason in expected_decisions
            ):
                cls._verify_business_decision(
                    connection,
                    command_row,
                    expected=expected_decisions,
                )
                continue
            command, result = cls._control_command_from_row(command_row)
            request_type = "task.cancel_requested" if command_type == "task.cancel" else "task.adjust_requested"
            request_rows = connection.execute(
                """SELECT e.task_id, e.attempt_id, e.seq FROM task_events AS e
                   JOIN tasks AS t ON t.task_id=e.task_id
                   WHERE t.scope_key=? AND e.event_type=? AND e.causation_id=?""",
                (scope_key, request_type, command_id),
            ).fetchall()
            outbox_count = connection.execute(
                """SELECT COUNT(*) FROM outbox AS o
                   JOIN tasks AS t ON t.task_id=o.task_id
                   WHERE t.scope_key=? AND o.command_id=?""",
                (scope_key, command_id),
            ).fetchone()[0]
            if len(request_rows) == 1:
                allowed_outbox_counts = {0, 1} if command_type == "task.cancel" else {1}
                if outbox_count not in allowed_outbox_counts:
                    raise cls._corrupt("formal Task control command has ambiguous durable ownership")
                fingerprint = _json_load(command_row["fingerprint"])
                mutation_precondition = cls._mutation_precondition_from_fingerprint(
                    command_type,
                    fingerprint,
                )
                request = request_rows[0]
                if mutation_precondition is not None and (
                    mutation_precondition.task_id != request["task_id"]
                    or mutation_precondition.attempt_id != request["attempt_id"]
                    or mutation_precondition.expected_event_head != int(request["seq"]) - 1
                ):
                    raise cls._corrupt("formal Task mutation precondition does not bind its request event")
                continue
            value = result.result
            attempt_row = (
                None
                if type(value) is not dict
                else connection.execute(
                    "SELECT task_id FROM attempts WHERE attempt_id=?",
                    (value.get("attempt_id"),),
                ).fetchone()
            )
            task_owner = (
                None
                if type(value) is not dict
                else connection.execute(
                    """SELECT scope_key, state, outcome, event_head
                       FROM tasks WHERE task_id=?""",
                    (value.get("task_id"),),
                ).fetchone()
            )
            try:
                repeated_state = None if type(value) is not dict else FormalTaskState(value.get("state"))
            except (TypeError, ValueError) as error:
                raise cls._corrupt("repeat Task cancellation result state is invalid") from error
            terminal_event = (
                None
                if task_owner is None or type(value) is not dict
                else connection.execute(
                    """SELECT event_id, occurred_at FROM task_events
                       WHERE task_id=? AND seq=? AND event_type='task.terminal'""",
                    (value.get("task_id"), task_owner["event_head"]),
                ).fetchone()
            )
            settled_repeat = (
                task_owner is not None
                and task_owner["state"] == FormalTaskState.TERMINAL.value
                and task_owner["outcome"] == TerminalOutcome.CANCELLED.value
                and terminal_event is not None
            )
            repeat_value_valid = type(value) is dict and set(value) == {
                "task_id",
                "attempt_id",
                "cancel_acknowledged",
                "applied",
                "state",
            }
            current_result_valid = (
                repeat_value_valid
                and value["applied"] is settled_repeat
                and (not settled_repeat or repeated_state is FormalTaskState.TERMINAL)
                and (settled_repeat or repeated_state is not FormalTaskState.TERMINAL)
                and result.observed_at
                == (terminal_event["occurred_at"] if settled_repeat else command_row["created_at"])
                and dict(result.extensions)
                == command_result_extensions(
                    (TaskCommandDisposition.APPLIED if settled_repeat else TaskCommandDisposition.ACCEPTED),
                    settlement_event_id=(terminal_event["event_id"] if settled_repeat else None),
                )
            )
            legacy_result_valid = (
                repeat_value_valid
                and value["applied"] is False
                and repeated_state is not FormalTaskState.TERMINAL
                and result.observed_at == command_row["created_at"]
                and dict(result.extensions) == {}
            )
            if (
                command_type != "task.cancel"
                or request_rows
                or outbox_count != 0
                or not repeat_value_valid
                or value["task_id"] != command.target_ref.id
                or command.payload
                or command.required_capabilities != ("task.cancel",)
                or attempt_row is None
                or attempt_row["task_id"] != value["task_id"]
                or task_owner is None
                or task_owner["scope_key"] != scope_key
                or value["cancel_acknowledged"] is not True
                or not (current_result_valid or legacy_result_valid)
            ):
                raise cls._corrupt("formal Task control command lacks canonical durable authority")

        executor_rows = connection.execute("SELECT * FROM executor_events ORDER BY source_event_id").fetchall()
        observations_by_source = {
            row["source_event_id"]: cls._executor_observation_from_row(row) for row in executor_rows
        }
        if len(observations_by_source) != len(executor_rows):
            raise cls._corrupt("formal Task Executor authority is duplicated")
        result_rows = connection.execute(
            """SELECT * FROM task_results
               ORDER BY task_id, attempt_id, source_event_id"""
        ).fetchall()
        result_sources: set[str] = set()
        for row in result_rows:
            result = cls._task_result_from_row(row)
            observation = observations_by_source.get(result.source_event_id)
            attempt_row = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=?",
                (result.attempt_id,),
            ).fetchone()
            if (
                observation is None
                or attempt_row is None
                or attempt_row["task_id"] != result.task_id
                or FormalAttemptState(attempt_row["state"]) is not FormalAttemptState.TERMINAL
                or attempt_row["outcome"] != TerminalOutcome.COMPLETED.value
                or observation.task_id != result.task_id
                or observation.attempt_id != result.attempt_id
                or observation.attempt_state is not FormalAttemptState.TERMINAL
                or observation.attempt_outcome is not TerminalOutcome.COMPLETED
                or observation.result_text != result.result_text
                or observation.result_artifacts != result.artifacts
                or observation.occurred_at != result.completed_at
                or result.source_event_id in result_sources
            ):
                raise cls._corrupt("formal Task result lacks exact completed Executor authority")
            result_sources.add(result.source_event_id)
        published_sources = {
            str(source_event_id)
            for source_event_id, observation in observations_by_source.items()
            if observation.result_text is not None
        }
        if result_sources != published_sources:
            raise cls._corrupt("formal Task result ledger does not exhaust published Executor truth")

        selections = connection.execute(
            """SELECT c.scope_key, c.session_id, t.scope_json
               FROM current_background_tasks AS c
               JOIN tasks AS t ON t.task_id=c.task_id
               ORDER BY c.scope_key, c.session_id"""
        ).fetchall()
        for selection in selections:
            scope = ScopeRef.from_dict(_json_load(selection["scope_json"]))
            if selection["scope_key"] != _scope_key(scope) or selection["session_id"] != scope.session_id:
                raise cls._corrupt("current Task selection crosses its exact Session authority")

    @classmethod
    def _verify_v5_semantics(cls, connection: sqlite3.Connection) -> None:
        """Reject ambiguous selection groups and cross-scope consumer authority."""

        selection_columns = _TASK_STORE_COLUMNS["attempts"][9:]
        required_selection_columns = tuple(column for column in selection_columns if column != "admission_reason")
        attempt_rows = connection.execute("SELECT * FROM attempts ORDER BY attempt_id").fetchall()
        for row in attempt_rows:
            values = tuple(row[column] for column in selection_columns)
            reprioritize_rows = connection.execute(
                """SELECT details_json FROM task_events
                   WHERE attempt_id=? AND event_type='task.reprioritize_applied'
                   ORDER BY seq""",
                (row["attempt_id"],),
            ).fetchall()
            binding_rows = connection.execute(
                """
                SELECT c.fingerprint,
                       c.created_at AS command_created_at,
                       c.result_json AS command_result_json,
                       o.command_id AS dispatch_command_id,
                       o.created_at AS dispatch_created_at,
                       o.state AS dispatch_state,
                       o.delivery_count, o.claimed_by, o.claimed_at,
                       o.claim_token, o.last_error
                FROM outbox AS o
                LEFT JOIN commands AS c
                  ON c.command_id=o.command_id
                 AND c.scope_key=(
                     SELECT scope_key FROM tasks WHERE task_id=o.task_id
                 )
                WHERE o.attempt_id=? AND o.kind=?
                """,
                (row["attempt_id"], OutboxKind.ATTEMPT_DISPATCH.value),
            ).fetchall()
            if len(binding_rows) != 1:
                raise cls._corrupt("formal Task Attempt lacks one immutable dispatch authority")
            dispatch_binding = binding_rows[0]
            has_recovery_table = connection.execute(
                """SELECT 1 FROM sqlite_master
                   WHERE type='table' AND name='durability_recoveries'"""
            ).fetchone()
            recovery_row = (
                None
                if has_recovery_table is None
                else connection.execute(
                    """SELECT * FROM durability_recoveries
                       WHERE recovery_attempt_id=?""",
                    (row["attempt_id"],),
                ).fetchone()
            )
            fingerprint = (
                None if dispatch_binding["fingerprint"] is None else _json_load(dispatch_binding["fingerprint"])
            )
            if recovery_row is not None:
                if any(row[column] is None for column in required_selection_columns):
                    raise cls._corrupt("durability recovery lost its persisted selection")
                producer_row = connection.execute(
                    "SELECT * FROM attempts WHERE attempt_id=?",
                    (recovery_row["producer_attempt_id"],),
                ).fetchone()
                boundary_rows = connection.execute(
                    """SELECT occurred_at FROM task_events
                       WHERE task_id=? AND attempt_id=? AND causation_id=?
                         AND event_type='task.recovery_accepted'""",
                    (
                        row["task_id"],
                        row["attempt_id"],
                        dispatch_binding["dispatch_command_id"],
                    ),
                ).fetchall()
                selection = _selection_from_attempt_row(row)
                producer_selection = None if producer_row is None else _selection_from_attempt_row(producer_row)
                if (
                    dispatch_binding["command_created_at"] is not None
                    or dispatch_binding["command_result_json"] is not None
                    or dispatch_binding["dispatch_command_id"] != recovery_row["recovery_id"]
                    or len(boundary_rows) != 1
                    or selection is None
                    or selection != producer_selection
                    or reprioritize_rows
                    or any(
                        timestamp != row["admission_enqueued_at"]
                        for timestamp in (
                            dispatch_binding["dispatch_created_at"],
                            recovery_row["created_at"],
                            boundary_rows[0]["occurred_at"],
                        )
                    )
                ):
                    raise cls._corrupt("durability recovery selection authority is not canonical")
                callback_rows = connection.execute(
                    "SELECT * FROM executor_events WHERE attempt_id=? ORDER BY source_seq",
                    (row["attempt_id"],),
                ).fetchall()
                expected_callback_binding = (
                    selection.adapter_id,
                    selection.capability_profile_digest,
                )
                if any(
                    (
                        observation.adapter_id,
                        observation.capability_profile_digest,
                    )
                    != expected_callback_binding
                    for observation in (
                        cls._executor_observation_from_row(callback_row) for callback_row in callback_rows
                    )
                ):
                    raise cls._corrupt("durability recovery callback binding changed")
                continue
            if all(value is None for value in values):
                if type(fingerprint) is dict and "executor_selection" in fingerprint:
                    raise cls._corrupt("selected formal Task Attempt cannot become legacy")
                if reprioritize_rows:
                    raise cls._corrupt("legacy formal Task Attempt has reprioritize authority")
                callback_rows = connection.execute(
                    "SELECT * FROM executor_events WHERE attempt_id=? ORDER BY source_seq",
                    (row["attempt_id"],),
                ).fetchall()
                if any(
                    (
                        observation.adapter_id,
                        observation.capability_profile_digest,
                    )
                    != (None, None)
                    for observation in (
                        cls._executor_observation_from_row(callback_row) for callback_row in callback_rows
                    )
                ):
                    raise cls._corrupt("legacy formal Task callback has selected authority")
                continue
            if any(row[column] is None for column in required_selection_columns):
                raise cls._corrupt("formal Task Attempt has a partial executor selection group")
            try:
                selection = _selection_from_attempt_row(row)
                assert selection is not None
                PersistentAdmissionRecord(
                    task_id=row["task_id"],
                    attempt_id=row["attempt_id"],
                    priority=selection.admission_priority,
                    reason=row["admission_reason"],
                    attempt_count=row["admission_attempt_count"],
                    next_eligible_at=row["admission_next_eligible_at"],
                    deadline_at=row["admission_deadline_at"],
                    enqueued_at=row["admission_enqueued_at"],
                    queued=False,
                )
                if (
                    row["state"] == FormalAttemptState.ACCEPTED.value
                    and (dispatch_binding["dispatch_state"] == OutboxState.PENDING.value)
                    and (
                        type(dispatch_binding["delivery_count"]) is not int
                        or dispatch_binding["delivery_count"] != row["admission_attempt_count"]
                        or dispatch_binding["claimed_by"] is not None
                        or dispatch_binding["claimed_at"] is not None
                        or dispatch_binding["claim_token"] is not None
                        or (row["admission_attempt_count"] == 0 and dispatch_binding["last_error"] is not None)
                        or (
                            row["admission_attempt_count"] > 0
                            and dispatch_binding["last_error"] != row["admission_reason"]
                        )
                    )
                ):
                    raise ValueError("selected pending dispatch lacks closed pre-effect history")
                boundary_rows = connection.execute(
                    """
                    SELECT occurred_at FROM task_events
                    WHERE task_id=? AND attempt_id=? AND causation_id=?
                      AND event_type IN ('task.accepted', 'task.retry_accepted')
                    """,
                    (
                        row["task_id"],
                        row["attempt_id"],
                        dispatch_binding["dispatch_command_id"],
                    ),
                ).fetchall()
                command_result = _json_load(dispatch_binding["command_result_json"])
                if (
                    len(boundary_rows) != 1
                    or type(command_result) is not dict
                    or any(
                        timestamp != row["admission_enqueued_at"]
                        for timestamp in (
                            dispatch_binding["dispatch_created_at"],
                            dispatch_binding["command_created_at"],
                            command_result.get("observed_at"),
                            boundary_rows[0]["occurred_at"],
                        )
                    )
                ):
                    raise ValueError("selected Attempt enqueue time changed from creation authority")
                if type(fingerprint) is not dict or "executor_selection" not in fingerprint:
                    raise ValueError("selected Attempt fingerprint changed")
                initial_selection = _selection_from_fingerprint_payload(fingerprint["executor_selection"])
                expected_priority = initial_selection.admission_priority
                for reprioritize_row in reprioritize_rows:
                    details = _json_load(reprioritize_row["details_json"])
                    expected_priority = AdmissionPriority(details["priority"])
                expected_selection = replace(initial_selection, admission_priority=expected_priority)
                if expected_selection != selection:
                    raise ValueError("selected Attempt fingerprint changed")
                callback_rows = connection.execute(
                    "SELECT * FROM executor_events WHERE attempt_id=? ORDER BY source_seq",
                    (row["attempt_id"],),
                ).fetchall()
                expected_callback_binding = (
                    selection.adapter_id,
                    selection.capability_profile_digest,
                )
                for callback_row in callback_rows:
                    observation = cls._executor_observation_from_row(callback_row)
                    if (
                        observation.adapter_id,
                        observation.capability_profile_digest,
                    ) != expected_callback_binding:
                        raise ValueError("selected callback binding changed")
            except (
                AssertionError,
                FormalTaskViolation,
                UnicodeEncodeError,
                ValueError,
            ) as error:
                raise cls._corrupt("formal Task Attempt has invalid executor admission facts") from error

        consumer_rows = cls._v5_consumer_rows(connection)
        cls._verify_v5_ack_semantics(connection, consumer_rows)

    @classmethod
    def _verify_v6_semantics(cls, connection: sqlite3.Connection) -> None:
        """Fail closed before accepting any v6 durability authority."""

        bindings = connection.execute(
            """SELECT task_id, producer_attempt_id AS origin_attempt_id
               FROM durability_checkpoints
               UNION
               SELECT task_id, origin_attempt_id
               FROM durability_effect_facts
               UNION
               SELECT task_id, producer_attempt_id AS origin_attempt_id
               FROM durability_recoveries
               ORDER BY task_id, origin_attempt_id"""
        ).fetchall()
        try:
            for row in bindings:
                task_row = cls._require_task_row_by_id(connection, row["task_id"])
                scope, _spec = _task_binding_from_row(task_row)
                binding = cls._durability_binding_from_connection(
                    connection,
                    scope=scope,
                    task_id=row["task_id"],
                    origin_attempt_id=row["origin_attempt_id"],
                )
                cls._verified_checkpoint_prefix(connection, binding)
                cls._verified_effect_prefix(connection, binding)

            recovery_rows = connection.execute(
                """SELECT * FROM durability_recoveries
                   ORDER BY task_id, producer_attempt_id, recovery_generation"""
            ).fetchall()
            seen_generations: dict[str, int] = {}
            for row in recovery_rows:
                task_row = cls._require_task_row_by_id(connection, row["task_id"])
                scope, _spec = _task_binding_from_row(task_row)
                binding = cls._durability_binding_from_connection(
                    connection,
                    scope=scope,
                    task_id=row["task_id"],
                    origin_attempt_id=row["producer_attempt_id"],
                )
                facts = ExecutorRecoveryFacts.from_bytes(bytes(row["recovery_facts"]))
                profile_json = _json_dump(binding.profile.to_dict())
                key = binding.task_id
                prior_generation = seen_generations.get(key, 0)
                producer = connection.execute(
                    "SELECT * FROM attempts WHERE attempt_id=? AND task_id=?",
                    (binding.origin_attempt_id, binding.task_id),
                ).fetchone()
                recovery = connection.execute(
                    "SELECT * FROM attempts WHERE attempt_id=? AND task_id=?",
                    (row["recovery_attempt_id"], binding.task_id),
                ).fetchone()
                if (
                    row["profile_json"] != profile_json
                    or facts.scope != binding.scope
                    or facts.task_id != binding.task_id
                    or facts.producer_attempt_id != binding.origin_attempt_id
                    or facts.candidate_recovery_attempt_id != row["recovery_attempt_id"]
                    or facts.profile != binding.profile
                    or facts.recovery_generation != row["recovery_generation"]
                    or row["recovery_generation"] != prior_generation + 1
                    or producer is None
                    or recovery is None
                    or producer["state"] != FormalAttemptState.TERMINAL.value
                    or recovery["attempt_number"] != producer["attempt_number"] + 1
                    or producer["attempt_number"] != row["recovery_generation"]
                ):
                    raise ValueError("durable recovery lineage is inconsistent")
                cls._verified_checkpoint_prefix(
                    connection,
                    binding,
                    expected_head=row["checkpoint_head"],
                    expected_prefix_digest=row["checkpoint_prefix_digest"],
                )
                cls._verified_effect_prefix(
                    connection,
                    binding,
                    expected_head=row["effect_head"],
                    expected_prefix_digest=row["effect_prefix_digest"],
                )
                seen_generations[key] = row["recovery_generation"]

            lease_rows = connection.execute("SELECT * FROM durability_mutator_leases ORDER BY task_id").fetchall()
            for row in lease_rows:
                if (
                    type(row["owner_id"]) is not str
                    or not row["owner_id"].strip()
                    or type(row["claim_token"]) is not str
                    or not row["claim_token"].strip()
                    or _utc_datetime(row["expires_at"]) <= _utc_datetime(row["claimed_at"])
                ):
                    raise ValueError("durability mutator lease is invalid")
            fence_rows = connection.execute("SELECT * FROM durability_recovery_fences ORDER BY task_id").fetchall()
            for row in fence_rows:
                task_row = cls._require_task_row_by_id(connection, row["task_id"])
                command_row = connection.execute(
                    """SELECT command_type, result_json FROM commands
                       WHERE scope_key=? AND command_id=?""",
                    (task_row["scope_key"], row["cancel_command_id"]),
                ).fetchone()
                producer = connection.execute(
                    "SELECT task_id, state, outcome FROM attempts WHERE attempt_id=?",
                    (row["producer_attempt_id"],),
                ).fetchone()
                if (
                    command_row is None
                    or command_row["command_type"] != "task.cancel"
                    or producer is None
                    or producer["task_id"] != row["task_id"]
                    or producer["state"] != FormalAttemptState.TERMINAL.value
                    or producer["outcome"] != TerminalOutcome.INTERRUPTED.value
                ):
                    raise ValueError("durability cancel fence is invalid")
                result = ResultEnvelope.from_dict(_json_load(command_row["result_json"]))
                if result.ok or result.error is None or result.error.reason != "TASK_ALREADY_TERMINAL":
                    raise ValueError("durability cancel fence lacks exact decision")
        except (DurabilityPrefixViolation, FormalTaskViolation, ValueError) as error:
            if isinstance(error, FormalTaskViolation) and error.reason == "TASK_STORE_CORRUPT":
                raise
            raise cls._corrupt("formal Task Store durability authority is corrupt") from error

    @classmethod
    def _v5_consumer_rows(
        cls,
        connection: sqlite3.Connection,
    ) -> list[sqlite3.Row]:
        """Load consumers only after proving their exact Task scope."""

        consumer_rows = connection.execute(
            """SELECT c.*, t.scope_json
               FROM task_event_consumption AS c
               JOIN tasks AS t ON t.task_id=c.task_id
               ORDER BY c.subject_id, c.project_id, c.task_id,
                        c.presentation_class"""
        ).fetchall()
        for row in consumer_rows:
            try:
                scope = ScopeRef.from_dict(_json_load(row["scope_json"]))
            except (ContractViolation, FormalTaskViolation) as error:
                raise cls._corrupt("formal Task consumer scope is not canonical") from error
            if (
                row["subject_id"] != scope.subject_id
                or row["project_id"] != scope.project_id
                or row["presentation_class"] not in {"text", "voice"}
            ):
                raise cls._corrupt("formal Task consumer crosses its exact Task scope")
        return consumer_rows

    @classmethod
    def _ack_history_step(
        cls,
        command: CommandEnvelope,
        *,
        presentation_class: str,
        observed_at: str,
        origin: str,
        previous: tuple[int, str | None, str | None, str | None],
    ) -> tuple[dict[str, object], tuple[str, int, str, str, str]]:
        """Build one closed v1 ACK result and its next immutable chain state."""

        payload = command.payload
        acknowledged_seq = payload["acked_through_seq"]
        acknowledged_event_id = payload["acked_event_id"]
        previous_seq, previous_event_id, previous_at, previous_sha256 = previous
        advanced = acknowledged_seq > previous_seq
        current_seq = acknowledged_seq if advanced else previous_seq
        current_event_id = acknowledged_event_id if advanced else previous_event_id
        if current_event_id is None:
            raise cls._corrupt("formal Task ACK history lost its current event")
        if origin == _LEGACY_CONSUMPTION_SEED_TYPE:
            current_at = previous_at
        elif origin == _RUNTIME_CONSUMPTION_ORIGIN:
            current_at = observed_at if advanced else previous_at
        else:
            raise cls._corrupt("formal Task ACK history origin is unsupported")
        if current_at is None:
            raise cls._corrupt("formal Task ACK history lost its current timestamp")
        previous_value = {
            "acked_through_seq": previous_seq,
            "acked_event_id": previous_event_id,
            "updated_at": previous_at,
            "history_sha256": previous_sha256,
        }
        current_value = {
            "acked_through_seq": current_seq,
            "acked_event_id": current_event_id,
            "updated_at": current_at,
        }
        binding = {
            "binding_type": _ACK_HISTORY_BINDING_TYPE,
            "version": _ACK_HISTORY_VERSION,
            "consumer": {
                "subject_id": command.scope.subject_id,
                "project_id": command.scope.project_id,
                "task_id": command.target_ref.id,
                "presentation_class": presentation_class,
            },
            "command": {
                "command_id": command.command_id,
                "scope": command.scope.to_dict(),
                "fingerprint_sha256": cls._sha256_hex(command.fingerprint()),
                "payload": dict(payload),
                "issued_at": command.issued_at,
                "observed_at": observed_at,
            },
            "origin": origin,
            "previous": previous_value,
            "advanced": advanced,
            "current": current_value,
        }
        history_sha256 = cls._json_value_sha256(binding)
        result_value: dict[str, object] = {
            "task_id": command.target_ref.id,
            "presentation_class": presentation_class,
            "acked_through_seq": current_seq,
            "acked_event_id": current_event_id,
            "advanced": advanced,
            "consumption_history": {
                "version": _ACK_HISTORY_VERSION,
                "origin": origin,
                "previous": previous_value,
                "current": {
                    **current_value,
                    "history_sha256": history_sha256,
                },
            },
        }
        return result_value, (
            origin,
            current_seq,
            current_event_id,
            current_at,
            history_sha256,
        )

    @classmethod
    def _ack_result_types_exact(cls, stored: object, expected: object) -> bool:
        """Compare an ACK result without Python's bool/number coercion."""

        if type(stored) is not type(expected):
            return False
        if type(stored) is dict:
            assert type(expected) is dict
            return stored.keys() == expected.keys() and all(
                cls._ack_result_types_exact(stored[key], expected[key]) for key in stored
            )
        if type(stored) is list:
            assert type(expected) is list
            return len(stored) == len(expected) and all(
                cls._ack_result_types_exact(stored_item, expected_item)
                for stored_item, expected_item in zip(stored, expected, strict=True)
            )
        return True

    @classmethod
    def _verify_v5_ack_semantics(
        cls,
        connection: sqlite3.Connection,
        consumer_rows: list[sqlite3.Row],
    ) -> dict[tuple[str, str, str, str], tuple[str, int, str, str, str]]:
        """Rebuild every ordered successful ACK and its exact consumer state."""

        durable = {
            (
                row["subject_id"],
                row["project_id"],
                row["task_id"],
                row["presentation_class"],
            ): (
                int(row["acked_through_seq"]),
                row["acked_event_id"],
                row["updated_at"],
            )
            for row in consumer_rows
        }
        legacy_anchors: dict[tuple[str, str, str, str], tuple[int, str, str, tuple[datetime, int]]] = {}
        legacy_seeds: dict[tuple[str, str, str, str], tuple[int, str, str, tuple[datetime, int]]] = {}
        for row in consumer_rows:
            anchor = cls._legacy_consumption_anchor_v1(connection, row)
            if anchor is None:
                continue
            seed_type, seed_seq, seed_event_id, seed_time, parsed_seed_time = anchor
            if seed_type != _LEGACY_CONSUMPTION_SEED_TYPE:
                raise cls._corrupt("formal Task consumer seed version is not canonical")
            key = (
                row["subject_id"],
                row["project_id"],
                row["task_id"],
                row["presentation_class"],
            )
            anchor_value = (
                seed_seq,
                seed_event_id,
                seed_time,
                parsed_seed_time,
            )
            legacy_anchors[key] = anchor_value
            if row["acked_through_seq"] == seed_seq and row["acked_event_id"] == seed_event_id:
                legacy_seeds[key] = anchor_value
        reconstructed: dict[tuple[str, str, str, str], tuple[str, int, str, str, str]] = {}
        command_rows = connection.execute(
            """SELECT rowid AS command_rowid, * FROM commands
               WHERE command_type='task.ack_events'
               ORDER BY command_rowid"""
        ).fetchall()
        for row in command_rows:
            try:
                stored_result = ResultEnvelope.from_dict(_json_load(row["result_json"]))
            except (ContractViolation, FormalTaskViolation) as error:
                raise cls._corrupt("formal Task ACK command result is not canonical") from error
            if not stored_result.ok:
                cls._verify_business_decision(
                    connection,
                    row,
                    expected=_ACK_BUSINESS_DECISIONS,
                )
                continue
            command, result = cls._control_command_from_row(row)
            payload = command.payload
            value = result.result
            task_row = connection.execute(
                "SELECT scope_json, event_head FROM tasks WHERE task_id=?",
                (command.target_ref.id,),
            ).fetchone()
            if task_row is None:
                raise cls._corrupt("formal Task ACK lost its target")
            try:
                task_scope = ScopeRef.from_dict(_json_load(task_row["scope_json"]))
            except (ContractViolation, FormalTaskViolation) as error:
                raise cls._corrupt("formal Task ACK target scope is not canonical") from error
            presentation_class = payload.get("presentation_class")
            acked_through_seq = payload.get("acked_through_seq")
            acked_event_id = payload.get("acked_event_id")
            expected_event_head = payload.get("expected_event_head")
            event_row = (
                None
                if type(acked_through_seq) is not int or type(acked_event_id) is not str
                else connection.execute(
                    """SELECT event_id, occurred_at FROM task_events
                       WHERE task_id=? AND seq=? AND event_id=?""",
                    (
                        command.target_ref.id,
                        acked_through_seq,
                        acked_event_id,
                    ),
                ).fetchone()
            )
            command_time = _canonical_utc_order_key(row["created_at"])
            event_time = None if event_row is None else _canonical_utc_order_key(event_row["occurred_at"])
            if (
                command.target_ref.kind.value != "task"
                or command.required_capabilities != ("task.ack_events",)
                or set(payload)
                != {
                    "presentation_class",
                    "acked_through_seq",
                    "acked_event_id",
                    "expected_event_head",
                }
                or presentation_class not in {"text", "voice"}
                or type(acked_through_seq) is not int
                or type(expected_event_head) is not int
                or not 0 <= acked_through_seq <= expected_event_head <= int(task_row["event_head"])
                or event_row is None
                or command_time is None
                or event_time is None
                or command_time <= event_time
                or command.scope.assurance is not Assurance.AUTHENTICATED
                or task_scope.assurance is not Assurance.AUTHENTICATED
                or command.scope.subject_id != task_scope.subject_id
                or command.scope.project_id != task_scope.project_id
            ):
                raise cls._corrupt("formal Task ACK command lacks exact consumer authority")
            key = (
                command.scope.subject_id,
                command.scope.project_id,
                command.target_ref.id,
                presentation_class,
            )
            previous_chain = reconstructed.get(key)
            anchor = legacy_anchors.get(key)
            if previous_chain is None:
                if (
                    anchor is not None
                    and acked_through_seq == anchor[0]
                    and acked_event_id == anchor[1]
                    and command_time > anchor[3]
                ):
                    # legacy_seed_v1 exists only when its first successful
                    # command is the exact seq-0 adoption handshake.
                    origin = _LEGACY_CONSUMPTION_SEED_TYPE
                    previous = (anchor[0], anchor[1], anchor[2], None)
                else:
                    origin = _RUNTIME_CONSUMPTION_ORIGIN
                    previous = (-1, None, None, None)
                legacy_seeds.pop(key, None)
            else:
                origin = previous_chain[0]
                previous = (
                    previous_chain[1],
                    previous_chain[2],
                    previous_chain[3],
                    previous_chain[4],
                )
            expected_value, current_chain = cls._ack_history_step(
                command,
                presentation_class=presentation_class,
                observed_at=row["created_at"],
                origin=origin,
                previous=previous,
            )
            try:
                result_bytes_match = canonical_json_bytes(value) == canonical_json_bytes(expected_value)
            except (ContractViolation, UnicodeError) as error:
                raise cls._corrupt("formal Task ACK result is not canonical") from error
            if (
                not result.ok
                or type(value) is not dict
                or not result_bytes_match
                or not cls._ack_result_types_exact(value, expected_value)
                or result.observed_at != row["created_at"]
                or dict(result.extensions) != command_result_extensions(TaskCommandDisposition.APPLIED)
            ):
                raise cls._corrupt("formal Task ACK result is not canonical")
            reconstructed[key] = current_chain

        expected = {key: seed[:3] for key, seed in legacy_seeds.items()}
        expected.update({key: (chain[1], chain[2], chain[3]) for key, chain in reconstructed.items()})
        if durable != expected:
            raise cls._corrupt("formal Task consumer ledger disagrees with ACK command history")
        return reconstructed

    @classmethod
    def _legacy_consumption_anchor_v1(
        cls,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> tuple[str, int, str, str, tuple[datetime, int]] | None:
        """Infer a Task4 seed-derived lineage from immutable event truth."""

        accepted = connection.execute(
            """SELECT event_id, event_type, occurred_at
               FROM task_events WHERE task_id=? AND seq=0""",
            (row["task_id"],),
        ).fetchone()
        current = connection.execute(
            """SELECT event_id FROM task_events
               WHERE task_id=? AND seq=? AND event_id=?""",
            (
                row["task_id"],
                row["acked_through_seq"],
                row["acked_event_id"],
            ),
        ).fetchone()
        seed_time = _canonical_utc_order_key(row["updated_at"])
        event_time = None if accepted is None else _canonical_utc_order_key(accepted["occurred_at"])
        if (
            accepted is None
            or accepted["event_type"] != "task.accepted"
            or current is None
            or seed_time is None
            or event_time is None
            or row["updated_at"] != accepted["occurred_at"]
        ):
            return None
        return (
            _LEGACY_CONSUMPTION_SEED_TYPE,
            0,
            accepted["event_id"],
            accepted["occurred_at"],
            event_time,
        )

    def _hit(self, name: str) -> None:
        if self._failpoint is not None:
            self._failpoint(name)

    @staticmethod
    def _creation_fingerprint(
        command: CommandEnvelope,
        spec: FormalTaskSpec,
        selection: PersistedExecutorSelection | None,
    ) -> bytes:
        payload: dict[str, object] = {
            "command": json.loads(command.fingerprint()),
            "resolved_spec": spec.to_dict(),
        }
        if selection is not None:
            payload["executor_selection"] = _selection_fingerprint_payload(selection)
        return canonical_json_bytes(payload)

    @staticmethod
    def _insert_attempt(
        connection: sqlite3.Connection,
        *,
        attempt_id: str,
        task_id: str,
        attempt_number: int,
        executor_id: str,
        state: FormalAttemptState,
        observed_at: str,
        selection: PersistedExecutorSelection | None,
        admission_policy: AdmissionPolicy | None,
    ) -> None:
        if selection is None:
            if admission_policy is not None:
                raise FormalTaskViolation(
                    "ADMISSION_SELECTION_REQUIRED",
                    "admission policy cannot relabel an unselected legacy Attempt",
                    ErrorCode.INVALID_ARGUMENT,
                )
            admission_values: tuple[object, ...] = (None,) * 10
        else:
            if not isinstance(selection, PersistedExecutorSelection):
                raise FormalTaskViolation(
                    "INVALID_EXECUTOR_SELECTION",
                    "selected Attempt requires the strict persisted carrier",
                    ErrorCode.INVALID_ARGUMENT,
                )
            policy = AdmissionPolicy() if admission_policy is None else admission_policy
            if not isinstance(policy, AdmissionPolicy):
                raise FormalTaskViolation(
                    "INVALID_ADMISSION_POLICY",
                    "selected Attempt requires a canonical admission policy",
                    ErrorCode.INVALID_ARGUMENT,
                )
            admission_values = (
                selection.adapter_id,
                selection.capability_profile_json.decode("utf-8"),
                selection.capability_profile_digest,
                selection.execution_requirements_json.decode("utf-8"),
                selection.admission_priority.value,
                None,
                0,
                observed_at,
                _utc_plus_seconds(observed_at, policy.deadline_seconds),
                observed_at,
            )
        connection.execute(
            """
            INSERT INTO attempts(
                attempt_id, task_id, attempt_number, executor_id, executor_ref,
                state, outcome, source_seq, updated_at,
                adapter_id, capability_profile_json, capability_profile_digest,
                execution_requirements_json, admission_priority,
                admission_reason, admission_attempt_count,
                admission_next_eligible_at, admission_deadline_at,
                admission_enqueued_at
            ) VALUES(?, ?, ?, ?, NULL, ?, NULL, -1, ?,
                     ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                attempt_id,
                task_id,
                attempt_number,
                executor_id,
                state.value,
                observed_at,
                *admission_values,
            ),
        )

    def create(
        self,
        command: CommandEnvelope,
        spec: FormalTaskSpec,
        *,
        observed_at: str,
        current_background_session_id: str | None = None,
        selection: PersistedExecutorSelection | None = None,
        admission_policy: AdmissionPolicy | None = None,
    ) -> ResultEnvelope:
        if current_background_session_id is not None and (
            type(current_background_session_id) is not str
            or not current_background_session_id.strip()
            or len(current_background_session_id) > 256
            or command.scope.session_id != current_background_session_id
        ):
            raise FormalTaskViolation(
                "CURRENT_BACKGROUND_SESSION_MISMATCH",
                "current background task requires the exact authorized Session",
                ErrorCode.PERMISSION_DENIED,
            )
        if require_payload_source(command) != spec.native_source:
            raise ValueError("NATIVE_TASK_SOURCE_SPEC_MISMATCH")
        fingerprint = self._creation_fingerprint(command, spec, selection)
        scope_key = _scope_key(command.scope)
        with self._transaction() as connection:
            replay = self._command_replay(connection, command, fingerprint)
            if replay is not None:
                return replay
            self._hit("create.before_ids")
            task_id = f"task-{uuid.uuid4().hex}"
            attempt_id = f"attempt-{uuid.uuid4().hex}"
            outbox_id = f"outbox-{uuid.uuid4().hex}"
            event_id = f"event-{uuid.uuid4().hex}"
            now = observed_at
            connection.execute(
                """
                INSERT INTO tasks(
                    task_id, scope_key, scope_json, spec_json, state, outcome,
                    attempt_id, correlation_id, event_head, created_at, updated_at,
                    create_command_id, predecessor_task_id, revision_number
                ) VALUES(?, ?, ?, ?, ?, NULL, ?, ?, 0, ?, ?, ?, NULL, 1)
                """,
                (
                    task_id,
                    scope_key,
                    _json_dump(command.scope.to_dict()),
                    _json_dump(spec.to_dict()),
                    FormalTaskState.ACCEPTED.value,
                    attempt_id,
                    command.correlation_id,
                    now,
                    now,
                    command.command_id,
                ),
            )
            self._hit("create.after_task")
            self._insert_attempt(
                connection,
                attempt_id=attempt_id,
                task_id=task_id,
                attempt_number=1,
                executor_id=spec.executor_id,
                state=FormalAttemptState.ACCEPTED,
                observed_at=now,
                selection=selection,
                admission_policy=admission_policy,
            )
            self._insert_event(
                connection,
                event_id=event_id,
                task_id=task_id,
                attempt_id=attempt_id,
                scope=command.scope,
                seq=0,
                event_type="task.accepted",
                state=FormalTaskState.ACCEPTED.value,
                outcome=None,
                producer="task_core",
                source_event_id=None,
                causation_id=command.command_id,
                correlation_id=command.correlation_id,
                occurred_at=now,
                details={"command_id": command.command_id},
            )
            self._hit("create.after_event")
            self._insert_outbox(
                connection,
                outbox_id=outbox_id,
                kind=OutboxKind.ATTEMPT_DISPATCH,
                task_id=task_id,
                attempt_id=attempt_id,
                command_id=command.command_id,
                scope=command.scope,
                spec=spec,
                now=now,
            )
            self._hit("create.after_outbox")
            result = ResultEnvelope.success(
                owner=command,
                result={
                    "task_id": task_id,
                    "attempt_id": attempt_id,
                    "state": FormalTaskState.ACCEPTED.value,
                    "outbox_id": outbox_id,
                },
                observed_at=observed_at,
                extensions={
                    **command_result_extensions(
                        TaskCommandDisposition.ACCEPTED,
                        admission_event_id=event_id,
                    ),
                    "live_voice.store": {"durability": "sqlite_outbox"},
                },
            )
            self._insert_command(
                connection,
                command,
                fingerprint,
                scope_key,
                result,
                observed_at,
            )
            self._hit("create.after_command")
            if current_background_session_id is not None:
                connection.execute(
                    """
                    INSERT INTO current_background_tasks(
                        scope_key, session_id, task_id, updated_at
                    ) VALUES(?, ?, ?, ?)
                    ON CONFLICT(scope_key, session_id) DO UPDATE SET
                        task_id=excluded.task_id,
                        updated_at=excluded.updated_at
                    """,
                    (
                        scope_key,
                        current_background_session_id,
                        task_id,
                        observed_at,
                    ),
                )
                self._hit("create.after_current_pointer")
            return result

    def get_current_background_task(self, scope: ScopeRef, *, session_id: str) -> PersistentTaskRecord | None:
        """Return a UI/conversation selection hint, never mutation authority."""

        if (
            type(session_id) is not str
            or not session_id.strip()
            or len(session_id) > 256
            or scope.session_id != session_id
        ):
            raise FormalTaskViolation(
                "CURRENT_BACKGROUND_SESSION_MISMATCH",
                "current background task requires the exact authorized Session",
                ErrorCode.PERMISSION_DENIED,
            )
        with self._reader() as connection:
            row = connection.execute(
                """
                SELECT t.* FROM current_background_tasks AS c
                JOIN tasks AS t ON t.task_id=c.task_id
                WHERE c.scope_key=? AND c.session_id=?
                """,
                (_scope_key(scope), session_id),
            ).fetchone()
            if row is None:
                return None
            task = self._task_from_row(row)
            if task.scope != scope:
                raise FormalTaskViolation(
                    "TASK_STORE_CORRUPT",
                    "current background task escaped its exact scope",
                    ErrorCode.INTERNAL,
                )
            return task

    def create_successor(
        self,
        command: CommandEnvelope,
        spec: FormalTaskSpec,
        *,
        observed_at: str,
        selection: PersistedExecutorSelection | None = None,
        admission_policy: AdmissionPolicy | None = None,
    ) -> ResultEnvelope:
        """Create a revision using the same transaction as deferred changes."""
        with self._transaction() as connection:
            return self._create_successor(
                connection,
                command,
                spec,
                observed_at=observed_at,
                selection=selection,
                admission_policy=admission_policy,
            )

    def _create_successor(self, connection, command, spec, *, observed_at, selection=None, admission_policy=None):
        self._validate_successor_command(command)
        if require_payload_source(command) != spec.native_source:
            raise ValueError("NATIVE_TASK_SOURCE_SPEC_MISMATCH")
        fingerprint = self._creation_fingerprint(command, spec, selection)
        scope_key = _scope_key(command.scope)
        predecessor_id = command.target_ref.id
        payload = command.payload
        replay = self._command_replay(connection, command, fingerprint)
        if replay is not None:
            return replay
        predecessor_row = self._require_task_row(connection, predecessor_id, command.scope)
        self._verify_durable_lineage(connection, predecessor_row)
        predecessor = self._task_from_row(predecessor_row)
        attempt_row = connection.execute(
            "SELECT * FROM attempts WHERE attempt_id=?",
            (predecessor.attempt_id,),
        ).fetchone()
        terminal_event = connection.execute(
            "SELECT * FROM task_events WHERE task_id=? AND seq=?",
            (predecessor_id, predecessor.event_head),
        ).fetchone()
        existing_successor = connection.execute(
            "SELECT task_id FROM tasks WHERE predecessor_task_id=?",
            (predecessor_id,),
        ).fetchone()
        eligible = {
            TerminalOutcome.COMPLETED,
            TerminalOutcome.FAILED,
            TerminalOutcome.CANCELLED,
            TerminalOutcome.INTERRUPTED,
        }
        try:
            requested_outcome = TerminalOutcome(payload["predecessor_outcome"])
        except (KeyError, TypeError, ValueError):
            return self._persist_business_decision(
                connection,
                command,
                fingerprint,
                disposition=TaskCommandDisposition.CONFLICT,
                code=ErrorCode.CONFLICT,
                reason="TASK_SUCCESSOR_PRECONDITION_CONFLICT",
                message="successor predecessor outcome is not canonical",
                observed_at=observed_at,
            )
        if (
            predecessor.state is not FormalTaskState.TERMINAL
            or predecessor.outcome not in eligible
            or attempt_row is None
            or attempt_row["task_id"] != predecessor_id
            or attempt_row["state"] != FormalAttemptState.TERMINAL.value
            or attempt_row["outcome"] != (None if predecessor.outcome is None else predecessor.outcome.value)
            or terminal_event is None
            or terminal_event["event_type"] != "task.terminal"
            or terminal_event["event_id"] != payload.get("predecessor_terminal_event_id")
            or payload.get("expected_predecessor_revision_number") != predecessor.revision_number
            or payload.get("expected_predecessor_event_head") != predecessor.event_head
            or requested_outcome is not predecessor.outcome
            or existing_successor is not None
        ):
            return self._persist_business_decision(
                connection,
                command,
                fingerprint,
                disposition=TaskCommandDisposition.CONFLICT,
                code=ErrorCode.CONFLICT,
                reason="TASK_SUCCESSOR_PRECONDITION_CONFLICT",
                message=("successor requires exact eligible immutable predecessor truth"),
                observed_at=observed_at,
            )
        prior_context = predecessor.spec.context
        if (
            spec.context.source,
            spec.context.stable_id,
            spec.context.uri,
            spec.context.scope,
        ) != (
            prior_context.source,
            prior_context.stable_id,
            prior_context.uri,
            prior_context.scope,
        ):
            raise FormalTaskViolation(
                "TASK_SUCCESSOR_CONTEXT_IDENTITY_MISMATCH",
                "successor must preserve predecessor project identity",
                ErrorCode.PERMISSION_DENIED,
            )
        expected_spec_payload = {
            "name": spec.name,
            "instruction": spec.instruction,
            "constraints": list(spec.constraints),
            "executor_id": spec.executor_id,
            "side_effect_class": spec.side_effect_class,
            "attributes": dict(spec.attributes),
        }
        if any(payload.get(key) != value for key, value in expected_spec_payload.items()):
            raise FormalTaskViolation(
                "TASK_SUCCESSOR_SPEC_MISMATCH",
                "successor resolved specification disagrees with command facts",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        result_rows = connection.execute(
            """SELECT * FROM task_results
               WHERE task_id=? AND attempt_id=? ORDER BY source_event_id""",
            (predecessor_id, predecessor.attempt_id),
        ).fetchall()
        requested_digest = payload.get("predecessor_result_sha256")
        if predecessor.outcome is TerminalOutcome.COMPLETED:
            if len(result_rows) != 1:
                return self._persist_business_decision(
                    connection,
                    command,
                    fingerprint,
                    disposition=TaskCommandDisposition.CONFLICT,
                    code=ErrorCode.CONFLICT,
                    reason="TASK_SUCCESSOR_RESULT_CONFLICT",
                    message=("completed predecessor lacks one canonical current result"),
                    observed_at=observed_at,
                )
            result_record = self._task_result_from_row(result_rows[0])
            expected_digest = hashlib.sha256(canonical_json_bytes(result_record.to_dict())).hexdigest()
            if requested_digest != expected_digest:
                return self._persist_business_decision(
                    connection,
                    command,
                    fingerprint,
                    disposition=TaskCommandDisposition.CONFLICT,
                    code=ErrorCode.CONFLICT,
                    reason="TASK_SUCCESSOR_RESULT_CONFLICT",
                    message=("successor result digest no longer matches predecessor"),
                    observed_at=observed_at,
                )
        elif requested_digest is not None or result_rows:
            return self._persist_business_decision(
                connection,
                command,
                fingerprint,
                disposition=TaskCommandDisposition.CONFLICT,
                code=ErrorCode.CONFLICT,
                reason="TASK_SUCCESSOR_RESULT_CONFLICT",
                message="non-completed predecessor must not bind a Task result",
                observed_at=observed_at,
            )
        self._hit("successor.before_ids")
        task_id = f"task-{uuid.uuid4().hex}"
        attempt_id = f"attempt-{uuid.uuid4().hex}"
        outbox_id = f"outbox-{uuid.uuid4().hex}"
        event_id = f"event-{uuid.uuid4().hex}"
        result = ResultEnvelope.success(
            owner=command,
            result={
                "task_id": task_id,
                "predecessor_task_id": predecessor_id,
                "revision_number": predecessor.revision_number + 1,
                "attempt_id": attempt_id,
                "state": FormalTaskState.ACCEPTED.value,
                "outbox_id": outbox_id,
            },
            observed_at=observed_at,
            extensions={
                **command_result_extensions(
                    TaskCommandDisposition.ACCEPTED,
                    admission_event_id=event_id,
                ),
                "live_voice.store": {"durability": "sqlite_outbox"},
            },
        )
        self._insert_command(
            connection,
            command,
            fingerprint,
            scope_key,
            result,
            observed_at,
        )
        self._hit("successor.after_command")
        connection.execute(
            """
            INSERT INTO tasks(
                task_id, scope_key, scope_json, spec_json, state, outcome,
                attempt_id, correlation_id, event_head, created_at, updated_at,
                create_command_id, predecessor_task_id, revision_number
            ) VALUES(?, ?, ?, ?, ?, NULL, ?, ?, 0, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                scope_key,
                _json_dump(command.scope.to_dict()),
                _json_dump(spec.to_dict()),
                FormalTaskState.ACCEPTED.value,
                attempt_id,
                command.correlation_id,
                observed_at,
                observed_at,
                command.command_id,
                predecessor_id,
                predecessor.revision_number + 1,
            ),
        )
        self._hit("successor.after_task")
        self._insert_attempt(
            connection,
            attempt_id=attempt_id,
            task_id=task_id,
            attempt_number=1,
            executor_id=spec.executor_id,
            state=FormalAttemptState.ACCEPTED,
            observed_at=observed_at,
            selection=selection,
            admission_policy=admission_policy,
        )
        self._hit("successor.after_attempt")
        self._insert_event(
            connection,
            event_id=event_id,
            task_id=task_id,
            attempt_id=attempt_id,
            scope=command.scope,
            seq=0,
            event_type="task.accepted",
            state=FormalTaskState.ACCEPTED.value,
            outcome=None,
            producer="task_core",
            source_event_id=None,
            causation_id=command.command_id,
            correlation_id=command.correlation_id,
            occurred_at=observed_at,
            details={"command_id": command.command_id},
        )
        self._hit("successor.after_event")
        self._insert_outbox(
            connection,
            outbox_id=outbox_id,
            kind=OutboxKind.ATTEMPT_DISPATCH,
            task_id=task_id,
            attempt_id=attempt_id,
            command_id=command.command_id,
            scope=command.scope,
            spec=spec,
            now=observed_at,
        )
        self._hit("successor.after_outbox")
        return result

    @staticmethod
    def _validate_successor_command(command: CommandEnvelope) -> None:
        """Re-prove the successor wire shape at the direct Store boundary."""

        try:
            if not isinstance(command, CommandEnvelope):
                raise TypeError("successor command must be a CommandEnvelope")
            reparsed = CommandEnvelope.from_dict(command.to_dict())
            payload = reparsed.payload
            revision_number = payload["expected_predecessor_revision_number"]
            event_head = payload["expected_predecessor_event_head"]
            if (
                reparsed.command_type != "task.create_successor"
                or reparsed.required_capabilities != ("task.create_successor",)
                or type(revision_number) is not int
                or revision_number < 1
                or revision_number > MAX_SAFE_INTEGER
                or type(event_head) is not int
                or event_head < 0
                or event_head > MAX_SAFE_INTEGER
            ):
                raise ValueError("successor command authority is not canonical")
        except (
            ContractViolation,
            KeyError,
            TypeError,
            ValueError,
            UnicodeError,
        ) as error:
            raise FormalTaskViolation(
                "TASK_SUCCESSOR_INVALID",
                "task.create_successor command is not canonical",
                ErrorCode.INVALID_ARGUMENT,
            ) from error

    def update(
        self,
        command: CommandEnvelope,
        *,
        observed_at: str,
    ) -> ResultEnvelope:
        """Atomically replace mutable pre-dispatch specification fields."""

        payload = command.payload
        if type(payload) is not dict or set(payload) != {
            "attempt_id",
            "expected_event_head",
            "instruction",
            "constraints",
        }:
            raise FormalTaskViolation(
                "TASK_UPDATE_INVALID",
                "task.update payload must contain the exact update authority",
                ErrorCode.INVALID_ARGUMENT,
            )
        fingerprint = command.fingerprint()
        scope_key = _scope_key(command.scope)
        task_id = command.target_ref.id
        with self._transaction() as connection:
            replay = self._command_replay(connection, command, fingerprint)
            if replay is not None:
                return replay
            task = self._require_task_row(connection, task_id, command.scope)
            self._verify_durable_lineage(connection, task)
            attempt = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=?",
                (task["attempt_id"],),
            ).fetchone()
            dispatch = connection.execute(
                """
                SELECT * FROM outbox
                WHERE task_id=? AND attempt_id=? AND kind=?
                ORDER BY created_at, outbox_id
                """,
                (
                    task_id,
                    task["attempt_id"],
                    OutboxKind.ATTEMPT_DISPATCH.value,
                ),
            ).fetchall()
            queue_control = self._queue_control(task, attempt, dispatch)
            if (
                not queue_control.can_update
                or payload["attempt_id"] != task["attempt_id"]
                or payload["expected_event_head"] != int(task["event_head"])
            ):
                return self._persist_business_decision(
                    connection,
                    command,
                    fingerprint,
                    disposition=TaskCommandDisposition.CONFLICT,
                    code=ErrorCode.STALE,
                    reason="TASK_UPDATE_PRECONDITION_STALE",
                    message=("task.update requires the exact untouched accepted dispatch"),
                    observed_at=observed_at,
                )
            prior_spec = FormalTaskSpec.from_dict(_json_load(task["spec_json"]))
            instruction = payload["instruction"]
            constraints = payload["constraints"]
            updated_spec = replace(
                prior_spec,
                instruction=(prior_spec.instruction if instruction is None else instruction),
                constraints=(prior_spec.constraints if constraints is None else tuple(constraints)),
            )
            requested_event = self._append_event(
                connection,
                task,
                event_type="task.update_requested",
                state=FormalTaskState.ACCEPTED.value,
                outcome=None,
                producer="task_core.control",
                source_event_id=None,
                causation_id=command.command_id,
                occurred_at=observed_at,
                details={"command_id": command.command_id},
            )
            self._hit("update.after_requested_event")
            connection.execute(
                "UPDATE tasks SET spec_json=?, updated_at=? WHERE task_id=?",
                (_json_dump(updated_spec.to_dict()), observed_at, task_id),
            )
            self._hit("update.after_task")
            dispatch_payload = self._outbox_payload(dispatch[0]["payload_json"])
            replacement_payload = {
                "scope": dispatch_payload[0].to_dict(),
                "spec": updated_spec.to_dict(),
                "executor_ref": dispatch_payload[2],
            }
            connection.execute(
                "UPDATE outbox SET payload_json=?, updated_at=? WHERE outbox_id=?",
                (
                    _json_dump(replacement_payload),
                    observed_at,
                    dispatch[0]["outbox_id"],
                ),
            )
            self._hit("update.after_outbox")
            current_task = self._require_task_row(connection, task_id, command.scope)
            applied_event = self._append_event(
                connection,
                current_task,
                event_type="task.update_applied",
                state=FormalTaskState.ACCEPTED.value,
                outcome=None,
                producer="task_core.control",
                source_event_id=None,
                causation_id=command.command_id,
                occurred_at=observed_at,
                details={"command_id": command.command_id},
            )
            self._hit("update.after_applied_event")
            result = ResultEnvelope.success(
                owner=command,
                result={
                    "task_id": task_id,
                    "attempt_id": task["attempt_id"],
                    "state": FormalTaskState.ACCEPTED.value,
                    "applied": True,
                    "outbox_id": dispatch[0]["outbox_id"],
                },
                observed_at=observed_at,
                extensions=command_result_extensions(
                    TaskCommandDisposition.APPLIED,
                    admission_event_id=requested_event.event_id,
                    settlement_event_id=applied_event.event_id,
                ),
            )
            self._insert_command(
                connection,
                command,
                fingerprint,
                scope_key,
                result,
                observed_at,
            )
            self._hit("update.after_command")
            return result

    def reprioritize(
        self,
        command: CommandEnvelope,
        *,
        observed_at: str,
    ) -> ResultEnvelope:
        """Atomically change the priority of one provably pre-effect dispatch."""

        payload = command.payload
        if (
            command.command_type != "task.reprioritize"
            or type(payload) is not dict
            or set(payload) != {"attempt_id", "expected_event_head", "priority", "reason"}
        ):
            raise FormalTaskViolation(
                "TASK_CONTROL_INVALID",
                "task.reprioritize payload must contain the exact queue authority",
                ErrorCode.INVALID_ARGUMENT,
            )
        try:
            priority = AdmissionPriority(payload["priority"])
        except (TypeError, ValueError) as error:
            raise FormalTaskViolation(
                "TASK_CONTROL_INVALID",
                "task.reprioritize priority is invalid",
                ErrorCode.INVALID_ARGUMENT,
            ) from error
        fingerprint = command.fingerprint()
        scope_key = _scope_key(command.scope)
        task_id = command.target_ref.id
        with self._transaction() as connection:
            replay = self._command_replay(connection, command, fingerprint)
            if replay is not None:
                return replay
            task = self._require_task_row(connection, task_id, command.scope)
            self._verify_durable_lineage(connection, task)
            attempt = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=?",
                (task["attempt_id"],),
            ).fetchone()
            dispatches = connection.execute(
                """
                SELECT * FROM outbox
                WHERE task_id=? AND attempt_id=? AND kind=?
                ORDER BY created_at, outbox_id
                """,
                (
                    task_id,
                    task["attempt_id"],
                    OutboxKind.ATTEMPT_DISPATCH.value,
                ),
            ).fetchall()
            stale = (
                attempt is None
                or attempt["task_id"] != task_id
                or payload["attempt_id"] != task["attempt_id"]
                or payload["expected_event_head"] != int(task["event_head"])
            )
            if stale:
                return self._persist_business_decision(
                    connection,
                    command,
                    fingerprint,
                    disposition=TaskCommandDisposition.CONFLICT,
                    code=ErrorCode.STALE,
                    reason="TASK_CONTROL_PRECONDITION_STALE",
                    message=("task.reprioritize requires the exact current attempt and event head"),
                    observed_at=observed_at,
                )
            assert attempt is not None
            if not self._queue_control(task, attempt, dispatches).can_reprioritize:
                return self._persist_business_decision(
                    connection,
                    command,
                    fingerprint,
                    disposition=TaskCommandDisposition.CONFLICT,
                    code=ErrorCode.CONFLICT,
                    reason="TASK_CONTROL_STATE_CONFLICT",
                    message="task state does not admit queued reprioritization",
                    observed_at=observed_at,
                )
            details = {"command_id": command.command_id, "priority": priority.value}
            requested_event = self._append_event(
                connection,
                task,
                event_type="task.reprioritize_requested",
                state=FormalTaskState.ACCEPTED.value,
                outcome=None,
                producer="task_core.control",
                source_event_id=None,
                causation_id=command.command_id,
                occurred_at=observed_at,
                details=details,
            )
            self._hit("reprioritize.after_requested_event")
            connection.execute(
                """
                UPDATE attempts SET admission_priority=?, updated_at=?
                WHERE attempt_id=?
                """,
                (priority.value, observed_at, attempt["attempt_id"]),
            )
            self._hit("reprioritize.after_attempt")
            current_task = self._require_task_row(connection, task_id, command.scope)
            applied_event = self._append_event(
                connection,
                current_task,
                event_type="task.reprioritize_applied",
                state=FormalTaskState.ACCEPTED.value,
                outcome=None,
                producer="task_core.control",
                source_event_id=None,
                causation_id=command.command_id,
                occurred_at=observed_at,
                details=details,
            )
            self._hit("reprioritize.after_applied_event")
            result = ResultEnvelope.success(
                owner=command,
                result={
                    "task_id": task_id,
                    "attempt_id": attempt["attempt_id"],
                    "state": FormalTaskState.ACCEPTED.value,
                    "priority": priority.value,
                    "applied": True,
                },
                observed_at=observed_at,
                extensions=command_result_extensions(
                    TaskCommandDisposition.APPLIED,
                    admission_event_id=requested_event.event_id,
                    settlement_event_id=applied_event.event_id,
                ),
            )
            self._insert_command(
                connection,
                command,
                fingerprint,
                scope_key,
                result,
                observed_at,
            )
            self._hit("reprioritize.after_command")
            return result

    def decide_unsupported_control(
        self,
        command: CommandEnvelope,
        *,
        observed_at: str,
    ) -> ResultEnvelope:
        """Persist one sanitized decision for controls without an Executor primitive."""

        if command.command_type not in {
            "task.provide_input",
            "task.pause",
            "task.resume",
            "task.reprioritize",
        }:
            raise FormalTaskViolation(
                "TASK_CONTROL_INVALID",
                "unsupported-control decision received a different operation",
                ErrorCode.INVALID_ARGUMENT,
            )
        payload = command.payload
        fingerprint = command.fingerprint()
        task_id = command.target_ref.id
        with self._transaction() as connection:
            replay = self._command_replay(connection, command, fingerprint)
            if replay is not None:
                return replay
            task = self._require_task_row(connection, task_id, command.scope)
            self._verify_durable_lineage(connection, task)
            attempt = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=?",
                (task["attempt_id"],),
            ).fetchone()
            current_event = connection.execute(
                "SELECT * FROM task_events WHERE task_id=? AND seq=?",
                (task_id, int(task["event_head"])),
            ).fetchone()
            stale = (
                attempt is None
                or attempt["task_id"] != task_id
                or payload.get("attempt_id") != task["attempt_id"]
                or payload.get("expected_event_head") != int(task["event_head"])
                or current_event is None
                or current_event["attempt_id"] != task["attempt_id"]
            )
            state_conflict = task["state"] == FormalTaskState.TERMINAL.value
            if command.command_type == "task.provide_input" and not stale:
                state_conflict = state_conflict or (
                    task["state"] != FormalTaskState.DECISION_REQUIRED.value
                    or current_event["event_type"] != "task.decision_required"
                    or payload.get("responds_to_event_id") != current_event["event_id"]
                )
            if stale:
                disposition = TaskCommandDisposition.CONFLICT
                error = ContractViolation(
                    ErrorCode.STALE,
                    "TASK_CONTROL_PRECONDITION_STALE",
                    "task control requires the exact current attempt and event head",
                ).error
            elif state_conflict:
                disposition = TaskCommandDisposition.CONFLICT
                error = ContractViolation(
                    ErrorCode.CONFLICT,
                    "TASK_CONTROL_STATE_CONFLICT",
                    "task state does not admit this control",
                ).error
            else:
                disposition = TaskCommandDisposition.UNSUPPORTED
                error = ContractViolation(
                    ErrorCode.CAPABILITY_UNAVAILABLE,
                    "TASK_CONTROL_UNSUPPORTED",
                    "the formal Task Core has no durable primitive for this control",
                ).error
            return self._persist_business_decision(
                connection,
                command,
                fingerprint,
                disposition=disposition,
                code=error.code,
                reason=error.reason,
                message=error.message,
                observed_at=observed_at,
            )

    def has_pending_adjustments(self, task_id: str, attempt_id: str, scope: ScopeRef) -> bool:
        """Read the exact live Attempt's admission fence without claiming work.

        The Executor waits for normal Core outbox delivery. This read is the
        model-boundary cutoff for adjustments accepted but not delivered yet.
        """
        with self._snapshot_reader() as connection:
            task = self._require_task_row(connection, task_id, scope)
            if task["attempt_id"] != attempt_id or task["state"] == FormalTaskState.TERMINAL.value:
                raise FormalTaskViolation(
                    "TASK_ADJUSTMENT_STALE", "model boundary no longer owns the live Attempt", ErrorCode.STALE
                )
            return (
                connection.execute(
                    "SELECT 1 FROM outbox WHERE task_id=? AND attempt_id=? AND kind=? AND state IN (?, ?) LIMIT 1",
                    (
                        task_id,
                        attempt_id,
                        OutboxKind.ATTEMPT_ADJUST.value,
                        OutboxState.PENDING.value,
                        OutboxState.CLAIMED.value,
                    ),
                ).fetchone()
                is not None
            )

    def adjust(
        self,
        command: CommandEnvelope,
        *,
        observed_at: str,
        mutation_precondition: TaskMutationPrecondition | None = None,
    ) -> ResultEnvelope:
        """Atomically admit one adjustment for an exact addressed attempt."""

        payload = command.payload
        if type(payload) is not dict or set(payload) - {"native_source"} != {"adjustment"}:
            raise FormalTaskViolation(
                "TASK_ADJUSTMENT_INVALID",
                "task.adjust payload must contain exactly one adjustment",
                ErrorCode.INVALID_ARGUMENT,
            )
        # Validate content before opening the write transaction; the final carrier
        # is reconstructed with the authoritative request-event sequence below.
        source = require_payload_source(command)
        TaskAdjustmentRequest(command.command_id, payload["adjustment"], 1, source)
        mutation_precondition = self._require_mutation_precondition(command, mutation_precondition)
        fingerprint = self._mutation_fingerprint(command, mutation_precondition)
        scope_key = _scope_key(command.scope)
        task_id = command.target_ref.id
        with self._transaction() as connection:
            replay = self._command_replay(connection, command, fingerprint)
            if replay is not None:
                return replay
            task = self._require_task_row(connection, task_id, command.scope)
            self._verify_durable_lineage(connection, task)
            if self._mutation_precondition_stale(task, mutation_precondition):
                return self._persist_business_decision(
                    connection,
                    command,
                    fingerprint,
                    disposition=TaskCommandDisposition.CONFLICT,
                    code=ErrorCode.STALE,
                    reason="TASK_MUTATION_PRECONDITION_STALE",
                    message=("task mutation requires the exact confirmed Attempt and event head"),
                    observed_at=observed_at,
                )
            attempt = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=?",
                (task["attempt_id"],),
            ).fetchone()
            if (
                not task["cancel_requested"]
                and not task["dispatch_fenced"]
                and (
                    task["state"] == FormalTaskState.RUNNING.value
                    or (
                        task["state"] == FormalTaskState.TERMINAL.value
                        and task["outcome"] == TerminalOutcome.COMPLETED.value
                    )
                )
                and self.adjustment_queue.closed(connection, task)
            ):
                return self.adjustment_queue.defer(connection, command, fingerprint, task, observed_at)
            if (
                task["state"] != FormalTaskState.RUNNING.value
                or task["cancel_requested"]
                or task["dispatch_fenced"]
                or attempt is None
                or attempt["task_id"] != task_id
                or attempt["state"] != FormalAttemptState.RUNNING.value
            ):
                return self._persist_business_decision(
                    connection,
                    command,
                    fingerprint,
                    disposition=TaskCommandDisposition.CONFLICT,
                    code=ErrorCode.CONFLICT,
                    reason="TASK_ADJUSTMENT_STATE_CONFLICT",
                    message="task.adjust requires the exact running Task and Attempt",
                    observed_at=observed_at,
                )
            requested_event = self._append_event(
                connection,
                task,
                event_type="task.adjust_requested",
                state=task["state"],
                outcome=None,
                producer="task_core.control",
                source_event_id=None,
                causation_id=command.command_id,
                occurred_at=observed_at,
                details={"command_id": command.command_id},
            )
            adjustment = TaskAdjustmentRequest(
                command.command_id,
                payload["adjustment"],
                requested_event.seq,
                source,
            )
            outbox_id = f"outbox-{uuid.uuid4().hex}"
            self._insert_outbox(
                connection,
                outbox_id=outbox_id,
                kind=OutboxKind.ATTEMPT_ADJUST,
                task_id=task_id,
                attempt_id=task["attempt_id"],
                command_id=command.command_id,
                scope=command.scope,
                spec=FormalTaskSpec.from_dict(_json_load(task["spec_json"])),
                now=observed_at,
                executor_ref=attempt["executor_ref"],
                adjustment=adjustment,
            )
            result = ResultEnvelope.success(
                owner=command,
                result={
                    "task_id": task_id,
                    "attempt_id": task["attempt_id"],
                    "adjustment_id": command.command_id,
                    "adjustment_state": TaskAdjustmentState.PENDING.value,
                    "reason": None,
                    "outbox_id": outbox_id,
                },
                observed_at=observed_at,
                extensions=command_result_extensions(
                    TaskCommandDisposition.ACCEPTED,
                    admission_event_id=requested_event.event_id,
                ),
            )
            self._insert_command(
                connection,
                command,
                fingerprint,
                scope_key,
                result,
                observed_at,
            )
            return result

    def _settle_cancel_before_dispatch(
        self,
        connection: sqlite3.Connection,
        *,
        task: sqlite3.Row,
        dispatch: sqlite3.Row,
        command_id: str,
        scope: ScopeRef,
        now: str,
    ) -> PersistentTaskEvent:
        """Settle cancellation only after the caller proves queue ownership."""
        task_id = task["task_id"]
        connection.execute(
            """
            UPDATE outbox SET state=?, updated_at=?
            WHERE outbox_id=? AND state=?
            """,
            (
                OutboxState.SUPPRESSED.value,
                now,
                dispatch["outbox_id"],
                OutboxState.PENDING.value,
            ),
        )
        connection.execute(
            """
            UPDATE attempts SET state=?, outcome=?, updated_at=?
            WHERE attempt_id=?
            """,
            (
                FormalAttemptState.TERMINAL.value,
                TerminalOutcome.CANCELLED.value,
                now,
                task["attempt_id"],
            ),
        )
        self._append_event(
            connection,
            task,
            event_type="attempt.terminal",
            state=FormalAttemptState.TERMINAL.value,
            outcome=TerminalOutcome.CANCELLED.value,
            producer="task_core.reconciliation",
            source_event_id=None,
            causation_id=command_id,
            occurred_at=now,
            details={"reason": "CANCELLED_BEFORE_DISPATCH"},
        )
        task = self._require_task_row(connection, task_id, scope)
        self._reject_open_adjustments_before_terminal(
            connection,
            task=task,
            observed_at=now,
        )
        return self._append_event(
            connection,
            self._require_task_row(connection, task_id, scope),
            event_type="task.terminal",
            state=FormalTaskState.TERMINAL.value,
            outcome=TerminalOutcome.CANCELLED.value,
            producer="task_core",
            source_event_id=None,
            causation_id=command_id,
            occurred_at=now,
            details={"reason": "CANCELLED_BEFORE_DISPATCH"},
            update_task=True,
        )

    def cancel(
        self,
        command: CommandEnvelope,
        *,
        observed_at: str,
        mutation_precondition: TaskMutationPrecondition | None = None,
    ) -> ResultEnvelope:
        mutation_precondition = self._require_mutation_precondition(command, mutation_precondition)
        fingerprint = self._mutation_fingerprint(command, mutation_precondition)
        scope_key = _scope_key(command.scope)
        task_id = command.target_ref.id
        with self._transaction() as connection:
            replay = self._command_replay(connection, command, fingerprint)
            if replay is not None:
                return replay
            task = self._require_task_row(connection, task_id, command.scope)
            self._verify_durable_lineage(connection, task)
            if self._mutation_precondition_stale(task, mutation_precondition):
                return self._persist_business_decision(
                    connection,
                    command,
                    fingerprint,
                    disposition=TaskCommandDisposition.CONFLICT,
                    code=ErrorCode.STALE,
                    reason="TASK_MUTATION_PRECONDITION_STALE",
                    message=("task mutation requires the exact confirmed Attempt and event head"),
                    observed_at=observed_at,
                )
            if task["state"] == FormalTaskState.TERMINAL.value:
                if task["outcome"] == TerminalOutcome.INTERRUPTED.value:
                    connection.execute(
                        """INSERT INTO durability_recovery_fences(
                               task_id, producer_attempt_id, cancel_command_id,
                               created_at)
                           VALUES(?, ?, ?, ?)
                           ON CONFLICT(task_id) DO NOTHING""",
                        (
                            task_id,
                            task["attempt_id"],
                            command.command_id,
                            observed_at,
                        ),
                    )
                    self._hit("durability.recovery_fence.after_insert")
                return self._persist_business_decision(
                    connection,
                    command,
                    fingerprint,
                    disposition=TaskCommandDisposition.CONFLICT,
                    code=ErrorCode.CONFLICT,
                    reason="TASK_ALREADY_TERMINAL",
                    message="terminal tasks cannot accept cancellation",
                    observed_at=observed_at,
                )
            if bool(task["cancel_requested"]):
                if mutation_precondition is not None:
                    # A different command cannot claim a positive no-op without
                    # a new immutable event that proves its command-time head.
                    # Preserve exact-command replay, but record a fresh request
                    # as a sanitized durable conflict bound to the current Store
                    # authority instead of inferring history during reopen.
                    return self._persist_business_decision(
                        connection,
                        command,
                        fingerprint,
                        disposition=TaskCommandDisposition.CONFLICT,
                        code=ErrorCode.CONFLICT,
                        reason="TASK_CANCEL_ALREADY_REQUESTED",
                        message="task cancellation has already been requested",
                        observed_at=observed_at,
                    )
                result = ResultEnvelope.success(
                    owner=command,
                    result={
                        "task_id": task_id,
                        "attempt_id": task["attempt_id"],
                        "cancel_acknowledged": True,
                        "applied": False,
                        "state": task["state"],
                    },
                    observed_at=observed_at,
                    extensions=command_result_extensions(TaskCommandDisposition.ACCEPTED),
                )
                self._insert_command(
                    connection,
                    command,
                    fingerprint,
                    scope_key,
                    result,
                    observed_at,
                )
                return result

            dispatch = connection.execute(
                """
                SELECT * FROM outbox
                WHERE task_id=? AND attempt_id=? AND kind=?
                ORDER BY created_at, outbox_id LIMIT 1
                """,
                (
                    task_id,
                    task["attempt_id"],
                    OutboxKind.ATTEMPT_DISPATCH.value,
                ),
            ).fetchone()
            if dispatch is None:
                raise FormalTaskViolation(
                    "TASK_DISPATCH_RECORD_MISSING",
                    "task has no durable dispatch record",
                    ErrorCode.INTERNAL,
                )
            attempt = connection.execute("SELECT * FROM attempts WHERE attempt_id=?", (task["attempt_id"],)).fetchone()
            closed_queue = attempt is not None and self._is_exact_unbound_queue(task, attempt, dispatch)
            now = observed_at
            connection.execute(
                """
                UPDATE tasks SET cancel_requested=1, dispatch_fenced=1, updated_at=?
                WHERE task_id=?
                """,
                (now, task_id),
            )
            self._hit("cancel.after_snapshot")
            requested_event = self._append_event(
                connection,
                task,
                event_type="task.cancel_requested",
                state=task["state"],
                outcome=task["outcome"],
                producer="task_core.control",
                source_event_id=None,
                causation_id=command.command_id,
                occurred_at=now,
                details={"command_id": command.command_id},
            )
            self._hit("cancel.after_request_event")
            task = self._require_task_row(connection, task_id, command.scope)
            terminal_before_dispatch = (
                dispatch["state"] == OutboxState.PENDING.value and int(dispatch["delivery_count"]) == 0
            ) or closed_queue
            cancel_outbox_id: str | None = None
            settlement_event: PersistentTaskEvent | None = None
            if terminal_before_dispatch:
                settlement_event = self._settle_cancel_before_dispatch(
                    connection,
                    task=task,
                    dispatch=dispatch,
                    command_id=command.command_id,
                    scope=command.scope,
                    now=now,
                )
            else:
                cancel_outbox_id = f"outbox-{uuid.uuid4().hex}"
                self._insert_outbox(
                    connection,
                    outbox_id=cancel_outbox_id,
                    kind=OutboxKind.ATTEMPT_CANCEL,
                    task_id=task_id,
                    attempt_id=task["attempt_id"],
                    command_id=command.command_id,
                    scope=command.scope,
                    spec=FormalTaskSpec.from_dict(_json_load(task["spec_json"])),
                    now=now,
                    executor_ref=connection.execute(
                        "SELECT executor_ref FROM attempts WHERE attempt_id=?",
                        (task["attempt_id"],),
                    ).fetchone()["executor_ref"],
                )
            self._hit("cancel.after_outbox_or_terminal")
            result = ResultEnvelope.success(
                owner=command,
                result={
                    "task_id": task_id,
                    "attempt_id": task["attempt_id"],
                    "cancel_acknowledged": True,
                    "applied": terminal_before_dispatch,
                    "state": (FormalTaskState.TERMINAL.value if terminal_before_dispatch else task["state"]),
                    "outbox_id": cancel_outbox_id,
                },
                observed_at=observed_at,
                extensions=command_result_extensions(
                    (TaskCommandDisposition.APPLIED if terminal_before_dispatch else TaskCommandDisposition.ACCEPTED),
                    admission_event_id=requested_event.event_id,
                    settlement_event_id=(None if settlement_event is None else settlement_event.event_id),
                ),
            )
            self._insert_command(
                connection,
                command,
                fingerprint,
                scope_key,
                result,
                observed_at,
            )
            self._hit("cancel.after_command")
            return result

    @staticmethod
    def _require_mutation_precondition(
        command: CommandEnvelope,
        precondition: TaskMutationPrecondition | None,
    ) -> TaskMutationPrecondition | None:
        if precondition is None:
            return None
        if type(precondition) is not TaskMutationPrecondition:
            raise FormalTaskViolation(
                "TASK_MUTATION_PRECONDITION_INVALID",
                "task mutation precondition must be trusted and typed",
                ErrorCode.INVALID_ARGUMENT,
            )
        if command.command_type not in {"task.cancel", "task.adjust"} or precondition.task_id != command.target_ref.id:
            raise FormalTaskViolation(
                "TASK_MUTATION_PRECONDITION_INVALID",
                "task mutation precondition does not bind the exact command target",
                ErrorCode.INVALID_ARGUMENT,
            )
        return precondition

    @staticmethod
    def _mutation_precondition_stale(
        task: sqlite3.Row,
        precondition: TaskMutationPrecondition | None,
    ) -> bool:
        return precondition is not None and (
            task["task_id"] != precondition.task_id
            or task["attempt_id"] != precondition.attempt_id
            or int(task["event_head"]) != precondition.expected_event_head
        )

    @staticmethod
    def _mutation_fingerprint(
        command: CommandEnvelope,
        precondition: TaskMutationPrecondition | None,
    ) -> bytes:
        if precondition is None:
            return command.fingerprint()
        if type(precondition) is not TaskMutationPrecondition:
            raise FormalTaskViolation(
                "TASK_MUTATION_PRECONDITION_INVALID",
                "task mutation precondition must have its exact trusted type",
                ErrorCode.INVALID_ARGUMENT,
            )
        return canonical_json_bytes(
            {
                "command": json.loads(command.fingerprint()),
                "mutation_precondition": {
                    "task_id": precondition.task_id,
                    "attempt_id": precondition.attempt_id,
                    "expected_event_head": precondition.expected_event_head,
                },
            }
        )

    @staticmethod
    def _mutation_precondition_from_fingerprint(
        command_type: str,
        fingerprint: object,
    ) -> TaskMutationPrecondition | None:
        if (
            command_type in {"task.cancel", "task.adjust"}
            and type(fingerprint) is dict
            and set(fingerprint) == {"command", "mutation_precondition"}
        ):
            return TaskMutationPrecondition.from_dict(fingerprint["mutation_precondition"])
        return None

    @staticmethod
    def _retry_fingerprint(
        command: CommandEnvelope,
        selection: PersistedExecutorSelection | None = None,
    ) -> bytes:
        if selection is None:
            return command.fingerprint()
        return canonical_json_bytes(
            {
                "command": json.loads(command.fingerprint()),
                "executor_selection": _selection_fingerprint_payload(selection),
            }
        )

    @classmethod
    def _is_durable_retry_business_error(
        cls,
        connection: sqlite3.Connection,
        command: CommandEnvelope,
        error: FormalTaskViolation,
    ) -> bool:
        expected = _RETRY_BUSINESS_DECISIONS.get(error.reason)
        if expected is None or error.code not in expected[1]:
            return False
        if error.reason != "TASK_RETRY_PRECONDITION_STALE":
            return True
        task_row = connection.execute(
            "SELECT correlation_id FROM tasks WHERE task_id=? AND scope_key=?",
            (command.target_ref.id, _scope_key(command.scope)),
        ).fetchone()
        return task_row is not None and task_row["correlation_id"] == command.correlation_id

    def read_retry_authority(
        self,
        command: CommandEnvelope,
        *,
        observed_at: str | None = None,
        selection: PersistedExecutorSelection | None = None,
    ) -> ResultEnvelope | TaskRetryAuthoritySnapshot:
        """Return exact replay or one side-effect-free retry admission snapshot."""

        fingerprint = self._retry_fingerprint(command, selection)
        with self._transaction() as connection:
            replay = self._verified_retry_replay(connection, command, fingerprint)
            if replay is not None:
                return replay
            try:
                return self._retry_authority_from_connection(connection, command)
            except FormalTaskViolation as error:
                if not self._is_durable_retry_business_error(connection, command, error):
                    raise
                return self._persist_business_decision(
                    connection,
                    command,
                    fingerprint,
                    disposition=TaskCommandDisposition.CONFLICT,
                    code=error.code,
                    reason=error.reason,
                    message=str(error),
                    observed_at=observed_at or command.issued_at,
                )

    def read_current_retry_authority(
        self,
        *,
        scope: ScopeRef,
        task_id: str,
    ) -> TaskRetryAuthoritySnapshot:
        """Derive the current retry payload without accepting client lineage."""

        if (
            not isinstance(scope, ScopeRef)
            or scope.assurance.value != "authenticated"
            or type(task_id) is not str
            or not task_id.strip()
        ):
            raise FormalTaskViolation(
                "TASK_RETRY_AUTHORITY_FACTS_INVALID",
                "retry authority requires an authenticated scope and exact task id",
                ErrorCode.INVALID_ARGUMENT,
            )
        with self._reader() as connection:
            connection.execute("BEGIN")
            task, attempt = self._retry_state_from_connection(
                connection,
                scope=scope,
                task_id=task_id,
            )
            return self._current_retry_authority_from_state(
                connection,
                task=task,
                attempt=attempt,
            )

    def read_applied_retry_replay(
        self,
        *,
        scope: ScopeRef,
        command_id: str,
        task_id: str,
        product_request: TaskRetryProductRequestFingerprint,
    ) -> AppliedTaskRetryReplay | None:
        """Resolve an applied retry from product-owned identity only.

        The original canonical command, including its server-derived predecessor
        payload, is reconstructed from the durable ledger.  Callers never submit
        that payload back to the Store to prove an applied replay.
        """

        if (
            not isinstance(scope, ScopeRef)
            or scope.assurance.value != "authenticated"
            or type(command_id) is not str
            or not command_id.strip()
            or type(task_id) is not str
            or not task_id.strip()
            or type(product_request) is not TaskRetryProductRequestFingerprint
        ):
            raise FormalTaskViolation(
                "TASK_RETRY_REPLAY_FACTS_INVALID",
                "applied retry lookup requires exact authenticated product facts",
                ErrorCode.INVALID_ARGUMENT,
            )
        with self._reader() as connection:
            connection.execute("BEGIN")
            row = connection.execute(
                """
                SELECT * FROM commands WHERE scope_key=? AND command_id=?
                """,
                (_scope_key(scope), command_id),
            ).fetchone()
            if row is None:
                return None
            try:
                stored_result = ResultEnvelope.from_dict(_json_load(row["result_json"]))
            except ContractViolation as error:
                raise self._corrupt("retry command result is not canonical") from error
            if not stored_result.ok:
                binding, _original_result = self._verify_business_decision(
                    connection,
                    row,
                    expected=_RETRY_BUSINESS_DECISIONS,
                )
                authority = binding["authority"]
                payload_authority = authority["payload"]
                if (
                    row["command_type"] != "task.retry"
                    or binding["command_type"] != "task.retry"
                    or binding["target_task_id"] != task_id
                    or payload_authority.get("product_request_sha256") != product_request.sha256
                ):
                    raise FormalTaskViolation(
                        "IDEMPOTENCY_CONFLICT",
                        "command_id is already bound to different retry facts",
                        ErrorCode.CONFLICT,
                    )
                task_row = self._require_task_row(connection, task_id, scope)
                self._verify_durable_lineage(connection, task_row)
                return None
            original_command, original_result, resolved_spec = self._command_ledger_from_row(row)
            if (
                row["command_type"] != "task.retry"
                or original_command.command_type != "task.retry"
                or original_command.command_id != command_id
                or original_command.scope != scope
                or original_command.target_ref.kind.value != "task"
                or original_command.target_ref.id != task_id
            ):
                raise FormalTaskViolation(
                    "IDEMPOTENCY_CONFLICT",
                    "command_id is already bound to different task facts",
                    ErrorCode.CONFLICT,
                )
            try:
                stored_product_request = TaskRetryProductRequestFingerprint.from_extensions(original_command.extensions)
            except FormalTaskViolation as error:
                raise self._corrupt("applied retry command lacks its product request identity") from error
            if stored_product_request != product_request:
                raise FormalTaskViolation(
                    "IDEMPOTENCY_CONFLICT",
                    "command_id is already bound to different product request facts",
                    ErrorCode.CONFLICT,
                )
            task_row = self._require_task_row(connection, task_id, scope)
            self._verify_durable_lineage(connection, task_row)
            if resolved_spec is not None:
                raise FormalTaskViolation(
                    "TASK_STORE_CORRUPT",
                    "retry command ledger contains create-only specification facts",
                    ErrorCode.INTERNAL,
                )
            result = original_result.result
            if result is None or result.get("task_id") != task_id:
                raise FormalTaskViolation(
                    "TASK_STORE_CORRUPT",
                    "retry command result does not bind the requested task",
                    ErrorCode.INTERNAL,
                )
            attempt_id = result.get("attempt_id")
            if type(attempt_id) is not str:
                raise FormalTaskViolation(
                    "TASK_STORE_CORRUPT",
                    "retry command result has no successor attempt",
                    ErrorCode.INTERNAL,
                )
            outbox_row = connection.execute(
                """
                SELECT payload_json FROM outbox
                WHERE outbox_id=? AND task_id=? AND attempt_id=? AND command_id=?
                  AND kind=?
                """,
                (
                    result.get("outbox_id"),
                    task_id,
                    attempt_id,
                    command_id,
                    OutboxKind.ATTEMPT_DISPATCH.value,
                ),
            ).fetchone()
            if outbox_row is None:
                raise FormalTaskViolation(
                    "TASK_STORE_CORRUPT",
                    "retry command is missing its durable dispatch",
                    ErrorCode.INTERNAL,
                )
            payload = self._outbox_payload(outbox_row["payload_json"])
            precondition = TaskRetryPrecondition.from_payload(original_command.payload)
            return AppliedTaskRetryReplay(
                original_command=original_command,
                original_result=original_result,
                precondition=precondition,
                resulting_spec=payload[1],
            )

    def read_durable_recovery_authority(
        self,
        *,
        scope: ScopeRef,
        task_id: str,
    ) -> DurableRecoveryAuthoritySnapshot:
        """Read an interrupted-producer snapshot without granting mutation."""

        with self._snapshot_reader() as connection:
            task, attempt = self._retry_state_from_connection(
                connection,
                scope=scope,
                task_id=task_id,
            )
            return self._current_durable_recovery_authority(
                connection,
                task=task,
                attempt=attempt,
            )

    def read_task_durability_diagnostics(
        self,
        *,
        scope: ScopeRef,
        task_id: str,
    ) -> TaskDurabilityDiagnosticSnapshot:
        """Read only current, content-free durability diagnostic identities.

        The snapshot is not mutation, recovery, reconciliation, or Executor
        authority.  Prefixes are verified before their latest public identity
        is returned; Task payload, checkpoint state, effect facts, errors and
        outbox payloads never cross this seam.
        """

        with self._snapshot_reader() as connection:
            task = self._require_task_row(connection, task_id, scope)
            attempt_id = task["attempt_id"]
            attempt = connection.execute(
                "SELECT * FROM attempts WHERE task_id=? AND attempt_id=?",
                (task_id, attempt_id),
            ).fetchone()
            if attempt is None:
                raise self._corrupt("current Task Attempt is missing")
            event_head = int(task["event_head"])
            event_head_row = connection.execute(
                "SELECT event_id FROM task_events WHERE task_id=? AND seq=?",
                (task_id, event_head),
            ).fetchone()
            if event_head_row is None:
                raise self._corrupt("current Task event head is missing")
            binding = self._durability_binding_from_connection(
                connection,
                scope=scope,
                task_id=task_id,
                origin_attempt_id=attempt_id,
            )
            checkpoints = self._verified_checkpoint_prefix(connection, binding)
            effects = self._verified_effect_prefix(connection, binding)
            diagnostic_checkpoints = checkpoints
            diagnostic_effects = effects
            diagnostic_checkpoint_attempt_id = attempt_id
            diagnostic_effect_attempt_id = attempt_id

            recovery = connection.execute(
                """SELECT recovery_id, producer_attempt_id, checkpoint_head,
                          checkpoint_prefix_digest, effect_head,
                          effect_prefix_digest
                   FROM durability_recoveries
                   WHERE task_id=? AND recovery_attempt_id=?""",
                (task_id, attempt_id),
            ).fetchone()
            recovery_id: str | None = None
            if recovery is not None:
                source_binding = self._durability_binding_from_connection(
                    connection,
                    scope=scope,
                    task_id=task_id,
                    origin_attempt_id=recovery["producer_attempt_id"],
                )
                source_checkpoints = self._verified_checkpoint_prefix(
                    connection,
                    source_binding,
                    expected_head=int(recovery["checkpoint_head"]),
                    expected_prefix_digest=recovery["checkpoint_prefix_digest"],
                )
                source_effects = self._verified_effect_prefix(
                    connection,
                    source_binding,
                    expected_head=int(recovery["effect_head"]),
                    expected_prefix_digest=recovery["effect_prefix_digest"],
                )
                if not diagnostic_checkpoints.records:
                    diagnostic_checkpoints = source_checkpoints
                    diagnostic_checkpoint_attempt_id = recovery["producer_attempt_id"]
                if not diagnostic_effects.records:
                    diagnostic_effects = source_effects
                    diagnostic_effect_attempt_id = recovery["producer_attempt_id"]
                recovery_id = recovery["recovery_id"]

            outbox_rows = connection.execute(
                _OUTBOX_BINDING_SELECT
                + """ WHERE o.task_id=? AND o.attempt_id=?
                     AND NOT EXISTS (
                         SELECT 1 FROM outbox AS newer
                         WHERE newer.task_id=o.task_id
                           AND newer.attempt_id=o.attempt_id
                           AND newer.kind=o.kind
                           AND (
                               newer.created_at>o.created_at
                               OR (newer.created_at=o.created_at
                                   AND newer.outbox_id>o.outbox_id)
                           )
                     )
                   ORDER BY o.kind""",
                (task_id, attempt_id),
            ).fetchall()
            for row in outbox_rows:
                if (
                    row["canonical_attempt_id"] != row["attempt_id"]
                    or row["attempt_task_id"] != row["task_id"]
                    or row["canonical_task_id"] != row["task_id"]
                    or row["task_attempt_id"] != row["attempt_id"]
                    or row["task_scope_key"] != _scope_key(scope)
                    or ScopeRef.from_dict(_json_load(row["task_scope_json"])) != scope
                    or row["bound_executor_id"] != attempt["executor_id"]
                    or int(row["delivery_count"]) < 0
                ):
                    raise self._corrupt("current outbox diagnostic binding is inconsistent")
                if row["canonical_command_id"] != row["command_id"]:
                    recovery_outbox = connection.execute(
                        """SELECT 1 FROM durability_recoveries
                           WHERE task_id=? AND recovery_attempt_id=?
                             AND recovery_id=?""",
                        (row["task_id"], row["attempt_id"], row["command_id"]),
                    ).fetchone()
                    if (
                        OutboxKind(row["kind"]) is not OutboxKind.ATTEMPT_DISPATCH
                        or row["canonical_command_id"] is not None
                        or recovery_outbox is None
                    ):
                        raise self._corrupt("current outbox diagnostic command binding is inconsistent")
            outbox = tuple(
                TaskOutboxDiagnosticFact(
                    outbox_id=row["outbox_id"],
                    kind=OutboxKind(row["kind"]),
                    task_id=row["task_id"],
                    attempt_id=row["attempt_id"],
                    command_id=row["command_id"],
                    state=OutboxState(row["state"]),
                    delivery_count=int(row["delivery_count"]),
                )
                for row in outbox_rows
            )
            reconciliation = task["reconciliation_state"]
            return TaskDurabilityDiagnosticSnapshot(
                task_id=task_id,
                attempt_id=attempt_id,
                executor_id=attempt["executor_id"],
                event_head=event_head,
                event_head_id=event_head_row["event_id"],
                checkpoint_id=(
                    None if not diagnostic_checkpoints.records else diagnostic_checkpoints.records[-1].checkpoint_id
                ),
                checkpoint_attempt_id=(
                    None if not diagnostic_checkpoints.records else diagnostic_checkpoint_attempt_id
                ),
                effect_id=(
                    None if not diagnostic_effects.records else diagnostic_effects.records[-1].binding.effect_id
                ),
                effect_attempt_id=(None if not diagnostic_effects.records else diagnostic_effect_attempt_id),
                recovery_id=recovery_id,
                reconciliation_state=(None if reconciliation is None else ReconciliationState(reconciliation)),
                outbox=outbox,
            )

    def read_durable_recovery_dispatch(
        self,
        *,
        scope: ScopeRef,
        task_id: str,
        recovery_attempt_id: str,
    ) -> tuple[str, int, int, str, int, str] | None:
        """Read exact Store continuation facts for one linked dispatch Attempt."""

        with self._snapshot_reader() as connection:
            self._require_task_row(connection, task_id, scope)
            row = connection.execute(
                """SELECT producer_attempt_id, recovery_generation,
                          checkpoint_head, checkpoint_prefix_digest,
                          effect_head, effect_prefix_digest
                   FROM durability_recoveries
                   WHERE task_id=? AND recovery_attempt_id=?""",
                (task_id, recovery_attempt_id),
            ).fetchone()
            if row is None:
                return None
            binding = self._durability_binding_from_connection(
                connection,
                scope=scope,
                task_id=task_id,
                origin_attempt_id=row["producer_attempt_id"],
            )
            checkpoints = self._verified_checkpoint_prefix(connection, binding)
            effects = self._verified_effect_prefix(connection, binding)
            if (
                checkpoints.head != row["checkpoint_head"]
                or checkpoints.prefix_digest != row["checkpoint_prefix_digest"]
                or effects.head != row["effect_head"]
                or effects.prefix_digest != row["effect_prefix_digest"]
            ):
                raise FormalTaskViolation(
                    "TASK_RECOVERY_PREFIX_STALE",
                    "linked recovery no longer binds current Store tips",
                    ErrorCode.STALE,
                )
            return (
                row["producer_attempt_id"],
                row["recovery_generation"],
                row["checkpoint_head"],
                row["checkpoint_prefix_digest"],
                row["effect_head"],
                row["effect_prefix_digest"],
            )

    def fork_durability_lineage(
        self,
        source_binding: DurabilityReadBinding,
        *,
        candidate_attempt_id: str,
        recovery_generation: int,
        observed_at: str,
        authorization: DurabilityMutationAuthorization | None = None,
    ) -> tuple[VerifiedCheckpointPrefix, VerifiedEffectPrefix]:
        """Atomically copy immutable producer facts into one linked lineage."""

        payload_digest = _durability_authorization_payload_digest(
            {
                "candidate_attempt_id": candidate_attempt_id,
                "recovery_generation": recovery_generation,
            }
        )
        with self._transaction() as connection:
            stored_source = self._durability_binding_from_connection(
                connection,
                scope=source_binding.scope,
                task_id=source_binding.task_id,
                origin_attempt_id=source_binding.origin_attempt_id,
            )
            if stored_source != source_binding:
                raise FormalTaskViolation(
                    "DURABILITY_BINDING_MISMATCH",
                    "lineage source does not match persisted selection",
                    ErrorCode.CONFLICT,
                )
            source_checkpoints, source_effects = self._consume_durability_authorization(
                connection,
                authorization,
                operation="lineage.fork",
                binding=source_binding,
                candidate_attempt_id=candidate_attempt_id,
                payload_digest=payload_digest,
                observed_at=observed_at,
            )
            recovery_row = connection.execute(
                """SELECT * FROM durability_recoveries
                   WHERE task_id=? AND producer_attempt_id=?
                     AND recovery_attempt_id=? AND recovery_generation=?""",
                (
                    source_binding.task_id,
                    source_binding.origin_attempt_id,
                    candidate_attempt_id,
                    recovery_generation,
                ),
            ).fetchone()
            candidate_binding = self._durability_binding_from_connection(
                connection,
                scope=source_binding.scope,
                task_id=source_binding.task_id,
                origin_attempt_id=candidate_attempt_id,
            )
            if (
                recovery_row is None
                or not source_checkpoints.records
                or candidate_binding.profile != source_binding.profile
                or candidate_binding.logical_origin_attempt_id != source_binding.logical_origin_attempt_id
            ):
                raise FormalTaskViolation(
                    "DURABILITY_LINEAGE_FORK_STALE",
                    "linked lineage no longer matches its authorized producer",
                    ErrorCode.STALE,
                )
            existing_checkpoints = self._checkpoint_rows(connection, candidate_binding)
            existing_effects = self._effect_rows(connection, candidate_binding)
            if not existing_effects:
                for row in self._effect_rows(connection, source_binding):
                    connection.execute(
                        """INSERT INTO durability_effect_facts(
                               task_id, origin_attempt_id, row_sequence, canonical,
                               payload_digest, created_at)
                           VALUES(?, ?, ?, ?, ?, ?)""",
                        (
                            candidate_binding.task_id,
                            candidate_attempt_id,
                            row.row_sequence,
                            row.canonical_bytes,
                            row.payload_digest,
                            observed_at,
                        ),
                    )
            target_effects = self._verified_effect_prefix(connection, candidate_binding)
            prior = source_checkpoints.records[-1]
            checkpoint = D1Checkpoint.create(
                checkpoint_id=f"checkpoint-{candidate_attempt_id}-1",
                scope=prior.scope,
                task_id=prior.task_id,
                producer_attempt_id=candidate_attempt_id,
                checkpoint_sequence=1,
                recovery_generation=recovery_generation,
                profile=prior.profile,
                complete=True,
                task_spec_digest=prior.task_spec_digest,
                context_version=prior.context_version,
                context_digest=prior.context_digest,
                input_digest=prior.input_digest,
                state_schema_id=prior.state_schema_id,
                state_schema_version=prior.state_schema_version,
                state_bytes=prior.state_bytes,
                effect_head=target_effects.head,
                effect_prefix_digest=target_effects.prefix_digest,
            )
            canonical = checkpoint.canonical_bytes()
            digest = hashlib.sha256(canonical).hexdigest()
            if not existing_checkpoints:
                connection.execute(
                    """INSERT INTO durability_checkpoints(
                           task_id, producer_attempt_id, row_sequence, canonical,
                           payload_digest, created_at)
                       VALUES(?, ?, 1, ?, ?, ?)""",
                    (
                        candidate_binding.task_id,
                        candidate_attempt_id,
                        canonical,
                        digest,
                        observed_at,
                    ),
                )
            elif len(existing_checkpoints) != 1 or existing_checkpoints[0].canonical_bytes != canonical:
                raise FormalTaskViolation(
                    "DURABILITY_LINEAGE_FORK_CONFLICT",
                    "linked lineage already contains different immutable facts",
                    ErrorCode.CONFLICT,
                )
            return (
                self._verified_checkpoint_prefix(connection, candidate_binding),
                target_effects,
            )

    def recover_durable_attempt(
        self,
        authority: DurableRecoveryAuthoritySnapshot,
        *,
        recovery_id: str,
        recovery_facts: ExecutorRecoveryFacts,
        checkpoint_head: int,
        checkpoint_prefix_digest: str,
        effect_head: int,
        effect_prefix_digest: str,
        observed_at: str,
        authorization: DurabilityMutationAuthorization | None = None,
        admission_policy: AdmissionPolicy | None = None,
    ) -> PersistentAttemptRecord:
        """Operator/Core-only linked recovery; no transport command is registered."""

        if type(recovery_id) is not str or not recovery_id.strip():
            raise FormalTaskViolation(
                "INVALID_DURABILITY_RECOVERY_ID",
                "durable recovery id must be one exact non-empty string",
                ErrorCode.INVALID_ARGUMENT,
            )
        with self._transaction() as connection:
            replay = connection.execute(
                "SELECT * FROM durability_recoveries WHERE recovery_id=?",
                (recovery_id,),
            ).fetchone()
            if replay is not None:
                replay_binding = self._durability_binding_from_connection(
                    connection,
                    scope=authority.task.scope,
                    task_id=replay["task_id"],
                    origin_attempt_id=replay["producer_attempt_id"],
                )
                replay_checkpoints = self._verified_checkpoint_prefix(connection, replay_binding)
                replay_effects = self._verified_effect_prefix(connection, replay_binding)
                if (
                    replay["task_id"] != authority.task.task_id
                    or replay["producer_attempt_id"] != authority.producer_attempt.attempt_id
                    or replay["recovery_attempt_id"] != recovery_facts.candidate_recovery_attempt_id
                    or replay["recovery_generation"] != recovery_facts.recovery_generation
                    or replay["profile_json"] != _json_dump(replay_binding.profile.to_dict())
                    or recovery_facts.scope != authority.task.scope
                    or recovery_facts.task_id != replay["task_id"]
                    or recovery_facts.producer_attempt_id != replay["producer_attempt_id"]
                    or recovery_facts.profile != replay_binding.profile
                    or bytes(replay["recovery_facts"]) != recovery_facts.canonical_bytes()
                    or replay["checkpoint_head"] != checkpoint_head
                    or replay["checkpoint_prefix_digest"] != checkpoint_prefix_digest
                    or replay["effect_head"] != effect_head
                    or replay["effect_prefix_digest"] != effect_prefix_digest
                    or replay_checkpoints.head != checkpoint_head
                    or replay_checkpoints.prefix_digest != checkpoint_prefix_digest
                    or replay_effects.head != effect_head
                    or replay_effects.prefix_digest != effect_prefix_digest
                ):
                    raise FormalTaskViolation(
                        "IDEMPOTENCY_CONFLICT",
                        "recovery id is already bound to different durability facts",
                        ErrorCode.CONFLICT,
                    )
                disposition = self._require_recovery_effect_safety(replay_effects)
                self._consume_durability_authorization(
                    connection,
                    authorization,
                    operation=f"recovery.admit.{disposition}",
                    binding=replay_binding,
                    candidate_attempt_id=recovery_facts.candidate_recovery_attempt_id,
                    payload_digest=_durability_authorization_payload_digest(
                        {
                            "recovery_id": recovery_id,
                            "recovery_facts_sha256": hashlib.sha256(recovery_facts.canonical_bytes()).hexdigest(),
                        }
                    ),
                    observed_at=observed_at,
                )
                attempt_row = connection.execute(
                    "SELECT * FROM attempts WHERE attempt_id=?",
                    (replay["recovery_attempt_id"],),
                ).fetchone()
                if attempt_row is None:
                    raise self._corrupt("durable recovery lost its linked Attempt")
                return self._attempt_from_row(attempt_row)

            task, attempt = self._retry_state_from_connection(
                connection,
                scope=authority.task.scope,
                task_id=authority.task.task_id,
            )
            current = self._current_durable_recovery_authority(
                connection,
                task=task,
                attempt=attempt,
            )
            if current != authority:
                raise FormalTaskViolation(
                    "TASK_RECOVERY_PRECONDITION_STALE",
                    "durable recovery predecessor changed before commit",
                    ErrorCode.STALE,
                )
            self._validate_durable_recovery(
                connection,
                authority=current,
                selection=attempt.selection,
                recovery_facts=recovery_facts,
                checkpoint_head=checkpoint_head,
                checkpoint_prefix_digest=checkpoint_prefix_digest,
                effect_head=effect_head,
                effect_prefix_digest=effect_prefix_digest,
                authorization=authorization,
                recovery_id=recovery_id,
                observed_at=observed_at,
            )
            attempt_id = recovery_facts.candidate_recovery_attempt_id
            outbox_id = f"outbox-{uuid.uuid4().hex}"
            event_id = f"event-{uuid.uuid4().hex}"
            next_seq = task.event_head + 1
            self._insert_attempt(
                connection,
                attempt_id=attempt_id,
                task_id=task.task_id,
                attempt_number=attempt.attempt_number + 1,
                executor_id=task.spec.executor_id,
                state=FormalAttemptState.ACCEPTED,
                observed_at=observed_at,
                selection=attempt.selection,
                admission_policy=admission_policy,
            )
            connection.execute(
                """INSERT INTO durability_recoveries(
                       recovery_id, task_id, producer_attempt_id,
                       recovery_attempt_id, recovery_generation, profile_json,
                       checkpoint_head, checkpoint_prefix_digest,
                       effect_head, effect_prefix_digest, recovery_facts, created_at)
                   VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    recovery_id,
                    task.task_id,
                    attempt.attempt_id,
                    attempt_id,
                    recovery_facts.recovery_generation,
                    _json_dump(recovery_facts.profile.to_dict()),
                    checkpoint_head,
                    checkpoint_prefix_digest,
                    effect_head,
                    effect_prefix_digest,
                    recovery_facts.canonical_bytes(),
                    observed_at,
                ),
            )
            self._insert_event(
                connection,
                event_id=event_id,
                task_id=task.task_id,
                attempt_id=attempt_id,
                scope=task.scope,
                seq=next_seq,
                event_type="task.recovery_accepted",
                state=FormalTaskState.ACCEPTED.value,
                outcome=None,
                producer="task_core",
                source_event_id=None,
                causation_id=recovery_id,
                correlation_id=task.correlation_id,
                occurred_at=observed_at,
                details={
                    "recovery_id": recovery_id,
                    "producer_attempt_id": attempt.attempt_id,
                    "producer_outcome": TerminalOutcome.INTERRUPTED.value,
                    "recovery_generation": recovery_facts.recovery_generation,
                    "recovery_budget_remaining": (authority.recovery_budget_remaining - 1),
                    "attempt_number": attempt.attempt_number + 1,
                },
            )
            self._insert_outbox(
                connection,
                outbox_id=outbox_id,
                kind=OutboxKind.ATTEMPT_DISPATCH,
                task_id=task.task_id,
                attempt_id=attempt_id,
                command_id=recovery_id,
                scope=task.scope,
                spec=task.spec,
                now=observed_at,
            )
            changed = connection.execute(
                """UPDATE tasks SET state=?, outcome=NULL, attempt_id=?,
                       cancel_requested=0, dispatch_fenced=0, event_head=?,
                       reconciliation_state=NULL, reconciliation_reason=NULL,
                       updated_at=?
                   WHERE task_id=? AND attempt_id=? AND state=? AND outcome=?
                     AND event_head=? AND cancel_requested=0
                     AND dispatch_fenced=0""",
                (
                    FormalTaskState.ACCEPTED.value,
                    attempt_id,
                    next_seq,
                    observed_at,
                    task.task_id,
                    attempt.attempt_id,
                    FormalTaskState.TERMINAL.value,
                    TerminalOutcome.INTERRUPTED.value,
                    task.event_head,
                ),
            ).rowcount
            if changed != 1:
                raise FormalTaskViolation(
                    "TASK_RECOVERY_PRECONDITION_STALE",
                    "durable recovery lost its Task CAS",
                    ErrorCode.STALE,
                )
            row = connection.execute("SELECT * FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
            assert row is not None
            return self._attempt_from_row(row)

    def retry(
        self,
        command: CommandEnvelope,
        spec: FormalTaskSpec,
        authority: TaskRetryAuthoritySnapshot,
        *,
        observed_at: str,
        selection: PersistedExecutorSelection | None = None,
        admission_policy: AdmissionPolicy | None = None,
    ) -> ResultEnvelope:
        """Atomically create one bounded successor attempt after exact re-CAS."""

        fingerprint = self._retry_fingerprint(command, selection)
        scope_key = _scope_key(command.scope)
        with self._transaction() as connection:
            replay = self._verified_retry_replay(connection, command, fingerprint)
            if replay is not None:
                return replay
            try:
                current = self._retry_authority_from_connection(connection, command)
            except FormalTaskViolation as error:
                if not self._is_durable_retry_business_error(connection, command, error):
                    raise
                return self._persist_business_decision(
                    connection,
                    command,
                    fingerprint,
                    disposition=TaskCommandDisposition.CONFLICT,
                    code=error.code,
                    reason=error.reason,
                    message=str(error),
                    observed_at=observed_at,
                )
            if current != authority:
                return self._persist_business_decision(
                    connection,
                    command,
                    fingerprint,
                    disposition=TaskCommandDisposition.CONFLICT,
                    code=ErrorCode.STALE,
                    reason="TASK_RETRY_PRECONDITION_STALE",
                    message="task retry authority changed before it could be applied",
                    observed_at=observed_at,
                )
            prior_spec = current.task.spec
            if (
                spec.name,
                spec.instruction,
                spec.origin,
                spec.executor_id,
                spec.required_capabilities,
                spec.side_effect_class,
                spec.constraints,
                spec.attributes,
                spec.native_source,
            ) != (
                prior_spec.name,
                prior_spec.instruction,
                prior_spec.origin,
                prior_spec.executor_id,
                prior_spec.required_capabilities,
                prior_spec.side_effect_class,
                prior_spec.constraints,
                prior_spec.attributes,
                prior_spec.native_source,
            ):
                raise FormalTaskViolation(
                    "TASK_RETRY_SPEC_MISMATCH",
                    "task.retry cannot replace the stable task specification",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
            if (
                spec.context.source,
                spec.context.stable_id,
                spec.context.uri,
                spec.context.scope,
            ) != (
                prior_spec.context.source,
                prior_spec.context.stable_id,
                prior_spec.context.uri,
                prior_spec.context.scope,
            ):
                raise FormalTaskViolation(
                    "TASK_RETRY_CONTEXT_IDENTITY_MISMATCH",
                    "retry context must preserve the task's stable project identity",
                    ErrorCode.PERMISSION_DENIED,
                )
            self._hit("retry.before_ids")
            attempt_id = f"attempt-{uuid.uuid4().hex}"
            outbox_id = f"outbox-{uuid.uuid4().hex}"
            event_id = f"event-{uuid.uuid4().hex}"
            precondition = current.precondition
            task = current.task
            next_seq = task.event_head + 1
            now = observed_at
            # D-058 retry is a bounded cross-attempt epoch compatibility path,
            # not a normal Task lifecycle edge.  It deliberately bypasses
            # ``_append_event``; P3-2 owns successor-Task revision semantics.
            self._insert_attempt(
                connection,
                attempt_id=attempt_id,
                task_id=task.task_id,
                attempt_number=precondition.attempt_number,
                executor_id=spec.executor_id,
                state=FormalAttemptState.ACCEPTED,
                observed_at=now,
                selection=selection,
                admission_policy=admission_policy,
            )
            self._hit("retry.after_attempt")
            self._insert_event(
                connection,
                event_id=event_id,
                task_id=task.task_id,
                attempt_id=attempt_id,
                scope=task.scope,
                seq=next_seq,
                event_type="task.retry_accepted",
                state=FormalTaskState.ACCEPTED.value,
                outcome=None,
                producer="task_core",
                source_event_id=None,
                causation_id=command.command_id,
                correlation_id=task.correlation_id,
                occurred_at=now,
                details={
                    "command_id": command.command_id,
                    "retry_of_attempt_id": precondition.previous_attempt_id,
                    "previous_outcome": precondition.previous_outcome.value,
                    "attempt_number": precondition.attempt_number,
                },
            )
            self._hit("retry.after_event")
            self._insert_outbox(
                connection,
                outbox_id=outbox_id,
                kind=OutboxKind.ATTEMPT_DISPATCH,
                task_id=task.task_id,
                attempt_id=attempt_id,
                command_id=command.command_id,
                scope=task.scope,
                spec=spec,
                now=now,
            )
            self._hit("retry.after_outbox")
            result = ResultEnvelope.success(
                owner=command,
                result={
                    "task_id": task.task_id,
                    "previous_attempt_id": precondition.previous_attempt_id,
                    "attempt_id": attempt_id,
                    "attempt_number": precondition.attempt_number,
                    "applied": True,
                    "state": FormalTaskState.ACCEPTED.value,
                    "outbox_id": outbox_id,
                },
                observed_at=observed_at,
                extensions={
                    **command_result_extensions(
                        TaskCommandDisposition.APPLIED,
                        admission_event_id=event_id,
                        settlement_event_id=event_id,
                    ),
                    "live_voice.store": {"durability": "sqlite_outbox"},
                },
            )
            self._insert_command(
                connection,
                command,
                fingerprint,
                scope_key,
                result,
                observed_at,
            )
            self._hit("retry.after_command")
            changed = connection.execute(
                """
                UPDATE tasks SET spec_json=?, state=?, outcome=NULL,
                    attempt_id=?, cancel_requested=0, dispatch_fenced=0,
                    event_head=?, reconciliation_state=NULL,
                    reconciliation_reason=NULL, updated_at=?
                WHERE task_id=? AND attempt_id=? AND state=? AND outcome=?
                  AND event_head=?
                """,
                (
                    _json_dump(spec.to_dict()),
                    FormalTaskState.ACCEPTED.value,
                    attempt_id,
                    next_seq,
                    now,
                    task.task_id,
                    precondition.previous_attempt_id,
                    FormalTaskState.TERMINAL.value,
                    precondition.previous_outcome.value,
                    task.event_head,
                ),
            ).rowcount
            if changed != 1:
                raise FormalTaskViolation(
                    "TASK_RETRY_PRECONDITION_STALE",
                    "task retry predecessor changed before commit",
                    ErrorCode.STALE,
                )
            self._hit("retry.after_task")
            return result

    def _retry_authority_from_connection(
        self, connection: sqlite3.Connection, command: CommandEnvelope
    ) -> TaskRetryAuthoritySnapshot:
        if command.command_type != "task.retry":
            raise FormalTaskViolation(
                "INVALID_FORMAL_TASK_OPERATION",
                "retry authority requires task.retry",
                ErrorCode.INVALID_ARGUMENT,
            )
        TaskRetryProductRequestFingerprint.from_extensions(command.extensions)
        precondition = TaskRetryPrecondition.from_payload(command.payload)
        task, attempt = self._retry_state_from_connection(
            connection,
            scope=command.scope,
            task_id=command.target_ref.id,
        )
        if command.correlation_id != task.correlation_id:
            raise FormalTaskViolation(
                "TASK_RETRY_PRECONDITION_STALE",
                "task.retry correlation does not match the authoritative task",
                ErrorCode.STALE,
            )
        if (
            task.state is FormalTaskState.TERMINAL
            and attempt.state is FormalAttemptState.TERMINAL
            and attempt.attempt_number >= 3
        ):
            raise FormalTaskViolation(
                "TASK_RETRY_LIMIT_EXCEEDED",
                "formal task permits at most three total attempts",
                ErrorCode.CONFLICT,
            )
        if (
            precondition.previous_attempt_id != attempt.attempt_id
            or precondition.attempt_number != attempt.attempt_number + 1
        ):
            raise FormalTaskViolation(
                "TASK_RETRY_PRECONDITION_STALE",
                "task.retry lineage does not match the current attempt epoch",
                ErrorCode.STALE,
            )
        authority = self._current_retry_authority_from_state(
            connection,
            task=task,
            attempt=attempt,
        )
        if precondition != authority.precondition:
            raise FormalTaskViolation(
                "TASK_RETRY_PRECONDITION_STALE",
                "task.retry lineage does not match the current terminal attempt",
                ErrorCode.STALE,
            )
        return authority

    def _retry_state_from_connection(
        self,
        connection: sqlite3.Connection,
        *,
        scope: ScopeRef,
        task_id: str,
    ) -> tuple[PersistentTaskRecord, PersistentAttemptRecord]:
        task_row = self._require_task_row(connection, task_id, scope)
        self._verify_durable_lineage(connection, task_row)
        task = self._task_from_row(task_row)
        attempt_row = connection.execute("SELECT * FROM attempts WHERE attempt_id=?", (task.attempt_id,)).fetchone()
        if attempt_row is None:
            raise FormalTaskViolation(
                "TASK_STORE_CORRUPT",
                "formal Task Store is missing the current attempt",
                ErrorCode.INTERNAL,
            )
        return task, self._attempt_from_row(attempt_row)

    def _current_retry_authority_from_state(
        self,
        connection: sqlite3.Connection,
        *,
        task: PersistentTaskRecord,
        attempt: PersistentAttemptRecord,
    ) -> TaskRetryAuthoritySnapshot:
        if (
            task.state is FormalTaskState.TERMINAL
            and attempt.state is FormalAttemptState.TERMINAL
            and attempt.attempt_number >= 3
        ):
            raise FormalTaskViolation(
                "TASK_RETRY_LIMIT_EXCEEDED",
                "formal task permits at most three total attempts",
                ErrorCode.CONFLICT,
            )
        if task.state is not FormalTaskState.TERMINAL:
            raise FormalTaskViolation(
                "TASK_RETRY_REQUIRES_TERMINAL",
                "task.retry requires a terminal current attempt",
                ErrorCode.CONFLICT,
            )
        if attempt.state is not FormalAttemptState.TERMINAL:
            raise FormalTaskViolation(
                "TASK_STORE_CORRUPT",
                "terminal task does not have a terminal current attempt",
                ErrorCode.INTERNAL,
            )
        if task.outcome != attempt.outcome:
            raise FormalTaskViolation(
                "TASK_STORE_CORRUPT",
                "task and attempt terminal outcomes disagree",
                ErrorCode.INTERNAL,
            )
        if task.outcome is not TerminalOutcome.CANCELLED:
            raise FormalTaskViolation(
                "TASK_RETRY_OUTCOME_NOT_ELIGIBLE",
                "new task.retry admission requires a cancelled predecessor",
                ErrorCode.CONFLICT,
            )
        expected = TaskRetryPrecondition(
            previous_attempt_id=attempt.attempt_id,
            previous_outcome=task.outcome,
            attempt_number=attempt.attempt_number + 1,
        )
        unsettled = connection.execute(
            """
            SELECT 1 FROM outbox
            WHERE task_id=? AND attempt_id=? AND state IN (?, ?)
            LIMIT 1
            """,
            (
                task.task_id,
                attempt.attempt_id,
                OutboxState.PENDING.value,
                OutboxState.CLAIMED.value,
            ),
        ).fetchone()
        if unsettled is not None:
            raise FormalTaskViolation(
                "TASK_RETRY_OUTBOX_PENDING",
                "predecessor delivery ownership is not settled",
                ErrorCode.UNAVAILABLE,
            )
        if task.reconciliation_state not in {None, ReconciliationState.RESOLVED}:
            raise FormalTaskViolation(
                "TASK_RETRY_RECONCILIATION_PENDING",
                "predecessor reconciliation ownership is not settled",
                ErrorCode.UNAVAILABLE,
            )
        return TaskRetryAuthoritySnapshot(task, attempt, expected)

    def _current_durable_recovery_authority(
        self,
        connection: sqlite3.Connection,
        *,
        task: PersistentTaskRecord,
        attempt: PersistentAttemptRecord,
    ) -> DurableRecoveryAuthoritySnapshot:
        if (
            task.state is not FormalTaskState.TERMINAL
            or attempt.state is not FormalAttemptState.TERMINAL
            or task.outcome is not TerminalOutcome.INTERRUPTED
            or attempt.outcome is not TerminalOutcome.INTERRUPTED
        ):
            raise FormalTaskViolation(
                "TASK_RECOVERY_REQUIRES_INTERRUPTED_PRODUCER",
                "durable recovery requires one terminal interrupted producer",
                ErrorCode.CONFLICT,
            )
        if attempt.attempt_number >= 3:
            raise FormalTaskViolation(
                "TASK_RECOVERY_LIMIT_EXCEEDED",
                "durable recovery permits at most three total Attempts",
                ErrorCode.CONFLICT,
            )
        if task.cancel_requested or task.dispatch_fenced:
            raise FormalTaskViolation(
                "TASK_RECOVERY_CANCELLED",
                "cancelled or fenced Task cannot admit recovery",
                ErrorCode.CONFLICT,
            )
        if task.reconciliation_state not in {None, ReconciliationState.RESOLVED}:
            raise FormalTaskViolation(
                "TASK_RECOVERY_RECONCILIATION_PENDING",
                "producer reconciliation ownership is not closed",
                ErrorCode.UNAVAILABLE,
            )
        unsettled = connection.execute(
            """SELECT 1 FROM outbox
               WHERE task_id=? AND attempt_id=? AND state IN (?, ?)
               LIMIT 1""",
            (
                task.task_id,
                attempt.attempt_id,
                OutboxState.PENDING.value,
                OutboxState.CLAIMED.value,
            ),
        ).fetchone()
        if unsettled is not None:
            raise FormalTaskViolation(
                "TASK_RECOVERY_PRODUCER_NOT_QUIESCENT",
                "producer delivery ownership is not settled",
                ErrorCode.UNAVAILABLE,
            )
        return DurableRecoveryAuthoritySnapshot(
            task=task,
            producer_attempt=attempt,
            recovery_generation=attempt.attempt_number,
            recovery_budget_remaining=3 - attempt.attempt_number,
        )

    @staticmethod
    def _require_recovery_effect_safety(prefix: VerifiedEffectPrefix) -> str:
        dispatch = next(
            (fact for fact in reversed(prefix.records) if type(fact) is ExternalEffectDispatch),
            None,
        )
        observation = next(
            (
                fact
                for fact in reversed(prefix.records)
                if type(fact) is ExternalEffectObservation
                and dispatch is not None
                and fact.binding.effect_id == dispatch.binding.effect_id
                and fact.dispatch_ordinal == dispatch.dispatch_ordinal
            ),
            None,
        )
        if dispatch is None or observation is None:
            raise FormalTaskViolation(
                "TASK_RECOVERY_EFFECT_UNKNOWN",
                "external effect truth is not closed for automatic recovery",
                ErrorCode.RESULT_UNKNOWN,
            )
        if observation.kind.value == "no_effect":
            return "continue"
        settlement = next(
            (
                fact
                for fact in reversed(prefix.records)
                if type(fact) is ExternalEffectSettlement
                and fact.binding.effect_id == dispatch.binding.effect_id
                and fact.recovery_generation == dispatch.recovery_generation
            ),
            None,
        )
        if observation.kind.value == "applied" and settlement is not None and settlement.kind.value == "resolved":
            return "applied"
        raise FormalTaskViolation(
            "TASK_RECOVERY_EFFECT_UNKNOWN",
            "external effect truth is not safe for automatic recovery",
            ErrorCode.RESULT_UNKNOWN,
        )

    def _validate_durable_recovery(
        self,
        connection: sqlite3.Connection,
        *,
        authority: DurableRecoveryAuthoritySnapshot,
        selection: PersistedExecutorSelection | None,
        recovery_facts: ExecutorRecoveryFacts,
        checkpoint_head: int | None,
        checkpoint_prefix_digest: str | None,
        effect_head: int | None,
        effect_prefix_digest: str | None,
        authorization: DurabilityMutationAuthorization | None,
        recovery_id: str,
        observed_at: str,
    ) -> None:
        producer = authority.producer_attempt
        task = authority.task
        binding = self._durability_binding_from_connection(
            connection,
            scope=task.scope,
            task_id=task.task_id,
            origin_attempt_id=producer.attempt_id,
        )
        generation_row = connection.execute(
            """SELECT COALESCE(MAX(recovery_generation), 0) AS generation
               FROM durability_recoveries
               WHERE task_id=?""",
            (task.task_id,),
        ).fetchone()
        cancel_fence = connection.execute(
            "SELECT 1 FROM durability_recovery_fences WHERE task_id=?",
            (task.task_id,),
        ).fetchone()
        if (
            selection is None
            or selection != producer.selection
            or binding.profile.durability_level not in {"D1", "D2"}
            or recovery_facts.scope != task.scope
            or recovery_facts.task_id != task.task_id
            or recovery_facts.producer_attempt_id != producer.attempt_id
            or recovery_facts.profile != binding.profile
            or recovery_facts.recovery_generation != generation_row["generation"] + 1
            or recovery_facts.recovery_generation != authority.recovery_generation
            or recovery_facts.is_expired(at=observed_at)
            or cancel_fence is not None
        ):
            raise FormalTaskViolation(
                "TASK_RECOVERY_FACTS_STALE",
                "durable recovery facts or Store claim do not bind current truth",
                ErrorCode.STALE,
            )
        if (
            checkpoint_head is None
            or checkpoint_head <= 0
            or checkpoint_prefix_digest is None
            or effect_head is None
            or effect_prefix_digest is None
        ):
            raise FormalTaskViolation(
                "TASK_RECOVERY_CHECKPOINT_REQUIRED",
                "durable recovery requires complete checkpoint and effect prefixes",
                ErrorCode.CAPABILITY_UNAVAILABLE,
            )
        checkpoints = self._verified_checkpoint_prefix(connection, binding)
        effects = self._verified_effect_prefix(connection, binding)
        if (
            checkpoints.head != checkpoint_head
            or checkpoints.prefix_digest != checkpoint_prefix_digest
            or effects.head != effect_head
            or effects.prefix_digest != effect_prefix_digest
        ):
            raise FormalTaskViolation(
                "TASK_RECOVERY_PREFIX_STALE",
                "recovery evidence does not bind the current full Store tips",
                ErrorCode.STALE,
            )
        if not checkpoints.records:
            raise FormalTaskViolation(
                "TASK_RECOVERY_CHECKPOINT_REQUIRED",
                "durable recovery requires one complete immutable checkpoint",
                ErrorCode.CAPABILITY_UNAVAILABLE,
            )
        checkpoint = checkpoints.records[-1]
        expected_context_version = str(task.spec.context.revision_value)
        if (
            checkpoint.recovery_generation != recovery_facts.recovery_generation - 1
            or checkpoint.task_spec_digest != hashlib.sha256(task.spec.fingerprint_bytes()).hexdigest()
            or checkpoint.context_version != expected_context_version
            or checkpoint.context_digest
            != hashlib.sha256(canonical_json_bytes(task.spec.context.to_dict())).hexdigest()
            or checkpoint.effect_head != effects.head
            or checkpoint.effect_prefix_digest != effects.prefix_digest
        ):
            raise FormalTaskViolation(
                "TASK_RECOVERY_CHECKPOINT_STALE",
                "checkpoint does not match exact Task/context/effect truth",
                ErrorCode.STALE,
            )
        disposition = self._require_recovery_effect_safety(effects)
        payload_digest = _durability_authorization_payload_digest(
            {
                "recovery_id": recovery_id,
                "recovery_facts_sha256": hashlib.sha256(recovery_facts.canonical_bytes()).hexdigest(),
            }
        )
        self._consume_durability_authorization(
            connection,
            authorization,
            operation=f"recovery.admit.{disposition}",
            binding=binding,
            candidate_attempt_id=recovery_facts.candidate_recovery_attempt_id,
            payload_digest=payload_digest,
            observed_at=observed_at,
        )

    @staticmethod
    def _corrupt(message: str) -> FormalTaskViolation:
        return FormalTaskViolation(
            "TASK_STORE_CORRUPT",
            message,
            ErrorCode.INTERNAL,
        )

    @classmethod
    def _command_ledger_from_row(
        cls, row: sqlite3.Row
    ) -> tuple[CommandEnvelope, ResultEnvelope, FormalTaskSpec | None]:
        def load() -> tuple[CommandEnvelope, ResultEnvelope, FormalTaskSpec | None]:
            result = ResultEnvelope.from_dict(_json_load(row["result_json"]))
            if result.command_id != row["command_id"] or (
                not result.ok and row["command_type"] != "task.create_successor"
            ):
                raise cls._corrupt("formal Task command ledger contains a non-canonical result")
            fingerprint_payload = _json_load(row["fingerprint"])
            resolved_spec: FormalTaskSpec | None = None
            selection: PersistedExecutorSelection | None = None
            if row["command_type"] in {"task.create", "task.create_successor"}:
                allowed_keys = {
                    frozenset({"command", "resolved_spec"}),
                    frozenset({"command", "resolved_spec", "executor_selection"}),
                }
                if type(fingerprint_payload) is not dict or frozenset(fingerprint_payload) not in allowed_keys:
                    raise cls._corrupt("task admission ledger fingerprint is not canonical")
                command_payload = fingerprint_payload["command"]
                resolved_spec = FormalTaskSpec.from_dict(fingerprint_payload["resolved_spec"])
                if "executor_selection" in fingerprint_payload:
                    selection = _selection_from_fingerprint_payload(fingerprint_payload["executor_selection"])
            elif row["command_type"] == "task.retry":
                if type(fingerprint_payload) is dict and set(fingerprint_payload) == {
                    "command",
                    "executor_selection",
                }:
                    command_payload = fingerprint_payload["command"]
                    selection = _selection_from_fingerprint_payload(fingerprint_payload["executor_selection"])
                else:
                    command_payload = fingerprint_payload
            else:
                raise cls._corrupt("attempt lineage references a non-admission command")
            if type(command_payload) is not dict or "request_id" in command_payload:
                raise cls._corrupt("formal Task command fingerprint is not canonical")
            canonical_command = dict(command_payload)
            canonical_command["request_id"] = result.request_id
            command = CommandEnvelope.from_dict(canonical_command)
            if (
                command.command_id != row["command_id"]
                or command.command_type != row["command_type"]
                or _scope_key(command.scope) != row["scope_key"]
            ):
                raise cls._corrupt("formal Task command ledger binding is inconsistent")
            if resolved_spec is not None:
                expected_payload: dict[str, object] = {
                    "command": json.loads(command.fingerprint()),
                    "resolved_spec": resolved_spec.to_dict(),
                }
                if selection is not None:
                    expected_payload["executor_selection"] = _selection_fingerprint_payload(selection)
                expected_fingerprint = canonical_json_bytes(expected_payload)
            else:
                expected_fingerprint = cls._retry_fingerprint(command, selection)
            if expected_fingerprint != row["fingerprint"]:
                raise cls._corrupt("formal Task command ledger fingerprint is inconsistent")
            return command, result, resolved_spec

        return _stored_record("command ledger", load)

    @classmethod
    def _outbox_payload(
        cls, payload_json: str | bytes
    ) -> tuple[ScopeRef, FormalTaskSpec, str | None, TaskAdjustmentRequest | None]:
        def load() -> tuple[ScopeRef, FormalTaskSpec, str | None, TaskAdjustmentRequest | None]:
            payload = _json_load(payload_json)
            if type(payload) is not dict or frozenset(payload) not in {
                frozenset({"scope", "spec", "executor_ref"}),
                frozenset({"scope", "spec", "executor_ref", "adjustment"}),
            }:
                raise cls._corrupt("formal Task dispatch payload is not canonical")
            scope = ScopeRef.from_dict(payload["scope"])
            spec = FormalTaskSpec.from_dict(payload["spec"])
            executor_ref = payload["executor_ref"]
            adjustment = None if "adjustment" not in payload else TaskAdjustmentRequest.from_dict(payload["adjustment"])
            if executor_ref is not None and type(executor_ref) is not str:
                raise cls._corrupt("formal Task dispatch executor_ref is invalid")
            if spec.context.scope != scope:
                raise cls._corrupt("formal Task dispatch context does not match scope")
            return scope, spec, executor_ref, adjustment

        return _stored_record("outbox", load)

    @classmethod
    def _control_command_from_row(cls, row: sqlite3.Row) -> tuple[CommandEnvelope, ResultEnvelope]:
        """Rebuild one control command without trusting its stored result."""

        def load() -> tuple[CommandEnvelope, ResultEnvelope]:
            result = ResultEnvelope.from_dict(_json_load(row["result_json"]))
            fingerprint = _json_load(row["fingerprint"])
            mutation_precondition = cls._mutation_precondition_from_fingerprint(
                row["command_type"],
                fingerprint,
            )
            if mutation_precondition is not None:
                command_fingerprint = fingerprint["command"]
            else:
                command_fingerprint = fingerprint
            if (
                result.command_id != row["command_id"]
                or (not result.ok and row["command_type"] != "task.adjust")
                or type(command_fingerprint) is not dict
                or "request_id" in command_fingerprint
            ):
                raise cls._corrupt("formal Task control command ledger is not canonical")
            command = CommandEnvelope.from_dict({"request_id": result.request_id, **command_fingerprint})
            if (
                command.command_id != row["command_id"]
                or command.command_type != row["command_type"]
                or _scope_key(command.scope) != row["scope_key"]
                or cls._mutation_fingerprint(command, mutation_precondition) != row["fingerprint"]
            ):
                raise cls._corrupt("formal Task control command ledger binding is inconsistent")
            return command, result

        return _stored_record("control command ledger", load)

    @staticmethod
    def _sha256_hex(value: bytes) -> str:
        return hashlib.sha256(value).hexdigest()

    @classmethod
    def _json_value_sha256(cls, value: object) -> str:
        return cls._sha256_hex(canonical_json_bytes(value))

    @classmethod
    def _decision_payload_authority(cls, command: CommandEnvelope) -> dict[str, object]:
        """Reduce a canonical command payload to closed non-content authority."""

        payload = command.payload
        authority: dict[str, object] = {}
        if command.command_type == "task.update":
            authority.update(
                {
                    "attempt_id": payload.get("attempt_id"),
                    "expected_event_head": payload.get("expected_event_head"),
                    "instruction_sha256": (
                        None if payload.get("instruction") is None else cls._json_value_sha256(payload["instruction"])
                    ),
                    "constraints_sha256": (
                        None if payload.get("constraints") is None else cls._json_value_sha256(payload["constraints"])
                    ),
                }
            )
        elif command.command_type in {
            "task.provide_input",
        }:
            authority.update(
                {
                    "attempt_id": payload.get("attempt_id"),
                    "expected_event_head": payload.get("expected_event_head"),
                    "responds_to_event_id": payload.get("responds_to_event_id"),
                    "text_sha256": (None if payload.get("text") is None else cls._json_value_sha256(payload["text"])),
                }
            )
        elif command.command_type in {"task.pause", "task.resume"}:
            authority.update(
                {
                    "attempt_id": payload.get("attempt_id"),
                    "expected_event_head": payload.get("expected_event_head"),
                    "reason_sha256": (
                        None if payload.get("reason") is None else cls._json_value_sha256(payload["reason"])
                    ),
                }
            )
        elif command.command_type == "task.reprioritize":
            authority.update(
                {
                    "attempt_id": payload.get("attempt_id"),
                    "expected_event_head": payload.get("expected_event_head"),
                    "priority": payload.get("priority"),
                    "reason_sha256": (
                        None if payload.get("reason") is None else cls._json_value_sha256(payload["reason"])
                    ),
                }
            )
        elif command.command_type == "task.adjust":
            authority["adjustment_sha256"] = cls._json_value_sha256(payload.get("adjustment"))
        elif command.command_type == "task.cancel":
            if payload:
                raise cls._corrupt("task.cancel decision payload is not empty")
        elif command.command_type == "task.retry":
            product_request = TaskRetryProductRequestFingerprint.from_extensions(command.extensions)
            authority.update(
                {
                    "previous_attempt_id": payload.get("previous_attempt_id"),
                    "previous_outcome": payload.get("previous_outcome"),
                    "attempt_number": payload.get("attempt_number"),
                    "product_request_sha256": product_request.sha256,
                }
            )
        elif command.command_type == "task.ack_events":
            authority.update(
                {
                    "presentation_class": payload.get("presentation_class"),
                    "acked_through_seq": payload.get("acked_through_seq"),
                    "acked_event_id": payload.get("acked_event_id"),
                    "expected_event_head": payload.get("expected_event_head"),
                }
            )
        elif command.command_type == "task.create_successor":
            authority.update(
                {
                    "expected_predecessor_revision_number": payload.get("expected_predecessor_revision_number"),
                    "expected_predecessor_event_head": payload.get("expected_predecessor_event_head"),
                    "predecessor_terminal_event_id": payload.get("predecessor_terminal_event_id"),
                    "predecessor_outcome": payload.get("predecessor_outcome"),
                    "predecessor_result_sha256": payload.get("predecessor_result_sha256"),
                    "name_sha256": cls._json_value_sha256(payload.get("name")),
                    "instruction_sha256": cls._json_value_sha256(payload.get("instruction")),
                    "constraints_sha256": cls._json_value_sha256(payload.get("constraints")),
                    "executor_id": payload.get("executor_id"),
                    "side_effect_class": payload.get("side_effect_class"),
                    "attributes_sha256": cls._json_value_sha256(payload.get("attributes")),
                }
            )
        else:
            raise cls._corrupt("formal Task business decision operation is not supported")
        return {
            "payload_sha256": cls._json_value_sha256(authority),
            **authority,
        }

    @classmethod
    def _decision_observed_authority(
        cls,
        connection: sqlite3.Connection,
        command: CommandEnvelope,
        *,
        reason: str,
    ) -> dict[str, object]:
        """Capture only immutable or monotonic facts used for the decision."""

        if command.command_type == "task.ack_events":
            task_row = connection.execute(
                "SELECT * FROM tasks WHERE task_id=?",
                (command.target_ref.id,),
            ).fetchone()
            if task_row is None:
                raise cls._corrupt("formal Task ACK decision lost its target")
            task_scope = ScopeRef.from_dict(_json_load(task_row["scope_json"]))
            if command.scope.subject_id != task_scope.subject_id or command.scope.project_id != task_scope.project_id:
                raise cls._corrupt("formal Task ACK decision crosses consumer scope")
            head_row = connection.execute(
                "SELECT event_id FROM task_events WHERE task_id=? AND seq=?",
                (task_row["task_id"], task_row["event_head"]),
            ).fetchone()
            acked_through_seq = command.payload.get("acked_through_seq")
            acknowledged_row = (
                None
                if type(acked_through_seq) is not int
                else connection.execute(
                    "SELECT event_id FROM task_events WHERE task_id=? AND seq=?",
                    (task_row["task_id"], acked_through_seq),
                ).fetchone()
            )
            if head_row is None:
                raise cls._corrupt("formal Task ACK decision lost its event-head authority")
            return {
                "reason": reason,
                "payload": cls._decision_payload_authority(command),
                "task": {
                    "task_id": task_row["task_id"],
                    "subject_id": task_scope.subject_id,
                    "project_id": task_scope.project_id,
                    "event_head": task_row["event_head"],
                    "head_event_id": head_row["event_id"],
                },
                "event_at_ack_seq_id": (None if acknowledged_row is None else acknowledged_row["event_id"]),
            }

        task_row = connection.execute(
            "SELECT * FROM tasks WHERE task_id=? AND scope_key=?",
            (command.target_ref.id, _scope_key(command.scope)),
        ).fetchone()
        if task_row is None:
            raise cls._corrupt("formal Task business decision lost its target")
        attempt_row = connection.execute(
            "SELECT * FROM attempts WHERE attempt_id=?",
            (task_row["attempt_id"],),
        ).fetchone()
        head_row = connection.execute(
            "SELECT * FROM task_events WHERE task_id=? AND seq=?",
            (task_row["task_id"], task_row["event_head"]),
        ).fetchone()
        dispatch_rows = connection.execute(
            """SELECT outbox_id, state, delivery_count, claimed_by, claimed_at,
                      claim_token, last_error FROM outbox
               WHERE task_id=? AND attempt_id=? AND kind=?
               ORDER BY outbox_id""",
            (
                task_row["task_id"],
                task_row["attempt_id"],
                OutboxKind.ATTEMPT_DISPATCH.value,
            ),
        ).fetchall()
        successor_rows = connection.execute(
            """SELECT task_id FROM tasks WHERE predecessor_task_id=?
               ORDER BY task_id""",
            (task_row["task_id"],),
        ).fetchall()
        result_rows = connection.execute(
            """SELECT * FROM task_results WHERE task_id=? AND attempt_id=?
               ORDER BY source_event_id""",
            (task_row["task_id"], task_row["attempt_id"]),
        ).fetchall()
        authority: dict[str, object] = {
            "reason": reason,
            "payload": cls._decision_payload_authority(command),
            "task": {
                "task_id": task_row["task_id"],
                "attempt_id": task_row["attempt_id"],
                "event_head": task_row["event_head"],
                "state": task_row["state"],
                "outcome": task_row["outcome"],
                "revision_number": task_row["revision_number"],
                "correlation_id": task_row["correlation_id"],
                "cancel_requested": task_row["cancel_requested"],
                "dispatch_fenced": task_row["dispatch_fenced"],
            },
            "attempt": (
                None
                if attempt_row is None
                else {
                    "attempt_id": attempt_row["attempt_id"],
                    "attempt_number": attempt_row["attempt_number"],
                    "state": attempt_row["state"],
                    "outcome": attempt_row["outcome"],
                }
            ),
            "head_event": (
                None
                if head_row is None
                else {
                    "event_id": head_row["event_id"],
                    "attempt_id": head_row["attempt_id"],
                    "seq": head_row["seq"],
                    "event_type": head_row["event_type"],
                    "state": head_row["state"],
                    "outcome": head_row["outcome"],
                    "occurred_at": head_row["occurred_at"],
                }
            ),
            "dispatch": [
                {
                    "outbox_id": row["outbox_id"],
                    "state": row["state"],
                    "delivery_count": row["delivery_count"],
                }
                for row in dispatch_rows
            ],
            "successor_task_ids": [row["task_id"] for row in successor_rows],
            "result_sha256s": [cls._json_value_sha256(cls._task_result_from_row(row).to_dict()) for row in result_rows],
        }
        if command.command_type == "task.reprioritize":
            dispatch_row = dispatch_rows[0] if len(dispatch_rows) == 1 else None
            admission_count = None if attempt_row is None else attempt_row["admission_attempt_count"]
            admission_reason = None if attempt_row is None else attempt_row["admission_reason"]
            delivery_matches = (
                dispatch_row is not None
                and type(admission_count) is int
                and dispatch_row["delivery_count"] == admission_count
            )
            authority["reprioritize"] = {
                "selection_present": (attempt_row is not None and attempt_row["adapter_id"] is not None),
                "pre_effect": (
                    attempt_row is not None and attempt_row["executor_ref"] is None and attempt_row["source_seq"] == -1
                ),
                "reconciliation_clear": task_row["reconciliation_state"] is None,
                "dispatch_pending_unclaimed": (
                    dispatch_row is not None
                    and dispatch_row["state"] == OutboxState.PENDING.value
                    and dispatch_row["claimed_by"] is None
                    and dispatch_row["claimed_at"] is None
                    and dispatch_row["claim_token"] is None
                ),
                "delivery_matches_admission": delivery_matches,
                "closed_defer_history": (
                    delivery_matches
                    and (
                        dispatch_row["delivery_count"] == 0
                        and admission_reason is None
                        or (
                            dispatch_row["delivery_count"] > 0
                            and admission_reason
                            in {
                                "EXECUTOR_PROJECT_BUSY",
                                "EXECUTOR_CAPACITY_EXHAUSTED",
                            }
                            and dispatch_row["last_error"] == admission_reason
                        )
                    )
                ),
            }
        return authority

    @classmethod
    def _business_decision_fingerprint(
        cls,
        connection: sqlite3.Connection,
        command: CommandEnvelope,
        replay_fingerprint: bytes,
        *,
        reason: str,
    ) -> bytes:
        authority = cls._decision_observed_authority(
            connection,
            command,
            reason=reason,
        )
        if reason in {
            "TASK_MUTATION_PRECONDITION_STALE",
            "TASK_CANCEL_ALREADY_REQUESTED",
        }:
            try:
                replay_payload = json.loads(replay_fingerprint)
                if (
                    type(replay_payload) is not dict
                    or set(replay_payload) != {"command", "mutation_precondition"}
                    or replay_payload["command"] != json.loads(command.fingerprint())
                ):
                    raise ValueError("non-canonical mutation replay")
                mutation_precondition = TaskMutationPrecondition.from_dict(replay_payload["mutation_precondition"])
            except (FormalTaskViolation, TypeError, ValueError) as error:
                raise cls._corrupt("formal Task mutation decision lost its exact precondition") from error
            authority["mutation_precondition"] = mutation_precondition.to_dict()
        binding: dict[str, object] = {
            "binding_type": _DECISION_BINDING_TYPE,
            "version": _DECISION_BINDING_VERSION,
            "command_sha256": cls._sha256_hex(command.fingerprint()),
            "replay_sha256": cls._sha256_hex(replay_fingerprint),
            "scope_sha256": cls._json_value_sha256(command.scope.to_dict()),
            "command_type": command.command_type,
            "target_task_id": command.target_ref.id,
            "correlation_id": command.correlation_id,
            "authority": authority,
            "authority_sha256": cls._json_value_sha256(authority),
        }
        binding["binding_sha256"] = cls._json_value_sha256(binding)
        return canonical_json_bytes(binding)

    @classmethod
    def _decision_binding_from_row(cls, row: sqlite3.Row) -> tuple[dict[str, object], ResultEnvelope]:
        """Decode a v1 sanitized negative decision without rebuilding its command."""

        def load() -> tuple[dict[str, object], ResultEnvelope]:
            result = ResultEnvelope.from_dict(_json_load(row["result_json"]))
            binding = _json_load(row["fingerprint"])
            expected_fields = {
                "binding_type",
                "version",
                "command_sha256",
                "replay_sha256",
                "scope_sha256",
                "command_type",
                "target_task_id",
                "correlation_id",
                "authority",
                "authority_sha256",
                "binding_sha256",
            }
            digest_fields = (
                "command_sha256",
                "replay_sha256",
                "scope_sha256",
                "authority_sha256",
                "binding_sha256",
            )
            if (
                result.ok
                or result.error is None
                or result.command_id != row["command_id"]
                or type(binding) is not dict
                or set(binding) != expected_fields
                or binding["binding_type"] != _DECISION_BINDING_TYPE
                or type(binding["version"]) is not int
                or binding["version"] != _DECISION_BINDING_VERSION
                or binding["command_type"] != row["command_type"]
                or type(binding["target_task_id"]) is not str
                or not binding["target_task_id"]
                or type(binding["correlation_id"]) is not str
                or not binding["correlation_id"]
                or type(binding["authority"]) is not dict
                or any(
                    type(binding[field]) is not str
                    or len(binding[field]) != 64
                    or any(character not in "0123456789abcdef" for character in binding[field])
                    for field in digest_fields
                )
                or binding["authority_sha256"] != cls._json_value_sha256(binding["authority"])
                or binding["binding_sha256"]
                != cls._json_value_sha256({key: value for key, value in binding.items() if key != "binding_sha256"})
            ):
                raise cls._corrupt("formal Task decision binding is not canonical")
            authority = binding["authority"]
            base_authority_fields = {
                "reason",
                "payload",
                "task",
                "attempt",
                "head_event",
                "dispatch",
                "successor_task_ids",
                "result_sha256s",
            }
            ack_authority_fields = {
                "reason",
                "payload",
                "task",
                "event_at_ack_seq_id",
            }
            authority_fields = frozenset(authority)
            reprioritize_snapshot = authority.get("reprioritize")
            mutation_precondition_snapshot = authority.get("mutation_precondition")
            if (
                authority_fields
                not in {
                    frozenset(base_authority_fields),
                    frozenset(base_authority_fields | {"reprioritize"}),
                    frozenset(base_authority_fields | {"mutation_precondition"}),
                    frozenset(ack_authority_fields),
                }
                or (
                    authority_fields == frozenset(ack_authority_fields) and binding["command_type"] != "task.ack_events"
                )
                or (
                    binding["command_type"] == "task.ack_events" and authority_fields != frozenset(ack_authority_fields)
                )
                or not isinstance(authority["payload"], dict)
                or (
                    "reprioritize" in authority
                    and (
                        binding["command_type"] != "task.reprioritize"
                        or type(reprioritize_snapshot) is not dict
                        or set(reprioritize_snapshot)
                        != {
                            "selection_present",
                            "pre_effect",
                            "reconciliation_clear",
                            "dispatch_pending_unclaimed",
                            "delivery_matches_admission",
                            "closed_defer_history",
                        }
                        or any(type(value) is not bool for value in reprioritize_snapshot.values())
                    )
                )
                or (
                    "mutation_precondition" in authority
                    and (
                        binding["command_type"] not in {"task.cancel", "task.adjust"}
                        or result.error.reason
                        not in {
                            "TASK_MUTATION_PRECONDITION_STALE",
                            "TASK_CANCEL_ALREADY_REQUESTED",
                        }
                        or type(mutation_precondition_snapshot) is not dict
                    )
                )
            ):
                raise cls._corrupt("formal Task decision authority is not canonical")
            return binding, result

        return _stored_record("decision command ledger", load)

    @classmethod
    def _verify_business_decision(
        cls,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        expected: Mapping[str, tuple[TaskCommandDisposition, frozenset[ErrorCode]]],
    ) -> tuple[dict[str, object], ResultEnvelope]:
        """Verify one negative row against closed immutable decision evidence."""

        binding, result = cls._decision_binding_from_row(row)
        error = result.error
        if error is None or error.reason not in expected:
            raise cls._corrupt("formal Task business decision reason is not canonical")
        if row["command_type"] == "task.ack_events":
            return cls._verify_ack_business_decision(
                connection,
                row,
                binding,
                result,
                expected=expected,
            )
        disposition, codes = expected[error.reason]
        task_row = connection.execute(
            "SELECT * FROM tasks WHERE task_id=? AND scope_key=?",
            (binding["target_task_id"], row["scope_key"]),
        ).fetchone()
        caused_events = connection.execute(
            """SELECT COUNT(*) FROM task_events AS e
               JOIN tasks AS t ON t.task_id=e.task_id
               WHERE t.scope_key=? AND e.causation_id=?""",
            (row["scope_key"], row["command_id"]),
        ).fetchone()[0]
        owned_outbox = connection.execute(
            """SELECT COUNT(*) FROM outbox AS o
               JOIN tasks AS t ON t.task_id=o.task_id
               WHERE t.scope_key=? AND o.command_id=?""",
            (row["scope_key"], row["command_id"]),
        ).fetchone()[0]
        if (
            task_row is None
            or binding["command_type"] != row["command_type"]
            or binding["scope_sha256"]
            != cls._json_value_sha256(ScopeRef.from_dict(_json_load(task_row["scope_json"])).to_dict())
            or result.result is not None
            or error.code not in codes
            or binding["authority"]["reason"] != error.reason
            or dict(result.extensions) != command_result_extensions(disposition)
            or result.observed_at != row["created_at"]
            or caused_events != 0
            or owned_outbox != 0
        ):
            raise cls._corrupt("formal Task business decision is not canonical")
        payload = binding["authority"]["payload"]
        cls._verify_decision_payload_authority(row["command_type"], payload)
        task, attempt, head, dispatch, successor_ids = cls._verify_decision_history(connection, row, binding, task_row)
        cls._verify_business_decision_reason(
            connection,
            row,
            binding,
            error.reason,
            payload,
            task,
            attempt,
            head,
            dispatch,
            successor_ids,
        )
        return binding, result

    @classmethod
    def _verify_ack_business_decision(
        cls,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        binding: dict[str, object],
        result: ResultEnvelope,
        *,
        expected: Mapping[str, tuple[TaskCommandDisposition, frozenset[ErrorCode]]],
    ) -> tuple[dict[str, object], ResultEnvelope]:
        """Verify one sanitized ACK conflict against its frozen Task head."""

        error = result.error
        if error is None or error.reason not in expected:
            raise cls._corrupt("formal Task ACK decision reason is not canonical")
        disposition, codes = expected[error.reason]
        try:
            command_scope = ScopeRef.from_dict(_json_load(row["scope_key"]))
        except (ContractViolation, FormalTaskViolation) as parse_error:
            raise cls._corrupt("formal Task ACK decision scope is not canonical") from parse_error
        task_row = connection.execute(
            "SELECT * FROM tasks WHERE task_id=?",
            (binding["target_task_id"],),
        ).fetchone()
        if task_row is None:
            raise cls._corrupt("formal Task ACK decision lost its target")
        try:
            task_scope = ScopeRef.from_dict(_json_load(task_row["scope_json"]))
        except (ContractViolation, FormalTaskViolation) as parse_error:
            raise cls._corrupt("formal Task ACK target scope is not canonical") from parse_error
        authority = binding["authority"]
        payload = authority["payload"]
        cls._verify_decision_payload_authority("task.ack_events", payload)
        task = authority["task"]
        task_fields = {
            "task_id",
            "subject_id",
            "project_id",
            "event_head",
            "head_event_id",
        }
        bound_head = None if type(task) is not dict else task.get("event_head")
        head_row = (
            None
            if type(bound_head) is not int
            else connection.execute(
                "SELECT event_id FROM task_events WHERE task_id=? AND seq=?",
                (binding["target_task_id"], bound_head),
            ).fetchone()
        )
        acked_through_seq = payload["acked_through_seq"]
        actual_event = (
            None
            if type(bound_head) is not int or acked_through_seq > bound_head
            else connection.execute(
                """SELECT event_id, occurred_at FROM task_events
                   WHERE task_id=? AND seq=?""",
                (binding["target_task_id"], acked_through_seq),
            ).fetchone()
        )
        actual_event_id = None if actual_event is None else actual_event["event_id"]
        decision_time = _canonical_utc_order_key(row["created_at"])
        actual_event_time = None if actual_event is None else _canonical_utc_order_key(actual_event["occurred_at"])
        if (
            _scope_key(command_scope) != row["scope_key"]
            or binding["scope_sha256"] != cls._json_value_sha256(command_scope.to_dict())
            or binding["command_type"] != "task.ack_events"
            or type(task) is not dict
            or set(task) != task_fields
            or task["task_id"] != binding["target_task_id"]
            or task["subject_id"] != task_scope.subject_id
            or task["project_id"] != task_scope.project_id
            or command_scope.assurance is not Assurance.AUTHENTICATED
            or task_scope.assurance is not Assurance.AUTHENTICATED
            or command_scope.subject_id != task_scope.subject_id
            or command_scope.project_id != task_scope.project_id
            or type(bound_head) is not int
            or not 0 <= bound_head <= int(task_row["event_head"])
            or head_row is None
            or task["head_event_id"] != head_row["event_id"]
            or authority["event_at_ack_seq_id"] != actual_event_id
            or decision_time is None
            or (actual_event is not None and actual_event_time is None)
            or result.result is not None
            or error.code not in codes
            or authority["reason"] != error.reason
            or dict(result.extensions) != command_result_extensions(disposition)
            or result.observed_at != row["created_at"]
        ):
            raise cls._corrupt("formal Task ACK decision is not canonical")
        expected_reason = (
            "TASK_ACK_PRECONDITION_STALE"
            if (acked_through_seq > payload["expected_event_head"] or payload["expected_event_head"] > bound_head)
            else (
                "TASK_ACK_EVENT_MISMATCH"
                if actual_event_id != payload["acked_event_id"]
                else (
                    "TASK_ACK_PRECONDITION_STALE"
                    if decision_time <= actual_event_time
                    or cls._ack_lacked_legacy_adoption(
                        connection,
                        row,
                        command_scope=command_scope,
                        task_id=binding["target_task_id"],
                        presentation_class=payload["presentation_class"],
                        acked_through_seq=acked_through_seq,
                    )
                    else None
                )
            )
        )
        if error.reason != expected_reason:
            raise cls._corrupt("formal Task ACK decision lacks closed historical evidence")
        return binding, result

    @classmethod
    def _ack_lacked_legacy_adoption(
        cls,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        command_scope: ScopeRef,
        task_id: str,
        presentation_class: str,
        acked_through_seq: int,
    ) -> bool:
        """Prove a valid-event ACK historically preceded seed adoption."""

        consumer = connection.execute(
            """SELECT * FROM task_event_consumption
               WHERE subject_id=? AND project_id=? AND task_id=?
                 AND presentation_class=?""",
            (
                command_scope.subject_id,
                command_scope.project_id,
                task_id,
                presentation_class,
            ),
        ).fetchone()
        anchor = None if consumer is None else cls._legacy_consumption_anchor_v1(connection, consumer)
        if anchor is None or acked_through_seq <= anchor[1]:
            return False
        command_rowid_row = connection.execute(
            "SELECT rowid FROM commands WHERE scope_key=? AND command_id=?",
            (row["scope_key"], row["command_id"]),
        ).fetchone()
        if command_rowid_row is None:
            raise cls._corrupt("formal Task ACK decision lost its ledger row")
        prior_rows = connection.execute(
            """SELECT rowid AS command_rowid, * FROM commands
               WHERE command_type='task.ack_events' AND rowid<?
               ORDER BY command_rowid""",
            (command_rowid_row["rowid"],),
        ).fetchall()
        for prior_row in prior_rows:
            try:
                prior_result = ResultEnvelope.from_dict(_json_load(prior_row["result_json"]))
            except (ContractViolation, FormalTaskViolation) as error:
                raise cls._corrupt("formal Task ACK command result is not canonical") from error
            if not prior_result.ok:
                continue
            prior_command, _result = cls._control_command_from_row(prior_row)
            prior_class = prior_command.payload.get("presentation_class")
            if (
                prior_command.scope.subject_id == command_scope.subject_id
                and prior_command.scope.project_id == command_scope.project_id
                and prior_command.target_ref.id == task_id
                and prior_class == presentation_class
            ):
                return False
        return True

    @staticmethod
    def _is_lower_sha256(value: object) -> bool:
        return type(value) is str and len(value) == 64 and all(character in "0123456789abcdef" for character in value)

    @classmethod
    def _verify_decision_payload_authority(
        cls,
        command_type: str,
        payload: object,
    ) -> None:
        """Validate the closed, content-free authority shape for one operation."""

        expected_fields = {
            "task.update": {
                "payload_sha256",
                "attempt_id",
                "expected_event_head",
                "instruction_sha256",
                "constraints_sha256",
            },
            "task.provide_input": {
                "payload_sha256",
                "attempt_id",
                "expected_event_head",
                "responds_to_event_id",
                "text_sha256",
            },
            "task.pause": {
                "payload_sha256",
                "attempt_id",
                "expected_event_head",
                "reason_sha256",
            },
            "task.resume": {
                "payload_sha256",
                "attempt_id",
                "expected_event_head",
                "reason_sha256",
            },
            "task.reprioritize": {
                "payload_sha256",
                "attempt_id",
                "expected_event_head",
                "priority",
                "reason_sha256",
            },
            "task.adjust": {"payload_sha256", "adjustment_sha256"},
            "task.cancel": {"payload_sha256"},
            "task.retry": {
                "payload_sha256",
                "previous_attempt_id",
                "previous_outcome",
                "attempt_number",
                "product_request_sha256",
            },
            "task.ack_events": {
                "payload_sha256",
                "presentation_class",
                "acked_through_seq",
                "acked_event_id",
                "expected_event_head",
            },
            "task.create_successor": {
                "payload_sha256",
                "expected_predecessor_revision_number",
                "expected_predecessor_event_head",
                "predecessor_terminal_event_id",
                "predecessor_outcome",
                "predecessor_result_sha256",
                "name_sha256",
                "instruction_sha256",
                "constraints_sha256",
                "executor_id",
                "side_effect_class",
                "attributes_sha256",
            },
        }.get(command_type)
        if type(payload) is not dict or expected_fields is None:
            raise cls._corrupt("formal Task decision payload authority is invalid")
        unsigned = {key: value for key, value in payload.items() if key != "payload_sha256"}
        if set(payload) != expected_fields or payload["payload_sha256"] != cls._json_value_sha256(unsigned):
            raise cls._corrupt("formal Task decision payload authority is invalid")

        def text(field: str) -> bool:
            value = payload[field]
            return type(value) is str and bool(value.strip()) and "\x00" not in value

        def uint(field: str, *, positive: bool = False) -> bool:
            value = payload[field]
            return type(value) is int and (value > 0 if positive else value >= 0) and value <= MAX_SAFE_INTEGER

        def digest(field: str, *, optional: bool = False) -> bool:
            value = payload[field]
            return (optional and value is None) or cls._is_lower_sha256(value)

        valid = True
        if command_type == "task.update":
            valid = (
                text("attempt_id")
                and uint("expected_event_head")
                and digest("instruction_sha256", optional=True)
                and digest("constraints_sha256", optional=True)
                and not (payload["instruction_sha256"] is None and payload["constraints_sha256"] is None)
            )
        elif command_type == "task.provide_input":
            valid = (
                text("attempt_id")
                and uint("expected_event_head")
                and text("responds_to_event_id")
                and digest("text_sha256")
            )
        elif command_type in {"task.pause", "task.resume"}:
            valid = text("attempt_id") and uint("expected_event_head") and digest("reason_sha256", optional=True)
        elif command_type == "task.reprioritize":
            valid = (
                text("attempt_id")
                and uint("expected_event_head")
                and payload["priority"] in {"low", "normal", "high", "urgent"}
                and digest("reason_sha256", optional=True)
            )
        elif command_type == "task.adjust":
            valid = digest("adjustment_sha256")
        elif command_type == "task.cancel":
            valid = True
        elif command_type == "task.retry":
            valid = (
                text("previous_attempt_id")
                and payload["previous_outcome"] in {TerminalOutcome.CANCELLED.value, TerminalOutcome.COMPLETED.value}
                and type(payload["attempt_number"]) is int
                and payload["attempt_number"] in {2, 3}
                and digest("product_request_sha256")
            )
        elif command_type == "task.ack_events":
            valid = (
                payload["presentation_class"] in {"text", "voice"}
                and uint("acked_through_seq")
                and text("acked_event_id")
                and uint("expected_event_head")
            )
        elif command_type == "task.create_successor":
            try:
                predecessor_outcome = TerminalOutcome(payload["predecessor_outcome"])
            except (TypeError, ValueError):
                valid = False
            else:
                requested_digest = payload["predecessor_result_sha256"]
                valid = (
                    uint("expected_predecessor_revision_number", positive=True)
                    and uint("expected_predecessor_event_head")
                    and text("predecessor_terminal_event_id")
                    and (
                        cls._is_lower_sha256(requested_digest)
                        if predecessor_outcome is TerminalOutcome.COMPLETED
                        else requested_digest is None
                    )
                    and digest("name_sha256")
                    and digest("instruction_sha256")
                    and digest("constraints_sha256")
                    and text("executor_id")
                    and payload["side_effect_class"] in {"read_only", "project_mutation"}
                    and digest("attributes_sha256")
                )
        if not valid:
            raise cls._corrupt("formal Task decision payload authority is invalid")

    @classmethod
    def _verify_decision_history(
        cls,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        binding: dict[str, object],
        task_row: sqlite3.Row,
    ) -> tuple[
        dict[str, object],
        dict[str, object],
        dict[str, object],
        dict[str, object],
        tuple[str, ...],
    ]:
        """Bind a decision snapshot to its immutable event-head prefix."""

        authority = binding["authority"]
        task = authority["task"]
        attempt = authority["attempt"]
        head = authority["head_event"]
        dispatches = authority["dispatch"]
        successor_task_ids = authority["successor_task_ids"]
        result_sha256s = authority["result_sha256s"]
        task_fields = {
            "task_id",
            "attempt_id",
            "event_head",
            "state",
            "outcome",
            "revision_number",
            "correlation_id",
            "cancel_requested",
            "dispatch_fenced",
        }
        attempt_fields = {"attempt_id", "attempt_number", "state", "outcome"}
        head_fields = {
            "event_id",
            "attempt_id",
            "seq",
            "event_type",
            "state",
            "outcome",
            "occurred_at",
        }
        dispatch_fields = {"outbox_id", "state", "delivery_count"}
        if (
            type(task) is not dict
            or set(task) != task_fields
            or type(attempt) is not dict
            or set(attempt) != attempt_fields
            or type(head) is not dict
            or set(head) != head_fields
            or type(dispatches) is not list
            or len(dispatches) != 1
            or type(dispatches[0]) is not dict
            or set(dispatches[0]) != dispatch_fields
            or type(successor_task_ids) is not list
            or any(type(item) is not str or not item for item in successor_task_ids)
            or successor_task_ids != sorted(set(successor_task_ids))
            or type(result_sha256s) is not list
            or any(not cls._is_lower_sha256(item) for item in result_sha256s)
            or task["task_id"] != binding["target_task_id"]
            or task["task_id"] != task_row["task_id"]
            or task["revision_number"] != task_row["revision_number"]
            or task["correlation_id"] != task_row["correlation_id"]
            or type(task["event_head"]) is not int
            or task["event_head"] < 0
            or type(task["cancel_requested"]) is not int
            or task["cancel_requested"] not in {0, 1}
            or type(task["dispatch_fenced"]) is not int
            or task["dispatch_fenced"] not in {0, 1}
        ):
            raise cls._corrupt("formal Task decision history is not canonical")
        head_row = connection.execute(
            "SELECT * FROM task_events WHERE task_id=? AND seq=?",
            (task["task_id"], task["event_head"]),
        ).fetchone()
        if head_row is None:
            raise cls._corrupt("formal Task decision lost its event-head evidence")
        expected_head = {
            "event_id": head_row["event_id"],
            "attempt_id": head_row["attempt_id"],
            "seq": head_row["seq"],
            "event_type": head_row["event_type"],
            "state": head_row["state"],
            "outcome": head_row["outcome"],
            "occurred_at": head_row["occurred_at"],
        }
        if (
            head != expected_head
            or task["attempt_id"] != head["attempt_id"]
            or task["state"] != head["state"]
            or task["outcome"] != head["outcome"]
        ):
            raise cls._corrupt("formal Task decision event-head binding changed")
        attempt_row = connection.execute(
            "SELECT * FROM attempts WHERE attempt_id=? AND task_id=?",
            (task["attempt_id"], task["task_id"]),
        ).fetchone()
        if attempt_row is None:
            raise cls._corrupt("formal Task decision lost its attempt evidence")
        attempt_state = FormalAttemptState.ACCEPTED.value
        attempt_outcome: str | None = None
        event_rows = connection.execute(
            """SELECT event_type, state, outcome FROM task_events
               WHERE task_id=? AND attempt_id=? AND seq<=? ORDER BY seq""",
            (task["task_id"], task["attempt_id"], task["event_head"]),
        ).fetchall()
        for event_row in event_rows:
            if event_row["event_type"] in {
                "attempt.accepted",
                "attempt.running",
                "attempt.terminal",
            }:
                attempt_state = event_row["state"]
                attempt_outcome = event_row["outcome"]
        expected_attempt = {
            "attempt_id": attempt_row["attempt_id"],
            "attempt_number": attempt_row["attempt_number"],
            "state": attempt_state,
            "outcome": attempt_outcome,
        }
        cancel_before_head = connection.execute(
            """SELECT 1 FROM task_events WHERE task_id=? AND attempt_id=?
               AND seq<=? AND event_type='task.cancel_requested' LIMIT 1""",
            (task["task_id"], task["attempt_id"], task["event_head"]),
        ).fetchone()
        if (
            attempt != expected_attempt
            or task["cancel_requested"] != int(cancel_before_head is not None)
            or task["dispatch_fenced"] != int(cancel_before_head is not None)
        ):
            raise cls._corrupt("formal Task decision attempt history changed")

        dispatch = dispatches[0]
        dispatch_row = connection.execute(
            """SELECT outbox_id, state, delivery_count FROM outbox
               WHERE task_id=? AND attempt_id=? AND kind=?""",
            (
                task["task_id"],
                task["attempt_id"],
                OutboxKind.ATTEMPT_DISPATCH.value,
            ),
        ).fetchone()
        try:
            bound_state = OutboxState(dispatch["state"])
            current_state = OutboxState(dispatch_row["state"])
            bound_delivery = dispatch["delivery_count"]
            current_delivery = dispatch_row["delivery_count"]
        except (TypeError, ValueError) as error:
            raise cls._corrupt("formal Task decision dispatch evidence is invalid") from error
        allowed_current = {
            OutboxState.PENDING: set(OutboxState),
            OutboxState.CLAIMED: set(OutboxState),
            OutboxState.DELIVERED: {OutboxState.DELIVERED},
            OutboxState.SUPPRESSED: {OutboxState.SUPPRESSED},
        }[bound_state]
        if (
            dispatch_row is None
            or type(bound_delivery) is not int
            or type(current_delivery) is not int
            or bound_delivery < 0
            or current_delivery < bound_delivery
            or current_state not in allowed_current
            or dispatch["outbox_id"] != dispatch_row["outbox_id"]
            or (
                bound_state is OutboxState.PENDING
                and bound_delivery != 0
                and "reprioritize" not in binding["authority"]
            )
            or (bound_state in {OutboxState.CLAIMED, OutboxState.DELIVERED} and bound_delivery < 1)
        ):
            raise cls._corrupt("formal Task decision dispatch history changed")

        decision_rowid = connection.execute(
            "SELECT rowid FROM commands WHERE scope_key=? AND command_id=?",
            (row["scope_key"], row["command_id"]),
        ).fetchone()
        successor_rows = connection.execute(
            """SELECT s.task_id, c.rowid AS command_rowid
               FROM tasks AS s
               JOIN task_events AS e ON e.task_id=s.task_id AND e.seq=0
               JOIN commands AS c
                 ON c.scope_key=s.scope_key AND c.command_id=e.causation_id
               WHERE s.predecessor_task_id=? ORDER BY s.task_id""",
            (task["task_id"],),
        ).fetchall()
        if decision_rowid is None:
            raise cls._corrupt("formal Task decision lost its command identity")
        historical_successors = tuple(
            item["task_id"] for item in successor_rows if item["command_rowid"] < decision_rowid["rowid"]
        )
        if tuple(successor_task_ids) != historical_successors:
            raise cls._corrupt("formal Task decision successor history changed")
        return task, attempt, head, dispatch, historical_successors

    @classmethod
    def _verify_business_decision_reason(
        cls,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        binding: dict[str, object],
        reason: str,
        payload: dict[str, object],
        task: dict[str, object],
        attempt: dict[str, object],
        head: dict[str, object],
        dispatch: dict[str, object],
        successor_ids: tuple[str, ...],
    ) -> None:
        """Re-evaluate the persisted reason from its bound historical prefix."""

        command_type = row["command_type"]
        valid_reason = False
        if command_type == "task.update":
            valid_reason = reason == "TASK_UPDATE_PRECONDITION_STALE" and (
                task["state"] != FormalTaskState.ACCEPTED.value
                or bool(task["cancel_requested"])
                or bool(task["dispatch_fenced"])
                or attempt["state"] != FormalAttemptState.ACCEPTED.value
                or payload["attempt_id"] != task["attempt_id"]
                or payload["expected_event_head"] != task["event_head"]
                or dispatch["state"] != OutboxState.PENDING.value
                or dispatch["delivery_count"] != 0
            )
        elif command_type in {
            "task.provide_input",
            "task.pause",
            "task.resume",
        }:
            stale = (
                payload["attempt_id"] != task["attempt_id"]
                or payload["expected_event_head"] != task["event_head"]
                or head["attempt_id"] != task["attempt_id"]
            )
            state_conflict = task["state"] == FormalTaskState.TERMINAL.value
            if command_type == "task.provide_input" and not stale:
                state_conflict = state_conflict or (
                    task["state"] != FormalTaskState.DECISION_REQUIRED.value
                    or head["event_type"] != "task.decision_required"
                    or payload["responds_to_event_id"] != head["event_id"]
                )
            expected_reason = (
                "TASK_CONTROL_PRECONDITION_STALE"
                if stale
                else ("TASK_CONTROL_STATE_CONFLICT" if state_conflict else "TASK_CONTROL_UNSUPPORTED")
            )
            valid_reason = reason == expected_reason
        elif command_type == "task.reprioritize":
            stale = (
                payload["attempt_id"] != task["attempt_id"]
                or payload["expected_event_head"] != task["event_head"]
                or head["attempt_id"] != task["attempt_id"]
            )
            reprioritize_snapshot = binding["authority"].get("reprioritize")
            if reprioritize_snapshot is None:
                # Preserve accepted Task 2 negative rows created before queue support.
                state_conflict = task["state"] == FormalTaskState.TERMINAL.value
                expected_reason = (
                    "TASK_CONTROL_PRECONDITION_STALE"
                    if stale
                    else ("TASK_CONTROL_STATE_CONFLICT" if state_conflict else "TASK_CONTROL_UNSUPPORTED")
                )
            else:
                eligible = (
                    task["state"] == FormalTaskState.ACCEPTED.value
                    and not bool(task["cancel_requested"])
                    and not bool(task["dispatch_fenced"])
                    and attempt["state"] == FormalAttemptState.ACCEPTED.value
                    and all(reprioritize_snapshot.values())
                )
                expected_reason = (
                    "TASK_CONTROL_PRECONDITION_STALE"
                    if stale
                    else (None if eligible else "TASK_CONTROL_STATE_CONFLICT")
                )
            valid_reason = reason == expected_reason
        elif command_type == "task.adjust":
            if reason == "TASK_MUTATION_PRECONDITION_STALE":
                precondition = TaskMutationPrecondition.from_dict(binding["authority"].get("mutation_precondition"))
                valid_reason = precondition.task_id == task["task_id"] and (
                    precondition.attempt_id != task["attempt_id"]
                    or precondition.expected_event_head != task["event_head"]
                )
            else:
                valid_reason = reason == "TASK_ADJUSTMENT_STATE_CONFLICT" and (
                    task["state"] != FormalTaskState.RUNNING.value
                    or attempt["state"] != FormalAttemptState.RUNNING.value
                )
        elif command_type == "task.cancel":
            if reason == "TASK_MUTATION_PRECONDITION_STALE":
                precondition = TaskMutationPrecondition.from_dict(binding["authority"].get("mutation_precondition"))
                valid_reason = precondition.task_id == task["task_id"] and (
                    precondition.attempt_id != task["attempt_id"]
                    or precondition.expected_event_head != task["event_head"]
                )
            elif reason == "TASK_CANCEL_ALREADY_REQUESTED":
                precondition = TaskMutationPrecondition.from_dict(binding["authority"].get("mutation_precondition"))
                valid_reason = (
                    bool(task["cancel_requested"])
                    and precondition.task_id == task["task_id"]
                    and precondition.attempt_id == task["attempt_id"]
                    and precondition.expected_event_head == task["event_head"]
                    and head["attempt_id"] == task["attempt_id"]
                    and head["seq"] == task["event_head"]
                )
            else:
                valid_reason = (
                    reason == "TASK_ALREADY_TERMINAL"
                    and task["state"] == FormalTaskState.TERMINAL.value
                    and head["event_type"] == "task.terminal"
                )
        elif command_type == "task.retry":
            if binding["correlation_id"] != task["correlation_id"]:
                expected_reason = None
            elif (
                task["state"] == FormalTaskState.TERMINAL.value
                and attempt["state"] == FormalAttemptState.TERMINAL.value
                and attempt["attempt_number"] >= 3
            ):
                expected_reason = "TASK_RETRY_LIMIT_EXCEEDED"
            elif (
                payload["previous_attempt_id"] != attempt["attempt_id"]
                or payload["attempt_number"] != attempt["attempt_number"] + 1
            ):
                expected_reason = "TASK_RETRY_PRECONDITION_STALE"
            elif task["state"] != FormalTaskState.TERMINAL.value:
                expected_reason = "TASK_RETRY_REQUIRES_TERMINAL"
            elif attempt["state"] != FormalAttemptState.TERMINAL.value or task["outcome"] != attempt["outcome"]:
                expected_reason = None
            elif task["outcome"] != TerminalOutcome.CANCELLED.value:
                expected_reason = "TASK_RETRY_OUTCOME_NOT_ELIGIBLE"
            elif payload["previous_outcome"] != task["outcome"]:
                expected_reason = "TASK_RETRY_PRECONDITION_STALE"
            else:
                expected_reason = None
            valid_reason = reason == expected_reason
        elif command_type == "task.create_successor":
            eligible = {
                TerminalOutcome.COMPLETED.value,
                TerminalOutcome.FAILED.value,
                TerminalOutcome.CANCELLED.value,
                TerminalOutcome.INTERRUPTED.value,
            }
            precondition_conflict = (
                task["state"] != FormalTaskState.TERMINAL.value
                or task["outcome"] not in eligible
                or attempt["state"] != FormalAttemptState.TERMINAL.value
                or attempt["outcome"] != task["outcome"]
                or head["event_type"] != "task.terminal"
                or payload["predecessor_terminal_event_id"] != head["event_id"]
                or payload["expected_predecessor_revision_number"] != task["revision_number"]
                or payload["expected_predecessor_event_head"] != task["event_head"]
                or payload["predecessor_outcome"] != task["outcome"]
                or bool(successor_ids)
            )
            result_rows = connection.execute(
                """SELECT * FROM task_results WHERE task_id=? AND attempt_id=?
                   ORDER BY source_event_id""",
                (task["task_id"], task["attempt_id"]),
            ).fetchall()
            result_digests = tuple(
                cls._json_value_sha256(cls._task_result_from_row(item).to_dict()) for item in result_rows
            )
            if tuple(binding["authority"]["result_sha256s"]) != result_digests:
                raise cls._corrupt("successor decision result history changed")
            result_conflict = not precondition_conflict and (
                (
                    task["outcome"] == TerminalOutcome.COMPLETED.value
                    and (len(result_digests) != 1 or payload["predecessor_result_sha256"] != result_digests[0])
                )
                or (
                    task["outcome"] != TerminalOutcome.COMPLETED.value
                    and (payload["predecessor_result_sha256"] is not None or bool(result_digests))
                )
            )
            expected_reason = (
                "TASK_SUCCESSOR_PRECONDITION_CONFLICT"
                if precondition_conflict
                else ("TASK_SUCCESSOR_RESULT_CONFLICT" if result_conflict else None)
            )
            valid_reason = reason == expected_reason
        if not valid_reason:
            raise cls._corrupt("formal Task business decision lacks closed historical evidence")

    @classmethod
    def _verify_update_authority(
        cls,
        connection: sqlite3.Connection,
        *,
        task: PersistentTaskRecord,
        attempt: PersistentAttemptRecord,
        base_spec: FormalTaskSpec,
        dispatch_row: sqlite3.Row,
        requests: Mapping[str, PersistentTaskEvent],
        settlements: Mapping[str, PersistentTaskEvent],
    ) -> FormalTaskSpec:
        """Reconstruct the exact pre-dispatch specification update chain."""

        if set(requests) != set(settlements):
            raise cls._corrupt("formal Task update authority is not fully settled")
        current_spec = base_spec
        for command_id, request in sorted(requests.items(), key=lambda item: item[1].seq):
            settlement = settlements[command_id]
            command_rows = connection.execute(
                "SELECT * FROM commands WHERE scope_key=? AND command_id=?",
                (_scope_key(task.scope), command_id),
            ).fetchall()
            if len(command_rows) != 1:
                raise cls._corrupt("formal Task update event lacks one exact command ledger")
            command_row = command_rows[0]
            command, result = cls._control_command_from_row(command_row)
            payload = command.payload
            if (
                command.command_type != "task.update"
                or command.target_ref.id != task.task_id
                or command.scope != task.scope
                or command.required_capabilities != ("task.update",)
                or type(payload) is not dict
                or set(payload)
                != {
                    "attempt_id",
                    "expected_event_head",
                    "instruction",
                    "constraints",
                }
                or payload["attempt_id"] != attempt.attempt_id
                or payload["expected_event_head"] != request.seq - 1
                or settlement.seq != request.seq + 1
                or request.state != FormalTaskState.ACCEPTED.value
                or settlement.state != FormalTaskState.ACCEPTED.value
                or request.outcome is not None
                or settlement.outcome is not None
                or request.occurred_at != settlement.occurred_at
                or command_row["created_at"] != request.occurred_at
                or result.observed_at != settlement.occurred_at
            ):
                raise cls._corrupt("formal Task update command does not bind its event pair")
            instruction = payload["instruction"]
            constraints = payload["constraints"]
            updated_spec = replace(
                current_spec,
                instruction=(current_spec.instruction if instruction is None else instruction),
                constraints=(current_spec.constraints if constraints is None else tuple(constraints)),
            )
            value = result.result
            if (
                type(value) is not dict
                or value
                != {
                    "task_id": task.task_id,
                    "attempt_id": attempt.attempt_id,
                    "state": FormalTaskState.ACCEPTED.value,
                    "applied": True,
                    "outbox_id": dispatch_row["outbox_id"],
                }
                or dict(result.extensions)
                != command_result_extensions(
                    TaskCommandDisposition.APPLIED,
                    admission_event_id=request.event_id,
                    settlement_event_id=settlement.event_id,
                )
                or connection.execute(
                    """SELECT COUNT(*) FROM outbox AS o
                       JOIN tasks AS t ON t.task_id=o.task_id
                       WHERE t.scope_key=? AND o.task_id=? AND o.command_id=?""",
                    (_scope_key(task.scope), task.task_id, command_id),
                ).fetchone()[0]
                != 0
            ):
                raise cls._corrupt("formal Task update result is not canonical")
            current_spec = updated_spec
        return current_spec

    @classmethod
    def _verify_reprioritize_authority(
        cls,
        connection: sqlite3.Connection,
        *,
        task: PersistentTaskRecord,
        attempt: PersistentAttemptRecord,
        requests: Mapping[str, PersistentTaskEvent],
        settlements: Mapping[str, PersistentTaskEvent],
    ) -> None:
        """Verify each queued priority change against its command and event pair."""

        if set(requests) != set(settlements):
            raise cls._corrupt("formal Task reprioritize authority is not fully settled")
        for command_id, request in sorted(requests.items(), key=lambda item: item[1].seq):
            settlement = settlements[command_id]
            command_rows = connection.execute(
                "SELECT * FROM commands WHERE scope_key=? AND command_id=?",
                (_scope_key(task.scope), command_id),
            ).fetchall()
            if len(command_rows) != 1:
                raise cls._corrupt("formal Task reprioritize event lacks one exact command ledger")
            command_row = command_rows[0]
            command, result = cls._control_command_from_row(command_row)
            payload = command.payload
            try:
                priority = AdmissionPriority(payload.get("priority"))
            except (TypeError, ValueError) as error:
                raise cls._corrupt("formal Task reprioritize priority is not canonical") from error
            details = {"command_id": command_id, "priority": priority.value}
            value = result.result
            if (
                command.command_type != "task.reprioritize"
                or command.target_ref.id != task.task_id
                or command.scope != task.scope
                or command.required_capabilities != ("task.reprioritize",)
                or type(payload) is not dict
                or set(payload) != {"attempt_id", "expected_event_head", "priority", "reason"}
                or payload["attempt_id"] != attempt.attempt_id
                or payload["expected_event_head"] != request.seq - 1
                or settlement.seq != request.seq + 1
                or request.details != details
                or settlement.details != details
                or request.state != FormalTaskState.ACCEPTED.value
                or settlement.state != FormalTaskState.ACCEPTED.value
                or request.outcome is not None
                or settlement.outcome is not None
                or request.occurred_at != settlement.occurred_at
                or command_row["created_at"] != request.occurred_at
                or result.observed_at != settlement.occurred_at
                or type(value) is not dict
                or value
                != {
                    "task_id": task.task_id,
                    "attempt_id": attempt.attempt_id,
                    "state": FormalTaskState.ACCEPTED.value,
                    "priority": priority.value,
                    "applied": True,
                }
                or dict(result.extensions)
                != command_result_extensions(
                    TaskCommandDisposition.APPLIED,
                    admission_event_id=request.event_id,
                    settlement_event_id=settlement.event_id,
                )
                or connection.execute(
                    """SELECT COUNT(*) FROM outbox AS o
                       JOIN tasks AS t ON t.task_id=o.task_id
                       WHERE t.scope_key=? AND o.command_id=?""",
                    (_scope_key(task.scope), command_id),
                ).fetchone()[0]
                != 0
            ):
                raise cls._corrupt("formal Task reprioritize command does not bind its queue settlement")

    @classmethod
    def _verify_successor_admission(
        cls,
        connection: sqlite3.Connection,
        *,
        task: PersistentTaskRecord,
        attempt: PersistentAttemptRecord,
        boundary: PersistentTaskEvent,
        command: CommandEnvelope,
        result: ResultEnvelope,
        resolved_spec: FormalTaskSpec,
        dispatch_row: sqlite3.Row,
    ) -> None:
        """Prove a successor's immutable predecessor and exact result binding."""

        predecessor_id = task.predecessor_task_id
        if predecessor_id is None:
            raise cls._corrupt("successor Task lacks its predecessor identity")
        predecessor_row = connection.execute("SELECT * FROM tasks WHERE task_id=?", (predecessor_id,)).fetchone()
        if predecessor_row is None:
            raise cls._corrupt("successor Task lost its predecessor")
        cls._verify_durable_lineage(connection, predecessor_row)
        predecessor = cls._task_from_row(predecessor_row)
        predecessor_attempt = connection.execute(
            "SELECT * FROM attempts WHERE attempt_id=?",
            (predecessor.attempt_id,),
        ).fetchone()
        terminal_event = connection.execute(
            "SELECT * FROM task_events WHERE task_id=? AND seq=?",
            (predecessor_id, predecessor.event_head),
        ).fetchone()
        payload = command.payload
        value = result.result
        eligible = {
            TerminalOutcome.COMPLETED,
            TerminalOutcome.FAILED,
            TerminalOutcome.CANCELLED,
            TerminalOutcome.INTERRUPTED,
        }
        if (
            predecessor.state is not FormalTaskState.TERMINAL
            or predecessor.outcome not in eligible
            or predecessor_attempt is None
            or predecessor_attempt["state"] != FormalAttemptState.TERMINAL.value
            or terminal_event is None
            or terminal_event["event_type"] != "task.terminal"
            or command.command_type != "task.create_successor"
            or command.target_ref.id != predecessor_id
            or payload.get("expected_predecessor_revision_number") != predecessor.revision_number
            or payload.get("expected_predecessor_event_head") != predecessor.event_head
            or payload.get("predecessor_terminal_event_id") != terminal_event["event_id"]
            or payload.get("predecessor_outcome") != predecessor.outcome.value
            or task.revision_number != predecessor.revision_number + 1
            or connection.execute(
                "SELECT COUNT(*) FROM tasks WHERE predecessor_task_id=?",
                (predecessor_id,),
            ).fetchone()[0]
            != 1
            or type(value) is not dict
            or value
            != {
                "task_id": task.task_id,
                "predecessor_task_id": predecessor_id,
                "revision_number": task.revision_number,
                "attempt_id": attempt.attempt_id,
                "state": FormalTaskState.ACCEPTED.value,
                "outbox_id": dispatch_row["outbox_id"],
            }
            or dict(result.extensions)
            != {
                **command_result_extensions(
                    TaskCommandDisposition.ACCEPTED,
                    admission_event_id=boundary.event_id,
                ),
                "live_voice.store": {"durability": "sqlite_outbox"},
            }
        ):
            raise cls._corrupt("successor admission does not bind predecessor truth")
        prior_context = predecessor.spec.context
        if (
            resolved_spec.context.source,
            resolved_spec.context.stable_id,
            resolved_spec.context.uri,
            resolved_spec.context.scope,
        ) != (
            prior_context.source,
            prior_context.stable_id,
            prior_context.uri,
            prior_context.scope,
        ):
            raise cls._corrupt("successor changed predecessor project identity")
        expected_spec = {
            "name": resolved_spec.name,
            "instruction": resolved_spec.instruction,
            "constraints": list(resolved_spec.constraints),
            "executor_id": resolved_spec.executor_id,
            "side_effect_class": resolved_spec.side_effect_class,
            "attributes": dict(resolved_spec.attributes),
        }
        if any(payload.get(key) != item for key, item in expected_spec.items()):
            raise cls._corrupt("successor command changed its resolved specification")
        result_rows = connection.execute(
            "SELECT * FROM task_results WHERE task_id=? AND attempt_id=?",
            (predecessor_id, predecessor.attempt_id),
        ).fetchall()
        digest = payload.get("predecessor_result_sha256")
        if predecessor.outcome is TerminalOutcome.COMPLETED:
            if len(result_rows) != 1:
                raise cls._corrupt("completed successor predecessor lost its result")
            expected_digest = hashlib.sha256(
                canonical_json_bytes(cls._task_result_from_row(result_rows[0]).to_dict())
            ).hexdigest()
            if digest != expected_digest:
                raise cls._corrupt("successor predecessor result digest changed")
        elif digest is not None or result_rows:
            raise cls._corrupt("non-completed successor predecessor owns a result")

    @classmethod
    def _executor_observation_from_row(cls, row: sqlite3.Row) -> ExecutorObservation:
        """Decode the exact canonical Executor fact stored for one source event."""

        def load() -> ExecutorObservation:
            payload = _json_load(row["canonical"])
            expected_keys = {
                "resolution",
                "executor_id",
                "executor_ref",
                "task_id",
                "attempt_id",
                "source_event_id",
                "source_seq",
                "attempt_state",
                "attempt_outcome",
                "occurred_at",
                "raw_status",
                "summary",
                "error",
                "result_text",
                "result_artifacts",
            }
            selected_keys = expected_keys | {
                "adapter_id",
                "capability_profile_digest",
            }
            if (
                type(payload) is not dict
                or frozenset(payload) not in {frozenset(expected_keys), frozenset(selected_keys)}
                or type(payload["result_artifacts"]) is not list
            ):
                raise cls._corrupt("formal Task Executor authority is not canonical")
            artifacts = tuple(
                TaskResultArtifact(
                    relative_path=item["relative_path"],
                    sha256=item["sha256"],
                )
                for item in payload["result_artifacts"]
                if type(item) is dict and set(item) == {"relative_path", "sha256"}
            )
            if len(artifacts) != len(payload["result_artifacts"]):
                raise cls._corrupt("formal Task Executor result artifacts are not canonical")
            observation = ExecutorObservation(
                resolution=ExecutorResolution(payload["resolution"]),
                executor_id=payload["executor_id"],
                executor_ref=payload["executor_ref"],
                task_id=payload["task_id"],
                attempt_id=payload["attempt_id"],
                source_event_id=payload["source_event_id"],
                source_seq=payload["source_seq"],
                attempt_state=(
                    None if payload["attempt_state"] is None else FormalAttemptState(payload["attempt_state"])
                ),
                attempt_outcome=(
                    None if payload["attempt_outcome"] is None else TerminalOutcome(payload["attempt_outcome"])
                ),
                occurred_at=payload["occurred_at"],
                raw_status=payload["raw_status"],
                summary=payload["summary"],
                error=payload["error"],
                result_text=payload["result_text"],
                result_artifacts=artifacts,
                adapter_id=payload.get("adapter_id"),
                capability_profile_digest=payload.get("capability_profile_digest"),
            )
            if (
                observation.resolution is not ExecutorResolution.KNOWN
                or observation.source_event_id != row["source_event_id"]
                or observation.attempt_id != row["attempt_id"]
                or observation.source_seq != int(row["source_seq"])
                or canonical_json_bytes(observation.canonical_dict()) != row["canonical"]
            ):
                raise cls._corrupt("formal Task Executor authority binding is inconsistent")
            return observation

        return _stored_record("Executor authority", load)

    @classmethod
    def _task_result_from_row(cls, row: sqlite3.Row) -> TaskResultRecord:
        """Decode a persisted Task result without consulting mutable files."""

        def load() -> TaskResultRecord:
            payload = _json_load(row["artifacts_json"])
            if type(payload) is not list:
                raise cls._corrupt("formal Task result artifacts are invalid")
            artifacts = tuple(
                TaskResultArtifact(
                    relative_path=item["relative_path"],
                    sha256=item["sha256"],
                )
                for item in payload
                if type(item) is dict and set(item) == {"relative_path", "sha256"}
            )
            if len(artifacts) != len(payload):
                raise cls._corrupt("formal Task result artifacts are invalid")
            return TaskResultRecord(
                task_id=row["task_id"],
                attempt_id=row["attempt_id"],
                source_event_id=row["source_event_id"],
                result_text=row["result_text"],
                artifacts=artifacts,
                completed_at=row["completed_at"],
            )

        return _stored_record("Task result", load)

    @classmethod
    def _verify_control_authority(
        cls,
        connection: sqlite3.Connection,
        *,
        task: PersistentTaskRecord,
        attempt: PersistentAttemptRecord,
        request: PersistentTaskEvent,
        disposition: PersistentTaskEvent | None = None,
        attempt_terminal: PersistentTaskEvent | None = None,
        task_terminal: PersistentTaskEvent | None = None,
    ) -> str | None:
        """Bind a control request, optional settlement, command and outbox."""

        command_id = request.causation_id
        command_rows = connection.execute(
            "SELECT * FROM commands WHERE scope_key=? AND command_id=?",
            (_scope_key(task.scope), command_id),
        ).fetchall()
        if len(command_rows) != 1:
            raise cls._corrupt("formal Task control event lacks one exact command ledger")
        command_row = command_rows[0]
        command, result = cls._control_command_from_row(command_row)
        expected_type = "task.cancel" if request.event_type == "task.cancel_requested" else "task.adjust"
        if (
            command.command_type != expected_type
            or command.target_ref.id != task.task_id
            or command.scope != task.scope
            or command.required_capabilities != (expected_type,)
            or command_row["created_at"] != request.occurred_at
        ):
            raise cls._corrupt("formal Task control command does not bind its request event")
        value = result.result
        if expected_type == "task.cancel" and type(value) is not dict:
            raise cls._corrupt("formal Task control result is not canonical")

        outbox_rows = connection.execute(
            """SELECT * FROM outbox
               WHERE task_id=? AND attempt_id=? AND command_id=?
                 AND kind IN (?, ?)""",
            (
                task.task_id,
                attempt.attempt_id,
                command_id,
                OutboxKind.ATTEMPT_CANCEL.value,
                OutboxKind.ATTEMPT_ADJUST.value,
            ),
        ).fetchall()
        if expected_type == "task.cancel":
            terminal_cancelled = task_terminal is not None and task_terminal.outcome == TerminalOutcome.CANCELLED.value
            terminal_pair_invalid = (attempt_terminal is None) != (task_terminal is None)
            if attempt_terminal is not None and task_terminal is not None:
                terminal_pair_invalid = terminal_pair_invalid or (
                    request.seq >= attempt_terminal.seq
                    or attempt_terminal.seq >= task_terminal.seq
                    or attempt_terminal.task_id != task.task_id
                    or task_terminal.task_id != task.task_id
                    or attempt_terminal.attempt_id != attempt.attempt_id
                    or task_terminal.attempt_id != attempt.attempt_id
                    or attempt_terminal.scope != task.scope
                    or task_terminal.scope != task.scope
                    or attempt_terminal.correlation_id != task.correlation_id
                    or task_terminal.correlation_id != task.correlation_id
                    or attempt_terminal.event_type != "attempt.terminal"
                    or task_terminal.event_type != "task.terminal"
                    or attempt_terminal.state != FormalAttemptState.TERMINAL.value
                    or task_terminal.state != FormalTaskState.TERMINAL.value
                    or attempt_terminal.outcome != task_terminal.outcome
                    or (
                        terminal_cancelled
                        and attempt_terminal.source_event_id is None
                        and (attempt_terminal.causation_id != command_id or task_terminal.causation_id != command_id)
                    )
                    or (
                        terminal_cancelled
                        and attempt_terminal.source_event_id is not None
                        and (
                            attempt_terminal.causation_id != attempt_terminal.source_event_id
                            or task_terminal.source_event_id != attempt_terminal.source_event_id
                            or task_terminal.causation_id != attempt_terminal.source_event_id
                        )
                    )
                )
            if terminal_pair_invalid:
                raise cls._corrupt("formal Task cancel settlement segment is not canonical")
            settled = terminal_cancelled
            if (
                disposition is not None
                or command.payload
                or set(value)
                != {
                    "task_id",
                    "attempt_id",
                    "cancel_acknowledged",
                    "applied",
                    "state",
                    "outbox_id",
                }
            ):
                raise cls._corrupt("formal Task cancel authority is not canonical")
            outbox_id = value["outbox_id"]
            common_invalid = (
                value["task_id"] != task.task_id
                or value["attempt_id"] != attempt.attempt_id
                or value["cancel_acknowledged"] is not True
                or (outbox_id is None and (outbox_rows or not settled))
                or (
                    outbox_id is not None
                    and (
                        type(outbox_id) is not str
                        or len(outbox_rows) != 1
                        or outbox_rows[0]["outbox_id"] != outbox_id
                        or outbox_rows[0]["kind"] != OutboxKind.ATTEMPT_CANCEL.value
                    )
                )
            )
            current_result_valid = (
                value["applied"] is settled
                and value["state"] == (FormalTaskState.TERMINAL.value if settled else request.state)
                and result.observed_at == (task_terminal.occurred_at if settled else request.occurred_at)
                and dict(result.extensions)
                == command_result_extensions(
                    (TaskCommandDisposition.APPLIED if settled else TaskCommandDisposition.ACCEPTED),
                    admission_event_id=request.event_id,
                    settlement_event_id=(task_terminal.event_id if settled else None),
                )
            )
            legacy_result_valid = (
                value["applied"] is True
                and value["state"] == (FormalTaskState.TERMINAL.value if outbox_id is None else request.state)
                and result.observed_at == request.occurred_at
                and dict(result.extensions) == {}
            )
            if common_invalid or not (current_result_valid or legacy_result_valid):
                raise cls._corrupt("formal Task cancel result does not bind durable control truth")
        else:
            if len(outbox_rows) != 1:
                raise cls._corrupt("formal Task adjustment lacks one exact durable outbox")
            outbox_id = outbox_rows[0]["outbox_id"]
            expected_state = (
                TaskAdjustmentState.PENDING
                if disposition is None
                else (
                    TaskAdjustmentState.APPLIED
                    if disposition.event_type == "task.adjust_applied"
                    else TaskAdjustmentState.REJECTED
                )
            )
            expected_reason = (
                None if expected_state is not TaskAdjustmentState.REJECTED else disposition.details.get("reason")
            )
            expected_disposition = (
                TaskCommandDisposition.ACCEPTED
                if expected_state is TaskAdjustmentState.PENDING
                else (
                    TaskCommandDisposition.APPLIED
                    if expected_state is TaskAdjustmentState.APPLIED
                    else (
                        TaskCommandDisposition.CONFLICT
                        if expected_reason == "TASK_TERMINAL_BEFORE_ADJUSTMENT"
                        else TaskCommandDisposition.REJECTED
                    )
                )
            )
            expected_error_code = (
                ErrorCode.CONFLICT
                if expected_disposition is TaskCommandDisposition.CONFLICT
                else ErrorCode.INVALID_ARGUMENT
            )
            positive_value_invalid = (
                type(value) is not dict
                or set(value)
                != {
                    "task_id",
                    "attempt_id",
                    "adjustment_id",
                    "adjustment_state",
                    "reason",
                    "outbox_id",
                }
                or value["task_id"] != task.task_id
                or value["attempt_id"] != attempt.attempt_id
                or value["adjustment_id"] != command_id
                or value["adjustment_state"] != expected_state.value
                or value["reason"] != expected_reason
                or value["outbox_id"] != outbox_id
            )
            legacy_result_valid = result.ok and not positive_value_invalid and dict(result.extensions) == {}
            current_result_valid = (
                (
                    expected_state is TaskAdjustmentState.REJECTED
                    and not result.ok
                    and result.error is not None
                    and result.error.code is expected_error_code
                    and result.error.reason == expected_reason
                    and value is None
                )
                or (expected_state is not TaskAdjustmentState.REJECTED and result.ok and not positive_value_invalid)
            ) and dict(result.extensions) == command_result_extensions(
                expected_disposition,
                admission_event_id=request.event_id,
                settlement_event_id=(None if disposition is None else disposition.event_id),
            )
            if (
                command.payload.keys() - {"native_source"} != {"adjustment"}
                or result.observed_at != (request.occurred_at if disposition is None else disposition.occurred_at)
                or outbox_rows[0]["kind"] != OutboxKind.ATTEMPT_ADJUST.value
                or not (current_result_valid or legacy_result_valid)
            ):
                raise cls._corrupt("formal Task adjustment result does not bind durable control truth")

        if outbox_rows:
            outbox = outbox_rows[0]
            scope, spec, executor_ref, adjustment = cls._outbox_payload(outbox["payload_json"])
            dispatch_rows = connection.execute(
                """SELECT payload_json FROM outbox
                   WHERE task_id=? AND attempt_id=? AND kind=?""",
                (
                    task.task_id,
                    attempt.attempt_id,
                    OutboxKind.ATTEMPT_DISPATCH.value,
                ),
            ).fetchall()
            if len(dispatch_rows) != 1:
                raise cls._corrupt("formal Task control outbox lacks dispatch specification authority")
            _dispatch_scope, dispatch_spec, _dispatch_ref, _dispatch_adjustment = cls._outbox_payload(
                dispatch_rows[0]["payload_json"]
            )
            raw_delivery_count = outbox["delivery_count"]
            try:
                outbox_state = OutboxState(outbox["state"])
                if expected_type == "task.cancel":
                    if type(raw_delivery_count) is not int:
                        raise TypeError("cancel delivery count is not an integer")
                    delivery_count = raw_delivery_count
                else:
                    delivery_count = int(raw_delivery_count)
            except (TypeError, ValueError) as error:
                raise cls._corrupt("formal Task control outbox lifecycle is invalid") from error
            claimed = outbox_state is OutboxState.CLAIMED
            claim_fields = (
                outbox["claimed_by"],
                outbox["claimed_at"],
                outbox["claim_token"],
            )
            if (
                scope != task.scope
                or spec.context.scope != task.scope
                or spec != dispatch_spec
                or spec.executor_id != attempt.executor_id
                or executor_ref != attempt.executor_ref
                or delivery_count < 0
                or claimed != all(value is not None for value in claim_fields)
                or (not claimed and any(value is not None for value in claim_fields))
                or (claimed and delivery_count < 1)
            ):
                raise cls._corrupt("formal Task control outbox binding is not canonical")
            if expected_type == "task.cancel":
                if adjustment is not None:
                    raise cls._corrupt("formal Task cancel outbox carries adjustment authority")
                if outbox_state is OutboxState.DELIVERED and (delivery_count < 1 or outbox["last_error"] is not None):
                    raise cls._corrupt("formal Task delivered cancel outbox lifecycle is not canonical")
            else:
                if (
                    adjustment is None
                    or adjustment.adjustment_id != command_id
                    or adjustment.adjustment != command.payload["adjustment"]
                    or adjustment.requested_seq != request.seq
                    or adjustment.native_source != require_payload_source(command)
                ):
                    raise cls._corrupt("formal Task adjustment outbox does not bind its request")
                if disposition is None:
                    if outbox_state not in {OutboxState.PENDING, OutboxState.CLAIMED}:
                        raise cls._corrupt("pending Task adjustment has a terminal outbox")
                elif expected_state is TaskAdjustmentState.APPLIED:
                    if outbox_state is not OutboxState.DELIVERED or outbox["last_error"] is not None:
                        raise cls._corrupt("applied Task adjustment lacks delivered authority")
                elif (
                    outbox_state not in {OutboxState.DELIVERED, OutboxState.SUPPRESSED}
                    or outbox["last_error"] != expected_reason
                ):
                    raise cls._corrupt("rejected Task adjustment lacks exact outbox authority")
            return str(outbox["outbox_id"])
        return None

    @classmethod
    def _verify_durable_lineage(cls, connection: sqlite3.Connection, task_row: sqlite3.Row) -> None:
        """Prove every attempt epoch from create through the current pointer."""

        try:
            task = cls._task_from_row(task_row)
            attempt_rows = connection.execute(
                """
                SELECT * FROM attempts WHERE task_id=?
                ORDER BY attempt_number, attempt_id
                """,
                (task.task_id,),
            ).fetchall()
            attempts = tuple(cls._attempt_from_row(row) for row in attempt_rows)
            if (
                not attempts
                or tuple(item.attempt_number for item in attempts) != tuple(range(1, len(attempts) + 1))
                or attempts[-1].attempt_id != task.attempt_id
                or len(attempts) > 3
            ):
                raise cls._corrupt("formal Task attempt ordinals are not one contiguous lineage")
            attempts_by_id = {item.attempt_id: item for item in attempts}
            if len(attempts_by_id) != len(attempts):
                raise cls._corrupt("formal Task attempt lineage contains duplicates")

            event_rows = connection.execute(
                "SELECT * FROM task_events WHERE task_id=? ORDER BY seq",
                (task.task_id,),
            ).fetchall()
            events = tuple(cls._event_from_row(row) for row in event_rows)
            if len(events) != task.event_head + 1 or tuple(event.seq for event in events) != tuple(
                range(task.event_head + 1)
            ):
                raise cls._corrupt("formal Task event history is not one contiguous prefix")
            for event in events:
                if (
                    event.task_id != task.task_id
                    or event.scope != task.scope
                    or event.correlation_id != task.correlation_id
                    or event.attempt_id not in attempts_by_id
                ):
                    raise cls._corrupt("formal Task event history crosses its durable authority")

            ordinal_stream = tuple(attempts_by_id[event.attempt_id].attempt_number for event in events)
            if ordinal_stream != tuple(sorted(ordinal_stream)):
                raise cls._corrupt("formal Task attempt segments are not monotonically ordered")
            accepted_events = tuple(event for event in events if event.event_type == "task.accepted")
            retry_events = tuple(event for event in events if event.event_type == "task.retry_accepted")
            recovery_events = tuple(event for event in events if event.event_type == "task.recovery_accepted")
            if len(accepted_events) != 1 or len(retry_events) + len(recovery_events) != len(attempts) - 1:
                raise cls._corrupt("formal Task admission boundaries are incomplete or duplicated")

            dispatch_specs: dict[int, FormalTaskSpec] = {}
            for ordinal, attempt in enumerate(attempts, 1):
                attempt_row = attempt_rows[ordinal - 1]
                segment = tuple(event for event in events if event.attempt_id == attempt.attempt_id)
                if not segment:
                    raise cls._corrupt("formal Task attempt has no durable event segment")
                boundary = segment[0]
                has_recovery_table = connection.execute(
                    """SELECT 1 FROM sqlite_master
                       WHERE type='table' AND name='durability_recoveries'"""
                ).fetchone()
                recovery_boundary_row = (
                    None
                    if has_recovery_table is None
                    else connection.execute(
                        """SELECT * FROM durability_recoveries
                           WHERE task_id=? AND recovery_attempt_id=?""",
                        (task.task_id, attempt.attempt_id),
                    ).fetchone()
                )
                expected_boundary = (
                    "task.accepted"
                    if ordinal == 1
                    else ("task.recovery_accepted" if recovery_boundary_row is not None else "task.retry_accepted")
                )
                if boundary.event_type != expected_boundary:
                    raise cls._corrupt("formal Task attempt segment lacks its admission boundary")
                if any(
                    event.event_type
                    in {
                        "task.accepted",
                        "task.retry_accepted",
                        "task.recovery_accepted",
                    }
                    for event in segment[1:]
                ):
                    raise cls._corrupt("formal Task attempt segment has duplicate admission boundaries")
                authority_id_key = "recovery_id" if recovery_boundary_row is not None else "command_id"
                command_id = boundary.details.get(authority_id_key)
                if (
                    type(command_id) is not str
                    or boundary.causation_id != command_id
                    or boundary.state != FormalTaskState.ACCEPTED.value
                    or boundary.outcome is not None
                    or boundary.producer != "task_core"
                    or boundary.source_event_id is not None
                ):
                    raise cls._corrupt("formal Task admission boundary is not canonical")
                dispatch_rows = connection.execute(
                    """SELECT * FROM outbox
                       WHERE task_id=? AND attempt_id=? AND kind=?""",
                    (
                        task.task_id,
                        attempt.attempt_id,
                        OutboxKind.ATTEMPT_DISPATCH.value,
                    ),
                ).fetchall()
                if len(dispatch_rows) != 1:
                    raise cls._corrupt("formal Task attempt lacks one exact dispatch authority")
                dispatch_row = dispatch_rows[0]
                try:
                    dispatch_state = OutboxState(dispatch_row["state"])
                    dispatch_delivery_count = int(dispatch_row["delivery_count"])
                except (TypeError, ValueError) as error:
                    raise cls._corrupt("formal Task dispatch lifecycle is invalid") from error
                dispatch_claim_fields = (
                    dispatch_row["claimed_by"],
                    dispatch_row["claimed_at"],
                    dispatch_row["claim_token"],
                )
                dispatch_claim_clear = all(value is None for value in dispatch_claim_fields)
                if (
                    dispatch_delivery_count < 0
                    or (
                        dispatch_state is OutboxState.CLAIMED
                        and (any(value is None for value in dispatch_claim_fields) or dispatch_delivery_count < 1)
                    )
                    or (dispatch_state is not OutboxState.CLAIMED and not dispatch_claim_clear)
                ):
                    raise cls._corrupt("formal Task dispatch claim lifecycle is invalid")
                executor_rows = connection.execute(
                    """SELECT * FROM executor_events
                       WHERE attempt_id=? ORDER BY source_seq, source_event_id""",
                    (attempt.attempt_id,),
                ).fetchall()
                observations = tuple(cls._executor_observation_from_row(row) for row in executor_rows)
                if (
                    tuple(item.source_seq for item in observations) != tuple(range(len(observations)))
                    or attempt.source_seq != len(observations) - 1
                ):
                    raise cls._corrupt("formal Task Executor authority is not one contiguous prefix")
                observations_by_source = {item.source_event_id: item for item in observations}
                if None in observations_by_source or len(observations_by_source) != len(observations):
                    raise cls._corrupt("formal Task Executor authority contains duplicate identity")
                for observation in observations:
                    if (
                        observation.task_id != task.task_id
                        or observation.attempt_id != attempt.attempt_id
                        or observation.executor_id != attempt.executor_id
                        or observation.executor_ref != attempt.executor_ref
                    ):
                        raise cls._corrupt("formal Task Executor authority crosses its attempt binding")
                task_segment_state = FormalTaskState.ACCEPTED.value
                task_segment_outcome: str | None = None
                attempt_segment_state = FormalAttemptState.ACCEPTED.value
                attempt_segment_outcome: str | None = None
                cancel_command_id: str | None = None
                cancel_request: PersistentTaskEvent | None = None
                adjustment_requests: dict[str, PersistentTaskEvent] = {}
                adjustment_dispositions: dict[str, PersistentTaskEvent] = {}
                update_requests: dict[str, PersistentTaskEvent] = {}
                update_settlements: dict[str, PersistentTaskEvent] = {}
                reprioritize_requests: dict[str, PersistentTaskEvent] = {}
                reprioritize_settlements: dict[str, PersistentTaskEvent] = {}
                verified_control_outbox_ids: set[str] = set()
                executor_attempt_sources: set[str] = set()
                executor_task_sources: set[str] = set()
                store_owned_attempt_terminal: PersistentTaskEvent | None = None
                attempt_terminal_event: PersistentTaskEvent | None = None
                task_terminal_event: PersistentTaskEvent | None = None
                for event in segment[1:]:
                    if task_segment_state == FormalTaskState.TERMINAL.value:
                        raise cls._corrupt("formal Task event appeared after terminal truth")
                    if event.event_type in {
                        "task.running",
                        "task.blocked",
                        "task.decision_required",
                        "task.terminal",
                    }:
                        expected_state = event.event_type.removeprefix("task.")
                        if event.state != expected_state:
                            raise cls._corrupt("formal Task event type disagrees with its state")
                        if event.producer not in {
                            "task_core",
                            "task_core.delivery",
                            "task_core.reconciliation",
                            "task_core.admission",
                        } or (event.event_type != "task.terminal" and event.producer != "task_core"):
                            raise cls._corrupt("formal Task lifecycle event has no canonical producer")
                        if event.source_event_id is not None:
                            observation = observations_by_source.get(event.source_event_id)
                            expected_attempt_state = (
                                FormalAttemptState.RUNNING
                                if event.event_type == "task.running"
                                else (FormalAttemptState.TERMINAL if event.event_type == "task.terminal" else None)
                            )
                            expected_details = (
                                None
                                if observation is None
                                else {
                                    "raw_status": observation.raw_status,
                                    "summary": observation.summary,
                                    "error": observation.error,
                                }
                            )
                            if (
                                observation is None
                                or expected_attempt_state is None
                                or observation.attempt_state is not expected_attempt_state
                                or event.producer != "task_core"
                                or event.causation_id != event.source_event_id
                                or event.state != observation.attempt_state.value
                                or event.outcome
                                != (None if observation.attempt_outcome is None else observation.attempt_outcome.value)
                                or event.occurred_at != observation.occurred_at
                                or event.details != expected_details
                                or event.source_event_id in executor_task_sources
                            ):
                                raise cls._corrupt("formal Task lifecycle event lacks exact Executor authority")
                            executor_task_sources.add(event.source_event_id)
                        elif event.event_type == "task.running":
                            raise cls._corrupt("formal Task running event lacks Executor authority")
                        elif event.event_type == "task.terminal":
                            expected_producer = (
                                "task_core"
                                if store_owned_attempt_terminal is not None
                                and store_owned_attempt_terminal.outcome == TerminalOutcome.CANCELLED.value
                                and store_owned_attempt_terminal.details == {"reason": "CANCELLED_BEFORE_DISPATCH"}
                                else (
                                    None
                                    if store_owned_attempt_terminal is None
                                    else store_owned_attempt_terminal.producer
                                )
                            )
                            if (
                                store_owned_attempt_terminal is None
                                or event.producer != expected_producer
                                or event.causation_id != store_owned_attempt_terminal.causation_id
                                or event.occurred_at != store_owned_attempt_terminal.occurred_at
                                or event.details != store_owned_attempt_terminal.details
                                or event.outcome != store_owned_attempt_terminal.outcome
                            ):
                                raise cls._corrupt("formal Task terminal event lacks exact Store authority")
                        validate_transition(
                            LifecycleKind.TASK,
                            task_segment_state,
                            event.state,
                            outcome=event.outcome,
                        )
                        if (
                            event.state
                            in {
                                FormalTaskState.RUNNING.value,
                                FormalTaskState.BLOCKED.value,
                                FormalTaskState.DECISION_REQUIRED.value,
                            }
                            and attempt_segment_state != FormalAttemptState.RUNNING.value
                        ) or (
                            event.state == FormalTaskState.TERMINAL.value
                            and (
                                attempt_segment_state != FormalAttemptState.TERMINAL.value
                                or event.outcome != attempt_segment_outcome
                            )
                        ):
                            raise cls._corrupt("formal Task lifecycle disagrees with its attempt")
                        task_segment_state = event.state
                        task_segment_outcome = event.outcome
                        if event.event_type == "task.terminal":
                            task_terminal_event = event
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
                        if (
                            event.producer != "task_core.control"
                            or event.source_event_id is not None
                            or event.state != task_segment_state
                            or event.outcome != task_segment_outcome
                        ):
                            raise cls._corrupt("formal Task control event is not state preserving")
                        command_id_value = event.details.get("command_id")
                        expected_details = (
                            {"command_id", "reason"}
                            if event.event_type == "task.adjust_rejected"
                            else (
                                {"command_id", "priority"}
                                if event.event_type.startswith("task.reprioritize_")
                                else {"command_id"}
                            )
                        )
                        reason = event.details.get("reason")
                        priority = event.details.get("priority")
                        if (
                            set(event.details) != expected_details
                            or type(command_id_value) is not str
                            or not command_id_value.strip()
                            or command_id_value != event.causation_id
                            or (
                                event.event_type.startswith("task.reprioritize_")
                                and priority not in {item.value for item in AdmissionPriority}
                            )
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
                            raise cls._corrupt("formal Task control event has invalid authority evidence")
                        if event.event_type == "task.cancel_requested":
                            if cancel_request is not None:
                                raise cls._corrupt("formal Task attempt has duplicate cancel requests")
                            cancel_command_id = command_id_value
                            cancel_request = event
                        elif event.event_type == "task.update_requested":
                            if (
                                task_segment_state != FormalTaskState.ACCEPTED.value
                                or attempt_segment_state != FormalAttemptState.ACCEPTED.value
                                or command_id_value in update_requests
                                or command_id_value in update_settlements
                            ):
                                raise cls._corrupt("formal Task update request is duplicated or stale")
                            update_requests[command_id_value] = event
                        elif event.event_type == "task.update_applied":
                            if command_id_value not in update_requests or command_id_value in update_settlements:
                                raise cls._corrupt("formal Task update settlement is unpaired")
                            update_settlements[command_id_value] = event
                        elif event.event_type == "task.reprioritize_requested":
                            if (
                                task_segment_state != FormalTaskState.ACCEPTED.value
                                or attempt_segment_state != FormalAttemptState.ACCEPTED.value
                                or command_id_value in reprioritize_requests
                                or command_id_value in reprioritize_settlements
                            ):
                                raise cls._corrupt("formal Task reprioritize request is duplicated or stale")
                            reprioritize_requests[command_id_value] = event
                        elif event.event_type == "task.reprioritize_applied":
                            request = reprioritize_requests.get(command_id_value)
                            if (
                                request is None
                                or command_id_value in reprioritize_settlements
                                or event.seq != request.seq + 1
                                or event.details != request.details
                            ):
                                raise cls._corrupt("formal Task reprioritize settlement is unpaired")
                            reprioritize_settlements[command_id_value] = event
                        elif event.event_type == "task.adjust_requested":
                            if command_id_value in adjustment_requests or command_id_value in adjustment_dispositions:
                                raise cls._corrupt("formal Task adjustment request is duplicated")
                            adjustment_requests[command_id_value] = event
                        else:
                            if (
                                command_id_value not in adjustment_requests
                                or command_id_value in adjustment_dispositions
                            ):
                                raise cls._corrupt("formal Task adjustment disposition is unpaired")
                            adjustment_dispositions[command_id_value] = event
                    elif event.event_type in {
                        "attempt.accepted",
                        "attempt.running",
                        "attempt.terminal",
                    }:
                        expected_state = event.event_type.removeprefix("attempt.")
                        if event.state != expected_state:
                            raise cls._corrupt("formal Task attempt event type disagrees with its state")
                        producer_is_executor = event.producer == attempt.executor_id
                        if producer_is_executor:
                            if event.source_event_id is None or event.causation_id != event.source_event_id:
                                raise cls._corrupt("formal Task attempt lacks Executor source evidence")
                            observation = observations_by_source.get(event.source_event_id)
                            expected_details = (
                                None
                                if observation is None
                                else {
                                    "raw_status": observation.raw_status,
                                    "summary": observation.summary,
                                    "error": observation.error,
                                }
                            )
                            if (
                                observation is None
                                or observation.attempt_state is None
                                or event.state != observation.attempt_state.value
                                or event.outcome
                                != (None if observation.attempt_outcome is None else observation.attempt_outcome.value)
                                or event.occurred_at != observation.occurred_at
                                or event.details != expected_details
                                or event.source_event_id in executor_attempt_sources
                            ):
                                raise cls._corrupt("formal Task attempt event lacks exact Executor authority")
                            executor_attempt_sources.add(event.source_event_id)
                        elif (
                            event.producer
                            not in {
                                "task_core.delivery",
                                "task_core.reconciliation",
                                "task_core.admission",
                            }
                            or event.source_event_id is not None
                            or event.state != FormalAttemptState.TERMINAL.value
                        ):
                            raise cls._corrupt("formal Task attempt event has no canonical producer")
                        if event.state == FormalAttemptState.ACCEPTED.value:
                            if (
                                attempt_segment_state != FormalAttemptState.ACCEPTED.value
                                or not producer_is_executor
                                or event.outcome is not None
                            ):
                                raise cls._corrupt("formal Task attempt acceptance is not canonical")
                        elif (
                            attempt_segment_state == FormalAttemptState.ACCEPTED.value
                            and event.state == FormalAttemptState.TERMINAL.value
                        ):
                            dispatch_rejection = (
                                event.producer == "task_core.delivery"
                                and event.outcome == TerminalOutcome.FAILED.value
                                and set(event.details) == {"reason", "error"}
                                and all(
                                    type(event.details[key]) is str and bool(event.details[key])
                                    for key in ("reason", "error")
                                )
                                and dispatch_row["outbox_id"] == event.causation_id
                                and dispatch_state is OutboxState.SUPPRESSED
                                and dispatch_delivery_count >= 1
                                and dispatch_claim_clear
                                and dispatch_row["last_error"]
                                == (f"{event.details['reason']}: {event.details['error']}")[:1000]
                                and dispatch_row["updated_at"] == event.occurred_at
                            )
                            cancelled_before_dispatch = (
                                event.producer == "task_core.reconciliation"
                                and event.outcome == TerminalOutcome.CANCELLED.value
                                and event.details == {"reason": "CANCELLED_BEFORE_DISPATCH"}
                                and cancel_command_id == event.causation_id
                                and attempt.executor_ref is None
                                and attempt.source_seq == -1
                                and dispatch_state is OutboxState.SUPPRESSED
                                and dispatch_claim_clear
                                and (
                                    (dispatch_delivery_count == 0 and dispatch_row["last_error"] is None)
                                    or (
                                        attempt.selection is not None
                                        and dispatch_delivery_count > 0
                                        and dispatch_delivery_count == attempt_row["admission_attempt_count"]
                                        and attempt_row["admission_reason"]
                                        in {
                                            "EXECUTOR_PROJECT_BUSY",
                                            "EXECUTOR_CAPACITY_EXHAUSTED",
                                        }
                                        and dispatch_row["last_error"] == attempt_row["admission_reason"]
                                    )
                                )
                                and dispatch_row["updated_at"] == event.occurred_at
                            )
                            lost_reconciliation = (
                                event.producer == "task_core.reconciliation"
                                and event.outcome == TerminalOutcome.INTERRUPTED.value
                                and event.causation_id == f"reconciliation:{attempt.attempt_id}"
                                and set(event.details) == {"reason"}
                                and type(event.details.get("reason")) is str
                                and dispatch_state is OutboxState.SUPPRESSED
                                and dispatch_claim_clear
                                and dispatch_row["last_error"] == "EXECUTOR_ATTEMPT_LOST"
                                and dispatch_row["updated_at"] == event.occurred_at
                            )
                            admission_timeout = (
                                event.producer == "task_core.admission"
                                and event.outcome == TerminalOutcome.FAILED.value
                                and event.details == {"reason": "EXECUTOR_ADMISSION_TIMEOUT"}
                                and event.causation_id == dispatch_row["outbox_id"]
                                and attempt.selection is not None
                                and attempt.executor_ref is None
                                and attempt.source_seq == -1
                                and dispatch_state is OutboxState.SUPPRESSED
                                and dispatch_delivery_count == int(attempt_row["admission_attempt_count"])
                                and dispatch_claim_clear
                                and dispatch_row["last_error"] == "EXECUTOR_ADMISSION_TIMEOUT"
                                and dispatch_row["updated_at"] == event.occurred_at
                            )
                            if not (
                                dispatch_rejection
                                or cancelled_before_dispatch
                                or lost_reconciliation
                                or admission_timeout
                            ):
                                raise cls._corrupt("formal Task accepted attempt terminal is not canonical")
                        else:
                            if not producer_is_executor and event.state == FormalAttemptState.TERMINAL.value:
                                lost_reconciliation = (
                                    attempt_segment_state == FormalAttemptState.RUNNING.value
                                    and event.producer == "task_core.reconciliation"
                                    and event.outcome == TerminalOutcome.INTERRUPTED.value
                                    and event.causation_id == f"reconciliation:{attempt.attempt_id}"
                                    and set(event.details) == {"reason"}
                                    and type(event.details.get("reason")) is str
                                    and bool(event.details["reason"])
                                    and dispatch_state is OutboxState.DELIVERED
                                    and dispatch_claim_clear
                                    and connection.execute(
                                        """SELECT 1 FROM outbox
                                           WHERE task_id=? AND attempt_id=?
                                             AND state IN (?, ?) LIMIT 1""",
                                        (
                                            task.task_id,
                                            attempt.attempt_id,
                                            OutboxState.PENDING.value,
                                            OutboxState.CLAIMED.value,
                                        ),
                                    ).fetchone()
                                    is None
                                )
                                if not lost_reconciliation:
                                    raise cls._corrupt(
                                        "formal Task running attempt terminal lacks exact Store authority"
                                    )
                            validate_transition(
                                LifecycleKind.ATTEMPT,
                                attempt_segment_state,
                                event.state,
                                outcome=event.outcome,
                            )
                        if not producer_is_executor and event.state == FormalAttemptState.TERMINAL.value:
                            if store_owned_attempt_terminal is not None:
                                raise cls._corrupt("formal Task attempt has duplicate Store terminal truth")
                            store_owned_attempt_terminal = event
                        if event.state == FormalAttemptState.TERMINAL.value:
                            attempt_terminal_event = event
                        attempt_segment_state = event.state
                        attempt_segment_outcome = event.outcome
                    else:
                        raise cls._corrupt("formal Task event type is not canonical")
                if set(reprioritize_requests) != set(reprioritize_settlements):
                    raise cls._corrupt("formal Task reprioritize authority lacks exact settlement")
                if cancel_request is not None:
                    outbox_id = cls._verify_control_authority(
                        connection,
                        task=task,
                        attempt=attempt,
                        request=cancel_request,
                        attempt_terminal=attempt_terminal_event,
                        task_terminal=task_terminal_event,
                    )
                    if outbox_id is not None:
                        verified_control_outbox_ids.add(outbox_id)
                for adjustment_id, request_event in adjustment_requests.items():
                    outbox_id = cls._verify_control_authority(
                        connection,
                        task=task,
                        attempt=attempt,
                        request=request_event,
                        disposition=adjustment_dispositions.get(adjustment_id),
                    )
                    if outbox_id is None:
                        raise cls._corrupt("formal Task adjustment authority lost its outbox")
                    verified_control_outbox_ids.add(outbox_id)
                actual_control_outbox_ids = {
                    str(row["outbox_id"])
                    for row in connection.execute(
                        """SELECT outbox_id FROM outbox
                           WHERE task_id=? AND attempt_id=? AND kind IN (?, ?)""",
                        (
                            task.task_id,
                            attempt.attempt_id,
                            OutboxKind.ATTEMPT_CANCEL.value,
                            OutboxKind.ATTEMPT_ADJUST.value,
                        ),
                    ).fetchall()
                }
                if actual_control_outbox_ids != verified_control_outbox_ids:
                    raise cls._corrupt("formal Task control outbox lacks exact event authority")
                attempt_outbox_ids = {
                    str(row["outbox_id"])
                    for row in connection.execute(
                        "SELECT outbox_id FROM outbox WHERE task_id=? AND attempt_id=?",
                        (task.task_id, attempt.attempt_id),
                    ).fetchall()
                }
                if attempt_outbox_ids != {
                    str(dispatch_row["outbox_id"]),
                    *actual_control_outbox_ids,
                }:
                    raise cls._corrupt("formal Task attempt contains an unknown durable outbox")
                expected_task_sources = {
                    str(item.source_event_id)
                    for item in observations
                    if item.attempt_state in {FormalAttemptState.RUNNING, FormalAttemptState.TERMINAL}
                }
                if (
                    executor_attempt_sources != set(observations_by_source)
                    or executor_task_sources != expected_task_sources
                ):
                    raise cls._corrupt("formal Task events do not exhaust exact Executor authority")
                if attempt_segment_state != attempt.state.value or attempt_segment_outcome != (
                    None if attempt.outcome is None else attempt.outcome.value
                ):
                    raise cls._corrupt("formal Task attempt row disagrees with its event history")
                recovery_row = recovery_boundary_row
                if recovery_row is not None:
                    if ordinal == 1:
                        raise cls._corrupt("initial formal Task Attempt cannot be a recovery")
                    predecessor = attempts[ordinal - 2]
                    prior_spec = dispatch_specs.get(ordinal - 1)
                    scope, dispatch_spec, executor_ref, adjustment = cls._outbox_payload(dispatch_row["payload_json"])
                    command_count = connection.execute(
                        """SELECT COUNT(*) FROM commands
                           WHERE scope_key=? AND command_id=?""",
                        (_scope_key(task.scope), command_id),
                    ).fetchone()[0]
                    predecessor_terminal = tuple(
                        event
                        for event in events
                        if event.attempt_id == predecessor.attempt_id
                        and event.event_type in {"attempt.terminal", "task.terminal"}
                    )
                    if (
                        recovery_row["recovery_id"] != command_id
                        or recovery_row["producer_attempt_id"] != predecessor.attempt_id
                        or recovery_row["recovery_generation"] != ordinal - 1
                        or command_count != 0
                        or boundary.producer != "task_core"
                        or set(boundary.details)
                        != {
                            "recovery_id",
                            "producer_attempt_id",
                            "producer_outcome",
                            "recovery_generation",
                            "recovery_budget_remaining",
                            "attempt_number",
                        }
                        or boundary.details.get("producer_attempt_id") != predecessor.attempt_id
                        or boundary.details.get("producer_outcome") != TerminalOutcome.INTERRUPTED.value
                        or boundary.details.get("recovery_generation") != ordinal - 1
                        or boundary.details.get("recovery_budget_remaining") != 3 - ordinal
                        or boundary.details.get("attempt_number") != ordinal
                        or predecessor.state is not FormalAttemptState.TERMINAL
                        or predecessor.outcome is not TerminalOutcome.INTERRUPTED
                        or len(predecessor_terminal) != 2
                        or any(event.outcome != TerminalOutcome.INTERRUPTED.value for event in predecessor_terminal)
                        or attempt.selection != predecessor.selection
                        or prior_spec is None
                        or dispatch_spec != prior_spec
                        or scope != task.scope
                        or executor_ref is not None
                        or adjustment is not None
                        or dispatch_row["command_id"] != command_id
                    ):
                        raise cls._corrupt("formal Task durability recovery admission is not canonical")
                    dispatch_specs[ordinal] = dispatch_spec
                    if ordinal == len(attempts) and (
                        task_segment_state != task.state.value
                        or task_segment_outcome != (None if task.outcome is None else task.outcome.value)
                        or dispatch_spec != task.spec
                    ):
                        raise cls._corrupt("formal Task recovery pointer disagrees with durable lineage")
                    continue
                command_rows = connection.execute(
                    """
                    SELECT * FROM commands WHERE scope_key=? AND command_id=?
                    """,
                    (_scope_key(task.scope), command_id),
                ).fetchall()
                dispatch_rows = connection.execute(
                    """
                    SELECT * FROM outbox
                    WHERE task_id=? AND attempt_id=? AND kind=?
                    """,
                    (
                        task.task_id,
                        attempt.attempt_id,
                        OutboxKind.ATTEMPT_DISPATCH.value,
                    ),
                ).fetchall()
                if len(command_rows) != 1 or len(dispatch_rows) != 1:
                    raise cls._corrupt("formal Task admission lacks one exact ledger and dispatch")
                command, result, resolved_spec = cls._command_ledger_from_row(command_rows[0])
                outbox_row = dispatch_rows[0]
                scope, dispatch_spec, executor_ref, adjustment = cls._outbox_payload(outbox_row["payload_json"])
                try:
                    outbox_state = OutboxState(outbox_row["state"])
                    delivery_count = int(outbox_row["delivery_count"])
                except (TypeError, ValueError) as error:
                    raise cls._corrupt("formal Task dispatch lifecycle is invalid") from error
                result_value = result.result
                if (
                    scope != task.scope
                    or dispatch_spec.executor_id != attempt.executor_id
                    or executor_ref is not None
                    or adjustment is not None
                    or outbox_row["command_id"] != command_id
                    or delivery_count < 0
                    or (
                        outbox_state is OutboxState.CLAIMED
                        and (
                            outbox_row["claimed_by"] is None
                            or outbox_row["claimed_at"] is None
                            or outbox_row["claim_token"] is None
                        )
                    )
                    or (
                        outbox_state is not OutboxState.CLAIMED
                        and (
                            outbox_row["claimed_by"] is not None
                            or outbox_row["claimed_at"] is not None
                            or outbox_row["claim_token"] is not None
                        )
                    )
                    or command.scope != task.scope
                    or command.correlation_id != task.correlation_id
                    or command.required_capabilities != (command.command_type,)
                    or result_value is None
                    or result_value.get("task_id") != task.task_id
                    or result_value.get("attempt_id") != attempt.attempt_id
                    or result_value.get("outbox_id") != outbox_row["outbox_id"]
                    or result_value.get("state") != FormalTaskState.ACCEPTED.value
                ):
                    raise cls._corrupt("formal Task admission ledger does not bind its successor")
                dispatch_specs[ordinal] = dispatch_spec
                cls._verify_reprioritize_authority(
                    connection,
                    task=task,
                    attempt=attempt,
                    requests=reprioritize_requests,
                    settlements=reprioritize_settlements,
                )

                if ordinal == 1:
                    if resolved_spec is None:
                        raise cls._corrupt("initial formal Task admission lacks a resolved spec")
                    verified_spec = cls._verify_update_authority(
                        connection,
                        task=task,
                        attempt=attempt,
                        base_spec=resolved_spec,
                        dispatch_row=outbox_row,
                        requests=update_requests,
                        settlements=update_settlements,
                    )
                    common_invalid = (
                        boundary.seq != 0
                        or set(boundary.details) != {"command_id"}
                        or verified_spec != dispatch_spec
                        or resolved_spec.origin != command.origin
                        or resolved_spec.required_capabilities != command.required_capabilities
                        or resolved_spec.native_source != require_payload_source(command)
                    )
                    if common_invalid:
                        raise cls._corrupt("initial formal Task admission is not canonical")
                    if task.predecessor_task_id is None:
                        if (
                            command.command_type != "task.create"
                            or command.target_ref.id != f"create:{command_id}"
                            or set(result_value)
                            != {
                                "task_id",
                                "attempt_id",
                                "state",
                                "outbox_id",
                            }
                        ):
                            raise cls._corrupt("initial formal Task create is not canonical")
                    else:
                        cls._verify_successor_admission(
                            connection,
                            task=task,
                            attempt=attempt,
                            boundary=boundary,
                            command=command,
                            result=result,
                            resolved_spec=resolved_spec,
                            dispatch_row=outbox_row,
                        )
                else:
                    predecessor = attempts[ordinal - 2]
                    unsettled_predecessor = connection.execute(
                        """
                        SELECT 1 FROM outbox
                        WHERE task_id=? AND attempt_id=? AND state IN (?, ?)
                        LIMIT 1
                        """,
                        (
                            task.task_id,
                            predecessor.attempt_id,
                            OutboxState.PENDING.value,
                            OutboxState.CLAIMED.value,
                        ),
                    ).fetchone()
                    if (
                        set(boundary.details)
                        != {
                            "command_id",
                            "retry_of_attempt_id",
                            "previous_outcome",
                            "attempt_number",
                        }
                        or command.command_type != "task.retry"
                        or command.target_ref.id != task.task_id
                        or resolved_spec is not None
                    ):
                        raise cls._corrupt("formal Task retry admission is not canonical")
                    prior_spec = dispatch_specs[ordinal - 1]
                    verified_spec = cls._verify_update_authority(
                        connection,
                        task=task,
                        attempt=attempt,
                        base_spec=replace(prior_spec, context=dispatch_spec.context),
                        dispatch_row=outbox_row,
                        requests=update_requests,
                        settlements=update_settlements,
                    )
                    if (
                        (
                            dispatch_spec.name,
                            dispatch_spec.origin,
                            dispatch_spec.executor_id,
                            dispatch_spec.required_capabilities,
                            dispatch_spec.side_effect_class,
                            dispatch_spec.attributes,
                            dispatch_spec.native_source,
                        )
                        != (
                            prior_spec.name,
                            prior_spec.origin,
                            prior_spec.executor_id,
                            prior_spec.required_capabilities,
                            prior_spec.side_effect_class,
                            prior_spec.attributes,
                            prior_spec.native_source,
                        )
                        or verified_spec != dispatch_spec
                        or (
                            dispatch_spec.context.source,
                            dispatch_spec.context.stable_id,
                            dispatch_spec.context.uri,
                            dispatch_spec.context.scope,
                        )
                        != (
                            prior_spec.context.source,
                            prior_spec.context.stable_id,
                            prior_spec.context.uri,
                            prior_spec.context.scope,
                        )
                    ):
                        raise cls._corrupt("formal Task retry changed stable specification identity")
                    TaskRetryProductRequestFingerprint.from_extensions(command.extensions)
                    precondition = TaskRetryPrecondition.from_payload(command.payload)
                    legacy_retry_extensions = {"live_voice.store": {"durability": "sqlite_outbox"}}
                    current_retry_extensions = {
                        **command_result_extensions(
                            TaskCommandDisposition.APPLIED,
                            admission_event_id=boundary.event_id,
                            settlement_event_id=boundary.event_id,
                        ),
                        "live_voice.store": {"durability": "sqlite_outbox"},
                    }
                    result_extensions = dict(result.extensions)
                    if (
                        boundary.details.get("retry_of_attempt_id") != predecessor.attempt_id
                        or boundary.details.get("previous_outcome")
                        != (None if predecessor.outcome is None else predecessor.outcome.value)
                        or boundary.details.get("attempt_number") != ordinal
                        or precondition.previous_attempt_id != predecessor.attempt_id
                        or precondition.previous_outcome != predecessor.outcome
                        or precondition.attempt_number != ordinal
                        or predecessor.state is not FormalAttemptState.TERMINAL
                        or predecessor.outcome
                        not in {
                            TerminalOutcome.CANCELLED,
                            TerminalOutcome.COMPLETED,
                        }
                        or (
                            predecessor.outcome is TerminalOutcome.COMPLETED
                            and result_extensions != legacy_retry_extensions
                        )
                        or (
                            predecessor.outcome is TerminalOutcome.CANCELLED
                            and result_extensions != legacy_retry_extensions
                            and result_extensions != current_retry_extensions
                        )
                        or unsettled_predecessor is not None
                        or set(result_value)
                        != {
                            "task_id",
                            "previous_attempt_id",
                            "attempt_id",
                            "attempt_number",
                            "applied",
                            "state",
                            "outbox_id",
                        }
                        or result_value.get("previous_attempt_id") != predecessor.attempt_id
                        or result_value.get("attempt_number") != ordinal
                        or result_value.get("applied") is not True
                    ):
                        raise cls._corrupt("formal Task retry lineage does not bind its predecessor")
                    predecessor_segment = tuple(event for event in events if event.attempt_id == predecessor.attempt_id)
                    terminal_events = tuple(
                        event for event in predecessor_segment if event.event_type == "task.terminal"
                    )
                    attempt_terminal_events = tuple(
                        event for event in predecessor_segment if event.event_type == "attempt.terminal"
                    )
                    if (
                        len(terminal_events) != 1
                        or len(attempt_terminal_events) != 1
                        or terminal_events[0].state != FormalTaskState.TERMINAL.value
                        or terminal_events[0].outcome != predecessor.outcome.value
                        or attempt_terminal_events[0].state != FormalAttemptState.TERMINAL.value
                        or attempt_terminal_events[0].outcome != predecessor.outcome.value
                    ):
                        raise cls._corrupt("formal Task retry predecessor lacks exact terminal truth")

                if ordinal == len(attempts) and (
                    task_segment_state != task.state.value
                    or task_segment_outcome != (None if task.outcome is None else task.outcome.value)
                ):
                    raise cls._corrupt("formal Task row disagrees with its event history")

            current_task_events = tuple(
                event
                for event in events
                if event.attempt_id == task.attempt_id
                and event.event_type
                in {
                    "task.accepted",
                    "task.retry_accepted",
                    "task.recovery_accepted",
                    "task.running",
                    "task.blocked",
                    "task.decision_required",
                    "task.terminal",
                }
            )
            if (
                not current_task_events
                or current_task_events[-1].state != task.state.value
                or current_task_events[-1].outcome != (None if task.outcome is None else task.outcome.value)
                or dispatch_specs[len(attempts)] != task.spec
            ):
                raise cls._corrupt("formal Task current pointer disagrees with its durable lineage")
        except FormalTaskViolation as error:
            if error.reason == "TASK_STORE_CORRUPT":
                raise
            raise cls._corrupt("formal Task durable attempt lineage is invalid") from error
        except (ContractViolation, KeyError, TypeError, ValueError) as error:
            raise cls._corrupt("formal Task durable attempt lineage is invalid") from error

    def _command_replay(
        self,
        connection: sqlite3.Connection,
        command: CommandEnvelope,
        fingerprint: bytes,
    ) -> ResultEnvelope | None:
        deferred = self.adjustment_queue.replay(connection, command, fingerprint)
        if deferred is not None:
            return deferred
        row = connection.execute(
            """
            SELECT * FROM commands
            WHERE scope_key=? AND command_id=?
            """,
            (_scope_key(command.scope), command.command_id),
        ).fetchone()
        if row is None:
            return None
        try:
            result = ResultEnvelope.from_dict(_json_load(row["result_json"]))
        except ContractViolation as error:
            raise FormalTaskViolation(
                "TASK_STORE_CORRUPT",
                "formal Task Store contains an invalid command result",
                ErrorCode.INTERNAL,
            ) from error
        try:
            stored_fingerprint = _json_load(row["fingerprint"])
        except FormalTaskViolation:
            stored_fingerprint = None
        decision_binding = type(stored_fingerprint) is dict and any(
            field in stored_fingerprint
            for field in {
                "binding_type",
                "binding_sha256",
                "authority_sha256",
                "replay_sha256",
            }
        )
        if not result.ok and decision_binding:
            binding, result = self._decision_binding_from_row(row)
            command_sha256 = self._sha256_hex(command.fingerprint())
            replay_sha256 = self._sha256_hex(fingerprint)
            if binding["command_sha256"] != command_sha256 or binding["replay_sha256"] != replay_sha256:
                raise FormalTaskViolation(
                    "IDEMPOTENCY_CONFLICT",
                    "command_id is already bound to different task facts",
                    ErrorCode.CONFLICT,
                )
            authority = binding["authority"]
            if (
                binding["scope_sha256"] != self._json_value_sha256(command.scope.to_dict())
                or binding["command_type"] != command.command_type
                or binding["target_task_id"] != command.target_ref.id
                or binding["correlation_id"] != command.correlation_id
                or authority["payload"] != self._decision_payload_authority(command)
            ):
                raise self._corrupt("formal Task decision binding disagrees with its command digest")
            expected = self._business_decisions_for_type(row["command_type"])
            self._verify_business_decision(connection, row, expected=expected)
            return result.for_request(command.request_id)
        if row["fingerprint"] != fingerprint:
            raise FormalTaskViolation(
                "IDEMPOTENCY_CONFLICT",
                "command_id is already bound to different task facts",
                ErrorCode.CONFLICT,
            )
        return result.for_request(command.request_id)

    @classmethod
    def _business_decisions_for_type(
        cls, command_type: str
    ) -> Mapping[str, tuple[TaskCommandDisposition, frozenset[ErrorCode]]]:
        if command_type == "task.update":
            return _UPDATE_BUSINESS_DECISIONS
        if command_type == "task.create_successor":
            return _SUCCESSOR_BUSINESS_DECISIONS
        if command_type == "task.retry":
            return _RETRY_BUSINESS_DECISIONS
        if command_type in {
            "task.provide_input",
            "task.pause",
            "task.resume",
            "task.reprioritize",
        }:
            return _CONTROL_BUSINESS_DECISIONS
        if command_type == "task.adjust":
            return _ADJUST_BUSINESS_DECISIONS
        if command_type == "task.cancel":
            return _CANCEL_BUSINESS_DECISIONS
        if command_type == "task.ack_events":
            return _ACK_BUSINESS_DECISIONS
        raise cls._corrupt("formal Task decision operation is unsupported")

    @classmethod
    def _persist_business_decision(
        cls,
        connection: sqlite3.Connection,
        command: CommandEnvelope,
        fingerprint: bytes,
        *,
        disposition: TaskCommandDisposition,
        code: ErrorCode,
        reason: str,
        message: str,
        observed_at: str,
    ) -> ResultEnvelope:
        """Persist one post-authorization closed negative business decision."""

        allowed_codes = {
            TaskCommandDisposition.REJECTED: {
                ErrorCode.INVALID_ARGUMENT,
            },
            TaskCommandDisposition.UNSUPPORTED: {
                ErrorCode.UNSUPPORTED,
                ErrorCode.CAPABILITY_UNAVAILABLE,
            },
            TaskCommandDisposition.CONFLICT: {
                ErrorCode.CONFLICT,
                ErrorCode.STALE,
            },
        }
        if code not in allowed_codes.get(disposition, set()):
            raise FormalTaskViolation(
                "TASK_COMMAND_DECISION_INVALID",
                "durable business decision has an invalid disposition family",
                ErrorCode.INTERNAL,
            )
        result = ResultEnvelope.failure(
            owner=command,
            error=ContractViolation(code, reason, message).error,
            observed_at=observed_at,
            extensions=command_result_extensions(disposition),
        )
        sanitized_fingerprint = cls._business_decision_fingerprint(
            connection,
            command,
            fingerprint,
            reason=reason,
        )
        cls._insert_command(
            connection,
            command,
            sanitized_fingerprint,
            _scope_key(command.scope),
            result,
            observed_at,
        )
        return result

    def _verified_retry_replay(
        self,
        connection: sqlite3.Connection,
        command: CommandEnvelope,
        fingerprint: bytes,
    ) -> ResultEnvelope | None:
        replay = self._command_replay(connection, command, fingerprint)
        if replay is None:
            return None
        if not replay.ok:
            row = connection.execute(
                "SELECT * FROM commands WHERE scope_key=? AND command_id=?",
                (_scope_key(command.scope), command.command_id),
            ).fetchone()
            if row is None:
                raise self._corrupt("retry decision disappeared during exact replay")
            self._verify_business_decision(
                connection,
                row,
                expected=_RETRY_BUSINESS_DECISIONS,
            )
            return replay
        task_row = self._require_task_row(connection, command.target_ref.id, command.scope)
        self._verify_durable_lineage(connection, task_row)
        result = replay.result
        if result is None or result.get("task_id") != command.target_ref.id:
            raise self._corrupt("applied retry result does not bind its durable task")
        boundary = connection.execute(
            """
            SELECT 1 FROM task_events
            WHERE task_id=? AND attempt_id=? AND event_type='task.retry_accepted'
              AND causation_id=?
            """,
            (
                command.target_ref.id,
                result.get("attempt_id"),
                command.command_id,
            ),
        ).fetchall()
        if len(boundary) != 1:
            raise self._corrupt("applied retry result lacks one exact durable boundary")
        return replay

    @staticmethod
    def _insert_command(
        connection: sqlite3.Connection,
        command: CommandEnvelope,
        fingerprint: bytes,
        scope_key: str,
        result: ResultEnvelope,
        created_at: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO commands(
                command_id, fingerprint, command_type, scope_key,
                result_json, created_at
            ) VALUES(?, ?, ?, ?, ?, ?)
            """,
            (
                command.command_id,
                fingerprint,
                command.command_type,
                scope_key,
                _json_dump(result.to_dict()),
                created_at,
            ),
        )

    @staticmethod
    def _insert_event(
        connection: sqlite3.Connection,
        *,
        event_id: str,
        task_id: str,
        attempt_id: str,
        scope: ScopeRef,
        seq: int,
        event_type: str,
        state: str,
        outcome: str | None,
        producer: str,
        source_event_id: str | None,
        causation_id: str,
        correlation_id: str,
        occurred_at: str,
        details: dict[str, object],
    ) -> None:
        connection.execute(
            """
            INSERT INTO task_events(
                task_id, seq, event_id, attempt_id, scope_json, event_type, state, outcome,
                producer, source_event_id, causation_id, correlation_id,
                occurred_at, details_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                seq,
                event_id,
                attempt_id,
                _json_dump(scope.to_dict()),
                event_type,
                state,
                outcome,
                producer,
                source_event_id,
                causation_id,
                correlation_id,
                occurred_at,
                _json_dump(details),
            ),
        )

    @staticmethod
    def _insert_outbox(
        connection: sqlite3.Connection,
        *,
        outbox_id: str,
        kind: OutboxKind,
        task_id: str,
        attempt_id: str,
        command_id: str,
        scope: ScopeRef,
        spec: FormalTaskSpec,
        now: str,
        executor_ref: str | None = None,
        adjustment: TaskAdjustmentRequest | None = None,
    ) -> None:
        if (kind is OutboxKind.ATTEMPT_ADJUST) != (adjustment is not None):
            raise FormalTaskViolation(
                "OUTBOX_ADJUSTMENT_BINDING_MISMATCH",
                "adjustment outbox kind and carrier must agree",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        payload: dict[str, object] = {
            "scope": scope.to_dict(),
            "spec": spec.to_dict(),
            "executor_ref": executor_ref,
        }
        if adjustment is not None:
            payload["adjustment"] = adjustment.to_dict()
        connection.execute(
            """
            INSERT INTO outbox(
                outbox_id, kind, task_id, attempt_id, command_id, payload_json,
                state, delivery_count, claimed_by, claimed_at, claim_token, last_error,
                created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, NULL, NULL, ?, ?)
            """,
            (
                outbox_id,
                kind.value,
                task_id,
                attempt_id,
                command_id,
                _json_dump(payload),
                OutboxState.PENDING.value,
                now,
                now,
            ),
        )

    def _require_admission_row(
        self,
        connection: sqlite3.Connection,
        *,
        outbox_id: str,
    ) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT o.*, a.adapter_id, a.capability_profile_json,
                   a.capability_profile_digest, a.execution_requirements_json,
                   a.admission_priority, a.admission_reason,
                   a.admission_attempt_count, a.admission_next_eligible_at,
                   a.admission_deadline_at, a.admission_enqueued_at,
                   a.state AS attempt_state,
                   a.outcome AS attempt_outcome,
                   a.executor_ref AS attempt_executor_ref,
                   a.source_seq AS attempt_source_seq,
                   t.state AS task_state,
                   t.outcome AS task_outcome,
                   t.attempt_id AS task_attempt_id
            FROM outbox AS o
            JOIN attempts AS a ON a.attempt_id=o.attempt_id
            JOIN tasks AS t ON t.task_id=o.task_id
            WHERE o.outbox_id=?
            """,
            (outbox_id,),
        ).fetchone()
        if row is None:
            raise FormalTaskViolation(
                "OUTBOX_BINDING_MISMATCH",
                "admission outbox lost its exact Task/Attempt binding",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        return row

    def _mark_admission_reconciliation_required(
        self,
        connection: sqlite3.Connection,
        *,
        task_id: str,
        observed_at: str,
    ) -> AdmissionDisposition:
        reason = "EXECUTOR_ADMISSION_OWNERSHIP_UNKNOWN_MANUAL_ACTION_REQUIRED"
        connection.execute(
            """
            UPDATE tasks SET reconciliation_state=?, reconciliation_reason=?,
                updated_at=?
            WHERE task_id=? AND state<>?
            """,
            (
                ReconciliationState.REQUIRED.value,
                reason,
                observed_at,
                task_id,
                FormalTaskState.TERMINAL.value,
            ),
        )
        connection.execute(
            """
            UPDATE outbox SET state=?, claimed_by=NULL, claimed_at=NULL,
                claim_token=NULL, last_error=?, updated_at=?
            WHERE task_id=? AND state IN (?, ?)
            """,
            (
                OutboxState.SUPPRESSED.value,
                reason,
                observed_at,
                task_id,
                OutboxState.PENDING.value,
                OutboxState.CLAIMED.value,
            ),
        )
        return AdmissionDisposition.RECONCILIATION_REQUIRED

    def _settle_admission_timeout(
        self,
        connection: sqlite3.Connection,
        *,
        outbox_id: str,
        observed_at: str,
    ) -> AdmissionDisposition:
        row = self._require_admission_row(connection, outbox_id=outbox_id)
        if (
            row["kind"] != OutboxKind.ATTEMPT_DISPATCH.value
            or row["adapter_id"] is None
            or row["task_attempt_id"] != row["attempt_id"]
            or row["task_state"] != FormalTaskState.ACCEPTED.value
            or row["attempt_state"] != FormalAttemptState.ACCEPTED.value
            or row["attempt_executor_ref"] is not None
            or int(row["attempt_source_seq"]) != -1
            or int(row["delivery_count"]) != int(row["admission_attempt_count"])
            or (
                int(row["admission_attempt_count"]) == 0
                and (row["admission_reason"] is not None or row["last_error"] is not None)
            )
            or (
                int(row["admission_attempt_count"]) > 0
                and (
                    row["admission_reason"]
                    not in {
                        "EXECUTOR_PROJECT_BUSY",
                        "EXECUTOR_CAPACITY_EXHAUSTED",
                    }
                    or row["last_error"] != row["admission_reason"]
                )
            )
        ):
            return self._mark_admission_reconciliation_required(
                connection,
                task_id=row["task_id"],
                observed_at=observed_at,
            )
        connection.execute(
            """
            UPDATE attempts SET state=?, outcome=?, updated_at=?
            WHERE attempt_id=? AND state=?
            """,
            (
                FormalAttemptState.TERMINAL.value,
                TerminalOutcome.FAILED.value,
                observed_at,
                row["attempt_id"],
                FormalAttemptState.ACCEPTED.value,
            ),
        )
        self._hit("admission.timeout.after_attempt")
        details = {"reason": "EXECUTOR_ADMISSION_TIMEOUT"}
        task = self._require_task_row_by_id(connection, row["task_id"])
        self._append_event(
            connection,
            task,
            event_type="attempt.terminal",
            state=FormalAttemptState.TERMINAL.value,
            outcome=TerminalOutcome.FAILED.value,
            producer="task_core.admission",
            source_event_id=None,
            causation_id=outbox_id,
            occurred_at=observed_at,
            details=details,
        )
        self._hit("admission.timeout.after_attempt_event")
        task = self._require_task_row_by_id(connection, row["task_id"])
        self._reject_open_adjustments_before_terminal(connection, task=task, observed_at=observed_at)
        task = self._require_task_row_by_id(connection, row["task_id"])
        self._append_event(
            connection,
            task,
            event_type="task.terminal",
            state=FormalTaskState.TERMINAL.value,
            outcome=TerminalOutcome.FAILED.value,
            producer="task_core.admission",
            source_event_id=None,
            causation_id=outbox_id,
            occurred_at=observed_at,
            details=details,
            update_task=True,
        )
        self._hit("admission.timeout.after_task_event")
        connection.execute(
            """
            UPDATE outbox SET state=?, claimed_by=NULL, claimed_at=NULL,
                claim_token=NULL, last_error=?, updated_at=?
            WHERE task_id=? AND state IN (?, ?)
            """,
            (
                OutboxState.SUPPRESSED.value,
                "EXECUTOR_ADMISSION_TIMEOUT",
                observed_at,
                row["task_id"],
                OutboxState.PENDING.value,
                OutboxState.CLAIMED.value,
            ),
        )
        self._hit("admission.timeout.after_outbox")
        return AdmissionDisposition.TIMED_OUT

    def claim_outbox(
        self,
        worker_id: str,
        *,
        observed_at: str | None = None,
        include_adjustments: bool = True,
    ) -> PersistentOutboxItem | None:
        if not worker_id.strip():
            raise ValueError("worker_id must be non-empty")
        now = observed_at or utc_now()
        _utc_datetime(now)
        claim_token = uuid.uuid4().hex
        with self._transaction() as connection:
            expired = connection.execute(
                """
                SELECT o.outbox_id, a.admission_deadline_at
                FROM outbox AS o
                JOIN attempts AS a ON a.attempt_id=o.attempt_id
                WHERE o.state=? AND o.kind=? AND a.adapter_id IS NOT NULL
                ORDER BY a.admission_deadline_at, o.outbox_id
                """,
                (
                    OutboxState.PENDING.value,
                    OutboxKind.ATTEMPT_DISPATCH.value,
                ),
            ).fetchall()
            now_value = _utc_datetime(now)
            for expired_row in expired:
                if now_value >= _utc_datetime(expired_row["admission_deadline_at"]):
                    self._settle_admission_timeout(
                        connection,
                        outbox_id=expired_row["outbox_id"],
                        observed_at=now,
                    )
            candidates = connection.execute(
                _OUTBOX_BINDING_SELECT
                + """
                  WHERE o.state=?
                    AND (
                      o.kind<>?
                      OR NOT EXISTS (
                        SELECT 1
                        FROM outbox AS prior
                        JOIN task_events AS prior_event
                          ON prior_event.task_id=prior.task_id
                         AND prior_event.attempt_id=prior.attempt_id
                         AND prior_event.event_type='task.adjust_requested'
                         AND prior_event.causation_id=prior.command_id
                        WHERE prior.task_id=o.task_id
                          AND prior.attempt_id=o.attempt_id
                          AND prior.kind=?
                          AND prior.state IN (?, ?)
                          AND prior_event.seq<ae.seq
                      )
                    )
                  ORDER BY
                    CASE WHEN o.kind<>? THEN 0 ELSE 1 END,
                    CASE WHEN o.kind<>? THEN o.updated_at END,
                    CASE
                      WHEN o.kind=? THEN
                        CASE a.admission_priority
                          WHEN 'urgent' THEN 0
                          WHEN 'high' THEN 1
                          WHEN 'normal' THEN 2
                          WHEN 'low' THEN 3
                          ELSE 2
                        END
                    END,
                    CASE WHEN o.kind=? THEN
                         CASE WHEN a.adapter_id IS NULL THEN o.updated_at
                              ELSE a.admission_enqueued_at END
                         ELSE o.created_at END,
                    o.created_at, o.outbox_id
                """,
                (
                    OutboxState.PENDING.value,
                    OutboxKind.ATTEMPT_ADJUST.value,
                    OutboxKind.ATTEMPT_ADJUST.value,
                    OutboxState.PENDING.value,
                    OutboxState.CLAIMED.value,
                    OutboxKind.ATTEMPT_DISPATCH.value,
                    OutboxKind.ATTEMPT_DISPATCH.value,
                    OutboxKind.ATTEMPT_DISPATCH.value,
                    OutboxKind.ATTEMPT_DISPATCH.value,
                ),
            )
            row = None
            for candidate in candidates:
                if not include_adjustments and candidate["kind"] == OutboxKind.ATTEMPT_ADJUST.value:
                    continue
                if (
                    candidate["canonical_attempt_id"] is None
                    or candidate["canonical_task_id"] is None
                    or candidate["attempt_task_id"] != candidate["task_id"]
                    or candidate["task_attempt_id"] != candidate["attempt_id"]
                ):
                    self._outbox_from_row(connection, candidate)
                    raise self._corrupt("formal Task outbox binding validation returned unexpectedly")
                if (
                    candidate["bound_task_state"] == FormalTaskState.TERMINAL.value
                    or candidate["bound_attempt_state"] == FormalAttemptState.TERMINAL.value
                ):
                    continue
                if (
                    candidate["kind"] == OutboxKind.ATTEMPT_DISPATCH.value
                    and candidate["bound_adapter_id"] is not None
                    and int(candidate["delivery_count"]) != int(candidate["bound_admission_attempt_count"])
                ):
                    self._mark_admission_reconciliation_required(
                        connection,
                        task_id=candidate["task_id"],
                        observed_at=now,
                    )
                    continue
                if (
                    candidate["kind"] == OutboxKind.ATTEMPT_DISPATCH.value
                    and candidate["bound_adapter_id"] is not None
                    and now_value < _utc_datetime(candidate["bound_admission_next_eligible_at"])
                ):
                    continue
                item = self._outbox_from_row(connection, candidate)
                if item.kind is OutboxKind.ATTEMPT_DISPATCH or item.executor_ref is not None:
                    row = candidate
                    break
            if row is None:
                return None
            changed = connection.execute(
                """
                UPDATE outbox
                SET state=?, delivery_count=delivery_count+1,
                    claimed_by=?, claimed_at=?, claim_token=?, updated_at=?
                WHERE outbox_id=? AND state=?
                """,
                (
                    OutboxState.CLAIMED.value,
                    worker_id,
                    now,
                    claim_token,
                    now,
                    row["outbox_id"],
                    OutboxState.PENDING.value,
                ),
            ).rowcount
            if changed != 1:
                return None
            claimed = connection.execute(
                _OUTBOX_BINDING_SELECT + " WHERE o.outbox_id=?",
                (row["outbox_id"],),
            ).fetchone()
            if claimed is None:
                raise FormalTaskViolation(
                    "TASK_STORE_CORRUPT",
                    "claimed formal Task outbox record vanished during reload",
                    ErrorCode.INTERNAL,
                )
            return self._outbox_from_row(connection, claimed)

    def renew_outbox_claim(
        self,
        item: PersistentOutboxItem,
        *,
        observed_at: str | None = None,
    ) -> None:
        """Renew only the exact live delivery; never reacquire or retarget it."""
        now = observed_at or utc_now()
        _utc_datetime(now)
        with self._transaction() as connection:
            row = connection.execute(
                _OUTBOX_BINDING_SELECT + " WHERE o.outbox_id=?",
                (item.outbox_id,),
            ).fetchone()
            if (
                row is None
                or row["state"] != OutboxState.CLAIMED.value
                or item.claim_token is None
                or row["claim_token"] != item.claim_token
                or row["task_attempt_id"] != item.attempt_id
                or row["bound_task_state"] == FormalTaskState.TERMINAL.value
                or row["bound_attempt_state"] == FormalAttemptState.TERMINAL.value
            ):
                raise FormalTaskViolation(
                    "OUTBOX_CLAIM_LOST",
                    "outbox claim is no longer live",
                    ErrorCode.CONFLICT,
                )
            stored = self._outbox_from_row(connection, row)
            if (
                stored.task_id != item.task_id
                or stored.attempt_id != item.attempt_id
                or stored.command_id != item.command_id
                or stored.kind != item.kind
                or stored.scope != item.scope
                or stored.selection != item.selection
                or stored.executor_ref != item.executor_ref
                or stored.adjustment != item.adjustment
                or _utc_datetime(row["claimed_at"]) > _utc_datetime(now)
            ):
                raise FormalTaskViolation(
                    "OUTBOX_BINDING_MISMATCH",
                    "outbox claim binding changed",
                    ErrorCode.CONFLICT,
                )
            connection.execute(
                "UPDATE outbox SET claimed_at=?, updated_at=? WHERE outbox_id=? AND state=? AND claim_token=?",
                (now, now, item.outbox_id, OutboxState.CLAIMED.value, item.claim_token),
            )

    def defer_admission(
        self,
        item: PersistentOutboxItem,
        *,
        reason: str,
        policy: AdmissionPolicy,
        observed_at: str,
    ) -> AdmissionDisposition:
        """Close one proven pre-effect capacity delivery without reallocating."""

        if reason not in {
            "EXECUTOR_PROJECT_BUSY",
            "EXECUTOR_CAPACITY_EXHAUSTED",
        }:
            raise FormalTaskViolation(
                "INVALID_ADMISSION_REASON",
                "admission defer reason is not a closed pre-effect outcome",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if not isinstance(policy, AdmissionPolicy):
            raise FormalTaskViolation(
                "INVALID_ADMISSION_POLICY",
                "admission deferral requires a canonical runtime policy",
                ErrorCode.INVALID_ARGUMENT,
            )
        observed = _utc_datetime(observed_at)
        if (
            not isinstance(item, PersistentOutboxItem)
            or item.kind is not OutboxKind.ATTEMPT_DISPATCH
            or item.selection is None
            or item.claim_token is None
        ):
            raise FormalTaskViolation(
                "ADMISSION_CLAIM_REQUIRED",
                "admission deferral requires the exact selected dispatch claim",
                ErrorCode.CONFLICT,
            )
        with self._transaction() as connection:
            row = self._require_admission_row(connection, outbox_id=item.outbox_id)
            stored_selection = _selection_from_attempt_row(row)
            if (
                row["state"] != OutboxState.CLAIMED.value
                or row["claim_token"] != item.claim_token
                or row["task_id"] != item.task_id
                or row["attempt_id"] != item.attempt_id
                or row["task_attempt_id"] != item.attempt_id
                or stored_selection != item.selection
            ):
                raise FormalTaskViolation(
                    "OUTBOX_CLAIM_LOST",
                    "admission deferral no longer owns the exact Store claim",
                    ErrorCode.CONFLICT,
                )
            current_count = int(row["admission_attempt_count"])
            next_count = current_count + 1
            if int(row["delivery_count"]) != next_count:
                return self._mark_admission_reconciliation_required(
                    connection,
                    task_id=item.task_id,
                    observed_at=observed_at,
                )
            deadline = _utc_datetime(row["admission_deadline_at"])
            if observed >= deadline or next_count >= policy.max_attempts:
                connection.execute(
                    """
                    UPDATE attempts SET admission_reason=?,
                        admission_attempt_count=?, updated_at=?
                    WHERE attempt_id=?
                    """,
                    (reason, next_count, observed_at, item.attempt_id),
                )
                changed = connection.execute(
                    """
                    UPDATE outbox SET last_error=?, updated_at=?
                    WHERE outbox_id=? AND state=? AND claim_token=?
                    """,
                    (
                        reason,
                        observed_at,
                        item.outbox_id,
                        OutboxState.CLAIMED.value,
                        item.claim_token,
                    ),
                ).rowcount
                if changed != 1:
                    raise FormalTaskViolation(
                        "OUTBOX_CLAIM_LOST",
                        "admission Store claim changed before timeout proof",
                        ErrorCode.CONFLICT,
                    )
                return self._settle_admission_timeout(
                    connection,
                    outbox_id=item.outbox_id,
                    observed_at=observed_at,
                )
            exponent = next_count - 1
            saturation_exponent = math.ceil(
                math.log2(policy.max_backoff_seconds) - math.log2(policy.initial_backoff_seconds)
            )
            candidate = (
                policy.max_backoff_seconds
                if exponent >= saturation_exponent
                else math.ldexp(policy.initial_backoff_seconds, exponent)
            )
            delay = min(policy.max_backoff_seconds, candidate)
            next_eligible_at = _utc_plus_seconds(observed_at, delay)
            changed = connection.execute(
                """
                UPDATE attempts SET admission_reason=?,
                    admission_attempt_count=?, admission_next_eligible_at=?,
                    updated_at=?
                WHERE attempt_id=? AND state=?
                """,
                (
                    reason,
                    next_count,
                    next_eligible_at,
                    observed_at,
                    item.attempt_id,
                    FormalAttemptState.ACCEPTED.value,
                ),
            ).rowcount
            if changed != 1:
                raise FormalTaskViolation(
                    "TASK_ATTEMPT_STALE",
                    "admission deferral targets a non-accepted Attempt",
                    ErrorCode.STALE,
                )
            changed = connection.execute(
                """
                UPDATE outbox SET state=?, claimed_by=NULL, claimed_at=NULL,
                    claim_token=NULL, last_error=?, updated_at=?
                WHERE outbox_id=? AND state=? AND claim_token=?
                """,
                (
                    OutboxState.PENDING.value,
                    reason,
                    observed_at,
                    item.outbox_id,
                    OutboxState.CLAIMED.value,
                    item.claim_token,
                ),
            ).rowcount
            if changed != 1:
                raise FormalTaskViolation(
                    "OUTBOX_CLAIM_LOST",
                    "admission Store claim changed before defer commit",
                    ErrorCode.CONFLICT,
                )
            task = self._require_task_row_by_id(connection, item.task_id)
            if bool(task["cancel_requested"]):
                cancel = connection.execute(
                    "SELECT * FROM outbox WHERE task_id=? AND attempt_id=? AND kind=?",
                    (item.task_id, item.attempt_id, OutboxKind.ATTEMPT_CANCEL.value),
                ).fetchone()
                if cancel is None:
                    raise self._corrupt("cancelled admission has no cancel delivery")
                dispatch = connection.execute("SELECT * FROM outbox WHERE outbox_id=?", (item.outbox_id,)).fetchone()
                settlement = self._settle_cancel_before_dispatch(
                    connection,
                    task=task,
                    dispatch=dispatch,
                    command_id=cancel["command_id"],
                    scope=item.scope,
                    now=observed_at,
                )
                self._settle_cancel_command_results(
                    connection,
                    task_id=item.task_id,
                    scope_key=task["scope_key"],
                    settlement=settlement,
                )
                connection.execute(
                    "UPDATE outbox SET state=?, updated_at=? WHERE outbox_id=?",
                    (OutboxState.SUPPRESSED.value, observed_at, cancel["outbox_id"]),
                )
            return AdmissionDisposition.DEFERRED

    def release_outbox(self, item: PersistentOutboxItem, error: str) -> bool:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM outbox WHERE outbox_id=?",
                (item.outbox_id,),
            ).fetchone()
            if (
                row is None
                or row["task_id"] != item.task_id
                or row["attempt_id"] != item.attempt_id
                or row["kind"] != item.kind.value
            ):
                raise FormalTaskViolation(
                    "OUTBOX_BINDING_MISMATCH",
                    "released outbox identity does not match its stored delivery",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
            task = self._require_task_row_by_id(connection, item.task_id)
            if task["attempt_id"] != item.attempt_id:
                raise FormalTaskViolation(
                    "TASK_ATTEMPT_STALE",
                    "outbox release targets an old task attempt",
                    ErrorCode.STALE,
                )
            attempt = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=?",
                (item.attempt_id,),
            ).fetchone()
            if attempt is None:
                raise self._corrupt("released outbox lost its durable attempt binding")
            terminal = (
                task["state"] == FormalTaskState.TERMINAL.value or attempt["state"] == FormalAttemptState.TERMINAL.value
            )
            selection = _stored_record("executor selection", lambda: _selection_from_attempt_row(attempt))
            if item.selection != selection:
                raise FormalTaskViolation(
                    "EXECUTOR_SELECTION_MISMATCH",
                    "released outbox does not match its persisted selection",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
            if not terminal and item.kind is OutboxKind.ATTEMPT_DISPATCH and selection is not None:
                if (
                    row["state"] != OutboxState.CLAIMED.value
                    or item.claim_token is None
                    or row["claim_token"] != item.claim_token
                ):
                    return False
                self._mark_admission_reconciliation_required(
                    connection,
                    task_id=item.task_id,
                    observed_at=utc_now(),
                )
                return True
            return (
                connection.execute(
                    """
                UPDATE outbox SET state=?, claimed_by=NULL, claimed_at=NULL,
                    claim_token=NULL, last_error=?, updated_at=?
                WHERE outbox_id=? AND state=? AND claim_token=?
                """,
                    (
                        (OutboxState.SUPPRESSED.value if terminal else OutboxState.PENDING.value),
                        ("TASK_TERMINAL_BEFORE_DELIVERY" if terminal else error[:1000]),
                        utc_now(),
                        item.outbox_id,
                        OutboxState.CLAIMED.value,
                        item.claim_token,
                    ),
                ).rowcount
                == 1
            )

    def reject_outbox(self, item: PersistentOutboxItem, error: FormalTaskViolation) -> None:
        """Terminally reject a non-retriable delivery without inventing an Executor fact."""

        now = utc_now()
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM outbox WHERE outbox_id=?", (item.outbox_id,)).fetchone()
            if (
                row is None
                or row["state"] != OutboxState.CLAIMED.value
                or item.claim_token is None
                or row["claim_token"] != item.claim_token
            ):
                raise FormalTaskViolation(
                    "OUTBOX_CLAIM_LOST",
                    "only the claimed outbox item can be rejected",
                    ErrorCode.CONFLICT,
                )
            if row["task_id"] != item.task_id or row["attempt_id"] != item.attempt_id or row["kind"] != item.kind.value:
                raise FormalTaskViolation(
                    "OUTBOX_BINDING_MISMATCH",
                    "claimed outbox identity does not match its stored delivery",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
            task = self._require_task_row_by_id(connection, item.task_id)
            if task["attempt_id"] != item.attempt_id:
                raise FormalTaskViolation(
                    "TASK_ATTEMPT_STALE",
                    "outbox rejection targets an old task attempt",
                    ErrorCode.STALE,
                )
            attempt = connection.execute("SELECT * FROM attempts WHERE attempt_id=?", (item.attempt_id,)).fetchone()
            if attempt is None or attempt["task_id"] != item.task_id:
                raise FormalTaskViolation(
                    "ATTEMPT_SCOPE_MISMATCH",
                    "rejected delivery does not belong to the formal task",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
            terminal_dispatch_rejection = (
                item.kind is OutboxKind.ATTEMPT_DISPATCH and task["state"] != FormalTaskState.TERMINAL.value
            )
            if terminal_dispatch_rejection:
                connection.execute(
                    """
                    UPDATE attempts SET state=?, outcome=?, updated_at=?
                    WHERE attempt_id=?
                    """,
                    (
                        FormalAttemptState.TERMINAL.value,
                        TerminalOutcome.FAILED.value,
                        now,
                        item.attempt_id,
                    ),
                )
                details = {"reason": error.reason, "error": str(error)}
                self._append_event(
                    connection,
                    task,
                    event_type="attempt.terminal",
                    state=FormalAttemptState.TERMINAL.value,
                    outcome=TerminalOutcome.FAILED.value,
                    producer="task_core.delivery",
                    source_event_id=None,
                    causation_id=item.outbox_id,
                    occurred_at=now,
                    details=details,
                )
                self._reject_open_adjustments_before_terminal(
                    connection,
                    task=self._require_task_row_by_id(connection, item.task_id),
                    observed_at=now,
                )
                self._append_event(
                    connection,
                    self._require_task_row_by_id(connection, item.task_id),
                    event_type="task.terminal",
                    state=FormalTaskState.TERMINAL.value,
                    outcome=TerminalOutcome.FAILED.value,
                    producer="task_core.delivery",
                    source_event_id=None,
                    causation_id=item.outbox_id,
                    occurred_at=now,
                    details=details,
                    update_task=True,
                )
            elif task["state"] != FormalTaskState.TERMINAL.value:
                connection.execute(
                    """
                    UPDATE tasks SET reconciliation_state=?, reconciliation_reason=?,
                        updated_at=? WHERE task_id=?
                    """,
                    (
                        ReconciliationState.PENDING.value,
                        f"{error.reason}: {error}"[:1000],
                        now,
                        item.task_id,
                    ),
                )
            connection.execute(
                """
                UPDATE outbox SET state=?, claimed_by=NULL, claimed_at=NULL,
                    claim_token=NULL, last_error=?, updated_at=? WHERE outbox_id=?
                """,
                (
                    OutboxState.SUPPRESSED.value,
                    f"{error.reason}: {error}"[:1000],
                    now,
                    item.outbox_id,
                ),
            )
            if terminal_dispatch_rejection:
                connection.execute(
                    """
                    UPDATE outbox SET state=?, last_error=?, updated_at=?
                    WHERE task_id=? AND state=? AND outbox_id<>?
                    """,
                    (
                        OutboxState.SUPPRESSED.value,
                        "TASK_TERMINAL_BEFORE_DELIVERY",
                        now,
                        item.task_id,
                        OutboxState.PENDING.value,
                        item.outbox_id,
                    ),
                )

    def complete_outbox(
        self,
        item: PersistentOutboxItem,
        *,
        executor_ref: str,
        observations: tuple[ExecutorObservation, ...],
    ) -> None:
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM outbox WHERE outbox_id=?", (item.outbox_id,)).fetchone()
            if (
                row is None
                or row["state"] != OutboxState.CLAIMED.value
                or item.claim_token is None
                or row["claim_token"] != item.claim_token
            ):
                raise FormalTaskViolation(
                    "OUTBOX_CLAIM_LOST",
                    "claimed outbox item is no longer deliverable",
                    ErrorCode.CONFLICT,
                )
            if row["task_id"] != item.task_id or row["attempt_id"] != item.attempt_id or row["kind"] != item.kind.value:
                raise FormalTaskViolation(
                    "OUTBOX_BINDING_MISMATCH",
                    "claimed outbox identity does not match its stored delivery",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
            attempt = connection.execute("SELECT * FROM attempts WHERE attempt_id=?", (item.attempt_id,)).fetchone()
            if attempt is None or attempt["executor_id"] != item.spec.executor_id:
                raise FormalTaskViolation(
                    "EXECUTOR_BINDING_MISMATCH",
                    "outbox executor does not match the stored attempt",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
            selection = _stored_record("executor selection", lambda: _selection_from_attempt_row(attempt))
            for observation in observations:
                expected_binding = (
                    (None, None)
                    if selection is None
                    else (
                        selection.adapter_id,
                        selection.capability_profile_digest,
                    )
                )
                if (
                    observation.adapter_id,
                    observation.capability_profile_digest,
                ) != expected_binding:
                    raise FormalTaskViolation(
                        "EXECUTOR_SELECTION_MISMATCH",
                        "Executor callback does not match the persisted selection",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
            task = self._require_task_row_by_id(connection, item.task_id)
            if task["attempt_id"] != item.attempt_id:
                raise FormalTaskViolation(
                    "TASK_ATTEMPT_STALE",
                    "outbox completion targets an old task attempt",
                    ErrorCode.STALE,
                )
            if attempt["executor_ref"] not in {None, executor_ref}:
                raise FormalTaskViolation(
                    "EXECUTOR_REFERENCE_CONFLICT",
                    "attempt cannot change its executor reference",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
            now = utc_now()
            if item.kind is OutboxKind.ATTEMPT_DISPATCH:
                pending_controls = connection.execute(
                    _OUTBOX_BINDING_SELECT + " WHERE o.attempt_id=? AND o.kind IN (?, ?) AND o.state=?",
                    (
                        item.attempt_id,
                        OutboxKind.ATTEMPT_CANCEL.value,
                        OutboxKind.ATTEMPT_ADJUST.value,
                        OutboxState.PENDING.value,
                    ),
                ).fetchall()
                for control_row in pending_controls:
                    control_item = self._outbox_from_row(connection, control_row)
                    payload = {
                        "scope": control_item.scope.to_dict(),
                        "spec": control_item.spec.to_dict(),
                        "executor_ref": executor_ref,
                    }
                    if control_item.adjustment is not None:
                        payload["adjustment"] = control_item.adjustment.to_dict()
                    changed = connection.execute(
                        """
                        UPDATE outbox SET payload_json=?, updated_at=?
                        WHERE outbox_id=? AND state=?
                        """,
                        (
                            _json_dump(payload),
                            now,
                            control_item.outbox_id,
                            OutboxState.PENDING.value,
                        ),
                    ).rowcount
                    if changed != 1:
                        raise FormalTaskViolation(
                            "TASK_STORE_CORRUPT",
                            "pending control outbox changed during Executor binding",
                            ErrorCode.INTERNAL,
                        )
            connection.execute(
                "UPDATE attempts SET executor_ref=?, updated_at=? WHERE attempt_id=?",
                (executor_ref, now, item.attempt_id),
            )
            for observation in observations:
                self._apply_observation(connection, observation)
            connection.execute(
                """
                UPDATE outbox SET state=?, claimed_by=NULL, claimed_at=NULL,
                    claim_token=NULL, last_error=NULL, updated_at=? WHERE outbox_id=?
                """,
                (OutboxState.DELIVERED.value, now, item.outbox_id),
            )
            if any(observation.attempt_state is FormalAttemptState.TERMINAL for observation in observations):
                self._hit("executor_terminal.after_outbox_settlement")

    @classmethod
    def _write_adjustment_command_result(
        cls,
        connection: sqlite3.Connection,
        *,
        scope_key: str,
        command_id: str,
        state: TaskAdjustmentState,
        reason: str | None,
        observed_at: str,
    ) -> None:
        row = connection.execute(
            "SELECT * FROM commands WHERE scope_key=? AND command_id=?",
            (scope_key, command_id),
        ).fetchone()
        if row is None:
            raise FormalTaskViolation(
                "TASK_STORE_CORRUPT",
                "task adjustment command ledger is unavailable",
                ErrorCode.INTERNAL,
            )
        try:
            command, stored = cls._control_command_from_row(row)
            result = stored.result
            if (
                not stored.ok
                or stored.command_id != command_id
                or type(result) is not dict
                or set(result)
                != {
                    "task_id",
                    "attempt_id",
                    "adjustment_id",
                    "adjustment_state",
                    "reason",
                    "outbox_id",
                }
                or result["adjustment_id"] != command_id
                or result["adjustment_state"]
                not in {
                    TaskAdjustmentState.PENDING.value,
                    state.value,
                }
            ):
                raise ValueError("non-canonical adjustment result")
            admission = connection.execute(
                """SELECT event_id FROM task_events
                   WHERE task_id=? AND event_type='task.adjust_requested'
                     AND causation_id=?""",
                (result["task_id"], command_id),
            ).fetchone()
            settlement = connection.execute(
                """SELECT event_id FROM task_events
                   WHERE task_id=? AND event_type IN (
                     'task.adjust_applied', 'task.adjust_rejected'
                   ) AND causation_id=?""",
                (result["task_id"], command_id),
            ).fetchone()
            if admission is None or settlement is None:
                raise ValueError("adjustment result lacks event authority")
            if state is TaskAdjustmentState.APPLIED:
                result["adjustment_state"] = state.value
                result["reason"] = reason
                final = ResultEnvelope.success(
                    owner=command,
                    result=result,
                    observed_at=observed_at,
                    extensions=command_result_extensions(
                        TaskCommandDisposition.APPLIED,
                        admission_event_id=admission["event_id"],
                        settlement_event_id=settlement["event_id"],
                    ),
                )
            else:
                disposition = (
                    TaskCommandDisposition.CONFLICT
                    if reason == "TASK_TERMINAL_BEFORE_ADJUSTMENT"
                    else TaskCommandDisposition.REJECTED
                )
                code = (
                    ErrorCode.CONFLICT if disposition is TaskCommandDisposition.CONFLICT else ErrorCode.INVALID_ARGUMENT
                )
                final = ResultEnvelope.failure(
                    owner=command,
                    error=ContractViolation(
                        code,
                        reason or "TASK_ADJUSTMENT_REJECTED",
                        "task adjustment was definitively rejected",
                    ).error,
                    observed_at=observed_at,
                    extensions=command_result_extensions(
                        disposition,
                        admission_event_id=admission["event_id"],
                        settlement_event_id=settlement["event_id"],
                    ),
                )
        except (ContractViolation, KeyError, TypeError, ValueError) as error:
            raise FormalTaskViolation(
                "TASK_STORE_CORRUPT",
                "task adjustment command result is not canonical",
                ErrorCode.INTERNAL,
            ) from error
        connection.execute(
            "UPDATE commands SET result_json=? WHERE scope_key=? AND command_id=?",
            (_json_dump(final.to_dict()), scope_key, command_id),
        )

    @classmethod
    def _settle_cancel_command_results(
        cls,
        connection: sqlite3.Connection,
        *,
        task_id: str,
        scope_key: str,
        settlement: PersistentTaskEvent,
    ) -> None:
        """Promote accepted cancel requests only on cancelled terminal truth."""

        if settlement.event_type != "task.terminal" or settlement.outcome != TerminalOutcome.CANCELLED.value:
            return
        rows = connection.execute(
            """SELECT * FROM commands
               WHERE scope_key=? AND command_type='task.cancel'""",
            (scope_key,),
        ).fetchall()
        for row in rows:
            try:
                result = ResultEnvelope.from_dict(_json_load(row["result_json"]))
            except ContractViolation as error:
                raise cls._corrupt("cancel settlement command result is not canonical") from error
            if not result.ok:
                cls._verify_business_decision(
                    connection,
                    row,
                    expected=_CANCEL_BUSINESS_DECISIONS,
                )
                continue
            command, stored = cls._control_command_from_row(row)
            value = stored.result
            if command.target_ref.id != task_id or type(value) is not dict or value.get("applied") is not False:
                continue
            request = connection.execute(
                """SELECT event_id FROM task_events
                   WHERE task_id=? AND event_type='task.cancel_requested'
                     AND causation_id=?""",
                (task_id, command.command_id),
            ).fetchone()
            value["applied"] = True
            value["state"] = FormalTaskState.TERMINAL.value
            payload = stored.to_dict()
            payload["result"] = value
            payload["observed_at"] = settlement.occurred_at
            payload["extensions"] = command_result_extensions(
                TaskCommandDisposition.APPLIED,
                admission_event_id=(None if request is None else request["event_id"]),
                settlement_event_id=settlement.event_id,
            )
            try:
                final = ResultEnvelope.from_dict(payload)
            except ContractViolation as error:
                raise cls._corrupt("cancel settlement command result is not canonical") from error
            connection.execute(
                "UPDATE commands SET result_json=? WHERE scope_key=? AND command_id=?",
                (_json_dump(final.to_dict()), scope_key, command.command_id),
            )

    def _finalize_adjustment(
        self,
        connection: sqlite3.Connection,
        *,
        outbox: sqlite3.Row,
        task: sqlite3.Row,
        state: TaskAdjustmentState,
        reason: str | None,
        observed_at: str,
    ) -> None:
        event_type = "task.adjust_applied" if state is TaskAdjustmentState.APPLIED else "task.adjust_rejected"
        details: dict[str, object] = {"command_id": outbox["command_id"]}
        if reason is not None:
            details["reason"] = reason
        self._append_event(
            connection,
            task,
            event_type=event_type,
            state=task["state"],
            outcome=None,
            producer="task_core.control",
            source_event_id=None,
            causation_id=outbox["command_id"],
            occurred_at=observed_at,
            details=details,
        )
        self._write_adjustment_command_result(
            connection,
            scope_key=task["scope_key"],
            command_id=outbox["command_id"],
            state=state,
            reason=reason,
            observed_at=observed_at,
        )

    def _reject_open_adjustments_before_terminal(
        self,
        connection: sqlite3.Connection,
        *,
        task: sqlite3.Row,
        observed_at: str,
    ) -> None:
        rows = connection.execute(
            """
            SELECT o.*, e.seq AS requested_seq
            FROM outbox AS o
            JOIN task_events AS e
              ON e.task_id=o.task_id
             AND e.attempt_id=o.attempt_id
             AND e.event_type='task.adjust_requested'
             AND e.causation_id=o.command_id
            WHERE o.task_id=? AND o.attempt_id=? AND o.kind=?
              AND o.state IN (?, ?)
            ORDER BY e.seq
            """,
            (
                task["task_id"],
                task["attempt_id"],
                OutboxKind.ATTEMPT_ADJUST.value,
                OutboxState.PENDING.value,
                OutboxState.CLAIMED.value,
            ),
        ).fetchall()
        current = task
        for row in rows:
            self._finalize_adjustment(
                connection,
                outbox=row,
                task=current,
                state=TaskAdjustmentState.REJECTED,
                reason="TASK_TERMINAL_BEFORE_ADJUSTMENT",
                observed_at=observed_at,
            )
            connection.execute(
                """
                UPDATE outbox SET state=?, claimed_by=NULL, claimed_at=NULL,
                    claim_token=NULL, last_error=?, updated_at=? WHERE outbox_id=?
                """,
                (
                    OutboxState.SUPPRESSED.value,
                    "TASK_TERMINAL_BEFORE_ADJUSTMENT",
                    observed_at,
                    row["outbox_id"],
                ),
            )
            current = self._require_task_row_by_id(connection, task["task_id"])

    def complete_adjustment_outbox(
        self,
        item: PersistentOutboxItem,
        delivery: TaskAdjustmentDeliveryResult,
        *,
        observed_at: str | None = None,
    ) -> TaskAdjustmentSettlement:
        """Commit the checkpoint result before the Executor terminal fence opens."""

        if item.kind is not OutboxKind.ATTEMPT_ADJUST or item.adjustment is None:
            raise FormalTaskViolation(
                "OUTBOX_ADJUSTMENT_BINDING_MISMATCH",
                "adjustment completion requires an adjustment outbox item",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        now = observed_at or utc_now()
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM outbox WHERE outbox_id=?", (item.outbox_id,)).fetchone()
            if (
                row is None
                or row["state"] != OutboxState.CLAIMED.value
                or item.claim_token is None
                or row["claim_token"] != item.claim_token
            ):
                raise FormalTaskViolation(
                    "OUTBOX_CLAIM_LOST",
                    "claimed adjustment outbox is no longer deliverable",
                    ErrorCode.CONFLICT,
                )
            if (
                row["kind"] != OutboxKind.ATTEMPT_ADJUST.value
                or row["task_id"] != item.task_id
                or row["attempt_id"] != item.attempt_id
                or row["command_id"] != item.command_id
                or delivery.adjustment_id != item.command_id
            ):
                raise FormalTaskViolation(
                    "OUTBOX_ADJUSTMENT_BINDING_MISMATCH",
                    "adjustment result does not bind the claimed delivery",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
            task = self._require_task_row_by_id(connection, item.task_id)
            attempt = connection.execute("SELECT * FROM attempts WHERE attempt_id=?", (item.attempt_id,)).fetchone()
            current_selection = (
                None
                if attempt is None
                else _stored_record(
                    "executor selection",
                    lambda: _selection_from_attempt_row(attempt),
                )
            )
            if current_selection != item.selection:
                raise FormalTaskViolation(
                    "EXECUTOR_SELECTION_MISMATCH",
                    "adjustment result does not match the current Attempt selection",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
            if (
                task["attempt_id"] != item.attempt_id
                or task["state"] == FormalTaskState.TERMINAL.value
                or attempt is None
                or attempt["task_id"] != item.task_id
                or attempt["state"] == FormalAttemptState.TERMINAL.value
                or attempt["executor_ref"] != delivery.executor_ref
                or item.executor_ref != delivery.executor_ref
            ):
                raise FormalTaskViolation(
                    "TASK_ADJUSTMENT_STALE",
                    "adjustment result no longer binds a live current attempt",
                    ErrorCode.STALE,
                )
            reason = delivery.reason
            self._finalize_adjustment(
                connection,
                outbox=row,
                task=task,
                state=delivery.state,
                reason=reason,
                observed_at=now,
            )
            connection.execute(
                """
                UPDATE outbox SET state=?, claimed_by=NULL, claimed_at=NULL,
                    claim_token=NULL, last_error=?, updated_at=? WHERE outbox_id=?
                """,
                (
                    OutboxState.DELIVERED.value,
                    reason,
                    now,
                    item.outbox_id,
                ),
            )
            has_more = (
                connection.execute(
                    """
                SELECT 1 FROM outbox
                WHERE task_id=? AND attempt_id=? AND kind=?
                  AND state IN (?, ?) AND outbox_id<>?
                LIMIT 1
                """,
                    (
                        item.task_id,
                        item.attempt_id,
                        OutboxKind.ATTEMPT_ADJUST.value,
                        OutboxState.PENDING.value,
                        OutboxState.CLAIMED.value,
                        item.outbox_id,
                    ),
                ).fetchone()
                is not None
            )
            return TaskAdjustmentSettlement(delivery.state, has_more)

    def apply_observations(self, observations: tuple[ExecutorObservation, ...]) -> TaskMutationResult:
        if not observations:
            raise FormalTaskViolation(
                "EXECUTOR_OBSERVATIONS_REQUIRED",
                "observation mutation requires at least one Executor fact",
                ErrorCode.INVALID_ARGUMENT,
            )
        with self._transaction() as connection:
            first = observations[0]
            for observation in observations:
                if (
                    observation.resolution is not ExecutorResolution.KNOWN
                    or observation.source_event_id is None
                    or observation.source_seq is None
                    or observation.attempt_state is None
                ):
                    raise FormalTaskViolation(
                        "EXECUTOR_EVENT_INCOMPLETE",
                        "known Executor mutation requires complete lifecycle facts",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
                existing = connection.execute(
                    "SELECT canonical FROM executor_events WHERE source_event_id=?",
                    (observation.source_event_id,),
                ).fetchone()
                if existing is not None and existing["canonical"] != canonical_json_bytes(observation.canonical_dict()):
                    raise FormalTaskViolation(
                        "EXECUTOR_EVENT_ID_CONFLICT",
                        "Executor event identity was reused with different facts",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
            if any(
                observation.task_id != first.task_id
                or observation.attempt_id != first.attempt_id
                or observation.executor_id != first.executor_id
                or observation.executor_ref != first.executor_ref
                for observation in observations
            ):
                raise FormalTaskViolation(
                    "EXECUTOR_OBSERVATION_BINDING_MISMATCH",
                    "one observation mutation must bind one exact attempt",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
            attempt = connection.execute("SELECT * FROM attempts WHERE attempt_id=?", (first.attempt_id,)).fetchone()
            selection = (
                None
                if attempt is None
                else _stored_record("executor selection", lambda: _selection_from_attempt_row(attempt))
            )
            expected_selection_binding = (
                (None, None)
                if selection is None
                else (
                    selection.adapter_id,
                    selection.capability_profile_digest,
                )
            )
            if any(
                (
                    observation.adapter_id,
                    observation.capability_profile_digest,
                )
                != expected_selection_binding
                for observation in observations
            ):
                raise FormalTaskViolation(
                    "EXECUTOR_SELECTION_MISMATCH",
                    "Executor callback does not match the persisted selection",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
            if (
                attempt is None
                or attempt["task_id"] != first.task_id
                or attempt["executor_id"] != first.executor_id
                or attempt["executor_ref"] != first.executor_ref
            ):
                raise FormalTaskViolation(
                    "EXECUTOR_OBSERVATION_BINDING_MISMATCH",
                    "Executor observation does not bind the exact attempt",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
            task = self._require_task_row_by_id(connection, first.task_id)
            if task["attempt_id"] != first.attempt_id:
                return self._mutation_result(
                    connection,
                    attempt,
                    TaskMutationDisposition.SUPERSEDED,
                )
            appended: list[PersistentTaskEvent] = []
            for observation in observations:
                appended.extend(self._apply_observation(connection, observation))
            frozen_attempt = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=?", (first.attempt_id,)
            ).fetchone()
            assert frozen_attempt is not None
            return self._mutation_result(
                connection,
                frozen_attempt,
                (TaskMutationDisposition.APPLIED if appended else TaskMutationDisposition.NOOP),
                events=tuple(appended),
            )

    def _apply_observation(
        self, connection: sqlite3.Connection, observation: ExecutorObservation
    ) -> tuple[PersistentTaskEvent, ...]:
        if observation.result_text is not None and "\x00" in observation.result_text:
            raise FormalTaskViolation(
                "INVALID_TASK_RESULT_TEXT",
                "Executor result text contains a forbidden NUL character",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if observation.resolution is not ExecutorResolution.KNOWN:
            raise FormalTaskViolation(
                "EXECUTOR_FACT_NOT_KNOWN",
                "only known Executor observations can change lifecycle state",
                ErrorCode.INVALID_ARGUMENT,
            )
        if observation.source_event_id is None or observation.source_seq is None or observation.attempt_state is None:
            raise FormalTaskViolation(
                "EXECUTOR_EVENT_INCOMPLETE",
                "known Executor observation lacks event identity or state",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        attempt = connection.execute("SELECT * FROM attempts WHERE attempt_id=?", (observation.attempt_id,)).fetchone()
        if (
            attempt is None
            or attempt["task_id"] != observation.task_id
            or attempt["executor_id"] != observation.executor_id
            or attempt["executor_ref"] != observation.executor_ref
        ):
            raise FormalTaskViolation(
                "EXECUTOR_OBSERVATION_BINDING_MISMATCH",
                "Executor observation does not bind the exact attempt",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        canonical = canonical_json_bytes(observation.canonical_dict())
        existing = connection.execute(
            "SELECT canonical FROM executor_events WHERE source_event_id=?",
            (observation.source_event_id,),
        ).fetchone()
        if existing is not None:
            if existing["canonical"] != canonical:
                raise FormalTaskViolation(
                    "EXECUTOR_EVENT_ID_CONFLICT",
                    "Executor event identity was reused with different facts",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
            return ()
        task = self._require_task_row_by_id(connection, observation.task_id)
        if task["attempt_id"] != observation.attempt_id:
            raise FormalTaskViolation(
                "TASK_ATTEMPT_STALE",
                "Executor observation targets an old task attempt",
                ErrorCode.STALE,
            )
        expected_source_seq = int(attempt["source_seq"]) + 1
        if observation.source_seq != expected_source_seq:
            raise FormalTaskViolation(
                "EXECUTOR_EVENT_SEQUENCE_GAP",
                f"expected Executor sequence {expected_source_seq}",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        connection.execute(
            """
            INSERT INTO executor_events(source_event_id, attempt_id, source_seq, canonical)
            VALUES(?, ?, ?, ?)
            """,
            (
                observation.source_event_id,
                observation.attempt_id,
                observation.source_seq,
                canonical,
            ),
        )
        if observation.attempt_state is FormalAttemptState.TERMINAL:
            self._hit("executor_terminal.after_source_fact")
        current = FormalAttemptState(attempt["state"])
        target = observation.attempt_state
        if target is current:
            if target is not FormalAttemptState.ACCEPTED:
                raise FormalTaskViolation(
                    "EXECUTOR_TRANSITION_REPEATED",
                    "non-initial Executor lifecycle state cannot be re-emitted",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
        elif (current is FormalAttemptState.ACCEPTED and target is FormalAttemptState.RUNNING) or (
            current is FormalAttemptState.RUNNING and target is FormalAttemptState.TERMINAL
        ):
            pass
        else:
            raise FormalTaskViolation(
                "INVALID_EXECUTOR_TRANSITION",
                f"Executor attempt cannot transition {current.value} -> {target.value}",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if target is FormalAttemptState.TERMINAL:
            if observation.attempt_outcome is None:
                raise FormalTaskViolation(
                    "TERMINAL_OUTCOME_REQUIRED",
                    "terminal Executor observation requires an outcome",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
        elif observation.attempt_outcome is not None:
            raise FormalTaskViolation(
                "NONTERMINAL_OUTCOME_FORBIDDEN",
                "nonterminal Executor observation cannot carry an outcome",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        connection.execute(
            """
            UPDATE attempts SET state=?, outcome=?, source_seq=?, updated_at=?
            WHERE attempt_id=?
            """,
            (
                target.value,
                (None if observation.attempt_outcome is None else observation.attempt_outcome.value),
                observation.source_seq,
                observation.occurred_at,
                observation.attempt_id,
            ),
        )
        if target is FormalAttemptState.TERMINAL:
            self._hit("executor_terminal.after_attempt")
        task = self._require_task_row_by_id(connection, observation.task_id)
        details = {
            "raw_status": observation.raw_status,
            "summary": observation.summary,
            "error": observation.error,
        }
        appended = [
            self._append_event(
                connection,
                task,
                event_type=f"attempt.{target.value}",
                state=target.value,
                outcome=(None if observation.attempt_outcome is None else observation.attempt_outcome.value),
                producer=observation.executor_id,
                source_event_id=observation.source_event_id,
                causation_id=observation.source_event_id,
                occurred_at=observation.occurred_at,
                details=details,
            )
        ]
        if target is FormalAttemptState.TERMINAL:
            self._hit("executor_terminal.after_attempt_event")
        task = self._require_task_row_by_id(connection, observation.task_id)
        task_state = FormalTaskState(task["state"])
        if target is FormalAttemptState.RUNNING and task_state is FormalTaskState.ACCEPTED:
            appended.append(
                self._append_event(
                    connection,
                    task,
                    event_type="task.running",
                    state=FormalTaskState.RUNNING.value,
                    outcome=None,
                    producer="task_core",
                    source_event_id=observation.source_event_id,
                    causation_id=observation.source_event_id,
                    occurred_at=observation.occurred_at,
                    details=details,
                    update_task=True,
                )
            )
        elif target is FormalAttemptState.TERMINAL:
            assert observation.attempt_outcome is not None
            if task_state is FormalTaskState.TERMINAL:
                raise FormalTaskViolation(
                    "TASK_TERMINAL_CONFLICT",
                    "terminal task cannot accept a new Executor outcome",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
            self._reject_open_adjustments_before_terminal(
                connection,
                task=task,
                observed_at=observation.occurred_at,
            )
            terminal_task_event = self._append_event(
                connection,
                self._require_task_row_by_id(connection, observation.task_id),
                event_type="task.terminal",
                state=FormalTaskState.TERMINAL.value,
                outcome=observation.attempt_outcome.value,
                producer="task_core",
                source_event_id=observation.source_event_id,
                causation_id=observation.source_event_id,
                occurred_at=observation.occurred_at,
                details=details,
                update_task=True,
            )
            appended.append(terminal_task_event)
            self._hit("executor_terminal.after_task_terminal")
            if observation.attempt_outcome is TerminalOutcome.CANCELLED:
                self._settle_cancel_command_results(
                    connection,
                    task_id=observation.task_id,
                    scope_key=task["scope_key"],
                    settlement=terminal_task_event,
                )
            if observation.attempt_outcome is TerminalOutcome.COMPLETED and observation.result_text is not None:
                artifacts_json = _json_dump([artifact.to_dict() for artifact in observation.result_artifacts])
                existing_result = connection.execute(
                    """
                    SELECT * FROM task_results
                    WHERE task_id=? AND attempt_id=? AND source_event_id=?
                    """,
                    (
                        observation.task_id,
                        observation.attempt_id,
                        observation.source_event_id,
                    ),
                ).fetchone()
                expected = (
                    observation.result_text,
                    artifacts_json,
                    observation.occurred_at,
                )
                if existing_result is None:
                    connection.execute(
                        """
                        INSERT INTO task_results(
                            task_id, attempt_id, source_event_id, result_text,
                            artifacts_json, completed_at
                        ) VALUES(?, ?, ?, ?, ?, ?)
                        """,
                        (
                            observation.task_id,
                            observation.attempt_id,
                            observation.source_event_id,
                            *expected,
                        ),
                    )
                elif (
                    existing_result["result_text"],
                    existing_result["artifacts_json"],
                    existing_result["completed_at"],
                ) != expected:
                    raise FormalTaskViolation(
                        "TASK_RESULT_ID_CONFLICT",
                        "completed task result identity was reused with different facts",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
                self._hit("executor_terminal.after_task_result")
            connection.execute(
                """
                UPDATE outbox SET state=?, last_error=?, updated_at=?
                WHERE task_id=? AND kind=? AND state=?
                """,
                (
                    OutboxState.SUPPRESSED.value,
                    "TASK_TERMINAL_BEFORE_CANCELLATION_DELIVERY",
                    observation.occurred_at,
                    observation.task_id,
                    OutboxKind.ATTEMPT_CANCEL.value,
                    OutboxState.PENDING.value,
                ),
            )
        return tuple(appended)

    def task_result(self, task_id: str, scope: ScopeRef) -> tuple[TaskResultAvailability, TaskResultRecord | None, str]:
        """Read one immutable final result without guessing or polling failures."""

        with self._reader() as connection:
            task_row = self._require_task_row(connection, task_id, scope)
            return self._task_result_for_row(connection, task_row)

    def consumer_task_result(
        self, task_id: str, scope: ScopeRef
    ) -> tuple[TaskResultAvailability, TaskResultRecord | None, str]:
        """Read result truth for one stable authenticated subject/project."""

        with self._reader() as connection:
            task_row = self._require_consumer_task_row(connection, task_id, scope)
            return self._task_result_for_row(connection, task_row)

    @classmethod
    def _task_result_for_row(
        cls,
        connection: sqlite3.Connection,
        task_row: sqlite3.Row,
    ) -> tuple[TaskResultAvailability, TaskResultRecord | None, str]:
        task = cls._task_from_row(task_row)
        if task.state is not FormalTaskState.TERMINAL:
            return TaskResultAvailability.NOT_READY, None, "TASK_RESULT_NOT_READY"
        if task.outcome is not TerminalOutcome.COMPLETED:
            reason = {
                TerminalOutcome.CANCELLED: "TASK_CANCELLED",
                TerminalOutcome.FAILED: "TASK_FAILED",
                TerminalOutcome.INTERRUPTED: "TASK_INTERRUPTED",
            }.get(task.outcome, "TASK_RESULT_UNAVAILABLE")
            return TaskResultAvailability.UNAVAILABLE, None, reason
        rows = connection.execute(
            """
            SELECT * FROM task_results
            WHERE task_id=? AND attempt_id=?
            ORDER BY completed_at, source_event_id
            """,
            (task.task_id, task.attempt_id),
        ).fetchall()
        if len(rows) != 1:
            return (
                TaskResultAvailability.UNAVAILABLE,
                None,
                "TASK_RESULT_NOT_CAPTURED",
            )
        result = cls._task_result_from_row(rows[0])
        return TaskResultAvailability.AVAILABLE, result, "TASK_RESULT_AVAILABLE"

    @classmethod
    def _mutation_result(
        cls,
        connection: sqlite3.Connection,
        attempt_row: sqlite3.Row,
        disposition: TaskMutationDisposition,
        *,
        events: tuple[PersistentTaskEvent, ...] = (),
    ) -> TaskMutationResult:
        boundary = connection.execute(
            """
            SELECT MAX(seq) AS through_seq FROM task_events
            WHERE task_id=? AND attempt_id=?
            """,
            (attempt_row["task_id"], attempt_row["attempt_id"]),
        ).fetchone()
        if boundary is None or boundary["through_seq"] is None:
            raise cls._corrupt("formal Task mutation target has no durable event segment")
        return TaskMutationResult(
            disposition=disposition,
            task_id=attempt_row["task_id"],
            attempt=cls._attempt_from_row(attempt_row),
            through_seq=int(boundary["through_seq"]),
            events=events,
        )

    def _append_event(
        self,
        connection: sqlite3.Connection,
        task: sqlite3.Row,
        *,
        event_type: str,
        state: str,
        outcome: str | None,
        producer: str,
        source_event_id: str | None,
        causation_id: str,
        occurred_at: str,
        details: Mapping[str, object],
        update_task: bool = False,
    ) -> PersistentTaskEvent:
        if update_task:
            try:
                validate_transition(
                    LifecycleKind.TASK,
                    str(task["state"]),
                    state,
                    outcome=outcome,
                )
            except ContractViolation as error:
                raise FormalTaskViolation(
                    error.reason,
                    str(error),
                    error.code,
                ) from error
        event_details = dict(details)
        seq = int(task["event_head"]) + 1
        event_id = f"event-{uuid.uuid4().hex}"
        self._insert_event(
            connection,
            event_id=event_id,
            task_id=task["task_id"],
            attempt_id=task["attempt_id"],
            scope=ScopeRef.from_dict(_json_load(task["scope_json"])),
            seq=seq,
            event_type=event_type,
            state=state,
            outcome=outcome,
            producer=producer,
            source_event_id=source_event_id,
            causation_id=causation_id,
            correlation_id=task["correlation_id"],
            occurred_at=occurred_at,
            details=event_details,
        )
        if update_task and event_type == "task.terminal":
            self._hit("executor_terminal.after_task_event")
        if update_task:
            connection.execute(
                """
                UPDATE tasks SET state=?, outcome=?, event_head=?, updated_at=?,
                    reconciliation_state=NULL, reconciliation_reason=NULL
                WHERE task_id=?
                """,
                (state, outcome, seq, occurred_at, task["task_id"]),
            )
        else:
            connection.execute(
                "UPDATE tasks SET event_head=?, updated_at=? WHERE task_id=?",
                (seq, occurred_at, task["task_id"]),
            )
        return PersistentTaskEvent(
            event_id=event_id,
            task_id=task["task_id"],
            attempt_id=task["attempt_id"],
            scope=ScopeRef.from_dict(_json_load(task["scope_json"])),
            seq=seq,
            event_type=event_type,
            state=state,
            outcome=outcome,
            producer=producer,
            source_event_id=source_event_id,
            causation_id=causation_id,
            correlation_id=task["correlation_id"],
            occurred_at=occurred_at,
            details=event_details,
        )

    def reset_expired_outbox_claims(self, *, claimed_before: str) -> int:
        with self._transaction() as connection:
            now = utc_now()
            suppressed = connection.execute(
                """
                UPDATE outbox SET state=?, claimed_by=NULL, claimed_at=NULL,
                    claim_token=NULL,
                    last_error=COALESCE(last_error, ?), updated_at=?
                WHERE state=? AND claimed_at IS NOT NULL AND claimed_at<=?
                  AND EXISTS (
                    SELECT 1 FROM tasks AS t
                    JOIN attempts AS a ON a.attempt_id=outbox.attempt_id
                    WHERE t.task_id=outbox.task_id
                      AND (t.attempt_id<>outbox.attempt_id
                           OR t.state=? OR a.state=?)
                  )
                """,
                (
                    OutboxState.SUPPRESSED.value,
                    "TASK_TERMINAL_BEFORE_DELIVERY",
                    now,
                    OutboxState.CLAIMED.value,
                    claimed_before,
                    FormalTaskState.TERMINAL.value,
                    FormalAttemptState.TERMINAL.value,
                ),
            ).rowcount
            selected_expired = connection.execute(
                """
                SELECT o.outbox_id, o.task_id
                FROM outbox AS o
                JOIN attempts AS a ON a.attempt_id=o.attempt_id
                JOIN tasks AS t ON t.task_id=o.task_id
                WHERE o.state=? AND o.claimed_at IS NOT NULL
                  AND o.claimed_at<=? AND o.kind=?
                  AND a.adapter_id IS NOT NULL
                  AND t.attempt_id=o.attempt_id
                  AND t.state<>? AND a.state<>?
                ORDER BY o.outbox_id
                """,
                (
                    OutboxState.CLAIMED.value,
                    claimed_before,
                    OutboxKind.ATTEMPT_DISPATCH.value,
                    FormalTaskState.TERMINAL.value,
                    FormalAttemptState.TERMINAL.value,
                ),
            ).fetchall()
            for expired in selected_expired:
                self._mark_admission_reconciliation_required(
                    connection,
                    task_id=expired["task_id"],
                    observed_at=now,
                )
            reset = connection.execute(
                """
                UPDATE outbox SET state=?, claimed_by=NULL, claimed_at=NULL,
                    claim_token=NULL, updated_at=?
                WHERE state=? AND claimed_at IS NOT NULL AND claimed_at<=?
                """,
                (
                    OutboxState.PENDING.value,
                    now,
                    OutboxState.CLAIMED.value,
                    claimed_before,
                ),
            ).rowcount
            return suppressed + len(selected_expired) + reset

    def mark_reconciliation_pending(
        self,
        task_id: str,
        attempt_id: str,
        reason: str,
        *,
        in_progress: bool = False,
    ) -> TaskMutationResult:
        state = ReconciliationState.IN_PROGRESS if in_progress else ReconciliationState.PENDING
        with self._transaction() as connection:
            task = self._require_task_row_by_id(connection, task_id)
            attempt = connection.execute("SELECT * FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
            if attempt is None or attempt["task_id"] != task_id:
                raise FormalTaskViolation(
                    "ATTEMPT_SCOPE_MISMATCH",
                    "reconciliation attempt does not belong to task",
                    ErrorCode.PERMISSION_DENIED,
                )
            if task["attempt_id"] != attempt_id:
                return self._mutation_result(
                    connection,
                    attempt,
                    TaskMutationDisposition.SUPERSEDED,
                )
            if task["state"] == FormalTaskState.TERMINAL.value:
                return self._mutation_result(connection, attempt, TaskMutationDisposition.NOOP)
            if task["reconciliation_state"] == ReconciliationState.REQUIRED.value:
                return self._mutation_result(connection, attempt, TaskMutationDisposition.NOOP)
            if task["reconciliation_state"] == state.value and task["reconciliation_reason"] == reason:
                return self._mutation_result(connection, attempt, TaskMutationDisposition.NOOP)
            connection.execute(
                """
                UPDATE tasks SET reconciliation_state=?, reconciliation_reason=?,
                    updated_at=? WHERE task_id=?
                """,
                (state.value, reason, utc_now(), task_id),
            )
            return self._mutation_result(connection, attempt, TaskMutationDisposition.APPLIED)

    @classmethod
    def _queue_control(cls, task, attempt, dispatches) -> TaskQueueControl:
        """Project the same durable queue facts used by update and reprioritize.

        Legacy unselected Tasks can still be updated, but do not advertise
        selected-Executor queue operations. Physical claim ownership is checked
        here rather than reconstructed by the application from admission labels.
        """
        if attempt is None or attempt["task_id"] != task["task_id"]:
            return TaskQueueControl("none", False, False)
        dispatch = dispatches[0] if len(dispatches) == 1 else None
        accepted = (
            task["state"] == FormalTaskState.ACCEPTED.value
            and not bool(task["cancel_requested"])
            and not bool(task["dispatch_fenced"])
            and attempt["state"] == FormalAttemptState.ACCEPTED.value
        )
        can_update = bool(
            accepted and dispatch is not None
            and dispatch["state"] == OutboxState.PENDING.value
            and int(dispatch["delivery_count"]) == 0
            and dispatch["claimed_by"] is None
            and dispatch["claimed_at"] is None
            and dispatch["claim_token"] is None
        )
        if _selection_from_attempt_row(attempt) is None:
            return TaskQueueControl("none", can_update, False)
        unclaimed = bool(
            accepted and task["reconciliation_state"] is None
            and attempt["executor_ref"] is None and int(attempt["source_seq"]) == -1
            and dispatch is not None and dispatch["state"] == OutboxState.PENDING.value
        )
        return TaskQueueControl(
            "unclaimed" if unclaimed else "taken_over", can_update,
            bool(unclaimed and cls._is_exact_unbound_queue(task, attempt, dispatch)),
        )

    @staticmethod
    def _is_exact_unbound_queue(
        task: sqlite3.Row,
        attempt: sqlite3.Row,
        dispatch: sqlite3.Row | None,
    ) -> bool:
        """Prove Store ownership, including a closed pre-effect admission defer."""
        attempt_count = int(attempt["admission_attempt_count"] or 0)
        return bool(
            task["state"] == FormalTaskState.ACCEPTED.value
            and attempt["state"] == FormalAttemptState.ACCEPTED.value
            and attempt["outcome"] is None
            and attempt["executor_ref"] is None
            and int(attempt["source_seq"]) == -1
            and _selection_from_attempt_row(attempt) is not None
            and dispatch is not None
            and dispatch["state"] == OutboxState.PENDING.value
            and dispatch["claimed_by"] is None
            and dispatch["claimed_at"] is None
            and dispatch["claim_token"] is None
            and int(dispatch["delivery_count"]) == attempt_count
            and (
                (attempt_count == 0 and attempt["admission_reason"] is None and dispatch["last_error"] is None)
                or (
                    attempt_count > 0
                    and attempt["admission_reason"]
                    in {
                        "EXECUTOR_PROJECT_BUSY",
                        "EXECUTOR_CAPACITY_EXHAUSTED",
                    }
                    and dispatch["last_error"] == attempt["admission_reason"]
                )
            )
        )

    def settle_unbound_queued_attempt(
        self,
        task_id: str,
        attempt_id: str,
    ) -> bool:
        """Prove one selected unbound Attempt is still Store-owned queue work.

        A selected dispatch has no Executor binding until its pending outbox
        delivery is accepted.  That is ordinary queue ownership, including a
        closed busy/capacity deferral, rather than status uncertainty.  Clear
        only the obsolete marker produced for that exact premise; every other
        reconciliation state remains fail-closed.
        """

        with self._transaction() as connection:
            task = self._require_task_row_by_id(connection, task_id)
            attempt = connection.execute("SELECT * FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
            if attempt is None or attempt["task_id"] != task_id:
                raise FormalTaskViolation(
                    "ATTEMPT_SCOPE_MISMATCH",
                    "queued reconciliation attempt does not belong to task",
                    ErrorCode.PERMISSION_DENIED,
                )
            if task["attempt_id"] != attempt_id:
                return False
            dispatches = connection.execute(
                """
                SELECT * FROM outbox
                WHERE task_id=? AND attempt_id=? AND kind=?
                ORDER BY created_at, outbox_id
                """,
                (task_id, attempt_id, OutboxKind.ATTEMPT_DISPATCH.value),
            ).fetchall()
            dispatch = dispatches[0] if len(dispatches) == 1 else None
            exact_queue = (
                not bool(task["cancel_requested"])
                and not bool(task["dispatch_fenced"])
                and self._is_exact_unbound_queue(task, attempt, dispatch)
            )
            if not exact_queue:
                return False
            reconciliation_state = task["reconciliation_state"]
            reconciliation_reason = task["reconciliation_reason"]
            if reconciliation_state is None:
                return True
            if (
                reconciliation_state != ReconciliationState.PENDING.value
                or reconciliation_reason != "ATTEMPT_NOT_YET_BOUND"
            ):
                return False
            changed = connection.execute(
                """
                UPDATE tasks SET reconciliation_state=NULL,
                    reconciliation_reason=NULL, updated_at=?
                WHERE task_id=? AND attempt_id=? AND state=?
                  AND reconciliation_state=? AND reconciliation_reason=?
                """,
                (
                    utc_now(),
                    task_id,
                    attempt_id,
                    FormalTaskState.ACCEPTED.value,
                    ReconciliationState.PENDING.value,
                    "ATTEMPT_NOT_YET_BOUND",
                ),
            ).rowcount
            return changed == 1

    def mark_reconciliation_resolved(self, task_id: str, attempt_id: str, reason: str) -> TaskMutationResult:
        with self._transaction() as connection:
            task = self._require_task_row_by_id(connection, task_id)
            attempt = connection.execute("SELECT * FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
            if attempt is None or attempt["task_id"] != task_id:
                raise FormalTaskViolation(
                    "ATTEMPT_SCOPE_MISMATCH",
                    "reconciliation attempt does not belong to task",
                    ErrorCode.PERMISSION_DENIED,
                )
            if task["attempt_id"] != attempt_id:
                return self._mutation_result(
                    connection,
                    attempt,
                    TaskMutationDisposition.SUPERSEDED,
                )
            if task["state"] == FormalTaskState.TERMINAL.value:
                return self._mutation_result(connection, attempt, TaskMutationDisposition.NOOP)
            if task["reconciliation_state"] == ReconciliationState.REQUIRED.value:
                return self._mutation_result(connection, attempt, TaskMutationDisposition.NOOP)
            if (
                task["reconciliation_state"] == ReconciliationState.RESOLVED.value
                and task["reconciliation_reason"] == reason
            ):
                return self._mutation_result(connection, attempt, TaskMutationDisposition.NOOP)
            connection.execute(
                """
                UPDATE tasks SET reconciliation_state=?, reconciliation_reason=?,
                    updated_at=? WHERE task_id=?
                """,
                (
                    ReconciliationState.RESOLVED.value,
                    reason,
                    utc_now(),
                    task_id,
                ),
            )
            return self._mutation_result(connection, attempt, TaskMutationDisposition.APPLIED)

    def resolve_lost_attempt(self, task_id: str, attempt_id: str, reason: str) -> TaskMutationResult:
        now = utc_now()
        with self._transaction() as connection:
            task = self._require_task_row_by_id(connection, task_id)
            attempt = connection.execute("SELECT * FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
            if attempt is None or attempt["task_id"] != task_id:
                raise FormalTaskViolation(
                    "ATTEMPT_SCOPE_MISMATCH",
                    "reconciliation attempt does not belong to task",
                    ErrorCode.PERMISSION_DENIED,
                )
            if task["attempt_id"] != attempt_id:
                return self._mutation_result(
                    connection,
                    attempt,
                    TaskMutationDisposition.SUPERSEDED,
                )
            if task["state"] == FormalTaskState.TERMINAL.value:
                return self._mutation_result(connection, attempt, TaskMutationDisposition.NOOP)
            connection.execute(
                """
                UPDATE attempts SET state=?, outcome=?, updated_at=?
                WHERE attempt_id=?
                """,
                (
                    FormalAttemptState.TERMINAL.value,
                    TerminalOutcome.INTERRUPTED.value,
                    now,
                    attempt_id,
                ),
            )
            appended = [
                self._append_event(
                    connection,
                    task,
                    event_type="attempt.terminal",
                    state=FormalAttemptState.TERMINAL.value,
                    outcome=TerminalOutcome.INTERRUPTED.value,
                    producer="task_core.reconciliation",
                    source_event_id=None,
                    causation_id=f"reconciliation:{attempt_id}",
                    occurred_at=now,
                    details={"reason": reason},
                )
            ]
            self._reject_open_adjustments_before_terminal(
                connection,
                task=self._require_task_row_by_id(connection, task_id),
                observed_at=now,
            )
            appended.append(
                self._append_event(
                    connection,
                    self._require_task_row_by_id(connection, task_id),
                    event_type="task.terminal",
                    state=FormalTaskState.TERMINAL.value,
                    outcome=TerminalOutcome.INTERRUPTED.value,
                    producer="task_core.reconciliation",
                    source_event_id=None,
                    causation_id=f"reconciliation:{attempt_id}",
                    occurred_at=now,
                    details={"reason": reason},
                    update_task=True,
                )
            )
            connection.execute(
                """
                UPDATE outbox SET state=?, claimed_by=NULL, claimed_at=NULL,
                    claim_token=NULL, last_error=?, updated_at=?
                WHERE task_id=? AND state IN (?, ?)
                """,
                (
                    OutboxState.SUPPRESSED.value,
                    "EXECUTOR_ATTEMPT_LOST",
                    now,
                    task_id,
                    OutboxState.PENDING.value,
                    OutboxState.CLAIMED.value,
                ),
            )
            frozen_attempt = connection.execute("SELECT * FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
            assert frozen_attempt is not None
            return self._mutation_result(
                connection,
                frozen_attempt,
                TaskMutationDisposition.APPLIED,
                events=tuple(appended),
            )

    def get_task(self, task_id: str, scope: ScopeRef) -> PersistentTaskRecord:
        with self._reader() as connection:
            return self._task_from_row(self._require_task_row(connection, task_id, scope))

    def get_consumer_task(self, task_id: str, scope: ScopeRef) -> PersistentTaskRecord:
        """Read one Task through the stable consumer scope, not Session identity."""

        with self._reader() as connection:
            return self._task_from_row(self._require_consumer_task_row(connection, task_id, scope))

    def _task_read_snapshot_from_rows(self, task: sqlite3.Row, attempt: sqlite3.Row) -> _TaskReadSnapshot:
        selection = _stored_record("executor selection", lambda: _selection_from_attempt_row(attempt))
        admission: PersistentAdmissionRecord | None = None
        if selection is not None:
            reconciliation_required = task["reconciliation_state"] == ReconciliationState.REQUIRED.value
            admission = _stored_record(
                "admission",
                lambda: PersistentAdmissionRecord(
                    task_id=task["task_id"],
                    attempt_id=attempt["attempt_id"],
                    priority=AdmissionPriority(attempt["admission_priority"]),
                    reason=attempt["admission_reason"],
                    attempt_count=int(attempt["admission_attempt_count"]),
                    next_eligible_at=attempt["admission_next_eligible_at"],
                    deadline_at=attempt["admission_deadline_at"],
                    enqueued_at=attempt["admission_enqueued_at"],
                    queued=bool(attempt["is_queued"]),
                    reconciliation_required=reconciliation_required,
                    reconciliation_reason=(task["reconciliation_reason"] if reconciliation_required else None),
                    manual_action=("verify_external_ownership_and_settle" if reconciliation_required else None),
                ),
            )
        return (
            self._task_from_row(task),
            self._attempt_from_row(attempt),
            admission,
        )

    def _task_read_attempt_row(self, connection: sqlite3.Connection, task: sqlite3.Row) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT a.*,
                   EXISTS(
                       SELECT 1 FROM outbox AS o
                       WHERE o.task_id=a.task_id
                         AND o.attempt_id=a.attempt_id
                         AND o.kind=? AND o.state=?
                   ) AS is_queued
            FROM attempts AS a
            WHERE a.attempt_id=? AND a.task_id=?
            """,
            (
                OutboxKind.ATTEMPT_DISPATCH.value,
                OutboxState.PENDING.value,
                task["attempt_id"],
                task["task_id"],
            ),
        ).fetchone()
        if row is None:
            raise self._corrupt("Task read snapshot lost its current Attempt")
        return row

    def task_read_snapshot(self, task_id: str, scope: ScopeRef, *, verify_lineage: bool = False) -> _TaskReadSnapshot:
        """Read Task, current Attempt, and admission from one SQLite snapshot."""

        with self._snapshot_reader() as connection:
            task = self._require_task_row(connection, task_id, scope)
            if verify_lineage:
                self._verify_durable_lineage(connection, task)
            self._hit("task_read_snapshot.after_task")
            attempt = self._task_read_attempt_row(connection, task)
            return self._task_read_snapshot_from_rows(task, attempt)

    def list_tasks(self, scope: ScopeRef) -> tuple[PersistentTaskRecord, ...]:
        with self._reader() as connection:
            rows = connection.execute(
                "SELECT * FROM tasks WHERE scope_key=? ORDER BY created_at, task_id",
                (_scope_key(scope),),
            ).fetchall()
            return tuple(self._task_from_row(row) for row in rows)

    @staticmethod
    def _task_page_scope(scope: ScopeRef, cursor: str | None, limit: int) -> str:
        if type(limit) is not int or not 1 <= limit <= _MAX_TASK_PAGE_LIMIT:
            raise FormalTaskViolation(
                "INVALID_TASK_PAGE_LIMIT",
                f"task.list limit must be between 1 and {_MAX_TASK_PAGE_LIMIT}",
                ErrorCode.INVALID_ARGUMENT,
            )
        if cursor is not None and (
            type(cursor) is not str or not cursor.strip() or "\x00" in cursor or len(cursor) > 256
        ):
            raise FormalTaskViolation(
                "INVALID_TASK_LIST_CURSOR",
                "task.list cursor must be one bounded Task identity",
                ErrorCode.INVALID_ARGUMENT,
            )
        return _scope_key(scope)

    def _task_page_rows(self, connection, scope: ScopeRef, scope_key: str, cursor: str | None, limit: int):
        if cursor is None:
            rows = connection.execute(
                """
                SELECT * FROM tasks
                WHERE scope_key=?
                ORDER BY created_at, task_id
                LIMIT ?
                """,
                (scope_key, limit + 1),
            ).fetchall()
        else:
            anchor = self._require_task_row(connection, cursor, scope)
            rows = connection.execute(
                """
                SELECT * FROM tasks
                WHERE scope_key=?
                  AND (created_at>? OR (created_at=? AND task_id>?))
                ORDER BY created_at, task_id
                LIMIT ?
                """,
                (
                    scope_key,
                    anchor["created_at"],
                    anchor["created_at"],
                    anchor["task_id"],
                    limit + 1,
                ),
            ).fetchall()
        return rows

    def list_tasks_page(
        self, scope: ScopeRef, *, cursor: str | None = None, limit: int = _DEFAULT_TASK_PAGE_LIMIT,
    ) -> tuple[tuple[PersistentTaskRecord, ...], str | None, bool]:
        """Read one bounded, restart-stable keyset page in exact Task scope."""
        scope_key = self._task_page_scope(scope, cursor, limit)
        with self._reader() as connection:
            rows = self._task_page_rows(connection, scope, scope_key, cursor, limit)
            has_more = len(rows) > limit
            tasks = tuple(self._task_from_row(row) for row in rows[:limit])
            next_cursor = tasks[-1].task_id if has_more and tasks else None
            return tasks, next_cursor, has_more

    def list_task_read_snapshots_page(
        self,
        scope: ScopeRef,
        *,
        cursor: str | None = None,
        limit: int = _DEFAULT_TASK_PAGE_LIMIT,
    ) -> tuple[tuple[_TaskReadSnapshot, ...], str | None, bool]:
        """Read one Task page and every current Attempt from one snapshot."""

        scope_key = self._task_page_scope(scope, cursor, limit)
        with self._snapshot_reader() as connection:
            rows = self._task_page_rows(connection, scope, scope_key, cursor, limit)
            has_more = len(rows) > limit
            page_rows = rows[:limit]
            self._hit("list_task_read_snapshots_page.after_tasks")
            snapshots = tuple(
                self._task_read_snapshot_from_rows(task, self._task_read_attempt_row(connection, task))
                for task in page_rows
            )
            next_cursor = snapshots[-1][0].task_id if has_more and snapshots else None
            return snapshots, next_cursor, has_more

    def list_task_authority_snapshots_page(
        self, scope: ScopeRef, *, cursor: str | None = None, limit: int = _DEFAULT_TASK_PAGE_LIMIT,
    ) -> tuple[tuple[TaskAuthorityReadSnapshot, ...], str | None, bool]:
        """Read Task, current Attempt, admission, event head and result atomically.

        Uses the same keyset paging and row decoders as ordinary Task reads.
        This is a read-only database snapshot, not a second lifecycle cache.
        """
        scope_key = self._task_page_scope(scope, cursor, limit)
        with self._snapshot_reader() as connection:
            rows = self._task_page_rows(connection, scope, scope_key, cursor, limit)
            has_more = len(rows) > limit
            self._hit("list_task_authority_snapshots_page.after_tasks")
            snapshots = []
            for row in rows[:limit]:
                attempt_row = self._task_read_attempt_row(connection, row)
                task, attempt, admission = self._task_read_snapshot_from_rows(
                    row, attempt_row)
                dispatches = connection.execute(
                    "SELECT * FROM outbox WHERE task_id=? AND attempt_id=? AND kind=?",
                    (task.task_id, task.attempt_id, OutboxKind.ATTEMPT_DISPATCH.value),
                ).fetchall()
                event_row = connection.execute(
                    "SELECT * FROM task_events WHERE task_id=? AND seq=?",
                    (task.task_id, task.event_head),
                ).fetchone()
                if event_row is None:
                    raise self._corrupt("Task authority snapshot lost its event head")
                availability, result, reason = self._task_result_for_row(connection, row)
                snapshots.append(TaskAuthorityReadSnapshot(
                    task, attempt, admission, self._event_from_row(event_row), availability, result, reason,
                    self._queue_control(row, attempt_row, dispatches)))
            next_cursor = snapshots[-1].task.task_id if has_more and snapshots else None
            return tuple(snapshots), next_cursor, has_more

    def get_attempt(self, attempt_id: str) -> PersistentAttemptRecord:
        with self._reader() as connection:
            row = connection.execute("SELECT * FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
            if row is None:
                raise FormalTaskViolation(
                    "ATTEMPT_NOT_FOUND",
                    "attempt is unavailable",
                    ErrorCode.NOT_FOUND,
                )
            return self._attempt_from_row(row)

    def admission_projection(self, task_id: str, scope: ScopeRef) -> PersistentAdmissionRecord | None:
        """Return persisted queue facts without changing canonical lifecycle."""

        with self._snapshot_reader() as connection:
            task = self._require_task_row(connection, task_id, scope)
            return self._task_read_snapshot_from_rows(task, self._task_read_attempt_row(connection, task))[2]

    def events(
        self,
        task_id: str,
        scope: ScopeRef,
        *,
        after_seq: int = -1,
        attempt_id: str | None = None,
    ) -> tuple[PersistentTaskEvent, ...]:
        return self._events(
            task_id,
            scope,
            after_seq=after_seq,
            attempt_id=attempt_id,
            consumer_scope=False,
        )

    def consumer_events(
        self,
        task_id: str,
        scope: ScopeRef,
        *,
        after_seq: int = -1,
        attempt_id: str | None = None,
    ) -> tuple[PersistentTaskEvent, ...]:
        """Read Task events for the stable authenticated consumer identity."""

        return self._events(
            task_id,
            scope,
            after_seq=after_seq,
            attempt_id=attempt_id,
            consumer_scope=True,
        )

    def _events(
        self,
        task_id: str,
        scope: ScopeRef,
        *,
        after_seq: int,
        attempt_id: str | None,
        consumer_scope: bool,
    ) -> tuple[PersistentTaskEvent, ...]:
        if type(after_seq) is not int or after_seq < -1:
            raise FormalTaskViolation(
                "INVALID_EVENT_CURSOR",
                "after_seq must be an integer at least -1",
                ErrorCode.INVALID_ARGUMENT,
            )
        with self._reader() as connection:
            if consumer_scope:
                self._require_consumer_task_row(connection, task_id, scope)
            else:
                self._require_task_row(connection, task_id, scope)
            if attempt_id is None:
                rows = connection.execute(
                    """
                    SELECT * FROM task_events WHERE task_id=? AND seq>?
                    ORDER BY seq
                    """,
                    (task_id, after_seq),
                ).fetchall()
            else:
                attempt_row = connection.execute(
                    "SELECT task_id FROM attempts WHERE attempt_id=?",
                    (attempt_id,),
                ).fetchone()
                if attempt_row is None or attempt_row["task_id"] != task_id:
                    raise FormalTaskViolation(
                        "TASK_ATTEMPT_STALE",
                        "event segment attempt does not belong to the task",
                        ErrorCode.STALE,
                    )
                rows = connection.execute(
                    """
                    SELECT * FROM task_events
                    WHERE task_id=? AND attempt_id=? AND seq>?
                    ORDER BY seq
                    """,
                    (task_id, attempt_id, after_seq),
                ).fetchall()
            return tuple(self._event_from_row(row) for row in rows)

    def events_page(
        self,
        task_id: str,
        scope: ScopeRef,
        *,
        after_seq: int = -1,
        limit: int = _DEFAULT_EVENT_PAGE_LIMIT,
    ) -> tuple[tuple[PersistentTaskEvent, ...], int, int | None, bool]:
        """Read one prefix-bounded event page against a frozen durable head."""

        if type(after_seq) is not int or after_seq < -1:
            raise FormalTaskViolation(
                "INVALID_EVENT_CURSOR",
                "after_seq must be an integer at least -1",
                ErrorCode.INVALID_ARGUMENT,
            )
        if type(limit) is not int or not 1 <= limit <= _MAX_EVENT_PAGE_LIMIT:
            raise FormalTaskViolation(
                "INVALID_EVENT_PAGE_LIMIT",
                f"task.events limit must be between 1 and {_MAX_EVENT_PAGE_LIMIT}",
                ErrorCode.INVALID_ARGUMENT,
            )
        with self._reader() as connection:
            task = self._require_task_row(connection, task_id, scope)
            head_seq = int(task["event_head"])
            if after_seq > head_seq:
                raise FormalTaskViolation(
                    "TASK_EVENT_CURSOR_STALE",
                    "task.events cursor is beyond the authoritative event head",
                    ErrorCode.STALE,
                )
            rows = connection.execute(
                """
                SELECT * FROM task_events
                WHERE task_id=? AND seq>? AND seq<=?
                ORDER BY seq
                LIMIT ?
                """,
                (task_id, after_seq, head_seq, limit + 1),
            ).fetchall()
            has_more = len(rows) > limit
            page_rows = rows[:limit]
            events = tuple(self._event_from_row(row) for row in page_rows)
            next_after_seq = events[-1].seq if has_more and events else None
            return events, head_seq, next_after_seq, has_more

    def unread_events_page(
        self,
        task_id: str,
        scope: ScopeRef,
        *,
        presentation_class: str,
        limit: int,
    ) -> TaskUnreadPage:
        """Read one class watermark and retained event prefix without mutation."""

        if type(presentation_class) is not str or presentation_class not in {
            "text",
            "voice",
        }:
            raise FormalTaskViolation(
                "INVALID_PRESENTATION_CLASS",
                "task unread presentation class must be text or voice",
                ErrorCode.INVALID_ARGUMENT,
            )
        if type(limit) is not int or not 1 <= limit <= _MAX_EVENT_PAGE_LIMIT:
            raise FormalTaskViolation(
                "INVALID_EVENT_PAGE_LIMIT",
                f"task.unread_events limit must be between 1 and {_MAX_EVENT_PAGE_LIMIT}",
                ErrorCode.INVALID_ARGUMENT,
            )
        with self._snapshot_reader() as connection:
            task = self._require_consumer_task_row(connection, task_id, scope)
            consumer = connection.execute(
                """SELECT acked_through_seq, acked_event_id
                   FROM task_event_consumption
                   WHERE subject_id=? AND project_id=? AND task_id=?
                     AND presentation_class=?""",
                (
                    scope.subject_id,
                    scope.project_id,
                    task_id,
                    presentation_class,
                ),
            ).fetchone()
            self._hit("unread_events_page.after_consumer")
            watermark = -1 if consumer is None else int(consumer["acked_through_seq"])
            acked_event_id = None if consumer is None else consumer["acked_event_id"]
            head_seq = int(task["event_head"])
            rows = connection.execute(
                """SELECT * FROM task_events
                   WHERE task_id=? AND seq>? AND seq<=?
                   ORDER BY seq
                   LIMIT ?""",
                (task_id, watermark, head_seq, limit + 1),
            ).fetchall()
            has_more = len(rows) > limit
            events = tuple(self._event_from_row(row) for row in rows[:limit])
            return TaskUnreadPage(
                task_id=task_id,
                presentation_class=presentation_class,
                watermark=watermark,
                acked_event_id=acked_event_id,
                head_seq=head_seq,
                events=events,
                next_after_seq=events[-1].seq if has_more and events else None,
                has_more=has_more,
            )

    def consumer_progress_authority_page(
        self,
        task_id: str,
        scope: ScopeRef,
        *,
        presentation_class: str,
        limit: int,
        after_seq: int | None = None,
        through_seq: int | None = None,
    ) -> TaskEventConsumerAuthorityPage:
        """Read one Store-verified, watermark-resumed Task-wide page."""

        if type(presentation_class) is not str or presentation_class not in {
            "text",
            "voice",
        }:
            raise FormalTaskViolation(
                "INVALID_PRESENTATION_CLASS",
                "TaskEvent consumer presentation class must be text or voice",
                ErrorCode.INVALID_ARGUMENT,
            )
        if type(limit) is not int or not 1 <= limit <= _MAX_EVENT_PAGE_LIMIT:
            raise FormalTaskViolation(
                "INVALID_EVENT_PAGE_LIMIT",
                f"TaskEvent consumer limit must be between 1 and {_MAX_EVENT_PAGE_LIMIT}",
                ErrorCode.INVALID_ARGUMENT,
            )
        if after_seq is not None and (type(after_seq) is not int or after_seq < -1):
            raise FormalTaskViolation(
                "INVALID_EVENT_CURSOR",
                "TaskEvent consumer cursor must be an integer at least -1",
                ErrorCode.INVALID_ARGUMENT,
            )
        if through_seq is not None and (type(through_seq) is not int or through_seq < 0):
            raise FormalTaskViolation(
                "INVALID_EVENT_CURSOR",
                "TaskEvent frozen head must be a non-negative integer",
                ErrorCode.INVALID_ARGUMENT,
            )
        with self._snapshot_reader() as connection:
            task_row = self._require_consumer_task_row(connection, task_id, scope)
            self._verify_durable_lineage(connection, task_row)
            task = self._task_from_row(task_row)
            attempt_row = connection.execute("SELECT * FROM attempts WHERE attempt_id=?", (task.attempt_id,)).fetchone()
            if attempt_row is None:
                raise FormalTaskViolation(
                    "TASK_STORE_CORRUPT",
                    "formal Task Store is missing the consumer Attempt",
                    ErrorCode.INTERNAL,
                )
            attempt = self._attempt_from_row(attempt_row)
            consumer = connection.execute(
                """SELECT acked_through_seq, acked_event_id
                   FROM task_event_consumption
                   WHERE subject_id=? AND project_id=? AND task_id=?
                     AND presentation_class=?""",
                (
                    scope.subject_id,
                    scope.project_id,
                    task.task_id,
                    presentation_class,
                ),
            ).fetchone()
            watermark = -1 if consumer is None else int(consumer["acked_through_seq"])
            acked_event_id = None if consumer is None else consumer["acked_event_id"]
            cursor_lifecycle_row = None
            cursor_boundary_row = None
            if watermark >= 0:
                acknowledged_row = connection.execute(
                    "SELECT event_id FROM task_events WHERE task_id=? AND seq=?",
                    (task.task_id, watermark),
                ).fetchone()
                cursor_lifecycle_row = connection.execute(
                    """SELECT * FROM task_events
                       WHERE task_id=? AND seq<=? AND event_type IN (
                         'task.accepted', 'task.retry_accepted',
                         'task.recovery_accepted', 'task.running', 'task.blocked',
                         'task.decision_required', 'task.terminal'
                       )
                       ORDER BY seq DESC LIMIT 1""",
                    (task.task_id, watermark),
                ).fetchone()
                cursor_boundary_row = connection.execute(
                    """SELECT * FROM task_events
                       WHERE task_id=? AND seq<=? AND event_type IN (
                         'task.accepted', 'task.retry_accepted',
                         'task.recovery_accepted'
                       )
                       ORDER BY seq DESC LIMIT 1""",
                    (task.task_id, watermark),
                ).fetchone()
                if (
                    acknowledged_row is None
                    or acknowledged_row["event_id"] != acked_event_id
                    or cursor_lifecycle_row is None
                    or cursor_boundary_row is None
                ):
                    raise FormalTaskViolation(
                        "TASK_STORE_CORRUPT",
                        "consumer watermark lacks exact Task lifecycle authority",
                        ErrorCode.INTERNAL,
                    )
            terminal_head_row = (
                connection.execute(
                    "SELECT * FROM task_events WHERE task_id=? AND seq=?",
                    (task.task_id, task.event_head),
                ).fetchone()
                if task.state is FormalTaskState.TERMINAL
                else None
            )
            segment_start_seq = watermark + 1
            minimum_after = watermark
            page_after_seq = minimum_after if after_seq is None else after_seq
            frozen_head = task.event_head if through_seq is None else through_seq
            if page_after_seq < minimum_after or page_after_seq > frozen_head or frozen_head > task.event_head:
                raise FormalTaskViolation(
                    "TASK_EVENT_CONSUMER_CURSOR_STALE",
                    "TaskEvent consumer page no longer binds its frozen cursor",
                    ErrorCode.STALE,
                )
            rows = connection.execute(
                """SELECT * FROM task_events
                   WHERE task_id=? AND seq>? AND seq<=?
                   ORDER BY seq LIMIT ?""",
                (
                    task.task_id,
                    page_after_seq,
                    frozen_head,
                    limit + 1,
                ),
            ).fetchall()
            has_more = len(rows) > limit
            events = tuple(self._event_from_row(row) for row in rows[:limit])
            return TaskEventConsumerAuthorityPage(
                task=task,
                attempt=attempt,
                presentation_class=presentation_class,
                watermark=watermark,
                acked_event_id=acked_event_id,
                cursor_baseline=TaskEventConsumerCursorBaseline(
                    task_id=task.task_id,
                    scope=task.scope,
                    correlation_id=task.correlation_id,
                    watermark=watermark,
                    acked_event_id=acked_event_id,
                    lifecycle_event=(
                        None if cursor_lifecycle_row is None else self._event_from_row(cursor_lifecycle_row)
                    ),
                    attempt_boundary=(
                        None if cursor_boundary_row is None else self._event_from_row(cursor_boundary_row)
                    ),
                ),
                segment_start_seq=segment_start_seq,
                page_after_seq=page_after_seq,
                start_seq=page_after_seq + 1,
                head_seq=frozen_head,
                events=events,
                next_after_seq=events[-1].seq if has_more and events else None,
                has_more=has_more,
                terminal_head_event=(None if terminal_head_row is None else self._event_from_row(terminal_head_row)),
            )

    def ack_events(
        self,
        command: CommandEnvelope,
        *,
        observed_at: str,
    ) -> ResultEnvelope:
        """Persist the first exact class ACK without changing canonical Task truth."""

        fingerprint = command.fingerprint()
        with self._transaction() as connection:
            replay = self._command_replay(connection, command, fingerprint)
            if replay is not None:
                if replay.ok:
                    self._verify_v5_ack_semantics(
                        connection,
                        self._v5_consumer_rows(connection),
                    )
                return replay
            try:
                reparsed = CommandEnvelope.from_dict(command.to_dict())
            except ContractViolation as error:
                raise FormalTaskViolation(
                    "TASK_ACK_INVALID",
                    "task.ack_events command is not canonical",
                    ErrorCode.INVALID_ARGUMENT,
                ) from error
            payload = reparsed.payload
            if (
                reparsed.command_type != "task.ack_events"
                or reparsed.required_capabilities != ("task.ack_events",)
                or set(payload)
                != {
                    "presentation_class",
                    "acked_through_seq",
                    "acked_event_id",
                    "expected_event_head",
                }
            ):
                raise FormalTaskViolation(
                    "TASK_ACK_INVALID",
                    "task.ack_events requires one exact ACK authority payload",
                    ErrorCode.INVALID_ARGUMENT,
                )
            task_id = reparsed.target_ref.id
            task = self._require_consumer_task_row(connection, task_id, reparsed.scope)
            acked_through_seq = payload["acked_through_seq"]
            expected_event_head = payload["expected_event_head"]
            current_event_head = int(task["event_head"])
            if not (
                type(acked_through_seq) is int
                and type(expected_event_head) is int
                and 0 <= acked_through_seq <= expected_event_head <= current_event_head
            ):
                result = self._persist_business_decision(
                    connection,
                    command,
                    fingerprint,
                    disposition=TaskCommandDisposition.CONFLICT,
                    code=ErrorCode.STALE,
                    reason="TASK_ACK_PRECONDITION_STALE",
                    message=("task ACK is beyond its observed or current event head"),
                    observed_at=observed_at,
                )
                self._hit("ack_events.before_commit")
                return result
            event = connection.execute(
                """SELECT event_id, occurred_at FROM task_events
                   WHERE task_id=? AND seq=? AND event_id=?""",
                (
                    task_id,
                    acked_through_seq,
                    payload["acked_event_id"],
                ),
            ).fetchone()
            if event is None:
                result = self._persist_business_decision(
                    connection,
                    command,
                    fingerprint,
                    disposition=TaskCommandDisposition.CONFLICT,
                    code=ErrorCode.CONFLICT,
                    reason="TASK_ACK_EVENT_MISMATCH",
                    message="task ACK does not bind the exact retained event",
                    observed_at=observed_at,
                )
                self._hit("ack_events.before_commit")
                return result
            observed_time = _canonical_utc_order_key(observed_at)
            event_time = _canonical_utc_order_key(event["occurred_at"])
            if observed_time is None:
                raise FormalTaskViolation(
                    "TASK_ACK_INVALID",
                    "task ACK observation time is not canonical UTC",
                    ErrorCode.INVALID_ARGUMENT,
                )
            if event_time is None:
                raise self._corrupt("formal Task ACK event timestamp is not canonical")
            if observed_time <= event_time:
                result = self._persist_business_decision(
                    connection,
                    command,
                    fingerprint,
                    disposition=TaskCommandDisposition.CONFLICT,
                    code=ErrorCode.STALE,
                    reason="TASK_ACK_PRECONDITION_STALE",
                    message="task ACK does not postdate its retained event",
                    observed_at=observed_at,
                )
                self._hit("ack_events.before_commit")
                return result
            existing = connection.execute(
                """SELECT * FROM task_event_consumption
                   WHERE subject_id=? AND project_id=? AND task_id=?
                     AND presentation_class=?""",
                (
                    reparsed.scope.subject_id,
                    reparsed.scope.project_id,
                    task_id,
                    payload["presentation_class"],
                ),
            ).fetchone()
            histories = self._verify_v5_ack_semantics(
                connection,
                self._v5_consumer_rows(connection),
            )
            consumer_key = (
                reparsed.scope.subject_id,
                reparsed.scope.project_id,
                task_id,
                payload["presentation_class"],
            )
            history = histories.get(consumer_key)
            legacy_anchor = None if existing is None else self._legacy_consumption_anchor_v1(connection, existing)
            if (
                legacy_anchor is not None
                and history is None
                and (acked_through_seq != legacy_anchor[1] or payload["acked_event_id"] != legacy_anchor[2])
            ):
                result = self._persist_business_decision(
                    connection,
                    command,
                    fingerprint,
                    disposition=TaskCommandDisposition.CONFLICT,
                    code=ErrorCode.STALE,
                    reason="TASK_ACK_PRECONDITION_STALE",
                    message="task ACK requires exact legacy seed adoption",
                    observed_at=observed_at,
                )
                self._hit("ack_events.before_commit")
                return result
            if history is not None:
                origin = history[0]
                previous = (
                    history[1],
                    history[2],
                    history[3],
                    history[4],
                )
            elif legacy_anchor is not None:
                origin = _LEGACY_CONSUMPTION_SEED_TYPE
                previous = (
                    legacy_anchor[1],
                    legacy_anchor[2],
                    legacy_anchor[3],
                    None,
                )
            else:
                origin = _RUNTIME_CONSUMPTION_ORIGIN
                previous = (-1, None, None, None)
            result_value, current_history = self._ack_history_step(
                reparsed,
                presentation_class=payload["presentation_class"],
                observed_at=observed_at,
                origin=origin,
                previous=previous,
            )
            advanced = bool(result_value["advanced"])
            if existing is None:
                connection.execute(
                    """INSERT INTO task_event_consumption(
                           subject_id, project_id, task_id, presentation_class,
                           acked_through_seq, acked_event_id, updated_at)
                       VALUES(?, ?, ?, ?, ?, ?, ?)""",
                    (
                        reparsed.scope.subject_id,
                        reparsed.scope.project_id,
                        task_id,
                        payload["presentation_class"],
                        current_history[1],
                        current_history[2],
                        current_history[3],
                    ),
                )
            elif advanced:
                connection.execute(
                    """UPDATE task_event_consumption
                       SET acked_through_seq=?, acked_event_id=?, updated_at=?
                       WHERE subject_id=? AND project_id=? AND task_id=?
                         AND presentation_class=?""",
                    (
                        current_history[1],
                        current_history[2],
                        current_history[3],
                        reparsed.scope.subject_id,
                        reparsed.scope.project_id,
                        task_id,
                        payload["presentation_class"],
                    ),
                )
            result = ResultEnvelope.success(
                owner=command,
                result=result_value,
                observed_at=observed_at,
                extensions=command_result_extensions(TaskCommandDisposition.APPLIED),
            )
            self._insert_command(
                connection,
                command,
                fingerprint,
                _scope_key(command.scope),
                result,
                observed_at,
            )
            self._hit("ack_events.before_commit")
            return result

    def event_authority_snapshot(self, task_id: str, scope: ScopeRef, *, max_events: int) -> TaskEventAuthoritySnapshot:
        return self._event_authority_snapshot(
            task_id,
            scope,
            max_events=max_events,
            consumer_scope=False,
        )

    def consumer_event_authority_snapshot(
        self, task_id: str, scope: ScopeRef, *, max_events: int
    ) -> TaskEventAuthoritySnapshot:
        """Read one atomic event prefix for a stable presentation consumer."""

        return self._event_authority_snapshot(
            task_id,
            scope,
            max_events=max_events,
            consumer_scope=True,
        )

    def _event_authority_snapshot(
        self,
        task_id: str,
        scope: ScopeRef,
        *,
        max_events: int,
        consumer_scope: bool,
    ) -> TaskEventAuthoritySnapshot:
        """Read task, attempt, and the exact durable prefix in one SQLite snapshot."""

        if type(max_events) is not int or max_events <= 0:
            raise FormalTaskViolation(
                "INVALID_TASK_EVENT_AUTHORITY_CAPACITY",
                "TaskEvent authority capacity must be a positive integer",
                ErrorCode.INVALID_ARGUMENT,
            )
        with self._reader() as connection:
            # The explicit read transaction pins every following SELECT to one
            # WAL snapshot. Concurrent appends are recovered later from cursor;
            # they can neither leak into nor create a hole inside this prefix.
            connection.execute("BEGIN")
            task_row = (
                self._require_consumer_task_row(connection, task_id, scope)
                if consumer_scope
                else self._require_task_row(connection, task_id, scope)
            )
            self._verify_durable_lineage(connection, task_row)
            task = self._task_from_row(task_row)
            attempt_row = connection.execute("SELECT * FROM attempts WHERE attempt_id=?", (task.attempt_id,)).fetchone()
            if attempt_row is None:
                raise FormalTaskViolation(
                    "TASK_STORE_CORRUPT",
                    "formal Task Store is missing the bound attempt",
                    ErrorCode.INTERNAL,
                )
            attempt = self._attempt_from_row(attempt_row)
            boundary_row = connection.execute(
                """
                SELECT MIN(seq) AS start_seq FROM task_events
                WHERE task_id=? AND attempt_id=?
                """,
                (task.task_id, task.attempt_id),
            ).fetchone()
            if boundary_row is None or boundary_row["start_seq"] is None:
                raise FormalTaskViolation(
                    "TASK_STORE_CORRUPT",
                    "formal Task Store is missing the current attempt segment",
                    ErrorCode.INTERNAL,
                )
            start_seq = int(boundary_row["start_seq"])
            if task.event_head - start_seq + 1 > max_events:
                raise FormalTaskViolation(
                    "TASK_EVENT_AUTHORITY_PREFIX_CAPACITY",
                    "TaskEvent authority segment exceeds the declared reader capacity",
                    ErrorCode.UNAVAILABLE,
                )
            self._hit("event_authority_snapshot.before_events")
            event_rows = connection.execute(
                """
                SELECT * FROM task_events
                WHERE task_id=? AND attempt_id=? AND seq>=? AND seq<=?
                ORDER BY seq
                """,
                (task.task_id, task.attempt_id, start_seq, task.event_head),
            ).fetchall()
            events = tuple(self._event_from_row(row) for row in event_rows)
            if attempt.attempt_number > 1:
                if not events or events[0].event_type not in {
                    "task.retry_accepted",
                    "task.recovery_accepted",
                }:
                    raise FormalTaskViolation(
                        "TASK_STORE_CORRUPT",
                        "current successor segment lacks its durable authority boundary",
                        ErrorCode.INTERNAL,
                    )
                boundary = events[0]
                is_recovery = boundary.event_type == "task.recovery_accepted"
                predecessor_attempt_id = boundary.details.get(
                    "producer_attempt_id" if is_recovery else "retry_of_attempt_id"
                )
                previous_outcome = boundary.details.get("producer_outcome" if is_recovery else "previous_outcome")
                predecessor_row = connection.execute(
                    "SELECT * FROM attempts WHERE attempt_id=?",
                    (predecessor_attempt_id,),
                ).fetchone()
                recovery_row = (
                    connection.execute(
                        """SELECT * FROM durability_recoveries
                           WHERE recovery_attempt_id=? AND task_id=?""",
                        (attempt.attempt_id, task.task_id),
                    ).fetchone()
                    if is_recovery
                    else None
                )
                if (
                    predecessor_row is None
                    or predecessor_row["task_id"] != task.task_id
                    or int(predecessor_row["attempt_number"]) != attempt.attempt_number - 1
                    or predecessor_row["state"] != FormalAttemptState.TERMINAL.value
                    or predecessor_row["outcome"] != previous_outcome
                    or previous_outcome
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
                            recovery_row is None
                            or recovery_row["producer_attempt_id"] != predecessor_attempt_id
                            or recovery_row["recovery_id"] != boundary.details.get("recovery_id")
                            or recovery_row["recovery_generation"] != attempt.attempt_number - 1
                            or boundary.details.get("recovery_generation") != attempt.attempt_number - 1
                            or boundary.details.get("recovery_budget_remaining") != 3 - attempt.attempt_number
                        )
                    )
                ):
                    raise FormalTaskViolation(
                        "TASK_STORE_CORRUPT",
                        "successor authority boundary does not match its durable predecessor",
                        ErrorCode.INTERNAL,
                    )
            return TaskEventAuthoritySnapshot(
                task=task,
                attempt=attempt,
                events=events,
                cursor=task.event_head,
                start_seq=start_seq,
            )

    def nonterminal_attempts(
        self,
    ) -> tuple[tuple[PersistentTaskRecord, PersistentAttemptRecord], ...]:
        with self._reader() as connection:
            rows = connection.execute(
                """
                SELECT t.*, a.attempt_id AS a_attempt_id, a.task_id AS a_task_id,
                    a.executor_id AS a_executor_id, a.executor_ref AS a_executor_ref,
                    a.attempt_number AS a_attempt_number,
                    a.state AS a_state, a.outcome AS a_outcome,
                    a.source_seq AS a_source_seq, a.updated_at AS a_updated_at,
                    a.adapter_id, a.capability_profile_json,
                    a.capability_profile_digest, a.execution_requirements_json,
                    a.admission_priority, a.admission_reason,
                    a.admission_attempt_count, a.admission_next_eligible_at,
                    a.admission_deadline_at, a.admission_enqueued_at
                FROM tasks t JOIN attempts a ON a.attempt_id=t.attempt_id
                WHERE t.state<>? ORDER BY t.created_at, t.task_id
                """,
                (FormalTaskState.TERMINAL.value,),
            ).fetchall()
            result = []
            for row in rows:
                attempt = PersistentAttemptRecord(
                    row["a_attempt_id"],
                    row["a_task_id"],
                    row["a_executor_id"],
                    row["a_executor_ref"],
                    FormalAttemptState(row["a_state"]),
                    (None if row["a_outcome"] is None else TerminalOutcome(row["a_outcome"])),
                    int(row["a_source_seq"]),
                    int(row["a_attempt_number"]),
                    _selection_from_attempt_row(row),
                )
                result.append((self._task_from_row(row), attempt))
            return tuple(result)

    def counts(self) -> dict[str, int]:
        with self._reader() as connection:
            return {
                table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in (
                    "commands",
                    "tasks",
                    "attempts",
                    "task_events",
                    "executor_events",
                    "outbox",
                )
            }

    @staticmethod
    def _require_task_row(connection: sqlite3.Connection, task_id: str, scope: ScopeRef) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM tasks WHERE task_id=? AND scope_key=?",
            (task_id, _scope_key(scope)),
        ).fetchone()
        if row is None:
            raise FormalTaskViolation(
                "TASK_NOT_FOUND",
                "task is unavailable in the authorized scope",
                ErrorCode.NOT_FOUND,
            )
        _task_binding_from_row(row)
        return row

    @staticmethod
    def _require_task_row_by_id(connection: sqlite3.Connection, task_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            raise FormalTaskViolation("TASK_NOT_FOUND", "task is unavailable", ErrorCode.NOT_FOUND)
        _task_binding_from_row(row)
        return row

    @classmethod
    def _require_consumer_task_row(
        cls,
        connection: sqlite3.Connection,
        task_id: str,
        scope: ScopeRef,
    ) -> sqlite3.Row:
        """Authorize one stable subject/project consumer independent of Session."""

        row = cls._require_task_row_by_id(connection, task_id)
        stored_scope, _spec = _task_binding_from_row(row)
        if (
            not isinstance(scope, ScopeRef)
            or scope.assurance is not Assurance.AUTHENTICATED
            or stored_scope.assurance is not Assurance.AUTHENTICATED
            or (scope.subject_id, scope.project_id) != (stored_scope.subject_id, stored_scope.project_id)
        ):
            raise FormalTaskViolation(
                "TASK_NOT_FOUND",
                "task is unavailable in the authorized consumer scope",
                ErrorCode.NOT_FOUND,
            )
        return row

    @staticmethod
    def _task_from_row(row: sqlite3.Row) -> PersistentTaskRecord:
        def load() -> PersistentTaskRecord:
            reconciliation_state = row["reconciliation_state"]
            scope, spec = _task_binding_from_row(row)
            return PersistentTaskRecord(
                task_id=row["task_id"],
                scope=scope,
                spec=spec,
                state=FormalTaskState(row["state"]),
                attempt_id=row["attempt_id"],
                correlation_id=row["correlation_id"],
                cancel_requested=bool(row["cancel_requested"]),
                dispatch_fenced=bool(row["dispatch_fenced"]),
                outcome=(None if row["outcome"] is None else TerminalOutcome(row["outcome"])),
                reconciliation_state=(
                    None if reconciliation_state is None else ReconciliationState(reconciliation_state)
                ),
                reconciliation_reason=row["reconciliation_reason"],
                create_command_id=row["create_command_id"],
                predecessor_task_id=row["predecessor_task_id"],
                revision_number=int(row["revision_number"]),
                event_head=int(row["event_head"]),
            )

        return _stored_record("task", load)

    @staticmethod
    def _attempt_from_row(row: sqlite3.Row) -> PersistentAttemptRecord:
        return _stored_record(
            "attempt",
            lambda: PersistentAttemptRecord(
                attempt_id=row["attempt_id"],
                task_id=row["task_id"],
                executor_id=row["executor_id"],
                executor_ref=row["executor_ref"],
                state=FormalAttemptState(row["state"]),
                outcome=(None if row["outcome"] is None else TerminalOutcome(row["outcome"])),
                source_seq=int(row["source_seq"]),
                attempt_number=int(row["attempt_number"]),
                selection=_selection_from_attempt_row(row),
            ),
        )

    @staticmethod
    def _event_from_row(row: sqlite3.Row) -> PersistentTaskEvent:
        def load() -> PersistentTaskEvent:
            details = _json_load(row["details_json"])
            if type(details) is not dict:
                raise FormalTaskViolation(
                    "TASK_STORE_CORRUPT",
                    "formal Task Store contains invalid event details",
                    ErrorCode.INTERNAL,
                )
            return PersistentTaskEvent(
                event_id=row["event_id"],
                task_id=row["task_id"],
                attempt_id=row["attempt_id"],
                scope=ScopeRef.from_dict(_json_load(row["scope_json"])),
                seq=int(row["seq"]),
                event_type=row["event_type"],
                state=row["state"],
                outcome=row["outcome"],
                producer=row["producer"],
                source_event_id=row["source_event_id"],
                causation_id=row["causation_id"],
                correlation_id=row["correlation_id"],
                occurred_at=row["occurred_at"],
                details=details,
            )

        return _stored_record("event", load)

    @classmethod
    def _outbox_from_row(cls, connection: sqlite3.Connection, row: sqlite3.Row) -> PersistentOutboxItem:
        def load() -> PersistentOutboxItem:
            kind = OutboxKind(row["kind"])
            payload = _json_load(row["payload_json"])
            expected_payload_keys = {"scope", "spec", "executor_ref"}
            if kind is OutboxKind.ATTEMPT_ADJUST:
                expected_payload_keys.add("adjustment")
            if type(payload) is not dict or set(payload) != expected_payload_keys:
                raise FormalTaskViolation(
                    "TASK_STORE_CORRUPT",
                    "formal Task Store contains an invalid outbox payload",
                    ErrorCode.INTERNAL,
                )
            row_keys = set(row.keys())
            attempt_selection: PersistedExecutorSelection | None = None
            attempt_row: sqlite3.Row | None = None
            scope = ScopeRef.from_dict(payload["scope"])
            spec = FormalTaskSpec.from_dict(payload["spec"])
            adjustment = (
                TaskAdjustmentRequest.from_dict(payload["adjustment"]) if kind is OutboxKind.ATTEMPT_ADJUST else None
            )
            if spec.context.scope != scope:
                raise FormalTaskViolation(
                    "OUTBOX_BINDING_MISMATCH",
                    "outbox context does not match its stored scope",
                    ErrorCode.PROTOCOL_VIOLATION,
                )
            if {
                "canonical_attempt_id",
                "attempt_task_id",
                "bound_executor_id",
                "bound_attempt_number",
                "canonical_task_id",
                "task_attempt_id",
                "task_scope_key",
                "task_scope_json",
                "task_spec_json",
                "bound_attempt_state",
                "bound_attempt_outcome",
                "predecessor_attempt_id",
                "predecessor_task_id",
                "predecessor_attempt_number",
                "predecessor_attempt_state",
                "predecessor_attempt_outcome",
                "bound_task_state",
                "bound_task_outcome",
                "task_correlation_id",
                "task_event_head",
                "task_cancel_requested",
                "task_dispatch_fenced",
                "canonical_command_id",
                "bound_command_type",
                "command_scope_key",
                "command_fingerprint",
                "command_result_json",
                "cancel_event_id",
                "cancel_event_task_id",
                "cancel_event_attempt_id",
                "cancel_event_scope_json",
                "cancel_event_type",
                "cancel_event_state",
                "cancel_event_outcome",
                "cancel_event_producer",
                "cancel_event_source_event_id",
                "cancel_event_causation_id",
                "cancel_event_correlation_id",
                "cancel_event_occurred_at",
                "cancel_event_details_json",
                "cancel_event_seq",
                "cancel_event_count",
                "retry_event_id",
                "retry_event_task_id",
                "retry_event_attempt_id",
                "retry_event_scope_json",
                "retry_event_type",
                "retry_event_state",
                "retry_event_outcome",
                "retry_event_producer",
                "retry_event_source_event_id",
                "retry_event_causation_id",
                "retry_event_correlation_id",
                "retry_event_occurred_at",
                "retry_event_details_json",
                "retry_event_seq",
                "retry_event_count",
                "retry_segment_start_seq",
                "adjust_event_id",
                "adjust_event_task_id",
                "adjust_event_attempt_id",
                "adjust_event_scope_json",
                "adjust_event_type",
                "adjust_event_state",
                "adjust_event_outcome",
                "adjust_event_producer",
                "adjust_event_source_event_id",
                "adjust_event_causation_id",
                "adjust_event_correlation_id",
                "adjust_event_occurred_at",
                "adjust_event_details_json",
                "adjust_event_seq",
                "adjust_event_count",
            }.issubset(row_keys):
                recovery_row = connection.execute(
                    """SELECT * FROM durability_recoveries
                       WHERE task_id=? AND recovery_attempt_id=?""",
                    (row["task_id"], row["attempt_id"]),
                ).fetchone()
                is_recovery_dispatch = (
                    kind is OutboxKind.ATTEMPT_DISPATCH
                    and recovery_row is not None
                    and recovery_row["recovery_id"] == row["command_id"]
                )
                if (
                    row["canonical_attempt_id"] is None
                    or row["canonical_task_id"] is None
                    or (row["canonical_command_id"] is None and not is_recovery_dispatch)
                    or row["attempt_task_id"] != row["task_id"]
                    or row["task_attempt_id"] != row["attempt_id"]
                ):
                    raise FormalTaskViolation(
                        "OUTBOX_BINDING_MISMATCH",
                        "outbox task and attempt do not have an exact canonical binding",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
                task_scope = ScopeRef.from_dict(_json_load(row["task_scope_json"]))
                task_spec = FormalTaskSpec.from_dict(_json_load(row["task_spec_json"]))
                if (
                    scope != task_scope
                    or row["task_scope_key"] != _scope_key(task_scope)
                    or spec != task_spec
                    or row["bound_executor_id"] != spec.executor_id
                    or payload["executor_ref"] != row["bound_executor_ref"]
                ):
                    raise FormalTaskViolation(
                        "OUTBOX_BINDING_MISMATCH",
                        "outbox scope or Executor does not match its canonical binding",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
                attempt_row = connection.execute(
                    "SELECT * FROM attempts WHERE attempt_id=?",
                    (row["attempt_id"],),
                ).fetchone()
                if attempt_row is None:
                    raise FormalTaskViolation(
                        "OUTBOX_BINDING_MISMATCH",
                        "outbox lost its persisted executor selection",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
                attempt_selection = _selection_from_attempt_row(attempt_row)
                if is_recovery_dispatch:
                    producer_row = connection.execute(
                        "SELECT * FROM attempts WHERE attempt_id=? AND task_id=?",
                        (
                            recovery_row["producer_attempt_id"],
                            row["task_id"],
                        ),
                    ).fetchone()
                    if (
                        producer_row is None
                        or row["canonical_command_id"] is not None
                        or row["bound_command_type"] is not None
                        or row["command_scope_key"] is not None
                        or row["command_fingerprint"] is not None
                        or row["command_result_json"] is not None
                        or _selection_from_attempt_row(producer_row) != attempt_selection
                        or FormalTaskState(row["bound_task_state"]) is not FormalTaskState.ACCEPTED
                        or FormalAttemptState(row["bound_attempt_state"]) is not FormalAttemptState.ACCEPTED
                        or row["bound_task_outcome"] is not None
                        or row["bound_attempt_outcome"] is not None
                        or row["bound_executor_ref"] is not None
                        or bool(row["task_cancel_requested"])
                        or bool(row["task_dispatch_fenced"])
                    ):
                        raise FormalTaskViolation(
                            "OUTBOX_COMMAND_BINDING_MISMATCH",
                            "recovery dispatch does not match Store continuation facts",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    return PersistentOutboxItem(
                        outbox_id=row["outbox_id"],
                        kind=kind,
                        task_id=row["task_id"],
                        attempt_id=row["attempt_id"],
                        command_id=row["command_id"],
                        scope=scope,
                        spec=spec,
                        executor_ref=payload["executor_ref"],
                        source_seq=int(row["bound_source_seq"]),
                        state=OutboxState(row["state"]),
                        delivery_count=int(row["delivery_count"]),
                        claim_token=row["claim_token"],
                        selection=attempt_selection,
                        admission=PersistentAdmissionRecord(
                            task_id=row["task_id"],
                            attempt_id=row["attempt_id"],
                            priority=attempt_selection.admission_priority,
                            reason=attempt_row["admission_reason"],
                            attempt_count=int(attempt_row["admission_attempt_count"]),
                            next_eligible_at=attempt_row["admission_next_eligible_at"],
                            deadline_at=attempt_row["admission_deadline_at"],
                            enqueued_at=attempt_row["admission_enqueued_at"],
                            queued=row["state"] == OutboxState.PENDING.value,
                        ),
                    )
                stored_fingerprint = _json_load(row["command_fingerprint"])
                fingerprint_selection: PersistedExecutorSelection | None = None
                mutation_precondition: TaskMutationPrecondition | None = None
                if kind is OutboxKind.ATTEMPT_DISPATCH and row["bound_command_type"] in {
                    "task.create",
                    "task.create_successor",
                }:
                    allowed_keys = {
                        frozenset({"command", "resolved_spec"}),
                        frozenset({"command", "resolved_spec", "executor_selection"}),
                    }
                    if type(stored_fingerprint) is not dict or frozenset(stored_fingerprint) not in allowed_keys:
                        raise FormalTaskViolation(
                            "OUTBOX_COMMAND_BINDING_MISMATCH",
                            "dispatch outbox lacks its exact create command binding",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    command_payload = stored_fingerprint["command"]
                    resolved_spec = FormalTaskSpec.from_dict(stored_fingerprint["resolved_spec"])
                    if "executor_selection" in stored_fingerprint:
                        fingerprint_selection = _selection_from_fingerprint_payload(
                            stored_fingerprint["executor_selection"]
                        )
                    if resolved_spec != spec:
                        task_row = connection.execute(
                            "SELECT * FROM tasks WHERE task_id=?",
                            (row["task_id"],),
                        ).fetchone()
                        if task_row is None:
                            raise FormalTaskViolation(
                                "OUTBOX_COMMAND_BINDING_MISMATCH",
                                "dispatch update lost its canonical task",
                                ErrorCode.PROTOCOL_VIOLATION,
                            )
                        cls._verify_durable_lineage(connection, task_row)
                elif (
                    kind is OutboxKind.ATTEMPT_DISPATCH
                    and row["bound_command_type"] == "task.retry"
                    and type(stored_fingerprint) is dict
                    and set(stored_fingerprint) == {"command", "executor_selection"}
                ):
                    command_payload = stored_fingerprint["command"]
                    fingerprint_selection = _selection_from_fingerprint_payload(
                        stored_fingerprint["executor_selection"]
                    )
                elif (
                    kind in {OutboxKind.ATTEMPT_CANCEL, OutboxKind.ATTEMPT_ADJUST}
                    and type(stored_fingerprint) is dict
                    and set(stored_fingerprint) == {"command", "mutation_precondition"}
                ):
                    command_payload = stored_fingerprint["command"]
                    mutation_precondition = TaskMutationPrecondition.from_dict(
                        stored_fingerprint["mutation_precondition"]
                    )
                else:
                    command_payload = stored_fingerprint
                if kind is OutboxKind.ATTEMPT_DISPATCH and fingerprint_selection is not None:
                    task_row = connection.execute(
                        "SELECT * FROM tasks WHERE task_id=?",
                        (row["task_id"],),
                    ).fetchone()
                    if task_row is None:
                        raise FormalTaskViolation(
                            "OUTBOX_COMMAND_BINDING_MISMATCH",
                            "selected dispatch lost its canonical Task",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    cls._verify_durable_lineage(connection, task_row)
                    current_priority = fingerprint_selection.admission_priority
                    reprioritize_rows = connection.execute(
                        """SELECT details_json FROM task_events
                           WHERE attempt_id=?
                             AND event_type='task.reprioritize_applied'
                           ORDER BY seq""",
                        (row["attempt_id"],),
                    ).fetchall()
                    for reprioritize_row in reprioritize_rows:
                        details = _json_load(reprioritize_row["details_json"])
                        current_priority = AdmissionPriority(details["priority"])
                    fingerprint_selection = replace(
                        fingerprint_selection,
                        admission_priority=current_priority,
                    )
                if kind is OutboxKind.ATTEMPT_DISPATCH and fingerprint_selection != attempt_selection:
                    raise FormalTaskViolation(
                        "OUTBOX_COMMAND_BINDING_MISMATCH",
                        "dispatch selection differs from its immutable Attempt",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
                if type(command_payload) is not dict or "request_id" in command_payload:
                    raise FormalTaskViolation(
                        "OUTBOX_COMMAND_BINDING_MISMATCH",
                        "outbox command fingerprint is invalid",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
                command = CommandEnvelope.from_dict({"request_id": "task-store-validation", **command_payload})
                result = ResultEnvelope.from_dict(_json_load(row["command_result_json"]))
                expected_command_type = (
                    row["bound_command_type"]
                    if kind is OutboxKind.ATTEMPT_DISPATCH
                    else ("task.cancel" if kind is OutboxKind.ATTEMPT_CANCEL else "task.adjust")
                )
                if (
                    expected_command_type not in {"task.create", "task.create_successor", "task.retry"}
                    and kind is OutboxKind.ATTEMPT_DISPATCH
                ) or (
                    row["bound_command_type"] != expected_command_type
                    or row["command_scope_key"] != row["task_scope_key"]
                    or command.command_id != row["command_id"]
                    or command.command_type != expected_command_type
                    or command.scope != scope
                    or tuple(command.required_capabilities) != (expected_command_type,)
                    or (
                        kind in {OutboxKind.ATTEMPT_CANCEL, OutboxKind.ATTEMPT_ADJUST}
                        and cls._mutation_fingerprint(command, mutation_precondition) != row["command_fingerprint"]
                    )
                    or not result.ok
                    or result.command_id != row["command_id"]
                ):
                    raise FormalTaskViolation(
                        "OUTBOX_COMMAND_BINDING_MISMATCH",
                        "outbox does not match its canonical command ledger entry",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
                command_result = result.result
                if kind is OutboxKind.ATTEMPT_DISPATCH and expected_command_type == "task.create":
                    expected_payload = {
                        "name": resolved_spec.name,
                        "instruction": resolved_spec.instruction,
                        "executor_id": resolved_spec.executor_id,
                        "side_effect_class": resolved_spec.side_effect_class,
                        "attributes": dict(resolved_spec.attributes),
                        **source_extension(resolved_spec.native_source),
                    }
                    if (
                        command.target_ref.id != f"create:{row['command_id']}"
                        or command.payload not in ({}, expected_payload)
                        or command.origin != resolved_spec.origin
                        or type(command_result) is not dict
                        or set(command_result) != {"task_id", "attempt_id", "state", "outbox_id"}
                        or command_result["task_id"] != row["task_id"]
                        or command_result["attempt_id"] != row["attempt_id"]
                        or command_result["state"] != FormalTaskState.ACCEPTED.value
                        or command_result["outbox_id"] != row["outbox_id"]
                    ):
                        raise FormalTaskViolation(
                            "OUTBOX_COMMAND_BINDING_MISMATCH",
                            "dispatch outbox does not match its create command facts",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                elif kind is OutboxKind.ATTEMPT_DISPATCH and expected_command_type == "task.create_successor":
                    task_row = connection.execute("SELECT * FROM tasks WHERE task_id=?", (row["task_id"],)).fetchone()
                    if task_row is None:
                        raise FormalTaskViolation(
                            "OUTBOX_COMMAND_BINDING_MISMATCH",
                            "successor dispatch lost its canonical Task",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    cls._verify_durable_lineage(connection, task_row)
                    if (
                        type(command_result) is not dict
                        or set(command_result)
                        != {
                            "task_id",
                            "predecessor_task_id",
                            "revision_number",
                            "attempt_id",
                            "state",
                            "outbox_id",
                        }
                        or command_result["task_id"] != row["task_id"]
                        or command_result["attempt_id"] != row["attempt_id"]
                        or command_result["state"] != FormalTaskState.ACCEPTED.value
                        or command_result["outbox_id"] != row["outbox_id"]
                    ):
                        raise FormalTaskViolation(
                            "OUTBOX_COMMAND_BINDING_MISMATCH",
                            "successor dispatch does not match its command facts",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                elif kind is OutboxKind.ATTEMPT_DISPATCH and expected_command_type == "task.retry":
                    TaskRetryProductRequestFingerprint.from_extensions(command.extensions)
                    expected_number = int(row["bound_attempt_number"])
                    if (
                        command.target_ref.id != row["task_id"]
                        or command.payload.get("attempt_number") != expected_number
                        or type(command_result) is not dict
                        or set(command_result)
                        != {
                            "task_id",
                            "previous_attempt_id",
                            "attempt_id",
                            "attempt_number",
                            "applied",
                            "state",
                            "outbox_id",
                        }
                        or command_result["task_id"] != row["task_id"]
                        or command_result["previous_attempt_id"] != command.payload.get("previous_attempt_id")
                        or command_result["attempt_id"] != row["attempt_id"]
                        or command_result["attempt_number"] != expected_number
                        or command_result["applied"] is not True
                        or command_result["state"] != FormalTaskState.ACCEPTED.value
                        or command_result["outbox_id"] != row["outbox_id"]
                    ):
                        raise FormalTaskViolation(
                            "OUTBOX_COMMAND_BINDING_MISMATCH",
                            "dispatch outbox does not match its retry command facts",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    if (
                        int(row["retry_event_count"]) != 1
                        or type(row["retry_event_id"]) is not str
                        or not row["retry_event_id"].strip()
                        or row["retry_segment_start_seq"] is None
                        or row["predecessor_attempt_id"] is None
                    ):
                        raise FormalTaskViolation(
                            "OUTBOX_COMMAND_BINDING_MISMATCH",
                            "retry dispatch lacks one exact durable lineage boundary",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    retry_event_scope = ScopeRef.from_dict(_json_load(row["retry_event_scope_json"]))
                    retry_event_details = _json_load(row["retry_event_details_json"])
                    retry_event_state = FormalTaskState(row["retry_event_state"])
                    retry_event_seq = int(row["retry_event_seq"])
                    predecessor_state = FormalAttemptState(row["predecessor_attempt_state"])
                    expected_previous_attempt_id = command.payload.get("previous_attempt_id")
                    expected_previous_outcome = command.payload.get("previous_outcome")
                    if (
                        expected_number not in {2, 3}
                        or row["predecessor_attempt_id"] != expected_previous_attempt_id
                        or row["predecessor_attempt_id"] != command_result["previous_attempt_id"]
                        or row["predecessor_task_id"] != row["task_id"]
                        or int(row["predecessor_attempt_number"]) != expected_number - 1
                        or predecessor_state is not FormalAttemptState.TERMINAL
                        or row["predecessor_attempt_outcome"] != expected_previous_outcome
                        or expected_previous_outcome
                        not in {
                            TerminalOutcome.CANCELLED.value,
                            TerminalOutcome.COMPLETED.value,
                        }
                        or row["retry_event_task_id"] != row["task_id"]
                        or row["retry_event_attempt_id"] != row["attempt_id"]
                        or retry_event_scope != scope
                        or row["retry_event_type"] != "task.retry_accepted"
                        or retry_event_state is not FormalTaskState.ACCEPTED
                        or row["retry_event_outcome"] is not None
                        or row["retry_event_producer"] != "task_core"
                        or row["retry_event_source_event_id"] is not None
                        or row["retry_event_causation_id"] != row["command_id"]
                        or row["retry_event_correlation_id"] != row["task_correlation_id"]
                        or row["retry_event_occurred_at"] != result.observed_at
                        or retry_event_details
                        != {
                            "command_id": row["command_id"],
                            "retry_of_attempt_id": expected_previous_attempt_id,
                            "previous_outcome": expected_previous_outcome,
                            "attempt_number": expected_number,
                        }
                        or retry_event_seq < 1
                        or retry_event_seq != int(row["retry_segment_start_seq"])
                        or retry_event_seq > int(row["task_event_head"])
                    ):
                        raise FormalTaskViolation(
                            "OUTBOX_COMMAND_BINDING_MISMATCH",
                            "retry dispatch does not match its durable lineage boundary",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                elif kind is OutboxKind.ATTEMPT_CANCEL:
                    cancel_result_current = (
                        type(command_result) is dict
                        and command_result.get("applied") is False
                        and dict(result.extensions)
                        == command_result_extensions(
                            TaskCommandDisposition.ACCEPTED,
                            admission_event_id=row["cancel_event_id"],
                        )
                    )
                    cancel_result_legacy = (
                        type(command_result) is dict
                        and command_result.get("applied") is True
                        and dict(result.extensions) == {}
                    )
                    if (
                        command.target_ref.id != row["task_id"]
                        or command.payload
                        or type(command_result) is not dict
                        or set(command_result)
                        != {
                            "task_id",
                            "attempt_id",
                            "cancel_acknowledged",
                            "applied",
                            "state",
                            "outbox_id",
                        }
                        or command_result["task_id"] != row["task_id"]
                        or command_result["attempt_id"] != row["attempt_id"]
                        or command_result["cancel_acknowledged"] is not True
                        or not (cancel_result_current or cancel_result_legacy)
                        or command_result["outbox_id"] != row["outbox_id"]
                    ):
                        raise FormalTaskViolation(
                            "OUTBOX_COMMAND_BINDING_MISMATCH",
                            "cancel outbox does not match its cancel command facts",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                elif kind is OutboxKind.ATTEMPT_ADJUST:
                    assert adjustment is not None
                    if (
                        command.target_ref.id != row["task_id"]
                        or command.payload
                        != {"adjustment": adjustment.adjustment, **source_extension(adjustment.native_source)}
                        or adjustment.adjustment_id != row["command_id"]
                        or type(command_result) is not dict
                        or set(command_result)
                        != {
                            "task_id",
                            "attempt_id",
                            "adjustment_id",
                            "adjustment_state",
                            "reason",
                            "outbox_id",
                        }
                        or command_result["task_id"] != row["task_id"]
                        or command_result["attempt_id"] != row["attempt_id"]
                        or command_result["adjustment_id"] != row["command_id"]
                        or command_result["adjustment_state"] != TaskAdjustmentState.PENDING.value
                        or command_result["reason"] is not None
                        or command_result["outbox_id"] != row["outbox_id"]
                    ):
                        raise FormalTaskViolation(
                            "OUTBOX_COMMAND_BINDING_MISMATCH",
                            "adjustment outbox does not match its command facts",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                if kind is OutboxKind.ATTEMPT_CANCEL:
                    if (
                        int(row["cancel_event_count"]) != 1
                        or type(row["cancel_event_id"]) is not str
                        or not row["cancel_event_id"].strip()
                    ):
                        raise FormalTaskViolation(
                            "OUTBOX_COMMAND_BINDING_MISMATCH",
                            "cancel outbox lacks one exact durable request event",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    cancel_event_scope = ScopeRef.from_dict(_json_load(row["cancel_event_scope_json"]))
                    cancel_event_details = _json_load(row["cancel_event_details_json"])
                    cancel_event_state = FormalTaskState(row["cancel_event_state"])
                    cancel_event_seq = int(row["cancel_event_seq"])
                    if (
                        row["cancel_event_task_id"] != row["task_id"]
                        or row["cancel_event_attempt_id"] != row["attempt_id"]
                        or cancel_event_scope != scope
                        or row["cancel_event_type"] != "task.cancel_requested"
                        or cancel_event_state is FormalTaskState.TERMINAL
                        or command_result["state"] != cancel_event_state.value
                        or row["cancel_event_outcome"] is not None
                        or row["cancel_event_producer"] != "task_core.control"
                        or row["cancel_event_source_event_id"] is not None
                        or row["cancel_event_causation_id"] != row["command_id"]
                        or row["cancel_event_correlation_id"] != row["task_correlation_id"]
                        or row["cancel_event_occurred_at"] != result.observed_at
                        or cancel_event_details != {"command_id": row["command_id"]}
                        or cancel_event_seq < 1
                        or cancel_event_seq > int(row["task_event_head"])
                        or (
                            mutation_precondition is not None
                            and (
                                mutation_precondition.task_id != row["task_id"]
                                or mutation_precondition.attempt_id != row["attempt_id"]
                                or mutation_precondition.expected_event_head != cancel_event_seq - 1
                            )
                        )
                    ):
                        raise FormalTaskViolation(
                            "OUTBOX_COMMAND_BINDING_MISMATCH",
                            "cancel result does not match its durable request event",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                elif kind is OutboxKind.ATTEMPT_ADJUST:
                    if (
                        int(row["adjust_event_count"]) != 1
                        or type(row["adjust_event_id"]) is not str
                        or not row["adjust_event_id"].strip()
                    ):
                        raise FormalTaskViolation(
                            "OUTBOX_COMMAND_BINDING_MISMATCH",
                            "adjustment outbox lacks one exact durable request event",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                    adjust_scope = ScopeRef.from_dict(_json_load(row["adjust_event_scope_json"]))
                    adjust_details = _json_load(row["adjust_event_details_json"])
                    adjust_state = FormalTaskState(row["adjust_event_state"])
                    adjust_seq = int(row["adjust_event_seq"])
                    if (
                        row["adjust_event_task_id"] != row["task_id"]
                        or row["adjust_event_attempt_id"] != row["attempt_id"]
                        or adjust_scope != scope
                        or row["adjust_event_type"] != "task.adjust_requested"
                        or adjust_state is FormalTaskState.TERMINAL
                        or row["adjust_event_outcome"] is not None
                        or row["adjust_event_producer"] != "task_core.control"
                        or row["adjust_event_source_event_id"] is not None
                        or row["adjust_event_causation_id"] != row["command_id"]
                        or row["adjust_event_correlation_id"] != row["task_correlation_id"]
                        or adjust_details != {"command_id": row["command_id"]}
                        or adjust_seq != adjustment.requested_seq
                        or adjust_seq > int(row["task_event_head"])
                        or (
                            mutation_precondition is not None
                            and (
                                mutation_precondition.task_id != row["task_id"]
                                or mutation_precondition.attempt_id != row["attempt_id"]
                                or mutation_precondition.expected_event_head != adjust_seq - 1
                            )
                        )
                    ):
                        raise FormalTaskViolation(
                            "OUTBOX_COMMAND_BINDING_MISMATCH",
                            "adjustment request does not match its durable event",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                task_state = FormalTaskState(row["bound_task_state"])
                attempt_state = FormalAttemptState(row["bound_attempt_state"])
                cancel_requested = row["task_cancel_requested"]
                dispatch_fenced = row["task_dispatch_fenced"]
                if (
                    cancel_requested not in {0, 1}
                    or dispatch_fenced not in {0, 1}
                    or task_state is FormalTaskState.TERMINAL
                    or row["bound_task_outcome"] is not None
                    or attempt_state is FormalAttemptState.TERMINAL
                    or row["bound_attempt_outcome"] is not None
                ):
                    raise FormalTaskViolation(
                        "OUTBOX_LIFECYCLE_MISMATCH",
                        "pending outbox does not match a deliverable task lifecycle",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
                if kind is OutboxKind.ATTEMPT_DISPATCH:
                    if (
                        task_state is not FormalTaskState.ACCEPTED
                        or attempt_state is not FormalAttemptState.ACCEPTED
                        or row["bound_executor_ref"] is not None
                        or bool(cancel_requested) != bool(dispatch_fenced)
                        or (bool(cancel_requested) and int(row["delivery_count"]) == 0)
                    ):
                        raise FormalTaskViolation(
                            "OUTBOX_LIFECYCLE_MISMATCH",
                            "dispatch outbox does not match its task lifecycle",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                elif kind is OutboxKind.ATTEMPT_CANCEL:
                    if (
                        not bool(cancel_requested)
                        or not bool(dispatch_fenced)
                        or (task_state is FormalTaskState.ACCEPTED) != (attempt_state is FormalAttemptState.ACCEPTED)
                        or (
                            task_state is not FormalTaskState.ACCEPTED
                            and attempt_state is not FormalAttemptState.RUNNING
                        )
                    ):
                        raise FormalTaskViolation(
                            "OUTBOX_LIFECYCLE_MISMATCH",
                            "cancel outbox lacks its durable cancellation lifecycle",
                            ErrorCode.PROTOCOL_VIOLATION,
                        )
                elif (task_state is FormalTaskState.ACCEPTED) != (attempt_state is FormalAttemptState.ACCEPTED) or (
                    task_state is not FormalTaskState.ACCEPTED and attempt_state is not FormalAttemptState.RUNNING
                ):
                    raise FormalTaskViolation(
                        "OUTBOX_LIFECYCLE_MISMATCH",
                        "adjustment outbox does not bind a nonterminal attempt",
                        ErrorCode.PROTOCOL_VIOLATION,
                    )
            source_seq = int(row["bound_source_seq"]) if "bound_source_seq" in row_keys else -1
            admission = (
                None
                if attempt_selection is None or attempt_row is None
                else PersistentAdmissionRecord(
                    task_id=row["task_id"],
                    attempt_id=row["attempt_id"],
                    priority=attempt_selection.admission_priority,
                    reason=attempt_row["admission_reason"],
                    attempt_count=int(attempt_row["admission_attempt_count"]),
                    next_eligible_at=attempt_row["admission_next_eligible_at"],
                    deadline_at=attempt_row["admission_deadline_at"],
                    enqueued_at=attempt_row["admission_enqueued_at"],
                    queued=(kind is OutboxKind.ATTEMPT_DISPATCH and row["state"] == OutboxState.PENDING.value),
                )
            )
            return PersistentOutboxItem(
                outbox_id=row["outbox_id"],
                kind=OutboxKind(row["kind"]),
                task_id=row["task_id"],
                attempt_id=row["attempt_id"],
                command_id=row["command_id"],
                scope=scope,
                spec=spec,
                executor_ref=payload["executor_ref"],
                source_seq=source_seq,
                state=OutboxState(row["state"]),
                delivery_count=int(row["delivery_count"]),
                claim_token=row["claim_token"],
                adjustment=adjustment,
                selection=attempt_selection,
                admission=admission,
            )

        return _stored_record("outbox", load)


__all__ = ["SqliteTaskStore"]
