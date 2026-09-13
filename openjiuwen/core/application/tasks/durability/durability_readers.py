# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Schema-neutral verified prefix readers for caller-owned durability rows.

The caller owns the database transaction, snapshot, and row extraction. These
helpers only validate a bounded in-memory prefix. They do not open a database,
write, manage a transaction, choose recovery, or invoke an external boundary.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Final, TypeAlias

from openjiuwen.core.application.tasks.contracts import (
    MAX_SAFE_INTEGER,
    Assurance,
    ScopeRef,
    canonical_json_bytes,
)
from openjiuwen.core.application.tasks.durability.durability_checkpoint import (
    MAX_D1_CHECKPOINT_WIRE_BYTES,
    D1Checkpoint,
)
from openjiuwen.core.application.tasks.durability.durability_effects import (
    EffectContinuationAuthorization,
    EffectDispatchReceipt,
    EffectFact,
    ExternalEffectBinding,
    ExternalEffectDispatch,
    ExternalEffectIntent,
    ExternalEffectObservation,
    ExternalEffectSettlement,
    effect_fact_from_bytes,
)
from openjiuwen.core.application.tasks.durability.durability_identity import (
    DurabilityIdentityViolation,
    DurabilityProfileBinding,
)

DURABILITY_PREFIX_CONTRACT_VERSION: Final = "live-voice.durability-prefix.v1"
MAX_DURABILITY_PREFIX_ROWS: Final = 1_024
MAX_DURABILITY_PREFIX_ITEM_BYTES: Final = MAX_D1_CHECKPOINT_WIRE_BYTES
MAX_DURABILITY_PREFIX_BYTES: Final = 8_388_608

_MAX_TEXT_BYTES = 512
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class DurabilityPrefixViolation(ValueError):
    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


def _text(value: object, field_name: str) -> str:
    if type(value) is not str or not value.strip():
        raise DurabilityPrefixViolation(
            "INVALID_DURABILITY_BINDING",
            f"{field_name} must be a non-empty exact string",
        )
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise DurabilityPrefixViolation(
            "INVALID_DURABILITY_BINDING",
            f"{field_name} must contain valid Unicode scalar values",
        ) from error
    if len(encoded) > _MAX_TEXT_BYTES:
        raise DurabilityPrefixViolation(
            "INVALID_DURABILITY_BINDING",
            f"{field_name} is outside the bounded range",
        )
    return value


def _scope(value: object) -> ScopeRef:
    if type(value) is not ScopeRef:
        raise DurabilityPrefixViolation(
            "INVALID_DURABILITY_BINDING",
            "durability read scope must be exact",
        )
    try:
        checked = ScopeRef.from_dict(value.to_dict())
    except (TypeError, ValueError) as error:
        raise DurabilityPrefixViolation(
            "INVALID_DURABILITY_BINDING",
            "durability read scope is invalid",
        ) from error
    if checked.assurance is not Assurance.AUTHENTICATED:
        raise DurabilityPrefixViolation(
            "INVALID_DURABILITY_BINDING",
            "durability read scope must be authenticated",
        )
    return checked


def _profile(value: object) -> DurabilityProfileBinding:
    if type(value) is not DurabilityProfileBinding:
        raise DurabilityPrefixViolation(
            "INVALID_DURABILITY_BINDING",
            "durability read profile must be exact",
        )
    try:
        return DurabilityProfileBinding.from_dict(value.to_dict())
    except DurabilityIdentityViolation as error:
        raise DurabilityPrefixViolation(
            "INVALID_DURABILITY_BINDING",
            "durability read profile is invalid",
        ) from error


def _digest(value: object) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise DurabilityPrefixViolation(
            "DURABILITY_PREFIX_CORRUPT",
            "durability row digest is invalid",
        )
    return value


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


@dataclass(frozen=True, slots=True)
class DurabilityReadBinding:
    scope: ScopeRef
    task_id: str
    origin_attempt_id: str
    profile: DurabilityProfileBinding
    logical_origin_attempt_id: str | None = None

    def __post_init__(self) -> None:
        _scope(self.scope)
        _text(self.task_id, "read_binding.task_id")
        _text(self.origin_attempt_id, "read_binding.origin_attempt_id")
        _profile(self.profile)
        logical_origin = self.logical_origin_attempt_id
        if logical_origin is None:
            logical_origin = self.origin_attempt_id
            object.__setattr__(self, "logical_origin_attempt_id", logical_origin)
        _text(logical_origin, "read_binding.logical_origin_attempt_id")

    def to_dict(self) -> dict[str, object]:
        return {
            "scope": self.scope.to_dict(),
            "task_id": self.task_id,
            "origin_attempt_id": self.origin_attempt_id,
            "logical_origin_attempt_id": self.logical_origin_attempt_id,
            "profile": self.profile.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class CheckpointPrefixRow:
    row_sequence: int
    binding: DurabilityReadBinding
    canonical_bytes: bytes = field(repr=False)
    payload_digest: str


@dataclass(frozen=True, slots=True)
class EffectPrefixRow:
    row_sequence: int
    binding: DurabilityReadBinding
    canonical_bytes: bytes = field(repr=False)
    payload_digest: str


class _AuthorityFreePrefix:
    __slots__ = ()

    @property
    def recovery_authority(self) -> bool:
        return False

    @property
    def task_mutation_authority(self) -> bool:
        return False

    @property
    def executor_authority(self) -> bool:
        return False

    @property
    def external_effect_authority(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class VerifiedCheckpointPrefix(_AuthorityFreePrefix):
    binding: DurabilityReadBinding
    head: int
    prefix_digest: str
    records: tuple[D1Checkpoint, ...]


@dataclass(frozen=True, slots=True)
class VerifiedEffectPrefix(_AuthorityFreePrefix):
    binding: DurabilityReadBinding
    head: int
    prefix_digest: str
    records: tuple[EffectFact, ...]


PrefixRow: TypeAlias = CheckpointPrefixRow | EffectPrefixRow


def _normalize_binding(value: object) -> DurabilityReadBinding:
    if type(value) is not DurabilityReadBinding:
        raise DurabilityPrefixViolation(
            "INVALID_DURABILITY_BINDING",
            "expected durability read binding must be exact",
        )
    return DurabilityReadBinding(
        scope=_scope(value.scope),
        task_id=_text(value.task_id, "read_binding.task_id"),
        origin_attempt_id=_text(value.origin_attempt_id, "read_binding.origin_attempt_id"),
        profile=_profile(value.profile),
        logical_origin_attempt_id=_text(
            value.logical_origin_attempt_id,
            "read_binding.logical_origin_attempt_id",
        ),
    )


def _bounded_unique_rows(
    rows: object,
    *,
    row_type: type[CheckpointPrefixRow] | type[EffectPrefixRow],
    expected_binding: DurabilityReadBinding,
    expected_head: object,
) -> tuple[PrefixRow, ...]:
    if type(rows) is not tuple or len(rows) > MAX_DURABILITY_PREFIX_ROWS:
        raise DurabilityPrefixViolation(
            "DURABILITY_PREFIX_OUT_OF_BOUNDS",
            "durability prefix row count is outside the bounded range",
        )
    if (
        type(expected_head) is not int
        or expected_head < 0
        or expected_head > MAX_DURABILITY_PREFIX_ROWS
        or expected_head > MAX_SAFE_INTEGER
    ):
        raise DurabilityPrefixViolation(
            "DURABILITY_PREFIX_OUT_OF_BOUNDS",
            "durability prefix head is outside the bounded range",
        )

    total_bytes = 0
    unique: dict[int, PrefixRow] = {}
    prior_sequence = 0
    for row in rows:
        if type(row) is not row_type:
            raise DurabilityPrefixViolation(
                "DURABILITY_PREFIX_CORRUPT",
                "durability prefix contains an invalid row",
            )
        if (
            type(row.row_sequence) is not int
            or row.row_sequence <= 0
            or row.row_sequence > MAX_SAFE_INTEGER
            or row.row_sequence < prior_sequence
        ):
            raise DurabilityPrefixViolation(
                "DURABILITY_PREFIX_CORRUPT",
                "durability prefix row sequence is invalid",
            )
        prior_sequence = row.row_sequence
        if (
            type(row.canonical_bytes) is not bytes
            or not row.canonical_bytes
            or len(row.canonical_bytes) > MAX_DURABILITY_PREFIX_ITEM_BYTES
        ):
            raise DurabilityPrefixViolation(
                "DURABILITY_PREFIX_OUT_OF_BOUNDS",
                "durability prefix item is outside the bounded range",
            )
        total_bytes += len(row.canonical_bytes)
        if total_bytes > MAX_DURABILITY_PREFIX_BYTES:
            raise DurabilityPrefixViolation(
                "DURABILITY_PREFIX_OUT_OF_BOUNDS",
                "durability prefix bytes are outside the bounded range",
            )
        digest = _digest(row.payload_digest)
        if _sha256(row.canonical_bytes) != digest:
            raise DurabilityPrefixViolation(
                "DURABILITY_PREFIX_CORRUPT",
                "durability prefix row digest does not match",
            )
        prior = unique.get(row.row_sequence)
        if prior is not None:
            if prior != row:
                raise DurabilityPrefixViolation(
                    "DURABILITY_PREFIX_CONFLICT",
                    "changed durability fact reuses one row sequence",
                )
            continue
        if type(row.binding) is not DurabilityReadBinding:
            raise DurabilityPrefixViolation(
                "DURABILITY_PREFIX_CORRUPT",
                "durability prefix row binding must be exact",
            )
        checked_row_binding = _normalize_binding(row.binding)
        if checked_row_binding != expected_binding:
            raise DurabilityPrefixViolation(
                "DURABILITY_BINDING_MISMATCH",
                "durability prefix row binding does not match the caller",
            )
        unique[row.row_sequence] = row

    ordered = tuple(unique[index] for index in sorted(unique))
    expected_sequences = tuple(range(1, expected_head + 1))
    if tuple(row.row_sequence for row in ordered) != expected_sequences:
        raise DurabilityPrefixViolation(
            "DURABILITY_PREFIX_PARTIAL",
            "durability prefix is stale, partial, or non-contiguous",
        )
    return ordered


def _prefix_digest(
    *,
    prefix_kind: str,
    binding: DurabilityReadBinding,
    head: int,
    rows: tuple[PrefixRow, ...],
) -> str:
    value = {
        "contract_version": DURABILITY_PREFIX_CONTRACT_VERSION,
        "prefix_kind": prefix_kind,
        "binding": binding.to_dict(),
        "head": head,
        "rows": [
            {
                "row_sequence": row.row_sequence,
                "payload_digest": row.payload_digest,
            }
            for row in rows
        ],
    }
    return _sha256(canonical_json_bytes(value))


def _check_expected_prefix(actual: str, expected: object) -> None:
    if expected is None:
        return
    if type(expected) is not str or _SHA256.fullmatch(expected) is None:
        raise DurabilityPrefixViolation(
            "DURABILITY_PREFIX_STALE",
            "expected durability prefix digest is invalid",
        )
    if actual != expected:
        raise DurabilityPrefixViolation(
            "DURABILITY_PREFIX_STALE",
            "durability prefix digest does not match the caller head",
        )


def verify_checkpoint_prefix(
    rows: tuple[CheckpointPrefixRow, ...],
    *,
    expected_binding: DurabilityReadBinding,
    expected_head: int,
    expected_prefix_digest: str | None = None,
) -> VerifiedCheckpointPrefix:
    """Verify all checkpoint rows before returning any usable fact."""

    binding = _normalize_binding(expected_binding)
    checked_rows = _bounded_unique_rows(
        rows,
        row_type=CheckpointPrefixRow,
        expected_binding=binding,
        expected_head=expected_head,
    )
    records: list[D1Checkpoint] = []
    records_by_sequence: dict[int, D1Checkpoint] = {}
    latest_checkpoint_sequence = -1
    try:
        for row in checked_rows:
            checkpoint = D1Checkpoint.from_bytes(row.canonical_bytes)
            if (
                checkpoint.scope != binding.scope
                or checkpoint.task_id != binding.task_id
                or checkpoint.producer_attempt_id != binding.origin_attempt_id
                or checkpoint.profile != binding.profile
            ):
                raise DurabilityPrefixViolation(
                    "DURABILITY_BINDING_MISMATCH",
                    "checkpoint content binding does not match the caller",
                )
            prior = records_by_sequence.get(checkpoint.checkpoint_sequence)
            if prior is not None:
                if prior != checkpoint:
                    raise DurabilityPrefixViolation(
                        "DURABILITY_PREFIX_CONFLICT",
                        "changed checkpoint reuses one checkpoint sequence",
                    )
                continue
            if checkpoint.checkpoint_sequence <= latest_checkpoint_sequence:
                raise DurabilityPrefixViolation(
                    "DURABILITY_PREFIX_CORRUPT",
                    "checkpoint sequence descends within the verified prefix",
                )
            records_by_sequence[checkpoint.checkpoint_sequence] = checkpoint
            records.append(checkpoint)
            latest_checkpoint_sequence = checkpoint.checkpoint_sequence
    except DurabilityPrefixViolation:
        raise
    except Exception as error:
        raise DurabilityPrefixViolation(
            "DURABILITY_PREFIX_CORRUPT",
            "checkpoint prefix contains invalid canonical bytes",
        ) from error
    digest = _prefix_digest(
        prefix_kind="checkpoint",
        binding=binding,
        head=expected_head,
        rows=checked_rows,
    )
    _check_expected_prefix(digest, expected_prefix_digest)
    return VerifiedCheckpointPrefix(
        binding=binding,
        head=expected_head,
        prefix_digest=digest,
        records=tuple(records),
    )


def verify_effect_prefix(
    rows: tuple[EffectPrefixRow, ...],
    *,
    expected_binding: DurabilityReadBinding,
    expected_head: int,
    expected_prefix_digest: str | None = None,
) -> VerifiedEffectPrefix:
    """Verify all effect rows before returning any usable fact."""

    binding = _normalize_binding(expected_binding)
    checked_rows = _bounded_unique_rows(
        rows,
        row_type=EffectPrefixRow,
        expected_binding=binding,
        expected_head=expected_head,
    )
    records: list[EffectFact] = []
    records_by_identity: dict[tuple[str, str, int], EffectFact] = {}
    intents_by_effect: dict[str, ExternalEffectIntent] = {}
    effects_by_operation: dict[int, str] = {}
    latest_operation_ordinal = 0
    continuations: set[tuple[str, str, int]] = set()
    dispatches: dict[tuple[str, int], ExternalEffectDispatch] = {}
    latest_observations: dict[tuple[str, int], ExternalEffectObservation] = {}
    latest_dispatch_ordinal: dict[str, int] = {}
    latest_observation_ordinal: dict[str, int] = {}
    latest_settlement_ordinal: dict[str, int] = {}
    try:
        for row in checked_rows:
            fact = effect_fact_from_bytes(row.canonical_bytes)
            fact_binding = fact.binding
            if type(fact_binding) is not ExternalEffectBinding or (
                fact_binding.scope != binding.scope
                or fact_binding.task_id != binding.task_id
                or fact_binding.origin_attempt_id != binding.logical_origin_attempt_id
                or fact_binding.profile != binding.profile
            ):
                raise DurabilityPrefixViolation(
                    "DURABILITY_BINDING_MISMATCH",
                    "effect content binding does not match the caller",
                )
            if type(fact) is ExternalEffectIntent:
                identity = ("intent", fact.binding.effect_id, 0)
            elif type(fact) is EffectContinuationAuthorization:
                identity = (
                    "continuation",
                    fact.binding.effect_id,
                    fact.recovery_generation,
                )
            elif type(fact) is ExternalEffectDispatch:
                identity = ("dispatch", fact.binding.effect_id, fact.dispatch_ordinal)
            elif type(fact) is EffectDispatchReceipt:
                identity = ("receipt", fact.binding.effect_id, fact.dispatch_ordinal)
            elif type(fact) is ExternalEffectObservation:
                identity = (
                    "observation",
                    fact.binding.effect_id,
                    fact.observation_ordinal,
                )
            elif type(fact) is ExternalEffectSettlement:
                identity = (
                    "settlement",
                    fact.binding.effect_id,
                    fact.settlement_ordinal,
                )
            else:
                raise DurabilityPrefixViolation(
                    "DURABILITY_PREFIX_CORRUPT",
                    "effect prefix contains an unsupported fact type",
                )
            prior = records_by_identity.get(identity)
            if prior is not None:
                if prior != fact:
                    raise DurabilityPrefixViolation(
                        "DURABILITY_PREFIX_CONFLICT",
                        "changed effect fact reuses one semantic identity",
                    )
                continue

            effect_id = fact.binding.effect_id
            if type(fact) is ExternalEffectIntent:
                mapped_effect = effects_by_operation.get(fact.binding.operation_ordinal)
                if mapped_effect is not None and mapped_effect != effect_id:
                    raise DurabilityPrefixViolation(
                        "DURABILITY_PREFIX_CONFLICT",
                        "one effect operation ordinal maps to changed identity",
                    )
                if fact.binding.operation_ordinal <= latest_operation_ordinal:
                    raise DurabilityPrefixViolation(
                        "DURABILITY_PREFIX_CORRUPT",
                        "effect operation ordinal descends within the verified prefix",
                    )
                intents_by_effect[effect_id] = fact
                effects_by_operation[fact.binding.operation_ordinal] = effect_id
                latest_operation_ordinal = fact.binding.operation_ordinal
            else:
                intent = intents_by_effect.get(effect_id)
                if intent is None:
                    raise DurabilityPrefixViolation(
                        "DURABILITY_PREFIX_CORRUPT",
                        "effect fact has no preceding intent",
                    )
                if fact.binding != intent.binding:
                    raise DurabilityPrefixViolation(
                        "DURABILITY_PREFIX_CONFLICT",
                        "effect fact changes its intent binding",
                    )

                actor_id = getattr(fact, "actor_attempt_id", fact.binding.origin_attempt_id)
                recovery_generation = getattr(fact, "recovery_generation", 0)
                if actor_id == fact.binding.origin_attempt_id:
                    if recovery_generation != 0:
                        raise DurabilityPrefixViolation(
                            "DURABILITY_PREFIX_CORRUPT",
                            "origin effect actor must use recovery generation zero",
                        )
                elif (
                    type(fact) is not EffectContinuationAuthorization
                    and (
                        effect_id,
                        actor_id,
                        recovery_generation,
                    )
                    not in continuations
                ):
                    raise DurabilityPrefixViolation(
                        "DURABILITY_BINDING_MISMATCH",
                        "linked effect actor lacks Store continuation authorization",
                    )

                if type(fact) is EffectContinuationAuthorization:
                    continuations.add((effect_id, fact.actor_attempt_id, fact.recovery_generation))
                elif type(fact) is ExternalEffectDispatch:
                    if fact.dispatch_ordinal <= latest_dispatch_ordinal.get(effect_id, 0):
                        raise DurabilityPrefixViolation(
                            "DURABILITY_PREFIX_CORRUPT",
                            "effect dispatch ordinal descends within the verified prefix",
                        )
                    previous = dispatches.get((effect_id, latest_dispatch_ordinal.get(effect_id, 0)))
                    if previous is not None:
                        previous_observation = latest_observations.get((effect_id, previous.dispatch_ordinal))
                        stable_key_replay = (
                            intent.replay_safe and previous.provider_operation_key == fact.provider_operation_key
                        )
                        observed_no_effect = (
                            previous_observation is not None and previous_observation.kind.value == "no_effect"
                        )
                        if not (stable_key_replay or observed_no_effect):
                            raise DurabilityPrefixViolation(
                                "DURABILITY_PREFIX_CONFLICT",
                                "effect dispatch cannot be retried from unknown truth",
                            )
                    latest_dispatch_ordinal[effect_id] = fact.dispatch_ordinal
                    dispatches[(effect_id, fact.dispatch_ordinal)] = fact
                elif type(fact) is EffectDispatchReceipt:
                    dispatch = dispatches.get((effect_id, fact.dispatch_ordinal))
                    if dispatch is None or (
                        dispatch.actor_attempt_id != fact.actor_attempt_id
                        or dispatch.recovery_generation != fact.recovery_generation
                        or dispatch.provider_operation_key != fact.provider_operation_key
                    ):
                        raise DurabilityPrefixViolation(
                            "DURABILITY_PREFIX_CORRUPT",
                            "effect receipt has no exact preceding dispatch",
                        )
                elif type(fact) is ExternalEffectObservation:
                    dispatch = dispatches.get((effect_id, fact.dispatch_ordinal))
                    if dispatch is None or (
                        dispatch.actor_attempt_id != fact.actor_attempt_id
                        or dispatch.recovery_generation != fact.recovery_generation
                    ):
                        raise DurabilityPrefixViolation(
                            "DURABILITY_PREFIX_CORRUPT",
                            "effect observation has no exact preceding dispatch",
                        )
                    if fact.observation_ordinal <= latest_observation_ordinal.get(effect_id, 0):
                        raise DurabilityPrefixViolation(
                            "DURABILITY_PREFIX_CORRUPT",
                            "effect observation ordinal descends within the verified prefix",
                        )
                    latest_observation_ordinal[effect_id] = fact.observation_ordinal
                    latest_observations[(effect_id, fact.dispatch_ordinal)] = fact
                elif type(fact) is ExternalEffectSettlement:
                    if (
                        fact.settlement_ordinal <= latest_settlement_ordinal.get(effect_id, 0)
                        or fact.evidence_head >= row.row_sequence
                    ):
                        raise DurabilityPrefixViolation(
                            "DURABILITY_PREFIX_CORRUPT",
                            "effect settlement order or evidence head is invalid",
                        )
                    if fact.kind.value != "manual_required" and not any(
                        key[0] == effect_id for key in latest_observations
                    ):
                        raise DurabilityPrefixViolation(
                            "DURABILITY_PREFIX_CORRUPT",
                            "resolved effect settlement lacks authoritative observation",
                        )
                    latest_settlement_ordinal[effect_id] = fact.settlement_ordinal
            records_by_identity[identity] = fact
            records.append(fact)
    except DurabilityPrefixViolation:
        raise
    except Exception as error:
        raise DurabilityPrefixViolation(
            "DURABILITY_PREFIX_CORRUPT",
            "effect prefix contains invalid canonical bytes",
        ) from error
    digest = _prefix_digest(
        prefix_kind="effect",
        binding=binding,
        head=expected_head,
        rows=checked_rows,
    )
    _check_expected_prefix(digest, expected_prefix_digest)
    return VerifiedEffectPrefix(
        binding=binding,
        head=expected_head,
        prefix_digest=digest,
        records=tuple(records),
    )


__all__ = [
    "DURABILITY_PREFIX_CONTRACT_VERSION",
    "MAX_DURABILITY_PREFIX_BYTES",
    "MAX_DURABILITY_PREFIX_ITEM_BYTES",
    "MAX_DURABILITY_PREFIX_ROWS",
    "CheckpointPrefixRow",
    "DurabilityPrefixViolation",
    "DurabilityReadBinding",
    "EffectPrefixRow",
    "VerifiedCheckpointPrefix",
    "VerifiedEffectPrefix",
    "verify_checkpoint_prefix",
    "verify_effect_prefix",
]
