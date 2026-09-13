# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Pure D2 external-effect facts, codec, and reconciliation decision.

This module records immutable descriptions only. It never invokes a Provider,
Tool, Executor, compensation, settlement, Store, or Task mutation.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Final, TypeAlias

from openjiuwen.core.application.tasks.contracts import (
    MAX_SAFE_INTEGER,
    Assurance,
    ScopeRef,
    canonical_json_bytes,
)
from openjiuwen.core.application.tasks.durability.durability_identity import (
    DurabilityIdentityViolation,
    DurabilityProfileBinding,
)

EXTERNAL_EFFECT_FACT_CONTRACT_VERSION: Final = "live-voice.d2-effect-fact.v1"
MAX_EXTERNAL_EFFECT_FACT_BYTES: Final = 65_536

_MAX_TEXT_BYTES = 512
_MAX_OBSERVATIONS = 1_024
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ExternalEffectContractViolation(ValueError):
    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


class EffectObservationKind(StrEnum):
    NO_EFFECT = "no_effect"
    APPLIED = "applied"
    UNKNOWN = "unknown"


class EffectReconciliationKind(StrEnum):
    NO_EFFECT = "no_effect"
    SAFELY_RETRYABLE = "safely_retryable"
    APPLIED = "applied"
    UNKNOWN = "unknown"
    MANUAL_REQUIRED = "manual_required"


class EffectSettlementKind(StrEnum):
    RESOLVED = "resolved"
    COMPENSATED = "compensated"
    MANUAL_REQUIRED = "manual_required"


def _text(value: object, field_name: str) -> str:
    if type(value) is not str or not value.strip():
        raise ExternalEffectContractViolation(
            "INVALID_EFFECT_TEXT",
            f"{field_name} must be a non-empty exact string",
        )
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ExternalEffectContractViolation(
            "INVALID_EFFECT_TEXT",
            f"{field_name} must contain valid Unicode scalar values",
        ) from error
    if len(encoded) > _MAX_TEXT_BYTES:
        raise ExternalEffectContractViolation(
            "INVALID_EFFECT_TEXT",
            f"{field_name} is outside the bounded range",
        )
    return value


def _positive(value: object, field_name: str) -> int:
    if type(value) is not int or value <= 0 or value > MAX_SAFE_INTEGER:
        raise ExternalEffectContractViolation(
            "INVALID_EFFECT_INTEGER",
            f"{field_name} must be one positive safe integer",
        )
    return value


def _nonnegative(value: object, field_name: str) -> int:
    if type(value) is not int or value < 0 or value > MAX_SAFE_INTEGER:
        raise ExternalEffectContractViolation(
            "INVALID_EFFECT_INTEGER",
            f"{field_name} must be one non-negative safe integer",
        )
    return value


def _digest(value: object, field_name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise ExternalEffectContractViolation(
            "INVALID_EFFECT_DIGEST",
            f"{field_name} must be lowercase SHA-256",
        )
    return value


def _scope(value: object) -> ScopeRef:
    if type(value) is not ScopeRef:
        raise ExternalEffectContractViolation(
            "INVALID_EFFECT_SCOPE",
            "effect scope must be exact",
        )
    try:
        checked = ScopeRef.from_dict(value.to_dict())
    except (TypeError, ValueError) as error:
        raise ExternalEffectContractViolation(
            "INVALID_EFFECT_SCOPE",
            "effect scope is invalid",
        ) from error
    if checked.assurance is not Assurance.AUTHENTICATED:
        raise ExternalEffectContractViolation(
            "INVALID_EFFECT_SCOPE",
            "effect scope must be authenticated",
        )
    return checked


def _profile(value: object) -> DurabilityProfileBinding:
    if type(value) is not DurabilityProfileBinding:
        raise ExternalEffectContractViolation(
            "INVALID_EFFECT_PROFILE",
            "effect profile binding must be exact",
        )
    try:
        return DurabilityProfileBinding.from_dict(value.to_dict())
    except DurabilityIdentityViolation as error:
        raise ExternalEffectContractViolation(
            "INVALID_EFFECT_PROFILE",
            "effect profile binding is invalid",
        ) from error


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _strict(payload: object, keys: set[str], field_name: str) -> dict[str, object]:
    if type(payload) is not dict or set(payload) != keys:
        raise ExternalEffectContractViolation(
            "INVALID_EFFECT_FACT",
            f"{field_name} has an invalid closed field set",
        )
    return payload


def _reject_duplicate_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ExternalEffectContractViolation(
                "INVALID_EFFECT_FACT",
                "effect fact JSON contains a duplicate key",
            )
        result[key] = value
    return result


class _AuthorityFreeEffectFact:
    __slots__ = ()

    @property
    def external_call_authority(self) -> bool:
        return False

    @property
    def compensation_authority(self) -> bool:
        return False

    @property
    def task_mutation_authority(self) -> bool:
        return False

    @property
    def settlement_authority(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class ExternalEffectBinding(_AuthorityFreeEffectFact):
    scope: ScopeRef
    task_id: str
    origin_attempt_id: str
    profile: DurabilityProfileBinding
    effect_id: str
    operation_kind: str
    operation_ordinal: int
    target_digest: str
    intended_effect_digest: str

    def __post_init__(self) -> None:
        _scope(self.scope)
        _text(self.task_id, "effect.task_id")
        _text(self.origin_attempt_id, "effect.origin_attempt_id")
        _profile(self.profile)
        if self.profile.durability_level != "D2":
            raise ExternalEffectContractViolation(
                "EFFECT_PROFILE_UNSUPPORTED",
                "external effects require an exact D2 profile",
            )
        _text(self.effect_id, "effect.effect_id")
        _text(self.operation_kind, "effect.operation_kind")
        _positive(self.operation_ordinal, "effect.operation_ordinal")
        _digest(self.target_digest, "effect.target_digest")
        _digest(self.intended_effect_digest, "effect.intended_effect_digest")

    @classmethod
    def from_dict(cls, payload: object) -> ExternalEffectBinding:
        data = _strict(
            payload,
            {
                "scope",
                "task_id",
                "origin_attempt_id",
                "profile",
                "effect_id",
                "operation_kind",
                "operation_ordinal",
                "target_digest",
                "intended_effect_digest",
            },
            "effect binding",
        )
        try:
            scope = ScopeRef.from_dict(data["scope"])
            profile = DurabilityProfileBinding.from_dict(data["profile"])
        except (TypeError, ValueError) as error:
            raise ExternalEffectContractViolation(
                "INVALID_EFFECT_FACT",
                "effect binding contains an invalid nested value",
            ) from error
        return cls(
            scope=scope,
            task_id=_text(data["task_id"], "effect.task_id"),
            origin_attempt_id=_text(data["origin_attempt_id"], "effect.origin_attempt_id"),
            profile=profile,
            effect_id=_text(data["effect_id"], "effect.effect_id"),
            operation_kind=_text(data["operation_kind"], "effect.operation_kind"),
            operation_ordinal=_positive(data["operation_ordinal"], "effect.operation_ordinal"),
            target_digest=_digest(data["target_digest"], "effect.target_digest"),
            intended_effect_digest=_digest(data["intended_effect_digest"], "effect.intended_effect_digest"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "scope": self.scope.to_dict(),
            "task_id": self.task_id,
            "origin_attempt_id": self.origin_attempt_id,
            "profile": self.profile.to_dict(),
            "effect_id": self.effect_id,
            "operation_kind": self.operation_kind,
            "operation_ordinal": self.operation_ordinal,
            "target_digest": self.target_digest,
            "intended_effect_digest": self.intended_effect_digest,
        }


@dataclass(frozen=True, slots=True)
class ExternalEffectIntent(_AuthorityFreeEffectFact):
    binding: ExternalEffectBinding
    replay_safe: bool

    def __post_init__(self) -> None:
        if type(self.binding) is not ExternalEffectBinding:
            raise ExternalEffectContractViolation(
                "INVALID_EFFECT_FACT",
                "effect intent binding must be exact",
            )
        ExternalEffectBinding.from_dict(self.binding.to_dict())
        if type(self.replay_safe) is not bool:
            raise ExternalEffectContractViolation(
                "INVALID_EFFECT_FACT",
                "effect replay safety must be exact bool",
            )

    @classmethod
    def from_dict(cls, payload: object) -> ExternalEffectIntent:
        data = _strict(payload, {"binding", "replay_safe"}, "effect intent")
        if type(data["replay_safe"]) is not bool:
            raise ExternalEffectContractViolation(
                "INVALID_EFFECT_FACT",
                "effect replay safety must be exact bool",
            )
        return cls(
            binding=ExternalEffectBinding.from_dict(data["binding"]),
            replay_safe=data["replay_safe"],
        )

    def to_dict(self) -> dict[str, object]:
        return {"binding": self.binding.to_dict(), "replay_safe": self.replay_safe}


@dataclass(frozen=True, slots=True)
class EffectContinuationAuthorization(_AuthorityFreeEffectFact):
    binding: ExternalEffectBinding
    actor_attempt_id: str
    recovery_generation: int
    checkpoint_head: int
    checkpoint_prefix_digest: str
    effect_head: int
    effect_prefix_digest: str

    def __post_init__(self) -> None:
        if type(self.binding) is not ExternalEffectBinding:
            raise ExternalEffectContractViolation(
                "INVALID_EFFECT_FACT",
                "continuation binding must be exact",
            )
        ExternalEffectBinding.from_dict(self.binding.to_dict())
        _text(self.actor_attempt_id, "continuation.actor_attempt_id")
        if self.actor_attempt_id == self.binding.origin_attempt_id:
            raise ExternalEffectContractViolation(
                "EFFECT_ACTOR_BINDING_INVALID",
                "continuation actor must be a linked Attempt",
            )
        _positive(self.recovery_generation, "continuation.recovery_generation")
        _nonnegative(self.checkpoint_head, "continuation.checkpoint_head")
        _digest(
            self.checkpoint_prefix_digest,
            "continuation.checkpoint_prefix_digest",
        )
        _nonnegative(self.effect_head, "continuation.effect_head")
        _digest(self.effect_prefix_digest, "continuation.effect_prefix_digest")

    @classmethod
    def from_dict(cls, payload: object) -> EffectContinuationAuthorization:
        data = _strict(
            payload,
            {
                "binding",
                "actor_attempt_id",
                "recovery_generation",
                "checkpoint_head",
                "checkpoint_prefix_digest",
                "effect_head",
                "effect_prefix_digest",
            },
            "effect continuation authorization",
        )
        return cls(
            binding=ExternalEffectBinding.from_dict(data["binding"]),
            actor_attempt_id=_text(data["actor_attempt_id"], "continuation.actor_attempt_id"),
            recovery_generation=_positive(data["recovery_generation"], "continuation.recovery_generation"),
            checkpoint_head=_nonnegative(data["checkpoint_head"], "continuation.checkpoint_head"),
            checkpoint_prefix_digest=_digest(
                data["checkpoint_prefix_digest"],
                "continuation.checkpoint_prefix_digest",
            ),
            effect_head=_nonnegative(data["effect_head"], "continuation.effect_head"),
            effect_prefix_digest=_digest(data["effect_prefix_digest"], "continuation.effect_prefix_digest"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "binding": self.binding.to_dict(),
            "actor_attempt_id": self.actor_attempt_id,
            "recovery_generation": self.recovery_generation,
            "checkpoint_head": self.checkpoint_head,
            "checkpoint_prefix_digest": self.checkpoint_prefix_digest,
            "effect_head": self.effect_head,
            "effect_prefix_digest": self.effect_prefix_digest,
        }


@dataclass(frozen=True, slots=True)
class ExternalEffectDispatch(_AuthorityFreeEffectFact):
    binding: ExternalEffectBinding
    actor_attempt_id: str
    dispatch_ordinal: int
    recovery_generation: int
    provider_operation_key: str

    def __post_init__(self) -> None:
        if type(self.binding) is not ExternalEffectBinding:
            raise ExternalEffectContractViolation(
                "INVALID_EFFECT_FACT",
                "effect dispatch binding must be exact",
            )
        ExternalEffectBinding.from_dict(self.binding.to_dict())
        _text(self.actor_attempt_id, "dispatch.actor_attempt_id")
        _positive(self.dispatch_ordinal, "dispatch.dispatch_ordinal")
        _nonnegative(self.recovery_generation, "dispatch.recovery_generation")
        _text(self.provider_operation_key, "dispatch.provider_operation_key")

    @classmethod
    def from_dict(cls, payload: object) -> ExternalEffectDispatch:
        data = _strict(
            payload,
            {
                "binding",
                "actor_attempt_id",
                "dispatch_ordinal",
                "recovery_generation",
                "provider_operation_key",
            },
            "effect dispatch",
        )
        return cls(
            binding=ExternalEffectBinding.from_dict(data["binding"]),
            actor_attempt_id=_text(data["actor_attempt_id"], "dispatch.actor_attempt_id"),
            dispatch_ordinal=_positive(data["dispatch_ordinal"], "dispatch.dispatch_ordinal"),
            recovery_generation=_nonnegative(data["recovery_generation"], "dispatch.recovery_generation"),
            provider_operation_key=_text(data["provider_operation_key"], "dispatch.provider_operation_key"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "binding": self.binding.to_dict(),
            "actor_attempt_id": self.actor_attempt_id,
            "dispatch_ordinal": self.dispatch_ordinal,
            "recovery_generation": self.recovery_generation,
            "provider_operation_key": self.provider_operation_key,
        }


@dataclass(frozen=True, slots=True)
class EffectDispatchReceipt(_AuthorityFreeEffectFact):
    binding: ExternalEffectBinding
    actor_attempt_id: str
    dispatch_ordinal: int
    recovery_generation: int
    provider_operation_key: str
    accepted: bool
    receipt_digest: str

    def __post_init__(self) -> None:
        if type(self.binding) is not ExternalEffectBinding:
            raise ExternalEffectContractViolation(
                "INVALID_EFFECT_FACT",
                "effect dispatch binding must be exact",
            )
        ExternalEffectBinding.from_dict(self.binding.to_dict())
        _text(self.actor_attempt_id, "receipt.actor_attempt_id")
        _positive(self.dispatch_ordinal, "dispatch.dispatch_ordinal")
        _nonnegative(self.recovery_generation, "dispatch.recovery_generation")
        _text(self.provider_operation_key, "dispatch.provider_operation_key")
        if type(self.accepted) is not bool:
            raise ExternalEffectContractViolation(
                "INVALID_EFFECT_FACT",
                "dispatch acceptance must be exact bool",
            )
        _digest(self.receipt_digest, "dispatch.receipt_digest")

    @classmethod
    def from_dict(cls, payload: object) -> EffectDispatchReceipt:
        data = _strict(
            payload,
            {
                "binding",
                "actor_attempt_id",
                "dispatch_ordinal",
                "recovery_generation",
                "provider_operation_key",
                "accepted",
                "receipt_digest",
            },
            "effect dispatch receipt",
        )
        if type(data["accepted"]) is not bool:
            raise ExternalEffectContractViolation(
                "INVALID_EFFECT_FACT",
                "dispatch acceptance must be exact bool",
            )
        return cls(
            binding=ExternalEffectBinding.from_dict(data["binding"]),
            actor_attempt_id=_text(data["actor_attempt_id"], "receipt.actor_attempt_id"),
            dispatch_ordinal=_positive(data["dispatch_ordinal"], "dispatch.dispatch_ordinal"),
            recovery_generation=_nonnegative(data["recovery_generation"], "dispatch.recovery_generation"),
            provider_operation_key=_text(data["provider_operation_key"], "dispatch.provider_operation_key"),
            accepted=data["accepted"],
            receipt_digest=_digest(data["receipt_digest"], "dispatch.receipt_digest"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "binding": self.binding.to_dict(),
            "actor_attempt_id": self.actor_attempt_id,
            "dispatch_ordinal": self.dispatch_ordinal,
            "recovery_generation": self.recovery_generation,
            "provider_operation_key": self.provider_operation_key,
            "accepted": self.accepted,
            "receipt_digest": self.receipt_digest,
        }


@dataclass(frozen=True, slots=True)
class ExternalEffectObservation(_AuthorityFreeEffectFact):
    binding: ExternalEffectBinding
    actor_attempt_id: str
    observation_ordinal: int
    dispatch_ordinal: int
    recovery_generation: int
    kind: EffectObservationKind
    evidence_digest: str

    def __post_init__(self) -> None:
        if type(self.binding) is not ExternalEffectBinding:
            raise ExternalEffectContractViolation(
                "INVALID_EFFECT_FACT",
                "effect observation binding must be exact",
            )
        ExternalEffectBinding.from_dict(self.binding.to_dict())
        _text(self.actor_attempt_id, "observation.actor_attempt_id")
        _positive(self.observation_ordinal, "observation.observation_ordinal")
        _positive(self.dispatch_ordinal, "observation.dispatch_ordinal")
        _nonnegative(self.recovery_generation, "observation.recovery_generation")
        if type(self.kind) is not EffectObservationKind:
            raise ExternalEffectContractViolation(
                "INVALID_EFFECT_FACT",
                "effect observation kind must use the closed vocabulary",
            )
        _digest(self.evidence_digest, "observation.evidence_digest")

    @classmethod
    def from_dict(cls, payload: object) -> ExternalEffectObservation:
        data = _strict(
            payload,
            {
                "binding",
                "actor_attempt_id",
                "observation_ordinal",
                "dispatch_ordinal",
                "recovery_generation",
                "kind",
                "evidence_digest",
            },
            "effect observation",
        )
        try:
            kind = EffectObservationKind(data["kind"])
        except (TypeError, ValueError) as error:
            raise ExternalEffectContractViolation(
                "INVALID_EFFECT_FACT",
                "effect observation kind is invalid",
            ) from error
        return cls(
            binding=ExternalEffectBinding.from_dict(data["binding"]),
            actor_attempt_id=_text(data["actor_attempt_id"], "observation.actor_attempt_id"),
            observation_ordinal=_positive(data["observation_ordinal"], "observation.observation_ordinal"),
            dispatch_ordinal=_positive(data["dispatch_ordinal"], "observation.dispatch_ordinal"),
            recovery_generation=_nonnegative(data["recovery_generation"], "observation.recovery_generation"),
            kind=kind,
            evidence_digest=_digest(data["evidence_digest"], "observation.evidence_digest"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "binding": self.binding.to_dict(),
            "actor_attempt_id": self.actor_attempt_id,
            "observation_ordinal": self.observation_ordinal,
            "dispatch_ordinal": self.dispatch_ordinal,
            "recovery_generation": self.recovery_generation,
            "kind": self.kind.value,
            "evidence_digest": self.evidence_digest,
        }


@dataclass(frozen=True, slots=True)
class ExternalEffectSettlement(_AuthorityFreeEffectFact):
    binding: ExternalEffectBinding
    actor_attempt_id: str
    settlement_ordinal: int
    recovery_generation: int
    kind: EffectSettlementKind
    evidence_head: int
    evidence_digest: str

    def __post_init__(self) -> None:
        if type(self.binding) is not ExternalEffectBinding:
            raise ExternalEffectContractViolation(
                "INVALID_EFFECT_FACT",
                "effect settlement binding must be exact",
            )
        ExternalEffectBinding.from_dict(self.binding.to_dict())
        _text(self.actor_attempt_id, "settlement.actor_attempt_id")
        _positive(self.settlement_ordinal, "settlement.settlement_ordinal")
        _nonnegative(self.recovery_generation, "settlement.recovery_generation")
        if type(self.kind) is not EffectSettlementKind:
            raise ExternalEffectContractViolation(
                "INVALID_EFFECT_FACT",
                "effect settlement kind must use the closed vocabulary",
            )
        _positive(self.evidence_head, "settlement.evidence_head")
        _digest(self.evidence_digest, "settlement.evidence_digest")

    @classmethod
    def from_dict(cls, payload: object) -> ExternalEffectSettlement:
        data = _strict(
            payload,
            {
                "binding",
                "actor_attempt_id",
                "settlement_ordinal",
                "recovery_generation",
                "kind",
                "evidence_head",
                "evidence_digest",
            },
            "effect settlement",
        )
        try:
            kind = EffectSettlementKind(data["kind"])
        except (TypeError, ValueError) as error:
            raise ExternalEffectContractViolation(
                "INVALID_EFFECT_FACT",
                "effect settlement kind is invalid",
            ) from error
        return cls(
            binding=ExternalEffectBinding.from_dict(data["binding"]),
            actor_attempt_id=_text(data["actor_attempt_id"], "settlement.actor_attempt_id"),
            settlement_ordinal=_positive(data["settlement_ordinal"], "settlement.settlement_ordinal"),
            recovery_generation=_nonnegative(data["recovery_generation"], "settlement.recovery_generation"),
            kind=kind,
            evidence_head=_positive(data["evidence_head"], "settlement.evidence_head"),
            evidence_digest=_digest(data["evidence_digest"], "settlement.evidence_digest"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "binding": self.binding.to_dict(),
            "actor_attempt_id": self.actor_attempt_id,
            "settlement_ordinal": self.settlement_ordinal,
            "recovery_generation": self.recovery_generation,
            "kind": self.kind.value,
            "evidence_head": self.evidence_head,
            "evidence_digest": self.evidence_digest,
        }


EffectFact: TypeAlias = (
    ExternalEffectIntent
    | EffectContinuationAuthorization
    | ExternalEffectDispatch
    | EffectDispatchReceipt
    | ExternalEffectObservation
    | ExternalEffectSettlement
)

_FACT_TYPES: Final = {
    "intent": ExternalEffectIntent,
    "continuation": EffectContinuationAuthorization,
    "dispatch": ExternalEffectDispatch,
    "receipt": EffectDispatchReceipt,
    "observation": ExternalEffectObservation,
    "settlement": ExternalEffectSettlement,
}


def _fact_kind(value: EffectFact) -> str:
    if type(value) is ExternalEffectIntent:
        return "intent"
    if type(value) is EffectContinuationAuthorization:
        return "continuation"
    if type(value) is ExternalEffectDispatch:
        return "dispatch"
    if type(value) is EffectDispatchReceipt:
        return "receipt"
    if type(value) is ExternalEffectObservation:
        return "observation"
    if type(value) is ExternalEffectSettlement:
        return "settlement"
    raise ExternalEffectContractViolation(
        "INVALID_EFFECT_FACT",
        "effect fact must use one exact supported value type",
    )


def effect_fact_bytes(value: EffectFact) -> bytes:
    kind = _fact_kind(value)
    fact = value.to_dict()
    unsigned = {
        "contract_version": EXTERNAL_EFFECT_FACT_CONTRACT_VERSION,
        "fact_kind": kind,
        "fact": fact,
    }
    return canonical_json_bytes({**unsigned, "fact_digest": _sha256(canonical_json_bytes(unsigned))})


def effect_fact_from_bytes(payload: object) -> EffectFact:
    if type(payload) is not bytes or not payload or len(payload) > MAX_EXTERNAL_EFFECT_FACT_BYTES:
        raise ExternalEffectContractViolation(
            "EFFECT_FACT_OUT_OF_BOUNDS",
            "effect fact wire bytes are outside the bounded range",
        )
    try:
        decoded = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=lambda _: (_ for _ in ()).throw(
                ExternalEffectContractViolation(
                    "INVALID_EFFECT_FACT",
                    "effect fact JSON contains a non-finite number",
                )
            ),
        )
    except ExternalEffectContractViolation:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ExternalEffectContractViolation(
            "INVALID_EFFECT_FACT",
            "effect fact is not valid UTF-8 JSON",
        ) from error
    data = _strict(
        decoded,
        {"contract_version", "fact_kind", "fact", "fact_digest"},
        "effect fact envelope",
    )
    if data["contract_version"] != EXTERNAL_EFFECT_FACT_CONTRACT_VERSION:
        raise ExternalEffectContractViolation(
            "INVALID_EFFECT_FACT",
            "effect fact contract version is unsupported",
        )
    kind = data["fact_kind"]
    if type(kind) is not str or kind not in _FACT_TYPES:
        raise ExternalEffectContractViolation(
            "INVALID_EFFECT_FACT",
            "effect fact kind is unsupported",
        )
    digest = _digest(data["fact_digest"], "fact_digest")
    unsigned = {
        "contract_version": data["contract_version"],
        "fact_kind": kind,
        "fact": data["fact"],
    }
    if _sha256(canonical_json_bytes(unsigned)) != digest:
        raise ExternalEffectContractViolation(
            "EFFECT_FACT_DIGEST_MISMATCH",
            "effect fact digest does not match",
        )
    fact = _FACT_TYPES[kind].from_dict(data["fact"])
    if effect_fact_bytes(fact) != payload:
        raise ExternalEffectContractViolation(
            "NON_CANONICAL_EFFECT_FACT",
            "effect fact wire bytes are not canonical",
        )
    return fact


@dataclass(frozen=True, slots=True)
class EffectReconciliationDecision(_AuthorityFreeEffectFact):
    kind: EffectReconciliationKind
    reason: str

    def __post_init__(self) -> None:
        if type(self.kind) is not EffectReconciliationKind:
            raise ExternalEffectContractViolation(
                "INVALID_RECONCILIATION_DECISION",
                "effect reconciliation kind must use the closed vocabulary",
            )
        _text(self.reason, "decision.reason")


def decide_effect_reconciliation(
    *,
    intent: ExternalEffectIntent,
    receipt: EffectDispatchReceipt | None,
    observations: tuple[ExternalEffectObservation, ...],
    manual_required: bool,
) -> EffectReconciliationDecision:
    """Classify exact facts without authorizing or performing any action."""

    if type(intent) is not ExternalEffectIntent:
        raise ExternalEffectContractViolation(
            "INVALID_EFFECT_FACT",
            "reconciliation intent must be exact",
        )
    ExternalEffectIntent.from_dict(intent.to_dict())
    if receipt is not None:
        if type(receipt) is not EffectDispatchReceipt:
            raise ExternalEffectContractViolation(
                "INVALID_EFFECT_FACT",
                "reconciliation receipt must be exact",
            )
        EffectDispatchReceipt.from_dict(receipt.to_dict())
        if receipt.binding != intent.binding:
            raise ExternalEffectContractViolation(
                "EFFECT_BINDING_MISMATCH",
                "effect receipt binding does not match the intent",
            )
    if type(observations) is not tuple or len(observations) > _MAX_OBSERVATIONS:
        raise ExternalEffectContractViolation(
            "EFFECT_FACT_OUT_OF_BOUNDS",
            "effect observations are outside the bounded range",
        )
    if type(manual_required) is not bool:
        raise ExternalEffectContractViolation(
            "INVALID_EFFECT_FACT",
            "manual-required fact must be exact bool",
        )

    by_ordinal: dict[int, ExternalEffectObservation] = {}
    for observation in observations:
        if type(observation) is not ExternalEffectObservation:
            raise ExternalEffectContractViolation(
                "INVALID_EFFECT_FACT",
                "effect observation must be exact",
            )
        checked = ExternalEffectObservation.from_dict(observation.to_dict())
        if checked.binding != intent.binding:
            raise ExternalEffectContractViolation(
                "EFFECT_BINDING_MISMATCH",
                "effect observation binding does not match the intent",
            )
        prior = by_ordinal.get(checked.observation_ordinal)
        if prior is not None and prior != checked:
            raise ExternalEffectContractViolation(
                "EFFECT_FACT_CONFLICT",
                "changed effect fact reuses one observation ordinal",
            )
        by_ordinal[checked.observation_ordinal] = checked

    latest = by_ordinal[max(by_ordinal)] if by_ordinal else None
    if latest is not None and latest.kind is EffectObservationKind.APPLIED:
        kind = EffectReconciliationKind.APPLIED
        reason = "observed_applied"
    elif manual_required:
        kind = EffectReconciliationKind.MANUAL_REQUIRED
        reason = "manual_resolution_required"
    elif latest is None or latest.kind is EffectObservationKind.UNKNOWN:
        kind = EffectReconciliationKind.UNKNOWN
        reason = "effect_outcome_unknown"
    elif intent.replay_safe:
        kind = EffectReconciliationKind.SAFELY_RETRYABLE
        reason = "observed_no_effect_and_replay_safe"
    else:
        kind = EffectReconciliationKind.NO_EFFECT
        reason = "observed_no_effect"
    return EffectReconciliationDecision(kind=kind, reason=reason)


__all__ = [
    "EXTERNAL_EFFECT_FACT_CONTRACT_VERSION",
    "MAX_EXTERNAL_EFFECT_FACT_BYTES",
    "EffectContinuationAuthorization",
    "EffectDispatchReceipt",
    "EffectFact",
    "EffectObservationKind",
    "EffectReconciliationDecision",
    "EffectReconciliationKind",
    "EffectSettlementKind",
    "ExternalEffectBinding",
    "ExternalEffectContractViolation",
    "ExternalEffectIntent",
    "ExternalEffectDispatch",
    "ExternalEffectObservation",
    "ExternalEffectSettlement",
    "decide_effect_reconciliation",
    "effect_fact_bytes",
    "effect_fact_from_bytes",
]
