# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Canonical authority-free Executor recovery generation facts.

The record binds a producer Attempt to a distinct candidate recovery Attempt
and one observed Executor epoch/generation. It is evidence only: expiry, a
valid digest, or a matching candidate never proves quiescence, lease ownership,
checkpoint resumability, Executor invocation, recovery, or Task mutation.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final

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

EXECUTOR_RECOVERY_FACTS_VERSION: Final = "live-voice.executor-recovery-facts.v1"
MAX_EXECUTOR_RECOVERY_FACTS_BYTES: Final = 32_768

_MAX_TEXT_BYTES = 512
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_UTC_TIMESTAMP = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$")


class ExecutorRecoveryFactsViolation(ValueError):
    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


def _text(value: object, field_name: str) -> str:
    if type(value) is not str or not value.strip():
        raise ExecutorRecoveryFactsViolation(
            "INVALID_RECOVERY_TEXT",
            f"{field_name} must be a non-empty exact string",
        )
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ExecutorRecoveryFactsViolation(
            "INVALID_RECOVERY_TEXT",
            f"{field_name} must contain valid Unicode scalar values",
        ) from error
    if len(encoded) > _MAX_TEXT_BYTES:
        raise ExecutorRecoveryFactsViolation(
            "INVALID_RECOVERY_TEXT",
            f"{field_name} is outside the bounded range",
        )
    return value


def _nonnegative(value: object, field_name: str) -> int:
    if type(value) is not int or value < 0 or value > MAX_SAFE_INTEGER:
        raise ExecutorRecoveryFactsViolation(
            "INVALID_RECOVERY_INTEGER",
            f"{field_name} must be one non-negative safe integer",
        )
    return value


def _digest(value: object, field_name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise ExecutorRecoveryFactsViolation(
            "INVALID_RECOVERY_DIGEST",
            f"{field_name} must be lowercase SHA-256",
        )
    return value


def _scope(value: object) -> ScopeRef:
    if type(value) is not ScopeRef:
        raise ExecutorRecoveryFactsViolation(
            "INVALID_RECOVERY_SCOPE",
            "recovery facts scope must be exact",
        )
    try:
        checked = ScopeRef.from_dict(value.to_dict())
    except (TypeError, ValueError) as error:
        raise ExecutorRecoveryFactsViolation(
            "INVALID_RECOVERY_SCOPE",
            "recovery facts scope is invalid",
        ) from error
    if checked.assurance is not Assurance.AUTHENTICATED:
        raise ExecutorRecoveryFactsViolation(
            "INVALID_RECOVERY_SCOPE",
            "recovery facts scope must be authenticated",
        )
    return checked


def _profile(value: object) -> DurabilityProfileBinding:
    if type(value) is not DurabilityProfileBinding:
        raise ExecutorRecoveryFactsViolation(
            "INVALID_RECOVERY_PROFILE",
            "recovery profile binding must be exact",
        )
    try:
        return DurabilityProfileBinding.from_dict(value.to_dict())
    except DurabilityIdentityViolation as error:
        raise ExecutorRecoveryFactsViolation(
            "INVALID_RECOVERY_PROFILE",
            "recovery profile binding is invalid",
        ) from error


def _timestamp_key(value: object, field_name: str) -> tuple[datetime, int]:
    if type(value) is not str:
        raise ExecutorRecoveryFactsViolation(
            "INVALID_RECOVERY_TIMESTAMP",
            f"{field_name} must be one canonical UTC timestamp",
        )
    matched = _UTC_TIMESTAMP.fullmatch(value)
    if matched is None:
        raise ExecutorRecoveryFactsViolation(
            "INVALID_RECOVERY_TIMESTAMP",
            f"{field_name} must be one canonical UTC timestamp",
        )
    year, month, day, hour, minute, second = (int(part) for part in matched.groups()[:6])
    fraction = matched.group(7) or ""
    if fraction.endswith("0"):
        raise ExecutorRecoveryFactsViolation(
            "INVALID_RECOVERY_TIMESTAMP",
            f"{field_name} must use the shortest exact fractional form",
        )
    nanoseconds = int(fraction.ljust(9, "0")) if fraction else 0
    try:
        instant = datetime(year, month, day, hour, minute, second, tzinfo=UTC)
    except ValueError as error:
        raise ExecutorRecoveryFactsViolation(
            "INVALID_RECOVERY_TIMESTAMP",
            f"{field_name} must be one real calendar timestamp",
        ) from error
    return instant, nanoseconds


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _reject_duplicate_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ExecutorRecoveryFactsViolation(
                "INVALID_RECOVERY_FACTS",
                "recovery facts JSON contains a duplicate key",
            )
        result[key] = value
    return result


@dataclass(frozen=True, slots=True)
class ExecutorRecoveryFacts:
    scope: ScopeRef
    task_id: str
    producer_attempt_id: str
    candidate_recovery_attempt_id: str
    profile: DurabilityProfileBinding
    recovery_generation: int
    executor_epoch_id: str
    executor_owner_generation: int
    observed_at: str
    expires_at: str
    evidence_digest: str
    integrity_digest: str
    contract_version: str = EXECUTOR_RECOVERY_FACTS_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != EXECUTOR_RECOVERY_FACTS_VERSION:
            raise ExecutorRecoveryFactsViolation(
                "UNSUPPORTED_RECOVERY_FACTS_VERSION",
                "recovery facts contract version is unsupported",
            )
        _scope(self.scope)
        _text(self.task_id, "recovery.task_id")
        _text(self.producer_attempt_id, "recovery.producer_attempt_id")
        _text(
            self.candidate_recovery_attempt_id,
            "recovery.candidate_recovery_attempt_id",
        )
        if self.producer_attempt_id == self.candidate_recovery_attempt_id:
            raise ExecutorRecoveryFactsViolation(
                "RECOVERY_ATTEMPT_BINDING_INVALID",
                "producer and candidate recovery Attempts must be distinct",
            )
        _profile(self.profile)
        _nonnegative(self.recovery_generation, "recovery.recovery_generation")
        _text(self.executor_epoch_id, "recovery.executor_epoch_id")
        _nonnegative(
            self.executor_owner_generation,
            "recovery.executor_owner_generation",
        )
        observed = _timestamp_key(self.observed_at, "recovery.observed_at")
        expires = _timestamp_key(self.expires_at, "recovery.expires_at")
        if expires <= observed:
            raise ExecutorRecoveryFactsViolation(
                "RECOVERY_EXPIRY_INVALID",
                "recovery facts expiry must be after observation",
            )
        _digest(self.evidence_digest, "recovery.evidence_digest")
        _digest(self.integrity_digest, "recovery.integrity_digest")
        if _sha256(canonical_json_bytes(self.unsigned_dict())) != self.integrity_digest:
            raise ExecutorRecoveryFactsViolation(
                "RECOVERY_FACTS_DIGEST_MISMATCH",
                "recovery facts integrity digest does not match",
            )

    @classmethod
    def create(
        cls,
        *,
        scope: ScopeRef,
        task_id: str,
        producer_attempt_id: str,
        candidate_recovery_attempt_id: str,
        profile: DurabilityProfileBinding,
        recovery_generation: int,
        executor_epoch_id: str,
        executor_owner_generation: int,
        observed_at: str,
        expires_at: str,
        evidence_digest: str,
    ) -> ExecutorRecoveryFacts:
        checked_scope = _scope(scope)
        checked_profile = _profile(profile)
        checked_task_id = _text(task_id, "recovery.task_id")
        checked_producer_attempt_id = _text(producer_attempt_id, "recovery.producer_attempt_id")
        checked_candidate_attempt_id = _text(
            candidate_recovery_attempt_id,
            "recovery.candidate_recovery_attempt_id",
        )
        if checked_producer_attempt_id == checked_candidate_attempt_id:
            raise ExecutorRecoveryFactsViolation(
                "RECOVERY_ATTEMPT_BINDING_INVALID",
                "producer and candidate recovery Attempts must be distinct",
            )
        checked_recovery_generation = _nonnegative(recovery_generation, "recovery.recovery_generation")
        checked_epoch_id = _text(executor_epoch_id, "recovery.executor_epoch_id")
        checked_owner_generation = _nonnegative(
            executor_owner_generation,
            "recovery.executor_owner_generation",
        )
        checked_observed_at = _text(observed_at, "recovery.observed_at")
        checked_expires_at = _text(expires_at, "recovery.expires_at")
        observed_key = _timestamp_key(checked_observed_at, "recovery.observed_at")
        expires_key = _timestamp_key(checked_expires_at, "recovery.expires_at")
        if expires_key <= observed_key:
            raise ExecutorRecoveryFactsViolation(
                "RECOVERY_EXPIRY_INVALID",
                "recovery facts expiry must be after observation",
            )
        checked_evidence_digest = _digest(evidence_digest, "recovery.evidence_digest")
        unsigned = {
            "contract_version": EXECUTOR_RECOVERY_FACTS_VERSION,
            "scope": checked_scope.to_dict(),
            "task_id": checked_task_id,
            "producer_attempt_id": checked_producer_attempt_id,
            "candidate_recovery_attempt_id": checked_candidate_attempt_id,
            "profile": checked_profile.to_dict(),
            "recovery_generation": checked_recovery_generation,
            "executor_epoch_id": checked_epoch_id,
            "executor_owner_generation": checked_owner_generation,
            "observed_at": checked_observed_at,
            "expires_at": checked_expires_at,
            "evidence_digest": checked_evidence_digest,
        }
        return cls(
            scope=checked_scope,
            task_id=checked_task_id,
            producer_attempt_id=checked_producer_attempt_id,
            candidate_recovery_attempt_id=checked_candidate_attempt_id,
            profile=checked_profile,
            recovery_generation=checked_recovery_generation,
            executor_epoch_id=checked_epoch_id,
            executor_owner_generation=checked_owner_generation,
            observed_at=checked_observed_at,
            expires_at=checked_expires_at,
            evidence_digest=checked_evidence_digest,
            integrity_digest=_sha256(canonical_json_bytes(unsigned)),
        )

    @classmethod
    def from_dict(cls, payload: object) -> ExecutorRecoveryFacts:
        keys = {
            "contract_version",
            "scope",
            "task_id",
            "producer_attempt_id",
            "candidate_recovery_attempt_id",
            "profile",
            "recovery_generation",
            "executor_epoch_id",
            "executor_owner_generation",
            "observed_at",
            "expires_at",
            "evidence_digest",
            "integrity_digest",
        }
        if type(payload) is not dict or set(payload) != keys:
            raise ExecutorRecoveryFactsViolation(
                "INVALID_RECOVERY_FACTS",
                "recovery facts have an invalid closed field set",
            )
        try:
            scope = ScopeRef.from_dict(payload["scope"])
            profile = DurabilityProfileBinding.from_dict(payload["profile"])
        except (TypeError, ValueError) as error:
            raise ExecutorRecoveryFactsViolation(
                "INVALID_RECOVERY_FACTS",
                "recovery facts contain an invalid nested binding",
            ) from error
        return cls(
            contract_version=_text(payload["contract_version"], "recovery.contract_version"),
            scope=scope,
            task_id=_text(payload["task_id"], "recovery.task_id"),
            producer_attempt_id=_text(payload["producer_attempt_id"], "recovery.producer_attempt_id"),
            candidate_recovery_attempt_id=_text(
                payload["candidate_recovery_attempt_id"],
                "recovery.candidate_recovery_attempt_id",
            ),
            profile=profile,
            recovery_generation=_nonnegative(payload["recovery_generation"], "recovery.recovery_generation"),
            executor_epoch_id=_text(payload["executor_epoch_id"], "recovery.executor_epoch_id"),
            executor_owner_generation=_nonnegative(
                payload["executor_owner_generation"],
                "recovery.executor_owner_generation",
            ),
            observed_at=_text(payload["observed_at"], "recovery.observed_at"),
            expires_at=_text(payload["expires_at"], "recovery.expires_at"),
            evidence_digest=_digest(payload["evidence_digest"], "recovery.evidence_digest"),
            integrity_digest=_digest(payload["integrity_digest"], "recovery.integrity_digest"),
        )

    @classmethod
    def from_bytes(cls, payload: object) -> ExecutorRecoveryFacts:
        if type(payload) is not bytes or not payload or len(payload) > MAX_EXECUTOR_RECOVERY_FACTS_BYTES:
            raise ExecutorRecoveryFactsViolation(
                "RECOVERY_FACTS_OUT_OF_BOUNDS",
                "recovery facts wire bytes are outside the bounded range",
            )
        try:
            decoded = json.loads(
                payload.decode("utf-8"),
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=lambda _: (_ for _ in ()).throw(
                    ExecutorRecoveryFactsViolation(
                        "INVALID_RECOVERY_FACTS",
                        "recovery facts JSON contains a non-finite number",
                    )
                ),
            )
        except ExecutorRecoveryFactsViolation:
            raise
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ExecutorRecoveryFactsViolation(
                "INVALID_RECOVERY_FACTS",
                "recovery facts are not valid UTF-8 JSON",
            ) from error
        facts = cls.from_dict(decoded)
        if facts.canonical_bytes() != payload:
            raise ExecutorRecoveryFactsViolation(
                "NON_CANONICAL_RECOVERY_FACTS",
                "recovery facts wire bytes are not canonical",
            )
        return facts

    def unsigned_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "scope": self.scope.to_dict(),
            "task_id": self.task_id,
            "producer_attempt_id": self.producer_attempt_id,
            "candidate_recovery_attempt_id": self.candidate_recovery_attempt_id,
            "profile": self.profile.to_dict(),
            "recovery_generation": self.recovery_generation,
            "executor_epoch_id": self.executor_epoch_id,
            "executor_owner_generation": self.executor_owner_generation,
            "observed_at": self.observed_at,
            "expires_at": self.expires_at,
            "evidence_digest": self.evidence_digest,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.unsigned_dict(), "integrity_digest": self.integrity_digest}

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())

    def is_expired(self, *, at: object) -> bool:
        return _timestamp_key(at, "recovery.expiry_query") >= _timestamp_key(
            self.expires_at,
            "recovery.expires_at",
        )

    @property
    def recovery_authority(self) -> bool:
        return False

    @property
    def lease_authority(self) -> bool:
        return False

    @property
    def checkpoint_resume_authority(self) -> bool:
        return False

    @property
    def executor_invocation_authority(self) -> bool:
        return False

    @property
    def task_mutation_authority(self) -> bool:
        return False

    @property
    def quiescence_authority(self) -> bool:
        return False


__all__ = [
    "EXECUTOR_RECOVERY_FACTS_VERSION",
    "MAX_EXECUTOR_RECOVERY_FACTS_BYTES",
    "ExecutorRecoveryFacts",
    "ExecutorRecoveryFactsViolation",
]
