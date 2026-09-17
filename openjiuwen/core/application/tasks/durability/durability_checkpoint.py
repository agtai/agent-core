# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Pure canonical D1 checkpoint value and codec.

The value binds checkpoint content to one authenticated scope, Task, producer
Attempt, context version, and exact Executor profile. It performs no storage,
resume, dispatch, reconciliation, or Task mutation.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from dataclasses import dataclass, field
from functools import partial
from typing import Final

from openjiuwen.core.application.tasks.contracts import (
    MAX_SAFE_INTEGER,
    ScopeRef,
    canonical_json_bytes,
)
from openjiuwen.core.application.tasks.durability import durability_identity
from openjiuwen.core.application.tasks.durability.durability_identity import (
    DurabilityProfileBinding,
)

D1_CHECKPOINT_CONTRACT_VERSION: Final = "live-voice.d1-checkpoint.v1"
MAX_D1_CHECKPOINT_STATE_BYTES: Final = 1_048_576
MAX_D1_CHECKPOINT_WIRE_BYTES: Final = 2_097_152

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class DurabilityCheckpointViolation(ValueError):
    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


_text = partial(
    durability_identity._text, violation=DurabilityCheckpointViolation,
    reason="INVALID_DURABILITY_TEXT",
)


_scope = partial(
    durability_identity._scope, violation=DurabilityCheckpointViolation,
    reason="INVALID_DURABILITY_SCOPE",
    field_name="checkpoint scope",
)


_profile = partial(
    durability_identity._profile, violation=DurabilityCheckpointViolation,
    reason="INVALID_DURABILITY_PROFILE",
    field_name="checkpoint profile binding",
)


def _positive(value: object, field_name: str) -> int:
    if type(value) is not int or value <= 0 or value > MAX_SAFE_INTEGER:
        raise DurabilityCheckpointViolation(
            "INVALID_DURABILITY_INTEGER",
            f"{field_name} must be one positive safe integer",
        )
    return value


def _nonnegative(value: object, field_name: str) -> int:
    if type(value) is not int or value < 0 or value > MAX_SAFE_INTEGER:
        raise DurabilityCheckpointViolation(
            "INVALID_DURABILITY_INTEGER",
            f"{field_name} must be one non-negative safe integer",
        )
    return value


def _digest(value: object, field_name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise DurabilityCheckpointViolation(
            "INVALID_DURABILITY_DIGEST",
            f"{field_name} must be lowercase SHA-256",
        )
    return value


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _decode_base64(value: object) -> bytes:
    if type(value) is not str:
        raise DurabilityCheckpointViolation(
            "INVALID_CHECKPOINT_SCHEMA",
            "checkpoint state payload must be canonical base64",
        )
    try:
        decoded = base64.b64decode(value, validate=True)
    except (ValueError, TypeError) as error:
        raise DurabilityCheckpointViolation(
            "INVALID_CHECKPOINT_SCHEMA",
            "checkpoint state payload must be canonical base64",
        ) from error
    if base64.b64encode(decoded).decode("ascii") != value:
        raise DurabilityCheckpointViolation(
            "INVALID_CHECKPOINT_SCHEMA",
            "checkpoint state payload must be canonical base64",
        )
    return decoded


def _reject_duplicate_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise DurabilityCheckpointViolation(
                "INVALID_CHECKPOINT_SCHEMA",
                "checkpoint JSON contains a duplicate key",
            )
        result[key] = value
    return result


@dataclass(frozen=True, slots=True)
class D1Checkpoint:
    checkpoint_id: str
    scope: ScopeRef
    task_id: str
    producer_attempt_id: str
    checkpoint_sequence: int
    recovery_generation: int
    profile: DurabilityProfileBinding
    complete: bool
    task_spec_digest: str
    context_version: str
    context_digest: str
    input_digest: str
    state_schema_id: str
    state_schema_version: int
    effect_head: int
    effect_prefix_digest: str
    state_bytes: bytes = field(repr=False)
    state_digest: str
    integrity_digest: str
    contract_version: str = D1_CHECKPOINT_CONTRACT_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != D1_CHECKPOINT_CONTRACT_VERSION:
            raise DurabilityCheckpointViolation(
                "UNSUPPORTED_CHECKPOINT_VERSION",
                "checkpoint contract version is unsupported",
            )
        _text(self.checkpoint_id, "checkpoint_id")
        _scope(self.scope)
        _text(self.task_id, "task_id")
        _text(self.producer_attempt_id, "producer_attempt_id")
        _nonnegative(self.checkpoint_sequence, "checkpoint_sequence")
        _nonnegative(self.recovery_generation, "recovery_generation")
        _profile(self.profile)
        if self.profile.durability_level not in {"D1", "D2"}:
            raise DurabilityCheckpointViolation(
                "CHECKPOINT_PROFILE_UNSUPPORTED",
                "checkpoint profile must declare D1 or D2 durability",
            )
        if self.complete is not True:
            raise DurabilityCheckpointViolation(
                "CHECKPOINT_INCOMPLETE",
                "checkpoint must be an explicitly complete immutable snapshot",
            )
        _digest(self.task_spec_digest, "task_spec_digest")
        _text(self.context_version, "context_version")
        _digest(self.context_digest, "context_digest")
        _digest(self.input_digest, "input_digest")
        _text(self.state_schema_id, "state_schema_id")
        _positive(self.state_schema_version, "state_schema_version")
        _nonnegative(self.effect_head, "effect_head")
        _digest(self.effect_prefix_digest, "effect_prefix_digest")
        if (
            type(self.state_bytes) is not bytes
            or not self.state_bytes
            or len(self.state_bytes) > MAX_D1_CHECKPOINT_STATE_BYTES
        ):
            raise DurabilityCheckpointViolation(
                "CHECKPOINT_STATE_OUT_OF_BOUNDS",
                "checkpoint state bytes are outside the bounded range",
            )
        _digest(self.state_digest, "state_digest")
        _digest(self.integrity_digest, "integrity_digest")
        if _sha256(self.state_bytes) != self.state_digest:
            raise DurabilityCheckpointViolation(
                "CHECKPOINT_STATE_DIGEST_MISMATCH",
                "checkpoint state digest does not match",
            )
        if _sha256(canonical_json_bytes(self.unsigned_dict())) != self.integrity_digest:
            raise DurabilityCheckpointViolation(
                "CHECKPOINT_INTEGRITY_MISMATCH",
                "checkpoint integrity digest does not match",
            )

    @classmethod
    def create(
        cls,
        *,
        checkpoint_id: str,
        scope: ScopeRef,
        task_id: str,
        producer_attempt_id: str,
        checkpoint_sequence: int,
        recovery_generation: int,
        profile: DurabilityProfileBinding,
        complete: bool,
        task_spec_digest: str,
        context_version: str,
        context_digest: str,
        input_digest: str,
        state_schema_id: str,
        state_schema_version: int,
        state_bytes: bytes,
        effect_head: int,
        effect_prefix_digest: str,
    ) -> D1Checkpoint:
        checked_scope = _scope(scope)
        checked_profile = _profile(profile)
        _text(checkpoint_id, "checkpoint_id")
        _text(task_id, "task_id")
        _text(producer_attempt_id, "producer_attempt_id")
        _nonnegative(checkpoint_sequence, "checkpoint_sequence")
        _nonnegative(recovery_generation, "recovery_generation")
        if checked_profile.durability_level not in {"D1", "D2"}:
            raise DurabilityCheckpointViolation(
                "CHECKPOINT_PROFILE_UNSUPPORTED",
                "checkpoint profile must declare D1 or D2 durability",
            )
        if complete is not True:
            raise DurabilityCheckpointViolation(
                "CHECKPOINT_INCOMPLETE",
                "checkpoint must be an explicitly complete immutable snapshot",
            )
        _digest(task_spec_digest, "task_spec_digest")
        _text(context_version, "context_version")
        _digest(context_digest, "context_digest")
        _digest(input_digest, "input_digest")
        _text(state_schema_id, "state_schema_id")
        _positive(state_schema_version, "state_schema_version")
        _nonnegative(effect_head, "effect_head")
        _digest(effect_prefix_digest, "effect_prefix_digest")
        if type(state_bytes) is not bytes or not state_bytes or len(state_bytes) > MAX_D1_CHECKPOINT_STATE_BYTES:
            raise DurabilityCheckpointViolation(
                "CHECKPOINT_STATE_OUT_OF_BOUNDS",
                "checkpoint state bytes are outside the bounded range",
            )
        state_digest = _sha256(state_bytes)
        unsigned = {
            "contract_version": D1_CHECKPOINT_CONTRACT_VERSION,
            "checkpoint_id": checkpoint_id,
            "scope": checked_scope.to_dict(),
            "task_id": task_id,
            "producer_attempt_id": producer_attempt_id,
            "checkpoint_sequence": checkpoint_sequence,
            "recovery_generation": recovery_generation,
            "profile": checked_profile.to_dict(),
            "complete": complete,
            "task_spec_digest": task_spec_digest,
            "context_version": context_version,
            "context_digest": context_digest,
            "input_digest": input_digest,
            "state_schema_id": state_schema_id,
            "state_schema_version": state_schema_version,
            "state_bytes_base64": base64.b64encode(state_bytes).decode("ascii"),
            "state_digest": state_digest,
            "effect_head": effect_head,
            "effect_prefix_digest": effect_prefix_digest,
        }
        return cls(
            checkpoint_id=checkpoint_id,
            scope=checked_scope,
            task_id=task_id,
            producer_attempt_id=producer_attempt_id,
            checkpoint_sequence=checkpoint_sequence,
            recovery_generation=recovery_generation,
            profile=checked_profile,
            complete=complete,
            task_spec_digest=task_spec_digest,
            context_version=context_version,
            context_digest=context_digest,
            input_digest=input_digest,
            state_schema_id=state_schema_id,
            state_schema_version=state_schema_version,
            state_bytes=state_bytes,
            state_digest=state_digest,
            effect_head=effect_head,
            effect_prefix_digest=effect_prefix_digest,
            integrity_digest=_sha256(canonical_json_bytes(unsigned)),
        )

    @classmethod
    def from_dict(cls, payload: object) -> D1Checkpoint:
        keys = {
            "contract_version",
            "checkpoint_id",
            "scope",
            "task_id",
            "producer_attempt_id",
            "checkpoint_sequence",
            "recovery_generation",
            "profile",
            "complete",
            "task_spec_digest",
            "context_version",
            "context_digest",
            "input_digest",
            "state_schema_id",
            "state_schema_version",
            "state_bytes_base64",
            "state_digest",
            "effect_head",
            "effect_prefix_digest",
            "integrity_digest",
        }
        if type(payload) is not dict or set(payload) != keys:
            raise DurabilityCheckpointViolation(
                "INVALID_CHECKPOINT_SCHEMA",
                "checkpoint has an invalid closed field set",
            )
        try:
            scope = ScopeRef.from_dict(payload["scope"])
            profile = DurabilityProfileBinding.from_dict(payload["profile"])
        except (TypeError, ValueError) as error:
            raise DurabilityCheckpointViolation(
                "INVALID_CHECKPOINT_SCHEMA",
                "checkpoint binding is invalid",
            ) from error
        return cls(
            contract_version=_text(payload["contract_version"], "contract_version"),
            checkpoint_id=_text(payload["checkpoint_id"], "checkpoint_id"),
            scope=scope,
            task_id=_text(payload["task_id"], "task_id"),
            producer_attempt_id=_text(payload["producer_attempt_id"], "producer_attempt_id"),
            checkpoint_sequence=_nonnegative(payload["checkpoint_sequence"], "checkpoint_sequence"),
            recovery_generation=_nonnegative(payload["recovery_generation"], "recovery_generation"),
            profile=profile,
            complete=payload["complete"],
            task_spec_digest=_digest(payload["task_spec_digest"], "task_spec_digest"),
            context_version=_text(payload["context_version"], "context_version"),
            context_digest=_digest(payload["context_digest"], "context_digest"),
            input_digest=_digest(payload["input_digest"], "input_digest"),
            state_schema_id=_text(payload["state_schema_id"], "state_schema_id"),
            state_schema_version=_positive(payload["state_schema_version"], "state_schema_version"),
            state_bytes=_decode_base64(payload["state_bytes_base64"]),
            state_digest=_digest(payload["state_digest"], "state_digest"),
            effect_head=_nonnegative(payload["effect_head"], "effect_head"),
            effect_prefix_digest=_digest(payload["effect_prefix_digest"], "effect_prefix_digest"),
            integrity_digest=_digest(payload["integrity_digest"], "integrity_digest"),
        )

    @classmethod
    def from_bytes(cls, payload: object) -> D1Checkpoint:
        if type(payload) is not bytes or not payload or len(payload) > MAX_D1_CHECKPOINT_WIRE_BYTES:
            raise DurabilityCheckpointViolation(
                "CHECKPOINT_WIRE_OUT_OF_BOUNDS",
                "checkpoint wire bytes are outside the bounded range",
            )
        try:
            decoded = json.loads(
                payload.decode("utf-8"),
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=lambda _: (_ for _ in ()).throw(
                    DurabilityCheckpointViolation(
                        "INVALID_CHECKPOINT_SCHEMA",
                        "checkpoint JSON contains a non-finite number",
                    )
                ),
            )
        except DurabilityCheckpointViolation:
            raise
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise DurabilityCheckpointViolation(
                "INVALID_CHECKPOINT_SCHEMA",
                "checkpoint is not valid UTF-8 JSON",
            ) from error
        checkpoint = cls.from_dict(decoded)
        if checkpoint.canonical_bytes() != payload:
            raise DurabilityCheckpointViolation(
                "NON_CANONICAL_CHECKPOINT",
                "checkpoint wire bytes are not canonical",
            )
        return checkpoint

    def unsigned_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "checkpoint_id": self.checkpoint_id,
            "scope": self.scope.to_dict(),
            "task_id": self.task_id,
            "producer_attempt_id": self.producer_attempt_id,
            "checkpoint_sequence": self.checkpoint_sequence,
            "recovery_generation": self.recovery_generation,
            "profile": self.profile.to_dict(),
            "complete": self.complete,
            "task_spec_digest": self.task_spec_digest,
            "context_version": self.context_version,
            "context_digest": self.context_digest,
            "input_digest": self.input_digest,
            "state_schema_id": self.state_schema_id,
            "state_schema_version": self.state_schema_version,
            "state_bytes_base64": base64.b64encode(self.state_bytes).decode("ascii"),
            "state_digest": self.state_digest,
            "effect_head": self.effect_head,
            "effect_prefix_digest": self.effect_prefix_digest,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.unsigned_dict(), "integrity_digest": self.integrity_digest}

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())

    @property
    def resume_authority(self) -> bool:
        return False

    @property
    def task_mutation_authority(self) -> bool:
        return False

    @property
    def executor_authority(self) -> bool:
        return False


__all__ = [
    "D1_CHECKPOINT_CONTRACT_VERSION",
    "MAX_D1_CHECKPOINT_STATE_BYTES",
    "MAX_D1_CHECKPOINT_WIRE_BYTES",
    "D1Checkpoint",
    "DurabilityCheckpointViolation",
]
