# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Opaque, one-operation durability mutation authorization.

Checkpoint, effect, prefix, and recovery contracts are immutable data.  Only
the Direct runtime can mint this private construction-token receipt, and the
authoritative Store consumes its exact lease in the mutation transaction.
"""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
from dataclasses import dataclass, field
from threading import RLock
from typing import Final
from weakref import WeakValueDictionary

from openjiuwen.core.application.tasks.contracts import (
    ScopeRef,
    canonical_json_bytes,
)
from openjiuwen.core.application.tasks.durability.durability_identity import DurabilityProfileBinding

_CONSTRUCTION_TOKEN = object()
_RECEIPT_SIGNING_KEY: Final = secrets.token_bytes(32)
_RECEIPT_REGISTRY_LOCK = RLock()
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_OPERATIONS: Final = frozenset(
    {
        "checkpoint.append",
        "effect.append",
        "lineage.fork",
        "recovery.admit.applied",
        "recovery.admit.continue",
    }
)


def _text(value: object) -> bool:
    return type(value) is str and bool(value.strip()) and len(value.encode("utf-8")) <= 512


@dataclass(frozen=True, slots=True, weakref_slot=True)
class DurabilityMutationAuthorization:
    """Opaque receipt exact-bound to one Store lease, prefix tip, and write."""

    operation: str
    scope: ScopeRef
    task_id: str
    producer_attempt_id: str
    candidate_attempt_id: str | None
    profile: DurabilityProfileBinding
    executor_owner_id: str
    executor_owner_generation: int
    checkpoint_head: int
    checkpoint_prefix_digest: str
    effect_head: int
    effect_prefix_digest: str
    payload_digest: str
    claim_owner_id: str
    claim_token: str
    claim_generation: int
    _store_identity: object = field(repr=False, compare=False)
    _receipt_nonce: str = field(repr=False, compare=False)
    _receipt_signature: bytes = field(repr=False, compare=False)
    _construction_token: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if (
            self._construction_token is not _CONSTRUCTION_TOKEN
            or self.operation not in _OPERATIONS
            or type(self.scope) is not ScopeRef
            or not _text(self.task_id)
            or not _text(self.producer_attempt_id)
            or (self.candidate_attempt_id is not None and not _text(self.candidate_attempt_id))
            or type(self.profile) is not DurabilityProfileBinding
            or not _text(self.executor_owner_id)
            or type(self.executor_owner_generation) is not int
            or self.executor_owner_generation < 0
            or type(self.checkpoint_head) is not int
            or self.checkpoint_head < 0
            or _SHA256.fullmatch(self.checkpoint_prefix_digest) is None
            or type(self.effect_head) is not int
            or self.effect_head < 0
            or _SHA256.fullmatch(self.effect_prefix_digest) is None
            or _SHA256.fullmatch(self.payload_digest) is None
            or not _text(self.claim_owner_id)
            or not _text(self.claim_token)
            or type(self.claim_generation) is not int
            or self.claim_generation <= 0
            or _SHA256.fullmatch(self._receipt_nonce) is None
            or type(self._receipt_signature) is not bytes
            or len(self._receipt_signature) != 32
        ):
            raise ValueError("invalid durability mutation authorization")

    def is_authentic_for_store(self, store: object) -> bool:
        with _RECEIPT_REGISTRY_LOCK:
            registered = _RECEIPT_REGISTRY.get(id(self))
        if registered is not self or self._store_identity is not store:
            return False
        expected = hmac.digest(
            _RECEIPT_SIGNING_KEY,
            canonical_json_bytes(_receipt_payload(self)),
            "sha256",
        )
        return hmac.compare_digest(self._receipt_signature, expected)


_RECEIPT_REGISTRY: WeakValueDictionary[int, DurabilityMutationAuthorization] = WeakValueDictionary()


def _receipt_payload(
    authorization: DurabilityMutationAuthorization,
) -> dict[str, object]:
    return {
        "operation": authorization.operation,
        "scope": authorization.scope.to_dict(),
        "task_id": authorization.task_id,
        "producer_attempt_id": authorization.producer_attempt_id,
        "candidate_attempt_id": authorization.candidate_attempt_id,
        "profile": authorization.profile.to_dict(),
        "executor_owner_id": authorization.executor_owner_id,
        "executor_owner_generation": authorization.executor_owner_generation,
        "checkpoint_head": authorization.checkpoint_head,
        "checkpoint_prefix_digest": authorization.checkpoint_prefix_digest,
        "effect_head": authorization.effect_head,
        "effect_prefix_digest": authorization.effect_prefix_digest,
        "payload_digest": authorization.payload_digest,
        "claim_owner_id": authorization.claim_owner_id,
        "claim_token": authorization.claim_token,
        "claim_generation": authorization.claim_generation,
        "store_identity": id(authorization._store_identity),
        "receipt_nonce": authorization._receipt_nonce,
    }


def _mint_durability_mutation_authorization(
    *,
    store: object,
    operation: str,
    scope: ScopeRef,
    task_id: str,
    producer_attempt_id: str,
    candidate_attempt_id: str | None,
    profile: DurabilityProfileBinding,
    executor_owner_id: str,
    executor_owner_generation: int,
    checkpoint_head: int,
    checkpoint_prefix_digest: str,
    effect_head: int,
    effect_prefix_digest: str,
    payload_digest: str,
    claim_owner_id: str,
    claim_token: str,
    claim_generation: int,
) -> DurabilityMutationAuthorization:
    """Private Direct/runtime issuer; callers outside the module get data only."""

    nonce = secrets.token_hex(32)
    unsigned = DurabilityMutationAuthorization(
        operation=operation,
        scope=scope,
        task_id=task_id,
        producer_attempt_id=producer_attempt_id,
        candidate_attempt_id=candidate_attempt_id,
        profile=profile,
        executor_owner_id=executor_owner_id,
        executor_owner_generation=executor_owner_generation,
        checkpoint_head=checkpoint_head,
        checkpoint_prefix_digest=checkpoint_prefix_digest,
        effect_head=effect_head,
        effect_prefix_digest=effect_prefix_digest,
        payload_digest=payload_digest,
        claim_owner_id=claim_owner_id,
        claim_token=claim_token,
        claim_generation=claim_generation,
        _store_identity=store,
        _receipt_nonce=nonce,
        _receipt_signature=b"\x00" * 32,
        _construction_token=_CONSTRUCTION_TOKEN,
    )
    authorization = DurabilityMutationAuthorization(
        operation=unsigned.operation,
        scope=unsigned.scope,
        task_id=unsigned.task_id,
        producer_attempt_id=unsigned.producer_attempt_id,
        candidate_attempt_id=unsigned.candidate_attempt_id,
        profile=unsigned.profile,
        executor_owner_id=unsigned.executor_owner_id,
        executor_owner_generation=unsigned.executor_owner_generation,
        checkpoint_head=unsigned.checkpoint_head,
        checkpoint_prefix_digest=unsigned.checkpoint_prefix_digest,
        effect_head=unsigned.effect_head,
        effect_prefix_digest=unsigned.effect_prefix_digest,
        payload_digest=unsigned.payload_digest,
        claim_owner_id=unsigned.claim_owner_id,
        claim_token=unsigned.claim_token,
        claim_generation=unsigned.claim_generation,
        _store_identity=store,
        _receipt_nonce=nonce,
        _receipt_signature=hmac.digest(
            _RECEIPT_SIGNING_KEY,
            canonical_json_bytes(_receipt_payload(unsigned)),
            "sha256",
        ),
        _construction_token=_CONSTRUCTION_TOKEN,
    )
    with _RECEIPT_REGISTRY_LOCK:
        _RECEIPT_REGISTRY[id(authorization)] = authorization
    return authorization


def _durability_authorization_payload_digest(payload: object) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


__all__ = ["DurabilityMutationAuthorization"]
