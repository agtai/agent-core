# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Authority-free identity values shared by pure durability assets.

These values describe the exact selected Executor Adapter/profile. They do not
select a profile, prove a capability, persist admission, or authorize work.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final

from openjiuwen.core.application.tasks.contracts import Assurance, ScopeRef

DURABILITY_PROFILE_BINDING_VERSION: Final = "live-voice.durability-profile-binding.v1"

_MAX_DURABILITY_TEXT_BYTES = 512
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_DURABILITY_LEVELS = frozenset({"D0", "D1", "D2"})


class DurabilityIdentityViolation(ValueError):
    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


def _text(
    value: object, field_name: str, *,
    violation: type[ValueError] = DurabilityIdentityViolation,
    reason: str = "INVALID_DURABILITY_PROFILE",
) -> str:
    if type(value) is not str or not value.strip():
        raise violation(
            reason,
            f"{field_name} must be a non-empty exact string",
        )
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise violation(
            reason,
            f"{field_name} must contain valid Unicode scalar values",
        ) from error
    if len(encoded) > _MAX_DURABILITY_TEXT_BYTES:
        raise violation(
            reason,
            f"{field_name} is outside the bounded range",
        )
    return value


def _digest(value: object, field_name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise DurabilityIdentityViolation(
            "INVALID_DURABILITY_PROFILE",
            f"{field_name} must be lowercase SHA-256",
        )
    return value


def _scope(
    value: object, *, violation: type[ValueError], reason: str, field_name: str,
) -> ScopeRef:
    if type(value) is not ScopeRef:
        raise violation(
            reason,
            f"{field_name} must be exact",
        )
    try:
        checked = ScopeRef.from_dict(value.to_dict())
    except (TypeError, ValueError) as error:
        raise violation(
            reason,
            f"{field_name} is invalid",
        ) from error
    if checked.assurance is not Assurance.AUTHENTICATED:
        raise violation(
            reason,
            f"{field_name} must be authenticated",
        )
    return checked


def _profile(
    value: object, *, violation: type[ValueError], reason: str, field_name: str,
) -> DurabilityProfileBinding:
    if type(value) is not DurabilityProfileBinding:
        raise violation(
            reason,
            f"{field_name} must be exact",
        )
    try:
        return DurabilityProfileBinding.from_dict(value.to_dict())
    except DurabilityIdentityViolation as error:
        raise violation(
            reason,
            f"{field_name} is invalid",
        ) from error


@dataclass(frozen=True, slots=True)
class DurabilityProfileBinding:
    """Exact persisted-profile identity without capability authority."""

    executor_id: str
    adapter_id: str
    profile_id: str
    profile_version: str
    profile_digest: str
    durability_level: str
    durability_capability_version: str
    contract_version: str = DURABILITY_PROFILE_BINDING_VERSION

    def __post_init__(self) -> None:
        if self.contract_version != DURABILITY_PROFILE_BINDING_VERSION:
            raise DurabilityIdentityViolation(
                "INVALID_DURABILITY_PROFILE",
                "durability profile binding version is unsupported",
            )
        _text(self.executor_id, "profile.executor_id")
        _text(self.adapter_id, "profile.adapter_id")
        _text(self.profile_id, "profile.profile_id")
        _text(self.profile_version, "profile.profile_version")
        _digest(self.profile_digest, "profile.profile_digest")
        if self.durability_level not in _DURABILITY_LEVELS:
            raise DurabilityIdentityViolation(
                "INVALID_DURABILITY_PROFILE",
                "profile.durability_level must be D0, D1, or D2",
            )
        _text(
            self.durability_capability_version,
            "profile.durability_capability_version",
        )

    @classmethod
    def from_dict(cls, payload: object) -> DurabilityProfileBinding:
        if type(payload) is not dict or set(payload) != {
            "contract_version",
            "executor_id",
            "adapter_id",
            "profile_id",
            "profile_version",
            "profile_digest",
            "durability_level",
            "durability_capability_version",
        }:
            raise DurabilityIdentityViolation(
                "INVALID_DURABILITY_PROFILE",
                "durability profile binding has an invalid closed field set",
            )
        return cls(
            contract_version=_text(payload["contract_version"], "profile.contract_version"),
            executor_id=_text(payload["executor_id"], "profile.executor_id"),
            adapter_id=_text(payload["adapter_id"], "profile.adapter_id"),
            profile_id=_text(payload["profile_id"], "profile.profile_id"),
            profile_version=_text(payload["profile_version"], "profile.profile_version"),
            profile_digest=_digest(payload["profile_digest"], "profile.profile_digest"),
            durability_level=_text(payload["durability_level"], "profile.durability_level"),
            durability_capability_version=_text(
                payload["durability_capability_version"],
                "profile.durability_capability_version",
            ),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "executor_id": self.executor_id,
            "adapter_id": self.adapter_id,
            "profile_id": self.profile_id,
            "profile_version": self.profile_version,
            "profile_digest": self.profile_digest,
            "durability_level": self.durability_level,
            "durability_capability_version": self.durability_capability_version,
        }

    @property
    def capability_authority(self) -> bool:
        return False


__all__ = [
    "DURABILITY_PROFILE_BINDING_VERSION",
    "DurabilityIdentityViolation",
    "DurabilityProfileBinding",
]
