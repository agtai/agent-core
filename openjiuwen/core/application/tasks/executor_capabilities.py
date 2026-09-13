# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Immutable Executor capability declarations and deterministic selection.

The values in this module carry stable protocol facts only.  They do not read
runtime availability, mutate Task/Attempt/Store state, call an Adapter, or
authorize fallback after a selected Adapter has accepted work.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Final

from openjiuwen.core.application.tasks.contracts import (
    MAX_SAFE_INTEGER,
    ErrorCode,
    canonical_json_bytes,
)
from openjiuwen.core.application.tasks.formal_task_models import FormalTaskViolation

EXECUTOR_CAPABILITY_PROFILE_SCHEMA_VERSION: Final = "live-voice.executor-capability-profile.v1"
TASK_EXECUTION_REQUIREMENTS_SCHEMA_VERSION: Final = "live-voice.task-execution-requirements.v1"
PROJECT_MUTATION_SIDE_EFFECT_FACT: Final = "side-effect.project-mutation"

_SAFE_IDENTIFIER = re.compile(r"^[a-z][a-z0-9._-]{0,127}$")
_VERSION = re.compile(r"^v[1-9][0-9]{0,5}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SENSITIVE_IDENTIFIER = re.compile(
    r"(?:api[-_]?key|authorization|bearer|credential|password|passwd|secret|token)",
    re.IGNORECASE,
)
_PROFILE_FIELDS = {
    "adapter_id",
    "adapter_protocol_version",
    "durability_level",
    "durability_version",
    "enforcement_facts",
    "executor_id",
    "max_live_attempts",
    "operation_versions",
    "profile_id",
    "project_serialization",
    "schema_version",
}
_REQUIREMENT_FIELDS = {
    "durability_level",
    "executor_id",
    "operation_versions",
    "project_serialization",
    "schema_version",
    "side_effect_class",
}
_SELECTION_FIELDS = {"profile", "profile_digest", "requirements"}
_SIDE_EFFECT_FACTS: Final = {
    "project_mutation": PROJECT_MUTATION_SIDE_EFFECT_FACT,
}
_DURABILITY_RANK: Final = {"D0": 0, "D1": 1, "D2": 2}


def _identifier(value: object, field_name: str) -> str:
    if type(value) is not str or _SAFE_IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a bounded identifier")
    if _SENSITIVE_IDENTIFIER.search(value) is not None:
        raise ValueError(f"{field_name} must not carry sensitive identity")
    return value


def _operation_versions(
    value: object,
    *,
    field_name: str = "operation_versions",
) -> tuple[tuple[str, str], ...]:
    if type(value) is not tuple or not value:
        raise ValueError(f"{field_name} must be a non-empty tuple")
    normalized: list[tuple[str, str]] = []
    operations: set[str] = set()
    for item in value:
        if type(item) is not tuple or len(item) != 2:
            raise ValueError(f"{field_name} must contain operation/version pairs")
        operation = _identifier(item[0], f"{field_name}.operation")
        version = item[1]
        if type(version) is not str or _VERSION.fullmatch(version) is None:
            raise ValueError(f"{field_name} contains an invalid version")
        if operation in operations:
            raise ValueError(f"{field_name} operations must be unique")
        operations.add(operation)
        normalized.append((operation, version))
    return tuple(sorted(normalized))


def _enforcement_facts(value: object) -> tuple[str, ...]:
    if type(value) is not tuple or not value:
        raise ValueError("enforcement_facts must be a non-empty tuple")
    facts = tuple(_identifier(fact, "enforcement_facts") for fact in value)
    if len(facts) != len(set(facts)):
        raise ValueError("enforcement_facts must be unique")
    return tuple(sorted(facts))


def _decoded_operation_versions(value: object) -> tuple[tuple[str, str], ...]:
    if type(value) is not list or not value:
        raise ValueError("operation_versions must be a non-empty JSON array")
    if any(type(item) is not list or len(item) != 2 for item in value):
        raise ValueError("operation_versions must contain two-item JSON arrays")
    return tuple((item[0], item[1]) for item in value)


@dataclass(frozen=True, slots=True)
class ExecutorCapabilityProfile:
    schema_version: str
    profile_id: str
    executor_id: str
    adapter_id: str
    adapter_protocol_version: str
    operation_versions: tuple[tuple[str, str], ...]
    durability_level: str
    durability_version: str
    project_serialization: str
    max_live_attempts: int
    enforcement_facts: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.schema_version != EXECUTOR_CAPABILITY_PROFILE_SCHEMA_VERSION:
            raise ValueError("unsupported Executor capability profile schema version")
        _identifier(self.profile_id, "profile_id")
        _identifier(self.executor_id, "executor_id")
        _identifier(self.adapter_id, "adapter_id")
        _identifier(self.adapter_protocol_version, "adapter_protocol_version")
        if self.durability_level not in _DURABILITY_RANK:
            raise ValueError("Executor capability durability level must be D0, D1, or D2")
        _identifier(self.durability_version, "durability_version")
        if self.project_serialization != "exclusive":
            raise ValueError("Executor project serialization must be exclusive")
        if (
            type(self.max_live_attempts) is not int
            or self.max_live_attempts < 1
            or self.max_live_attempts > MAX_SAFE_INTEGER
        ):
            raise ValueError("max_live_attempts must be a positive JSON-safe integer")
        object.__setattr__(
            self,
            "operation_versions",
            _operation_versions(self.operation_versions),
        )
        object.__setattr__(
            self,
            "enforcement_facts",
            _enforcement_facts(self.enforcement_facts),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "profile_id": self.profile_id,
            "executor_id": self.executor_id,
            "adapter_id": self.adapter_id,
            "adapter_protocol_version": self.adapter_protocol_version,
            "operation_versions": [list(item) for item in self.operation_versions],
            "durability_level": self.durability_level,
            "durability_version": self.durability_version,
            "project_serialization": self.project_serialization,
            "max_live_attempts": self.max_live_attempts,
            "enforcement_facts": list(self.enforcement_facts),
        }

    @classmethod
    def from_dict(cls, payload: object) -> ExecutorCapabilityProfile:
        if type(payload) is not dict or set(payload) != _PROFILE_FIELDS:
            raise ValueError("Executor capability profile fields are incomplete or unknown")
        facts = payload["enforcement_facts"]
        if type(facts) is not list:
            raise ValueError("enforcement_facts must be a JSON array")
        return cls(
            schema_version=payload["schema_version"],
            profile_id=payload["profile_id"],
            executor_id=payload["executor_id"],
            adapter_id=payload["adapter_id"],
            adapter_protocol_version=payload["adapter_protocol_version"],
            operation_versions=_decoded_operation_versions(payload["operation_versions"]),
            durability_level=payload["durability_level"],
            durability_version=payload["durability_version"],
            project_serialization=payload["project_serialization"],
            max_live_attempts=payload["max_live_attempts"],
            enforcement_facts=tuple(facts),
        )

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())

    def digest_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()


@dataclass(frozen=True, slots=True)
class TaskExecutionRequirements:
    schema_version: str
    executor_id: str
    operation_versions: tuple[tuple[str, str], ...]
    durability_level: str
    side_effect_class: str
    project_serialization: str

    def __post_init__(self) -> None:
        if self.schema_version != TASK_EXECUTION_REQUIREMENTS_SCHEMA_VERSION:
            raise ValueError("unsupported Task execution requirements schema version")
        _identifier(self.executor_id, "executor_id")
        if self.durability_level not in _DURABILITY_RANK:
            raise ValueError("Task execution requirements durability level must be D0, D1, or D2")
        _identifier(self.side_effect_class, "side_effect_class")
        if self.project_serialization != "exclusive":
            raise ValueError("Task project serialization must be exclusive")
        object.__setattr__(
            self,
            "operation_versions",
            _operation_versions(self.operation_versions),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "executor_id": self.executor_id,
            "operation_versions": [list(item) for item in self.operation_versions],
            "durability_level": self.durability_level,
            "side_effect_class": self.side_effect_class,
            "project_serialization": self.project_serialization,
        }

    @classmethod
    def from_dict(cls, payload: object) -> TaskExecutionRequirements:
        if type(payload) is not dict or set(payload) != _REQUIREMENT_FIELDS:
            raise ValueError("Task execution requirement fields are incomplete or unknown")
        return cls(
            schema_version=payload["schema_version"],
            executor_id=payload["executor_id"],
            operation_versions=_decoded_operation_versions(payload["operation_versions"]),
            durability_level=payload["durability_level"],
            side_effect_class=payload["side_effect_class"],
            project_serialization=payload["project_serialization"],
        )

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())


def _compatible(
    profile: ExecutorCapabilityProfile,
    requirements: TaskExecutionRequirements,
) -> bool:
    side_effect_fact = _SIDE_EFFECT_FACTS.get(requirements.side_effect_class)
    if side_effect_fact is None:
        return False
    supported_operations = dict(profile.operation_versions)
    return (
        profile.executor_id == requirements.executor_id
        and _DURABILITY_RANK[profile.durability_level] >= _DURABILITY_RANK[requirements.durability_level]
        and profile.project_serialization == requirements.project_serialization
        and side_effect_fact in profile.enforcement_facts
        and all(
            supported_operations.get(operation) == version for operation, version in requirements.operation_versions
        )
    )


@dataclass(frozen=True, slots=True)
class ExecutorSelection:
    profile: ExecutorCapabilityProfile
    profile_digest: str
    requirements: TaskExecutionRequirements

    def __post_init__(self) -> None:
        if type(self.profile) is not ExecutorCapabilityProfile:
            raise ValueError("selection profile must be an exact capability profile")
        if type(self.requirements) is not TaskExecutionRequirements:
            raise ValueError("selection requirements must be exact")
        if (
            type(self.profile_digest) is not str
            or _SHA256.fullmatch(self.profile_digest) is None
            or self.profile.digest_sha256() != self.profile_digest
        ):
            raise ValueError("selection profile digest does not match")
        if not _compatible(self.profile, self.requirements):
            raise ValueError("selection profile is not compatible with requirements")

    def to_dict(self) -> dict[str, object]:
        return {
            "profile": self.profile.to_dict(),
            "profile_digest": self.profile_digest,
            "requirements": self.requirements.to_dict(),
        }

    @classmethod
    def from_dict(cls, payload: object) -> ExecutorSelection:
        if type(payload) is not dict or set(payload) != _SELECTION_FIELDS:
            raise ValueError("Executor selection fields are incomplete or unknown")
        return cls(
            profile=ExecutorCapabilityProfile.from_dict(payload["profile"]),
            profile_digest=payload["profile_digest"],
            requirements=TaskExecutionRequirements.from_dict(payload["requirements"]),
        )

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())


def select_executor(
    profiles: Iterable[ExecutorCapabilityProfile],
    requirements: TaskExecutionRequirements,
) -> ExecutorSelection:
    """Select the lowest stable rank among profiles with exact compatibility."""

    if type(requirements) is not TaskExecutionRequirements:
        raise ValueError("requirements must be exact TaskExecutionRequirements")
    try:
        candidates = tuple(profiles)
    except TypeError as error:
        raise ValueError("profiles must be an iterable of exact profiles") from error
    if any(type(profile) is not ExecutorCapabilityProfile for profile in candidates):
        raise ValueError("profiles must contain exact ExecutorCapabilityProfile values")
    compatible = tuple(profile for profile in candidates if _compatible(profile, requirements))
    ranks = tuple((profile.profile_id, profile.adapter_id) for profile in compatible)
    if len(ranks) != len(set(ranks)):
        raise ValueError("compatible profile rank identities must be unique")
    if not compatible:
        raise FormalTaskViolation(
            "EXECUTOR_CAPABILITY_UNAVAILABLE",
            "no Executor capability profile satisfies the exact task requirements",
            ErrorCode.CAPABILITY_UNAVAILABLE,
        )
    selected = min(compatible, key=lambda profile: (profile.profile_id, profile.adapter_id))
    return ExecutorSelection(
        profile=selected,
        profile_digest=selected.digest_sha256(),
        requirements=requirements,
    )


__all__ = [
    "EXECUTOR_CAPABILITY_PROFILE_SCHEMA_VERSION",
    "PROJECT_MUTATION_SIDE_EFFECT_FACT",
    "TASK_EXECUTION_REQUIREMENTS_SCHEMA_VERSION",
    "ExecutorCapabilityProfile",
    "ExecutorSelection",
    "TaskExecutionRequirements",
    "select_executor",
]
