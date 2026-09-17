# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Trusted application codecs for retained task-source evidence.

An application registers its decoder during startup. The SDK never imports the
application and never treats unknown persisted evidence as an absent source.
The ``native_source`` wire key is retained for existing consumers and stores.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from threading import RLock
from typing import Any

from .contracts import CommandEnvelope, ScopeRef


class TaskSourceError(ValueError):
    def __init__(self, reason: str = "NATIVE_TASK_SOURCE_INVALID") -> None:
        super().__init__(reason)
        self.reason = reason


class TaskSourceEvidence(ABC):
    """Validated, immutable evidence owned by its originating application."""

    operation: str
    target_id: str | None

    @property
    @abstractmethod
    def scope(self) -> ScopeRef: ...

    @property
    @abstractmethod
    def digest(self) -> str: ...

    @abstractmethod
    def to_dict(self) -> dict[str, Any]: ...

    @classmethod
    @abstractmethod
    def from_dict(cls, value: object) -> TaskSourceEvidence: ...

    @abstractmethod
    def require_request(self, *, scope: ScopeRef, operation: str, instruction: str) -> None: ...

    @abstractmethod
    def agent_request(self, proposal: str, *, verified_update: bool = False) -> str: ...


_CODECS: dict[str, type[TaskSourceEvidence]] = {}
_LOCK = RLock()


def register_source_codec(version: str, codec: type[TaskSourceEvidence]) -> None:
    """Register a trusted decoder; an existing version cannot be rebound."""
    if not isinstance(version, str) or not version or len(version) > 256:
        raise TaskSourceError("TASK_SOURCE_VERSION_INVALID")
    if not isinstance(codec, type) or not issubclass(codec, TaskSourceEvidence):
        raise TaskSourceError("TASK_SOURCE_CODEC_INVALID")
    with _LOCK:
        prior = _CODECS.get(version)
        if prior is not None and prior is not codec:
            raise TaskSourceError("TASK_SOURCE_CODEC_ALREADY_REGISTERED")
        _CODECS[version] = codec


def source_from_payload(payload: Mapping[str, Any]) -> TaskSourceEvidence | None:
    if "native_source" not in payload:
        return None
    value = payload["native_source"]
    if not isinstance(value, dict) or not isinstance(value.get("contract_version"), str):
        raise TaskSourceError()
    with _LOCK:
        codec = _CODECS.get(value["contract_version"])
    if codec is None:
        raise TaskSourceError("TASK_SOURCE_CODEC_UNAVAILABLE")
    source = codec.from_dict(value)
    if not isinstance(source, codec):
        raise TaskSourceError("TASK_SOURCE_CODEC_RESULT_INVALID")
    if (
        getattr(source, "operation", None) not in {"task.create", "task.create_successor", "task.adjust"}
        or not hasattr(source, "target_id")
        or (source.target_id is not None and not isinstance(source.target_id, str))
    ):
        raise TaskSourceError("TASK_SOURCE_CODEC_RESULT_INVALID")
    return source


def source_extension(source: TaskSourceEvidence | None) -> dict[str, Any]:
    return {} if source is None else {"native_source": source.to_dict()}


def source_payload_fields(payload: Mapping[str, Any], base: set[str]) -> set[str]:
    return base | ({"native_source"} if "native_source" in payload else set())


def require_payload_source(command: CommandEnvelope) -> TaskSourceEvidence | None:
    source = source_from_payload(command.payload)
    if source is not None:
        source.require_request(
            scope=command.scope,
            operation=command.command_type,
            instruction=command.payload.get("adjustment" if command.command_type == "task.adjust" else "instruction"),
        )
        if command.command_type != "task.create" and source.target_id != command.target_ref.id:
            raise TaskSourceError("NATIVE_TASK_SOURCE_TARGET_MISMATCH")
    return source
