# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Task command, authority and progress primitives.

The legacy wire version and enum values are retained for existing databases and
consumers. This module has no application, speech provider or transport dependency.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Final, Generic, Protocol, TypeAlias, TypeVar

CONTRACT_VERSION: Final = "live-voice.contract.v2"

MAX_SAFE_INTEGER: Final = 9_007_199_254_740_991


class ErrorCode(StrEnum):
    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    UNSUPPORTED = "UNSUPPORTED"
    UNAUTHENTICATED = "UNAUTHENTICATED"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    NOT_FOUND = "NOT_FOUND"
    CONFLICT = "CONFLICT"
    STALE = "STALE"
    CAPABILITY_UNAVAILABLE = "CAPABILITY_UNAVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"
    PROTOCOL_VIOLATION = "PROTOCOL_VIOLATION"
    RESULT_UNKNOWN = "RESULT_UNKNOWN"
    INTERNAL = "INTERNAL"


class Assurance(StrEnum):
    REQUEST_ASSERTED = "request_asserted"
    AUTHENTICATED = "authenticated"


class IdentityKind(StrEnum):
    CONNECTION = "connection"
    MEDIA_SESSION = "media_session"
    TRACK = "track"
    INTERACTION = "interaction"
    TURN = "turn"
    RESPONSE = "response"
    ROUND = "round"
    TASK = "task"
    ATTEMPT = "attempt"
    COMMAND = "command"
    REQUEST = "request"
    EVENT = "event"


class CancelScope(StrEnum):
    PLAYBACK_STOP = "playback.stop"
    RESPONSE_CANCEL = "response.cancel"
    ROUND_CANCEL = "round.cancel"
    TASK_CANCEL = "task.cancel"


class SideEffectTarget(StrEnum):
    AGENT = "agent"
    TOOL = "tool"
    TASK = "task"


class LifecycleKind(StrEnum):
    INTERACTION = "interaction"
    TURN = "turn"
    RESPONSE = "response"
    ROUND = "round"
    TASK = "task"
    ATTEMPT = "attempt"


class TerminalOutcome(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"
    UNKNOWN = "unknown"


class Knowledge(StrEnum):
    KNOWN = "known"
    UNKNOWN = "unknown"


class ContextRevisionKind(StrEnum):
    VERSION = "version"
    SNAPSHOT = "snapshot"
    UNVERSIONED = "unversioned"


class WorkState(StrEnum):
    ACCEPTED = "accepted"
    RUNNING = "running"
    BLOCKED = "blocked"
    DECISION_REQUIRED = "decision_required"
    TERMINAL = "terminal"


class WorkSourceAuthority(StrEnum):
    HARNESS = "harness"
    TASK_CORE = "task_core"
    EXECUTOR = "executor"


class WorkUrgency(StrEnum):
    NORMAL = "normal"
    ATTENTION = "attention"
    URGENT = "urgent"
    UNKNOWN = "unknown"


class Speakability(StrEnum):
    NOT_SPEAKABLE = "not_speakable"
    ELIGIBLE = "eligible"
    ATTENTION_REQUESTED = "attention_requested"


@dataclass(frozen=True, slots=True)
class _FrozenObject:
    items: tuple[tuple[str, FrozenJson], ...]


@dataclass(frozen=True, slots=True)
class _FrozenArray:
    items: tuple[FrozenJson, ...]


FrozenJson: TypeAlias = None | bool | int | float | str | _FrozenObject | _FrozenArray


@dataclass(frozen=True, slots=True)
class ContractError:
    code: ErrorCode
    reason: str | None
    message: str
    retriable: bool
    correlation_id: str | None
    _details: _FrozenObject = field(repr=False)

    @property
    def details(self) -> dict[str, object]:
        return _thaw_object(self._details)

    def to_dict(self) -> dict[str, object]:
        return {
            "code": self.code.value,
            "reason": self.reason,
            "message": self.message,
            "retriable": self.retriable,
            "correlation_id": self.correlation_id,
            "details": self.details,
        }

    @classmethod
    def from_dict(cls, payload: object) -> ContractError:
        data = _strict_object(payload, field_name="error")
        _require_exact_keys(
            data,
            required={
                "code",
                "reason",
                "message",
                "retriable",
                "correlation_id",
                "details",
            },
            field_name="error",
        )
        return cls(
            code=_enum(ErrorCode, data["code"], "error.code"),
            reason=_optional_stable_reason(data["reason"], "error.reason"),
            message=_required_text(data["message"], "error.message"),
            retriable=_bool(data["retriable"], "error.retriable"),
            correlation_id=_optional_id(data["correlation_id"], "error.correlation_id"),
            _details=_freeze_object(data["details"], "error.details"),
        )


class ContractViolation(ValueError):
    def __init__(
        self,
        code: ErrorCode,
        reason: str,
        message: str,
        *,
        correlation_id: str | None = None,
        details: Mapping[str, object] | None = None,
        retriable: bool = False,
    ) -> None:
        super().__init__(message)
        self.error = ContractError(
            code=code,
            reason=reason,
            message=message,
            retriable=retriable,
            correlation_id=correlation_id,
            _details=_freeze_object(dict(details or {}), "error.details"),
        )

    @property
    def code(self) -> ErrorCode:
        return self.error.code

    @property
    def reason(self) -> str:
        assert self.error.reason is not None
        return self.error.reason


_EnumT = TypeVar("_EnumT", bound=StrEnum)

_ValueT = TypeVar("_ValueT")

_FactT = TypeVar("_FactT")

_NAMESPACE_RE = re.compile(r"^[a-z][a-z0-9_-]*(?:\.[a-z0-9][a-z0-9_-]*)+$")

_REASON_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")

_LOWER_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$")

_URI_SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")

_CONTEXT_WHITESPACE: Final = frozenset(
    {
        0x0085,
        0x00A0,
        0x1680,
        *range(0x2000, 0x200B),
        0x2028,
        0x2029,
        0x202F,
        0x205F,
        0x3000,
        0xFEFF,
    }
)


def _violation(
    reason: str,
    message: str,
    *,
    code: ErrorCode = ErrorCode.INVALID_ARGUMENT,
    details: Mapping[str, object] | None = None,
) -> ContractViolation:
    return ContractViolation(code, reason, message, details=details)


def _validate_unicode(value: str, field_name: str) -> str:
    if any(0xD800 <= ord(char) <= 0xDFFF for char in value):
        raise _violation(
            "INVALID_UNICODE_SCALAR",
            f"{field_name} contains an unpaired surrogate",
        )
    return value


def _required_text(value: object, field_name: str) -> str:
    if type(value) is not str or not value.strip():
        raise _violation("INVALID_REQUIRED_TEXT", f"{field_name} must be a non-empty string")
    return _validate_unicode(value, field_name)


def _context_required_text(value: object, field_name: str) -> str:
    if type(value) is not str:
        raise _violation("INVALID_REQUIRED_TEXT", f"{field_name} must be a non-empty string")
    normalized = _validate_unicode(value, field_name)
    if not any(
        ord(char) > 0x20 and not 0x7F <= ord(char) <= 0x9F and ord(char) not in _CONTEXT_WHITESPACE
        for char in normalized
    ):
        raise _violation("INVALID_REQUIRED_TEXT", f"{field_name} must be a non-empty string")
    return normalized


def _context_uri(value: object) -> str:
    uri = _context_required_text(value, "context_ref.uri")
    scheme = _URI_SCHEME_RE.match(uri)
    if (
        scheme is None
        or scheme.end() == len(uri)
        or any(ord(char) <= 0x20 or 0x7F <= ord(char) <= 0x9F or ord(char) in _CONTEXT_WHITESPACE for char in uri)
    ):
        raise _violation(
            "INVALID_CONTEXT_URI",
            "context_ref.uri must be a non-empty absolute URI without whitespace or controls",
        )
    return uri


def _optional_id(value: object, field_name: str) -> str | None:
    return None if value is None else _required_text(value, field_name)


def _optional_stable_reason(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    reason = _required_text(value, field_name)
    if not _REASON_RE.fullmatch(reason):
        raise _violation(
            "INVALID_ERROR_REASON",
            f"{field_name} must use stable UPPER_SNAKE_CASE",
        )
    return reason


def _enum(enum_type: type[_EnumT], value: object, field_name: str) -> _EnumT:
    if isinstance(value, enum_type):
        return value
    if type(value) is not str:
        raise _violation("INVALID_ENUM", f"{field_name} must be a string")
    try:
        return enum_type(value)
    except ValueError as exc:
        raise _violation("INVALID_ENUM", f"unknown {field_name} {value!r}") from exc


def _bool(value: object, field_name: str) -> bool:
    if type(value) is not bool:
        raise _violation("INVALID_BOOLEAN", f"{field_name} must be a boolean")
    return value


def _uint(value: object, field_name: str) -> int:
    if type(value) is not int or value < 0 or value > MAX_SAFE_INTEGER:
        raise _violation(
            "INVALID_SAFE_INTEGER",
            f"{field_name} must be an integer between 0 and {MAX_SAFE_INTEGER}",
        )
    return value


def _timestamp(value: object, field_name: str) -> str:
    text = _required_text(value, field_name)
    if not _UTC_RE.fullmatch(text):
        raise _violation("INVALID_UTC_TIMESTAMP", f"{field_name} must be an RFC3339 UTC timestamp")
    try:
        datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as exc:
        raise _violation("INVALID_UTC_TIMESTAMP", f"{field_name} must be an RFC3339 UTC timestamp") from exc
    return text


def _namespaced(value: object, field_name: str) -> str:
    text = _required_text(value, field_name)
    if not _NAMESPACE_RE.fullmatch(text):
        raise _violation(
            "INVALID_NAMESPACED_VALUE",
            f"{field_name} must be a lower-case namespaced string",
        )
    return text


def _strict_object(value: object, *, field_name: str) -> dict[str, object]:
    if type(value) is not dict:
        raise _violation("INVALID_JSON_OBJECT", f"{field_name} must be a plain JSON object")
    for key in value:
        if type(key) is not str:
            raise _violation("INVALID_JSON_KEY", f"{field_name} keys must be strings")
        _validate_unicode(key, f"{field_name} key")
    return value


def _strict_array(value: object, *, field_name: str) -> list[object]:
    if type(value) is not list:
        raise _violation("INVALID_JSON_ARRAY", f"{field_name} must be a JSON array")
    return value


def _require_exact_keys(
    data: Mapping[str, object],
    *,
    required: set[str],
    field_name: str,
    optional: set[str] | None = None,
) -> None:
    allowed = required | (optional or set())
    keys = set(data)
    missing = sorted(required - keys)
    unknown = sorted(keys - allowed)
    if missing:
        raise _violation(
            "MISSING_REQUIRED_FIELD",
            f"{field_name} is missing: {', '.join(missing)}",
        )
    if unknown:
        raise _violation("UNKNOWN_FIELD", f"{field_name} has unknown fields: {', '.join(unknown)}")


def _freeze_json(
    value: object,
    field_name: str,
    active: set[int] | None = None,
) -> FrozenJson:
    if value is None or type(value) is bool:
        return value
    if type(value) is str:
        return _validate_unicode(value, field_name)
    if type(value) is int:
        if abs(value) > MAX_SAFE_INTEGER:
            raise _violation(
                "INVALID_SAFE_INTEGER",
                f"{field_name} integer exceeds the cross-language safe range",
            )
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise _violation("NON_FINITE_NUMBER", f"{field_name} must be finite")
        if value.is_integer() and abs(value) > MAX_SAFE_INTEGER:
            raise _violation(
                "INVALID_SAFE_INTEGER",
                f"{field_name} integer exceeds the cross-language safe range",
            )
        return value
    if type(value) not in {dict, list}:
        raise _violation(
            "INVALID_JSON_VALUE",
            f"{field_name} contains non-JSON value {type(value).__name__}",
        )

    active = set() if active is None else active
    identity = id(value)
    if identity in active:
        raise _violation("CYCLIC_JSON", f"{field_name} contains a cycle")
    active.add(identity)
    try:
        if type(value) is list:
            return _FrozenArray(
                tuple(_freeze_json(item, f"{field_name}[{index}]", active) for index, item in enumerate(value))
            )
        data = _strict_object(value, field_name=field_name)
        return _FrozenObject(
            tuple(
                sorted(
                    ((key, _freeze_json(item, f"{field_name}.{key}", active)) for key, item in data.items()),
                    key=lambda item: item[0].encode("utf-16-be"),
                )
            )
        )
    finally:
        active.remove(identity)


def _freeze_object(value: object, field_name: str) -> _FrozenObject:
    frozen = _freeze_json(value, field_name)
    if not isinstance(frozen, _FrozenObject):
        raise _violation("INVALID_JSON_OBJECT", f"{field_name} must be an object")
    return frozen


def _thaw_json(value: FrozenJson) -> object:
    if isinstance(value, _FrozenObject):
        return {key: _thaw_json(item) for key, item in value.items}
    if isinstance(value, _FrozenArray):
        return [_thaw_json(item) for item in value.items]
    return value


def _thaw_object(value: _FrozenObject) -> dict[str, object]:
    thawed = _thaw_json(value)
    assert isinstance(thawed, dict)
    return thawed


def _canonical_number(value: int | float) -> str:
    if type(value) is int:
        return str(value)
    if value == 0:
        return "0"
    text = repr(value).lower()
    if "e" not in text:
        return text[:-2] if text.endswith(".0") else text
    mantissa, exponent_text = text.split("e")
    exponent = int(exponent_text)
    sign = ""
    if mantissa.startswith("-"):
        sign, mantissa = "-", mantissa[1:]
    digits = mantissa.replace(".", "")
    decimal_position = 1 + exponent
    absolute = abs(value)
    if 1e-6 <= absolute < 1e21:
        if decimal_position <= 0:
            return sign + "0." + "0" * (-decimal_position) + digits
        if decimal_position >= len(digits):
            return sign + digits + "0" * (decimal_position - len(digits))
        return sign + digits[:decimal_position] + "." + digits[decimal_position:]
    normalized = digits[0]
    if len(digits) > 1:
        normalized += "." + digits[1:].rstrip("0")
        normalized = normalized.rstrip(".")
    exponent_sign = "+" if exponent >= 0 else ""
    return f"{sign}{normalized}e{exponent_sign}{exponent}"


def _canonical_frozen(value: FrozenJson) -> str:
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, (int, float)):
        return _canonical_number(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, _FrozenArray):
        return "[" + ",".join(_canonical_frozen(item) for item in value.items) + "]"
    assert isinstance(value, _FrozenObject)
    return (
        "{"
        + ",".join(json.dumps(key, ensure_ascii=False) + ":" + _canonical_frozen(item) for key, item in value.items)
        + "}"
    )


def canonical_json(value: object) -> str:
    return _canonical_frozen(_freeze_json(value, "$"))


def canonical_json_bytes(value: object) -> bytes:
    return canonical_json(value).encode("utf-8")


@dataclass(frozen=True, slots=True)
class ScopeRef:
    subject_id: str
    project_id: str | None
    session_id: str | None
    assurance: Assurance

    @classmethod
    def from_dict(cls, payload: object) -> ScopeRef:
        data = _strict_object(payload, field_name="scope")
        _require_exact_keys(
            data,
            required={"subject_id", "project_id", "session_id", "assurance"},
            field_name="scope",
        )
        return cls(
            subject_id=_required_text(data["subject_id"], "scope.subject_id"),
            project_id=_optional_id(data["project_id"], "scope.project_id"),
            session_id=_optional_id(data["session_id"], "scope.session_id"),
            assurance=_enum(Assurance, data["assurance"], "scope.assurance"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "subject_id": self.subject_id,
            "project_id": self.project_id,
            "session_id": self.session_id,
            "assurance": self.assurance.value,
        }


@dataclass(frozen=True, slots=True)
class ContextRevision:
    kind: ContextRevisionKind
    value: str | None

    @classmethod
    def from_dict(cls, payload: object) -> ContextRevision:
        data = _strict_object(payload, field_name="context_ref.revision")
        kind = _enum(
            ContextRevisionKind,
            data.get("kind"),
            "context_ref.revision.kind",
        )
        required = (
            {"kind"}
            if kind is ContextRevisionKind.UNVERSIONED
            else {
                "kind",
                "value",
            }
        )
        _require_exact_keys(
            data,
            required=required,
            field_name="context_ref.revision",
        )
        return cls(
            kind=kind,
            value=(
                None
                if kind is ContextRevisionKind.UNVERSIONED
                else _context_required_text(data["value"], "context_ref.revision.value")
            ),
        )

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {"kind": self.kind.value}
        if self.value is not None:
            result["value"] = self.value
        return result


@dataclass(frozen=True, slots=True)
class ContextRedaction:
    policy_id: str
    redacted: bool
    fields: tuple[str, ...]

    @classmethod
    def from_dict(cls, payload: object) -> ContextRedaction:
        data = _strict_object(payload, field_name="context_ref.redaction")
        _require_exact_keys(
            data,
            required={"policy_id", "redacted", "fields"},
            field_name="context_ref.redaction",
        )
        fields: list[str] = []
        for index, item in enumerate(_strict_array(data["fields"], field_name="context_ref.redaction.fields")):
            fields.append(_context_required_text(item, f"context_ref.redaction.fields[{index}]"))
        return cls(
            policy_id=_context_required_text(data["policy_id"], "context_ref.redaction.policy_id"),
            redacted=_bool(data["redacted"], "context_ref.redaction.redacted"),
            fields=tuple(fields),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "policy_id": self.policy_id,
            "redacted": self.redacted,
            "fields": list(self.fields),
        }


@dataclass(frozen=True, slots=True)
class ContextRef:
    source: str
    stable_id: str
    uri: str
    revision: ContextRevision
    scope: ScopeRef
    permissions: tuple[str, ...]
    expires_at: str | None
    redaction: ContextRedaction
    _extensions: _FrozenObject = field(repr=False)

    @property
    def extensions(self) -> dict[str, object]:
        return _thaw_object(self._extensions)

    @classmethod
    def from_dict(cls, payload: object) -> ContextRef:
        data = _strict_object(payload, field_name="context_ref")
        _require_exact_keys(
            data,
            required={
                "source",
                "stable_id",
                "uri",
                "revision",
                "scope",
                "permissions",
                "expires_at",
                "redaction",
                "extensions",
            },
            field_name="context_ref",
        )
        uri = _context_uri(data["uri"])
        permissions: list[str] = []
        for index, item in enumerate(_strict_array(data["permissions"], field_name="context_ref.permissions")):
            permissions.append(_namespaced(item, f"context_ref.permissions[{index}]"))
        expires_at = data["expires_at"]
        return cls(
            source=_namespaced(data["source"], "context_ref.source"),
            stable_id=_context_required_text(data["stable_id"], "context_ref.stable_id"),
            uri=uri,
            revision=ContextRevision.from_dict(data["revision"]),
            scope=ScopeRef.from_dict(data["scope"]),
            permissions=tuple(permissions),
            expires_at=(None if expires_at is None else _timestamp(expires_at, "context_ref.expires_at")),
            redaction=ContextRedaction.from_dict(data["redaction"]),
            _extensions=_extensions(data["extensions"], "context_ref.extensions"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "stable_id": self.stable_id,
            "uri": self.uri,
            "revision": self.revision.to_dict(),
            "scope": self.scope.to_dict(),
            "permissions": list(self.permissions),
            "expires_at": self.expires_at,
            "redaction": self.redaction.to_dict(),
            "extensions": self.extensions,
        }


@dataclass(frozen=True, slots=True)
class IdentityRef:
    kind: IdentityKind
    id: str

    @classmethod
    def from_dict(cls, payload: object, *, expected_kind: IdentityKind | None = None) -> IdentityRef:
        data = _strict_object(payload, field_name="identity_ref")
        _require_exact_keys(data, required={"kind", "id"}, field_name="identity_ref")
        ref = cls(
            kind=_enum(IdentityKind, data["kind"], "identity_ref.kind"),
            id=_required_text(data["id"], "identity_ref.id"),
        )
        if expected_kind is not None and ref.kind is not expected_kind:
            raise _violation(
                "IDENTITY_KIND_MISMATCH",
                f"expected {expected_kind.value}, got {ref.kind.value}",
            )
        return ref

    def to_dict(self) -> dict[str, str]:
        return {"kind": self.kind.value, "id": self.id}


@dataclass(frozen=True, slots=True)
class ConnectionEpochRef:
    connection_id: str
    connection_epoch: int

    @classmethod
    def from_dict(cls, payload: object) -> ConnectionEpochRef:
        data = _strict_object(payload, field_name="connection_epoch_ref")
        _require_exact_keys(
            data,
            required={"connection_id", "connection_epoch"},
            field_name="connection_epoch_ref",
        )
        return cls(
            connection_id=_required_text(data["connection_id"], "connection_epoch_ref.connection_id"),
            connection_epoch=_uint(data["connection_epoch"], "connection_epoch_ref.connection_epoch"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "connection_id": self.connection_id,
            "connection_epoch": self.connection_epoch,
        }


@dataclass(frozen=True, slots=True)
class OriginRef:
    kind: str
    turn_id: str | None
    commit_id: str | None

    @classmethod
    def from_dict(cls, payload: object) -> OriginRef:
        data = _strict_object(payload, field_name="origin")
        _require_exact_keys(data, required={"kind", "turn_id", "commit_id"}, field_name="origin")
        kind = data["kind"]
        if kind not in {"structured", "committed_turn"}:
            raise _violation("INVALID_ORIGIN", f"unknown origin.kind {kind!r}")
        turn_id = _optional_id(data["turn_id"], "origin.turn_id")
        commit_id = _optional_id(data["commit_id"], "origin.commit_id")
        if kind == "structured" and (turn_id is not None or commit_id is not None):
            raise _violation("INVALID_ORIGIN", "structured origin forbids turn_id and commit_id")
        if kind == "committed_turn" and (turn_id is None or commit_id is None):
            raise _violation("INVALID_ORIGIN", "committed_turn origin requires turn_id and commit_id")
        return cls(kind=kind, turn_id=turn_id, commit_id=commit_id)

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "turn_id": self.turn_id,
            "commit_id": self.commit_id,
        }


_COMMAND_TARGETS: Final = MappingProxyType(
    {
        "task.create": IdentityKind.TASK,
        "task.adjust": IdentityKind.TASK,
        "task.update": IdentityKind.TASK,
        "task.provide_input": IdentityKind.TASK,
        "task.pause": IdentityKind.TASK,
        "task.resume": IdentityKind.TASK,
        "task.reprioritize": IdentityKind.TASK,
        "task.create_successor": IdentityKind.TASK,
        "task.ack_events": IdentityKind.TASK,
        "task.retry": IdentityKind.TASK,
        CancelScope.PLAYBACK_STOP.value: IdentityKind.RESPONSE,
        CancelScope.RESPONSE_CANCEL.value: IdentityKind.RESPONSE,
        CancelScope.ROUND_CANCEL.value: IdentityKind.ROUND,
        CancelScope.TASK_CANCEL.value: IdentityKind.TASK,
    }
)

_QUERY_TARGETS: Final = MappingProxyType(
    {
        "task.get": IdentityKind.TASK,
        "task.list": IdentityKind.TASK,
        "task.status": IdentityKind.TASK,
        "task.events": IdentityKind.TASK,
        "task.result": IdentityKind.TASK,
        "task.unread_events": IdentityKind.TASK,
    }
)

_WAVE2_COMMAND_TYPES: Final = frozenset(
    {
        "task.update",
        "task.provide_input",
        "task.pause",
        "task.resume",
        "task.reprioritize",
        "task.create_successor",
        "task.ack_events",
    }
)

_TASK_PRIORITIES: Final = frozenset({"low", "normal", "high", "urgent"})

_PRESENTATION_CLASSES: Final = frozenset({"text", "voice"})

_TASK_SIDE_EFFECT_CLASSES: Final = frozenset({"read_only", "project_mutation"})

_COMMAND_DISPOSITIONS: Final = frozenset(
    {
        "accepted",
        "applied",
        "rejected",
        "unsupported",
        "conflict",
        "timeout",
        "unknown",
    }
)

_POSITIVE_COMMAND_DISPOSITIONS: Final = frozenset({"accepted", "applied"})

_COMMAND_DISPOSITION_ERROR_CODES: Final = MappingProxyType(
    {
        "rejected": frozenset(
            {
                ErrorCode.INVALID_ARGUMENT,
                ErrorCode.UNAUTHENTICATED,
                ErrorCode.PERMISSION_DENIED,
                ErrorCode.NOT_FOUND,
            }
        ),
        "unsupported": frozenset({ErrorCode.UNSUPPORTED, ErrorCode.CAPABILITY_UNAVAILABLE}),
        "conflict": frozenset({ErrorCode.CONFLICT, ErrorCode.STALE}),
        "timeout": frozenset({ErrorCode.TIMEOUT}),
        "unknown": frozenset({ErrorCode.RESULT_UNKNOWN}),
    }
)

_CORE_CAPABILITIES: Final = frozenset(
    {
        *_COMMAND_TARGETS,
        *_QUERY_TARGETS,
        "event.replay",
        "recognize.batch",
        "recognize.stream",
        "synthesize.batch",
        "synthesize.stream",
        "cancel.ack",
    }
)


def _extensions(value: object, field_name: str) -> _FrozenObject:
    data = _strict_object(value, field_name=field_name)
    for key in data:
        _namespaced(key, f"{field_name} key")
    return _freeze_object(data, field_name)


def _context_refs(value: object, field_name: str, *, scope: ScopeRef) -> tuple[ContextRef, ...]:
    parsed: list[ContextRef] = []
    for index, item in enumerate(_strict_array(value, field_name=field_name)):
        ref = ContextRef.from_dict(item)
        if ref.scope != scope:
            raise _violation(
                "CONTEXT_SCOPE_MISMATCH",
                f"{field_name}[{index}] does not match the enclosing scope",
                code=ErrorCode.PERMISSION_DENIED,
            )
        parsed.append(ref)
    return tuple(parsed)


def _capability_list(value: object, field_name: str) -> tuple[str, ...]:
    values = _strict_array(value, field_name=field_name)
    parsed: list[str] = []
    for index, item in enumerate(values):
        capability = _namespaced(item, f"{field_name}[{index}]")
        if capability not in _CORE_CAPABILITIES:
            raise _violation(
                "UNKNOWN_REQUIRED_CAPABILITY",
                f"unknown required capability {capability!r}",
                code=ErrorCode.UNSUPPORTED,
            )
        if capability in parsed:
            raise _violation(
                "DUPLICATE_REQUIRED_CAPABILITY",
                f"duplicate required capability {capability!r}",
            )
        parsed.append(capability)
    return tuple(parsed)


def _require_operation_capability(values: tuple[str, ...], operation: str, field_name: str) -> None:
    if values != (operation,):
        raise _violation(
            "REQUIRED_CAPABILITY_MISMATCH",
            f"{field_name} must contain only {operation!r}",
            code=ErrorCode.PERMISSION_DENIED,
        )


def _bounded_text(
    value: object,
    field_name: str,
    *,
    max_utf8_bytes: int | None,
) -> str:
    text = _required_text(value, field_name)
    if "\x00" in text or (max_utf8_bytes is not None and len(text.encode("utf-8")) > max_utf8_bytes):
        raise _violation(
            "INVALID_BOUNDED_TEXT",
            f"{field_name} contains NUL or exceeds its UTF-8 byte bound",
        )
    return text


def _optional_bounded_text(
    value: object,
    field_name: str,
    *,
    max_utf8_bytes: int,
) -> str | None:
    if value is None:
        return None
    return _bounded_text(value, field_name, max_utf8_bytes=max_utf8_bytes)


def _constraint_list(
    value: object,
    field_name: str,
    *,
    nullable: bool,
) -> list[str] | None:
    if value is None:
        if nullable:
            return None
        raise _violation(
            "INVALID_TASK_CONSTRAINTS",
            f"{field_name} must be an array",
        )
    values = _strict_array(value, field_name=field_name)
    if len(values) > 16:
        raise _violation(
            "INVALID_TASK_CONSTRAINTS",
            f"{field_name} cannot contain more than 16 entries",
        )
    parsed = [
        _bounded_text(
            item,
            f"{field_name}[{index}]",
            max_utf8_bytes=1_024,
        )
        for index, item in enumerate(values)
    ]
    if len(set(parsed)) != len(parsed):
        raise _violation(
            "INVALID_TASK_CONSTRAINTS",
            f"{field_name} entries must be unique",
        )
    if sum(len(item.encode("utf-8")) for item in parsed) > 4_096:
        raise _violation(
            "INVALID_TASK_CONSTRAINTS",
            f"{field_name} exceeds its aggregate UTF-8 byte bound",
        )
    return parsed


def _closed_string_map(value: object, field_name: str) -> dict[str, str]:
    data = _strict_object(value, field_name=field_name)
    parsed: dict[str, str] = {}
    for key, item in data.items():
        parsed[_bounded_text(key, f"{field_name} key", max_utf8_bytes=None)] = _bounded_text(
            item, f"{field_name}.{key}", max_utf8_bytes=None
        )
    return parsed


def _closed_value(value: object, field_name: str, *, values: frozenset[str]) -> str:
    if type(value) is not str or value not in values:
        raise _violation(
            "INVALID_ENUM",
            f"unknown {field_name} {value!r}",
        )
    return value


def _successor_outcome_and_digest(data: Mapping[str, object]) -> None:
    outcome = _enum(
        TerminalOutcome,
        data["predecessor_outcome"],
        "command.payload.predecessor_outcome",
    )
    digest = data["predecessor_result_sha256"]
    if outcome is TerminalOutcome.COMPLETED:
        if type(digest) is not str or not _LOWER_SHA256_RE.fullmatch(digest):
            raise _violation(
                "INVALID_PREDECESSOR_RESULT_DIGEST",
                "completed predecessor requires a lowercase SHA-256 digest",
            )
    elif digest is not None:
        raise _violation(
            "INVALID_PREDECESSOR_RESULT_DIGEST",
            "non-completed predecessor forbids a result digest",
        )


def _command_payload(command_type: str, value: object) -> _FrozenObject:
    data = _strict_object(value, field_name="command.payload")
    if command_type in {
        CancelScope.PLAYBACK_STOP.value,
        CancelScope.RESPONSE_CANCEL.value,
    }:
        _require_exact_keys(
            data,
            required={"interaction_id", "response_generation"},
            field_name="command.payload",
        )
        _required_text(data["interaction_id"], "command.payload.interaction_id")
        _uint(data["response_generation"], "command.payload.response_generation")
    elif command_type in {
        CancelScope.ROUND_CANCEL.value,
        CancelScope.TASK_CANCEL.value,
    }:
        _require_exact_keys(data, required=set(), field_name="command.payload")
    elif command_type == "task.retry":
        _require_exact_keys(
            data,
            required={"previous_attempt_id", "previous_outcome", "attempt_number"},
            field_name="command.payload",
        )
        _required_text(data["previous_attempt_id"], "command.payload.previous_attempt_id")
        outcome = _enum(
            TerminalOutcome,
            data["previous_outcome"],
            "command.payload.previous_outcome",
        )
        if outcome not in {TerminalOutcome.CANCELLED, TerminalOutcome.COMPLETED}:
            raise _violation(
                "TASK_RETRY_OUTCOME_NOT_ELIGIBLE",
                "task.retry permits only cancelled or completed predecessors",
                code=ErrorCode.CONFLICT,
            )
        attempt_number = _uint(data["attempt_number"], "command.payload.attempt_number")
        if attempt_number not in {2, 3}:
            raise _violation(
                "TASK_RETRY_ATTEMPT_NUMBER_INVALID",
                "task.retry attempt_number must be 2 or 3",
            )
    elif command_type == "task.adjust":
        _require_exact_keys(
            data,
            required={"adjustment"},
            optional={"native_source"},
            field_name="command.payload",
        )
        adjustment = _required_text(data["adjustment"], "command.payload.adjustment")
        if "\x00" in adjustment or len(adjustment.encode("utf-8")) > 4_096:
            raise _violation(
                "INVALID_TASK_ADJUSTMENT",
                "task.adjust payload exceeds its closed content bound",
            )
    elif command_type == "task.update":
        _require_exact_keys(
            data,
            required={
                "attempt_id",
                "expected_event_head",
                "instruction",
                "constraints",
            },
            field_name="command.payload",
        )
        _required_text(data["attempt_id"], "command.payload.attempt_id")
        _uint(data["expected_event_head"], "command.payload.expected_event_head")
        instruction = data["instruction"]
        constraints = data["constraints"]
        if instruction is None and constraints is None:
            raise _violation(
                "EMPTY_TASK_UPDATE",
                "task.update requires instruction or constraints",
            )
        if instruction is not None:
            _bounded_text(
                instruction,
                "command.payload.instruction",
                max_utf8_bytes=4_096,
            )
        _constraint_list(
            constraints,
            "command.payload.constraints",
            nullable=True,
        )
    elif command_type == "task.provide_input":
        _require_exact_keys(
            data,
            required={
                "attempt_id",
                "expected_event_head",
                "responds_to_event_id",
                "text",
            },
            field_name="command.payload",
        )
        _required_text(data["attempt_id"], "command.payload.attempt_id")
        _uint(data["expected_event_head"], "command.payload.expected_event_head")
        _required_text(
            data["responds_to_event_id"],
            "command.payload.responds_to_event_id",
        )
        _bounded_text(
            data["text"],
            "command.payload.text",
            max_utf8_bytes=4_096,
        )
    elif command_type in {"task.pause", "task.resume"}:
        _require_exact_keys(
            data,
            required={"attempt_id", "expected_event_head", "reason"},
            field_name="command.payload",
        )
        _required_text(data["attempt_id"], "command.payload.attempt_id")
        _uint(data["expected_event_head"], "command.payload.expected_event_head")
        _optional_bounded_text(
            data["reason"],
            "command.payload.reason",
            max_utf8_bytes=1_024,
        )
    elif command_type == "task.reprioritize":
        _require_exact_keys(
            data,
            required={
                "attempt_id",
                "expected_event_head",
                "priority",
                "reason",
            },
            field_name="command.payload",
        )
        _required_text(data["attempt_id"], "command.payload.attempt_id")
        _uint(data["expected_event_head"], "command.payload.expected_event_head")
        _closed_value(
            data["priority"],
            "command.payload.priority",
            values=_TASK_PRIORITIES,
        )
        _optional_bounded_text(
            data["reason"],
            "command.payload.reason",
            max_utf8_bytes=1_024,
        )
    elif command_type == "task.create_successor":
        _require_exact_keys(
            data,
            required={
                "expected_predecessor_revision_number",
                "expected_predecessor_event_head",
                "predecessor_terminal_event_id",
                "predecessor_outcome",
                "predecessor_result_sha256",
                "name",
                "instruction",
                "constraints",
                "executor_id",
                "side_effect_class",
                "attributes",
            },
            optional={"native_source"},
            field_name="command.payload",
        )
        _uint(
            data["expected_predecessor_revision_number"],
            "command.payload.expected_predecessor_revision_number",
        )
        _uint(
            data["expected_predecessor_event_head"],
            "command.payload.expected_predecessor_event_head",
        )
        _required_text(
            data["predecessor_terminal_event_id"],
            "command.payload.predecessor_terminal_event_id",
        )
        _successor_outcome_and_digest(data)
        _bounded_text(
            data["name"],
            "command.payload.name",
            max_utf8_bytes=None,
        )
        _bounded_text(
            data["instruction"],
            "command.payload.instruction",
            max_utf8_bytes=4_096,
        )
        _constraint_list(
            data["constraints"],
            "command.payload.constraints",
            nullable=False,
        )
        _bounded_text(
            data["executor_id"],
            "command.payload.executor_id",
            max_utf8_bytes=None,
        )
        _closed_value(
            data["side_effect_class"],
            "command.payload.side_effect_class",
            values=_TASK_SIDE_EFFECT_CLASSES,
        )
        _closed_string_map(data["attributes"], "command.payload.attributes")
    elif command_type == "task.ack_events":
        _require_exact_keys(
            data,
            required={
                "presentation_class",
                "acked_through_seq",
                "acked_event_id",
                "expected_event_head",
            },
            field_name="command.payload",
        )
        _closed_value(
            data["presentation_class"],
            "command.payload.presentation_class",
            values=_PRESENTATION_CLASSES,
        )
        _uint(data["acked_through_seq"], "command.payload.acked_through_seq")
        _required_text(data["acked_event_id"], "command.payload.acked_event_id")
        _uint(data["expected_event_head"], "command.payload.expected_event_head")
    return _freeze_object(data, "command.payload")


def _query_payload(query_type: str, value: object) -> _FrozenObject:
    data = _strict_object(value, field_name="query.payload")
    if query_type == "task.unread_events":
        _require_exact_keys(
            data,
            required={"presentation_class", "limit"},
            field_name="query.payload",
        )
        _closed_value(
            data["presentation_class"],
            "query.payload.presentation_class",
            values=_PRESENTATION_CLASSES,
        )
        limit = _uint(data["limit"], "query.payload.limit")
        if not 1 <= limit <= 500:
            raise _violation(
                "INVALID_UNREAD_LIMIT",
                "query.payload.limit must be between 1 and 500",
            )
    return _freeze_object(data, "query.payload")


def _result_extensions(
    value: object,
    *,
    command_result: bool,
    ok: bool,
    error: ContractError | None,
) -> _FrozenObject:
    field_name = "result.extensions"
    data = _strict_object(value, field_name=field_name)
    for key in data:
        _namespaced(key, f"{field_name} key")
    if "live_voice.command" not in data:
        return _freeze_object(data, field_name)
    if not command_result:
        raise _violation(
            "COMMAND_RESULT_EXTENSION_FORBIDDEN",
            "query results cannot carry a command disposition",
            code=ErrorCode.PROTOCOL_VIOLATION,
        )
    command = _strict_object(
        data["live_voice.command"],
        field_name="result.extensions.live_voice.command",
    )
    _require_exact_keys(
        command,
        required={
            "disposition",
            "admission_event_id",
            "settlement_event_id",
        },
        field_name="result.extensions.live_voice.command",
    )
    disposition_value = command["disposition"]
    if type(disposition_value) is not str or disposition_value not in _COMMAND_DISPOSITIONS:
        raise _violation(
            "INVALID_COMMAND_DISPOSITION",
            "result command disposition is unknown",
            code=ErrorCode.PROTOCOL_VIOLATION,
        )
    _optional_id(
        command["admission_event_id"],
        "result.extensions.live_voice.command.admission_event_id",
    )
    _optional_id(
        command["settlement_event_id"],
        "result.extensions.live_voice.command.settlement_event_id",
    )
    if disposition_value in _POSITIVE_COMMAND_DISPOSITIONS:
        if not ok or error is not None:
            raise _violation(
                "COMMAND_DISPOSITION_RESULT_MISMATCH",
                "accepted/applied command disposition requires ok=true",
                code=ErrorCode.PROTOCOL_VIOLATION,
            )
    else:
        if ok or error is None:
            raise _violation(
                "COMMAND_DISPOSITION_RESULT_MISMATCH",
                "negative command disposition requires ok=false",
                code=ErrorCode.PROTOCOL_VIOLATION,
            )
        allowed_codes = _COMMAND_DISPOSITION_ERROR_CODES[disposition_value]
        if error.code not in allowed_codes:
            raise _violation(
                "COMMAND_DISPOSITION_ERROR_MISMATCH",
                "command disposition does not match its error family",
                code=ErrorCode.PROTOCOL_VIOLATION,
            )
    return _freeze_object(data, field_name)


@dataclass(frozen=True, slots=True)
class CommandEnvelope:
    request_id: str
    command_id: str
    command_type: str
    issued_at: str
    scope: ScopeRef
    correlation_id: str
    causation_id: str | None
    origin: OriginRef
    target_ref: IdentityRef
    context_refs: tuple[ContextRef, ...]
    required_capabilities: tuple[str, ...]
    _payload: _FrozenObject = field(repr=False)
    _extensions: _FrozenObject = field(repr=False)
    contract_version: str = CONTRACT_VERSION

    @property
    def payload(self) -> dict[str, object]:
        return _thaw_object(self._payload)

    @property
    def extensions(self) -> dict[str, object]:
        return _thaw_object(self._extensions)

    @classmethod
    def from_dict(
        cls,
        payload: object,
        *,
        identities: IdentityAuthority | None = None,
        commits: CommittedInputAuthority | None = None,
    ) -> CommandEnvelope:
        data = _strict_object(payload, field_name="command")
        required = {
            "contract_version",
            "request_id",
            "command_id",
            "command_type",
            "issued_at",
            "scope",
            "correlation_id",
            "causation_id",
            "origin",
            "target_ref",
            "context_refs",
            "required_capabilities",
            "payload",
            "extensions",
        }
        _require_exact_keys(data, required=required, field_name="command")
        if data["contract_version"] != CONTRACT_VERSION:
            raise _violation(
                "UNSUPPORTED_CONTRACT_VERSION",
                f"expected {CONTRACT_VERSION}",
                code=ErrorCode.UNSUPPORTED,
            )
        command_type = _namespaced(data["command_type"], "command.command_type")
        expected_kind = _COMMAND_TARGETS.get(command_type)
        if expected_kind is None:
            raise _violation(
                "UNSUPPORTED_COMMAND_TYPE",
                f"unsupported command type {command_type!r}",
                code=ErrorCode.UNSUPPORTED,
            )
        scope = ScopeRef.from_dict(data["scope"])
        origin = OriginRef.from_dict(data["origin"])
        target_ref = IdentityRef.from_dict(data["target_ref"], expected_kind=expected_kind)
        required_capabilities = _capability_list(data["required_capabilities"], "command.required_capabilities")
        if command_type in _WAVE2_COMMAND_TYPES:
            _require_operation_capability(
                required_capabilities,
                command_type,
                "command.required_capabilities",
            )
        result = cls(
            request_id=_required_text(data["request_id"], "command.request_id"),
            command_id=_required_text(data["command_id"], "command.command_id"),
            command_type=command_type,
            issued_at=_timestamp(data["issued_at"], "command.issued_at"),
            scope=scope,
            correlation_id=_required_text(data["correlation_id"], "command.correlation_id"),
            causation_id=_optional_id(data["causation_id"], "command.causation_id"),
            origin=origin,
            target_ref=target_ref,
            context_refs=_context_refs(data["context_refs"], "command.context_refs", scope=scope),
            required_capabilities=required_capabilities,
            _payload=_command_payload(command_type, data["payload"]),
            _extensions=_extensions(data["extensions"], "command.extensions"),
        )
        if identities is not None:
            if command_type != "task.create":
                identities.require(target_ref, scope=scope)
            if command_type in {
                CancelScope.PLAYBACK_STOP.value,
                CancelScope.RESPONSE_CANCEL.value,
            }:
                interaction = IdentityRef(
                    IdentityKind.INTERACTION,
                    str(result.payload["interaction_id"]),
                )
                identities.require(interaction, scope=scope)
                identities.require(target_ref, scope=scope, parent=interaction)
            if origin.kind == "committed_turn":
                identities.require(IdentityRef(IdentityKind.TURN, origin.turn_id or ""), scope=scope)
        if origin.kind == "committed_turn" and commits is not None:
            commits.require_origin(origin, scope)
        return result

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "request_id": self.request_id,
            "command_id": self.command_id,
            "command_type": self.command_type,
            "issued_at": self.issued_at,
            "scope": self.scope.to_dict(),
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "origin": self.origin.to_dict(),
            "target_ref": self.target_ref.to_dict(),
            "context_refs": [ref.to_dict() for ref in self.context_refs],
            "required_capabilities": list(self.required_capabilities),
            "payload": self.payload,
            "extensions": self.extensions,
        }

    def fingerprint(self) -> bytes:
        payload = self.to_dict()
        del payload["request_id"]
        return canonical_json_bytes(payload)


@dataclass(frozen=True, slots=True)
class QueryEnvelope:
    request_id: str
    query_type: str
    issued_at: str
    scope: ScopeRef
    correlation_id: str
    causation_id: str | None
    target_ref: IdentityRef
    context_refs: tuple[ContextRef, ...]
    required_capabilities: tuple[str, ...]
    _payload: _FrozenObject = field(repr=False)
    _extensions: _FrozenObject = field(repr=False)
    contract_version: str = CONTRACT_VERSION

    @property
    def payload(self) -> dict[str, object]:
        return _thaw_object(self._payload)

    @property
    def extensions(self) -> dict[str, object]:
        return _thaw_object(self._extensions)

    @classmethod
    def from_dict(
        cls,
        payload: object,
        *,
        identities: IdentityAuthority | None = None,
    ) -> QueryEnvelope:
        data = _strict_object(payload, field_name="query")
        required = {
            "contract_version",
            "request_id",
            "query_type",
            "issued_at",
            "scope",
            "correlation_id",
            "causation_id",
            "target_ref",
            "context_refs",
            "required_capabilities",
            "payload",
            "extensions",
        }
        _require_exact_keys(data, required=required, field_name="query")
        if data["contract_version"] != CONTRACT_VERSION:
            raise _violation(
                "UNSUPPORTED_CONTRACT_VERSION",
                f"expected {CONTRACT_VERSION}",
                code=ErrorCode.UNSUPPORTED,
            )
        query_type = _namespaced(data["query_type"], "query.query_type")
        expected_kind = _QUERY_TARGETS.get(query_type)
        if expected_kind is None:
            raise _violation(
                "UNSUPPORTED_QUERY_TYPE",
                f"unsupported read-only query {query_type!r}",
                code=ErrorCode.UNSUPPORTED,
            )
        scope = ScopeRef.from_dict(data["scope"])
        target_ref = IdentityRef.from_dict(data["target_ref"], expected_kind=expected_kind)
        required_capabilities = _capability_list(data["required_capabilities"], "query.required_capabilities")
        if query_type == "task.unread_events":
            _require_operation_capability(
                required_capabilities,
                query_type,
                "query.required_capabilities",
            )
        result = cls(
            request_id=_required_text(data["request_id"], "query.request_id"),
            query_type=query_type,
            issued_at=_timestamp(data["issued_at"], "query.issued_at"),
            scope=scope,
            correlation_id=_required_text(data["correlation_id"], "query.correlation_id"),
            causation_id=_optional_id(data["causation_id"], "query.causation_id"),
            target_ref=target_ref,
            context_refs=_context_refs(data["context_refs"], "query.context_refs", scope=scope),
            required_capabilities=required_capabilities,
            _payload=_query_payload(query_type, data["payload"]),
            _extensions=_extensions(data["extensions"], "query.extensions"),
        )
        if identities is not None:
            identities.require(target_ref, scope=scope)
        return result

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "request_id": self.request_id,
            "query_type": self.query_type,
            "issued_at": self.issued_at,
            "scope": self.scope.to_dict(),
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "target_ref": self.target_ref.to_dict(),
            "context_refs": [ref.to_dict() for ref in self.context_refs],
            "required_capabilities": list(self.required_capabilities),
            "payload": self.payload,
            "extensions": self.extensions,
        }


@dataclass(frozen=True, slots=True)
class ResultEnvelope:
    request_id: str
    command_id: str | None
    ok: bool
    _result: _FrozenObject | None
    error: ContractError | None
    observed_at: str
    _extensions: _FrozenObject = field(repr=False)
    contract_version: str = CONTRACT_VERSION

    @property
    def result(self) -> dict[str, object] | None:
        return None if self._result is None else _thaw_object(self._result)

    @property
    def extensions(self) -> dict[str, object]:
        return _thaw_object(self._extensions)

    @classmethod
    def success(
        cls,
        *,
        owner: CommandEnvelope | QueryEnvelope,
        result: Mapping[str, object],
        observed_at: str,
        extensions: Mapping[str, object] | None = None,
    ) -> ResultEnvelope:
        return cls(
            request_id=owner.request_id,
            command_id=(owner.command_id if isinstance(owner, CommandEnvelope) else None),
            ok=True,
            _result=_freeze_object(dict(result), "result.result"),
            error=None,
            observed_at=_timestamp(observed_at, "result.observed_at"),
            _extensions=_result_extensions(
                dict(extensions or {}),
                command_result=isinstance(owner, CommandEnvelope),
                ok=True,
                error=None,
            ),
        )

    @classmethod
    def failure(
        cls,
        *,
        owner: CommandEnvelope | QueryEnvelope,
        error: ContractError,
        observed_at: str,
        extensions: Mapping[str, object] | None = None,
    ) -> ResultEnvelope:
        return cls(
            request_id=owner.request_id,
            command_id=(owner.command_id if isinstance(owner, CommandEnvelope) else None),
            ok=False,
            _result=None,
            error=error,
            observed_at=_timestamp(observed_at, "result.observed_at"),
            _extensions=_result_extensions(
                dict(extensions or {}),
                command_result=isinstance(owner, CommandEnvelope),
                ok=False,
                error=error,
            ),
        )

    @classmethod
    def from_dict(
        cls,
        payload: object,
        *,
        owner: CommandEnvelope | QueryEnvelope | None = None,
    ) -> ResultEnvelope:
        data = _strict_object(payload, field_name="result")
        _require_exact_keys(
            data,
            required={
                "contract_version",
                "request_id",
                "command_id",
                "ok",
                "result",
                "error",
                "observed_at",
                "extensions",
            },
            field_name="result",
        )
        if data["contract_version"] != CONTRACT_VERSION:
            raise _violation(
                "UNSUPPORTED_CONTRACT_VERSION",
                f"expected {CONTRACT_VERSION}",
                code=ErrorCode.UNSUPPORTED,
            )
        ok = _bool(data["ok"], "result.ok")
        result_value = None if data["result"] is None else _freeze_object(data["result"], "result.result")
        error_value = None if data["error"] is None else ContractError.from_dict(data["error"])
        if ok and (result_value is None or error_value is not None):
            raise _violation(
                "INVALID_RESULT_EXCLUSIVITY",
                "ok result requires result and forbids error",
                code=ErrorCode.PROTOCOL_VIOLATION,
            )
        if not ok and (result_value is not None or error_value is None):
            raise _violation(
                "INVALID_RESULT_EXCLUSIVITY",
                "failed result requires error and forbids result",
                code=ErrorCode.PROTOCOL_VIOLATION,
            )
        request_id = _required_text(data["request_id"], "result.request_id")
        command_id = _optional_id(data["command_id"], "result.command_id")
        command_result = isinstance(owner, CommandEnvelope) if owner is not None else command_id is not None
        result = cls(
            request_id=request_id,
            command_id=command_id,
            ok=ok,
            _result=result_value,
            error=error_value,
            observed_at=_timestamp(data["observed_at"], "result.observed_at"),
            _extensions=_result_extensions(
                data["extensions"],
                command_result=command_result,
                ok=ok,
                error=error_value,
            ),
        )
        if owner is not None:
            expected_command_id = owner.command_id if isinstance(owner, CommandEnvelope) else None
            if result.request_id != owner.request_id or result.command_id != expected_command_id:
                raise _violation(
                    "RESULT_OWNER_MISMATCH",
                    "result request_id/command_id does not match its owner",
                    code=ErrorCode.PROTOCOL_VIOLATION,
                )
        return result

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "request_id": self.request_id,
            "command_id": self.command_id,
            "ok": self.ok,
            "result": self.result,
            "error": None if self.error is None else self.error.to_dict(),
            "observed_at": self.observed_at,
            "extensions": self.extensions,
        }

    def for_request(self, request_id: str) -> ResultEnvelope:
        return ResultEnvelope(
            request_id=_required_text(request_id, "result.request_id"),
            command_id=self.command_id,
            ok=self.ok,
            _result=self._result,
            error=self.error,
            observed_at=self.observed_at,
            _extensions=self._extensions,
        )


@dataclass(frozen=True, slots=True)
class KnownFact(Generic[_FactT]):
    knowledge: Knowledge
    value: _FactT | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.knowledge, Knowledge):
            raise _violation(
                "INVALID_ENUM",
                "known_fact.knowledge must be known or unknown",
            )
        if self.knowledge is Knowledge.KNOWN and self.value is None:
            raise _violation("KNOWN_FACT_VALUE_REQUIRED", "a known fact requires a value")
        if self.knowledge is Knowledge.UNKNOWN and self.value is not None:
            raise _violation("UNKNOWN_FACT_VALUE_FORBIDDEN", "an unknown fact forbids a value")


def _known_fact(
    payload: object,
    field_name: str,
    parser: Callable[[object], _FactT],
) -> KnownFact[_FactT]:
    data = _strict_object(payload, field_name=field_name)
    knowledge = _enum(Knowledge, data.get("knowledge"), f"{field_name}.knowledge")
    required = {"knowledge", "value"} if knowledge is Knowledge.KNOWN else {"knowledge"}
    _require_exact_keys(data, required=required, field_name=field_name)
    return KnownFact(
        knowledge,
        None if knowledge is Knowledge.UNKNOWN else parser(data["value"]),
    )


def _fact_text(value: object, field_name: str) -> str:
    if type(value) is not str:
        raise _violation("INVALID_FACT_TEXT", f"{field_name} must be a string")
    return _validate_unicode(value, field_name)


@dataclass(frozen=True, slots=True)
class WorkProgressSource:
    authority: WorkSourceAuthority
    event_id: str
    source_work_ref: IdentityRef
    adapter: str | None

    @classmethod
    def from_dict(cls, payload: object) -> WorkProgressSource:
        data = _strict_object(payload, field_name="work_progress.source")
        _require_exact_keys(
            data,
            required={"authority", "event_id", "source_work_ref", "adapter"},
            field_name="work_progress.source",
        )
        source_work_ref = IdentityRef.from_dict(data["source_work_ref"])
        if source_work_ref.kind not in {
            IdentityKind.ROUND,
            IdentityKind.TASK,
            IdentityKind.ATTEMPT,
        }:
            raise _violation(
                "INVALID_PROGRESS_SOURCE_KIND",
                "source_work_ref must identify a round, task, or attempt",
            )
        authority = _enum(
            WorkSourceAuthority,
            data["authority"],
            "work_progress.source.authority",
        )
        expected_kind = {
            WorkSourceAuthority.HARNESS: IdentityKind.ROUND,
            WorkSourceAuthority.TASK_CORE: IdentityKind.TASK,
            WorkSourceAuthority.EXECUTOR: IdentityKind.ATTEMPT,
        }[authority]
        if source_work_ref.kind is not expected_kind:
            raise _violation(
                "PROGRESS_SOURCE_AUTHORITY_MISMATCH",
                f"{authority.value} progress requires {expected_kind.value} source_work_ref",
                code=ErrorCode.PERMISSION_DENIED,
            )
        return cls(
            authority=authority,
            event_id=_required_text(data["event_id"], "work_progress.source.event_id"),
            source_work_ref=source_work_ref,
            adapter=_optional_id(data["adapter"], "work_progress.source.adapter"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "authority": self.authority.value,
            "event_id": self.event_id,
            "source_work_ref": self.source_work_ref.to_dict(),
            "adapter": self.adapter,
        }


@dataclass(frozen=True, slots=True)
class WorkProgressEventV2:
    work_ref: IdentityRef
    source: WorkProgressSource
    seq: int
    state: WorkState
    outcome: TerminalOutcome | None
    summary: KnownFact[str]
    blocking_question: KnownFact[str]
    artifact_refs: KnownFact[tuple[ContextRef, ...]]
    urgency: WorkUrgency
    speakability: Speakability

    @classmethod
    def from_dict(
        cls,
        payload: object,
        *,
        scope: ScopeRef | None = None,
        identities: IdentityAuthority | None = None,
    ) -> WorkProgressEventV2:
        data = _strict_object(payload, field_name="work_progress")
        _require_exact_keys(
            data,
            required={
                "work_ref",
                "source",
                "seq",
                "state",
                "outcome",
                "summary",
                "blocking_question",
                "artifact_refs",
                "urgency",
                "speakability",
            },
            field_name="work_progress",
        )
        work_ref = IdentityRef.from_dict(data["work_ref"])
        if work_ref.kind not in {IdentityKind.ROUND, IdentityKind.TASK}:
            raise _violation("INVALID_WORK_REF_KIND", "work_ref must identify a round or task")
        source = WorkProgressSource.from_dict(data["source"])
        if source.source_work_ref.kind in {IdentityKind.ROUND, IdentityKind.TASK}:
            if source.source_work_ref != work_ref:
                raise _violation(
                    "PROGRESS_SOURCE_WORK_MISMATCH",
                    "round/task source_work_ref must equal work_ref",
                )
        elif work_ref.kind is not IdentityKind.TASK:
            raise _violation(
                "PROGRESS_ATTEMPT_PARENT_MISMATCH",
                "an attempt source can project only to a task",
            )
        if scope is not None and identities is not None:
            identities.require(work_ref, scope=scope)
            identities.require(
                source.source_work_ref,
                scope=scope,
                parent=(work_ref if source.source_work_ref.kind is IdentityKind.ATTEMPT else None),
            )
        state = _enum(WorkState, data["state"], "work_progress.state")
        outcome = None if data["outcome"] is None else _enum(TerminalOutcome, data["outcome"], "work_progress.outcome")
        if state is WorkState.TERMINAL and outcome is None:
            raise _violation(
                "TERMINAL_OUTCOME_REQUIRED",
                "terminal WorkProgress requires an outcome",
            )
        if state is not WorkState.TERMINAL and outcome is not None:
            raise _violation(
                "NON_TERMINAL_OUTCOME_FORBIDDEN",
                "non-terminal WorkProgress forbids an outcome",
            )

        def parse_artifacts(value: object) -> tuple[ContextRef, ...]:
            values = _strict_array(value, field_name="work_progress.artifact_refs.value")
            refs = tuple(ContextRef.from_dict(item) for item in values)
            if scope is not None:
                for ref in refs:
                    if ref.scope != scope:
                        raise _violation(
                            "CONTEXT_SCOPE_MISMATCH",
                            "artifact context scope must match WorkProgress scope",
                            code=ErrorCode.PERMISSION_DENIED,
                        )
            return refs

        return cls(
            work_ref=work_ref,
            source=source,
            seq=_uint(data["seq"], "work_progress.seq"),
            state=state,
            outcome=outcome,
            summary=_known_fact(
                data["summary"],
                "work_progress.summary",
                lambda value: _fact_text(value, "work_progress.summary.value"),
            ),
            blocking_question=_known_fact(
                data["blocking_question"],
                "work_progress.blocking_question",
                lambda value: _fact_text(value, "work_progress.blocking_question.value"),
            ),
            artifact_refs=_known_fact(
                data["artifact_refs"],
                "work_progress.artifact_refs",
                parse_artifacts,
            ),
            urgency=_enum(WorkUrgency, data["urgency"], "work_progress.urgency"),
            speakability=_enum(Speakability, data["speakability"], "work_progress.speakability"),
        )

    @staticmethod
    def _fact_dict(fact: KnownFact[_FactT], serializer: Callable[[_FactT], object]) -> dict[str, object]:
        if fact.knowledge is Knowledge.UNKNOWN:
            return {"knowledge": Knowledge.UNKNOWN.value}
        assert fact.value is not None
        return {
            "knowledge": Knowledge.KNOWN.value,
            "value": serializer(fact.value),
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "work_ref": self.work_ref.to_dict(),
            "source": self.source.to_dict(),
            "seq": self.seq,
            "state": self.state.value,
            "outcome": None if self.outcome is None else self.outcome.value,
            "summary": self._fact_dict(self.summary, lambda value: value),
            "blocking_question": self._fact_dict(self.blocking_question, lambda value: value),
            "artifact_refs": self._fact_dict(
                self.artifact_refs,
                lambda value: [ref.to_dict() for ref in value],
            ),
            "urgency": self.urgency.value,
            "speakability": self.speakability.value,
        }


_LIFECYCLE_TRANSITIONS: Final = MappingProxyType(
    {
        LifecycleKind.INTERACTION: MappingProxyType(
            {
                "open": frozenset({"closing", "closed"}),
                "closing": frozenset({"closed"}),
            }
        ),
        LifecycleKind.TURN: MappingProxyType({"capturing": frozenset({"committed", "cancelled"})}),
        LifecycleKind.RESPONSE: MappingProxyType(
            {
                "accepted": frozenset({"generating", "terminal"}),
                "generating": frozenset({"speaking", "terminal"}),
                "speaking": frozenset({"terminal"}),
            }
        ),
        LifecycleKind.ROUND: MappingProxyType(
            {
                "accepted": frozenset({"running", "blocked", "decision_required", "terminal"}),
                "running": frozenset({"blocked", "decision_required", "terminal"}),
                "blocked": frozenset({"running", "decision_required", "terminal"}),
                "decision_required": frozenset({"running", "blocked", "terminal"}),
            }
        ),
        LifecycleKind.TASK: MappingProxyType(
            {
                "accepted": frozenset({"running", "blocked", "decision_required", "terminal"}),
                "running": frozenset({"blocked", "decision_required", "terminal"}),
                "blocked": frozenset({"running", "decision_required", "terminal"}),
                "decision_required": frozenset({"running", "blocked", "terminal"}),
            }
        ),
        LifecycleKind.ATTEMPT: MappingProxyType(
            {"accepted": frozenset({"running"}), "running": frozenset({"terminal"})}
        ),
    }
)


def validate_transition(
    kind: LifecycleKind | str,
    current: str,
    next_state: str,
    *,
    outcome: TerminalOutcome | str | None = None,
) -> None:
    lifecycle = _enum(LifecycleKind, kind, "lifecycle.kind")
    current_state = _required_text(current, "lifecycle.current")
    target_state = _required_text(next_state, "lifecycle.next")
    allowed = _LIFECYCLE_TRANSITIONS[lifecycle].get(current_state, frozenset())
    if target_state not in allowed:
        raise _violation(
            "INVALID_LIFECYCLE_TRANSITION",
            f"{lifecycle.value} cannot transition from {current_state!r} to {target_state!r}",
            code=ErrorCode.CONFLICT,
        )
    if target_state == "terminal":
        if outcome is None:
            raise _violation("TERMINAL_OUTCOME_REQUIRED", "terminal transitions require an outcome")
        _enum(TerminalOutcome, outcome, "lifecycle.outcome")
    elif outcome is not None:
        raise _violation(
            "NON_TERMINAL_OUTCOME_FORBIDDEN",
            "outcome is only valid for terminal transitions",
        )


class IdentityAuthority(Protocol):
    """Application-owned identity registry used for envelope admission."""

    def require(self, identity: IdentityRef, *, scope: ScopeRef, parent: IdentityRef | None = None) -> object: ...


class CommittedInputAuthority(Protocol):
    """Application-owned committed-input check; the SDK owns no speech turns."""

    def require_origin(self, origin: OriginRef, scope: ScopeRef) -> object: ...
