# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Wire protocol v1 constants, shared verbatim by both sides of the sidecar boundary.

This module is STDLIB ONLY. It is imported by the agent-core process as
``openjiuwen.harness.tools.browser_move.drivers.browser_use.sidecar.wire``
and by the sidecar process as a flat sibling module ``wire`` (the sidecar
runs outside the ``openjiuwen`` package and must not import it). Keeping the
method-name and error-code constants in one file that both sides load is
what lets a unit test catch sidecar drift without spawning a sidecar.
"""

from __future__ import annotations

WIRE_VERSION = 1

# BrowserDriver protocol method names, verbatim, plus the two control methods.
DRIVER_METHOD_NAMES: tuple[str, ...] = (
    "connect",
    "health",
    "close",
    "observe",
    "screenshot",
    "list_tabs",
    "resolve",
    "stamp",
    "navigate",
    "go_back",
    "go_forward",
    "reload",
    "click",
    "type_text",
    "press_key",
    "select_option",
    "set_checked",
    "scroll",
    "upload_files",
    "drag",
    "hover",
    "handle_dialog",
    "drop",
    "switch_tab",
    "close_tab",
    "evaluate",
    "wait_load_state",
)

CONTROL_METHOD_NAMES: tuple[str, ...] = ("cancel", "shutdown")

WIRE_METHOD_NAMES: frozenset[str] = frozenset(DRIVER_METHOD_NAMES) | frozenset(CONTROL_METHOD_NAMES)

# Error codes accepted on the wire, mapped by transport.py to exception
# classes in drivers/errors.py. Kept as plain strings here (not importing
# drivers.errors) so this file stays free of any openjiuwen import.
ERROR_CODES: tuple[str, ...] = (
    "DriverError",
    "DriverNotConnected",
    "DriverConnectionError",
    "DriverConnectionLost",
    "ElementNotFound",
    "AmbiguousElement",
    "StaleIndexError",
    "StaleNodeError",
    "ActionTimeout",
    "NavigationError",
    "ObserveError",
    "EvaluateError",
    "DriverUnsupported",
)

DEFAULT_ERROR_CODE = "DriverError"

MAX_LINE_BYTES = 8 * 1024 * 1024


def make_request(request_id: int, method: str, params: dict | None = None) -> dict:
    """Build a request payload: ``{"id": ..., "method": ..., "params": ...}``."""
    return {"id": request_id, "method": method, "params": params or {}}


def make_success(request_id: int, result: object) -> dict:
    """Build a success reply payload: ``{"id": ..., "ok": ...}``."""
    return {"id": request_id, "ok": result}


def make_failure(request_id: int, code: str, message: str, data: dict | None = None) -> dict:
    """Build a failure reply payload: ``{"id": ..., "error": {...}}``."""
    error: dict[str, object] = {"code": code, "message": message}
    if data:
        error["data"] = data
    return {"id": request_id, "error": error}


def make_hello(*, browser_use_version: str, python_version: str, pid: int) -> dict:
    """Build the unsolicited hello line the sidecar emits first."""
    return {
        "kind": "hello",
        "wire": WIRE_VERSION,
        "browser_use": browser_use_version,
        "python": python_version,
        "pid": pid,
    }


def make_log(level: str, message: str) -> dict:
    """Build an unsolicited log line."""
    return {"kind": "log", "level": level, "message": message}


__all__ = [
    "CONTROL_METHOD_NAMES",
    "DEFAULT_ERROR_CODE",
    "DRIVER_METHOD_NAMES",
    "ERROR_CODES",
    "MAX_LINE_BYTES",
    "WIRE_METHOD_NAMES",
    "WIRE_VERSION",
    "make_failure",
    "make_hello",
    "make_log",
    "make_request",
    "make_success",
]
