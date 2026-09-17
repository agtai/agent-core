# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Exception tree for BrowserDriver implementations.

Two staleness kinds live here and only here: ``StaleIndexError`` (a
previously observed selector-map index no longer safely identifies the same
node) and ``StaleNodeError`` (a backend node id no longer resolves at all).
Neither adjudicates a PageState generation or target_id -- the driver has no
concept of either; the runtime translates these into its own stale-target
payloads.
"""

from __future__ import annotations


class DriverError(Exception):
    """Base class for every error a BrowserDriver may raise."""


class DriverNotConnected(DriverError):
    """A driver method was called before ``connect()`` succeeded."""


class DriverConnectionError(DriverError):
    """``connect()`` or sidecar spawn/handshake failed."""


class DriverConnectionLost(DriverError):
    """The sidecar process died or the CDP connection dropped mid-flight."""


class ElementNotFound(DriverError):
    """The referenced element does not resolve to any current DOM node."""


class AmbiguousElement(DriverError):
    """The referenced element matches more than one current DOM node."""


class StaleIndexError(DriverError):
    """An ``IndexRef``'s observation index no longer identifies the same node."""


class StaleNodeError(DriverError):
    """A ``NodeRef``'s backend node id no longer resolves."""


class ActionTimeout(DriverError):
    """A hands action did not complete within its timeout."""


class NavigationError(DriverError):
    """A navigation action failed."""


class ObserveError(DriverError):
    """``observe()`` failed to capture a page state."""


class EvaluateError(DriverError):
    """In-page JavaScript evaluation failed.

    Carries the raw JS error message/stack so callers can surface a useful
    diagnostic without the driver needing to understand PageState.
    """

    def __init__(self, message: str, *, js_message: str = "", js_stack: str = "") -> None:
        super().__init__(message)
        self.js_message = js_message
        self.js_stack = js_stack


class DriverUnsupported(DriverError):
    """The requested backend or capability is not available."""


__all__ = [
    "ActionTimeout",
    "AmbiguousElement",
    "DriverConnectionError",
    "DriverConnectionLost",
    "DriverError",
    "DriverNotConnected",
    "DriverUnsupported",
    "ElementNotFound",
    "EvaluateError",
    "NavigationError",
    "ObserveError",
    "StaleIndexError",
    "StaleNodeError",
]
