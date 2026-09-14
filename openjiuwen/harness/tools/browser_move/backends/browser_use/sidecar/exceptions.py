# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Sidecar-local exception classes.

STDLIB ONLY. The sidecar cannot import ``openjiuwen.harness.tools.browser_move.
backends.contract.errors`` (that would pull the main env's ``openjiuwen`` package, and
transitively ``anthropic>=0.120.2``, back into the sidecar closure -- exactly
the conflict the sidecar exists to avoid).

Instead, these classes are named IDENTICALLY to the wire error codes in
``wire.ERROR_CODES``. ``main.py`` maps a raised exception to a wire error
code purely by ``type(exc).__name__``, so the two sides stay in sync without
sharing a class hierarchy. ``backends/browser_use/sidecar/wire.py`` and
``backends/contract/errors.py`` are the sources of truth for the code strings; a unit
test in the main env asserts every name here has a matching class there.
"""

from __future__ import annotations


class DriverError(Exception):
    """Generic fallback error."""


class DriverNotConnected(DriverError):
    """A method was invoked before the session was connected."""


class DriverConnectionError(DriverError):
    """Attaching to the browser over CDP failed."""


class DriverConnectionLost(DriverError):
    """The CDP connection dropped mid-flight."""


class ElementNotFound(DriverError):
    """The referenced element does not resolve to any current DOM node."""


class AmbiguousElement(DriverError):
    """The referenced element matches more than one current DOM node."""


class StaleIndexError(DriverError):
    """A selector-map index no longer identifies the same node."""


class StaleNodeError(DriverError):
    """A backend node id no longer resolves."""


class ActionTimeout(DriverError):
    """A hands action did not complete within its timeout."""


class NavigationError(DriverError):
    """A navigation action failed."""


class ObserveError(DriverError):
    """Capturing a page state failed."""


class EvaluateError(DriverError):
    """In-page JavaScript evaluation failed."""

    def __init__(self, message: str, *, js_message: str = "", js_stack: str = "") -> None:
        super().__init__(message)
        self.js_message = js_message
        self.js_stack = js_stack


class DriverUnsupported(DriverError):
    """The requested capability is not available."""


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
