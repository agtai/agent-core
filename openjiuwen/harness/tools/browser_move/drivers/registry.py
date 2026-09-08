# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""BrowserDriver backend registry.

``browser_use`` is the only registered backend and the default. Per D5,
``playwright_mcp`` is reserved as a NAME ONLY: it is not registered, not
implemented, and not documented as available. Registering it here is a
programming error, not a runtime choice.
"""

from __future__ import annotations

from typing import Callable

from .base import BrowserDriver
from .errors import DriverUnsupported

RESERVED_BACKEND_NAMES = frozenset({"playwright_mcp"})

BrowserDriverFactory = Callable[..., BrowserDriver]

_REGISTRY: dict[str, BrowserDriverFactory] = {}


def register_browser_driver(backend: str, factory: BrowserDriverFactory) -> None:
    """Register a driver factory under ``backend``.

    Raises ``ValueError`` for a reserved name (see ``RESERVED_BACKEND_NAMES``)
    so an accidental future registration of the MCP-compat name fails loudly
    instead of silently shadowing the reservation.
    """
    name = str(backend or "").strip()
    if not name:
        raise ValueError("backend name must not be empty")
    if name in RESERVED_BACKEND_NAMES:
        raise ValueError(f"backend name '{name}' is reserved and must not be registered")
    _REGISTRY[name] = factory


def create_browser_driver(backend: str, **cfg: object) -> BrowserDriver:
    """Instantiate a registered driver by backend name.

    Raises ``DriverUnsupported`` for an unknown backend, listing the
    currently registered set so the caller can fix its configuration.
    """
    name = str(backend or "").strip()
    factory = _REGISTRY.get(name)
    if factory is None:
        registered = ", ".join(sorted(_REGISTRY)) or "<none>"
        raise DriverUnsupported(f"Unknown browser driver backend '{name}'. Registered: {registered}")
    return factory(**cfg)


def registered_backends() -> tuple[str, ...]:
    """Return the currently registered backend names, sorted."""
    return tuple(sorted(_REGISTRY))


def _register_builtin_drivers() -> None:
    """Register the built-in ``browser_use`` backend as the default."""
    from .browser_use.driver import BrowserUseDriver

    register_browser_driver("browser_use", BrowserUseDriver)


_register_builtin_drivers()


__all__ = [
    "RESERVED_BACKEND_NAMES",
    "BrowserDriverFactory",
    "create_browser_driver",
    "register_browser_driver",
    "registered_backends",
]
