# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""A2: pluggable source registry."""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeAlias

from .models import CaseConfig, EvidenceItem, PermissionUsed

SourceFn: TypeAlias = Callable[[CaseConfig], tuple[list[EvidenceItem], PermissionUsed]]

_REGISTRY: dict[str, SourceFn] = {}


def register_source(name: str):
    def decorator(fn: SourceFn) -> SourceFn:
        _REGISTRY[name] = fn
        return fn

    return decorator


def get_source(name: str) -> SourceFn:
    if name not in _REGISTRY:
        known = ", ".join(sorted(_REGISTRY)) or "(empty)"
        raise KeyError(f"Unknown source '{name}'. Registered: {known}")
    return _REGISTRY[name]


def list_sources() -> list[str]:
    return sorted(_REGISTRY)
