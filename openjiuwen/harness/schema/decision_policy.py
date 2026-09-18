# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Structural contract for model slots that decide their own steps against a live runtime.

``harness/subagents`` is a generic assembly layer; concrete decision policies (e.g. the Jev
policy under ``harness/tools/browser_move/policy``) are vendor-specific implementations. This
protocol lets the assembly layer recognise such a model without importing any concrete class.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class DecisionPolicyModel(Protocol):
    """A model that decides browser steps itself and needs the live runtime to do it."""

    def bind_runtime(self, runtime: Any) -> None:
        """Attach the live runtime the policy will probe and act through."""
        ...
