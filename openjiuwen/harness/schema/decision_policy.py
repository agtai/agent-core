# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Structural contract for model slots that decide their own steps against a live runtime.

``harness/subagents`` is a generic assembly layer; concrete decision policies live outside this
repository. This protocol lets the assembly layer recognise such a model without importing any
concrete class.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from openjiuwen.harness.tools.browser_move.playwright_runtime.runtime import BrowserAgentRuntime


@runtime_checkable
class DecisionPolicyModel(Protocol):
    """A model that decides browser steps itself and needs the live runtime to do it."""

    def bind_runtime(self, runtime: "BrowserAgentRuntime") -> None:
        """Attach the live runtime the policy will probe and act through."""
        ...
