# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Shared native-image policy helpers for harness rails."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from openjiuwen.harness.image_modality_probe import get_cached_image_support


def resolve_image_input_support_status(
    agent: Any,
    explicit_value: bool | None = None,
) -> bool | None:
    """Return image-input support as True/False, or None while still unresolved.

    A boolean configuration is authoritative. ``None`` is auto mode and uses the
    probe cache for the agent's current main model. Cache miss stays ``None`` so
    callers can distinguish "pending/unverified" from "confirmed unsupported".
    """
    if explicit_value is not None:
        return explicit_value

    deep_config = getattr(agent, "deep_config", None) or getattr(
        agent,
        "_deep_config",
        None,
    )
    configured_value = getattr(deep_config, "enable_read_image_multimodal", None)
    if isinstance(configured_value, bool):
        return configured_value

    model = getattr(deep_config, "model", None)
    return get_cached_image_support(model)


def should_enable_read_image_multimodal(
    agent: Any,
    explicit_value: bool | None = None,
) -> bool:
    """Resolve whether the agent's current model may receive image bytes.

    A boolean configuration is authoritative. ``None`` is auto mode and uses
    the probe cache for the agent's current main model. Dedicated vision tools
    are intentionally irrelevant: native input and tool-based vision are two
    independent capabilities and may both be available.

    Pending/unverified probe results are treated as False for multimodal
    *consumption* only. Screenshot *capture* is a separate capability and must
    not be gated solely on this helper.
    """
    return resolve_image_input_support_status(agent, explicit_value) is True


def build_read_image_multimodal_resolver(
    agent: Any,
    explicit_value: bool | None = None,
) -> Callable[[], bool]:
    """Build a live native-image resolver without retaining the whole agent."""

    def resolve() -> bool:
        return should_enable_read_image_multimodal(agent, explicit_value)

    return resolve
