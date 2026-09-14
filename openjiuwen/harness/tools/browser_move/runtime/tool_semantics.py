#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Single source of truth for browser tool semantics shared by capture and scoring.

Two questions about a browser tool are deliberately kept apart:

* "does this action group need a fresh page capture?" — owned by the refresh /
  observation sets in ``browser_state_context_processor``;
* "may this action group change the semantic state that progress scoring
  digests (url / form_values / selected_filters / result_count)?" — owned by
  :func:`tool_is_semantically_neutral` here.

A pointer-level, query-level or dialog-arming tool needs the first and must be
excluded from the second: it can reveal content but cannot by itself move the
semantic state, so scoring its unchanged digest as "no progress" is a category
error, not a loop.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Iterable, Mapping

# Progress label used for a neutral observation that left the digest untouched.
NEUTRAL_PROGRESS_NAME = "observation"
NEUTRAL_PROGRESS_NAMES = frozenset({NEUTRAL_PROGRESS_NAME})

# Tools that can never change url / form_values / selected_filters /
# result_count on their own, whatever arguments they are given.
SEMANTICALLY_NEUTRAL_TOOL_NAMES = frozenset(
    {
        "browser_find",
        "browser_handle_dialog",
        "browser_hover",
        "browser_probe_cards",
        "browser_probe_interactives",
        "browser_snapshot",
        "browser_take_screenshot",
    }
)
# Tools whose neutrality depends on their arguments.
_SCRIPT_TOOL_NAMES = frozenset({"browser_evaluate", "browser_run_code", "browser_run_code_unsafe"})
_TABS_TOOL_NAMES = frozenset({"browser_tabs"})
_BATCH_TOOL_NAMES = frozenset({"browser_batch_interact"})
# Batch operations that only read or wait. Must stay a superset of the runtime
# safe-read and explicit-selector operation sets (asserted by unit tests).
BATCH_READ_ONLY_OPS = frozenset(
    {
        "extract_text",
        "extract_value",
        "wait_for_selector",
        "wait_for_first_card_title",
        "wait_for_sort_state",
        "wait_for_result_count",
        "wait_for_dom_text_change",
        "wait_for_stable",
        "wait_for_text",
        "wait_for_load_state",
        "wait_for_url",
        "wait_for_tab",
    }
)
# Markers are spelled the way they appear in real page scripts and matched
# case-insensitively. Never match these against a pre-lowercased expression:
# doing so silently kills every mixed-case alternative, which is how
# dispatchEvent / setAttribute mutations used to be scored as extractions and
# therefore excluded from loop detection (see F_03).
_SCRIPT_MUTATION_RE = re.compile(
    r"\.click\s*\(|dispatchEvent|\.value\s*=|setAttribute\s*\(",
    re.IGNORECASE,
)
_SCRIPT_EXTRACTION_RE = re.compile(
    r"textContent|innerText|return|querySelector|document\.title",
    re.IGNORECASE,
)


def coerce_tool_args(tool_args: Any) -> Dict[str, Any]:
    """Return tool arguments as a mapping, tolerating JSON-encoded arguments."""
    if isinstance(tool_args, dict):
        return tool_args
    if isinstance(tool_args, str):
        try:
            parsed = json.loads(tool_args)
        except ValueError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def operation_intent(tool_name: str, args: Mapping[str, Any]) -> str:
    """Classify what a call does, independent of selector or generation noise."""
    steps = args.get("steps")
    if isinstance(steps, list):
        operations = sorted(
            {
                str(step.get("op") or "").strip().lower()
                for step in steps
                if isinstance(step, dict) and str(step.get("op") or "").strip()
            }
        )
        return "+".join(operations)[:120] or "batch"
    name = str(tool_name or "").strip().lower().rsplit("browser_", 1)[-1]
    if name in {"evaluate", "run_code", "run_code_unsafe"}:
        expression = str(args.get("function") or args.get("expression") or args.get("script") or args.get("code") or "")
        if _SCRIPT_MUTATION_RE.search(expression):
            return "script_mutation"
        if _SCRIPT_EXTRACTION_RE.search(expression):
            return "script_extraction"
        return "script_inspection"
    return name or "other"


def batch_steps_are_read_only(args: Mapping[str, Any]) -> bool:
    """Return True when every step of a batch call only reads or waits."""
    steps = args.get("steps")
    if not isinstance(steps, list) or not steps:
        return False
    return all(
        isinstance(step, dict) and str(step.get("op") or "").strip().lower() in BATCH_READ_ONLY_OPS for step in steps
    )


def _matches_tool_name(normalized_name: str, candidates: Iterable[str]) -> bool:
    return any(
        normalized_name == expected
        or normalized_name.endswith(f".{expected}")
        or normalized_name.endswith(f"_{expected}")
        for expected in candidates
    )


def tool_is_semantically_neutral(tool_name: str, tool_args: Any = None) -> bool:
    """Return True when this call cannot by itself change the semantic state.

    Neutral calls still need a page capture and still consume phase attempts;
    they are only excluded from no-progress and loop scoring.
    """
    normalized_name = str(tool_name or "").strip().lower()
    if not normalized_name:
        return False
    if _matches_tool_name(normalized_name, SEMANTICALLY_NEUTRAL_TOOL_NAMES):
        return True
    args = coerce_tool_args(tool_args)
    if _matches_tool_name(normalized_name, _SCRIPT_TOOL_NAMES):
        return operation_intent(normalized_name, args) != "script_mutation"
    if _matches_tool_name(normalized_name, _TABS_TOOL_NAMES):
        return str(args.get("action") or "list").strip().lower() == "list"
    if _matches_tool_name(normalized_name, _BATCH_TOOL_NAMES):
        return batch_steps_are_read_only(args)
    return False


def progress_is_semantically_neutral(progress_name: Any) -> bool:
    """Return True for a progress label produced by a neutral observation."""
    return str(progress_name or "").strip().lower() in NEUTRAL_PROGRESS_NAMES


__all__ = [
    "BATCH_READ_ONLY_OPS",
    "NEUTRAL_PROGRESS_NAME",
    "NEUTRAL_PROGRESS_NAMES",
    "SEMANTICALLY_NEUTRAL_TOOL_NAMES",
    "batch_steps_are_read_only",
    "coerce_tool_args",
    "operation_intent",
    "progress_is_semantically_neutral",
    "tool_is_semantically_neutral",
]
