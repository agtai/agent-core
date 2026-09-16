#!/usr/bin/env python
# coding: utf-8

from __future__ import annotations

import pytest

from openjiuwen.harness.tools.browser_move.runtime.browser_capabilities import (
    ADVANCED_CODE_BROWSER_TOOL_NAMES,
    CORE_BROWSER_TOOL_NAMES,
    UNSAFE_DEV_BROWSER_TOOL_NAMES,
)
from openjiuwen.harness.tools.browser_move.runtime.runtime import (
    _BATCH_EXPLICIT_SELECTOR_OPS,
    _BATCH_SAFE_READ_SELECTOR_OPS,
    BrowserRuntimeRail,
)
from openjiuwen.harness.tools.browser_move.runtime.tool_semantics import (
    BATCH_READ_ONLY_OPS,
    SEMANTICALLY_NEUTRAL_TOOL_NAMES,
    batch_steps_are_read_only,
    coerce_tool_args,
    operation_intent,
    progress_is_semantically_neutral,
    tool_is_semantically_neutral,
)


@pytest.mark.parametrize(
    "tool_name",
    [
        "browser_snapshot",
        "browser_find",
        "browser_probe_cards",
        "browser_probe_interactives",
        "browser_take_screenshot",
        "browser_hover",
        "browser_handle_dialog",
        "mcp_playwright-official_browser_hover",
        "playwright.browser_handle_dialog",
    ],
)
def test_pointer_query_and_dialog_tools_are_semantically_neutral(tool_name: str) -> None:
    assert tool_is_semantically_neutral(tool_name, {}) is True


@pytest.mark.parametrize(
    ("tool_name", "tool_args"),
    [
        ("browser_navigate", {"url": "https://example.test"}),
        ("browser_navigate_back", {}),
        ("browser_click", {"target_id": "e1"}),
        ("browser_type", {"text": "hello"}),
        ("browser_press_key", {"key": "Enter"}),
        ("browser_fill_form", {"fields": []}),
        ("browser_select_option", {"values": ["a"]}),
        ("browser_drag", {"from": "e1", "to": "e2"}),
        ("browser_drop", {"target_id": "e1"}),
        ("browser_file_upload", {"paths": ["a.txt"]}),
        ("browser_close", {}),
        ("browser_custom_action", {"name": "login"}),
    ],
)
def test_state_changing_tools_are_not_neutral(tool_name: str, tool_args: dict) -> None:
    assert tool_is_semantically_neutral(tool_name, tool_args) is False


def test_script_neutrality_follows_the_existing_operation_intent_check() -> None:
    read_only = {"function": "() => { return document.title; }"}
    mutation = {"function": "() => document.querySelector('button').click()"}

    assert operation_intent("browser_evaluate", read_only) == "script_extraction"
    assert operation_intent("browser_evaluate", mutation) == "script_mutation"
    # Anything that reads without a mutation marker is neutral too, so an
    # unclassifiable inspection script is never scored as no-progress.
    assert operation_intent("browser_evaluate", {"function": "() => 1"}) == "script_inspection"
    assert tool_is_semantically_neutral("browser_evaluate", {"function": "() => 1"}) is True
    assert tool_is_semantically_neutral("browser_evaluate", read_only) is True
    assert tool_is_semantically_neutral("browser_run_code", read_only) is True
    assert tool_is_semantically_neutral("browser_run_code_unsafe", read_only) is True
    assert tool_is_semantically_neutral("browser_evaluate", mutation) is False


@pytest.mark.parametrize(
    "expression",
    [
        "() => el.dispatchEvent(new Event('change'))",
        "() => el.dispatchevent(new Event('change'))",
        "() => el.DISPATCHEVENT(new Event('change'))",
        "() => el.setAttribute('value', '42')",
        "() => el.setattribute('value', '42')",
        "() => el.SETATTRIBUTE('value', '42')",
    ],
)
def test_mixed_case_mutation_markers_classify_as_script_mutation(expression: str) -> None:
    # The markers used to be matched against a lowercased expression, so every
    # mixed-case spelling fell through to script_extraction and the call was
    # then treated as neutral, i.e. invisible to no-progress and loop scoring.
    args = {"function": expression}

    assert operation_intent("browser_evaluate", args) == "script_mutation"
    assert tool_is_semantically_neutral("browser_evaluate", args) is False
    assert tool_is_semantically_neutral("browser_run_code", args) is False
    assert tool_is_semantically_neutral("browser_run_code_unsafe", args) is False


@pytest.mark.parametrize(
    "expression",
    [
        "() => el.textContent",
        "() => el.textcontent",
        "() => el.TEXTCONTENT",
        "() => el.innerText",
        "() => el.innertext",
        "() => document.querySelector('.price')",
        "() => document.queryselector('.price')",
    ],
)
def test_mixed_case_extraction_markers_classify_as_script_extraction(expression: str) -> None:
    args = {"function": expression}

    assert operation_intent("browser_evaluate", args) == "script_extraction"
    assert tool_is_semantically_neutral("browser_evaluate", args) is True


def test_mutation_markers_win_over_extraction_markers() -> None:
    args = {"function": "() => document.querySelector('#qty').dispatchEvent(new Event('input'))"}

    assert operation_intent("browser_evaluate", args) == "script_mutation"
    assert tool_is_semantically_neutral("browser_evaluate", args) is False


def test_unmarked_script_still_reads_as_a_neutral_inspection() -> None:
    args = {"function": "() => 1"}

    assert operation_intent("browser_evaluate", args) == "script_inspection"
    assert tool_is_semantically_neutral("browser_evaluate", args) is True


@pytest.mark.parametrize("tool_name", sorted(SEMANTICALLY_NEUTRAL_TOOL_NAMES))
def test_every_semantically_neutral_tool_is_a_read_only_recovery(tool_name: str) -> None:
    """Neutral must be a subset of read-only recovery; the converse must not hold.

    The two lists live in different modules on purpose - browser_tabs(select)
    is a read-only recovery that still changes the url, and browser_drop is
    neither - so only this one direction is locked down.
    """
    assert tool_is_semantically_neutral(tool_name, {}) is True
    assert BrowserRuntimeRail._is_read_only_recovery(tool_name, {}) is True


def test_read_only_recovery_is_strictly_wider_than_neutrality() -> None:
    assert BrowserRuntimeRail._is_read_only_recovery("browser_tabs", {"action": "select"}) is True
    assert tool_is_semantically_neutral("browser_tabs", {"action": "select"}) is False
    assert BrowserRuntimeRail._is_read_only_recovery("browser_drop", {"target_id": "e1"}) is False
    assert tool_is_semantically_neutral("browser_drop", {"target_id": "e1"}) is False


# Expected action class for every catalogued browser tool, written out by hand
# so a future name (browser_dropdown_select, browser_find_and_click) that
# collides with one of the substring checks in _classify_action_class fails
# here instead of being silently misclassified at runtime.
_EXPECTED_ACTION_CLASSES = {
    "browser_click": "interaction",
    "browser_close": "other",
    "browser_drag": "interaction",
    "browser_drop": "file_drop",
    "browser_evaluate": "script_exploration",
    "browser_file_upload": "form",
    "browser_fill_form": "form",
    "browser_find": "target_discovery",
    "browser_handle_dialog": "dialog_control",
    "browser_hover": "pointer_reveal",
    "browser_navigate": "navigation",
    "browser_navigate_back": "navigation",
    "browser_press_key": "form",
    "browser_run_code": "script_exploration",
    "browser_run_code_unsafe": "script_exploration",
    "browser_select_option": "form",
    "browser_snapshot": "target_discovery",
    "browser_tabs": "navigation",
    "browser_take_screenshot": "other",
    "browser_type": "form",
}
_CATALOG_TOOL_NAMES = (
    *CORE_BROWSER_TOOL_NAMES,
    *ADVANCED_CODE_BROWSER_TOOL_NAMES,
    *UNSAFE_DEV_BROWSER_TOOL_NAMES,
)


@pytest.mark.xfail(reason="Superseded by BU driver (Policy A): asserts base agtai/develop #1147 rail/catalog/semantic behavior replaced by the browser_use driver. Tracked for later reconciliation.", strict=False)
def test_action_class_table_covers_the_whole_catalog() -> None:
    assert set(_CATALOG_TOOL_NAMES) == set(_EXPECTED_ACTION_CLASSES)


@pytest.mark.parametrize("tool_name", sorted(_EXPECTED_ACTION_CLASSES))
def test_catalog_tools_keep_their_expected_action_class(tool_name: str) -> None:
    state = BrowserRuntimeRail._build_phase_state("t")

    assert BrowserRuntimeRail._classify_action_class(tool_name, {}, state) == _EXPECTED_ACTION_CLASSES[tool_name]


@pytest.mark.parametrize(
    "tool_name",
    ["browser_hover", "browser_find", "browser_handle_dialog", "browser_drop"],
)
def test_vendor_prefixed_names_keep_the_same_action_class(tool_name: str) -> None:
    state = BrowserRuntimeRail._build_phase_state("t")
    prefixed = f"mcp_playwright-official_{tool_name}"

    assert BrowserRuntimeRail._classify_action_class(prefixed, {}, state) == _EXPECTED_ACTION_CLASSES[tool_name]


def test_tabs_are_neutral_only_when_listing() -> None:
    assert tool_is_semantically_neutral("browser_tabs", {}) is True
    assert tool_is_semantically_neutral("browser_tabs", {"action": "list"}) is True
    assert tool_is_semantically_neutral("browser_tabs", {"action": "select"}) is False
    assert tool_is_semantically_neutral("browser_tabs", {"action": "close"}) is False


def test_batch_is_neutral_only_when_every_step_is_read_only() -> None:
    read_only = {"steps": [{"op": "extract_text"}, {"op": "wait_for_selector"}]}
    mixed = {"steps": [{"op": "extract_text"}, {"op": "click"}]}

    assert batch_steps_are_read_only(read_only) is True
    assert batch_steps_are_read_only(mixed) is False
    assert batch_steps_are_read_only({"steps": []}) is False
    assert tool_is_semantically_neutral("browser_batch_interact", read_only) is True
    assert tool_is_semantically_neutral("browser_batch_interact", mixed) is False


def test_batch_read_only_ops_stay_a_superset_of_the_runtime_selector_op_sets() -> None:
    assert _BATCH_SAFE_READ_SELECTOR_OPS <= BATCH_READ_ONLY_OPS
    assert _BATCH_EXPLICIT_SELECTOR_OPS <= BATCH_READ_ONLY_OPS


def test_neutrality_accepts_json_encoded_arguments() -> None:
    assert coerce_tool_args('{"action": "select"}') == {"action": "select"}
    assert coerce_tool_args("not json") == {}
    assert tool_is_semantically_neutral("browser_tabs", '{"action": "select"}') is False
    assert tool_is_semantically_neutral("browser_tabs", '{"action": "list"}') is True


def test_progress_neutrality_only_matches_the_observation_label() -> None:
    assert progress_is_semantically_neutral("observation") is True
    assert progress_is_semantically_neutral("no_progress") is False
    assert progress_is_semantically_neutral("unknown") is False
    assert progress_is_semantically_neutral(None) is False


def test_runtime_delegates_intent_and_batch_read_only_checks() -> None:
    args = {"function": "() => document.title"}

    assert BrowserRuntimeRail._operation_intent("browser_evaluate", args) == operation_intent("browser_evaluate", args)
    assert BrowserRuntimeRail._coerce_tool_args('{"a": 1}') == {"a": 1}
