#!/usr/bin/env python
# coding: utf-8

"""Replan-gate behaviour for semantically neutral browser action sequences.

The lab-20 regression: snapshot -> find -> hover -> read-only evaluate ->
handle_dialog on one URL can never change the semantic digest, so before the
neutrality split it read as three consecutive no-progress states and killed the
task with ``semantic_replan_budget_exhausted`` while every tool call had
returned ok.
"""

from __future__ import annotations

import pytest

from openjiuwen.harness.tools.browser_move.runtime.browser_working_context import (
    BROWSER_TASK_STATE_KEY,
    BrowserWorkingContextStore,
)
from openjiuwen.harness.tools.browser_move.runtime.runtime import BrowserRuntimeRail
from openjiuwen.harness.tools.browser_move.runtime.semantic_state import SemanticStateTracker
from openjiuwen.harness.tools.browser_move.runtime.tool_semantics import tool_is_semantically_neutral


class _FakeSession:
    def __init__(self, session_id: str = "browser-gate-session") -> None:
        self._session_id = session_id
        self._state: dict = {}

    def get_session_id(self) -> str:
        return self._session_id

    def get_state(self, key: str):
        return self._state.get(key)

    def update_state(self, payload) -> None:
        self._state.update(payload)


_HOVERS_URL = "https://the-internet.herokuapp.com/hovers"

_LAB_20_SEQUENCE = [
    ("browser_snapshot", {}),
    ("browser_find", {"text": "user1"}),
    ("browser_hover", {"target_id": "e_avatar_1"}),
    ("browser_evaluate", {"function": "() => { return document.body.innerText; }"}),
    ("browser_handle_dialog", {"accept": True}),
]


def _session_on_hovers_page() -> _FakeSession:
    session = _FakeSession()
    state = BrowserRuntimeRail._build_phase_state(f"Open {_HOVERS_URL} and read the revealed caption")
    state["last_page"] = {"url": _HOVERS_URL, "title": "The Internet"}
    session.update_state({BROWSER_TASK_STATE_KEY: state})
    return session


def _unchanged_raw_state() -> dict:
    return {
        "url": _HOVERS_URL,
        "form_values": [],
        "selected_filters": [],
        "result_count": 0,
        "field_coverage": [],
    }


def _run_sequence(session: _FakeSession, sequence: list[tuple[str, dict]]) -> list[str]:
    """Drive gate + scoring the way the rail does, with a never-changing digest."""
    tracker = SemanticStateTracker()
    action_classes: list[str] = []
    for index, (tool_name, tool_args) in enumerate(sequence):
        action_class = BrowserRuntimeRail._consume_phase_budget(
            session,
            tool_name,
            tool_args,
            current_page_state={"url": _HOVERS_URL, "title": "The Internet"},
        )
        action_classes.append(action_class)
        BrowserRuntimeRail._record_recent_action(
            session,
            tool_name=tool_name,
            tool_args=tool_args,
            tool_result={"ok": True, "url": _HOVERS_URL},
            action_class=action_class,
            elapsed_ms=1,
            progress_delta={"success": True},
        )
        progress = tracker.observe(
            _unchanged_raw_state(),
            action_group_id=f"group-{index}",
            mutating=not tool_is_semantically_neutral(tool_name, tool_args),
        )
        BrowserWorkingContextStore.sync_semantic_progress(session, progress)
    return action_classes


def test_neutral_lab_sequence_is_never_blocked_by_the_semantic_gate() -> None:
    session = _session_on_hovers_page()

    action_classes = _run_sequence(session, _LAB_20_SEQUENCE)

    state = session.get_state(BROWSER_TASK_STATE_KEY)
    assert state["status"] != "blocked"
    assert state.get("blockers") in (None, [])
    assert state.get("replan_required") in (None, False)
    assert state["semantic_progress"]["progress"] == "observation"
    assert state["semantic_progress"]["consecutive_no_progress"] == 0
    # Distinct classes keep hover / dialog / discovery from colliding on one
    # strategy fingerprint.
    assert action_classes[1:] == [
        "target_discovery",
        "pointer_reveal",
        "script_exploration",
        "dialog_control",
    ]


def test_neutral_tools_still_consume_phase_attempts() -> None:
    session = _session_on_hovers_page()

    _run_sequence(session, _LAB_20_SEQUENCE)

    phases = session.get_state(BROWSER_TASK_STATE_KEY)["phases"]
    assert sum(int(phase.get("attempts") or 0) for phase in phases.values()) == len(_LAB_20_SEQUENCE)


def test_neutral_only_loop_still_terminates_through_the_phase_budget() -> None:
    session = _session_on_hovers_page()
    phases = session.get_state(BROWSER_TASK_STATE_KEY)["phases"]
    budget = max(int(phase.get("budget") or 1) for phase in phases.values())

    with pytest.raises(ValueError, match="budget exhausted"):
        _run_sequence(session, [("browser_hover", {"target_id": "e_avatar_1"})] * (budget + 2))

    assert session.get_state(BROWSER_TASK_STATE_KEY)["status"] == "partial"


def _mutate_until_blocked(sequence: list[tuple[str, dict]]) -> tuple[_FakeSession, str]:
    """Drive calls with a never-changing digest until the gate blocks.

    ``mutating`` is derived from the neutrality classifier exactly as the rail
    does it, so a call that is wrongly judged neutral is never scored and this
    driver can never reach the blocked state.
    """
    session = _FakeSession()
    state = BrowserRuntimeRail._build_phase_state("Compare products, apply filters, and complete the checkout form")
    state["last_page"] = {"url": _HOVERS_URL, "title": "The Internet"}
    session.update_state({BROWSER_TASK_STATE_KEY: state})

    tracker = SemanticStateTracker()
    last_error = ""
    for index, (tool_name, tool_args) in enumerate(sequence):
        try:
            action_class = BrowserRuntimeRail._consume_phase_budget(
                session,
                tool_name,
                tool_args,
                current_page_state={"url": _HOVERS_URL, "title": "The Internet"},
            )
        except ValueError as exc:
            last_error = str(exc)
            if session.get_state(BROWSER_TASK_STATE_KEY)["status"] == "blocked":
                return session, last_error
            continue
        BrowserRuntimeRail._record_recent_action(
            session,
            tool_name=tool_name,
            tool_args=tool_args,
            tool_result={"ok": True, "url": _HOVERS_URL},
            action_class=action_class,
            elapsed_ms=1,
            progress_delta={"success": True},
        )
        BrowserWorkingContextStore.sync_semantic_progress(
            session,
            tracker.observe(
                _unchanged_raw_state(),
                action_group_id=f"mutate-{index}",
                mutating=not tool_is_semantically_neutral(tool_name, tool_args),
            ),
        )
    raise AssertionError(f"mutating sequence never reached the blocked state: {last_error}")


def test_repeated_identical_clicks_with_an_unchanged_digest_still_end_blocked() -> None:
    click = ("browser_click", {"target_id": "e_avatar_1"})

    session, message = _mutate_until_blocked([click] * 10)

    state = session.get_state(BROWSER_TASK_STATE_KEY)
    assert state["status"] == "blocked"
    assert state["replan_required"] is True
    assert "semantic_replan_denial_budget_exhausted" in state["blockers"]
    assert "Semantic loop detected" in message


def _materially_different_mutations(count: int = 12) -> list[tuple[str, dict]]:
    """A run of distinct mutating strategies, none of which moves the digest."""
    templates = [
        ("browser_click", lambda index: {"target_id": f"e_row_{index}"}),
        ("browser_type", lambda index: {"target_id": f"e_search_{index}", "text": f"user{index}"}),
        ("browser_select_option", lambda index: {"target_id": f"e_sort_{index}", "values": [f"opt{index}"]}),
        ("browser_press_key", lambda index: {"key": "Enter", "target_id": f"e_form_{index}"}),
    ]
    sequence = []
    for index in range(count):
        tool_name, build_args = templates[index % len(templates)]
        sequence.append((tool_name, build_args(index)))
    return sequence


def test_two_failed_replan_trials_still_exhaust_the_semantic_replan_budget() -> None:
    # Each call is a materially different mutating strategy, so the gate keeps
    # granting replan trials; the digest never moves, so every trial fails.
    session, message = _mutate_until_blocked(_materially_different_mutations())

    state = session.get_state(BROWSER_TASK_STATE_KEY)
    assert state["status"] == "blocked"
    assert state["blockers"] == ["semantic_replan_budget_exhausted"]
    assert int(state["replan_count"]) >= 2
    assert "semantic" in message.lower()
    assert "budget exhausted" in message.lower()


def test_exhausted_gate_reports_actionable_detail() -> None:
    session, message = _mutate_until_blocked(_materially_different_mutations())

    detail = session.get_state(BROWSER_TASK_STATE_KEY)["blocker_detail"]
    assert "three_consecutive_no_progress_states" in detail
    assert "browser_" in detail
    assert detail.strip() in message


def test_hover_and_dialog_can_serve_as_a_bounded_read_only_recovery() -> None:
    for tool_name, tool_args in (
        ("browser_hover", {"target_id": "e1"}),
        ("browser_handle_dialog", {"accept": True}),
    ):
        session = _FakeSession()
        state = BrowserRuntimeRail._build_phase_state("reveal the caption")
        action_class = BrowserRuntimeRail._classify_action_class(tool_name, tool_args, state)
        state.update(
            {
                "status": "replan_required",
                "replan_required": True,
                "blocked_strategy": action_class,
                "failed_strategies": [action_class],
            }
        )
        session.update_state({BROWSER_TASK_STATE_KEY: state})

        assert BrowserRuntimeRail._consume_phase_budget(session, tool_name, tool_args) == action_class
        with pytest.raises(ValueError, match="already ran without verified semantic progress"):
            BrowserRuntimeRail._consume_phase_budget(session, tool_name, tool_args)


_DISPATCH_EVENT_SCRIPT = {"function": "() => el.dispatchEvent(new Event('change'))"}


def test_repeated_dispatch_event_script_with_an_unchanged_digest_ends_blocked() -> None:
    """A dispatchEvent loop must be scored, not excused as a neutral observation.

    While the mutation markers were matched against a lowercased expression,
    this script classified as script_extraction, so neutrality hid it from
    no-progress and loop scoring and the gate could never fire.
    """
    assert tool_is_semantically_neutral("browser_evaluate", _DISPATCH_EVENT_SCRIPT) is False

    session, message = _mutate_until_blocked([("browser_evaluate", _DISPATCH_EVENT_SCRIPT)] * 10)

    state = session.get_state(BROWSER_TASK_STATE_KEY)
    assert state["status"] == "blocked"
    assert state["replan_required"] is True
    assert "semantic" in message.lower()


def test_dispatch_event_script_is_not_a_read_only_recovery() -> None:
    assert BrowserRuntimeRail._is_read_only_recovery("browser_evaluate", _DISPATCH_EVENT_SCRIPT) is False
    assert (
        BrowserRuntimeRail._is_read_only_recovery(
            "browser_evaluate",
            {"function": "() => el.setAttribute('value', '42')"},
        )
        is False
    )
    assert (
        BrowserRuntimeRail._is_read_only_recovery(
            "browser_evaluate",
            {"function": "() => document.body.innerText"},
        )
        is True
    )


def test_drop_is_not_a_read_only_recovery() -> None:
    assert BrowserRuntimeRail._is_read_only_recovery("browser_drop", {"target_id": "e1"}) is False
    assert BrowserRuntimeRail._is_read_only_recovery("browser_hover", {"target_id": "e1"}) is True
    assert BrowserRuntimeRail._is_read_only_recovery("browser_handle_dialog", {"accept": True}) is True


def test_neutral_observation_does_not_burn_a_pending_replan_trial() -> None:
    session = _FakeSession()
    state = BrowserRuntimeRail._build_phase_state("reveal the caption")
    state.update(
        {
            "status": "replan_trial",
            "replan_required": True,
            "replan_trial_pending": True,
            "trial_strategy": "pointer_reveal",
            "semantic_revision": 1,
        }
    )
    session.update_state({BROWSER_TASK_STATE_KEY: state})

    BrowserWorkingContextStore.sync_semantic_progress(
        session,
        {
            "revision": 2,
            "progress": "observation",
            "observable_progress": False,
            "replan_required": False,
            "semantic_state": {"url": _HOVERS_URL},
        },
    )

    updated = session.get_state(BROWSER_TASK_STATE_KEY)
    assert updated["replan_trial_pending"] is True
    assert updated["failed_strategies"] == []
    assert updated["status"] == "replan_trial"


def test_hover_then_click_on_one_target_are_materially_different_strategies() -> None:
    state = BrowserRuntimeRail._build_phase_state("reveal then open the profile")
    args = {"target_id": "e_avatar_1"}

    hover = BrowserRuntimeRail._strategy_fingerprint(
        state,
        "browser_hover",
        args,
        BrowserRuntimeRail._classify_action_class("browser_hover", args, state),
    )
    click = BrowserRuntimeRail._strategy_fingerprint(
        state,
        "browser_click",
        args,
        BrowserRuntimeRail._classify_action_class("browser_click", args, state),
    )

    assert hover != click
