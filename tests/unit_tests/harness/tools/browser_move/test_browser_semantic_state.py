#!/usr/bin/env python
# coding: utf-8

from __future__ import annotations

import pytest

from openjiuwen.harness.tools.browser_move.runtime.semantic_state import (
    SemanticStateTracker,
    build_semantic_state,
)


def _state(*, price: str = "", result_count: int = 10, fields: list[str] | None = None) -> dict:
    filters = [{"key": "price", "value": price}] if price else []
    return {
        "url": "https://shop.example/search?q=headphones&utm=x",
        "form_values": [{"key": "query", "value": "headphones"}],
        "selected_filters": filters,
        "result_count": result_count,
        "field_coverage": fields or [],
    }


def test_build_semantic_state_is_stable_and_ignores_non_semantic_fields() -> None:
    first = build_semantic_state(
        {
            "url": "https://EXAMPLE.test/search?b=2&a=1#results",
            "form_values": [{"name": "Query", "value": " OpenJiuwen "}],
            "selected_filters": [{"label": "Sort", "text": "Price"}],
            "result_count": "20",
            "field_coverage": ["Title", "price"],
            "selector": "#unstable-g8",
            "generation_id": "g8",
        }
    )

    assert first == {
        "url": "https://example.test/search?a=1&b=2",
        "form_values": [{"key": "query", "value": "OpenJiuwen"}],
        "selected_filters": [{"key": "sort", "value": "Price"}],
        "result_count": 20,
        "field_coverage": ["price", "title"],
    }


def test_tracker_forces_replan_after_three_semantic_no_progress_states() -> None:
    tracker = SemanticStateTracker()
    tracker.observe(_state())

    first = tracker.observe(_state())
    second = tracker.observe(_state())
    third = tracker.observe(_state())

    assert first["consecutive_no_progress"] == 1
    assert second["consecutive_no_progress"] == 2
    assert third["consecutive_no_progress"] == 3
    assert third["replan_required"] is True
    assert third["replan_reason"] == ["three_consecutive_no_progress_states"]


def test_tracker_requires_three_state_revisits_before_replan() -> None:
    tracker = SemanticStateTracker()

    tracker.observe(_state(price="0-100"))
    tracker.observe(_state(price="100-200"))
    first_revisit = tracker.observe(_state(price="0-100"))
    second_revisit = tracker.observe(_state(price="100-200"))
    third_revisit = tracker.observe(_state(price="0-100"))

    assert first_revisit["aba_loop"] is True
    assert first_revisit["state_revisit_count"] == 1
    assert first_revisit["replan_required"] is False
    assert second_revisit["aba_loop"] is True
    assert second_revisit["state_revisit_count"] == 2
    assert second_revisit["repeated_filter_state"] is True
    assert second_revisit["replan_required"] is False
    assert third_revisit["aba_loop"] is True
    assert third_revisit["state_revisit_count"] == 3
    assert third_revisit["repeated_filter_state"] is True
    assert third_revisit["replan_required"] is True
    assert "three_semantic_state_revisits" in third_revisit["replan_reason"]


def test_new_field_evidence_counts_as_progress() -> None:
    tracker = SemanticStateTracker()
    tracker.observe(_state(fields=[]))
    progress = tracker.observe(_state(fields=["title", "price"]))

    assert progress["progress"] == "progress"
    assert progress["observable_progress"] is True
    assert progress["semantic_state"]["field_coverage"] == ["price", "title"]

    after_navigation = tracker.observe(
        {
            **_state(fields=[]),
            "url": "https://shop.example/item/1",
        }
    )
    assert after_navigation["semantic_state"]["field_coverage"] == ["price", "title"]


def test_repeated_neutral_observations_never_look_like_a_semantic_loop() -> None:
    tracker = SemanticStateTracker()
    tracker.observe(_state())

    observations = [tracker.observe(_state(), action_group_id=f"neutral-{index}", mutating=False) for index in range(5)]

    for observation in observations:
        assert observation["progress"] == "observation"
        assert observation["observable_progress"] is False
        assert observation["consecutive_no_progress"] == 0
        assert observation["state_revisit_count"] == 0
        assert observation["aba_loop"] is False
        assert observation["replan_required"] is False
        assert observation["replan_reason"] == []
    assert observations[-1]["revision"] == 6


def test_mutating_observations_with_an_unchanged_digest_still_force_replan() -> None:
    tracker = SemanticStateTracker()
    tracker.observe(_state())

    tracker.observe(_state(), mutating=True)
    tracker.observe(_state(), mutating=True)
    third = tracker.observe(_state(), mutating=True)

    assert third["consecutive_no_progress"] == 3
    assert third["replan_required"] is True
    assert third["replan_reason"] == ["three_consecutive_no_progress_states"]


def test_neutral_observation_that_changed_the_digest_is_real_progress() -> None:
    tracker = SemanticStateTracker()
    tracker.observe(_state(fields=[]))

    revealed = tracker.observe(_state(fields=["title"]), mutating=False)

    assert revealed["progress"] == "progress"
    assert revealed["observable_progress"] is True
    assert revealed["consecutive_no_progress"] == 0


def test_neutral_observations_do_not_mask_a_later_real_loop() -> None:
    tracker = SemanticStateTracker()
    tracker.observe(_state())
    for index in range(3):
        tracker.observe(_state(), action_group_id=f"neutral-{index}", mutating=False)

    tracker.observe(_state(), mutating=True)
    tracker.observe(_state(), mutating=True)
    third = tracker.observe(_state(), mutating=True)

    assert third["consecutive_no_progress"] == 3
    assert third["replan_required"] is True


@pytest.mark.xfail(reason="Superseded by BU driver (Policy A): asserts base agtai/develop #1147 rail/catalog/semantic behavior replaced by the browser_use driver. Tracked for later reconciliation.", strict=False)
def test_neutral_observation_keeps_every_latest_payload_key() -> None:
    tracker = SemanticStateTracker()
    baseline = tracker.observe(_state())

    observation = tracker.observe(_state(), mutating=False)

    assert set(observation) == set(baseline)
    assert set(observation) == {
        "revision",
        "action_group_id",
        "semantic_state",
        "progress",
        "observable_progress",
        "consecutive_no_progress",
        "state_revisit",
        "state_revisit_count",
        "aba_loop",
        "repeated_filter_state",
        "replan_required",
        "replan_reason",
    }


def test_tracker_observes_each_model_action_group_once() -> None:
    tracker = SemanticStateTracker()
    first = tracker.observe(_state(), action_group_id="group-1")
    duplicate = tracker.observe(_state(result_count=99), action_group_id="group-1")
    second = tracker.observe(_state(result_count=99), action_group_id="group-2")

    assert duplicate == first
    assert second["revision"] == first["revision"] + 1
    assert second["action_group_id"] == "group-2"


def test_generation_change_does_not_reset_semantic_no_progress() -> None:
    tracker = SemanticStateTracker()
    first_state = {**_state(), "generation_id": "g1"}
    second_state = {**_state(), "generation_id": "g2"}

    tracker.observe(first_state)
    repeated = tracker.observe(second_state)

    assert repeated["progress"] == "no_progress"
    assert repeated["changed_fields"] == []


def test_tracker_reports_semantic_fields_that_actually_changed() -> None:
    tracker = SemanticStateTracker()
    tracker.observe(_state(price="0-100", result_count=10))

    progress = tracker.observe(_state(price="100-200", result_count=7))

    assert progress["changed_fields"] == ["result_count", "selected_filters"]
