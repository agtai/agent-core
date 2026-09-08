# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""B2 batch_executor coverage over FakeDriver (no live browser)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from openjiuwen.harness.tools.browser_move.playwright_runtime.batch_executor import execute_batch
from tests.unit_tests.harness.tools.browser_move.fakes.fake_driver import FakeDriver


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


async def _connected_driver() -> FakeDriver:
    driver = FakeDriver(driver_generation=1)
    await driver.connect(cdp_url="http://127.0.0.1:9222")
    return driver


def test_target_ops_click_fill_type_select_set_checked() -> None:
    async def _coro() -> dict[str, Any]:
        driver = await _connected_driver()
        return await execute_batch(
            driver,
            steps=[
                {"op": "click", "selector": "#go"},
                {"op": "fill", "selector": "#name", "value": "Ada"},
                {"op": "type", "selector": "#bio", "value": "hi", "delay_ms": 1},
                {"op": "select_option", "selector": "#nat", "option_text": "SG"},
                {"op": "set_checked", "selector": "#agree", "checked": True},
            ],
            generation_id="g1",
        )

    result = _run(_coro())
    assert result["ok"] is True
    assert result["status"] == "completed"
    assert [step["op"] for step in result["steps"]] == [
        "click",
        "fill",
        "type",
        "select_option",
        "set_checked",
    ]


def test_option_target_ops_autocomplete_and_select_visible_text() -> None:
    async def _coro() -> dict[str, Any]:
        driver = await _connected_driver()
        return await execute_batch(
            driver,
            steps=[
                {
                    "op": "autocomplete",
                    "placeholder": "From",
                    "value": "Singapore",
                    "choose_text": "Singapore (SIN)",
                    "resolved_option_target_id": "t_g1_1",
                },
                {
                    "op": "select_visible_text",
                    "option_role": "option",
                    "option_name": "Kuala Lumpur (KUL)",
                },
            ],
            generation_id="g1",
        )

    result = _run(_coro())
    assert result["ok"] is True
    assert result["steps"][0]["op"] == "autocomplete"
    assert result["steps"][1]["op"] == "select_visible_text"


def test_condition_ops_wait_family() -> None:
    async def _coro() -> dict[str, Any]:
        driver = await _connected_driver()
        return await execute_batch(
            driver,
            steps=[
                {"op": "wait_for_selector", "selector": "#ready"},
                {"op": "wait_for_text", "text": "Done"},
                {"op": "wait_for_load_state", "state": "domcontentloaded"},
                {"op": "wait_for_url", "url_contains": "example"},
                {"op": "wait_for_first_card_title", "selector": "h2", "expected_text": "ok"},
                {"op": "wait_for_sort_state", "selector": "#sort", "expected_value": "ok"},
                {"op": "wait_for_result_count", "selector": ".row", "min_count": 1},
                {"op": "wait_for_dom_text_change", "selector": "#msg", "previous_text": "old"},
                {"op": "wait_for_stable", "selector": "#panel", "stable_ms": 50},
                {"op": "wait_for_tab", "min_tabs": 1, "activate": False},
            ],
            condition_timeout_ms=2000,
            generation_id="g1",
        )

    result = _run(_coro())
    assert result["ok"] is True
    assert len(result["conditions"]) == 10


def test_sleep_extract_and_screenshot() -> None:
    async def _coro() -> dict[str, Any]:
        driver = await _connected_driver()
        return await execute_batch(
            driver,
            steps=[
                {"op": "sleep", "ms": 10},
                {"op": "extract_text", "selector": "h1", "field": "title"},
                {"op": "extract_value", "selector": "#email", "field": "email"},
                {"op": "screenshot", "path": "screenshots/batch.png"},
            ],
            generation_id="g1",
        )

    result = _run(_coro())
    assert result["ok"] is True
    assert result["extracted"]["title"]
    assert result["extracted"]["email"]
    assert result["steps"][-1]["path"] == "screenshots/batch.png"


def test_mid_sequence_failure_yields_per_step_error_without_aborting_optional_tail() -> None:
    async def _coro() -> dict[str, Any]:
        driver = await _connected_driver()

        original_click = driver.click

        async def _click(ref, **kwargs):  # type: ignore[no-untyped-def]
            css = getattr(ref, "css", "")
            if css == "#boom":
                from openjiuwen.harness.tools.browser_move.drivers.base import ActResult

                return ActResult(ok=False, detail="click intercepted", document_changed=False, driver_generation=1)
            return await original_click(ref, **kwargs)

        driver.click = _click  # type: ignore[method-assign]
        return await execute_batch(
            driver,
            steps=[
                {"op": "click", "selector": "#ok"},
                {"op": "click", "selector": "#boom"},
                {"op": "click", "selector": "#later", "optional": True},
            ],
            continue_on_error=False,
            generation_id="g1",
        )

    result = _run(_coro())
    assert result["ok"] is False
    assert result["status"] in {"failed", "partial"}
    assert result["steps"][0]["ok"] is True
    assert result["steps"][1]["ok"] is False
    assert "intercept" in str(result["steps"][1]["error"]).lower()
    # Fail-fast: third step not executed
    assert len(result["steps"]) == 2


def test_continue_on_error_keeps_going() -> None:
    async def _coro() -> dict[str, Any]:
        driver = await _connected_driver()
        original_click = driver.click

        async def _click(ref, **kwargs):  # type: ignore[no-untyped-def]
            css = getattr(ref, "css", "")
            if css == "#boom":
                from openjiuwen.harness.tools.browser_move.drivers.base import ActResult

                return ActResult(ok=False, detail="boom", document_changed=False, driver_generation=1)
            return await original_click(ref, **kwargs)

        driver.click = _click  # type: ignore[method-assign]
        return await execute_batch(
            driver,
            steps=[
                {"op": "click", "selector": "#boom"},
                {"op": "sleep", "ms": 1},
            ],
            continue_on_error=True,
            generation_id="g1",
        )

    result = _run(_coro())
    assert result["ok"] is False
    assert result["status"] == "partial"
    assert len(result["steps"]) == 2
    assert result["steps"][1]["ok"] is True


@pytest.mark.parametrize(
    "steps",
    [
        [{"op": "fill", "selector": "#x"}],  # missing value
        [{"op": "click"}],  # missing target
        [{"op": "wait_for_text"}],  # missing text
    ],
)
def test_validation_still_owned_by_action_module(steps: list[dict[str, Any]]) -> None:
    from openjiuwen.harness.tools.browser_move.controllers.action import validate_batch_steps

    errors = validate_batch_steps(steps)
    assert errors
