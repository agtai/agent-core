#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""B1 id-bridge, generation accounting, and single-step batch routing tests."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from unittest.mock import AsyncMock

from openjiuwen.harness.tools.browser_move.drivers.base import IndexRef, NodeRef, SelectorRef
from openjiuwen.harness.tools.browser_move.playwright_runtime.config import BrowserInstanceConfig
from openjiuwen.harness.tools.browser_move.playwright_runtime.page_state import (
    BrowserPageState,
    BrowserTarget,
)
from openjiuwen.harness.tools.browser_move.playwright_runtime.runtime import BrowserAgentRuntime

from tests.unit_tests.harness.tools.browser_move.fakes.fake_driver import FakeDriver


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


def _make_driver_runtime(driver: FakeDriver) -> BrowserAgentRuntime:
    instance = BrowserInstanceConfig(browser_driver_backend="browser_use")
    runtime = BrowserAgentRuntime.__new__(BrowserAgentRuntime)
    runtime._instance = instance
    runtime._page_state = BrowserPageState()
    runtime._page_generation = runtime._page_state.generation
    runtime._reference_generations = runtime._page_state.reference_generations
    runtime._selector_primary_links = runtime._page_state.selector_primary_links
    runtime._last_observed_url = ""
    runtime._driver = driver
    runtime._driver_info = None
    runtime._service = SimpleNamespace(cdp_endpoint="http://127.0.0.1:9222")
    return runtime


def _bu_target(
    page_state: BrowserPageState,
    *,
    bu_index: str = "",
    backend_node_id: str = "42",
    driver_generation: str = "1",
    selector: str = "",
) -> BrowserTarget:
    locator: dict[str, str] = {}
    if bu_index:
        locator["bu_index"] = bu_index
    if backend_node_id:
        locator["backend_node_id"] = backend_node_id
    if driver_generation:
        locator["driver_generation"] = driver_generation
    if selector:
        locator["selector"] = selector
    return page_state._new_target(
        source="bu",
        locator=locator,
        selector=selector,
        visible=True,
        enabled=True,
        actionable=True,
        clickable=True,
        role="button",
        name="Go",
        text="Go",
    )


def test_selector_present_skips_stamp_and_clicks_by_selector() -> None:
    async def exercise() -> None:
        driver = FakeDriver(driver_generation=2)
        await driver.connect(cdp_url="http://127.0.0.1:9222")
        runtime = _make_driver_runtime(driver)
        target = _bu_target(runtime._page_state, selector="#submit", driver_generation="2")
        materialized = await runtime._materialize_ax_target(target)
        assert materialized.locator.get("selector") == "#submit"
        assert not any(call.method == "stamp" for call in driver.act_calls)

        step = {
            "op": "click",
            "selector": "#submit",
            "description": "Submit",
        }
        result = await runtime._run_single_batch_primitive_via_driver(step)
        assert result is not None
        assert result["ok"] is True
        click_calls = [call for call in driver.act_calls if call.method == "click"]
        assert len(click_calls) == 1
        assert isinstance(click_calls[0].kwargs["ref"], SelectorRef)
        assert click_calls[0].kwargs["ref"].css == "#submit"
        assert not any(call.method == "stamp" for call in driver.act_calls)

    _run(exercise())


def test_fresh_bu_index_stamps_with_index_ref() -> None:
    async def exercise() -> None:
        driver = FakeDriver(driver_generation=5)
        await driver.connect(cdp_url="http://127.0.0.1:9222")
        runtime = _make_driver_runtime(driver)
        target = _bu_target(
            runtime._page_state,
            bu_index="3",
            backend_node_id="42",
            driver_generation="5",
        )
        await runtime._materialize_ax_target(target)
        stamp_calls = [call for call in driver.act_calls if call.method == "stamp"]
        assert len(stamp_calls) == 1
        ref = stamp_calls[0].kwargs["ref"]
        assert isinstance(ref, IndexRef)
        assert ref.index == 3
        assert ref.driver_generation == 5

    _run(exercise())


def test_stale_driver_generation_retries_stamp_with_node_ref() -> None:
    async def exercise() -> None:
        driver = FakeDriver(driver_generation=5)
        await driver.connect(cdp_url="http://127.0.0.1:9222")
        runtime = _make_driver_runtime(driver)
        target = _bu_target(
            runtime._page_state,
            bu_index="3",
            backend_node_id="42",
            driver_generation="3",
        )
        await runtime._materialize_ax_target(target)
        stamp_calls = [call for call in driver.act_calls if call.method == "stamp"]
        assert len(stamp_calls) == 2
        assert isinstance(stamp_calls[0].kwargs["ref"], IndexRef)
        assert isinstance(stamp_calls[1].kwargs["ref"], NodeRef)
        assert stamp_calls[1].kwargs["ref"].backend_node_id == 42

    _run(exercise())


def test_vanished_backend_node_surfaces_stale_target_error() -> None:
    async def exercise() -> None:
        driver = FakeDriver(driver_generation=1, vanished_backend_node_ids=frozenset({99}))
        await driver.connect(cdp_url="http://127.0.0.1:9222")
        runtime = _make_driver_runtime(driver)
        target = _bu_target(runtime._page_state, backend_node_id="99", driver_generation="")
        target.locator.pop("driver_generation", None)
        target.locator.pop("bu_index", None)
        with pytest.raises(ValueError, match="vanished"):
            await runtime._materialize_ax_target(target)

    _run(exercise())


def test_refresh_target_id_ambiguity_raises() -> None:
    page_state = BrowserPageState()
    stale = page_state._new_target(
        source="bu",
        locator={"backend_node_id": "1"},
        role="button",
        name="Go",
        text="Go",
        kind="action",
        visible=True,
        enabled=True,
        actionable=True,
    )
    page_state.advance()
    for backend_node_id in ("2", "3"):
        page_state._new_target(
            source="bu",
            locator={"backend_node_id": backend_node_id},
            role="button",
            name="Go",
            text="Go",
            kind="action",
            visible=True,
            enabled=True,
            actionable=True,
        )
    with pytest.raises(ValueError, match=r"ambiguous \(2 matches\)"):
        page_state.refresh_target_id(stale.target_id)


def test_document_changed_bumps_generation_id_not_driver_generation() -> None:
    driver = FakeDriver(driver_generation=9)
    runtime = _make_driver_runtime(driver)
    runtime._last_observed_url = "https://old.example/"
    assert runtime.generation_id == "g0"
    runtime._apply_document_changed(changed=True, url="https://new.example/", title="New")
    assert runtime.generation_id == "g1"
    assert driver.driver_generation == 9
    exported = runtime.export_page_state()
    assert exported["generation_id"] == "g1"
    assert "driver_generation" not in exported


def test_plain_click_does_not_bump_generation_id() -> None:
    driver = FakeDriver(driver_generation=4)
    runtime = _make_driver_runtime(driver)
    runtime._apply_document_changed(changed=False, url="https://same.example/", title="Same")
    assert runtime.generation_id == "g0"
    runtime._apply_document_changed(changed=False)
    assert runtime.generation_id == "g0"


def test_snapshot_ref_click_and_type_use_current_browser_driver_identity() -> None:
    async def exercise() -> None:
        driver = FakeDriver(driver_generation=2)
        await driver.connect(cdp_url="http://127.0.0.1:9222")
        runtime = _make_driver_runtime(driver)
        runtime.ensure_runtime_ready = AsyncMock()

        observation = SimpleNamespace(
            url="https://example.test/form",
            title="Form",
            driver_generation=2,
            elements=(
                SimpleNamespace(
                    index=2,
                    backend_node_id=42,
                    frame_id="frame-1",
                    role="textbox",
                    name="Customer name",
                    visible=True,
                ),
                SimpleNamespace(
                    index=3,
                    backend_node_id=43,
                    frame_id="frame-1",
                    role="button",
                    name="Submit",
                    visible=True,
                ),
            ),
            ax_text='\n'.join(
                [
                    '- textbox "Customer name" [2]',
                    '- button "Submit" [3]',
                ]
            ),
        )

        runtime._register_observation_targets(observation)
        runtime._register_snapshot_refs(observation.ax_text, replace=True)

        typed = await runtime.type_text(generation_id="g0", ref="[2]", text="Ada Lovelace")
        clicked = await runtime.click(generation_id="g0", ref="3")

        assert typed["ok"] is True
        assert clicked["ok"] is True

        stamp_calls = [call for call in driver.act_calls if call.method == "stamp"]
        type_calls = [call for call in driver.act_calls if call.method == "type_text"]
        click_calls = [call for call in driver.act_calls if call.method == "click"]
        assert len(stamp_calls) == 2
        assert isinstance(stamp_calls[0].kwargs["ref"], IndexRef)
        assert stamp_calls[0].kwargs["ref"].index == 2
        assert stamp_calls[0].kwargs["ref"].driver_generation == 2
        assert isinstance(stamp_calls[1].kwargs["ref"], IndexRef)
        assert stamp_calls[1].kwargs["ref"].index == 3
        assert stamp_calls[1].kwargs["ref"].driver_generation == 2
        assert isinstance(type_calls[0].kwargs["ref"], SelectorRef)
        assert "data-openjiuwen-target-id" in type_calls[0].kwargs["ref"].css
        assert isinstance(click_calls[0].kwargs["ref"], SelectorRef)
        assert "data-openjiuwen-target-id" in click_calls[0].kwargs["ref"].css

    _run(exercise())


def test_stale_snapshot_ref_reports_current_generation_on_browser_driver_path() -> None:
    async def exercise() -> None:
        driver = FakeDriver(driver_generation=4)
        await driver.connect(cdp_url="http://127.0.0.1:9222")
        runtime = _make_driver_runtime(driver)
        runtime.ensure_runtime_ready = AsyncMock()

        runtime._page_state.advance(url="https://example.test/current")
        runtime._register_observation_targets(
            SimpleNamespace(
                url="https://example.test/current",
                title="Current",
                elements=(
                    SimpleNamespace(
                        index=2,
                        backend_node_id=42,
                        frame_id="frame-1",
                        role="textbox",
                        name="Customer name",
                        visible=True,
                    ),
                ),
                driver_generation=4,
                ax_text='- textbox "Customer name" [2]',
            )
        )
        runtime._register_snapshot_refs('- textbox "Customer name" [2]', replace=True)

        result = await runtime.type_text(generation_id="g0", ref="2", text="Ada")

        assert result["ok"] is False
        assert "Stale PageState generation g0" in (result["error"] or "")

    _run(exercise())


@pytest.mark.parametrize(
    ("step", "expected_tool"),
    [
        ({"op": "click", "selector": "#go", "description": "Go"}, "browser_click"),
        ({"op": "fill", "selector": "#name", "value": "Ada"}, "browser_type"),
        ({"op": "press", "key": "Enter"}, "browser_press_key"),
        ({"op": "sleep", "ms": 250}, "browser_wait_for"),
        ({"op": "wait_for_text", "text": "Done"}, "browser_wait_for"),
    ],
)
def test_single_batch_primitive_spec_routes_supported_ops(step: dict[str, Any], expected_tool: str) -> None:
    spec = BrowserAgentRuntime._single_batch_primitive_spec(step)
    assert spec is not None
    tool_name, _tool_args = spec
    assert tool_name == expected_tool


def test_multi_step_batch_uses_driver_batch_executor() -> None:
    driver = FakeDriver(driver_generation=1)
    runtime = _make_driver_runtime(driver)

    async def _coro() -> dict[str, Any]:
        await driver.connect(cdp_url="http://127.0.0.1:9222")
        return await runtime._run_batch_via_driver(
            [
                {"op": "click", "selector": "#first"},
                {"op": "click", "selector": "#second"},
            ],
            generation_id="g0",
        )

    result = _run(_coro())
    assert result["execution_mode"] == "driver_batch"
    assert result["action"] == "browser_batch_interact"
    click_calls = [call for call in driver.act_calls if call.method == "click"]
    assert len(click_calls) >= 2


def test_non_primitive_wait_op_routes_through_driver_batch() -> None:
    driver = FakeDriver(driver_generation=1)
    runtime = _make_driver_runtime(driver)
    assert BrowserAgentRuntime._single_batch_primitive_spec({"op": "wait_for_selector", "selector": "#x"}) is None

    async def _coro() -> dict[str, Any]:
        await driver.connect(cdp_url="http://127.0.0.1:9222")
        return await runtime._run_batch_via_driver(
            [{"op": "wait_for_selector", "selector": "#x"}],
            generation_id="g0",
        )

    result = _run(_coro())
    assert result["execution_mode"] == "driver_batch"
    assert any(call.method == "evaluate" for call in driver.act_calls)
