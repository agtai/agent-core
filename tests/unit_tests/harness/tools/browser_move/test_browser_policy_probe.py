# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Runtime hooks used by decision policies: probe_for_policy registers targets, activate_page fronts the tab."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

from openjiuwen.core.foundation.tool import McpServerConfig
from openjiuwen.harness.tools.browser_move.runtime.config import BrowserRunGuardrails
from openjiuwen.harness.tools.browser_move.runtime.runtime import BrowserAgentRuntime
from tests.unit_tests.harness.tools.browser_move.fakes.fake_driver import FakeDriver


def _make_runtime() -> BrowserAgentRuntime:
    mcp_cfg = McpServerConfig(
        server_id="test-policy-runtime",
        server_name="test-policy-runtime",
        server_path="stdio://playwright",
        client_type="stdio",
        params={"cwd": str(Path.cwd())},
    )
    return BrowserAgentRuntime(
        provider="openai",
        api_key="test-key",
        api_base="https://example.invalid/v1",
        model_name="test-model",
        mcp_cfg=mcp_cfg,
        guardrails=BrowserRunGuardrails(max_steps=3, max_failures=1, timeout_s=30, retry_once=False),
    )


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


def test_probe_for_policy_registers_stamped_elements_as_targets() -> None:
    runtime = _make_runtime()
    runtime.ensure_runtime_ready = AsyncMock()  # type: ignore[method-assign]
    runtime._evaluate_page_js = AsyncMock(  # type: ignore[method-assign]
        return_value={
            "url": "https://flights.test/",
            "title": "Flights",
            "elements": [
                {
                    "node": 7,
                    "selector_hint": '[data-openjiuwen-jev="7"]',
                    "selector_hint_validated": True,
                    "match_count": 1,
                    "role": "combobox",
                    "label": "Where to?",
                    "text": "Where to?",
                    "editable": True,
                    "visible": True,
                    "enabled": True,
                    "actionable": True,
                    "clickable": True,
                }
            ],
        }
    )

    result = _run(runtime.probe_for_policy("(params) => ({})", {"max_items": 250}))

    element = result["elements"][0]
    assert element["target_id"].startswith("t_g"), "probe elements must become PageState targets"
    assert result["generation_id"] == runtime.generation_id
    assert runtime._evaluate_page_js.await_args.kwargs["args"] == {"max_items": 250}
    resolved = _run(
        runtime._resolve_catalog_element_ref(generation_id=result["generation_id"], target_id=element["target_id"])
    )
    assert getattr(resolved, "css", None) == '[data-openjiuwen-jev="7"]'


def test_probe_for_policy_returns_a_failure_envelope_when_evaluate_raises() -> None:
    """B3: a page-script error must degrade to a structured failure, not propagate."""
    runtime = _make_runtime()
    runtime.ensure_runtime_ready = AsyncMock()  # type: ignore[method-assign]
    runtime._evaluate_page_js = AsyncMock(side_effect=RuntimeError("detached frame"))  # type: ignore[method-assign]

    result = _run(runtime.probe_for_policy("(params) => ({})", {"max_items": 250}))

    assert result["ok"] is False
    assert result["error"]
    assert result["elements"] == []


def test_activate_page_switches_to_the_tab_at_url() -> None:
    runtime = _make_runtime()
    driver = FakeDriver()
    _run(driver.connect(cdp_url="http://fake"))
    runtime._ensure_browser_driver = AsyncMock(return_value=driver)  # type: ignore[method-assign]

    assert _run(runtime.activate_page("https://example.test/")) is True
    assert [call.method for call in driver.act_calls if call.method == "switch_tab"] == ["switch_tab"]
