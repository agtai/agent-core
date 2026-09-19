#!/usr/bin/env python
# coding: utf-8
"""Runtime hooks used by decision policies: probe_for_policy registers targets, activate_page fronts the tab."""
# pylint: disable=protected-access

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

from openjiuwen.core.foundation.tool import McpServerConfig
from openjiuwen.harness.tools.browser_move.playwright_runtime.config import BrowserRunGuardrails
from openjiuwen.harness.tools.browser_move.playwright_runtime.runtime import BrowserAgentRuntime

_SOURCE = "(params) => ({})"
_PARAMS = {"max_items": 250, "stamp_attribute": "data-openjiuwen-policy"}
_ELEMENT = {
    "node": 7,
    "selector_hint": '[data-openjiuwen-policy="7"]',
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


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


def _make_runtime() -> BrowserAgentRuntime:
    mcp_cfg = McpServerConfig(
        server_id="test-policy-runtime",
        server_name="test-policy-runtime",
        server_path="stdio://playwright",
        client_type="stdio",
        params={"cwd": str(Path.cwd())},
    )
    runtime = BrowserAgentRuntime(
        provider="openai",
        api_key="test-key",
        api_base="https://example.invalid/v1",
        model_name="test-model",
        mcp_cfg=mcp_cfg,
        guardrails=BrowserRunGuardrails(max_steps=3, max_failures=1, timeout_s=30, retry_once=False),
    )
    runtime.ensure_runtime_ready = AsyncMock()  # type: ignore[method-assign]
    return runtime


def test_probe_for_policy_registers_elements_as_targets_and_passes_params() -> None:
    runtime = _make_runtime()
    runtime._code_executor = AsyncMock(
        return_value={"url": "https://flights.test/", "title": "Flights", "elements": [dict(_ELEMENT)]}
    )

    result = _run(runtime.probe_for_policy(_SOURCE, _PARAMS))

    element = result["elements"][0]
    assert element["target_id"].startswith("t_g"), "probe elements must become PageState targets"
    assert result["generation_id"] == runtime.generation_id
    js_code = runtime._code_executor.await_args.args[0]
    assert _SOURCE in js_code
    assert json.dumps(_PARAMS, ensure_ascii=False) in js_code
    target = runtime.resolve_model_target_id(element["target_id"])
    assert target.locator.get("selector") == _ELEMENT["selector_hint"]


def test_probe_for_policy_returns_a_failure_envelope_when_the_script_raises() -> None:
    runtime = _make_runtime()
    runtime._code_executor = AsyncMock(side_effect=RuntimeError("detached frame"))

    result = _run(runtime.probe_for_policy(_SOURCE, _PARAMS))

    assert result["ok"] is False
    assert "detached frame" in result["error"]
    assert result["elements"] == []


def test_probe_for_policy_reports_a_missing_code_executor() -> None:
    runtime = _make_runtime()
    runtime._code_executor = None

    result = _run(runtime.probe_for_policy(_SOURCE, _PARAMS))

    assert result["ok"] is False
    assert result["error"] == "browser_code_executor_not_ready"
    assert result["elements"] == []


def test_activate_page_brings_the_page_at_url_to_the_front() -> None:
    runtime = _make_runtime()
    runtime._code_executor = AsyncMock(return_value={"ok": True, "url": "https://example.test/"})

    assert _run(runtime.activate_page("https://example.test/")) is True
    js_code = runtime._code_executor.await_args.args[0]
    assert "bringToFront" in js_code
    assert '"https://example.test/"' in js_code


def test_activate_page_returns_false_when_the_script_fails() -> None:
    runtime = _make_runtime()
    runtime._code_executor = AsyncMock(side_effect=RuntimeError("no page"))

    assert _run(runtime.activate_page("https://example.test/")) is False
