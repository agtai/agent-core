#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Unit tests for browser_hover / find / handle_dialog / drop on the BU path."""

from __future__ import annotations

import asyncio
import importlib.util
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from openjiuwen.core.foundation.tool import McpServerConfig
from openjiuwen.harness.tools.browser_move.drivers.base import ActResult, SelectorRef
from openjiuwen.harness.tools.browser_move.drivers.browser_use.driver import BrowserUseDriver
from openjiuwen.harness.tools.browser_move.drivers.browser_use.sidecar import wire
from openjiuwen.harness.tools.browser_move.playwright_runtime.browser_capabilities import (
    BROWSER_DRIVER_DEFERRED_CORE_TOOL_NAMES,
)
from openjiuwen.harness.tools.browser_move.playwright_runtime.config import BrowserRunGuardrails
from openjiuwen.harness.tools.browser_move.playwright_runtime.page_state import BrowserPageState
from openjiuwen.harness.tools.browser_move.playwright_runtime.runtime import (
    BROWSER_CATALOG_RUNTIME_TOOL_NAMES,
    BrowserAgentRuntime,
)

_SIDECAR_DIR = (
    Path(__file__).resolve().parents[5]
    / "openjiuwen"
    / "harness"
    / "tools"
    / "browser_move"
    / "drivers"
    / "browser_use"
    / "sidecar"
)


def _load_sidecar_module(name: str) -> Any:
    if name in sys.modules and getattr(sys.modules[name], "__file__", "").startswith(str(_SIDECAR_DIR)):
        return sys.modules[name]
    path = _SIDECAR_DIR / f"{name}.py"
    if str(_SIDECAR_DIR) not in sys.path:
        sys.path.insert(0, str(_SIDECAR_DIR))
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_cdp = _load_sidecar_module("cdp")
_exceptions = _load_sidecar_module("exceptions")


def _run(coro):
    return asyncio.run(coro)


def _make_runtime() -> BrowserAgentRuntime:
    mcp_cfg = McpServerConfig(
        server_id="test",
        server_name="test",
        server_path="stdio://playwright",
        client_type="stdio",
        params={"cwd": "."},
    )
    return BrowserAgentRuntime(
        provider="openai",
        api_key="test-key",
        api_base="https://example.invalid/v1",
        model_name="test-model",
        mcp_cfg=mcp_cfg,
        guardrails=BrowserRunGuardrails(max_steps=3, max_failures=1, timeout_s=30, retry_once=False),
    )


def _make_driver_with_transport(transport: AsyncMock) -> BrowserUseDriver:
    driver = BrowserUseDriver.__new__(BrowserUseDriver)
    driver._transport = transport
    driver._driver_generation = 3
    return driver


def test_deferred_core_set_empty_and_catalog_includes_four() -> None:
    assert BROWSER_DRIVER_DEFERRED_CORE_TOOL_NAMES == frozenset()
    for name in ("browser_hover", "browser_find", "browser_handle_dialog", "browser_drop"):
        assert name in BROWSER_CATALOG_RUNTIME_TOOL_NAMES
    assert "hover" in wire.DRIVER_METHOD_NAMES
    assert "handle_dialog" in wire.DRIVER_METHOD_NAMES
    assert "drop" in wire.DRIVER_METHOD_NAMES
    assert "find" not in wire.DRIVER_METHOD_NAMES  # runtime/PageState only


class _FakeInputSend:
    def __init__(self, client: "_FakeCdpClient") -> None:
        self._client = client

    async def dispatchMouseEvent(self, params: dict[str, Any], session_id: str | None = None) -> dict[str, Any]:
        del session_id
        self._client.calls.append(("Input.dispatchMouseEvent", dict(params)))
        return {}

    async def dispatchDragEvent(self, params: dict[str, Any], session_id: str | None = None) -> dict[str, Any]:
        del session_id
        self._client.calls.append(("Input.dispatchDragEvent", dict(params)))
        return {}


class _FakePageSend:
    def __init__(self, client: "_FakeCdpClient") -> None:
        self._client = client

    async def enable(self, params: dict[str, Any], session_id: str | None = None) -> dict[str, Any]:
        del params, session_id
        self._client.calls.append(("Page.enable", {}))
        return {}

    async def handleJavaScriptDialog(
        self, params: dict[str, Any], session_id: str | None = None
    ) -> dict[str, Any]:
        del session_id
        self._client.calls.append(("Page.handleJavaScriptDialog", dict(params)))
        return {}


class _FakeCdpClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.send = SimpleNamespace(Input=_FakeInputSend(self), Page=_FakePageSend(self))
        self._event_registry = SimpleNamespace(register=MagicMock(), unregister=MagicMock())


def test_cdp_hover_move_dispatches_mouse_moved() -> None:
    client = _FakeCdpClient()

    async def _go() -> None:
        await _cdp.dispatch_mouse_move(client, "s1", x=10.0, y=20.0)

    _run(_go())
    assert any(
        name == "Input.dispatchMouseEvent" and params.get("type") == "mouseMoved"
        for name, params in client.calls
    )


def test_cdp_external_drop_rejects_unsupported_mime_without_dispatch() -> None:
    client = _FakeCdpClient()
    with pytest.raises(_exceptions.DriverUnsupported, match="image/png"):
        _run(
            _cdp.perform_external_drop(
                client,
                "s1",
                x=1.0,
                y=2.0,
                items=[{"mimeType": "image/png", "data": "x"}],
            )
        )
    assert not any(name == "Input.dispatchDragEvent" for name, _ in client.calls)


def test_cdp_external_drop_happy_path_with_temp_file() -> None:
    client = _FakeCdpClient()
    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as handle:
        handle.write(b"payload")
        path = handle.name
    try:
        result = _run(
            _cdp.perform_external_drop(
                client,
                "s1",
                x=5.0,
                y=6.0,
                paths=[path],
                items=[{"mimeType": "text/plain", "data": "hello"}],
            )
        )
    finally:
        Path(path).unlink(missing_ok=True)
    assert result["ok"] is True
    types = [params.get("type") for name, params in client.calls if name == "Input.dispatchDragEvent"]
    assert types == ["dragEnter", "dragOver", "drop"]


def test_cdp_handle_javascript_dialog_calls_cdp() -> None:
    client = _FakeCdpClient()
    _run(_cdp.handle_javascript_dialog(client, "s1", accept=True, prompt_text="hi"))
    assert ("Page.handleJavaScriptDialog", {"accept": True, "promptText": "hi"}) in client.calls


def test_page_state_find_requires_snapshot() -> None:
    state = BrowserPageState(generation=1)
    assert state.has_searchable_snapshot() is False


def test_page_state_find_matches_ax_text() -> None:
    state = BrowserPageState(generation=2)
    state.register_ax_snapshot('- button "Submit" [ref=e12]\n- link "Next" [ref=e13]')
    matches = state.find_in_snapshot("Submit", limit=5)
    assert matches
    assert matches[0]["ref"] == "e12"
    assert "Submit" in matches[0]["context"]


def test_runtime_hover_missing_target_fails_closed() -> None:
    runtime = _make_runtime()
    runtime._page_state = BrowserPageState(generation=1)
    runtime.ensure_runtime_ready = AsyncMock()
    runtime._ensure_browser_driver = AsyncMock()
    result = _run(
        BrowserAgentRuntime.hover(
            runtime,
            generation_id="g1",
            target_id="",
            ref="",
            selector="",
        )
    )
    assert result["ok"] is False
    assert "failed" in (result.get("error") or "").lower()
    runtime._ensure_browser_driver.assert_not_called()


def test_runtime_find_without_snapshot_fails_closed() -> None:
    runtime = _make_runtime()
    runtime._page_state = BrowserPageState(generation=1)
    runtime.ensure_runtime_ready = AsyncMock()
    result = _run(BrowserAgentRuntime.find(runtime, query="hello"))
    assert result["ok"] is False
    assert "browser_snapshot" in (result.get("error") or "")
    assert result["matches"] == []


def test_runtime_find_happy_path() -> None:
    runtime = _make_runtime()
    state = BrowserPageState(generation=1)
    state.register_ax_snapshot('- button "Go" [ref=e1]')
    runtime._page_state = state
    runtime.ensure_runtime_ready = AsyncMock()
    result = _run(BrowserAgentRuntime.find(runtime, query="Go", generation_id="g1"))
    assert result["ok"] is True
    assert result["count"] >= 1


def test_runtime_drop_missing_paths_and_data_fails_closed() -> None:
    runtime = _make_runtime()
    runtime._page_state = BrowserPageState(generation=1)
    runtime.ensure_runtime_ready = AsyncMock()
    runtime._ensure_browser_driver = AsyncMock()
    result = _run(
        BrowserAgentRuntime.drop(
            runtime,
            generation_id="g1",
            paths=[],
            data=[],
            selector="#drop",
        )
    )
    assert result["ok"] is False
    assert "paths or data" in (result.get("error") or "")
    runtime._ensure_browser_driver.assert_not_called()


def test_runtime_drop_missing_path_fails_before_driver() -> None:
    runtime = _make_runtime()
    runtime._page_state = BrowserPageState(generation=1)
    runtime.ensure_runtime_ready = AsyncMock()
    runtime._ensure_browser_driver = AsyncMock()
    missing = str(Path(tempfile.gettempdir()) / "openjiuwen_missing_drop_file.txt")
    result = _run(
        BrowserAgentRuntime.drop(
            runtime,
            generation_id="g1",
            paths=[missing],
            selector="#drop",
        )
    )
    assert result["ok"] is False
    assert "not found" in (result.get("error") or "").lower()
    runtime._ensure_browser_driver.assert_not_called()


def test_driver_hover_serializes_to_sidecar() -> None:
    transport = AsyncMock()
    transport.request = AsyncMock(
        return_value={"ok": True, "detail": "hovered", "document_changed": False, "driver_generation": 4}
    )
    driver = _make_driver_with_transport(transport)
    result = _run(driver.hover(SelectorRef(css="#btn")))
    assert result.ok is True
    method, params = transport.request.call_args.args
    assert method == "hover"
    assert params["ref"]["kind"] == "selector"


def test_driver_handle_dialog_serializes() -> None:
    transport = AsyncMock()
    transport.request = AsyncMock(
        return_value={
            "ok": True,
            "detail": "armed for next dialog; call before the action that opens it",
            "document_changed": False,
            "driver_generation": 4,
        }
    )
    driver = _make_driver_with_transport(transport)
    result = _run(driver.handle_dialog(accept=True, prompt_text="x"))
    assert result.ok is True
    method, params = transport.request.call_args.args
    assert method == "handle_dialog"
    assert params == {"accept": True, "prompt_text": "x"}


def test_driver_drop_missing_path_skips_sidecar() -> None:
    transport = AsyncMock()
    transport.request = AsyncMock()
    driver = _make_driver_with_transport(transport)
    missing = str(Path(tempfile.gettempdir()) / "openjiuwen_missing_drop2.txt")
    result = _run(driver.drop(SelectorRef(css="#zone"), paths=[missing]))
    assert result.ok is False
    assert "not found" in result.detail.lower()
    transport.request.assert_not_called()


def test_driver_drop_temp_file_calls_sidecar() -> None:
    transport = AsyncMock()
    transport.request = AsyncMock(
        return_value={"ok": True, "detail": "dropped", "document_changed": False, "driver_generation": 5}
    )
    driver = _make_driver_with_transport(transport)
    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as handle:
        handle.write(b"x")
        path = handle.name
    try:
        result = _run(
            driver.drop(
                SelectorRef(css="#zone"),
                paths=[path],
                data=[{"mimeType": "text/plain", "data": "hi"}],
            )
        )
    finally:
        Path(path).unlink(missing_ok=True)
    assert result.ok is True
    method, params = transport.request.call_args.args
    assert method == "drop"
    assert params["paths"] == [str(Path(path).resolve())]
    assert params["data"][0]["mimeType"] == "text/plain"


def test_runtime_hover_happy_path_mock() -> None:
    runtime = _make_runtime()
    runtime._page_state = BrowserPageState(generation=1)
    runtime.ensure_runtime_ready = AsyncMock()
    runtime._resolve_catalog_element_ref = AsyncMock(return_value=SelectorRef(css="#a"))
    driver = AsyncMock()
    driver.hover = AsyncMock(
        return_value=ActResult(ok=True, detail="hovered", document_changed=False, driver_generation=1)
    )
    runtime._ensure_browser_driver = AsyncMock(return_value=driver)
    result = _run(BrowserAgentRuntime.hover(runtime, generation_id="g1", selector="#a"))
    assert result["ok"] is True
    driver.hover.assert_called_once()


def test_runtime_handle_dialog_happy_path_mock() -> None:
    runtime = _make_runtime()
    runtime.ensure_runtime_ready = AsyncMock()
    driver = AsyncMock()
    driver.handle_dialog = AsyncMock(
        return_value=ActResult(
            ok=True,
            detail="armed for next dialog; call before the action that opens it",
            document_changed=False,
            driver_generation=1,
        )
    )
    runtime._ensure_browser_driver = AsyncMock(return_value=driver)
    result = _run(BrowserAgentRuntime.handle_dialog(runtime, accept=False))
    assert result["ok"] is True
    assert "armed" in (result.get("detail") or "")
