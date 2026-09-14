#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Unit tests for sidecar CDP HTML5 drag helpers (stdlib + duck-typed fake client)."""

from __future__ import annotations

import asyncio
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

_SIDECAR_DIR = (
    Path(__file__).resolve().parents[5]
    / "openjiuwen"
    / "harness"
    / "tools"
    / "browser_move"
    / "backends"
    / "browser_use"
    / "sidecar"
)


def _load_sidecar_module(name: str) -> Any:
    """Load a flat sidecar module (``import exceptions`` / ``import cdp``) under its own name."""
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


def _run(coro):
    return asyncio.run(coro)


class _FakeInputSend:
    def __init__(self, client: "_FakeCdpClient") -> None:
        self._client = client

    async def dispatchMouseEvent(self, params: dict[str, Any], session_id: str | None = None) -> dict[str, Any]:
        del session_id
        self._client.calls.append(("Input.dispatchMouseEvent", dict(params)))
        if (
            params.get("type") == "mouseMoved"
            and (params.get("buttons") or 0) & 1
            and self._client.auto_intercept_on_move
            and not self._client._intercept_emitted
        ):
            self._client._intercept_emitted = True
            self._client.emit_drag_intercepted({"items": [{"mimeType": "text/plain", "data": "A"}], "files": [], "dragOperationsMask": 1})
        return {}

    async def setInterceptDrags(self, params: dict[str, Any], session_id: str | None = None) -> dict[str, Any]:
        del session_id
        self._client.calls.append(("Input.setInterceptDrags", dict(params)))
        self._client.intercept_enabled = bool(params.get("enabled"))
        return {}

    async def dispatchDragEvent(self, params: dict[str, Any], session_id: str | None = None) -> dict[str, Any]:
        del session_id
        self._client.calls.append(("Input.dispatchDragEvent", dict(params)))
        return {}

    async def dispatchKeyEvent(self, params: dict[str, Any], session_id: str | None = None) -> dict[str, Any]:
        del session_id
        self._client.calls.append(("Input.dispatchKeyEvent", dict(params)))
        return {}

    async def insertText(self, params: dict[str, Any], session_id: str | None = None) -> dict[str, Any]:
        del session_id
        self._client.calls.append(("Input.insertText", dict(params)))
        return {}


class _FakeRuntimeSend:
    def __init__(self, client: "_FakeCdpClient") -> None:
        self._client = client

    async def callFunctionOn(self, params: dict[str, Any], session_id: str | None = None) -> dict[str, Any]:
        del session_id
        self._client.calls.append(("Runtime.callFunctionOn", dict(params)))
        decl = str(params.get("functionDeclaration") or "")
        if "DragEvent" in decl:
            if self._client.js_drag_ok:
                return {"result": {"value": {"ok": True, "dropped": True}}}
            return {"result": {"value": {"ok": False, "error": "js drag no-op"}}}
        if "getBoundingClientRect" in decl:
            return {"result": {"value": {"x": 0, "y": 0, "width": 10, "height": 10}}}
        return {"result": {"value": None}}

    async def evaluate(self, params: dict[str, Any], session_id: str | None = None) -> dict[str, Any]:
        del session_id
        self._client.calls.append(("Runtime.evaluate", dict(params)))
        return {"result": {"value": None}}


class _FakeDomSend:
    def __init__(self, client: "_FakeCdpClient") -> None:
        self._client = client

    async def resolveNode(self, params: dict[str, Any], session_id: str | None = None) -> dict[str, Any]:
        del session_id
        backend = int(params.get("backendNodeId") or 0)
        self._client.calls.append(("DOM.resolveNode", dict(params)))
        return {"object": {"objectId": f"oid-{backend}"}}


class _FakeSend:
    def __init__(self, client: "_FakeCdpClient") -> None:
        self.Input = _FakeInputSend(client)
        self.Runtime = _FakeRuntimeSend(client)
        self.DOM = _FakeDomSend(client)


class _FakeEventRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, Any] = {}

    def register(self, method: str, callback: Any) -> None:
        self._handlers[method] = callback

    def unregister(self, method: str) -> None:
        self._handlers.pop(method, None)


class _FakeCdpClient:
    def __init__(self, *, auto_intercept_on_move: bool = False, js_drag_ok: bool = True) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.auto_intercept_on_move = auto_intercept_on_move
        self.js_drag_ok = js_drag_ok
        self.intercept_enabled = False
        self._intercept_emitted = False
        self._event_registry = _FakeEventRegistry()
        self.send = _FakeSend(self)
        self.register = SimpleNamespace(Input=SimpleNamespace(dragIntercepted=self._register_drag_intercepted))

    def _register_drag_intercepted(self, callback: Any) -> None:
        self._event_registry.register("Input.dragIntercepted", callback)

    def emit_drag_intercepted(self, data: dict[str, Any]) -> None:
        handler = self._event_registry._handlers.get("Input.dragIntercepted")
        if handler is not None:
            handler({"data": data}, "sess-1")


def _method_names(client: _FakeCdpClient) -> list[str]:
    return [name for name, _ in client.calls]


def test_perform_drag_html5_cdp_emits_intercept_then_drag_events() -> None:
    client = _FakeCdpClient(auto_intercept_on_move=True)
    result = _run(
        _cdp.perform_drag(
            client,
            "sess-1",
            sx=10.0,
            sy=10.0,
            tx=100.0,
            ty=100.0,
            steps=3,
            source_is_html5=True,
            source_backend_node_id=1,
            target_backend_node_id=2,
        )
    )
    assert result["ok"] is True
    assert result["mode"] == "html5_cdp"
    names = _method_names(client)
    assert "Input.setInterceptDrags" in names
    assert "Input.dispatchDragEvent" in names
    drag_types = [params["type"] for name, params in client.calls if name == "Input.dispatchDragEvent"]
    assert drag_types == ["dragEnter", "dragOver", "drop"]
    mouse_moves = [params for name, params in client.calls if name == "Input.dispatchMouseEvent" and params["type"] == "mouseMoved"]
    assert any(int(m.get("buttons") or 0) & 1 for m in mouse_moves)
    # intercept disabled in finally
    intercept_flags = [params.get("enabled") for name, params in client.calls if name == "Input.setInterceptDrags"]
    assert True in intercept_flags and False in intercept_flags


def test_perform_drag_html5_without_intercept_uses_js_fallback() -> None:
    client = _FakeCdpClient(auto_intercept_on_move=False, js_drag_ok=True)
    result = _run(
        _cdp.perform_drag(
            client,
            "sess-1",
            sx=1.0,
            sy=1.0,
            tx=50.0,
            ty=50.0,
            steps=2,
            source_is_html5=True,
            source_backend_node_id=11,
            target_backend_node_id=22,
        )
    )
    assert result["ok"] is True
    assert result["mode"] == "html5_js"
    assert any(name == "Runtime.callFunctionOn" for name, _ in client.calls)
    assert not any(name == "Input.dispatchDragEvent" for name, _ in client.calls)


def test_perform_drag_html5_no_intercept_js_noop_fails_closed() -> None:
    client = _FakeCdpClient(auto_intercept_on_move=False, js_drag_ok=False)
    result = _run(
        _cdp.perform_drag(
            client,
            "sess-1",
            sx=1.0,
            sy=1.0,
            tx=50.0,
            ty=50.0,
            steps=2,
            source_is_html5=True,
            source_backend_node_id=11,
            target_backend_node_id=22,
        )
    )
    assert result["ok"] is False
    assert result["mode"] == "html5_failed"
    assert "js drag" in str(result["detail"]).lower() or "no-op" in str(result["detail"]).lower() or "html5" in str(result["detail"]).lower()


def test_perform_drag_html5_missing_node_ids_fails_closed() -> None:
    client = _FakeCdpClient(auto_intercept_on_move=False)
    result = _run(
        _cdp.perform_drag(
            client,
            "sess-1",
            sx=1.0,
            sy=1.0,
            tx=50.0,
            ty=50.0,
            steps=1,
            source_is_html5=True,
            source_backend_node_id=None,
            target_backend_node_id=None,
        )
    )
    assert result["ok"] is False
    assert result["mode"] == "html5_failed"
    assert "backend node" in str(result["detail"]).lower()


def test_session_adapter_drag_raises_when_box_missing() -> None:
    session_adapter = _load_sidecar_module("session_adapter")
    exceptions = _load_sidecar_module("exceptions")
    adapter = session_adapter.SessionAdapter()
    adapter._safe_current_url = AsyncMock(return_value="https://example.test/")
    adapter._resolve_ref_to_backend_node_id = AsyncMock(side_effect=[1, 2])
    adapter._element_box = AsyncMock(return_value=None)

    with pytest.raises(exceptions.ElementNotFound, match="bounding box"):
        _run(
            adapter.drag(
                {"kind": "node", "backend_node_id": 1},
                {"kind": "node", "backend_node_id": 2},
            )
        )


def test_perform_drag_pointer_path_when_not_html5() -> None:
    client = _FakeCdpClient(auto_intercept_on_move=False)
    result = _run(
        _cdp.perform_drag(
            client,
            "sess-1",
            sx=0.0,
            sy=0.0,
            tx=20.0,
            ty=20.0,
            steps=2,
            source_is_html5=False,
        )
    )
    assert result["ok"] is True
    assert result["mode"] == "pointer"
    assert result["detail"] == "dragged"
    assert not any(name == "Input.dispatchDragEvent" for name, _ in client.calls)
