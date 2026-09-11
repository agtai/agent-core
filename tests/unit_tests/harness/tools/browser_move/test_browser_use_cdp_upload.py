#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Unit tests for sidecar CDP file-upload helpers (stdlib + duck-typed fake client)."""

from __future__ import annotations

import asyncio
import importlib.util
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

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
_session_adapter = _load_sidecar_module("session_adapter")


def _run(coro):
    return asyncio.run(coro)


class _FakeDomSend:
    def __init__(self, client: "_FakeCdpClient") -> None:
        self._client = client

    async def setFileInputFiles(self, params: dict[str, Any], session_id: str | None = None) -> dict[str, Any]:
        del session_id
        self._client.calls.append(("DOM.setFileInputFiles", dict(params)))
        files = list(params.get("files") or [])
        if self._client.attach_ok:
            self._client.attached_files = files
        else:
            self._client.attached_files = []
        return {}

    async def resolveNode(self, params: dict[str, Any], session_id: str | None = None) -> dict[str, Any]:
        del session_id
        backend = int(params.get("backendNodeId") or 0)
        self._client.calls.append(("DOM.resolveNode", dict(params)))
        return {"object": {"objectId": f"oid-{backend}"}}


class _FakeRuntimeSend:
    def __init__(self, client: "_FakeCdpClient") -> None:
        self._client = client

    async def callFunctionOn(self, params: dict[str, Any], session_id: str | None = None) -> dict[str, Any]:
        del session_id
        self._client.calls.append(("Runtime.callFunctionOn", dict(params)))
        decl = str(params.get("functionDeclaration") or "")
        if "isFile" in decl:
            return {
                "result": {
                    "value": {
                        "isFile": self._client.is_file_input,
                        "multiple": self._client.multiple,
                        "files": len(self._client.attached_files),
                    }
                }
            }
        if "this.files" in decl:
            return {"result": {"value": len(self._client.attached_files)}}
        return {"result": {"value": None}}


class _FakeSend:
    def __init__(self, client: "_FakeCdpClient") -> None:
        self.DOM = _FakeDomSend(client)
        self.Runtime = _FakeRuntimeSend(client)


class _FakeCdpClient:
    def __init__(
        self,
        *,
        attach_ok: bool = True,
        is_file_input: bool = True,
        multiple: bool = False,
    ) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.attach_ok = attach_ok
        self.is_file_input = is_file_input
        self.multiple = multiple
        self.attached_files: list[str] = []
        self.send = _FakeSend(self)


def test_set_file_input_files_uses_backend_node_id() -> None:
    client = _FakeCdpClient()
    _run(_cdp.set_file_input_files(client, "sess-1", 42, ["/abs/a.txt", "/abs/b.txt"]))
    assert client.calls == [
        (
            "DOM.setFileInputFiles",
            {"files": ["/abs/a.txt", "/abs/b.txt"], "backendNodeId": 42},
        )
    ]


def test_session_adapter_upload_files_calls_set_file_input_files_and_verifies() -> None:
    adapter = _session_adapter.SessionAdapter()
    client = _FakeCdpClient(attach_ok=True, is_file_input=True, multiple=False)
    adapter._cdp_session = SimpleNamespace(cdp_client=client, session_id="s1")
    adapter._session = SimpleNamespace(get_current_page_url=AsyncMock(return_value="https://example.test/upload"))
    adapter._resolve_ref_to_backend_node_id = AsyncMock(return_value=99)
    adapter._ensure_cdp_session = AsyncMock(return_value=adapter._cdp_session)

    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as handle:
        handle.write(b"payload")
        path = handle.name
    try:
        result = _run(adapter.upload_files({"kind": "node", "backend_node_id": 99}, [path]))
    finally:
        Path(path).unlink(missing_ok=True)

    assert result["ok"] is True
    assert result["detail"] == "uploaded 1 file(s)"
    set_calls = [c for c in client.calls if c[0] == "DOM.setFileInputFiles"]
    assert len(set_calls) == 1
    assert set_calls[0][1]["backendNodeId"] == 99
    assert set_calls[0][1]["files"] == [str(Path(path).resolve())]


def test_session_adapter_upload_files_fails_closed_when_attach_leaves_empty_files() -> None:
    adapter = _session_adapter.SessionAdapter()
    client = _FakeCdpClient(attach_ok=False, is_file_input=True, multiple=False)
    adapter._cdp_session = SimpleNamespace(cdp_client=client, session_id="s1")
    adapter._session = SimpleNamespace(get_current_page_url=AsyncMock(return_value="https://example.test/upload"))
    adapter._resolve_ref_to_backend_node_id = AsyncMock(return_value=7)
    adapter._ensure_cdp_session = AsyncMock(return_value=adapter._cdp_session)

    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as handle:
        handle.write(b"payload")
        path = handle.name
    try:
        result = _run(adapter.upload_files({"kind": "node", "backend_node_id": 7}, [path]))
    finally:
        Path(path).unlink(missing_ok=True)

    assert result["ok"] is False
    assert "no files after attach" in result["detail"]
    assert any(c[0] == "DOM.setFileInputFiles" for c in client.calls)


def test_session_adapter_upload_files_rejects_multi_on_non_multiple_input() -> None:
    adapter = _session_adapter.SessionAdapter()
    client = _FakeCdpClient(attach_ok=True, is_file_input=True, multiple=False)
    adapter._cdp_session = SimpleNamespace(cdp_client=client, session_id="s1")
    adapter._session = SimpleNamespace(get_current_page_url=AsyncMock(return_value="https://example.test/upload"))
    adapter._resolve_ref_to_backend_node_id = AsyncMock(return_value=7)
    adapter._ensure_cdp_session = AsyncMock(return_value=adapter._cdp_session)

    paths: list[str] = []
    try:
        for i in range(2):
            handle = tempfile.NamedTemporaryFile(suffix=f"_{i}.txt", delete=False)
            handle.write(b"x")
            handle.close()
            paths.append(handle.name)
        result = _run(adapter.upload_files({"kind": "node", "backend_node_id": 7}, paths))
    finally:
        for p in paths:
            Path(p).unlink(missing_ok=True)

    assert result["ok"] is False
    assert "does not accept multiple files" in result["detail"]
    assert not any(c[0] == "DOM.setFileInputFiles" for c in client.calls)


def test_session_adapter_upload_files_missing_path_skips_cdp() -> None:
    adapter = _session_adapter.SessionAdapter()
    client = _FakeCdpClient()
    adapter._cdp_session = SimpleNamespace(cdp_client=client, session_id="s1")
    adapter._session = SimpleNamespace(get_current_page_url=AsyncMock(return_value="https://example.test/upload"))
    adapter._resolve_ref_to_backend_node_id = AsyncMock(return_value=7)
    adapter._ensure_cdp_session = AsyncMock(return_value=adapter._cdp_session)
    missing = str(Path(tempfile.gettempdir()) / "openjiuwen_sidecar_missing_upload.txt")

    result = _run(adapter.upload_files({"kind": "node", "backend_node_id": 7}, [missing]))

    assert result["ok"] is False
    assert "not found" in result["detail"].lower()
    assert "not readable" not in result["detail"].lower()
    adapter._resolve_ref_to_backend_node_id.assert_not_called()
    assert client.calls == []


def test_session_adapter_upload_files_strips_quotes_and_resolves_absolute() -> None:
    adapter = _session_adapter.SessionAdapter()
    client = _FakeCdpClient(attach_ok=True, is_file_input=True, multiple=False)
    adapter._cdp_session = SimpleNamespace(cdp_client=client, session_id="s1")
    adapter._session = SimpleNamespace(get_current_page_url=AsyncMock(return_value="https://example.test/upload"))
    adapter._resolve_ref_to_backend_node_id = AsyncMock(return_value=11)
    adapter._ensure_cdp_session = AsyncMock(return_value=adapter._cdp_session)

    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as handle:
        handle.write(b"payload")
        path = handle.name
    try:
        result = _run(adapter.upload_files({"kind": "node", "backend_node_id": 11}, [f'"{path}"']))
    finally:
        Path(path).unlink(missing_ok=True)

    assert result["ok"] is True
    set_calls = [c for c in client.calls if c[0] == "DOM.setFileInputFiles"]
    assert len(set_calls) == 1
    assert set_calls[0][1]["files"] == [str(Path(path).resolve())]
