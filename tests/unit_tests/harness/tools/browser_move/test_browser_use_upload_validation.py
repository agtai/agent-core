#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Unit tests for browser_use driver upload path validation (fail closed)."""

from __future__ import annotations

import asyncio
import os
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from openjiuwen.harness.tools.browser_move.backends.contract.base import ActResult, SelectorRef
from openjiuwen.harness.tools.browser_move.backends.browser_use.driver import BrowserUseDriver
from openjiuwen.harness.tools.browser_move.runtime.runtime import BrowserAgentRuntime
from openjiuwen.harness.tools.browser_move.shared.upload_paths import resolve_upload_file_paths


def _run(coro):
    return asyncio.run(coro)


def _make_driver_with_transport(transport: AsyncMock) -> BrowserUseDriver:
    driver = BrowserUseDriver.__new__(BrowserUseDriver)
    driver._transport = transport
    driver._driver_generation = 7
    return driver


def test_upload_files_missing_path_returns_failed_act_result_without_sidecar() -> None:
    transport = AsyncMock()
    transport.request = AsyncMock()
    driver = _make_driver_with_transport(transport)
    missing = str(Path(tempfile.gettempdir()) / "openjiuwen_missing_upload_no_such_file.txt")

    result = _run(driver.upload_files(SelectorRef(css="input[type=file]"), [missing]))

    assert isinstance(result, ActResult)
    assert result.ok is False
    assert "not found" in result.detail.lower()
    assert "not readable" not in result.detail.lower()
    assert "openjiuwen_missing_upload_no_such_file.txt" in result.detail
    assert "BROWSER_UPLOAD_ROOT" in result.detail
    transport.request.assert_not_called()


def test_upload_files_existing_path_calls_sidecar_with_resolved_absolute() -> None:
    transport = AsyncMock()
    transport.request = AsyncMock(
        return_value={
            "ok": True,
            "detail": "uploaded 1 file(s)",
            "document_changed": False,
            "driver_generation": 8,
        }
    )
    driver = _make_driver_with_transport(transport)

    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as handle:
        handle.write(b"hello")
        existing = handle.name
    try:
        parent = Path(existing).parent
        rel = Path(existing).name
        old_cwd = Path.cwd()
        try:
            os.chdir(parent)
            result = _run(driver.upload_files(SelectorRef(css="input[type=file]"), [rel]))
        finally:
            os.chdir(old_cwd)
    finally:
        Path(existing).unlink(missing_ok=True)

    assert result.ok is True
    transport.request.assert_called_once()
    method, params = transport.request.call_args.args
    assert method == "upload_files"
    assert params["paths"] == [str(Path(existing).resolve())]


def test_upload_files_tilde_path_is_expanded_before_sidecar() -> None:
    transport = AsyncMock()
    transport.request = AsyncMock(
        return_value={
            "ok": True,
            "detail": "uploaded 1 file(s)",
            "document_changed": False,
            "driver_generation": 8,
        }
    )
    driver = _make_driver_with_transport(transport)

    home = Path.home()
    with tempfile.NamedTemporaryFile(suffix=".txt", dir=home, delete=False) as handle:
        handle.write(b"home-upload")
        existing = Path(handle.name)
    try:
        tilde_path = f"~/{existing.name}"
        result = _run(driver.upload_files(SelectorRef(css="input[type=file]"), [tilde_path]))
    finally:
        existing.unlink(missing_ok=True)

    assert result.ok is True
    method, params = transport.request.call_args.args
    assert method == "upload_files"
    assert params["paths"] == [str(existing.resolve())]
    assert "~" not in params["paths"][0]


def test_upload_files_strips_wrapping_quotes_before_is_file() -> None:
    transport = AsyncMock()
    transport.request = AsyncMock(
        return_value={
            "ok": True,
            "detail": "uploaded 1 file(s)",
            "document_changed": False,
            "driver_generation": 8,
        }
    )
    driver = _make_driver_with_transport(transport)

    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as handle:
        handle.write(b"quoted")
        existing = handle.name
    try:
        quoted = f"'{existing}'"
        result = _run(driver.upload_files(SelectorRef(css="input[type=file]"), [quoted]))
    finally:
        Path(existing).unlink(missing_ok=True)

    assert result.ok is True
    method, params = transport.request.call_args.args
    assert params["paths"] == [str(Path(existing).resolve())]


def test_resolve_upload_file_paths_uses_browser_upload_root_for_basename() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        target = root / "staged_upload.txt"
        target.write_text("staged", encoding="utf-8")
        with patch(
            "openjiuwen.harness.tools.browser_move.shared.upload_paths.resolve_upload_root",
            return_value=root.resolve(),
        ):
            existing, err = resolve_upload_file_paths(["staged_upload.txt"])

    assert err is None
    assert existing == [str(target.resolve())]


def test_runtime_file_upload_missing_path_fails_closed() -> None:
    runtime = BrowserAgentRuntime.__new__(BrowserAgentRuntime)
    runtime.ensure_runtime_ready = AsyncMock()
    runtime.export_page_state = MagicMock(return_value={})
    runtime._ensure_browser_driver = AsyncMock()
    runtime._resolve_catalog_element_ref = AsyncMock()
    missing = str(Path(tempfile.gettempdir()) / "openjiuwen_missing_runtime_upload.txt")

    result = _run(
        BrowserAgentRuntime.file_upload(
            runtime,
            generation_id="g1",
            paths=[missing],
            selector="input[type=file]",
        )
    )

    assert result["ok"] is False
    assert "not found" in str(result.get("error") or "").lower()
    assert "not readable" not in str(result.get("error") or "").lower()
    assert "openjiuwen_missing_runtime_upload.txt" in str(result.get("error") or "")
    runtime._ensure_browser_driver.assert_not_called()
    runtime._resolve_catalog_element_ref.assert_not_called()


def test_runtime_file_upload_normalizes_existing_path_before_driver() -> None:
    runtime = BrowserAgentRuntime.__new__(BrowserAgentRuntime)
    runtime.ensure_runtime_ready = AsyncMock()
    runtime.export_page_state = MagicMock(return_value={})
    runtime._resolve_catalog_element_ref = AsyncMock(return_value=SelectorRef(css="input[type=file]"))
    runtime._apply_document_changed = MagicMock()
    driver = AsyncMock()
    driver.upload_files = AsyncMock(
        return_value=ActResult(ok=True, detail="uploaded 1 file(s)", document_changed=False, driver_generation=1)
    )
    runtime._ensure_browser_driver = AsyncMock(return_value=driver)

    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as handle:
        handle.write(b"runtime")
        existing = handle.name
    try:
        parent = Path(existing).parent
        rel = Path(existing).name
        old_cwd = Path.cwd()
        try:
            os.chdir(parent)
            result = _run(
                BrowserAgentRuntime.file_upload(
                    runtime,
                    generation_id="g1",
                    paths=[rel],
                    selector="input[type=file]",
                )
            )
        finally:
            os.chdir(old_cwd)
    finally:
        Path(existing).unlink(missing_ok=True)

    assert result["ok"] is True
    uploaded_paths = driver.upload_files.call_args.args[1]
    assert list(uploaded_paths) == [str(Path(existing).resolve())]
