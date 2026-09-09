#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Unit tests for browser-use sidecar NDJSON transport line limits."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from openjiuwen.harness.tools.browser_move.drivers import errors as driver_errors
from openjiuwen.harness.tools.browser_move.drivers.browser_use.sidecar import wire
from openjiuwen.harness.tools.browser_move.drivers.browser_use.transport import SidecarTransport

# asyncio StreamReader default; screenshot NDJSON replies routinely exceed this.
_DEFAULT_STREAM_LIMIT = 64 * 1024
# Representative viewport screenshot base64 payload size (above default, below wire max).
_SCREENSHOT_SIZED_B64_LEN = 70 * 1024


def _screenshot_success_line(*, request_id: int = 1, b64_len: int = _SCREENSHOT_SIZED_B64_LEN) -> bytes:
    b64 = "A" * b64_len
    payload = wire.make_success(request_id, {"screenshot_b64": b64, "full_page": False})
    return (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")


def _feed_reader(payload: bytes, *, limit: int) -> asyncio.StreamReader:
    reader = asyncio.StreamReader(limit=limit)
    reader.feed_data(payload)
    reader.feed_eof()
    return reader


def _transport_with_stdout(reader: asyncio.StreamReader) -> SidecarTransport:
    transport = SidecarTransport()
    transport._process = SimpleNamespace(stdout=reader, stdin=None, stderr=None, returncode=None)
    return transport


@pytest.mark.asyncio
@pytest.mark.level0
async def test_default_stream_limit_cannot_read_screenshot_sized_line() -> None:
    """Document the pre-fix failure mode: default 64 KiB readline limit.

    Python 3.11+ ``StreamReader.readline`` wraps ``LimitOverrunError`` as
    ``ValueError`` with the same message text.
    """
    line = _screenshot_success_line()
    assert len(line) > _DEFAULT_STREAM_LIMIT
    reader = _feed_reader(line, limit=_DEFAULT_STREAM_LIMIT)
    with pytest.raises((asyncio.LimitOverrunError, ValueError), match="chunk is longer than limit"):
        await reader.readline()


@pytest.mark.asyncio
@pytest.mark.level0
async def test_read_reply_accepts_screenshot_sized_ndjson_line() -> None:
    line = _screenshot_success_line()
    assert _DEFAULT_STREAM_LIMIT < len(line) < wire.MAX_LINE_BYTES
    transport = _transport_with_stdout(_feed_reader(line, limit=wire.MAX_LINE_BYTES))

    reply = await transport._read_reply(1)

    assert reply["id"] == 1
    assert reply["ok"]["full_page"] is False
    assert len(reply["ok"]["screenshot_b64"]) == _SCREENSHOT_SIZED_B64_LEN


@pytest.mark.asyncio
@pytest.mark.level0
async def test_read_reply_maps_limit_overrun_to_driver_connection_lost() -> None:
    # Force StreamReader oversize: line content exceeds the raised wire limit.
    oversized = _screenshot_success_line(b64_len=wire.MAX_LINE_BYTES + 1024)
    assert len(oversized) > wire.MAX_LINE_BYTES
    transport = _transport_with_stdout(_feed_reader(oversized, limit=wire.MAX_LINE_BYTES))

    with pytest.raises(driver_errors.DriverConnectionLost, match="max line size"):
        await transport._read_reply(1)


@pytest.mark.asyncio
@pytest.mark.level0
async def test_read_reply_rejects_line_longer_than_max_after_read() -> None:
    """Defense-in-depth when a reader returns an oversize chunk without LimitOverrunError."""
    oversized = _screenshot_success_line(b64_len=wire.MAX_LINE_BYTES + 64)
    # Use a higher reader limit so readline succeeds; transport must still reject.
    transport = _transport_with_stdout(_feed_reader(oversized, limit=wire.MAX_LINE_BYTES * 2))

    with pytest.raises(driver_errors.DriverConnectionLost, match="max line size"):
        await transport._read_reply(1)


@pytest.mark.asyncio
@pytest.mark.level0
async def test_request_returns_screenshot_payload_from_large_ok_line() -> None:
    line = _screenshot_success_line(request_id=1)
    stdin = AsyncMock()
    stdin.write = lambda _data: None
    stdin.drain = AsyncMock()
    transport = _transport_with_stdout(_feed_reader(line, limit=wire.MAX_LINE_BYTES))
    assert transport._process is not None
    transport._process.stdin = stdin

    result = await transport.request("screenshot", {"full_page": False})

    assert result["full_page"] is False
    assert len(result["screenshot_b64"]) == _SCREENSHOT_SIZED_B64_LEN
    stdin.drain.assert_awaited()


@pytest.mark.asyncio
@pytest.mark.level0
async def test_start_passes_max_line_bytes_as_stream_limit() -> None:
    hello = wire.make_hello(browser_use_version="0.13.10", python_version="3.12.0", pid=1)
    hello_line = (json.dumps(hello) + "\n").encode("utf-8")
    stdout = _feed_reader(hello_line, limit=wire.MAX_LINE_BYTES)
    stderr = _feed_reader(b"", limit=wire.MAX_LINE_BYTES)
    fake_proc = SimpleNamespace(
        stdout=stdout,
        stderr=stderr,
        stdin=AsyncMock(),
        returncode=None,
        terminate=lambda: None,
        kill=lambda: None,
        wait=AsyncMock(return_value=0),
    )

    with (
        patch(
            "openjiuwen.harness.tools.browser_move.drivers.browser_use.transport.discover_sidecar_python",
            return_value=Path("/fake/browser-use-python"),
        ),
        patch(
            "openjiuwen.harness.tools.browser_move.drivers.browser_use.transport.asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
            return_value=fake_proc,
        ) as mock_exec,
        patch(
            "openjiuwen.harness.tools.browser_move.drivers.browser_use.transport.asyncio.ensure_future",
            side_effect=lambda coro: (coro.close(), None)[1],
        ),
    ):
        transport = SidecarTransport()
        message = await transport.start(timeout_s=1.0)

    assert message["kind"] == "hello"
    assert mock_exec.await_args is not None
    assert mock_exec.await_args.kwargs["limit"] == wire.MAX_LINE_BYTES
