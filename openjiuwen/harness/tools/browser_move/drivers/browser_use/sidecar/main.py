# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Sidecar entrypoint: NDJSON read/dispatch/write loop over stdin/stdout.

Spawned by ``drivers/browser_use/transport.py`` as ``python <this file>``
(by path, not ``-m``, because ``openjiuwen`` is deliberately not installed
in the sidecar venv). Imports only stdlib plus the flat sibling modules in
this directory; ``session_adapter`` is the only one of those that reaches
into ``browser_use``.

Protocol: one JSON object per line, UTF-8, newline-delimited (wire.py). The
first line out is an unsolicited ``hello``. Every subsequent request gets
exactly one reply line (``ok`` or ``error``). ``cancel`` and ``shutdown`` are
control methods handled directly by this loop, not forwarded to the adapter.
"""

from __future__ import annotations

import asyncio
import json
import os
import platform
import queue
import sys
import threading
from typing import Any

import exceptions
import session_adapter
import wire


def _reconfigure_stdio() -> None:
    """Force UTF-8 with plain ``\\n`` newlines on both stdio streams."""
    for stream in (sys.stdin, sys.stdout):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8", newline="\n")


def _stdin_reader_thread(line_queue: queue.Queue) -> None:
    """Blocking readline loop run on a dedicated thread; feeds an async-visible queue."""
    try:
        for raw_line in sys.stdin:
            line_queue.put(raw_line)
    finally:
        line_queue.put(None)  # EOF sentinel


class _StdoutWriter:
    """Serializes NDJSON writes to stdout across concurrently-running request tasks."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()

    async def write(self, payload: dict[str, Any]) -> None:
        line = json.dumps(payload, ensure_ascii=False)
        async with self._lock:
            sys.stdout.write(line + "\n")
            sys.stdout.flush()


def _error_code_for(exc: Exception) -> str:
    name = type(exc).__name__
    return name if name in wire.ERROR_CODES else wire.DEFAULT_ERROR_CODE


def _error_data_for(exc: Exception) -> dict[str, Any] | None:
    if isinstance(exc, exceptions.EvaluateError):
        return {"js_message": exc.js_message, "js_stack": exc.js_stack}
    return None


async def _dispatch(
    adapter: session_adapter.SessionAdapter,
    writer: _StdoutWriter,
    request_id: int,
    method: str,
    params: dict[str, Any],
) -> None:
    if method not in wire.DRIVER_METHOD_NAMES:
        await writer.write(wire.make_failure(request_id, "DriverUnsupported", f"unknown method {method!r}"))
        return
    handler = getattr(adapter, method, None)
    if handler is None:
        await writer.write(wire.make_failure(request_id, "DriverUnsupported", f"method {method!r} not implemented"))
        return
    try:
        result = await handler(**params)
    except asyncio.CancelledError:
        await writer.write(wire.make_failure(request_id, "ActionTimeout", f"{method} cancelled"))
        raise
    except exceptions.DriverError as exc:
        await writer.write(wire.make_failure(request_id, _error_code_for(exc), str(exc), _error_data_for(exc)))
    except Exception as exc:  # noqa: BLE001 - any adapter failure must produce a wire reply, not a crash
        await writer.write(wire.make_failure(request_id, wire.DEFAULT_ERROR_CODE, str(exc)))
    else:
        await writer.write(wire.make_success(request_id, result))


async def _run() -> None:
    _reconfigure_stdio()

    adapter = session_adapter.SessionAdapter()
    writer = _StdoutWriter()
    await writer.write(
        wire.make_hello(
            browser_use_version=session_adapter.browser_use_version(),
            python_version=platform.python_version(),
            pid=os.getpid(),
        )
    )

    line_queue: queue.Queue = queue.Queue()
    reader_thread = threading.Thread(target=_stdin_reader_thread, args=(line_queue,), daemon=True)
    reader_thread.start()

    loop = asyncio.get_event_loop()
    active_tasks: dict[int, asyncio.Task] = {}

    while True:
        raw_line = await loop.run_in_executor(None, line_queue.get)
        if raw_line is None:
            break
        line = raw_line.strip()
        if not line:
            continue
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue

        method = message.get("method")
        request_id = message.get("id")

        if method == "cancel":
            target_id = (message.get("params") or {}).get("id")
            task = active_tasks.get(target_id)
            if task is not None:
                task.cancel()
            continue

        if method == "shutdown":
            for task in list(active_tasks.values()):
                task.cancel()
            if active_tasks:
                await asyncio.gather(*active_tasks.values(), return_exceptions=True)
            await adapter.close()
            if request_id is not None:
                await writer.write(wire.make_success(request_id, {}))
            break

        task = asyncio.ensure_future(_dispatch(adapter, writer, request_id, method, message.get("params") or {}))
        active_tasks[request_id] = task
        task.add_done_callback(lambda _t, rid=request_id: active_tasks.pop(rid, None))


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
