# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""NDJSON-over-stdio transport to the browser-use sidecar process.

Owns interpreter discovery, process spawn, the hello handshake, one
in-flight request at a time, cancellation/timeout, and wire error-code to
exception mapping. ``drivers/browser_use/driver.py`` is the only caller;
this module never imports ``browser_use`` itself, keeping the agent-core
process free of the sidecar's dependency closure.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import platform
from pathlib import Path
from typing import Any

from openjiuwen.harness.tools.browser_move.drivers import errors as driver_errors
from openjiuwen.harness.tools.browser_move.drivers.browser_use.sidecar import wire
from openjiuwen.harness.tools.browser_move.playwright_runtime.browser_logging import get_browser_agent_logger

_logger = get_browser_agent_logger()

_SIDECAR_VENV_DIR = ".venvs/browser-use"
_REQUEST_TIMEOUT_S = 30.0
_SHUTDOWN_GRACE_S = 5.0

_ERROR_CODE_TO_EXCEPTION: dict[str, type[driver_errors.DriverError]] = {
    "DriverError": driver_errors.DriverError,
    "DriverNotConnected": driver_errors.DriverNotConnected,
    "DriverConnectionError": driver_errors.DriverConnectionError,
    "DriverConnectionLost": driver_errors.DriverConnectionLost,
    "ElementNotFound": driver_errors.ElementNotFound,
    "AmbiguousElement": driver_errors.AmbiguousElement,
    "StaleIndexError": driver_errors.StaleIndexError,
    "StaleNodeError": driver_errors.StaleNodeError,
    "ActionTimeout": driver_errors.ActionTimeout,
    "NavigationError": driver_errors.NavigationError,
    "ObserveError": driver_errors.ObserveError,
    "EvaluateError": driver_errors.EvaluateError,
    "DriverUnsupported": driver_errors.DriverUnsupported,
}


def _sidecar_dir() -> Path:
    return Path(__file__).resolve().parent / "sidecar"


def _default_venv_python() -> Path:
    repo_root = Path(__file__).resolve().parents[6]
    if platform.system() == "Windows":
        return repo_root / _SIDECAR_VENV_DIR / "Scripts" / "python.exe"
    return repo_root / _SIDECAR_VENV_DIR / "bin" / "python"


def discover_sidecar_python() -> Path:
    """Resolve the sidecar interpreter path, or raise ``DriverUnsupported`` with a creation recipe."""
    override = os.getenv("BROWSER_USE_SIDECAR_PYTHON")
    if override:
        candidate = Path(override)
        if candidate.is_file():
            return candidate
        raise driver_errors.DriverUnsupported(
            f"BROWSER_USE_SIDECAR_PYTHON={override!r} does not point at an existing interpreter"
        )

    candidate = _default_venv_python()
    if candidate.is_file():
        return candidate

    raise driver_errors.DriverUnsupported(
        "No browser-use sidecar interpreter found. Create one with:\n"
        "  uv venv --python 3.12 .venvs/browser-use\n"
        '  uv pip install --python .venvs/browser-use "browser-use==0.13.10"\n'
        "or set BROWSER_USE_SIDECAR_PYTHON to an existing interpreter."
    )


def _sidecar_env() -> dict[str, str]:
    """A minimal environment for the sidecar: no LLM credentials, ever."""
    env: dict[str, str] = {}
    for key in ("PATH",):
        value = os.environ.get(key)
        if value is not None:
            env[key] = value
    if platform.system() == "Windows":
        for key in ("SystemRoot", "TEMP", "TMP", "USERPROFILE", "HOMEDRIVE", "HOMEPATH", "LOCALAPPDATA", "APPDATA"):
            value = os.environ.get(key)
            if value is not None:
                env[key] = value
    else:
        for key in ("HOME", "TMPDIR"):
            value = os.environ.get(key)
            if value is not None:
                env[key] = value
    for key, value in os.environ.items():
        if key.startswith("BROWSER_USE_") and key != "BROWSER_USE_SIDECAR_PYTHON":
            env[key] = value
    # Same catalog root as list_upload_files / runtime path resolution.
    upload_root = os.environ.get("BROWSER_UPLOAD_ROOT")
    if upload_root is not None and str(upload_root).strip():
        env["BROWSER_UPLOAD_ROOT"] = str(upload_root).strip()
    env.setdefault("ANONYMIZED_TELEMETRY", "false")
    return env


class SidecarTransport:
    """One live sidecar process plus its NDJSON request/response cycle."""

    def __init__(self) -> None:
        self._process: asyncio.subprocess.Process | None = None
        self._next_id = 1
        self._lock = asyncio.Lock()
        self._hello: dict[str, Any] | None = None
        self._closed = False

    @property
    def hello(self) -> dict[str, Any] | None:
        return self._hello

    async def start(self, *, timeout_s: float = 30.0) -> dict[str, Any]:
        """Spawn the sidecar and read its hello line."""
        python_path = discover_sidecar_python()
        main_path = _sidecar_dir() / "main.py"
        # asyncio StreamReader defaults to a 64 KiB line limit; screenshot
        # NDJSON replies routinely exceed that. Align with wire.MAX_LINE_BYTES.
        self._process = await asyncio.create_subprocess_exec(
            str(python_path),
            str(main_path),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=_sidecar_env(),
            limit=wire.MAX_LINE_BYTES,
        )
        asyncio.ensure_future(self._drain_stderr())

        assert self._process.stdout is not None
        try:
            raw_line = await asyncio.wait_for(self._readline_stdout(), timeout=timeout_s)
        except asyncio.TimeoutError as exc:
            await self._terminate()
            raise driver_errors.DriverConnectionError("sidecar did not send hello within timeout") from exc
        except driver_errors.DriverConnectionLost as exc:
            await self._terminate()
            raise driver_errors.DriverConnectionError(str(exc)) from exc

        if not raw_line:
            await self._terminate()
            raise driver_errors.DriverConnectionError("sidecar exited before sending hello")

        try:
            message = json.loads(raw_line.decode("utf-8").strip())
        except json.JSONDecodeError as exc:
            await self._terminate()
            raise driver_errors.DriverConnectionError(f"sidecar hello line is not valid JSON: {raw_line!r}") from exc

        if message.get("kind") != "hello":
            await self._terminate()
            raise driver_errors.DriverConnectionError(f"expected hello line, got: {message!r}")

        if message.get("wire") != wire.WIRE_VERSION:
            await self._terminate()
            raise driver_errors.DriverConnectionError(
                f"sidecar wire version {message.get('wire')!r} != expected {wire.WIRE_VERSION!r}"
            )

        self._hello = message
        return message

    async def request(self, method: str, params: dict[str, Any] | None = None, *, timeout_s: float | None = None) -> Any:
        """Send one request, await its matching reply. One in-flight request at a time."""
        if self._process is None or self._process.stdin is None or self._process.stdout is None:
            raise driver_errors.DriverNotConnected("sidecar transport has not been started")

        async with self._lock:
            request_id = self._next_id
            self._next_id += 1
            payload = wire.make_request(request_id, method, params)
            line = json.dumps(payload, ensure_ascii=False) + "\n"

            try:
                self._process.stdin.write(line.encode("utf-8"))
                await self._process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError) as exc:
                raise driver_errors.DriverConnectionLost("sidecar stdin pipe is closed") from exc

            effective_timeout = timeout_s if timeout_s is not None else _REQUEST_TIMEOUT_S
            try:
                reply = await asyncio.wait_for(self._read_reply(request_id), timeout=effective_timeout)
            except asyncio.TimeoutError:
                await self._cancel(request_id)
                raise driver_errors.ActionTimeout(f"{method} timed out after {effective_timeout}s") from None

            return self._unwrap_reply(reply)

    async def _readline_stdout(self) -> bytes:
        """Read one stdout line, mapping StreamReader oversize to DriverConnectionLost.

        ``asyncio.StreamReader.readline`` fails when a separator is found but the
        chunk exceeds the reader limit (classic message: ``"Separator is found,
        but chunk is longer than limit"``). On Python 3.11+ that surfaces as
        ``ValueError`` wrapping ``LimitOverrunError``; older paths may raise
        ``LimitOverrunError`` directly. Map both to the wire max-line contract
        so large base64 screenshot NDJSON replies become a clear driver error
        instead of a raw asyncio failure.
        """
        assert self._process is not None and self._process.stdout is not None
        try:
            raw_line = await self._process.stdout.readline()
        except asyncio.LimitOverrunError as exc:
            raise driver_errors.DriverConnectionLost(
                "sidecar emitted a line exceeding the max line size"
            ) from exc
        except ValueError as exc:
            if "chunk is longer than limit" not in str(exc):
                raise
            raise driver_errors.DriverConnectionLost(
                "sidecar emitted a line exceeding the max line size"
            ) from exc
        if raw_line and len(raw_line) > wire.MAX_LINE_BYTES:
            raise driver_errors.DriverConnectionLost("sidecar emitted a line exceeding the max line size")
        return raw_line

    async def _read_reply(self, request_id: int) -> dict[str, Any]:
        """Read lines until the reply for ``request_id`` arrives, forwarding log lines as we go."""
        assert self._process is not None and self._process.stdout is not None
        while True:
            raw_line = await self._readline_stdout()
            if not raw_line:
                raise driver_errors.DriverConnectionLost("sidecar closed stdout unexpectedly")
            try:
                message = json.loads(raw_line.decode("utf-8").strip())
            except json.JSONDecodeError:
                _logger.warning("browser-use sidecar emitted a non-JSON line: %r", raw_line[:200])
                continue

            if message.get("kind") == "log":
                self._forward_log(message)
                continue
            if message.get("kind") == "hello":
                continue
            if message.get("id") == request_id:
                return message
            # A reply for a different (already-timed-out/cancelled) id; drop it.

    async def _cancel(self, request_id: int) -> None:
        if self._process is None or self._process.stdin is None:
            return
        try:
            payload = wire.make_request(0, "cancel", {"id": request_id})
            self._process.stdin.write((json.dumps(payload) + "\n").encode("utf-8"))
            await self._process.stdin.drain()
        except (BrokenPipeError, ConnectionResetError):
            pass

    @staticmethod
    def _unwrap_reply(message: dict[str, Any]) -> Any:
        if "ok" in message:
            return message["ok"]
        error = message.get("error") or {}
        code = str(error.get("code", wire.DEFAULT_ERROR_CODE))
        exc_class = _ERROR_CODE_TO_EXCEPTION.get(code, driver_errors.DriverError)
        message_text = str(error.get("message", "sidecar error"))
        data = error.get("data") or {}
        if exc_class is driver_errors.EvaluateError:
            raise driver_errors.EvaluateError(
                message_text, js_message=str(data.get("js_message", "")), js_stack=str(data.get("js_stack", ""))
            )
        raise exc_class(message_text)

    def _forward_log(self, message: dict[str, Any]) -> None:
        level_name = str(message.get("level", "info")).upper()
        text = str(message.get("message", ""))
        if level_name == "ERROR":
            _logger.error("browser-use sidecar: %s", text)
        elif level_name == "WARNING":
            _logger.warning("browser-use sidecar: %s", text)
        else:
            _logger.info("browser-use sidecar: %s", text)

    async def _drain_stderr(self) -> None:
        if self._process is None or self._process.stderr is None:
            return
        while True:
            raw_line = await self._process.stderr.readline()
            if not raw_line:
                break
            text = raw_line.decode("utf-8", errors="replace").rstrip()
            if text:
                _logger.warning("browser-use sidecar stderr: %s", text)

    async def close(self) -> None:
        """Ask the sidecar to shut down, then terminate it if it does not exit in time."""
        if self._closed:
            return
        self._closed = True
        if self._process is None:
            return
        try:
            await self.request("shutdown", {}, timeout_s=_SHUTDOWN_GRACE_S)
        except driver_errors.DriverError:
            pass
        await self._terminate()

    async def _terminate(self) -> None:
        if self._process is None:
            return
        if self._process.returncode is None:
            try:
                self._process.terminate()
                await asyncio.wait_for(self._process.wait(), timeout=_SHUTDOWN_GRACE_S)
            except asyncio.TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    self._process.kill()
        self._process = None


__all__ = ["SidecarTransport", "discover_sidecar_python"]
