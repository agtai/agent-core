# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Local mock ERP: static order pages + a live claim form API.

Run from this file:

    python3 erp_server.py [port]

Or via web_mocks/start-web.sh. Bind is 127.0.0.1 only.
"""

from __future__ import annotations

import json
import sys
import threading
import time
from datetime import date
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

WEB_DIR = Path(__file__).resolve().parent / "web_mocks"
DEFAULT_PORT = 8765
HOST = "127.0.0.1"

_LOCK = threading.Lock()
_STATE: dict[str, Any] = {}


def empty_state() -> dict[str, Any]:
    return {
        "status": "empty",
        "message": "Waiting for the agent to fill this claim.",
        "title": "",
        "actor": "",
        "cost_center": "",
        "date_start": "",
        "date_end": "",
        "lines": [],
        "totals": {},
        "claim_id": "",
        "recalled": [],
        "findings": [],
        "policy": {},
        "case_id": "",
        "sink": "expense",
    }


def reset_state() -> dict[str, Any]:
    with _LOCK:
        _STATE.clear()
        _STATE.update(empty_state())
        return dict(_STATE)


def get_state() -> dict[str, Any]:
    with _LOCK:
        if not _STATE:
            _STATE.update(empty_state())
        return dict(_STATE)


def _as_amount(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def _looks_like_taxi(description: str) -> bool:
    text = (description or "").lower()
    skip = ("air france", "flight", "train", "metro", "subway", "法航", "火车", "地铁", "航班")
    if any(k in text for k in skip):
        return False
    keys = ("taxi", "uber", "cab", "airport", "cdg", "icn", "→", "->", "打车", "出租", "机场")
    return any(k in text for k in keys)


def _span_nights(start: str, end: str) -> int:
    try:
        return max((date.fromisoformat(end) - date.fromisoformat(start)).days, 0)
    except ValueError:
        return 0


def _recompute(state: dict[str, Any]) -> None:
    lines = list(state.get("lines") or [])
    totals: dict[str, float] = {}
    for line in lines:
        amount = _as_amount(line.get("amount"))
        line["amount"] = amount
        currency = str(line.get("currency") or "").strip()
        if amount is not None and currency:
            totals[currency] = round(totals.get(currency, 0.0) + amount, 2)
        line["category"] = str(line.get("category") or "").strip()
        line["description"] = str(line.get("description") or "")
        line["date"] = str(line.get("date") or "")
        line["evidence"] = str(line.get("evidence") or "")
        line["source"] = str(line.get("source") or "manual")
        line["line_id"] = str(line.get("line_id") or line.get("evidence") or "")
    state["lines"] = lines
    state["totals"] = totals
    state["findings"] = _findings(state)


def _findings(state: dict[str, Any]) -> list[dict[str, str]]:
    policy = state.get("policy") or {}
    if policy.get("enabled") is False:
        return []
    findings: list[dict[str, str]] = []
    lines = state.get("lines") or []
    denied = policy.get("denied_sources") or []
    for src in denied:
        findings.append(
            {
                "kind": "missing",
                "message": (
                    f"You denied «{src}» access; related receipts may be missing "
                    "— re-open the source or upload manually."
                ),
            }
        )
    categories = {(line.get("category") or "").strip() for line in lines}
    for required in policy.get("require_categories") or []:
        if required not in categories:
            findings.append(
                {
                    "kind": "missing",
                    "message": f"Missing item: no «{required}» category evidence in the draft.",
                }
            )
    meal_cats = set(policy.get("meal_categories") or ["meal"])
    taxi_cats = set(policy.get("taxi_categories") or ["transport"])
    max_meal = policy.get("max_meal_amount")
    max_taxi = policy.get("max_taxi_amount")
    for line in lines:
        cat = (line.get("category") or "").strip()
        amount = line.get("amount")
        if amount is None:
            continue
        desc = str(line.get("description") or "")
        currency = str(line.get("currency") or "")
        if max_meal is not None and cat in meal_cats and float(amount) > float(max_meal):
            findings.append(
                {
                    "kind": "policy",
                    "message": (
                        f"Over policy: meal «{desc}» is {float(amount):.2f} {currency}, "
                        f"above the per-meal cap {float(max_meal):.2f}."
                    ),
                }
            )
        if (
            max_taxi is not None
            and cat in taxi_cats
            and _looks_like_taxi(desc)
            and float(amount) > float(max_taxi)
        ):
            findings.append(
                {
                    "kind": "policy",
                    "message": (
                        f"Over policy: taxi «{desc}» is {float(amount):.2f} {currency}, "
                        f"above the per-ride cap {float(max_taxi):.2f}."
                    ),
                }
            )
    template = str(policy.get("template") or state.get("sink") or "")
    start = str(state.get("date_start") or "")
    end = str(state.get("date_end") or "")
    if template == "expense" and _span_nights(start, end) >= 1:
        lodging_labels = {"lodging", "hotel", "住宿"}
        already = any(
            f["kind"] == "missing"
            and ("lodging" in f["message"].lower() or "hotel" in f["message"].lower())
            for f in findings
        )
        if not already and not (categories & lodging_labels):
            findings.append(
                {
                    "kind": "missing",
                    "message": "Missing receipt: multi-night trip but no lodging/hotel evidence.",
                }
            )
    if not findings and template == "expense":
        findings.append(
            {
                "kind": "info",
                "message": "Policy scan: no missing or over-cap items under current rules.",
            }
        )
    return findings


def _claim_id(state: dict[str, Any]) -> str:
    if state.get("claim_id"):
        return str(state["claim_id"])
    token = str(state.get("case_id") or "CLAIM").upper().replace("_", "-")
    return f"EXP-{token}-001"


def patch_state(payload: dict[str, Any]) -> dict[str, Any]:
    incoming = dict(payload)
    reset = bool(incoming.pop("reset", False))
    append = incoming.pop("append_line", None)
    with _LOCK:
        if reset or not _STATE:
            _STATE.clear()
            _STATE.update(empty_state())
        for key, value in incoming.items():
            _STATE[key] = value
        if isinstance(append, dict):
            lines = list(_STATE.get("lines") or [])
            lines.append(append)
            _STATE["lines"] = lines
        _recompute(_STATE)
        return dict(_STATE)


def mutate_lines(payload: dict[str, Any]) -> tuple[dict[str, Any], int]:
    with _LOCK:
        if _STATE.get("status") != "draft":
            return dict(_STATE), 409
        action = str(payload.get("action") or "")
        lines = list(_STATE.get("lines") or [])
        try:
            if action == "update":
                idx = int(payload["index"])
                patch = dict(payload.get("line") or {})
                lines[idx] = {**lines[idx], **patch}
            elif action == "delete":
                idx = int(payload["index"])
                del lines[idx]
            elif action == "add":
                line = dict(payload.get("line") or {})
                line.setdefault("source", "manual")
                line.setdefault("currency", "EUR")
                line.setdefault("line_id", f"manual:{int(time.time())}")
                if not line.get("evidence"):
                    line["evidence"] = line["line_id"]
                lines.append(line)
            else:
                return dict(_STATE), 400
        except (KeyError, ValueError, IndexError, TypeError):
            return dict(_STATE), 400
        _STATE["lines"] = lines
        _recompute(_STATE)
        _STATE["message"] = "Draft updated. Edit, add a receipt, then Submit or Cancel."
        return dict(_STATE), 200


def submit_state(payload: dict[str, Any] | None = None) -> tuple[dict[str, Any], int]:
    extra = dict(payload or {})
    with _LOCK:
        if _STATE.get("status") == "submitted":
            return dict(_STATE), 200
        if _STATE.get("status") not in {"draft", "filling"}:
            return dict(_STATE), 409
        if extra.get("lines") is not None:
            _STATE["lines"] = extra["lines"]
        _recompute(_STATE)
        if not _STATE.get("lines"):
            _STATE["message"] = "Nothing to submit — add at least one line."
            return dict(_STATE), 400
        if extra.get("claim_id"):
            _STATE["claim_id"] = extra["claim_id"]
        _STATE["claim_id"] = _claim_id(_STATE)
        _STATE["status"] = "submitted"
        _STATE["message"] = f"Submitted {_STATE['claim_id']} — pending finance."
        return dict(_STATE), 200


class ReuseThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True


class ErpHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(WEB_DIR), **kwargs)

    def log_message(self, format: str, *args) -> None:  # noqa: A002
        sys.stderr.write("[erp] " + (format % args) + "\n")

    def _json(self, code: int, body: dict[str, Any]) -> None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(data)

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        if not raw:
            return {}
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path.rstrip("/") or "/"
        if path in {"/api/erp", "/api/erp/state"}:
            self._json(200, get_state())
            return
        super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path.rstrip("/") or "/"
        payload = self._read_json()
        if path in {"/api/erp", "/api/erp/fill"}:
            self._json(200, patch_state(payload))
            return
        if path == "/api/erp/reset":
            self._json(200, reset_state())
            return
        if path in {"/api/erp/line", "/api/erp/lines"}:
            body, code = mutate_lines(payload)
            self._json(code, body)
            return
        if path == "/api/erp/submit":
            body, code = submit_state(payload)
            self._json(code, body)
            return
        if path == "/api/erp/cancel":
            self._json(
                200,
                patch_state(
                    {
                        "status": "cancelled",
                        "message": "Employee cancelled. Claim was not sent to finance.",
                    }
                ),
            )
            return
        self._json(404, {"error": "unknown endpoint", "path": path})


def make_server(port: int = DEFAULT_PORT) -> ThreadingHTTPServer:
    reset_state()
    return ReuseThreadingHTTPServer((HOST, port), ErpHandler)


def serve_in_thread(port: int = DEFAULT_PORT) -> ThreadingHTTPServer:
    server = make_server(port)
    thread = threading.Thread(target=server.serve_forever, daemon=True, name="erp-mock")
    thread.start()
    return server


def serve_forever(port: int = DEFAULT_PORT) -> None:
    if not WEB_DIR.is_dir():
        raise SystemExit(f"web_mocks not found: {WEB_DIR}")
    server = make_server(port)
    print(f"Serving {WEB_DIR} on http://{HOST}:{port}/")
    print(f"ERP form: http://{HOST}:{port}/expense.html")
    print("Keep this terminal open. Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        server.server_close()


if __name__ == "__main__":
    chosen = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT
    serve_forever(chosen)
