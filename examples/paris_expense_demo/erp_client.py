# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Fill / submit the local mock ERP that the browser is showing."""

from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .models import CollectionDraft, DraftLine, PermissionUsed, PolicyFinding, SubmitResult

DEFAULT_ERP_ORIGIN = "http://127.0.0.1:8765"

ERP_WRITE = PermissionUsed(
    source="erp_browser",
    scope="write:erp.form",
    purpose="Fill the reimbursement form in the browser",
)


class ErpUnavailable(RuntimeError):
    """Mock ERP HTTP API is not running (old static server or nothing listening)."""


class ErpWrongServer(ErpUnavailable):
    """Something else is already bound on the ERP port."""


def _request(origin: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    url = origin.rstrip("/") + path
    data = None
    headers = {"Accept": "application/json"}
    method = "GET"
    if payload is not None:
        method = "POST"
        raw = json.dumps(payload).encode("utf-8")
        data = raw
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            parsed = {}
        if isinstance(parsed, dict) and "status" in parsed:
            parsed["_http"] = exc.code
            return parsed
        raise ErpUnavailable(f"{method} {url} -> HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise ErpUnavailable(f"{method} {url} failed: {exc.reason}") from exc
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise ErpWrongServer(
            f"{url} is not the mock ERP API (got HTML/static files). "
            "Stop the old http.server and run web_mocks/start-web.sh again."
        ) from exc
    if not isinstance(parsed, dict) or "status" not in parsed:
        raise ErpUnavailable(f"{url} returned unexpected JSON")
    return parsed


def ping_erp(origin: str = DEFAULT_ERP_ORIGIN) -> dict[str, Any]:
    return _request(origin, "/api/erp")


def ensure_erp(
    origin: str = DEFAULT_ERP_ORIGIN,
    *,
    autostart: bool = True,
) -> str:
    """Return origin if the API is up. Optionally spawn erp_server.py."""
    try:
        ping_erp(origin)
        return origin
    except ErpWrongServer:
        raise
    except ErpUnavailable:
        if not autostart:
            raise
    parsed = urlparse(origin)
    port = parsed.port or 8765
    script = Path(__file__).resolve().parent / "erp_server.py"
    try:
        subprocess.Popen(
            [sys.executable, str(script), str(port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except OSError as exc:
        raise ErpUnavailable(f"Could not start mock ERP on port {port}: {exc}") from exc
    deadline = time.time() + 3.0
    last_exc: ErpUnavailable | None = None
    while time.time() < deadline:
        try:
            ping_erp(origin)
            return origin
        except ErpUnavailable as exc:
            last_exc = exc
            time.sleep(0.1)
    raise ErpUnavailable(
        f"Could not reach mock ERP at {origin}: {last_exc}. "
        "If the port is busy, stop it and run web_mocks/start-web.sh."
    )


def _line_payload(draft: CollectionDraft) -> list[dict[str, Any]]:
    rows = []
    for line in draft.lines:
        rows.append(
            {
                "line_id": line.line_id,
                "date": line.date,
                "category": line.category or "",
                "description": line.description,
                "amount": line.amount,
                "currency": line.currency or "",
                "source": line.source,
                "evidence": line.evidence_ref,
            }
        )
    return rows


def fill_draft(
    draft: CollectionDraft,
    *,
    origin: str = DEFAULT_ERP_ORIGIN,
    pause_s: float = 0.28,
    recalled: list[str] | None = None,
    cost_center: str = "",
) -> dict[str, Any]:
    """Reset the form, then type header + each line so the open page updates."""
    ping_erp(origin)
    _request(origin, "/api/erp/reset", {})
    header = {
        "status": "filling",
        "message": "Agent is filling the reimbursement form…",
        "title": draft.title,
        "actor": draft.actor,
        "cost_center": cost_center,
        "date_start": draft.date_start,
        "date_end": draft.date_end,
        "lines": [],
        "totals": draft.total_by_currency,
        "claim_id": "",
        "recalled": list(recalled or []),
        "findings": [{"kind": f.kind, "message": f.message} for f in draft.findings],
        "policy": dict(draft.policy_snapshot),
        "case_id": draft.case_id,
        "sink": draft.sink,
    }
    _request(origin, "/api/erp/fill", header)
    if pause_s:
        time.sleep(pause_s)
    for row in _line_payload(draft):
        _request(
            origin,
            "/api/erp/fill",
            {"status": "filling", "append_line": row, "totals": draft.total_by_currency},
        )
        if pause_s:
            time.sleep(pause_s)
    return _request(
        origin,
        "/api/erp/fill",
        {
            "status": "draft",
            "message": "Form filled. Review on this page — edit, delete, or add a receipt, then Submit.",
            "totals": draft.total_by_currency,
        },
    )


def submit_erp(
    result: SubmitResult,
    *,
    origin: str = DEFAULT_ERP_ORIGIN,
) -> dict[str, Any]:
    return _request(
        origin,
        "/api/erp/submit",
        {
            "claim_id": result.claim_id,
            "status": "submitted",
            "totals": result.totals,
            "message": f"Submitted {result.claim_id} — pending finance.",
        },
    )


def wait_decision(
    origin: str = DEFAULT_ERP_ORIGIN,
    *,
    timeout_s: float | None = None,
    poll_s: float = 0.6,
) -> dict[str, Any]:
    """Block until the ERP page is submitted or cancelled."""
    started = time.time()
    while True:
        state = ping_erp(origin)
        if state.get("status") in {"submitted", "cancelled"}:
            return state
        if timeout_s is not None and (time.time() - started) >= timeout_s:
            raise ErpUnavailable(f"Timed out waiting for ERP approval at {origin}")
        time.sleep(poll_s)


def apply_erp_lines(draft: CollectionDraft, state: dict[str, Any]) -> CollectionDraft:
    """Replace draft lines / findings with whatever the employee left on the page."""
    rows: list[DraftLine] = []
    for idx, row in enumerate(state.get("lines") or []):
        if not isinstance(row, dict):
            continue
        amount = row.get("amount")
        try:
            amount_f = float(amount) if amount is not None and amount != "" else None
        except (TypeError, ValueError):
            amount_f = None
        rows.append(
            DraftLine(
                line_id=str(row.get("line_id") or f"erp:{idx + 1}"),
                source=str(row.get("source") or "manual"),
                date=str(row.get("date") or draft.date_start),
                description=str(row.get("description") or ""),
                evidence_ref=str(row.get("evidence") or row.get("line_id") or f"erp:{idx + 1}"),
                amount=amount_f,
                currency=str(row.get("currency") or "") or None,
                category=str(row.get("category") or "") or None,
            )
        )
    draft.lines = rows
    findings = []
    for item in state.get("findings") or []:
        if not isinstance(item, dict):
            continue
        kind = str(item.get("kind") or "info")
        if kind not in {"missing", "policy", "info"}:
            kind = "info"
        findings.append(PolicyFinding(kind=kind, message=str(item.get("message") or "")))  # type: ignore[arg-type]
    draft.findings = findings
    return draft


def cancel_erp(origin: str = DEFAULT_ERP_ORIGIN) -> dict[str, Any]:
    return _request(origin, "/api/erp/cancel", {})

