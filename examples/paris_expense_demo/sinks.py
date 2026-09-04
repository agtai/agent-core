# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Pluggable sinks for approved drafts."""

from __future__ import annotations

from .models import CollectionDraft, PermissionUsed, SubmitResult


def submit_draft(draft: CollectionDraft) -> tuple[SubmitResult, PermissionUsed]:
    sink = draft.sink
    token = draft.case_id.upper().replace("_", "-")
    prefixes = {
        "expense": ("EXP", "write:claims.create", "submitted_pending_finance"),
        "ledger": ("REC", "write:ledger.post", "reconcile_posted"),
        "notes": ("BRF", "write:notes.save", "brief_saved"),
        "archive": ("MED", "write:archive.put", "pack_archived"),
    }
    prefix, scope, status = prefixes.get(
        sink, ("OUT", f"write:{sink}.create", "submitted")
    )
    result = SubmitResult(
        claim_id=f"{prefix}-{token}-001",
        status=status,
        submitted_lines=len(draft.lines),
        totals=dict(draft.total_by_currency),
        sink=sink,
    )
    permission = PermissionUsed(
        source=sink,
        scope=scope,
        purpose=f"Persist the approved {draft.template} package",
    )
    return result, permission
