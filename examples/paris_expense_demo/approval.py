# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Terminal approval card rendering (template-aware)."""

from __future__ import annotations

from .config import source_label, template_labels
from .models import CollectionDraft, PermissionUsed, SubmitResult


def render_approval_card(draft: CollectionDraft) -> str:
    labels = template_labels(draft.template)
    width = 72
    bar = "=" * width
    thin = "-" * width
    lines_out: list[str] = [
        bar,
        f" {labels['card_title']}",
        bar,
        f" Case:     {draft.case_id}  ({draft.template})",
        f" Title:    {draft.title}",
        f" Actor:    {draft.actor}",
        f" Window:   {draft.date_start} → {draft.date_end}",
        f" Sink:     {source_label(draft.sink)} ({draft.sink})",
        thin,
        f" {labels['lines_header']}",
        thin,
    ]
    for idx, line in enumerate(draft.lines, start=1):
        category = (line.category or "—")[:10]
        if line.amount is None or not line.currency:
            money = "        —"
        else:
            money = f"{line.amount:>8.2f} {line.currency}"
        src = source_label(line.source)
        lines_out.append(
            f"  {idx:>2}. [{line.date}] {category:<10} {money}  ← {src}"
        )
        lines_out.append(f"      {line.description}")
        lines_out.append(f"      evidence: {line.evidence_ref}")

    totals = draft.total_by_currency
    if totals:
        lines_out.append(thin)
        lines_out.append(" Totals:")
        for currency, total in sorted(totals.items()):
            lines_out.append(f"    {total:>10.2f} {currency}")

    lines_out.append(thin)
    lines_out.append(" Permissions used while collecting:")
    for perm in draft.permissions:
        lines_out.append(f"  • {source_label(perm.source)}: {perm.scope}")
        lines_out.append(f"      ({perm.purpose})")

    if draft.plan_reason:
        lines_out.append(thin)
        lines_out.append(" Source plan:")
        lines_out.append(f"  - {draft.plan_reason}")

    if draft.findings:
        lines_out.append(thin)
        lines_out.append(" Missing / policy tips:")
        kind_en = {"missing": "missing", "policy": "policy", "info": "info"}
        for finding in draft.findings:
            tag = kind_en.get(finding.kind, finding.kind)
            lines_out.append(f"  [{tag}] {finding.message}")

    if draft.notes:
        lines_out.append(thin)
        lines_out.append(" Notes:")
        for note in draft.notes:
            lines_out.append(f"  - {note}")

    lines_out.append(bar)
    lines_out.append(" Confirm: type  y  to submit,  n  to cancel.")
    lines_out.append(bar)
    return "\n".join(lines_out)


def render_submit_result(
    draft: CollectionDraft,
    result: SubmitResult,
    write_permission: PermissionUsed,
) -> str:
    labels = template_labels(draft.template)
    width = 72
    bar = "=" * width
    parts = [
        bar,
        f" {labels['done_title']}",
        bar,
        f" ID:     {result.claim_id}",
        f" Status: {result.status}",
        f" Sink:   {source_label(result.sink)} ({result.sink})",
        f" Lines:  {result.submitted_lines}",
    ]
    if result.totals:
        parts.append(" Totals:")
        for currency, total in sorted(result.totals.items()):
            parts.append(f"    {total:>10.2f} {currency}")
    parts.append(
        f" Write permission: {source_label(write_permission.source)}:{write_permission.scope}"
    )
    parts.append(bar)
    return "\n".join(parts)
