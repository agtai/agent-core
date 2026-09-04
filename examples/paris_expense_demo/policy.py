# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Missing-receipt / over-policy tips on a CollectionDraft."""

from __future__ import annotations

from .config import source_label
from .models import CaseConfig, CollectionDraft, PolicyFinding


def evaluate_policy(
    draft: CollectionDraft,
    case: CaseConfig,
    *,
    planned_sources: list[str] | None = None,
) -> list[PolicyFinding]:
    policy = case.policy
    if not policy.enabled:
        return []

    findings: list[PolicyFinding] = []
    planned = planned_sources or []
    denied = set(draft.denied_sources)

    for src in planned:
        if src in denied:
            findings.append(
                PolicyFinding(
                    kind="missing",
                    message=(
                        f"You denied «{source_label(src)}» access; "
                        f"related receipts may be missing — re-open the source or upload manually."
                    ),
                )
            )

    categories = {(line.category or "").strip() for line in draft.lines}
    for required in policy.require_categories:
        if required not in categories:
            findings.append(
                PolicyFinding(
                    kind="missing",
                    message=f"Missing item: no «{required}» category evidence in the draft.",
                )
            )

    meal_cats = set(policy.meal_categories)
    taxi_cats = set(policy.taxi_categories)
    for line in draft.lines:
        cat = (line.category or "").strip()
        if line.amount is None:
            continue
        if (
            policy.max_meal_amount is not None
            and cat in meal_cats
            and line.amount > policy.max_meal_amount
        ):
            findings.append(
                PolicyFinding(
                    kind="policy",
                    message=(
                        f"Over policy: meal «{line.description}» is "
                        f"{line.amount:.2f} {line.currency or ''}, "
                        f"above the per-meal cap {policy.max_meal_amount:.2f}."
                    ),
                )
            )
        if (
            policy.max_taxi_amount is not None
            and cat in taxi_cats
            and _looks_like_taxi(line.description)
            and line.amount > policy.max_taxi_amount
        ):
            findings.append(
                PolicyFinding(
                    kind="policy",
                    message=(
                        f"Over policy: taxi «{line.description}» is "
                        f"{line.amount:.2f} {line.currency or ''}, "
                        f"above the per-ride cap {policy.max_taxi_amount:.2f}."
                    ),
                )
            )

    if case.template == "expense" and _span_nights(case.date_start, case.date_end) >= 1:
        lodging_labels = {"lodging", "hotel", "住宿"}
        already = any(
            f.kind == "missing"
            and ("lodging" in f.message.lower() or "hotel" in f.message.lower() or "住宿" in f.message)
            for f in findings
        )
        if not already and not (categories & lodging_labels):
            findings.append(
                PolicyFinding(
                    kind="missing",
                    message="Missing receipt: multi-night trip but no lodging/hotel evidence.",
                )
            )

    if not findings and case.template == "expense":
        findings.append(
            PolicyFinding(
                kind="info",
                message="Policy scan: no missing or over-cap items under current rules.",
            )
        )
    return findings


def _looks_like_taxi(description: str) -> bool:
    text = description.lower()
    keys = ("taxi", "uber", "cab", "airport", "cdg", "icn", "→", "->", "打车", "出租", "机场")
    if any(
        k in text
        for k in ("air france", "flight", "train", "metro", "subway", "法航", "火车", "地铁", "航班")
    ):
        return False
    return any(k in text for k in keys)


def _span_nights(start: str, end: str) -> int:
    try:
        from datetime import date

        a = date.fromisoformat(start)
        b = date.fromisoformat(end)
        return max((b - a).days, 0)
    except ValueError:
        return 0
