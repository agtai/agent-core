# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""A5: configurable merge (sort / dedupe / category map)."""

from __future__ import annotations

from .models import DraftLine, EvidenceItem, MergeConfig


def evidence_to_line(item: EvidenceItem, category_map: dict[str, str]) -> DraftLine:
    category = item.category
    if category and category in category_map:
        category = category_map[category]
    return DraftLine(
        line_id=item.item_id,
        source=item.source,
        date=item.date,
        description=item.description,
        evidence_ref=item.evidence_ref,
        amount=item.amount,
        currency=item.currency,
        category=category,
    )


def _sort_key(line: DraftLine, fields: list[str]):
    # Prefer human-facing sources before ledger dumps when sorting by source
    # (e.g. berlin reconcile: keep email receipt over db duplicate).
    source_rank = {
        "email": 0,
        "photos": 1,
        "booking": 2,
        "rides": 3,
        "browser": 4,
        "db": 5,
    }
    keys = []
    for field in fields:
        value = getattr(line, field, "")
        if value is None:
            value = ""
        if field == "source":
            keys.append((source_rank.get(str(value), 50), str(value)))
        else:
            keys.append(value)
    return tuple(keys)


def _dedupe_key(line: DraftLine, fields: list[str]):
    keys = []
    for field in fields:
        value = getattr(line, field, None)
        keys.append(value)
    return tuple(keys)


def merge_evidence(
    items: list[EvidenceItem],
    config: MergeConfig,
) -> list[DraftLine]:
    lines = [evidence_to_line(item, config.category_map) for item in items]
    lines.sort(key=lambda line: _sort_key(line, config.sort_by))

    if not config.dedupe_by:
        return lines

    seen: set[tuple] = set()
    unique: list[DraftLine] = []
    for line in lines:
        key = _dedupe_key(line, config.dedupe_by)
        if key in seen:
            continue
        seen.add(key)
        unique.append(line)
    return unique
