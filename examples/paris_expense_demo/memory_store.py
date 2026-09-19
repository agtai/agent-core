# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""File-backed user memory for the demo (profile, policy, chat, past claims)."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import yaml

from .config import source_label
from .models import CaseConfig, PermissionUsed
from .permission_gate import ask_source_permission

MEMORY_DIR = Path(__file__).resolve().parent / "memory"

MEMORY_PERMISSION = PermissionUsed(
    source="memory",
    scope="read:memory.user",
    purpose="Recall profile, trip notes, policy caps, and past claims",
)


@dataclass(frozen=True)
class MemoryRecord:
    record_id: str
    kind: str
    text: str
    scope: str = ""
    date: str = ""
    data: dict[str, Any] = field(default_factory=dict)


@dataclass
class RecalledMemory:
    actor: str
    granted: bool = False
    records: list[MemoryRecord] = field(default_factory=list)
    cost_center: str = ""
    preferred_sources: list[str] = field(default_factory=list)
    trip_hint: str = ""
    snippets: list[str] = field(default_factory=list)
    policy_max_meal: float | None = None
    policy_max_taxi: float | None = None
    notes: list[str] = field(default_factory=list)

    @property
    def augmented_query(self) -> str:
        extra = " ".join(part for part in (self.trip_hint, " ".join(self.snippets[:2])) if part)
        return extra.strip()


def actor_slug(actor: str) -> str:
    return "_".join(actor.strip().lower().split())


def load_actor_records(actor: str) -> list[MemoryRecord]:
    path = MEMORY_DIR / f"{actor_slug(actor)}.yaml"
    if not path.is_file():
        return []
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    rows = raw.get("records") or []
    records: list[MemoryRecord] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        records.append(
            MemoryRecord(
                record_id=str(row.get("id") or row.get("record_id") or ""),
                kind=str(row.get("kind") or "note"),
                text=str(row.get("text") or "").strip(),
                scope=str(row.get("scope") or ""),
                date=str(row.get("date") or ""),
                data=dict(row.get("data") or {}),
            )
        )
    return records


def _query_tokens(query: str, case: CaseConfig) -> set[str]:
    blob = " ".join(
        [
            query,
            case.query,
            case.title,
            case.case_id,
            case.date_start,
            case.date_end,
        ]
    ).lower()
    return {tok for tok in blob.replace("—", " ").replace("–", " ").split() if len(tok) >= 3}


def _record_matches(record: MemoryRecord, tokens: set[str]) -> bool:
    if record.kind in {"profile", "policy"}:
        return True
    hay = " ".join(
        [
            record.text,
            record.kind,
            record.date,
            " ".join(str(v) for v in record.data.values() if not isinstance(v, (dict, list))),
            " ".join(str(x) for x in (record.data.get("keywords") or [])),
        ]
    ).lower()
    return any(tok in hay for tok in tokens)


def recall_memory(
    case: CaseConfig,
    *,
    auto_grant: bool = False,
) -> RecalledMemory:
    """Ask permission, then return the records that match this case."""
    records = load_actor_records(case.actor)
    recalled = RecalledMemory(actor=case.actor)
    if not records:
        recalled.notes.append(f"No local memory pack for {case.actor}.")
        return recalled

    granted = ask_source_permission(MEMORY_PERMISSION, auto_grant=auto_grant)
    recalled.granted = granted
    if not granted:
        recalled.notes.append("Memory read denied; falling back to the case file only.")
        return recalled

    tokens = _query_tokens(case.query, case)
    picked = [rec for rec in records if _record_matches(rec, tokens)]
    if not picked:
        picked = [rec for rec in records if rec.kind in {"profile", "policy"}]
    recalled.records = picked

    for rec in picked:
        if rec.text:
            stamp = f"{rec.date} · " if rec.date else ""
            recalled.snippets.append(f"{stamp}{rec.text}")
        if rec.kind == "profile":
            recalled.cost_center = str(rec.data.get("cost_center") or recalled.cost_center)
            preferred = rec.data.get("preferred_sources") or []
            recalled.preferred_sources = [str(s) for s in preferred]
        elif rec.kind == "policy":
            if rec.data.get("max_meal_amount") is not None:
                recalled.policy_max_meal = float(rec.data["max_meal_amount"])
            if rec.data.get("max_taxi_amount") is not None:
                recalled.policy_max_taxi = float(rec.data["max_taxi_amount"])
        elif rec.kind == "conversation":
            city = rec.data.get("trip_city")
            start = rec.data.get("date_start") or case.date_start
            end = rec.data.get("date_end") or case.date_end
            if city:
                recalled.trip_hint = f"{city} trip {start} → {end}"

    kinds = ", ".join(sorted({rec.kind for rec in picked}))
    recalled.notes.append(
        f"Recalled {len(picked)} memory record(s) for {case.actor} ({kinds})."
    )
    if recalled.cost_center:
        recalled.notes.append(f"Cost center from memory: {recalled.cost_center}.")
    if recalled.policy_max_meal is not None or recalled.policy_max_taxi is not None:
        meal = recalled.policy_max_meal
        taxi = recalled.policy_max_taxi
        recalled.notes.append(
            "Usual policy from memory: "
            + ", ".join(
                part
                for part in (
                    f"meals ≤ {meal:.0f}" if meal is not None else "",
                    f"taxis ≤ {taxi:.0f}" if taxi is not None else "",
                )
                if part
            )
            + "."
        )
    return recalled


def apply_memory(case: CaseConfig, recalled: RecalledMemory) -> CaseConfig:
    """Overlay recalled policy caps / preferred sources onto the case."""
    if not recalled.granted:
        return case
    policy = case.policy
    updates: dict[str, Any] = {}
    if recalled.policy_max_meal is not None:
        updates["max_meal_amount"] = recalled.policy_max_meal
    if recalled.policy_max_taxi is not None:
        updates["max_taxi_amount"] = recalled.policy_max_taxi
    if updates:
        policy = replace(policy, **updates)
    sources = list(case.sources)
    if recalled.preferred_sources:
        preferred = [s for s in recalled.preferred_sources if s in (case.available_sources or case.sources)]
        if preferred:
            sources = preferred
    return replace(case, policy=policy, sources=sources)


def render_memory_card(recalled: RecalledMemory) -> str:
    width = 72
    bar = "=" * width
    lines = [
        bar,
        " MEMORY RECALL",
        bar,
        f" Actor:    {recalled.actor}",
        f" Access:   {'granted' if recalled.granted else 'denied / none'}",
    ]
    if recalled.cost_center:
        lines.append(f" Cost ctr: {recalled.cost_center}")
    if recalled.trip_hint:
        lines.append(f" Trip:     {recalled.trip_hint}")
    if recalled.preferred_sources:
        labels = ", ".join(source_label(s) for s in recalled.preferred_sources)
        lines.append(f" Sources:  {labels} (from past claims)")
    if recalled.snippets:
        lines.append("-" * width)
        lines.append(" What I already know:")
        for snippet in recalled.snippets:
            lines.append(f"  • {snippet}")
    if recalled.notes:
        lines.append("-" * width)
        for note in recalled.notes:
            lines.append(f"  - {note}")
    lines.append(bar)
    return "\n".join(lines)
