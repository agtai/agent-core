# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Load case packs (YAML) from cases/."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .models import CaseConfig, MergeConfig, PolicyConfig, TemplateId

CASES_DIR = Path(__file__).resolve().parent / "cases"

_VALID_TEMPLATES = {"expense", "reconcile", "trip_summary", "media_pack"}


def list_case_ids() -> list[str]:
    return sorted(p.stem for p in CASES_DIR.glob("*.yaml"))


def load_case(case_id: str) -> CaseConfig:
    path = CASES_DIR / f"{case_id}.yaml"
    if not path.is_file():
        known = ", ".join(list_case_ids()) or "(none)"
        raise FileNotFoundError(f"Unknown case '{case_id}'. Available: {known}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return _parse_case(raw, default_id=case_id)


def _parse_policy(raw: dict[str, Any] | None) -> PolicyConfig:
    data = raw or {}
    return PolicyConfig(
        enabled=bool(data.get("enabled", True)),
        max_meal_amount=(
            float(data["max_meal_amount"]) if data.get("max_meal_amount") is not None else None
        ),
        max_taxi_amount=(
            float(data["max_taxi_amount"]) if data.get("max_taxi_amount") is not None else None
        ),
        require_categories=[str(x) for x in data.get("require_categories") or []],
        taxi_categories=[str(x) for x in data.get("taxi_categories") or ["transport"]],
        meal_categories=[str(x) for x in data.get("meal_categories") or ["meal"]],
    )


def _parse_case(raw: dict[str, Any], default_id: str) -> CaseConfig:
    template = str(raw.get("template", "expense"))
    if template not in _VALID_TEMPLATES:
        raise ValueError(f"Invalid template '{template}'. Expected one of {_VALID_TEMPLATES}")
    merge_raw = raw.get("merge") or {}
    merge = MergeConfig(
        sort_by=list(merge_raw.get("sort_by") or ["date", "source", "line_id"]),
        dedupe_by=list(merge_raw["dedupe_by"]) if merge_raw.get("dedupe_by") else None,
        category_map={str(k): str(v) for k, v in (merge_raw.get("category_map") or {}).items()},
    )
    sources = [str(s) for s in raw.get("sources") or []]
    available = [str(s) for s in raw.get("available_sources") or sources]
    return CaseConfig(
        case_id=str(raw.get("case_id") or default_id),
        template=template,  # type: ignore[arg-type]
        title=str(raw["title"]),
        actor=str(raw["actor"]),
        query=str(raw["query"]),
        date_start=str(raw["date_start"]),
        date_end=str(raw["date_end"]),
        sources=sources,
        merge=merge,
        sink=str(raw.get("sink") or "expense"),
        notes=[str(n) for n in raw.get("notes") or []],
        fixtures=dict(raw.get("fixtures") or {}),
        policy=_parse_policy(raw.get("policy")),
        available_sources=available,
    )


def template_labels(template: TemplateId) -> dict[str, str]:
    """UI copy for approval / submit by scenario template."""
    table = {
        "expense": {
            "card_title": "APPROVAL CARD — expense draft ready to submit",
            "lines_header": "Lines to submit:",
            "submit_verb": "Submitting to expense system…",
            "done_title": "SUBMITTED",
        },
        "reconcile": {
            "card_title": "APPROVAL CARD — reconcile package ready",
            "lines_header": "Matched / staged lines:",
            "submit_verb": "Posting reconcile package…",
            "done_title": "RECONCILE POSTED",
        },
        "trip_summary": {
            "card_title": "APPROVAL CARD — trip brief ready",
            "lines_header": "Items in the brief:",
            "submit_verb": "Saving trip brief…",
            "done_title": "BRIEF SAVED",
        },
        "media_pack": {
            "card_title": "APPROVAL CARD — media pack ready",
            "lines_header": "Assets to archive:",
            "submit_verb": "Archiving media pack…",
            "done_title": "PACK ARCHIVED",
        },
    }
    return table[template]


def source_label(name: str) -> str:
    return {
        "email": "Email",
        "photos": "Photos",
        "booking": "Booking",
        "rides": "Rides",
        "browser": "Browser",
        "db": "Database",
        "expense": "Expense",
        "ledger": "Ledger",
        "notes": "Notes",
        "archive": "Archive",
    }.get(name, name)
