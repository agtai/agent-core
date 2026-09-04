# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Generic models for multi-source collect → merge → approve → submit."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

TemplateId = Literal["expense", "reconcile", "trip_summary", "media_pack"]


@dataclass(frozen=True)
class PermissionUsed:
    source: str
    scope: str
    purpose: str


@dataclass(frozen=True)
class EvidenceItem:
    """Raw item returned by a source adapter (pre-merge)."""

    item_id: str
    source: str
    date: str
    description: str
    evidence_ref: str
    amount: float | None = None
    currency: str | None = None
    category: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DraftLine:
    """One line on the human-reviewable draft (post-merge)."""

    line_id: str
    source: str
    date: str
    description: str
    evidence_ref: str
    amount: float | None = None
    currency: str | None = None
    category: str | None = None


# Back-compat alias used by older demo call sites / web copy.
ExpenseLine = DraftLine


@dataclass(frozen=True)
class MergeConfig:
    sort_by: list[str] = field(default_factory=lambda: ["date", "source", "line_id"])
    dedupe_by: list[str] | None = None  # e.g. ["date", "amount", "currency"]
    category_map: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class PolicyConfig:
    """Rule thresholds for missing-receipt / over-policy tips."""

    enabled: bool = True
    max_meal_amount: float | None = 80.0
    max_taxi_amount: float | None = 55.0
    require_categories: list[str] = field(default_factory=list)
    # Categories treated as taxi/ride (after category_map).
    taxi_categories: list[str] = field(default_factory=lambda: ["transport"])
    meal_categories: list[str] = field(default_factory=lambda: ["meal"])


@dataclass(frozen=True)
class PolicyFinding:
    kind: Literal["missing", "policy", "info"]
    message: str


@dataclass(frozen=True)
class CaseConfig:
    """A1: configurable case (not hard-coded to Paris)."""

    case_id: str
    template: TemplateId
    title: str
    actor: str
    query: str
    date_start: str
    date_end: str
    sources: list[str]
    merge: MergeConfig
    sink: str
    notes: list[str] = field(default_factory=list)
    fixtures: dict[str, Any] = field(default_factory=dict)
    policy: PolicyConfig = field(default_factory=PolicyConfig)
    # Candidate sources the planner may choose from (defaults to sources).
    available_sources: list[str] = field(default_factory=list)


@dataclass
class CollectionDraft:
    """Merged package ready for approval."""

    case_id: str
    template: TemplateId
    title: str
    actor: str
    date_start: str
    date_end: str
    lines: list[DraftLine] = field(default_factory=list)
    permissions: list[PermissionUsed] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    findings: list[PolicyFinding] = field(default_factory=list)
    plan_reason: str = ""
    denied_sources: list[str] = field(default_factory=list)
    sink: str = "expense"

    @property
    def total_by_currency(self) -> dict[str, float]:
        totals: dict[str, float] = {}
        for line in self.lines:
            if line.amount is None or not line.currency:
                continue
            totals[line.currency] = round(totals.get(line.currency, 0.0) + line.amount, 2)
        return totals

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# Back-compat name
ExpenseDraft = CollectionDraft


@dataclass(frozen=True)
class SubmitResult:
    claim_id: str
    status: str
    submitted_lines: int
    totals: dict[str, float]
    sink: str = "expense"
