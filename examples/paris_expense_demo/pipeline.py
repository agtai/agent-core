# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Fixed orchestration: plan → permission → collect → merge → policy → draft."""

from __future__ import annotations

from dataclasses import replace

from .config import load_case, source_label
from .memory_store import MEMORY_PERMISSION, apply_memory, recall_memory, render_memory_card
from .merge import merge_evidence
from .models import CaseConfig, CollectionDraft, PermissionUsed, SubmitResult
from .permission_gate import ask_source_permission
from .planner import SourcePlan, plan_sources
from .policy import evaluate_policy
from .registry import get_source
from .sources import permission_meta
from . import sources as _sources  # noqa: F401  — register adapters
from . import sinks


def collect_case(
    case: CaseConfig,
    *,
    plan_mode: str = "auto",
    auto_grant: bool = False,
    deny_sources: set[str] | None = None,
    query_override: str | None = None,
) -> CollectionDraft:
    """Plan sources, ask permission per source, merge, then attach policy tips."""
    deny_sources = deny_sources or set()
    recalled = recall_memory(case, auto_grant=auto_grant)
    print(render_memory_card(recalled))
    print()
    case = apply_memory(case, recalled)
    base_query = (query_override or case.query).strip()
    if recalled.augmented_query:
        base_query = f"{base_query} {recalled.augmented_query}".strip()
    plan: SourcePlan = plan_sources(case, mode=plan_mode, query_override=base_query)
    print(f"Plan ({plan.mode}): {plan.reason}")
    print(
        "Sources to call: "
        + " → ".join(source_label(s) for s in plan.sources)
        + "\n"
    )

    evidence = []
    permissions: list[PermissionUsed] = []
    denied: list[str] = []
    if recalled.granted:
        permissions.append(MEMORY_PERMISSION)

    for name in plan.sources:
        meta = permission_meta(name)
        granted = ask_source_permission(
            meta,
            auto_grant=auto_grant,
            forced_deny=deny_sources,
        )
        if not granted:
            denied.append(name)
            continue
        items, perm = get_source(name)(case)
        evidence.extend(items)
        permissions.append(perm)

    lines = merge_evidence(evidence, case.merge)
    notes = list(case.notes)
    notes.extend(recalled.notes)
    notes.append(
        f"plan_mode={plan.mode}; collected: "
        + (
            ", ".join(
                source_label(p.source)
                for p in permissions
                if p.source != "memory"
            )
            or "(none)"
        )
    )
    if denied:
        notes.append(
            "Denied sources: " + ", ".join(source_label(s) for s in denied)
        )

    draft = CollectionDraft(
        case_id=case.case_id,
        template=case.template,
        title=case.title,
        actor=case.actor,
        date_start=case.date_start,
        date_end=case.date_end,
        lines=lines,
        permissions=permissions,
        notes=notes,
        plan_reason=plan.reason,
        denied_sources=denied,
        sink=case.sink,
        cost_center=recalled.cost_center,
        memory_snippets=list(recalled.snippets),
        policy_snapshot={
            "enabled": case.policy.enabled,
            "max_meal_amount": case.policy.max_meal_amount,
            "max_taxi_amount": case.policy.max_taxi_amount,
            "require_categories": list(case.policy.require_categories),
            "meal_categories": list(case.policy.meal_categories),
            "taxi_categories": list(case.policy.taxi_categories),
            "template": case.template,
            "denied_sources": [source_label(s) for s in denied],
        },
    )
    draft.findings = evaluate_policy(draft, case, planned_sources=plan.sources)
    return draft


def collect_by_id(case_id: str, **kwargs) -> CollectionDraft:
    return collect_case(load_case(case_id), **kwargs)


def submit_draft(draft: CollectionDraft) -> tuple[SubmitResult, PermissionUsed]:
    return sinks.submit_draft(draft)


def collect_paris_trip(trip_hint: str | None = None) -> CollectionDraft:
    """Back-compat entry for the original Paris demo."""
    case = load_case("paris_expense")
    if trip_hint:
        case = replace(case, query=trip_hint)
    return collect_case(case, plan_mode="fixed", auto_grant=True)
