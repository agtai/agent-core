# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Plan which mock sources to call (heuristic or optional LLM)."""

from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass

from .config import source_label
from .models import CaseConfig
from .registry import list_sources


@dataclass(frozen=True)
class SourcePlan:
    sources: list[str]
    reason: str
    mode: str  # heuristic | llm | fixed


def plan_sources(
    case: CaseConfig,
    *,
    mode: str = "auto",
    query_override: str | None = None,
) -> SourcePlan:
    """
    mode:
      - fixed: use case.sources as-is
      - heuristic: pick sources from query / template
      - llm: call a model (needs API_KEY)
      - auto: llm if API_KEY else heuristic
    """
    query = (query_override or case.query).strip()
    available = case.available_sources or case.sources or list_sources()
    available = [s for s in available if s in set(list_sources())]

    resolved = mode
    if mode == "auto":
        resolved = "llm" if _has_llm_credentials() else "heuristic"
    if resolved == "fixed":
        chosen = [s for s in case.sources if s in available] or list(available)
        return SourcePlan(
            sources=chosen,
            reason="Fixed plan from case YAML sources (no smart planning).",
            mode="fixed",
        )
    if resolved == "llm":
        try:
            return _plan_with_llm(case, query, available)
        except Exception as exc:  # noqa: BLE001 — demo fallback
            fallback = _plan_heuristic(case, query, available)
            return SourcePlan(
                sources=fallback.sources,
                reason=f"LLM planning failed ({exc}); fell back to heuristic: {fallback.reason}",
                mode="heuristic",
            )
    return _plan_heuristic(case, query, available)


def _has_llm_credentials() -> bool:
    return bool(
        os.getenv("API_KEY", "").strip()
        or os.getenv("OPENAI_API_KEY", "").strip()
        or os.getenv("GLM_API_KEY", "").strip()
    )


def _plan_heuristic(case: CaseConfig, query: str, available: list[str]) -> SourcePlan:
    q = query.lower()
    picked: list[str] = []
    reasons: list[str] = []

    def want(*names: str, why: str) -> None:
        for name in names:
            if name in available and name not in picked:
                picked.append(name)
        reasons.append(why)

    if case.template == "reconcile" or any(k in query for k in ("对账", "reconcile", "ledger")):
        want("email", "db", why="reconcile: email receipts vs finance DB")
    elif case.template == "trip_summary" or any(
        k in query for k in ("周报", "简报", "总结", "summarize", "brief")
    ):
        want("booking", "rides", "browser", why="trip summary: itinerary sources")
    elif case.template == "media_pack" or any(k in query for k in ("素材", "打包", "media", "pack")):
        want("photos", "browser", why="media pack: photos and web clips")
    else:
        if any(k in query for k in ("报销", "expense", "差旅", "trip", "paris", "巴黎")):
            want("email", "photos", "booking", "rides", why="expense: email/photos/hotel/rides")
        else:
            want(*case.sources, why="no special intent; using case default sources")

    if any(k in q for k in ("邮件", "email", "inbox", "receipt")):
        want("email", why="query mentions email/receipts")
    if any(k in q for k in ("相册", "照片", "photo", "ocr")):
        want("photos", why="query mentions photos")
    if any(k in q for k in ("酒店", "hotel", "booking", "预订")):
        want("booking", why="query mentions hotel/booking")
    if any(k in q for k in ("打车", "taxi", "uber", "出行", "ride")):
        want("rides", why="query mentions rides")
    if any(k in q for k in ("浏览器", "网页", "browser", "intranet")):
        want("browser", why="query mentions browser/web")
    if any(k in q for k in ("数据库", "台账", "db", "ledger")):
        want("db", why="query mentions database/ledger")

    if not picked:
        picked = list(available)
        reasons.append("heuristic miss; collecting all available sources")

    labels = ", ".join(source_label(s) for s in picked)
    reason = f"Heuristic plan → {labels}. " + "; ".join(reasons[:3])
    return SourcePlan(sources=picked, reason=reason, mode="heuristic")


def _plan_with_llm(case: CaseConfig, query: str, available: list[str]) -> SourcePlan:
    api_key = (
        os.getenv("API_KEY", "").strip()
        or os.getenv("OPENAI_API_KEY", "").strip()
        or os.getenv("GLM_API_KEY", "").strip()
    )
    model_name = os.getenv("MODEL_NAME", os.getenv("OPENAI_MODEL", "gpt-4o-mini")).strip()
    api_base = os.getenv(
        "API_BASE",
        os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1"),
    ).strip()
    provider = os.getenv("MODEL_PROVIDER", "OpenAI").strip()

    from openjiuwen.core.foundation.llm import SystemMessage, UserMessage, init_model

    catalog = ", ".join(f"{s}({source_label(s)})" for s in available)
    system = (
        "You are a travel-assistant source planner. Given the user request, pick a subset "
        "of available sources to call. Output JSON only: "
        '{"sources":["email",...],"reason":"one short English sentence"}. '
        "sources must be a subset of the available list, ordered by call sequence; "
        "do not invent names outside the list."
    )
    user = (
        f"template={case.template}\n"
        f"window={case.date_start}→{case.date_end}\n"
        f"available={catalog}\n"
        f"user_request={query}\n"
        f"case_default_sources={case.sources}"
    )
    model = init_model(
        provider=provider,
        model_name=model_name,
        api_key=api_key,
        api_base=api_base,
        temperature=0.1,
        max_tokens=300,
        verify_ssl=False,
    )

    async def _call() -> str:
        resp = await model.invoke(
            [SystemMessage(content=system), UserMessage(content=user)]
        )
        content = getattr(resp, "content", None) or str(resp)
        return str(content)

    raw = asyncio.run(_call())
    data = _extract_json(raw)
    chosen = [s for s in data.get("sources") or [] if s in available]
    if not chosen:
        raise ValueError(f"Model returned no valid sources: {raw[:200]}")
    reason = str(data.get("reason") or "model plan")
    labels = ", ".join(source_label(s) for s in chosen)
    return SourcePlan(
        sources=chosen,
        reason=f"LLM plan → {labels}. {reason}",
        mode="llm",
    )


def _extract_json(text: str) -> dict:
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            raise
        return json.loads(match.group(0))
