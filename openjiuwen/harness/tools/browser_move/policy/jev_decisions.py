# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""TypeSafe decisions at the integration boundary: request shape, wire call, answer validation.

The policy probe's elements become one numbered table plus one candidate head per operation
(CLICK, TYPE_TEXT, SELECT) and an optional value head. One POST returns every head at once; only
the head matching the chosen operation is used.
"""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from openjiuwen.core.common.exception.codes import StatusCode
from openjiuwen.core.common.exception.errors import build_error
from openjiuwen.harness.tools.browser_move.policy import prompts

DEFAULT_DECISIONS_URL = "https://openrouter.ai/api/alpha/decisions"
DEFAULT_MODEL = "typesafe/jev-1.13"
NONE_VALUE = "none"
_OPERATION_LABELS = {
    "CLICK": (
        "Press a control on the page: a button, a link, a menu entry, an autocomplete suggestion or a calendar day."
    ),
    "TYPE_TEXT": "Type a value into an editable field, replacing what it holds.",
    "SELECT": "Pick one of the listed options of a native dropdown.",
    "SCROLL_DOWN": "Move the viewport down the page.",
    "SCROLL_UP": "Move the viewport up the page.",
    "WAIT": "Give the page time to finish updating.",
    "DONE": "The page shows every requirement of the task met.",
    "BLOCKED": "None of the offered operations can move the task forward.",
}
_RETRY_STATUSES = frozenset({429, 503, 529})
_HISTORY_KEYS = ("action", "kind", "text", "page_changed")


@dataclass(frozen=True)
class Candidate:
    """One executable choice: an element (by probe item) under one operation."""

    operation: str
    index: str
    item: dict[str, Any]
    option_label: str = ""


@dataclass
class ActionSpace:
    """Numbered element table plus per-operation candidate heads built from one probe."""

    elements: list[dict[str, Any]] = field(default_factory=list)
    heads: dict[str, dict[str, Candidate]] = field(default_factory=dict)
    operations: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Decision:
    """Validated outcome of one decisions request."""

    operation: str
    candidate: Candidate | None
    value_choice: str
    confidence: float
    operation_probabilities: dict[str, float]
    latency_ms: int
    usage: dict[str, Any]
    model: str


def build_action_space(snapshot: dict[str, Any]) -> ActionSpace:
    """Fold probe elements into jev's table: one row per element, one head per operation."""
    space = ActionSpace()
    for item in snapshot.get("elements") or []:
        if not item.get("target_id"):
            continue
        index = str(len(space.elements) + 1)
        row = {"index": index, "role": item.get("role"), "label": item.get("label"), "value": item.get("value") or ""}
        for key in ("checked", "selected", "expanded", "region"):
            if item.get(key):
                row[key] = item[key]
        operations: list[str] = []
        options = item.get("options")
        if isinstance(options, list) and options:
            head = space.heads.setdefault("SELECT", {})
            row["options"] = []
            for position, option in enumerate(options, start=1):
                key = f"{index}:{position}"
                head[key] = Candidate("SELECT", key, item, option_label=str(option.get("label", "")))
                row["options"].append({"index": key, "label": option.get("label")})
            operations.append("SELECT")
        else:
            if item.get("editable"):
                space.heads.setdefault("TYPE_TEXT", {})[index] = Candidate("TYPE_TEXT", index, item)
                operations.append("TYPE_TEXT")
            if not (item.get("editable") and str(item.get("expanded")) == "true"):
                space.heads.setdefault("CLICK", {})[index] = Candidate("CLICK", index, item)
                operations.append("CLICK")
        row["operations"] = operations
        space.elements.append(row)
    space.operations = [op for op in ("CLICK", "TYPE_TEXT", "SELECT") if op in space.heads]
    if snapshot.get("can_scroll_down"):
        space.operations.append("SCROLL_DOWN")
    if snapshot.get("can_scroll_up"):
        space.operations.append("SCROLL_UP")
    space.operations += ["WAIT", "DONE", "BLOCKED"]
    return space


def build_request(
    space: ActionSpace,
    snapshot: dict[str, Any],
    *,
    goal: str,
    history: list[dict[str, Any]],
    values: list[str],
    language: str,
    model: str,
) -> dict[str, Any]:
    """Assemble the ``{model, state, questions}`` body for one tick."""
    rules = prompts.OPERATION_RULES[language]
    questions: dict[str, Any] = {
        "operation": {
            "type": "choice",
            "criteria": {op: _OPERATION_LABELS[op] for op in space.operations},
            "instructions": {"goal": goal, "rules": rules},
        }
    }
    for operation, head in space.heads.items():
        questions[f"{operation.lower()}_target"] = {
            "type": "choice",
            "criteria": {
                key: {
                    "element": f"[{key}] {candidate.item.get('label', '')}",
                    "current_value": candidate.item.get("value") or "",
                    **({"option": candidate.option_label} if candidate.option_label else {}),
                    **{
                        k: candidate.item[k]
                        for k in ("role", "checked", "selected", "expanded", "region")
                        if candidate.item.get(k)
                    },
                }
                for key, candidate in head.items()
            },
            "instructions": {"goal": goal, "operation": operation, "rules": [rules, prompts.TARGET_RULES[language]]},
        }
    if values and "TYPE_TEXT" in space.heads:
        questions["text_value"] = {
            "type": "choice",
            "criteria": {**{value: value for value in values}, NONE_VALUE: "No offered value fits the chosen field."},
            "instructions": {"goal": goal, "rules": prompts.VALUE_RULES[language]},
        }
    return {
        "model": model,
        "state": {
            "page": {
                "url": snapshot.get("url", ""),
                "title": snapshot.get("title", ""),
                "text": snapshot.get("text", ""),
            },
            "elements": space.elements,
            "recent_actions": [{key: entry.get(key) for key in _HISTORY_KEYS} for entry in history[-10:]],
        },
        "questions": questions,
    }


def _unit_interval(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and 0 <= value <= 1


def validate_choice(answer: Any, ids: list[str]) -> dict[str, Any]:
    """Accept only a choice among ``ids`` whose distribution covers exactly ``ids`` and peaks at that choice."""
    fields = answer if isinstance(answer, dict) else {}
    choice, distribution, confidence = fields.get("choice"), fields.get("probabilities"), fields.get("confidence")
    accepted = (
        isinstance(distribution, dict)
        and choice in ids
        and set(distribution) == set(ids)
        and _unit_interval(confidence)
        and all(_unit_interval(weight) for weight in distribution.values())
        and math.isclose(sum(distribution.values()), 1.0, abs_tol=0.02)
        and distribution[choice] >= max(distribution.values()) - 1e-6
    )
    if not accepted:
        raise build_error(StatusCode.MODEL_CALL_FAILED, error_msg="decisions answer is not an offered choice")
    return answer


def interpret(result: dict[str, Any], space: ActionSpace, body: dict[str, Any], latency_ms: int) -> Decision:
    """Map a validated answer set onto one candidate (or a control operation)."""
    answers = result.get("answers") or {}
    operation_answer = validate_choice(answers.get("operation"), list(body["questions"]["operation"]["criteria"]))
    operation = str(operation_answer["choice"])
    candidate: Candidate | None = None
    if operation in space.heads:
        head_name = f"{operation.lower()}_target"
        target_answer = validate_choice(answers.get(head_name), list(body["questions"][head_name]["criteria"]))
        candidate = space.heads[operation][str(target_answer["choice"])]
    value_choice = ""
    if operation == "TYPE_TEXT" and "text_value" in body["questions"]:
        value_answer = validate_choice(answers.get("text_value"), list(body["questions"]["text_value"]["criteria"]))
        value_choice = str(value_answer["choice"])
    return Decision(
        operation=operation,
        candidate=candidate,
        value_choice=value_choice,
        confidence=float(operation_answer["confidence"]),
        operation_probabilities=dict(operation_answer["probabilities"]),
        latency_ms=latency_ms,
        usage=dict(result.get("usage") or {}),
        model=str(result.get("model") or body["model"]),
    )


class JevDecisionsClient:
    """Async HTTP client for the decisions endpoint with a warm keep-alive connection."""

    def __init__(self, *, api_key: str, url: str, model: str, timeout_s: float) -> None:
        if not api_key:
            raise build_error(StatusCode.MODEL_SERVICE_CONFIG_ERROR, error_msg="decisions api key is empty")
        self.url = url
        self.model = model
        self._client = httpx.AsyncClient(timeout=timeout_s, headers={"Authorization": f"Bearer {api_key}"})

    async def warm(self) -> None:
        """Open the TLS connection ahead of the first decision; failures here are ignored."""
        try:
            await self._client.get(self.url.rsplit("/", 1)[0] + "/", timeout=5.0)
        except httpx.HTTPError:
            return None

    async def decide(self, body: dict[str, Any]) -> tuple[dict[str, Any], int]:
        started = time.perf_counter()
        for attempt in range(3):
            try:
                response = await self._client.post(self.url, json=body)
            except httpx.HTTPError as exc:
                raise build_error(
                    StatusCode.MODEL_CALL_FAILED, cause=exc, error_msg="decisions connection failed"
                ) from exc
            if response.status_code in _RETRY_STATUSES and attempt < 2:
                await asyncio.sleep(0.5 * 2**attempt)
                continue
            if response.is_error:
                raise build_error(
                    StatusCode.MODEL_CALL_FAILED,
                    error_msg=f"decisions endpoint returned HTTP {response.status_code}: {response.text[:300]}",
                )
            return response.json(), round((time.perf_counter() - started) * 1000)
        raise build_error(StatusCode.MODEL_CALL_FAILED, error_msg="decisions endpoint unavailable")

    async def close(self) -> None:
        await self._client.aclose()
