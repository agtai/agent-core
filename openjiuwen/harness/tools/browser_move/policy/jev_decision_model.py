# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""``JevDecisionModel``: TypeSafe Jev in the browser subagent's model slot.

On a browser turn (the tool list contains ``browser_click``) it settles and probes the page through
the bound ``BrowserAgentRuntime``, asks Jev once, and returns exactly one ``browser_*`` tool call.
Every other call (summaries, value generation) goes to the wrapped chat model.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import statistics
import time
from dataclasses import dataclass, field
from typing import Any, AsyncIterator
from urllib.parse import urlparse

from openjiuwen.core.common.exception.codes import StatusCode
from openjiuwen.core.common.exception.errors import BaseError, build_error
from openjiuwen.core.common.logging import logger
from openjiuwen.core.foundation.llm import AssistantMessage, AssistantMessageChunk, ToolCall
from openjiuwen.core.foundation.llm.model import Model
from openjiuwen.harness.tools.browser_move.policy import prompts
from openjiuwen.harness.tools.browser_move.policy.jev_decisions import (
    DECISIONS_TIMEOUT_S,
    DEFAULT_DECISIONS_URL,
    DEFAULT_MODEL,
    NONE_VALUE,
    Decision,
    JevDecisionsClient,
    build_action_space,
    build_request,
    interpret,
)
from openjiuwen.harness.tools.browser_move.policy.probe_js import POLICY_PROBE_JS, STAMP_ATTRIBUTE

BROWSER_TURN_TOOL = "browser_click"
MAX_CONSECUTIVE_WAITS = 5
MAX_NO_CHANGE_STEPS = 3
MAX_PREFETCHED_FIELDS = 8
PROBE_SETTLE_MS = 500  # must equal probe_js.py's JS default so an un-escalated probe's timing is unchanged
PROBE_QUIET_MS = 60  # must equal probe_js.py's JS default DOM-quiet window
ACTION_SETTLE_START_MS = 250  # first in-page wait for an action whose effect has not shown yet
ACTION_SETTLE_BUDGET_MS = 1000  # total in-page wait one action gets before the classifier sees an unchanged page
MAX_PROBE_SETTLE_MS = 1500  # keeps load(3s)+settle+1s JS lastResort >=1s under the 30s transport request timeout
WAIT_SETTLE_BUDGET_MS = 3000  # total in-page settle time one WAIT streak may spend before the step gives up
_URL_RE = re.compile(r"https?://[^\s'\"<>]+")


@dataclass
class _Run:
    """One task's worth of Jev state, from the goal turn to a terminal ``DONE``/``BLOCKED``.

    A fresh ``_Run`` is the unit of isolation between tasks served by the same model instance: its
    goal, history, pending action and prefetched values never leak into the run that follows it.
    """

    goal: str
    started_at: float
    history: list[dict[str, Any]] = field(default_factory=list)
    pending: dict[str, Any] | None = None
    prefetched: dict[str, asyncio.Task[str | None]] = field(default_factory=dict)
    values: list[str] = field(default_factory=list)
    values_task: asyncio.Task[list[str]] | None = None
    tick: int = 0
    consecutive_waits: int = 0
    settle_spent_ms: int = 0  # in-page wait already spent on the current WAIT streak; resets with consecutive_waits
    ticks: list[dict[str, Any]] = field(default_factory=list)
    finished: bool = False


class JevDecisionModel(Model):
    """A ``Model`` whose browser-turn answers come from TypeSafe Jev instead of a chat model."""

    def __init__(
        self,
        fallback: Model,
        *,
        language: str,
        client: JevDecisionsClient | None,
        goal_value_cache: bool,
        prefetch_values: bool,
        value_model: Model | None,
    ) -> None:
        """``goal_value_cache`` offers goal-extracted values to Jev as a choice head; off means every typed
        value comes from the chat model given the field and page context. ``prefetch_values`` makes that
        chat-model call for every editable field as soon as a probe shows it, one per field per document,
        whether or not Jev ever types there; off makes it only when TYPE_TEXT is chosen."""
        super().__init__(fallback.model_client_config, fallback.model_config)
        self._fallback = fallback
        self._value_model = value_model or fallback  # typed values want a fast small model; chat turns do not
        self._goal_value_cache = goal_value_cache
        self._prefetch_enabled = prefetch_values
        self._language = language if language in prompts.OPERATION_RULES else "en"
        self._decisions = client or JevDecisionsClient(
            api_key=os.getenv("TYPESAFE_API_KEY") or os.getenv("OPENROUTER_API_KEY") or "",
            url=os.getenv("TYPESAFE_API_URL") or DEFAULT_DECISIONS_URL,
            model=os.getenv("TYPESAFE_MODEL") or DEFAULT_MODEL,
            timeout_s=DECISIONS_TIMEOUT_S,
        )
        self._runtime: Any = None
        self._run: _Run | None = None

    # -- wiring -------------------------------------------------------------

    def bind_runtime(self, runtime: Any) -> None:
        self._runtime = runtime

    # -- public accessors (delegate to the current/most recent run) ---------

    @property
    def ticks(self) -> list[dict[str, Any]]:
        return self._run.ticks if self._run is not None else []

    @property
    def started_at(self) -> float:
        return self._run.started_at if self._run is not None else 0.0

    # -- Model surface ------------------------------------------------------

    async def invoke(self, messages: Any, *, tools: Any = None, **kwargs: Any) -> AssistantMessage:
        if not self._is_browser_turn(tools):
            return await self._fallback.invoke(messages, tools=tools, **kwargs)
        return await self._decide_message(messages)

    async def stream(self, messages: Any, *, tools: Any = None, **kwargs: Any) -> AsyncIterator[AssistantMessageChunk]:
        if not self._is_browser_turn(tools):
            async for chunk in self._fallback.stream(messages, tools=tools, **kwargs):
                yield chunk
            return
        message = await self._decide_message(messages)
        yield AssistantMessageChunk(
            content=message.content, tool_calls=message.tool_calls, finish_reason=message.finish_reason
        )

    # -- one tick -----------------------------------------------------------

    @staticmethod
    def _is_browser_turn(tools: Any) -> bool:
        return any(getattr(tool, "name", None) == BROWSER_TURN_TOOL for tool in tools or [])

    @staticmethod
    def _cancel_run_tasks(run: _Run) -> None:
        """Cancel a superseded run's background work so it cannot leak into the run that follows it."""
        if run.values_task is not None:
            run.values_task.cancel()
        for task in run.prefetched.values():
            task.cancel()
        run.prefetched.clear()

    async def _decide_message(self, messages: Any) -> AssistantMessage:
        if self._runtime is None:
            raise build_error(
                StatusCode.MODEL_SERVICE_CONFIG_ERROR, error_msg="JevDecisionModel.bind_runtime() was not called"
            )
        goal = self._goal_from(messages)
        run = self._run
        if run is None or run.finished or goal != run.goal:
            if run is not None:
                self._cancel_run_tasks(run)
            run = _Run(goal=goal, started_at=time.perf_counter())
            self._run = run
            if self._goal_value_cache:
                run.values_task = asyncio.create_task(self._extract_values(goal))
            await self._decisions.warm()
            url = _URL_RE.search(goal)
            if url:
                nav_args = {"url": url.group(0).rstrip(".,)")}
                return self._tool_message(run, "browser_navigate", nav_args, label="navigate")

        snapshot, probe_ms = await self._probe(run)
        settle_probes, settle_ms = 0, 0
        for _ in range(MAX_CONSECUTIVE_WAITS + 1):
            space = build_action_space(snapshot)
            values = run.values if run.values_task is None or run.values_task.done() else []
            if run.values_task is not None and run.values_task.done() and not run.values:
                try:
                    run.values = run.values_task.result()
                except Exception:  # noqa: BLE001 - a failed goal-value extraction degrades to an empty value list
                    logger.warning("[JevDecisionModel] goal-value extraction failed", exc_info=True)
                    run.values = []
                values = run.values
            body = build_request(
                space,
                snapshot,
                goal=run.goal,
                history=run.history,
                values=values,
                language=self._language,
                model=self._decisions.model,
            )
            try:
                result, jev_ms = await self._decisions.decide(body)
                decision = interpret(result, space, body, jev_ms)
            except BaseError as exc:
                logger.warning("[JevDecisionModel] decisions request failed: %s", exc, exc_info=True)
                return self._final(run, "BLOCKED", snapshot, f"decisions request failed: {exc}")
            run.tick += 1
            record = {
                "tick": run.tick,
                "probe_ms": probe_ms,
                "settle_probes": settle_probes,
                "settle_ms": settle_ms,
                "action_settle_probes": snapshot.get("action_settle_probes", 0),
                "action_settle_ms": snapshot.get("action_settle_ms", 0),
                "jev_ms": jev_ms,
                "operation": decision.operation,
                "target": decision.candidate.item.get("label") if decision.candidate else None,
                "confidence": round(decision.confidence, 3),
                "elements": len(space.elements),
                "elapsed_ms": round((time.perf_counter() - run.started_at) * 1000),
            }
            run.ticks.append(record)
            if decision.operation == "WAIT":
                run.consecutive_waits += 1
                run.history.append({"action": "wait", "kind": "wait", "text": None, "page_changed": None})
                if run.consecutive_waits > MAX_CONSECUTIVE_WAITS:
                    return self._final(run, "BLOCKED", snapshot, "waited without progress")
                snapshot, settle_probes, settle_ms, progressed = await self._settle_wait(run, snapshot)
                if not progressed:
                    return self._final(run, "BLOCKED", snapshot, "waited without progress")
                probe_ms = settle_ms
                continue
            run.consecutive_waits = 0
            run.settle_spent_ms = 0
            if decision.operation in {"DONE", "BLOCKED"}:
                return self._final(run, decision.operation, snapshot, "")
            return await self._act(run, decision, snapshot, record)
        return self._final(run, "BLOCKED", snapshot, "waited without progress")

    async def _settle_wait(self, run: _Run, asked_snapshot: dict[str, Any]) -> tuple[dict[str, Any], int, int, bool]:
        """Spend a WAIT verdict in-page instead of paying another decisions request.

        The classifier's answer on an unchanged page cannot change until the page does, so a decision
        request spent re-asking it is pure latency; re-probing costs nothing by comparison. Escalates
        ``settle_ms`` by doubling from ``PROBE_SETTLE_MS`` (clamped to ``MAX_PROBE_SETTLE_MS``) on every
        retry, and stops once either the probed ``page_key`` differs from the one the classifier just saw
        or ``WAIT_SETTLE_BUDGET_MS`` is spent -- a page that never goes DOM-quiet (a CSS animation, a
        ticking clock) would otherwise burn its full settle cap forever.

        The budget spans the whole WAIT streak (``run.settle_spent_ms``), not one verdict: a per-call
        budget would be re-granted on every WAIT and let ``MAX_CONSECUTIVE_WAITS`` verdicts stretch one
        stuck step to minutes. The returned flag is ``False`` once that budget is gone, and the caller
        must treat it as terminal -- re-asking the classifier about a snapshot it already answered WAIT
        on buys the same answer at full request cost. ``MAX_CONSECUTIVE_WAITS`` still bounds the other
        shape of stuck, where ``page_key`` keeps flipping but the page stays unactionable.
        """
        asked_page_key = asked_snapshot.get("page_key")
        snapshot = asked_snapshot
        settle_ms = PROBE_SETTLE_MS
        probes = 0
        measured_ms = 0
        while run.settle_spent_ms < WAIT_SETTLE_BUDGET_MS:
            requested_ms = min(settle_ms, WAIT_SETTLE_BUDGET_MS - run.settle_spent_ms)
            snapshot, probe_ms = await self._probe(run, settle_ms=requested_ms)
            probes += 1
            measured_ms += probe_ms
            run.settle_spent_ms += requested_ms
            if snapshot.get("page_key") != asked_page_key:
                return snapshot, probes, measured_ms, True
            settle_ms = min(settle_ms * 2, MAX_PROBE_SETTLE_MS)
        return snapshot, probes, measured_ms, False

    async def _probe(self, run: _Run, *, settle_ms: int = PROBE_SETTLE_MS) -> tuple[dict[str, Any], int]:
        started = time.perf_counter()
        pending, run.pending = run.pending, None
        after = {"kind": pending["kind"], "node": pending["node"]} if pending is not None else None
        snapshot = await self._raw_probe(settle_ms=settle_ms, quiet_ms=PROBE_QUIET_MS, after=after)
        if pending is not None:
            changed = snapshot.get("page_key") != pending["page_key"]
            if not changed:
                snapshot, changed = await self._settle_action(run, snapshot, pending["page_key"])
            pending["entry"]["page_changed"] = changed
            recent = run.history[-MAX_NO_CHANGE_STEPS:]
            if len(recent) == MAX_NO_CHANGE_STEPS and all(
                h["page_changed"] is False and h["kind"] != "wait" for h in recent
            ):
                snapshot["stalled"] = True
        self._prefetch_values(run, snapshot)
        return snapshot, round((time.perf_counter() - started) * 1000)

    async def _raw_probe(self, *, settle_ms: int, quiet_ms: int, after: dict[str, Any] | None) -> dict[str, Any]:
        params = {
            "stamp_attribute": STAMP_ATTRIBUTE,
            "after": after,
            "max_items": 250,
            "settle_ms": settle_ms,
            "quiet_ms": quiet_ms,
        }
        snapshot = await self._runtime.probe_for_policy(POLICY_PROBE_JS, params)
        if snapshot.get("error"):
            logger.warning("[JevDecisionModel] policy probe reported %s", snapshot["error"])
        if snapshot.get("visibility") == "hidden" and await self._runtime.activate_page(str(snapshot.get("url") or "")):
            logger.info("[JevDecisionModel] activated hidden tab %s", snapshot.get("url"))
            snapshot = await self._runtime.probe_for_policy(POLICY_PROBE_JS, params)
        return snapshot

    async def _settle_action(
        self, run: _Run, snapshot: dict[str, Any], before_key: Any
    ) -> tuple[dict[str, Any], bool]:
        """Wait in-page for an action's delayed effect (a closing dialog, a late re-render) before deciding.

        A probe right after the action can return before the effect shows; the classifier would then be
        asked about the page it already acted on and answer WAIT at full request cost. The waits double
        from ``ACTION_SETTLE_START_MS`` and are wall-clock waits (``quiet_ms`` equals the window, so a quiet
        DOM does not cut them short). ``ACTION_SETTLE_BUDGET_MS`` bounds the penalty for an action that
        truly did nothing, and the time counts against the WAIT streak budget so one step never settles
        longer than ``WAIT_SETTLE_BUDGET_MS`` in total.
        """
        wait_ms, spent, probes = ACTION_SETTLE_START_MS, 0, 0
        while spent < ACTION_SETTLE_BUDGET_MS:
            requested = min(wait_ms, ACTION_SETTLE_BUDGET_MS - spent)
            snapshot = await self._raw_probe(settle_ms=requested, quiet_ms=requested, after=None)
            probes += 1
            spent += requested
            run.settle_spent_ms += requested
            if snapshot.get("page_key") != before_key:
                break
            wait_ms = min(wait_ms * 2, MAX_PROBE_SETTLE_MS)
        snapshot["action_settle_probes"] = probes
        snapshot["action_settle_ms"] = spent
        return snapshot, snapshot.get("page_key") != before_key

    async def _act(
        self, run: _Run, decision: Decision, snapshot: dict[str, Any], record: dict[str, Any]
    ) -> AssistantMessage:
        if snapshot.get("stalled"):
            return self._final(run, "BLOCKED", snapshot, "three actions without page change")
        candidate = decision.candidate
        generation_id = str(snapshot.get("generation_id") or "")
        text: str | None = None
        match decision.operation:
            case "CLICK":
                name, args, kind = (
                    "browser_click",
                    {"generation_id": generation_id, "target_id": candidate.item["target_id"]},
                    "click",
                )
            case "TYPE_TEXT":
                text, record["value_source"], record["value_ms"] = await self._value_for(run, decision, snapshot)
                if text is None:
                    return self._final(run, "BLOCKED", snapshot, f"no value for field {candidate.item.get('label')!r}")
                name, kind = "browser_type", "fill"
                args = {
                    "generation_id": generation_id,
                    "target_id": candidate.item["target_id"],
                    "text": text,
                    "clear": True,
                }
            case "SELECT":
                name, kind = "browser_select_option", "select"
                args = {
                    "generation_id": generation_id,
                    "target_id": candidate.item["target_id"],
                    "label": candidate.option_label,
                }
            case "SCROLL_DOWN":
                name, args, kind = "browser_press_key", {"key": "PageDown"}, "scroll"
            case "SCROLL_UP":
                name, args, kind = "browser_press_key", {"key": "PageUp"}, "scroll"
            case _:
                return self._final(run, "BLOCKED", snapshot, f"unsupported operation {decision.operation}")
        label = candidate.item.get("label", "") if candidate else decision.operation
        if candidate and candidate.option_label:
            label = f"{label} → {candidate.option_label}"
        entry = {"action": label, "kind": kind, "text": text, "page_changed": None}
        run.history.append(entry)
        run.pending = {
            "kind": kind,
            "node": candidate.item.get("node") if candidate else None,
            "page_key": snapshot.get("page_key"),
            "entry": entry,
        }
        record["text"] = text
        return self._tool_message(run, name, args, label=label)

    @staticmethod
    def _document_key(snapshot: dict[str, Any]) -> str:
        url = urlparse(str(snapshot.get("url") or ""))
        return f"{url.netloc}{url.path}"

    @classmethod
    def _field_key(cls, item: dict[str, Any], snapshot: dict[str, Any]) -> str:
        return f"{cls._document_key(snapshot)}|{item.get('node')}|{item.get('label', '')}"

    def _prefetch_values(self, run: _Run, snapshot: dict[str, Any]) -> None:
        """Start value generation for every editable field now, so a later TYPE_TEXT does not wait for it.

        One call per field per document: the key is the stamp id plus label under the URL path, so a value
        generated on the first probe stays usable while the page fills in, and a navigation cancels the previous
        document's outstanding calls. Prefilled fields are included: a wrong default (the site's guessed origin)
        is replaced as often as an empty field is filled.
        """
        if not self._prefetch_enabled:
            return
        scope = self._document_key(snapshot) + "|"
        for key in [key for key in run.prefetched if not key.startswith(scope)]:
            run.prefetched.pop(key).cancel()
        for item in snapshot.get("elements") or []:
            if not item.get("editable") or not item.get("target_id"):
                continue
            key = self._field_key(item, snapshot)
            if key not in run.prefetched and len(run.prefetched) < MAX_PREFETCHED_FIELDS:
                run.prefetched[key] = asyncio.create_task(self._generate_value(run, item, snapshot))

    async def _value_for(self, run: _Run, decision: Decision, snapshot: dict[str, Any]) -> tuple[str | None, str, int]:
        started = time.perf_counter()
        if decision.value_choice and decision.value_choice != NONE_VALUE:
            return decision.value_choice, "cache", round((time.perf_counter() - started) * 1000)
        field_item = decision.candidate.item if decision.candidate else {}
        task = run.prefetched.pop(self._field_key(field_item, snapshot), None)
        if task is not None:
            try:
                value = await task
            except Exception:  # noqa: BLE001 - a failed prefetch degrades to _act's "no value" BLOCKED path
                logger.warning("[JevDecisionModel] prefetched value generation failed", exc_info=True)
                value = None
            return value, "prefetch", round((time.perf_counter() - started) * 1000)
        generated = await self._generate_value(run, field_item, snapshot)
        return generated, "llm", round((time.perf_counter() - started) * 1000)

    async def _generate_value(self, run: _Run, field_item: dict[str, Any], snapshot: dict[str, Any]) -> str | None:
        context = {
            "goal": run.goal,
            "field": {k: field_item.get(k) for k in ("label", "role", "value", "region")},
            "page": {"title": snapshot.get("title"), "text": str(snapshot.get("text", ""))[:6000]},
            "recent_actions": [{k: h.get(k) for k in ("action", "text")} for h in run.history[-6:]],
        }
        reply = await self._value_model.invoke(
            [
                {"role": "system", "content": prompts.VALUE_GENERATION[self._language]},
                {"role": "user", "content": json.dumps(context, ensure_ascii=False)},
            ],
            max_tokens=256,
            temperature=0,
            response_format={"type": "json_object"},
        )
        value = _json_field(reply.content, "text")
        return value.strip() if isinstance(value, str) and value.strip() else None

    async def _extract_values(self, goal: str) -> list[str]:
        try:
            reply = await self._fallback.invoke(
                [
                    {"role": "system", "content": prompts.VALUE_EXTRACTION[self._language]},
                    {"role": "user", "content": goal},
                ],
                max_tokens=256,
                temperature=0,
                response_format={"type": "json_object"},
            )
        except Exception:  # noqa: BLE001 - value cache is an optimisation; the per-field LLM path remains
            logger.warning(
                "[JevDecisionModel] value extraction failed; falling back to per-field generation", exc_info=True
            )
            return []
        values = _json_field(reply.content, "values")
        return [str(v) for v in values if str(v).strip()][:24] if isinstance(values, list) else []

    # -- messages -----------------------------------------------------------

    def _tool_message(self, run: _Run, name: str, args: dict[str, Any], *, label: str) -> AssistantMessage:
        call = ToolCall(id=f"jev-{run.tick}-{len(run.history)}", type="function", name=name, arguments=json.dumps(args))
        return AssistantMessage(content="", tool_calls=[call], finish_reason="tool_calls")

    def _final(self, run: _Run, status: str, snapshot: dict[str, Any], reason: str) -> AssistantMessage:
        run.finished = True
        summary = {
            "status": status,
            "reason": reason,
            "url": snapshot.get("url"),
            "title": snapshot.get("title"),
            "steps": len([h for h in run.history if h["kind"] != "wait"]),
            "elapsed_ms": round((time.perf_counter() - run.started_at) * 1000),
            "page_text": str(snapshot.get("text", ""))[:2000],
        }
        return AssistantMessage(content=json.dumps(summary, ensure_ascii=False), finish_reason="stop")

    @staticmethod
    def _goal_from(messages: Any) -> str:
        goal = ""
        for message in messages or []:
            role = getattr(message, "role", None) or (message.get("role") if isinstance(message, dict) else None)
            if role == "user":
                content = getattr(message, "content", None) or (
                    message.get("content") if isinstance(message, dict) else ""
                )
                goal = content if isinstance(content, str) else json.dumps(content, ensure_ascii=False)
        return goal.strip()

    def report(self) -> dict[str, Any]:
        """Timing summary in the shape of jev-ultrafast's performance notes, for the current/most recent run."""
        run = self._run
        if run is None:
            return {
                "elapsed_ms": 0,
                "jev_requests": 0,
                "interactions": 0,
                "waits": 0,
                "median_jev_ms": 0,
                "median_probe_ms": 0,
                "settle_probes": 0,
                "settle_ms": 0,
                "values": {"cache": 0, "prefetch": 0, "llm": 0},
                "history": [],
            }
        jev = [t["jev_ms"] for t in run.ticks]
        return {
            "elapsed_ms": round((time.perf_counter() - run.started_at) * 1000) if run.started_at else 0,
            "jev_requests": len(jev),
            "interactions": len([h for h in run.history if h["kind"] != "wait"]),
            "waits": len([h for h in run.history if h["kind"] == "wait"]),
            "median_jev_ms": int(statistics.median(jev)) if jev else 0,
            "median_probe_ms": int(statistics.median(t["probe_ms"] for t in run.ticks)) if run.ticks else 0,
            "settle_probes": sum(t.get("settle_probes", 0) for t in run.ticks),
            "settle_ms": sum(t.get("settle_ms", 0) for t in run.ticks),
            "values": {
                source: len([t for t in run.ticks if t.get("value_source") == source])
                for source in ("cache", "prefetch", "llm")
            },
            "history": run.history,
        }


def _json_field(content: Any, key: str) -> Any:
    try:
        return json.loads(content if isinstance(content, str) else "{}").get(key)
    except (ValueError, AttributeError):
        return None
