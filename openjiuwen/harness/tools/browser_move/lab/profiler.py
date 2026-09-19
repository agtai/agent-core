# coding: utf-8
"""Lab profiler: where a Jev-driven browser run spends its wall clock, reported in jev-ultrafast's units."""

from __future__ import annotations

import functools
import json
import statistics
import time
from dataclasses import dataclass, field
from typing import Any, Callable
from urllib.parse import urlparse

from openjiuwen.core.single_agent.ability_manager import AbilityManager
from openjiuwen.harness.tools.browser_move.backends.browser_use.transport import SidecarTransport
from openjiuwen.harness.tools.browser_move.policy.jev_decision_model import JevDecisionModel
from openjiuwen.harness.tools.browser_move.policy.jev_decisions import JevDecisionsClient
from openjiuwen.harness.tools.browser_move.runtime.runtime import BrowserAgentRuntime

BROWSER_ACTION_TOOLS = frozenset({"browser_click", "browser_type", "browser_select_option", "browser_press_key"})


@dataclass
class Span:
    kind: str
    name: str
    t0: float
    t1: float = 0.0
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def ms(self) -> int:
        return round((self.t1 - self.t0) * 1000)

    def contains(self, other: "Span") -> bool:
        return self.t0 <= other.t0 and other.t1 <= self.t1


def _tool_names(tool_call: Any) -> str:
    calls = tool_call if isinstance(tool_call, list) else [tool_call]
    return ",".join(str(getattr(call, "name", "?")) for call in calls)


def _turn_name(tools: Any) -> str:
    return "browser" if JevDecisionModel._is_browser_turn(tools) else "chat"


class JevProfiler:
    """Patches the seams of one run (model turns, probes, decisions, tools, sidecar wire) and totals them.

    The window that jev-ultrafast times is "first decision request through the final DONE/BLOCKED turn";
    ``report()`` uses the same window so the two agents are compared on the same clock.
    """

    def __init__(self) -> None:
        self.spans: list[Span] = []
        self._originals: list[tuple[type, str, Any]] = []

    # -- instrumentation ----------------------------------------------------

    def _open(self, kind: str, name: str) -> Span:
        span = Span(kind=kind, name=name, t0=time.perf_counter())
        self.spans.append(span)
        return span

    def _patch(self, cls: type, attr: str, kind: str, name_of: Callable[..., str]) -> None:
        original = getattr(cls, attr)
        self._originals.append((cls, attr, original))

        @functools.wraps(original)
        async def timed(*args: Any, **kwargs: Any) -> Any:
            span = self._open(kind, name_of(*args, **kwargs))
            try:
                return await original(*args, **kwargs)
            finally:
                span.t1 = time.perf_counter()

        setattr(cls, attr, timed)

    def _patch_turns(self) -> None:
        original_invoke, original_stream = JevDecisionModel.invoke, JevDecisionModel.stream
        self._originals.append((JevDecisionModel, "invoke", original_invoke))
        self._originals.append((JevDecisionModel, "stream", original_stream))

        @functools.wraps(original_invoke)
        async def invoke(policy: Any, messages: Any, *, tools: Any = None, **kwargs: Any) -> Any:
            span = self._open("turn", _turn_name(tools))
            try:
                message = await original_invoke(policy, messages, tools=tools, **kwargs)
                span.meta["finish_reason"] = message.finish_reason
                if not message.tool_calls:
                    span.meta["content"] = message.content
                return message
            finally:
                span.t1 = time.perf_counter()

        @functools.wraps(original_stream)
        async def stream(policy: Any, messages: Any, *, tools: Any = None, **kwargs: Any) -> Any:
            span = self._open("turn", _turn_name(tools))
            try:
                async for chunk in original_stream(policy, messages, tools=tools, **kwargs):
                    if chunk.finish_reason:
                        span.meta["finish_reason"] = chunk.finish_reason
                    if chunk.content and not chunk.tool_calls:
                        span.meta["content"] = span.meta.get("content", "") + chunk.content
                    yield chunk
            finally:
                span.t1 = time.perf_counter()

        JevDecisionModel.invoke = invoke  # type: ignore[method-assign]
        JevDecisionModel.stream = stream  # type: ignore[method-assign]

    def _patch_decide(self) -> None:
        original = JevDecisionsClient.decide
        self._originals.append((JevDecisionsClient, "decide", original))

        @functools.wraps(original)
        async def decide(client: Any, body: dict[str, Any]) -> Any:
            span = self._open("jev", str(body.get("model", "")))
            span.meta["elements"] = [f"[{row['index']}] {row.get('label', '')}" for row in body["state"]["elements"]]
            span.meta["operations"] = list(body["questions"]["operation"]["criteria"])
            try:
                result, latency = await original(client, body)
                span.meta["answers"] = {
                    head: {"choice": answer.get("choice"), "confidence": answer.get("confidence")}
                    for head, answer in (result.get("answers") or {}).items()
                }
                return result, latency
            finally:
                span.t1 = time.perf_counter()

        JevDecisionsClient.decide = decide  # type: ignore[method-assign]

    def attach(self) -> "JevProfiler":
        self._patch_decide()
        self._patch(
            BrowserAgentRuntime,
            "probe_for_policy",
            "probe",
            lambda rt, source, params: f"settle={params.get('settle_ms')}",
        )
        self._patch(BrowserAgentRuntime, "activate_page", "activate", lambda rt, url: str(url))
        self._patch(
            JevDecisionModel, "_generate_value", "value", lambda m, run, item, snap: str(item.get("label", ""))
        )
        self._patch(JevDecisionModel, "_extract_values", "values", lambda m, goal: "goal")
        self._patch(AbilityManager, "execute", "tool", lambda mgr, ctx, tool_call, *a, **k: _tool_names(tool_call))
        self._patch(SidecarTransport, "request", "wire", lambda transport, method, *a, **k: str(method))
        self._patch_turns()
        return self

    def detach(self) -> None:
        for cls, attr, original in reversed(self._originals):
            setattr(cls, attr, original)
        self._originals.clear()

    # -- report -------------------------------------------------------------

    def report(self, policy: JevDecisionModel) -> dict[str, Any]:
        spans = sorted((s for s in self.spans if s.t1), key=lambda s: s.t0)
        turns = [s for s in spans if s.kind == "turn"]
        browser_turns = [s for s in turns if s.name == "browser"]
        jev = [s for s in spans if s.kind == "jev"]
        if not jev or not browser_turns:
            return {"error": "no decision request recorded", "spans": len(spans)}
        final = next((s for s in reversed(browser_turns) if s.meta.get("finish_reason") == "stop"), browser_turns[-1])
        start, end = jev[0].t0, final.t1
        inside = [s for s in spans if s.t1 > start and s.t0 < end]

        def clipped(span: Span) -> int:
            return round((min(span.t1, end) - max(span.t0, start)) * 1000)

        def total(kind: str) -> int:
            """Union of the kind's intervals inside the window, so a turn served through both ``stream`` and
            ``invoke`` (or a nested tool call) is not counted twice."""
            merged: list[list[float]] = []
            for s in sorted((s for s in inside if s.kind == kind), key=lambda s: s.t0):
                t0, t1 = max(s.t0, start), min(s.t1, end)
                if merged and t0 <= merged[-1][1]:
                    merged[-1][1] = max(merged[-1][1], t1)
                else:
                    merged.append([t0, t1])
            return round(sum(t1 - t0 for t0, t1 in merged) * 1000)

        def nested_wire(parent_kind: str) -> int:
            parents = [p for p in inside if p.kind == parent_kind]
            return sum(clipped(w) for w in inside if w.kind == "wire" and any(p.contains(w) for p in parents))

        def by_name(kind: str, source: list[Span]) -> dict[str, dict[str, int]]:
            grouped: dict[str, list[int]] = {}
            for s in source:
                if s.kind == kind:
                    grouped.setdefault(s.name, []).append(clipped(s))
            return {name: {"count": len(ms), "ms": sum(ms)} for name, ms in grouped.items()}

        window_ms = round((end - start) * 1000)
        turn_ms, tool_ms = total("turn"), total("tool")
        jev_ms, probe_ms, activate_ms = total("jev"), total("probe"), total("activate")
        value_wait_ms = sum(int(t.get("value_ms") or 0) for t in policy.ticks)
        values = [s for s in spans if s.kind == "value"]
        summary = policy.report()
        final_state = _final_state(final.meta.get("content"))
        return {
            "elapsed_ms": window_ms,
            "timing": "First decision request through the final DONE/BLOCKED turn; navigation, page load and the "
            "first probe are excluded, as in jev-ultrafast.",
            "run_ms": round((spans[-1].t1 - turns[0].t0) * 1000),
            "decision_requests": len(jev),
            "decision_median_ms": int(statistics.median(s.ms for s in jev)),
            "decision_total_ms": sum(s.ms for s in jev),
            "browser_actions": len([s for s in inside if s.kind == "tool" and s.name in BROWSER_ACTION_TOOLS]),
            "wait_actions": summary["waits"],
            "text_calls": [{"field": s.name, "latency_ms": s.ms} for s in values],
            "phases": {
                "jev": jev_ms,
                "probe": probe_ms,
                "probe_wire": nested_wire("probe"),
                "activate": activate_ms,
                "value_wait": value_wait_ms,
                "model_overhead": turn_ms - jev_ms - probe_ms - activate_ms - value_wait_ms,
                "tool": tool_ms,
                "tool_wire": nested_wire("tool"),
                "framework": window_ms - turn_ms - tool_ms,
            },
            "tools": by_name("tool", inside),
            "wire": by_name("wire", inside),
            "background_value_ms": sum(s.ms for s in values),
            "steps": self._steps(browser_turns, spans, start, end),
            "ticks": policy.ticks,
            "final": final_state,
            "verified": final_state.get("status") == "DONE"
            and urlparse(str(final_state.get("url") or "")).path == "/travel/flights/search",
            "spans": [
                {"kind": s.kind, "name": s.name, "t0_ms": round((s.t0 - start) * 1000), "ms": s.ms, **s.meta}
                for s in spans
                if s.kind != "turn"
            ],
        }

    @staticmethod
    def _steps(browser_turns: list[Span], spans: list[Span], start: float, end: float) -> list[dict[str, Any]]:
        steps: list[dict[str, Any]] = []
        for index, turn in enumerate(browser_turns):
            if turn.t1 < start or turn.t0 > end:
                continue
            following = browser_turns[index + 1] if index + 1 < len(browser_turns) else None
            limit = following.t0 if following else end
            tool = next((s for s in spans if s.kind == "tool" and turn.t1 <= s.t0 <= limit), None)
            probes = [s for s in spans if s.kind == "probe" and turn.contains(s)]
            decisions = [s for s in spans if s.kind == "jev" and turn.contains(s)]
            steps.append(
                {
                    "turn_ms": turn.ms,
                    "probes": len(probes),
                    "probe_ms": sum(s.ms for s in probes),
                    "jev": len(decisions),
                    "jev_ms": sum(s.ms for s in decisions),
                    "tool": tool.name if tool else "",
                    "tool_ms": tool.ms if tool else 0,
                    "tool_wire_ms": sum(w.ms for w in spans if w.kind == "wire" and tool and tool.contains(w)),
                    "gap_ms": round((limit - turn.t1) * 1000) - (tool.ms if tool else 0),
                    "finish": turn.meta.get("finish_reason", ""),
                }
            )
        return steps


def _final_state(content: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(content) if isinstance(content, str) else {}
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def render(report: dict[str, Any]) -> str:
    """Console view: the jev-ultrafast headline numbers, the phase split, then one line per step."""
    if "error" in report:
        return f"profile: {report['error']}"
    phases = report["phases"]
    lines = [
        f"window {report['elapsed_ms']} ms (first decision → final)   "
        f"run {report['run_ms']} ms (first turn → last span)",
        f"decisions {report['decision_requests']} × median {report['decision_median_ms']} ms = "
        f"{report['decision_total_ms']} ms   actions {report['browser_actions']}   waits {report['wait_actions']}   "
        f"text calls {len(report['text_calls'])} ({sum(t['latency_ms'] for t in report['text_calls'])} ms, background)",
        "phases (ms, critical path):  "
        + "  ".join(f"{name} {ms}" for name, ms in phases.items()),
        "wire: " + "  ".join(f"{name} {v['count']}×{v['ms']}ms" for name, v in report["wire"].items()),
        "tools: " + "  ".join(f"{name} {v['count']}×{v['ms']}ms" for name, v in report["tools"].items()),
        f"final {report['final'].get('status')} {report['final'].get('url', '')}   verified={report['verified']}",
        " #  turn  probe(n)   jev(n)     tool               tool(wire)   gap",
    ]
    for index, step in enumerate(report["steps"], start=1):
        lines.append(
            f"{index:>2} {step['turn_ms']:>5}  {step['probe_ms']:>5}({step['probes']})  "
            f"{step['jev_ms']:>5}({step['jev']})  {step['tool'][:18]:<18} "
            f"{step['tool_ms']:>5}({step['tool_wire_ms']:>4})"
            f"  {step['gap_ms']:>4}  {step['finish']}"
        )
    return "\n".join(lines)
