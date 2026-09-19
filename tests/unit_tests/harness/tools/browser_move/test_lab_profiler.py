# coding: utf-8
"""The lab profiler's accounting: jev-ultrafast's window, clipped phases that add up, one row per step."""

import json
from types import SimpleNamespace

from openjiuwen.harness.tools.browser_move.lab.profiler import JevProfiler, Span, render

FINAL = json.dumps({"status": "DONE", "url": "https://www.google.com/travel/flights/search?tfs=x"})


def _recorded_run() -> JevProfiler:
    profiler = JevProfiler()

    def add(kind: str, name: str, t0: float, t1: float, **meta: object) -> None:
        profiler.spans.append(Span(kind, name, t0, t1, dict(meta)))

    add("turn", "browser", 0.0, 0.72, finish_reason="tool_calls")
    add("probe", "settle=500", 0.0, 0.2)
    add("wire", "evaluate", 0.01, 0.19)
    add("jev", "typesafe/jev-1.13", 0.2, 0.7)
    add("tool", "browser_click", 0.73, 0.76)
    add("wire", "click", 0.735, 0.755)
    add("turn", "browser", 0.8, 1.41, finish_reason="stop", content=FINAL)
    add("probe", "settle=500", 0.8, 0.9)
    add("wire", "evaluate", 0.81, 0.89)
    add("jev", "typesafe/jev-1.13", 0.9, 1.4)
    return profiler


def test_report_starts_at_the_first_decision_and_its_phases_add_up() -> None:
    policy = SimpleNamespace(ticks=[{"value_ms": 0}, {}], report=lambda: {"waits": 0})
    report = _recorded_run().report(policy)

    assert report["elapsed_ms"] == 1210
    assert report["decision_requests"] == 2
    assert report["decision_total_ms"] == 1000
    assert report["browser_actions"] == 1
    assert report["verified"] is True
    phases = report["phases"]
    assert phases["probe"] == 100, "the probe before the first decision is outside the window"
    assert phases["probe_wire"] == 80
    assert phases["tool"] == 30 and phases["tool_wire"] == 20
    critical_path = ("jev", "probe", "activate", "value_wait", "model_overhead", "tool", "framework")
    assert sum(phases[name] for name in critical_path) == 1210
    assert [step["tool"] for step in report["steps"]] == ["browser_click", ""]
    assert report["steps"][0]["gap_ms"] == 50
    assert "window 1210 ms" in render(report)


def test_a_turn_served_through_both_stream_and_invoke_is_counted_once() -> None:
    profiler = _recorded_run()
    profiler.spans.append(Span("turn", "browser", 0.8, 1.41, {"finish_reason": "stop", "content": FINAL}))
    policy = SimpleNamespace(ticks=[{"value_ms": 0}, {}], report=lambda: {"waits": 0})

    phases = profiler.report(policy)["phases"]

    assert phases["framework"] == 50, "an overlapping turn span must not push the framework gap negative"
    assert phases["model_overhead"] == 30


def test_report_without_a_decision_names_the_gap() -> None:
    policy = SimpleNamespace(ticks=[], report=lambda: {"waits": 0})
    assert JevProfiler().report(policy) == {"error": "no decision request recorded", "spans": 0}
