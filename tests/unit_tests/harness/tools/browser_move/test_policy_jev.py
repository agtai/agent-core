# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Jev policy: action space and request shape, answer validation, and decision-model turns."""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any
from unittest import IsolatedAsyncioTestCase, TestCase, mock

import httpx

from openjiuwen.core.common.exception.codes import StatusCode
from openjiuwen.core.common.exception.errors import BaseError, build_error
from openjiuwen.core.foundation.llm import Model, ModelClientConfig
from openjiuwen.core.foundation.llm.model_clients.base_model_client import BaseModelClient
from openjiuwen.core.foundation.tool.schema import ToolInfo
from openjiuwen.harness.schema.decision_policy import DecisionPolicyModel
from openjiuwen.harness.tools.browser_move.policy import jev_decisions
from openjiuwen.harness.tools.browser_move.policy.jev_decision_model import (
    MAX_CONSECUTIVE_WAITS,
    MAX_PROBE_SETTLE_MS,
    PROBE_SETTLE_MS,
    WAIT_SETTLE_BUDGET_MS,
    JevDecisionModel,
)

_OPERATIONS = ["CLICK", "TYPE_TEXT", "SELECT", "SCROLL_DOWN", "WAIT", "DONE", "BLOCKED"]
_CONTROL_OPERATIONS = ["WAIT", "DONE", "BLOCKED"]
_SNAPSHOT: dict[str, Any] = {
    "url": "https://flights.test/",
    "title": "Flights",
    "text": "Where from? Where to?",
    "can_scroll_down": True,
    "can_scroll_up": False,
    "page_key": "k1",
    "generation_id": "g1",
    "elements": [
        {"node": 1, "target_id": "t_g1_1", "role": "button", "label": "Search", "value": "", "editable": False},
        {
            "node": 2,
            "target_id": "t_g1_2",
            "role": "combobox",
            "label": "Where to?",
            "value": "",
            "editable": True,
            "expanded": "false",
        },
        {
            "node": 3,
            "target_id": "t_g1_3",
            "role": "combobox",
            "label": "Class",
            "value": "Economy",
            "editable": False,
            "options": [{"label": "Business", "value": "b"}, {"label": "First", "value": "f"}],
        },
    ],
}
_FAILED_PROBE_ENVELOPE: dict[str, Any] = {
    "ok": False,
    "error": "policy probe failed: boom",
    "elements": [],
    "page_state": {},
}


def _answer(choice: str, ids: list[str], confidence: float = 0.9) -> dict[str, Any]:
    top = confidence if len(ids) > 1 else 1.0
    rest = (1.0 - top) / max(1, len(ids) - 1)
    probabilities = {i: (top if i == choice else rest) for i in ids}
    return {"choice": choice, "confidence": confidence, "probabilities": probabilities}


def _answers(operation: str, value: str) -> dict[str, Any]:
    return {
        "answers": {
            "operation": _answer(operation, _OPERATIONS),
            "type_text_target": _answer("2", ["2"]),
            "click_target": _answer("1", ["1", "2"]),
            "select_target": _answer("3:1", ["3:1", "3:2"]),
            "text_value": _answer(value, ["London", "none"]),
        }
    }


def _control_answer(operation: str) -> dict[str, Any]:
    """An ``operation`` answer scoped to an action space with no addressable elements."""
    return {"answers": {"operation": _answer(operation, _CONTROL_OPERATIONS)}}


def _op_answer(operation: str) -> dict[str, Any]:
    """An ``operation`` answer scoped to the full ``_SNAPSHOT`` action space, with no target head.

    Valid for control operations (``WAIT`` / ``DONE`` / ``BLOCKED`` / scroll) that never index into
    ``space.heads``, on a snapshot that has real addressable elements.
    """
    return {"answers": {"operation": _answer(operation, _OPERATIONS)}}


def _malformed_operation_answer() -> dict[str, Any]:
    """An ``operation`` answer whose probabilities do not sum to 1, over the full ``_OPERATIONS`` set."""
    return {
        "answers": {
            "operation": {
                "choice": "WAIT",
                "confidence": 0.9,
                "probabilities": {op: 0.3 for op in _OPERATIONS},
            }
        }
    }


class TestActionSpaceAndRequest(TestCase):
    def test_heads_and_questions_follow_the_probe(self) -> None:
        space = jev_decisions.build_action_space(_SNAPSHOT)
        self.assertEqual(space.operations, _OPERATIONS)
        self.assertEqual(sorted(space.heads["CLICK"]), ["1", "2"])
        self.assertEqual(list(space.heads["TYPE_TEXT"]), ["2"])
        self.assertEqual(list(space.heads["SELECT"]), ["3:1", "3:2"])
        body = jev_decisions.build_request(
            space, _SNAPSHOT, goal="Fly to London", history=[], values=["London"], language="en", model="m"
        )
        self.assertEqual(
            sorted(body["questions"]), ["click_target", "operation", "select_target", "text_value", "type_text_target"]
        )
        self.assertEqual(body["questions"]["type_text_target"]["criteria"]["2"]["element"], "[2] Where to?")
        self.assertEqual(body["questions"]["select_target"]["criteria"]["3:2"]["option"], "First")
        self.assertIn("none", body["questions"]["text_value"]["criteria"])
        self.assertEqual(body["state"]["elements"][1]["operations"], ["TYPE_TEXT", "CLICK"])

    def test_non_offered_choice_is_rejected(self) -> None:
        with self.assertRaises(BaseError):
            jev_decisions.validate_choice(_answer("9", ["1", "2"]), ["1", "2"])

    def test_empty_elements_yield_control_only_action_space(self) -> None:
        """B3: the probe's failure envelope must still fold into a decidable (WAIT/DONE/BLOCKED) space."""
        space = jev_decisions.build_action_space(_FAILED_PROBE_ENVELOPE)
        self.assertEqual(space.operations, _CONTROL_OPERATIONS)
        self.assertEqual(space.heads, {})


def _fake_model_init(self: Model, model_client_config: Any, model_config: Any) -> None:
    self.model_client_config = model_client_config
    self.model_config = model_config
    self._client = None


class _FakeRuntime:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def probe_for_policy(self, source: str, params: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(params)
        return json.loads(json.dumps(_SNAPSHOT))


class _ScriptedPageKeyRuntime:
    """A runtime that returns ``_SNAPSHOT`` with ``page_key`` overridden per probe call, in order.

    Once the scripted sequence is exhausted, the last key repeats -- this only matters for the
    settle loop's clamp behaviour, which the escalation tests exercise explicitly.
    """

    def __init__(self, page_keys: list[str]) -> None:
        self.page_keys = page_keys
        self.calls: list[dict[str, Any]] = []

    async def probe_for_policy(self, source: str, params: dict[str, Any]) -> dict[str, Any]:
        index = min(len(self.calls), len(self.page_keys) - 1)
        self.calls.append(params)
        snapshot = json.loads(json.dumps(_SNAPSHOT))
        snapshot["page_key"] = self.page_keys[index]
        return snapshot


class _AlwaysFailingRuntime:
    """A runtime whose probe never recovers, mirroring the B3-fixed ``probe_for_policy`` failure envelope."""

    async def probe_for_policy(self, source: str, params: dict[str, Any]) -> dict[str, Any]:
        return json.loads(json.dumps(_FAILED_PROBE_ENVELOPE))


class _FakeClient:
    model = "typesafe/jev-test"
    url = "https://decisions.test/api/alpha/decisions"

    def __init__(self, scripted: list[dict[str, Any]]) -> None:
        self.scripted = scripted
        self.bodies: list[dict[str, Any]] = []

    async def warm(self) -> None:
        return None

    async def decide(self, body: dict[str, Any]) -> tuple[dict[str, Any], int]:
        self.bodies.append(body)
        return self.scripted.pop(0), 7


class _RaisingClient(_FakeClient):
    """A decisions client whose transport always fails, as B4 requires the model to tolerate."""

    async def decide(self, body: dict[str, Any]) -> tuple[dict[str, Any], int]:
        self.bodies.append(body)
        raise build_error(StatusCode.MODEL_CALL_FAILED, error_msg="decisions endpoint unavailable")


def _fallback() -> mock.MagicMock:
    fallback = mock.MagicMock(spec=Model)
    fallback.model_client_config = None
    fallback.model_config = None

    async def fake_invoke(messages: list[dict[str, str]], **_kwargs: Any) -> mock.MagicMock:
        asks_for_values = "values" in messages[0]["content"]
        payload = {"values": ["London"]} if asks_for_values else {"text": "London"}
        return mock.MagicMock(content=json.dumps(payload))

    fallback.invoke = fake_invoke
    return fallback


def _decider(
    scripted: list[dict[str, Any]],
    *,
    goal_value_cache: bool = True,
    client: Any = None,
    runtime: Any = None,
) -> JevDecisionModel:
    with mock.patch.object(Model, "__init__", _fake_model_init):
        decider = JevDecisionModel(
            _fallback(),
            language="en",
            client=client or _FakeClient(scripted),
            goal_value_cache=goal_value_cache,
            value_model=None,
        )
    decider.bind_runtime(runtime or _FakeRuntime())
    return decider


_TOOLS = [ToolInfo(name="browser_click"), ToolInfo(name="browser_type")]
_MESSAGES = [{"role": "user", "content": "Fly to London"}]


class TestJevDecisionModel(IsolatedAsyncioTestCase):
    async def test_first_tick_types_via_llm_value_then_done(self) -> None:
        decider = _decider([_answers("TYPE_TEXT", "none"), _answers("DONE", "none")])

        first = await decider.invoke(_MESSAGES, tools=_TOOLS)
        self.assertEqual(first.tool_calls[0].name, "browser_type")
        self.assertEqual(
            json.loads(first.tool_calls[0].arguments),
            {"generation_id": "g1", "target_id": "t_g1_2", "text": "London", "clear": True},
        )
        self.assertEqual(decider.ticks[0]["value_source"], "prefetch", "empty fields are prefetched at probe time")
        self.assertNotIn("text_value", decider._decisions.bodies[0]["questions"])

        second = await decider.invoke(_MESSAGES, tools=_TOOLS)
        self.assertIsNone(second.tool_calls)
        self.assertEqual(json.loads(second.content)["status"], "DONE")
        self.assertEqual(decider._runtime.calls[1]["after"], {"kind": "fill", "node": 2})
        self.assertEqual(decider.report()["interactions"], 1)

    async def test_cached_value_is_chosen_by_jev(self) -> None:
        decider = _decider([_answers("TYPE_TEXT", "none")])
        await decider.invoke(_MESSAGES, tools=_TOOLS)
        await decider._run.values_task
        decider._run.pending = None
        decider._decisions.scripted.append(_answers("TYPE_TEXT", "London"))

        message = await decider.invoke(_MESSAGES, tools=_TOOLS)
        self.assertEqual(json.loads(message.tool_calls[0].arguments)["text"], "London")
        self.assertIn("text_value", decider._decisions.bodies[-1]["questions"])
        self.assertEqual(decider.ticks[-1]["value_source"], "cache")

    async def test_cache_off_means_no_value_head_and_prefetch_source(self) -> None:
        decider = _decider([_answers("TYPE_TEXT", "none")], goal_value_cache=False)
        message = await decider.invoke(_MESSAGES, tools=_TOOLS)
        self.assertEqual(json.loads(message.tool_calls[0].arguments)["text"], "London")
        self.assertNotIn("text_value", decider._decisions.bodies[0]["questions"])
        self.assertIsNone(decider._run.values_task)
        self.assertEqual(decider.ticks[0]["value_source"], "prefetch")
        self.assertEqual(decider.report()["values"], {"cache": 0, "prefetch": 1, "llm": 0})

    async def test_non_browser_turn_goes_to_fallback(self) -> None:
        fallback = _fallback()
        fallback.invoke = mock.AsyncMock(return_value="chat-reply")
        with mock.patch.object(Model, "__init__", _fake_model_init):
            decider = JevDecisionModel(
                fallback, language="en", client=_FakeClient([]), goal_value_cache=False, value_model=None
            )
        reply = await decider.invoke([{"role": "user", "content": "summarise"}], tools=[ToolInfo(name="read_file")])
        self.assertEqual(reply, "chat-reply")

    async def test_report_before_any_turn_returns_the_zeroed_shape(self) -> None:
        decider = _decider([])
        self.assertEqual(
            decider.report(),
            {
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
            },
        )
        self.assertEqual(decider.ticks, [])
        self.assertEqual(decider.started_at, 0.0)


class TestJevRunIsolation(IsolatedAsyncioTestCase):
    """B1: per-task state must not leak into the task that follows it."""

    async def test_second_task_with_a_new_goal_starts_a_clean_run(self) -> None:
        decider = _decider([_answers("DONE", "none")], goal_value_cache=False)

        first_messages = [{"role": "user", "content": "Fly to London"}]
        first = await decider.invoke(first_messages, tools=_TOOLS)
        self.assertIsNone(first.tool_calls)
        self.assertEqual(json.loads(first.content)["status"], "DONE")
        self.assertTrue(decider._run.finished)

        decider._decisions.scripted.append(_answers("TYPE_TEXT", "none"))
        second_messages = [{"role": "user", "content": "Book a hotel in https://hotels.test/berlin"}]
        navigate = await decider.invoke(second_messages, tools=_TOOLS)
        self.assertEqual(navigate.tool_calls[0].name, "browser_navigate", "the URL shortcut must fire again")
        self.assertEqual(json.loads(navigate.tool_calls[0].arguments)["url"], "https://hotels.test/berlin")
        self.assertEqual(decider._run.goal, second_messages[0]["content"])

        second = await decider.invoke(second_messages, tools=_TOOLS)
        self.assertEqual(second.tool_calls[0].name, "browser_type")
        body = decider._decisions.bodies[-1]
        self.assertEqual(body["questions"]["operation"]["instructions"]["goal"], second_messages[0]["content"])
        self.assertEqual(body["state"]["recent_actions"], [], "the second task must not inherit the first history")

    async def test_starting_a_new_run_cancels_the_previous_runs_background_tasks(self) -> None:
        decider = _decider([_answers("DONE", "none")])

        await decider.invoke(_MESSAGES, tools=_TOOLS)
        first_run = decider._run
        self.assertIsNotNone(first_run.values_task)
        self.assertTrue(first_run.prefetched, "the empty 'Where to?' field is prefetched on the first probe")
        stale_values_task = first_run.values_task
        stale_prefetch_task = next(iter(first_run.prefetched.values()))

        decider._decisions.scripted.append(_answers("DONE", "none"))
        await decider.invoke([{"role": "user", "content": "Fly to Berlin"}], tools=_TOOLS)

        # Cancellation is cooperative: a task that never got a chance to run only reflects the
        # cancel() request once the event loop actually delivers it.
        for task in (stale_values_task, stale_prefetch_task):
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self.assertTrue(stale_values_task.cancelled() or stale_values_task.done())
        self.assertTrue(stale_prefetch_task.cancelled() or stale_prefetch_task.done())
        self.assertIsNot(decider._run, first_run)


class TestJevClientAttributeIsolation(TestCase):
    """B2: the Jev transport must not collide with the base ``Model._client`` slot."""

    def test_base_client_survives_and_decisions_client_is_the_injected_double(self) -> None:
        client_config = ModelClientConfig(
            client_provider="OpenAI",
            api_key="sk-test-key",
            api_base="https://api.openai.com/v1",
            verify_ssl=False,
        )
        fallback = Model(client_config, None)
        decisions = _FakeClient([])

        decider = JevDecisionModel(fallback, language="en", client=decisions, goal_value_cache=False, value_model=None)

        self.assertIsInstance(decider._client, BaseModelClient)
        self.assertIsNot(decider._client, decisions)
        self.assertIs(decider._decisions, decisions)


class TestJevProbeFailureDegradesToBlocked(IsolatedAsyncioTestCase):
    """B3: a probe that never recovers must not spin or raise — it must reach BLOCKED."""

    async def test_always_failing_probe_terminates_blocked_within_the_wait_budget(self) -> None:
        scripted = [_control_answer("WAIT") for _ in range(MAX_CONSECUTIVE_WAITS + 1)]
        decider = _decider(scripted, goal_value_cache=False, runtime=_AlwaysFailingRuntime())

        message = await decider.invoke(_MESSAGES, tools=_TOOLS)

        self.assertIsNone(message.tool_calls)
        summary = json.loads(message.content)
        self.assertEqual(summary["status"], "BLOCKED")
        self.assertEqual(len(decider._decisions.bodies), MAX_CONSECUTIVE_WAITS + 1)


class TestJevWaitCollapsesIntoInPageSettling(IsolatedAsyncioTestCase):
    """A WAIT verdict must be spent re-probing in-page, not by paying another decisions request."""

    async def test_wait_is_absorbed_by_the_probe_not_the_wire(self) -> None:
        decider = _decider([_op_answer("WAIT"), _answers("CLICK", "none")], goal_value_cache=False)

        message = await decider.invoke(_MESSAGES, tools=_TOOLS)

        self.assertEqual(message.tool_calls[0].name, "browser_click")
        self.assertEqual(len(decider._decisions.bodies), 2, "one WAIT verdict must cost exactly one re-ask")
        self.assertGreater(
            len(decider._runtime.calls), 2, "the extra waiting must show up as extra probes, not extra decide() calls"
        )

    async def test_escalating_settle_doubles_and_clamps_at_the_probe(self) -> None:
        runtime = _ScriptedPageKeyRuntime(["k1"] * 8)
        decider = _decider([_op_answer("WAIT"), _op_answer("DONE")], goal_value_cache=False, runtime=runtime)

        await decider.invoke(_MESSAGES, tools=_TOOLS)

        settle_calls = [call["settle_ms"] for call in runtime.calls[1:]]
        self.assertEqual(settle_calls, [PROBE_SETTLE_MS, PROBE_SETTLE_MS * 2, MAX_PROBE_SETTLE_MS])
        self.assertLessEqual(sum(settle_calls), WAIT_SETTLE_BUDGET_MS)
        self.assertTrue(all(ms <= MAX_PROBE_SETTLE_MS for ms in settle_calls))

    async def test_never_changing_page_stays_bounded_by_both_budgets(self) -> None:
        scripted = [_op_answer("WAIT") for _ in range(MAX_CONSECUTIVE_WAITS + 1)]
        runtime = _ScriptedPageKeyRuntime(["k1"] * 64)
        decider = _decider(scripted, goal_value_cache=False, runtime=runtime)

        message = await decider.invoke(_MESSAGES, tools=_TOOLS)

        self.assertIsNone(message.tool_calls)
        summary = json.loads(message.content)
        self.assertEqual(summary["status"], "BLOCKED")
        self.assertIn("waited without progress", summary["reason"])
        self.assertLessEqual(len(decider._decisions.bodies), MAX_CONSECUTIVE_WAITS + 1)

        settle_calls = [call["settle_ms"] for call in runtime.calls[1:]]
        for start in range(0, len(settle_calls), 3):
            group = settle_calls[start : start + 3]
            self.assertEqual(group, [PROBE_SETTLE_MS, PROBE_SETTLE_MS * 2, MAX_PROBE_SETTLE_MS])
            self.assertLessEqual(sum(group), WAIT_SETTLE_BUDGET_MS, "the settle budget bounds each WAIT episode")

    async def test_non_wait_path_is_unchanged_one_probe_one_decide_one_action(self) -> None:
        decider = _decider([_answers("CLICK", "none")], goal_value_cache=False)

        message = await decider.invoke(_MESSAGES, tools=_TOOLS)

        self.assertEqual(message.tool_calls[0].name, "browser_click")
        self.assertEqual(len(decider._decisions.bodies), 1)
        self.assertEqual(len(decider._runtime.calls), 1)
        self.assertEqual(decider.ticks[-1]["settle_probes"], 0)
        self.assertEqual(decider.ticks[-1]["settle_ms"], 0)

    async def test_page_changed_reflects_the_first_post_action_probe_not_a_later_settle_discovery(self) -> None:
        # call#1: invoke1's only probe, run.pending is still None. call#2: invoke2's initial probe,
        # consumes the pending CLICK -- page has not visibly changed yet. call#3/#4: settle probes at
        # 500ms/1000ms; the page only actually changes on the second settle probe.
        runtime = _ScriptedPageKeyRuntime(["k1", "k1", "k1", "k2"])
        decider = _decider(
            [_answers("CLICK", "none"), _op_answer("WAIT"), _op_answer("DONE")],
            goal_value_cache=False,
            runtime=runtime,
        )

        await decider.invoke(_MESSAGES, tools=_TOOLS)
        second = await decider.invoke(_MESSAGES, tools=_TOOLS)

        self.assertIsNone(second.tool_calls)
        self.assertEqual(json.loads(second.content)["status"], "DONE")
        click_entry = next(h for h in decider._run.history if h["kind"] == "click")
        self.assertIs(
            click_entry["page_changed"],
            False,
            "page_changed must reflect the probe immediately after the action, not a later settle discovery",
        )


class TestJevDecisionFailureDegradesToBlocked(IsolatedAsyncioTestCase):
    """B4: a decisions-endpoint failure must degrade the turn, not crash it."""

    async def test_transport_failure_returns_blocked_summary(self) -> None:
        decider = _decider([], client=_RaisingClient([]))

        message = await decider.invoke(_MESSAGES, tools=_TOOLS)

        self.assertIsNone(message.tool_calls)
        summary = json.loads(message.content)
        self.assertEqual(summary["status"], "BLOCKED")

    async def test_malformed_answer_returns_blocked_summary(self) -> None:
        decider = _decider([_malformed_operation_answer()])

        message = await decider.invoke(_MESSAGES, tools=_TOOLS)

        self.assertIsNone(message.tool_calls)
        summary = json.loads(message.content)
        self.assertEqual(summary["status"], "BLOCKED")


class TestJevDecisionsClientMalformedBody(IsolatedAsyncioTestCase):
    """Hole 1 (completes B4): a 200 response with a non-JSON body must not crash the run."""

    @staticmethod
    def _client_with_transport(handler: Any) -> jev_decisions.JevDecisionsClient:
        client = jev_decisions.JevDecisionsClient(
            api_key="test-key",
            url="https://decisions.test/api/alpha/decisions",
            model="typesafe/jev-test",
            timeout_s=5.0,
        )
        client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        return client

    async def test_decide_converts_a_malformed_body_into_model_call_failed(self) -> None:
        secret = "sk-should-not-leak-1234567890"

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=f"not-json-at-all {secret}".encode())

        client = self._client_with_transport(handler)

        with self.assertRaises(BaseError) as ctx:
            await client.decide({"model": "m", "questions": {}})

        self.assertEqual(ctx.exception.status, StatusCode.MODEL_CALL_FAILED)
        self.assertNotIn(secret, str(ctx.exception))

    async def test_a_malformed_body_degrades_the_turn_to_blocked_without_leaking_the_raw_body(self) -> None:
        secret = "sk-should-not-leak-1234567890"

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=f"not-json-at-all {secret}".encode())

        real_client = self._client_with_transport(handler)
        decider = _decider([], goal_value_cache=False, client=real_client)

        message = await decider.invoke(_MESSAGES, tools=_TOOLS)

        self.assertIsNone(message.tool_calls)
        summary = json.loads(message.content)
        self.assertEqual(summary["status"], "BLOCKED")
        self.assertNotIn(secret, message.content)


class TestJevPrefetchedValueIsolation(IsolatedAsyncioTestCase):
    """B5: prefetched values must not cross pages, and a failed prefetch must degrade gracefully."""

    async def test_failed_prefetch_falls_through_to_the_blocked_path(self) -> None:
        decider = _decider([_answers("TYPE_TEXT", "none")], goal_value_cache=False)

        async def _raise(*_args: Any, **_kwargs: Any) -> None:
            raise RuntimeError("value generation blew up")

        with mock.patch.object(JevDecisionModel, "_generate_value", _raise):
            message = await decider.invoke(_MESSAGES, tools=_TOOLS)

        self.assertIsNone(message.tool_calls)
        summary = json.loads(message.content)
        self.assertEqual(summary["status"], "BLOCKED")
        self.assertIn("no value for field", summary["reason"])

    async def test_prefetched_value_does_not_cross_a_page_boundary(self) -> None:
        decider = _decider([_answers("CLICK", "none")], goal_value_cache=False)
        await decider.invoke(_MESSAGES, tools=_TOOLS)
        run = decider._run
        self.assertTrue(run.prefetched, "the empty 'Where to?' field is prefetched on the first probe")
        self.assertTrue(all(key.startswith("k1|") for key in run.prefetched))
        stale_task = next(iter(run.prefetched.values()))

        other_page = json.loads(json.dumps(_SNAPSHOT))
        other_page["page_key"] = "k2"
        decider._prefetch_values(run, other_page)

        self.assertTrue(run.prefetched, "the new page's editable field is prefetched")
        self.assertTrue(all(key.startswith("k2|") for key in run.prefetched), "stale page-k1 prefetches must be gone")
        with contextlib.suppress(asyncio.CancelledError):
            await stale_task
        self.assertTrue(stale_task.cancelled() or stale_task.done())


class TestJevGoalValueExtractionDegradesGracefully(IsolatedAsyncioTestCase):
    """Hole 2 (completes B5): an unguarded ``values_task.result()`` must not crash the run."""

    async def test_a_failed_values_task_degrades_to_an_empty_value_list(self) -> None:
        decider = _decider([_answers("TYPE_TEXT", "none")], goal_value_cache=True)

        async def _raise(self: JevDecisionModel, goal: str) -> list[str]:
            raise RuntimeError("value extraction blew up")

        with mock.patch.object(JevDecisionModel, "_extract_values", _raise):
            message = await decider.invoke(_MESSAGES, tools=_TOOLS)

        # `_raise` has no internal `await`, so by the time `_decide_message` reads `.result()` the
        # task -- scheduled ahead of several other awaits in the same call -- has already failed.
        self.assertTrue(decider._run.values_task.done())
        self.assertEqual(decider._run.values, [])
        self.assertEqual(message.tool_calls[0].name, "browser_type")
        self.assertEqual(
            json.loads(message.tool_calls[0].arguments)["text"], "London", "the per-field prefetch path still fills"
        )


class TestDecisionPolicyModelProtocol(TestCase):
    """B6: the subagent layer recognises decision policies structurally, not by concrete class."""

    def test_jev_decision_model_satisfies_the_protocol(self) -> None:
        decider = _decider([])
        self.assertIsInstance(decider, DecisionPolicyModel)

    def test_a_minimal_stub_also_satisfies_the_protocol(self) -> None:
        class _Stub:
            def bind_runtime(self, runtime: Any) -> None:
                return None

        self.assertIsInstance(_Stub(), DecisionPolicyModel)

    def test_an_object_without_bind_runtime_does_not_satisfy_the_protocol(self) -> None:
        class _NotAPolicy:
            pass

        self.assertNotIsInstance(_NotAPolicy(), DecisionPolicyModel)
