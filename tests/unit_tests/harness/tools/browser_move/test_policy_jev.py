# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Jev policy: action space and request shape, answer validation, and decision-model turns."""

from __future__ import annotations

import json
from typing import Any
from unittest import IsolatedAsyncioTestCase, TestCase, mock

from openjiuwen.core.common.exception.errors import BaseError
from openjiuwen.core.foundation.llm.model import Model
from openjiuwen.core.foundation.tool.schema import ToolInfo
from openjiuwen.harness.tools.browser_move.policy import jev_decisions
from openjiuwen.harness.tools.browser_move.policy.jev_decision_model import JevDecisionModel

_OPERATIONS = ["CLICK", "TYPE_TEXT", "SELECT", "SCROLL_DOWN", "WAIT", "DONE", "BLOCKED"]
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


def _decider(scripted: list[dict[str, Any]], *, goal_value_cache: bool = True) -> JevDecisionModel:
    with mock.patch.object(Model, "__init__", _fake_model_init):
        decider = JevDecisionModel(
            _fallback(),
            language="en",
            client=_FakeClient(scripted),
            goal_value_cache=goal_value_cache,
            value_model=None,
        )
    decider.bind_runtime(_FakeRuntime())
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
        self.assertNotIn("text_value", decider._client.bodies[0]["questions"])

        second = await decider.invoke(_MESSAGES, tools=_TOOLS)
        self.assertIsNone(second.tool_calls)
        self.assertEqual(json.loads(second.content)["status"], "DONE")
        self.assertEqual(decider._runtime.calls[1]["after"], {"kind": "fill", "node": 2})
        self.assertEqual(decider.report()["interactions"], 1)

    async def test_cached_value_is_chosen_by_jev(self) -> None:
        decider = _decider([_answers("TYPE_TEXT", "none")])
        await decider.invoke(_MESSAGES, tools=_TOOLS)
        await decider._values_task
        decider._pending = None
        decider._client.scripted.append(_answers("TYPE_TEXT", "London"))

        message = await decider.invoke(_MESSAGES, tools=_TOOLS)
        self.assertEqual(json.loads(message.tool_calls[0].arguments)["text"], "London")
        self.assertIn("text_value", decider._client.bodies[-1]["questions"])
        self.assertEqual(decider.ticks[-1]["value_source"], "cache")

    async def test_cache_off_means_no_value_head_and_prefetch_source(self) -> None:
        decider = _decider([_answers("TYPE_TEXT", "none")], goal_value_cache=False)
        message = await decider.invoke(_MESSAGES, tools=_TOOLS)
        self.assertEqual(json.loads(message.tool_calls[0].arguments)["text"], "London")
        self.assertNotIn("text_value", decider._client.bodies[0]["questions"])
        self.assertIsNone(decider._values_task)
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
