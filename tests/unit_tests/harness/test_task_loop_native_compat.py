# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""The shared task executor preserves each harness's actual round identity."""

import asyncio
from types import SimpleNamespace

import pytest

from openjiuwen.agent_teams.harness import NativeHarness, HarnessState
from openjiuwen.agent_teams.harness.state import ActiveRound
from openjiuwen.core.controller.schema.task import Task, TaskStatus
from openjiuwen.core.foundation.llm import AssistantMessage, ToolCall
from openjiuwen.core.single_agent.interrupt.state import INTERRUPTION_KEY
from openjiuwen.core.single_agent.rail.base import RunContext, RunKind
from openjiuwen.harness.deep_agent import DeepAgent, DeepAgentConfig
from openjiuwen.harness.factory import DeepAgentParts
from openjiuwen.harness.schema.interaction import ActiveInteractionRound, RoundWorkItem
from openjiuwen.harness.task_loop.task_loop_event_executor import DEEP_TASK_TYPE, TaskLoopEventExecutor
from tests.unit_tests.agent.react_agent.interrupt.test_exact_agent_input import real_agent  # noqa: F401
from tests.unit_tests.harness.test_deep_agent_event_executor import _make_deps


def native_harness(f):
    parts = DeepAgentParts(config=DeepAgentConfig(
        card=f.agent.card, model=f.model, enable_task_loop=True,
        auto_create_workspace=False, enable_read_image_multimodal=False),
        rails=[], tool_cards=[], tool_instances=[])
    harness = NativeHarness(SimpleNamespace(resolve_parts=lambda context: parts))
    # The controller and scheduler drive the real configured ReAct/model/tool.
    harness.set_react_agent(f.agent, initialized=True)
    harness.ability_manager = f.agent.ability_manager
    return harness


@pytest.mark.asyncio
async def test_native_harness_cold_send_runs_real_react_without_deep_work_item(real_agent):  # noqa: F811
    f = real_agent
    harness = native_harness(f)
    chunks = []

    async def collect():
        async for chunk in harness.outputs():
            chunks.append(chunk)

    await harness.start(session=f.session)
    reader = asyncio.create_task(collect())
    try:
        for answer in ("First Team answer", "Next Team answer"):
            f.responses.append(AssistantMessage(content=answer))
            await harness.send(answer)
            async with asyncio.timeout(5):
                while harness.state is not HarnessState.IDLE or harness.active_round is not None:
                    await asyncio.sleep(0.005)
        assert len(f.provider_calls) == 2
        assert not f.responses
        assert f.session.get_state(INTERRUPTION_KEY) is None
    finally:
        await harness.stop()
        await asyncio.wait_for(reader, 3)
    assert "First Team answer" in repr(chunks) and "Next Team answer" in repr(chunks)


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ["native", "matching_deep", "other_deep"])
async def test_origin_uses_only_matching_deep_work_or_actual_task_context(real_agent, owner_kind):  # noqa: F811
    f = real_agent
    task_id = "actual-scheduler-task"
    if owner_kind == "native":
        agent = native_harness(f)
        agent._st.active = ActiveRound(round_id=1, task_id=task_id, original_query="Execute A",
            deep_agent=agent, task=asyncio.current_task(), steering_queue=asyncio.Queue())
    else:
        agent = DeepAgent(f.agent.card).configure(DeepAgentConfig(
            model=f.model, enable_task_loop=True, auto_create_workspace=False,
            enable_read_image_multimodal=False))
        agent.set_react_agent(f.agent, initialized=True)
        agent.ability_manager = f.agent.ability_manager
        agent._active_interaction_round = ActiveInteractionRound(
            work=RoundWorkItem.user(request_id="original-deep-request", inputs={"query": "Execute A"}),
            task_id=task_id if owner_kind == "matching_deep" else "another-scheduler-task")
    deps, manager = _make_deps()
    context = RunContext(session_id=f.session.get_session_id(), extra={
        "_interaction_request_id": "actual-task-request", "private_policy": "keep-original",
        "source_metadata": {"source_binding_id": "actual-task-binding"}})
    await manager.add_task(Task(task_id=task_id, session_id=f.session.get_session_id(),
        task_type=DEEP_TASK_TYPE, description="Execute A", status=TaskStatus.SUBMITTED,
        metadata={"run_kind": RunKind.NORMAL, "run_context": context}))
    f.responses.append(AssistantMessage(content="", tool_calls=[ToolCall(
        id=f.call_id, type="function", name=f.tool_name, arguments='{"target":"A"}')]))
    executor = TaskLoopEventExecutor(deps, agent)
    outputs = [chunk async for chunk in executor.execute_ability(task_id, f.session)]
    pending = f.session.get_state(INTERRUPTION_KEY)
    assert pending is not None and not f.effects and len(f.provider_calls) == 1
    origin = pending.execution_origin
    assert origin["request_id"] == ("original-deep-request" if owner_kind == "matching_deep"
                                    else "actual-task-request")
    assert origin["kind"] == "user" and origin["session_id"] == f.session.get_session_id()
    assert origin["run_context"]["extra"] == {
        "private_policy": "keep-original", "source_metadata": {"source_binding_id": "actual-task-binding"}}
    assert outputs[0].payload.metadata["source_task_id"] == task_id
    assert outputs[0].payload.metadata["source_binding_id"] == "actual-task-binding"
