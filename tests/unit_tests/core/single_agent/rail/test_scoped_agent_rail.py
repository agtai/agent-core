# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Execution guards use the native callback path without an instance registry."""

import asyncio
from types import SimpleNamespace

import pytest

from openjiuwen.core.runner.callback.errors import AbortError
from openjiuwen.core.single_agent.agent_callback_manager import AgentCallbackManager, scoped_agent_rail
from openjiuwen.core.single_agent.rail.base import AgentCallbackContext, AgentCallbackEvent, AgentRail


class Guard(AgentRail):
    def __init__(self, expected, effects):
        super().__init__()
        self.expected = expected
        self.effects = effects

    async def before_tool_call(self, ctx):
        if ctx.agent is not self.expected:
            raise AbortError("wrong execution owner")
        self.effects.append("guard")


def agent(name):
    return SimpleNamespace(agent_callback_manager=AgentCallbackManager(name))


@pytest.mark.asyncio
async def test_real_callback_selection_cannot_bypass_scope_and_default_is_unchanged():
    root, other = agent("scope-root"), agent("scope-other")
    effects = []
    ctx = AgentCallbackContext(agent=root)
    with scoped_agent_rail(Guard(root, effects)):
        await ctx.fire(AgentCallbackEvent.BEFORE_TOOL_CALL)
        effects.append("tool")
        ctx.agent = other
        with pytest.raises(AbortError, match="wrong execution owner"):
            await ctx.fire(AgentCallbackEvent.BEFORE_TOOL_CALL)
            effects.append("forbidden")
    await ctx.fire(AgentCallbackEvent.BEFORE_TOOL_CALL)
    assert effects == ["guard", "tool"]
    assert not other.agent_callback_manager.has_hooks(AgentCallbackEvent.BEFORE_TOOL_CALL)


@pytest.mark.asyncio
async def test_concurrent_scopes_and_nested_cleanup_are_isolated():
    async def execute(name):
        root, effects = agent(name), []
        ctx = AgentCallbackContext(agent=root)
        with scoped_agent_rail(Guard(root, effects)):
            await asyncio.sleep(0)
            with scoped_agent_rail(Guard(root, effects)):
                await ctx.fire(AgentCallbackEvent.BEFORE_TOOL_CALL)
            await ctx.fire(AgentCallbackEvent.BEFORE_TOOL_CALL)
        await ctx.fire(AgentCallbackEvent.BEFORE_TOOL_CALL)
        return effects

    assert await asyncio.gather(execute("scope-a"), execute("scope-b")) == [["guard"] * 3] * 2


@pytest.mark.asyncio
async def test_scope_exit_revokes_inherited_task_before_any_guard_or_tool_effect():
    root, effects = agent("scope-leaked"), []
    release = asyncio.Event()

    async def delayed():
        await release.wait()
        await AgentCallbackContext(agent=root).fire(AgentCallbackEvent.BEFORE_TOOL_CALL)
        effects.append("forbidden")

    with scoped_agent_rail(Guard(root, effects)):
        child = asyncio.create_task(delayed())
    release.set()
    with pytest.raises(AbortError, match="SCOPED_AGENT_RAIL_CLOSED"):
        await child
    assert effects == []


@pytest.mark.asyncio
async def test_scoped_guard_runs_after_native_registered_preprocessing():
    root, effects = agent("scope-order"), []

    async def prepare(ctx):
        effects.append("prepare")

    manager = root.agent_callback_manager
    await manager.register_callback(AgentCallbackEvent.BEFORE_TOOL_CALL, prepare)
    try:
        with scoped_agent_rail(Guard(root, effects)):
            await AgentCallbackContext(agent=root).fire(AgentCallbackEvent.BEFORE_TOOL_CALL)
        assert effects == ["prepare", "guard"]
    finally:
        await manager.unregister(AgentCallbackEvent.BEFORE_TOOL_CALL, prepare)


@pytest.mark.asyncio
@pytest.mark.parametrize("event", [AgentCallbackEvent.BEFORE_TOOL_CALL, AgentCallbackEvent.BEFORE_MODEL_CALL])
@pytest.mark.parametrize("before", [False, True])
async def test_revocation_while_guard_is_suspended_prevents_operation(event, before):
    root = agent("scope-suspended")
    entered, release = asyncio.Event(), asyncio.Event()
    effects = []

    class Suspended(AgentRail):
        async def before_tool_call(self, ctx):
            entered.set()
            await release.wait()

        before_model_call = before_tool_call

    async def run():
        await AgentCallbackContext(agent=root).fire(event)
        effects.append("forbidden operation")

    with scoped_agent_rail(Suspended(), before_events=frozenset({event}) if before else frozenset()):
        child = asyncio.create_task(run())
        await entered.wait()
    release.set()
    with pytest.raises(AbortError, match="SCOPED_AGENT_RAIL_CLOSED"):
        await child
    assert effects == []
