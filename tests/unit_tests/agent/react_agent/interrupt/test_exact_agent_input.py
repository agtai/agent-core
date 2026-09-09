# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Real ReAct/model/tool/state ownership for exact human input admission."""

import asyncio
import copy
import pickle
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio

from openjiuwen.core.foundation.kv_cache import KVCacheAffinityConfig
from openjiuwen.core.foundation.llm import AssistantMessage, AssistantMessageChunk, Model, ModelClientConfig, ModelRequestConfig, ToolCall
from openjiuwen.core.foundation.tool import Tool, ToolCard
from openjiuwen.core.runner import Runner
from openjiuwen.core.runner.callback import AsyncCallbackFramework
from openjiuwen.core.session import InteractiveInput
from openjiuwen.core.session.agent import create_agent_session
from openjiuwen.core.session.checkpointer import CheckpointerFactory
from openjiuwen.core.session.checkpointer.inmemory import InMemoryCheckpointer
from openjiuwen.core.single_agent.agents.react_agent import ReActAgent, ReActAgentConfig
from openjiuwen.core.single_agent.interrupt.state import INTERRUPTION_KEY
from openjiuwen.core.single_agent.schema.agent_card import AgentCard
from openjiuwen.harness.rails.interrupt.confirm_rail import ConfirmInterruptRail


@pytest_asyncio.fixture
async def real_agent(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(Runner, "callback_framework", AsyncCallbackFramework())
    monkeypatch.setattr(CheckpointerFactory, "_default_checkpointer", InMemoryCheckpointer())
    effects, responses, provider_calls = [], [], []
    tool_name = "exact_input_effect_" + uuid4().hex
    call_id = "provider-reused-call-id"

    class RecordingTool(Tool):
        def __init__(self):
            super().__init__(ToolCard(id=tool_name, name=tool_name, description="Controlled external effect",
                input_params={"type": "object", "properties": {"target": {"type": "string"}},
                              "required": ["target"]}))

        async def invoke(self, inputs, **kwargs):
            effects.append(inputs["target"])
            return {"applied": inputs["target"]}

        async def stream(self, inputs, **kwargs):
            yield await self.invoke(inputs, **kwargs)

    class RecordingClient:
        async def invoke(self, messages=None, **kwargs):
            provider_calls.append(len(messages or []))
            assert responses, "Unexpected provider call"
            return responses.pop(0)

        async def stream(self, messages=None, **kwargs):
            message = await self.invoke(messages=messages, **kwargs)
            yield AssistantMessageChunk(**message.model_dump())

    monkeypatch.setattr("openjiuwen.core.foundation.llm.model.create_model_client", lambda **kwargs: RecordingClient())
    model = Model(model_config=ModelRequestConfig(model="exact-input-model"),
        model_client_config=ModelClientConfig(client_provider="OpenAI", api_key="test-only",
                                               api_base="http://127.0.0.1:1"))
    card = AgentCard(id="exact-input-" + uuid4().hex)
    agent = ReActAgent(card=card)
    agent.configure(ReActAgentConfig(model_name="exact-input-model", max_iterations=3,
        kv_cache_affinity_config=KVCacheAffinityConfig(enable_kv_cache_release=False, enable_kv_cache_affinity=False)))
    agent.set_llm(model)
    tool = RecordingTool()
    Runner.resource_mgr.add_tool(tool)
    agent.ability_manager.add(tool.card)
    await agent.register_rail(ConfirmInterruptRail(tool_names=[tool_name]))
    session = create_agent_session(session_id=card.id, card=card)
    await session.pre_run(inputs={})

    async def interrupt(target="A", **kwargs):
        responses.append(AssistantMessage(content="", tool_calls=[ToolCall(
            id=call_id, type="function", name=tool_name, arguments='{"target":"' + target + '"}')]))
        return await agent.invoke({"query": "Execute " + target}, session, **kwargs)

    yield SimpleNamespace(agent=agent, session=session, effects=effects, responses=responses,
                          provider_calls=provider_calls, model=model, interrupt=interrupt, call_id=call_id, tool_name=tool_name)
    await session.post_run()
    Runner.resource_mgr.remove_tool(tool_name)


def answer(token, call_id, *, before_effect=None, prepare_effect=None):
    value = InteractiveInput(expected_pending_token=token, before_effect=before_effect, prepare_effect=prepare_effect)
    value.update(call_id, {"approved": True, "auto_confirm": False, "feedback": ""})
    return value


@pytest.mark.asyncio
async def test_reused_provider_id_cannot_replay_previous_pending_generation(real_agent):
    fixture = real_agent
    first = await fixture.interrupt("A")
    token_a = first["pending_token"]
    old_answer = answer(token_a, fixture.call_id)
    fixture.responses.append(AssistantMessage(content="A done"))
    assert (await fixture.agent.invoke({"query": old_answer}, fixture.session))["output"] == "A done"
    assert fixture.effects == ["A"]
    assert fixture.session.get_state(INTERRUPTION_KEY) is None

    second = await fixture.interrupt("B")
    token_b = second["pending_token"]
    assert token_a and token_b and token_a != token_b
    state = fixture.session.get_state(INTERRUPTION_KEY)
    assert state.pending_token == token_b
    assert second["state"][0].payload.value.pending_token == token_b
    before = state.model_dump()
    history_before = messages(fixture)
    provider_count = len(fixture.provider_calls)
    from openjiuwen.core.single_agent.interrupt.response import AgentInputError

    with pytest.raises(AgentInputError, match="pending_token_mismatch"):
        await fixture.agent.invoke({"query": old_answer}, fixture.session)
    assert fixture.session.get_state(INTERRUPTION_KEY).model_dump() == before
    assert messages(fixture) == history_before
    assert fixture.effects == ["A"] and len(fixture.provider_calls) == provider_count
    fixture.responses.append(AssistantMessage(content="B done"))
    assert (await fixture.agent.invoke({"query": answer(token_b, fixture.call_id)}, fixture.session))["output"] == "B done"
    assert fixture.effects == ["A", "B"]


def messages(fixture):
    context = fixture.agent.context_engine.get_context(session_id=fixture.session.get_session_id())
    return [item.model_dump() for item in context.get_messages()]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["raise", "false", "true", "awaitable"])
async def test_guard_rejects_without_pending_history_or_effect_mutation(real_agent, mode):
    from openjiuwen.core.single_agent.interrupt.response import AgentInputError

    fixture = real_agent
    pending = await fixture.interrupt()
    state_before, history_before = fixture.session.get_state(INTERRUPTION_KEY).model_dump(), messages(fixture)
    error = PermissionError("revoked")

    async def asynchronous():
        raise AssertionError("Must not await the final guard")

    def guard():
        if mode == "raise":
            raise error
        return {"false": False, "true": True}.get(mode) if mode != "awaitable" else asynchronous()

    with pytest.raises(PermissionError if mode == "raise" else AgentInputError) as caught:
        await fixture.agent.invoke({"query": answer(pending["pending_token"], fixture.call_id,
                                                    before_effect=guard)}, fixture.session)
    if mode == "raise":
        assert caught.value is error
    assert fixture.session.get_state(INTERRUPTION_KEY).model_dump() == state_before
    assert messages(fixture) == history_before
    assert fixture.effects == [] and len(fixture.provider_calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_async_prepare_revocation_or_cancel_preserves_retry(real_agent, cancel):
    fixture = real_agent
    pending = await fixture.interrupt()
    state_before, history_before = fixture.session.get_state(INTERRUPTION_KEY).model_dump(), messages(fixture)
    entered, release = asyncio.Event(), asyncio.Event()

    async def prepare():
        entered.set()
        await release.wait()
        raise PermissionError("revoked after preparation")

    task = asyncio.create_task(fixture.agent.invoke({"query": answer(
        pending["pending_token"], fixture.call_id, prepare_effect=prepare)}, fixture.session))
    await asyncio.wait_for(entered.wait(), 5)
    if cancel:
        task.cancel()
    else:
        release.set()
    with pytest.raises(asyncio.CancelledError if cancel else PermissionError):
        await task
    assert fixture.session.get_state(INTERRUPTION_KEY).model_dump() == state_before
    assert messages(fixture) == history_before
    assert fixture.effects == [] and len(fixture.provider_calls) == 1
    fixture.responses.append(AssistantMessage(content="retry done"))
    await fixture.agent.invoke({"query": answer(pending["pending_token"], fixture.call_id)}, fixture.session)
    assert fixture.effects == ["A"]


@pytest.mark.asyncio
async def test_concurrent_exact_claims_execute_tool_once(real_agent):
    from openjiuwen.core.single_agent.interrupt.response import AgentInputError

    fixture = real_agent
    pending = await fixture.interrupt()
    reached, release = 0, asyncio.Event()

    async def prepare():
        nonlocal reached
        reached += 1
        if reached == 2:
            release.set()
        await release.wait()

    fixture.responses.append(AssistantMessage(content="done once"))
    results = await asyncio.gather(*(fixture.agent.invoke({"query": answer(
        pending["pending_token"], fixture.call_id, prepare_effect=prepare)}, fixture.session)
        for _ in range(2)), return_exceptions=True)
    assert sum(isinstance(item, AgentInputError) for item in results) == 1
    assert fixture.effects == ["A"] and len(fixture.provider_calls) == 2
    assert fixture.session.get_state(INTERRUPTION_KEY) is None


@pytest.mark.asyncio
async def test_reentrant_guard_cannot_claim_same_pending(real_agent):
    from openjiuwen.core.single_agent.interrupt.response import AgentInputError

    fixture = real_agent
    pending = await fixture.interrupt()
    snapshot = fixture.session.get_state(INTERRUPTION_KEY)
    seen = []

    def guard():
        with pytest.raises(AgentInputError, match="pending_claim_in_progress"):
            fixture.agent._hitl_handler.claim_exact(answer(pending["pending_token"], fixture.call_id),
                                                    fixture.session, snapshot)
        seen.append("blocked")

    fixture.responses.append(AssistantMessage(content="done"))
    await fixture.agent.invoke({"query": answer(pending["pending_token"], fixture.call_id, before_effect=guard)},
                                fixture.session)
    assert fixture.effects == ["A"] and seen == ["blocked"]


@pytest.mark.asyncio
async def test_partial_reply_rotates_pending_token(real_agent):
    from openjiuwen.core.single_agent.interrupt.response import AgentInputError

    fixture = real_agent
    fixture.responses.append(AssistantMessage(content="", tool_calls=[ToolCall(
        id=target, type="function", name=fixture.tool_name, arguments='{"target":"' + target + '"}')
        for target in ("A", "B")]))
    first = await fixture.agent.invoke({"query": "Execute both"}, fixture.session)
    partial = await fixture.agent.invoke({"query": answer(first["pending_token"], "A")}, fixture.session)
    assert partial["result_type"] == "interrupt" and partial["interrupt_ids"] == ["B"]
    assert partial["pending_token"] != first["pending_token"]
    assert fixture.effects == ["A"] and len(fixture.provider_calls) == 1
    with pytest.raises(AgentInputError, match="pending_token_mismatch"):
        await fixture.agent.invoke({"query": answer(first["pending_token"], "B")}, fixture.session)
    fixture.responses.append(AssistantMessage(content="both done"))
    await fixture.agent.invoke({"query": answer(partial["pending_token"], "B")}, fixture.session)
    assert fixture.effects == ["A", "B"]


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
async def test_old_pending_without_token_is_legacy_only(real_agent, legacy):
    from openjiuwen.core.single_agent.interrupt.response import AgentInputError

    fixture = real_agent
    original = await fixture.interrupt()
    old_state = fixture.session.get_state(INTERRUPTION_KEY)
    old_state.pending_token = None
    fixture.agent._hitl_handler.save(old_state, fixture.session)
    user_input = answer(None if legacy else original["pending_token"], fixture.call_id)
    if legacy:
        fixture.responses.append(AssistantMessage(content="legacy done"))
        await fixture.agent.invoke({"query": user_input}, fixture.session)
        assert fixture.effects == ["A"]
    else:
        with pytest.raises(AgentInputError, match="pending_input_missing"):
            await fixture.agent.invoke({"query": user_input}, fixture.session)
        assert fixture.effects == [] and len(fixture.provider_calls) == 1


def test_trusted_callbacks_are_copied_but_never_serialized():
    def before():
        return None

    def prepare():
        return None
    value = answer("token", "tool", before_effect=before, prepare_effect=prepare)
    clone = copy.deepcopy(value)
    assert clone.before_effect is before and clone.prepare_effect is prepare
    assert clone.user_inputs == value.user_inputs and clone.user_inputs is not value.user_inputs
    for serialized in (value.model_dump(), value.model_dump_json(), repr(value)):
        assert "before_effect" not in str(serialized) and "prepare_effect" not in str(serialized)
    restored = pickle.loads(pickle.dumps(value))
    assert restored.expected_pending_token == "token"
    assert restored.before_effect is None and restored.prepare_effect is None and not restored.claimed


@pytest.mark.asyncio
async def test_exact_stream_preclaim_rejection_preserves_history_and_original_error(real_agent):
    f = real_agent
    pending = await f.interrupt()
    before = messages(f)
    error = RuntimeError('stream permission revoked')
    def guard():
        raise error
    value = answer(pending['pending_token'], f.call_id, before_effect=guard)
    with pytest.raises(RuntimeError) as caught:
        async for _ in f.agent.stream({'query': value}, f.session):
            pass
    assert caught.value is error
    assert messages(f) == before and not f.effects
    assert f.session.get_state(INTERRUPTION_KEY).pending_token == pending['pending_token']


@pytest.mark.asyncio
@pytest.mark.parametrize('bad', ['foreign-id', 'empty', 'raw', 'scope'])
async def test_exact_preflight_rejects_unrelated_input_before_callback(real_agent, bad):
    from openjiuwen.core.session.interaction.interactive_input import AgentInputError
    f = real_agent
    origin = {'kind': 'user', 'request_id': 'original', 'session_id': f.session.get_session_id(),
              'run_context': None}
    pending = await f.interrupt(_execution_origin=origin)
    calls = []
    value = answer(pending['pending_token'], f.call_id, before_effect=lambda: calls.append(True))
    if bad == 'foreign-id':
        value.user_inputs = {'foreign': {'approved': True}}
    elif bad == 'empty':
        value.user_inputs.clear()
    elif bad == 'raw':
        value.raw_inputs = 'yes'
    else:
        state = f.session.get_state(INTERRUPTION_KEY)
        state.execution_origin['session_id'] = 'other-session'
        f.session.update_state({INTERRUPTION_KEY: state})
    before = f.session.get_state(INTERRUPTION_KEY)
    with pytest.raises(AgentInputError):
        await f.agent.invoke({'query': value}, f.session)
    assert f.session.get_state(INTERRUPTION_KEY) == before and not f.effects and not calls


@pytest.mark.asyncio
async def test_sync_guard_invalidation_is_rechecked_before_claim(real_agent):
    from openjiuwen.core.session.interaction.interactive_input import AgentInputError
    f = real_agent
    pending = await f.interrupt()
    def invalidate():
        f.agent._hitl_handler.clear(f.session)
    value = answer(pending['pending_token'], f.call_id, before_effect=invalidate)
    with pytest.raises(AgentInputError, match='pending_input_missing'):
        await f.agent.invoke({'query': value}, f.session)
    assert not value.claimed and not f.effects


@pytest.mark.asyncio
async def test_context_preparation_failure_does_not_clear_exact_state(real_agent, monkeypatch):
    f = real_agent
    pending = await f.interrupt()
    before = messages(f)
    error = RuntimeError('context preparation failed')
    async def fail(*args, **kwargs):
        raise error
    monkeypatch.setattr(f.agent.context_engine, 'create_context', fail)
    with pytest.raises(RuntimeError) as caught:
        await f.agent.invoke({'query': answer(pending['pending_token'], f.call_id)}, f.session)
    assert caught.value is error
    assert f.session.get_state(INTERRUPTION_KEY).pending_token == pending['pending_token']
    assert messages(f) == before and not f.effects


@pytest_asyncio.fixture
async def nested(real_agent):
    f = real_agent
    child = f.agent
    delegate_name = 'delegate_' + uuid4().hex
    class Delegate(Tool):
        def __init__(self):
            super().__init__(ToolCard(id=delegate_name, name=delegate_name, description='Original child',
                input_params={'type': 'object', 'properties': {'query': {'type': 'string'}}}))
        async def invoke(self, inputs, **kwargs):
            return await child.invoke(inputs, f.session)
        async def stream(self, inputs, **kwargs):
            yield await self.invoke(inputs, **kwargs)
    tool = Delegate()
    Runner.resource_mgr.add_tool(tool)
    f.parent = ReActAgent(card=AgentCard(id='parent-' + uuid4().hex))
    f.parent.configure(ReActAgentConfig(model_name='exact-input-model', max_iterations=3,
        kv_cache_affinity_config=KVCacheAffinityConfig(enable_kv_cache_release=False, enable_kv_cache_affinity=False)))
    f.parent.set_llm(f.model)
    f.parent.ability_manager.add(tool.card)
    f.parent_session = create_agent_session(session_id=f.parent.card.id, card=f.parent.card)
    await f.parent_session.pre_run(inputs={})
    f.responses.extend([
        AssistantMessage(content='', tool_calls=[ToolCall(id='delegate-call', type='function',
            name=delegate_name, arguments='{"query":"Execute A"}')]),
        AssistantMessage(content='', tool_calls=[ToolCall(id=f.call_id, type='function',
            name=f.tool_name, arguments='{"target":"A"}')]),
    ])
    f.parent_pending = await f.parent.invoke({'query': 'Execute through child'}, f.parent_session)
    yield f
    await f.parent_session.post_run()
    Runner.resource_mgr.remove_tool(delegate_name)


@pytest.mark.asyncio
async def test_nested_exact_uses_saved_child_token_and_parent_guard_once(nested):
    f = nested
    parent_token = f.parent_pending['pending_token']
    child_token = f.session.get_state(INTERRUPTION_KEY).pending_token
    assert child_token != parent_token
    calls = []
    f.responses.extend([AssistantMessage(content='Child done'), AssistantMessage(content='Parent done')])
    result = await f.parent.invoke({'query': answer(parent_token, f.call_id,
        before_effect=lambda: calls.append(True))}, f.parent_session)
    assert result['output'] == 'Parent done'
    assert f.effects == ['A'] and calls == [True]
    assert f.session.get_state(INTERRUPTION_KEY) is None


@pytest.mark.asyncio
@pytest.mark.parametrize('bad', ['missing', 'mixed'])
async def test_nested_missing_or_mixed_saved_child_tokens_preserve_parent(nested, bad):
    from openjiuwen.core.session.interaction.interactive_input import AgentInputError
    f = nested
    state = f.parent_session.get_state(INTERRUPTION_KEY)
    entry = next(iter(state.interrupted_tools.values()))
    request = entry.interrupt_requests[f.call_id]
    if bad == 'missing':
        request.pending_token = None
    else:
        other = request.model_copy(deep=True)
        other.pending_token = 'other-child-token'
        entry.interrupt_requests['second-child-id'] = other
    f.parent_session.update_state({INTERRUPTION_KEY: state})
    count = len(f.provider_calls)
    with pytest.raises(AgentInputError, match='pending_child_token_invalid'):
        await f.parent.invoke({'query': answer(state.pending_token, f.call_id)}, f.parent_session)
    assert f.parent_session.get_state(INTERRUPTION_KEY) == state and not f.effects
    assert len(f.provider_calls) == count


@pytest.mark.asyncio
async def test_nested_stale_child_token_does_not_consume_new_child(nested):
    f = nested
    child_a = f.session.get_state(INTERRUPTION_KEY).pending_token
    f.responses.append(AssistantMessage(content='Child A done separately'))
    await f.agent.invoke({'query': answer(child_a, f.call_id)}, f.session)
    child_b = await f.interrupt('B')
    before = f.session.get_state(INTERRUPTION_KEY)
    history = messages(f)
    calls = len(f.provider_calls)
    f.responses.append(AssistantMessage(content='The child reply was rejected; task incomplete'))
    carrier = answer(f.parent_pending['pending_token'], f.call_id)
    result = await f.parent.invoke({'query': carrier}, f.parent_session)
    assert carrier.claimed and f.parent_session.get_state(INTERRUPTION_KEY) is None
    assert 'incomplete' in result['output']
    assert f.session.get_state(INTERRUPTION_KEY) == before and before.pending_token == child_b['pending_token']
    assert messages(f) == history and f.effects == ['A']
    assert len(f.provider_calls) == calls + 1  # only the parent summarizes its execution error


@pytest.mark.asyncio
async def test_canonical_token_overrides_metadata_and_reemission_preserves_generation(real_agent):
    from openjiuwen.core.single_agent.rail.base import InvokeInputs
    f = real_agent
    first = await f.interrupt()
    state = f.session.get_state(INTERRUPTION_KEY)
    request = first['state'][0].payload.value.model_copy(deep=True)
    request.metadata['pending_token'] = 'self-reported'
    result = await f.agent._hitl_handler.commit_interrupt(state,
        f.agent.context_engine.get_context(session_id=f.session.get_session_id()), f.session,
        InvokeInputs(query='ignored'), [(f.call_id, request)])
    assert result['pending_token'] == first['pending_token']
    typed = result['state'][0].payload.value
    assert typed.pending_token == first['pending_token'] and 'pending_token' not in typed.metadata
    assert result['state'][0].model_dump()['payload']['value']['pending_token'] == first['pending_token']
    assert f.session.get_state(INTERRUPTION_KEY).pending_token == first['pending_token']
