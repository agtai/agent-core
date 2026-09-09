# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Original Deep supervisor, scheduler, ReAct and real input claim integration."""
import asyncio
import copy

import pytest
import pytest_asyncio

from openjiuwen.core.foundation.llm import AssistantMessage, ToolCall
from openjiuwen.harness.deep_agent import DeepAgent, DeepAgentConfig
from openjiuwen.harness.schema.interaction import SendInputRequest
from tests.unit_tests.agent.react_agent.interrupt.test_exact_agent_input import real_agent, answer  # noqa: F401


@pytest_asyncio.fixture
async def deep(real_agent):  # noqa: F811 - imported pytest fixture
    f = real_agent
    f.deep = DeepAgent(f.agent.card).configure(DeepAgentConfig(
        model=f.model, enable_task_loop=True, auto_create_workspace=False, enable_read_image_multimodal=False))
    f.deep.set_react_agent(f.agent, initialized=True)
    f.deep.ability_manager = f.agent.ability_manager
    await f.deep.start(session=f.session)
    f.chunks = []

    async def collect():
        async for chunk in f.stream:
            f.chunks.append(chunk)
    f.collect = collect
    f.reader = None
    yield f
    await f.deep.stop()
    if f.reader is not None:
        await asyncio.wait_for(f.reader, 3)


async def until(predicate):
    async with asyncio.timeout(5):
        while not predicate():
            await asyncio.sleep(0.005)


def context(binding='original-binding'):
    return {'extra': {'source_metadata': {'source_binding_id': binding}, 'private': 'keep-original'}}


async def start_pending(f, *, managed=True, kind='user'):
    f.responses.append(AssistantMessage(content='', tool_calls=[ToolCall(
        id=f.call_id, type='function', name=f.tool_name, arguments='{"target":"A"}')]))
    if kind == 'user':
        inputs = {'query': 'Execute A'}
        if managed:
            inputs['run'] = {'kind': 'normal', 'context': context()}
        await f.deep.send_input(SendInputRequest(request_id='original-request', inputs=inputs))
    else:
        await f.deep.goal_manager.set('Execute A', run_context=context() if managed else None)
    f.stream = await f.deep.attach_output()
    f.reader = asyncio.create_task(f.collect())
    await until(lambda: f.session.get_state('__react_agent_interruption__') is not None or f.reader.done())
    await until(lambda: f.deep._active_interaction_round is None)
    return f.deep.peek_pending_input()


@pytest.mark.asyncio
async def test_managed_user_same_reader_and_original_work_receipt(deep):
    f = deep
    pending = await start_pending(f)
    assert pending and pending['execution_origin']['request_id'] == 'original-request'
    assert pending['execution_origin']['kind'] == 'user'
    assert not f.reader.done()
    original = copy.deepcopy(pending)
    pending['execution_origin']['run_context']['extra']['private'] = 'tampered'
    assert f.deep.peek_pending_input() == original
    f.responses.append(AssistantMessage(content='A done'))
    reply = answer(original['pending_token'], f.call_id)
    receipt = await asyncio.wait_for(f.deep.send_input(SendInputRequest(
        request_id='reply-not-source', inputs={'query': reply})), 5)
    assert receipt == {'accepted': True, 'pending_token': original['pending_token']}
    await asyncio.wait_for(f.reader, 5)
    assert f.effects == ['A']
    assert f.deep.peek_pending_input() is None


def source_chunks(f):
    result = []
    for chunk in f.chunks:
        payload = getattr(chunk, 'payload', None)
        if isinstance(payload, dict) and payload.get('source_task_id'):
            result.append(payload)
        elif getattr(payload, 'metadata', None):
            result.append(payload.metadata)
    return result


@pytest.mark.asyncio
async def test_ordinary_queued_user_waits_for_managed_claim_and_keeps_own_source(deep):
    f = deep
    pending = await start_pending(f)
    count = len(f.provider_calls)
    await f.deep.send_input(SendInputRequest(request_id='next-user', inputs={
        'query': 'Next user task', 'run': {'context': context('next-binding')}}))
    await asyncio.sleep(.04)
    assert f.deep.peek_pending_input() == pending and len(f.provider_calls) == count and not f.effects
    f.responses.extend([AssistantMessage(content='A done'), AssistantMessage(content='Next done')])
    assert (await f.deep.send_input(SendInputRequest(request_id='reply', inputs={
        'query': answer(pending['pending_token'], f.call_id)})))['accepted']
    await asyncio.wait_for(f.reader, 5)
    originals = [p for p in source_chunks(f) if p.get('source_binding_id') == 'original-binding']
    assert len({p['source_task_id'] for p in originals}) == 2
    assert {p.get('source_request_id') for p in originals} == {'original-request'}
    assert any(p.get('source_binding_id') == 'next-binding' and p.get('source_request_id') == 'next-user'
               for p in source_chunks(f))
    assert f.effects == ['A']


@pytest.mark.asyncio
async def test_managed_goal_claim_completes_original_attempt_on_same_reader(deep):
    f = deep
    pending = await start_pending(f, kind='goal')
    record = f.deep.goal_manager.peek()
    assert record.attempt_count == 1 and record.last_assessment is None and not f.reader.done()
    assert pending['execution_origin']['kind'] == 'goal' and pending['execution_origin']['request_id'] is None
    await asyncio.sleep(.04)
    assert f.deep.goal_manager.peek().attempt_count == 1 and len(f.provider_calls) == 1
    f.responses.extend([
        AssistantMessage(content='', tool_calls=[ToolCall(id='report', type='function',
            name='submit_goal_report', arguments='{"status":"complete","evidence":"A applied"}')]),
        AssistantMessage(content='Goal A done'),
        AssistantMessage(content='{"status":"complete","evidence":"A actually applied"}'),
    ])
    receipt = await f.deep.send_input(SendInputRequest(request_id='reply', inputs={
        'query': answer(pending['pending_token'], f.call_id)}))
    assert receipt['accepted']
    await asyncio.wait_for(f.reader, 5)
    result = f.deep.goal_manager.peek()
    assert result.status.value == 'completed' and result.attempt_count == 1
    assert result.goal_id == record.goal_id and result.revision == record.revision
    assert f.effects == ['A'] and not f.responses
    sources = [p for p in source_chunks(f) if p.get('source_binding_id') == 'original-binding']
    assert {p.get('source_run_kind') for p in sources} == {'goal'}
    assert {p.get('source_request_id') for p in sources} == {None}
    assert len({p['source_task_id'] for p in sources}) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize('operation', ['abort', 'clear', 'replace'])
async def test_goal_invalidation_settles_preclaim_waiter_without_tool_effect(deep, operation):
    from openjiuwen.core.session.interaction.interactive_input import AgentInputError
    f = deep
    pending = await start_pending(f, kind='goal')
    entered, release = asyncio.Event(), asyncio.Event()
    async def prepare():
        entered.set()
        await release.wait()
    reply = answer(pending['pending_token'], f.call_id, prepare_effect=prepare)
    observer = asyncio.create_task(f.deep.send_input(SendInputRequest(request_id='reply', inputs={'query': reply})))
    await asyncio.wait_for(entered.wait(), 5)
    if operation == 'abort':
        await f.deep.goal_manager.pause()
        await f.deep.cancel_round(reason='host-abort')
    elif operation == 'clear':
        await f.deep.goal_manager.clear()
    else:
        # Keep replacement work queued while checking invalidation effects.
        await f.deep.goal_manager.set('Replacement', overwrite_confirmed=True, run_context=context('replacement'))
        await f.deep.goal_manager.pause()
    release.set()
    with pytest.raises((AgentInputError, asyncio.CancelledError)):
        await asyncio.wait_for(observer, 5)
    assert not f.effects and not reply.claimed and f.deep.peek_pending_input() is None


@pytest.mark.asyncio
async def test_observer_cancel_does_not_cancel_original_queued_owner(deep):
    f = deep
    pending = await start_pending(f)
    entered, release = asyncio.Event(), asyncio.Event()
    async def prepare():
        entered.set()
        await release.wait()
    reply = answer(pending['pending_token'], f.call_id, prepare_effect=prepare)
    observer = asyncio.create_task(f.deep.send_input(SendInputRequest(request_id='reply', inputs={'query': reply})))
    await asyncio.wait_for(entered.wait(), 5)
    observer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await observer
    assert not reply.claimed and f.deep.peek_pending_input() == pending
    f.responses.append(AssistantMessage(content='A done'))
    release.set()
    await asyncio.wait_for(f.reader, 5)
    assert reply.claimed and f.effects == ['A']


@pytest.mark.asyncio
async def test_deep_guard_rejection_preserves_reader_history_and_retry(deep):
    from tests.unit_tests.agent.react_agent.interrupt.test_exact_agent_input import messages
    f = deep
    pending = await start_pending(f)
    before = messages(f)
    error = TimeoutError('revoked after asynchronous preparation')
    async def prepare():
        await asyncio.sleep(0)
        raise error
    with pytest.raises(TimeoutError) as caught:
        await f.deep.send_input(SendInputRequest(request_id='reply', inputs={
            'query': answer(pending['pending_token'], f.call_id, prepare_effect=prepare)}))
    assert caught.value is error
    assert messages(f) == before and f.deep.peek_pending_input() == pending and not f.reader.done()
    f.responses.append(AssistantMessage(content='A done'))
    assert (await f.deep.send_input(SendInputRequest(request_id='retry', inputs={
        'query': answer(pending['pending_token'], f.call_id)})))['accepted']
    await asyncio.wait_for(f.reader, 5)


@pytest.mark.asyncio
@pytest.mark.parametrize('extra', [{'conversation_id': 'wrong'}, {'run': {'context': context('wrong')}}])
async def test_deep_wrong_scope_context_rejects_before_queue_and_callback(deep, extra):
    from openjiuwen.core.session.interaction.interactive_input import AgentInputError
    f = deep
    pending = await start_pending(f)
    calls = []
    reply = answer(pending['pending_token'], f.call_id, before_effect=lambda: calls.append(True))
    with pytest.raises(AgentInputError):
        await f.deep.send_input(SendInputRequest(request_id='reply', inputs={'query': reply, **extra}))
    assert not calls and not f.effects and f.deep.peek_pending_input() == pending
    assert not f.deep.event_manager.has_pending_work()


@pytest.mark.asyncio
async def test_unbound_legacy_user_still_finishes_reader(deep):
    f = deep
    pending = await start_pending(f, managed=False)
    await asyncio.wait_for(f.reader, 5)
    assert pending and pending['execution_origin']['run_context'] is None
    assert not f.effects


@pytest.mark.asyncio
async def test_goal_pause_resume_keeps_parked_attempt_and_pending(deep):
    f = deep
    pending = await start_pending(f, kind='goal')
    original = f.deep.goal_manager.peek()
    await f.deep.goal_manager.pause()
    paused = f.deep.goal_manager.peek()
    assert paused.attempt_count == 1 and paused.status.value == 'paused'
    assert f.deep.peek_pending_input() == pending
    resumed = await f.deep.goal_manager.resume()
    assert resumed.revision == original.revision and resumed.attempt_count == 1
    assert f.deep.peek_pending_input() == pending
    await f.deep.goal_manager.clear()
    await asyncio.wait_for(f.reader, 5)


@pytest.mark.asyncio
async def test_deep_duplicate_queued_strict_input_receives_one_claim(deep):
    from openjiuwen.core.session.interaction.interactive_input import AgentInputError
    f = deep
    pending = await start_pending(f)
    entered, release = asyncio.Event(), asyncio.Event()
    async def prepare():
        entered.set()
        await release.wait()
    first = asyncio.create_task(f.deep.send_input(SendInputRequest(request_id='one', inputs={
        'query': answer(pending['pending_token'], f.call_id, prepare_effect=prepare)})))
    await asyncio.wait_for(entered.wait(), 5)
    second = asyncio.create_task(f.deep.send_input(SendInputRequest(request_id='two', inputs={
        'query': answer(pending['pending_token'], f.call_id)})))
    await until(lambda: f.deep.event_manager.has_pending_work())
    f.responses.append(AssistantMessage(content='A done'))
    release.set()
    results = await asyncio.gather(first, second, return_exceptions=True)
    assert sum(isinstance(result, dict) and result.get('accepted') is True for result in results) == 1
    assert sum(isinstance(result, AgentInputError) for result in results) == 1
    await asyncio.wait_for(f.reader, 5)
    assert f.effects == ['A']


@pytest.mark.asyncio
async def test_deep_original_pending_aba_rejects_old_answer(deep):
    from openjiuwen.core.session.interaction.interactive_input import AgentInputError
    f = deep
    first = await start_pending(f)
    f.responses.append(AssistantMessage(content='A done'))
    await f.deep.send_input(SendInputRequest(request_id='reply-A', inputs={
        'query': answer(first['pending_token'], f.call_id)}))
    await asyncio.wait_for(f.reader, 5)
    second = await start_pending(f)
    assert first['pending_token'] != second['pending_token']
    calls = len(f.provider_calls)
    with pytest.raises(AgentInputError, match='pending_token_mismatch'):
        await f.deep.send_input(SendInputRequest(request_id='replay-A', inputs={
            'query': answer(first['pending_token'], f.call_id)}))
    assert f.deep.peek_pending_input() == second and len(f.provider_calls) == calls and f.effects == ['A']
    f.responses.append(AssistantMessage(content='Second A done'))
    await f.deep.send_input(SendInputRequest(request_id='reply-B', inputs={
        'query': answer(second['pending_token'], f.call_id)}))
    await asyncio.wait_for(f.reader, 5)
    assert f.effects == ['A', 'A']


@pytest.mark.asyncio
async def test_public_abort_invalidates_pending_and_settles_prepare_owner(deep):
    from openjiuwen.core.session.interaction.interactive_input import AgentInputError
    f = deep
    pending = await start_pending(f)
    entered = asyncio.Event()
    async def prepare():
        entered.set()
        await asyncio.Event().wait()
    observer = asyncio.create_task(f.deep.send_input(SendInputRequest(request_id='reply', inputs={
        'query': answer(pending['pending_token'], f.call_id, prepare_effect=prepare)})))
    await asyncio.wait_for(entered.wait(), 5)
    await f.deep.abort()
    with pytest.raises((AgentInputError, asyncio.CancelledError)):
        await asyncio.wait_for(observer, 5)
    assert f.deep.peek_pending_input() is None and not f.effects


@pytest.mark.asyncio
async def test_managed_invalid_origin_rejects_before_enqueue_legacy_rich_context_still_runs(deep):
    f = deep
    before = context()
    before['extra']['non_json'] = object()
    with pytest.raises(ValueError):
        await f.deep.send_input(SendInputRequest(request_id='managed-bad', inputs={
            'query': 'bad', 'run': {'context': before}}))
    assert not f.deep.event_manager.has_pending_work() and not f.provider_calls
    f.responses.append(AssistantMessage(content='Legacy context accepted'))
    await f.deep.send_input(SendInputRequest(request_id='legacy', inputs={
        'query': 'Legacy', 'run': {'context': {'extra': {'non_json': object()}}}}))
    f.stream = await f.deep.attach_output()
    f.reader = asyncio.create_task(f.collect())
    await asyncio.wait_for(f.reader, 5)
    assert len(f.provider_calls) == 1 and f.deep.peek_pending_input() is None


@pytest.mark.asyncio
async def test_goal_largest_legal_context_has_origin_envelope_room(deep):
    f = deep
    import json
    run_context = context()
    run_context['extra']['private'] = ''
    budget = 65536 - len(json.dumps(run_context, ensure_ascii=False, separators=(',', ':')).encode('utf-8'))
    run_context['extra']['private'] = 'x' * budget
    f.responses.append(AssistantMessage(content='', tool_calls=[ToolCall(
        id=f.call_id, type='function', name=f.tool_name, arguments='{"target":"A"}')]))
    await f.deep.goal_manager.set('Goal with full context', run_context=run_context)
    f.stream = await f.deep.attach_output()
    f.reader = asyncio.create_task(f.collect())
    await until(lambda: f.deep.peek_pending_input() is not None)
    assert f.deep.peek_pending_input()['execution_origin']['run_context']['extra']['private'] == 'x' * budget
    await f.deep.goal_manager.clear()


@pytest.mark.asyncio
async def test_reply_queued_while_idle_close_waits_is_not_lost(deep, monkeypatch):
    f = deep
    pending = await start_pending(f)
    entered, release = asyncio.Event(), asyncio.Event()
    original = f.deep._close_idle_output_if_finished
    async def close_wait():
        entered.set()
        await release.wait()
        await original()
    monkeypatch.setattr(f.deep, '_close_idle_output_if_finished', close_wait)
    f.deep._notify_work()
    await asyncio.wait_for(entered.wait(), 5)
    observer = asyncio.create_task(f.deep.send_input(SendInputRequest(request_id='reply', inputs={
        'query': answer(pending['pending_token'], f.call_id)})))
    await until(lambda: f.deep.event_manager.has_pending_work(input_only=True))
    f.responses.append(AssistantMessage(content='A done'))
    release.set()
    assert (await asyncio.wait_for(observer, 5))['accepted']
    await asyncio.wait_for(f.reader, 5)


@pytest.mark.asyncio
async def test_goal_execution_failure_after_claim_blocks_original_attempt(deep):
    f = deep
    pending = await start_pending(f, kind='goal')
    receipt = await f.deep.send_input(SendInputRequest(request_id='reply', inputs={
        'query': answer(pending['pending_token'], f.call_id)}))
    assert receipt['accepted']
    await asyncio.wait_for(f.reader, 5)
    result = f.deep.goal_manager.peek()
    assert result.status.value == 'blocked' and result.attempt_count == 1
    assert f.effects == ['A'] and f.deep.peek_pending_input() is None


@pytest.mark.asyncio
async def test_outer_rail_cannot_downgrade_strict_input_to_legacy(deep):
    from openjiuwen.core.single_agent.rail.base import AgentCallbackEvent
    from openjiuwen.core.session.interaction.interactive_input import AgentInputError
    f = deep
    pending = await start_pending(f)
    async def replace(ctx):
        ctx.inputs.query = 'approve everything'
    await f.deep.register_callback(AgentCallbackEvent.BEFORE_INVOKE, replace)
    with pytest.raises(AgentInputError, match='pending_input_replaced'):
        await f.deep.send_input(SendInputRequest(request_id='reply', inputs={
            'query': answer(pending['pending_token'], f.call_id)}))
    assert f.deep.peek_pending_input() == pending and not f.effects and len(f.provider_calls) == 1


@pytest.mark.asyncio
async def test_supervisor_failure_settles_queued_receipt_and_preserves_pending(deep, monkeypatch):
    f = deep
    pending = await start_pending(f)
    error = RuntimeError('original supervisor failed before dequeue')
    def fail(**kwargs):
        raise error
    monkeypatch.setattr(f.deep.event_manager, 'next_work', fail)
    with pytest.raises(RuntimeError) as caught:
        await asyncio.wait_for(f.deep.send_input(SendInputRequest(request_id='reply', inputs={
            'query': answer(pending['pending_token'], f.call_id)})), 5)
    assert caught.value is error and f.deep.peek_pending_input() == pending and not f.effects
