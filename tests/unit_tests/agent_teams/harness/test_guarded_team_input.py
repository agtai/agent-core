"""Exact leader input guard is consumed by the existing native supervisor."""
import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import pytest_asyncio

from openjiuwen.agent_teams.agent.team_agent import TeamAgent
from openjiuwen.agent_teams.harness import NativeHarness, HarnessState
from openjiuwen.agent_teams.harness.team_harness import TeamHarness
from openjiuwen.agent_teams.interaction import GodViewMessage, OperatorMessage
from openjiuwen.agent_teams.runtime.manager import TeamRuntimeManager
from openjiuwen.agent_teams.runtime.pool import ActiveTeam
from openjiuwen.agent_teams.schema.team import TeamRole
from openjiuwen.core.runner import Runner
from openjiuwen.core.runner.runner import GLOBAL_RUNNER
from tests.unit_tests.agent_teams.harness.fixtures import (
    make_spec, make_card, start_harness, drain_outputs, wait_for_state,
)

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def runtime(monkeypatch):
    await Runner.start()
    native = NativeHarness(make_spec())
    fake = await start_harness(native)
    outputs = []
    reader = asyncio.create_task(drain_outputs(native, outputs))
    harness = TeamHarness(make_spec(), None, native, role=TeamRole.LEADER, member_name='leader')
    team = TeamAgent(make_card('guarded-team'))
    team._configurator.harness = harness
    manager = TeamRuntimeManager()
    entry = ActiveTeam('guarded-team', team, 'public-session')
    manager.pool._teams[entry.team_name] = entry
    monkeypatch.setattr(GLOBAL_RUNNER, '_team_runtime_manager', manager)
    yield SimpleNamespace(native=native, fake=fake, entry=entry, manager=manager, team=team, outputs=outputs)
    await native.stop()
    await reader
    # Do not make Runner.stop drive an unrelated TeamAgent lifecycle in fixture.
    manager.pool._teams.clear()
    await Runner.stop()


async def send(runtime, guard, payload=None):
    return await Runner.interact_agent_team(
        payload if payload is not None else GodViewMessage(body='guarded input'),
        team_name=runtime.entry.team_name, session_id='public-session', before_effect=guard)


async def test_real_runner_to_supervisor_guard_and_legacy(runtime):
    guard = Mock(return_value=None)
    assert await send(runtime, guard)
    assert await wait_for_state(runtime.native, HarnessState.IDLE)
    assert [call['query'] for call in runtime.fake.invocations] == ['guarded input']
    guard.assert_called_once_with()
    assert runtime.entry.interact_gate.inflight == 0
    assert await runtime.manager.interact('legacy', team_name='guarded-team', session_id='public-session')
    assert await wait_for_state(runtime.native, HarnessState.IDLE)
    assert [call['query'] for call in runtime.fake.invocations] == ['guarded input', 'legacy']


@pytest.mark.parametrize('mutation', ['revoke', 'replace', 'closing', 'session', 'native'])
async def test_queued_rejection_has_zero_round_or_input_effect(runtime, monkeypatch, mutation):
    entered, release = asyncio.Event(), asyncio.Event()
    original_dispatch = runtime.native._dispatch
    async def delay(cmd):
        entered.set()
        await release.wait()
        await original_dispatch(cmd)
    monkeypatch.setattr(runtime.native, '_dispatch', delay)
    allowed = True
    def guard():
        if not allowed:
            raise PermissionError('revoked')
    task = asyncio.create_task(send(runtime, guard))
    await asyncio.wait_for(entered.wait(), 2)
    if mutation == 'revoke':
        allowed = False
    elif mutation == 'replace':
        runtime.manager.pool._teams['guarded-team'] = ActiveTeam('guarded-team', runtime.team, 'public-session')
    elif mutation == 'closing':
        runtime.entry.closing = True
    elif mutation == 'session':
        runtime.entry.current_session_id = 'different'
    else:
        runtime.team.harness._native = object()
    release.set()
    try:
        result = await asyncio.wait_for(task, 2)
        assert not result
        assert runtime.fake.invocations == []
        assert runtime.native.state is HarnessState.IDLE
        assert runtime.entry.interact_gate.inflight == 0
        assert not runtime.native._st.supervisor_task.done()
    finally:
        runtime.team.harness._native = runtime.native


@pytest.mark.parametrize('payload', ['plain', OperatorMessage(body='broadcast'), GodViewMessage(body={})])
async def test_guarded_nonexact_payload_rejected_before_gate(runtime, payload):
    guard = Mock(return_value=None)
    assert not await send(runtime, guard, payload)
    guard.assert_not_called()
    assert runtime.fake.invocations == []
    assert runtime.entry.interact_gate.inflight == 0


@pytest.mark.parametrize('value', [False, True, 'allow'])
async def test_non_none_guard_return_does_not_kill_original_supervisor(runtime, value):
    assert not await send(runtime, lambda: value)
    assert runtime.fake.invocations == []
    assert not runtime.native._st.supervisor_task.done()
    assert await send(runtime, lambda: None)


async def test_callback_cannot_replace_original_owner_then_return_allow(runtime):
    def replace():
        runtime.manager.pool._teams['guarded-team'] = ActiveTeam('guarded-team', runtime.team, 'public-session')
    assert not await send(runtime, replace)
    assert runtime.fake.invocations == []
    assert not runtime.native._st.supervisor_task.done()


async def test_async_guard_is_not_awaited_or_applied(runtime):
    effects = []
    async def guard():
        effects.append('forbidden')
    assert not await send(runtime, guard)
    assert effects == [] and runtime.fake.invocations == []


@pytest.mark.parametrize('cancel', [True, False])
async def test_cancel_or_gate_close_while_queued_does_not_deliver(runtime, monkeypatch, cancel):
    entered, release = asyncio.Event(), asyncio.Event()
    dispatch = runtime.native._dispatch
    async def delay(cmd):
        entered.set()
        await release.wait()
        await dispatch(cmd)
    monkeypatch.setattr(runtime.native, '_dispatch', delay)
    guard = Mock(return_value=None)
    task = asyncio.create_task(send(runtime, guard))
    await asyncio.wait_for(entered.wait(), 2)
    close = None
    if cancel:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        close = asyncio.create_task(runtime.entry.interact_gate.close_and_drain())
        await asyncio.sleep(0)
    release.set()
    if not cancel:
        assert not await task
        await close
    # A legacy send after the rejected command is a FIFO supervisor barrier.
    await runtime.native.send('old Text remains allowed')
    assert await wait_for_state(runtime.native, HarnessState.IDLE)
    assert [call['query'] for call in runtime.fake.invocations] == ['old Text remains allowed']
    guard.assert_not_called()
    assert runtime.entry.interact_gate.inflight == 0


async def test_guarded_missing_manager_never_creates_runtime(monkeypatch):
    monkeypatch.setattr(GLOBAL_RUNNER, '_team_runtime_manager', None)
    creator = Mock(side_effect=AssertionError('must not create runtime'))
    monkeypatch.setattr(GLOBAL_RUNNER, '_get_team_runtime_manager', creator)
    result = await Runner.interact_agent_team(GodViewMessage(body='hi'), team_name='missing',
        session_id='missing', before_effect=lambda: None)
    assert not result and result.reason == 'not_active'
    creator.assert_not_called()


async def test_guarded_steer_uses_original_running_round(runtime):
    from tests.unit_tests.agent_teams.harness.fixtures import wait_invoke_running
    runtime.fake.sleep_seconds = 10
    await runtime.native.send('original work')
    await wait_invoke_running(runtime.fake)
    original = runtime.native._st.active
    guard = Mock(return_value=None)
    assert await send(runtime, guard)
    assert runtime.native._st.active is original
    assert runtime.native.event_handler.interaction_queues.steering.get_nowait() == 'guarded input'
    guard.assert_called_once_with()
    assert len(runtime.fake.invocations) == 1


async def test_wrong_session_rejects_before_input_or_guard(runtime):
    guard = Mock(return_value=None)
    result = await Runner.interact_agent_team(GodViewMessage(body='foreign input'),
        team_name=runtime.entry.team_name, session_id='foreign-session', before_effect=guard)
    assert not result and result.reason == 'not_active'
    assert runtime.fake.invocations == []
    assert runtime.entry.interact_gate.inflight == 0
    guard.assert_not_called()


async def test_observer_cancel_after_admission_keeps_original_round(runtime, monkeypatch):
    from tests.unit_tests.agent_teams.harness.fixtures import wait_invoke_running

    entered, release = asyncio.Event(), asyncio.Event()
    emit = runtime.native._emit_round
    async def delay(kind, *args, **kwargs):
        if kind == 'started':
            entered.set()
            await release.wait()
        return await emit(kind, *args, **kwargs)
    monkeypatch.setattr(runtime.native, '_emit_round', delay)
    runtime.fake.sleep_seconds = 10
    guard = Mock(return_value=None)
    task = asyncio.create_task(send(runtime, guard))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        await wait_invoke_running(runtime.fake)
        original = runtime.native._st.active
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()
        # FIFO barrier behind the already admitted command; its old ACK is gone,
        # but neither the original work nor its supervisor may be cancelled.
        await runtime.native.send('next input')
        assert runtime.native._st.active is original
        assert len(runtime.fake.invocations) == 1
        assert runtime.entry.interact_gate.inflight == 0
        assert not runtime.native._st.supervisor_task.done()
        guard.assert_called_once_with()
    finally:
        release.set()
        await runtime.native.abort(immediate=True)
