# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""The outer task loop must be bounded by default.

Under ``enable_task_loop`` the inner ReAct agent is configured with
``max_iterations=sys.maxsize`` (``DeepAgent._create_react_agent``), on the
premise that the outer task loop owns termination.  These tests pin the other
half of that premise: the auto-injected ``TaskCompletionRail`` has to carry
stop conditions, or nothing stops the loop at all.
"""

from __future__ import annotations

from openjiuwen.core.single_agent.schema.agent_card import AgentCard
from openjiuwen.harness.deep_agent import DeepAgent
from openjiuwen.harness.rails.task_completion_rail import TaskCompletionRail
from openjiuwen.harness.schema.config import DeepAgentConfig
from openjiuwen.harness.schema.stop_condition import (
    MaxRoundsEvaluator,
    TimeoutEvaluator,
)
from openjiuwen.harness.task_loop.loop_coordinator import LoopCoordinator


def _evaluator_types(rail: TaskCompletionRail) -> set[type]:
    return {type(ev) for ev in rail.build_evaluators()}


def test_rail_without_arguments_builds_no_stop_conditions() -> None:
    """The shape of the defect, pinned so it cannot come back silently.

    ``TaskCompletionRail()`` is a legitimate construction — a caller may want to
    supply every bound itself — but paired with an unbounded inner agent it
    leaves the loop with no way to end.
    """
    assert TaskCompletionRail().build_evaluators() == []


def test_a_coordinator_with_no_evaluators_never_stops() -> None:
    """``should_continue`` consults its evaluators with OR semantics, so an
    empty chain answers True forever — a loop that cannot end rather than one
    that ends late."""
    coord = LoopCoordinator(evaluators=[])
    coord.reset()
    for _ in range(1000):
        coord.increment_iteration()
    assert coord.should_continue() is True


def test_config_defaults_bound_the_task_loop() -> None:
    """Finite by default.  A bound nobody sets is a bound nobody has."""
    cfg = DeepAgentConfig()
    assert cfg.task_loop_max_rounds is not None
    assert cfg.task_loop_timeout_seconds is not None
    assert cfg.task_loop_max_rounds > 0
    assert cfg.task_loop_timeout_seconds > 0


def test_defaults_carry_into_the_rails_evaluators() -> None:
    """The config values have to reach the evaluator chain; carrying them only
    as far as the rail's attributes would look correct and stop nothing."""
    cfg = DeepAgentConfig()
    rail = TaskCompletionRail(
        max_rounds=cfg.task_loop_max_rounds,
        timeout_seconds=cfg.task_loop_timeout_seconds,
    )
    assert MaxRoundsEvaluator in _evaluator_types(rail)
    assert TimeoutEvaluator in _evaluator_types(rail)


def test_the_round_bound_actually_stops_a_coordinator() -> None:
    """End to end over the pieces the loop really uses: build the evaluators
    from config, hand them to a coordinator, and run it past the bound."""
    rail = TaskCompletionRail(max_rounds=3, timeout_seconds=None)
    coord = LoopCoordinator(evaluators=rail.build_evaluators())
    coord.reset()
    assert coord.should_continue() is True
    for _ in range(3):
        coord.increment_iteration()
    assert coord.should_continue() is False
    assert coord.stop_reason is not None


def test_an_explicit_none_still_removes_the_bound() -> None:
    """Unbounded stays reachable for callers that mean it — the default is the
    only thing that changes."""
    cfg = DeepAgentConfig(
        task_loop_max_rounds=None,
        task_loop_timeout_seconds=None,
    )
    rail = TaskCompletionRail(
        max_rounds=cfg.task_loop_max_rounds,
        timeout_seconds=cfg.task_loop_timeout_seconds,
    )
    assert rail.build_evaluators() == []


def _agent_with_task_loop(**overrides: object) -> DeepAgent:
    """A DeepAgent with its config-driven rails queued but not yet registered.

    This is the state ``start()`` finds the agent in: ``_queue_pending_rails``
    has run, ``_ensure_initialized`` has not.
    """
    agent = DeepAgent(AgentCard(name="deep", description="test"))
    agent._queue_pending_rails(DeepAgentConfig(enable_task_loop=True, **overrides))
    return agent


def test_the_injected_rail_is_still_pending_when_start_builds_the_loop() -> None:
    """``start()`` never calls ``_ensure_initialized``, so the rail that carries
    the bounds has not been registered yet when the coordinator is built."""
    agent = _agent_with_task_loop()
    assert agent._task_completion_rail is None
    assert agent.find_pending_rails_by_type(TaskCompletionRail)


def test_pending_rails_still_bound_the_coordinator() -> None:
    """Reading only ``_task_completion_rail`` would hand the coordinator an
    empty chain on that path, which is the unbounded loop again."""
    agent = _agent_with_task_loop()
    evaluator_types = {type(ev) for ev in agent._stop_condition_evaluators()}
    assert MaxRoundsEvaluator in evaluator_types
    assert TimeoutEvaluator in evaluator_types


def test_a_caller_supplied_rail_wins_over_the_config_defaults() -> None:
    """A rail passed through ``rails=`` is queued the same way, and its own
    bounds are the ones the loop must use."""
    caller_rail = TaskCompletionRail(max_rounds=7, timeout_seconds=None)
    agent = _agent_with_task_loop(rails=[caller_rail])
    agent._pending_rails = [rail for rail in agent._pending_rails if rail is not caller_rail]
    agent._pending_rails.insert(0, caller_rail)

    evaluators = agent._stop_condition_evaluators()
    assert [type(ev) for ev in evaluators] == [MaxRoundsEvaluator]


def test_no_task_loop_leaves_the_chain_empty() -> None:
    """With the task loop off the inner agent keeps its own max_iterations, so
    an empty chain here is not an unbounded loop."""
    agent = DeepAgent(AgentCard(name="deep", description="test"))
    agent._queue_pending_rails(DeepAgentConfig(enable_task_loop=False))
    assert agent._stop_condition_evaluators() == []


def test_the_fallback_rail_survives_an_unconfigured_agent() -> None:
    """``start()`` registers this rail, and a DeepAgent built directly rather
    than through ``create_deep_agent`` has no config to read bounds from."""
    agent = DeepAgent(AgentCard(name="deep", description="test"))
    assert agent._deep_config is None

    rail = agent._default_task_completion_rail()
    evaluator_types = {type(ev) for ev in rail.build_evaluators()}
    assert MaxRoundsEvaluator in evaluator_types
    assert TimeoutEvaluator in evaluator_types
