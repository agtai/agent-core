# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
"""AgentCallbackManager Class Definition"""

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Optional

from openjiuwen.core.single_agent.rail.base import AgentCallbackContext, AgentCallbackEvent, AgentRail, AnyAgentCallback


@dataclass
class _ExecutionRail:
    callbacks: dict
    before_events: frozenset[AgentCallbackEvent]
    active: bool = True


_execution_rails: ContextVar[tuple[_ExecutionRail, ...]] = ContextVar("agent_execution_rails", default=())


def _require_active(scopes, event):
    if any(event in owner.callbacks and not owner.active for owner in scopes):
        from openjiuwen.core.runner.callback.errors import AbortError

        raise AbortError("SCOPED_AGENT_RAIL_CLOSED")


@contextmanager
def scoped_agent_rail(rail: AgentRail, *, before_events: frozenset[AgentCallbackEvent] = frozenset()):
    """Apply a trusted rail to this execution, including inherited async tasks.

    Callbacks run after ordinary instance hooks unless their event is listed in
    before_events (for guards that must also precede hook projections). Each
    phase runs in outer-to-inner scope order, independent of registry priorities.
    They propagate failures instead of using the ordinary callback error policy.
    The scope is independent of ``ctx.agent``; guards must validate their exact
    identity themselves. Exiting revokes inherited scopes, even in unfinished
    child tasks. This is not a sandbox for code that replaces its Python context.
    No tools are registered and no process-global callback registry is changed.
    """
    if not isinstance(rail, AgentRail):
        raise TypeError("execution scope requires an AgentRail")
    callbacks = dict(rail.get_callbacks())
    if not isinstance(before_events, frozenset) or not before_events <= callbacks.keys():
        raise ValueError("before_events must name callbacks implemented by the rail")
    owner = _ExecutionRail(callbacks, before_events)
    token = _execution_rails.set((*_execution_rails.get(), owner))
    try:
        yield
    finally:
        owner.active = False
        _execution_rails.reset(token)


class AgentCallbackManager:
    """Manager for Middlewares.

    Supports both function-style and middleware-style callbacks with priority ordering.
    """

    def __init__(self, agent_id, event_namespace: Optional[str] = None):
        self.agent_id = agent_id
        self.event_namespace = event_namespace or agent_id

    async def register_callback(
        self, event: AgentCallbackEvent, callback: AnyAgentCallback, priority: int = 100
    ) -> "AgentCallbackManager":
        """Register an agent callback for an event.

        Args:
            event: The agent callback event to register for
            callback: The callback function (sync or async)
            priority: Execution priority (higher = runs first)

        Returns:
            self for chaining
        """
        # Wrap sync callbacks to async
        if not asyncio.iscoroutinefunction(callback):
            original = callback

            async def async_wrapper(ctx: AgentCallbackContext):
                original(ctx)

            callback = async_wrapper

        agent_event = self._get_agent_event(event)
        from openjiuwen.core.runner import Runner

        await Runner.callback_framework.register(agent_event, callback, priority=priority)
        return self

    async def register_rail(self, rail: AgentRail, agent: "object") -> "AgentCallbackManager":
        """Register a rail instance.

        Args:
            rail: AgentRail instance
            agent: BaseAgent instance (for tool registration)

        Returns:
            self for chaining
        """
        for event, callback in rail.get_callbacks().items():
            await self.register_callback(event, callback, rail.callback_priority(event))

        return self

    async def unregister_rail(self, rail: AgentRail, agent: "object") -> None:
        """Unregister a rail instance.

        Args:
            rail: AgentRail instance to remove
            agent: BaseAgent instance (for tool removal)
        """
        for event, callback in rail.get_callbacks().items():
            await self.unregister(event, callback)

    async def unregister(self, event: AgentCallbackEvent, callback: AnyAgentCallback) -> None:
        """Unregister a hook callback.

        Args:
            event: The hook event
            callback: The callback to remove
        """
        agent_event = self._get_agent_event(event)
        from openjiuwen.core.runner import Runner

        await Runner.callback_framework.unregister(agent_event, callback)

    async def clear(self, event: Optional[AgentCallbackEvent] = None) -> None:
        """Clear hooks.

        Args:
            event: Specific event to clear, or None to clear all
        """
        from openjiuwen.core.runner import Runner

        if event:
            agent_event = self._get_agent_event(event)
            await Runner.callback_framework.unregister_event(agent_event)
        else:
            for e in AgentCallbackEvent:
                agent_event = self._get_agent_event(e)
                await Runner.callback_framework.unregister_event(agent_event)

    def has_hooks(self, event: AgentCallbackEvent) -> bool:
        """Check if any hooks are registered for an event.

        Args:
            event: The hook event to check

        Returns:
            True if hooks are registered
        """
        agent_event = self._get_agent_event(event)
        from openjiuwen.core.runner import Runner

        return (any(event in owner.callbacks for owner in _execution_rails.get())
                or len(Runner.callback_framework.list_callbacks(agent_event)) > 0)

    async def execute(
        self,
        event: AgentCallbackEvent,
        ctx: AgentCallbackContext,
    ) -> AgentCallbackContext:
        """Execute all hooks for an event.

        Args:
            event: The hook event
            ctx: The hook context

        Returns:
            The (potentially modified) context
        """
        from openjiuwen.core.runner import Runner

        agent_event = self._get_agent_event(event)
        scopes = _execution_rails.get()
        _require_active(scopes, event)
        for owner in scopes:
            if event in owner.before_events:
                await owner.callbacks[event](ctx)
                _require_active(scopes, event)
        await Runner.callback_framework.trigger(agent_event, ctx)
        _require_active(scopes, event)
        for owner in scopes:
            callback = owner.callbacks.get(event)
            if callback is not None and event not in owner.before_events:
                await callback(ctx)
                _require_active(scopes, event)
        return ctx

    def _get_agent_event(self, event: AgentCallbackEvent) -> str:
        """Unified generation of event name with instance prefix to avoid duplicate name

        Args:
            event: Original callback event

        Returns:
            Event name string prefixed with the callback namespace
        """
        return f"{self.event_namespace}_{event}"
