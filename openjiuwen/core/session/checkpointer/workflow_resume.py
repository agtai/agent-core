# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Opt-in, single-call checkpoint admission for exact workflow continuation."""

from __future__ import annotations

import base64
import asyncio
import hashlib
import inspect
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass
from typing import Callable


class WorkflowResumeError(ValueError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class _GuardFailure(Exception):
    """Keep authorization errors out of legacy timeout/recursion translation."""

    def __init__(self, error):
        super().__init__(str(error))
        self.error = error


@dataclass(frozen=True, kw_only=True)
class WorkflowResumeGuard:
    """Require a proven checkpoint and synchronous authorization before restoration.

    Use with ``Runner.run_workflow(..., resume_guard=...)``. This mode supports
    complete answers to interrupted root nodes; raw inputs, nested interruptions,
    old checkpoints without a pairing proof, and unknown providers fail closed.
    The callback must synchronously return None or raise; its exceptions
    propagate unchanged. Other return values are rejected.
    """

    before_effect: Callable[[], None]

    def __post_init__(self):
        if not callable(self.before_effect):
            raise TypeError("before_effect must be callable")


def _sync_admit(callback):
    result = callback()
    if inspect.isawaitable(result):
        close = getattr(result, "close", None)
        if callable(close):
            close()
        raise WorkflowResumeError("strict_resume_async_guard_unsupported")
    if result is not None:
        raise WorkflowResumeError("strict_resume_guard_result_invalid")


@dataclass
class _ResumeTarget:
    workflow: object
    public_session: object
    session_id: str
    workflow_id: str
    inputs: object
    guard: WorkflowResumeGuard
    caller_task: object
    checkpointer: object = None

    def validate(self, session, checkpointer):
        if self.caller_task.cancelling() or self.caller_task.done() or asyncio.current_task().cancelling():
            raise asyncio.CancelledError
        if (session.session_id() != self.session_id or session.workflow_id() != self.workflow_id
                or self.workflow._card.id != self.workflow_id
                or checkpointer is not self.checkpointer or session.checkpointer() is not self.checkpointer):
            raise WorkflowResumeError("strict_resume_owner_mismatch")

    def admit(self, session, checkpointer):
        self.validate(session, checkpointer)
        try:
            _sync_admit(self.guard.before_effect)
        except Exception as error:
            raise _GuardFailure(error) from error
        self.validate(session, checkpointer)


_resume_target: ContextVar[_ResumeTarget | None] = ContextVar("_workflow_resume_target", default=None)
PREPARED_WORKFLOW_RESUME = "_prepared_workflow_resume"


@contextmanager
def workflow_resume_scope(workflow, session, inputs, guard):
    if guard is None:
        yield
        return
    from openjiuwen.core.session.interaction.interactive_input import InteractiveInput

    if not isinstance(guard, WorkflowResumeGuard) or not isinstance(inputs, InteractiveInput):
        raise WorkflowResumeError("strict_resume_input_required")
    sid, wid = session.get_session_id(), workflow._card.id
    if any(not isinstance(value, str) or not value or value != value.strip() for value in (sid, wid)):
        raise WorkflowResumeError("strict_resume_scope_invalid")
    target = _ResumeTarget(workflow, session, sid, wid, deepcopy(inputs), guard, asyncio.current_task())
    token = _resume_target.set(target)
    try:
        yield
    except _GuardFailure as failure:
        raise failure.error from None
    finally:
        _resume_target.reset(token)


def get_workflow_resume_target(workflow, session, inputs):
    target = _resume_target.get()
    if target is None or target.workflow is not workflow:
        return None
    if (target.public_session is not session or session.get_session_id() != target.session_id
            or workflow._card.id != target.workflow_id or inputs != target.inputs):
        raise WorkflowResumeError("strict_resume_owner_mismatch")
    return target


def normalized_blob(pair):
    if pair is None or len(pair) != 2 or pair[0] is None or pair[1] is None:
        raise WorkflowResumeError("strict_resume_checkpoint_missing")
    kind, value = pair
    if isinstance(kind, bytes):
        kind = kind.decode("utf-8")
    if isinstance(value, str):
        value = base64.b64decode(value, validate=True)
    if not isinstance(kind, str) or not kind or kind == "empty" or not isinstance(value, bytes) or not value:
        raise WorkflowResumeError("strict_resume_checkpoint_missing")
    return kind, value


def checkpoint_proof(session_id, workflow_id, state_blob, updates_blob, graph_blob):
    """Bind all saved parts and their exact scope; no inference from mere existence."""
    parts = tuple(normalized_blob(part) for part in (state_blob, updates_blob, graph_blob))
    digest = hashlib.sha256(b"workflow-resume-v1")
    # Length framing is independent of Python object aliasing and KV decoding.
    for value in (session_id.encode(), workflow_id.encode(),
                  *(value for kind, blob in parts for value in (kind.encode(), blob))):
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)
    return digest.hexdigest()


@dataclass
class PreparedWorkflowResume:
    session_id: str
    workflow_id: str
    state: dict
    updates: dict
    graph_state: object
    inputs: object
    consumed: bool = False

    def apply(self, session):
        from openjiuwen.core.common.constants.constant import INTERACTIVE_INPUT
        from openjiuwen.core.session.internal.workflow import NodeSession

        session.state().set_state(self.state)
        for node_id, value in self.inputs.user_inputs.items():
            node = NodeSession(session, node_id)
            previous = node.state().get(INTERACTIVE_INPUT)
            answers = [*previous, value] if isinstance(previous, list) else [value]
            node.state().update({INTERACTIVE_INPUT: answers})
        session.state().commit()
        session.state().set_updates(self.updates)

    def consume_graph(self, session_id, workflow_id):
        if self.consumed or session_id != self.session_id or workflow_id != self.workflow_id:
            raise WorkflowResumeError("strict_resume_snapshot_mismatch")
        self.consumed = True
        return self.graph_state


def prepare_snapshot(session, inputs, state, updates, graph_state):
    from openjiuwen.core.common.constants.constant import INTERACTION
    from openjiuwen.core.graph.pregel.base import GraphInterrupt, Interrupt
    from openjiuwen.core.graph.pregel.constants import TASK_STATUS_INTERRUPT
    from openjiuwen.core.graph.store import GraphState, PendingNode
    from openjiuwen.core.session.interaction.interaction import InteractionOutput
    from openjiuwen.core.session.stream.base import OutputSchema
    from openjiuwen.core.session.state.base import (
        IO_STATE_KEY, GLOBAL_STATE_KEY, COMP_STATE_KEY, WORKFLOW_STATE_KEY,
        IO_STATE_UPDATES_KEY, GLOBAL_STATE_UPDATES_KEY, COMP_STATE_UPDATES_KEY, WORKFLOW_STATE_UPDATES_KEY,
    )

    for value, keys in ((state, (IO_STATE_KEY, GLOBAL_STATE_KEY, COMP_STATE_KEY, WORKFLOW_STATE_KEY)),
                        (updates, (IO_STATE_UPDATES_KEY, GLOBAL_STATE_UPDATES_KEY,
                                   COMP_STATE_UPDATES_KEY, WORKFLOW_STATE_UPDATES_KEY))):
        if (not isinstance(value, dict) or set(value) != set(keys)
                or any(not isinstance(value[key], dict) and not (index == 1 and value[key] is None)
                       for index, key in enumerate(keys))):
            raise WorkflowResumeError("strict_resume_checkpoint_invalid")
    if (not isinstance(graph_state, GraphState) or graph_state.ns != session.workflow_id()
            or type(graph_state.step) is not int or graph_state.step < 0
            or not isinstance(graph_state.channel_values, dict) or not isinstance(graph_state.pending_buffer, list)
            or not isinstance(graph_state.node_version, dict) or not isinstance(graph_state.pending_node, dict)
            or not graph_state.pending_node):
        raise WorkflowResumeError("strict_resume_graph_invalid")
    for name, node in graph_state.pending_node.items():
        if (not isinstance(name, str) or not isinstance(node, PendingNode)
                or node.node_name != name or node.status != TASK_STATUS_INTERRUPT):
            raise WorkflowResumeError("strict_resume_graph_invalid")
        if not isinstance(node.exception, list) or not node.exception:
            raise WorkflowResumeError("strict_resume_inputs_unsupported")
        for exception in node.exception:
            if (not isinstance(exception, GraphInterrupt) or not isinstance(exception.value, tuple)
                    or not exception.value):
                raise WorkflowResumeError("strict_resume_inputs_unsupported")
            for interruption in exception.value:
                if (not isinstance(interruption, Interrupt) or not isinstance(interruption.value, OutputSchema)
                        or interruption.value.type != INTERACTION
                        or not isinstance(interruption.value.payload, InteractionOutput)
                        or interruption.value.payload.id != name):
                    # A parent graph's interrupted node is not proof of its
                    # child graph; answering the parent must not restart it.
                    raise WorkflowResumeError("strict_resume_inputs_unsupported")
    if (inputs.raw_inputs is not None or not inputs.user_inputs
            or set(inputs.user_inputs) != set(graph_state.pending_node)
            or any(value is None for value in inputs.user_inputs.values())):
        raise WorkflowResumeError("strict_resume_inputs_unsupported")
    return PreparedWorkflowResume(session.session_id(), session.workflow_id(), state, updates,
                                  graph_state, deepcopy(inputs))
