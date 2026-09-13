# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Process-local Executor checkpoint binding; never supplied by a model/request."""

import hashlib
import json
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

from openjiuwen.core.foundation.llm import AssistantMessage, ToolMessage
from openjiuwen.core.single_agent.rail.base import AgentRail, ToolCallInputs

_READ_ROUND_LIMIT = 6
_READ_BATCH_TOOLS = frozenset({"read_file"})


def _unprocessed_message(message: Any, expected_type: type) -> bool:
    # SessionModelContext installs this identity key on original messages too.
    return type(message) is expected_type and set(message.metadata) <= {"context_message_id"}


class BackgroundReadProgress:
    """Converge repeated completed read batches without interpreting file text."""

    def __init__(self) -> None:
        self._digest: str | None = None
        self._call_ids: tuple[str, ...] = ()
        self._rounds = 0

    def _reset(self) -> None:
        self._digest, self._call_ids, self._rounds = None, (), 0

    def stalled(self, context: Any) -> bool:
        messages = context.get_messages(size=64)
        index = next((i for i in range(len(messages) - 1, -1, -1) if isinstance(messages[i], AssistantMessage)), None)
        if index is None:
            self._reset()
            return False
        calls = messages[index].tool_calls
        tail = messages[index + 1 :]
        if (
            not _unprocessed_message(messages[index], AssistantMessage)
            or not calls
            or len(calls) > 16
            or len(tail) != len(calls)
            or not all(_unprocessed_message(message, ToolMessage) for message in tail)
            or any(not isinstance(call.id, str) or not call.id or len(call.id) > 256 for call in calls)
        ):
            # New accepted requirements, incomplete calls and a normal answer
            # all end the consecutive read-only sequence.
            self._reset()
            return False
        results = {message.tool_call_id: message.content for message in tail}
        ids = tuple(sorted(call.id for call in calls))
        if len(results) != len(calls) or len(set(ids)) != len(calls) or set(ids) != set(results):
            self._reset()
            return False
        batch = []
        total_bytes = 0
        for call in calls:
            if call.name not in _READ_BATCH_TOOLS or not isinstance(results[call.id], str):
                self._reset()
                return False
            try:
                args = json.loads(call.arguments) if isinstance(call.arguments, str) else dict(call.arguments)
                if not isinstance(args, dict):
                    raise ValueError("non-object tool arguments")
                args.pop("call_goal", None)
                argument_bytes = json.dumps(
                    args, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
                ).encode("utf-8")
                result_bytes = results[call.id].encode("utf-8")
            except (TypeError, ValueError, UnicodeError):
                self._reset()
                return False
            total_bytes += len(argument_bytes) + len(result_bytes)
            if total_bytes > 1_048_576:
                self._reset()
                return False
            # The SDK plain-text ReadFileTool returns this exact cat_n form.
            # Error/interruption placeholders, empty/PDF/notebook results and
            # offloaded/compressed representations have no comparable evidence.
            lines = results[call.id].splitlines()
            if not lines or any(not line.startswith(f"{number:>6}\t") for number, line in enumerate(lines, 1)):
                self._reset()
                return False
            batch.append(
                (call.name, hashlib.sha256(argument_bytes).hexdigest(), hashlib.sha256(result_bytes).hexdigest())
            )
        digest = hashlib.sha256(json.dumps(sorted(batch)).encode("utf-8")).hexdigest()
        if digest == self._digest and ids == self._call_ids:
            return False  # Repeated callback for one completed model round.
        self._rounds = self._rounds + 1 if digest == self._digest else 1
        self._digest, self._call_ids = digest, ids
        return self._rounds >= _READ_ROUND_LIMIT


@dataclass(slots=True)
class BackgroundTaskCheckpoint:
    session_id: str
    adopt: Callable[[Any], Awaitable[None]]
    closed: bool = False
    file_plan: Any | None = None
    failure_reason: str | None = field(default=None, init=False)
    _read_progress: BackgroundReadProgress = field(default_factory=BackgroundReadProgress, init=False)

    def check_model_progress(self, context: Any) -> None:
        if self.closed:
            raise RuntimeError("BACKGROUND_TASK_CHECKPOINT_BINDING_MISMATCH")
        self.raise_if_failed()
        if self._read_progress.stalled(context):
            self.failure_reason = "BACKGROUND_TASK_READ_NO_PROGRESS"
            self.raise_if_failed()

    def raise_if_failed(self) -> None:
        if self.failure_reason is not None:
            raise RuntimeError(self.failure_reason)


_current: ContextVar[BackgroundTaskCheckpoint | None] = ContextVar("background_task_model_checkpoint", default=None)


@contextmanager
def background_task_checkpoint(
    session_id: str, adopt: Callable[[Any], Awaitable[None]], *, file_plan=None
) -> Iterator[None]:
    owner = BackgroundTaskCheckpoint(session_id, adopt, file_plan=file_plan)
    token = _current.set(owner)
    try:
        yield
    finally:
        owner.closed = True
        _current.reset(token)


def current_background_task_checkpoint(session_id: str) -> BackgroundTaskCheckpoint | None:
    owner = _current.get()
    if owner is not None and (owner.closed or owner.session_id != session_id):
        raise RuntimeError("BACKGROUND_TASK_CHECKPOINT_BINDING_MISMATCH")
    return owner


class TaskCheckpointRail(AgentRail):
    """Task policy on native callbacks; the application supplies identity facts.

    Bind with scoped_agent_rail, not just to the root's callback registry: tool
    callbacks can select a different Agent from their context. No Voice or Host
    type is imported and this rail creates no execution or persistence owner.
    """

    def __init__(self, checkpoint, *, root_agent, binding_is_current, session_identity):
        super().__init__()
        self.checkpoint = checkpoint
        self.root_agent = root_agent
        self.binding_is_current = binding_is_current
        self.session_identity = session_identity

    async def before_model_call(self, ctx):
        if self.checkpoint.closed or not self.binding_is_current():
            raise RuntimeError("BACKGROUND_TASK_CHECKPOINT_STALE")
        if ctx.agent is self.root_agent:
            await self.checkpoint.adopt(ctx.context)
            self.checkpoint.check_model_progress(ctx.context)

    async def before_tool_call(self, ctx):
        checkpoint = self.checkpoint
        if checkpoint.file_plan is None:
            return
        try:
            if (checkpoint.closed or not self.binding_is_current()
                    or ctx.agent is not self.root_agent
                    or self.session_identity(ctx) != checkpoint.session_id
                    or not isinstance(ctx.inputs, ToolCallInputs)
                    or ctx.inputs.tool_name != getattr(ctx.inputs.tool_call, "name", None)):
                raise RuntimeError("FILE_EFFECT_TOOL_IDENTITY_MISMATCH")
            arguments = ctx.inputs.tool_call.arguments
            if isinstance(arguments, str):
                arguments = json.loads(arguments)
            await checkpoint.file_plan.before_tool(ctx.inputs.tool_name, arguments)
        except Exception as error:
            checkpoint.failure_reason = getattr(error, "reason", "BACKGROUND_FILE_EFFECT_REJECTED")
            raise


def file_effect_plan_tool():
    """An owned tool whose invocation uses only the exact live checkpoint."""
    from openjiuwen.core.foundation.tool import Tool, ToolCard
    from openjiuwen.harness.tools.base_tool import ToolOutput

    class DeclareFileEffectPlan(Tool):
        def __init__(self):
            super().__init__(
                ToolCard(
                    id="declare_file_effect_plan",
                    name="declare_file_effect_plan",
                    parallel_safe=False,
                    description="Propose the complete exact file effects of this authorized Task before any write. "
                    "This only narrows existing authority. Do not treat a replacement as user consent. "
                    "Use the current requirement_head provided by the server. Call alone and wait for acceptance.",
                    input_params={
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "requirement_head": {"type": "string"},
                            "preserve_existing": {
                                "type": "boolean",
                                "description": "True when all original files must remain unchanged.",
                            },
                            "effects": {
                                "type": "array",
                                "minItems": 1,
                                "maxItems": 32,
                                "items": {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "properties": {
                                        "path": {
                                            "type": "string",
                                            "description": "Exact relative file path using / separators.",
                                        },
                                        "operation": {"type": "string", "enum": ["create", "replace", "delete"]},
                                    },
                                    "required": ["path", "operation"],
                                },
                            },
                            "required_outputs": {"type": "array", "maxItems": 32, "items": {"type": "string"}},
                        },
                        "required": ["requirement_head", "preserve_existing", "effects", "required_outputs"],
                    },
                )
            )

        async def invoke(self, inputs, **kwargs):
            owner = _current.get()
            if owner is None or owner.closed or owner.file_plan is None:
                raise RuntimeError("FILE_EFFECT_PLAN_OWNER_REQUIRED")
            owner.raise_if_failed()
            try:
                plan = await owner.file_plan.seal(inputs)
            except ValueError as error:
                return ToolOutput(success=False, error=str(error))
            return ToolOutput(
                success=True,
                data={
                    "status": "accepted",
                    "revision": plan.revision,
                    "plan_digest": plan.digest,
                    "requirement_head": plan.requirement_head,
                    "effects": [effect.to_dict() for effect in plan.effects],
                    "required_outputs": list(plan.required_outputs),
                },
            )

        async def stream(self, inputs, **kwargs):
            yield await self.invoke(inputs, **kwargs)

    return DeclareFileEffectPlan()
