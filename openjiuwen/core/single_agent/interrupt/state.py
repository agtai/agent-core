# -*- coding: UTF-8 -*-
# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

from __future__ import annotations
from dataclasses import field
from typing import Dict
from pydantic import BaseModel, field_validator

from openjiuwen.core.foundation.llm import AssistantMessage
from openjiuwen.core.foundation.llm.schema.tool_call import ToolCall
from openjiuwen.core.single_agent.interrupt.response import InterruptRequest

INTERRUPTION_KEY = "__react_agent_interruption__"
RESUME_USER_INPUT_KEY = "_resume_user_input"
INTERRUPT_AUTO_CONFIRM_KEY = "__interrupt_auto_confirm__"
RESUME_START_ITERATION_KEY = "_resume_start_iteration"


class BaseInterruptionState(BaseModel):
    """Common interruption state fields."""
    ai_message: AssistantMessage
    iteration: int
    original_query: str = ""


class ToolInterruptEntry(BaseModel):
    tool_call: ToolCall
    interrupt_requests: Dict[str, InterruptRequest] = field(default_factory=dict)
    is_sub_agent: bool = False


class ToolInterruptionState(BaseInterruptionState):
    """Tool interruption state for resume support.
    """
    interrupted_tools: Dict[str, ToolInterruptEntry] = field(default_factory=dict)
    auto_confirm_mapping: Dict[str, str] = field(default_factory=dict)
    pending_token: str | None = None
    execution_origin: dict | None = None

    @field_validator("execution_origin", mode="before")
    @classmethod
    def copy_execution_origin(cls, value):
        return copy_execution_origin(value)


def copy_execution_origin(value):
    if value is None:
        return None
    from openjiuwen.core.session.agent import _copy_json_mapping

    # Leave room for SDK identities around an existing legal 64 KiB context.
    snapshot = _copy_json_mapping(value, max_bytes=131072)
    if (set(snapshot) != {"kind", "request_id", "session_id", "run_context"}
            or snapshot["kind"] not in ("user", "goal")
            or not isinstance(snapshot["session_id"], str) or not snapshot["session_id"]
            or (snapshot["request_id"] is not None and not isinstance(snapshot["request_id"], str))
            or (snapshot["run_context"] is not None and not isinstance(snapshot["run_context"], dict))
            or (snapshot["kind"] == "goal" and snapshot["request_id"] is not None)):
        raise ValueError("invalid SDK execution origin")
    return snapshot
