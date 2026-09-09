# -*- coding: UTF-8 -*-
# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

from __future__ import annotations

import asyncio
import copy
import inspect
from dataclasses import dataclass
from typing import Any, Callable, Dict

from pydantic import BaseModel, Field, PrivateAttr

from openjiuwen.core.common.exception.codes import StatusCode
from openjiuwen.core.common.exception.errors import build_error

_sentinel = object()


class AgentInputError(ValueError):
    """An exact Agent input was rejected before its pending state was claimed."""

    def __init__(self, code):
        super().__init__(code)
        self.code = code


@dataclass
class _InputControl:
    before_effect: Callable[[], None] | None = None
    prepare_effect: Callable | None = None
    sdk_before_effect: Callable[[], None] | None = None
    receipt: asyncio.Future | None = None
    claimed: bool = False


class InteractiveInput(BaseModel):
    # key is id of interaction, value is input for the id
    user_inputs: Dict[str, Any] = Field(default_factory=dict)

    # input not bind to any id, used for the first interaction
    raw_inputs: Any = Field(default=None)
    expected_pending_token: str | None = Field(default=None, frozen=True)
    _control: _InputControl = PrivateAttr(default_factory=_InputControl)

    def __init__(self, raw_inputs: Any = _sentinel, *, expected_pending_token: str | None = None,
                 before_effect: Callable[[], None] | None = None, prepare_effect: Callable | None = None):
        if expected_pending_token is not None:
            try:
                valid = (isinstance(expected_pending_token, str) and bool(expected_pending_token.strip())
                         and len(expected_pending_token.encode("utf-8")) <= 256 and "\x00" not in expected_pending_token)
            except UnicodeError:
                valid = False
            if not valid:
                raise AgentInputError("pending_token_invalid")
        for callback in (before_effect, prepare_effect):
            if callback is not None and (not callable(callback) or expected_pending_token is None):
                raise AgentInputError("input_guard_invalid")
        super().__init__(expected_pending_token=expected_pending_token)
        self._control.before_effect = before_effect
        self._control.prepare_effect = prepare_effect
        if raw_inputs is None:
            raise build_error(StatusCode.INTERACTION_INPUT_INVALID, reason="value of raw_inputs is none")
        if raw_inputs is _sentinel:
            self.raw_inputs = None
            return
        self.raw_inputs = raw_inputs

    @property
    def before_effect(self):
        """Trusted process-local authorization; never part of a serialized input."""
        return self._control.before_effect

    @property
    def prepare_effect(self):
        return self._control.prepare_effect

    async def _prepare_claim(self):
        callback = self.prepare_effect
        if callback is not None:
            result = callback()
            if inspect.isawaitable(result):
                result = await result
            if result is not None:
                raise AgentInputError("input_guard_result_invalid")

    @property
    def claimed(self) -> bool:
        return self._control.claimed

    def __deepcopy__(self, memo=None):
        memo = {} if memo is None else memo
        clone = type(self).model_construct(
            user_inputs=copy.deepcopy(self.user_inputs, memo),
            raw_inputs=copy.deepcopy(self.raw_inputs, memo),
            expected_pending_token=self.expected_pending_token,
        )
        clone._control = self._control
        memo[id(self)] = clone
        return clone

    def __getstate__(self):
        state = super().__getstate__()
        state["__pydantic_private__"] = {}
        return state

    def __setstate__(self, state):
        super().__setstate__(state)
        self._control = _InputControl()

    def _begin_claim_receipt(self):
        if self._control.receipt is not None or self.claimed:
            raise AgentInputError("input_already_dispatched")
        future = asyncio.get_running_loop().create_future()
        # A cancelled observer need not retrieve a later owner rejection.
        future.add_done_callback(lambda item: None if item.cancelled() else item.exception())
        self._control.receipt = future
        return future

    def _accept_claim(self):
        self._control.claimed = True
        receipt = self._control.receipt
        if receipt is not None and not receipt.done():
            receipt.set_result({"accepted": True, "pending_token": self.expected_pending_token})

    def _reject_claim(self, error):
        receipt = self._control.receipt
        if receipt is not None and not receipt.done():
            receipt.set_exception(error)

    def update(self, node_id: str, value: Any):
        if self.raw_inputs is not None:
            raise build_error(StatusCode.INTERACTION_INPUT_INVALID, reason="raw_inputs existed, update is invalid")
        if node_id is None or value is None:
            raise build_error(StatusCode.INTERACTION_INPUT_INVALID, reason="value is none or node_id is none")
        self.user_inputs[node_id] = value
