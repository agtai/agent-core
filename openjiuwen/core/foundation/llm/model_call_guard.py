# -*- coding: UTF-8 -*-
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Scoped admission immediately before the original model client's inference entry."""

from __future__ import annotations

import inspect
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from typing import Any, Awaitable, Callable, Iterator, TYPE_CHECKING

if TYPE_CHECKING:
    from openjiuwen.core.foundation.llm.model import Model


ModelCallGuard = Callable[[dict[str, Any]], dict[str, Any] | Awaitable[dict[str, Any]]]


@dataclass(frozen=True)
class _Guard:
    model: Model
    client: object
    before_call: ModelCallGuard


_current_guard: ContextVar[_Guard | None] = ContextVar("_openjiuwen_model_call_guard", default=None)
_current_target: ContextVar[tuple[object, object] | None] = ContextVar("_openjiuwen_model_call_target", default=None)


class _ModelCallGuardTimeout(Exception):
    """Carry admission timeout through Model's existing provider-timeout handler."""

    def __init__(self, error: TimeoutError):
        super().__init__(str(error))
        self.error = error


@contextmanager
def model_call_guard_scope(model: Model, before_call: ModelCallGuard) -> Iterator[None]:
    """Guard calls through exactly ``model`` and its current client in this context.

    ``before_call`` receives a shallow copy of the actual client keyword arguments,
    after Model's input notifications and transforms. It may return a dict directly
    or asynchronously; that dict is passed to the original client without another
    SDK input callback. Exceptions and cancellation propagate without entering the
    client. A replaced client fails closed. Provider-internal waits/retries are
    outside this admission boundary.

    Wrap ``await model.invoke(...)`` in the scope. For an iterator exposed to other
    callers, scope each ``await anext(iterator)`` / ``await iterator.aclose()`` and
    yield outside the scope. Enter and exit must occur in the same context. The
    stream guard runs once at client entry, not for every frame. Nested scopes
    restore their predecessor; other Models do not inherit this guard, even when
    they share a client. Nested calls to the selected Model use the same guard.
    """
    from openjiuwen.core.foundation.llm.model import Model

    if not isinstance(model, Model) or model._client is None:
        raise TypeError("model_call_guard_scope requires a configured Model")
    if not callable(before_call):
        raise TypeError("before_call must be callable")
    token = _current_guard.set(_Guard(model, model._client, before_call))
    try:
        yield
    finally:
        _current_guard.reset(token)


@contextmanager
def _model_call_target(model: Model, client: object) -> Iterator[None]:
    guard = _current_guard.get()
    if guard is not None and guard.model is model and guard.client is not client:
        raise RuntimeError("guarded Model client was replaced")
    token = _current_target.set((model, client))
    try:
        yield
    finally:
        _current_target.reset(token)


def _as_keyword_arguments(fn: Callable, args: tuple, kwargs: dict) -> dict:
    if not args:
        return dict(kwargs)
    signature = inspect.signature(fn)
    bound = signature.bind(*args, **kwargs)
    result = {}
    for name, value in bound.arguments.items():
        kind = signature.parameters[name].kind
        if kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.VAR_POSITIONAL):
            raise TypeError("guarded model calls require keyword-representable arguments")
        if kind == inspect.Parameter.VAR_KEYWORD:
            result.update(value)
        else:
            result[name] = value
    return result


async def _prepare_model_call(client: object, fn: Callable, args: tuple, kwargs: dict) -> tuple[tuple, dict]:
    guard = _current_guard.get()
    target = _current_target.get()
    if guard is None or target is None or target[0] is not guard.model:
        return args, kwargs
    if target[1] is not guard.client or client is not guard.client or guard.model._client is not guard.client:
        raise RuntimeError("guarded Model client was replaced")
    final_kwargs = guard.before_call(_as_keyword_arguments(fn, args, kwargs))
    if inspect.isawaitable(final_kwargs):
        final_kwargs = await final_kwargs
    if not isinstance(final_kwargs, dict):
        raise TypeError("model call guard must return a dict of client keyword arguments")
    if guard.model._client is not guard.client:
        raise RuntimeError("guarded Model client was replaced")
    return (), final_kwargs


def _guard_client_inference(client: object, fn: Callable, *, stream: bool = False) -> Callable:
    # A custom registry may return one client to several Models. Keep the one
    # guard at the original entry, inside every Model's existing callback layer.
    if getattr(fn, "_model_call_guard_client", None) is client:
        return fn

    if stream:
        @wraps(fn)
        async def guarded_stream(*args, **kwargs):
            try:
                call_args, call_kwargs = await _prepare_model_call(client, fn, args, kwargs)
            except TimeoutError as error:
                raise _ModelCallGuardTimeout(error) from error
            source = fn(*call_args, **call_kwargs)
            try:
                if hasattr(source, "__aiter__"):
                    async for item in source:
                        yield item
                else:
                    for item in source:
                        yield item
            finally:
                close = getattr(source, "aclose", None)
                if callable(close):
                    await close()

        wrapped = guarded_stream
    else:
        @wraps(fn)
        async def guarded_invoke(*args, **kwargs):
            call_args, call_kwargs = await _prepare_model_call(client, fn, args, kwargs)
            result = fn(*call_args, **call_kwargs)
            return await result if inspect.isawaitable(result) else result

        wrapped = guarded_invoke
    wrapped._model_call_guard_client = client
    return wrapped
