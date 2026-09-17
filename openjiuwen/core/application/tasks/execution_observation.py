# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Optional application telemetry, scoped to an execution rather than a process.

Callbacks observe only; task admission and persistence never depend on telemetry.
Context propagation also preserves nested filesystem spans in worker threads.
"""

import inspect
from contextlib import nullcontext
from contextvars import ContextVar
from functools import wraps

_observer = ContextVar("application_task_observer", default=None)


def profiled(stage, *identity_arguments):
    def decorate(function):
        def observer_for(args):
            application = getattr(args[0], "_application", None) if args else None
            return getattr(application, "telemetry", None) or _observer.get()

        if inspect.iscoroutinefunction(function):

            @wraps(function)
            async def observed(*args, **kwargs):
                observer = observer_for(args)
                if observer is None:
                    return await function(*args, **kwargs)
                token = _observer.set(observer)
                try:
                    return await observer.profiled(stage, *identity_arguments)(function)(*args, **kwargs)
                finally:
                    _observer.reset(token)
        else:

            @wraps(function)
            def observed(*args, **kwargs):
                observer = observer_for(args)
                if observer is None:
                    return function(*args, **kwargs)
                return observer.profiled(stage, *identity_arguments)(function)(*args, **kwargs)

        return observed

    return decorate


def ProfileSpan(stage, **fields):
    observer = _observer.get()
    return nullcontext() if observer is None else observer.ProfileSpan(stage, **fields)


def profile_tool_event(payload, **fields):
    observer = _observer.get()
    if observer is not None:
        observer.profile_tool_event(payload, **fields)
