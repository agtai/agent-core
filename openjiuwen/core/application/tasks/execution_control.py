# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Transient caller control, separate from accepted application work lifetime."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from contextvars import ContextVar
from typing import Any, Protocol, TypeVar

T = TypeVar("T")
CURRENT_INTERACTION_CONTROL: ContextVar[Any | None] = ContextVar("application_interaction_control", default=None)


class ReadOnlyControl(Protocol):
    """Only operations whose cancellation and settlement belong to the caller."""

    def check(self) -> None: ...


async def read_only_operation(
    control: ReadOnlyControl, interrupted: asyncio.Event, operation: Awaitable[T], *, timeout: float | None = None  # noqa: ASYNC109
) -> T:
    """Race a read against interruption; settle both owned awaits before return.

    Never use this for persistent Task dispatch or other unknown-outcome writes.
    """
    work = asyncio.ensure_future(operation)
    stop = asyncio.create_task(interrupted.wait())
    try:
        done, _ = await asyncio.wait({work, stop}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        control.check()
        if work not in done:
            raise TimeoutError
        return await work
    finally:
        for task in (work, stop):
            if not task.done():
                task.cancel()
        await asyncio.gather(work, stop, return_exceptions=True)
