# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Final inference admission uses real Model callbacks and one original client."""

import asyncio

import pytest

from openjiuwen.core.foundation import llm
from openjiuwen.core.runner import Runner
from openjiuwen.core.runner.callback import AsyncCallbackFramework
from openjiuwen.core.runner.callback.events import LLMCallEvents


class RecordingClient:
    def __init__(self):
        self.calls = []
        self.error = None
        self.frames = [llm.AssistantMessageChunk(content="one"), llm.AssistantMessageChunk(content="two")]

    async def invoke(self, messages, **kwargs):
        self.calls.append(("invoke", {"messages": messages, **kwargs}))
        if self.error:
            raise self.error
        return llm.AssistantMessage(content="ok")

    async def stream(self, messages, **kwargs):
        self.calls.append(("stream", {"messages": messages, **kwargs}))
        if self.error:
            raise self.error
        for frame in self.frames:
            yield frame


@pytest.fixture
def models(monkeypatch):
    framework = AsyncCallbackFramework()
    monkeypatch.setattr(Runner, "callback_framework", framework)

    def build(client=None, *, timeout=None):
        client = client or RecordingClient()
        monkeypatch.setattr("openjiuwen.core.foundation.llm.model.create_model_client", lambda **_: client)
        model = llm.Model(
            model_client_config=llm.ModelClientConfig(
                client_provider="OpenAI", api_key="test-only", api_base="http://127.0.0.1:1",
                stream_first_chunk_timeout=timeout, stream_idle_timeout=timeout,
            ),
            model_config=llm.ModelRequestConfig(model="selected-model"),
        )
        return model, client

    return framework, build


guard_scope = llm.model_call_guard_scope


async def call(model, stream=False, **kwargs):
    if stream:
        return [frame async for frame in model.stream(messages=[], **kwargs)]
    return await model.invoke(messages=[], **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("hook", ["emit", "transform"])
async def test_revocation_during_real_input_hooks_blocks_raw_client(models, stream, hook):
    framework, build = models
    model, client = build(timeout=2)
    entered, release = asyncio.Event(), asyncio.Event()
    allowed = True

    async def wait_hook(*args, **kwargs):
        entered.set()
        await release.wait()
        return args, kwargs

    event = LLMCallEvents.LLM_STREAM_INPUT if stream else LLMCallEvents.LLM_INVOKE_INPUT
    await framework.register(event, wait_hook, callback_type="transform" if hook == "transform" else "")

    async def authorize(kwargs):
        if not allowed:
            raise PermissionError("revoked after input callback")
        return kwargs

    async def run():
        with guard_scope(model, authorize):
            return await call(model, stream)

    task = asyncio.create_task(run())
    await asyncio.wait_for(entered.wait(), 2)
    allowed = False
    release.set()
    result = (await asyncio.gather(task, return_exceptions=True))[0]
    assert client.calls == []
    assert isinstance(result, PermissionError)


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [False, True])
async def test_guard_rewrites_actual_transformed_kwargs_once(models, stream):
    framework, build = models
    model, client = build(timeout=2)
    original_tools = [{"type": "function", "function": {"name": "read_file"}}]
    final_seen = []

    async def change_inputs(*args, **kwargs):
        return args, {**kwargs, "model": "unselected", "tools": [{"type": "web_search"}]}

    event = LLMCallEvents.LLM_STREAM_INPUT if stream else LLMCallEvents.LLM_INVOKE_INPUT
    await framework.register(event, change_inputs, callback_type="transform")

    async def authorize(kwargs):
        final_seen.append(kwargs)
        return {**kwargs, "model": "selected-model", "tools": []}

    with guard_scope(model, authorize):
        result = await call(model, stream, tools=original_tools)
    assert len(final_seen) == 1
    assert final_seen[0]["model"] == "unselected"
    assert final_seen[0]["tools"] == [{"type": "web_search"}]
    assert client.calls[0][1]["model"] == "selected-model"
    assert client.calls[0][1]["tools"] == []
    assert original_tools[0]["function"]["name"] == "read_file"
    assert result == client.frames if stream else result.content == "ok"


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("shared_client", [False, True])
async def test_callback_other_model_does_not_inherit_guard(models, stream, shared_client):
    framework, build = models
    selected, first_client = build()
    other, other_client = build(first_client if shared_client else None)
    seen = []
    in_callback = False

    async def callback(*_args, **_kwargs):
        nonlocal in_callback
        if not in_callback:
            in_callback = True
            try:
                await call(other, stream)
            finally:
                in_callback = False

    event = LLMCallEvents.LLM_STREAM_INPUT if stream else LLMCallEvents.LLM_INVOKE_INPUT
    await framework.register(event, callback)

    def authorize(kwargs):
        seen.append(kwargs)
        return {**kwargs, "guard_marker": "selected"}

    with guard_scope(selected, authorize):
        await call(selected, stream)
    assert len(seen) == 1
    assert first_client.calls[-1][1]["guard_marker"] == "selected"
    other_calls = other_client.calls[:-1] if shared_client else other_client.calls
    assert other_calls
    assert all("guard_marker" not in kwargs for _, kwargs in other_calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [False, True])
async def test_concurrent_scopes_same_model_keep_their_own_callback(models, stream):
    _, build = models
    model, client = build(timeout=2)
    first_entered, second_entered, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def run(label, entered):
        async def authorize(kwargs):
            entered.set()
            await release.wait()
            return {**kwargs, "guard_marker": label}

        with guard_scope(model, authorize):
            return await call(model, stream)

    tasks = [asyncio.create_task(run("first", first_entered)), asyncio.create_task(run("second", second_entered))]
    await asyncio.wait_for(asyncio.gather(first_entered.wait(), second_entered.wait()), 2)
    assert client.calls == []
    release.set()
    await asyncio.gather(*tasks)
    assert sorted(kwargs["guard_marker"] for _, kwargs in client.calls) == ["first", "second"]
    await call(model, stream)
    assert "guard_marker" not in client.calls[-1][1]


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [False, True])
async def test_nested_scope_exception_restores_outer_and_legacy(models, stream):
    _, build = models
    model, client = build()
    outer_seen = []
    denied = PermissionError("inner denied")

    def outer(kwargs):
        outer_seen.append(kwargs)
        return kwargs

    def inner(_kwargs):
        raise denied

    with guard_scope(model, outer):
        with pytest.raises(PermissionError) as error:
            with guard_scope(model, inner):
                await call(model, stream)
        assert error.value is denied
        assert client.calls == []
        await call(model, stream)
    await call(model, stream)
    assert len(outer_seen) == 1
    assert len(client.calls) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [False, True])
async def test_cancelled_guard_has_zero_effect_and_restores_context(models, stream):
    _, build = models
    model, client = build(timeout=2)
    entered = asyncio.Event()
    calls = []

    async def blocked(kwargs):
        calls.append(kwargs)
        entered.set()
        await asyncio.Event().wait()
        return kwargs

    async def run():
        try:
            with guard_scope(model, blocked):
                await call(model, stream)
        except asyncio.CancelledError:
            assert client.calls == []
            # Same task after scope exit must use the legacy path.
            await call(model, stream)
            raise

    task = asyncio.create_task(run())
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(calls) == 1
    assert len(client.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("result", [None, [], "allow"])
async def test_invalid_guard_result_has_zero_effect(models, stream, result):
    _, build = models
    model, client = build()
    with guard_scope(model, lambda _: result):
        with pytest.raises(TypeError, match="must return a dict"):
            await call(model, stream)
    assert client.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("when", ["entry", "input", "guard"])
async def test_replaced_client_fails_closed(models, stream, when):
    framework, build = models
    model, client = build()
    _, replacement = build()

    async def replace(*_args, **_kwargs):
        model._client = replacement

    async def authorize(kwargs):
        if when == "guard":
            await replace()
        return kwargs

    if when == "input":
        event = LLMCallEvents.LLM_STREAM_INPUT if stream else LLMCallEvents.LLM_INVOKE_INPUT
        await framework.register(event, replace)
    with guard_scope(model, authorize):
        if when == "entry":
            await replace()
        with pytest.raises(RuntimeError, match="client was replaced"):
            await call(model, stream)
    assert client.calls == replacement.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [False, True])
async def test_positional_transform_is_bound_before_guard(models, stream):
    framework, build = models
    model, client = build()
    messages = [llm.UserMessage(content="changed")]
    seen = []

    async def positional(*_args, **kwargs):
        kwargs.pop("messages")
        return (messages,), kwargs

    def authorize(kwargs):
        seen.append(kwargs)
        return kwargs

    event = LLMCallEvents.LLM_STREAM_INPUT if stream else LLMCallEvents.LLM_INVOKE_INPUT
    await framework.register(event, positional, callback_type="transform")
    with guard_scope(model, authorize):
        await call(model, stream)
    assert seen[0]["messages"] is messages
    assert client.calls[0][1]["messages"] is messages


@pytest.mark.asyncio
async def test_stream_frame_scopes_restore_before_public_yield_and_cross_task_close(models):
    _, build = models
    model, client = build(timeout=2)
    seen = []
    iterator = model.stream(messages=[])

    def authorize(kwargs):
        seen.append(kwargs)
        return kwargs

    async def next_frame():
        with guard_scope(model, authorize):
            return await anext(iterator)

    first = await asyncio.create_task(next_frame())
    await call(model)
    second = await asyncio.create_task(next_frame())

    async def close():
        with guard_scope(model, authorize):
            await iterator.aclose()

    await asyncio.create_task(close())
    assert [first, second] == client.frames
    assert len(seen) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [False, True])
async def test_original_client_error_and_objects_are_preserved(models, stream):
    _, build = models
    model, client = build()
    error = ValueError("original client error")
    client.error = error
    opaque = object()
    with guard_scope(model, lambda kwargs: kwargs):
        with pytest.raises(ValueError) as caught:
            await call(model, stream, opaque=opaque)
    assert caught.value is error
    assert client.calls[0][1]["opaque"] is opaque
    assert model._client is client


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [False, True])
async def test_guard_timeout_is_not_relabelled_as_provider_frame_timeout(models, stream):
    _, build = models
    model, client = build(timeout=2)
    timeout = TimeoutError("authorization service timed out")

    async def authorize(_kwargs):
        raise timeout

    with guard_scope(model, authorize):
        with pytest.raises(TimeoutError) as caught:
            await call(model, stream)
    assert caught.value is timeout
    assert client.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("guarded", [False, True])
@pytest.mark.parametrize("stream", [False, True])
async def test_sync_custom_client_compatibility(models, stream, guarded):
    _, build = models

    class SyncClient(RecordingClient):
        def invoke(self, **kwargs):
            self.calls.append(("invoke", kwargs))
            return llm.AssistantMessage(content="sync")

        def stream(self, **kwargs):
            self.calls.append(("stream", kwargs))
            yield from self.frames

    model, client = build(SyncClient())
    if guarded:
        with guard_scope(model, lambda kwargs: {**kwargs, "guard_marker": True}):
            result = await call(model, stream)
    else:
        result = await call(model, stream)
    assert result == client.frames if stream else result.content == "sync"
    assert client.calls[0][1].get("guard_marker", False) == guarded


@pytest.mark.asyncio
async def test_nested_callback_same_model_is_guarded_each_time(models):
    framework, build = models
    model, client = build()
    active = False
    seen = []

    async def input_callback(*_args, **_kwargs):
        nonlocal active
        if not active:
            active = True
            try:
                await call(model)
            finally:
                active = False

    def authorize(kwargs):
        seen.append(kwargs)
        return kwargs

    await framework.register(LLMCallEvents.LLM_INVOKE_INPUT, input_callback)
    with guard_scope(model, authorize):
        await call(model)
    assert len(client.calls) == len(seen) == 2


@pytest.mark.asyncio
async def test_ambiguous_positional_args_rejected_only_when_guarded(models):
    framework, build = models

    class VariadicClient(RecordingClient):
        async def invoke(self, *args, **kwargs):
            self.calls.append(("invoke", kwargs))
            return llm.AssistantMessage(content=str(args))

    model, client = build(VariadicClient())

    async def positional(*_args, **kwargs):
        return ("ambiguous",), kwargs

    await framework.register(LLMCallEvents.LLM_INVOKE_INPUT, positional, callback_type="transform")
    with guard_scope(model, lambda kwargs: kwargs):
        with pytest.raises(TypeError, match="keyword-representable"):
            await call(model)
    assert client.calls == []
    await call(model)
    assert len(client.calls) == 1
