"""Source views preserve execution provenance without owning a new session."""
import asyncio

import pytest

from openjiuwen.core.controller.schema.controller_output import ControllerOutputChunk, ControllerOutputPayload
from openjiuwen.core.session.agent import Session
from openjiuwen.core.session.stream import OutputSchema


def test_source_views_share_deepagent_runtime_cache_and_its_removal():
    from openjiuwen.core.single_agent.schema.agent_card import AgentCard
    from openjiuwen.harness.deep_agent import DeepAgent

    agent = DeepAgent(AgentCard(name="shared-state", description="test"))
    session = Session(session_id="shared-runtime-cache")
    outer = session.with_source_metadata({"source_binding_id": "outer"})
    executor = session.with_source_metadata({"source_binding_id": "executor"})
    state = agent.load_state(outer)
    state.pending_follow_ups.append("still pending")
    assert agent.load_state(session) is state
    assert agent.load_state(executor) is state
    agent.save_state(executor)
    assert agent.load_state(outer).pending_follow_ups == ["still pending"]
    agent.clear_state(outer)
    assert not hasattr(session, "_deepagent_runtime_state")
    assert not hasattr(executor, "_deepagent_runtime_state")
    assert agent.load_state(executor).pending_follow_ups == ["still pending"]


@pytest.mark.asyncio
async def test_source_views_share_state_writer_and_keep_late_output_provenance():
    session = Session(session_id="source-views")
    source = {"source_binding_id": "old", "details": {"tag": "first"}}
    old = session.with_source_metadata(source)
    new = session.with_source_metadata({"source_binding_id": "new"})
    source["details"]["tag"] = "changed"
    old.update_state({"shared": "yes"})
    assert new.get_state("shared") == session.get_state("shared") == "yes"
    assert old._inner is new._inner is session._inner
    await new.write_stream(OutputSchema(type="answer", index=0, payload={"content": "new"}))
    await asyncio.create_task(old.write_stream(OutputSchema(type="answer", index=0, payload={"content": "late"})))
    iterator = session.stream_iterator()
    first, second = await anext(iterator), await anext(iterator)
    assert first.payload["source_binding_id"] == "new"
    assert second.payload["source_binding_id"] == "old"
    assert second.payload["details"]["tag"] == "first"
    second.payload["details"]["tag"] = "consumer changed"
    await old.write_custom_stream({"event": "later"})
    assert (await anext(iterator)).details["tag"] == "first"
    await old.close_stream()
    assert not session._inner.stream_writer_manager().stream_emitter().is_closed()
    await iterator.aclose()


@pytest.mark.asyncio
async def test_source_view_preserves_typed_and_serialized_controller_payloads():
    session = Session(session_id="controller-source")
    view = session.with_source_metadata({"source_binding_id": "trusted"})
    chunk = ControllerOutputChunk(index=0, payload=ControllerOutputPayload(
        type="task_completion", data=[], metadata={"task_id": "task-1", "source_binding_id": "spoof"}))
    await view.write_stream(chunk)
    await view.write_stream(chunk.model_dump())
    iterator = session.stream_iterator()
    typed, serialized = await anext(iterator), await anext(iterator)
    assert isinstance(typed.payload, ControllerOutputPayload)
    assert typed.payload.metadata == {"task_id": "task-1", "source_binding_id": "trusted"}
    assert serialized.payload["metadata"]["source_binding_id"] == "trusted"
    assert chunk.payload.metadata["source_binding_id"] == "spoof"
    await iterator.aclose()
