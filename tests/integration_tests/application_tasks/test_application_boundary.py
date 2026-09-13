"""Application-independent SDK admission, storage and codec boundaries."""

import ast
import asyncio
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from openjiuwen.core.application.tasks import PersistentTaskCore, SqliteTaskStore, contracts
from openjiuwen.core.application.tasks.formal_task_models import (
    ExecutorDeliveryResult,
    FormalAttemptState,
    TaskAuthorizationGrant,
)
from openjiuwen.core.application.tasks.source import (
    TaskSourceError,
    TaskSourceEvidence,
    register_source_codec,
    source_from_payload,
)

from .test_task_event_subscription import EXPIRY, NOW, _context, _observation, _scope


def test_sdk_import_is_independent_of_jiuwenswarm(tmp_path):
    root = Path(__file__).resolve().parents[3]
    script = (
        "import sys; sys.path.insert(0, " + repr(str(root)) + "); "
        "from openjiuwen.core.application.tasks import PersistentTaskCore, SqliteTaskStore; "
        "assert not any(n == 'jiuwenswarm' or n.startswith('jiuwenswarm.') for n in sys.modules)"
    )
    subprocess.run([sys.executable, "-I", "-c", script], cwd=tmp_path, check=True)
    package = root / "openjiuwen/core/application/tasks"
    for path in package.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom):
                assert not (node.module or "").startswith("jiuwenswarm")
            elif isinstance(node, ast.Import):
                assert all(not x.name.startswith("jiuwenswarm") for x in node.names)
    assert not hasattr(contracts, "TurnCommitLedger")
    assert not hasattr(contracts, "IdentityRegistry")


def test_public_core_dispatch_reopen_and_wrong_scope(tmp_path):
    class Executor:
        executor_id = "example.agent"

        def __init__(self):
            self.calls = []

        async def dispatch(self, item):
            self.calls.append(item.attempt_id)
            task = store.get_task(item.task_id, item.scope)
            observation = _observation(task, source_seq=0, state=FormalAttemptState.RUNNING)
            return ExecutorDeliveryResult(observation.executor_ref, (observation,))

    command = contracts.CommandEnvelope.from_dict(
        {
            "contract_version": contracts.CONTRACT_VERSION,
            "request_id": "request",
            "command_id": "command",
            "command_type": "task.create",
            "issued_at": NOW,
            "scope": _scope().to_dict(),
            "correlation_id": "correlation",
            "causation_id": None,
            "origin": {"kind": "structured", "turn_id": None, "commit_id": None},
            "target_ref": {"kind": "task", "id": "create:command"},
            "context_refs": [],
            "required_capabilities": ["task.create"],
            "extensions": {},
            "payload": {
                "name": "Example",
                "instruction": "Perform a bounded project operation.",
                "executor_id": "example.agent",
                "side_effect_class": "project_mutation",
                "attributes": {"model_identity": "configured", "model_config_version": "v1"},
            },
        }
    )
    grant = TaskAuthorizationGrant(
        "user-1", _scope(), "task.create", "command", None, frozenset({"task.create"}), "confirmation", True, EXPIRY
    )
    store = SqliteTaskStore(tmp_path / "tasks.db")
    executor = Executor()
    core = PersistentTaskCore(store, executor)
    rejected = core.execute(command, replace(grant, scope=_scope("other")), context=_context(tmp_path), now=NOW)
    assert not rejected.ok and executor.calls == []
    accepted = core.execute(command, grant, context=_context(tmp_path), now=NOW)
    assert accepted.ok and executor.calls == []  # Durable acceptance is not execution.
    assert core.execute(command, grant, context=_context(tmp_path), now=NOW) == accepted
    asyncio.run(core.drain_outbox())
    assert len(executor.calls) == 1
    reopened = SqliteTaskStore(tmp_path / "tasks.db")
    task = reopened.get_task(accepted.result["task_id"], _scope())
    assert task.state.value == "running"
    assert asyncio.run(PersistentTaskCore(reopened, executor).drain_outbox()) == 0
    assert len(executor.calls) == 1


@pytest.mark.parametrize("value", [None, {}, {"contract_version": "unknown.application.v1"}])
def test_unknown_or_malformed_source_never_becomes_legacy(value):
    assert source_from_payload({}) is None
    with pytest.raises(TaskSourceError):
        source_from_payload({"native_source": value})


def test_codec_must_provide_consumed_fields_and_cannot_be_rebound():
    class MissingFields(TaskSourceEvidence):
        scope = _scope()
        digest = "a" * 64

        def to_dict(self):
            return {"contract_version": "test.missing-fields.v1"}

        @classmethod
        def from_dict(cls, value):
            return cls()

        def require_request(self, **kwargs):
            pass

        def agent_request(self, proposal, **kwargs):
            return proposal

    register_source_codec("test.missing-fields.v1", MissingFields)
    with pytest.raises(TaskSourceError, match="TASK_SOURCE_CODEC_RESULT_INVALID"):
        source_from_payload({"native_source": MissingFields().to_dict()})

    class Other(MissingFields):
        pass

    with pytest.raises(TaskSourceError, match="TASK_SOURCE_CODEC_ALREADY_REGISTERED"):
        register_source_codec("test.missing-fields.v1", Other)
