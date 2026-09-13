"""SDK-only project execution: real Git application, no Host installation required."""

from __future__ import annotations

import asyncio
import subprocess
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

from openjiuwen.core.application.tasks.contracts import Assurance, OriginRef, ScopeRef
from openjiuwen.core.application.tasks.formal_task_models import (
    FormalAttemptState,
    FormalTaskSpec,
    OutboxKind,
    OutboxState,
    PersistentOutboxItem,
    ResolvedTaskContext,
)
from openjiuwen.core.application.tasks.project_executor import (
    FORMAL_PROJECT_EXECUTOR_ID,
    FORMAL_RUNTIME_SUPPORT_POLICY,
    DirectProjectCodeExecutorAdapter,
    ProjectExecutionBinding,
)


class _DirectProjectExecutor:
    def __init__(self):
        self.requests = []

    async def process_background_code_task_stream(self, request):
        self.requests.append(request)
        (Path(request.params["project_dir"]) / "result.txt").write_text("sdk result", encoding="utf-8")
        yield SimpleNamespace(is_complete=True, payload={"event_type": "chat.final", "content": "sdk result"})


class Application:
    telemetry = None

    def create_request(self, invocation):
        return SimpleNamespace(
            request_id=invocation.request_id,
            session_id=invocation.session_id,
            params={"project_dir": invocation.project_dir},
        )

    def execution_guard(self):
        return nullcontext()

    def runtime_support_governance(self, root):
        return {"policy": dict(FORMAL_RUNTIME_SUPPORT_POLICY), "application_paths": {}}


def _git(project: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(project), *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return completed.stdout.strip()


def _git_project(project: Path, *, ignore: str | None = None) -> None:
    project.mkdir(parents=True, exist_ok=True)
    _git(project, "init")
    _git(project, "config", "user.name", "Live Voice Test")
    _git(project, "config", "user.email", "live-voice-test@example.invalid")
    (project / "README.md").write_text("baseline\n", encoding="utf-8")
    if ignore is not None:
        (project / ".gitignore").write_text(ignore, encoding="utf-8")
    _git(project, "add", ".")
    _git(project, "commit", "-m", "baseline")


def _scope() -> ScopeRef:
    return ScopeRef("user-1", "project-1", "session-1", Assurance.AUTHENTICATED)


def _spec(project: Path) -> FormalTaskSpec:
    return FormalTaskSpec(
        name="Formal project task",
        instruction="Create one bounded source change.",
        origin=OriginRef("structured", None, None),
        context=ResolvedTaskContext(
            source="gateway.project_registry",
            stable_id="project-1",
            uri=project.resolve().as_uri(),
            revision_kind="version",
            revision_value="a77516a0",
            scope=_scope(),
            permissions=("task.execute", "project.write"),
            expires_at="2026-08-05T13:00:00Z",
            redaction_policy_id="live_voice.project.v1",
        ),
        executor_id=FORMAL_PROJECT_EXECUTOR_ID,
        required_capabilities=("task.create",),
        side_effect_class="project_mutation",
        attributes=(
            ("model_config_version", "catalog-v1"),
            ("model_identity", "default#0"),
        ),
    )


async def _clean_dispatch_fence() -> None:
    return None


def _direct_binding(
    project: Path,
    executor: _DirectProjectExecutor,
    *,
    releases: list[str] | None = None,
) -> ProjectExecutionBinding:
    return ProjectExecutionBinding(
        execution_agent=object(),
        project_executor=executor,
        effective_execution_root=str(project.resolve()),
        execution_target={
            "project_dir": str(project.resolve()),
            "project_id": "project-1",
            "origin_session_id": "session-1",
            "origin_channel_id": "web",
        },
        owner_scope={
            "channel_id": "formal-task-core",
            "session_id": "session-1",
            "app_id": "live-voice",
        },
        resolved_revision_kind="version",
        resolved_revision_value="a77516a0",
        model_identity="default#0",
        model_config_version="catalog-v1",
        context_release=(None if releases is None else lambda: releases.append("released")),
        dispatch_fence=_clean_dispatch_fence,
    )


def _item(project: Path, *, kind=OutboxKind.ATTEMPT_DISPATCH, source_seq=-1):
    return PersistentOutboxItem(
        outbox_id="outbox-1",
        kind=kind,
        task_id="task-1",
        attempt_id="attempt-1",
        command_id="command-1",
        scope=_scope(),
        spec=_spec(project),
        executor_ref=(None if kind is OutboxKind.ATTEMPT_DISPATCH else "sch-1"),
        source_seq=source_seq,
        state=OutboxState.CLAIMED,
        delivery_count=1,
    )


@pytest.mark.asyncio
async def test_sdk_project_execution_applies_exact_result_and_restores_without_agent_replay(tmp_path):
    project = tmp_path / "project"
    _git_project(project)
    original = (project / "README.md").read_bytes()
    agent = _DirectProjectExecutor()

    class Resolver:
        async def resolve(self, spec, *, for_dispatch):
            return _direct_binding(project, agent)

    database = tmp_path / "attempts.db"
    executor = DirectProjectCodeExecutorAdapter(
        Resolver(), database, application=Application(), clock=lambda: "2026-08-05T12:00:00Z"
    )
    try:
        receipt = await executor.dispatch(_item(project))
        assert receipt.executor_ref == "d0-project:attempt-1"
        async with asyncio.timeout(30):
            await asyncio.gather(*(asyncio.shield(task) for task in tuple(executor._running.values())))
        record = executor._journal.get("attempt-1")
        assert record.state is FormalAttemptState.TERMINAL
        assert record.outcome.value == "completed"
        assert (project / "result.txt").read_text(encoding="utf-8") == "sdk result"
        assert (project / "README.md").read_bytes() == original
        assert len(agent.requests) == 1
    finally:
        await executor.close()
    restored = DirectProjectCodeExecutorAdapter(
        Resolver(), database, application=Application(), clock=lambda: "2026-08-05T12:00:01Z"
    )
    try:
        await restored.prepare_startup()
        assert restored._journal.get("attempt-1").outcome.value == "completed"
        assert len(agent.requests) == 1
    finally:
        await restored.close()
