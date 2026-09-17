# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Read authoritative Task results and disclose only matching bounded artifacts.

Dialogue turn selection and presentation are application responsibilities.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Mapping
from pathlib import Path

from .contracts import ErrorCode, ScopeRef
from .formal_task_models import FormalTaskViolation, PersistentTaskRecord, TaskResultArtifact, utc_now

_TASK_RESULT_ARTIFACT_CONTENT_MAX_BYTES = 16_384


class TaskResultReader:
    @staticmethod
    def _validated_task_result_context_parts(
        task_result: Mapping[str, object],
    ) -> tuple[str, tuple[TaskResultArtifact, ...], str, str, str]:
        result_text = task_result.get("result_text")
        artifacts = task_result.get("artifacts")
        task_id = task_result.get("task_id")
        attempt_id = task_result.get("attempt_id")
        source_event_id = task_result.get("source_event_id")
        if (
            not isinstance(result_text, str)
            or not result_text.strip()
            or not isinstance(artifacts, list)
            or not isinstance(task_id, str)
            or not isinstance(attempt_id, str)
            or not isinstance(source_event_id, str)
        ):
            raise FormalTaskViolation(
                "TASK_RESULT_CONTEXT_INVALID",
                "available task result is not safe for Agent context",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        if len(task_id) > 256 or len(attempt_id) > 256 or len(source_event_id) > 256:
            raise FormalTaskViolation(
                "TASK_RESULT_CONTEXT_INVALID",
                "available task result identity exceeds its closed bound",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        try:
            bounded_artifacts = tuple(
                TaskResultArtifact(
                    relative_path=item["relative_path"],
                    sha256=item["sha256"],
                )
                for item in artifacts
                if isinstance(item, Mapping) and set(item) == {"relative_path", "sha256"}
            )
        except (KeyError, TypeError, FormalTaskViolation) as exc:
            raise FormalTaskViolation(
                "TASK_RESULT_CONTEXT_INVALID",
                "available task result artifacts are invalid",
                ErrorCode.PROTOCOL_VIOLATION,
            ) from exc
        if len(bounded_artifacts) != len(artifacts) or len(bounded_artifacts) > 32:
            raise FormalTaskViolation(
                "TASK_RESULT_CONTEXT_INVALID",
                "available task result artifacts exceed their closed bound",
                ErrorCode.PROTOCOL_VIOLATION,
            )
        try:
            result_text.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise FormalTaskViolation(
                "TASK_RESULT_CONTEXT_INVALID",
                "available task result text is not valid UTF-8",
                ErrorCode.PROTOCOL_VIOLATION,
            ) from exc
        return result_text, bounded_artifacts, task_id, attempt_id, source_event_id

    @classmethod
    def _verified_result_artifact_snapshots(
        cls,
        *,
        scope: ScopeRef,
        task: PersistentTaskRecord,
        task_result: Mapping[str, object],
    ) -> tuple[dict[str, str], ...]:
        (
            _result_text,
            artifacts,
            task_id,
            attempt_id,
            _source_event_id,
        ) = cls._validated_task_result_context_parts(task_result)
        if task.scope != scope or task_id != task.task_id or attempt_id != task.attempt_id:
            raise FormalTaskViolation(
                "TASK_RESULT_CONTEXT_INVALID",
                "available task result does not match the current task attempt",
                ErrorCode.PERMISSION_DENIED,
            )

        context = task.spec.context
        # The authenticated task.result operation is the disclosure authority.
        # Context remains only the scoped locator; expiry/redaction are rechecked
        # here and immutable artifact SHA-256 gates every disclosed byte below.
        context.require_usable(
            scope=scope,
            required_permissions=frozenset(),
            destructive=False,
            now=utc_now(),
        )
        project_path = context.file_path
        if not project_path:
            return tuple(
                {
                    "relative_path": artifact.relative_path,
                    "status": "context_unavailable",
                }
                for artifact in artifacts
            )
        try:
            project_root = Path(project_path).resolve(strict=True)
        except (OSError, RuntimeError):
            return tuple(
                {
                    "relative_path": artifact.relative_path,
                    "status": "context_unavailable",
                }
                for artifact in artifacts
            )
        if not project_root.is_dir():
            return tuple(
                {
                    "relative_path": artifact.relative_path,
                    "status": "context_unavailable",
                }
                for artifact in artifacts
            )

        snapshots: list[dict[str, str]] = []
        retained_content_bytes = 0
        for artifact in artifacts:
            relative_path = artifact.relative_path
            if os.name == "nt" and ":" in relative_path:
                snapshots.append({"relative_path": relative_path, "status": "outside_project"})
                continue
            lexical_path = project_root.joinpath(*relative_path.split("/"))
            cursor = project_root
            is_symlink = False
            for part in relative_path.split("/"):
                cursor = cursor / part
                try:
                    if cursor.is_symlink():
                        is_symlink = True
                        break
                except OSError:
                    break
            if is_symlink:
                snapshots.append({"relative_path": relative_path, "status": "symlink"})
                continue
            try:
                resolved_path = lexical_path.resolve(strict=True)
            except (FileNotFoundError, NotADirectoryError):
                snapshots.append({"relative_path": relative_path, "status": "missing"})
                continue
            except (OSError, RuntimeError):
                snapshots.append({"relative_path": relative_path, "status": "not_regular_file"})
                continue
            try:
                resolved_path.relative_to(project_root)
            except ValueError:
                snapshots.append({"relative_path": relative_path, "status": "outside_project"})
                continue
            try:
                if not resolved_path.is_file():
                    snapshots.append(
                        {
                            "relative_path": relative_path,
                            "status": "not_regular_file",
                        }
                    )
                    continue
                artifact_size = resolved_path.stat().st_size
            except OSError:
                snapshots.append({"relative_path": relative_path, "status": "not_regular_file"})
                continue
            if artifact_size > _TASK_RESULT_ARTIFACT_CONTENT_MAX_BYTES:
                snapshots.append({"relative_path": relative_path, "status": "too_large"})
                continue
            if retained_content_bytes + artifact_size > _TASK_RESULT_ARTIFACT_CONTENT_MAX_BYTES:
                snapshots.append(
                    {
                        "relative_path": relative_path,
                        "status": "context_limit_exceeded",
                    }
                )
                continue
            try:
                with resolved_path.open("rb") as artifact_file:
                    artifact_bytes = artifact_file.read(_TASK_RESULT_ARTIFACT_CONTENT_MAX_BYTES + 1)
            except OSError:
                snapshots.append({"relative_path": relative_path, "status": "not_regular_file"})
                continue
            if len(artifact_bytes) > _TASK_RESULT_ARTIFACT_CONTENT_MAX_BYTES:
                snapshots.append({"relative_path": relative_path, "status": "too_large"})
                continue
            if retained_content_bytes + len(artifact_bytes) > _TASK_RESULT_ARTIFACT_CONTENT_MAX_BYTES:
                snapshots.append(
                    {
                        "relative_path": relative_path,
                        "status": "context_limit_exceeded",
                    }
                )
                continue
            if hashlib.sha256(artifact_bytes).hexdigest() != artifact.sha256:
                snapshots.append({"relative_path": relative_path, "status": "hash_mismatch"})
                continue
            try:
                content = artifact_bytes.decode("utf-8", errors="strict")
            except UnicodeDecodeError:
                snapshots.append({"relative_path": relative_path, "status": "not_text"})
                continue
            if "\x00" in content or any(ord(character) < 32 and character not in "\n\r\t" for character in content):
                snapshots.append({"relative_path": relative_path, "status": "not_text"})
                continue
            retained_content_bytes += len(artifact_bytes)
            snapshots.append(
                {
                    "relative_path": relative_path,
                    "status": "verified",
                    "content": content,
                }
            )
        return tuple(snapshots)
