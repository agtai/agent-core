# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""SessionGoalStore — persist GoalRecord via Session state.

Uses ``Session.get_state()`` / ``Session.update_state()`` as the single
source of truth for goal state. JiuwenSwarm must not write sidecar JSON,
SQLite, or history as an alternative goal state source.
"""
from __future__ import annotations

import inspect
import json
import logging
from typing import TYPE_CHECKING, Optional

from openjiuwen.harness.goal.schema import GoalOperationError, GoalRecord, _copy_goal_run_context
from openjiuwen.core.session.agent import _copy_json_mapping

if TYPE_CHECKING:
    from openjiuwen.core.session.agent import Session

logger = logging.getLogger(__name__)

SESSION_GOAL_RECORD_KEY = "harness.goal.record"


class SessionGoalStore:
    """Session-scoped goal persistence backed by Session state API."""

    def __init__(self, session: Session) -> None:
        self._session = session

    @property
    def session_id(self) -> str:
        return self._session.get_session_id()

    def load(self) -> Optional[GoalRecord]:
        """Load the current GoalRecord from session state."""
        return self._read(repair=True)

    def peek(self) -> Optional[GoalRecord]:
        """Read without repairing or clearing state; corrupt data is unavailable."""
        return self._read(repair=False)

    def _read(self, *, repair: bool) -> Optional[GoalRecord]:
        data = self._session.get_state(SESSION_GOAL_RECORD_KEY)
        if data is None:
            return None
        if not isinstance(data, dict):
            if not repair:
                raise GoalOperationError(operation="get", code="invalid_state",
                                         message="Stored Goal state is invalid")
            logger.warning(
                "[GoalStore] Invalid goal state type %s in session %s, clearing",
                type(data).__name__,
                self.session_id,
            )
            self.clear()
            return None
        # Bindings cannot use the state's recursive dict merge: removed keys
        # must disappear and JSON null must survive. Old dict records remain
        # readable, but an invalid binding must never be repaired into execution.
        data = dict(data)
        try:
            context = data.get("run_context")
            if isinstance(context, str):
                if len(context.encode("utf-8")) > 65536:
                    raise ValueError("stored run context exceeds size bound")
                context = _copy_json_mapping(json.loads(context))
            data["run_context"] = _copy_goal_run_context(context)
            self._validate_run_context(data["run_context"], goal_id=data.get("goal_id"),
                                      revision=data.get("revision"))
        except (ValueError, TypeError, RecursionError) as exc:
            raise GoalOperationError(operation="get", code="invalid_state",
                                     message="Stored Goal run context is invalid") from exc
        try:
            record = GoalRecord.from_dict(data)
            if not repair and record.session_id != self.session_id:
                raise GoalOperationError(operation="get", code="invalid_state",
                                         message="Stored Goal belongs to another session")
            return record
        except (KeyError, ValueError, TypeError) as exc:
            if not repair:
                raise GoalOperationError(operation="get", code="invalid_state",
                                         message="Stored Goal state is invalid") from exc
            logger.warning(
                "[GoalStore] Failed to deserialize goal state in session %s: %s, clearing",
                self.session_id,
                exc,
            )
            self.clear()
            return None

    def save(self, record: GoalRecord) -> None:
        """Persist a GoalRecord to session state."""
        self._validate_run_context(record.run_context, goal_id=record.goal_id, revision=record.revision)
        data = record.to_dict()
        if record.run_context is not None:
            data["run_context"] = json.dumps(_copy_goal_run_context(record.run_context),
                                             ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        self._session.update_state({SESSION_GOAL_RECORD_KEY: data})

    def _validate_run_context(self, context, *, goal_id, revision) -> None:
        """Preflight the complete output projection without changing state."""
        from openjiuwen.harness.schema.interaction import _source_metadata_for_run

        # Actual round ids are UUID hex strings. Goal continuation keeps its
        # revision; idle resume validates its next revision before admission.
        source = _source_metadata_for_run(context, session_id=self.session_id, task_id="0" * 32,
                                          run_kind="goal", goal_id=goal_id, revision=revision)
        view = getattr(self._session, "with_source_metadata", None)
        if source is not None and callable(view):
            view(source)  # Also account for static labels on the actual Session.

    def clear(self) -> None:
        """Remove the GoalRecord from session state."""
        self._session.update_state({SESSION_GOAL_RECORD_KEY: None})

    async def commit(self) -> None:
        """Flush the backing session when it supports explicit persistence."""
        commit = getattr(self._session, "commit", None)
        if not callable(commit):
            return
        result = commit()
        if inspect.isawaitable(result):
            await result


__all__ = ["SESSION_GOAL_RECORD_KEY", "SessionGoalStore"]
