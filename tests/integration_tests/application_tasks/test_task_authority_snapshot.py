# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Real SQLite checks for the Store-owned aggregate authority read."""
import sqlite3
from dataclasses import replace

import pytest

from openjiuwen.core.application.tasks.formal_task_models import FormalTaskViolation
from openjiuwen.core.application.tasks.task_store import SqliteTaskStore

from .test_task_event_subscription import _create_task, _scope


def _dump(store):
    with sqlite3.connect(store.database_path) as connection:
        return tuple(connection.iterdump())


def test_authority_page_matches_existing_reads_and_preserves_scope(tmp_path):
    store = SqliteTaskStore(tmp_path / "authority.sqlite")
    first = _create_task(store, tmp_path, suffix="one")
    second = _create_task(store, tmp_path, suffix="two")
    before = _dump(store)
    page, cursor, more = store.list_task_authority_snapshots_page(_scope(), limit=1)
    assert more and cursor == first.task_id
    snapshot = page[0]
    assert snapshot.task == first
    assert (snapshot.task, snapshot.attempt, snapshot.admission) == (
        store.list_task_read_snapshots_page(_scope(), limit=1)[0][0])
    assert snapshot.event_head == store.events(first.task_id, _scope())[-1]
    assert (snapshot.result_availability, snapshot.result, snapshot.result_reason) == (
        store.task_result(first.task_id, _scope()))
    tail, cursor, more = store.list_task_authority_snapshots_page(_scope(), cursor=cursor, limit=1)
    assert [item.task.task_id for item in tail] == [second.task_id]
    assert cursor is None and not more
    assert store.list_tasks_page(_scope(), limit=1)[0] == (first,)
    for foreign in (_scope("other"), replace(_scope(), project_id="other"), replace(_scope(), session_id="other")):
        assert store.list_task_authority_snapshots_page(foreign) == ((), None, False)
        with pytest.raises(FormalTaskViolation):
            store.list_task_authority_snapshots_page(foreign, cursor=first.task_id)
    assert _dump(store) == before


@pytest.mark.parametrize("params", [{"limit": True}, {"limit": 0}, {"limit": 501}, {"cursor": ""}, {"cursor": 1}])
def test_authority_page_rejects_invalid_bounds_without_writes(tmp_path, params):
    store = SqliteTaskStore(tmp_path / "bounds.sqlite")
    before = _dump(store)
    with pytest.raises(FormalTaskViolation):
        store.list_task_authority_snapshots_page(_scope(), **params)
    assert _dump(store) == before


def test_authority_page_rejects_missing_head_without_repair(tmp_path):
    store = SqliteTaskStore(tmp_path / "corrupt.sqlite")
    task = _create_task(store, tmp_path)
    with sqlite3.connect(store.database_path) as connection:
        connection.execute("DELETE FROM task_events WHERE task_id=? AND seq=?", (task.task_id, task.event_head))
    before = _dump(store)
    with pytest.raises(FormalTaskViolation):
        store.list_task_authority_snapshots_page(_scope())
    assert _dump(store) == before
