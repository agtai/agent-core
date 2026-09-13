# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Application Work checkpoints: exact sequence CAS and no automatic replay.

The historical table name is retained for compatibility. Input journals and
notification tables are optional application extensions, not SDK prerequisites.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .contracts import ErrorCode, ScopeRef, canonical_json_bytes
from .work_runtime import WorkSnapshot, WorkState, WorkViolation

_SCHEMA_VERSION = 1


_MAX_PAYLOAD_BYTES = 192 * 1024


_IMMUTABLE_FIELDS = (
    "scope",
    "work_id",
    "revision",
    "request_id",
    "input_id",
    "instruction",
    "model_identity",
    "model_config_version",
    "context_id",
    "foreground",
    "accepted_at",
    "supersedes_revision",
)


_TRANSITIONS = {
    WorkState.ACCEPTED: frozenset(
        {
            WorkState.RUNNING,
            WorkState.CANCELLING,
            WorkState.CANCELLED,
            WorkState.SUPERSEDED,
            WorkState.FAILED,
            WorkState.UNKNOWN,
        }
    ),
    WorkState.RUNNING: frozenset(
        {
            WorkState.COMPLETED,
            WorkState.CANCELLING,
            WorkState.CANCELLED,
            WorkState.SUPERSEDED,
            WorkState.FAILED,
            WorkState.UNKNOWN,
        }
    ),
    WorkState.CANCELLING: frozenset({WorkState.CANCELLED, WorkState.UNKNOWN}),
    WorkState.COMPLETED: frozenset({WorkState.SUPERSEDED, WorkState.UNKNOWN}),
    WorkState.FAILED: frozenset({WorkState.SUPERSEDED, WorkState.UNKNOWN}),
    WorkState.CANCELLED: frozenset({WorkState.UNKNOWN}),
    WorkState.SUPERSEDED: frozenset({WorkState.UNKNOWN}),
    WorkState.UNKNOWN: frozenset(),
}

_COLUMNS = {
    "native_work_checkpoint": (
        ("schema_version", "INTEGER", 0),
        ("scope_sha256", "TEXT", 1),
        ("work_id", "TEXT", 2),
        ("revision", "INTEGER", 3),
        ("sequence", "INTEGER", 0),
        ("request_id", "TEXT", 0),
        ("identity_sha256", "TEXT", 0),
        ("snapshot_json", "TEXT", 0),
        ("snapshot_sha256", "TEXT", 0),
    )
}


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _scope_digest(scope: ScopeRef) -> str:
    if not isinstance(scope, ScopeRef):
        raise WorkViolation("NATIVE_WORK_JOURNAL_SCOPE_INVALID", "canonical work scope is required")
    canonical = ScopeRef.from_dict(scope.to_dict())
    return _digest(canonical_json_bytes(canonical.to_dict()))


def verify_work_tables(connection: sqlite3.Connection, expected_columns) -> None:
    for table, expected in expected_columns.items():
        columns = connection.execute(f"PRAGMA table_info({table})").fetchall()
        actual = tuple((str(row["name"]), str(row["type"]).upper(), int(row["pk"])) for row in columns)
        if actual != expected or any(not row["notnull"] for row in columns):
            raise WorkViolation(
                "NATIVE_WORK_JOURNAL_SCHEMA_UNSUPPORTED",
                "Native work journal schema is unsupported",
                ErrorCode.UNAVAILABLE,
            )
        if connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name=? LIMIT 1",
            (table,),
        ).fetchone():
            raise WorkViolation(
                "NATIVE_WORK_JOURNAL_SCHEMA_UNSUPPORTED",
                "Native work journal triggers are unsupported",
                ErrorCode.UNAVAILABLE,
            )


class SqliteWorkStore:
    """Existing regular SQLite database, atomic checkpoints and application hooks."""

    def __init__(self, database_path: str | Path, *, max_records: int = 128) -> None:
        if type(max_records) is not int or max_records < 1:
            raise WorkViolation("NATIVE_WORK_JOURNAL_BOUNDS_INVALID", "journal bounds must be positive integers")
        self.database_path = Path(database_path)
        self._max_records = max_records
        self._require_existing_database()
        with self._connection(write=True, verify=False) as connection:
            self._initialize_application_schema(connection)
            connection.execute("""
                CREATE TABLE IF NOT EXISTS native_work_checkpoint (
                    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
                    scope_sha256 TEXT NOT NULL,
                    work_id TEXT NOT NULL,
                    revision INTEGER NOT NULL CHECK(revision > 0),
                    sequence INTEGER NOT NULL CHECK(sequence > 0),
                    request_id TEXT NOT NULL,
                    identity_sha256 TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL,
                    snapshot_sha256 TEXT NOT NULL,
                    PRIMARY KEY(scope_sha256, work_id, revision),
                    UNIQUE(scope_sha256, request_id)
                )
            """)
            self._verify_schema(connection)

    def _initialize_application_schema(self, connection: sqlite3.Connection) -> None:
        """Applications may add tables in the same constructor transaction."""

    def _verify_application_schema(self, connection: sqlite3.Connection) -> None:
        """Applications may enforce additional invariants on every transaction."""

    def _verify_schema(self, connection: sqlite3.Connection) -> None:
        verify_work_tables(connection, _COLUMNS)
        self._verify_application_schema(connection)

    def _require_existing_database(self) -> None:
        if self.database_path.is_symlink() or not self.database_path.is_file():
            raise WorkViolation(
                "NATIVE_WORK_JOURNAL_UNAVAILABLE",
                "Work requires an existing regular database",
                ErrorCode.UNAVAILABLE,
            )

    @contextmanager
    def _connection(self, *, write: bool = False, verify: bool = True):
        self._require_existing_database()
        connection = None
        try:
            connection = sqlite3.connect(
                self.database_path.absolute().as_uri() + "?mode=rw",
                uri=True,
                timeout=5.0,
                isolation_level=None,
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=5000")
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            if verify:
                self._verify_schema(connection)
            yield connection
            connection.commit()
        except (sqlite3.Error, OverflowError) as error:
            if connection is not None:
                connection.rollback()
            raise WorkViolation(
                "NATIVE_WORK_JOURNAL_UNAVAILABLE",
                "Native work checkpoint transaction is unavailable",
                ErrorCode.UNAVAILABLE,
            ) from error
        except BaseException:
            if connection is not None:
                connection.rollback()
            raise
        finally:
            if connection is not None:
                connection.close()

    @staticmethod
    def _encode(snapshot: WorkSnapshot) -> tuple[str, str, str]:
        if not isinstance(snapshot, WorkSnapshot):
            raise WorkViolation(
                "NATIVE_WORK_CHECKPOINT_INVALID",
                "checkpoint requires an immutable work snapshot",
            )
        payload = snapshot.to_dict()
        encoded = canonical_json_bytes(payload)
        if len(encoded) > _MAX_PAYLOAD_BYTES:
            raise WorkViolation(
                "NATIVE_WORK_CHECKPOINT_TOO_LARGE",
                "checkpoint exceeds its closed size bound",
            )
        identity = _digest(canonical_json_bytes({key: payload[key] for key in _IMMUTABLE_FIELDS}))
        return encoded.decode("utf-8"), _digest(encoded), identity

    @classmethod
    def _decode(cls, row: sqlite3.Row) -> WorkSnapshot:
        try:
            encoded = row["snapshot_json"].encode("utf-8")
            if (
                row["schema_version"] != _SCHEMA_VERSION
                or len(encoded) > _MAX_PAYLOAD_BYTES
                or _digest(encoded) != row["snapshot_sha256"]
            ):
                raise ValueError("invalid checkpoint envelope")
            snapshot = WorkSnapshot.from_dict(json.loads(encoded))
            _, digest, identity = cls._encode(snapshot)
            if (
                digest != row["snapshot_sha256"]
                or identity != row["identity_sha256"]
                or _scope_digest(snapshot.scope) != row["scope_sha256"]
                or snapshot.work_id != row["work_id"]
                or snapshot.revision != row["revision"]
                or snapshot.sequence != row["sequence"]
                or snapshot.request_id != row["request_id"]
            ):
                raise ValueError("checkpoint row identity disagrees with payload")
            return snapshot
        except (ValueError, TypeError, KeyError, AttributeError) as error:
            raise WorkViolation(
                "NATIVE_WORK_CHECKPOINT_CORRUPT",
                "Native work checkpoint integrity failed",
                ErrorCode.UNAVAILABLE,
            ) from error

    def save(self, snapshot: WorkSnapshot) -> None:
        encoded, payload_sha, identity_sha = self._encode(snapshot)
        scope_sha = _scope_digest(snapshot.scope)
        key = (scope_sha, snapshot.work_id, snapshot.revision)
        with self._connection(write=True) as connection:
            prior = connection.execute(
                "SELECT * FROM native_work_checkpoint WHERE scope_sha256=? AND work_id=? AND revision=?",
                key,
            ).fetchone()
            if prior is not None:
                previous = self._decode(prior)
                if snapshot.sequence == previous.sequence and prior["snapshot_sha256"] == payload_sha:
                    return
                if snapshot.sequence != previous.sequence + 1:
                    raise WorkViolation(
                        "NATIVE_WORK_CHECKPOINT_SEQUENCE_CONFLICT",
                        "checkpoint must advance exactly one sequence or replay exactly",
                        ErrorCode.CONFLICT,
                    )
                if identity_sha != prior["identity_sha256"]:
                    raise WorkViolation(
                        "NATIVE_WORK_CHECKPOINT_IDENTITY_CONFLICT",
                        "checkpoint cannot change admitted work identity",
                        ErrorCode.CONFLICT,
                    )
                if snapshot.state is not previous.state and snapshot.state not in _TRANSITIONS[previous.state]:
                    raise WorkViolation(
                        "NATIVE_WORK_CHECKPOINT_STATE_CONFLICT",
                        "checkpoint cannot revive a retired work revision",
                        ErrorCode.CONFLICT,
                    )
                connection.execute(
                    "UPDATE native_work_checkpoint SET sequence=?, snapshot_json=?, snapshot_sha256=? "
                    "WHERE scope_sha256=? AND work_id=? AND revision=? AND sequence=?",
                    (snapshot.sequence, encoded, payload_sha, *key, previous.sequence),
                )
                return
            if snapshot.sequence != 1 or snapshot.state is not WorkState.ACCEPTED or snapshot.execution_settled:
                raise WorkViolation(
                    "NATIVE_WORK_CHECKPOINT_ADMISSION_REQUIRED",
                    "new checkpoint requires initial accepted state",
                    ErrorCode.CONFLICT,
                )
            if connection.execute(
                "SELECT 1 FROM native_work_checkpoint WHERE scope_sha256=? AND request_id=?",
                (scope_sha, snapshot.request_id),
            ).fetchone():
                raise WorkViolation(
                    "NATIVE_WORK_CHECKPOINT_REQUEST_CONFLICT",
                    "request already names another work revision",
                    ErrorCode.CONFLICT,
                )
            latest = connection.execute(
                "SELECT MAX(revision) FROM native_work_checkpoint WHERE scope_sha256=? AND work_id=?",
                (scope_sha, snapshot.work_id),
            ).fetchone()[0]
            if snapshot.revision != (int(latest) + 1 if latest is not None else 1):
                raise WorkViolation(
                    "NATIVE_WORK_CHECKPOINT_REVISION_CONFLICT",
                    "new revision must follow its retained predecessor",
                    ErrorCode.CONFLICT,
                )
            count = connection.execute("SELECT COUNT(*) FROM native_work_checkpoint").fetchone()[0]
            if count >= self._max_records:
                raise WorkViolation(
                    "NATIVE_WORK_JOURNAL_FULL",
                    "bounded work journal is full",
                    ErrorCode.UNAVAILABLE,
                )
            connection.execute(
                "INSERT INTO native_work_checkpoint VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    _SCHEMA_VERSION,
                    scope_sha,
                    snapshot.work_id,
                    snapshot.revision,
                    snapshot.sequence,
                    snapshot.request_id,
                    identity_sha,
                    encoded,
                    payload_sha,
                ),
            )

    def restore(self) -> tuple[WorkSnapshot, ...]:
        with self._connection() as connection:
            count = connection.execute("SELECT COUNT(*) FROM native_work_checkpoint").fetchone()[0]
            if count > self._max_records:
                raise WorkViolation(
                    "NATIVE_WORK_JOURNAL_FULL",
                    "retained work exceeds configured capacity",
                    ErrorCode.UNAVAILABLE,
                )
            if connection.execute(
                "SELECT 1 FROM native_work_checkpoint WHERE length(CAST(snapshot_json AS BLOB)) > ? LIMIT 1",
                (_MAX_PAYLOAD_BYTES,),
            ).fetchone():
                raise WorkViolation(
                    "NATIVE_WORK_CHECKPOINT_CORRUPT",
                    "retained checkpoint exceeds its size bound",
                    ErrorCode.UNAVAILABLE,
                )
            return tuple(
                self._decode(row)
                for row in connection.execute(
                    "SELECT * FROM native_work_checkpoint ORDER BY scope_sha256, work_id, revision"
                ).fetchall()
            )
