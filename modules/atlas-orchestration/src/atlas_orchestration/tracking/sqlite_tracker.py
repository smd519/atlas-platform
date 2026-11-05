"""Persistent SQLite execution tracker implementation using stdlib sqlite3."""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from atlas_core import ExecutionStatus

from atlas_orchestration.exceptions import (
    InvalidWorkflowStateError,
    OrchestrationError,
    PersistenceError,
    StepExecutionNotFoundError,
    WorkflowExecutionNotFoundError,
)
from atlas_orchestration.models.step_execution import StepAttempt, StepExecution
from atlas_orchestration.models.workflow_execution import WorkflowExecution
from atlas_orchestration.models.workflow_step import validate_json_compatible
from atlas_orchestration.tracking.execution_tracker import ExecutionTracker

SCHEMA_VERSION = 1

_DDL_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version INTEGER PRIMARY KEY,
        applied_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS workflow_executions (
        execution_id TEXT PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        workflow_version INTEGER NOT NULL,
        status TEXT NOT NULL,
        start_time TEXT,
        end_time TEXT,
        context_overrides_json TEXT NOT NULL,
        error_message TEXT,
        created_order INTEGER NOT NULL
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_workflow_executions_wf_status
    ON workflow_executions (workflow_id, status);
    """,
    """
    CREATE TABLE IF NOT EXISTS step_executions (
        step_execution_id TEXT PRIMARY KEY,
        execution_id TEXT NOT NULL REFERENCES workflow_executions(execution_id),
        step_id TEXT NOT NULL,
        module_target TEXT NOT NULL,
        status TEXT NOT NULL,
        attempt_number INTEGER NOT NULL,
        start_time TEXT,
        end_time TEXT,
        duration_seconds REAL,
        module_execution_ref TEXT,
        error_code TEXT,
        error_message TEXT,
        output_references_json TEXT NOT NULL,
        step_order INTEGER NOT NULL
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_step_executions_execution_id
    ON step_executions (execution_id, step_order);
    """,
    """
    CREATE TABLE IF NOT EXISTS step_attempts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        step_execution_id TEXT NOT NULL REFERENCES step_executions(step_execution_id),
        execution_id TEXT NOT NULL REFERENCES workflow_executions(execution_id),
        step_id TEXT NOT NULL,
        attempt_number INTEGER NOT NULL,
        status TEXT NOT NULL,
        start_time TEXT NOT NULL,
        end_time TEXT,
        duration_seconds REAL,
        error_code TEXT,
        error_message TEXT,
        timed_out INTEGER NOT NULL DEFAULT 0,
        UNIQUE(step_execution_id, attempt_number)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_step_attempts_step_exec
    ON step_attempts (step_execution_id, attempt_number);
    """,
)


class SQLiteExecutionTracker(ExecutionTracker):
    """Persistent, thread-safe :class:`ExecutionTracker` backed by SQLite."""

    def __init__(
        self,
        db_path: str | Path = ":memory:",
        *,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._db_path = str(db_path)
        self._lock = threading.RLock()
        if self._db_path != ":memory:":
            parent_dir = Path(self._db_path).expanduser().resolve().parent
            parent_dir.mkdir(parents=True, exist_ok=True)

        try:
            self._conn = sqlite3.connect(
                self._db_path,
                timeout=timeout_seconds,
                check_same_thread=False,
            )
            self._conn.row_factory = sqlite3.Row
            self._initialize_database()
        except OrchestrationError:
            raise
        except Exception as exc:
            raise PersistenceError(
                f"Failed to initialize SQLiteExecutionTracker at '{self._db_path}': {exc}"
            ) from exc

    @property
    def db_path(self) -> str:
        """Database path or ``':memory:'`` identifier."""
        return self._db_path

    def _initialize_database(self) -> None:
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("PRAGMA foreign_keys = ON;")
            cursor.execute("PRAGMA journal_mode = WAL;")
            cursor.close()

            with self._conn:
                for ddl in _DDL_STATEMENTS:
                    self._conn.execute(ddl)
                row = self._conn.execute(
                    "SELECT version FROM schema_migrations WHERE version = ?;",
                    (SCHEMA_VERSION,),
                ).fetchone()
                if row is None:
                    self._conn.execute(
                        "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?);",
                        (SCHEMA_VERSION, datetime.now(UTC).isoformat()),
                    )

    def get_journal_mode(self) -> str:
        """Return the active SQLite journal mode (e.g. ``'wal'`` or ``'memory'``)."""
        with self._lock:
            row = self._conn.execute("PRAGMA journal_mode;").fetchone()
            return str(row[0]).lower() if row else ""

    def get_schema_version(self) -> int:
        """Return the latest applied schema migration version."""
        with self._lock:
            row = self._conn.execute(
                "SELECT MAX(version) AS max_ver FROM schema_migrations;"
            ).fetchone()
            if row is None or row["max_ver"] is None:
                raise PersistenceError("No schema_migrations version found in SQLite tracker.")
            return int(row["max_ver"])

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        with self._lock:
            self._conn.close()

    def __enter__(self) -> SQLiteExecutionTracker:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def create_workflow_execution(self, execution: WorkflowExecution) -> WorkflowExecution:
        """Persist a new :class:`WorkflowExecution` record in SQLite."""
        if not isinstance(execution, WorkflowExecution):
            raise InvalidWorkflowStateError(
                f"Expected WorkflowExecution, got {type(execution).__name__}."
            )
        snapshot = execution.model_copy(deep=True)
        context_json = self._serialize_json_dict(
            snapshot.context_overrides,
            field_name="context_overrides",
        )

        with self._lock:
            existing = self._conn.execute(
                "SELECT execution_id FROM workflow_executions WHERE execution_id = ?;",
                (snapshot.execution_id,),
            ).fetchone()
            if existing is not None:
                raise InvalidWorkflowStateError(
                    f"Workflow execution '{snapshot.execution_id}' already exists.",
                    workflow_id=snapshot.workflow_id,
                    workflow_version=snapshot.workflow_version,
                    execution_id=snapshot.execution_id,
                )

            try:
                with self._conn:
                    order_row = self._conn.execute(
                        "SELECT COALESCE(MAX(created_order), 0) + 1 AS next_order "
                        "FROM workflow_executions;"
                    ).fetchone()
                    next_order = int(order_row["next_order"]) if order_row else 1
                    self._conn.execute(
                        """
                        INSERT INTO workflow_executions (
                            execution_id,
                            workflow_id,
                            workflow_version,
                            status,
                            start_time,
                            end_time,
                            context_overrides_json,
                            error_message,
                            created_order
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        (
                            snapshot.execution_id,
                            snapshot.workflow_id,
                            snapshot.workflow_version,
                            str(snapshot.status.value),
                            self._format_dt(snapshot.start_time),
                            self._format_dt(snapshot.end_time),
                            context_json,
                            snapshot.error_message,
                            next_order,
                        ),
                    )
            except sqlite3.Error as exc:
                raise PersistenceError(
                    f"Failed to persist workflow execution '{snapshot.execution_id}': {exc}",
                    workflow_id=snapshot.workflow_id,
                    workflow_version=snapshot.workflow_version,
                    execution_id=snapshot.execution_id,
                ) from exc

            return self.get_workflow_execution(snapshot.execution_id)

    def get_workflow_execution(self, execution_id: str) -> WorkflowExecution:
        """Retrieve a :class:`WorkflowExecution` from SQLite by ID."""
        clean_id = execution_id.strip() if isinstance(execution_id, str) else ""
        if not clean_id:
            raise WorkflowExecutionNotFoundError("Execution identifier must not be empty.")

        with self._lock:
            try:
                row = self._conn.execute(
                    "SELECT * FROM workflow_executions WHERE execution_id = ?;",
                    (clean_id,),
                ).fetchone()
            except sqlite3.Error as exc:
                raise PersistenceError(
                    f"Failed to query workflow execution '{clean_id}': {exc}",
                    execution_id=clean_id,
                ) from exc

            if row is None:
                raise WorkflowExecutionNotFoundError(
                    f"Workflow execution '{clean_id}' not found.",
                    execution_id=clean_id,
                )
            return self._row_to_workflow_execution(row)

    def update_workflow_execution(self, execution: WorkflowExecution) -> WorkflowExecution:
        """Update an existing :class:`WorkflowExecution` record in SQLite."""
        if not isinstance(execution, WorkflowExecution):
            raise InvalidWorkflowStateError(
                f"Expected WorkflowExecution, got {type(execution).__name__}."
            )
        snapshot = execution.model_copy(deep=True)
        context_json = self._serialize_json_dict(
            snapshot.context_overrides,
            field_name="context_overrides",
        )

        with self._lock:
            existing = self._conn.execute(
                "SELECT workflow_id, workflow_version FROM workflow_executions "
                "WHERE execution_id = ?;",
                (snapshot.execution_id,),
            ).fetchone()
            if existing is None:
                raise WorkflowExecutionNotFoundError(
                    f"Workflow execution '{snapshot.execution_id}' not found.",
                    workflow_id=snapshot.workflow_id,
                    workflow_version=snapshot.workflow_version,
                    execution_id=snapshot.execution_id,
                )
            if (
                str(existing["workflow_id"]) != snapshot.workflow_id
                or int(existing["workflow_version"]) != snapshot.workflow_version
            ):
                raise InvalidWorkflowStateError(
                    "Cannot change workflow_id or workflow_version of an existing execution.",
                    workflow_id=snapshot.workflow_id,
                    workflow_version=snapshot.workflow_version,
                    execution_id=snapshot.execution_id,
                )

            try:
                with self._conn:
                    self._conn.execute(
                        """
                        UPDATE workflow_executions
                        SET status = ?,
                            start_time = ?,
                            end_time = ?,
                            context_overrides_json = ?,
                            error_message = ?
                        WHERE execution_id = ?;
                        """,
                        (
                            str(snapshot.status.value),
                            self._format_dt(snapshot.start_time),
                            self._format_dt(snapshot.end_time),
                            context_json,
                            snapshot.error_message,
                            snapshot.execution_id,
                        ),
                    )
            except sqlite3.Error as exc:
                raise PersistenceError(
                    f"Failed to update workflow execution '{snapshot.execution_id}': {exc}",
                    workflow_id=snapshot.workflow_id,
                    workflow_version=snapshot.workflow_version,
                    execution_id=snapshot.execution_id,
                ) from exc

            return self.get_workflow_execution(snapshot.execution_id)

    def list_workflow_executions(
        self,
        workflow_id: str | None = None,
        status: ExecutionStatus | None = None,
    ) -> list[WorkflowExecution]:
        """List workflow executions ordered by creation order, with optional filters."""
        clean_wf_id = workflow_id.strip() if isinstance(workflow_id, str) else None
        clauses: list[str] = []
        params: list[Any] = []
        if clean_wf_id is not None:
            clauses.append("workflow_id = ?")
            params.append(clean_wf_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(str(status.value))

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        query = f"SELECT * FROM workflow_executions {where_sql} ORDER BY created_order ASC;"

        with self._lock:
            try:
                rows = self._conn.execute(query, tuple(params)).fetchall()
            except sqlite3.Error as exc:
                raise PersistenceError(f"Failed to list workflow executions: {exc}") from exc
            return [self._row_to_workflow_execution(r) for r in rows]

    def create_step_execution(self, step_execution: StepExecution) -> StepExecution:
        """Persist a new :class:`StepExecution` record and any initial attempts in SQLite."""
        if not isinstance(step_execution, StepExecution):
            raise InvalidWorkflowStateError(
                f"Expected StepExecution, got {type(step_execution).__name__}."
            )
        snapshot = step_execution.model_copy(deep=True)
        outputs_json = self._serialize_json_dict(
            snapshot.output_references,
            field_name="output_references",
        )

        with self._lock:
            parent = self._conn.execute(
                "SELECT workflow_id, workflow_version FROM workflow_executions "
                "WHERE execution_id = ?;",
                (snapshot.execution_id,),
            ).fetchone()
            if parent is None:
                raise WorkflowExecutionNotFoundError(
                    f"Parent workflow execution '{snapshot.execution_id}' not found "
                    f"for step '{snapshot.step_id}'.",
                    execution_id=snapshot.execution_id,
                    step_id=snapshot.step_id,
                    step_execution_id=snapshot.step_execution_id,
                )

            existing = self._conn.execute(
                "SELECT step_execution_id FROM step_executions WHERE step_execution_id = ?;",
                (snapshot.step_execution_id,),
            ).fetchone()
            if existing is not None:
                raise InvalidWorkflowStateError(
                    f"Step execution '{snapshot.step_execution_id}' already exists.",
                    workflow_id=str(parent["workflow_id"]),
                    workflow_version=int(parent["workflow_version"]),
                    execution_id=snapshot.execution_id,
                    step_id=snapshot.step_id,
                    step_execution_id=snapshot.step_execution_id,
                )

            try:
                with self._conn:
                    order_row = self._conn.execute(
                        "SELECT COALESCE(MAX(step_order), 0) + 1 AS next_order "
                        "FROM step_executions WHERE execution_id = ?;",
                        (snapshot.execution_id,),
                    ).fetchone()
                    next_order = int(order_row["next_order"]) if order_row else 1
                    self._conn.execute(
                        """
                        INSERT INTO step_executions (
                            step_execution_id,
                            execution_id,
                            step_id,
                            module_target,
                            status,
                            attempt_number,
                            start_time,
                            end_time,
                            duration_seconds,
                            module_execution_ref,
                            error_code,
                            error_message,
                            output_references_json,
                            step_order
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        (
                            snapshot.step_execution_id,
                            snapshot.execution_id,
                            snapshot.step_id,
                            snapshot.module_target,
                            str(snapshot.status.value),
                            snapshot.attempt_number,
                            self._format_dt(snapshot.start_time),
                            self._format_dt(snapshot.end_time),
                            snapshot.duration_seconds,
                            snapshot.module_execution_ref,
                            snapshot.error_code,
                            snapshot.error_message,
                            outputs_json,
                            next_order,
                        ),
                    )
                    for attempt in snapshot.attempts:
                        self._upsert_attempt_row(
                            step_execution_id=snapshot.step_execution_id,
                            execution_id=snapshot.execution_id,
                            step_id=snapshot.step_id,
                            attempt=attempt,
                        )
            except sqlite3.Error as exc:
                raise PersistenceError(
                    f"Failed to persist step execution '{snapshot.step_execution_id}': {exc}",
                    execution_id=snapshot.execution_id,
                    step_id=snapshot.step_id,
                    step_execution_id=snapshot.step_execution_id,
                ) from exc

            return self.get_step_execution(snapshot.step_execution_id)

    def get_step_execution(self, step_execution_id: str) -> StepExecution:
        """Retrieve a :class:`StepExecution` and its ordered attempts from SQLite by ID."""
        clean_id = step_execution_id.strip() if isinstance(step_execution_id, str) else ""
        if not clean_id:
            raise StepExecutionNotFoundError("Step execution identifier must not be empty.")

        with self._lock:
            try:
                row = self._conn.execute(
                    "SELECT * FROM step_executions WHERE step_execution_id = ?;",
                    (clean_id,),
                ).fetchone()
            except sqlite3.Error as exc:
                raise PersistenceError(
                    f"Failed to query step execution '{clean_id}': {exc}",
                    step_execution_id=clean_id,
                ) from exc

            if row is None:
                raise StepExecutionNotFoundError(
                    f"Step execution '{clean_id}' not found.",
                    step_execution_id=clean_id,
                )
            attempts = self._load_attempts(clean_id)
            return self._row_to_step_execution(row, attempts=attempts)

    def update_step_execution(self, step_execution: StepExecution) -> StepExecution:
        """Update an existing :class:`StepExecution` and sync its attempts in SQLite."""
        if not isinstance(step_execution, StepExecution):
            raise InvalidWorkflowStateError(
                f"Expected StepExecution, got {type(step_execution).__name__}."
            )
        snapshot = step_execution.model_copy(deep=True)
        outputs_json = self._serialize_json_dict(
            snapshot.output_references,
            field_name="output_references",
        )

        with self._lock:
            existing = self._conn.execute(
                "SELECT execution_id, step_id FROM step_executions WHERE step_execution_id = ?;",
                (snapshot.step_execution_id,),
            ).fetchone()
            if existing is None:
                raise StepExecutionNotFoundError(
                    f"Step execution '{snapshot.step_execution_id}' not found.",
                    execution_id=snapshot.execution_id,
                    step_id=snapshot.step_id,
                    step_execution_id=snapshot.step_execution_id,
                )
            if (
                str(existing["execution_id"]) != snapshot.execution_id
                or str(existing["step_id"]) != snapshot.step_id
            ):
                raise InvalidWorkflowStateError(
                    "Cannot change execution_id or step_id of an existing step execution.",
                    execution_id=snapshot.execution_id,
                    step_id=snapshot.step_id,
                    step_execution_id=snapshot.step_execution_id,
                )

            try:
                with self._conn:
                    self._conn.execute(
                        """
                        UPDATE step_executions
                        SET module_target = ?,
                            status = ?,
                            attempt_number = ?,
                            start_time = ?,
                            end_time = ?,
                            duration_seconds = ?,
                            module_execution_ref = ?,
                            error_code = ?,
                            error_message = ?,
                            output_references_json = ?
                        WHERE step_execution_id = ?;
                        """,
                        (
                            snapshot.module_target,
                            str(snapshot.status.value),
                            snapshot.attempt_number,
                            self._format_dt(snapshot.start_time),
                            self._format_dt(snapshot.end_time),
                            snapshot.duration_seconds,
                            snapshot.module_execution_ref,
                            snapshot.error_code,
                            snapshot.error_message,
                            outputs_json,
                            snapshot.step_execution_id,
                        ),
                    )
                    for attempt in snapshot.attempts:
                        self._upsert_attempt_row(
                            step_execution_id=snapshot.step_execution_id,
                            execution_id=snapshot.execution_id,
                            step_id=snapshot.step_id,
                            attempt=attempt,
                        )
            except sqlite3.Error as exc:
                raise PersistenceError(
                    f"Failed to update step execution '{snapshot.step_execution_id}': {exc}",
                    execution_id=snapshot.execution_id,
                    step_id=snapshot.step_id,
                    step_execution_id=snapshot.step_execution_id,
                ) from exc

            return self.get_step_execution(snapshot.step_execution_id)

    def list_step_executions(self, execution_id: str) -> list[StepExecution]:
        """Return all :class:`StepExecution` records for ``execution_id`` in creation order."""
        clean_id = execution_id.strip() if isinstance(execution_id, str) else ""
        if not clean_id:
            raise WorkflowExecutionNotFoundError("Execution identifier must not be empty.")

        with self._lock:
            wf_row = self._conn.execute(
                "SELECT execution_id FROM workflow_executions WHERE execution_id = ?;",
                (clean_id,),
            ).fetchone()
            if wf_row is None:
                raise WorkflowExecutionNotFoundError(
                    f"Workflow execution '{clean_id}' not found.",
                    execution_id=clean_id,
                )

            try:
                rows = self._conn.execute(
                    "SELECT * FROM step_executions WHERE execution_id = ? ORDER BY step_order ASC;",
                    (clean_id,),
                ).fetchall()
            except sqlite3.Error as exc:
                raise PersistenceError(
                    f"Failed to list step executions for '{clean_id}': {exc}",
                    execution_id=clean_id,
                ) from exc

            results: list[StepExecution] = []
            for row in rows:
                s_id = str(row["step_execution_id"])
                attempts = self._load_attempts(s_id)
                results.append(self._row_to_step_execution(row, attempts=attempts))
            return results

    def record_step_attempt(
        self,
        step_execution_id: str,
        attempt: StepAttempt,
    ) -> StepExecution:
        """Persist or update a :class:`StepAttempt` row in SQLite and return the updated step."""
        if not isinstance(attempt, StepAttempt):
            raise InvalidWorkflowStateError(f"Expected StepAttempt, got {type(attempt).__name__}.")
        with self._lock:
            step = self.get_step_execution(step_execution_id)
            try:
                with self._conn:
                    self._upsert_attempt_row(
                        step_execution_id=step.step_execution_id,
                        execution_id=step.execution_id,
                        step_id=step.step_id,
                        attempt=attempt,
                    )
                    new_attempt_num = max(step.attempt_number, attempt.attempt_number)
                    new_start = step.start_time or attempt.start_time
                    self._conn.execute(
                        """
                        UPDATE step_executions
                        SET attempt_number = ?,
                            start_time = ?
                        WHERE step_execution_id = ?;
                        """,
                        (
                            new_attempt_num,
                            self._format_dt(new_start),
                            step.step_execution_id,
                        ),
                    )
            except sqlite3.Error as exc:
                raise PersistenceError(
                    f"Failed to record attempt {attempt.attempt_number} for step execution "
                    f"'{step_execution_id}': {exc}",
                    execution_id=step.execution_id,
                    step_id=step.step_id,
                    step_execution_id=step_execution_id,
                ) from exc
            return self.get_step_execution(step_execution_id)

    def list_step_attempts(self, step_execution_id: str) -> list[StepAttempt]:
        """Return all :class:`StepAttempt` records for ``step_execution_id`` in attempt order."""
        clean_id = step_execution_id.strip() if isinstance(step_execution_id, str) else ""
        if not clean_id:
            raise StepExecutionNotFoundError("Step execution identifier must not be empty.")
        with self._lock:
            # Verify step exists
            self.get_step_execution(clean_id)
            return self._load_attempts(clean_id)

    def start(self, execution_id: str) -> None:
        """Transition ``execution_id`` to ``RUNNING`` status with a start timestamp."""
        with self._lock:
            current = self.get_workflow_execution(execution_id)
            now = datetime.now(UTC)
            start_ts = current.start_time or now
            updated = WorkflowExecution(
                execution_id=current.execution_id,
                workflow_id=current.workflow_id,
                workflow_version=current.workflow_version,
                status=ExecutionStatus.RUNNING,
                start_time=start_ts,
                end_time=current.end_time,
                context_overrides=current.context_overrides,
                error_message=current.error_message,
            )
            self.update_workflow_execution(updated)

    def complete(self, execution_id: str) -> None:
        """Transition ``execution_id`` to ``SUCCEEDED`` status with a completion timestamp."""
        with self._lock:
            current = self.get_workflow_execution(execution_id)
            now = datetime.now(UTC)
            start_ts = current.start_time or now
            end_ts = max(now, start_ts)
            updated = WorkflowExecution(
                execution_id=current.execution_id,
                workflow_id=current.workflow_id,
                workflow_version=current.workflow_version,
                status=ExecutionStatus.SUCCEEDED,
                start_time=start_ts,
                end_time=end_ts,
                context_overrides=current.context_overrides,
                error_message=current.error_message,
            )
            self.update_workflow_execution(updated)

    def fail(self, execution_id: str, reason: str) -> None:
        """Transition ``execution_id`` to ``FAILED`` status with ``reason`` and end timestamp."""
        with self._lock:
            current = self.get_workflow_execution(execution_id)
            now = datetime.now(UTC)
            start_ts = current.start_time or now
            end_ts = max(now, start_ts)
            updated = WorkflowExecution(
                execution_id=current.execution_id,
                workflow_id=current.workflow_id,
                workflow_version=current.workflow_version,
                status=ExecutionStatus.FAILED,
                start_time=start_ts,
                end_time=end_ts,
                context_overrides=current.context_overrides,
                error_message=reason,
            )
            self.update_workflow_execution(updated)

    def _upsert_attempt_row(
        self,
        *,
        step_execution_id: str,
        execution_id: str,
        step_id: str,
        attempt: StepAttempt,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO step_attempts (
                step_execution_id,
                execution_id,
                step_id,
                attempt_number,
                status,
                start_time,
                end_time,
                duration_seconds,
                error_code,
                error_message,
                timed_out
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(step_execution_id, attempt_number) DO UPDATE SET
                status = excluded.status,
                start_time = excluded.start_time,
                end_time = excluded.end_time,
                duration_seconds = excluded.duration_seconds,
                error_code = excluded.error_code,
                error_message = excluded.error_message,
                timed_out = excluded.timed_out;
            """,
            (
                step_execution_id,
                execution_id,
                step_id,
                attempt.attempt_number,
                str(attempt.status.value),
                self._format_dt(attempt.start_time),
                self._format_dt(attempt.end_time),
                attempt.duration_seconds,
                attempt.error_code,
                attempt.error_message,
                1 if attempt.timed_out else 0,
            ),
        )

    def _load_attempts(self, step_execution_id: str) -> list[StepAttempt]:
        try:
            rows = self._conn.execute(
                "SELECT * FROM step_attempts WHERE step_execution_id = ? "
                "ORDER BY attempt_number ASC;",
                (step_execution_id,),
            ).fetchall()
        except sqlite3.Error as exc:
            raise PersistenceError(
                f"Failed to load attempts for step execution '{step_execution_id}': {exc}",
                step_execution_id=step_execution_id,
            ) from exc

        attempts: list[StepAttempt] = []
        for row in rows:
            start_dt = self._parse_dt(row["start_time"], field_name="step_attempts.start_time")
            if start_dt is None:
                raise PersistenceError(
                    f"Corrupted NULL start_time in step_attempts for '{step_execution_id}'.",
                    step_execution_id=step_execution_id,
                )
            end_dt = self._parse_dt(row["end_time"], field_name="step_attempts.end_time")
            try:
                attempts.append(
                    StepAttempt(
                        attempt_number=int(row["attempt_number"]),
                        status=ExecutionStatus(str(row["status"])),
                        start_time=start_dt,
                        end_time=end_dt,
                        duration_seconds=(
                            float(row["duration_seconds"])
                            if row["duration_seconds"] is not None
                            else None
                        ),
                        error_code=row["error_code"],
                        error_message=row["error_message"],
                        timed_out=bool(int(row["timed_out"])),
                    )
                )
            except Exception as exc:
                raise PersistenceError(
                    f"Corrupted step_attempt record for '{step_execution_id}': {exc}",
                    step_execution_id=step_execution_id,
                ) from exc
        return attempts

    def _row_to_workflow_execution(self, row: sqlite3.Row) -> WorkflowExecution:
        exec_id = str(row["execution_id"])
        context_overrides = self._deserialize_json_dict(
            row["context_overrides_json"],
            field_name="context_overrides_json",
            execution_id=exec_id,
        )
        start_dt = self._parse_dt(row["start_time"], field_name="workflow_executions.start_time")
        end_dt = self._parse_dt(row["end_time"], field_name="workflow_executions.end_time")
        try:
            return WorkflowExecution(
                execution_id=exec_id,
                workflow_id=str(row["workflow_id"]),
                workflow_version=int(row["workflow_version"]),
                status=ExecutionStatus(str(row["status"])),
                start_time=start_dt,
                end_time=end_dt,
                context_overrides=context_overrides,
                error_message=row["error_message"],
            )
        except Exception as exc:
            raise PersistenceError(
                f"Corrupted workflow_execution record '{exec_id}': {exc}",
                execution_id=exec_id,
            ) from exc

    def _row_to_step_execution(
        self,
        row: sqlite3.Row,
        *,
        attempts: list[StepAttempt],
    ) -> StepExecution:
        step_exec_id = str(row["step_execution_id"])
        exec_id = str(row["execution_id"])
        output_refs = self._deserialize_json_dict(
            row["output_references_json"],
            field_name="output_references_json",
            execution_id=exec_id,
            step_execution_id=step_exec_id,
        )
        start_dt = self._parse_dt(row["start_time"], field_name="step_executions.start_time")
        end_dt = self._parse_dt(row["end_time"], field_name="step_executions.end_time")
        try:
            return StepExecution(
                step_execution_id=step_exec_id,
                execution_id=exec_id,
                step_id=str(row["step_id"]),
                module_target=str(row["module_target"]),
                status=ExecutionStatus(str(row["status"])),
                attempt_number=int(row["attempt_number"]),
                start_time=start_dt,
                end_time=end_dt,
                duration_seconds=(
                    float(row["duration_seconds"]) if row["duration_seconds"] is not None else None
                ),
                module_execution_ref=row["module_execution_ref"],
                error_code=row["error_code"],
                error_message=row["error_message"],
                output_references=output_refs,
                attempts=attempts,
            )
        except Exception as exc:
            raise PersistenceError(
                f"Corrupted step_execution record '{step_exec_id}': {exc}",
                execution_id=exec_id,
                step_execution_id=step_exec_id,
            ) from exc

    @staticmethod
    def _format_dt(dt: datetime | None) -> str | None:
        return dt.isoformat() if dt is not None else None

    @staticmethod
    def _parse_dt(raw: Any, *, field_name: str) -> datetime | None:
        if raw is None:
            return None
        if not isinstance(raw, str):
            raise PersistenceError(f"Expected ISO timestamp string for '{field_name}'.")
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise PersistenceError(
                f"Invalid ISO timestamp '{raw}' in '{field_name}': {exc}"
            ) from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed

    @staticmethod
    def _serialize_json_dict(value: dict[str, Any], *, field_name: str) -> str:
        validated = validate_json_compatible(
            value,
            path=field_name,
            error_cls=PersistenceError,
        )
        try:
            return json.dumps(validated, sort_keys=True)
        except Exception as exc:
            raise PersistenceError(f"Failed to serialize '{field_name}' to JSON: {exc}") from exc

    @staticmethod
    def _deserialize_json_dict(
        raw_json: Any,
        *,
        field_name: str,
        execution_id: str | None = None,
        step_execution_id: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(raw_json, str):
            raise PersistenceError(
                f"Expected JSON string in '{field_name}', got {type(raw_json).__name__}.",
                execution_id=execution_id,
                step_execution_id=step_execution_id,
            )
        try:
            parsed = json.loads(raw_json)
        except Exception as exc:
            raise PersistenceError(
                f"Corrupted JSON in '{field_name}': {exc}",
                execution_id=execution_id,
                step_execution_id=step_execution_id,
            ) from exc
        if not isinstance(parsed, dict):
            raise PersistenceError(
                f"Expected JSON object in '{field_name}', got {type(parsed).__name__}.",
                execution_id=execution_id,
                step_execution_id=step_execution_id,
            )
        validated = validate_json_compatible(
            parsed,
            path=field_name,
            error_cls=PersistenceError,
        )
        assert isinstance(validated, dict)
        return validated


__all__ = [
    "SCHEMA_VERSION",
    "SQLiteExecutionTracker",
]
