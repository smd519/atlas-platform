"""Unit tests for SQLiteExecutionTracker persistence, schema versioning, WAL, and attempts."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from atlas_core import ExecutionStatus
from atlas_orchestration.exceptions import (
    InvalidWorkflowStateError,
    PersistenceError,
    StepExecutionNotFoundError,
    WorkflowExecutionNotFoundError,
)
from atlas_orchestration.models.step_execution import StepAttempt, StepExecution
from atlas_orchestration.models.workflow_execution import WorkflowExecution
from atlas_orchestration.tracking.sqlite_tracker import SCHEMA_VERSION, SQLiteExecutionTracker


class TestSQLiteExecutionTracker:
    """Tests for SQLiteExecutionTracker in-memory and file-backed modes."""

    def test_schema_version_and_wal_mode_on_file_db(self, tmp_path: Path) -> None:
        db_file = tmp_path / "orchestration_state.db"
        with SQLiteExecutionTracker(db_file) as tracker:
            assert tracker.get_schema_version() == SCHEMA_VERSION
            assert tracker.get_journal_mode() == "wal"

        # Re-open same file and verify idempotent schema initialization
        with SQLiteExecutionTracker(db_file) as tracker2:
            assert tracker2.get_schema_version() == SCHEMA_VERSION
            assert tracker2.get_journal_mode() == "wal"

    def test_workflow_execution_crud_and_filtering_persisted_across_restarts(
        self,
        tmp_path: Path,
    ) -> None:
        db_file = tmp_path / "wf_exec.db"
        t0 = datetime.now(UTC)

        with SQLiteExecutionTracker(db_file) as tracker:
            exec1 = tracker.create_workflow_execution(
                WorkflowExecution(
                    execution_id="exec_1",
                    workflow_id="wf_alpha",
                    workflow_version=1,
                    status=ExecutionStatus.PENDING,
                    context_overrides={"env": "prod", "batch_size": 500},
                )
            )
            assert exec1.execution_id == "exec_1"
            assert exec1.context_overrides == {"env": "prod", "batch_size": 500}

            tracker.start("exec_1")
            running = tracker.get_workflow_execution("exec_1")
            assert running.status == ExecutionStatus.RUNNING
            assert running.start_time is not None

            tracker.complete("exec_1")
            completed = tracker.get_workflow_execution("exec_1")
            assert completed.status == ExecutionStatus.SUCCEEDED
            assert completed.end_time is not None

            tracker.create_workflow_execution(
                WorkflowExecution(
                    execution_id="exec_2",
                    workflow_id="wf_beta",
                    workflow_version=2,
                    status=ExecutionStatus.PENDING,
                    start_time=t0,
                )
            )
            tracker.fail("exec_2", "Step 1 timed out")

        # Re-open from disk and verify full state survival
        with SQLiteExecutionTracker(db_file) as reopened:
            all_execs = reopened.list_workflow_executions()
            assert [e.execution_id for e in all_execs] == ["exec_1", "exec_2"]

            alpha_execs = reopened.list_workflow_executions(workflow_id="wf_alpha")
            assert len(alpha_execs) == 1
            assert alpha_execs[0].status == ExecutionStatus.SUCCEEDED
            assert alpha_execs[0].context_overrides == {"env": "prod", "batch_size": 500}

            failed_execs = reopened.list_workflow_executions(status=ExecutionStatus.FAILED)
            assert len(failed_execs) == 1
            assert failed_execs[0].execution_id == "exec_2"
            assert failed_execs[0].error_message == "Step 1 timed out"

    def test_step_execution_and_attempts_persistence(self, tmp_path: Path) -> None:
        db_file = tmp_path / "steps.db"
        t0 = datetime.now(UTC)

        with SQLiteExecutionTracker(db_file) as tracker:
            tracker.create_workflow_execution(
                WorkflowExecution(
                    execution_id="exec_1",
                    workflow_id="wf_1",
                    workflow_version=1,
                    status=ExecutionStatus.RUNNING,
                    start_time=t0,
                )
            )
            step1 = tracker.create_step_execution(
                StepExecution(
                    step_execution_id="se_1",
                    execution_id="exec_1",
                    step_id="ingest",
                    module_target="ingestion",
                    status=ExecutionStatus.RUNNING,
                    start_time=t0,
                )
            )
            step2 = tracker.create_step_execution(
                StepExecution(
                    step_execution_id="se_2",
                    execution_id="exec_1",
                    step_id="process",
                    module_target="processing",
                    status=ExecutionStatus.PENDING,
                )
            )
            assert step1.step_execution_id == "se_1"
            assert step2.step_execution_id == "se_2"

            attempt1 = StepAttempt(
                attempt_number=1,
                status=ExecutionStatus.FAILED,
                start_time=t0,
                end_time=t0 + timedelta(seconds=1.2),
                duration_seconds=1.2,
                error_code="StepTimeoutError",
                error_message="Timed out on attempt 1",
                timed_out=True,
            )
            attempt2 = StepAttempt(
                attempt_number=2,
                status=ExecutionStatus.SUCCEEDED,
                start_time=t0 + timedelta(seconds=2.0),
                end_time=t0 + timedelta(seconds=2.8),
                duration_seconds=0.8,
            )
            tracker.record_step_attempt("se_1", attempt1)
            tracker.record_step_attempt("se_1", attempt2)

            updated_s1 = StepExecution(
                step_execution_id="se_1",
                execution_id="exec_1",
                step_id="ingest",
                module_target="ingestion",
                status=ExecutionStatus.SUCCEEDED,
                attempt_number=2,
                start_time=t0,
                end_time=t0 + timedelta(seconds=2.8),
                duration_seconds=2.8,
                module_execution_ref="ing_99",
                output_references={"artifact_path": "/data/bronze.parquet", "rows": 120},
                attempts=[attempt1, attempt2],
            )
            tracker.update_step_execution(updated_s1)

        with SQLiteExecutionTracker(db_file) as reopened:
            steps = reopened.list_step_executions("exec_1")
            assert [s.step_id for s in steps] == ["ingest", "process"]
            s1 = steps[0]
            assert s1.status == ExecutionStatus.SUCCEEDED
            assert s1.attempt_number == 2
            assert s1.duration_seconds == pytest.approx(2.8)
            assert s1.module_execution_ref == "ing_99"
            assert s1.output_references == {"artifact_path": "/data/bronze.parquet", "rows": 120}
            assert len(s1.attempts) == 2
            assert s1.attempts[0].attempt_number == 1
            assert s1.attempts[0].timed_out is True
            assert s1.attempts[0].error_code == "StepTimeoutError"
            assert s1.attempts[1].attempt_number == 2
            assert s1.attempts[1].status == ExecutionStatus.SUCCEEDED

            listed_attempts = reopened.list_step_attempts("se_1")
            assert len(listed_attempts) == 2

    def test_error_conditions_and_invariants(self) -> None:
        with SQLiteExecutionTracker(":memory:") as tracker:
            with pytest.raises(WorkflowExecutionNotFoundError):
                tracker.get_workflow_execution("missing_exec")

            with pytest.raises(WorkflowExecutionNotFoundError):
                tracker.get_workflow_execution("   ")

            with pytest.raises(StepExecutionNotFoundError):
                tracker.get_step_execution("missing_step")

            with pytest.raises(StepExecutionNotFoundError):
                tracker.get_step_execution("")

            with pytest.raises(WorkflowExecutionNotFoundError):
                tracker.list_step_executions("missing_exec")

            # Creating step for non-existent workflow execution fails
            with pytest.raises(WorkflowExecutionNotFoundError):
                tracker.create_step_execution(
                    StepExecution(
                        step_execution_id="se_orphan",
                        execution_id="missing_exec",
                        step_id="s1",
                        module_target="ingestion",
                    )
                )

            wf_exec = tracker.create_workflow_execution(
                WorkflowExecution(
                    execution_id="exec_1",
                    workflow_id="wf_1",
                    workflow_version=1,
                )
            )
            # Duplicate workflow execution rejected
            with pytest.raises(InvalidWorkflowStateError):
                tracker.create_workflow_execution(wf_exec)

            # Mutating workflow_id on update rejected
            with pytest.raises(InvalidWorkflowStateError):
                tracker.update_workflow_execution(
                    WorkflowExecution(
                        execution_id="exec_1",
                        workflow_id="wf_changed",
                        workflow_version=1,
                    )
                )

            step_exec = tracker.create_step_execution(
                StepExecution(
                    step_execution_id="se_1",
                    execution_id="exec_1",
                    step_id="s1",
                    module_target="ingestion",
                )
            )
            # Duplicate step execution rejected
            with pytest.raises(InvalidWorkflowStateError):
                tracker.create_step_execution(step_exec)

            # Mutating step_id on update rejected
            with pytest.raises(InvalidWorkflowStateError):
                tracker.update_step_execution(
                    StepExecution(
                        step_execution_id="se_1",
                        execution_id="exec_1",
                        step_id="s_changed",
                        module_target="ingestion",
                    )
                )

    def test_corrupted_json_in_database_raises_persistence_error(
        self,
        tmp_path: Path,
    ) -> None:
        db_file = tmp_path / "corrupt.db"
        with SQLiteExecutionTracker(db_file) as tracker:
            tracker.create_workflow_execution(
                WorkflowExecution(
                    execution_id="exec_corrupt",
                    workflow_id="wf_1",
                    workflow_version=1,
                )
            )

        # Directly corrupt context_overrides_json in SQLite
        raw_conn = sqlite3.connect(str(db_file))
        with raw_conn:
            raw_conn.execute(
                "UPDATE workflow_executions SET context_overrides_json = '{not valid json' "
                "WHERE execution_id = 'exec_corrupt';"
            )
        raw_conn.close()

        with SQLiteExecutionTracker(db_file) as tracker2, pytest.raises(PersistenceError):
            tracker2.get_workflow_execution("exec_corrupt")
