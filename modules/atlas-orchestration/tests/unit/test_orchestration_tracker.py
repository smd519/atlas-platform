"""Unit tests for InMemoryExecutionTracker lifecycle, thread-safety, and error handling."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from atlas_core import ExecutionStatus
from atlas_orchestration import (
    InMemoryExecutionTracker,
    InvalidWorkflowStateError,
    StepExecution,
    StepExecutionNotFoundError,
    WorkflowExecution,
    WorkflowExecutionNotFoundError,
)


def test_tracker_workflow_and_step_execution_crud() -> None:
    tracker = InMemoryExecutionTracker()
    t0 = datetime.now(UTC)
    t1 = t0 + timedelta(seconds=3)

    wf_exec = WorkflowExecution(
        execution_id="exec_001",
        workflow_id="wf_urban",
        workflow_version=1,
        status=ExecutionStatus.PENDING,
    )
    created_wf = tracker.create_workflow_execution(wf_exec)
    assert created_wf.execution_id == "exec_001"
    assert created_wf.status == ExecutionStatus.PENDING

    tracker.start("exec_001")
    running_wf = tracker.get_workflow_execution("exec_001")
    assert running_wf.status == ExecutionStatus.RUNNING
    assert running_wf.start_time is not None

    step_exec = StepExecution(
        step_execution_id="step_exec_001",
        execution_id="exec_001",
        step_id="ingest_raw",
        module_target="atlas-ingestion",
        status=ExecutionStatus.RUNNING,
        start_time=t0,
    )
    created_step = tracker.create_step_execution(step_exec)
    assert created_step.step_execution_id == "step_exec_001"

    # Update step execution to SUCCEEDED
    updated_step = StepExecution(
        step_execution_id="step_exec_001",
        execution_id="exec_001",
        step_id="ingest_raw",
        module_target="atlas-ingestion",
        status=ExecutionStatus.SUCCEEDED,
        start_time=t0,
        end_time=t1,
        output_references={"rows": 120},
    )
    tracker.update_step_execution(updated_step)
    fetched_step = tracker.get_step_execution("step_exec_001")
    assert fetched_step.status == ExecutionStatus.SUCCEEDED
    assert fetched_step.output_references == {"rows": 120}

    # Complete workflow execution via ExecutionManager interface
    tracker.complete("exec_001")
    completed_wf = tracker.get_workflow_execution("exec_001")
    assert completed_wf.status == ExecutionStatus.SUCCEEDED
    assert completed_wf.end_time is not None

    steps = tracker.list_step_executions("exec_001")
    assert len(steps) == 1
    assert steps[0].status == ExecutionStatus.SUCCEEDED


def test_tracker_start_and_fail_execution_manager_methods() -> None:
    tracker = InMemoryExecutionTracker()
    wf = tracker.create_workflow_execution(
        WorkflowExecution(
            execution_id="exec_fail_1",
            workflow_id="wf_sensor_sync",
            workflow_version=1,
            status=ExecutionStatus.PENDING,
        )
    )
    tracker.start(wf.execution_id)
    started = tracker.get_workflow_execution(wf.execution_id)
    assert started.status == ExecutionStatus.RUNNING
    assert started.start_time is not None

    tracker.fail(wf.execution_id, "Connection timeout")
    failed = tracker.get_workflow_execution(wf.execution_id)
    assert failed.status == ExecutionStatus.FAILED
    assert failed.error_message == "Connection timeout"
    assert failed.end_time is not None

    running_list = tracker.list_workflow_executions(status=ExecutionStatus.RUNNING)
    failed_list = tracker.list_workflow_executions(
        workflow_id="wf_sensor_sync",
        status=ExecutionStatus.FAILED,
    )
    assert running_list == []
    assert len(failed_list) == 1


def test_tracker_enforces_uniqueness_and_referential_integrity() -> None:
    tracker = InMemoryExecutionTracker()
    wf = WorkflowExecution(execution_id="exec_dup", workflow_id="wf_1")
    tracker.create_workflow_execution(wf)

    # Duplicate workflow execution_id rejected
    with pytest.raises(InvalidWorkflowStateError, match="already exists"):
        tracker.create_workflow_execution(wf)

    # Step referencing unknown workflow execution rejected
    orphan_step = StepExecution(
        execution_id="unknown_exec",
        step_id="s1",
        module_target="atlas-ingestion",
    )
    with pytest.raises(WorkflowExecutionNotFoundError):
        tracker.create_step_execution(orphan_step)

    # Duplicate step_execution_id rejected
    step = StepExecution(
        step_execution_id="s_exec_dup",
        execution_id="exec_dup",
        step_id="s1",
        module_target="atlas-ingestion",
    )
    tracker.create_step_execution(step)
    with pytest.raises(InvalidWorkflowStateError, match="already exists"):
        tracker.create_step_execution(step)

    # Cannot change workflow_id on update
    changed_wf = WorkflowExecution(execution_id="exec_dup", workflow_id="wf_other")
    with pytest.raises(InvalidWorkflowStateError, match="Cannot change workflow_id"):
        tracker.update_workflow_execution(changed_wf)

    # Cannot change step_id on update
    changed_step = StepExecution(
        step_execution_id="s_exec_dup",
        execution_id="exec_dup",
        step_id="different_step",
        module_target="atlas-ingestion",
    )
    with pytest.raises(InvalidWorkflowStateError, match="Cannot change execution_id or step_id"):
        tracker.update_step_execution(changed_step)

    # Missing lookups raise appropriate NotFound exceptions
    with pytest.raises(WorkflowExecutionNotFoundError):
        tracker.get_workflow_execution("missing_exec")
    with pytest.raises(WorkflowExecutionNotFoundError):
        tracker.list_step_executions("missing_exec")
    with pytest.raises(StepExecutionNotFoundError):
        tracker.get_step_execution("missing_step_exec")
    with pytest.raises(StepExecutionNotFoundError):
        tracker.update_step_execution(
            StepExecution(
                step_execution_id="missing_step_exec",
                execution_id="exec_dup",
                step_id="s1",
                module_target="atlas-ingestion",
            )
        )


def test_tracker_concurrent_step_tracking_thread_safety() -> None:
    tracker = InMemoryExecutionTracker()
    wf = WorkflowExecution(
        execution_id="exec_concurrent",
        workflow_id="wf_concurrent",
        status=ExecutionStatus.RUNNING,
    )
    tracker.create_workflow_execution(wf)

    num_steps = 20

    def record_step(idx: int) -> str:
        step = StepExecution(
            step_execution_id=f"step_exec_{idx}",
            execution_id="exec_concurrent",
            step_id=f"step_{idx}",
            module_target="atlas-processing",
            status=ExecutionStatus.SUCCEEDED,
            output_references={"idx": idx},
        )
        created = tracker.create_step_execution(step)
        return created.step_execution_id

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(record_step, i) for i in range(num_steps)]
        results = [f.result(timeout=10) for f in futures]

    assert len(results) == num_steps
    tracked_steps = tracker.list_step_executions("exec_concurrent")
    assert len(tracked_steps) == num_steps
