"""Unit tests for RetryPolicy, exponential backoff, retry eligibility, and StepAttempt."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from atlas_core import ExecutionStatus
from atlas_orchestration.exceptions import (
    CyclicDependencyError,
    InvalidStepDefinitionError,
    InvalidWorkflowDefinitionError,
    InvalidWorkflowDependencyError,
    InvalidWorkflowStateError,
    PersistenceError,
    StepExecutionError,
    StepTimeoutError,
    UnknownModuleTargetError,
    UnresolvedContextReferenceError,
)
from atlas_orchestration.execution.context import ExecutionContext
from atlas_orchestration.execution.executors.ingestion import IngestionExecutor
from atlas_orchestration.execution.executors.processing import ProcessingExecutor
from atlas_orchestration.execution.executors.quality import QualityExecutor
from atlas_orchestration.models.retry import RetryPolicy, is_retryable_exception
from atlas_orchestration.models.step_execution import StepAttempt, StepExecution
from atlas_orchestration.models.workflow_execution import WorkflowExecution
from atlas_orchestration.models.workflow_step import WorkflowStep
from atlas_orchestration.tracking.memory import InMemoryExecutionTracker


class TestRetryPolicyAndBackoff:
    """Tests for RetryPolicy validation and exponential backoff calculation."""

    def test_default_retry_policy(self) -> None:
        policy = RetryPolicy()
        assert policy.max_attempts == 1
        assert policy.initial_delay_seconds == 1.0
        assert policy.backoff_multiplier == 2.0
        assert policy.max_delay_seconds is None

    def test_exponential_backoff_calculation(self) -> None:
        policy = RetryPolicy(
            max_attempts=5,
            initial_delay_seconds=1.5,
            backoff_multiplier=2.0,
            max_delay_seconds=None,
        )
        assert policy.calculate_delay(0) == pytest.approx(1.5)
        assert policy.calculate_delay(1) == pytest.approx(3.0)
        assert policy.calculate_delay(2) == pytest.approx(6.0)
        assert policy.calculate_delay(3) == pytest.approx(12.0)

    def test_max_delay_seconds_cap(self) -> None:
        policy = RetryPolicy(
            max_attempts=5,
            initial_delay_seconds=2.0,
            backoff_multiplier=3.0,
            max_delay_seconds=10.0,
        )
        assert policy.calculate_delay(0) == pytest.approx(2.0)
        assert policy.calculate_delay(1) == pytest.approx(6.0)
        assert policy.calculate_delay(2) == pytest.approx(10.0)
        assert policy.calculate_delay(3) == pytest.approx(10.0)

    def test_zero_initial_delay(self) -> None:
        policy = RetryPolicy(
            max_attempts=3,
            initial_delay_seconds=0.0,
            backoff_multiplier=2.0,
            max_delay_seconds=0.0,
        )
        assert policy.calculate_delay(0) == 0.0
        assert policy.calculate_delay(1) == 0.0

    def test_invalid_retry_policy_rejected(self) -> None:
        with pytest.raises(InvalidStepDefinitionError):
            RetryPolicy(max_attempts=0)

        with pytest.raises(InvalidStepDefinitionError):
            RetryPolicy(initial_delay_seconds=-0.5)

        with pytest.raises(InvalidStepDefinitionError):
            RetryPolicy(backoff_multiplier=0.5)

        with pytest.raises(InvalidStepDefinitionError):
            RetryPolicy(initial_delay_seconds=5.0, max_delay_seconds=2.0)

        with pytest.raises(InvalidStepDefinitionError):
            RetryPolicy(max_attempts=True)

        with pytest.raises(InvalidStepDefinitionError):
            RetryPolicy.model_validate({"max_attempts": 2, "extra_field": 1})

    def test_negative_retry_index_rejected(self) -> None:
        policy = RetryPolicy(max_attempts=3)
        with pytest.raises(InvalidStepDefinitionError, match="retry_index must be an integer >= 0"):
            policy.calculate_delay(-1)

    def test_workflow_step_with_retry_policy_and_timeout(self) -> None:
        step = WorkflowStep(
            step_id="ingest_step",
            module_target="ingestion",
            action="ingest",
            retry_policy=RetryPolicy(max_attempts=3, initial_delay_seconds=0.5),
            timeout_seconds=15.0,
        )
        assert step.retry_policy is not None
        assert step.retry_policy.max_attempts == 3
        assert step.timeout_seconds == 15.0

        with pytest.raises(InvalidStepDefinitionError):
            WorkflowStep(
                step_id="bad_timeout",
                module_target="ingestion",
                action="ingest",
                timeout_seconds=0.0,
            )

        with pytest.raises(InvalidStepDefinitionError):
            WorkflowStep(
                step_id="negative_timeout",
                module_target="ingestion",
                action="ingest",
                timeout_seconds=-5.0,
            )


class TestRetryEligibility:
    """Tests for retryable vs non-retryable exceptions."""

    def test_retryable_exceptions(self) -> None:
        assert is_retryable_exception(StepExecutionError("Transient failure")) is True
        assert is_retryable_exception(StepTimeoutError("Timed out", timeout_seconds=5.0)) is True
        assert is_retryable_exception(TimeoutError("OS timeout")) is True
        assert is_retryable_exception(RuntimeError("Connection reset")) is True

    def test_non_retryable_exceptions(self) -> None:
        assert (
            is_retryable_exception(
                StepExecutionError("Invalid schema", retryable=False, error_code="INVALID_SCHEMA")
            )
            is False
        )
        assert (
            is_retryable_exception(
                StepTimeoutError("Non-retryable timeout", timeout_seconds=1.0, retryable=False)
            )
            is False
        )
        assert is_retryable_exception(UnknownModuleTargetError("Unknown target")) is False
        assert is_retryable_exception(UnresolvedContextReferenceError("Bad ref")) is False
        assert is_retryable_exception(InvalidWorkflowDefinitionError("Bad wf")) is False
        assert is_retryable_exception(CyclicDependencyError("Cycle")) is False
        assert is_retryable_exception(InvalidWorkflowDependencyError("Missing dep")) is False
        assert is_retryable_exception(InvalidWorkflowStateError("Invalid state")) is False
        assert is_retryable_exception(PersistenceError("DB error")) is False

    def test_concrete_executors_raise_non_retryable_for_config_errors(self) -> None:
        ctx = ExecutionContext(
            workflow_id="wf_test",
            workflow_version=1,
            execution_id="exec_1",
        )
        for executor, target in [
            (IngestionExecutor(), "ingestion"),
            (ProcessingExecutor(), "processing"),
            (QualityExecutor(), "quality"),
        ]:
            bad_action_step = WorkflowStep(
                step_id="s1",
                module_target=target,
                action="unsupported_action",
            )
            with pytest.raises(StepExecutionError) as exc_info:
                executor.execute(bad_action_step, ctx)
            assert exc_info.value.retryable is False
            assert exc_info.value.error_code == "UNSUPPORTED_ACTION"
            assert is_retryable_exception(exc_info.value) is False

            missing_dep_step = WorkflowStep(
                step_id="s1",
                module_target=target,
                action="run",
            )
            with pytest.raises(StepExecutionError) as exc_info:
                executor.execute(missing_dep_step, ctx, dependencies=None)
            assert exc_info.value.retryable is False
            assert exc_info.value.error_code == "MISSING_DEPENDENCY"
            assert is_retryable_exception(exc_info.value) is False


class TestStepAttemptModelAndTracking:
    """Tests for StepAttempt validation and attempt recording in InMemoryExecutionTracker."""

    def test_step_attempt_validation(self) -> None:
        t0 = datetime.now(UTC)
        t1 = t0 + timedelta(seconds=1.25)
        attempt = StepAttempt(
            attempt_number=1,
            status=ExecutionStatus.FAILED,
            start_time=t0,
            end_time=t1,
            duration_seconds=1.25,
            error_code="STEP_TIMEOUT",
            error_message="Step timed out after 1.0s",
            timed_out=True,
        )
        assert attempt.attempt_number == 1
        assert attempt.timed_out is True
        assert attempt.duration_seconds == 1.25

        with pytest.raises(InvalidWorkflowStateError):
            StepAttempt(
                attempt_number=0,
                status=ExecutionStatus.FAILED,
                start_time=t0,
            )

        with pytest.raises(InvalidWorkflowStateError):
            StepAttempt(
                attempt_number=1,
                status=ExecutionStatus.FAILED,
                start_time=t1,
                end_time=t0,
            )

    def test_step_execution_validates_duplicate_attempt_numbers(self) -> None:
        t0 = datetime.now(UTC)
        a1 = StepAttempt(attempt_number=1, status=ExecutionStatus.FAILED, start_time=t0)
        a1_dup = StepAttempt(attempt_number=1, status=ExecutionStatus.SUCCEEDED, start_time=t0)
        with pytest.raises(InvalidWorkflowStateError, match="Duplicate attempt_number"):
            StepExecution(
                execution_id="exec_1",
                step_id="step_1",
                module_target="ingestion",
                attempts=[a1, a1_dup],
            )

    def test_in_memory_tracker_records_and_lists_step_attempts(self) -> None:
        tracker = InMemoryExecutionTracker()
        wf_exec = tracker.create_workflow_execution(
            WorkflowExecution(
                execution_id="exec_1",
                workflow_id="wf_1",
                workflow_version=1,
                status=ExecutionStatus.RUNNING,
            )
        )
        step_exec = tracker.create_step_execution(
            StepExecution(
                step_execution_id="step_exec_1",
                execution_id=wf_exec.execution_id,
                step_id="step_1",
                module_target="ingestion",
                status=ExecutionStatus.RUNNING,
            )
        )
        t0 = datetime.now(UTC)
        attempt1 = StepAttempt(
            attempt_number=1,
            status=ExecutionStatus.FAILED,
            start_time=t0,
            end_time=t0 + timedelta(seconds=0.5),
            duration_seconds=0.5,
            error_code="StepExecutionError",
            error_message="Transient failure",
        )
        attempt2 = StepAttempt(
            attempt_number=2,
            status=ExecutionStatus.SUCCEEDED,
            start_time=t0 + timedelta(seconds=1.0),
            end_time=t0 + timedelta(seconds=1.4),
            duration_seconds=0.4,
        )
        tracker.record_step_attempt(step_exec.step_execution_id, attempt1)
        updated_step = tracker.record_step_attempt(step_exec.step_execution_id, attempt2)

        assert updated_step.attempt_number == 2
        attempts = tracker.list_step_attempts(step_exec.step_execution_id)
        assert len(attempts) == 2
        assert attempts[0].attempt_number == 1
        assert attempts[0].status == ExecutionStatus.FAILED
        assert attempts[1].attempt_number == 2
        assert attempts[1].status == ExecutionStatus.SUCCEEDED
