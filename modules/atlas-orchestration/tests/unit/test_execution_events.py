"""Unit tests for structured execution events and fault-isolated event publishing."""

from __future__ import annotations

import pytest
from atlas_orchestration.exceptions import InvalidWorkflowStateError
from atlas_orchestration.models.events import (
    ExecutionEvent,
    ExecutionEventListener,
    ExecutionEventPublisher,
    StepCancelled,
    StepFailed,
    StepRetryScheduled,
    StepStarted,
    StepSucceeded,
    StepTimedOut,
    WorkflowFailed,
    WorkflowStarted,
    WorkflowSucceeded,
)


class RecordingListener:
    """Test listener implementing ExecutionEventListener."""

    def __init__(self) -> None:
        self.events: list[ExecutionEvent] = []

    def handle_event(self, event: ExecutionEvent) -> None:
        self.events.append(event)


class FailingListener:
    """Test listener that raises RuntimeError on every event."""

    def handle_event(self, event: ExecutionEvent) -> None:
        raise RuntimeError(f"Simulated listener failure for {event.event_type}")


class TestExecutionEvents:
    """Tests for structured execution event models and ExecutionEventPublisher."""

    def test_concrete_event_models_and_types(self) -> None:
        wf_started = WorkflowStarted(execution_id="exec_1", workflow_id="wf_1")
        assert wf_started.event_type == "workflow.started"
        assert wf_started.workflow_version == 1

        wf_succeeded = WorkflowSucceeded(
            execution_id="exec_1",
            workflow_id="wf_1",
            duration_seconds=3.2,
        )
        assert wf_succeeded.event_type == "workflow.succeeded"
        assert wf_succeeded.duration_seconds == 3.2

        wf_failed = WorkflowFailed(
            execution_id="exec_1",
            workflow_id="wf_1",
            error_code="StepExecutionError",
            error_message="Step failed",
        )
        assert wf_failed.event_type == "workflow.failed"

        step_started = StepStarted(
            execution_id="exec_1",
            workflow_id="wf_1",
            step_id="s1",
            attempt_number=1,
        )
        assert step_started.event_type == "step.started"

        step_succeeded = StepSucceeded(
            execution_id="exec_1",
            workflow_id="wf_1",
            step_id="s1",
            attempt_number=1,
            duration_seconds=0.75,
        )
        assert step_succeeded.event_type == "step.succeeded"

        step_retry = StepRetryScheduled(
            execution_id="exec_1",
            workflow_id="wf_1",
            step_id="s1",
            attempt_number=1,
            delay_seconds=1.5,
            next_attempt_number=2,
            max_attempts=3,
            error_code="StepExecutionError",
            error_message="Transient error",
        )
        assert step_retry.event_type == "step.retry_scheduled"
        assert step_retry.delay_seconds == 1.5
        assert step_retry.next_attempt_number == 2
        assert step_retry.max_attempts == 3

        step_timeout = StepTimedOut(
            execution_id="exec_1",
            workflow_id="wf_1",
            step_id="s1",
            attempt_number=1,
            timeout_seconds=5.0,
            duration_seconds=5.1,
            error_code="StepTimeoutError",
            error_message="Timed out",
        )
        assert step_timeout.event_type == "step.timed_out"
        assert step_timeout.timeout_seconds == 5.0

        step_failed = StepFailed(
            execution_id="exec_1",
            workflow_id="wf_1",
            step_id="s1",
            attempt_number=3,
            error_code="StepExecutionError",
            error_message="Permanent failure",
        )
        assert step_failed.event_type == "step.failed"

        step_cancelled = StepCancelled(
            execution_id="exec_1",
            workflow_id="wf_1",
            step_id="s2",
        )
        assert step_cancelled.event_type == "step.cancelled"

    def test_invalid_event_fields_rejected(self) -> None:
        with pytest.raises(InvalidWorkflowStateError):
            WorkflowStarted(execution_id="", workflow_id="wf_1")

        with pytest.raises(InvalidWorkflowStateError):
            StepStarted(
                execution_id="exec_1",
                workflow_id="wf_1",
                step_id="s1",
                attempt_number=0,
            )

        with pytest.raises(InvalidWorkflowStateError):
            StepRetryScheduled(
                execution_id="exec_1",
                workflow_id="wf_1",
                step_id="s1",
                delay_seconds=-1.0,
            )

        with pytest.raises(InvalidWorkflowStateError):
            StepRetryScheduled(
                execution_id="exec_1",
                workflow_id="wf_1",
                step_id="s1",
                next_attempt_number=4,
                max_attempts=3,
            )

    def test_publisher_registers_delivers_and_unregisters_listeners(self) -> None:
        listener_1 = RecordingListener()
        assert isinstance(listener_1, ExecutionEventListener)
        callable_events: list[ExecutionEvent] = []

        publisher = ExecutionEventPublisher([listener_1])
        publisher.register_listener(callable_events.append)
        # Duplicate registration is ignored
        publisher.register_listener(listener_1)
        assert len(publisher.listeners) == 2

        ev1 = WorkflowStarted(execution_id="exec_1", workflow_id="wf_1")
        ev2 = StepStarted(execution_id="exec_1", workflow_id="wf_1", step_id="s1", attempt_number=1)
        publisher.emit(ev1)
        publisher.emit(ev2)

        assert listener_1.events == [ev1, ev2]
        assert callable_events == [ev1, ev2]
        assert publisher.emitted_events == [ev1, ev2]

        publisher.unregister_listener(callable_events.append)
        ev3 = WorkflowSucceeded(execution_id="exec_1", workflow_id="wf_1")
        publisher.emit(ev3)

        assert len(listener_1.events) == 3
        assert len(callable_events) == 2

    def test_publisher_isolates_listener_exceptions(self) -> None:
        before_listener = RecordingListener()
        failing_listener = FailingListener()
        after_listener = RecordingListener()

        publisher = ExecutionEventPublisher([before_listener, failing_listener, after_listener])
        ev = WorkflowStarted(execution_id="exec_1", workflow_id="wf_1")
        publisher.emit(ev)

        assert before_listener.events == [ev]
        assert after_listener.events == [ev]
        assert len(publisher.delivery_failures) == 1
        failed_event, exc = publisher.delivery_failures[0]
        assert failed_event == ev
        assert isinstance(exc, RuntimeError)

    def test_invalid_listener_rejected(self) -> None:
        publisher = ExecutionEventPublisher()
        with pytest.raises(InvalidWorkflowStateError):
            publisher.register_listener("not_a_listener")  # type: ignore[arg-type]
