"""Structured execution event models, listener protocol, and fault-isolated publisher."""

from __future__ import annotations

import copy
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

from atlas_core import generate_execution_id, get_logger, require_not_empty
from pydantic import BaseModel, ConfigDict, Field, model_validator

from atlas_orchestration.exceptions import InvalidWorkflowStateError
from atlas_orchestration.models.workflow_step import (
    ensure_timezone_aware,
    validate_json_compatible,
)

logger = get_logger(__name__)


class ExecutionEvent(BaseModel):
    """Immutable base model for all structured workflow and step execution events."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_id: str = Field(default_factory=generate_execution_id)
    event_type: str
    execution_id: str
    workflow_id: str
    workflow_version: int = 1
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    step_id: str | None = None
    step_execution_id: str | None = None
    attempt_number: int | None = None
    duration_seconds: float | None = None
    error_code: str | None = None
    error_message: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_event(self) -> ExecutionEvent:
        clean_event_id = self.event_id.strip() if isinstance(self.event_id, str) else ""
        clean_event_type = self.event_type.strip() if isinstance(self.event_type, str) else ""
        clean_exec_id = self.execution_id.strip() if isinstance(self.execution_id, str) else ""
        clean_wf_id = self.workflow_id.strip() if isinstance(self.workflow_id, str) else ""
        try:
            require_not_empty(clean_event_id, "event_id")
            require_not_empty(clean_event_type, "event_type")
            require_not_empty(clean_exec_id, "execution_id")
            require_not_empty(clean_wf_id, "workflow_id")
        except ValueError as exc:
            raise InvalidWorkflowStateError(str(exc)) from exc

        if isinstance(self.workflow_version, bool) or self.workflow_version < 1:
            raise InvalidWorkflowStateError(
                f"Event workflow_version must be >= 1, got {self.workflow_version!r}."
            )
        if self.attempt_number is not None and (
            isinstance(self.attempt_number, bool) or self.attempt_number < 1
        ):
            raise InvalidWorkflowStateError(
                f"Event attempt_number must be >= 1 when provided, got {self.attempt_number!r}."
            )
        if self.duration_seconds is not None and (
            isinstance(self.duration_seconds, bool) or self.duration_seconds < 0.0
        ):
            raise InvalidWorkflowStateError(
                f"Event duration_seconds must be >= 0.0, got {self.duration_seconds!r}."
            )

        ts = ensure_timezone_aware(
            self.timestamp,
            field_name="timestamp",
            error_cls=InvalidWorkflowStateError,
        )
        assert ts is not None

        validated_meta = validate_json_compatible(
            self.metadata,
            path="metadata",
            error_cls=InvalidWorkflowStateError,
        )

        object.__setattr__(self, "event_id", clean_event_id)
        object.__setattr__(self, "event_type", clean_event_type)
        object.__setattr__(self, "execution_id", clean_exec_id)
        object.__setattr__(self, "workflow_id", clean_wf_id)
        object.__setattr__(self, "timestamp", ts)
        object.__setattr__(self, "metadata", copy.deepcopy(validated_meta))
        return self


class WorkflowStarted(ExecutionEvent):
    """Emitted when a workflow transitions to ``RUNNING``."""

    event_type: str = "workflow.started"


class WorkflowSucceeded(ExecutionEvent):
    """Emitted when a workflow completes all steps and transitions to ``SUCCEEDED``."""

    event_type: str = "workflow.succeeded"


class WorkflowFailed(ExecutionEvent):
    """Emitted when a workflow fails and transitions to ``FAILED``."""

    event_type: str = "workflow.failed"


class StepStarted(ExecutionEvent):
    """Emitted immediately before an execution attempt of a workflow step begins."""

    event_type: str = "step.started"


class StepSucceeded(ExecutionEvent):
    """Emitted when a workflow step attempt succeeds."""

    event_type: str = "step.succeeded"


class StepFailed(ExecutionEvent):
    """Emitted when a workflow step permanently fails (no retries remaining or non-retryable)."""

    event_type: str = "step.failed"


class StepCancelled(ExecutionEvent):
    """Emitted when a pending downstream step is cancelled due to upstream failure."""

    event_type: str = "step.cancelled"


class StepRetryScheduled(ExecutionEvent):
    """Emitted when a failed step attempt is eligible for retry and scheduled after backoff."""

    event_type: str = "step.retry_scheduled"
    delay_seconds: float = 0.0
    next_attempt_number: int = 2
    max_attempts: int = 2

    @model_validator(mode="after")
    def _validate_retry_scheduled(self) -> StepRetryScheduled:
        if isinstance(self.delay_seconds, bool) or self.delay_seconds < 0.0:
            raise InvalidWorkflowStateError(
                f"StepRetryScheduled delay_seconds must be >= 0.0, got {self.delay_seconds!r}."
            )
        if isinstance(self.next_attempt_number, bool) or self.next_attempt_number < 2:
            raise InvalidWorkflowStateError(
                f"StepRetryScheduled next_attempt_number must be >= 2, "
                f"got {self.next_attempt_number!r}."
            )
        if isinstance(self.max_attempts, bool) or self.max_attempts < self.next_attempt_number:
            raise InvalidWorkflowStateError(
                f"StepRetryScheduled max_attempts ({self.max_attempts}) must be >= "
                f"next_attempt_number ({self.next_attempt_number})."
            )
        return self


class StepTimedOut(ExecutionEvent):
    """Emitted when a step attempt exceeds its configured ``timeout_seconds``."""

    event_type: str = "step.timed_out"
    timeout_seconds: float | None = None


@runtime_checkable
class ExecutionEventListener(Protocol):
    """Protocol for synchronous listeners receiving :class:`ExecutionEvent` instances."""

    def handle_event(self, event: ExecutionEvent) -> None:
        """Handle a single emitted :class:`ExecutionEvent`."""


EventSubscriber = ExecutionEventListener | Callable[[ExecutionEvent], None]


class ExecutionEventPublisher:
    """Synchronous, fault-isolated publisher for :class:`ExecutionEvent` notifications.

    Listeners are invoked in registration order. Standard exceptions raised by
    any listener are caught, logged, and recorded in :attr:`delivery_failures`
    so that observer failures never abort or corrupt workflow execution.
    """

    def __init__(
        self,
        listeners: list[EventSubscriber] | tuple[EventSubscriber, ...] | None = None,
    ) -> None:
        self._lock = threading.RLock()
        self._listeners: list[EventSubscriber] = []
        self._delivery_failures: list[tuple[ExecutionEvent, Exception]] = []
        self._emitted_events: list[ExecutionEvent] = []
        if listeners:
            for listener in listeners:
                self.register_listener(listener)

    @property
    def listeners(self) -> tuple[EventSubscriber, ...]:
        """Return a snapshot tuple of registered event listeners."""
        with self._lock:
            return tuple(self._listeners)

    @property
    def emitted_events(self) -> list[ExecutionEvent]:
        """Return a copy of all events emitted through this publisher."""
        with self._lock:
            return list(self._emitted_events)

    @property
    def delivery_failures(self) -> list[tuple[ExecutionEvent, Exception]]:
        """Return a copy of ``(event, exception)`` pairs from failed listener deliveries."""
        with self._lock:
            return list(self._delivery_failures)

    def register_listener(self, listener: EventSubscriber) -> None:
        """Register a listener to receive subsequent execution events."""
        if not isinstance(listener, ExecutionEventListener) and not callable(listener):
            raise InvalidWorkflowStateError(
                "Listener must implement ExecutionEventListener.handle_event or be callable."
            )
        with self._lock:
            if listener not in self._listeners:
                self._listeners.append(listener)

    def unregister_listener(self, listener: EventSubscriber) -> None:
        """Remove a previously registered listener if present."""
        with self._lock:
            if listener in self._listeners:
                self._listeners.remove(listener)

    def emit(self, event: ExecutionEvent) -> None:
        """Synchronously deliver ``event`` to all registered listeners with fault isolation."""
        with self._lock:
            self._emitted_events.append(event)
            current_listeners = list(self._listeners)

        for listener in current_listeners:
            try:
                if isinstance(listener, ExecutionEventListener):
                    listener.handle_event(event)
                else:
                    listener(event)
            except Exception as exc:
                with self._lock:
                    self._delivery_failures.append((event, exc))
                logger.warning(
                    "ExecutionEventListener failed while handling event '%s' "
                    "(execution_id=%s, step_id=%s): %s",
                    event.event_type,
                    event.execution_id,
                    event.step_id,
                    exc,
                )


__all__ = [
    "EventSubscriber",
    "ExecutionEvent",
    "ExecutionEventListener",
    "ExecutionEventPublisher",
    "StepCancelled",
    "StepFailed",
    "StepRetryScheduled",
    "StepStarted",
    "StepSucceeded",
    "StepTimedOut",
    "WorkflowFailed",
    "WorkflowStarted",
    "WorkflowSucceeded",
]
