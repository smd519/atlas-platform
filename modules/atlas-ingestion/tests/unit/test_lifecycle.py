"""Unit tests for the ingestion lifecycle state machine."""

from __future__ import annotations

import pytest
from atlas_core import ExecutionStatus
from atlas_ingestion.exceptions import LifecycleTransitionError
from atlas_ingestion.lifecycle import IngestionLifecycle, IngestionState


def test_lifecycle_happy_path_transitions() -> None:
    lifecycle = IngestionLifecycle()
    assert lifecycle.state == IngestionState.PENDING
    assert lifecycle.is_terminal is False
    assert lifecycle.to_execution_status() == ExecutionStatus.PENDING

    assert lifecycle.start_extraction() == IngestionState.EXTRACTING
    assert lifecycle.to_execution_status() == ExecutionStatus.RUNNING

    assert lifecycle.complete_extraction() == IngestionState.EXTRACTED
    assert lifecycle.to_execution_status() == ExecutionStatus.RUNNING

    assert lifecycle.start_loading() == IngestionState.LOADING
    assert lifecycle.to_execution_status() == ExecutionStatus.RUNNING

    assert lifecycle.complete() == IngestionState.COMPLETED
    assert lifecycle.is_terminal is True
    assert lifecycle.to_execution_status() == ExecutionStatus.SUCCEEDED
    assert lifecycle.history == (
        IngestionState.PENDING,
        IngestionState.EXTRACTING,
        IngestionState.EXTRACTED,
        IngestionState.LOADING,
        IngestionState.COMPLETED,
    )


@pytest.mark.parametrize(
    "failure_stage",
    [
        IngestionState.PENDING,
        IngestionState.EXTRACTING,
        IngestionState.EXTRACTED,
        IngestionState.LOADING,
    ],
)
def test_lifecycle_can_transition_to_failed_from_non_terminal_states(
    failure_stage: IngestionState,
) -> None:
    lifecycle = IngestionLifecycle(initial_state=failure_stage)
    assert lifecycle.can_transition_to(IngestionState.FAILED) is True
    assert lifecycle.fail() == IngestionState.FAILED
    assert lifecycle.is_terminal is True
    assert lifecycle.to_execution_status() == ExecutionStatus.FAILED


@pytest.mark.parametrize(
    ("start_state", "invalid_target"),
    [
        (IngestionState.PENDING, IngestionState.EXTRACTED),
        (IngestionState.PENDING, IngestionState.LOADING),
        (IngestionState.PENDING, IngestionState.COMPLETED),
        (IngestionState.EXTRACTING, IngestionState.LOADING),
        (IngestionState.EXTRACTING, IngestionState.COMPLETED),
        (IngestionState.EXTRACTED, IngestionState.COMPLETED),
        (IngestionState.EXTRACTED, IngestionState.EXTRACTING),
        (IngestionState.LOADING, IngestionState.EXTRACTING),
        (IngestionState.COMPLETED, IngestionState.FAILED),
        (IngestionState.COMPLETED, IngestionState.PENDING),
        (IngestionState.FAILED, IngestionState.EXTRACTING),
        (IngestionState.FAILED, IngestionState.COMPLETED),
    ],
)
def test_lifecycle_rejects_invalid_transitions(
    start_state: IngestionState,
    invalid_target: IngestionState,
) -> None:
    lifecycle = IngestionLifecycle(initial_state=start_state)
    assert lifecycle.can_transition_to(invalid_target) is False
    with pytest.raises(LifecycleTransitionError):
        lifecycle.transition_to(invalid_target)
