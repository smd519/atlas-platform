"""Manages ingestion lifecycle stages and transitions."""

from __future__ import annotations

from enum import StrEnum

from atlas_core import ExecutionStatus

from atlas_ingestion.exceptions import LifecycleTransitionError


class IngestionState(StrEnum):
    """Discrete states of a single ingestion operation."""

    PENDING = "pending"
    EXTRACTING = "extracting"
    EXTRACTED = "extracted"
    LOADING = "loading"
    COMPLETED = "completed"
    FAILED = "failed"


VALID_TRANSITIONS: dict[IngestionState, frozenset[IngestionState]] = {
    IngestionState.PENDING: frozenset({IngestionState.EXTRACTING, IngestionState.FAILED}),
    IngestionState.EXTRACTING: frozenset({IngestionState.EXTRACTED, IngestionState.FAILED}),
    IngestionState.EXTRACTED: frozenset({IngestionState.LOADING, IngestionState.FAILED}),
    IngestionState.LOADING: frozenset({IngestionState.COMPLETED, IngestionState.FAILED}),
    IngestionState.COMPLETED: frozenset(),
    IngestionState.FAILED: frozenset(),
}

TERMINAL_STATES: frozenset[IngestionState] = frozenset(
    {
        IngestionState.COMPLETED,
        IngestionState.FAILED,
    }
)


class IngestionLifecycle:
    """State machine enforcing valid transitions for an ingestion execution."""

    def __init__(self, initial_state: IngestionState = IngestionState.PENDING) -> None:
        if not isinstance(initial_state, IngestionState):
            raise LifecycleTransitionError(f"Invalid initial lifecycle state: {initial_state!r}")
        self._state: IngestionState = initial_state
        self._history: list[IngestionState] = [initial_state]

    @property
    def state(self) -> IngestionState:
        """Current lifecycle state."""
        return self._state

    @property
    def history(self) -> tuple[IngestionState, ...]:
        """Ordered sequence of states visited by this lifecycle instance."""
        return tuple(self._history)

    @property
    def is_terminal(self) -> bool:
        """Return True if the lifecycle has reached a terminal state."""
        return self._state in TERMINAL_STATES

    def can_transition_to(self, target: IngestionState) -> bool:
        """Return True if transitioning from the current state to ``target`` is valid."""
        allowed = VALID_TRANSITIONS.get(self._state, frozenset())
        return target in allowed

    def transition_to(self, target: IngestionState) -> IngestionState:
        """Transition to ``target`` state or raise :class:`LifecycleTransitionError`."""
        if not isinstance(target, IngestionState):
            raise LifecycleTransitionError(
                f"Target state must be an IngestionState, got {target!r}."
            )
        if not self.can_transition_to(target):
            raise LifecycleTransitionError(
                f"Invalid ingestion lifecycle transition from '{self._state}' to '{target}'."
            )
        self._state = target
        self._history.append(target)
        return self._state

    def start_extraction(self) -> IngestionState:
        """Transition from PENDING to EXTRACTING."""
        return self.transition_to(IngestionState.EXTRACTING)

    def complete_extraction(self) -> IngestionState:
        """Transition from EXTRACTING to EXTRACTED."""
        return self.transition_to(IngestionState.EXTRACTED)

    def start_loading(self) -> IngestionState:
        """Transition from EXTRACTED to LOADING."""
        return self.transition_to(IngestionState.LOADING)

    def complete(self) -> IngestionState:
        """Transition from LOADING to COMPLETED."""
        return self.transition_to(IngestionState.COMPLETED)

    def fail(self) -> IngestionState:
        """Transition from any non-terminal state to FAILED."""
        return self.transition_to(IngestionState.FAILED)

    def to_execution_status(self) -> ExecutionStatus:
        """Map the current ingestion lifecycle state to core :class:`ExecutionStatus`."""
        if self._state == IngestionState.PENDING:
            return ExecutionStatus.PENDING
        if self._state == IngestionState.COMPLETED:
            return ExecutionStatus.SUCCEEDED
        if self._state == IngestionState.FAILED:
            return ExecutionStatus.FAILED
        return ExecutionStatus.RUNNING


__all__ = [
    "IngestionLifecycle",
    "IngestionState",
    "TERMINAL_STATES",
    "VALID_TRANSITIONS",
]
