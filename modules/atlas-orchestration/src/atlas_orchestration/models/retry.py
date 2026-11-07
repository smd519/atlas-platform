"""Retry policy model, exponential backoff calculation, and retry eligibility rules."""

from __future__ import annotations

import math
from typing import Any

from atlas_core import ConfigurationError
from atlas_core import ValidationError as CoreValidationError
from pydantic import BaseModel, ConfigDict, model_validator

from atlas_orchestration.exceptions import (
    DuplicateExecutorRegistrationError,
    DuplicateWorkflowError,
    ExecutionContextError,
    InvalidStepDefinitionError,
    InvalidWorkflowDefinitionError,
    InvalidWorkflowStateError,
    PersistenceError,
    StepExecutionError,
    StepTimeoutError,
    UnknownModuleTargetError,
    UnresolvedContextReferenceError,
    WorkflowNotFoundError,
)


class RetryPolicy(BaseModel):
    """Immutable configuration for step execution retries and exponential backoff.

    Parameters
    ----------
    max_attempts:
        Total number of execution attempts (>= 1). ``1`` means no retries;
        ``3`` means 1 initial execution plus up to 2 retries.
    initial_delay_seconds:
        Base backoff delay in seconds before the first retry (>= 0.0).
    backoff_multiplier:
        Exponential multiplier applied per retry index (>= 1.0).
    max_delay_seconds:
        Optional upper cap on the computed backoff delay (>= 0.0).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    max_attempts: int = 1
    initial_delay_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    max_delay_seconds: float | None = None

    def __init__(self, **data: Any) -> None:
        raw_attempts = data.get("max_attempts", 1)
        if isinstance(raw_attempts, bool) or not isinstance(raw_attempts, int) or raw_attempts < 1:
            raise InvalidStepDefinitionError(
                f"RetryPolicy max_attempts must be an integer >= 1, got {raw_attempts!r}."
            )
        for float_field in (
            "initial_delay_seconds",
            "backoff_multiplier",
            "max_delay_seconds",
        ):
            if float_field in data and data[float_field] is not None:
                val = data[float_field]
                if isinstance(val, bool) or not isinstance(val, (int, float)):
                    raise InvalidStepDefinitionError(
                        f"RetryPolicy '{float_field}' must be a number, got {val!r}."
                    )
        try:
            super().__init__(**data)
        except InvalidStepDefinitionError:
            raise
        except Exception as exc:
            raise InvalidStepDefinitionError(f"Invalid RetryPolicy configuration: {exc}") from exc

    @model_validator(mode="after")
    def _validate_policy(self) -> RetryPolicy:
        if self.max_attempts < 1:
            raise InvalidStepDefinitionError(
                f"RetryPolicy max_attempts must be >= 1, got {self.max_attempts}."
            )
        if math.isnan(self.initial_delay_seconds) or math.isinf(self.initial_delay_seconds):
            raise InvalidStepDefinitionError(
                "RetryPolicy initial_delay_seconds must be a finite number."
            )
        if self.initial_delay_seconds < 0.0:
            raise InvalidStepDefinitionError(
                f"RetryPolicy initial_delay_seconds must be >= 0.0, "
                f"got {self.initial_delay_seconds}."
            )
        if math.isnan(self.backoff_multiplier) or math.isinf(self.backoff_multiplier):
            raise InvalidStepDefinitionError(
                "RetryPolicy backoff_multiplier must be a finite number."
            )
        if self.backoff_multiplier < 1.0:
            raise InvalidStepDefinitionError(
                f"RetryPolicy backoff_multiplier must be >= 1.0, got {self.backoff_multiplier}."
            )
        if self.max_delay_seconds is not None:
            if math.isnan(self.max_delay_seconds) or math.isinf(self.max_delay_seconds):
                raise InvalidStepDefinitionError(
                    "RetryPolicy max_delay_seconds must be a finite number."
                )
            if self.max_delay_seconds < 0.0:
                raise InvalidStepDefinitionError(
                    f"RetryPolicy max_delay_seconds must be >= 0.0, got {self.max_delay_seconds}."
                )
            if self.max_delay_seconds < self.initial_delay_seconds:
                raise InvalidStepDefinitionError(
                    f"RetryPolicy max_delay_seconds ({self.max_delay_seconds}) cannot be less "
                    f"than initial_delay_seconds ({self.initial_delay_seconds})."
                )
        return self

    def calculate_delay(self, retry_index: int) -> float:
        """Compute the exponential backoff delay in seconds for ``retry_index`` (0-based).

        Formula: ``min(initial_delay * backoff_multiplier ** retry_index, max_delay)``
        """
        if isinstance(retry_index, bool) or not isinstance(retry_index, int) or retry_index < 0:
            raise InvalidStepDefinitionError(
                f"retry_index must be an integer >= 0, got {retry_index!r}."
            )
        delay = float(self.initial_delay_seconds * (self.backoff_multiplier**retry_index))
        if self.max_delay_seconds is not None:
            delay = min(delay, float(self.max_delay_seconds))
        return delay


_NON_RETRYABLE_TYPES: tuple[type[Exception], ...] = (
    InvalidWorkflowDefinitionError,
    UnknownModuleTargetError,
    UnresolvedContextReferenceError,
    ExecutionContextError,
    InvalidWorkflowStateError,
    DuplicateExecutorRegistrationError,
    DuplicateWorkflowError,
    WorkflowNotFoundError,
    PersistenceError,
    ConfigurationError,
    CoreValidationError,
    TypeError,
    ValueError,
    KeyError,
)


def is_retryable_exception(exc: Exception) -> bool:
    """Determine whether ``exc`` is eligible for step retry.

    Rules
    -----
    1. Structural, validation, configuration, unknown target, and context resolution
       exceptions are **never** retried.
    2. Any exception exposing an explicit boolean ``retryable`` attribute is respected.
    3. :class:`StepTimeoutError`, :class:`TimeoutError`, :class:`StepExecutionError`,
       and unexpected runtime exceptions during executor invocation are retryable.
    """
    if isinstance(exc, _NON_RETRYABLE_TYPES):
        return False

    explicit_flag = getattr(exc, "retryable", None)
    if isinstance(explicit_flag, bool):
        return explicit_flag

    if isinstance(exc, (StepTimeoutError, TimeoutError, StepExecutionError)):
        return True

    return True


__all__ = [
    "RetryPolicy",
    "is_retryable_exception",
]
