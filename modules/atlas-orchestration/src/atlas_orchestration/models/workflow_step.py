"""WorkflowStep domain model and JSON compatibility validators."""

from __future__ import annotations

import copy
import math
from datetime import datetime
from typing import Any

from atlas_core import require_not_empty
from pydantic import BaseModel, ConfigDict, Field, model_validator

from atlas_orchestration.exceptions import (
    InvalidStepDefinitionError,
    InvalidWorkflowDefinitionError,
    InvalidWorkflowDependencyError,
    OrchestrationError,
)


def validate_json_compatible(
    value: Any,
    *,
    path: str = "value",
    error_cls: type[OrchestrationError] = InvalidWorkflowDefinitionError,
) -> Any:
    """Recursively verify that ``value`` is strictly JSON-compatible and return a deep copy."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise error_cls(f"Non-finite float at '{path}' is not JSON-compatible.")
        return value
    if isinstance(value, list):
        return [
            validate_json_compatible(item, path=f"{path}[{idx}]", error_cls=error_cls)
            for idx, item in enumerate(value)
        ]
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for k, v in value.items():
            if not isinstance(k, str):
                raise error_cls(
                    f"Dictionary key {k!r} at '{path}' must be a string for JSON compatibility."
                )
            result[k] = validate_json_compatible(v, path=f"{path}.{k}", error_cls=error_cls)
        return result
    raise error_cls(f"Value of type '{type(value).__name__}' at '{path}' is not JSON-compatible.")


def ensure_timezone_aware(
    dt: datetime | None,
    *,
    field_name: str,
    error_cls: type[Exception],
) -> datetime | None:
    """Ensure ``dt`` is timezone-aware if provided."""
    if dt is None:
        return None
    if not isinstance(dt, datetime):
        raise error_cls(f"'{field_name}' must be a datetime instance.")
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise error_cls(f"Timestamp '{field_name}' must be timezone-aware.")
    return dt


class WorkflowStep(BaseModel):
    """Immutable specification of a single logical step within a workflow."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    step_id: str
    module_target: str
    action: str
    parameters: dict[str, Any] = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except (InvalidStepDefinitionError, InvalidWorkflowDependencyError):
            raise
        except Exception as exc:
            raise InvalidStepDefinitionError(f"Invalid workflow step definition: {exc}") from exc

    @model_validator(mode="after")
    def _validate_step(self) -> WorkflowStep:
        clean_id = self.step_id.strip() if isinstance(self.step_id, str) else ""
        clean_target = self.module_target.strip() if isinstance(self.module_target, str) else ""
        clean_action = self.action.strip() if isinstance(self.action, str) else ""
        try:
            require_not_empty(clean_id, "step_id")
            require_not_empty(clean_target, "module_target")
            require_not_empty(clean_action, "action")
        except ValueError as exc:
            raise InvalidStepDefinitionError(str(exc), step_id=clean_id or None) from exc

        cleaned_deps: list[str] = []
        seen_deps: set[str] = set()
        for dep in self.depends_on:
            if not isinstance(dep, str) or not dep.strip():
                raise InvalidStepDefinitionError(
                    f"Step '{clean_id}' contains an empty dependency identifier.",
                    step_id=clean_id,
                )
            dep_id = dep.strip()
            if dep_id == clean_id:
                raise InvalidWorkflowDependencyError(
                    f"Step '{clean_id}' cannot depend on itself.",
                    step_id=clean_id,
                )
            if dep_id in seen_deps:
                raise InvalidWorkflowDependencyError(
                    f"Step '{clean_id}' contains duplicate dependency '{dep_id}'.",
                    step_id=clean_id,
                )
            seen_deps.add(dep_id)
            cleaned_deps.append(dep_id)

        validated_params = validate_json_compatible(
            self.parameters,
            path=f"step({clean_id}).parameters",
            error_cls=InvalidStepDefinitionError,
        )

        object.__setattr__(self, "step_id", clean_id)
        object.__setattr__(self, "module_target", clean_target)
        object.__setattr__(self, "action", clean_action)
        object.__setattr__(self, "parameters", copy.deepcopy(validated_params))
        object.__setattr__(self, "depends_on", cleaned_deps)
        return self


__all__ = [
    "WorkflowStep",
    "ensure_timezone_aware",
    "validate_json_compatible",
]
