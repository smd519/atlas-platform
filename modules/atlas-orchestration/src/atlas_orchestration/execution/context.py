"""Isolated execution context with separated variable, step-output, and metadata namespaces."""

from __future__ import annotations

import copy
import json
import threading
from datetime import UTC, datetime
from typing import Any

from atlas_core import generate_execution_id, require_not_empty

from atlas_orchestration.exceptions import ExecutionContextError
from atlas_orchestration.models.workflow_step import (
    ensure_timezone_aware,
    validate_json_compatible,
)

_MISSING = object()


class ExecutionContext:
    """Isolated runtime execution context with three strictly separated namespaces.

    Namespaces
    ----------
    1. ``workflow_variables``: Workflow-scoped input parameters and runtime variables.
    2. ``step_outputs``: Step-scoped output dictionaries keyed by ``step_id``.
    3. ``execution_metadata``: Runtime execution metadata (IDs, correlation ID, timestamps).
    """

    def __init__(
        self,
        *,
        workflow_id: str,
        workflow_version: int = 1,
        execution_id: str | None = None,
        workflow_variables: dict[str, Any] | None = None,
        step_outputs: dict[str, dict[str, Any]] | None = None,
        execution_metadata: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        created_at: datetime | None = None,
    ) -> None:
        clean_wf_id = workflow_id.strip() if isinstance(workflow_id, str) else ""
        try:
            require_not_empty(clean_wf_id, "workflow_id")
        except ValueError as exc:
            raise ExecutionContextError(str(exc)) from exc

        if (
            isinstance(workflow_version, bool)
            or not isinstance(workflow_version, int)
            or (workflow_version < 1)
        ):
            raise ExecutionContextError(
                f"workflow_version must be an integer >= 1, got {workflow_version!r}.",
                workflow_id=clean_wf_id,
            )

        raw_exec_id = (
            execution_id.strip()
            if isinstance(execution_id, str) and execution_id.strip()
            else (generate_execution_id() if execution_id is None else "")
        )
        if not raw_exec_id:
            raise ExecutionContextError(
                "execution_id must be a non-empty string.",
                workflow_id=clean_wf_id,
                workflow_version=workflow_version,
            )

        ts = created_at if created_at is not None else datetime.now(UTC)
        ensure_timezone_aware(
            ts,
            field_name="created_at",
            error_cls=ExecutionContextError,
        )

        clean_corr_id: str | None = None
        if correlation_id is not None:
            if not isinstance(correlation_id, str) or not correlation_id.strip():
                raise ExecutionContextError(
                    "correlation_id must be a non-empty string when provided.",
                    workflow_id=clean_wf_id,
                    workflow_version=workflow_version,
                    execution_id=raw_exec_id,
                )
            clean_corr_id = correlation_id.strip()

        self._lock = threading.RLock()
        self._execution_id = raw_exec_id
        self._workflow_id = clean_wf_id
        self._workflow_version = workflow_version
        self._correlation_id = clean_corr_id
        self._created_at = ts

        self._workflow_variables: dict[str, Any] = {}
        if workflow_variables is not None:
            if not isinstance(workflow_variables, dict):
                raise ExecutionContextError(
                    "workflow_variables must be a dictionary.",
                    workflow_id=clean_wf_id,
                    workflow_version=workflow_version,
                    execution_id=raw_exec_id,
                )
            for k, v in workflow_variables.items():
                self.set_variable(k, v)

        self._step_outputs: dict[str, dict[str, Any]] = {}
        if step_outputs is not None:
            if not isinstance(step_outputs, dict):
                raise ExecutionContextError(
                    "step_outputs must be a dictionary mapping step_id to dict.",
                    workflow_id=clean_wf_id,
                    workflow_version=workflow_version,
                    execution_id=raw_exec_id,
                )
            for s_id, out in step_outputs.items():
                self.set_step_output(s_id, out)

        self._execution_metadata: dict[str, Any] = {}
        if execution_metadata is not None:
            if not isinstance(execution_metadata, dict):
                raise ExecutionContextError(
                    "execution_metadata must be a dictionary.",
                    workflow_id=clean_wf_id,
                    workflow_version=workflow_version,
                    execution_id=raw_exec_id,
                )
            for k, v in execution_metadata.items():
                self.set_metadata(k, v)

        # Ensure standard system metadata fields are populated
        self._execution_metadata.update(
            {
                "execution_id": self._execution_id,
                "workflow_id": self._workflow_id,
                "workflow_version": self._workflow_version,
                "created_at": self._created_at.isoformat(),
                "correlation_id": self._correlation_id,
            }
        )

    @property
    def execution_id(self) -> str:
        """Return the unique execution identifier."""
        return self._execution_id

    @property
    def workflow_id(self) -> str:
        """Return the workflow identifier."""
        return self._workflow_id

    @property
    def workflow_version(self) -> int:
        """Return the workflow version."""
        return self._workflow_version

    @property
    def correlation_id(self) -> str | None:
        """Return the optional correlation identifier."""
        return self._correlation_id

    @property
    def created_at(self) -> datetime:
        """Return the UTC creation timestamp of this context."""
        return self._created_at

    @property
    def workflow_variables(self) -> dict[str, Any]:
        """Return an isolated deep copy of all workflow variables."""
        with self._lock:
            return copy.deepcopy(self._workflow_variables)

    @property
    def step_outputs(self) -> dict[str, dict[str, Any]]:
        """Return an isolated deep copy of all recorded step outputs."""
        with self._lock:
            return copy.deepcopy(self._step_outputs)

    @property
    def execution_metadata(self) -> dict[str, Any]:
        """Return an isolated deep copy of all execution metadata."""
        with self._lock:
            return copy.deepcopy(self._execution_metadata)

    # ------------------------------------------------------------------
    # Namespace 1: Workflow Variables
    # ------------------------------------------------------------------

    def set_variable(self, key: str, value: Any) -> None:
        """Set a JSON-compatible workflow variable inside the ``workflow_variables`` namespace."""
        clean_key = self._validate_key(key, "variable key")
        validated_val = validate_json_compatible(
            value,
            path=f"workflow_variables.{clean_key}",
            error_cls=ExecutionContextError,
        )
        with self._lock:
            self._workflow_variables[clean_key] = copy.deepcopy(validated_val)

    def get_variable(self, key: str, default: Any = _MISSING) -> Any:
        """Retrieve a deep copy of a workflow variable by ``key``.

        Raises
        ------
        ExecutionContextError
            If ``key`` does not exist and no ``default`` is provided.
        """
        clean_key = self._validate_key(key, "variable key")
        with self._lock:
            if clean_key in self._workflow_variables:
                return copy.deepcopy(self._workflow_variables[clean_key])
            if default is not _MISSING:
                return copy.deepcopy(default)
            raise ExecutionContextError(
                f"Workflow variable '{clean_key}' not found in execution context.",
                workflow_id=self._workflow_id,
                workflow_version=self._workflow_version,
                execution_id=self._execution_id,
            )

    def has_variable(self, key: str) -> bool:
        """Return ``True`` if ``key`` exists in ``workflow_variables``."""
        clean_key = key.strip() if isinstance(key, str) else ""
        with self._lock:
            return clean_key in self._workflow_variables

    # ------------------------------------------------------------------
    # Namespace 2: Step Outputs
    # ------------------------------------------------------------------

    def set_step_output(self, step_id: str, output: dict[str, Any]) -> None:
        """Store a JSON-compatible output dictionary for ``step_id`` in ``step_outputs``."""
        clean_step_id = self._validate_key(step_id, "step_id")
        if not isinstance(output, dict):
            raise ExecutionContextError(
                f"Output for step '{clean_step_id}' must be a dictionary, "
                f"got {type(output).__name__}.",
                workflow_id=self._workflow_id,
                workflow_version=self._workflow_version,
                execution_id=self._execution_id,
                step_id=clean_step_id,
            )
        validated_output = validate_json_compatible(
            output,
            path=f"step_outputs.{clean_step_id}",
            error_cls=ExecutionContextError,
        )
        with self._lock:
            self._step_outputs[clean_step_id] = copy.deepcopy(validated_output)

    def get_step_output(self, step_id: str) -> dict[str, Any]:
        """Return a deep copy of the output dictionary for ``step_id``.

        Raises
        ------
        ExecutionContextError
            If no output has been recorded for ``step_id``.
        """
        clean_step_id = self._validate_key(step_id, "step_id")
        with self._lock:
            if clean_step_id not in self._step_outputs:
                raise ExecutionContextError(
                    f"No output recorded for step '{clean_step_id}'.",
                    workflow_id=self._workflow_id,
                    workflow_version=self._workflow_version,
                    execution_id=self._execution_id,
                    step_id=clean_step_id,
                )
            return copy.deepcopy(self._step_outputs[clean_step_id])

    def get_step_output_value(
        self,
        step_id: str,
        key: str,
        default: Any = _MISSING,
    ) -> Any:
        """Return a specific value from ``step_id``'s output dictionary."""
        clean_step_id = self._validate_key(step_id, "step_id")
        clean_key = self._validate_key(key, "output key")
        with self._lock:
            if clean_step_id not in self._step_outputs:
                if default is not _MISSING:
                    return copy.deepcopy(default)
                raise ExecutionContextError(
                    f"No output recorded for step '{clean_step_id}'.",
                    workflow_id=self._workflow_id,
                    workflow_version=self._workflow_version,
                    execution_id=self._execution_id,
                    step_id=clean_step_id,
                )
            step_dict = self._step_outputs[clean_step_id]
            if clean_key in step_dict:
                return copy.deepcopy(step_dict[clean_key])
            if default is not _MISSING:
                return copy.deepcopy(default)
            raise ExecutionContextError(
                f"Output key '{clean_key}' not found in output of step '{clean_step_id}'.",
                workflow_id=self._workflow_id,
                workflow_version=self._workflow_version,
                execution_id=self._execution_id,
                step_id=clean_step_id,
            )

    def has_step_output(self, step_id: str) -> bool:
        """Return ``True`` if ``step_id`` has recorded output in ``step_outputs``."""
        clean_step_id = step_id.strip() if isinstance(step_id, str) else ""
        with self._lock:
            return clean_step_id in self._step_outputs

    # ------------------------------------------------------------------
    # Namespace 3: Execution Metadata
    # ------------------------------------------------------------------

    def set_metadata(self, key: str, value: Any) -> None:
        """Set a JSON-compatible metadata entry in ``execution_metadata``."""
        clean_key = self._validate_key(key, "metadata key")
        protected = {"execution_id", "workflow_id", "workflow_version", "created_at"}
        if clean_key in protected:
            raise ExecutionContextError(
                f"Protected system metadata key '{clean_key}' cannot be overwritten.",
                workflow_id=self._workflow_id,
                workflow_version=self._workflow_version,
                execution_id=self._execution_id,
            )
        validated_val = validate_json_compatible(
            value,
            path=f"execution_metadata.{clean_key}",
            error_cls=ExecutionContextError,
        )
        with self._lock:
            self._execution_metadata[clean_key] = copy.deepcopy(validated_val)

    def get_metadata(self, key: str, default: Any = _MISSING) -> Any:
        """Retrieve a deep copy of a metadata entry from ``execution_metadata``."""
        clean_key = self._validate_key(key, "metadata key")
        with self._lock:
            if clean_key in self._execution_metadata:
                return copy.deepcopy(self._execution_metadata[clean_key])
            if default is not _MISSING:
                return copy.deepcopy(default)
            raise ExecutionContextError(
                f"Execution metadata key '{clean_key}' not found in execution context.",
                workflow_id=self._workflow_id,
                workflow_version=self._workflow_version,
                execution_id=self._execution_id,
            )

    def has_metadata(self, key: str) -> bool:
        """Return ``True`` if ``key`` exists in ``execution_metadata``."""
        clean_key = key.strip() if isinstance(key, str) else ""
        with self._lock:
            return clean_key in self._execution_metadata

    # ------------------------------------------------------------------
    # Serialization & Snapshot
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Serialize the context and all three namespaces into a JSON-compatible dictionary."""
        with self._lock:
            return {
                "execution_id": self._execution_id,
                "workflow_id": self._workflow_id,
                "workflow_version": self._workflow_version,
                "correlation_id": self._correlation_id,
                "created_at": self._created_at.isoformat(),
                "workflow_variables": copy.deepcopy(self._workflow_variables),
                "step_outputs": copy.deepcopy(self._step_outputs),
                "execution_metadata": copy.deepcopy(self._execution_metadata),
            }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ExecutionContext:
        """Reconstruct an :class:`ExecutionContext` from a serialized dictionary."""
        if not isinstance(data, dict):
            raise ExecutionContextError("Serialized context data must be a dictionary.")

        raw_created = data.get("created_at")
        parsed_created: datetime | None = None
        if isinstance(raw_created, str):
            try:
                parsed_created = datetime.fromisoformat(raw_created)
            except ValueError as exc:
                raise ExecutionContextError(
                    f"Invalid ISO-8601 timestamp for 'created_at': {raw_created!r}"
                ) from exc
        elif isinstance(raw_created, datetime):
            parsed_created = raw_created
        elif raw_created is not None:
            raise ExecutionContextError("Invalid 'created_at' value in serialized context.")

        raw_meta = data.get("execution_metadata") or {}
        if not isinstance(raw_meta, dict):
            raise ExecutionContextError("execution_metadata must be a dictionary.")
        protected = {"execution_id", "workflow_id", "workflow_version", "created_at"}
        custom_meta = {k: v for k, v in raw_meta.items() if k not in protected}

        return cls(
            execution_id=data.get("execution_id"),
            workflow_id=str(data.get("workflow_id") or ""),
            workflow_version=data.get("workflow_version", 1),
            correlation_id=data.get("correlation_id"),
            created_at=parsed_created,
            workflow_variables=data.get("workflow_variables"),
            step_outputs=data.get("step_outputs"),
            execution_metadata=custom_meta,
        )

    def to_json(self) -> str:
        """Serialize the execution context to a JSON string."""
        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_json(cls, raw: str) -> ExecutionContext:
        """Deserialize an :class:`ExecutionContext` from a JSON string."""
        try:
            parsed = json.loads(raw)
        except Exception as exc:
            raise ExecutionContextError(f"Invalid JSON for ExecutionContext: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ExecutionContextError("Serialized ExecutionContext JSON must be an object.")
        return cls.from_dict(parsed)

    def snapshot(self) -> ExecutionContext:
        """Return a deep, isolated clone of this :class:`ExecutionContext`."""
        return ExecutionContext.from_dict(self.to_dict())

    def _validate_key(self, key: str, label: str) -> str:
        if not isinstance(key, str) or not key.strip():
            raise ExecutionContextError(
                f"{label} must be a non-empty string.",
                workflow_id=self._workflow_id,
                workflow_version=self._workflow_version,
                execution_id=self._execution_id,
            )
        return key.strip()


__all__ = ["ExecutionContext"]
