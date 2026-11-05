"""Sequential transformation executor."""

from __future__ import annotations

from collections.abc import Sequence
from time import perf_counter
from typing import Any

from pydantic import BaseModel, Field

from atlas_processing.engine import PandasProcessingEngine, ProcessingEngine
from atlas_processing.exceptions import (
    ProcessingError,
    ProcessingExecutionError,
    TransformationError,
)
from atlas_processing.transformations.base import Transformation, TransformationStep
from atlas_processing.transformations.registry import (
    TransformationRegistry,
    create_default_transformation_registry,
)


class StepExecutionMetrics(BaseModel):
    """Execution telemetry for a single transformation step."""

    step_index: int
    transformation_id: str
    step_name: str
    input_row_count: int
    output_row_count: int
    duration_seconds: float


class ExecutionOutput(BaseModel):
    """Aggregate result returned by :class:`TransformationExecutor`."""

    data: Any
    input_row_count: int
    output_row_count: int
    input_columns: list[str] = Field(default_factory=list)
    output_columns: list[str] = Field(default_factory=list)
    steps_executed: int = 0
    step_metrics: list[StepExecutionMetrics] = Field(default_factory=list)


class TransformationExecutor:
    """Resolves and executes an ordered sequence of transformations on a dataset."""

    def __init__(
        self,
        engine: ProcessingEngine | None = None,
        registry: TransformationRegistry | None = None,
    ) -> None:
        self._engine = engine or PandasProcessingEngine()
        self._registry = registry or create_default_transformation_registry()

    @property
    def engine(self) -> ProcessingEngine:
        """Return the configured :class:`ProcessingEngine`."""
        return self._engine

    @property
    def registry(self) -> TransformationRegistry:
        """Return the configured :class:`TransformationRegistry`."""
        return self._registry

    def execute(
        self,
        data: Any,
        steps: Sequence[TransformationStep | Transformation | dict[str, Any]],
    ) -> ExecutionOutput:
        """Execute ``steps`` sequentially against ``data`` and return :class:`ExecutionOutput`."""
        if data is None:
            raise ProcessingExecutionError("Cannot execute transformations on None dataset.")

        try:
            current_data = self._engine.to_dataset(data)
            initial_rows = self._engine.row_count(current_data)
            initial_cols = self._engine.columns(current_data)
        except ProcessingError:
            raise
        except Exception as exc:
            raise ProcessingExecutionError(
                f"Failed to initialize dataset in engine '{self._engine.engine_name}': {exc}"
            ) from exc

        resolved_transformations = [self._registry.create(step) for step in steps]
        step_metrics: list[StepExecutionMetrics] = []

        for idx, transformation in enumerate(resolved_transformations):
            step_in_rows = self._engine.row_count(current_data)
            t0 = perf_counter()
            try:
                current_data = transformation.apply(current_data, self._engine)
            except TransformationError:
                raise
            except ProcessingError:
                raise
            except Exception as exc:
                raise TransformationError(
                    f"Transformation step {idx} ('{transformation.name}') failed: {exc}"
                ) from exc
            elapsed = max(0.0, perf_counter() - t0)
            step_out_rows = self._engine.row_count(current_data)
            step_metrics.append(
                StepExecutionMetrics(
                    step_index=idx,
                    transformation_id=transformation.transformation_id,
                    step_name=transformation.name,
                    input_row_count=step_in_rows,
                    output_row_count=step_out_rows,
                    duration_seconds=elapsed,
                )
            )

        final_rows = self._engine.row_count(current_data)
        final_cols = self._engine.columns(current_data)

        return ExecutionOutput(
            data=current_data,
            input_row_count=initial_rows,
            output_row_count=final_rows,
            input_columns=initial_cols,
            output_columns=final_cols,
            steps_executed=len(step_metrics),
            step_metrics=step_metrics,
        )


__all__ = [
    "ExecutionOutput",
    "StepExecutionMetrics",
    "TransformationExecutor",
]
