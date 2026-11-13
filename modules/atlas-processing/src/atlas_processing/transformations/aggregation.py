"""Configurable grouping and aggregation transformations."""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict

from atlas_processing.exceptions import TransformationError
from atlas_processing.transformations.base import Transformation

if TYPE_CHECKING:
    from atlas_processing.engine import ProcessingEngine


class AggregationFunction(StrEnum):
    """Supported aggregation functions."""

    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"


_AGG_ALIASES: dict[str, AggregationFunction] = {
    "count": AggregationFunction.COUNT,
    "sum": AggregationFunction.SUM,
    "avg": AggregationFunction.AVG,
    "mean": AggregationFunction.AVG,
    "average": AggregationFunction.AVG,
    "min": AggregationFunction.MIN,
    "minimum": AggregationFunction.MIN,
    "max": AggregationFunction.MAX,
    "maximum": AggregationFunction.MAX,
}


class AggregationSpec(BaseModel):
    """Specification for aggregating a single column."""

    model_config = ConfigDict(extra="forbid")

    column: str
    function: AggregationFunction
    alias: str | None = None

    def __init__(self, **data: Any) -> None:
        raw_fn = data.get("function")
        if isinstance(raw_fn, str) and not isinstance(raw_fn, AggregationFunction):
            resolved = _AGG_ALIASES.get(raw_fn.strip().lower())
            if resolved is None:
                raise TransformationError(f"Unsupported aggregation function: '{raw_fn}'.")
            data["function"] = resolved
        try:
            super().__init__(**data)
        except TransformationError:
            raise
        except Exception as exc:
            raise TransformationError(f"Invalid aggregation specification: {exc}") from exc

    @property
    def output_column(self) -> str:
        """Resolved output column name for this aggregation."""
        if self.alias and self.alias.strip():
            return self.alias.strip()
        return f"{self.column}_{self.function.value}"


class AggregateTransformation(Transformation):
    """Configurable grouping and aggregation transformation."""

    def __init__(
        self,
        *,
        group_by: list[str] | str | None = None,
        aggregations: list[AggregationSpec | dict[str, Any]] | dict[str, str] | None = None,
        name: str | None = None,
    ) -> None:
        if isinstance(group_by, str):
            self._group_by = [group_by]
        else:
            self._group_by = list(group_by or [])
        self._raw_aggregations = aggregations
        self._aggregations: list[AggregationSpec] = []
        super().__init__(name=name)

    @property
    def transformation_id(self) -> str:
        return "aggregate"

    @property
    def group_by(self) -> tuple[str, ...]:
        """Return the grouping columns."""
        return tuple(self._group_by)

    @property
    def aggregations(self) -> tuple[AggregationSpec, ...]:
        """Return the validated aggregation specifications."""
        return tuple(self._aggregations)

    def validate_config(self) -> None:
        cleaned_groups: list[str] = []
        for col in self._group_by:
            if not isinstance(col, str) or not col.strip():
                raise TransformationError("Column names in 'group_by' cannot be empty.")
            cleaned_groups.append(col.strip())
        self._group_by = cleaned_groups

        parsed_specs: list[AggregationSpec] = []
        if isinstance(self._raw_aggregations, dict):
            for col, fn in self._raw_aggregations.items():
                parsed_specs.append(AggregationSpec(column=col, function=fn))
        elif isinstance(self._raw_aggregations, list):
            for item in self._raw_aggregations:
                if isinstance(item, AggregationSpec):
                    parsed_specs.append(item)
                elif isinstance(item, dict):
                    parsed_specs.append(AggregationSpec(**item))
                else:
                    raise TransformationError(f"Invalid aggregation specification: {item!r}.")

        if not parsed_specs:
            raise TransformationError(
                "AggregateTransformation requires at least one aggregation specification."
            )

        for spec in parsed_specs:
            col = spec.column.strip() if isinstance(spec.column, str) else ""
            if not col:
                raise TransformationError("Aggregation 'column' cannot be empty.")
            spec.column = col

        self._aggregations = parsed_specs

    def apply(self, data: Any, engine: ProcessingEngine) -> Any:
        return engine.apply_aggregation(
            data,
            group_by=self._group_by,
            aggregations=self._aggregations,
        )


__all__ = [
    "AggregateTransformation",
    "AggregationFunction",
    "AggregationSpec",
]
