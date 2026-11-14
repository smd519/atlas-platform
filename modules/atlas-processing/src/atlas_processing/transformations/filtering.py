"""Configurable row filtering transformations."""

from __future__ import annotations

from collections.abc import Iterable
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict

from atlas_processing.exceptions import TransformationError
from atlas_processing.transformations.base import Transformation

if TYPE_CHECKING:
    from atlas_processing.engine import ProcessingEngine


class FilterOperator(StrEnum):
    """Supported comparison and null-check operators for row filtering."""

    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    IS_NULL = "is_null"
    NOT_NULL = "not_null"
    IN = "in"
    NOT_IN = "not_in"


_OPERATOR_ALIASES: dict[str, FilterOperator] = {
    "==": FilterOperator.EQ,
    "=": FilterOperator.EQ,
    "eq": FilterOperator.EQ,
    "!=": FilterOperator.NE,
    "ne": FilterOperator.NE,
    ">": FilterOperator.GT,
    "gt": FilterOperator.GT,
    ">=": FilterOperator.GTE,
    "gte": FilterOperator.GTE,
    "<": FilterOperator.LT,
    "lt": FilterOperator.LT,
    "<=": FilterOperator.LTE,
    "lte": FilterOperator.LTE,
    "is_null": FilterOperator.IS_NULL,
    "isnull": FilterOperator.IS_NULL,
    "not_null": FilterOperator.NOT_NULL,
    "notnull": FilterOperator.NOT_NULL,
    "in": FilterOperator.IN,
    "not_in": FilterOperator.NOT_IN,
}

_NULL_OPERATORS: frozenset[FilterOperator] = frozenset(
    {FilterOperator.IS_NULL, FilterOperator.NOT_NULL}
)


class FilterCondition(BaseModel):
    """Single row-filtering predicate."""

    model_config = ConfigDict(extra="forbid")

    column: str
    operator: FilterOperator
    value: Any = None

    def __init__(self, **data: Any) -> None:
        raw_op = data.get("operator")
        if isinstance(raw_op, str) and not isinstance(raw_op, FilterOperator):
            normalized = _OPERATOR_ALIASES.get(raw_op.strip().lower())
            if normalized is None:
                raise TransformationError(f"Unsupported filter operator: '{raw_op}'.")
            data["operator"] = normalized
        try:
            super().__init__(**data)
        except TransformationError:
            raise
        except Exception as exc:
            raise TransformationError(f"Invalid filter condition: {exc}") from exc


class FilterTransformation(Transformation):
    """Configurable row filtering transformation supporting single or multiple conditions."""

    def __init__(
        self,
        *,
        column: str | None = None,
        operator: FilterOperator | str | None = None,
        value: Any = None,
        conditions: list[FilterCondition | dict[str, Any]] | None = None,
        logical_operator: str = "and",
        name: str | None = None,
    ) -> None:
        self._raw_column = column
        self._raw_operator = operator
        self._raw_value = value
        self._raw_conditions = conditions
        self._logical_operator = (
            logical_operator.strip().lower() if isinstance(logical_operator, str) else ""
        )
        self._conditions: list[FilterCondition] = []
        super().__init__(name=name)

    @property
    def transformation_id(self) -> str:
        return "filter"

    @property
    def conditions(self) -> tuple[FilterCondition, ...]:
        """Return the validated filter conditions."""
        return tuple(self._conditions)

    @property
    def logical_operator(self) -> str:
        """Return the logical combiner ('and' or 'or')."""
        return self._logical_operator

    def validate_config(self) -> None:
        if self._logical_operator not in {"and", "or"}:
            raise TransformationError(
                f"Logical operator must be 'and' or 'or', got '{self._logical_operator}'."
            )

        parsed_conditions: list[FilterCondition] = []
        if self._raw_conditions is not None:
            for item in self._raw_conditions:
                if isinstance(item, FilterCondition):
                    parsed_conditions.append(item)
                elif isinstance(item, dict):
                    parsed_conditions.append(FilterCondition(**item))
                else:
                    raise TransformationError(f"Invalid filter condition specification: {item!r}.")

        if self._raw_column is not None or self._raw_operator is not None:
            if self._raw_column is None or self._raw_operator is None:
                raise TransformationError(
                    "Single filter condition requires both 'column' and 'operator'."
                )
            parsed_conditions.append(
                FilterCondition(
                    column=self._raw_column,
                    operator=self._raw_operator,
                    value=self._raw_value,
                )
            )

        if not parsed_conditions:
            raise TransformationError(
                "FilterTransformation requires at least one filter condition."
            )

        for cond in parsed_conditions:
            col = cond.column.strip() if isinstance(cond.column, str) else ""
            if not col:
                raise TransformationError("Filter condition 'column' cannot be empty.")
            cond.column = col

            if cond.operator not in _NULL_OPERATORS and cond.value is None:
                raise TransformationError(
                    f"Filter operator '{cond.operator}' on column '{col}' requires a value."
                )
            if cond.operator in {FilterOperator.IN, FilterOperator.NOT_IN} and (
                isinstance(cond.value, (str, bytes)) or not isinstance(cond.value, Iterable)
            ):
                raise TransformationError(
                    f"Filter operator '{cond.operator}' on column '{col}' requires an iterable "
                    "collection of values."
                )

        self._conditions = parsed_conditions

    def apply(self, data: Any, engine: ProcessingEngine) -> Any:
        return engine.apply_filter(
            data,
            conditions=self._conditions,
            logical_operator=self._logical_operator,
        )


__all__ = [
    "FilterCondition",
    "FilterOperator",
    "FilterTransformation",
]
