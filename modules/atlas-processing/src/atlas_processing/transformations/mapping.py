"""Configurable column mapping, projection, and derived column transformations."""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

from atlas_processing.exceptions import TransformationError
from atlas_processing.transformations.base import Transformation

if TYPE_CHECKING:
    from atlas_processing.engine import ProcessingEngine


class DerivedExpressionType(StrEnum):
    """Supported engine-agnostic derived column expression types."""

    CONSTANT = "constant"
    COPY = "copy"
    ADD = "add"
    SUBTRACT = "subtract"
    MULTIPLY = "multiply"
    DIVIDE = "divide"
    CONCAT = "concat"
    COALESCE = "coalesce"


class ColumnOperation(StrEnum):
    """Supported unary column transformation operations."""

    UPPER = "upper"
    LOWER = "lower"
    STRIP = "strip"
    ABS = "abs"
    ROUND = "round"
    TO_INT = "to_int"
    TO_FLOAT = "to_float"
    TO_STR = "to_str"


class DerivedColumn(BaseModel):
    """Specification for creating a new or overwritten derived column."""

    model_config = ConfigDict(extra="forbid")

    target_column: str
    expression: DerivedExpressionType = DerivedExpressionType.CONSTANT
    source_columns: list[str] = Field(default_factory=list)
    value: Any = None
    separator: str = ""

    def __init__(self, **data: Any) -> None:
        raw_expr = data.get("expression", DerivedExpressionType.CONSTANT)
        if isinstance(raw_expr, str) and not isinstance(raw_expr, DerivedExpressionType):
            try:
                data["expression"] = DerivedExpressionType(raw_expr.strip().lower())
            except ValueError as exc:
                raise TransformationError(
                    f"Unsupported derived column expression '{raw_expr}'."
                ) from exc
        try:
            super().__init__(**data)
        except TransformationError:
            raise
        except Exception as exc:
            raise TransformationError(f"Invalid derived column specification: {exc}") from exc


class MapTransformation(Transformation):
    """Configurable column transformation for rename, select, drop, derived, and unary ops."""

    def __init__(
        self,
        *,
        rename_columns: dict[str, str] | None = None,
        select_columns: list[str] | None = None,
        drop_columns: list[str] | None = None,
        derived_columns: list[DerivedColumn | dict[str, Any]] | None = None,
        operations: dict[str, ColumnOperation | str] | None = None,
        value_mappings: dict[str, dict[Any, Any]] | None = None,
        name: str | None = None,
    ) -> None:
        self._rename_columns = dict(rename_columns or {})
        self._select_columns = list(select_columns or [])
        self._drop_columns = list(drop_columns or [])
        self._raw_derived_columns = list(derived_columns or [])
        self._raw_operations = dict(operations or {})
        self._value_mappings = {k: dict(v) for k, v in (value_mappings or {}).items()}
        self._derived_columns: list[DerivedColumn] = []
        self._operations: dict[str, ColumnOperation] = {}
        super().__init__(name=name)

    @property
    def transformation_id(self) -> str:
        return "map"

    def validate_config(self) -> None:
        has_action = any(
            [
                self._rename_columns,
                self._select_columns,
                self._drop_columns,
                self._raw_derived_columns,
                self._raw_operations,
                self._value_mappings,
            ]
        )
        if not has_action:
            raise TransformationError(
                "MapTransformation requires at least one mapping configuration "
                "(rename_columns, select_columns, drop_columns, derived_columns, "
                "operations, or value_mappings)."
            )

        for old_col, new_col in self._rename_columns.items():
            if not old_col.strip() or not new_col.strip():
                raise TransformationError("Column names in 'rename_columns' cannot be empty.")

        for col in self._select_columns:
            if not isinstance(col, str) or not col.strip():
                raise TransformationError("Column names in 'select_columns' cannot be empty.")

        for col in self._drop_columns:
            if not isinstance(col, str) or not col.strip():
                raise TransformationError("Column names in 'drop_columns' cannot be empty.")

        parsed_derived: list[DerivedColumn] = []
        for item in self._raw_derived_columns:
            spec = item if isinstance(item, DerivedColumn) else DerivedColumn(**item)
            if not spec.target_column.strip():
                raise TransformationError("DerivedColumn 'target_column' cannot be empty.")
            if spec.expression == DerivedExpressionType.COPY and len(spec.source_columns) != 1:
                raise TransformationError(
                    f"DerivedColumn '{spec.target_column}' with 'copy' requires "
                    "exactly 1 source column."
                )
            if (
                spec.expression
                in {
                    DerivedExpressionType.ADD,
                    DerivedExpressionType.SUBTRACT,
                    DerivedExpressionType.MULTIPLY,
                    DerivedExpressionType.DIVIDE,
                }
                and len(spec.source_columns) != 2
            ):
                raise TransformationError(
                    f"DerivedColumn '{spec.target_column}' with '{spec.expression}' requires "
                    "exactly 2 source columns."
                )
            if (
                spec.expression
                in {
                    DerivedExpressionType.CONCAT,
                    DerivedExpressionType.COALESCE,
                }
                and len(spec.source_columns) < 1
            ):
                raise TransformationError(
                    f"DerivedColumn '{spec.target_column}' with '{spec.expression}' requires "
                    "at least 1 source column."
                )
            parsed_derived.append(spec)
        self._derived_columns = parsed_derived

        parsed_ops: dict[str, ColumnOperation] = {}
        for col, op in self._raw_operations.items():
            if not col.strip():
                raise TransformationError("Column name in 'operations' cannot be empty.")
            if isinstance(op, ColumnOperation):
                parsed_ops[col] = op
            elif isinstance(op, str):
                try:
                    parsed_ops[col] = ColumnOperation(op.strip().lower())
                except ValueError as exc:
                    raise TransformationError(
                        f"Unsupported column operation '{op}' for column '{col}'."
                    ) from exc
            else:
                raise TransformationError(
                    f"Invalid operation specification for column '{col}': {op!r}."
                )
        self._operations = parsed_ops

    def apply(self, data: Any, engine: ProcessingEngine) -> Any:
        return engine.apply_mapping(
            data,
            rename_columns=self._rename_columns,
            select_columns=self._select_columns,
            drop_columns=self._drop_columns,
            derived_columns=self._derived_columns,
            operations=self._operations,
            value_mappings=self._value_mappings,
        )


__all__ = [
    "ColumnOperation",
    "DerivedColumn",
    "DerivedExpressionType",
    "MapTransformation",
]
