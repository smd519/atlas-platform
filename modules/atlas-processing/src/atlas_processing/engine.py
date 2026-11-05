"""Processing execution engine abstraction and initial pandas implementation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from atlas_processing.exceptions import ProcessingExecutionError, TransformationError
from atlas_processing.transformations.aggregation import (
    AggregationFunction,
    AggregationSpec,
)
from atlas_processing.transformations.filtering import FilterCondition, FilterOperator
from atlas_processing.transformations.mapping import (
    ColumnOperation,
    DerivedColumn,
    DerivedExpressionType,
)


class ProcessingEngine(ABC):
    """Abstract interface for dataset transformation execution engines.

    Decouples declarative transformation definitions from the underlying execution
    runtime (e.g., pandas, and in future versions PySpark or DuckDB).
    """

    @property
    @abstractmethod
    def engine_name(self) -> str:
        """Return the identifier of this execution engine."""

    @abstractmethod
    def to_dataset(self, data: Any) -> Any:
        """Coerce raw input ``data`` into the engine's internal dataset representation."""

    @abstractmethod
    def row_count(self, dataset: Any) -> int:
        """Return the number of rows in ``dataset``."""

    @abstractmethod
    def columns(self, dataset: Any) -> list[str]:
        """Return the ordered list of column names in ``dataset``."""

    @abstractmethod
    def apply_filter(
        self,
        dataset: Any,
        *,
        conditions: list[FilterCondition],
        logical_operator: str = "and",
    ) -> Any:
        """Execute row-filtering ``conditions`` against ``dataset``."""

    @abstractmethod
    def apply_mapping(
        self,
        dataset: Any,
        *,
        rename_columns: dict[str, str],
        select_columns: list[str],
        drop_columns: list[str],
        derived_columns: list[DerivedColumn],
        operations: dict[str, ColumnOperation],
        value_mappings: dict[str, dict[Any, Any]],
    ) -> Any:
        """Execute column mapping, projection, and derived column operations on ``dataset``."""

    @abstractmethod
    def apply_aggregation(
        self,
        dataset: Any,
        *,
        group_by: list[str],
        aggregations: list[AggregationSpec],
    ) -> Any:
        """Execute grouping and aggregation specifications on ``dataset``."""


class PandasProcessingEngine(ProcessingEngine):
    """In-memory processing engine backed by :mod:`pandas`."""

    @property
    def engine_name(self) -> str:
        return "pandas"

    def to_dataset(self, data: Any) -> pd.DataFrame:
        if isinstance(data, pd.DataFrame):
            return data.copy()
        if isinstance(data, list):
            return pd.DataFrame(data)
        if isinstance(data, dict):
            return pd.DataFrame([data])
        raise ProcessingExecutionError(
            f"PandasProcessingEngine cannot convert input of type '{type(data).__name__}' "
            "to DataFrame."
        )

    def row_count(self, dataset: Any) -> int:
        df = self.to_dataset(dataset)
        return len(df)

    def columns(self, dataset: Any) -> list[str]:
        df = self.to_dataset(dataset)
        return [str(col) for col in df.columns]

    def apply_filter(
        self,
        dataset: Any,
        *,
        conditions: list[FilterCondition],
        logical_operator: str = "and",
    ) -> pd.DataFrame:
        df = self.to_dataset(dataset)
        if not conditions:
            return df

        masks: list[pd.Series] = []
        for cond in conditions:
            self._require_columns(df, [cond.column], context="filter")
            series = df[cond.column]
            mask = self._evaluate_condition(series, cond)
            masks.append(mask)

        combined = masks[0]
        for next_mask in masks[1:]:
            if logical_operator == "or":
                combined = combined | next_mask
            else:
                combined = combined & next_mask

        return df.loc[combined].reset_index(drop=True)

    def apply_mapping(
        self,
        dataset: Any,
        *,
        rename_columns: dict[str, str],
        select_columns: list[str],
        drop_columns: list[str],
        derived_columns: list[DerivedColumn],
        operations: dict[str, ColumnOperation],
        value_mappings: dict[str, dict[Any, Any]],
    ) -> pd.DataFrame:
        df = self.to_dataset(dataset)

        if rename_columns:
            self._require_columns(df, list(rename_columns.keys()), context="rename_columns")
            df = df.rename(columns=rename_columns)

        for derived in derived_columns:
            self._require_columns(
                df,
                derived.source_columns,
                context=f"derived_column '{derived.target_column}'",
            )
            df[derived.target_column] = self._compute_derived_column(df, derived)

        for col, mapping_dict in value_mappings.items():
            self._require_columns(df, [col], context="value_mappings")
            df[col] = df[col].replace(mapping_dict)

        for col, op in operations.items():
            self._require_columns(df, [col], context=f"operation '{op}'")
            df[col] = self._apply_column_operation(df[col], op, col)

        if drop_columns:
            self._require_columns(df, drop_columns, context="drop_columns")
            df = df.drop(columns=drop_columns)

        if select_columns:
            self._require_columns(df, select_columns, context="select_columns")
            df = df[select_columns].copy()

        return df.reset_index(drop=True)

    def apply_aggregation(
        self,
        dataset: Any,
        *,
        group_by: list[str],
        aggregations: list[AggregationSpec],
    ) -> pd.DataFrame:
        df = self.to_dataset(dataset)
        self._require_columns(df, group_by, context="group_by")
        self._require_columns(
            df,
            [spec.column for spec in aggregations],
            context="aggregations",
        )

        fn_map: dict[AggregationFunction, str] = {
            AggregationFunction.COUNT: "count",
            AggregationFunction.SUM: "sum",
            AggregationFunction.AVG: "mean",
            AggregationFunction.MIN: "min",
            AggregationFunction.MAX: "max",
        }

        try:
            if group_by:
                named_aggs = {
                    spec.output_column: pd.NamedAgg(
                        column=spec.column,
                        aggfunc=fn_map[spec.function],
                    )
                    for spec in aggregations
                }
                grouped = df.groupby(group_by, as_index=False, dropna=False)
                return grouped.agg(**named_aggs)

            summary_row: dict[str, Any] = {}
            for spec in aggregations:
                pandas_fn = fn_map[spec.function]
                summary_row[spec.output_column] = getattr(df[spec.column], pandas_fn)()
            return pd.DataFrame([summary_row])
        except TransformationError:
            raise
        except Exception as exc:
            raise TransformationError(f"Failed to execute aggregation: {exc}") from exc

    @staticmethod
    def _require_columns(
        df: pd.DataFrame,
        columns: list[str],
        *,
        context: str,
    ) -> None:
        missing = [col for col in columns if col not in df.columns]
        if missing:
            raise TransformationError(
                f"Missing required column(s) {missing} during {context}. "
                f"Available columns: {list(df.columns)}"
            )

    @staticmethod
    def _evaluate_condition(series: pd.Series, cond: FilterCondition) -> pd.Series:
        try:
            op = cond.operator
            val = cond.value
            if op == FilterOperator.EQ:
                return series == val
            if op == FilterOperator.NE:
                return series != val
            if op == FilterOperator.GT:
                return series > val
            if op == FilterOperator.GTE:
                return series >= val
            if op == FilterOperator.LT:
                return series < val
            if op == FilterOperator.LTE:
                return series <= val
            if op == FilterOperator.IS_NULL:
                return series.isna()
            if op == FilterOperator.NOT_NULL:
                return series.notna()
            if op == FilterOperator.IN:
                return series.isin(list(val))
            if op == FilterOperator.NOT_IN:
                return ~series.isin(list(val))
        except Exception as exc:
            raise TransformationError(
                f"Failed evaluating filter '{cond.operator}' on column '{cond.column}': {exc}"
            ) from exc
        raise TransformationError(f"Unsupported filter operator: '{cond.operator}'.")

    @staticmethod
    def _compute_derived_column(df: pd.DataFrame, spec: DerivedColumn) -> Any:
        try:
            expr = spec.expression
            cols = spec.source_columns
            if expr == DerivedExpressionType.CONSTANT:
                return spec.value
            if expr == DerivedExpressionType.COPY:
                return df[cols[0]]
            if expr == DerivedExpressionType.ADD:
                return df[cols[0]] + df[cols[1]]
            if expr == DerivedExpressionType.SUBTRACT:
                return df[cols[0]] - df[cols[1]]
            if expr == DerivedExpressionType.MULTIPLY:
                return df[cols[0]] * df[cols[1]]
            if expr == DerivedExpressionType.DIVIDE:
                return df[cols[0]] / df[cols[1]]
            if expr == DerivedExpressionType.CONCAT:
                str_cols = [df[c].astype(str) for c in cols]
                result = str_cols[0]
                for next_col in str_cols[1:]:
                    result = result + spec.separator + next_col
                return result
            if expr == DerivedExpressionType.COALESCE:
                result = df[cols[0]]
                for next_col in cols[1:]:
                    result = result.combine_first(df[next_col])
                if spec.value is not None:
                    result = result.fillna(spec.value)
                return result
        except Exception as exc:
            raise TransformationError(
                f"Failed computing derived column '{spec.target_column}': {exc}"
            ) from exc
        raise TransformationError(f"Unsupported derived expression '{spec.expression}'.")

    @staticmethod
    def _apply_column_operation(
        series: pd.Series,
        op: ColumnOperation,
        column_name: str,
    ) -> pd.Series:
        try:
            if op == ColumnOperation.UPPER:
                return series.astype(str).str.upper()
            if op == ColumnOperation.LOWER:
                return series.astype(str).str.lower()
            if op == ColumnOperation.STRIP:
                return series.astype(str).str.strip()
            if op == ColumnOperation.ABS:
                return series.abs()
            if op == ColumnOperation.ROUND:
                return series.round()
            if op == ColumnOperation.TO_INT:
                return series.astype(int)
            if op == ColumnOperation.TO_FLOAT:
                return series.astype(float)
            if op == ColumnOperation.TO_STR:
                return series.astype(str)
        except Exception as exc:
            raise TransformationError(
                f"Failed applying operation '{op}' on column '{column_name}': {exc}"
            ) from exc
        raise TransformationError(f"Unsupported column operation '{op}'.")


__all__ = [
    "PandasProcessingEngine",
    "ProcessingEngine",
]
