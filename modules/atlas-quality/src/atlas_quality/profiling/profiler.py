"""Reusable, optional dataset profiling for data quality assessments."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field

from atlas_quality.exceptions import QualityProfilingError


def _infer_series_logical_type(series: pd.Series) -> str:
    dtype = series.dtype
    if pd.api.types.is_bool_dtype(dtype):
        return "boolean"
    if pd.api.types.is_integer_dtype(dtype):
        return "integer"
    if pd.api.types.is_float_dtype(dtype):
        return "float"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "datetime"

    non_null = series.dropna()
    if len(non_null) == 0:
        return "unknown"

    sample = non_null.iloc[:100]
    if all(isinstance(v, bool) for v in sample):
        return "boolean"
    if all(isinstance(v, int) and not isinstance(v, bool) for v in sample):
        return "integer"
    if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in sample):
        return "float"
    if all(isinstance(v, str) for v in sample):
        return "string"
    return "string"


class ColumnProfile(BaseModel):
    """Statistical profile summary for a single dataset column."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    column_name: str
    data_type: str
    logical_type: str
    row_count: int
    null_count: int
    non_null_count: int
    null_percentage: float
    distinct_count: int
    duplicate_count: int
    min_value: float | str | None = None
    max_value: float | str | None = None
    mean_value: float | None = None
    std_value: float | None = None


class DatasetProfile(BaseModel):
    """Immutable dataset-level and column-level statistical profile."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    row_count: int
    column_count: int
    column_names: tuple[str, ...]
    duplicate_row_count: int = 0
    columns: dict[str, ColumnProfile] = Field(default_factory=dict)
    profiled_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class DatasetProfiler:
    """Computes column and dataset statistical profiles using vectorized operations."""

    def profile(self, data: Any) -> DatasetProfile:
        """Compute a :class:`DatasetProfile` for ``data`` without mutating the input."""
        if data is None:
            raise QualityProfilingError("Cannot profile a None dataset.")

        try:
            df = self._to_dataframe(data)
            row_count = len(df)
            col_names = tuple(str(c) for c in df.columns)
            col_count = len(col_names)

            dup_rows = int(df.duplicated().sum()) if row_count > 0 and col_count > 0 else 0
            col_profiles: dict[str, ColumnProfile] = {}

            for col in col_names:
                series = df[col]
                null_count = int(series.isna().sum())
                non_null_count = row_count - null_count
                null_pct = round((null_count / row_count) * 100.0, 4) if row_count > 0 else 0.0
                try:
                    distinct_count = int(series.nunique(dropna=True))
                except TypeError:
                    distinct_count = int(series.astype(str).nunique(dropna=True))

                dup_count = max(0, non_null_count - distinct_count)
                logical_type = _infer_series_logical_type(series)

                min_val: float | str | None = None
                max_val: float | str | None = None
                mean_val: float | None = None
                std_val: float | None = None

                if non_null_count > 0 and logical_type in {"integer", "float"}:
                    numeric_series = pd.to_numeric(series.dropna(), errors="coerce").dropna()
                    if len(numeric_series) > 0:
                        min_val = float(numeric_series.min())
                        max_val = float(numeric_series.max())
                        mean_val = round(float(numeric_series.mean()), 6)
                        if len(numeric_series) > 1:
                            std_val = round(float(numeric_series.std()), 6)
                elif non_null_count > 0 and logical_type in {"string", "datetime"}:
                    str_series = series.dropna().astype(str)
                    if len(str_series) > 0:
                        min_val = str(str_series.min())
                        max_val = str(str_series.max())

                col_profiles[col] = ColumnProfile(
                    column_name=col,
                    data_type=str(series.dtype),
                    logical_type=logical_type,
                    row_count=row_count,
                    null_count=null_count,
                    non_null_count=non_null_count,
                    null_percentage=null_pct,
                    distinct_count=distinct_count,
                    duplicate_count=dup_count,
                    min_value=min_val,
                    max_value=max_val,
                    mean_value=mean_val,
                    std_value=std_val,
                )

            return DatasetProfile(
                row_count=row_count,
                column_count=col_count,
                column_names=col_names,
                duplicate_row_count=dup_rows,
                columns=col_profiles,
            )
        except QualityProfilingError:
            raise
        except Exception as exc:
            raise QualityProfilingError(f"Failed to profile dataset: {exc}") from exc

    @staticmethod
    def _to_dataframe(data: Any) -> pd.DataFrame:
        if isinstance(data, pd.DataFrame):
            return data
        if isinstance(data, list):
            return pd.DataFrame(data)
        if isinstance(data, dict):
            if all(isinstance(v, (list, tuple)) for v in data.values()):
                return pd.DataFrame(data)
            return pd.DataFrame([data])
        raise QualityProfilingError(
            f"Unsupported dataset type '{type(data).__name__}' for profiling."
        )


__all__ = [
    "ColumnProfile",
    "DatasetProfile",
    "DatasetProfiler",
]
