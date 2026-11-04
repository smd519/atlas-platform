"""Quality rule execution engine and dataset adapters."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from time import perf_counter
from typing import Any

import pandas as pd  # type: ignore[import-untyped]

from atlas_quality.exceptions import (
    QualityEngineError,
    QualityRuleConfigurationError,
    QualityRuleExecutionError,
    UnknownQualityRuleError,
)
from atlas_quality.models.quality_result import QualityResult, RuleOutcomeStatus
from atlas_quality.models.quality_rule import (
    QualityRuleCategory,
    QualityRuleConfig,
    QualityRuleSeverity,
)
from atlas_quality.profiling.profiler import _infer_series_logical_type
from atlas_quality.rules.base import QualityDatasetAdapter, QualityRule
from atlas_quality.rules.registry import (
    QualityRuleRegistry,
    create_default_rule_registry,
)


def _normalize_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if hasattr(value, "item") and callable(value.item):
        try:
            return value.item()
        except Exception:
            return value
    return value


class PandasDatasetAdapter(QualityDatasetAdapter):
    """Dataset adapter backed by a :class:`pandas.DataFrame`."""

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df
        self._cached_rows: list[dict[str, Any]] | None = None
        self._cached_columns: dict[str, list[Any]] = {}

    @property
    def dataframe(self) -> pd.DataFrame:
        """Return the underlying DataFrame."""
        return self._df

    def row_count(self) -> int:
        return len(self._df)

    def columns(self) -> tuple[str, ...]:
        return tuple(str(c) for c in self._df.columns)

    def column_logical_type(self, column: str) -> str:
        if column not in self._df.columns:
            return "unknown"
        series = self._df[column]
        non_null = series.dropna()
        if len(non_null) > 0:
            sample = non_null.iloc[:100]
            if all(isinstance(v, (date, datetime, pd.Timestamp)) for v in sample):
                return "datetime"
        return _infer_series_logical_type(series)

    def column_values(self, column: str) -> Sequence[Any]:
        if column not in self._cached_columns:
            raw_list = self._df[column].tolist()
            self._cached_columns[column] = [_normalize_scalar(v) for v in raw_list]
        return self._cached_columns[column]

    def rows_as_dicts(self) -> Sequence[dict[str, Any]]:
        if self._cached_rows is None:
            cols = self.columns()
            col_lists = {c: self.column_values(c) for c in cols}
            total = self.row_count()
            self._cached_rows = [{c: col_lists[c][i] for c in cols} for i in range(total)]
        return self._cached_rows


def coerce_dataset_adapter(data: Any) -> QualityDatasetAdapter:
    """Coerce supported dataset inputs into a :class:`QualityDatasetAdapter`."""
    if isinstance(data, QualityDatasetAdapter):
        return data
    if data is None:
        raise QualityEngineError("Cannot assess a None dataset.")
    if isinstance(data, pd.DataFrame):
        return PandasDatasetAdapter(data)
    if isinstance(data, list):
        if all(isinstance(item, dict) for item in data):
            return PandasDatasetAdapter(pd.DataFrame(data))
        raise QualityEngineError("List dataset input must be a list of record dictionaries.")
    if isinstance(data, dict):
        if all(isinstance(v, (list, tuple)) for v in data.values()):
            return PandasDatasetAdapter(pd.DataFrame(data))
        return PandasDatasetAdapter(pd.DataFrame([data]))
    raise QualityEngineError(f"Unsupported dataset input type '{type(data).__name__}'.")


class QualityEngine:
    """Executes configured quality rules against a dataset and collects outcomes.

    Responsibilities:
    - Resolve and validate configured rules from a :class:`QualityRuleRegistry`.
    - Execute enabled rules sequentially (with extension hook for future parallel engines).
    - Isolate rule execution exceptions into standardized error :class:`QualityResult`
      records unless ``fail_fast=True`` is explicitly requested.
    - Never make dataset lifecycle or promotion decisions.
    """

    def __init__(self, registry: QualityRuleRegistry | None = None) -> None:
        self._registry = registry or create_default_rule_registry()

    @property
    def registry(self) -> QualityRuleRegistry:
        """Return the engine's :class:`QualityRuleRegistry`."""
        return self._registry

    def execute(
        self,
        data: Any,
        rules: Sequence[QualityRule | QualityRuleConfig | dict[str, Any]],
        *,
        reference_datasets: Mapping[str, Any] | None = None,
        fail_fast: bool = False,
    ) -> tuple[list[QualityResult], int]:
        """Execute ``rules`` against ``data`` and return ``(results, evaluated_row_count)``."""
        adapter = coerce_dataset_adapter(data)
        resolved_refs: dict[str, QualityDatasetAdapter] = {}
        if reference_datasets:
            for ref_name, ref_data in reference_datasets.items():
                resolved_refs[ref_name] = coerce_dataset_adapter(ref_data)

        results: list[QualityResult] = []
        for spec in rules:
            t0 = perf_counter()
            try:
                rule = self._registry.create(spec)
                if not rule.enabled:
                    continue
                res = rule.evaluate_dataset(adapter, reference_datasets=resolved_refs)
                elapsed = max(0.0, perf_counter() - t0)
                results.append(res.model_copy(update={"execution_duration_seconds": elapsed}))
                if fail_fast and not res.passed:
                    break
            except (UnknownQualityRuleError, QualityRuleConfigurationError) as cfg_exc:
                elapsed = max(0.0, perf_counter() - t0)
                if fail_fast:
                    raise
                results.append(self._build_error_result(spec=spec, error=cfg_exc, duration=elapsed))
            except Exception as exec_exc:
                elapsed = max(0.0, perf_counter() - t0)
                if fail_fast:
                    if isinstance(exec_exc, QualityRuleExecutionError):
                        raise
                    raise QualityRuleExecutionError(
                        f"Rule execution failed: {exec_exc}"
                    ) from exec_exc
                results.append(
                    self._build_error_result(spec=spec, error=exec_exc, duration=elapsed)
                )

        return results, adapter.row_count()

    @staticmethod
    def _build_error_result(
        *,
        spec: QualityRule | QualityRuleConfig | dict[str, Any],
        error: Exception,
        duration: float,
    ) -> QualityResult:
        if isinstance(spec, QualityRule):
            rule_id = spec.rule_id
            rule_name = spec.rule_name
            category = spec.category
            severity = spec.severity
        elif isinstance(spec, QualityRuleConfig):
            rule_id = spec.rule_id
            rule_name = spec.name or spec.rule_id
            category = spec.category or QualityRuleCategory.VALIDITY
            severity = spec.severity
        else:
            rule_id = str(spec.get("rule_id") or "unknown_rule")
            rule_name = str(spec.get("name") or rule_id)
            category = QualityRuleCategory.VALIDITY
            severity = QualityRuleSeverity.ERROR

        cause = error.__cause__
        cause_str = f"{type(cause).__name__}: {cause}" if cause is not None else None
        return QualityResult(
            rule_id=rule_id,
            rule_name=rule_name,
            category=category,
            severity=severity,
            passed=False,
            status=RuleOutcomeStatus.ERROR,
            evaluated_records=0,
            failed_records=0,
            success_percentage=0.0,
            execution_duration_seconds=duration,
            is_execution_error=True,
            error_type=type(error).__name__,
            error_message=str(error),
            error_cause=cause_str,
            message=f"Rule execution error ({type(error).__name__}): {error}",
        )


__all__ = [
    "PandasDatasetAdapter",
    "QualityEngine",
    "coerce_dataset_adapter",
]
