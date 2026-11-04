"""Completeness quality rules (null checks, non-null percentage, required fields)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from atlas_quality.exceptions import QualityRuleConfigurationError
from atlas_quality.models.quality_issue import QualityIssue
from atlas_quality.models.quality_result import QualityResult
from atlas_quality.models.quality_rule import (
    QualityRuleCategory,
    QualityRuleScope,
    QualityRuleSeverity,
)
from atlas_quality.rules.base import QualityDatasetAdapter, QualityRule


def _is_blank_or_null(value: Any, *, treat_blank_as_null: bool) -> bool:
    if value is None:
        return True
    if treat_blank_as_null and isinstance(value, str) and not value.strip():
        return True
    return False


class NotNullRule(QualityRule):
    """Verify that a column contains no null values (or meets a completeness threshold)."""

    rule_id = "completeness.not_null"
    default_category = QualityRuleCategory.COMPLETENESS
    default_severity = QualityRuleSeverity.ERROR
    default_scope = QualityRuleScope.COLUMN

    def validate_config(self) -> None:
        self._require_column_param("column")
        min_ratio = self.params.get("min_completeness", 1.0)
        if not isinstance(min_ratio, (int, float)) or not (0.0 <= float(min_ratio) <= 1.0):
            raise QualityRuleConfigurationError(
                "Parameter 'min_completeness' must be a number between 0.0 and 1.0.",
                rule_id=self.rule_id,
            )

    def evaluate_dataset(
        self,
        adapter: QualityDatasetAdapter,
        *,
        reference_datasets: Mapping[str, QualityDatasetAdapter] | None = None,
    ) -> QualityResult:
        col = self._require_column_param("column")
        self._require_columns_Exist(adapter, [col])
        min_ratio = float(self.params.get("min_completeness", 1.0))
        treat_blank = bool(self.params.get("treat_blank_as_null", False))

        values = adapter.column_values(col)
        total_rows = len(values)
        if total_rows == 0:
            return self._build_result(
                passed=True,
                evaluated_records=0,
                failed_records=0,
            )

        null_indices = [
            idx
            for idx, v in enumerate(values)
            if _is_blank_or_null(v, treat_blank_as_null=treat_blank)
        ]
        null_count = len(null_indices)
        non_null_ratio = (total_rows - null_count) / total_rows
        passed = non_null_ratio >= min_ratio

        issues: list[QualityIssue] = []
        if not passed:
            actual_pct_val = non_null_ratio * 100.0
            req_pct_val = min_ratio * 100.0
            issues.append(
                self._build_issue(
                    f"Column '{col}' has {null_count} null/missing record(s) "
                    f"({actual_pct_val:.2f}% complete, required >= {req_pct_val:.2f}%).",
                    affected_column=col,
                    affected_record_count=null_count,
                    record_references=null_indices,
                    sample_values=[values[i] for i in null_indices],
                    metadata={
                        "completeness_ratio": round(non_null_ratio, 4),
                        "min_completeness": min_ratio,
                    },
                )
            )

        return self._build_result(
            passed=passed,
            evaluated_records=total_rows,
            failed_records=null_count,
            issues=issues,
        )


class CompletenessThresholdRule(QualityRule):
    """Validate that a column's non-null percentage meets a configurable threshold."""

    rule_id = "completeness.threshold"
    default_category = QualityRuleCategory.COMPLETENESS
    default_severity = QualityRuleSeverity.WARNING
    default_scope = QualityRuleScope.COLUMN

    def validate_config(self) -> None:
        self._require_column_param("column")
        threshold = self.params.get("threshold", self.params.get("min_percentage"))
        if threshold is None or not isinstance(threshold, (int, float)):
            raise QualityRuleConfigurationError(
                "Rule 'completeness.threshold' requires a numeric 'threshold' (0.0-1.0 or 0-100).",
                rule_id=self.rule_id,
            )
        val = float(threshold)
        if val < 0.0 or val > 100.0:
            raise QualityRuleConfigurationError(
                f"Completeness 'threshold' must be between 0 and 100, got {val}.",
                rule_id=self.rule_id,
            )

    def evaluate_dataset(
        self,
        adapter: QualityDatasetAdapter,
        *,
        reference_datasets: Mapping[str, QualityDatasetAdapter] | None = None,
    ) -> QualityResult:
        col = self._require_column_param("column")
        self._require_columns_Exist(adapter, [col])
        raw_thresh = float(self.params.get("threshold", self.params.get("min_percentage")))
        min_pct = raw_thresh * 100.0 if raw_thresh <= 1.0 else raw_thresh
        treat_blank = bool(self.params.get("treat_blank_as_null", False))

        values = adapter.column_values(col)
        total_rows = len(values)
        if total_rows == 0:
            return self._build_result(passed=True, evaluated_records=0, failed_records=0)

        null_indices = [
            idx
            for idx, v in enumerate(values)
            if _is_blank_or_null(v, treat_blank_as_null=treat_blank)
        ]
        null_count = len(null_indices)
        actual_pct = ((total_rows - null_count) / total_rows) * 100.0
        passed = actual_pct >= min_pct

        issues: list[QualityIssue] = []
        if not passed:
            issues.append(
                self._build_issue(
                    f"Column '{col}' completeness is {actual_pct:.2f}%, "
                    f"below threshold {min_pct:.2f}%.",
                    affected_column=col,
                    affected_record_count=null_count,
                    record_references=null_indices,
                    sample_values=[values[i] for i in null_indices],
                    metadata={
                        "actual_percentage": round(actual_pct, 4),
                        "threshold_percentage": min_pct,
                    },
                )
            )

        return self._build_result(
            passed=passed,
            evaluated_records=total_rows,
            failed_records=0 if passed else null_count,
            issues=issues,
        )


class RequiredFieldsCompletenessRule(QualityRule):
    """Verify row-level completeness across a set of required fields."""

    rule_id = "completeness.required_fields"
    default_category = QualityRuleCategory.COMPLETENESS
    default_severity = QualityRuleSeverity.ERROR
    default_scope = QualityRuleScope.ROW

    def validate_config(self) -> None:
        cols = self.params.get("columns") or self.params.get("required_fields")
        if not isinstance(cols, (list, tuple)) or not cols:
            raise QualityRuleConfigurationError(
                "Rule 'completeness.required_fields' requires a non-empty list 'columns' "
                "(or 'required_fields').",
                rule_id=self.rule_id,
            )
        for col in cols:
            if not isinstance(col, str) or not col.strip():
                raise QualityRuleConfigurationError(
                    "Field names in 'columns' must be non-empty strings.",
                    rule_id=self.rule_id,
                )

    def evaluate_dataset(
        self,
        adapter: QualityDatasetAdapter,
        *,
        reference_datasets: Mapping[str, QualityDatasetAdapter] | None = None,
    ) -> QualityResult:
        raw_cols = self.params.get("columns") or self.params.get("required_fields") or []
        cols = [str(c).strip() for c in raw_cols]
        self._require_columns_Exist(adapter, cols)
        treat_blank = bool(self.params.get("treat_blank_as_null", True))

        rows = adapter.rows_as_dicts()
        total_rows = len(rows)
        if total_rows == 0:
            return self._build_result(passed=True, evaluated_records=0, failed_records=0)

        incomplete_indices: list[int] = []
        per_column_missing: dict[str, list[int]] = {c: [] for c in cols}

        for idx, row in enumerate(rows):
            row_incomplete = False
            for c in cols:
                if _is_blank_or_null(row.get(c), treat_blank_as_null=treat_blank):
                    per_column_missing[c].append(idx)
                    row_incomplete = True
            if row_incomplete:
                incomplete_indices.append(idx)

        issues: list[QualityIssue] = []
        for c, missing_idxs in per_column_missing.items():
            if missing_idxs:
                issues.append(
                    self._build_issue(
                        f"Required field '{c}' is null/blank in {len(missing_idxs)} record(s).",
                        affected_column=c,
                        affected_record_count=len(missing_idxs),
                        record_references=missing_idxs,
                    )
                )

        return self._build_result(
            passed=len(incomplete_indices) == 0,
            evaluated_records=total_rows,
            failed_records=len(incomplete_indices),
            issues=issues,
        )


__all__ = [
    "CompletenessThresholdRule",
    "NotNullRule",
    "RequiredFieldsCompletenessRule",
]
