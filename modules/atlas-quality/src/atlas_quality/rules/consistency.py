"""Consistency quality rules (cross-column comparison, conditional, and arithmetic)."""

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

_COMPARISON_OPERATORS: dict[str, str] = {
    "eq": "eq",
    "==": "eq",
    "=": "eq",
    "ne": "ne",
    "!=": "ne",
    "gt": "gt",
    ">": "gt",
    "gte": "gte",
    ">=": "gte",
    "lt": "lt",
    "<": "lt",
    "lte": "lte",
    "<=": "lte",
}

_CONDITION_OPERATORS: frozenset[str] = frozenset(
    {"eq", "ne", "gt", "gte", "lt", "lte", "in", "not_in", "is_null", "not_null"}
)

_ARITHMETIC_EXPRESSIONS: frozenset[str] = frozenset({"add", "subtract", "multiply", "divide"})


def _compare_values(left: Any, right: Any, operator: str) -> bool:
    try:
        if operator == "eq":
            return bool(left == right)
        if operator == "ne":
            return bool(left != right)
        if operator == "gt":
            return bool(left > right)
        if operator == "gte":
            return bool(left >= right)
        if operator == "lt":
            return bool(left < right)
        if operator == "lte":
            return bool(left <= right)
    except TypeError:
        return False
    return False


def _evaluate_predicate(val: Any, operator: str, expected: Any) -> bool:
    if operator == "is_null":
        return val is None
    if operator == "not_null":
        return val is not None and (not isinstance(val, str) or bool(val.strip()))
    if val is None:
        return False
    if operator == "in":
        return val in expected if isinstance(expected, (list, tuple, set, frozenset)) else False
    if operator == "not_in":
        return val not in expected if isinstance(expected, (list, tuple, set, frozenset)) else False
    return _compare_values(val, expected, operator)


class ColumnComparisonRule(QualityRule):
    """Validate a comparison relationship between two columns (e.g., start_date <= end_date)."""

    rule_id = "consistency.column_comparison"
    default_category = QualityRuleCategory.CONSISTENCY
    default_severity = QualityRuleSeverity.ERROR
    default_scope = QualityRuleScope.ROW

    def validate_config(self) -> None:
        left = self._require_column_param("left_column")
        right = self._require_column_param("right_column")
        if left == right:
            raise QualityRuleConfigurationError(
                "'left_column' and 'right_column' must be distinct.",
                rule_id=self.rule_id,
            )
        raw_op = str(self.params.get("operator", "")).strip().lower()
        if raw_op not in _COMPARISON_OPERATORS:
            raise QualityRuleConfigurationError(
                f"Unsupported comparison operator '{raw_op}'. "
                f"Supported: {sorted(set(_COMPARISON_OPERATORS.values()))}",
                rule_id=self.rule_id,
            )

    def evaluate_dataset(
        self,
        adapter: QualityDatasetAdapter,
        *,
        reference_datasets: Mapping[str, QualityDatasetAdapter] | None = None,
    ) -> QualityResult:
        left_col = self._require_column_param("left_column")
        right_col = self._require_column_param("right_column")
        self._require_columns_Exist(adapter, [left_col, right_col])
        op = _COMPARISON_OPERATORS[str(self.params["operator"]).strip().lower()]
        allow_null = bool(self.params.get("allow_null", True))

        rows = adapter.rows_as_dicts()
        total_rows = len(rows)
        if total_rows == 0:
            return self._build_result(passed=True, evaluated_records=0, failed_records=0)

        invalid_indices: list[int] = []
        invalid_samples: list[Any] = []
        for idx, row in enumerate(rows):
            l_val = row.get(left_col)
            r_val = row.get(right_col)
            if l_val is None or r_val is None:
                if not allow_null:
                    invalid_indices.append(idx)
                    invalid_samples.append({left_col: l_val, right_col: r_val})
                continue
            if not _compare_values(l_val, r_val, op):
                invalid_indices.append(idx)
                invalid_samples.append({left_col: l_val, right_col: r_val})

        issues: list[QualityIssue] = []
        if invalid_indices:
            issues.append(
                self._build_issue(
                    f"Cross-column comparison '{left_col} {op} {right_col}' failed for "
                    f"{len(invalid_indices)} record(s).",
                    affected_columns=[left_col, right_col],
                    affected_record_count=len(invalid_indices),
                    record_references=invalid_indices,
                    sample_values=invalid_samples,
                    metadata={
                        "left_column": left_col,
                        "right_column": right_col,
                        "operator": op,
                    },
                )
            )

        return self._build_result(
            passed=len(invalid_indices) == 0,
            evaluated_records=total_rows,
            failed_records=len(invalid_indices),
            issues=issues,
        )


class ConditionalRequirementRule(QualityRule):
    """Validate that when a condition holds on one column, a requirement holds on another column."""

    rule_id = "consistency.conditional_requirement"
    default_category = QualityRuleCategory.CONSISTENCY
    default_severity = QualityRuleSeverity.ERROR
    default_scope = QualityRuleScope.ROW

    def validate_config(self) -> None:
        self._require_column_param("when_column")
        self._require_column_param("then_column")
        when_op = str(self.params.get("when_operator", "eq")).strip().lower()
        then_op = str(self.params.get("then_operator", "not_null")).strip().lower()
        norm_when = _COMPARISON_OPERATORS.get(when_op, when_op)
        norm_then = _COMPARISON_OPERATORS.get(then_op, then_op)
        if norm_when not in _CONDITION_OPERATORS:
            raise QualityRuleConfigurationError(
                f"Unsupported 'when_operator' '{when_op}'.",
                rule_id=self.rule_id,
            )
        if norm_then not in _CONDITION_OPERATORS:
            raise QualityRuleConfigurationError(
                f"Unsupported 'then_operator' '{then_op}'.",
                rule_id=self.rule_id,
            )

    def evaluate_dataset(
        self,
        adapter: QualityDatasetAdapter,
        *,
        reference_datasets: Mapping[str, QualityDatasetAdapter] | None = None,
    ) -> QualityResult:
        when_col = self._require_column_param("when_column")
        then_col = self._require_column_param("then_column")
        self._require_columns_Exist(adapter, [when_col, then_col])

        raw_when_op = str(self.params.get("when_operator", "eq")).strip().lower()
        raw_then_op = str(self.params.get("then_operator", "not_null")).strip().lower()
        when_op = _COMPARISON_OPERATORS.get(raw_when_op, raw_when_op)
        then_op = _COMPARISON_OPERATORS.get(raw_then_op, raw_then_op)
        when_val = self.params.get("when_value")
        then_val = self.params.get("then_value")

        rows = adapter.rows_as_dicts()
        total_rows = len(rows)
        if total_rows == 0:
            return self._build_result(passed=True, evaluated_records=0, failed_records=0)

        invalid_indices: list[int] = []
        invalid_samples: list[Any] = []
        for idx, row in enumerate(rows):
            if _evaluate_predicate(row.get(when_col), when_op, when_val):
                if not _evaluate_predicate(row.get(then_col), then_op, then_val):
                    invalid_indices.append(idx)
                    invalid_samples.append(
                        {when_col: row.get(when_col), then_col: row.get(then_col)}
                    )

        issues: list[QualityIssue] = []
        if invalid_indices:
            issues.append(
                self._build_issue(
                    f"Conditional requirement failed: when '{when_col}' ({when_op}), "
                    f"'{then_col}' must satisfy ({then_op}) ({len(invalid_indices)} record(s)).",
                    affected_columns=[when_col, then_col],
                    affected_record_count=len(invalid_indices),
                    record_references=invalid_indices,
                    sample_values=invalid_samples,
                    metadata={
                        "when_column": when_col,
                        "when_operator": when_op,
                        "then_column": then_col,
                        "then_operator": then_op,
                    },
                )
            )

        return self._build_result(
            passed=len(invalid_indices) == 0,
            evaluated_records=total_rows,
            failed_records=len(invalid_indices),
            issues=issues,
        )


class ArithmeticConsistencyRule(QualityRule):
    """Validate safe, structured arithmetic consistency across columns (without eval)."""

    rule_id = "consistency.expression"
    default_category = QualityRuleCategory.CONSISTENCY
    default_severity = QualityRuleSeverity.ERROR
    default_scope = QualityRuleScope.ROW

    def validate_config(self) -> None:
        self._require_column_param("target_column")
        expr = str(self.params.get("operation", self.params.get("expression", ""))).strip().lower()
        if expr not in _ARITHMETIC_EXPRESSIONS:
            raise QualityRuleConfigurationError(
                f"Unsupported arithmetic consistency operation '{expr}'. "
                f"Supported safe operations: {sorted(_ARITHMETIC_EXPRESSIONS)}",
                rule_id=self.rule_id,
            )
        src_cols = self.params.get("source_columns")
        if not isinstance(src_cols, (list, tuple)) or len(src_cols) < 2:
            raise QualityRuleConfigurationError(
                "Rule 'consistency.expression' requires 'source_columns' with at least 2 columns.",
                rule_id=self.rule_id,
            )
        for col in src_cols:
            if not isinstance(col, str) or not col.strip():
                raise QualityRuleConfigurationError(
                    "Column names in 'source_columns' must be non-empty strings.",
                    rule_id=self.rule_id,
                )
        tolerance = self.params.get("tolerance", 1e-6)
        if not isinstance(tolerance, (int, float)) or float(tolerance) < 0.0:
            raise QualityRuleConfigurationError(
                "Parameter 'tolerance' must be a non-negative number.",
                rule_id=self.rule_id,
            )

    def evaluate_dataset(
        self,
        adapter: QualityDatasetAdapter,
        *,
        reference_datasets: Mapping[str, QualityDatasetAdapter] | None = None,
    ) -> QualityResult:
        target_col = self._require_column_param("target_column")
        src_cols = [str(c).strip() for c in self.params["source_columns"]]
        self._require_columns_Exist(adapter, [*src_cols, target_col])
        op = str(self.params.get("operation", self.params.get("expression"))).strip().lower()
        tolerance = float(self.params.get("tolerance", 1e-6))
        allow_null = bool(self.params.get("allow_null", True))

        rows = adapter.rows_as_dicts()
        total_rows = len(rows)
        if total_rows == 0:
            return self._build_result(passed=True, evaluated_records=0, failed_records=0)

        invalid_indices: list[int] = []
        invalid_samples: list[Any] = []
        for idx, row in enumerate(rows):
            vals = [row.get(c) for c in src_cols]
            target_val = row.get(target_col)
            if any(v is None for v in vals) or target_val is None:
                if not allow_null:
                    invalid_indices.append(idx)
                    invalid_samples.append({c: row.get(c) for c in [*src_cols, target_col]})
                continue
            try:
                nums = [float(v) for v in vals if v is not None]
                expected_num = float(target_val)
                acc = nums[0]
                for nxt in nums[1:]:
                    if op == "add":
                        acc += nxt
                    elif op == "subtract":
                        acc -= nxt
                    elif op == "multiply":
                        acc *= nxt
                    elif op == "divide":
                        if nxt == 0.0:
                            raise ZeroDivisionError("division by zero")
                        acc /= nxt
                if abs(acc - expected_num) > tolerance:
                    invalid_indices.append(idx)
                    invalid_samples.append({c: row.get(c) for c in [*src_cols, target_col]})
            except (TypeError, ValueError, ZeroDivisionError):
                invalid_indices.append(idx)
                invalid_samples.append({c: row.get(c) for c in [*src_cols, target_col]})

        issues: list[QualityIssue] = []
        if invalid_indices:
            issues.append(
                self._build_issue(
                    f"Arithmetic consistency '{op}({src_cols}) == {target_col}' failed for "
                    f"{len(invalid_indices)} record(s).",
                    affected_columns=[*src_cols, target_col],
                    affected_record_count=len(invalid_indices),
                    record_references=invalid_indices,
                    sample_values=invalid_samples,
                    metadata={
                        "operation": op,
                        "source_columns": src_cols,
                        "target_column": target_col,
                        "tolerance": tolerance,
                    },
                )
            )

        return self._build_result(
            passed=len(invalid_indices) == 0,
            evaluated_records=total_rows,
            failed_records=len(invalid_indices),
            issues=issues,
        )


__all__ = [
    "ArithmeticConsistencyRule",
    "ColumnComparisonRule",
    "ConditionalRequirementRule",
]
