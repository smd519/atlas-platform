"""Uniqueness quality rules (single-column, composite-key, and full-row duplicate detection)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
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

_DUPLICATE_MODES: frozenset[str] = frozenset({"all", "subsequent"})


def _find_duplicate_indices(
    keys: Sequence[tuple[Any, ...]],
    *,
    ignore_nulls: bool,
    duplicate_mode: str,
) -> tuple[list[int], list[Any]]:
    counts: dict[tuple[Any, ...], list[int]] = {}
    for idx, key in enumerate(keys):
        if ignore_nulls and all(v is None for v in key):
            continue
        counts.setdefault(key, []).append(idx)

    dup_indices: list[int] = []
    dup_values: list[Any] = []
    for key, idxs in counts.items():
        if len(idxs) > 1:
            selected = idxs if duplicate_mode == "all" else idxs[1:]
            dup_indices.extend(selected)
            val_repr = key[0] if len(key) == 1 else key
            dup_values.extend([val_repr] * len(selected))

    dup_indices.sort()
    return dup_indices, dup_values


class UniqueColumnRule(QualityRule):
    """Validate that a single column contains unique values."""

    rule_id = "uniqueness.unique_column"
    default_category = QualityRuleCategory.UNIQUENESS
    default_severity = QualityRuleSeverity.ERROR
    default_scope = QualityRuleScope.COLUMN

    def validate_config(self) -> None:
        self._require_column_param("column")
        mode = str(self.params.get("duplicate_mode", "subsequent")).strip().lower()
        if mode not in _DUPLICATE_MODES:
            raise QualityRuleConfigurationError(
                f"Invalid 'duplicate_mode' '{mode}'. Allowed: {sorted(_DUPLICATE_MODES)}",
                rule_id=self.rule_id,
            )
        max_dups = self.params.get("max_duplicates", 0)
        if not isinstance(max_dups, int) or max_dups < 0:
            raise QualityRuleConfigurationError(
                "Parameter 'max_duplicates' must be a non-negative integer.",
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
        ignore_nulls = bool(self.params.get("ignore_nulls", True))
        mode = str(self.params.get("duplicate_mode", "subsequent")).strip().lower()
        max_duplicates = int(self.params.get("max_duplicates", 0))

        values = adapter.column_values(col)
        total_rows = len(values)
        if total_rows == 0:
            return self._build_result(passed=True, evaluated_records=0, failed_records=0)

        keys = [(v,) for v in values]
        dup_indices, dup_samples = _find_duplicate_indices(
            keys,
            ignore_nulls=ignore_nulls,
            duplicate_mode=mode,
        )
        dup_count = len(dup_indices)
        passed = dup_count <= max_duplicates

        issues: list[QualityIssue] = []
        if not passed:
            issues.append(
                self._build_issue(
                    f"Column '{col}' contains {dup_count} duplicate record(s) "
                    f"(allowed <= {max_duplicates}).",
                    affected_column=col,
                    affected_record_count=dup_count,
                    record_references=dup_indices,
                    sample_values=dup_samples,
                    metadata={"duplicate_mode": mode, "max_duplicates": max_duplicates},
                )
            )

        return self._build_result(
            passed=passed,
            evaluated_records=total_rows,
            failed_records=0 if passed else dup_count,
            issues=issues,
        )


class CompositeKeyUniquenessRule(QualityRule):
    """Validate that a composite key across two or more columns is unique."""

    rule_id = "uniqueness.composite_key"
    default_category = QualityRuleCategory.UNIQUENESS
    default_severity = QualityRuleSeverity.ERROR
    default_scope = QualityRuleScope.ROW

    def validate_config(self) -> None:
        cols = self.params.get("columns")
        if not isinstance(cols, (list, tuple)) or len(cols) < 1:
            raise QualityRuleConfigurationError(
                "Rule 'uniqueness.composite_key' requires a non-empty list parameter 'columns'.",
                rule_id=self.rule_id,
            )
        for col in cols:
            if not isinstance(col, str) or not col.strip():
                raise QualityRuleConfigurationError(
                    "Column names in 'columns' must be non-empty strings.",
                    rule_id=self.rule_id,
                )
        mode = str(self.params.get("duplicate_mode", "subsequent")).strip().lower()
        if mode not in _DUPLICATE_MODES:
            raise QualityRuleConfigurationError(
                f"Invalid 'duplicate_mode' '{mode}'. Allowed: {sorted(_DUPLICATE_MODES)}",
                rule_id=self.rule_id,
            )

    def evaluate_dataset(
        self,
        adapter: QualityDatasetAdapter,
        *,
        reference_datasets: Mapping[str, QualityDatasetAdapter] | None = None,
    ) -> QualityResult:
        cols = [str(c).strip() for c in self.params["columns"]]
        self._require_columns_Exist(adapter, cols)
        ignore_nulls = bool(self.params.get("ignore_nulls", False))
        mode = str(self.params.get("duplicate_mode", "subsequent")).strip().lower()
        max_duplicates = int(self.params.get("max_duplicates", 0))

        rows = adapter.rows_as_dicts()
        total_rows = len(rows)
        if total_rows == 0:
            return self._build_result(passed=True, evaluated_records=0, failed_records=0)

        keys = [tuple(row.get(c) for c in cols) for row in rows]
        dup_indices, dup_samples = _find_duplicate_indices(
            keys,
            ignore_nulls=ignore_nulls,
            duplicate_mode=mode,
        )
        dup_count = len(dup_indices)
        passed = dup_count <= max_duplicates

        issues: list[QualityIssue] = []
        if not passed:
            issues.append(
                self._build_issue(
                    f"Composite key {cols} contains {dup_count} duplicate record(s).",
                    affected_columns=cols,
                    affected_record_count=dup_count,
                    record_references=dup_indices,
                    sample_values=dup_samples,
                    metadata={"key_columns": cols, "duplicate_mode": mode},
                )
            )

        return self._build_result(
            passed=passed,
            evaluated_records=total_rows,
            failed_records=0 if passed else dup_count,
            issues=issues,
        )


class DuplicateRowsRule(QualityRule):
    """Detect duplicate full rows across all dataset columns."""

    rule_id = "uniqueness.no_duplicate_rows"
    default_category = QualityRuleCategory.UNIQUENESS
    default_severity = QualityRuleSeverity.WARNING
    default_scope = QualityRuleScope.DATASET

    def validate_config(self) -> None:
        mode = str(self.params.get("duplicate_mode", "subsequent")).strip().lower()
        if mode not in _DUPLICATE_MODES:
            raise QualityRuleConfigurationError(
                f"Invalid 'duplicate_mode' '{mode}'. Allowed: {sorted(_DUPLICATE_MODES)}",
                rule_id=self.rule_id,
            )

    def evaluate_dataset(
        self,
        adapter: QualityDatasetAdapter,
        *,
        reference_datasets: Mapping[str, QualityDatasetAdapter] | None = None,
    ) -> QualityResult:
        cols = list(adapter.columns())
        rows = adapter.rows_as_dicts()
        total_rows = len(rows)
        if total_rows == 0 or not cols:
            return self._build_result(passed=True, evaluated_records=0, failed_records=0)

        mode = str(self.params.get("duplicate_mode", "subsequent")).strip().lower()
        max_duplicates = int(self.params.get("max_duplicates", 0))
        keys = [tuple(row.get(c) for c in cols) for row in rows]
        dup_indices, dup_samples = _find_duplicate_indices(
            keys,
            ignore_nulls=False,
            duplicate_mode=mode,
        )
        dup_count = len(dup_indices)
        passed = dup_count <= max_duplicates

        issues: list[QualityIssue] = []
        if not passed:
            issues.append(
                self._build_issue(
                    f"Dataset contains {dup_count} duplicate row(s).",
                    affected_columns=cols,
                    affected_record_count=dup_count,
                    record_references=dup_indices,
                    sample_values=dup_samples,
                    metadata={"duplicate_mode": mode},
                )
            )

        return self._build_result(
            passed=passed,
            evaluated_records=total_rows,
            failed_records=0 if passed else dup_count,
            issues=issues,
        )


__all__ = [
    "CompositeKeyUniquenessRule",
    "DuplicateRowsRule",
    "UniqueColumnRule",
]

