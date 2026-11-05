"""Schema validation quality rules (required, unexpected, column types, compatibility)."""

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

_TYPE_ALIASES: dict[str, frozenset[str]] = {
    "int": frozenset({"integer"}),
    "integer": frozenset({"integer"}),
    "int64": frozenset({"integer"}),
    "float": frozenset({"float", "integer"}),
    "double": frozenset({"float", "integer"}),
    "float64": frozenset({"float", "integer"}),
    "numeric": frozenset({"integer", "float"}),
    "number": frozenset({"integer", "float"}),
    "str": frozenset({"string"}),
    "string": frozenset({"string"}),
    "text": frozenset({"string"}),
    "bool": frozenset({"boolean"}),
    "boolean": frozenset({"boolean"}),
    "date": frozenset({"datetime", "string"}),
    "datetime": frozenset({"datetime"}),
    "timestamp": frozenset({"datetime"}),
    "any": frozenset({"integer", "float", "boolean", "datetime", "string", "unknown"}),
}


class RequiredColumnsRule(QualityRule):
    """Verify that all required columns are present in the dataset."""

    rule_id = "schema.required_columns"
    default_category = QualityRuleCategory.SCHEMA
    default_severity = QualityRuleSeverity.CRITICAL
    default_scope = QualityRuleScope.DATASET

    def validate_config(self) -> None:
        cols = self.params.get("required_columns") or self.params.get("columns")
        if not isinstance(cols, (list, tuple)) or not cols:
            raise QualityRuleConfigurationError(
                "Rule 'schema.required_columns' requires a non-empty list parameter "
                "'required_columns' (or 'columns').",
                rule_id=self.rule_id,
            )
        for col in cols:
            if not isinstance(col, str) or not col.strip():
                raise QualityRuleConfigurationError(
                    "Column names in 'required_columns' must be non-empty strings.",
                    rule_id=self.rule_id,
                )

    def evaluate_dataset(
        self,
        adapter: QualityDatasetAdapter,
        *,
        reference_datasets: Mapping[str, QualityDatasetAdapter] | None = None,
    ) -> QualityResult:
        raw_cols = self.params.get("required_columns") or self.params.get("columns") or []
        required = [str(c).strip() for c in raw_cols]
        actual_set = set(adapter.columns())
        missing = [c for c in required if c not in actual_set]

        issues: list[QualityIssue] = []
        if missing:
            issues.append(
                self._build_issue(
                    f"Missing required column(s): {missing}.",
                    affected_columns=missing,
                    affected_record_count=adapter.row_count(),
                    metadata={"missing_columns": missing, "required_columns": required},
                )
            )

        return self._build_result(
            passed=len(missing) == 0,
            evaluated_records=len(required),
            failed_records=len(missing),
            issues=issues,
        )


class UnexpectedColumnsRule(QualityRule):
    """Detect columns that are not in the allowed schema column list."""

    rule_id = "schema.unexpected_columns"
    default_category = QualityRuleCategory.SCHEMA
    default_severity = QualityRuleSeverity.WARNING
    default_scope = QualityRuleScope.DATASET

    def validate_config(self) -> None:
        cols = self.params.get("allowed_columns") or self.params.get("expected_columns")
        if not isinstance(cols, (list, tuple)) or not cols:
            raise QualityRuleConfigurationError(
                "Rule 'schema.unexpected_columns' requires a non-empty list parameter "
                "'allowed_columns' (or 'expected_columns').",
                rule_id=self.rule_id,
            )
        for col in cols:
            if not isinstance(col, str) or not col.strip():
                raise QualityRuleConfigurationError(
                    "Column names in 'allowed_columns' must be non-empty strings.",
                    rule_id=self.rule_id,
                )

    def evaluate_dataset(
        self,
        adapter: QualityDatasetAdapter,
        *,
        reference_datasets: Mapping[str, QualityDatasetAdapter] | None = None,
    ) -> QualityResult:
        raw_allowed = (
            self.params.get("allowed_columns") or self.params.get("expected_columns") or []
        )
        allowed_set = {str(c).strip() for c in raw_allowed}
        actual_cols = list(adapter.columns())
        unexpected = [c for c in actual_cols if c not in allowed_set]

        issues: list[QualityIssue] = []
        if unexpected:
            issues.append(
                self._build_issue(
                    f"Unexpected column(s) present in dataset: {unexpected}.",
                    affected_columns=unexpected,
                    affected_record_count=adapter.row_count(),
                    metadata={"unexpected_columns": unexpected},
                )
            )

        eval_count = max(1, len(actual_cols))
        return self._build_result(
            passed=len(unexpected) == 0,
            evaluated_records=eval_count,
            failed_records=len(unexpected),
            issues=issues,
        )


class ColumnTypesRule(QualityRule):
    """Validate that dataset columns match expected logical data types."""

    rule_id = "schema.column_types"
    default_category = QualityRuleCategory.SCHEMA
    default_severity = QualityRuleSeverity.ERROR
    default_scope = QualityRuleScope.DATASET

    def validate_config(self) -> None:
        expected_types = self.params.get("expected_types")
        if not isinstance(expected_types, dict) or not expected_types:
            raise QualityRuleConfigurationError(
                "Rule 'schema.column_types' requires a non-empty dict parameter 'expected_types'.",
                rule_id=self.rule_id,
            )
        for col, expected in expected_types.items():
            if not isinstance(col, str) or not col.strip():
                raise QualityRuleConfigurationError(
                    "Column names in 'expected_types' must be non-empty strings.",
                    rule_id=self.rule_id,
                )
            norm = str(expected).strip().lower() if isinstance(expected, str) else ""
            if norm not in _TYPE_ALIASES:
                raise QualityRuleConfigurationError(
                    f"Unsupported expected type '{expected}' for column '{col}'. "
                    f"Supported types: {sorted(_TYPE_ALIASES.keys())}",
                    rule_id=self.rule_id,
                )

    def evaluate_dataset(
        self,
        adapter: QualityDatasetAdapter,
        *,
        reference_datasets: Mapping[str, QualityDatasetAdapter] | None = None,
    ) -> QualityResult:
        expected_types: dict[str, str] = self.params["expected_types"]
        existing_cols = set(adapter.columns())
        issues: list[QualityIssue] = []
        mismatches = 0

        for col, expected_raw in expected_types.items():
            col_name = col.strip()
            expected_norm = expected_raw.strip().lower()
            if col_name not in existing_cols:
                mismatches += 1
                issues.append(
                    self._build_issue(
                        f"Column '{col_name}' expected type '{expected_norm}' is missing.",
                        affected_column=col_name,
                        affected_record_count=adapter.row_count(),
                        metadata={"expected_type": expected_norm, "actual_type": "missing"},
                    )
                )
                continue

            # Empty datasets without values have 'unknown' logical type unless typed by DataFrame
            actual_type = adapter.column_logical_type(col_name)
            if adapter.row_count() == 0 and actual_type == "unknown":
                continue

            accepted_logical_types = _TYPE_ALIASES[expected_norm]
            if actual_type not in accepted_logical_types:
                mismatches += 1
                issues.append(
                    self._build_issue(
                        f"Column '{col_name}' has type '{actual_type}', "
                        f"expected '{expected_norm}'.",
                        affected_column=col_name,
                        affected_record_count=adapter.row_count(),
                        metadata={
                            "expected_type": expected_norm,
                            "actual_type": actual_type,
                        },
                    )
                )

        return self._build_result(
            passed=mismatches == 0,
            evaluated_records=len(expected_types),
            failed_records=mismatches,
            issues=issues,
        )


class SchemaCompatibilityRule(QualityRule):
    """Combined schema contract validating required columns, types, and optional strictness."""

    rule_id = "schema.compatibility"
    default_category = QualityRuleCategory.SCHEMA
    default_severity = QualityRuleSeverity.ERROR
    default_scope = QualityRuleScope.DATASET

    def validate_config(self) -> None:
        expected_schema: Any = self.params.get("expected_schema")
        required_cols: Any = self.params.get("required_columns")
        if not expected_schema and not required_cols:
            raise QualityRuleConfigurationError(
                "Rule 'schema.compatibility' requires 'expected_schema' (dict of column -> type) "
                "or 'required_columns' (list of columns).",
                rule_id=self.rule_id,
            )
        if expected_schema is not None:
            if not isinstance(expected_schema, dict) or not expected_schema:
                raise QualityRuleConfigurationError(
                    "'expected_schema' must be a non-empty dict mapping columns to types.",
                    rule_id=self.rule_id,
                )
            for col, exp_type in expected_schema.items():
                norm = str(exp_type).strip().lower() if isinstance(exp_type, str) else ""
                if norm not in _TYPE_ALIASES:
                    raise QualityRuleConfigurationError(
                        f"Unsupported type '{exp_type}' for column '{col}' in 'expected_schema'.",
                        rule_id=self.rule_id,
                    )

    def evaluate_dataset(
        self,
        adapter: QualityDatasetAdapter,
        *,
        reference_datasets: Mapping[str, QualityDatasetAdapter] | None = None,
    ) -> QualityResult:
        expected_schema: dict[str, str] = dict(self.params.get("expected_schema") or {})
        raw_required = self.params.get("required_columns")
        required_cols = (
            [str(c).strip() for c in raw_required]
            if isinstance(raw_required, (list, tuple))
            else list(expected_schema.keys())
        )
        allow_extra: bool = bool(self.params.get("allow_extra_columns", True))

        actual_cols = list(adapter.columns())
        actual_set = set(actual_cols)
        issues: list[QualityIssue] = []
        failed_checks = 0
        total_checks = max(1, len(required_cols) + len(expected_schema) + (0 if allow_extra else 1))

        missing = [c for c in required_cols if c not in actual_set]
        if missing:
            failed_checks += len(missing)
            issues.append(
                self._build_issue(
                    f"Schema missing required column(s): {missing}.",
                    affected_columns=missing,
                    affected_record_count=adapter.row_count(),
                    metadata={"missing_columns": missing},
                )
            )

        if not allow_extra:
            allowed_set = set(required_cols) | set(expected_schema.keys())
            unexpected = [c for c in actual_cols if c not in allowed_set]
            if unexpected:
                failed_checks += len(unexpected)
                issues.append(
                    self._build_issue(
                        f"Schema disallows unexpected column(s): {unexpected}.",
                        affected_columns=unexpected,
                        affected_record_count=adapter.row_count(),
                        metadata={"unexpected_columns": unexpected},
                    )
                )

        for col, expected_raw in expected_schema.items():
            if col not in actual_set:
                continue
            actual_type = adapter.column_logical_type(col)
            if adapter.row_count() == 0 and actual_type == "unknown":
                continue
            expected_norm = expected_raw.strip().lower()
            if actual_type not in _TYPE_ALIASES[expected_norm]:
                failed_checks += 1
                issues.append(
                    self._build_issue(
                        f"Schema type mismatch on '{col}': expected '{expected_norm}', "
                        f"got '{actual_type}'.",
                        affected_column=col,
                        affected_record_count=adapter.row_count(),
                        metadata={"expected_type": expected_norm, "actual_type": actual_type},
                    )
                )

        return self._build_result(
            passed=len(issues) == 0,
            evaluated_records=total_checks,
            failed_records=min(total_checks, failed_checks),
            issues=issues,
        )


__all__ = [
    "ColumnTypesRule",
    "RequiredColumnsRule",
    "SchemaCompatibilityRule",
    "UnexpectedColumnsRule",
]
