"""Validity quality rules (allowed values, numeric ranges, length, regex, date/type)."""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import date, datetime
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


class AllowedValuesRule(QualityRule):
    """Validate that non-null column values belong to an allowed set."""

    rule_id = "validity.allowed_values"
    default_category = QualityRuleCategory.VALIDITY
    default_severity = QualityRuleSeverity.ERROR
    default_scope = QualityRuleScope.COLUMN

    def validate_config(self) -> None:
        self._require_column_param("column")
        allowed = self.params.get("allowed_values")
        if not isinstance(allowed, (list, tuple, set, frozenset)) or len(allowed) == 0:
            raise QualityRuleConfigurationError(
                "Rule 'validity.allowed_values' requires a non-empty list/set 'allowed_values'.",
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
        allowed_set = set(self.params["allowed_values"])
        allow_null = bool(self.params.get("allow_null", True))

        values = adapter.column_values(col)
        total_rows = len(values)
        if total_rows == 0:
            return self._build_result(passed=True, evaluated_records=0, failed_records=0)

        invalid_indices: list[int] = []
        invalid_samples: list[Any] = []
        for idx, val in enumerate(values):
            if val is None:
                if not allow_null:
                    invalid_indices.append(idx)
                    invalid_samples.append(val)
                continue
            if val not in allowed_set:
                invalid_indices.append(idx)
                invalid_samples.append(val)

        issues: list[QualityIssue] = []
        if invalid_indices:
            issues.append(
                self._build_issue(
                    f"Column '{col}' has {len(invalid_indices)} record(s) outside allowed values.",
                    affected_column=col,
                    affected_record_count=len(invalid_indices),
                    record_references=invalid_indices,
                    sample_values=invalid_samples,
                    metadata={"allowed_values": sorted(str(x) for x in allowed_set)},
                )
            )

        return self._build_result(
            passed=len(invalid_indices) == 0,
            evaluated_records=total_rows,
            failed_records=len(invalid_indices),
            issues=issues,
        )


class NumericRangeRule(QualityRule):
    """Validate that numeric column values fall within [min_value, max_value]."""

    rule_id = "validity.numeric_range"
    default_category = QualityRuleCategory.VALIDITY
    default_severity = QualityRuleSeverity.ERROR
    default_scope = QualityRuleScope.COLUMN

    def validate_config(self) -> None:
        self._require_column_param("column")
        min_v = self.params.get("min_value")
        max_v = self.params.get("max_value")
        if min_v is None and max_v is None:
            raise QualityRuleConfigurationError(
                "Rule 'validity.numeric_range' requires 'min_value' and/or 'max_value'.",
                rule_id=self.rule_id,
            )
        if min_v is not None and (isinstance(min_v, bool) or not isinstance(min_v, (int, float))):
            raise QualityRuleConfigurationError(
                "Parameter 'min_value' must be a number.",
                rule_id=self.rule_id,
            )
        if max_v is not None and (isinstance(max_v, bool) or not isinstance(max_v, (int, float))):
            raise QualityRuleConfigurationError(
                "Parameter 'max_value' must be a number.",
                rule_id=self.rule_id,
            )
        if min_v is not None and max_v is not None and float(min_v) > float(max_v):
            raise QualityRuleConfigurationError(
                f"'min_value' ({min_v}) cannot be greater than 'max_value' ({max_v}).",
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
        min_v = self.params.get("min_value")
        max_v = self.params.get("max_value")
        inclusive = bool(self.params.get("inclusive", True))
        allow_null = bool(self.params.get("allow_null", True))

        values = adapter.column_values(col)
        total_rows = len(values)
        if total_rows == 0:
            return self._build_result(passed=True, evaluated_records=0, failed_records=0)

        invalid_indices: list[int] = []
        invalid_samples: list[Any] = []
        for idx, val in enumerate(values):
            if val is None:
                if not allow_null:
                    invalid_indices.append(idx)
                    invalid_samples.append(val)
                continue
            if isinstance(val, bool) or not isinstance(val, (int, float)):
                invalid_indices.append(idx)
                invalid_samples.append(val)
                continue
            num = float(val)
            if min_v is not None:
                below = num < float(min_v) if inclusive else num <= float(min_v)
                if below:
                    invalid_indices.append(idx)
                    invalid_samples.append(val)
                    continue
            if max_v is not None:
                above = num > float(max_v) if inclusive else num >= float(max_v)
                if above:
                    invalid_indices.append(idx)
                    invalid_samples.append(val)

        issues: list[QualityIssue] = []
        if invalid_indices:
            issues.append(
                self._build_issue(
                    f"Column '{col}' has {len(invalid_indices)} record(s) outside numeric range "
                    f"[{min_v}, {max_v}].",
                    affected_column=col,
                    affected_record_count=len(invalid_indices),
                    record_references=invalid_indices,
                    sample_values=invalid_samples,
                    metadata={"min_value": min_v, "max_value": max_v, "inclusive": inclusive},
                )
            )

        return self._build_result(
            passed=len(invalid_indices) == 0,
            evaluated_records=total_rows,
            failed_records=len(invalid_indices),
            issues=issues,
        )


class StringLengthRule(QualityRule):
    """Validate that string column values have length within [min_length, max_length]."""

    rule_id = "validity.string_length"
    default_category = QualityRuleCategory.VALIDITY
    default_severity = QualityRuleSeverity.ERROR
    default_scope = QualityRuleScope.COLUMN

    def validate_config(self) -> None:
        self._require_column_param("column")
        min_len = self.params.get("min_length")
        max_len = self.params.get("max_length")
        if min_len is None and max_len is None:
            raise QualityRuleConfigurationError(
                "Rule 'validity.string_length' requires 'min_length' and/or 'max_length'.",
                rule_id=self.rule_id,
            )
        if min_len is not None and (not isinstance(min_len, int) or min_len < 0):
            raise QualityRuleConfigurationError(
                "Parameter 'min_length' must be a non-negative integer.",
                rule_id=self.rule_id,
            )
        if max_len is not None and (not isinstance(max_len, int) or max_len < 0):
            raise QualityRuleConfigurationError(
                "Parameter 'max_length' must be a non-negative integer.",
                rule_id=self.rule_id,
            )
        if min_len is not None and max_len is not None and min_len > max_len:
            raise QualityRuleConfigurationError(
                f"'min_length' ({min_len}) cannot exceed 'max_length' ({max_len}).",
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
        min_len = self.params.get("min_length")
        max_len = self.params.get("max_length")
        allow_null = bool(self.params.get("allow_null", True))

        values = adapter.column_values(col)
        total_rows = len(values)
        if total_rows == 0:
            return self._build_result(passed=True, evaluated_records=0, failed_records=0)

        invalid_indices: list[int] = []
        invalid_samples: list[Any] = []
        for idx, val in enumerate(values):
            if val is None:
                if not allow_null:
                    invalid_indices.append(idx)
                    invalid_samples.append(val)
                continue
            if not isinstance(val, str):
                invalid_indices.append(idx)
                invalid_samples.append(val)
                continue
            length = len(val)
            if (min_len is not None and length < min_len) or (
                max_len is not None and length > max_len
            ):
                invalid_indices.append(idx)
                invalid_samples.append(val)

        issues: list[QualityIssue] = []
        if invalid_indices:
            issues.append(
                self._build_issue(
                    f"Column '{col}' has {len(invalid_indices)} record(s) violating string length "
                    f"[{min_len}, {max_len}].",
                    affected_column=col,
                    affected_record_count=len(invalid_indices),
                    record_references=invalid_indices,
                    sample_values=invalid_samples,
                    metadata={"min_length": min_len, "max_length": max_len},
                )
            )

        return self._build_result(
            passed=len(invalid_indices) == 0,
            evaluated_records=total_rows,
            failed_records=len(invalid_indices),
            issues=issues,
        )


class RegexMatchRule(QualityRule):
    """Validate that string column values match a regular expression pattern."""

    rule_id = "validity.regex"
    default_category = QualityRuleCategory.VALIDITY
    default_severity = QualityRuleSeverity.ERROR
    default_scope = QualityRuleScope.COLUMN

    def validate_config(self) -> None:
        self._require_column_param("column")
        pattern = self.params.get("pattern")
        if not isinstance(pattern, str) or not pattern:
            raise QualityRuleConfigurationError(
                "Rule 'validity.regex' requires a non-empty string parameter 'pattern'.",
                rule_id=self.rule_id,
            )
        try:
            re.compile(pattern)
        except re.error as exc:
            raise QualityRuleConfigurationError(
                f"Invalid regular expression '{pattern}': {exc}",
                rule_id=self.rule_id,
            ) from exc

    def evaluate_dataset(
        self,
        adapter: QualityDatasetAdapter,
        *,
        reference_datasets: Mapping[str, QualityDatasetAdapter] | None = None,
    ) -> QualityResult:
        col = self._require_column_param("column")
        self._require_columns_Exist(adapter, [col])
        pattern_str: str = self.params["pattern"]
        compiled = re.compile(pattern_str)
        full_match = bool(self.params.get("full_match", True))
        allow_null = bool(self.params.get("allow_null", True))

        values = adapter.column_values(col)
        total_rows = len(values)
        if total_rows == 0:
            return self._build_result(passed=True, evaluated_records=0, failed_records=0)

        invalid_indices: list[int] = []
        invalid_samples: list[Any] = []
        for idx, val in enumerate(values):
            if val is None:
                if not allow_null:
                    invalid_indices.append(idx)
                    invalid_samples.append(val)
                continue
            if not isinstance(val, str):
                invalid_indices.append(idx)
                invalid_samples.append(val)
                continue
            matched = compiled.fullmatch(val) if full_match else compiled.search(val)
            if matched is None:
                invalid_indices.append(idx)
                invalid_samples.append(val)

        issues: list[QualityIssue] = []
        if invalid_indices:
            issues.append(
                self._build_issue(
                    f"Column '{col}' has {len(invalid_indices)} record(s) not matching pattern "
                    f"'{pattern_str}'.",
                    affected_column=col,
                    affected_record_count=len(invalid_indices),
                    record_references=invalid_indices,
                    sample_values=invalid_samples,
                    metadata={"pattern": pattern_str, "full_match": full_match},
                )
            )

        return self._build_result(
            passed=len(invalid_indices) == 0,
            evaluated_records=total_rows,
            failed_records=len(invalid_indices),
            issues=issues,
        )


_SUPPORTED_PARSE_TYPES: frozenset[str] = frozenset(
    {"date", "datetime", "int", "integer", "float", "boolean", "bool"}
)


def _can_parse_value(val: Any, target_type: str, date_format: str | None) -> bool:
    if target_type in {"date", "datetime"}:
        if isinstance(val, (date, datetime)):
            return True
        if not isinstance(val, str) or not val.strip():
            return False
        text = val.strip()
        if date_format:
            try:
                datetime.strptime(text, date_format)
                return True
            except ValueError:
                return False
        try:
            datetime.fromisoformat(text.replace("Z", "+00:00"))
            return True
        except ValueError:
            return False

    if target_type in {"int", "integer"}:
        if isinstance(val, bool):
            return False
        if isinstance(val, int):
            return True
        if isinstance(val, str):
            try:
                int(val.strip())
                return True
            except ValueError:
                return False
        return False

    if target_type == "float":
        if isinstance(val, bool):
            return False
        if isinstance(val, (int, float)):
            return True
        if isinstance(val, str):
            try:
                float(val.strip())
                return True
            except ValueError:
                return False
        return False

    if target_type in {"bool", "boolean"}:
        if isinstance(val, bool):
            return True
        if isinstance(val, str):
            return val.strip().lower() in {"true", "false", "1", "0", "yes", "no"}
        return False

    return False


class DateAndTypeParseRule(QualityRule):
    """Validate that column values are valid or parseable as a target type or date format."""

    rule_id = "validity.type_or_date"
    default_category = QualityRuleCategory.VALIDITY
    default_severity = QualityRuleSeverity.ERROR
    default_scope = QualityRuleScope.COLUMN

    def validate_config(self) -> None:
        self._require_column_param("column")
        target = str(self.params.get("target_type", "date")).strip().lower()
        if target not in _SUPPORTED_PARSE_TYPES:
            supported_types = sorted(_SUPPORTED_PARSE_TYPES)
            raise QualityRuleConfigurationError(
                f"Unsupported 'target_type' '{target}'. Supported: {supported_types}",
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
        target = str(self.params.get("target_type", "date")).strip().lower()
        date_format: str | None = self.params.get("date_format")
        allow_null = bool(self.params.get("allow_null", True))

        values = adapter.column_values(col)
        total_rows = len(values)
        if total_rows == 0:
            return self._build_result(passed=True, evaluated_records=0, failed_records=0)

        invalid_indices: list[int] = []
        invalid_samples: list[Any] = []
        for idx, val in enumerate(values):
            if val is None:
                if not allow_null:
                    invalid_indices.append(idx)
                    invalid_samples.append(val)
                continue
            if not _can_parse_value(val, target, date_format):
                invalid_indices.append(idx)
                invalid_samples.append(val)

        issues: list[QualityIssue] = []
        if invalid_indices:
            issues.append(
                self._build_issue(
                    f"Column '{col}' has {len(invalid_indices)} record(s) not valid as '{target}'.",
                    affected_column=col,
                    affected_record_count=len(invalid_indices),
                    record_references=invalid_indices,
                    sample_values=invalid_samples,
                    metadata={"target_type": target, "date_format": date_format},
                )
            )

        return self._build_result(
            passed=len(invalid_indices) == 0,
            evaluated_records=total_rows,
            failed_records=len(invalid_indices),
            issues=issues,
        )


__all__ = [
    "AllowedValuesRule",
    "DateAndTypeParseRule",
    "NumericRangeRule",
    "RegexMatchRule",
    "StringLengthRule",
]
