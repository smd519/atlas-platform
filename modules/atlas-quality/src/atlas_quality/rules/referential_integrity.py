"""Referential integrity quality rules against reference datasets or key sets."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from atlas_quality.exceptions import QualityRuleConfigurationError, QualityRuleExecutionError
from atlas_quality.models.quality_issue import QualityIssue
from atlas_quality.models.quality_result import QualityResult
from atlas_quality.models.quality_rule import (
    QualityRuleCategory,
    QualityRuleScope,
    QualityRuleSeverity,
)
from atlas_quality.rules.base import QualityDatasetAdapter, QualityRule


class ForeignKeyRule(QualityRule):
    """Validate single or composite foreign-key relationships against a reference dataset."""

    rule_id = "referential_integrity.foreign_key"
    default_category = QualityRuleCategory.REFERENTIAL_INTEGRITY
    default_severity = QualityRuleSeverity.ERROR
    default_scope = QualityRuleScope.CROSS_DATASET

    def validate_config(self) -> None:
        key_mapping = self.params.get("key_mapping")
        src_col = self.params.get("column") or self.params.get("source_column")
        ref_col = self.params.get("reference_column")

        if key_mapping is not None:
            if not isinstance(key_mapping, dict) or not key_mapping:
                raise QualityRuleConfigurationError(
                    "'key_mapping' must be a non-empty dict mapping source to reference columns.",
                    rule_id=self.rule_id,
                )
            for k, v in key_mapping.items():
                if (
                    not isinstance(k, str)
                    or not k.strip()
                    or not isinstance(v, str)
                    or not v.strip()
                ):
                    raise QualityRuleConfigurationError(
                        "Columns in 'key_mapping' must be non-empty strings.",
                        rule_id=self.rule_id,
                    )
        else:
            if not isinstance(src_col, str) or not src_col.strip():
                raise QualityRuleConfigurationError(
                    "Rule 'referential_integrity.foreign_key' requires 'column' "
                    "(or 'source_column') or 'key_mapping'.",
                    rule_id=self.rule_id,
                )
            if self.params.get("reference_values") is None and (
                not isinstance(ref_col, str) or not ref_col.strip()
            ):
                raise QualityRuleConfigurationError(
                    "Rule 'referential_integrity.foreign_key' requires 'reference_column' when "
                    "checking a reference dataset.",
                    rule_id=self.rule_id,
                )

        has_ref_name = bool(
            isinstance(self.params.get("reference_dataset"), str)
            and self.params["reference_dataset"].strip()
        )
        has_inline_ref = self.params.get("reference_data") is not None
        has_ref_vals = isinstance(
            self.params.get("reference_values"), (list, tuple, set, frozenset)
        )
        if not (has_ref_name or has_inline_ref or has_ref_vals):
            raise QualityRuleConfigurationError(
                "Rule 'referential_integrity.foreign_key' requires 'reference_dataset' (name), "
                "'reference_data' (dataset), or 'reference_values' (collection).",
                rule_id=self.rule_id,
            )

    def evaluate_dataset(
        self,
        adapter: QualityDatasetAdapter,
        *,
        reference_datasets: Mapping[str, QualityDatasetAdapter] | None = None,
    ) -> QualityResult:
        raw_mapping = self.params.get("key_mapping")
        if isinstance(raw_mapping, dict):
            mapping: dict[str, str] = {k.strip(): v.strip() for k, v in raw_mapping.items()}
        else:
            src_col = str(self.params.get("column") or self.params.get("source_column")).strip()
            ref_col = str(self.params.get("reference_column") or src_col).strip()
            mapping = {src_col: ref_col}

        src_cols = list(mapping.keys())
        ref_cols = list(mapping.values())
        self._require_columns_Exist(adapter, src_cols)

        allow_null = bool(self.params.get("allow_null", True))
        valid_keys: set[tuple[Any, ...]] = set()

        if isinstance(self.params.get("reference_values"), (list, tuple, set, frozenset)):
            for item in self.params["reference_values"]:
                if isinstance(item, tuple):
                    valid_keys.add(item)
                elif isinstance(item, list):
                    valid_keys.add(tuple(item))
                else:
                    valid_keys.add((item,))
        else:
            ref_adapter = self._resolve_reference_adapter(reference_datasets)
            existing_ref_cols = set(ref_adapter.columns())
            missing_ref_cols = [c for c in ref_cols if c not in existing_ref_cols]
            if missing_ref_cols:
                raise QualityRuleConfigurationError(
                    f"Reference dataset is missing column(s) {missing_ref_cols}. "
                    f"Available columns: {list(ref_adapter.columns())}",
                    rule_id=self.rule_id,
                )
            for ref_row in ref_adapter.rows_as_dicts():
                key_tuple = tuple(ref_row.get(c) for c in ref_cols)
                if all(v is not None for v in key_tuple):
                    valid_keys.add(key_tuple)

        rows = adapter.rows_as_dicts()
        total_rows = len(rows)
        if total_rows == 0:
            return self._build_result(passed=True, evaluated_records=0, failed_records=0)

        orphan_indices: list[int] = []
        orphan_samples: list[Any] = []
        for idx, row in enumerate(rows):
            src_key = tuple(row.get(c) for c in src_cols)
            if any(v is None for v in src_key):
                if not allow_null:
                    orphan_indices.append(idx)
                    orphan_samples.append(src_key[0] if len(src_key) == 1 else src_key)
                continue
            if src_key not in valid_keys:
                orphan_indices.append(idx)
                orphan_samples.append(src_key[0] if len(src_key) == 1 else src_key)

        issues: list[QualityIssue] = []
        if orphan_indices:
            issues.append(
                self._build_issue(
                    f"Referential integrity violation on {src_cols}: "
                    f"{len(orphan_indices)} orphan record(s) not found in reference.",
                    affected_columns=src_cols,
                    affected_record_count=len(orphan_indices),
                    record_references=orphan_indices,
                    sample_values=orphan_samples,
                    metadata={
                        "key_mapping": mapping,
                        "reference_dataset": self.params.get("reference_dataset"),
                    },
                )
            )

        return self._build_result(
            passed=len(orphan_indices) == 0,
            evaluated_records=total_rows,
            failed_records=len(orphan_indices),
            issues=issues,
        )

    def _resolve_reference_adapter(
        self,
        reference_datasets: Mapping[str, QualityDatasetAdapter] | None,
    ) -> QualityDatasetAdapter:
        from atlas_quality.engine import coerce_dataset_adapter

        inline_data = self.params.get("reference_data")
        if inline_data is not None:
            if isinstance(inline_data, QualityDatasetAdapter):
                return inline_data
            return coerce_dataset_adapter(inline_data)

        ref_name = str(self.params.get("reference_dataset", "")).strip()
        if not reference_datasets or ref_name not in reference_datasets:
            raise QualityRuleExecutionError(
                f"Missing required reference dataset '{ref_name}' for rule '{self.rule_id}'.",
                rule_id=self.rule_id,
            )
        return reference_datasets[ref_name]


__all__ = ["ForeignKeyRule"]
