"""Engine-independent base QualityRule contract and dataset abstraction."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from time import perf_counter
from typing import Any

from atlas_core import QualityRule as CoreQualityRule

from atlas_quality.exceptions import QualityRuleConfigurationError
from atlas_quality.models.quality_issue import QualityIssue
from atlas_quality.models.quality_result import QualityResult, RuleOutcomeStatus
from atlas_quality.models.quality_rule import (
    QualityRuleCategory,
    QualityRuleConfig,
    QualityRuleScope,
    QualityRuleSeverity,
)


class QualityDatasetAdapter(ABC):
    """Engine-independent dataset inspection interface used by quality rules.

    Decouples rule definitions in ``atlas_quality.rules`` from concrete runtime
    engines (e.g., pandas in v0.1.0, PySpark or SQL in future releases).
    """

    @abstractmethod
    def row_count(self) -> int:
        """Return total number of rows in the dataset."""

    @abstractmethod
    def columns(self) -> tuple[str, ...]:
        """Return ordered column names in the dataset."""

    @abstractmethod
    def column_logical_type(self, column: str) -> str:
        """Return normalized logical type ('integer', 'float', 'boolean', 'datetime', 'string')."""

    @abstractmethod
    def column_values(self, column: str) -> Sequence[Any]:
        """Return the sequence of values for ``column`` (with nulls represented as ``None``)."""

    @abstractmethod
    def rows_as_dicts(self) -> Sequence[dict[str, Any]]:
        """Return dataset rows as dictionaries with normalized ``None`` for null values."""


class QualityRule(CoreQualityRule, ABC):
    """Abstract base class for engine-independent data quality rules.

    Implements :class:`atlas_core.QualityRule` while providing rich, immutable
    :class:`QualityResult` and :class:`QualityIssue` outputs.
    """

    rule_id: str = ""
    default_category: QualityRuleCategory = QualityRuleCategory.VALIDITY
    default_severity: QualityRuleSeverity = QualityRuleSeverity.ERROR
    default_scope: QualityRuleScope = QualityRuleScope.COLUMN

    def __init__(
        self,
        config: QualityRuleConfig | None = None,
        *,
        name: str | None = None,
        description: str | None = None,
        enabled: bool = True,
        severity: QualityRuleSeverity | str | None = None,
        sample_limit: int = 5,
        expose_samples: bool = False,
        **params: Any,
    ) -> None:
        if not self.rule_id or not self.rule_id.strip():
            raise QualityRuleConfigurationError(
                f"Rule class '{type(self).__name__}' must define a non-empty 'rule_id'."
            )

        if config is not None:
            merged_params = {**config.params, **params}
            self._config = QualityRuleConfig(
                rule_id=config.rule_id or self.rule_id,
                name=name or config.name or self.rule_id,
                description=description if description is not None else config.description,
                enabled=config.enabled if "enabled" not in params else bool(enabled),
                severity=severity if severity is not None else config.severity,
                category=config.category or self.default_category,
                scope=config.scope or self.default_scope,
                params=merged_params,
                sample_limit=config.sample_limit,
                expose_samples=config.expose_samples,
                metadata=dict(config.metadata),
            )
        else:
            resolved_sev = severity if severity is not None else self.default_severity
            self._config = QualityRuleConfig(
                rule_id=self.rule_id,
                name=name or self.rule_id,
                description=description,
                enabled=enabled,
                severity=resolved_sev,  # type: ignore[arg-type]
                category=self.default_category,
                scope=self.default_scope,
                params=params,
                sample_limit=sample_limit,
                expose_samples=expose_samples,
            )

        self.validate_config()

    @property
    def config(self) -> QualityRuleConfig:
        """Return the immutable :class:`QualityRuleConfig` for this rule."""
        return self._config

    @property
    def rule_name(self) -> str:
        """Return the human-readable rule name (implements :class:`atlas_core.QualityRule`)."""
        return self._config.name or self.rule_id

    @property
    def category(self) -> QualityRuleCategory:
        """Return the rule's category."""
        return self._config.category or self.default_category

    @property
    def severity(self) -> QualityRuleSeverity:
        """Return the configured rule severity."""
        return self._config.severity

    @property
    def scope(self) -> QualityRuleScope:
        """Return the rule evaluation scope."""
        return self._config.scope or self.default_scope

    @property
    def enabled(self) -> bool:
        """Return True if the rule is enabled."""
        return self._config.enabled

    @property
    def params(self) -> Mapping[str, Any]:
        """Return the rule configuration parameters."""
        return self._config.params

    @abstractmethod
    def validate_config(self) -> None:
        """Validate ``self.params`` or raise :class:`QualityRuleConfigurationError`."""

    @abstractmethod
    def evaluate_dataset(
        self,
        adapter: QualityDatasetAdapter,
        *,
        reference_datasets: Mapping[str, QualityDatasetAdapter] | None = None,
    ) -> QualityResult:
        """Evaluate the rule against an engine-independent :class:`QualityDatasetAdapter`."""

    def evaluate(
        self,
        data: Any,
        *,
        adapter: QualityDatasetAdapter | None = None,
        reference_datasets: Mapping[str, Any] | None = None,
    ) -> QualityResult:
        """Coerce ``data`` to an adapter if needed and execute :meth:`evaluate_dataset`."""
        if not self.enabled:
            return QualityResult(
                rule_id=self._config.rule_id,
                rule_name=self.rule_name,
                category=self.category,
                severity=self.severity,
                passed=True,
                status=RuleOutcomeStatus.SKIPPED,
                evaluated_records=0,
                failed_records=0,
                success_percentage=100.0,
                execution_duration_seconds=0.0,
                message=f"Rule '{self.rule_name}' is disabled and was skipped.",
            )

        from atlas_quality.engine import coerce_dataset_adapter

        t0 = perf_counter()
        resolved_adapter = adapter if adapter is not None else coerce_dataset_adapter(data)
        resolved_refs: dict[str, QualityDatasetAdapter] = {}
        if reference_datasets:
            for ref_name, ref_data in reference_datasets.items():
                resolved_refs[ref_name] = (
                    ref_data
                    if isinstance(ref_data, QualityDatasetAdapter)
                    else coerce_dataset_adapter(ref_data)
                )

        result = self.evaluate_dataset(resolved_adapter, reference_datasets=resolved_refs)
        elapsed = max(0.0, perf_counter() - t0)
        if result.execution_duration_seconds == 0.0 and elapsed > 0.0:
            return result.model_copy(update={"execution_duration_seconds": elapsed})
        return result

    def validate(self, data: Any) -> bool:
        """Implement :class:`atlas_core.QualityRule` by returning boolean pass status."""
        return self.evaluate(data).passed

    def _require_column_param(self, key: str = "column") -> str:
        val = self.params.get(key)
        if not isinstance(val, str) or not val.strip():
            raise QualityRuleConfigurationError(
                f"Rule '{self.rule_id}' requires a non-empty string parameter '{key}'.",
                rule_id=self.rule_id,
            )
        return val.strip()

    def _require_columns_Exist(
        self,
        adapter: QualityDatasetAdapter,
        columns: Sequence[str],
    ) -> None:
        existing = set(adapter.columns())
        missing = [c for c in columns if c not in existing]
        if missing:
            raise QualityRuleConfigurationError(
                f"Rule '{self.rule_id}' references missing column(s) {missing}. "
                f"Available columns: {list(adapter.columns())}",
                rule_id=self.rule_id,
            )

    def _build_issue(
        self,
        description: str,
        *,
        affected_column: str | None = None,
        affected_columns: Sequence[str] = (),
        affected_record_count: int = 0,
        record_references: Sequence[int | str] = (),
        sample_values: Sequence[Any] = (),
        metadata: dict[str, Any] | None = None,
    ) -> QualityIssue:
        return QualityIssue(
            rule_id=self._config.rule_id,
            severity=self.severity,
            category=self.category,
            description=description,
            affected_column=affected_column,
            affected_columns=tuple(affected_columns),
            affected_record_count=affected_record_count,
            record_references=tuple(record_references),
            sample_values=tuple(sample_values),
            expose_samples=self._config.expose_samples,
            sample_limit=self._config.sample_limit,
            metadata=metadata or {},
        )

    def _build_result(
        self,
        *,
        passed: bool,
        evaluated_records: int,
        failed_records: int,
        issues: Sequence[QualityIssue] = (),
        message: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> QualityResult:
        return QualityResult(
            rule_id=self._config.rule_id,
            rule_name=self.rule_name,
            category=self.category,
            severity=self.severity,
            passed=passed,
            evaluated_records=evaluated_records,
            failed_records=failed_records,
            issues=tuple(issues),
            message=message,
            metadata=metadata or {},
        )


__all__ = [
    "QualityDatasetAdapter",
    "QualityRule",
]
