"""Registry for discovering and instantiating data quality rules."""

from __future__ import annotations

from typing import Any

from atlas_quality.exceptions import (
    DuplicateQualityRuleError,
    QualityRuleConfigurationError,
    UnknownQualityRuleError,
)
from atlas_quality.models.quality_rule import QualityRuleConfig
from atlas_quality.rules.base import QualityRule
from atlas_quality.rules.completeness import (
    CompletenessThresholdRule,
    NotNullRule,
    RequiredFieldsCompletenessRule,
)
from atlas_quality.rules.consistency import (
    ArithmeticConsistencyRule,
    ColumnComparisonRule,
    ConditionalRequirementRule,
)
from atlas_quality.rules.referential_integrity import ForeignKeyRule
from atlas_quality.rules.schema import (
    ColumnTypesRule,
    RequiredColumnsRule,
    SchemaCompatibilityRule,
    UnexpectedColumnsRule,
)
from atlas_quality.rules.uniqueness import (
    CompositeKeyUniquenessRule,
    DuplicateRowsRule,
    UniqueColumnRule,
)
from atlas_quality.rules.validity import (
    AllowedValuesRule,
    DateAndTypeParseRule,
    NumericRangeRule,
    RegexMatchRule,
    StringLengthRule,
)


class QualityRuleRegistry:
    """Registers and resolves :class:`QualityRule` implementations by identifier."""

    def __init__(self) -> None:
        self._rules: dict[str, type[QualityRule]] = {}

    def register(
        self,
        rule_id_or_cls: str | type[QualityRule],
        rule_cls: type[QualityRule] | None = None,
        *,
        overwrite: bool = False,
    ) -> None:
        """Register a :class:`QualityRule` subclass under its ``rule_id``."""
        if isinstance(rule_id_or_cls, type) and issubclass(rule_id_or_cls, QualityRule):
            resolved_cls = rule_id_or_cls
            key = resolved_cls.rule_id.strip() if isinstance(resolved_cls.rule_id, str) else ""
        else:
            key = str(rule_id_or_cls).strip() if isinstance(rule_id_or_cls, str) else ""
            resolved_cls = rule_cls  # type: ignore[assignment]

        if not key:
            raise QualityRuleConfigurationError("Rule registration identifier cannot be empty.")
        if (
            resolved_cls is None
            or not isinstance(resolved_cls, type)
            or not issubclass(resolved_cls, QualityRule)
        ):
            raise QualityRuleConfigurationError(
                f"Registered class for '{key}' must be a subclass of QualityRule."
            )
        if key in self._rules and not overwrite:
            raise DuplicateQualityRuleError(
                f"Quality rule '{key}' is already registered.",
                rule_id=key,
            )
        self._rules[key] = resolved_cls

    def get(self, rule_id: str) -> type[QualityRule]:
        """Return the :class:`QualityRule` subclass registered for ``rule_id``."""
        key = rule_id.strip() if isinstance(rule_id, str) else ""
        try:
            return self._rules[key]
        except KeyError as exc:
            raise UnknownQualityRuleError(
                f"Quality rule '{rule_id}' is not registered.",
                rule_id=rule_id,
            ) from exc

    def exists(self, rule_id: str) -> bool:
        """Return True if ``rule_id`` is registered."""
        return rule_id in self._rules

    def list_rules(self) -> list[str]:
        """Return the sorted list of registered rule identifiers."""
        return sorted(self._rules.keys())

    def create(
        self,
        rule_spec: QualityRule | QualityRuleConfig | dict[str, Any],
    ) -> QualityRule:
        """Resolve and instantiate a :class:`QualityRule` from a config, dict, or rule instance."""
        if isinstance(rule_spec, QualityRule):
            rule_spec.validate_config()
            return rule_spec

        cfg = (
            rule_spec
            if isinstance(rule_spec, QualityRuleConfig)
            else QualityRuleConfig(**rule_spec)
        )
        rule_cls = self.get(cfg.rule_id)
        try:
            return rule_cls(config=cfg)
        except QualityRuleConfigurationError:
            raise
        except Exception as exc:
            raise QualityRuleConfigurationError(
                f"Failed to configure quality rule '{cfg.rule_id}': {exc}",
                rule_id=cfg.rule_id,
            ) from exc


def create_default_rule_registry() -> QualityRuleRegistry:
    """Create a :class:`QualityRuleRegistry` populated with all built-in quality rules."""
    registry = QualityRuleRegistry()
    builtin_rules: tuple[type[QualityRule], ...] = (
        RequiredColumnsRule,
        UnexpectedColumnsRule,
        ColumnTypesRule,
        SchemaCompatibilityRule,
        NotNullRule,
        CompletenessThresholdRule,
        RequiredFieldsCompletenessRule,
        UniqueColumnRule,
        CompositeKeyUniquenessRule,
        DuplicateRowsRule,
        AllowedValuesRule,
        NumericRangeRule,
        StringLengthRule,
        RegexMatchRule,
        DateAndTypeParseRule,
        ColumnComparisonRule,
        ConditionalRequirementRule,
        ArithmeticConsistencyRule,
        ForeignKeyRule,
    )
    for rule_cls in builtin_rules:
        registry.register(rule_cls)
    return registry


__all__ = [
    "QualityRuleRegistry",
    "create_default_rule_registry",
]

