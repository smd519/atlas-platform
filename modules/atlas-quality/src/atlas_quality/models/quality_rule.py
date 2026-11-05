"""Quality rule configuration, severity, category, and scope models."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from atlas_core import require_not_empty
from pydantic import BaseModel, ConfigDict, Field, model_validator

from atlas_quality.exceptions import QualityRuleConfigurationError


class QualityRuleSeverity(StrEnum):
    """Severity levels for data quality rules and findings."""

    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class QualityRuleCategory(StrEnum):
    """Standard categories for data quality rules."""

    SCHEMA = "SCHEMA"
    COMPLETENESS = "COMPLETENESS"
    UNIQUENESS = "UNIQUENESS"
    VALIDITY = "VALIDITY"
    CONSISTENCY = "CONSISTENCY"
    REFERENTIAL_INTEGRITY = "REFERENTIAL_INTEGRITY"


class QualityRuleScope(StrEnum):
    """Granularity scope at which a quality rule evaluates data."""

    COLUMN = "COLUMN"
    ROW = "ROW"
    DATASET = "DATASET"
    CROSS_DATASET = "CROSS_DATASET"


class QualityRuleConfig(BaseModel):
    """Immutable declarative configuration for a quality rule instance."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    rule_id: str
    name: str | None = None
    description: str | None = None
    enabled: bool = True
    severity: QualityRuleSeverity = QualityRuleSeverity.ERROR
    category: QualityRuleCategory | None = None
    scope: QualityRuleScope | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    sample_limit: int = 5
    expose_samples: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)

    def __init__(self, **data: Any) -> None:
        if "severity" in data and isinstance(data["severity"], str):
            raw_sev = data["severity"].strip().upper()
            try:
                data["severity"] = QualityRuleSeverity(raw_sev)
            except ValueError as exc:
                raise QualityRuleConfigurationError(
                    f"Invalid quality rule severity '{data['severity']}'."
                ) from exc
        if (
            "category" in data
            and data["category"] is not None
            and isinstance(data["category"], str)
        ):
            raw_cat = data["category"].strip().upper()
            try:
                data["category"] = QualityRuleCategory(raw_cat)
            except ValueError as exc:
                raise QualityRuleConfigurationError(
                    f"Invalid quality rule category '{data['category']}'."
                ) from exc
        if "scope" in data and data["scope"] is not None and isinstance(data["scope"], str):
            raw_scope = data["scope"].strip().upper()
            try:
                data["scope"] = QualityRuleScope(raw_scope)
            except ValueError as exc:
                raise QualityRuleConfigurationError(
                    f"Invalid quality rule scope '{data['scope']}'."
                ) from exc
        try:
            super().__init__(**data)
        except QualityRuleConfigurationError:
            raise
        except Exception as exc:
            raise QualityRuleConfigurationError(
                f"Invalid quality rule configuration: {exc}"
            ) from exc

    @model_validator(mode="after")
    def _validate_rule_config(self) -> QualityRuleConfig:
        cleaned_id = self.rule_id.strip() if isinstance(self.rule_id, str) else ""
        try:
            require_not_empty(cleaned_id, "rule_id")
        except ValueError as exc:
            raise QualityRuleConfigurationError(str(exc)) from exc
        object.__setattr__(self, "rule_id", cleaned_id)

        resolved_name = (self.name or cleaned_id).strip()
        if not resolved_name:
            raise QualityRuleConfigurationError("Rule 'name' cannot be empty.", rule_id=cleaned_id)
        object.__setattr__(self, "name", resolved_name)

        if self.sample_limit < 0:
            raise QualityRuleConfigurationError(
                f"sample_limit must be >= 0, got {self.sample_limit}.",
                rule_id=cleaned_id,
            )
        return self


__all__ = [
    "QualityRuleCategory",
    "QualityRuleConfig",
    "QualityRuleScope",
    "QualityRuleSeverity",
]

