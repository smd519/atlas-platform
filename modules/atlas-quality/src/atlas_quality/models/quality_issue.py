"""Immutable representation of an individual data quality finding."""

from __future__ import annotations

from typing import Any

from atlas_core import generate_execution_id, require_not_empty
from pydantic import BaseModel, ConfigDict, Field, model_validator

from atlas_quality.exceptions import QualityRuleConfigurationError
from atlas_quality.models.quality_rule import QualityRuleCategory, QualityRuleSeverity


class QualityIssue(BaseModel):
    """Immutable record of an individual quality finding produced by a rule."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    issue_id: str = Field(default_factory=generate_execution_id)
    rule_id: str
    severity: QualityRuleSeverity
    category: QualityRuleCategory
    description: str
    affected_column: str | None = None
    affected_columns: tuple[str, ...] = ()
    affected_record_count: int = 0
    record_references: tuple[int | str, ...] = ()
    sample_values: tuple[Any, ...] = ()
    expose_samples: bool = False
    sample_limit: int = 5
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _sanitize_issue(self) -> QualityIssue:
        try:
            require_not_empty(self.issue_id.strip(), "issue_id")
            require_not_empty(self.rule_id.strip(), "rule_id")
            require_not_empty(self.description.strip(), "description")
        except ValueError as exc:
            raise QualityRuleConfigurationError(str(exc)) from exc

        if self.affected_record_count < 0:
            raise QualityRuleConfigurationError(
                f"affected_record_count must be >= 0, got {self.affected_record_count}."
            )
        if self.sample_limit < 0:
            raise QualityRuleConfigurationError(
                f"sample_limit must be >= 0, got {self.sample_limit}."
            )

        if self.affected_column and not self.affected_columns:
            object.__setattr__(self, "affected_columns", (self.affected_column,))
        elif self.affected_columns and self.affected_column is None:
            object.__setattr__(self, "affected_column", self.affected_columns[0])

        limit = self.sample_limit
        if len(self.record_references) > limit:
            object.__setattr__(self, "record_references", tuple(self.record_references[:limit]))

        # Never expose sensitive source values by default unless explicitly enabled.
        if not self.expose_samples or limit == 0:
            object.__setattr__(self, "sample_values", ())
        elif len(self.sample_values) > limit:
            object.__setattr__(self, "sample_values", tuple(self.sample_values[:limit]))

        return self


__all__ = ["QualityIssue"]

