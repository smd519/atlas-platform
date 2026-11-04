"""Public quality rules and registry exports."""

from atlas_quality.rules.base import QualityDatasetAdapter, QualityRule
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
from atlas_quality.rules.registry import (
    QualityRuleRegistry,
    create_default_rule_registry,
)
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

__all__ = [
    "AllowedValuesRule",
    "ArithmeticConsistencyRule",
    "ColumnComparisonRule",
    "ColumnTypesRule",
    "CompletenessThresholdRule",
    "CompositeKeyUniquenessRule",
    "ConditionalRequirementRule",
    "DateAndTypeParseRule",
    "DuplicateRowsRule",
    "ForeignKeyRule",
    "NotNullRule",
    "NumericRangeRule",
    "QualityDatasetAdapter",
    "QualityRule",
    "QualityRuleRegistry",
    "RegexMatchRule",
    "RequiredColumnsRule",
    "RequiredFieldsCompletenessRule",
    "SchemaCompatibilityRule",
    "StringLengthRule",
    "UnexpectedColumnsRule",
    "UniqueColumnRule",
    "create_default_rule_registry",
]

