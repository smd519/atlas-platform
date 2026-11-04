"""Unit tests for all built-in quality rules and QualityRuleRegistry."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd  # type: ignore[import-untyped]
import pytest
from atlas_quality import (
    AllowedValuesRule,
    ArithmeticConsistencyRule,
    ColumnComparisonRule,
    ColumnTypesRule,
    CompletenessThresholdRule,
    CompositeKeyUniquenessRule,
    ConditionalRequirementRule,
    DateAndTypeParseRule,
    DuplicateQualityRuleError,
    DuplicateRowsRule,
    ForeignKeyRule,
    NotNullRule,
    NumericRangeRule,
    QualityDatasetAdapter,
    QualityResult,
    QualityRule,
    QualityRuleCategory,
    QualityRuleConfigurationError,
    QualityRuleExecutionError,
    RegexMatchRule,
    RequiredColumnsRule,
    RequiredFieldsCompletenessRule,
    SchemaCompatibilityRule,
    StringLengthRule,
    UnexpectedColumnsRule,
    UniqueColumnRule,
    UnknownQualityRuleError,
    create_default_rule_registry,
)


def test_rule_registry_and_custom_rule() -> None:
    """QualityRuleRegistry resolves all 19 built-in rules and supports custom rules."""
    registry = create_default_rule_registry()
    assert len(registry.list_rules()) == 19

    with pytest.raises(DuplicateQualityRuleError):
        registry.register(NotNullRule)

    with pytest.raises(UnknownQualityRuleError):
        registry.get("nonexistent.rule")

    class CustomRowCountRule(QualityRule):
        rule_id = "custom.min_rows"
        default_category = QualityRuleCategory.COMPLETENESS

        def validate_config(self) -> None:
            min_rows = self.params.get("min_rows")
            if not isinstance(min_rows, int) or min_rows < 0:
                raise QualityRuleConfigurationError("'min_rows' must be >= 0.")

        def evaluate_dataset(
            self,
            adapter: QualityDatasetAdapter,
            *,
            reference_datasets: Mapping[str, QualityDatasetAdapter] | None = None,
        ) -> QualityResult:
            rows = adapter.row_count()
            min_rows = int(self.params["min_rows"])
            passed = rows >= min_rows
            return self._build_result(
                passed=passed,
                evaluated_records=rows,
                failed_records=0 if passed else (min_rows - rows),
            )

    registry.register(CustomRowCountRule)
    assert registry.exists("custom.min_rows")
    custom = registry.create({"rule_id": "custom.min_rows", "params": {"min_rows": 2}})
    assert custom.validate([{"a": 1}, {"a": 2}]) is True
    assert custom.validate([{"a": 1}]) is False


def test_schema_rules() -> None:
    """Test RequiredColumnsRule, UnexpectedColumnsRule, ColumnTypesRule, SchemaCompatibilityRule."""
    data = [{"id": 1, "name": "alice", "amount": 25.5, "extra": "x"}]

    # RequiredColumnsRule
    req_pass = RequiredColumnsRule(required_columns=["id", "name"])
    assert req_pass.evaluate(data).passed is True
    req_fail = RequiredColumnsRule(required_columns=["id", "missing_col"])
    res_req = req_fail.evaluate(data)
    assert res_req.passed is False
    assert res_req.issues[0].affected_columns == ("missing_col",)

    # UnexpectedColumnsRule
    unexp_fail = UnexpectedColumnsRule(allowed_columns=["id", "name", "amount"])
    res_unexp = unexp_fail.evaluate(data)
    assert res_unexp.passed is False
    assert res_unexp.issues[0].affected_columns == ("extra",)

    # ColumnTypesRule
    types_pass = ColumnTypesRule(
        expected_types={"id": "integer", "name": "string", "amount": "float"}
    )
    assert types_pass.evaluate(data).passed is True
    types_fail = ColumnTypesRule(expected_types={"name": "integer"})
    assert types_fail.evaluate(data).passed is False

    # SchemaCompatibilityRule
    compat_pass = SchemaCompatibilityRule(
        expected_schema={"id": "int", "name": "str", "amount": "numeric"},
        allow_extra_columns=True,
    )
    assert compat_pass.evaluate(data).passed is True
    compat_strict_fail = SchemaCompatibilityRule(
        expected_schema={"id": "int", "name": "str", "amount": "numeric"},
        allow_extra_columns=False,
    )
    assert compat_strict_fail.evaluate(data).passed is False


def test_completeness_rules() -> None:
    """Test NotNullRule, CompletenessThresholdRule, RequiredFieldsCompletenessRule."""
    data = [
        {"id": 1, "email": "a@example.com", "phone": "123"},
        {"id": 2, "email": None, "phone": "   "},
        {"id": 3, "email": "c@example.com", "phone": "456"},
        {"id": 4, "email": "d@example.com", "phone": "789"},
    ]

    # NotNullRule
    assert NotNullRule(column="id").evaluate(data).passed is True
    nn_fail = NotNullRule(column="email").evaluate(data)
    assert nn_fail.passed is False
    assert nn_fail.failed_records == 1

    # CompletenessThresholdRule (75% non-null)
    assert CompletenessThresholdRule(column="email", threshold=75.0).evaluate(data).passed is True
    assert CompletenessThresholdRule(column="email", threshold=80.0).evaluate(data).passed is False

    # RequiredFieldsCompletenessRule (with treat_blank_as_null=True)
    rf_res = RequiredFieldsCompletenessRule(
        columns=["email", "phone"], treat_blank_as_null=True
    ).evaluate(data)
    assert rf_res.passed is False
    assert rf_res.failed_records == 1
    assert len(rf_res.issues) == 2


def test_uniqueness_rules() -> None:
    """Test UniqueColumnRule, CompositeKeyUniquenessRule, DuplicateRowsRule."""
    data = [
        {"tenant": "T1", "id": 10, "val": "a"},
        {"tenant": "T1", "id": 10, "val": "a"},
        {"tenant": "T2", "id": 10, "val": "b"},
        {"tenant": "T2", "id": None, "val": "c"},
        {"tenant": "T2", "id": None, "val": "d"},
    ]

    # UniqueColumnRule (ignores nulls by default)
    uc_fail = UniqueColumnRule(column="id", duplicate_mode="subsequent").evaluate(data)
    assert uc_fail.passed is False
    assert uc_fail.failed_records == 2

    # CompositeKeyUniquenessRule
    ck_fail = CompositeKeyUniquenessRule(columns=["tenant", "id"]).evaluate(data)
    assert ck_fail.passed is False
    assert ck_fail.failed_records == 2  # (T1, 10) dup and (T2, None) dup

    ck_ignore_null = CompositeKeyUniquenessRule(
        columns=["tenant", "id"], ignore_nulls=True
    ).evaluate(
        [{"tenant": "T1", "id": 1}, {"tenant": None, "id": None}, {"tenant": None, "id": None}]
    )
    assert ck_ignore_null.passed is True

    # DuplicateRowsRule
    dr_res = DuplicateRowsRule().evaluate(data)
    assert dr_res.passed is False
    assert dr_res.failed_records == 1


def test_validity_rules() -> None:
    """Test AllowedValues, NumericRange, StringLength, RegexMatch, and DateAndTypeParse rules."""
    data = [
        {"status": "ACTIVE", "score": 85, "code": "AB-12", "created_at": "2026-11-01"},
        {"status": "INVALID", "score": 150, "code": "X", "created_at": "not-a-date"},
        {"status": "PENDING", "score": 0, "code": "CD-34", "created_at": "2026-11-03"},
    ]

    # AllowedValuesRule
    av_res = AllowedValuesRule(column="status", allowed_values=["ACTIVE", "PENDING"]).evaluate(data)
    assert av_res.passed is False
    assert av_res.failed_records == 1

    # NumericRangeRule
    nr_res = NumericRangeRule(column="score", min_value=0, max_value=100).evaluate(data)
    assert nr_res.passed is False
    assert nr_res.failed_records == 1

    # StringLengthRule
    sl_res = StringLengthRule(column="code", min_length=4, max_length=6).evaluate(data)
    assert sl_res.passed is False
    assert sl_res.failed_records == 1

    # RegexMatchRule
    rx_res = RegexMatchRule(column="code", pattern=r"^[A-Z]{2}-\d{2}$").evaluate(data)
    assert rx_res.passed is False
    assert rx_res.failed_records == 1

    # DateAndTypeParseRule
    dt_res = DateAndTypeParseRule(
        column="created_at", target_type="date", date_format="%Y-%m-%d"
    ).evaluate(data)
    assert dt_res.passed is False
    assert dt_res.failed_records == 1


def test_consistency_rules() -> None:
    """Test ColumnComparisonRule, ConditionalRequirementRule, ArithmeticConsistencyRule."""
    data = [
        {
            "start_ts": 100,
            "end_ts": 200,
            "status": "SHIPPED",
            "tracking_no": "TRK-1",
            "price": 10.0,
            "qty": 3,
            "total": 30.0,
        },
        {
            "start_ts": 300,
            "end_ts": 250,
            "status": "SHIPPED",
            "tracking_no": None,
            "price": 10.0,
            "qty": 2,
            "total": 99.0,
        },
    ]

    # ColumnComparisonRule
    cmp_res = ColumnComparisonRule(
        left_column="start_ts", right_column="end_ts", operator="lte"
    ).evaluate(data)
    assert cmp_res.passed is False
    assert cmp_res.failed_records == 1

    # ConditionalRequirementRule
    cond_res = ConditionalRequirementRule(
        when_column="status",
        when_operator="eq",
        when_value="SHIPPED",
        then_column="tracking_no",
        then_operator="not_null",
    ).evaluate(data)
    assert cond_res.passed is False
    assert cond_res.failed_records == 1

    # ArithmeticConsistencyRule
    arith_res = ArithmeticConsistencyRule(
        operation="multiply",
        source_columns=["price", "qty"],
        target_column="total",
    ).evaluate(data)
    assert arith_res.passed is False
    assert arith_res.failed_records == 1


def test_referential_integrity_rule() -> None:
    """Test ForeignKeyRule with single and composite keys and missing reference handling."""
    orders = [
        {"order_id": 1, "customer_id": "C1", "country": "US"},
        {"order_id": 2, "customer_id": "C99", "country": "US"},
        {"order_id": 3, "customer_id": None, "country": "US"},
    ]
    customers = [
        {"id": "C1", "country": "US"},
        {"id": "C2", "country": "EU"},
    ]

    fk_rule = ForeignKeyRule(
        column="customer_id",
        reference_dataset="customers",
        reference_column="id",
    )
    res = fk_rule.evaluate(orders, reference_datasets={"customers": customers})
    assert res.passed is False
    assert res.failed_records == 1
    assert res.issues[0].record_references == (1,)

    # Composite key mapping
    comp_fk = ForeignKeyRule(
        key_mapping={"customer_id": "id", "country": "country"},
        reference_dataset="customers",
    )
    res_comp = comp_fk.evaluate(orders, reference_datasets={"customers": customers})
    assert res_comp.passed is False
    assert res_comp.failed_records == 1

    # Missing reference dataset raises QualityRuleExecutionError
    with pytest.raises(QualityRuleExecutionError, match="Missing required reference dataset"):
        fk_rule.evaluate(orders, reference_datasets={})


def test_empty_dataset_and_invalid_configurations() -> None:
    """All rules handle empty datasets safely and reject malformed configurations."""
    empty_df = pd.DataFrame(columns=["id", "amount", "status"])
    assert NotNullRule(column="id").evaluate(empty_df).passed is True
    assert UniqueColumnRule(column="id").evaluate(empty_df).passed is True
    assert NumericRangeRule(column="amount", min_value=0).evaluate(empty_df).passed is True
    assert (
        AllowedValuesRule(column="status", allowed_values=["A"]).evaluate(empty_df).passed is True
    )

    with pytest.raises(QualityRuleConfigurationError):
        NumericRangeRule(column="amount", min_value=100, max_value=10)

    with pytest.raises(QualityRuleConfigurationError):
        RegexMatchRule(column="status", pattern="[unclosed")

    with pytest.raises(QualityRuleConfigurationError):
        ColumnComparisonRule(left_column="id", right_column="id", operator="eq")
