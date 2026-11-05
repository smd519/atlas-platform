"""Unit tests for transformations, registry, engine, and executor."""

from __future__ import annotations

from typing import Any

import pytest
from atlas_processing import (
    AggregateTransformation,
    DuplicateTransformationError,
    FilterTransformation,
    MapTransformation,
    PandasProcessingEngine,
    ProcessingEngine,
    Transformation,
    TransformationError,
    TransformationExecutor,
    TransformationRegistry,
    TransformationStep,
    UnknownTransformationError,
    create_default_transformation_registry,
)


def _to_records(df: Any) -> list[dict[str, Any]]:
    """Helper to convert pandas DataFrame output to list of dicts for assertions."""
    records: list[dict[str, Any]] = df.to_dict(orient="records")
    return records


def test_transformation_registry_and_custom_registration() -> None:
    """TransformationRegistry registers built-in and custom transformations cleanly."""
    registry = create_default_transformation_registry()
    assert set(registry.list_transformations()) == {"aggregate", "filter", "map"}

    with pytest.raises(DuplicateTransformationError, match="already registered"):
        registry.register("filter", FilterTransformation)

    class CustomLimitTransformation(Transformation):
        def __init__(self, *, n: int = 1, name: str | None = None) -> None:
            self._n = n
            super().__init__(name=name)

        @property
        def transformation_id(self) -> str:
            return "limit"

        def validate_config(self) -> None:
            if not isinstance(self._n, int) or self._n < 0:
                raise TransformationError("Parameter 'n' must be a non-negative integer.")

        def apply(self, data: Any, engine: ProcessingEngine) -> Any:
            df = engine.to_dataset(data)
            return df.iloc[: self._n].reset_index(drop=True)

    registry.register("limit", CustomLimitTransformation)
    assert registry.exists("limit")
    assert "limit" in registry.list_transformations()

    step = TransformationStep(transformation_id="limit", params={"n": 2})
    instance = registry.create(step)
    out = _to_records(instance.transform([{"a": 1}, {"a": 2}, {"a": 3}]))
    assert out == [{"a": 1}, {"a": 2}]

    with pytest.raises(UnknownTransformationError, match="is not registered"):
        registry.get("nonexistent")

    empty_reg = TransformationRegistry()
    with pytest.raises(TransformationError, match="cannot be empty"):
        empty_reg.register("  ", FilterTransformation)


def test_filter_transformation_operators() -> None:
    """FilterTransformation supports comparison, null-check, membership, and logical operators."""
    records = [
        {"id": 1, "status": "active", "amount": 100.0, "region": "EU"},
        {"id": 2, "status": "inactive", "amount": 50.0, "region": "US"},
        {"id": 3, "status": "active", "amount": 250.0, "region": None},
        {"id": 4, "status": "pending", "amount": 10.0, "region": "APAC"},
    ]

    # Single condition eq
    f_eq = FilterTransformation(column="status", operator="eq", value="active")
    assert [r["id"] for r in _to_records(f_eq.process(records))] == [1, 3]

    # Single condition ne
    f_ne = FilterTransformation(column="status", operator="ne", value="active")
    assert [r["id"] for r in _to_records(f_ne.process(records))] == [2, 4]

    # Numeric comparisons: gt, gte, lt, lte
    assert [
        r["id"]
        for r in _to_records(
            FilterTransformation(column="amount", operator="gt", value=100).process(records)
        )
    ] == [3]
    assert [
        r["id"]
        for r in _to_records(
            FilterTransformation(column="amount", operator="gte", value=100).process(records)
        )
    ] == [1, 3]
    assert [
        r["id"]
        for r in _to_records(
            FilterTransformation(column="amount", operator="lt", value=50).process(records)
        )
    ] == [4]
    assert [
        r["id"]
        for r in _to_records(
            FilterTransformation(column="amount", operator="lte", value=50).process(records)
        )
    ] == [2, 4]

    # Null checks
    assert [
        r["id"]
        for r in _to_records(
            FilterTransformation(column="region", operator="is_null").process(records)
        )
    ] == [3]
    assert [
        r["id"]
        for r in _to_records(
            FilterTransformation(column="region", operator="not_null").process(records)
        )
    ] == [1, 2, 4]

    # Set membership: in / not_in
    assert [
        r["id"]
        for r in _to_records(
            FilterTransformation(column="region", operator="in", value=["EU", "APAC"]).process(
                records
            )
        )
    ] == [1, 4]
    assert [
        r["id"]
        for r in _to_records(
            FilterTransformation(
                column="status", operator="not_in", value=["inactive", "pending"]
            ).process(records)
        )
    ] == [1, 3]

    # Multi-condition AND / OR
    f_and = FilterTransformation(
        logical_operator="and",
        conditions=[
            {"column": "status", "operator": "eq", "value": "active"},
            {"column": "region", "operator": "not_null"},
        ],
    )
    assert [r["id"] for r in _to_records(f_and.process(records))] == [1]

    f_or = FilterTransformation(
        logical_operator="or",
        conditions=[
            {"column": "amount", "operator": "gte", "value": 200},
            {"column": "status", "operator": "eq", "value": "pending"},
        ],
    )
    assert [r["id"] for r in _to_records(f_or.process(records))] == [3, 4]


def test_filter_transformation_errors() -> None:
    """FilterTransformation raises TransformationError on invalid configs or missing columns."""
    with pytest.raises(TransformationError, match="requires at least one filter condition"):
        FilterTransformation()

    with pytest.raises(TransformationError, match="Unsupported filter operator"):
        FilterTransformation(column="a", operator="regex", value=".*")

    with pytest.raises(TransformationError, match="requires a value"):
        FilterTransformation(column="a", operator="eq", value=None)

    with pytest.raises(TransformationError, match="requires an iterable"):
        FilterTransformation(column="a", operator="in", value="not-a-list")

    f = FilterTransformation(column="missing_col", operator="eq", value=1)
    with pytest.raises(TransformationError, match="Missing required column"):
        f.process([{"id": 1}])


def test_map_transformation_operations() -> None:
    """MapTransformation supports rename, derived columns, unary ops, and select."""
    records = [
        {
            "first_name": " alice ",
            "last_name": "smith",
            "price": 10.0,
            "qty": 3,
            "discount": None,
            "tier": "G",
            "extra": "x",
        },
        {
            "first_name": " bob ",
            "last_name": "jones",
            "price": 20.0,
            "qty": 2,
            "discount": 5.0,
            "tier": "S",
            "extra": "y",
        },
    ]

    mapper = MapTransformation(
        rename_columns={"qty": "quantity"},
        operations={
            "first_name": "strip",
            "last_name": "upper",
        },
        value_mappings={
            "tier": {"G": "Gold", "S": "Silver"},
        },
        derived_columns=[
            {
                "target_column": "gross_total",
                "expression": "multiply",
                "source_columns": ["price", "quantity"],
            },
            {
                "target_column": "effective_discount",
                "expression": "coalesce",
                "source_columns": ["discount"],
                "value": 0.0,
            },
            {
                "target_column": "net_total",
                "expression": "subtract",
                "source_columns": ["gross_total", "effective_discount"],
            },
            {
                "target_column": "currency",
                "expression": "constant",
                "value": "USD",
            },
        ],
        drop_columns=["extra"],
        select_columns=["first_name", "last_name", "tier", "gross_total", "net_total", "currency"],
    )

    result = _to_records(mapper.process(records))
    assert result == [
        {
            "first_name": "alice",
            "last_name": "SMITH",
            "tier": "Gold",
            "gross_total": 30.0,
            "net_total": 30.0,
            "currency": "USD",
        },
        {
            "first_name": "bob",
            "last_name": "JONES",
            "tier": "Silver",
            "gross_total": 40.0,
            "net_total": 35.0,
            "currency": "USD",
        },
    ]


def test_map_transformation_additional_expressions_and_errors() -> None:
    """MapTransformation supports copy, add, divide, concat, and validates invalid configs."""
    records = [{"a": 10, "b": 2, "txt": "HELLO", "num_str": "42"}]
    mapper = MapTransformation(
        derived_columns=[
            {"target_column": "a_copy", "expression": "copy", "source_columns": ["a"]},
            {"target_column": "sum_ab", "expression": "add", "source_columns": ["a", "b"]},
            {"target_column": "div_ab", "expression": "divide", "source_columns": ["a", "b"]},
            {
                "target_column": "combined_txt",
                "expression": "concat",
                "source_columns": ["txt", "num_str"],
                "separator": "-",
            },
        ],
        operations={
            "txt": "lower",
            "num_str": "to_int",
        },
    )
    res = _to_records(mapper.process(records))
    assert res[0]["a_copy"] == 10
    assert res[0]["sum_ab"] == 12
    assert res[0]["div_ab"] == 5.0
    assert res[0]["combined_txt"] == "HELLO-42"
    assert res[0]["txt"] == "hello"
    assert res[0]["num_str"] == 42

    with pytest.raises(TransformationError, match="requires at least one mapping configuration"):
        MapTransformation()

    with pytest.raises(TransformationError, match="Missing required column"):
        MapTransformation(select_columns=["unknown"]).process(records)


def test_aggregate_transformation_grouped_and_ungrouped() -> None:
    """AggregateTransformation supports count, sum, avg, min, max with and without group_by."""
    records = [
        {"dept": "eng", "salary": 100.0},
        {"dept": "eng", "salary": 150.0},
        {"dept": "sales", "salary": 80.0},
    ]

    grouped = AggregateTransformation(
        group_by=["dept"],
        aggregations=[
            {"column": "salary", "function": "count", "alias": "headcount"},
            {"column": "salary", "function": "sum", "alias": "total_salary"},
            {"column": "salary", "function": "avg", "alias": "avg_salary"},
            {"column": "salary", "function": "min", "alias": "min_salary"},
            {"column": "salary", "function": "max", "alias": "max_salary"},
        ],
    )
    res_grouped = _to_records(grouped.process(records))
    assert len(res_grouped) == 2
    by_dept = {r["dept"]: r for r in res_grouped}
    assert by_dept["eng"] == {
        "dept": "eng",
        "headcount": 2,
        "total_salary": 250.0,
        "avg_salary": 125.0,
        "min_salary": 100.0,
        "max_salary": 150.0,
    }
    assert by_dept["sales"]["headcount"] == 1

    # Whole-dataset aggregation (empty group_by)
    overall = AggregateTransformation(
        group_by=[],
        aggregations=[
            {"column": "salary", "function": "sum", "alias": "company_payroll"},
            {"column": "salary", "function": "count", "alias": "total_employees"},
        ],
    )
    res_overall = _to_records(overall.process(records))
    assert res_overall == [{"company_payroll": 330.0, "total_employees": 3}]


def test_aggregate_transformation_errors() -> None:
    """AggregateTransformation validates configuration and missing columns."""
    with pytest.raises(TransformationError, match="requires at least one aggregation"):
        AggregateTransformation(group_by=["a"], aggregations=[])

    with pytest.raises(TransformationError, match="Unsupported aggregation function"):
        AggregateTransformation(aggregations=[{"column": "a", "function": "median"}])

    agg = AggregateTransformation(
        group_by=["missing"],
        aggregations=[{"column": "a", "function": "sum"}],
    )
    with pytest.raises(TransformationError, match="Missing required column"):
        agg.process([{"a": 1}])


def test_transformation_executor_and_engine() -> None:
    """TransformationExecutor runs multi-step pipelines and collects metrics."""
    engine = PandasProcessingEngine()
    executor = TransformationExecutor(engine=engine)

    raw_data = [
        {"id": 1, "category": "A", "amount": 20.0},
        {"id": 2, "category": "B", "amount": -5.0},
        {"id": 3, "category": "A", "amount": 30.0},
    ]
    steps = [
        TransformationStep(
            name="positive_only",
            transformation_id="filter",
            params={"column": "amount", "operator": "gt", "value": 0},
        ),
        TransformationStep(
            name="add_tax",
            transformation_id="map",
            params={
                "derived_columns": [
                    {"target_column": "tax_rate", "expression": "constant", "value": 1.1},
                    {
                        "target_column": "total_with_tax",
                        "expression": "multiply",
                        "source_columns": ["amount", "tax_rate"],
                    },
                ]
            },
        ),
        TransformationStep(
            name="by_category",
            transformation_id="aggregate",
            params={
                "group_by": ["category"],
                "aggregations": [
                    {"column": "total_with_tax", "function": "sum", "alias": "category_total"},
                    {"column": "id", "function": "count", "alias": "order_count"},
                ],
            },
        ),
    ]

    output = executor.execute(raw_data, steps)
    assert output.input_row_count == 3
    assert output.output_row_count == 1
    assert output.steps_executed == 3
    assert len(output.step_metrics) == 3
    assert output.step_metrics[0].step_name == "positive_only"
    assert output.step_metrics[0].output_row_count == 2
    rows = _to_records(output.data)
    assert rows[0]["category"] == "A"
    assert pytest.approx(rows[0]["category_total"]) == 55.0
    assert rows[0]["order_count"] == 2
