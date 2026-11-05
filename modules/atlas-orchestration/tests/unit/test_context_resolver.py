"""Unit tests for ContextResolver dynamic parameter resolution and type preservation."""

from __future__ import annotations

import copy
from typing import Any

import pytest
from atlas_orchestration import (
    ContextResolver,
    ExecutionContext,
    UnresolvedContextReferenceError,
)


def test_context_resolver_step_outputs_and_nested_structures() -> None:
    resolver = ContextResolver()
    context = ExecutionContext(
        workflow_id="wf_customer",
        workflow_version=1,
        execution_id="exec_res_1",
        workflow_variables={"env": "prod", "limits": {"max_rows": 5000}},
        correlation_id="corr_99",
    )
    context.set_step_output(
        "ingestion_step",
        {
            "artifact_path": "data/customers.parquet",
            "source_name": "customer_api",
            "record_count": 1500,
            "is_valid": True,
            "ratio": 0.985,
            "columns": ["id", "name", "email"],
            "nested": {"layer": "bronze", "partition": {"date": "2026-11-04"}},
            "nullable": None,
        },
    )

    raw_params: dict[str, Any] = {
        "input_path": "${steps.ingestion_step.artifact_path}",
        "expected_count": "${steps.ingestion_step.record_count}",
        "active": "${steps.ingestion_step.is_valid}",
        "threshold": "${steps.ingestion_step.ratio}",
        "cols": "${steps.ingestion_step.columns}",
        "partition_info": "${steps.ingestion_step.nested.partition}",
        "null_field": "${steps.ingestion_step.nullable}",
        "options": {
            "format": "parquet",
            "source": "${steps.ingestion_step.source_name}",
            "partition_date": "${steps.ingestion_step.nested.partition.date}",
            "max_rows": "${variables.limits.max_rows}",
            "correlation": "${metadata.correlation_id}",
        },
        "interpolated_uri": (
            "s3://${variables.env}/${steps.ingestion_step.nested.layer}/"
            "${steps.ingestion_step.record_count}"
        ),
        "list_items": [
            "${steps.ingestion_step.artifact_path}",
            {"count": "${steps.ingestion_step.record_count}"},
        ],
    }
    original_snapshot = copy.deepcopy(raw_params)

    resolved = resolver.resolve(raw_params, context, step_id="transform_step")

    # Original parameters must never be mutated
    assert raw_params == original_snapshot

    # Exact values & native Python types preserved
    assert resolved["input_path"] == "data/customers.parquet"
    assert isinstance(resolved["expected_count"], int)
    assert resolved["expected_count"] == 1500
    assert resolved["active"] is True
    assert isinstance(resolved["threshold"], float)
    assert resolved["threshold"] == 0.985
    assert resolved["cols"] == ["id", "name", "email"]
    assert resolved["partition_info"] == {"date": "2026-11-04"}
    assert resolved["null_field"] is None
    assert resolved["options"] == {
        "format": "parquet",
        "source": "customer_api",
        "partition_date": "2026-11-04",
        "max_rows": 5000,
        "correlation": "corr_99",
    }
    assert resolved["interpolated_uri"] == "s3://prod/bronze/1500"
    assert resolved["list_items"] == [
        "data/customers.parquet",
        {"count": 1500},
    ]


@pytest.mark.parametrize(
    "bad_ref",
    [
        "${steps.missing_step.artifact_path}",
        "${steps.ingestion_step.missing_key}",
        "${steps.ingestion_step.nested.missing_deep_key}",
        "${steps.ingestion_step.record_count.not_a_dict}",
        "${steps.only_two_parts}",
        "${variables.missing_var}",
        "${variables}",
        "${metadata.missing_meta}",
        "${metadata}",
        "${unknown_ns.foo.bar}",
        "${steps.ingestion_step.artifact_path",  # missing closing brace
        "${}",
        "${   }",
        "${steps..artifact_path}",
        "${steps.ingestion_step.${nested}}",
    ],
)
def test_context_resolver_raises_unresolved_reference_error(bad_ref: str) -> None:
    resolver = ContextResolver()
    context = ExecutionContext(workflow_id="wf_err", execution_id="exec_err")
    context.set_step_output(
        "ingestion_step",
        {
            "artifact_path": "data/customers.parquet",
            "record_count": 100,
            "nested": {"ok": 1},
        },
    )

    with pytest.raises(UnresolvedContextReferenceError) as exc_info:
        resolver.resolve({"param": bad_ref}, context, step_id="downstream_step")
    assert exc_info.value.step_id == "downstream_step"
    assert exc_info.value.execution_id == "exec_err"
