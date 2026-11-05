"""Unit tests for isolated ExecutionContext namespaces, immutability, and serialization."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from atlas_orchestration import ExecutionContext, ExecutionContextError


def test_execution_context_three_isolated_namespaces() -> None:
    ctx = ExecutionContext(
        workflow_id="wf_heat",
        workflow_version=2,
        execution_id="exec_001",
        workflow_variables={"shared_key": "workflow_var_value"},
        step_outputs={"shared_key": {"result": "step_output_value"}},
        execution_metadata={"shared_key": "metadata_value"},
        correlation_id="corr_123",
    )

    # All three namespaces hold "shared_key" independently without collision
    assert ctx.get_variable("shared_key") == "workflow_var_value"
    assert ctx.get_step_output("shared_key") == {"result": "step_output_value"}
    assert ctx.get_step_output_value("shared_key", "result") == "step_output_value"
    assert ctx.get_metadata("shared_key") == "metadata_value"
    assert ctx.correlation_id == "corr_123"
    assert ctx.has_variable("shared_key") is True
    assert ctx.has_step_output("shared_key") is True
    assert ctx.has_metadata("shared_key") is True

    # Updating step output does not alter workflow_variables or execution_metadata
    ctx.set_step_output("shared_key", {"result": "updated_step_output"})
    assert ctx.get_variable("shared_key") == "workflow_var_value"
    assert ctx.get_metadata("shared_key") == "metadata_value"
    assert ctx.get_step_output_value("shared_key", "result") == "updated_step_output"


def test_execution_context_deep_copy_isolation() -> None:
    nested_var = {"items": [1, 2, 3]}
    nested_out = {"metrics": {"count": 10}}

    ctx = ExecutionContext(
        workflow_id="wf_isolation",
        workflow_variables={"cfg": nested_var},
    )
    ctx.set_step_output("step_1", nested_out)

    # Mutating caller input dicts does not mutate context state
    nested_var["items"].append(999)
    nested_out["metrics"]["count"] = -1
    assert ctx.get_variable("cfg") == {"items": [1, 2, 3]}
    assert ctx.get_step_output("step_1") == {"metrics": {"count": 10}}

    # Mutating returned dicts/properties does not mutate context state
    returned_vars = ctx.workflow_variables
    returned_vars["cfg"]["items"].append(888)
    returned_outputs = ctx.step_outputs
    returned_outputs["step_1"]["metrics"]["count"] = 777
    assert ctx.get_variable("cfg") == {"items": [1, 2, 3]}
    assert ctx.get_step_output("step_1") == {"metrics": {"count": 10}}


def test_execution_context_missing_lookups_and_protected_metadata() -> None:
    ctx = ExecutionContext(workflow_id="wf_err", workflow_version=1)

    assert ctx.get_variable("missing", default="fallback") == "fallback"
    with pytest.raises(ExecutionContextError, match="not found"):
        ctx.get_variable("missing")

    assert ctx.get_step_output_value("missing_step", "k", default=42) == 42
    with pytest.raises(ExecutionContextError, match="No output recorded"):
        ctx.get_step_output("missing_step")

    ctx.set_step_output("s1", {"a": 1})
    assert ctx.get_step_output_value("s1", "missing_k", default="none") == "none"
    with pytest.raises(ExecutionContextError, match="Output key 'missing_k' not found"):
        ctx.get_step_output_value("s1", "missing_k")

    assert ctx.get_metadata("missing_meta", default=None) is None
    with pytest.raises(ExecutionContextError, match="not found"):
        ctx.get_metadata("missing_meta")

    # Protected system metadata keys cannot be overwritten
    for protected_key in ("execution_id", "workflow_id", "workflow_version", "created_at"):
        with pytest.raises(ExecutionContextError, match="Protected system metadata"):
            ctx.set_metadata(protected_key, "override")


def test_execution_context_rejects_non_json_values() -> None:
    ctx = ExecutionContext(workflow_id="wf_json")

    with pytest.raises(ExecutionContextError, match="not JSON-compatible"):
        ctx.set_variable("bad_var", object())

    with pytest.raises(ExecutionContextError, match="must be a dictionary"):
        ctx.set_step_output("s1", ["not", "a", "dict"])  # type: ignore[arg-type]

    with pytest.raises(ExecutionContextError, match="Non-finite float"):
        ctx.set_step_output("s1", {"score": float("inf")})

    with pytest.raises(ExecutionContextError, match="not JSON-compatible"):
        ctx.set_metadata("bad_meta", {1, 2})


def test_execution_context_serialization_and_snapshot_roundtrip() -> None:
    now = datetime.now(UTC)
    ctx = ExecutionContext(
        workflow_id="wf_roundtrip",
        workflow_version=3,
        execution_id="exec_rt_1",
        correlation_id="corr_rt_1",
        created_at=now,
        workflow_variables={"env": "prod", "thresholds": [0.1, 0.9]},
        step_outputs={"ingest": {"dataset_id": "ds_01", "rows": 500}},
        execution_metadata={"triggered_by": "cli"},
    )

    json_str = ctx.to_json()
    restored = ExecutionContext.from_json(json_str)

    assert restored.execution_id == "exec_rt_1"
    assert restored.workflow_id == "wf_roundtrip"
    assert restored.workflow_version == 3
    assert restored.correlation_id == "corr_rt_1"
    assert restored.created_at == now
    assert restored.workflow_variables == {"env": "prod", "thresholds": [0.1, 0.9]}
    assert restored.step_outputs == {"ingest": {"dataset_id": "ds_01", "rows": 500}}
    assert restored.get_metadata("triggered_by") == "cli"

    snap = ctx.snapshot()
    snap.set_variable("env", "staging")
    assert ctx.get_variable("env") == "prod"
    assert snap.get_variable("env") == "staging"
