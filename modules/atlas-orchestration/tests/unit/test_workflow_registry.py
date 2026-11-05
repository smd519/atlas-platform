"""Unit tests for thread-safe versioned WorkflowRegistry."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from atlas_orchestration import (
    CyclicDependencyError,
    DuplicateWorkflowError,
    InvalidWorkflowDefinitionError,
    WorkflowDefinition,
    WorkflowNotFoundError,
    WorkflowRegistry,
    WorkflowStep,
    WorkflowVersionConflictError,
)


def _make_workflow(workflow_id: str = "wf_climate", version: int = 1) -> WorkflowDefinition:
    return WorkflowDefinition(
        workflow_id=workflow_id,
        name=f"Workflow {workflow_id} v{version}",
        version=version,
        steps=[
            WorkflowStep(
                step_id="step_1",
                module_target="atlas-ingestion",
                action="ingest",
                parameters={"version": version},
            )
        ],
    )


def test_registry_register_get_get_latest_and_list() -> None:
    registry = WorkflowRegistry()
    v1 = _make_workflow("wf_alpha", version=1)
    v2 = _make_workflow("wf_alpha", version=2)
    other = _make_workflow("wf_beta", version=1)

    reg_v1 = registry.register(v1)
    reg_v2 = registry.register(v2)
    registry.register(other)

    assert reg_v1 == v1
    assert reg_v2 == v2
    assert registry.get("wf_alpha", version=1) == v1
    assert registry.get("wf_alpha", version=2) == v2
    assert registry.get("wf_alpha") == v2
    assert registry.get_latest("wf_alpha") == v2

    alpha_list = registry.list_workflows(workflow_id="wf_alpha")
    assert [w.version for w in alpha_list] == [1, 2]

    all_workflows = registry.list_workflows()
    assert [(w.workflow_id, w.version) for w in all_workflows] == [
        ("wf_alpha", 1),
        ("wf_alpha", 2),
        ("wf_beta", 1),
    ]


def test_registry_prevents_version_overwrite_and_validates_before_register() -> None:
    registry = WorkflowRegistry()
    wf = _make_workflow("wf_alpha", version=1)
    registry.register(wf)

    with pytest.raises(WorkflowVersionConflictError) as exc_info:
        registry.register(wf)
    assert isinstance(exc_info.value, DuplicateWorkflowError)
    assert exc_info.value.workflow_id == "wf_alpha"
    assert exc_info.value.workflow_version == 1

    # Invalid cyclic workflow must not be registered
    cyclic = WorkflowDefinition(
        workflow_id="wf_cyclic",
        name="Cyclic",
        version=1,
        steps=[
            WorkflowStep(step_id="a", module_target="m", action="run", depends_on=["b"]),
            WorkflowStep(step_id="b", module_target="m", action="run", depends_on=["a"]),
        ],
    )
    with pytest.raises(CyclicDependencyError):
        registry.register(cyclic)
    assert registry.list_workflows("wf_cyclic") == []


def test_registry_unregister_specific_version_and_all_versions() -> None:
    registry = WorkflowRegistry()
    registry.register(_make_workflow("wf_alpha", version=1))
    registry.register(_make_workflow("wf_alpha", version=2))
    registry.register(_make_workflow("wf_beta", version=1))

    registry.unregister("wf_alpha", version=2)
    assert registry.get_latest("wf_alpha").version == 1

    with pytest.raises(WorkflowNotFoundError):
        registry.get("wf_alpha", version=2)

    registry.unregister("wf_alpha")
    with pytest.raises(WorkflowNotFoundError):
        registry.get("wf_alpha")

    with pytest.raises(WorkflowNotFoundError):
        registry.unregister("wf_unknown")

    with pytest.raises(WorkflowNotFoundError):
        registry.unregister("wf_beta", version=99)

    with pytest.raises(InvalidWorkflowDefinitionError):
        registry.get("wf_beta", version=0)


def test_registry_concurrent_registration_thread_safety() -> None:
    registry = WorkflowRegistry()
    num_workers = 16
    barrier = threading.Barrier(num_workers)
    wf = _make_workflow("wf_concurrent", version=1)

    def attempt_register() -> str:
        barrier.wait(timeout=5)
        try:
            registry.register(wf)
            return "registered"
        except WorkflowVersionConflictError:
            return "conflict"

    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        futures = [pool.submit(attempt_register) for _ in range(num_workers)]
        outcomes = [f.result(timeout=10) for f in futures]

    assert outcomes.count("registered") == 1
    assert outcomes.count("conflict") == num_workers - 1
    assert len(registry.list_workflows("wf_concurrent")) == 1
