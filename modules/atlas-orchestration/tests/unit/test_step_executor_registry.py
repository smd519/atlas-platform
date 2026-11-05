"""Unit tests for StepExecutorRegistry."""

from __future__ import annotations

import pytest
from atlas_orchestration import (
    DuplicateExecutorRegistrationError,
    InvalidStepDefinitionError,
    MockStepExecutor,
    StepExecutorRegistry,
    UnknownModuleTargetError,
)


def test_step_executor_registry_register_resolve_and_list() -> None:
    registry = StepExecutorRegistry()
    ingest_exec = MockStepExecutor()
    process_exec = MockStepExecutor()

    registry.register("atlas-ingestion", ingest_exec)
    registry.register("ATLAS-PROCESSING", process_exec)

    assert registry.has_executor("atlas-ingestion") is True
    assert registry.has_executor("atlas-processing") is True
    assert registry.has_executor("unknown") is False
    assert registry.resolve("atlas-ingestion") is ingest_exec
    assert registry.get("atlas-processing") is process_exec
    assert registry.list_targets() == ["atlas-ingestion", "atlas-processing"]


def test_step_executor_registry_prevents_duplicates_unless_overwrite() -> None:
    registry = StepExecutorRegistry()
    exec_1 = MockStepExecutor()
    exec_2 = MockStepExecutor()

    registry.register("atlas-ingestion", exec_1)
    with pytest.raises(DuplicateExecutorRegistrationError) as exc_info:
        registry.register("atlas-ingestion", exec_2)
    assert exc_info.value.module_target == "atlas-ingestion"
    assert registry.resolve("atlas-ingestion") is exec_1

    registry.register("atlas-ingestion", exec_2, overwrite=True)
    assert registry.resolve("atlas-ingestion") is exec_2


def test_step_executor_registry_unknown_target_and_validation() -> None:
    registry = StepExecutorRegistry()
    registry.register("atlas-ingestion", MockStepExecutor())

    with pytest.raises(UnknownModuleTargetError) as exc_info:
        registry.resolve("atlas-unknown")
    assert exc_info.value.module_target == "atlas-unknown"

    with pytest.raises(UnknownModuleTargetError):
        registry.resolve("   ")

    with pytest.raises(InvalidStepDefinitionError):
        registry.register("", MockStepExecutor())

    with pytest.raises(InvalidStepDefinitionError):
        registry.register("atlas-quality", "not-an-executor")  # type: ignore[arg-type]

    registry.unregister("atlas-ingestion")
    assert registry.has_executor("atlas-ingestion") is False
    with pytest.raises(UnknownModuleTargetError):
        registry.unregister("atlas-ingestion")
