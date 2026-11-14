"""Unit tests for atlas-runner dependency wiring and shared metadata provider injection."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from atlas_core import MetadataProvider
from atlas_ingestion import IngestionService
from atlas_metadata import MetadataService
from atlas_processing import ProcessingService
from atlas_quality import QualityService
from atlas_runner.dependencies import (
    build_default_dependencies,
    create_orchestration_service,
)


def test_default_dependencies_instantiates_exactly_one_shared_metadata_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exactly one MetadataService must be instantiated and shared across all consumers."""
    instantiation_count = 0
    real_init = MetadataService.__init__

    def tracked_init(self: MetadataService, *args: Any, **kwargs: Any) -> None:
        nonlocal instantiation_count
        instantiation_count += 1
        real_init(self, *args, **kwargs)

    monkeypatch.setattr(MetadataService, "__init__", tracked_init)

    deps = build_default_dependencies()

    # 1. Verify instantiation count
    assert instantiation_count == 1, (
        f"Expected exactly 1 MetadataService instantiation, got {instantiation_count}"
    )

    # 2. Verify OrchestrationDependencies has the shared instance
    shared_metadata = deps.metadata_provider
    assert shared_metadata is not None
    assert isinstance(shared_metadata, MetadataService)

    # 3. Verify Ingestion, Processing, and Quality receive the identical instance
    assert isinstance(deps.ingestion_service, IngestionService)
    assert isinstance(deps.processing_service, ProcessingService)
    assert isinstance(deps.quality_service, QualityService)

    assert deps.ingestion_service._pipeline._metadata is shared_metadata
    assert deps.processing_service._pipeline._metadata is shared_metadata
    assert deps.quality_service._pipeline._metadata is shared_metadata


def test_explicit_metadata_provider_is_propagated_to_all_services() -> None:
    """Explicit metadata_provider is propagated without creating a new MetadataService."""
    mock_provider = MagicMock(spec=MetadataProvider)

    deps = build_default_dependencies(metadata_provider=mock_provider)

    assert deps.metadata_provider is mock_provider
    assert isinstance(deps.ingestion_service, IngestionService)
    assert isinstance(deps.processing_service, ProcessingService)
    assert isinstance(deps.quality_service, QualityService)

    assert deps.ingestion_service._pipeline._metadata is mock_provider
    assert deps.processing_service._pipeline._metadata is mock_provider
    assert deps.quality_service._pipeline._metadata is mock_provider


def test_falsy_injected_metadata_provider_is_not_replaced_by_default() -> None:
    """A deliberately injected metadata_provider that evaluates as falsy is preserved."""

    class FalsyMetadataProvider(MetadataProvider):
        def register_dataset(self, dataset: Any, *, overwrite: bool = False) -> Any:
            return None

        def get_dataset(self, dataset_id: str) -> Any:
            return None

        def __bool__(self) -> bool:
            return False

    falsy_provider = FalsyMetadataProvider()
    assert bool(falsy_provider) is False

    deps = build_default_dependencies(metadata_provider=falsy_provider)

    assert deps.metadata_provider is falsy_provider
    assert isinstance(deps.ingestion_service, IngestionService)
    assert isinstance(deps.processing_service, ProcessingService)
    assert isinstance(deps.quality_service, QualityService)

    assert deps.ingestion_service._pipeline._metadata is falsy_provider
    assert deps.processing_service._pipeline._metadata is falsy_provider
    assert deps.quality_service._pipeline._metadata is falsy_provider


def test_create_orchestration_service_wires_shared_metadata() -> None:
    """OrchestrationService created with default dependencies receives the shared provider."""
    service = create_orchestration_service()

    deps = service.dependencies
    shared_metadata = deps.metadata_provider
    assert shared_metadata is not None
    assert isinstance(shared_metadata, MetadataService)

    assert isinstance(deps.ingestion_service, IngestionService)
    assert isinstance(deps.processing_service, ProcessingService)
    assert isinstance(deps.quality_service, QualityService)

    assert deps.ingestion_service._pipeline._metadata is shared_metadata
    assert deps.processing_service._pipeline._metadata is shared_metadata
    assert deps.quality_service._pipeline._metadata is shared_metadata
