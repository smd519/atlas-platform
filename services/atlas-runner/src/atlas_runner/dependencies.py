"""Platform service initialization and dependency wiring for atlas-runner."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from atlas_connectors import ConnectorRegistry, LocalFileConnector, RestConnector
from atlas_core import MetadataProvider, StorageProvider
from atlas_ingestion import IngestionService
from atlas_metadata import MetadataService
from atlas_orchestration import (
    OrchestrationDependencies,
    OrchestrationService,
    WorkspaceContext,
)
from atlas_processing import ProcessingService
from atlas_quality import QualityService
from atlas_storage import FileSystemBackend, StorageManager

from atlas_runner.config import (
    load_workspace_config,
    resolve_metadata_database_url,
)
from atlas_runner.exceptions import RunnerConfigurationError
from atlas_runner.models import MetadataConfig, WorkspaceConfig


def _resolve_metadata_config(
    *,
    workspace: str | Path | WorkspaceContext | None = None,
    workspace_config: WorkspaceConfig | Mapping[str, Any] | None = None,
    metadata_config: MetadataConfig | Mapping[str, Any] | None = None,
) -> MetadataConfig:
    """Resolve the effective :class:`MetadataConfig` from explicit input or workspace files."""
    if metadata_config is not None:
        if isinstance(metadata_config, MetadataConfig):
            return metadata_config
        if isinstance(metadata_config, Mapping):
            try:
                return MetadataConfig.model_validate(dict(metadata_config))
            except Exception as exc:
                raise RunnerConfigurationError(
                    f"Invalid metadata configuration: {exc}",
                    field="metadata",
                ) from exc
        raise RunnerConfigurationError(
            f"Invalid metadata_config type: expected MetadataConfig or mapping, "
            f"got {type(metadata_config).__name__}.",
            field="metadata",
        )

    if workspace_config is not None:
        if isinstance(workspace_config, WorkspaceConfig):
            return workspace_config.metadata
        if isinstance(workspace_config, Mapping):
            try:
                parsed_ws = WorkspaceConfig.model_validate(dict(workspace_config))
            except Exception as exc:
                raise RunnerConfigurationError(
                    f"Invalid workspace configuration: {exc}",
                    field="metadata",
                ) from exc
            return parsed_ws.metadata
        raise RunnerConfigurationError(
            f"Invalid workspace_config type: expected WorkspaceConfig or mapping, "
            f"got {type(workspace_config).__name__}.",
            field="workspace_config",
        )

    if workspace is not None:
        loaded_ws_cfg, _ = load_workspace_config(workspace)
        return loaded_ws_cfg.metadata

    return MetadataConfig()


def build_metadata_service(
    *,
    workspace: str | Path | WorkspaceContext | None = None,
    workspace_config: WorkspaceConfig | Mapping[str, Any] | None = None,
    metadata_config: MetadataConfig | Mapping[str, Any] | None = None,
) -> MetadataService:
    """Construct a shared :class:`MetadataService` according to workspace metadata configuration."""
    resolved_cfg = _resolve_metadata_config(
        workspace=workspace,
        workspace_config=workspace_config,
        metadata_config=metadata_config,
    )

    if resolved_cfg.backend == "memory":
        return MetadataService()

    if resolved_cfg.backend == "sqlalchemy":
        if not resolved_cfg.database_url:
            raise RunnerConfigurationError(
                "metadata.database_url is required when metadata.backend is 'sqlalchemy'.",
                field="metadata.database_url",
            )
        resolved_url = resolve_metadata_database_url(
            resolved_cfg.database_url,
            workspace=workspace,
            create_dirs=True,
            field="metadata.database_url",
        )
        from atlas_metadata.repositories.sqlalchemy import SqlAlchemyMetadataRepository

        repository = SqlAlchemyMetadataRepository.from_url(resolved_url)
        return MetadataService(repository=repository)

    raise RunnerConfigurationError(
        f"Unsupported metadata backend '{resolved_cfg.backend}'. "
        "Supported backends: 'memory', 'sqlalchemy'.",
        field="metadata.backend",
    )


def build_default_dependencies(
    *,
    workspace: str | Path | WorkspaceContext | None = None,
    workspace_config: WorkspaceConfig | Mapping[str, Any] | None = None,
    metadata_config: MetadataConfig | Mapping[str, Any] | None = None,
    storage_backend: StorageProvider | None = None,
    storage_manager: StorageManager | None = None,
    connector_registry: ConnectorRegistry | None = None,
    metadata_provider: MetadataProvider | None = None,
    ingestion_service: IngestionService | None = None,
    processing_service: ProcessingService | None = None,
    quality_service: QualityService | None = None,
) -> OrchestrationDependencies:
    """Initialize platform services and return an :class:`OrchestrationDependencies` container."""
    backend = storage_backend or FileSystemBackend()
    resolved_storage = storage_manager or StorageManager(backend=backend)

    resolved_connectors = connector_registry or ConnectorRegistry()
    if not resolved_connectors.exists("local_file"):
        resolved_connectors.register("local_file", LocalFileConnector)
    if not resolved_connectors.exists("rest"):
        resolved_connectors.register("rest", RestConnector)

    shared_metadata: MetadataProvider = (
        metadata_provider
        if metadata_provider is not None
        else build_metadata_service(
            workspace=workspace,
            workspace_config=workspace_config,
            metadata_config=metadata_config,
        )
    )

    resolved_ingestion = ingestion_service or IngestionService(
        connector_registry=resolved_connectors,
        storage_manager=resolved_storage,
        metadata_provider=shared_metadata,
    )
    resolved_processing = processing_service or ProcessingService(
        storage_manager=resolved_storage,
        metadata_provider=shared_metadata,
    )
    resolved_quality = quality_service or QualityService(
        storage_manager=resolved_storage,
        metadata_provider=shared_metadata,
    )

    return OrchestrationDependencies(
        ingestion_service=resolved_ingestion,
        processing_service=resolved_processing,
        quality_service=resolved_quality,
        storage_provider=backend,
        metadata_provider=shared_metadata,
    )


def create_orchestration_service(
    dependencies: OrchestrationDependencies | None = None,
    *,
    workspace: str | Path | WorkspaceContext | None = None,
    workspace_config: WorkspaceConfig | Mapping[str, Any] | None = None,
    metadata_config: MetadataConfig | Mapping[str, Any] | None = None,
) -> OrchestrationService:
    """Create an :class:`OrchestrationService` wired with platform dependencies."""
    resolved_deps = dependencies or build_default_dependencies(
        workspace=workspace,
        workspace_config=workspace_config,
        metadata_config=metadata_config,
    )
    return OrchestrationService(dependencies=resolved_deps)


__all__ = [
    "build_default_dependencies",
    "build_metadata_service",
    "create_orchestration_service",
]
