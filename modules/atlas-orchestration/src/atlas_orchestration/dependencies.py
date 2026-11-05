"""Dependency injection boundaries for atlas-orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from atlas_core import MetadataProvider, StorageProvider


@runtime_checkable
class ModuleServiceProtocol(Protocol):
    """Marker protocol for injectable platform module services."""


@dataclass(frozen=True)
class OrchestrationDependencies:
    """Immutable dependency injection container for external module services.

    Holds optional references to platform services and :mod:`atlas_core` contracts
    without importing or instantiating concrete downstream module implementations.
    """

    ingestion_service: Any | None = None
    processing_service: Any | None = None
    quality_service: Any | None = None
    storage_provider: StorageProvider | Any | None = None
    metadata_provider: MetadataProvider | Any | None = None
    extra_services: dict[str, Any] = field(default_factory=dict)

    def resolve_service(self, module_target: str) -> Any | None:
        """Resolve an injected service for ``module_target``, or return ``None``."""
        clean = module_target.strip().lower() if isinstance(module_target, str) else ""
        if not clean:
            return None
        mapping: dict[str, Any | None] = {
            "ingestion": self.ingestion_service,
            "atlas-ingestion": self.ingestion_service,
            "atlas_ingestion": self.ingestion_service,
            "processing": self.processing_service,
            "atlas-processing": self.processing_service,
            "atlas_processing": self.processing_service,
            "quality": self.quality_service,
            "atlas-quality": self.quality_service,
            "atlas_quality": self.quality_service,
            "storage": self.storage_provider,
            "atlas-storage": self.storage_provider,
            "atlas_storage": self.storage_provider,
            "metadata": self.metadata_provider,
            "atlas-metadata": self.metadata_provider,
            "atlas_metadata": self.metadata_provider,
        }
        if clean in mapping and mapping[clean] is not None:
            return mapping[clean]
        return self.extra_services.get(module_target.strip()) or self.extra_services.get(clean)


__all__ = [
    "ModuleServiceProtocol",
    "OrchestrationDependencies",
]
