"""Extraction component for atlas-ingestion."""

from atlas_ingestion.extraction.extractor import (
    Extractor,
    create_default_connector_registry,
)

__all__ = [
    "Extractor",
    "create_default_connector_registry",
]
