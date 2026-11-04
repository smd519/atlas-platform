"""Loading component for atlas-ingestion."""

from atlas_ingestion.loading.loader import (
    Loader,
    create_default_storage_registry,
)

__all__ = [
    "Loader",
    "create_default_storage_registry",
]
