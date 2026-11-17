"""SQLAlchemy persistence backend for atlas-metadata repositories.

Importing this package requires SQLAlchemy; the in-memory repository does not.
"""

from atlas_metadata.repositories.sqlalchemy.artifact import SqlAlchemyArtifactRepository
from atlas_metadata.repositories.sqlalchemy.base import Base, SessionFactory
from atlas_metadata.repositories.sqlalchemy.database import (
    create_metadata_engine,
    create_session_factory,
    initialize_database,
)
from atlas_metadata.repositories.sqlalchemy.dataset import SqlAlchemyDatasetRepository
from atlas_metadata.repositories.sqlalchemy.events import (
    register_session_events,
    validate_workspace_ownership,
)
from atlas_metadata.repositories.sqlalchemy.models import TenantOwnedMixin
from atlas_metadata.repositories.sqlalchemy.repository import SqlAlchemyMetadataRepository
from atlas_metadata.repositories.sqlalchemy.schema import SqlAlchemySchemaRepository
from atlas_metadata.repositories.sqlalchemy.session import MetadataSession

__all__ = [
    "Base",
    "MetadataSession",
    "SessionFactory",
    "SqlAlchemyArtifactRepository",
    "SqlAlchemyDatasetRepository",
    "SqlAlchemyMetadataRepository",
    "SqlAlchemySchemaRepository",
    "TenantOwnedMixin",
    "create_metadata_engine",
    "create_session_factory",
    "initialize_database",
    "register_session_events",
    "validate_workspace_ownership",
]
