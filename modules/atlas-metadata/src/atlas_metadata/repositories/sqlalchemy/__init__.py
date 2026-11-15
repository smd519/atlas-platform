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
from atlas_metadata.repositories.sqlalchemy.repository import SqlAlchemyMetadataRepository
from atlas_metadata.repositories.sqlalchemy.schema import SqlAlchemySchemaRepository

__all__ = [
    "Base",
    "SessionFactory",
    "SqlAlchemyArtifactRepository",
    "SqlAlchemyDatasetRepository",
    "SqlAlchemyMetadataRepository",
    "SqlAlchemySchemaRepository",
    "create_metadata_engine",
    "create_session_factory",
    "initialize_database",
]
