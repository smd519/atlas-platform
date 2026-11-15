"""Unified SQLAlchemy-backed :class:`MetadataRepository`."""

from __future__ import annotations

from typing import Any

from sqlalchemy import Engine
from sqlalchemy.engine import URL

from atlas_metadata.repositories.contracts import MetadataRepository
from atlas_metadata.repositories.sqlalchemy.artifact import SqlAlchemyArtifactRepository
from atlas_metadata.repositories.sqlalchemy.base import SessionFactory
from atlas_metadata.repositories.sqlalchemy.database import (
    create_metadata_engine,
    create_session_factory,
    initialize_database,
)
from atlas_metadata.repositories.sqlalchemy.dataset import SqlAlchemyDatasetRepository
from atlas_metadata.repositories.sqlalchemy.schema import (
    DEFAULT_MAX_ALLOCATION_ATTEMPTS,
    SqlAlchemySchemaRepository,
)


class SqlAlchemyMetadataRepository(
    SqlAlchemyDatasetRepository,
    SqlAlchemySchemaRepository,
    SqlAlchemyArtifactRepository,
    MetadataRepository,
):
    """Persistent :class:`MetadataRepository` that can be injected into ``MetadataService``.

    Example::

        repository = SqlAlchemyMetadataRepository.from_url("sqlite:////abs/path/catalog.db")
        service = MetadataService(repository=repository)
    """

    def __init__(
        self,
        session_factory: SessionFactory,
        *,
        engine: Engine | None = None,
        max_allocation_attempts: int = DEFAULT_MAX_ALLOCATION_ATTEMPTS,
    ) -> None:
        super().__init__(session_factory, max_allocation_attempts=max_allocation_attempts)
        self._engine = engine

    @classmethod
    def from_url(
        cls,
        database_url: str | URL,
        *,
        initialize: bool = True,
        max_allocation_attempts: int = DEFAULT_MAX_ALLOCATION_ATTEMPTS,
        **engine_kwargs: Any,
    ) -> SqlAlchemyMetadataRepository:
        """Create a repository for ``database_url``, creating missing tables when ``initialize``.

        Initialization never drops or recreates existing tables or data. ``engine_kwargs`` are
        forwarded to :func:`create_metadata_engine`.
        """
        engine = create_metadata_engine(database_url, **engine_kwargs)
        if initialize:
            initialize_database(engine)
        return cls(
            create_session_factory(engine),
            engine=engine,
            max_allocation_attempts=max_allocation_attempts,
        )

    @property
    def engine(self) -> Engine | None:
        """Return the engine this repository was created with via :meth:`from_url`, if any."""
        return self._engine

    def dispose(self) -> None:
        """Release all pooled connections held by the engine created via :meth:`from_url`."""
        if self._engine is not None:
            self._engine.dispose()


__all__ = ["SqlAlchemyMetadataRepository"]
