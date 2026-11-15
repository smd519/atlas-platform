"""SQLAlchemy declarative base and shared repository plumbing.

Everything in this package is infrastructure. The Pydantic domain models never import it.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import MetaData
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from atlas_metadata.exceptions import MetadataRepositoryError

# Explicit constraint naming keeps DDL deterministic across SQLite and PostgreSQL and makes
# a later move to migration tooling straightforward.
NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    """Declarative base for all atlas-metadata ORM records."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


SessionFactory = sessionmaker[Session]


@contextmanager
def translate_database_errors(operation: str) -> Iterator[None]:
    """Convert unexpected SQLAlchemy failures into :class:`MetadataRepositoryError`.

    :class:`IntegrityError` is deliberately passed through so that write operations can map
    constraint violations to the precise domain exception (duplicate dataset, schema conflict,
    and so on).
    """
    try:
        yield
    except IntegrityError:
        raise
    except SQLAlchemyError as exc:
        raise MetadataRepositoryError(
            f"Metadata repository operation '{operation}' failed: {exc}"
        ) from exc


class SqlAlchemyRepositoryBase:
    """Holds the session factory and workspace scope shared by SQLAlchemy repositories.

    Every repository method opens its own short-lived :class:`Session` from the factory (reads
    with ``with factory() as session``; writes with ``with factory.begin() as session``), so a
    session is never shared across threads or concurrent DAG tasks and its connection is always
    returned to the pool when the operation ends.
    """

    def __init__(self, session_factory: SessionFactory, *, workspace_id: str) -> None:
        if not isinstance(workspace_id, str) or not workspace_id.strip():
            raise ValueError("workspace_id must be a non-empty string.")
        self._session_factory = session_factory
        self._workspace_id = workspace_id.strip()

    @property
    def workspace_id(self) -> str:
        """Return the logical workspace identifier scoped to this repository instance."""
        return self._workspace_id


__all__ = [
    "NAMING_CONVENTION",
    "Base",
    "SessionFactory",
    "SqlAlchemyRepositoryBase",
    "translate_database_errors",
]
