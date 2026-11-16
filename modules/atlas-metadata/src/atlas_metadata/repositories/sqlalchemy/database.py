"""Engine, session factory, and schema initialization for SQLAlchemy metadata persistence.

The database is selected purely by a SQLAlchemy URL, for example::

    sqlite:////absolute/workspace/path/metadata/catalog.db
    postgresql+psycopg://user:password@host:5432/atlas      (future)

Nothing here hardcodes a path. SQLite specifics are confined to the clearly separated
"SQLite" section below; general initialization is backend-neutral.

SQLite limitation: SQLite allows a single writer at a time. WAL mode lets readers proceed while
a write is in progress, and ``busy_timeout`` makes a second writer wait instead of failing
immediately, but write throughput is still serialized. Use a client/server database for heavy
concurrent writes.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.engine import URL, make_url
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from atlas_metadata.repositories.sqlalchemy import models as _models  # noqa: F401  (registers tables)
from atlas_metadata.repositories.sqlalchemy.base import Base
from atlas_metadata.repositories.sqlalchemy.events import register_session_events
from atlas_metadata.repositories.sqlalchemy.session import MetadataSession

DEFAULT_SQLITE_TIMEOUT_SECONDS = 30.0


def create_metadata_engine(
    database_url: str | URL,
    *,
    echo: bool = False,
    sqlite_timeout: float = DEFAULT_SQLITE_TIMEOUT_SECONDS,
    **engine_kwargs: Any,
) -> Engine:
    """Create an :class:`~sqlalchemy.Engine` for ``database_url``.

    SQLite-specific connection configuration is applied only when the URL targets SQLite.
    Extra ``engine_kwargs`` are forwarded to :func:`sqlalchemy.create_engine`.
    """
    url = make_url(database_url)
    if url.get_backend_name() == "sqlite":
        return _create_sqlite_engine(url, echo=echo, timeout=sqlite_timeout, **engine_kwargs)
    return create_engine(url, echo=echo, **engine_kwargs)


def create_session_factory(engine: Engine) -> sessionmaker[MetadataSession]:
    """Return a :class:`MetadataSession` factory bound to ``engine`` with ownership checks.

    ``expire_on_commit=False`` lets repositories map records to domain models after commit
    without triggering extra queries. Sessions are short-lived and never shared.
    """
    factory = sessionmaker(bind=engine, class_=MetadataSession, expire_on_commit=False)
    register_session_events(factory)
    return factory


def initialize_database(engine: Engine) -> None:
    """Create any missing metadata tables.

    This is explicit and repeatable: it uses ``create_all`` with ``checkfirst`` semantics, so it
    never drops, recreates, or alters existing tables or data.
    """
    Base.metadata.create_all(engine)


# ----------------------------------------------------------------------
# SQLite (isolated)
# ----------------------------------------------------------------------


def _is_in_memory_sqlite(url: URL) -> bool:
    return url.database in (None, "", ":memory:") or url.query.get("mode") == "memory"


def _ensure_sqlite_parent_directory(url: URL) -> None:
    """Create the parent directory of a file-based SQLite database."""
    if _is_in_memory_sqlite(url) or url.database is None or url.query.get("uri") == "true":
        return
    Path(url.database).expanduser().parent.mkdir(parents=True, exist_ok=True)


def _create_sqlite_engine(
    url: URL,
    *,
    echo: bool,
    timeout: float,
    **engine_kwargs: Any,
) -> Engine:
    in_memory = _is_in_memory_sqlite(url)
    if not in_memory:
        _ensure_sqlite_parent_directory(url)

    connect_args: dict[str, Any] = dict(engine_kwargs.pop("connect_args", {}))
    connect_args.setdefault("timeout", timeout)
    # Sessions are opened per operation and each connection is used by one thread at a time,
    # so pooled connections may safely be handed to a different thread.
    connect_args.setdefault("check_same_thread", False)

    if in_memory:
        # A private in-memory database exists per connection; share a single connection so the
        # schema and data are visible to every session. Intended for tests and single-threaded
        # use only.
        engine_kwargs.setdefault("poolclass", StaticPool)

    engine = create_engine(url, echo=echo, connect_args=connect_args, **engine_kwargs)

    @event.listens_for(engine, "connect")
    def _configure_connection(dbapi_connection: Any, _record: Any) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
            if not in_memory:
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
        finally:
            cursor.close()

    return engine


__all__ = [
    "DEFAULT_SQLITE_TIMEOUT_SECONDS",
    "create_metadata_engine",
    "create_session_factory",
    "initialize_database",
]
