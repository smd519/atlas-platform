"""Scoped SQLAlchemy Session subclass for atlas-metadata persistence."""

from __future__ import annotations

from sqlalchemy.orm import Session


class MetadataSession(Session):
    """SQLAlchemy :class:`~sqlalchemy.orm.Session` subclass scoped to ``atlas-metadata``.

    Using a dedicated subclass ensures that metadata persistence rules and session configuration
    never leak onto the global :class:`sqlalchemy.orm.Session` class used by other modules.
    """


__all__ = ["MetadataSession"]

