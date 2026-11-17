"""Write-path workspace ownership validation for tenant-owned SQLAlchemy ORM records."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from sqlalchemy import event, inspect
from sqlalchemy.orm import Session, UOWTransaction, sessionmaker

from atlas_metadata.repositories.sqlalchemy.models import TenantOwnedMixin


def _iter_tenant_entities(items: Iterable[object]) -> list[TenantOwnedMixin]:
    return [item for item in items if isinstance(item, TenantOwnedMixin)]


def validate_workspace_ownership(
    session: Session,
    flush_context: UOWTransaction,
    instances: object,
) -> None:
    """Enforce workspace ownership invariants on ``session.new``, ``dirty``, and ``deleted``.

    Rules:
    1. Entities not inheriting from :class:`TenantOwnedMixin` (platform-global entities) are
       ignored and do not require ``session.info["workspace_id"]``.
    2. When any :class:`TenantOwnedMixin` entity is flushed, ``session.info.get("workspace_id")``
       must be a non-empty string (fail closed with :class:`ValueError` otherwise).
    3. ``session.new``: ``entity.workspace_id`` must equal ``session.info["workspace_id"]``.
    4. ``session.dirty``: ``inspect(entity).attrs.workspace_id.history`` must have no changes
       (otherwise raises ``ValueError("Cannot mutate workspace_id of a persistent entity")``),
       and ``entity.workspace_id`` must equal ``session.info["workspace_id"]``.
    5. ``session.deleted``: the original persisted ``workspace_id`` from attribute history (and
       current ``entity.workspace_id``) must equal ``session.info["workspace_id"]``.
    """
    del flush_context, instances

    new_tenants = _iter_tenant_entities(session.new)
    dirty_tenants = _iter_tenant_entities(session.dirty)
    deleted_tenants = _iter_tenant_entities(session.deleted)

    if not new_tenants and not dirty_tenants and not deleted_tenants:
        return

    raw_session_ws = session.info.get("workspace_id")
    if not isinstance(raw_session_ws, str) or not raw_session_ws.strip():
        raise ValueError(
            "Cannot flush tenant-owned metadata entity without a valid 'workspace_id' "
            "in session.info."
        )
    session_ws = raw_session_ws.strip()

    for entity in new_tenants:
        entity_ws = getattr(entity, "workspace_id", None)
        if not isinstance(entity_ws, str) or entity_ws != session_ws:
            raise ValueError(
                f"Workspace ownership mismatch on insert for {type(entity).__name__}: "
                f"entity workspace_id={entity_ws!r} does not match "
                f"session workspace_id={session_ws!r}."
            )

    for entity in dirty_tenants:
        state = inspect(entity)
        assert state is not None
        ws_history = state.attrs.workspace_id.history
        if ws_history.has_changes():
            raise ValueError("Cannot mutate workspace_id of a persistent entity")

        entity_ws = getattr(entity, "workspace_id", None)
        if not isinstance(entity_ws, str) or entity_ws != session_ws:
            raise ValueError(
                f"Workspace ownership mismatch on update for {type(entity).__name__}: "
                f"entity workspace_id={entity_ws!r} does not match "
                f"session workspace_id={session_ws!r}."
            )

    for entity in deleted_tenants:
        state = inspect(entity)
        assert state is not None
        ws_history = state.attrs.workspace_id.history
        if ws_history.deleted:
            original_ws: Any = ws_history.deleted[0]
        elif ws_history.unchanged:
            original_ws = ws_history.unchanged[0]
        else:
            original_ws = getattr(entity, "workspace_id", None)

        current_ws = getattr(entity, "workspace_id", None)
        if (
            not isinstance(original_ws, str)
            or original_ws != session_ws
            or current_ws != session_ws
        ):
            raise ValueError(
                f"Workspace ownership mismatch on delete for {type(entity).__name__}: "
                f"persisted workspace_id={original_ws!r} does not match "
                f"session workspace_id={session_ws!r}."
            )


def register_session_events(target: sessionmaker[Any] | type[Session] | Session) -> None:
    """Register :func:`validate_workspace_ownership` on ``target`` if not already registered."""
    if not event.contains(target, "before_flush", validate_workspace_ownership):
        event.listen(target, "before_flush", validate_workspace_ownership)


__all__ = [
    "register_session_events",
    "validate_workspace_ownership",
]
