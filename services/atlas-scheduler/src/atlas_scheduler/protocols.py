"""Persistence and dispatch protocol abstractions for atlas-scheduler."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from atlas_scheduler.models import ScheduleDefinition


@runtime_checkable
class ScheduleProvider(Protocol):
    """Generic persistence abstraction for retrieving validated schedule definitions."""

    def get_schedules(self) -> list[ScheduleDefinition]:
        """Return the current collection of validated :class:`ScheduleDefinition` models."""
        ...


__all__ = ["ScheduleProvider"]

