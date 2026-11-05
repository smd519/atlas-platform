"""Execution tracking contract for atlas-orchestration."""

from __future__ import annotations

from abc import ABC

from atlas_orchestration.execution.state import StateManager


class ExecutionTracker(StateManager, ABC):
    """Abstract contract for tracking workflow and step execution lifecycles.

    Consolidates :class:`StateManager` and :class:`atlas_core.ExecutionManager`
    into a single authoritative interface for execution-state persistence.
    """


__all__ = ["ExecutionTracker"]

