"""Execution tracking abstractions for atlas-ingestion."""

from atlas_ingestion.tracking.execution_tracker import (
    ExecutionTracker,
    IngestionExecutionRecord,
)

__all__ = [
    "ExecutionTracker",
    "IngestionExecutionRecord",
]
