"""Partition path generation strategies."""

from __future__ import annotations

from datetime import datetime

from atlas_storage.exceptions import PartitioningError


class PartitionStrategy:
    """Generates partition path segments for lakehouse storage."""

    @staticmethod
    def by_date(
        execution_date: datetime,
    ) -> str:
        """Return a ``year=YYYY/month=MM/day=DD`` partition path for ``execution_date``."""
        if not isinstance(execution_date, datetime):
            raise PartitioningError(
                "PartitionStrategy.by_date requires a datetime, "
                f"got '{type(execution_date).__name__}'."
            )
        return f"year={execution_date:%Y}/month={execution_date:%m}/day={execution_date:%d}"


__all__ = ["PartitionStrategy"]
