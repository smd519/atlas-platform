"""YAML-backed ScheduleProvider implementation for Phase 1."""

from __future__ import annotations

import logging
from pathlib import Path

import yaml  # type: ignore[import-untyped]
from pydantic import ValidationError

from atlas_scheduler.exceptions import ScheduleValidationError
from atlas_scheduler.models import ScheduleDefinition

logger = logging.getLogger("atlas_scheduler.providers.yaml")


class YamlScheduleProvider:
    """Discovers, parses, and validates YAML schedule definitions from a workspace.

    Reads ``{workspace_path}/schedules/*.yml`` and ``*.yaml`` dynamically on every
    :meth:`get_schedules` call without caching state in memory.
    """

    def __init__(self, workspace_path: Path | str) -> None:
        self._workspace_path = Path(workspace_path).resolve()
        self._schedules_dir = self._workspace_path / "schedules"

    @property
    def workspace_path(self) -> Path:
        """Return the resolved physical customer workspace path."""
        return self._workspace_path

    @property
    def schedules_dir(self) -> Path:
        """Return the physical schedules directory ``{workspace_path}/schedules``."""
        return self._schedules_dir

    def get_schedules(self) -> list[ScheduleDefinition]:
        """Discover and return all valid :class:`ScheduleDefinition` models from disk."""
        if not self._schedules_dir.exists() or not self._schedules_dir.is_dir():
            logger.warning(
                "Schedules directory does not exist or is not a directory: %s",
                self._schedules_dir,
            )
            return []

        yaml_files = sorted(
            path
            for path in self._schedules_dir.iterdir()
            if path.is_file() and path.suffix.lower() in (".yml", ".yaml")
        )

        candidates_by_id: dict[str, list[tuple[Path, ScheduleDefinition]]] = {}

        for file_path in yaml_files:
            try:
                raw_text = file_path.read_text(encoding="utf-8")
            except OSError as exc:
                logger.error("Failed to read schedule YAML file '%s': %s", file_path, exc)
                continue

            try:
                parsed = yaml.safe_load(raw_text)
            except yaml.YAMLError as exc:
                logger.error("Malformed YAML in schedule file '%s': %s", file_path, exc)
                continue

            if not isinstance(parsed, dict):
                logger.error(
                    "Invalid schedule YAML in '%s': expected a top-level mapping, got %s.",
                    file_path,
                    type(parsed).__name__,
                )
                continue

            try:
                schedule = ScheduleDefinition.model_validate(parsed)
            except (ValidationError, ScheduleValidationError, ValueError) as exc:
                logger.error(
                    "Invalid schedule definition in '%s': %s",
                    file_path,
                    exc,
                )
                continue

            candidates_by_id.setdefault(schedule.schedule_id, []).append((file_path, schedule))

        schedules: list[ScheduleDefinition] = []
        for schedule_id, entries in candidates_by_id.items():
            if len(entries) > 1:
                conflicting_paths = ", ".join(str(fp) for fp, _ in entries)
                logger.error(
                    "Duplicate schedule_id '%s' defined in multiple files [%s]; "
                    "excluding conflicting definitions.",
                    schedule_id,
                    conflicting_paths,
                )
                continue
            schedules.append(entries[0][1])

        return schedules


__all__ = ["YamlScheduleProvider"]
