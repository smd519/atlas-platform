"""Unit tests for YamlScheduleProvider discovery, validation, error resilience, and reloading."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
from atlas_scheduler import ScheduleDefinition, YamlScheduleProvider


def _write_schedule(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


class TestYamlScheduleProvider:
    """YamlScheduleProvider discovery, error handling, and dynamic reload behavior."""

    def test_discovers_valid_yml_and_yaml_schedules(self, tmp_path: Path) -> None:
        _write_schedule(
            tmp_path / "schedules" / "01_daily.yml",
            """
schedule_id: daily_morning
name: Daily Morning Batch
enabled: true
trigger:
  type: cron
  expression: "0 6 * * *"
  timezone: America/Toronto
jobs:
  - morning_batch
  - financials_batch
""",
        )
        _write_schedule(
            tmp_path / "schedules" / "02_hourly.yaml",
            """
schedule_id: hourly_sensors
name: Hourly Sensors
enabled: false
trigger:
  type: cron
  expression: "0 * * * *"
  timezone: UTC
jobs:
  - sensors_job
""",
        )

        provider = YamlScheduleProvider(workspace_path=tmp_path)
        schedules = provider.get_schedules()

        assert len(schedules) == 2
        assert all(isinstance(s, ScheduleDefinition) for s in schedules)
        assert schedules[0].schedule_id == "daily_morning"
        assert schedules[0].enabled is True
        assert schedules[0].jobs == ["morning_batch", "financials_batch"]
        assert schedules[1].schedule_id == "hourly_sensors"
        assert schedules[1].enabled is False
        assert schedules[1].jobs == ["sensors_job"]

    def test_handles_missing_schedules_directory_gracefully(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        provider = YamlScheduleProvider(workspace_path=tmp_path)
        with caplog.at_level(logging.WARNING, logger="atlas_scheduler.providers.yaml"):
            schedules = provider.get_schedules()

        assert schedules == []
        assert "Schedules directory does not exist" in caplog.text

    def test_logs_malformed_yaml_and_invalid_models_while_loading_valid_files(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # 1. Malformed YAML syntax
        _write_schedule(
            tmp_path / "schedules" / "01_broken.yml",
            "schedule_id: [unclosed_list\ntrigger: ::bad",
        )
        # 2. Invalid Pydantic model (bad cron expression)
        _write_schedule(
            tmp_path / "schedules" / "02_invalid_model.yml",
            """
schedule_id: invalid_cron_sched
trigger:
  type: cron
  expression: "not a cron"
  timezone: UTC
jobs:
  - some_job
""",
        )
        # 3. Non-mapping YAML root
        _write_schedule(
            tmp_path / "schedules" / "03_list_root.yml",
            "- item1\n- item2\n",
        )
        # 4. Valid schedule file
        _write_schedule(
            tmp_path / "schedules" / "04_valid.yml",
            """
schedule_id: valid_schedule
name: Valid Schedule
trigger:
  type: cron
  expression: "15 8 * * *"
  timezone: UTC
jobs:
  - valid_job
""",
        )

        provider = YamlScheduleProvider(workspace_path=tmp_path)
        with caplog.at_level(logging.ERROR, logger="atlas_scheduler.providers.yaml"):
            schedules = provider.get_schedules()

        assert len(schedules) == 1
        assert schedules[0].schedule_id == "valid_schedule"
        assert "Malformed YAML in schedule file" in caplog.text
        assert "Invalid schedule definition" in caplog.text
        assert "expected a top-level mapping" in caplog.text

    def test_reloads_newly_added_and_modified_schedule_files_dynamically(
        self, tmp_path: Path
    ) -> None:
        sched_1 = _write_schedule(
            tmp_path / "schedules" / "daily.yml",
            """
schedule_id: daily_morning
name: Version 1
enabled: true
trigger:
  type: cron
  expression: "0 6 * * *"
  timezone: America/Toronto
jobs:
  - morning_batch
""",
        )

        provider = YamlScheduleProvider(workspace_path=tmp_path)
        first_load = provider.get_schedules()
        assert len(first_load) == 1
        assert first_load[0].name == "Version 1"
        assert first_load[0].jobs == ["morning_batch"]

        # Modify existing file and add a second schedule file without recreating provider
        _write_schedule(
            sched_1,
            """
schedule_id: daily_morning
name: Version 2
enabled: true
trigger:
  type: cron
  expression: "30 7 * * *"
  timezone: America/Toronto
jobs:
  - morning_batch
  - extra_batch
""",
        )
        _write_schedule(
            tmp_path / "schedules" / "evening.yaml",
            """
schedule_id: evening_batch
name: Evening Batch
enabled: true
trigger:
  type: cron
  expression: "0 20 * * *"
  timezone: UTC
jobs:
  - night_job
""",
        )

        second_load = provider.get_schedules()
        assert len(second_load) == 2
        by_id = {s.schedule_id: s for s in second_load}
        assert by_id["daily_morning"].name == "Version 2"
        assert by_id["daily_morning"].trigger.expression == "30 7 * * *"
        assert by_id["daily_morning"].jobs == ["morning_batch", "extra_batch"]
        assert by_id["evening_batch"].jobs == ["night_job"]

