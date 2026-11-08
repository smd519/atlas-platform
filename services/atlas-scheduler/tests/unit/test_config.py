"""Unit tests for atlas-scheduler workspace resolution, domain models, and service decoupling."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
from atlas_scheduler import (
    ScheduleDefinition,
    SchedulerConfigurationError,
    ScheduleValidationError,
    TriggerDefinition,
    WorkspaceContext,
    WorkspaceIsolationError,
    parse_cron_expression,
    resolve_workspace,
)
from pydantic import ValidationError


class TestWorkspaceResolution:
    """Standalone workspace resolution (ATLAS_WORKSPACE vs. absolute paths) and isolation."""

    def test_logical_workspace_id_resolution(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("ATLAS_WORKSPACE", str(tmp_path))
        customer_dir = tmp_path / "customer_001"
        customer_dir.mkdir(parents=True, exist_ok=True)

        ctx = resolve_workspace("customer_001")
        assert isinstance(ctx, WorkspaceContext)
        assert ctx.workspace_id == "customer_001"
        assert ctx.workspace_path == customer_dir.resolve()

    def test_absolute_workspace_path_resolution(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("ATLAS_WORKSPACE", raising=False)
        customer_dir = tmp_path / "customer_001"
        customer_dir.mkdir(parents=True, exist_ok=True)

        ctx = resolve_workspace(str(customer_dir.resolve()))
        assert ctx.workspace_id == str(customer_dir.resolve())
        assert ctx.workspace_path == customer_dir.resolve()

    def test_missing_atlas_workspace_for_logical_id_raises_configuration_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("ATLAS_WORKSPACE", raising=False)
        with pytest.raises(SchedulerConfigurationError, match="ATLAS_WORKSPACE"):
            resolve_workspace("customer_001")

    def test_nonexistent_workspace_raises_configuration_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("ATLAS_WORKSPACE", str(tmp_path))
        with pytest.raises(SchedulerConfigurationError, match="does not exist"):
            resolve_workspace("missing_customer")

        with pytest.raises(SchedulerConfigurationError, match="does not exist"):
            resolve_workspace(tmp_path / "missing_abs_dir")

        file_path = tmp_path / "not_a_dir.txt"
        file_path.write_text("hello", encoding="utf-8")
        with pytest.raises(SchedulerConfigurationError, match="not a directory"):
            resolve_workspace(file_path)

    def test_path_traversal_and_workspace_isolation(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        base = tmp_path / "managed_root"
        base.mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("ATLAS_WORKSPACE", str(base))

        for malicious in ("../../etc/shadow", "../outside", "sub/nested", "a\\b", "."):
            with pytest.raises((WorkspaceIsolationError, SchedulerConfigurationError)):
                resolve_workspace(malicious)

    def test_empty_workspace_raises_configuration_error(self) -> None:
        with pytest.raises(SchedulerConfigurationError, match="must not be empty"):
            resolve_workspace("   ")


class TestScheduleAndTriggerDomainModels:
    """Validation of ScheduleDefinition, TriggerDefinition, cron expressions, and timezones."""

    def test_valid_schedule_definition(self) -> None:
        schedule = ScheduleDefinition(
            schedule_id="daily_morning",
            name="Daily Morning Batch",
            enabled=True,
            trigger=TriggerDefinition(
                type="cron",
                expression="0 6 * * *",
                timezone="America/Toronto",
            ),
            jobs=["morning_batch", "financials_batch"],
        )
        assert schedule.schedule_id == "daily_morning"
        assert schedule.name == "Daily Morning Batch"
        assert schedule.enabled is True
        assert schedule.trigger.type == "cron"
        assert schedule.trigger.expression == "0 6 * * *"
        assert schedule.trigger.timezone == "America/Toronto"
        assert schedule.jobs == ["morning_batch", "financials_batch"]

    def test_disabled_schedule_is_valid_domain_model(self) -> None:
        schedule = ScheduleDefinition(
            schedule_id="disabled_schedule",
            name="Paused Schedule",
            enabled=False,
            trigger=TriggerDefinition(
                type="cron",
                expression="*/15 * * * *",
                timezone="UTC",
            ),
            jobs=["morning_batch"],
        )
        assert schedule.enabled is False

    def test_invalid_schedule_definitions_rejected(self) -> None:
        valid_trigger = TriggerDefinition(
            type="cron",
            expression="0 6 * * *",
            timezone="UTC",
        )

        # Empty schedule_id
        with pytest.raises((ValidationError, ScheduleValidationError)):
            ScheduleDefinition(
                schedule_id="   ",
                trigger=valid_trigger,
                jobs=["morning_batch"],
            )

        # Empty jobs list
        with pytest.raises((ValidationError, ScheduleValidationError)):
            ScheduleDefinition(
                schedule_id="no_jobs",
                trigger=valid_trigger,
                jobs=[],
            )

        # Blank job ID inside jobs list
        with pytest.raises((ValidationError, ScheduleValidationError)):
            ScheduleDefinition(
                schedule_id="blank_job",
                trigger=valid_trigger,
                jobs=["morning_batch", "  "],
            )

    def test_invalid_trigger_definitions_rejected(self) -> None:
        # Unsupported trigger type
        with pytest.raises((ValidationError, ScheduleValidationError), match="Only 'cron'"):
            TriggerDefinition(
                type="interval",
                expression="0 6 * * *",
                timezone="UTC",
            )

        # Invalid cron expression (wrong field count, out of bounds, impossible date)
        for bad_cron in ("0 6 * *", "60 6 * * *", "0 25 * * *", "0 6 31 2 *"):
            with pytest.raises((ValidationError, ScheduleValidationError)):
                TriggerDefinition(
                    type="cron",
                    expression=bad_cron,
                    timezone="UTC",
                )

        # Invalid IANA timezone
        with pytest.raises((ValidationError, ScheduleValidationError), match="Invalid IANA"):
            TriggerDefinition(
                type="cron",
                expression="0 6 * * *",
                timezone="Not/A_Real_Timezone",
            )

    def test_cron_parser_supports_ranges_steps_lists_and_names(self) -> None:
        parsed = parse_cron_expression("0,30 8-17/3 * JAN,JUN MON-FRI")
        assert parsed.minutes == (0, 30)
        assert parsed.hours == (8, 11, 14, 17)
        assert parsed.months == frozenset({1, 6})
        assert parsed.days_of_week == frozenset({1, 2, 3, 4, 5})


class TestStrictServiceDecoupling:
    """Verify atlas_scheduler never imports anything from atlas_runner."""

    def test_no_atlas_runner_imports_anywhere_in_atlas_scheduler(self) -> None:
        scheduler_src = Path(__file__).resolve().parents[2] / "src" / "atlas_scheduler"
        py_files = list(scheduler_src.rglob("*.py"))
        assert len(py_files) > 0

        for py_file in py_files:
            tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        assert not alias.name.startswith("atlas_runner"), (
                            f"Forbidden import '{alias.name}' in {py_file}"
                        )
                elif isinstance(node, ast.ImportFrom) and node.module:
                    assert not node.module.startswith("atlas_runner"), (
                        f"Forbidden from-import '{node.module}' in {py_file}"
                    )
