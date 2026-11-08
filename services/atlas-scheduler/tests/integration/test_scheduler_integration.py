"""Integration tests for atlas-scheduler across workspace, YAML discovery, and dispatch."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from atlas_scheduler import (
    RunnerDispatcher,
    Scheduler,
    SchedulerConfigurationError,
    WorkspaceIsolationError,
    YamlScheduleProvider,
    main,
    resolve_workspace,
)


class MockPopen:
    """Mock Popen tracking dispatched commands and completion without launching real processes."""

    def __init__(
        self,
        cmd: list[str],
        env: dict[str, str],
        *,
        pid: int,
        return_code: int | None = 0,
    ) -> None:
        self.cmd = list(cmd)
        self.env = dict(env)
        self.pid = pid
        self.return_code = return_code

    def poll(self) -> int | None:
        return self.return_code


class TestSchedulerIntegration:
    """End-to-end scheduler control-plane flow with temporary and external workspaces."""

    def test_end_to_end_workspace_discovery_cron_evaluation_and_dynamic_reload(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("ATLAS_WORKSPACE", str(tmp_path))
        ws_dir = tmp_path / "customer_001"
        schedules_dir = ws_dir / "schedules"
        schedules_dir.mkdir(parents=True, exist_ok=True)

        (schedules_dir / "daily.yml").write_text(
            """
schedule_id: daily_morning
name: Daily Morning Schedule
description: Executes the morning and financials batches every day.
enabled: true

trigger:
  type: cron
  expression: "0 6 * * *"
  timezone: America/Toronto

jobs:
  - morning_batch
  - financials_batch
""",
            encoding="utf-8",
        )

        launched_procs: list[MockPopen] = []

        def popen_spy(cmd: list[str], *, env: dict[str, str], **_: Any) -> Any:
            proc = MockPopen(cmd, env, pid=5000 + len(launched_procs))
            launched_procs.append(proc)
            return proc

        workspace = resolve_workspace("customer_001")
        assert workspace.workspace_id == "customer_001"
        assert workspace.workspace_path == ws_dir.resolve()

        provider = YamlScheduleProvider(workspace_path=workspace.workspace_path)
        dispatcher = RunnerDispatcher(workspace=workspace, popen_factory=popen_spy)
        scheduler = Scheduler(provider=provider, dispatcher=dispatcher, interval=60)

        toronto = ZoneInfo("America/Toronto")
        # 1. Tick at 06:00:05 America/Toronto dispatches both jobs in daily_morning
        tick_1 = datetime(2026, 11, 8, 6, 0, 5, tzinfo=toronto)
        dispatched_1 = scheduler.tick(now=tick_1)

        assert len(dispatched_1) == 2
        assert len(launched_procs) == 2
        assert launched_procs[0].cmd == [
            "uv",
            "run",
            "atlas-runner",
            "--workspace",
            "customer_001",
            "--job",
            "morning_batch",
        ]
        assert launched_procs[1].cmd == [
            "uv",
            "run",
            "atlas-runner",
            "--workspace",
            "customer_001",
            "--job",
            "financials_batch",
        ]
        assert launched_procs[0].env["ATLAS_WORKSPACE"] == str(tmp_path)

        # 2. Idempotency: another tick within the same occurrence must not dispatch twice
        tick_duplicate = datetime(2026, 11, 8, 6, 0, 30, tzinfo=toronto)
        dispatched_duplicate = scheduler.tick(now=tick_duplicate)

        assert dispatched_duplicate == []
        assert len(launched_procs) == 2

        # 3. Dynamically add sensors.yaml and verify reload without duplicate dispatch
        (schedules_dir / "sensors.yaml").write_text(
            """
schedule_id: hourly_sensors
name: Hourly Sensors Schedule
enabled: true

trigger:
  type: cron
  expression: "0 7 * * *"
  timezone: America/Toronto

jobs:
  - sensors_batch
""",
            encoding="utf-8",
        )

        discovered_schedules = provider.get_schedules()
        assert {s.schedule_id for s in discovered_schedules} == {
            "daily_morning",
            "hourly_sensors",
        }

        # Tick again during the 06:00 window after sensors.yaml is added:
        # daily_morning must NOT dispatch again, and hourly_sensors is not due until 07:00
        tick_before_seven = datetime(2026, 11, 8, 6, 1, 0, tzinfo=toronto)
        assert scheduler.tick(now=tick_before_seven) == []
        assert len(launched_procs) == 2

        # Tick at 07:00:03 America/Toronto: only hourly_sensors is dispatched
        tick_2 = datetime(2026, 11, 8, 7, 0, 3, tzinfo=toronto)
        dispatched_2 = scheduler.tick(now=tick_2)

        assert len(dispatched_2) == 1
        assert [r.schedule_id for r in dispatched_2] == ["hourly_sensors"]
        assert len(launched_procs) == 3
        assert launched_procs[2].cmd == [
            "uv",
            "run",
            "atlas-runner",
            "--workspace",
            "customer_001",
            "--job",
            "sensors_batch",
        ]

    def test_process_completion_monitoring_across_ticks(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setenv("ATLAS_WORKSPACE", str(tmp_path))
        ws_dir = tmp_path / "customer_001"
        schedules_dir = ws_dir / "schedules"
        schedules_dir.mkdir(parents=True, exist_ok=True)

        (schedules_dir / "batch.yml").write_text(
            """
schedule_id: morning_pipeline
name: Morning Pipeline
enabled: true
trigger:
  type: cron
  expression: "0 6 * * *"
  timezone: America/Toronto
jobs:
  - corporations_batch
  - sensors_batch
""",
            encoding="utf-8",
        )

        procs_by_job: dict[str, MockPopen] = {}

        def popen_spy(cmd: list[str], *, env: dict[str, str], **_: Any) -> Any:
            job_id = cmd[-1]
            # Start both subprocesses as still running (return_code=None)
            proc = MockPopen(cmd, env, pid=7000 + len(procs_by_job), return_code=None)
            procs_by_job[job_id] = proc
            return proc

        workspace = resolve_workspace("customer_001")
        provider = YamlScheduleProvider(workspace_path=workspace.workspace_path)
        dispatcher = RunnerDispatcher(workspace=workspace, popen_factory=popen_spy)
        scheduler = Scheduler(provider=provider, dispatcher=dispatcher, interval=60)

        toronto = ZoneInfo("America/Toronto")
        tick_1 = datetime(2026, 11, 8, 6, 0, 5, tzinfo=toronto)
        dispatched = scheduler.tick(now=tick_1)

        assert len(dispatched) == 2
        assert dispatcher.active_count == 2
        assert all(r.exit_code is None for r in dispatched)

        # Next tick while processes are still running does not block and leaves active_count=2
        tick_2 = datetime(2026, 11, 8, 6, 0, 20, tzinfo=toronto)
        assert scheduler.tick(now=tick_2) == []
        assert dispatcher.active_count == 2

        # Simulate corporations_batch succeeding (0) and sensors_batch failing (1)
        procs_by_job["corporations_batch"].return_code = 0
        procs_by_job["sensors_batch"].return_code = 1

        tick_3 = datetime(2026, 11, 8, 6, 0, 40, tzinfo=toronto)
        with caplog.at_level(logging.INFO, logger="atlas_scheduler.dispatcher"):
            scheduler.tick(now=tick_3)

        assert dispatcher.active_count == 0
        records_by_job = {r.job_id: r for r in dispatcher.records}
        assert records_by_job["corporations_batch"].exit_code == 0
        assert records_by_job["corporations_batch"].completed_at is not None
        assert records_by_job["sensors_batch"].exit_code == 1
        assert records_by_job["sensors_batch"].completed_at is not None
        assert "Runner subprocess completed successfully" in caplog.text
        assert "Runner subprocess failed" in caplog.text

    def test_workspace_isolation_and_resolution_modes(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        managed_root = tmp_path / "managed_workspaces"
        customer_dir = managed_root / "customer_001"
        (customer_dir / "schedules").mkdir(parents=True, exist_ok=True)
        (customer_dir / "schedules" / "daily.yml").write_text(
            """
schedule_id: daily_morning
enabled: true
trigger:
  type: cron
  expression: "0 6 * * *"
  timezone: UTC
jobs:
  - morning_batch
""",
            encoding="utf-8",
        )

        # 1. Missing ATLAS_WORKSPACE for logical ID raises SchedulerConfigurationError
        monkeypatch.delenv("ATLAS_WORKSPACE", raising=False)
        with pytest.raises(SchedulerConfigurationError, match="ATLAS_WORKSPACE"):
            resolve_workspace("customer_001")

        # 2. Path traversal is rejected with WorkspaceIsolationError
        monkeypatch.setenv("ATLAS_WORKSPACE", str(managed_root))
        for traversal in ("../../etc/passwd", "../outside_dir", "customer_001/../../escape"):
            with pytest.raises(WorkspaceIsolationError):
                resolve_workspace(traversal)

        # 3. Logical workspace ID resolves under ATLAS_WORKSPACE and preserves logical ID
        logical_ctx = resolve_workspace("customer_001")
        assert logical_ctx.workspace_id == "customer_001"
        assert logical_ctx.workspace_path == customer_dir.resolve()
        logical_dispatcher = RunnerDispatcher(workspace=logical_ctx)
        assert logical_dispatcher.build_command("morning_batch") == [
            "uv",
            "run",
            "atlas-runner",
            "--workspace",
            "customer_001",
            "--job",
            "morning_batch",
        ]

        # 4. Absolute workspace path remains supported even without ATLAS_WORKSPACE
        monkeypatch.delenv("ATLAS_WORKSPACE", raising=False)
        abs_ctx = resolve_workspace(str(customer_dir.resolve()))
        assert abs_ctx.workspace_id == str(customer_dir.resolve())
        assert abs_ctx.workspace_path == customer_dir.resolve()
        abs_dispatcher = RunnerDispatcher(workspace=abs_ctx)
        assert abs_dispatcher.build_command("morning_batch") == [
            "uv",
            "run",
            "atlas-runner",
            "--workspace",
            str(customer_dir.resolve()),
            "--job",
            "morning_batch",
        ]

    def test_cli_composition_root_with_mocked_subprocess(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("ATLAS_WORKSPACE", str(tmp_path))
        ws_dir = tmp_path / "customer_001"
        schedules_dir = ws_dir / "schedules"
        schedules_dir.mkdir(parents=True, exist_ok=True)

        now_utc = datetime.now(UTC)
        (schedules_dir / "now.yml").write_text(
            f"""
schedule_id: immediate_schedule
name: Immediate Schedule
enabled: true
trigger:
  type: cron
  expression: "{now_utc.minute} {now_utc.hour} * * *"
  timezone: UTC
jobs:
  - morning_batch
""",
            encoding="utf-8",
        )

        launched_cmds: list[list[str]] = []

        def fake_popen(cmd: list[str], *, env: dict[str, str], **_: Any) -> Any:
            launched_cmds.append(list(cmd))
            return MockPopen(cmd, env, pid=9001)

        monkeypatch.setattr("subprocess.Popen", fake_popen)

        exit_code = main(
            ["--workspace", "customer_001", "--interval", "10", "--log-level", "DEBUG"],
            max_ticks=1,
        )
        assert exit_code == 0
        assert launched_cmds == [
            [
                "uv",
                "run",
                "atlas-runner",
                "--workspace",
                "customer_001",
                "--job",
                "morning_batch",
            ]
        ]

    def test_external_customer_001_schedule_discovery_and_dispatch(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        external_base = Path(r"D:\work-space\atlas-workspace")
        external_ws = external_base / "customer_001"
        if not external_ws.exists():
            pytest.skip("External customer_001 workspace is not present.")

        monkeypatch.setenv("ATLAS_WORKSPACE", str(external_base))
        workspace = resolve_workspace("customer_001")
        provider = YamlScheduleProvider(workspace_path=workspace.workspace_path)
        schedules = provider.get_schedules()

        assert len(schedules) >= 1
        daily = next(s for s in schedules if s.schedule_id == "daily_morning")
        assert daily.trigger.expression == "0 6 * * *"
        assert daily.trigger.timezone == "America/Toronto"
        assert daily.jobs == ["morning_batch"]

        launched_cmds: list[list[str]] = []

        def fake_popen(cmd: list[str], *, env: dict[str, str], **_: Any) -> Any:
            launched_cmds.append(list(cmd))
            return MockPopen(cmd, env, pid=9900)

        dispatcher = RunnerDispatcher(workspace=workspace, popen_factory=fake_popen)
        scheduler = Scheduler(provider=provider, dispatcher=dispatcher)

        toronto = ZoneInfo("America/Toronto")
        records = scheduler.tick(now=datetime(2026, 11, 8, 6, 0, 12, tzinfo=toronto))
        assert len(records) == 1
        assert launched_cmds == [
            [
                "uv",
                "run",
                "atlas-runner",
                "--workspace",
                "customer_001",
                "--job",
                "morning_batch",
            ]
        ]
