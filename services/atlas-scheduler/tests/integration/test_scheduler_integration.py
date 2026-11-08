"""Integration tests for atlas-scheduler across workspace, YAML discovery, and dispatch."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from atlas_scheduler import (
    RunnerDispatcher,
    Scheduler,
    YamlScheduleProvider,
    main,
    resolve_workspace,
)


class MockPopen:
    """Mock Popen tracking dispatched commands without launching real processes."""

    def __init__(self, cmd: list[str], env: dict[str, str], *, pid: int) -> None:
        self.cmd = list(cmd)
        self.env = dict(env)
        self.pid = pid

    def poll(self) -> int | None:
        return 0


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

        # 2. Dynamically add a new schedule file and tick at 07:00:03 America/Toronto
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

        tick_2 = datetime(2026, 11, 8, 7, 0, 3, tzinfo=toronto)
        dispatched_2 = scheduler.tick(now=tick_2)

        assert len(dispatched_2) == 1
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
