"""Unit tests for RunnerDispatcher subprocess creation, environment propagation, and monitoring."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pytest
from atlas_scheduler import (
    RunnerDispatcher,
    WorkspaceContext,
)


class FakePopenProcess:
    """Controllable mock subprocess.Popen instance."""

    def __init__(
        self,
        cmd: list[str],
        env: dict[str, str],
        *,
        pid: int = 4242,
        return_code: int | None = None,
    ) -> None:
        self.cmd = list(cmd)
        self.env = dict(env)
        self.pid = pid
        self.return_code = return_code

    def poll(self) -> int | None:
        return self.return_code


class TestRunnerDispatcher:
    """Command construction, environment preservation, PID tracking, and completion polling."""

    def test_command_construction_with_logical_workspace_id(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("ATLAS_WORKSPACE", str(tmp_path))
        launched: list[FakePopenProcess] = []

        def fake_popen(cmd: list[str], *, env: dict[str, str], **_: Any) -> Any:
            proc = FakePopenProcess(cmd, env, pid=1001)
            launched.append(proc)
            return proc

        ws_ctx = WorkspaceContext(
            workspace_id="customer_001",
            workspace_path=(tmp_path / "customer_001").resolve(),
        )
        dispatcher = RunnerDispatcher(workspace=ws_ctx, popen_factory=fake_popen)
        record = dispatcher.dispatch("daily_morning", "morning_batch")

        assert len(launched) == 1
        assert launched[0].cmd == [
            "uv",
            "run",
            "atlas-runner",
            "--workspace",
            "customer_001",
            "--job",
            "morning_batch",
        ]
        # Never passes physical workspace path when using managed logical workspace contract
        assert str(ws_ctx.workspace_path) not in launched[0].cmd
        assert launched[0].env.get("ATLAS_WORKSPACE") == str(tmp_path)
        assert record.pid == 1001
        assert record.launch_succeeded is True

    def test_command_construction_with_absolute_workspace_path(self, tmp_path: Path) -> None:
        launched: list[FakePopenProcess] = []

        def fake_popen(cmd: list[str], *, env: dict[str, str], **_: Any) -> Any:
            proc = FakePopenProcess(cmd, env, pid=1002)
            launched.append(proc)
            return proc

        abs_path = (tmp_path / "customer_001").resolve()
        ws_ctx = WorkspaceContext(workspace_id=str(abs_path), workspace_path=abs_path)
        dispatcher = RunnerDispatcher(workspace=ws_ctx, popen_factory=fake_popen)
        dispatcher.dispatch("daily_morning", "morning_batch")

        assert launched[0].cmd == [
            "uv",
            "run",
            "atlas-runner",
            "--workspace",
            str(abs_path),
            "--job",
            "morning_batch",
        ]

    def test_preserves_inherited_environment_and_atlas_workspace(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("ATLAS_WORKSPACE", str(tmp_path))
        monkeypatch.setenv("CUSTOM_INHERITED_VAR", "preserved_value")
        captured_env: dict[str, str] = {}

        def fake_popen(cmd: list[str], *, env: dict[str, str], **_: Any) -> Any:
            captured_env.update(env)
            return FakePopenProcess(cmd, env, pid=2001)

        dispatcher = RunnerDispatcher(workspace="customer_001", popen_factory=fake_popen)
        dispatcher.dispatch("sched_1", "job_1")

        assert captured_env["ATLAS_WORKSPACE"] == str(tmp_path)
        assert captured_env["CUSTOM_INHERITED_VAR"] == "preserved_value"

    def test_non_blocking_completion_monitoring_and_exit_code_tracking(self) -> None:
        procs: dict[str, FakePopenProcess] = {}

        def fake_popen(cmd: list[str], *, env: dict[str, str], **_: Any) -> Any:
            job_id = cmd[-1]
            proc = FakePopenProcess(cmd, env, pid=3000 + len(procs), return_code=None)
            procs[job_id] = proc
            return proc

        dispatcher = RunnerDispatcher(workspace="customer_001", popen_factory=fake_popen)
        rec_ok = dispatcher.dispatch("daily_morning", "morning_batch")
        rec_fail = dispatcher.dispatch("daily_morning", "financials_batch")

        assert dispatcher.active_count == 2
        assert dispatcher.poll_processes() == []
        assert rec_ok.completed_at is None
        assert rec_ok.exit_code is None

        # Simulate morning_batch succeeding (0) and financials_batch failing (2)
        procs["morning_batch"].return_code = 0
        procs["financials_batch"].return_code = 2

        completed = dispatcher.poll_processes()
        assert len(completed) == 2
        assert dispatcher.active_count == 0
        assert rec_ok.exit_code == 0
        assert rec_ok.completed_at is not None
        assert rec_fail.exit_code == 2
        assert rec_fail.completed_at is not None

    def test_subprocess_launch_failure_is_logged_without_crashing_or_retrying(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        call_count = 0

        def failing_popen(cmd: list[str], *, env: dict[str, str], **_: Any) -> Any:
            nonlocal call_count
            call_count += 1
            raise OSError("uv executable not found")

        dispatcher = RunnerDispatcher(workspace="customer_001", popen_factory=failing_popen)
        with caplog.at_level(logging.ERROR, logger="atlas_scheduler.dispatcher"):
            record = dispatcher.dispatch("daily_morning", "morning_batch")

        assert call_count == 1  # No automatic retry in Phase 1
        assert record.launch_succeeded is False
        assert record.pid is None
        assert "uv executable not found" in (record.error_message or "")
        assert "Failed to launch runner subprocess" in caplog.text
        assert dispatcher.active_count == 0

    def test_launches_runner_when_started_outside_atlas_repository_root(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        external_cwd = tmp_path / "external_working_dir"
        external_cwd.mkdir(parents=True, exist_ok=True)
        monkeypatch.chdir(external_cwd)
        monkeypatch.setenv("ATLAS_WORKSPACE", str(tmp_path / "workspaces"))

        captured_cwd: list[str | None] = []
        captured_cmd: list[list[str]] = []

        def fake_popen(
            cmd: list[str],
            *,
            env: dict[str, str],
            cwd: str | None = None,
            **_: Any,
        ) -> Any:
            captured_cmd.append(list(cmd))
            captured_cwd.append(cwd)
            return FakePopenProcess(cmd, env, pid=4050)

        dispatcher = RunnerDispatcher(workspace="customer_001", popen_factory=fake_popen)
        record = dispatcher.dispatch("daily_morning", "morning_batch")

        assert record.launch_succeeded is True
        assert dispatcher.working_directory is not None
        assert (dispatcher.working_directory / "pyproject.toml").is_file()
        assert dispatcher.working_directory != external_cwd.resolve()
        assert captured_cwd == [str(dispatcher.working_directory)]
        assert captured_cmd == [
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
