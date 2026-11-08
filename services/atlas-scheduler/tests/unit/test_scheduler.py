"""Unit tests for Scheduler engine: cron, idempotency, failure isolation, misfire, and shutdown."""

from __future__ import annotations

import ast
import concurrent.futures
import inspect
import logging
import signal
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import atlas_scheduler.engine as engine_module
import pytest
from atlas_scheduler import (
    DispatchedProcessRecord,
    RunnerDispatcher,
    ScheduleDefinition,
    Scheduler,
    TriggerDefinition,
    main,
)


class FakeScheduleProvider:
    """In-memory test double implementing the ScheduleProvider protocol."""

    def __init__(
        self,
        schedules: list[ScheduleDefinition] | None = None,
        *,
        should_raise: bool = False,
    ) -> None:
        self.schedules: list[ScheduleDefinition] = list(schedules or [])
        self.should_raise = should_raise
        self.call_count = 0

    def get_schedules(self) -> list[ScheduleDefinition]:
        self.call_count += 1
        if self.should_raise:
            raise RuntimeError("Simulated provider persistence outage")
        return list(self.schedules)


class FakeRunnerDispatcher(RunnerDispatcher):
    """Test double recording dispatched jobs without spawning any subprocesses."""

    def __init__(self) -> None:
        self.dispatched: list[tuple[str, str, datetime | None]] = []
        self.poll_count = 0

    def dispatch(
        self,
        schedule_id: str,
        job_id: str,
        *,
        scheduled_occurrence: datetime | None = None,
    ) -> DispatchedProcessRecord:
        self.dispatched.append((schedule_id, job_id, scheduled_occurrence))
        return DispatchedProcessRecord(
            schedule_id=schedule_id,
            job_id=job_id,
            command=("uv", "run", "atlas-runner", "--workspace", "customer_001", "--job", job_id),
            pid=12345,
            launched_at=datetime.now(UTC),
            launch_succeeded=True,
            scheduled_occurrence=scheduled_occurrence,
        )

    def poll_processes(self) -> list[DispatchedProcessRecord]:
        self.poll_count += 1
        return []


def _make_schedule(
    schedule_id: str = "daily_morning",
    *,
    expression: str = "0 6 * * *",
    timezone: str = "America/Toronto",
    jobs: list[str] | None = None,
    enabled: bool = True,
) -> ScheduleDefinition:
    return ScheduleDefinition(
        schedule_id=schedule_id,
        name=f"Schedule {schedule_id}",
        enabled=enabled,
        trigger=TriggerDefinition(
            type="cron",
            expression=expression,
            timezone=timezone,
        ),
        jobs=jobs or ["morning_batch"],
    )


class TestSchedulerEngine:
    """Cron evaluation, timezones, idempotency, failure isolation, misfire, and dynamic reload."""

    def test_idempotency_key_and_restart_behavior(self) -> None:
        sched = _make_schedule(
            "daily_morning",
            expression="0 6 * * *",
            timezone="America/Toronto",
            jobs=["morning_batch", "financials_batch"],
        )
        provider = FakeScheduleProvider([sched])
        dispatcher = FakeRunnerDispatcher()
        scheduler = Scheduler(provider=provider, dispatcher=dispatcher, interval=60)

        toronto = ZoneInfo("America/Toronto")
        expected_occurrence_utc = datetime(2026, 11, 8, 6, 0, 0, tzinfo=toronto).astimezone(UTC)

        # 1. Delayed tick at 06:00:08 dispatches both jobs once
        tick_1_time = datetime(2026, 11, 8, 6, 0, 8, tzinfo=toronto)
        records_1 = scheduler.tick(now=tick_1_time)
        assert len(records_1) == 2
        assert scheduler.dispatched_occurrences == {("daily_morning", expected_occurrence_utc)}
        assert dispatcher.dispatched == [
            ("daily_morning", "morning_batch", expected_occurrence_utc),
            ("daily_morning", "financials_batch", expected_occurrence_utc),
        ]

        # 2. Repeated ticks within the same occurrence window do not re-dispatch
        tick_2_time = datetime(2026, 11, 8, 6, 0, 45, tzinfo=toronto)
        tick_3_time = datetime(2026, 11, 8, 6, 1, 15, tzinfo=toronto)
        assert scheduler.tick(now=tick_2_time) == []
        assert scheduler.tick(now=tick_3_time) == []
        assert len(dispatcher.dispatched) == 2

        # 3. Next day's 06:00:05 occurrence IS dispatched
        next_day_time = datetime(2026, 11, 9, 6, 0, 5, tzinfo=toronto)
        records_next = scheduler.tick(now=next_day_time)
        assert len(records_next) == 2
        assert len(dispatcher.dispatched) == 4

        # 4. Simulated scheduler process restart resets in-memory idempotency state
        restarted_dispatcher = FakeRunnerDispatcher()
        restarted_scheduler = Scheduler(
            provider=provider,
            dispatcher=restarted_dispatcher,
            interval=60,
        )
        assert restarted_scheduler.dispatched_occurrences == set()
        restarted_records = restarted_scheduler.tick(now=tick_1_time)
        assert len(restarted_records) == 2

    def test_disabled_schedules_are_never_dispatched(self) -> None:
        provider = FakeScheduleProvider([_make_schedule(enabled=False)])
        dispatcher = FakeRunnerDispatcher()
        scheduler = Scheduler(provider=provider, dispatcher=dispatcher)

        toronto = ZoneInfo("America/Toronto")
        due_time = datetime(2026, 11, 8, 6, 0, 2, tzinfo=toronto)
        assert scheduler.tick(now=due_time) == []
        assert dispatcher.dispatched == []

    def test_multiple_jobs_per_schedule_and_independent_timezone_schedules(self) -> None:
        sched_a = _make_schedule(
            "morning_sched",
            expression="0 6 * * *",
            timezone="America/Toronto",
            jobs=["morning_batch", "financials_batch"],
        )
        sched_b = _make_schedule(
            "utc_sched",
            expression="0 11 * * *",
            timezone="UTC",
            jobs=["sensors_batch"],
        )
        provider = FakeScheduleProvider([sched_a, sched_b])
        dispatcher = FakeRunnerDispatcher()
        scheduler = Scheduler(provider=provider, dispatcher=dispatcher)

        # On 2026-11-08 (EST, UTC-5), 06:00 America/Toronto == 11:00 UTC
        tick_time = datetime(2026, 11, 8, 11, 0, 10, tzinfo=UTC)
        records = scheduler.tick(now=tick_time)

        assert len(records) == 3
        assert [(r.schedule_id, r.job_id) for r in records] == [
            ("morning_sched", "morning_batch"),
            ("morning_sched", "financials_batch"),
            ("utc_sched", "sensors_batch"),
        ]
        # Both occurrence timestamps are normalized to UTC in idempotency state
        expected_utc = datetime(2026, 11, 8, 11, 0, 0, tzinfo=UTC)
        assert scheduler.dispatched_occurrences == {
            ("morning_sched", expected_utc),
            ("utc_sched", expected_utc),
        }

    def test_dispatch_failure_isolation_across_three_jobs(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Verify second job launch failure does not block first/third jobs or cause retries."""
        sched = _make_schedule(
            "multi_job_schedule",
            expression="0 6 * * *",
            timezone="America/Toronto",
            jobs=["corporations_batch", "sensors_batch", "financials_batch"],
        )
        provider = FakeScheduleProvider([sched])
        attempted_jobs: list[str] = []

        class MockProcess:
            def __init__(self, pid: int) -> None:
                self.pid = pid

            def poll(self) -> int | None:
                return None

        def selective_popen(cmd: list[str], *, env: dict[str, str], **_: Any) -> Any:
            job_id = cmd[-1]
            attempted_jobs.append(job_id)
            if job_id == "sensors_batch":
                raise OSError("Simulated subprocess launch failure for sensors_batch")
            return MockProcess(pid=8000 + len(attempted_jobs))

        dispatcher = RunnerDispatcher(workspace="customer_001", popen_factory=selective_popen)
        scheduler = Scheduler(provider=provider, dispatcher=dispatcher, interval=60)

        toronto = ZoneInfo("America/Toronto")
        tick_time = datetime(2026, 11, 8, 6, 0, 5, tzinfo=toronto)

        with caplog.at_level(logging.ERROR, logger="atlas_scheduler.dispatcher"):
            records = scheduler.tick(now=tick_time)

        # All 3 jobs were attempted once in order; no automatic retry occurred
        assert attempted_jobs == ["corporations_batch", "sensors_batch", "financials_batch"]
        assert len(records) == 3

        assert records[0].job_id == "corporations_batch"
        assert records[0].launch_succeeded is True
        assert records[0].pid is not None

        assert records[1].job_id == "sensors_batch"
        assert records[1].launch_succeeded is False
        assert records[1].pid is None
        assert "Simulated subprocess launch failure for sensors_batch" in caplog.text

        assert records[2].job_id == "financials_batch"
        assert records[2].launch_succeeded is True
        assert records[2].pid is not None

        # Scheduler remains running and the occurrence remains reserved against duplicate dispatch
        assert scheduler.is_running is True
        duplicate_tick = datetime(2026, 11, 8, 6, 0, 30, tzinfo=toronto)
        assert scheduler.tick(now=duplicate_tick) == []
        assert len(attempted_jobs) == 3

    def test_misfire_policy_bounds_and_skips_stale_occurrences(self) -> None:
        provider = FakeScheduleProvider([_make_schedule(expression="0 * * * *", timezone="UTC")])
        dispatcher = FakeRunnerDispatcher()
        # Default interval=60 -> misfire_grace_seconds == max(60 * 2, 300) == 300s
        scheduler = Scheduler(provider=provider, dispatcher=dispatcher, interval=60)
        assert scheduler.misfire_grace_seconds == 300

        # 1. With larger interval=200 -> misfire_grace_seconds == max(200 * 2, 300) == 400s
        scheduler_large = Scheduler(provider=provider, dispatcher=dispatcher, interval=200)
        assert scheduler_large.misfire_grace_seconds == 400

        # 2. Occurrence outside the 300s grace window (10:10:00 is 600s after 10:00:00) is skipped
        stale_tick = datetime(2026, 11, 8, 10, 10, 0, tzinfo=UTC)
        assert scheduler.tick(now=stale_tick) == []
        assert dispatcher.dispatched == []

        # 3. After hours of downtime, waking at 14:02:00 (within 300s of 14:00:00) dispatches
        # ONLY the 14:00:00 occurrence and never replays 10:00, 11:00, 12:00, or 13:00
        recovery_tick = datetime(2026, 11, 8, 14, 2, 0, tzinfo=UTC)
        recovered = scheduler.tick(now=recovery_tick)
        assert len(recovered) == 1
        assert recovered[0].scheduled_occurrence == datetime(2026, 11, 8, 14, 0, 0, tzinfo=UTC)
        assert len(dispatcher.dispatched) == 1

    def test_dynamic_schedule_reload_addition_disable_and_removal(self) -> None:
        provider = FakeScheduleProvider([])
        dispatcher = FakeRunnerDispatcher()
        scheduler = Scheduler(provider=provider, dispatcher=dispatcher)

        t1 = datetime(2026, 11, 8, 12, 0, 5, tzinfo=UTC)
        assert scheduler.tick(now=t1) == []
        assert provider.call_count == 1

        # 1. Dynamically add an enabled schedule due at 12:00 UTC and a disabled schedule
        hourly_sched = _make_schedule("hourly_utc", expression="0 * * * *", timezone="UTC")
        disabled_sched = _make_schedule(
            "disabled_utc",
            expression="0 * * * *",
            timezone="UTC",
            enabled=False,
        )
        provider.schedules.extend([hourly_sched, disabled_sched])

        t2 = datetime(2026, 11, 8, 12, 0, 20, tzinfo=UTC)
        records = scheduler.tick(now=t2)
        assert provider.call_count == 2
        assert len(records) == 1
        assert records[0].schedule_id == "hourly_utc"

        # 2. Remove hourly_utc before the 13:00 UTC occurrence: it is no longer considered
        provider.schedules.clear()
        t3 = datetime(2026, 11, 8, 13, 0, 10, tzinfo=UTC)
        assert scheduler.tick(now=t3) == []
        assert provider.call_count == 3
        assert len(dispatcher.dispatched) == 1

    def test_provider_failure_is_logged_without_terminating_scheduler(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        provider = FakeScheduleProvider(
            [_make_schedule(expression="0 12 * * *", timezone="UTC")],
            should_raise=True,
        )
        dispatcher = FakeRunnerDispatcher()
        scheduler = Scheduler(provider=provider, dispatcher=dispatcher)

        tick_time = datetime(2026, 11, 8, 12, 0, 10, tzinfo=UTC)
        assert scheduler.tick(now=tick_time) == []
        assert "Simulated provider persistence outage" in caplog.text

        # When provider recovers on next tick, scheduler dispatches normally
        provider.should_raise = False
        records = scheduler.tick(now=datetime(2026, 11, 8, 12, 0, 25, tzinfo=UTC))
        assert len(records) == 1

    def test_graceful_shutdown_stops_loop_and_preserves_dispatched_processes(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setenv("ATLAS_WORKSPACE", str(tmp_path))
        ws_dir = tmp_path / "customer_001"
        schedules_dir = ws_dir / "schedules"
        schedules_dir.mkdir(parents=True, exist_ok=True)

        now_utc = datetime.now(UTC)
        (schedules_dir / "active.yml").write_text(
            f"""
schedule_id: active_schedule
enabled: true
trigger:
  type: cron
  expression: "{now_utc.minute} {now_utc.hour} * * *"
  timezone: UTC
jobs:
  - long_running_job
""",
            encoding="utf-8",
        )

        class ActiveMockProcess:
            def __init__(self) -> None:
                self.pid = 54321
                self.terminated = False
                self.killed = False

            def poll(self) -> int | None:
                return None

            def terminate(self) -> None:
                self.terminated = True

            def kill(self) -> None:
                self.killed = True

        spawned: list[ActiveMockProcess] = []

        def fake_popen(cmd: list[str], *, env: dict[str, str], **_: Any) -> Any:
            proc = ActiveMockProcess()
            spawned.append(proc)
            # Trigger SIGTERM handler right after first subprocess is launched
            handler = signal.getsignal(signal.SIGTERM)
            if callable(handler):
                handler(signal.SIGTERM, None)
            return proc

        monkeypatch.setattr("subprocess.Popen", fake_popen)
        threads_before = threading.active_count()

        with caplog.at_level(logging.INFO, logger="atlas_scheduler"):
            exit_code = main(["--workspace", "customer_001", "--interval", "60"])

        assert exit_code == 0
        assert len(spawned) == 1
        # Dispatched runner subprocess was NOT terminated or killed by scheduler shutdown
        assert spawned[0].terminated is False
        assert spawned[0].killed is False
        assert "initiating graceful scheduler shutdown" in caplog.text
        assert "Atlas Scheduler loop stopped gracefully" in caplog.text
        assert threading.active_count() <= threads_before

    def test_thread_safety_prevents_duplicate_dispatch_under_concurrent_ticks(self) -> None:
        provider = FakeScheduleProvider(
            [_make_schedule("concurrent_sched", expression="0 12 * * *", timezone="UTC")]
        )
        dispatcher = FakeRunnerDispatcher()
        scheduler = Scheduler(provider=provider, dispatcher=dispatcher)
        due_time = datetime(2026, 11, 8, 12, 0, 5, tzinfo=UTC)

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(scheduler.tick, due_time) for _ in range(16)]
            results = [f.result() for f in futures]

        total_dispatched = sum(len(r) for r in results)
        assert total_dispatched == 1
        assert len(dispatcher.dispatched) == 1


class TestProtocolIsolation:
    """Verify Scheduler engine has zero YAML, filesystem, or database dependencies."""

    def test_engine_module_has_strict_protocol_isolation(self) -> None:
        source = inspect.getsource(engine_module)
        tree = ast.parse(source)

        forbidden_modules = {
            "yaml",
            "pathlib",
            "os",
            "glob",
            "sqlite3",
            "sqlalchemy",
            "atlas_runner",
            "atlas_scheduler.providers",
            "atlas_scheduler.providers.yaml_provider",
            "atlas_scheduler.config",
        }
        forbidden_names = {
            "YamlScheduleProvider",
            "WorkspaceContext",
            "Path",
        }

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert alias.name not in forbidden_modules, (
                        f"Forbidden import '{alias.name}' in engine.py"
                    )
            elif isinstance(node, ast.ImportFrom):
                mod = node.module or ""
                assert mod not in forbidden_modules, f"Forbidden from-import '{mod}' in engine.py"
                for alias in node.names:
                    assert alias.name not in forbidden_names, (
                        f"Forbidden symbol '{alias.name}' imported in engine.py"
                    )
