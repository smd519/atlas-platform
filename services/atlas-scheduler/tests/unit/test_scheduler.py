"""Unit tests for Scheduler engine: cron, DST, idempotency, misfire, and protocol isolation."""

from __future__ import annotations

import ast
import concurrent.futures
import inspect
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import atlas_scheduler.engine as engine_module
import pytest
from atlas_scheduler import (
    DispatchedProcessRecord,
    RunnerDispatcher,
    ScheduleDefinition,
    Scheduler,
    TriggerDefinition,
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
    """Cron evaluation, timezones, DST, delayed ticks, idempotency, and dynamic reload."""

    def test_delayed_tick_recognizes_due_occurrence_and_prevents_duplicates(self) -> None:
        provider = FakeScheduleProvider([_make_schedule()])
        dispatcher = FakeRunnerDispatcher()
        scheduler = Scheduler(provider=provider, dispatcher=dispatcher, interval=60)

        toronto = ZoneInfo("America/Toronto")
        # Scheduled at 06:00:00 America/Toronto; scheduler wakes slightly late at 06:00:08
        tick_1_time = datetime(2026, 11, 8, 6, 0, 8, tzinfo=toronto)
        records_1 = scheduler.tick(now=tick_1_time)
        assert len(records_1) == 1
        assert dispatcher.dispatched == [
            (
                "daily_morning",
                "morning_batch",
                datetime(2026, 11, 8, 6, 0, 0, tzinfo=toronto).astimezone(UTC),
            )
        ]

        # Subsequent ticks within the same minute or misfire window do NOT re-dispatch
        tick_2_time = datetime(2026, 11, 8, 6, 0, 45, tzinfo=toronto)
        tick_3_time = datetime(2026, 11, 8, 6, 1, 15, tzinfo=toronto)
        assert scheduler.tick(now=tick_2_time) == []
        assert scheduler.tick(now=tick_3_time) == []
        assert len(dispatcher.dispatched) == 1

        # Next day's 06:00:05 occurrence IS dispatched
        next_day_time = datetime(2026, 11, 9, 6, 0, 5, tzinfo=toronto)
        records_next = scheduler.tick(now=next_day_time)
        assert len(records_next) == 1
        assert len(dispatcher.dispatched) == 2

    def test_disabled_schedules_are_never_dispatched(self) -> None:
        provider = FakeScheduleProvider([_make_schedule(enabled=False)])
        dispatcher = FakeRunnerDispatcher()
        scheduler = Scheduler(provider=provider, dispatcher=dispatcher)

        toronto = ZoneInfo("America/Toronto")
        due_time = datetime(2026, 11, 8, 6, 0, 2, tzinfo=toronto)
        assert scheduler.tick(now=due_time) == []
        assert dispatcher.dispatched == []

    def test_multiple_jobs_per_schedule_and_independent_schedules(self) -> None:
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

        # On 2026-11-08 (EST, UTC-5), 06:00 America/Toronto == 11:00 UTC!
        tick_time = datetime(2026, 11, 8, 11, 0, 10, tzinfo=UTC)
        records = scheduler.tick(now=tick_time)

        assert len(records) == 3
        assert [(r.schedule_id, r.job_id) for r in records] == [
            ("morning_sched", "morning_batch"),
            ("morning_sched", "financials_batch"),
            ("utc_sched", "sensors_batch"),
        ]

    def test_daylight_saving_time_transitions_in_america_toronto(self) -> None:
        """Verify cron evaluation across EDT (UTC-4) and EST (UTC-5) and fall-back Sunday."""
        sched = _make_schedule(
            "dst_schedule",
            expression="30 1 * * *",
            timezone="America/Toronto",
            jobs=["dst_job"],
        )
        provider = FakeScheduleProvider([sched])
        dispatcher = FakeRunnerDispatcher()
        scheduler = Scheduler(provider=provider, dispatcher=dispatcher)
        toronto = ZoneInfo("America/Toronto")

        # 1. Summer EDT (UTC-4): 2026-07-15 01:30 EDT == 05:30 UTC
        summer_tick = datetime(2026, 7, 15, 1, 30, 5, tzinfo=toronto)
        res_summer = scheduler.tick(now=summer_tick)
        assert len(res_summer) == 1
        assert res_summer[0].scheduled_occurrence == datetime(2026, 7, 15, 5, 30, tzinfo=UTC)

        # 2. Fall-back day (2026-11-01): 01:30 occurs in EDT (05:30 UTC) and EST (06:30 UTC)
        # Verify the schedule fires once at 05:30 UTC and does NOT fire a duplicate at 06:30 UTC
        fallback_first = datetime(2026, 11, 1, 5, 30, 10, tzinfo=UTC)
        fallback_second = datetime(2026, 11, 1, 6, 30, 10, tzinfo=UTC)
        assert len(scheduler.tick(now=fallback_first)) == 1
        assert scheduler.tick(now=fallback_second) == []

    def test_misfire_policy_skips_stale_occurrences_outside_grace_window(self) -> None:
        provider = FakeScheduleProvider([_make_schedule(expression="0 6 * * *")])
        dispatcher = FakeRunnerDispatcher()
        scheduler = Scheduler(
            provider=provider,
            dispatcher=dispatcher,
            interval=60,
            misfire_grace_seconds=300,
        )

        toronto = ZoneInfo("America/Toronto")
        # Scheduler starts at 10:00:00 (4 hours after 06:00:00 > 300s misfire grace window)
        late_startup = datetime(2026, 11, 8, 10, 0, 0, tzinfo=toronto)
        assert scheduler.tick(now=late_startup) == []
        assert dispatcher.dispatched == []

    def test_dynamic_schedule_reload_on_every_tick(self) -> None:
        provider = FakeScheduleProvider([])
        dispatcher = FakeRunnerDispatcher()
        scheduler = Scheduler(provider=provider, dispatcher=dispatcher)

        t1 = datetime(2026, 11, 8, 12, 0, 5, tzinfo=UTC)
        assert scheduler.tick(now=t1) == []
        assert provider.call_count == 1

        # Dynamically add a schedule due at 12:00 UTC
        provider.schedules.append(
            _make_schedule("dynamic_noon", expression="0 12 * * *", timezone="UTC")
        )
        t2 = datetime(2026, 11, 8, 12, 0, 20, tzinfo=UTC)
        records = scheduler.tick(now=t2)
        assert provider.call_count == 2
        assert len(records) == 1
        assert records[0].schedule_id == "dynamic_noon"

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

    def test_graceful_shutdown_stops_loop(self) -> None:
        provider = FakeScheduleProvider([])
        dispatcher = FakeRunnerDispatcher()
        ticks = 0

        def clock_and_stop() -> datetime:
            nonlocal ticks
            ticks += 1
            if ticks >= 2:
                scheduler.stop()
            return datetime(2026, 11, 8, 12, 0, 0, tzinfo=UTC)

        scheduler = Scheduler(
            provider=provider,
            dispatcher=dispatcher,
            interval=1,
            clock=clock_and_stop,
            sleep_fn=lambda _: None,
        )
        scheduler.start()
        assert ticks == 2
        assert scheduler.is_running is False

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
