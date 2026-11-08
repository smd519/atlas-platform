"""Persistence-independent Scheduler control-plane engine.

Architectural Boundary & Limitations
------------------------------------
- Depends exclusively on :class:`ScheduleProvider` and :class:`RunnerDispatcher`.
- Contains zero YAML, filesystem, or database dependencies.
- Phase 1 Idempotency Limitation: Uses thread-safe in-memory idempotency keyed by
  ``(schedule_id, scheduled_occurrence_utc)``. In-memory idempotency does not
  survive scheduler process restarts; persistent execution history belongs to a
  future phase.
- Phase 1 Misfire Policy: Uses a bounded misfire grace window
  (``max(interval * 2, 300)`` seconds). Slightly delayed ticks within the
  grace window dispatch the due occurrence once; older historical occurrences
  outside the grace window are skipped to prevent unbounded replay backlogs.
- Phase 1 Timezone & DST Scope: Phase 1 supports timezone-aware cron scheduling
  using standard timezone libraries. Special scheduling guarantees around
  daylight-saving transitions, including nonexistent and ambiguous local times,
  are not explicitly guaranteed. This behavior can be addressed in a future
  scheduling enhancement.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from datetime import UTC, datetime

from atlas_scheduler.dispatcher import DispatchedProcessRecord, RunnerDispatcher
from atlas_scheduler.models import ScheduleDefinition, evaluate_most_recent_occurrence
from atlas_scheduler.protocols import ScheduleProvider

logger = logging.getLogger("atlas_scheduler.engine")


class Scheduler:
    """Control-plane scheduling engine that evaluates triggers and dispatches jobs."""

    def __init__(
        self,
        provider: ScheduleProvider,
        dispatcher: RunnerDispatcher,
        interval: int = 60,
        *,
        clock: Callable[[], datetime] | None = None,
        sleep_fn: Callable[[float], None] | None = None,
    ) -> None:
        if isinstance(interval, bool) or not isinstance(interval, int) or interval <= 0:
            raise ValueError(f"Scheduler interval must be a positive integer, got {interval!r}.")

        self._provider = provider
        self._dispatcher = dispatcher
        self._interval = interval
        self._misfire_grace_seconds = max(interval * 2, 300)
        self._clock: Callable[[], datetime] = clock or (lambda: datetime.now(UTC))
        self._sleep_fn = sleep_fn
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._dispatched_occurrences: set[tuple[str, datetime]] = set()

    @property
    def provider(self) -> ScheduleProvider:
        """Return the configured :class:`ScheduleProvider`."""
        return self._provider

    @property
    def dispatcher(self) -> RunnerDispatcher:
        """Return the configured :class:`RunnerDispatcher`."""
        return self._dispatcher

    @property
    def interval(self) -> int:
        """Return the polling interval in seconds."""
        return self._interval

    @property
    def misfire_grace_seconds(self) -> int:
        """Return the bounded misfire grace window in seconds."""
        return self._misfire_grace_seconds

    @property
    def is_running(self) -> bool:
        """Return True if the scheduler has not been stopped."""
        return not self._stop_event.is_set()

    @property
    def dispatched_occurrences(self) -> set[tuple[str, datetime]]:
        """Return a snapshot of processed ``(schedule_id, occurrence_utc)`` keys."""
        with self._lock:
            return set(self._dispatched_occurrences)

    def _reserve_occurrence(self, schedule_id: str, occurrence_utc: datetime) -> bool:
        """Atomically reserve ``(schedule_id, occurrence_utc)`` for idempotency.

        Returns True if newly reserved, or False if already processed.
        """
        key = (schedule_id, occurrence_utc)
        with self._lock:
            if key in self._dispatched_occurrences:
                return False
            self._dispatched_occurrences.add(key)
            return True

    def tick(self, now: datetime | None = None) -> list[DispatchedProcessRecord]:
        """Execute a single scheduling tick.

        1. Polls existing subprocesses for completion without blocking.
        2. Retrieves current schedules via ``provider.get_schedules()``.
        3. Evaluates enabled cron triggers against ``now`` (or ``self._clock()``).
        4. Dispatches due jobs within the bounded misfire window at most once per occurrence.
        """
        if hasattr(self._dispatcher, "poll_processes"):
            try:
                self._dispatcher.poll_processes()
            except Exception as exc:
                logger.error("Error polling dispatched processes during tick: %s", exc)

        raw_now = now if now is not None else self._clock()
        if (
            not isinstance(raw_now, datetime)
            or raw_now.tzinfo is None
            or raw_now.tzinfo.utcoffset(raw_now) is None
        ):
            raise ValueError("Scheduler tick timestamp must be a timezone-aware datetime.")
        current_utc = raw_now.astimezone(UTC)

        try:
            schedules: list[ScheduleDefinition] = self._provider.get_schedules()
        except Exception as exc:
            logger.error("ScheduleProvider failed during tick: %s", exc)
            return []

        dispatched_in_tick: list[DispatchedProcessRecord] = []

        for schedule in schedules:
            if not schedule.enabled:
                logger.debug("Skipping disabled schedule '%s'.", schedule.schedule_id)
                continue

            try:
                occurrence_local = evaluate_most_recent_occurrence(
                    schedule.trigger.expression,
                    schedule.trigger.timezone,
                    current_utc,
                )
            except Exception as exc:
                logger.error(
                    "Failed to evaluate trigger for schedule '%s': %s",
                    schedule.schedule_id,
                    exc,
                )
                continue

            occurrence_utc = occurrence_local.astimezone(UTC)
            delay_seconds = (current_utc - occurrence_utc).total_seconds()

            if delay_seconds < 0.0 or delay_seconds > self._misfire_grace_seconds:
                continue

            # Atomically mark occurrence as processed before dispatching jobs
            if not self._reserve_occurrence(schedule.schedule_id, occurrence_utc):
                continue

            logger.info(
                "Schedule '%s' is due (occurrence=%s, delay=%.1fs); dispatching %d job(s).",
                schedule.schedule_id,
                occurrence_local.isoformat(),
                delay_seconds,
                len(schedule.jobs),
            )

            for job_id in schedule.jobs:
                try:
                    record = self._dispatcher.dispatch(
                        schedule_id=schedule.schedule_id,
                        job_id=job_id,
                        scheduled_occurrence=occurrence_utc,
                    )
                    dispatched_in_tick.append(record)
                except Exception as exc:
                    logger.error(
                        "Unexpected error dispatching job '%s' for schedule '%s': %s",
                        job_id,
                        schedule.schedule_id,
                        exc,
                    )

        return dispatched_in_tick

    def start(self, *, max_ticks: int | None = None) -> None:
        """Run the continuous scheduling loop until :meth:`stop` is called."""
        self._stop_event.clear()
        logger.info(
            "Starting Atlas Scheduler loop (interval=%ds, misfire_grace=%ds).",
            self._interval,
            self._misfire_grace_seconds,
        )

        ticks_executed = 0
        while not self._stop_event.is_set():
            self.tick()
            ticks_executed += 1
            if max_ticks is not None and ticks_executed >= max_ticks:
                break
            if self._stop_event.is_set():
                break
            if self._sleep_fn is not None:
                self._sleep_fn(float(self._interval))
            else:
                self._stop_event.wait(timeout=float(self._interval))

        logger.info(
            "Atlas Scheduler loop stopped gracefully after %d tick(s). "
            "Already-dispatched runner subprocesses continue independently.",
            ticks_executed,
        )

    def stop(self) -> None:
        """Signal the scheduler loop to stop accepting new ticks without killing running jobs."""
        logger.info("Stopping Atlas Scheduler; no new scheduling ticks will be started.")
        self._stop_event.set()


__all__ = ["Scheduler"]
