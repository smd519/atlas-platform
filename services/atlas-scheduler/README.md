# atlas-scheduler

Control-plane schedule discovery, trigger evaluation, and job dispatch service for the Atlas Platform.

---

## Architectural Boundaries

- **Control Plane vs. Data Plane**: `atlas-scheduler` determines **when** a job should execute; `atlas-runner` determines **how** the job executes.
- **Strict Service Independence**: `atlas-scheduler` never imports `atlas_runner`, never loads Job or Workflow YAMLs, and never instantiates orchestration dependencies.
- **Persistence Independence (`ScheduleProvider`)**: `Scheduler` (`atlas_scheduler.engine`) depends exclusively on the `ScheduleProvider` protocol (`get_schedules() -> list[ScheduleDefinition]`) and has zero YAML, filesystem, or database dependencies.
- **Phase 1 Limitations**:
  - **In-Memory Idempotency**: Tracks `(schedule_id, scheduled_occurrence_utc)` in memory to prevent duplicate dispatches within a running process. Does not survive process restarts.
  - **Bounded Misfire Policy**: Evaluates the most recent scheduled occurrence per tick within a bounded misfire grace window (`default = max(interval * 2, 300)` seconds) without replaying historical backlogs after prolonged outages.

---

## CLI Usage

```bash
# Managed workspace mode (resolves $ATLAS_WORKSPACE/customer_001)
uv run atlas-scheduler --workspace customer_001

# Explicit polling interval and log level
uv run atlas-scheduler --workspace customer_001 --interval 30 --log-level DEBUG

# Explicit absolute workspace path mode
uv run atlas-scheduler --workspace D:\work-space\atlas-workspace\customer_001
```
