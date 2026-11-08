# atlas-scheduler

## 1. Purpose

`atlas-scheduler` is an independent control-plane service for the Atlas Platform responsible for discovering schedule definitions, evaluating time-based cron triggers, and dispatching due jobs to `atlas-runner`.

The scheduler enforces a strict separation between control-plane scheduling and data-plane execution:

- **`atlas-scheduler`** determines **WHEN** a job should execute.
- **`atlas-runner`** determines **HOW** a job executes.
- **`atlas-orchestration`** manages workflow execution lifecycles.

`atlas-scheduler` never imports `atlas_runner`, never parses Job or Workflow YAMLs, never initializes platform execution dependencies, and never executes workflows directly.

---

## 2. Architecture

```text
ScheduleProvider
       |
       v
   Scheduler
       |
       v
RunnerDispatcher
       |
       v
  atlas-runner
       |
       v
atlas-orchestration
```

- **Workspace Resolution (`atlas_scheduler.config`)**: Standalone workspace resolution supporting managed logical workspace identifiers (`$ATLAS_WORKSPACE/<workspace_id>`) and explicit absolute workspace paths while enforcing workspace isolation boundaries.
- **Schedule Provider (`atlas_scheduler.protocols`, `atlas_scheduler.providers`)**: Persistence abstraction returning validated `ScheduleDefinition` domain models.
- **Scheduler Engine (`atlas_scheduler.engine`)**: Persistence-independent control-plane engine that depends exclusively on `ScheduleProvider` and `RunnerDispatcher`, containing zero YAML, filesystem, or database dependencies.
- **Runner Dispatcher (`atlas_scheduler.dispatcher`)**: Spawns one independent `atlas-runner` subprocess per due job and monitors completion non-blockingly.

---

## 3. Schedule YAML Example

Schedule definitions live inside `{workspace_path}/schedules/*.yml` (or `*.yaml`):

```yaml
schedule_id: daily_morning
name: Daily Morning Batch
enabled: true

trigger:
  type: cron
  expression: "0 6 * * *"
  timezone: America/Toronto

jobs:
  - corporations_batch
  - sensors_batch
```

---

## 4. CLI Usage

### Managed Workspace Mode

Requires `ATLAS_WORKSPACE` to point to the managed workspaces root directory (resolves `$ATLAS_WORKSPACE/customer_001`):

```bash
uv run atlas-scheduler --workspace customer_001
```

### Custom Polling Interval and Log Level

```bash
uv run atlas-scheduler --workspace customer_001 --interval 30 --log-level DEBUG
```

### Explicit Absolute Workspace Path Mode

```bash
uv run atlas-scheduler --workspace D:\work-space\atlas-workspace\customer_001
```

### CLI Options

- `--workspace` (required): Logical workspace ID (requires `ATLAS_WORKSPACE` environment variable) or an explicit absolute path to an existing workspace directory.
- `--interval` (optional, default `60`): Polling interval in seconds (must be a positive integer).
- `--log-level` (optional, default `INFO`): Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`).

---

## 5. Execution Semantics

- **One Subprocess per Job**: Each job listed in a schedule's `jobs` list is launched as an independent `uv run atlas-runner --workspace <workspace_id> --job <job_id>` subprocess.
- **Independent Job Dispatch**: Multiple jobs within a single schedule (and across distinct schedules) are dispatched independently; a launch failure in one job is logged and does not prevent subsequent jobs in the schedule from launching.
- **No Direct Job Execution**: The scheduler never loads Job/Workflow YAMLs or executes data-plane logic itself.
- **Non-Blocking Process Monitoring**: `RunnerDispatcher.poll_processes()` inspects active subprocesses on each tick without blocking the scheduling loop, recording completion timestamps and exit codes and removing finished processes from active tracking.
- **No Automatic Retries**: Subprocess launch failures or non-zero runner exit codes are logged and recorded without automatic scheduler-level retries.
- **In-Memory Idempotency**: Processed occurrences are tracked in memory using the key `(schedule_id, scheduled_occurrence_utc)`. Repeated ticks within the same occurrence window cannot dispatch duplicate runs, while a scheduler process restart resets in-memory idempotency state.
- **Bounded Misfire Policy**: The engine uses a bounded misfire grace window (`max(interval * 2, 300)` seconds). Slightly delayed ticks within the grace window dispatch the most recent eligible occurrence once; older historical occurrences outside the window are skipped so no unbounded execution backlog is created.
- **Timezone & Daylight Saving Time Scope**: Phase 1 supports timezone-aware cron scheduling using standard timezone libraries. Special scheduling guarantees around daylight-saving transitions, including nonexistent and ambiguous local times, are not explicitly guaranteed. This behavior can be addressed in a future scheduling enhancement.
- **Graceful Shutdown**: On `SIGINT` or `SIGTERM`, the scheduler stops starting new ticks and exits the scheduling loop cleanly without terminating already-dispatched `atlas-runner` subprocesses.

---

## 6. Provider Extensibility

`Scheduler` depends strictly on the `ScheduleProvider` protocol (`get_schedules() -> list[ScheduleDefinition]`), enabling future persistence backends without modifying the scheduling engine:

```text
             ScheduleProvider
                    |
          +---------+---------+
          |                   |
          v                   v
 YamlScheduleProvider  DatabaseScheduleProvider
          |                   |
          +---------+---------+
                    |
                    v
                Scheduler
```

Only `YamlScheduleProvider` is implemented in Phase 1. YAML/database synchronization will be handled by a separate future service.


## 7. Future Roadmap

Phase 1 uses YAML-based schedule definitions through the `ScheduleProvider` abstraction. The following enhancements are deferred to future phases:

1. **Database Provider**  
   Implement `DatabaseScheduleProvider` as an alternative to `YamlScheduleProvider` without modifying the scheduler engine.

2. **Configuration Synchronization**  
   Introduce a dedicated synchronization service to push and pull schedule definitions between YAML files and a database, including change detection and conflict resolution.

3. **Persistent Idempotency**  
   Persist scheduled occurrence records to prevent duplicate dispatches across scheduler restarts.

4. **Execution Recovery**  
   Integrate scheduler dispatch tracking with `atlas-orchestration` execution history to support recovery and operational visibility.

5. **Distributed Scheduling**  
   Introduce coordination and distributed locking mechanisms when multiple scheduler instances are required.

6. **Advanced Scheduling**  
   Support retry policies, misfire handling, and daylight-saving transition guarantees when production requirements justify the added complexity.

### Architectural Principle

> `atlas-scheduler` remains a consumer of the `ScheduleProvider` abstraction.
>
> Database persistence, synchronization, and configuration management must remain outside the scheduler engine and should be implemented through dedicated providers or supporting services.