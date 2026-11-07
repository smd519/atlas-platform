# Atlas Orchestration (`atlas-orchestration`)

## 1. Overview

`atlas-orchestration` is the workflow orchestration module of the **Atlas Platform** modular monorepo.

Version `0.1.0` encompasses:
- **Phase 1: Domain Foundation** — strongly typed domain models, DAG validation and 3-color DFS cycle detection, deterministic Kahn's topological sorting, thread-safe versioned `WorkflowRegistry`, isolated 3-namespace `ExecutionContext`, and thread-safe `InMemoryExecutionTracker`.
- **Phase 2: Sequential Workflow Execution** — sequential `WorkflowEngine`, dynamic `${...}` `ContextResolver` with native Python type preservation, `StepExecutorRegistry`, `MockStepExecutor`, and concrete module adapters (`IngestionExecutor`, `ProcessingExecutor`, `QualityExecutor`).
- **Phase 3: Execution Reliability & Observability** — configurable per-step `RetryPolicy` with exponential backoff and retry eligibility classification, `StepAttempt` attempt history tracking with context isolation, cooperative monotonic timeout enforcement (`ExecutionDeadline`, `StepTimeoutError`), structured execution event system (`ExecutionEvent`, `ExecutionEventPublisher`), and persistent SQLite execution tracking (`SQLiteExecutionTracker` with WAL mode and schema versioning).

---

## 2. Architecture

```text
+-----------------------------------------------------------------------+
|                        OrchestrationService                           |
|  (Workflow Registration, Validation, Sequential Execution, Queries)   |
+-------------------+---------------------------+-----------------------+
                    |                           |
                    v                           v
+---------------------------------+ +-----------------------------------+
|         WorkflowEngine          | |         WorkflowRegistry          |
|  - Validates via Validator      | |  - Thread-safe (RLock)            |
|  - Executes topological order   | |  - Versioned (workflow_id, ver)   |
|  - Resolves dynamic parameters  | +-----------------------------------+
|  - Retries & exponential backoff|
|  - Cooperative timeout checks   | +-----------------------------------+
|  - Emits ExecutionEvents        | |   ExecutionTracker Implementations|
|  - Fail-fast step cancellation  | |  - InMemoryExecutionTracker       |
+--------+---------------+--------+ |  - SQLiteExecutionTracker (WAL)   |
         |               |          +-----------------------------------+
         v               v
+----------------+ +----------------------------------------------------+
| ContextResolver| |                StepExecutorRegistry                |
| ${steps...}    | |  - "atlas-ingestion"  -> IngestionExecutor         |
| ${variables...}| |  - "atlas-processing" -> ProcessingExecutor        |
| ${metadata...} | |  - "atlas-quality"    -> QualityExecutor           |
+----------------+ +----------------------------------------------------+
```

---

## 3. Package Structure

```text
modules/atlas-orchestration/
├── pyproject.toml
├── README.md
├── src/
│   └── atlas_orchestration/
│       ├── __init__.py
│       ├── py.typed
│       ├── exceptions.py
│       ├── registry.py
│       ├── service.py
│       ├── dependencies.py
│       ├── models/
│       │   ├── __init__.py
│       │   ├── events.py
│       │   ├── retry.py
│       │   ├── workflow.py
│       │   ├── workflow_step.py
│       │   ├── workflow_execution.py
│       │   ├── step_execution.py
│       │   └── workflow_result.py
│       ├── validation/
│       │   ├── __init__.py
│       │   └── workflow_validator.py
│       ├── execution/
│       │   ├── __init__.py
│       │   ├── context.py
│       │   ├── engine.py
│       │   ├── executor.py
│       │   ├── registry.py
│       │   ├── resolver.py
│       │   ├── state.py
│       │   ├── timeout.py
│       │   └── executors/
│       │       ├── __init__.py
│       │       ├── ingestion.py
│       │       ├── mock.py
│       │       ├── processing.py
│       │       └── quality.py
│       └── tracking/
│           ├── __init__.py
│           ├── execution_tracker.py
│           ├── memory.py
│           └── sqlite_tracker.py
└── tests/
    ├── unit/
    └── integration/
```

---

## 4. Sequential Execution Lifecycle, Retries & Timeouts

### Workflow States
`PENDING → RUNNING → SUCCEEDED / FAILED`

### Step States
`PENDING → RUNNING → SUCCEEDED / FAILED / CANCELLED`

### Execution Algorithm (`WorkflowEngine`)
1. Validate the `WorkflowDefinition` via `WorkflowValidator` and obtain the deterministic topological execution order.
2. Initialize an isolated `ExecutionContext` with workflow variables and metadata.
3. Persist a `WorkflowExecution` record in `PENDING` status, transition it to `RUNNING`, and emit `WorkflowStarted`.
4. Pre-create `StepExecution` records in `PENDING` status for all steps in topological order.
5. For each step in topological order:
   - Transition the step's `StepExecution` to `RUNNING`.
   - For each attempt `1 .. max_attempts` (configured via `step.retry_policy`):
     - Emit `StepStarted(attempt_number=...)`.
     - Resolve dynamic `${...}` parameter references via `ContextResolver` without mutating the original `WorkflowDefinition`.
     - Execute the step with cooperative monotonic timeout enforcement (`execute_with_timeout`).
     - If the attempt succeeds within `step.timeout_seconds`:
       - Record a `SUCCEEDED` `StepAttempt` in the tracker.
       - Commit normalized outputs to `context.step_outputs[step_id]` **once**.
       - Transition `StepExecution` to `SUCCEEDED` and emit `StepSucceeded`.
     - If the attempt fails or times out:
       - Record a `FAILED` `StepAttempt` (with `timed_out=True` if `StepTimeoutError`) without polluting `context.step_outputs`.
       - Emit `StepTimedOut` if timed out.
       - If eligible for retry (`attempt < max_attempts` and `is_retryable_exception(exc)`), compute backoff delay via `RetryPolicy.calculate_delay`, emit `StepRetryScheduled`, sleep via the injected `sleep_fn`, and proceed to the next attempt.
       - Otherwise, transition `StepExecution` to `FAILED`, emit `StepFailed`, cancel all subsequent unexecuted steps (`CANCELLED` + `StepCancelled`), transition `WorkflowExecution` to `FAILED`, and emit `WorkflowFailed`.
6. When all steps succeed, transition `WorkflowExecution` to `SUCCEEDED` and emit `WorkflowSucceeded`.

---

## 5. Persistent SQLite Execution Tracking (`SQLiteExecutionTracker`)

`SQLiteExecutionTracker` provides durable, zero-dependency execution state persistence using Python's standard `sqlite3` module:

- Supports both `:memory:` and file-backed database paths.
- Enables `PRAGMA journal_mode = WAL;` and `PRAGMA foreign_keys = ON;`.
- Tracks schema migrations in `schema_migrations` (`SCHEMA_VERSION = 1`).
- Persists `workflow_executions`, `step_executions`, and `step_attempts` atomically inside transactions.

---

## 6. Development and Testing

```bash
# Run unit and integration tests for atlas-orchestration
pytest modules/atlas-orchestration/tests -v

# Run Ruff linter and formatter checks
ruff check modules/atlas-orchestration
ruff format --check modules/atlas-orchestration

# Run Mypy strict type checking
mypy --strict modules/atlas-orchestration/src modules/atlas-orchestration/tests
```
