# Atlas Orchestration (`atlas-orchestration`)

## 1. Overview

`atlas-orchestration` is the workflow orchestration module of the **Atlas Platform** modular monorepo.

Version `0.1.0` delivers **Phase 1: Domain Foundation**, establishing the strongly typed domain models, DAG validation engine, thread-safe versioned workflow registry, isolated execution context, step execution and state contracts, execution lifecycle tracker, and dependency injection boundaries required to coordinate multi-step data workflows across Atlas Platform modules.

---

## 2. Architecture

`atlas-orchestration` is designed around strict separation of concerns and clean dependency boundaries:

```text
+-------------------------------------------------------------------+
|                      OrchestrationService                         |
|   (Workflow Validation, Registration, Queries, DI Container)      |
+-----------------+-------------------------+-----------------------+
                  |                         |
                  v                         v
+-------------------------------+ +---------------------------------+
|       WorkflowRegistry        | |       ExecutionTracker          |
|  - Thread-safe (RLock)        | |  (InMemoryExecutionTracker)     |
|  - (workflow_id, version)     | |  - Extends StateManager &       |
|  - Immutable snapshots        | |    atlas_core.ExecutionManager  |
+---------------+---------------+ +---------------------------------+
                |
                v
+-------------------------------+ +---------------------------------+
|       WorkflowValidator       | |        ExecutionContext         |
|  - Structural validation      | |  1. workflow_variables          |
|  - Dependency verification    | |  2. step_outputs                |
|  - 3-color DFS cycle check    | |  3. execution_metadata          |
|  - Topological ordering       | |  - JSON-safe & deep-copied      |
+-------------------------------+ +---------------------------------+
```

### Dependency Boundaries

- **Direct Runtime Dependency**: `atlas-core` (reusing `Execution`, `ExecutionStatus`, `ExecutionManager`, `Result`, `OrchestrationError`, `generate_execution_id`, and `require_not_empty`).
- **Decoupled Service Wiring**: Downstream module services (`atlas-ingestion`, `atlas-processing`, `atlas-quality`, `atlas-storage`, `atlas-metadata`) are injected via `OrchestrationDependencies` without introducing hard package imports or circular dependencies.

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
│       │   ├── executor.py
│       │   └── state.py
│       └── tracking/
│           ├── __init__.py
│           ├── execution_tracker.py
│           └── memory.py
└── tests/
    ├── unit/
    └── integration/
```

---

## 4. Core Concepts

- **`WorkflowStep`**: Immutable definition of a single workflow step (`step_id`, `module_target`, `action`, JSON-compatible `parameters`, and `depends_on` prerequisite step IDs).
- **`WorkflowDefinition`**: Immutable, versioned workflow blueprint (`workflow_id`, `name`, `description`, `version >= 1`, `steps`, and JSON-compatible `metadata`).
- **`WorkflowExecution`**: Extends `atlas_core.Execution` with workflow-specific state (`execution_id`, `workflow_id`, `workflow_version`, `status`, timezone-aware `start_time` / `end_time`, `context_overrides`, and `error_message`).
- **`StepExecution`**: Execution record for a single step run (`step_execution_id`, `execution_id`, `step_id`, `module_target`, `status`, `attempt_number`, `start_time`, `end_time`, `module_execution_ref`, `error_code`, `error_message`, and `output_references`).
- **`WorkflowResult`**: Extends `atlas_core.Result` to capture the immutable outcome of a workflow execution (`execution_id`, `workflow_id`, `status`, `step_results`, `duration`, `success`, and `message`).
- **`WorkflowRegistry`**: Thread-safe (`threading.RLock`) in-memory registry that validates workflows prior to registration and enforces atomic uniqueness of `(workflow_id, version)`.

---

## 5. Execution Contracts

### `ExecutionContext`

`ExecutionContext` provides an isolated, thread-safe runtime data carrier with **three strictly separated namespaces** so step outputs cannot accidentally overwrite workflow parameters or system metadata:

1. **`workflow_variables`** (`set_variable`, `get_variable`, `has_variable`): Workflow-scoped input parameters and configuration values.
2. **`step_outputs`** (`set_step_output`, `get_step_output`, `get_step_output_value`, `has_step_output`): Output dictionaries scoped by `step_id`.
3. **`execution_metadata`** (`set_metadata`, `get_metadata`, `has_metadata`): Runtime execution metadata (`execution_id`, `workflow_id`, `workflow_version`, `created_at`, `correlation_id`, and custom tags).

All values stored in `ExecutionContext` are validated for strict JSON compatibility and deep-copied on write and read. Full serialization is supported via `to_dict()`, `from_dict()`, `to_json()`, `from_json()`, and `snapshot()`.

### `BaseStepExecutor`

Abstract contract for step execution implementations:

```python
class BaseStepExecutor(ABC):
    @abstractmethod
    def execute(self, step: WorkflowStep, context: ExecutionContext) -> StepExecution: ...
```

### `StateManager` & `ExecutionTracker`

`StateManager` extends `atlas_core.ExecutionManager` to define persistence operations for `WorkflowExecution` and `StepExecution`. `ExecutionTracker` consolidates `StateManager` as the single authoritative tracking contract, with `InMemoryExecutionTracker` providing a thread-safe (`threading.RLock`) in-memory implementation.

---

## 6. Workflow Validation

`WorkflowValidator` performs comprehensive structural and graph validation before any workflow is registered:

- Verifies non-empty `workflow_id`, `name`, `version >= 1`, and at least one `WorkflowStep`.
- Verifies unique `step_id`s across the workflow and JSON-compatible `parameters` and `metadata`.
- Rejects unknown dependency references (`InvalidWorkflowDependencyError`), self-dependencies, and duplicate dependencies.
- Performs **3-color Depth-First Search (DFS)** cycle detection across the step dependency graph to detect direct cycles (`A -> B -> A`), indirect cycles (`A -> B -> C -> A`), and cycles within complex multi-branch graphs, raising `CyclicDependencyError` with the exact `cycle: tuple[str, ...]` path.
- Computes a deterministic topological execution order using Kahn's algorithm (`WorkflowValidationResult.topological_order`).

---

## 7. Usage Examples

### Defining, Validating, and Registering a Workflow

```python
from atlas_orchestration import (
    OrchestrationService,
    WorkflowDefinition,
    WorkflowStep,
)

service = OrchestrationService()

workflow_v1 = WorkflowDefinition(
    workflow_id="urban_heat_pipeline",
    name="Urban Heat Island Pipeline",
    version=1,
    steps=[
        WorkflowStep(
            step_id="ingest_sensors",
            module_target="atlas-ingestion",
            action="ingest",
            parameters={"source": "noaa_hourly"},
        ),
        WorkflowStep(
            step_id="clean_readings",
            module_target="atlas-processing",
            action="transform",
            parameters={"drop_nulls": True},
            depends_on=["ingest_sensors"],
        ),
        WorkflowStep(
            step_id="validate_curated",
            module_target="atlas-quality",
            action="evaluate",
            parameters={"suite": "temperature_bounds"},
            depends_on=["clean_readings"],
        ),
    ],
)

validation = service.validate_workflow(workflow_v1)
assert validation.topological_order == (
    "ingest_sensors",
    "clean_readings",
    "validate_curated",
)

registered = service.register_workflow(workflow_v1)
latest = service.get_latest_workflow("urban_heat_pipeline")
```

### Using `ExecutionContext` and `InMemoryExecutionTracker`

```python
from datetime import UTC, datetime
from atlas_orchestration import (
    ExecutionContext,
    ExecutionStatus,
    InMemoryExecutionTracker,
    StepExecution,
    WorkflowExecution,
)

context = ExecutionContext(
    workflow_id="urban_heat_pipeline",
    workflow_version=1,
    workflow_variables={"region": "us-east"},
    correlation_id="corr-001",
)
context.set_step_output("ingest_sensors", {"row_count": 2500, "dataset_id": "ds_raw_01"})
assert context.get_step_output_value("ingest_sensors", "row_count") == 2500

tracker = InMemoryExecutionTracker()
wf_exec = tracker.create_workflow_execution(
    WorkflowExecution(
        execution_id=context.execution_id,
        workflow_id="urban_heat_pipeline",
        workflow_version=1,
        status=ExecutionStatus.RUNNING,
        start_time=datetime.now(UTC),
    )
)
step_exec = tracker.create_step_execution(
    StepExecution(
        execution_id=wf_exec.execution_id,
        step_id="ingest_sensors",
        module_target="atlas-ingestion",
        status=ExecutionStatus.SUCCEEDED,
        start_time=datetime.now(UTC),
        end_time=datetime.now(UTC),
        output_references=context.get_step_output("ingest_sensors"),
    )
)
tracker.complete(wf_exec.execution_id)
completed = tracker.get_workflow_execution(wf_exec.execution_id)
assert completed.status == ExecutionStatus.SUCCEEDED
```

---

## 8. Public API Reference

| Symbol | Category | Description |
| --- | --- | --- |
| `OrchestrationService` | Service | Primary entry point for workflow validation, versioned registration, and tracking queries |
| `WorkflowRegistry` | Registry | Thread-safe versioned registry for `WorkflowDefinition` |
| `WorkflowValidator` | Validation | Structural, dependency, and DAG cycle validator |
| `WorkflowValidationResult` | Validation | Immutable validation result containing `topological_order` |
| `WorkflowDefinition` | Model | Immutable versioned workflow blueprint |
| `WorkflowStep` | Model | Immutable step specification within a workflow |
| `WorkflowExecution` | Model | Workflow execution state model extending `atlas_core.Execution` |
| `StepExecution` | Model | Step execution state model |
| `WorkflowResult` | Model | Immutable workflow execution result extending `atlas_core.Result` |
| `ExecutionContext` | Execution | Isolated 3-namespace runtime context with JSON validation and serialization |
| `BaseStepExecutor` | Execution | Abstract contract for executing a single `WorkflowStep` |
| `StateManager` | Execution | Abstract state persistence contract extending `atlas_core.ExecutionManager` |
| `ExecutionTracker` | Tracking | Consolidated execution state tracking contract |
| `InMemoryExecutionTracker` | Tracking | Thread-safe in-memory `ExecutionTracker` implementation |
| `OrchestrationDependencies` | DI | Dependency injection container for platform module services |
| `OrchestrationError` | Exception | Base exception for `atlas-orchestration` (extends `atlas_core.OrchestrationError`) |

---

## 9. Development and Testing

Run all quality gates from the repository root:

```bash
# Run unit and integration tests for atlas-orchestration
pytest modules/atlas-orchestration/tests -v

# Run Ruff linter and formatter checks
ruff check modules/atlas-orchestration
ruff format --check modules/atlas-orchestration

# Run Mypy strict type checking
mypy --strict modules/atlas-orchestration/src modules/atlas-orchestration/tests

# Build distribution artifacts
uv build modules/atlas-orchestration
```

---

## 10. Implementation Scope

### Implemented in Phase 1 (v0.1.0)
- Domain exception hierarchy (`OrchestrationError` and specialized subclasses)
- Immutable domain models (`WorkflowDefinition`, `WorkflowStep`, `WorkflowExecution`, `StepExecution`, `WorkflowResult`)
- DAG and dependency `WorkflowValidator` with 3-color DFS cycle detection and topological sorting
- Thread-safe versioned `WorkflowRegistry`
- Isolated 3-namespace `ExecutionContext` with JSON serialization
- Execution contracts (`BaseStepExecutor`, `StateManager`, `ExecutionTracker`) and `InMemoryExecutionTracker`
- `OrchestrationDependencies` and `OrchestrationService` foundation

### Out of Scope for Phase 1
- Runtime DAG traversal and step dispatching
- Parallel/concurrent step execution engine
- Retry policies, backoff timers, and circuit breakers
- Cron/event scheduling (`atlas-scheduler`)
- Persistent database state backends

---

## 11. Roadmap

- **Phase 1 (v0.1.0 — Current)**: Domain Foundation — models, DAG validator, versioned registry, isolated execution context, tracking contracts, and in-memory tracker.
- **Phase 2 (v0.2.0)**: Sequential Execution Engine — topological step runner, module dispatcher via `OrchestrationDependencies`, and step lifecycle orchestration.
- **Phase 3 (v0.3.0)**: Resilience & Parallelism — concurrent branch execution, configurable step retry policies, timeout enforcement, and failure recovery hooks.
- **Phase 4 (v0.4.0)**: Persistent Tracking & Observability — database-backed `ExecutionTracker`, lineage/event emission to `atlas-metadata`, and external scheduler integration.
