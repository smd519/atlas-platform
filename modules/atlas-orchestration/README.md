# Atlas Orchestration (`atlas-orchestration`)

## 1. Overview

`atlas-orchestration` is the workflow orchestration module of the **Atlas Platform** modular monorepo.

Version `0.1.0` encompasses:
- **Phase 1: Domain Foundation** — strongly typed domain models, DAG validation and 3-color DFS cycle detection, deterministic Kahn's topological sorting, thread-safe versioned `WorkflowRegistry`, isolated 3-namespace `ExecutionContext`, and thread-safe `InMemoryExecutionTracker`.
- **Phase 2: Sequential Workflow Execution** — sequential `WorkflowEngine`, dynamic `${...}` `ContextResolver` with native Python type preservation, `StepExecutorRegistry`, `MockStepExecutor`, and concrete module adapters (`IngestionExecutor`, `ProcessingExecutor`, `QualityExecutor`).

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
|  - Fail-fast step cancellation  |
+--------+---------------+--------+
         |               |
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
│       │   └── executors/
│       │       ├── __init__.py
│       │       ├── ingestion.py
│       │       ├── mock.py
│       │       ├── processing.py
│       │       └── quality.py
│       └── tracking/
│           ├── __init__.py
│           ├── execution_tracker.py
│           └── memory.py
└── tests/
    ├── unit/
    └── integration/
```

---

## 4. Sequential Execution Lifecycle

### Workflow States
`PENDING → RUNNING → SUCCEEDED / FAILED`

### Step States
`PENDING → RUNNING → SUCCEEDED / FAILED / CANCELLED`

### Execution Algorithm (`WorkflowEngine`)
1. Validate the `WorkflowDefinition` via `WorkflowValidator` and obtain the deterministic topological execution order.
2. Initialize an isolated `ExecutionContext` with workflow variables and metadata.
3. Persist a `WorkflowExecution` record in `PENDING` status and transition it to `RUNNING`.
4. Pre-create `StepExecution` records in `PENDING` status for all steps in topological order.
5. For each step in topological order:
   - Transition the step's `StepExecution` to `RUNNING`.
   - Resolve dynamic `${...}` parameter references via `ContextResolver` without mutating the original `WorkflowDefinition`.
   - Resolve the registered `BaseStepExecutor` from `StepExecutorRegistry` by `step.module_target`.
   - Execute the step synchronously and store its normalized outputs in `context.step_outputs[step_id]`.
   - Transition the step's `StepExecution` to `SUCCEEDED`.
6. **Fail-Fast & Cancellation**:
   - If parameter resolution, executor lookup, or step execution fails, the current step transitions to `FAILED`, all subsequent unexecuted steps transition to `CANCELLED`, and the overall `WorkflowExecution` transitions to `FAILED`.

---

## 5. Dynamic Context Resolution (`ContextResolver`)

`ContextResolver` resolves dynamic references in nested step parameters without `eval()`:

- `${steps.<step_id>.<output_key>[.<nested_key>...]}` → `ExecutionContext.step_outputs`
- `${variables.<var_name>[.<nested_key>...]}` / `${workflow.<var_name>}` → `ExecutionContext.workflow_variables`
- `${metadata.<key>}` → `ExecutionContext.execution_metadata`
- Preserves native Python types (`int`, `float`, `bool`, `list`, `dict`, `None`, `str`) when an entire string value is a single reference, and performs string interpolation when embedded within larger text.
- Raises `UnresolvedContextReferenceError` for missing steps, missing output keys, or malformed reference expressions.

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
