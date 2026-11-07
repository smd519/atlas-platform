"""Phase 3 integration tests for retries, backoff, timeouts, events, and SQLite persistence."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from atlas_core import ExecutionStatus
from atlas_orchestration import (
    ExecutionContext,
    ExecutionEvent,
    MockStepExecutor,
    OrchestrationService,
    RetryPolicy,
    SQLiteExecutionTracker,
    StepExecutionError,
    StepExecutorRegistry,
    WorkflowDefinition,
    WorkflowEngine,
    WorkflowStep,
)
from atlas_orchestration.dependencies import OrchestrationDependencies
from atlas_orchestration.execution.executor import BaseStepExecutor
from atlas_orchestration.models.step_execution import StepExecution


class FakeClock:
    """Deterministic monotonic clock and sleep recorder for integration tests."""

    def __init__(self, initial: float = 0.0) -> None:
        self.current = initial
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += seconds

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.current += seconds


class AttemptScriptedExecutor(BaseStepExecutor):
    """Executor that runs a per-attempt script of durations, exceptions, or outputs."""

    def __init__(
        self,
        clock: FakeClock,
        scripts: dict[str, list[dict[str, Any]]],
    ) -> None:
        self.clock = clock
        self.scripts = scripts
        self.attempt_counts: dict[str, int] = {}
        self.received_steps: list[WorkflowStep] = []

    def execute(
        self,
        step: WorkflowStep,
        context: ExecutionContext,
        dependencies: OrchestrationDependencies | None = None,
    ) -> StepExecution:
        self.received_steps.append(step.model_copy(deep=True))
        idx = self.attempt_counts.get(step.step_id, 0)
        self.attempt_counts[step.step_id] = idx + 1

        step_script = self.scripts.get(step.step_id, [])
        spec = step_script[min(idx, len(step_script) - 1)] if step_script else {}

        duration = float(spec.get("duration", 0.1))
        self.clock.advance(duration)

        if "raise" in spec and spec["raise"] is not None:
            exc = spec["raise"]
            if isinstance(exc, Exception):
                raise exc
            raise StepExecutionError(str(exc), step_id=step.step_id)

        outputs = dict(spec.get("outputs", {"step_id": step.step_id, "attempt": idx + 1}))
        return StepExecution(
            execution_id=context.execution_id,
            step_id=step.step_id,
            module_target=step.module_target,
            status=ExecutionStatus.SUCCEEDED,
            module_execution_ref=f"exec_{step.step_id}_a{idx + 1}",
            output_references=outputs,
        )


class TestPhase3EngineIntegration:
    """End-to-end integration tests for Phase 3 reliability and observability."""

    def test_scenario_1_transient_failure_recovered_on_retry(self) -> None:
        clock = FakeClock()
        executor = AttemptScriptedExecutor(
            clock,
            scripts={
                "ingest": [
                    {
                        "duration": 0.2,
                        "outputs": {"artifact_path": "/bronze/data.parquet", "rows": 100},
                    }
                ],
                "process": [
                    {"duration": 0.3, "raise": StepExecutionError("Temporary I/O glitch 1")},
                    {"duration": 0.4, "raise": StepExecutionError("Temporary I/O glitch 2")},
                    {
                        "duration": 0.5,
                        "outputs": {
                            "artifact_path": "/silver/curated.parquet",
                            "output_row_count": 95,
                        },
                    },
                ],
                "quality": [
                    {
                        "duration": 0.2,
                        "outputs": {"overall_status": "PASSED", "quality_score": 1.0},
                    }
                ],
            },
        )
        reg = StepExecutorRegistry()
        reg.register("custom", executor)

        events: list[ExecutionEvent] = []
        engine = WorkflowEngine(
            reg,
            sleep_fn=clock.sleep,
            monotonic_clock=clock,
        )
        engine.register_event_listener(events.append)

        wf = WorkflowDefinition(
            workflow_id="wf_retry_recover",
            version=1,
            name="Retry Recovery Workflow",
            steps=[
                WorkflowStep(
                    step_id="ingest",
                    module_target="custom",
                    action="ingest",
                    parameters={"source": "${workflow.source_uri}"},
                ),
                WorkflowStep(
                    step_id="process",
                    module_target="custom",
                    action="process",
                    depends_on=["ingest"],
                    parameters={"input_path": "${steps.ingest.artifact_path}"},
                    retry_policy=RetryPolicy(
                        max_attempts=3,
                        initial_delay_seconds=1.5,
                        backoff_multiplier=2.0,
                    ),
                ),
                WorkflowStep(
                    step_id="quality",
                    module_target="custom",
                    action="assess",
                    depends_on=["process"],
                    parameters={
                        "curated_path": "${steps.process.artifact_path}",
                        "expected_rows": "${steps.process.output_row_count}",
                    },
                ),
            ],
        )

        ctx = ExecutionContext(workflow_id=wf.workflow_id, workflow_version=wf.version)
        result = engine.execute(
            wf,
            parameters={"source_uri": "s3://bucket/raw.csv"},
            context=ctx,
        )

        assert result.status == ExecutionStatus.SUCCEEDED
        assert clock.sleeps == [pytest.approx(1.5), pytest.approx(3.0)]

        step_map = {s.step_id: s for s in result.step_results}
        assert step_map["ingest"].attempt_number == 1
        assert len(step_map["ingest"].attempts) == 1

        proc_step = step_map["process"]
        assert proc_step.status == ExecutionStatus.SUCCEEDED
        assert proc_step.attempt_number == 3
        assert len(proc_step.attempts) == 3
        assert [a.status for a in proc_step.attempts] == [
            ExecutionStatus.FAILED,
            ExecutionStatus.FAILED,
            ExecutionStatus.SUCCEEDED,
        ]
        assert proc_step.error_code is None
        assert proc_step.error_message is None

        # Verify context output committed cleanly and resolved into quality step
        assert ctx.step_outputs["process"] == {
            "artifact_path": "/silver/curated.parquet",
            "output_row_count": 95,
        }
        quality_received = [s for s in executor.received_steps if s.step_id == "quality"][0]
        assert quality_received.parameters == {
            "curated_path": "/silver/curated.parquet",
            "expected_rows": 95,
        }

        event_types = [e.event_type for e in events]
        assert event_types == [
            "workflow.started",
            "step.started",
            "step.succeeded",
            "step.started",
            "step.retry_scheduled",
            "step.started",
            "step.retry_scheduled",
            "step.started",
            "step.succeeded",
            "step.started",
            "step.succeeded",
            "workflow.succeeded",
        ]

    def test_scenario_2_retries_exhausted_cancels_downstream(self) -> None:
        clock = FakeClock()
        executor = AttemptScriptedExecutor(
            clock,
            scripts={
                "ingest": [{"duration": 0.1, "outputs": {"artifact_path": "/bronze/a.parquet"}}],
                "process": [
                    {"duration": 0.2, "raise": StepExecutionError("Fail 1")},
                    {"duration": 0.2, "raise": StepExecutionError("Fail 2")},
                    {"duration": 0.2, "raise": StepExecutionError("Fail 3")},
                ],
                "quality": [{"duration": 0.1, "outputs": {"overall_status": "PASSED"}}],
            },
        )
        reg = StepExecutorRegistry()
        reg.register("custom", executor)

        events: list[ExecutionEvent] = []
        engine = WorkflowEngine(reg, sleep_fn=clock.sleep, monotonic_clock=clock)
        engine.register_event_listener(events.append)

        wf = WorkflowDefinition(
            workflow_id="wf_exhausted",
            version=1,
            name="Exhausted Workflow",
            steps=[
                WorkflowStep(step_id="ingest", module_target="custom", action="ingest"),
                WorkflowStep(
                    step_id="process",
                    module_target="custom",
                    action="process",
                    depends_on=["ingest"],
                    retry_policy=RetryPolicy(
                        max_attempts=3,
                        initial_delay_seconds=0.5,
                        backoff_multiplier=2.0,
                    ),
                ),
                WorkflowStep(
                    step_id="quality",
                    module_target="custom",
                    action="assess",
                    depends_on=["process"],
                ),
            ],
        )

        ctx = ExecutionContext(workflow_id=wf.workflow_id, workflow_version=wf.version)
        result = engine.execute(wf, context=ctx)

        assert result.status == ExecutionStatus.FAILED
        assert clock.sleeps == [pytest.approx(0.5), pytest.approx(1.0)]
        assert "process" not in ctx.step_outputs
        assert "quality" not in executor.attempt_counts

        step_map = {s.step_id: s for s in result.step_results}
        assert step_map["ingest"].status == ExecutionStatus.SUCCEEDED
        assert step_map["process"].status == ExecutionStatus.FAILED
        assert step_map["process"].attempt_number == 3
        assert len(step_map["process"].attempts) == 3
        assert step_map["quality"].status == ExecutionStatus.CANCELLED

        event_types = [e.event_type for e in events]
        assert event_types == [
            "workflow.started",
            "step.started",
            "step.succeeded",
            "step.started",
            "step.retry_scheduled",
            "step.started",
            "step.retry_scheduled",
            "step.started",
            "step.failed",
            "step.cancelled",
            "workflow.failed",
        ]

    def test_scenario_3_non_retryable_failure_aborts_immediately(self) -> None:
        clock = FakeClock()
        executor = AttemptScriptedExecutor(
            clock,
            scripts={
                "step_a": [
                    {
                        "duration": 0.1,
                        "raise": StepExecutionError(
                            "Invalid configuration parameter",
                            retryable=False,
                            error_code="INVALID_PARAMETERS",
                        ),
                    }
                ],
                "step_b": [{"duration": 0.1, "outputs": {"ok": True}}],
            },
        )
        reg = StepExecutorRegistry()
        reg.register("custom", executor)

        events: list[ExecutionEvent] = []
        engine = WorkflowEngine(reg, sleep_fn=clock.sleep, monotonic_clock=clock)
        engine.register_event_listener(events.append)

        wf = WorkflowDefinition(
            workflow_id="wf_non_retryable",
            version=1,
            name="Non Retryable Workflow",
            steps=[
                WorkflowStep(
                    step_id="step_a",
                    module_target="custom",
                    action="run",
                    retry_policy=RetryPolicy(max_attempts=5, initial_delay_seconds=1.0),
                ),
                WorkflowStep(
                    step_id="step_b",
                    module_target="custom",
                    action="run",
                    depends_on=["step_a"],
                ),
            ],
        )

        result = engine.execute(wf)
        assert result.status == ExecutionStatus.FAILED
        assert clock.sleeps == []
        assert executor.attempt_counts["step_a"] == 1

        step_map = {s.step_id: s for s in result.step_results}
        assert step_map["step_a"].attempt_number == 1
        assert len(step_map["step_a"].attempts) == 1
        assert step_map["step_a"].error_code == "INVALID_PARAMETERS"
        assert step_map["step_b"].status == ExecutionStatus.CANCELLED

        event_types = [e.event_type for e in events]
        assert "step.retry_scheduled" not in event_types
        assert event_types == [
            "workflow.started",
            "step.started",
            "step.failed",
            "step.cancelled",
            "workflow.failed",
        ]

    def test_scenario_4_timeout_with_retry_recovery_and_terminal_timeout(self) -> None:
        # Part A: Timeout on attempt 1, succeeds within timeout on attempt 2
        clock_a = FakeClock()
        executor_a = AttemptScriptedExecutor(
            clock_a,
            scripts={
                "slow_step": [
                    {"duration": 5.0, "outputs": {"should_not_commit": True}},
                    {"duration": 1.0, "outputs": {"committed": True}},
                ]
            },
        )
        reg_a = StepExecutorRegistry()
        reg_a.register("custom", executor_a)

        events_a: list[ExecutionEvent] = []
        engine_a = WorkflowEngine(reg_a, sleep_fn=clock_a.sleep, monotonic_clock=clock_a)
        engine_a.register_event_listener(events_a.append)

        wf_a = WorkflowDefinition(
            workflow_id="wf_timeout_recover",
            version=1,
            name="Timeout Recovery",
            steps=[
                WorkflowStep(
                    step_id="slow_step",
                    module_target="custom",
                    action="run",
                    timeout_seconds=2.0,
                    retry_policy=RetryPolicy(max_attempts=2, initial_delay_seconds=0.5),
                )
            ],
        )
        ctx_a = ExecutionContext(workflow_id=wf_a.workflow_id, workflow_version=wf_a.version)
        res_a = engine_a.execute(wf_a, context=ctx_a)

        assert res_a.status == ExecutionStatus.SUCCEEDED
        assert ctx_a.step_outputs["slow_step"] == {"committed": True}
        step_a = res_a.step_results[0]
        assert len(step_a.attempts) == 2
        assert step_a.attempts[0].timed_out is True
        assert step_a.attempts[0].status == ExecutionStatus.FAILED
        assert step_a.attempts[1].timed_out is False
        assert step_a.attempts[1].status == ExecutionStatus.SUCCEEDED
        assert [e.event_type for e in events_a] == [
            "workflow.started",
            "step.started",
            "step.timed_out",
            "step.retry_scheduled",
            "step.started",
            "step.succeeded",
            "workflow.succeeded",
        ]

        # Part B: Timeout on all attempts
        clock_b = FakeClock()
        executor_b = AttemptScriptedExecutor(
            clock_b,
            scripts={
                "always_slow": [
                    {"duration": 4.0, "outputs": {"ok": False}},
                    {"duration": 3.5, "outputs": {"ok": False}},
                ]
            },
        )
        reg_b = StepExecutorRegistry()
        reg_b.register("custom", executor_b)
        events_b: list[ExecutionEvent] = []
        engine_b = WorkflowEngine(reg_b, sleep_fn=clock_b.sleep, monotonic_clock=clock_b)
        engine_b.register_event_listener(events_b.append)

        wf_b = WorkflowDefinition(
            workflow_id="wf_timeout_fail",
            version=1,
            name="Timeout Fail",
            steps=[
                WorkflowStep(
                    step_id="always_slow",
                    module_target="custom",
                    action="run",
                    timeout_seconds=2.0,
                    retry_policy=RetryPolicy(max_attempts=2, initial_delay_seconds=0.25),
                )
            ],
        )
        res_b = engine_b.execute(wf_b)
        assert res_b.status == ExecutionStatus.FAILED
        step_b = res_b.step_results[0]
        assert step_b.status == ExecutionStatus.FAILED
        assert len(step_b.attempts) == 2
        assert all(a.timed_out for a in step_b.attempts)
        assert [e.event_type for e in events_b] == [
            "workflow.started",
            "step.started",
            "step.timed_out",
            "step.retry_scheduled",
            "step.started",
            "step.timed_out",
            "step.failed",
            "workflow.failed",
        ]

    def test_scenario_5_persistent_sqlite_tracker_end_to_end(
        self,
        tmp_path: Path,
    ) -> None:
        db_file = tmp_path / "e2e_orchestration.db"
        clock = FakeClock()
        mock_exec = MockStepExecutor(
            step_outputs={
                "step_1": {"dataset_uri": "file:///bronze/ds.parquet"},
                "step_2": {"curated_uri": "file:///silver/ds.parquet"},
            },
            fail_until_attempt={"step_2": 2},
        )

        wf = WorkflowDefinition(
            workflow_id="wf_sqlite_e2e",
            version=1,
            name="SQLite E2E Workflow",
            steps=[
                WorkflowStep(step_id="step_1", module_target="mock", action="run"),
                WorkflowStep(
                    step_id="step_2",
                    module_target="mock",
                    action="run",
                    depends_on=["step_1"],
                    parameters={"input": "${steps.step_1.dataset_uri}"},
                    retry_policy=RetryPolicy(max_attempts=3, initial_delay_seconds=0.2),
                ),
            ],
        )

        with SQLiteExecutionTracker(db_file) as sqlite_tracker:
            service = OrchestrationService(
                tracker=sqlite_tracker,
                sleep_fn=clock.sleep,
                monotonic_clock=clock,
                register_default_executors=False,
            )
            service.register_executor("mock", mock_exec)
            service.register_workflow(wf)

            result = service.execute_workflow(
                "wf_sqlite_e2e",
                execution_id="exec_sqlite_100",
                parameters={"env": "staging"},
            )
            assert result.status == ExecutionStatus.SUCCEEDED

        # Re-instantiate SQLiteExecutionTracker from the same file and verify durability
        with SQLiteExecutionTracker(db_file) as reopened_tracker:
            persisted_wf = reopened_tracker.get_workflow_execution("exec_sqlite_100")
            assert persisted_wf.status == ExecutionStatus.SUCCEEDED
            assert persisted_wf.workflow_id == "wf_sqlite_e2e"
            assert persisted_wf.context_overrides == {"env": "staging"}

            persisted_steps = reopened_tracker.list_step_executions("exec_sqlite_100")
            assert [s.step_id for s in persisted_steps] == ["step_1", "step_2"]
            assert persisted_steps[0].attempt_number == 1
            assert len(persisted_steps[0].attempts) == 1

            assert persisted_steps[1].status == ExecutionStatus.SUCCEEDED
            assert persisted_steps[1].attempt_number == 2
            assert persisted_steps[1].output_references == {
                "curated_uri": "file:///silver/ds.parquet"
            }
            assert len(persisted_steps[1].attempts) == 2
            assert persisted_steps[1].attempts[0].status == ExecutionStatus.FAILED
            assert persisted_steps[1].attempts[1].status == ExecutionStatus.SUCCEEDED

    def test_scenario_6_listener_failure_isolation_during_workflow(self) -> None:
        clock = FakeClock()
        mock_exec = MockStepExecutor(
            step_outputs={"s1": {"val": 10}},
            fail_until_attempt={"s1": 2},
        )
        service = OrchestrationService(
            sleep_fn=clock.sleep,
            monotonic_clock=clock,
            register_default_executors=False,
        )
        service.register_executor("mock", mock_exec)

        recorded_events: list[ExecutionEvent] = []

        def broken_listener(event: ExecutionEvent) -> None:
            raise RuntimeError(f"Observer crashed on {event.event_type}")

        service.register_event_listener(broken_listener)
        service.register_event_listener(recorded_events.append)

        wf = WorkflowDefinition(
            workflow_id="wf_broken_observer",
            version=1,
            name="Broken Observer Workflow",
            steps=[
                WorkflowStep(
                    step_id="s1",
                    module_target="mock",
                    action="run",
                    retry_policy=RetryPolicy(max_attempts=2, initial_delay_seconds=0.1),
                )
            ],
        )

        result = service.execute_workflow(wf)
        assert result.status == ExecutionStatus.SUCCEEDED
        assert len(recorded_events) == 6
        assert len(service.event_publisher.delivery_failures) == 6
