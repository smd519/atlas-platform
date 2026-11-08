"""Unit and integration tests for OrchestrationService.execute_job() and JobResult."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from atlas_core import ExecutionStatus
from atlas_orchestration import (
    BaseStepExecutor,
    ExecutionContext,
    InvalidWorkflowDefinitionError,
    InvalidWorkflowStateError,
    JobResult,
    MockStepExecutor,
    OrchestrationDependencies,
    OrchestrationService,
    SQLiteExecutionTracker,
    StepExecution,
    WorkflowDefinition,
    WorkflowResult,
    WorkflowStep,
    WorkflowVersionConflictError,
    WorkspaceContext,
)


def _make_workflow(
    workflow_id: str,
    step_ids: list[str],
    *,
    version: int = 1,
    module_target: str = "mock-module",
) -> WorkflowDefinition:
    steps: list[WorkflowStep] = []
    for idx, step_id in enumerate(step_ids):
        depends = [step_ids[idx - 1]] if idx > 0 else []
        steps.append(
            WorkflowStep(
                step_id=step_id,
                module_target=module_target,
                action="run",
                depends_on=depends,
                parameters={"workflow_id": workflow_id, "step_id": step_id},
            )
        )
    return WorkflowDefinition(
        workflow_id=workflow_id,
        name=f"Workflow {workflow_id}",
        version=version,
        steps=steps,
    )


class OrderRecordingExecutor(BaseStepExecutor):
    """Executor that records the exact execution order of (workflow_id, step_id) pairs."""

    def __init__(self, *, fail_on: tuple[str, str] | None = None) -> None:
        self.executed: list[tuple[str, str]] = []
        self.contexts: list[ExecutionContext] = []
        self._fail_on = fail_on

    def execute(
        self,
        step: WorkflowStep,
        context: ExecutionContext,
        dependencies: OrchestrationDependencies,
    ) -> StepExecution:
        wf_id = context.workflow_id
        self.executed.append((wf_id, step.step_id))
        self.contexts.append(context.snapshot())
        if self._fail_on == (wf_id, step.step_id):
            return StepExecution(
                execution_id=context.execution_id,
                step_id=step.step_id,
                module_target=step.module_target,
                status=ExecutionStatus.FAILED,
                error_code="STEP_FAILED",
                error_message=f"Simulated failure in {wf_id}.{step.step_id}",
            )
        return StepExecution(
            execution_id=context.execution_id,
            step_id=step.step_id,
            module_target=step.module_target,
            status=ExecutionStatus.SUCCEEDED,
            output_references={"done": f"{wf_id}:{step.step_id}"},
        )


class TestJobResultAndWorkspaceContextModels:
    """Domain model validation for WorkspaceContext and JobResult."""

    def test_workspace_context_normalizes_root_path(self, tmp_path: Path) -> None:
        ws = WorkspaceContext(tmp_path)
        assert ws.root_path == tmp_path.resolve()
        assert ws.workspace_path == tmp_path.resolve()
        assert ws.path == tmp_path.resolve()

        ws_kw = WorkspaceContext(workspace_path=str(tmp_path))
        assert ws_kw.root_path == tmp_path.resolve()

    def test_workspace_context_rejects_empty_path(self) -> None:
        with pytest.raises(InvalidWorkflowStateError):
            WorkspaceContext("")

    def test_job_result_aggregates_workflow_outcomes(self) -> None:
        wf_res_1 = WorkflowResult(
            execution_id="exec-wf-1",
            workflow_id="corporations_pipeline",
            status=ExecutionStatus.SUCCEEDED,
            duration=1.2,
        )
        wf_res_2 = WorkflowResult(
            execution_id="exec-wf-2",
            workflow_id="sensors_pipeline",
            status=ExecutionStatus.SUCCEEDED,
            duration=0.8,
        )
        job_res = JobResult(
            job_id="morning_batch",
            execution_id="exec-job-1",
            status=ExecutionStatus.SUCCEEDED,
            workflow_results=[wf_res_1, wf_res_2],
            duration=2.0,
        )
        assert job_res.job_id == "morning_batch"
        assert job_res.execution_id == "exec-job-1"
        assert job_res.status == ExecutionStatus.SUCCEEDED
        assert job_res.success is True
        assert len(job_res.workflow_results) == 2
        assert job_res.workflow_results[0].workflow_id == "corporations_pipeline"
        assert job_res.workflow_results[1].workflow_id == "sensors_pipeline"


class TestExecuteJobBatchOrchestration:
    """OrchestrationService.execute_job() sequential execution, tracking, and failure behavior."""

    def test_execute_job_runs_multiple_workflows_sequentially(self, tmp_path: Path) -> None:
        recorder = OrderRecordingExecutor()
        service = OrchestrationService()
        service.register_executor("mock-module", recorder)

        wf_1 = _make_workflow("corporations_pipeline", ["ingest", "process", "quality"])
        wf_2 = _make_workflow("sensors_pipeline", ["ingest", "transform", "assess"])
        workspace = WorkspaceContext(tmp_path)

        register_calls: list[str] = []
        execute_wf_calls: list[str] = []
        orig_register = service.register_workflow
        orig_execute = service.execute_workflow

        def spy_register(workflow: WorkflowDefinition) -> WorkflowDefinition:
            register_calls.append(workflow.workflow_id)
            return orig_register(workflow)

        def spy_execute(workflow: WorkflowDefinition | str, **kwargs: Any) -> WorkflowResult:
            wf_id = workflow if isinstance(workflow, str) else workflow.workflow_id
            execute_wf_calls.append(wf_id)
            return orig_execute(workflow, **kwargs)

        service.register_workflow = spy_register  # type: ignore[method-assign]
        service.execute_workflow = spy_execute  # type: ignore[method-assign]

        result = service.execute_job(
            job_id="morning_batch",
            workflows=[wf_1, wf_2],
            workspace=workspace,
        )

        # 1. Both workflows were registered and executed via existing orchestration methods
        assert register_calls == ["corporations_pipeline", "sensors_pipeline"]
        assert execute_wf_calls == ["corporations_pipeline", "sensors_pipeline"]

        # 2. Execution was strictly sequential across both workflows and their DAG steps
        assert recorder.executed == [
            ("corporations_pipeline", "ingest"),
            ("corporations_pipeline", "process"),
            ("corporations_pipeline", "quality"),
            ("sensors_pipeline", "ingest"),
            ("sensors_pipeline", "transform"),
            ("sensors_pipeline", "assess"),
        ]

        # 3. JobResult aggregates both workflow results
        assert isinstance(result, JobResult)
        assert result.job_id == "morning_batch"
        assert result.status == ExecutionStatus.SUCCEEDED
        assert result.success is True
        assert result.start_time is not None
        assert result.end_time is not None
        assert result.duration >= 0.0
        assert len(result.workflow_results) == 2
        assert result.workflow_results[0].workflow_id == "corporations_pipeline"
        assert result.workflow_results[0].status == ExecutionStatus.SUCCEEDED
        assert result.workflow_results[1].workflow_id == "sensors_pipeline"
        assert result.workflow_results[1].status == ExecutionStatus.SUCCEEDED

        # 4. Parent Job execution status is tracked and finalized in ExecutionTracker
        parent_exec = service.get_workflow_execution(result.execution_id)
        assert parent_exec.workflow_id == "morning_batch"
        assert parent_exec.status == ExecutionStatus.SUCCEEDED
        assert parent_exec.start_time is not None
        assert parent_exec.end_time is not None
        assert parent_exec.context_overrides["job_id"] == "morning_batch"
        assert parent_exec.context_overrides["workflow_ids"] == [
            "corporations_pipeline",
            "sensors_pipeline",
        ]

        # 5. Child workflow contexts carry correlation_id == parent job execution_id
        for ctx in recorder.contexts:
            assert ctx.correlation_id == result.execution_id
            assert ctx.get_metadata("job_id") == "morning_batch"
            assert ctx.get_metadata("workspace") == str(tmp_path.resolve())

    def test_execute_job_stops_on_first_workflow_failure_and_marks_job_failed(
        self, tmp_path: Path
    ) -> None:
        recorder = OrderRecordingExecutor(fail_on=("corporations_pipeline", "process"))
        service = OrchestrationService()
        service.register_executor("mock-module", recorder)

        wf_1 = _make_workflow("corporations_pipeline", ["ingest", "process", "quality"])
        wf_2 = _make_workflow("sensors_pipeline", ["ingest", "transform"])
        workspace = WorkspaceContext(tmp_path)

        result = service.execute_job(
            job_id="morning_batch",
            workflows=[wf_1, wf_2],
            workspace=workspace,
        )

        # Fail-fast: wf_1 failed at 'process', so 'quality' was cancelled and wf_2 never ran
        assert recorder.executed == [
            ("corporations_pipeline", "ingest"),
            ("corporations_pipeline", "process"),
        ]
        assert result.status == ExecutionStatus.FAILED
        assert result.success is False
        assert len(result.workflow_results) == 1
        assert result.workflow_results[0].workflow_id == "corporations_pipeline"
        assert result.workflow_results[0].status == ExecutionStatus.FAILED
        assert "Simulated failure in corporations_pipeline.process" in (result.message or "")

        # Parent Job execution record reflects FAILED status
        parent_exec = service.get_workflow_execution(result.execution_id)
        assert parent_exec.status == ExecutionStatus.FAILED
        assert "Simulated failure in corporations_pipeline.process" in (
            parent_exec.error_message or ""
        )

    def test_execute_job_second_workflow_failure_preserves_first_workflow_result(
        self, tmp_path: Path
    ) -> None:
        recorder = OrderRecordingExecutor(fail_on=("sensors_pipeline", "transform"))
        service = OrchestrationService()
        service.register_executor("mock-module", recorder)

        wf_1 = _make_workflow("corporations_pipeline", ["ingest", "process"])
        wf_2 = _make_workflow("sensors_pipeline", ["ingest", "transform"])
        workspace = WorkspaceContext(tmp_path)

        result = service.execute_job(
            job_id="morning_batch",
            workflows=[wf_1, wf_2],
            workspace=workspace,
        )

        assert result.status == ExecutionStatus.FAILED
        assert result.success is False
        assert len(result.workflow_results) == 2
        assert result.workflow_results[0].workflow_id == "corporations_pipeline"
        assert result.workflow_results[0].status == ExecutionStatus.SUCCEEDED
        assert result.workflow_results[1].workflow_id == "sensors_pipeline"
        assert result.workflow_results[1].status == ExecutionStatus.FAILED

        parent_exec = service.get_workflow_execution(result.execution_id)
        assert parent_exec.status == ExecutionStatus.FAILED

    def test_execute_job_persists_parent_and_child_executions_in_sqlite_tracker(
        self, tmp_path: Path
    ) -> None:
        db_path = tmp_path / "tracking.db"
        with SQLiteExecutionTracker(db_path) as tracker:
            service = OrchestrationService(tracker=tracker)
            service.register_executor("mock-module", MockStepExecutor())

            wf_1 = _make_workflow("wf_alpha", ["step_1"])
            wf_2 = _make_workflow("wf_beta", ["step_1"])
            workspace = WorkspaceContext(tmp_path)

            result = service.execute_job(
                job_id="sqlite_batch_job",
                workflows=[wf_1, wf_2],
                workspace=workspace,
            )

            assert result.status == ExecutionStatus.SUCCEEDED
            all_execs = tracker.list_workflow_executions()
            assert len(all_execs) == 3
            assert [e.workflow_id for e in all_execs] == [
                "sqlite_batch_job",
                "wf_alpha",
                "wf_beta",
            ]
            assert all(e.status == ExecutionStatus.SUCCEEDED for e in all_execs)

    def test_execute_job_validates_inputs_and_propagates_registration_conflict(
        self, tmp_path: Path
    ) -> None:
        service = OrchestrationService()
        service.register_executor("mock-module", MockStepExecutor())
        wf = _make_workflow("wf_single", ["step_1"])
        workspace = WorkspaceContext(tmp_path)

        with pytest.raises(InvalidWorkflowDefinitionError):
            service.execute_job(job_id="", workflows=[wf], workspace=workspace)

        with pytest.raises(InvalidWorkflowDefinitionError):
            service.execute_job(job_id="bad_job", workflows=[], workspace=workspace)

        with pytest.raises(InvalidWorkflowStateError):
            service.execute_job(
                job_id="bad_ws",
                workflows=[wf],
                workspace=tmp_path,  # type: ignore[arg-type]
            )

        # First job registers wf_single v1; second job on same service hits version conflict
        res = service.execute_job(job_id="job_1", workflows=[wf], workspace=workspace)
        assert res.status == ExecutionStatus.SUCCEEDED

        with pytest.raises(WorkflowVersionConflictError):
            service.execute_job(job_id="job_2", workflows=[wf], workspace=workspace)

        failed_jobs = service.list_workflow_executions(
            workflow_id="job_2", status=ExecutionStatus.FAILED
        )
        assert len(failed_jobs) == 1
