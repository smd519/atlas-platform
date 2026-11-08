"""Unit tests for atlas-runner hardening: isolation, --validate-only, and stateless runs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from atlas_core import ExecutionStatus
from atlas_orchestration import (
    CyclicDependencyError,
    InvalidStepDefinitionError,
    InvalidWorkflowDefinitionError,
    JobResult,
    WorkflowDefinition,
    WorkflowVersionConflictError,
    WorkspaceContext,
)
from atlas_runner import (
    AtlasRunner,
    RunnerConfigurationError,
    RunnerExecutionError,
    WorkspaceIsolationError,
    build_default_dependencies,
    create_orchestration_service,
    load_job_definition,
    load_workflow_definition,
    main,
    merge_parameters,
    resolve_job_reference,
    resolve_workflow_reference,
    resolve_workspace,
    resolve_workspace_path,
)
from pydantic import ValidationError


def _write_yaml(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _valid_workflow_yaml() -> str:
    return """
workflow_id: unit_test_pipeline
name: Unit Test Pipeline
version: 1
steps:
  - step_id: ingest
    module_target: atlas-ingestion
    action: ingest
    parameters:
      dataset_id: default_dataset
      source_format: csv
      format_options:
        delimiter: ","
        encoding: utf-8
      source:
        source_id: src_csv
        source_name: Source CSV
        source_type: local_file
        config:
          path: drop/sample.csv
      destination:
        layer: bronze
        path: data/bronze/sample.parquet
        format: parquet
  - step_id: process
    module_target: atlas-processing
    action: transform
    depends_on:
      - ingest
    parameters:
      dataset_id: "${steps.ingest.dataset_id}"
      source: "${steps.ingest.destination}"
      destination:
        layer: silver
        path: data/silver/sample.parquet
        format: parquet
      transformations:
        - transformation_id: filter
          name: filter_non_null_id
          params:
            column: id
            operator: not_null
  - step_id: quality
    module_target: atlas-quality
    action: assess
    depends_on:
      - process
    parameters:
      dataset_id: "${steps.process.dataset_id}"
      storage_location: "${steps.process.destination}"
      rules:
        - rule_id: completeness.not_null
          name: id_not_null
          params:
            column: id
"""


class TestMissingConfigurationFiles:
    """Missing job or workflow YAML produces a clear FileNotFoundError."""

    def test_missing_job_yaml_raises_file_not_found(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="jobs.*missing\\.yml"):
            load_job_definition(tmp_path, "jobs/missing.yml")

    def test_missing_workflow_yaml_raises_file_not_found(self, tmp_path: Path) -> None:
        _write_yaml(
            tmp_path / "jobs" / "job.yml",
            """
job_id: sample_job
workflow_ref: workflows/does-not-exist.yml
parameters:
  dataset_id: sample_ds
""",
        )
        runner = AtlasRunner()
        with pytest.raises(FileNotFoundError, match="workflows.*does-not-exist\\.yml"):
            runner.run(tmp_path, "jobs/job.yml")


class TestInvalidYamlHandling:
    """Invalid YAML produces a clear configuration error."""

    def test_malformed_job_yaml_raises_configuration_error(self, tmp_path: Path) -> None:
        _write_yaml(
            tmp_path / "jobs" / "bad.yml",
            "job_id: [unterminated_list\nworkflow_ref: ::invalid",
        )
        with pytest.raises(RunnerConfigurationError, match="Invalid YAML syntax") as exc_info:
            load_job_definition(tmp_path, "jobs/bad.yml")
        assert exc_info.value.field == "yaml_syntax"
        assert exc_info.value.config_file == str((tmp_path / "jobs" / "bad.yml").resolve())

    def test_non_mapping_yaml_raises_configuration_error(self, tmp_path: Path) -> None:
        _write_yaml(
            tmp_path / "workflows" / "list.yml",
            "- item1\n- item2\n",
        )
        with pytest.raises(RunnerConfigurationError, match="top-level mapping") as exc_info:
            load_workflow_definition(tmp_path, "workflows/list.yml")
        assert exc_info.value.field == "root"

    def test_malformed_workflow_yaml_raises_configuration_error(self, tmp_path: Path) -> None:
        _write_yaml(
            tmp_path / "workflows" / "broken.yml",
            "workflow_id: test\nsteps: {\n  unclosed: [1, 2\n",
        )
        with pytest.raises(RunnerConfigurationError, match="Invalid YAML syntax"):
            load_workflow_definition(tmp_path, "workflows/broken.yml")


class TestPydanticValidation:
    """Invalid job or workflow structure is rejected by Pydantic."""

    def test_invalid_job_missing_required_fields_rejected(self, tmp_path: Path) -> None:
        _write_yaml(
            tmp_path / "jobs" / "incomplete.yml",
            "job_id: only_job_id\n",
        )
        with pytest.raises(ValidationError):
            load_job_definition(tmp_path, "jobs/incomplete.yml")

    def test_invalid_workflow_missing_steps_rejected(self, tmp_path: Path) -> None:
        _write_yaml(
            tmp_path / "workflows" / "empty_steps.yml",
            """
workflow_id: invalid_wf
name: Invalid Workflow
version: 1
steps: []
""",
        )
        with pytest.raises((ValidationError, InvalidWorkflowDefinitionError)):
            load_workflow_definition(tmp_path, "workflows/empty_steps.yml")

    def test_invalid_workflow_extra_fields_rejected(self, tmp_path: Path) -> None:
        _write_yaml(
            tmp_path / "workflows" / "extra_field.yml",
            """
workflow_id: invalid_wf
name: Invalid Workflow
unknown_field: not_allowed
steps:
  - step_id: ingest
    module_target: atlas-ingestion
    action: ingest
""",
        )
        with pytest.raises((ValidationError, InvalidWorkflowDefinitionError)):
            load_workflow_definition(tmp_path, "workflows/extra_field.yml")

    def test_invalid_step_structure_rejected(self, tmp_path: Path) -> None:
        _write_yaml(
            tmp_path / "workflows" / "bad_step.yml",
            """
workflow_id: invalid_step_wf
name: Invalid Step Workflow
steps:
  - step_id: ingest
    module_target: ""
    action: ingest
""",
        )
        with pytest.raises(
            (ValidationError, InvalidWorkflowDefinitionError, InvalidStepDefinitionError)
        ):
            load_workflow_definition(tmp_path, "workflows/bad_step.yml")


class TestParameterMerge:
    """Job parameters correctly override workflow defaults without recursive merging."""

    def test_merge_parameters_top_level_override_and_preserve_defaults(self) -> None:
        workflow_defaults: dict[str, Any] = {
            "dataset_id": "default_dataset",
            "source_format": "csv",
            "format_options": {"delimiter": ",", "encoding": "utf-8"},
        }
        job_overrides: dict[str, Any] = {
            "dataset_id": "corporations_active",
            "format_options": {"delimiter": ";"},
        }

        merged = merge_parameters(workflow_defaults, job_overrides)

        assert merged["dataset_id"] == "corporations_active"
        assert merged["source_format"] == "csv"
        assert merged["format_options"] == {"delimiter": ";"}
        assert workflow_defaults["dataset_id"] == "default_dataset"
        assert workflow_defaults["format_options"] == {"delimiter": ",", "encoding": "utf-8"}

    def test_workflow_definition_applies_job_parameter_overrides(self, tmp_path: Path) -> None:
        _write_yaml(tmp_path / "workflows" / "pipeline.yml", _valid_workflow_yaml())

        workflow_def, merged_params, _ = load_workflow_definition(
            tmp_path,
            "workflows/pipeline.yml",
            job_parameters={
                "dataset_id": "corporations_active",
                "format_options": {"delimiter": "|"},
            },
        )

        assert isinstance(workflow_def, WorkflowDefinition)
        assert merged_params["dataset_id"] == "corporations_active"

        ingest_step = workflow_def.get_step("ingest")
        assert ingest_step is not None
        assert ingest_step.parameters["dataset_id"] == "corporations_active"
        assert ingest_step.parameters["source_format"] == "csv"
        assert ingest_step.parameters["format_options"] == {"delimiter": "|"}


class TestWorkspaceIsolationAndPathResolution:
    """Strict workspace isolation rejects path traversal and external absolute paths."""

    def test_resolve_workspace_path_relative_and_absolute_within_workspace(
        self, tmp_path: Path
    ) -> None:
        rel_resolved = resolve_workspace_path(tmp_path, "jobs/corporations.yml")
        assert rel_resolved == (tmp_path / "jobs" / "corporations.yml").resolve()

        abs_input = (tmp_path / "other" / "file.yml").resolve()
        abs_resolved = resolve_workspace_path(tmp_path, abs_input)
        assert abs_resolved == abs_input

    def test_step_data_paths_resolve_against_workspace_preserving_references(
        self, tmp_path: Path
    ) -> None:
        _write_yaml(tmp_path / "workflows" / "pipeline.yml", _valid_workflow_yaml())

        workflow_def, _, resolved_wf_path = load_workflow_definition(
            tmp_path,
            "workflows/pipeline.yml",
            job_parameters={"dataset_id": "corporations_active"},
        )

        assert resolved_wf_path == (tmp_path / "workflows" / "pipeline.yml").resolve()

        ingest_step = workflow_def.get_step("ingest")
        process_step = workflow_def.get_step("process")
        quality_step = workflow_def.get_step("quality")
        assert ingest_step is not None
        assert process_step is not None
        assert quality_step is not None

        assert ingest_step.parameters["source"]["config"]["path"] == str(
            (tmp_path / "drop" / "sample.csv").resolve()
        )
        assert ingest_step.parameters["destination"]["path"] == str(
            (tmp_path / "data" / "bronze" / "sample.parquet").resolve()
        )
        assert process_step.parameters["source"] == "${steps.ingest.destination}"
        assert process_step.parameters["destination"]["path"] == str(
            (tmp_path / "data" / "silver" / "sample.parquet").resolve()
        )
        assert quality_step.parameters["storage_location"] == "${steps.process.destination}"

    def test_malicious_workflow_parameter_path_traversal_raises_workspace_isolation_error(
        self, tmp_path: Path
    ) -> None:
        workspace = tmp_path / "customer_ws"
        workspace.mkdir(parents=True, exist_ok=True)

        _write_yaml(
            workspace / "workflows" / "malicious.yml",
            """
workflow_id: malicious_wf
name: Malicious Workflow
version: 1
steps:
  - step_id: process
    module_target: atlas-processing
    action: transform
    parameters:
      dataset_id: test_ds
      input_path: data/bronze/input.parquet
      output_path: ../../outside_workspace.parquet
""",
        )
        _write_yaml(
            workspace / "jobs" / "malicious_job.yml",
            """
job_id: malicious_job
workflow_ref: workflows/malicious.yml
parameters: {}
""",
        )

        runner = AtlasRunner()
        with pytest.raises(WorkspaceIsolationError) as exc_info:
            runner.run(workspace, "jobs/malicious_job.yml")

        assert exc_info.value.field == "steps.process.parameters.output_path"
        assert exc_info.value.offending_path == "../../outside_workspace.parquet"
        assert exc_info.value.config_file == str(
            (workspace / "workflows" / "malicious.yml").resolve()
        )

    def test_job_path_or_workflow_ref_escaping_workspace_raises_isolation_error(
        self, tmp_path: Path
    ) -> None:
        workspace = tmp_path / "customer_ws"
        workspace.mkdir(parents=True, exist_ok=True)

        with pytest.raises(WorkspaceIsolationError):
            load_job_definition(workspace, "../../etc/shadow")

        _write_yaml(
            workspace / "jobs" / "escape_wf.yml",
            """
job_id: escape_wf
workflow_ref: ../../outside_workflow.yml
parameters: {}
""",
        )
        runner = AtlasRunner()
        with pytest.raises(WorkspaceIsolationError) as exc_info:
            runner.validate(workspace, "jobs/escape_wf.yml")
        assert exc_info.value.field == "workflow_ref"


class TestValidateOnlyModeAndPreciseErrorReporting:
    """--validate-only catches cyclic dependencies and skips service dependency construction."""

    def test_validate_only_catches_cyclic_workflow_without_initializing_dependencies(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        wf_file = _write_yaml(
            tmp_path / "workflows" / "cyclic.yml",
            """
workflow_id: cyclic_pipeline
name: Cyclic Pipeline
version: 1
steps:
  - step_id: step_a
    module_target: atlas-ingestion
    action: ingest
    depends_on:
      - step_b
    parameters:
      dataset_id: ds
  - step_id: step_b
    module_target: atlas-processing
    action: transform
    depends_on:
      - step_a
    parameters:
      dataset_id: ds
""",
        )
        _write_yaml(
            tmp_path / "jobs" / "cyclic_job.yml",
            """
job_id: cyclic_job
workflow_ref: workflows/cyclic.yml
parameters:
  dataset_id: ds_override
""",
        )

        dependency_calls = 0

        def forbidden_dependency_factory() -> Any:
            nonlocal dependency_calls
            dependency_calls += 1
            raise AssertionError("Dependencies must NOT be constructed in --validate-only mode.")

        runner = AtlasRunner(dependency_factory=forbidden_dependency_factory)

        # 1. Direct validate() raises CyclicDependencyError with exact file and field
        with pytest.raises(CyclicDependencyError) as exc_info:
            runner.validate(tmp_path, "jobs/cyclic_job.yml")

        assert dependency_calls == 0
        assert runner.last_dependencies is None
        assert runner.orchestrator is None
        assert getattr(exc_info.value, "config_file", None) == str(wf_file.resolve())
        assert getattr(exc_info.value, "field", None) == "steps.step_a.depends_on"

        # 2. CLI --validate-only returns non-zero exit code, pinpoints file & field, skips deps
        exit_code = main(
            [
                "--workspace",
                str(tmp_path),
                "--job",
                "jobs/cyclic_job.yml",
                "--validate-only",
            ],
            runner=runner,
        )
        assert exit_code != 0
        assert dependency_calls == 0

        captured = capsys.readouterr()
        assert "Status: INVALID" in captured.err
        assert str(wf_file.resolve()) in captured.err
        assert "Field: steps.step_a.depends_on" in captured.err
        assert "step_a -> step_b -> step_a" in captured.err

    def test_validate_only_succeeds_without_initializing_dependencies(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        _write_yaml(tmp_path / "workflows" / "pipeline.yml", _valid_workflow_yaml())
        _write_yaml(
            tmp_path / "jobs" / "valid_job.yml",
            """
job_id: valid_job
workflow_ref: workflows/pipeline.yml
parameters:
  dataset_id: valid_dataset
""",
        )

        dependency_calls = 0

        def forbidden_dependency_factory() -> Any:
            nonlocal dependency_calls
            dependency_calls += 1
            raise AssertionError("Dependencies must NOT be constructed in --validate-only mode.")

        runner = AtlasRunner(dependency_factory=forbidden_dependency_factory)
        exit_code = main(
            [
                "--workspace",
                str(tmp_path),
                "--job",
                "jobs/valid_job.yml",
                "--validate-only",
            ],
            runner=runner,
        )

        assert exit_code == 0
        assert dependency_calls == 0
        assert runner.last_dependencies is None
        assert runner.orchestrator is None

        captured = capsys.readouterr()
        assert "========== ATLAS VALIDATION ==========" in captured.out
        assert "Status: VALID" in captured.out
        assert "  ingest:  VALID" in captured.out
        assert "  process:  VALID" in captured.out
        assert "  quality:  VALID" in captured.out


class TestStatelessExecutionAndStandardizedReporting:
    """Sequential runs instantiate separate dependency graphs and emit standardized reports."""

    def test_sequential_runs_instantiate_separate_dependency_graphs(self, tmp_path: Path) -> None:
        _write_yaml(tmp_path / "workflows" / "pipeline.yml", _valid_workflow_yaml())
        _write_yaml(
            tmp_path / "jobs" / "job_1.yml",
            """
job_id: job_1
workflow_ref: workflows/pipeline.yml
parameters:
  dataset_id: ds_1
""",
        )
        _write_yaml(
            tmp_path / "jobs" / "job_2.yml",
            """
job_id: job_2
workflow_ref: workflows/pipeline.yml
parameters:
  dataset_id: ds_2
""",
        )
        csv_file = tmp_path / "drop" / "sample.csv"
        csv_file.parent.mkdir(parents=True, exist_ok=True)
        csv_file.write_text("id,name\n1,Alpha\n2,Beta\n", encoding="utf-8")

        created_deps: list[Any] = []

        def tracking_dependency_factory() -> Any:
            deps = build_default_dependencies()
            created_deps.append(deps)
            return deps

        runner = AtlasRunner(dependency_factory=tracking_dependency_factory)

        res_1 = runner.run(tmp_path, "jobs/job_1.yml")
        orch_1 = runner.orchestrator
        deps_1 = runner.last_dependencies

        res_2 = runner.run(tmp_path, "jobs/job_2.yml")
        orch_2 = runner.orchestrator
        deps_2 = runner.last_dependencies

        assert res_1.status == ExecutionStatus.SUCCEEDED
        assert res_2.status == ExecutionStatus.SUCCEEDED
        assert len(created_deps) == 2
        assert deps_1 is not None and deps_2 is not None
        assert deps_1 is not deps_2
        assert orch_1 is not None and orch_2 is not None
        assert orch_1 is not orch_2
        assert deps_1.ingestion_service is not deps_2.ingestion_service
        assert deps_1.processing_service is not deps_2.processing_service
        assert deps_1.quality_service is not deps_2.quality_service
        assert deps_1.storage_provider is not deps_2.storage_provider

    def test_runner_and_cli_standardized_report_and_exit_codes(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        _write_yaml(tmp_path / "workflows" / "pipeline.yml", _valid_workflow_yaml())
        _write_yaml(
            tmp_path / "jobs" / "sample.yml",
            """
job_id: sample_job
workflow_ref: workflows/pipeline.yml
parameters:
  dataset_id: sample_dataset
""",
        )

        # 1. Before input CSV exists, execution fails and CLI returns non-zero exit code
        exit_code_fail = main(["--workspace", str(tmp_path), "--job", "jobs/sample.yml"])
        assert exit_code_fail != 0
        fail_out = capsys.readouterr().out
        assert "========== ATLAS EXECUTION ==========" in fail_out
        assert "Job: sample_job" in fail_out
        assert "Status: FAILED" in fail_out
        assert "  ingest:  FAILED" in fail_out
        assert "  process:  CANCELLED" in fail_out
        assert "  quality:  CANCELLED" in fail_out

        runner = AtlasRunner()
        with pytest.raises(RunnerExecutionError):
            runner.run(tmp_path, "jobs/sample.yml", raise_on_error=True)

        # 2. When an explicit OrchestrationService is injected, version conflict integrity holds
        injected_orch = runner.orchestrator
        assert injected_orch is not None
        pinned_runner = AtlasRunner(orchestration_service=injected_orch)
        with pytest.raises(WorkflowVersionConflictError):
            pinned_runner.run(tmp_path, "jobs/sample.yml")

        # 3. Create input CSV and execute successfully
        csv_file = tmp_path / "drop" / "sample.csv"
        csv_file.parent.mkdir(parents=True, exist_ok=True)
        csv_file.write_text("id,name\n1,Alpha\n2,Beta\n", encoding="utf-8")

        exit_code_ok = main(["--workspace", str(tmp_path), "--job", "jobs/sample.yml"])
        assert exit_code_ok == 0
        ok_out = capsys.readouterr().out
        assert "========== ATLAS EXECUTION ==========" in ok_out
        assert "Job: sample_job" in ok_out
        assert "Workflow: unit_test_pipeline" in ok_out
        assert "Status: SUCCEEDED" in ok_out
        assert (
            "Steps:\n  ingest:  SUCCEEDED\n  process:  SUCCEEDED\n  quality:  SUCCEEDED" in ok_out
        )
        assert "Duration: " in ok_out
        assert " seconds\n====================================" in ok_out


class TestBatchWorkflowAssemblyAndHandoff:
    """Runner batch Job YAML loading, workflow resolution, assembly, and execute_job() handoff."""

    def test_load_job_yaml_with_multiple_workflows_and_resolve_references(
        self, tmp_path: Path
    ) -> None:
        _write_yaml(
            tmp_path / "jobs" / "morning_batch.yml",
            """
job_id: morning_batch
name: Morning Batch
description: Daily ingestion and processing batch.

workflows:
  - corporations_pipeline
  - sensors_pipeline
""",
        )
        _write_yaml(
            tmp_path / "workflows" / "corporations-pipeline.yml",
            _valid_workflow_yaml().replace("unit_test_pipeline", "corporations_pipeline"),
        )
        _write_yaml(
            tmp_path / "workflows" / "sensors_pipeline.yml",
            _valid_workflow_yaml().replace("unit_test_pipeline", "sensors_pipeline"),
        )

        job_def, resolved_job = load_job_definition(tmp_path, "jobs/morning_batch.yml")
        assert job_def.job_id == "morning_batch"
        assert job_def.name == "Morning Batch"
        assert job_def.workflows == ["corporations_pipeline", "sensors_pipeline"]
        assert resolved_job == (tmp_path / "jobs" / "morning_batch.yml").resolve()

        # Logical workflow names resolve to files in workflows/ (including hyphen/underscore)
        corp_path = resolve_workflow_reference(tmp_path, "corporations_pipeline")
        sensors_path = resolve_workflow_reference(tmp_path, "sensors_pipeline")
        assert corp_path == (tmp_path / "workflows" / "corporations-pipeline.yml").resolve()
        assert sensors_path == (tmp_path / "workflows" / "sensors_pipeline.yml").resolve()

        wf_1, _, _ = load_workflow_definition(tmp_path, "corporations_pipeline")
        wf_2, _, _ = load_workflow_definition(tmp_path, "sensors_pipeline")
        assert wf_1.workflow_id == "corporations_pipeline"
        assert wf_2.workflow_id == "sensors_pipeline"

    def test_invalid_workflow_references_produce_meaningful_errors(self, tmp_path: Path) -> None:
        _write_yaml(
            tmp_path / "jobs" / "bad_ref.yml",
            """
job_id: bad_ref_job
workflows:
  - missing_workflow_pipeline
""",
        )
        runner = AtlasRunner()
        with pytest.raises(FileNotFoundError, match="missing_workflow_pipeline"):
            runner.run(tmp_path, "jobs/bad_ref.yml")

        _write_yaml(
            tmp_path / "jobs" / "escape_batch.yml",
            """
job_id: escape_batch_job
workflows:
  - ../../outside_workflow
""",
        )
        with pytest.raises(WorkspaceIsolationError):
            runner.run(tmp_path, "jobs/escape_batch.yml")

    def test_runner_calls_execute_job_once_and_never_calls_execute_workflow(
        self, tmp_path: Path
    ) -> None:
        _write_yaml(
            tmp_path / "workflows" / "corporations_pipeline.yml",
            _valid_workflow_yaml().replace("unit_test_pipeline", "corporations_pipeline"),
        )
        _write_yaml(
            tmp_path / "workflows" / "sensors_pipeline.yml",
            _valid_workflow_yaml().replace("unit_test_pipeline", "sensors_pipeline"),
        )
        _write_yaml(
            tmp_path / "jobs" / "morning_batch.yml",
            """
job_id: morning_batch
name: Morning Batch
workflows:
  - corporations_pipeline
  - sensors_pipeline
parameters:
  dataset_id: batch_override_dataset
""",
        )
        csv_file = tmp_path / "drop" / "sample.csv"
        csv_file.parent.mkdir(parents=True, exist_ok=True)
        csv_file.write_text("id,name\n1,Alpha\n2,Beta\n", encoding="utf-8")

        deps = build_default_dependencies()
        real_orchestrator = create_orchestration_service(deps)

        execute_job_calls: list[tuple[str, list[WorkflowDefinition], WorkspaceContext]] = []
        orig_execute_job = real_orchestrator.execute_job
        orig_execute_workflow = real_orchestrator.execute_workflow

        in_execute_job = False

        def spy_execute_job(
            job_id: str,
            workflows: list[WorkflowDefinition],
            workspace: WorkspaceContext,
        ) -> JobResult:
            nonlocal in_execute_job
            execute_job_calls.append((job_id, list(workflows), workspace))
            in_execute_job = True
            try:
                return orig_execute_job(job_id=job_id, workflows=workflows, workspace=workspace)
            finally:
                in_execute_job = False

        def guarded_execute_workflow(*args: Any, **kwargs: Any) -> Any:
            if not in_execute_job:
                raise AssertionError(
                    "AtlasRunner must NOT call execute_workflow() directly; "
                    "it must delegate via execute_job()."
                )
            return orig_execute_workflow(*args, **kwargs)

        real_orchestrator.execute_job = spy_execute_job  # type: ignore[method-assign]
        real_orchestrator.execute_workflow = guarded_execute_workflow  # type: ignore[method-assign]

        runner = AtlasRunner(orchestration_service=real_orchestrator)
        result = runner.run(tmp_path, "jobs/morning_batch.yml")

        # 1. execute_job() called exactly once
        assert len(execute_job_calls) == 1
        called_job_id, called_workflows, called_workspace = execute_job_calls[0]
        assert called_job_id == "morning_batch"
        assert isinstance(called_workspace, WorkspaceContext)
        assert called_workspace.root_path == tmp_path.resolve()
        assert len(called_workflows) == 2
        assert [w.workflow_id for w in called_workflows] == [
            "corporations_pipeline",
            "sensors_pipeline",
        ]

        # 2. Job-level parameters applied to both assembled WorkflowDefinitions
        for wf_def in called_workflows:
            ingest_step = wf_def.get_step("ingest")
            assert ingest_step is not None
            assert ingest_step.parameters["dataset_id"] == "batch_override_dataset"

        # 3. JobResult returned with both workflow results
        assert isinstance(result, JobResult)
        assert result.job_id == "morning_batch"
        assert result.status == ExecutionStatus.SUCCEEDED
        assert len(result.workflow_results) == 2
        assert result.workflow_results[0].workflow_id == "corporations_pipeline"
        assert result.workflow_results[1].workflow_id == "sensors_pipeline"


class TestManagedWorkspaceAndLogicalIdResolution:
    """Managed Workspace (ATLAS_WORKSPACE) and logical Job/Workflow ID resolution tests."""

    def test_workspace_resolution_with_atlas_workspace_env_var(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("ATLAS_WORKSPACE", str(tmp_path))
        customer_dir = tmp_path / "customer_001"
        customer_dir.mkdir(parents=True, exist_ok=True)

        ws_ctx = resolve_workspace("customer_001")
        assert ws_ctx.workspace_id == "customer_001"
        assert ws_ctx.workspace_path == customer_dir.resolve()
        assert ws_ctx.root_path == customer_dir.resolve()

        # Absolute path still works directly and preserves backward compatibility
        abs_other = tmp_path / "explicit_abs_ws"
        abs_ctx = resolve_workspace(abs_other)
        assert abs_ctx.workspace_id == "explicit_abs_ws"
        assert abs_ctx.workspace_path == abs_other.resolve()

    def test_logical_workspace_without_atlas_workspace_raises_clear_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("ATLAS_WORKSPACE", raising=False)
        with pytest.raises(RunnerConfigurationError, match="ATLAS_WORKSPACE") as exc_info:
            resolve_workspace("customer_001")
        assert exc_info.value.field == "workspace"

    def test_logical_workspace_traversal_outside_atlas_workspace_raises_isolation_error(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("ATLAS_WORKSPACE", str(tmp_path / "managed_root"))
        with pytest.raises(WorkspaceIsolationError):
            resolve_workspace("../../outside_root")

    def test_logical_job_id_maps_to_jobs_subdirectory_with_yml_extension(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("ATLAS_WORKSPACE", str(tmp_path))
        ws_dir = tmp_path / "customer_001"
        expected_job_file = _write_yaml(
            ws_dir / "jobs" / "morning_batch.yml",
            """
job_id: morning_batch
name: Morning Batch
workflows:
  - corporations_pipeline
""",
        )

        # 1. Direct resolve_job_reference with logical workspace and logical job ID
        resolved_path = resolve_job_reference("customer_001", "morning_batch")
        assert resolved_path == expected_job_file.resolve()

        # 2. load_job_definition with logical workspace ID and logical job ID
        job_def, loaded_path = load_job_definition("customer_001", "morning_batch")
        assert loaded_path == expected_job_file.resolve()
        assert job_def.job_id == "morning_batch"
        assert job_def.workflows == ["corporations_pipeline"]

    def test_logical_workflow_id_maps_to_workflows_subdirectory_with_yml_extension(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("ATLAS_WORKSPACE", str(tmp_path))
        ws_dir = tmp_path / "customer_001"
        expected_wf_file = _write_yaml(
            ws_dir / "workflows" / "corporations_pipeline.yml",
            _valid_workflow_yaml().replace("unit_test_pipeline", "corporations_pipeline"),
        )

        resolved_wf = resolve_workflow_reference("customer_001", "corporations_pipeline")
        assert resolved_wf == expected_wf_file.resolve()

    def test_cli_execution_with_logical_workspace_and_logical_job_id(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        monkeypatch.setenv("ATLAS_WORKSPACE", str(tmp_path))
        ws_dir = tmp_path / "customer_001"
        _write_yaml(
            ws_dir / "jobs" / "morning_batch.yml",
            """
job_id: morning_batch
name: Morning Batch
workflows:
  - corporations_pipeline
""",
        )
        _write_yaml(
            ws_dir / "workflows" / "corporations_pipeline.yml",
            _valid_workflow_yaml().replace("unit_test_pipeline", "corporations_pipeline"),
        )
        csv_file = ws_dir / "drop" / "sample.csv"
        csv_file.parent.mkdir(parents=True, exist_ok=True)
        csv_file.write_text("id,name\n1,Alpha\n2,Beta\n", encoding="utf-8")

        exit_code = main(["--workspace", "customer_001", "--job", "morning_batch"])
        assert exit_code == 0

        out = capsys.readouterr().out
        assert "Job: morning_batch" in out
        assert "Workflow: corporations_pipeline" in out
        assert "Status: SUCCEEDED" in out
