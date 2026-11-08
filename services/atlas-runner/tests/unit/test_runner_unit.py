"""Unit tests for atlas-runner configuration, parameter merge, path resolution, and CLI."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from atlas_core import ExecutionStatus
from atlas_orchestration import (
    InvalidStepDefinitionError,
    InvalidWorkflowDefinitionError,
    WorkflowDefinition,
    WorkflowVersionConflictError,
)
from atlas_runner import (
    AtlasRunner,
    RunnerConfigurationError,
    RunnerExecutionError,
    load_job_definition,
    load_workflow_definition,
    main,
    merge_parameters,
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
        with pytest.raises(RunnerConfigurationError, match="Invalid YAML syntax"):
            load_job_definition(tmp_path, "jobs/bad.yml")

    def test_non_mapping_yaml_raises_configuration_error(self, tmp_path: Path) -> None:
        _write_yaml(
            tmp_path / "workflows" / "list.yml",
            "- item1\n- item2\n",
        )
        with pytest.raises(RunnerConfigurationError, match="top-level mapping"):
            load_workflow_definition(tmp_path, "workflows/list.yml")

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
        # Top-level replacement only (no recursive merging of nested dicts)
        assert merged["format_options"] == {"delimiter": ";"}
        # Original inputs are not mutated
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
        # Unspecified parameter remains unchanged
        assert ingest_step.parameters["source_format"] == "csv"
        # Nested dict replaced at top-level without recursive merge
        assert ingest_step.parameters["format_options"] == {"delimiter": "|"}


class TestRelativePathResolution:
    """Relative paths resolve against the supplied workspace."""

    def test_resolve_workspace_path_relative_and_absolute(self, tmp_path: Path) -> None:
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
        # Dynamic context reference must remain untouched for runtime ContextResolver
        assert process_step.parameters["source"] == "${steps.ingest.destination}"
        assert process_step.parameters["destination"]["path"] == str(
            (tmp_path / "data" / "silver" / "sample.parquet").resolve()
        )
        assert quality_step.parameters["storage_location"] == "${steps.process.destination}"


class TestRunnerEndToEndInTempWorkspace:
    """Runner executes a complete workflow in a temporary workspace with proper exit codes."""

    def test_runner_and_cli_success_and_failure_exit_codes(self, tmp_path: Path) -> None:
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

        failing_runner = AtlasRunner()
        with pytest.raises(RunnerExecutionError):
            failing_runner.run(tmp_path, "jobs/sample.yml", raise_on_error=True)

        # 2. Re-registering the same workflow version on the same orchestrator propagates conflict
        with pytest.raises(WorkflowVersionConflictError):
            failing_runner.run(tmp_path, "jobs/sample.yml")

        # 3. Create input CSV and execute successfully on a fresh runner
        csv_file = tmp_path / "drop" / "sample.csv"
        csv_file.parent.mkdir(parents=True, exist_ok=True)
        csv_file.write_text("id,name\n1,Alpha\n2,Beta\n", encoding="utf-8")

        runner = AtlasRunner()
        result = runner.run(tmp_path, "jobs/sample.yml")
        assert result.status == ExecutionStatus.SUCCEEDED
        assert (tmp_path / "data" / "bronze" / "sample.parquet").exists()
        assert (tmp_path / "data" / "silver" / "sample.parquet").exists()

        exit_code_ok = main(["--workspace", str(tmp_path), "--job", "jobs/sample.yml"])
        assert exit_code_ok == 0
