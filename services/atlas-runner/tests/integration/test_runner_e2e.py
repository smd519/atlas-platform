"""End-to-end integration tests for atlas-runner."""

from __future__ import annotations

from pathlib import Path

import pandas as pd  # type: ignore[import-untyped]
import pytest
from atlas_core import ExecutionStatus
from atlas_runner import AtlasRunner, main


def test_end_to_end_corporations_pipeline_in_workspace(tmp_path: Path) -> None:
    """Execute a corporations CSV -> Bronze -> Silver -> Quality pipeline via YAML."""
    workspace = tmp_path / "ATLAS-WORKSPACE"
    drop_dir = workspace / "drop"
    jobs_dir = workspace / "jobs"
    workflows_dir = workspace / "workflows"
    drop_dir.mkdir(parents=True, exist_ok=True)
    jobs_dir.mkdir(parents=True, exist_ok=True)
    workflows_dir.mkdir(parents=True, exist_ok=True)

    csv_content = (
        "Corporation number,Business number (BN),Corporate name - form 1,"
        "Governing legislation,Status,City/town,Province/territory,Country\n"
        "8660115,835752437,MINDANGLER CAPITAL INC.,"
        "Canada Business Corporations Act,Active,Ottawa,ON,CA\n"
        "821080,100094283,AIRMEC CLIMATISATION LTEE,"
        "Canada Business Corporations Act,Active,ANJOU,QC,CA\n"
        ",822414447,MISSING CORP NUMBER INC.,"
        "Canada Business Corporations Act,Active,Pointe-Claire,QC,CA\n"
    )
    (drop_dir / "corporations-active-cbca-en.csv").write_text(csv_content, encoding="utf-8")

    (jobs_dir / "corporations.yml").write_text(
        """
job_id: corporations_pipeline

workflow_ref: workflows/corporations-pipeline.yml

parameters:
  dataset_id: corporations_active
""",
        encoding="utf-8",
    )

    (workflows_dir / "corporations-pipeline.yml").write_text(
        """
workflow_id: corporations_pipeline
name: Corporations Active CBCA Pipeline
version: 1
steps:
  - step_id: ingest
    module_target: atlas-ingestion
    action: ingest
    parameters:
      dataset_id: corporations_default
      source_format: csv
      source:
        source_id: corporations_cbca_csv
        source_name: Corporations Active CBCA CSV
        source_type: local_file
        config:
          path: drop/corporations-active-cbca-en.csv
      destination:
        layer: bronze
        path: data/bronze/corporations-active-cbca-en.parquet
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
        path: data/silver/corporations-active-cbca-en.parquet
        format: parquet
      transformations:
        - transformation_id: filter
          name: require_corporation_number
          params:
            column: "Corporation number"
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
          name: corporation_number_not_null
          params:
            column: "Corporation number"
""",
        encoding="utf-8",
    )

    runner = AtlasRunner()
    result = runner.run(workspace=workspace, job_path="jobs/corporations.yml")

    assert result.status == ExecutionStatus.SUCCEEDED
    assert result.workflow_id == "corporations_pipeline"
    assert [s.step_id for s in result.step_results] == ["ingest", "process", "quality"]
    assert all(s.status == ExecutionStatus.SUCCEEDED for s in result.step_results)

    bronze_file = workspace / "data" / "bronze" / "corporations-active-cbca-en.parquet"
    silver_file = workspace / "data" / "silver" / "corporations-active-cbca-en.parquet"
    assert bronze_file.exists()
    assert silver_file.exists()

    bronze_df = pd.read_parquet(bronze_file)
    silver_df = pd.read_parquet(silver_file)
    assert len(bronze_df) == 3
    assert len(silver_df) == 2
    assert silver_df["Corporation number"].isna().sum() == 0

    exit_code = main(["--workspace", str(workspace), "--job", "jobs/corporations.yml"])
    assert exit_code == 0


def test_end_to_end_multi_workflow_batch_job_via_cli(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Execute a multi-workflow batch Job (corporations + sensors) sequentially via runner CLI."""
    workspace = tmp_path / "customer_001"
    drop_dir = workspace / "drop"
    jobs_dir = workspace / "jobs"
    workflows_dir = workspace / "workflows"
    drop_dir.mkdir(parents=True, exist_ok=True)
    jobs_dir.mkdir(parents=True, exist_ok=True)
    workflows_dir.mkdir(parents=True, exist_ok=True)

    (drop_dir / "corporations-active-cbca-en.csv").write_text(
        "Corporation number,Business number (BN),Corporate name - form 1,"
        "Governing legislation,Status,City/town,Province/territory,Country\n"
        "8660115,835752437,MINDANGLER CAPITAL INC.,"
        "Canada Business Corporations Act,Active,Ottawa,ON,CA\n"
        "821080,100094283,AIRMEC CLIMATISATION LTEE,"
        "Canada Business Corporations Act,Active,ANJOU,QC,CA\n",
        encoding="utf-8",
    )
    (drop_dir / "sensors.csv").write_text(
        "sensor_id,temperature,city\n"
        "s1,21.5,Toronto\n"
        "s2,24.0,Montreal\n"
        "s3,,Vancouver\n"
        "s4,18.5,Ottawa\n",
        encoding="utf-8",
    )

    (jobs_dir / "morning_batch.yml").write_text(
        """
job_id: morning_batch
name: Morning Batch
description: Daily ingestion and processing batch.

workflows:
  - corporations_pipeline
  - sensors_pipeline
""",
        encoding="utf-8",
    )

    (workflows_dir / "corporations_pipeline.yml").write_text(
        """
workflow_id: corporations_pipeline
name: Corporations Active CBCA Pipeline
version: 1
steps:
  - step_id: ingest
    module_target: atlas-ingestion
    action: ingest
    parameters:
      dataset_id: corporations_default
      source_format: csv
      source:
        source_id: corporations_cbca_csv
        source_name: Corporations Active CBCA CSV
        source_type: local_file
        config:
          path: drop/corporations-active-cbca-en.csv
      destination:
        layer: bronze
        path: data/bronze/corporations-active-cbca-en.parquet
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
        path: data/silver/corporations-active-cbca-en.parquet
        format: parquet
      transformations:
        - transformation_id: filter
          name: require_corporation_number
          params:
            column: "Corporation number"
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
          name: corporation_number_not_null
          params:
            column: "Corporation number"
""",
        encoding="utf-8",
    )

    (workflows_dir / "sensors_pipeline.yml").write_text(
        """
workflow_id: sensors_pipeline
name: Sensors Pipeline
version: 1
steps:
  - step_id: ingest
    module_target: atlas-ingestion
    action: ingest
    parameters:
      dataset_id: sensors_default
      source_format: csv
      source:
        source_id: sensors_csv
        source_name: Sensors CSV
        source_type: local_file
        config:
          path: drop/sensors.csv
      destination:
        layer: bronze
        path: data/bronze/sensors.parquet
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
        path: data/silver/sensors.parquet
        format: parquet
      transformations:
        - transformation_id: filter
          name: remove_missing_temperature
          params:
            column: temperature
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
          name: sensor_id_not_null
          params:
            column: sensor_id
""",
        encoding="utf-8",
    )

    runner = AtlasRunner()
    exit_code = main(
        ["--workspace", str(workspace), "--job", "jobs/morning_batch.yml"],
        runner=runner,
    )
    assert exit_code == 0

    out = capsys.readouterr().out
    assert "Job: morning_batch" in out
    assert "Workflow: corporations_pipeline" in out
    assert "Workflow: sensors_pipeline" in out
    assert "Status: SUCCEEDED" in out

    orchestrator = runner.orchestrator
    assert orchestrator is not None

    # Verify parent Job execution and both child workflow executions are tracked
    tracked_executions = orchestrator.list_workflow_executions()
    assert len(tracked_executions) == 3
    assert [e.workflow_id for e in tracked_executions] == [
        "morning_batch",
        "corporations_pipeline",
        "sensors_pipeline",
    ]
    assert all(e.status == ExecutionStatus.SUCCEEDED for e in tracked_executions)

    # Verify output Parquet files for both workflows
    corp_silver = workspace / "data" / "silver" / "corporations-active-cbca-en.parquet"
    sensors_silver = workspace / "data" / "silver" / "sensors.parquet"
    assert corp_silver.exists()
    assert sensors_silver.exists()
    assert len(pd.read_parquet(corp_silver)) == 2
    assert len(pd.read_parquet(sensors_silver)) == 3


def test_external_customer_workspace_corporations_artifacts() -> None:
    """Verify the external customer workspace job and generated Bronze/Silver Parquet outputs."""
    external_ws = Path(r"D:\work-space\atlas-workspace\customer_001")
    try:
        ws_exists = external_ws.exists()
    except OSError:
        pytest.skip("External workspace is not accessible in sandboxed test environment.")

    if not ws_exists:
        pytest.skip(
            "External workspace D:\\work-space\\atlas-workspace\\customer_001 is not present."
        )

    bronze_path = external_ws / "data" / "bronze" / "corporations-active-cbca-en.parquet"
    silver_path = external_ws / "data" / "silver" / "corporations-active-cbca-en.parquet"
    assert bronze_path.exists()
    assert silver_path.exists()

    bronze_df = pd.read_parquet(bronze_path)
    silver_df = pd.read_parquet(silver_path)
    assert len(bronze_df) == 645677
    assert len(silver_df) == 645677
    assert "Corporation number" in silver_df.columns
    assert "Corporate name - form 1" in silver_df.columns
    assert "Status" in silver_df.columns


def test_customer_001_morning_batch_end_to_end(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Validate and execute the customer_001 morning_batch job (corporations + sensors)."""
    external_ws = Path(r"D:\work-space\atlas-workspace\customer_001")
    try:
        ws_exists = external_ws.exists()
    except OSError:
        pytest.skip("External workspace is not accessible in sandboxed test environment.")

    if not ws_exists:
        pytest.skip(
            "External workspace D:\\work-space\\atlas-workspace\\customer_001 is not present."
        )

    # 1. Validate directly against D:\work-space\atlas-workspace\customer_001 (read-only)
    runner = AtlasRunner()
    report = runner.validate(workspace=external_ws, job_path="jobs/morning_batch.yml")
    assert report.is_valid is True
    assert report.job_id == "morning_batch"
    assert [wf.workflow_id for wf in report.workflows] == [
        "corporations_pipeline",
        "sensors_pipeline",
    ]

    # 2. Execute end-to-end using customer_001's exact YAML configs and data (in isolated copy
    #    so production Parquet files in D:\work-space\atlas-workspace\customer_001 are preserved)
    test_ws = tmp_path / "customer_001_run"
    (test_ws / "jobs").mkdir(parents=True, exist_ok=True)
    (test_ws / "workflows").mkdir(parents=True, exist_ok=True)
    (test_ws / "drop").mkdir(parents=True, exist_ok=True)

    (test_ws / "jobs" / "morning_batch.yml").write_text(
        (external_ws / "jobs" / "morning_batch.yml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (test_ws / "workflows" / "corporations_pipeline.yml").write_text(
        (external_ws / "workflows" / "corporations_pipeline.yml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (test_ws / "workflows" / "sensors_pipeline.yml").write_text(
        (external_ws / "workflows" / "sensors_pipeline.yml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (test_ws / "drop" / "sensors.csv").write_text(
        (external_ws / "drop" / "sensors.csv").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    with (external_ws / "drop" / "corporations-active-cbca-en.csv").open(
        encoding="utf-8"
    ) as src_csv:
        head_lines = [next(src_csv) for _ in range(25)]
    (test_ws / "drop" / "corporations-active-cbca-en.csv").write_text(
        "".join(head_lines),
        encoding="utf-8",
    )

    exec_runner = AtlasRunner()
    exit_code = main(
        ["--workspace", str(test_ws), "--job", "jobs/morning_batch.yml"],
        runner=exec_runner,
    )
    assert exit_code == 0

    out = capsys.readouterr().out
    assert "Job: morning_batch" in out
    assert "Workflow: corporations_pipeline" in out
    assert "Workflow: sensors_pipeline" in out
    assert "Status: SUCCEEDED" in out

    orchestrator = exec_runner.orchestrator
    assert orchestrator is not None
    executions = orchestrator.list_workflow_executions()
    assert [e.workflow_id for e in executions] == [
        "morning_batch",
        "corporations_pipeline",
        "sensors_pipeline",
    ]
    assert all(e.status == ExecutionStatus.SUCCEEDED for e in executions)
