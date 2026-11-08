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


def test_external_customer_workspace_corporations_artifacts() -> None:
    """Verify the external customer workspace job and generated Bronze/Silver Parquet outputs."""
    external_ws = Path(r"D:\WORK-SPACE\ATLAS-WORKSPACE")
    try:
        ws_exists = external_ws.exists()
    except OSError:
        pytest.skip("External workspace is not accessible in sandboxed test environment.")

    if not ws_exists:
        pytest.skip("External workspace D:\\WORK-SPACE\\ATLAS-WORKSPACE is not present.")

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
