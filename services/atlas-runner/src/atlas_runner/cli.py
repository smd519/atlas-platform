"""Command-line entry point and standardized reporting for atlas-runner."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from atlas_core import ExecutionStatus
from atlas_orchestration import WorkflowResult

from atlas_runner.config import extract_error_context
from atlas_runner.models import ValidationReport
from atlas_runner.runner import AtlasRunner


def build_argument_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser for ``atlas-runner``."""
    parser = argparse.ArgumentParser(
        prog="atlas-runner",
        description="Execute or validate a YAML-defined Atlas customer job and workflow.",
    )
    parser.add_argument(
        "--workspace",
        required=True,
        help="Path to the customer workspace root directory.",
    )
    parser.add_argument(
        "--job",
        required=True,
        help="Path to the job YAML file (relative to workspace or absolute within workspace).",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        default=False,
        help="Validate Job and Workflow YAMLs and DAG structure without initializing services.",
    )
    return parser


def format_execution_report(
    result: WorkflowResult,
    *,
    job_id: str | None = None,
) -> str:
    """Format the standardized execution report matching the exact required structure."""
    resolved_job_id = job_id or getattr(result, "job_id", None) or "unknown"
    overall_status = str(
        result.status.value if hasattr(result.status, "value") else result.status
    ).upper()
    duration_val = float(result.duration) if result.duration is not None else 0.0

    lines = [
        "========== ATLAS EXECUTION ==========",
        f"Job: {resolved_job_id}",
        f"Workflow: {result.workflow_id}",
        f"Execution: {result.execution_id}",
        f"Status: {overall_status}",
        "",
        "Steps:",
    ]
    for step in result.step_results:
        step_status = str(
            step.status.value if hasattr(step.status, "value") else step.status
        ).upper()
        lines.append(f"  {step.step_id}:  {step_status}")
    lines.extend(
        [
            "",
            f"Duration: {duration_val:.1f} seconds",
            "====================================",
        ]
    )
    return "\n".join(lines)


def format_validation_report(report: ValidationReport) -> str:
    """Format a dry-run validation report for ``--validate-only`` mode."""
    lines = [
        "========== ATLAS VALIDATION ==========",
        f"Job: {report.job_id}",
        f"Job File: {report.job_file}",
        f"Workflow: {report.workflow_id} (v{report.workflow_version})",
        f"Workflow File: {report.workflow_file}",
        "Status: VALID",
        "",
        "Steps:",
    ]
    for step_id in report.topological_order:
        lines.append(f"  {step_id}:  VALID")
    lines.append("======================================")
    return "\n".join(lines)


def format_validation_error_report(
    exc: BaseException,
    *,
    default_file: str | None = None,
) -> str:
    """Format a precise validation failure report identifying the file and field."""
    config_file, field, detail = extract_error_context(exc, default_file=default_file)
    return "\n".join(
        [
            "========== ATLAS VALIDATION ==========",
            "Status: INVALID",
            f"File: {config_file}",
            f"Field: {field}",
            f"Error: {detail}",
            "======================================",
        ]
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    runner: AtlasRunner | None = None,
) -> int:
    """CLI entry point returning exit code 0 on success and non-zero on failure."""
    parser = build_argument_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    active_runner = runner or AtlasRunner()

    if args.validate_only:
        try:
            report = active_runner.validate(
                workspace=args.workspace,
                job_path=args.job,
            )
        except Exception as exc:
            err_report = format_validation_error_report(exc, default_file=str(args.job))
            print(err_report, file=sys.stderr)
            return 1

        print(format_validation_report(report))
        return 0

    try:
        result = active_runner.run(
            workspace=args.workspace,
            job_path=args.job,
            raise_on_error=False,
        )
    except Exception as exc:
        err_report = format_validation_error_report(exc, default_file=str(args.job))
        print(err_report, file=sys.stderr)
        return 1

    print(format_execution_report(result, job_id=active_runner.last_job_id))

    if result.status != ExecutionStatus.SUCCEEDED:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "build_argument_parser",
    "format_execution_report",
    "format_validation_error_report",
    "format_validation_report",
    "main",
]
