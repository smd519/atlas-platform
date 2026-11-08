"""Command-line entry point for atlas-runner."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from atlas_core import ExecutionStatus
from atlas_orchestration import WorkflowResult

from atlas_runner.runner import AtlasRunner


def build_argument_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser for ``atlas-runner``."""
    parser = argparse.ArgumentParser(
        prog="atlas-runner",
        description="Execute a YAML-defined Atlas customer job and workflow.",
    )
    parser.add_argument(
        "--workspace",
        required=True,
        help="Path to the customer workspace root directory.",
    )
    parser.add_argument(
        "--job",
        required=True,
        help="Path to the job YAML file (relative to workspace or absolute).",
    )
    return parser


def format_execution_report(result: WorkflowResult) -> str:
    """Format execution ID, workflow ID, overall status, and individual step results."""
    lines = [
        "",
        "========== ATLAS EXECUTION ==========",
        f"Workflow: {result.workflow_id}",
        f"Execution: {result.execution_id}",
        f"Status: {result.status}",
    ]
    for step in result.step_results:
        lines.append(f"{step.step_id}: {step.status}")
    lines.append("====================================")
    if result.status != ExecutionStatus.SUCCEEDED and result.message:
        lines.append(f"Error: {result.message}")
    return "\n".join(lines)


def main(
    argv: Sequence[str] | None = None,
    *,
    runner: AtlasRunner | None = None,
) -> int:
    """CLI entry point returning exit code 0 on success and non-zero on failure."""
    parser = build_argument_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    active_runner = runner or AtlasRunner()
    try:
        result = active_runner.run(
            workspace=args.workspace,
            job_path=args.job,
            raise_on_error=False,
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(format_execution_report(result))

    if result.status != ExecutionStatus.SUCCEEDED:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "build_argument_parser",
    "format_execution_report",
    "main",
]
