"""Command-line entry point and application composition root for atlas-scheduler."""

from __future__ import annotations

import argparse
import logging
import signal
import sys
from collections.abc import Sequence
from types import FrameType

from atlas_core import configure_logging

from atlas_scheduler.config import resolve_workspace
from atlas_scheduler.dispatcher import RunnerDispatcher
from atlas_scheduler.engine import Scheduler
from atlas_scheduler.exceptions import SchedulerError
from atlas_scheduler.providers.yaml_provider import YamlScheduleProvider

logger = logging.getLogger("atlas_scheduler.cli")


def build_argument_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser for ``atlas-scheduler``."""
    parser = argparse.ArgumentParser(
        prog="atlas-scheduler",
        description="Atlas Platform control-plane schedule discovery and job dispatch daemon.",
    )
    parser.add_argument(
        "--workspace",
        required=True,
        help="Logical workspace ID (resolved via ATLAS_WORKSPACE) or absolute workspace path.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Polling interval in seconds (default: 60).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        type=str.upper,
        help="Logging verbosity level (default: INFO).",
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    max_ticks: int | None = None,
) -> int:
    """Application composition root for ``atlas-scheduler``."""
    parser = build_argument_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    configure_logging(args.log_level)
    logging.getLogger("atlas_scheduler").setLevel(
        getattr(logging, args.log_level.upper(), logging.INFO)
    )

    if args.interval <= 0:
        logger.error("Invalid --interval %d: must be a positive integer.", args.interval)
        print(
            f"Error: --interval must be a positive integer, got {args.interval}.",
            file=sys.stderr,
        )
        return 1

    try:
        workspace = resolve_workspace(args.workspace)
    except SchedulerError as exc:
        logger.error("Workspace resolution failed: %s", exc)
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    provider = YamlScheduleProvider(workspace_path=workspace.workspace_path)
    dispatcher = RunnerDispatcher(workspace=workspace)
    scheduler = Scheduler(
        provider=provider,
        dispatcher=dispatcher,
        interval=args.interval,
    )

    def _handle_shutdown_signal(signum: int, _frame: FrameType | None) -> None:
        sig_name = (
            signal.Signals(signum).name
            if signum in signal.Signals._value2member_map_
            else str(signum)
        )
        logger.info("Received signal %s; initiating graceful scheduler shutdown.", sig_name)
        scheduler.stop()

    prev_sigint = signal.getsignal(signal.SIGINT)
    prev_sigterm = signal.getsignal(signal.SIGTERM)
    try:
        signal.signal(signal.SIGINT, _handle_shutdown_signal)
        signal.signal(signal.SIGTERM, _handle_shutdown_signal)
    except ValueError:
        # Signal registration is only allowed in the main thread
        pass

    try:
        scheduler.start(max_ticks=max_ticks)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received; stopping scheduler gracefully.")
        scheduler.stop()
    finally:
        try:
            signal.signal(signal.SIGINT, prev_sigint)
            signal.signal(signal.SIGTERM, prev_sigterm)
        except ValueError:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "build_argument_parser",
    "main",
]
