"""Subprocess runner dispatcher and non-blocking process lifecycle tracker."""

from __future__ import annotations

import inspect
import logging
import os
import subprocess
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from atlas_scheduler.config import WorkspaceContext

logger = logging.getLogger("atlas_scheduler.dispatcher")


def _resolve_default_working_directory() -> Path | None:
    """Resolve the Atlas workspace/package root containing ``pyproject.toml``.

    Ensures ``uv run atlas-runner`` can locate the workspace configuration even
    when ``atlas-scheduler`` is invoked from an arbitrary working directory
    outside the repository root.
    """
    current = Path(__file__).resolve()
    discovered: Path | None = None
    for parent in current.parents:
        if (parent / "pyproject.toml").is_file():
            discovered = parent
    return discovered


def _factory_accepts_cwd(factory: Callable[..., Any]) -> bool:
    """Return True if ``factory`` accepts a ``cwd`` keyword argument."""
    if factory is subprocess.Popen:
        return True
    try:
        sig = inspect.signature(factory)
    except (TypeError, ValueError):
        return False
    for param in sig.parameters.values():
        if param.name == "cwd" and param.kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            return True
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            return True
    return False


@dataclass
class DispatchedProcessRecord:
    """In-memory lifecycle record for a dispatched ``atlas-runner`` subprocess."""

    schedule_id: str
    job_id: str
    command: tuple[str, ...]
    pid: int | None
    launched_at: datetime
    completed_at: datetime | None = None
    exit_code: int | None = None
    launch_succeeded: bool = True
    error_message: str | None = None
    scheduled_occurrence: datetime | None = None


class RunnerDispatcher:
    """Dispatches due jobs as independent ``atlas-runner`` subprocesses and monitors completion."""

    def __init__(
        self,
        workspace: WorkspaceContext | str,
        *,
        runner_command_prefix: Sequence[str] | None = None,
        env: Mapping[str, str] | None = None,
        working_directory: Path | str | None = None,
        popen_factory: Callable[..., subprocess.Popen[Any]] | None = None,
    ) -> None:
        if isinstance(workspace, WorkspaceContext):
            ws_id = workspace.workspace_id.strip()
        elif isinstance(workspace, str):
            ws_id = workspace.strip()
        else:
            raise TypeError(
                f"workspace must be a WorkspaceContext or str, got {type(workspace).__name__}."
            )
        if not ws_id:
            raise ValueError("Dispatcher workspace identifier must not be empty.")

        self._workspace_id = ws_id
        self._command_prefix = tuple(runner_command_prefix or ("uv", "run", "atlas-runner"))
        self._env_overrides = dict(env) if env is not None else None
        self._working_directory = (
            Path(working_directory).resolve()
            if working_directory is not None
            else _resolve_default_working_directory()
        )
        self._popen_factory = popen_factory
        self._lock = threading.RLock()
        self._records: list[DispatchedProcessRecord] = []
        self._active_processes: list[tuple[subprocess.Popen[Any], DispatchedProcessRecord]] = []

    @property
    def workspace_id(self) -> str:
        """Return the workspace identifier (logical ID or absolute path) passed to runner."""
        return self._workspace_id

    @property
    def working_directory(self) -> Path | None:
        """Return the resolved working directory used when spawning runner subprocesses."""
        return self._working_directory

    @property
    def records(self) -> list[DispatchedProcessRecord]:
        """Return a snapshot of all dispatched subprocess records."""
        with self._lock:
            return list(self._records)

    @property
    def active_count(self) -> int:
        """Return the number of currently running subprocesses."""
        with self._lock:
            return len(self._active_processes)

    def build_command(self, job_id: str) -> list[str]:
        """Construct the ``atlas-runner`` CLI command for ``job_id``."""
        clean_job_id = job_id.strip()
        if not clean_job_id:
            raise ValueError("job_id must not be empty.")
        return [
            *self._command_prefix,
            "--workspace",
            self._workspace_id,
            "--job",
            clean_job_id,
        ]

    def build_environment(self) -> dict[str, str]:
        """Build the subprocess environment preserving inherited variables."""
        inherited = dict(os.environ)
        if self._env_overrides is not None:
            inherited.update(self._env_overrides)
        return inherited

    def dispatch(
        self,
        schedule_id: str,
        job_id: str,
        *,
        scheduled_occurrence: datetime | None = None,
    ) -> DispatchedProcessRecord:
        """Launch an independent ``atlas-runner`` subprocess for ``(schedule_id, job_id)``."""
        cmd = self.build_command(job_id)
        env = self.build_environment()
        launched_at = datetime.now(UTC)

        try:
            factory = self._popen_factory if self._popen_factory is not None else subprocess.Popen
            if self._working_directory is not None and _factory_accepts_cwd(factory):
                proc = factory(cmd, env=env, cwd=str(self._working_directory))
            else:
                proc = factory(cmd, env=env)
            pid = getattr(proc, "pid", None)
            record = DispatchedProcessRecord(
                schedule_id=schedule_id,
                job_id=job_id,
                command=tuple(cmd),
                pid=pid,
                launched_at=launched_at,
                launch_succeeded=True,
                scheduled_occurrence=scheduled_occurrence,
            )
            with self._lock:
                self._records.append(record)
                self._active_processes.append((proc, record))

            logger.info(
                "Launched runner subprocess for schedule='%s' job='%s' pid=%s cmd=%s",
                schedule_id,
                job_id,
                pid,
                cmd,
            )
            return record
        except Exception as exc:
            err_msg = str(exc) or type(exc).__name__
            record = DispatchedProcessRecord(
                schedule_id=schedule_id,
                job_id=job_id,
                command=tuple(cmd),
                pid=None,
                launched_at=launched_at,
                completed_at=launched_at,
                launch_succeeded=False,
                error_message=err_msg,
                scheduled_occurrence=scheduled_occurrence,
            )
            with self._lock:
                self._records.append(record)

            logger.error(
                "Failed to launch runner subprocess for schedule='%s' job='%s': %s",
                schedule_id,
                job_id,
                err_msg,
            )
            return record

    def poll_processes(self) -> list[DispatchedProcessRecord]:
        """Check running subprocesses without blocking and update completion records."""
        completed_now: list[DispatchedProcessRecord] = []
        with self._lock:
            still_running: list[tuple[subprocess.Popen[Any], DispatchedProcessRecord]] = []
            for proc, record in self._active_processes:
                try:
                    return_code = proc.poll()
                except Exception as exc:
                    logger.error(
                        "Error polling subprocess pid=%s (schedule='%s' job='%s'): %s",
                        record.pid,
                        record.schedule_id,
                        record.job_id,
                        exc,
                    )
                    still_running.append((proc, record))
                    continue

                if return_code is None:
                    still_running.append((proc, record))
                else:
                    record.completed_at = datetime.now(UTC)
                    record.exit_code = int(return_code)
                    completed_now.append(record)
                    if record.exit_code == 0:
                        logger.info(
                            "Runner subprocess completed successfully: schedule='%s' "
                            "job='%s' pid=%s exit_code=0",
                            record.schedule_id,
                            record.job_id,
                            record.pid,
                        )
                    else:
                        logger.error(
                            "Runner subprocess failed: schedule='%s' job='%s' pid=%s exit_code=%s",
                            record.schedule_id,
                            record.job_id,
                            record.pid,
                            record.exit_code,
                        )
            self._active_processes = still_running

        return completed_now


__all__ = [
    "DispatchedProcessRecord",
    "RunnerDispatcher",
]
