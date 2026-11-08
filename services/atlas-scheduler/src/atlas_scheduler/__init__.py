"""Atlas Scheduler — Control-plane schedule discovery, trigger evaluation, and job dispatch."""

from atlas_scheduler.cli import build_argument_parser, main
from atlas_scheduler.config import WorkspaceContext, resolve_workspace
from atlas_scheduler.dispatcher import DispatchedProcessRecord, RunnerDispatcher
from atlas_scheduler.engine import Scheduler
from atlas_scheduler.exceptions import (
    SchedulerConfigurationError,
    SchedulerError,
    ScheduleValidationError,
    WorkspaceIsolationError,
)
from atlas_scheduler.models import (
    ParsedCronExpression,
    ScheduleDefinition,
    TriggerDefinition,
    evaluate_most_recent_occurrence,
    parse_cron_expression,
)
from atlas_scheduler.protocols import ScheduleProvider
from atlas_scheduler.providers import YamlScheduleProvider

__version__ = "0.1.0"

__all__ = [
    "DispatchedProcessRecord",
    "ParsedCronExpression",
    "RunnerDispatcher",
    "ScheduleDefinition",
    "ScheduleProvider",
    "ScheduleValidationError",
    "Scheduler",
    "SchedulerConfigurationError",
    "SchedulerError",
    "TriggerDefinition",
    "WorkspaceContext",
    "WorkspaceIsolationError",
    "YamlScheduleProvider",
    "__version__",
    "build_argument_parser",
    "evaluate_most_recent_occurrence",
    "main",
    "parse_cron_expression",
    "resolve_workspace",
]

