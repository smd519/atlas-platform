"""Atlas Runner — Hardened YAML-driven workflow execution and validation entry point."""

from atlas_orchestration import JobResult, WorkspaceContext

from atlas_runner.cli import (
    build_argument_parser,
    format_execution_report,
    format_validation_error_report,
    format_validation_report,
    main,
)
from atlas_runner.config import (
    apply_job_parameters_to_workflow_data,
    extract_error_context,
    load_job_definition,
    load_workflow_definition,
    load_workspace_config,
    load_yaml_file,
    merge_parameters,
    resolve_job_reference,
    resolve_metadata_database_path,
    resolve_metadata_database_url,
    resolve_step_paths,
    resolve_workflow_reference,
    resolve_workspace,
    resolve_workspace_path,
    validate_workflow_dag,
)
from atlas_runner.dependencies import (
    build_default_dependencies,
    build_metadata_service,
    create_orchestration_service,
)
from atlas_runner.exceptions import (
    RunnerConfigurationError,
    RunnerError,
    RunnerExecutionError,
    WorkspaceIsolationError,
)
from atlas_runner.models import (
    SUPPORTED_METADATA_BACKENDS,
    JobDefinition,
    JobManifest,
    MetadataBackend,
    MetadataConfig,
    ValidationReport,
    WorkspaceConfig,
)
from atlas_runner.runner import AtlasRunner

__version__ = "0.1.0"

__all__ = [
    "AtlasRunner",
    "JobDefinition",
    "JobManifest",
    "JobResult",
    "MetadataBackend",
    "MetadataConfig",
    "RunnerConfigurationError",
    "RunnerError",
    "RunnerExecutionError",
    "SUPPORTED_METADATA_BACKENDS",
    "ValidationReport",
    "WorkspaceConfig",
    "WorkspaceContext",
    "WorkspaceIsolationError",
    "__version__",
    "apply_job_parameters_to_workflow_data",
    "build_argument_parser",
    "build_default_dependencies",
    "build_metadata_service",
    "create_orchestration_service",
    "extract_error_context",
    "format_execution_report",
    "format_validation_error_report",
    "format_validation_report",
    "load_job_definition",
    "load_workflow_definition",
    "load_workspace_config",
    "load_yaml_file",
    "main",
    "merge_parameters",
    "resolve_job_reference",
    "resolve_metadata_database_path",
    "resolve_metadata_database_url",
    "resolve_step_paths",
    "resolve_workflow_reference",
    "resolve_workspace",
    "resolve_workspace_path",
    "validate_workflow_dag",
]
