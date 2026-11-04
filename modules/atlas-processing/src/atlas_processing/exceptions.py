"""Processing-specific exception hierarchy."""

from __future__ import annotations

from atlas_core import ProcessingError as CoreProcessingError


class ProcessingError(CoreProcessingError):
    """Base exception for all failures in the atlas-processing module."""

    def __init__(
        self,
        message: str,
        *,
        request_id: str | None = None,
        execution_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.request_id = request_id
        self.execution_id = execution_id


class InvalidProcessingRequestError(ProcessingError, ValueError):
    """Raised when a processing request has missing or invalid configuration."""


class TransformationError(ProcessingError):
    """Raised when a transformation configuration is invalid or fails during execution."""


class DuplicateTransformationError(TransformationError):
    """Raised when attempting to register a transformation identifier that already exists."""


class UnknownTransformationError(TransformationError):
    """Raised when a requested transformation identifier is not registered."""


class ProcessingExecutionError(ProcessingError):
    """Raised when the processing engine or executor fails during transformation execution."""


class ProcessingStorageReadError(ProcessingError):
    """Raised when reading the input dataset from storage fails."""


class ProcessingStorageWriteError(ProcessingError):
    """Raised when persisting the transformed output dataset to storage fails."""


class ProcessingLifecycleError(ProcessingError):
    """Raised when an invalid processing lifecycle transition or tracking operation occurs."""


__all__ = [
    "DuplicateTransformationError",
    "InvalidProcessingRequestError",
    "ProcessingError",
    "ProcessingExecutionError",
    "ProcessingLifecycleError",
    "ProcessingStorageReadError",
    "ProcessingStorageWriteError",
    "TransformationError",
    "UnknownTransformationError",
]

