"""Base transformation contract and step definition."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from atlas_core import Processor, require_not_empty
from pydantic import BaseModel, ConfigDict, Field, model_validator

from atlas_processing.exceptions import TransformationError

if TYPE_CHECKING:
    from atlas_processing.engine import ProcessingEngine


class TransformationStep(BaseModel):
    """Declarative specification for a single transformation step in a pipeline."""

    model_config = ConfigDict(extra="forbid")

    transformation_id: str
    params: dict[str, Any] = Field(default_factory=dict)
    name: str | None = None

    @model_validator(mode="after")
    def _validate_step(self) -> TransformationStep:
        cleaned_id = (
            self.transformation_id.strip() if isinstance(self.transformation_id, str) else ""
        )
        try:
            require_not_empty(cleaned_id, "transformation_id")
        except ValueError as exc:
            raise TransformationError(str(exc)) from exc
        self.transformation_id = cleaned_id
        return self


class Transformation(Processor, ABC):
    """Abstract base contract for engine-independent dataset transformations.

    Implements :class:`atlas_core.Processor` while separating declarative
    transformation configuration from execution engine mechanics.
    """

    def __init__(self, *, name: str | None = None) -> None:
        self._name = name
        self.validate_config()

    @property
    @abstractmethod
    def transformation_id(self) -> str:
        """Unique identifier for this transformation type."""

    @property
    def name(self) -> str:
        """Human-readable name for this transformation step."""
        return self._name or self.transformation_id

    @abstractmethod
    def validate_config(self) -> None:
        """Validate transformation configuration or raise :class:`TransformationError`."""

    @abstractmethod
    def apply(self, data: Any, engine: ProcessingEngine) -> Any:
        """Execute this transformation on ``data`` using ``engine``."""

    def transform(self, data: Any, engine: ProcessingEngine | None = None) -> Any:
        """Transform ``data`` using ``engine`` (defaults to ``PandasProcessingEngine``)."""
        if engine is None:
            from atlas_processing.engine import PandasProcessingEngine

            engine = PandasProcessingEngine()
        return self.apply(data, engine)

    def process(self, data: Any) -> Any:
        """Implement :class:`atlas_core.Processor` by delegating to :meth:`transform`."""
        return self.transform(data)


__all__ = [
    "Transformation",
    "TransformationStep",
]
