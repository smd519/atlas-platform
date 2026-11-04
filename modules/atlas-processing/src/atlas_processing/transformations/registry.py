"""Registry for discovering and instantiating transformations."""

from __future__ import annotations

from typing import Any

from atlas_processing.exceptions import (
    DuplicateTransformationError,
    TransformationError,
    UnknownTransformationError,
)
from atlas_processing.transformations.aggregation import AggregateTransformation
from atlas_processing.transformations.base import Transformation, TransformationStep
from atlas_processing.transformations.filtering import FilterTransformation
from atlas_processing.transformations.mapping import MapTransformation


class TransformationRegistry:
    """Registers and resolves :class:`Transformation` implementations by identifier."""

    def __init__(self) -> None:
        self._transformations: dict[str, type[Transformation]] = {}

    def register(
        self,
        transformation_id: str,
        transformation_cls: type[Transformation],
        *,
        overwrite: bool = False,
    ) -> None:
        """Register ``transformation_cls`` under ``transformation_id``."""
        key = transformation_id.strip() if isinstance(transformation_id, str) else ""
        if not key:
            raise TransformationError("Transformation registration identifier cannot be empty.")
        if not isinstance(transformation_cls, type) or not issubclass(
            transformation_cls, Transformation
        ):
            raise TransformationError(
                f"Registered class for '{key}' must be a subclass of Transformation."
            )
        if key in self._transformations and not overwrite:
            raise DuplicateTransformationError(f"Transformation '{key}' is already registered.")
        self._transformations[key] = transformation_cls

    def get(self, transformation_id: str) -> type[Transformation]:
        """Return the :class:`Transformation` class registered under ``transformation_id``."""
        key = transformation_id.strip() if isinstance(transformation_id, str) else ""
        try:
            return self._transformations[key]
        except KeyError as exc:
            raise UnknownTransformationError(
                f"Transformation '{transformation_id}' is not registered."
            ) from exc

    def exists(self, transformation_id: str) -> bool:
        """Return True if ``transformation_id`` is registered."""
        return transformation_id in self._transformations

    def list_transformations(self) -> list[str]:
        """Return the list of registered transformation identifiers."""
        return list(self._transformations.keys())

    def create(
        self,
        step: TransformationStep | Transformation | dict[str, Any],
    ) -> Transformation:
        """Resolve and instantiate a :class:`Transformation` from ``step``."""
        if isinstance(step, Transformation):
            step.validate_config()
            return step

        parsed_step = step if isinstance(step, TransformationStep) else TransformationStep(**step)
        transformation_cls = self.get(parsed_step.transformation_id)
        init_kwargs = dict(parsed_step.params)
        if parsed_step.name is not None and "name" not in init_kwargs:
            init_kwargs["name"] = parsed_step.name

        try:
            return transformation_cls(**init_kwargs)
        except TransformationError:
            raise
        except Exception as exc:
            raise TransformationError(
                f"Failed to initialize transformation '{parsed_step.transformation_id}': {exc}"
            ) from exc


def create_default_transformation_registry() -> TransformationRegistry:
    """Create a :class:`TransformationRegistry` populated with built-in transformations."""
    registry = TransformationRegistry()
    registry.register("filter", FilterTransformation)
    registry.register("map", MapTransformation)
    registry.register("aggregate", AggregateTransformation)
    return registry


__all__ = [
    "TransformationRegistry",
    "create_default_transformation_registry",
]
