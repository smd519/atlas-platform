"""Storage location domain model."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, model_validator
from pydantic import ValidationError as PydanticValidationError

from atlas_storage.exceptions import StorageConfigurationError


class StorageLocation(BaseModel):
    """Defines a physical or logical target storage location and format."""

    layer: str
    path: str
    format: str

    def __init__(self, **data: Any) -> None:
        try:
            super().__init__(**data)
        except StorageConfigurationError:
            raise
        except PydanticValidationError as exc:
            raise StorageConfigurationError(
                f"Invalid storage location configuration: {exc}"
            ) from exc

    @model_validator(mode="after")
    def _validate_location(self) -> StorageLocation:
        layer = self.layer.strip() if isinstance(self.layer, str) else ""
        path = self.path.strip() if isinstance(self.path, str) else ""
        fmt = self.format.strip().lower() if isinstance(self.format, str) else ""

        if not layer:
            raise StorageConfigurationError("StorageLocation 'layer' cannot be empty.")
        if not path:
            raise StorageConfigurationError("StorageLocation 'path' cannot be empty.")
        if not fmt:
            raise StorageConfigurationError("StorageLocation 'format' cannot be empty.")

        self.layer = layer
        self.path = path
        self.format = fmt
        return self


__all__ = ["StorageLocation"]
