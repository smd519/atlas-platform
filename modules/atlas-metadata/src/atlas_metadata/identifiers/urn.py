"""Centralized, validated URN model for Atlas platform metadata entities."""

from __future__ import annotations

import re
from typing import Any

from pydantic import BaseModel, ConfigDict, model_serializer, model_validator

from atlas_metadata.exceptions import InvalidURNError

URN_PREFIX = "urn:atlas"
_COMPONENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/\-:@]*$")
_ENTITY_TYPE_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def _validate_component(value: Any, field_name: str, *, lower: bool = False) -> str:
    if not isinstance(value, str):
        raise InvalidURNError(f"URN '{field_name}' must be a string, got {type(value).__name__}.")
    cleaned = value.strip()
    if lower:
        cleaned = cleaned.lower()
    if not cleaned:
        raise InvalidURNError(f"URN '{field_name}' cannot be empty.")
    if field_name == "entity_type":
        if not _ENTITY_TYPE_PATTERN.match(cleaned):
            raise InvalidURNError(
                f"Invalid URN entity_type '{value}'. Must match {_ENTITY_TYPE_PATTERN.pattern}."
            )
    else:
        if ":" in cleaned or not _COMPONENT_PATTERN.match(cleaned):
            raise InvalidURNError(
                f"Invalid URN {field_name} '{value}'. "
                "Components must be non-empty alphanumeric tokens without colons."
            )
    return cleaned


class URN(BaseModel):
    """Immutable, validated URN (`urn:atlas:{entity_type}:{namespace}:{entity_name}`).

    Centralizes URN construction, parsing, validation, equality, hashing, and
    serialization for all metadata models.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    entity_type: str
    namespace: str = "default"
    entity_name: str

    def __init__(self, *args: Any, **data: Any) -> None:
        if len(args) == 1 and not data:
            arg = args[0]
            if isinstance(arg, str):
                parsed = self._parse_urn_string(arg)
                data = parsed
            elif isinstance(arg, URN):
                data = {
                    "entity_type": arg.entity_type,
                    "namespace": arg.namespace,
                    "entity_name": arg.entity_name,
                }
            elif isinstance(arg, dict):
                data = dict(arg)
            else:
                raise InvalidURNError(f"Cannot construct URN from {arg!r}.")
        elif args:
            raise InvalidURNError("URN accepts either a single URN string or keyword arguments.")

        try:
            super().__init__(**data)
        except InvalidURNError:
            raise
        except Exception as exc:
            raise InvalidURNError(f"Invalid URN specification: {exc}") from exc

    @model_validator(mode="before")
    @classmethod
    def _coerce_input(cls, value: Any) -> Any:
        if isinstance(value, cls):
            return {
                "entity_type": value.entity_type,
                "namespace": value.namespace,
                "entity_name": value.entity_name,
            }
        if isinstance(value, str):
            return cls._parse_urn_string(value)
        return value

    @model_validator(mode="after")
    def _validate_components(self) -> URN:
        norm_type = _validate_component(self.entity_type, "entity_type", lower=True)
        norm_ns = _validate_component(self.namespace, "namespace", lower=True)
        norm_name = _validate_component(self.entity_name, "entity_name", lower=False)

        object.__setattr__(self, "entity_type", norm_type)
        object.__setattr__(self, "namespace", norm_ns)
        object.__setattr__(self, "entity_name", norm_name)
        return self

    @staticmethod
    def _parse_urn_string(raw: str) -> dict[str, str]:
        if not isinstance(raw, str) or not raw.strip():
            raise InvalidURNError("URN string cannot be empty.", urn=str(raw))
        text = raw.strip()
        parts = text.split(":")
        if len(parts) != 5 or parts[0].lower() != "urn" or parts[1].lower() != "atlas":
            raise InvalidURNError(
                f"Malformed URN '{raw}'. "
                "Expected format 'urn:atlas:<entity_type>:<namespace>:<entity_name>'.",
                urn=raw,
            )
        return {
            "entity_type": parts[2],
            "namespace": parts[3],
            "entity_name": parts[4],
        }

    @classmethod
    def parse(cls, value: str | URN) -> URN:
        """Parse and validate a URN string or return an existing :class:`URN`."""
        if isinstance(value, cls):
            return value
        if not isinstance(value, str):
            raise InvalidURNError(f"Expected URN string, got {type(value).__name__}.")
        return cls(**cls._parse_urn_string(value))

    @classmethod
    def for_dataset(cls, entity_name: str, *, namespace: str = "default") -> URN:
        """Construct a validated dataset URN (`urn:atlas:dataset:{namespace}:{entity_name}`)."""
        return cls(entity_type="dataset", namespace=namespace, entity_name=entity_name)

    @classmethod
    def for_schema(
        cls,
        dataset_id: str,
        version: int,
        *,
        namespace: str = "default",
    ) -> URN:
        """Construct a validated schema URN (`urn:atlas:schema:{namespace}:{id}.v{version}`)."""
        if not isinstance(version, int) or isinstance(version, bool) or version < 1:
            raise InvalidURNError(f"Schema version must be an integer >= 1, got {version!r}.")
        clean_ds = _validate_component(dataset_id, "entity_name")
        return cls(
            entity_type="schema",
            namespace=namespace,
            entity_name=f"{clean_ds}.v{version}",
        )

    @classmethod
    def for_artifact(cls, artifact_id: str, *, namespace: str = "default") -> URN:
        """Construct a validated artifact URN (`urn:atlas:artifact:{namespace}:{artifact_id}`)."""
        return cls(entity_type="artifact", namespace=namespace, entity_name=artifact_id)

    def to_string(self) -> str:
        """Return the canonical string representation of this URN."""
        return f"{URN_PREFIX}:{self.entity_type}:{self.namespace}:{self.entity_name}"

    @model_serializer(when_used="json")
    def _serialize_json(self) -> str:
        return self.to_string()

    def __str__(self) -> str:
        return self.to_string()

    def __repr__(self) -> str:
        return (
            f"URN(entity_type={self.entity_type!r}, "
            f"namespace={self.namespace!r}, "
            f"entity_name={self.entity_name!r})"
        )

    def __eq__(self, other: object) -> bool:
        if isinstance(other, URN):
            return (
                self.entity_type == other.entity_type
                and self.namespace == other.namespace
                and self.entity_name == other.entity_name
            )
        return False

    def __hash__(self) -> int:
        return hash((self.entity_type, self.namespace, self.entity_name))


__all__ = [
    "URN",
    "URN_PREFIX",
]
