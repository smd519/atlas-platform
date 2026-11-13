"""Identifier generation and canonical URN utilities for the Atlas Platform."""

from __future__ import annotations

import uuid

URN_PREFIX = "urn:atlas"
DEFAULT_NAMESPACE = "default"


def generate_execution_id() -> str:
    """Generate a unique execution identifier."""
    return str(uuid.uuid4())


def generate_dataset_id() -> str:
    """Generate a unique dataset identifier."""
    return str(uuid.uuid4())


def generate_artifact_id() -> str:
    """Generate a unique artifact identifier."""
    return str(uuid.uuid4())


def build_urn(
    entity_type: str,
    entity_name: str,
    *,
    namespace: str = DEFAULT_NAMESPACE,
) -> str:
    """Build a canonical Atlas URN (`urn:atlas:{entity_type}:{namespace}:{entity_name}`)."""
    clean_type = entity_type.strip().lower() if isinstance(entity_type, str) else ""
    clean_ns = (
        namespace.strip().lower()
        if isinstance(namespace, str) and namespace.strip()
        else DEFAULT_NAMESPACE
    )
    clean_name = entity_name.strip() if isinstance(entity_name, str) else ""
    if not clean_type:
        raise ValueError("URN entity_type cannot be empty.")
    if not clean_name:
        raise ValueError("URN entity_name cannot be empty.")
    return f"{URN_PREFIX}:{clean_type}:{clean_ns}:{clean_name}"


def build_dataset_urn(
    dataset_id: str,
    *,
    namespace: str = DEFAULT_NAMESPACE,
) -> str:
    """Build a canonical dataset URN (`urn:atlas:dataset:{namespace}:{dataset_id}`)."""
    return build_urn("dataset", dataset_id, namespace=namespace)


def build_artifact_urn(
    artifact_id: str,
    *,
    namespace: str = DEFAULT_NAMESPACE,
) -> str:
    """Build a canonical artifact URN (`urn:atlas:artifact:{namespace}:{artifact_id}`)."""
    return build_urn("artifact", artifact_id, namespace=namespace)


def is_canonical_urn(value: str, *, entity_type: str | None = None) -> bool:
    """Return True if ``value`` is a well-formed canonical `urn:atlas:...` string."""
    if not isinstance(value, str):
        return False
    parts = value.strip().split(":")
    if len(parts) != 5 or parts[0].lower() != "urn" or parts[1].lower() != "atlas":
        return False
    if not parts[2].strip() or not parts[3].strip() or not parts[4].strip():
        return False
    if entity_type is not None and parts[2].strip().lower() != entity_type.strip().lower():
        return False
    return True


__all__ = [
    "DEFAULT_NAMESPACE",
    "URN_PREFIX",
    "build_artifact_urn",
    "build_dataset_urn",
    "build_urn",
    "generate_artifact_id",
    "generate_dataset_id",
    "generate_execution_id",
    "is_canonical_urn",
]
