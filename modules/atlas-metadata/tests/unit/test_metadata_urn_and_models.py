"""Unit tests for URN, DatasetMetadata, SchemaMetadata, ColumnMetadata, and ArtifactMetadata."""

from __future__ import annotations

import json

import pytest
from atlas_core import Artifact, Dataset
from atlas_metadata import (
    URN,
    ArtifactMetadata,
    ColumnMetadata,
    DatasetMetadata,
    InvalidMetadataError,
    InvalidSchemaError,
    InvalidURNError,
    SchemaMetadata,
)
from pydantic import ValidationError


def test_urn_construction_parsing_equality_hashing_and_serialization() -> None:
    """URN validates components, parses strings, supports hashing/equality, and serializes."""
    u1 = URN(entity_type="Dataset", namespace="Commerce", entity_name="orders_silver")
    assert u1.entity_type == "dataset"
    assert u1.namespace == "commerce"
    assert u1.entity_name == "orders_silver"
    assert str(u1) == "urn:atlas:dataset:commerce:orders_silver"
    assert u1.to_string() == "urn:atlas:dataset:commerce:orders_silver"

    u2 = URN.parse("urn:atlas:dataset:commerce:orders_silver")
    u3 = URN("urn:atlas:dataset:commerce:orders_silver")
    u4 = URN.for_dataset("orders_silver", namespace="commerce")
    assert u1 == u2 == u3 == u4
    assert hash(u1) == hash(u2) == hash(u3) == hash(u4)
    assert len({u1, u2, u3, u4}) == 1
    assert u1 != "urn:atlas:dataset:commerce:orders_silver"

    # JSON serialization produces canonical URN string
    dumped = u1.model_dump_json()
    assert json.loads(dumped) == "urn:atlas:dataset:commerce:orders_silver"

    # Factory helpers
    s_urn = URN.for_schema("orders_silver", 2, namespace="commerce")
    assert str(s_urn) == "urn:atlas:schema:commerce:orders_silver.v2"

    a_urn = URN.for_artifact("art-001", namespace="commerce")
    assert str(a_urn) == "urn:atlas:artifact:commerce:art-001"


@pytest.mark.parametrize(
    "bad_input",
    [
        "",
        "   ",
        "not_a_urn",
        "urn:other:dataset:default:orders",
        "urn:atlas:dataset:only_four_parts",
        "urn:atlas:dataset:ns:name:extra",
        "urn:atlas:123invalid_type:default:orders",
        "urn:atlas:dataset::orders",
        "urn:atlas:dataset:default:",
    ],
)
def test_urn_rejects_malformed_strings(bad_input: str) -> None:
    """URN raises InvalidURNError on invalid URN strings or components."""
    with pytest.raises(InvalidURNError):
        URN.parse(bad_input)


def test_dataset_metadata_accepts_urn_and_validates_custom_properties() -> None:
    """DatasetMetadata accepts URN instances or strings and validates JSON custom properties."""
    urn = URN.for_dataset("customers", namespace="crm")
    ds = DatasetMetadata(
        dataset_id="customers",
        urn=urn,
        name="CRM Customers",
        layer="Silver",
        format="parquet",
        owner="crm-team",
        tags=["pii", "customers", "pii"],  # deduplicates while preserving order
        custom_properties={
            "retention_days": 90,
            "encrypted": True,
            "regions": ["US", "EU"],
            "nested": {"tier": 1, "notes": None},
        },
    )
    assert ds.urn == urn
    assert ds.namespace == "crm"
    assert ds.layer == "silver"
    assert ds.tags == ["pii", "customers"]
    assert ds.custom_properties["retention_days"] == 90

    # Roundtrip conversion with atlas_core.Dataset
    core_ds = ds.to_core_dataset()
    assert isinstance(core_ds, Dataset)
    assert core_ds.id == "customers"
    reconstructed = DatasetMetadata.from_core_dataset(core_ds)
    assert reconstructed.urn == ds.urn
    assert reconstructed.owner == "crm-team"
    assert reconstructed.tags == ["pii", "customers"]

    # Wrong URN entity_type is rejected
    with pytest.raises(InvalidURNError, match="entity_type 'dataset'"):
        DatasetMetadata(
            dataset_id="customers",
            urn=URN.for_artifact("customers"),
            name="Bad URN",
        )

    # Non-JSON custom properties are rejected
    with pytest.raises(InvalidMetadataError, match="not JSON-compatible"):
        DatasetMetadata(
            dataset_id="customers",
            name="Bad Props",
            custom_properties={"callback": lambda x: x},
        )

    with pytest.raises(InvalidMetadataError, match="Non-finite float"):
        DatasetMetadata(
            dataset_id="customers",
            name="NaN Props",
            custom_properties={"score": float("nan")},
        )


def test_schema_and_column_metadata_immutability_and_validation() -> None:
    """SchemaMetadata and ColumnMetadata are strictly immutable and validate columns."""
    col1 = ColumnMetadata(
        name="order_id",
        data_type="STRING",
        primary_key=True,
        tags=["pk"],
        custom_properties={"indexed": True},
    )
    assert col1.data_type == "string"
    assert col1.nullable is False  # primary_key defaults nullable to False

    col2 = ColumnMetadata(name="amount", data_type="FLOAT", nullable=False, default_value=0.0)

    schema = SchemaMetadata(
        dataset_id="orders",
        namespace="sales",
        version=1,
        columns=[col1, col2],
        description="Initial orders schema",
    )
    assert str(schema.urn) == "urn:atlas:schema:sales:orders.v1"
    assert schema.column_names == ("order_id", "amount")
    assert schema.get_column("order_id") == col1
    assert schema.get_column("nonexistent") is None

    # Immutability check on SchemaMetadata and ColumnMetadata
    with pytest.raises(ValidationError):
        schema.version = 2  # type: ignore[misc]

    with pytest.raises(ValidationError):
        col1.name = "mutated"  # type: ignore[misc]

    # Duplicate columns rejected
    with pytest.raises(InvalidSchemaError, match="Duplicate column name"):
        SchemaMetadata(
            dataset_id="orders",
            version=1,
            columns=[col1, ColumnMetadata(name="order_id", data_type="int")],
        )

    # Empty columns rejected
    with pytest.raises(InvalidSchemaError, match="at least one ColumnMetadata"):
        SchemaMetadata(dataset_id="orders", version=1, columns=[])

    # Version < 1 rejected
    with pytest.raises(InvalidSchemaError, match="version"):
        SchemaMetadata(dataset_id="orders", version=0, columns=[col1])


def test_artifact_metadata_validation_and_core_interoperability() -> None:
    """ArtifactMetadata validates URNs, metrics, and converts to/from atlas_core.Artifact."""
    art = ArtifactMetadata(
        artifact_id="art-100",
        dataset_id="orders",
        namespace="sales",
        uri="/data/silver/orders.parquet",
        format="PARQUET",
        layer="silver",
        execution_id="exec-1",
        schema_version=1,
        row_count=500,
        byte_size=4096,
        tags=["curated"],
        custom_properties={"compression": "snappy"},
    )
    assert str(art.urn) == "urn:atlas:artifact:sales:art-100"
    assert art.format == "parquet"

    core_art = art.to_core_artifact()
    assert isinstance(core_art, Artifact)
    assert core_art.id == "art-100"
    from_core = ArtifactMetadata.from_core_artifact(core_art, namespace="sales", row_count=500)
    assert from_core.urn == art.urn
    assert from_core.row_count == 500

    with pytest.raises(InvalidMetadataError, match="row_count"):
        ArtifactMetadata(
            artifact_id="art-bad",
            dataset_id="orders",
            uri="/tmp/a.csv",
            format="csv",
            row_count=-5,
        )


def test_dataset_upstream_urns_and_artifact_execution_id_serialization() -> None:
    """DatasetMetadata.upstream_urns and ArtifactMetadata.execution_id normalize and serialize."""
    upstream_urn_obj = URN.for_dataset("raw_orders", namespace="commerce")
    ds = DatasetMetadata(
        dataset_id="orders_silver",
        namespace="commerce",
        name="Orders Silver",
        layer="silver",
        format="parquet",
        upstream_urns=[
            str(upstream_urn_obj),
            "  urn:atlas:dataset:commerce:raw_customers  ",
            "urn:atlas:dataset:commerce:raw_orders",  # duplicate normalized string
        ],
    )
    assert ds.upstream_urns == [
        "urn:atlas:dataset:commerce:raw_orders",
        "urn:atlas:dataset:commerce:raw_customers",
    ]
    assert all(isinstance(u, str) for u in ds.upstream_urns)

    ds_payload = json.loads(ds.model_dump_json())
    assert ds_payload["upstream_urns"] == [
        "urn:atlas:dataset:commerce:raw_orders",
        "urn:atlas:dataset:commerce:raw_customers",
    ]

    core_ds = ds.to_core_dataset()
    reconstructed_ds = DatasetMetadata.from_core_dataset(core_ds)
    assert reconstructed_ds.upstream_urns == ds.upstream_urns

    art = ArtifactMetadata(
        artifact_id="art-prov-1",
        dataset_id="orders_silver",
        namespace="commerce",
        uri="data/silver/orders.parquet",
        format="parquet",
        layer="silver",
        execution_id="  exec-prov-99  ",
        row_count=42,
    )
    assert art.execution_id == "exec-prov-99"

    art_payload = json.loads(art.model_dump_json())
    assert art_payload["execution_id"] == "exec-prov-99"
    assert art_payload["uri"] == "data/silver/orders.parquet"
    assert art_payload["dataset_id"] == "orders_silver"
