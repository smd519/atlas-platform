"""Integration tests for MetadataService, dependency injection, and public API exports."""

from __future__ import annotations

import atlas_metadata
from atlas_core import Artifact, Dataset, MetadataProvider
from atlas_metadata import (
    URN,
    ArtifactMetadata,
    ColumnMetadata,
    DatasetCatalog,
    DatasetMetadata,
    InMemoryMetadataRepository,
    MetadataService,
    SchemaRegistry,
)


def test_metadata_service_implements_core_metadata_provider_and_di() -> None:
    """MetadataService implements atlas_core.MetadataProvider and supports dependency injection."""
    shared_repo = InMemoryMetadataRepository()
    catalog = DatasetCatalog(repository=shared_repo)
    schema_registry = SchemaRegistry(repository=shared_repo)
    service = MetadataService(
        repository=shared_repo,
        catalog=catalog,
        schema_registry=schema_registry,
    )
    assert isinstance(service, MetadataProvider)

    # Register via atlas_core.Dataset contract
    core_ds = Dataset(
        id="transactions_bronze",
        name="Raw Transactions",
        layer="bronze",
        source_id="src-payments",
        format="json",
        path="/lake/bronze/transactions.json",
        description="Raw payment transactions",
        metadata={"owner": "payments-team", "tags": ["finance", "raw"]},
    )
    service.register_dataset(core_ds)

    fetched_core = service.get_dataset("transactions_bronze")
    assert fetched_core is not None
    assert fetched_core.id == "transactions_bronze"
    assert fetched_core.name == "Raw Transactions"

    fetched_meta = service.get_dataset_metadata("transactions_bronze")
    assert fetched_meta is not None
    assert fetched_meta.urn == URN.for_dataset("transactions_bronze")
    assert fetched_meta.owner == "payments-team"
    assert fetched_meta.tags == ["finance", "raw"]


def test_end_to_end_dataset_schema_and_artifact_lifecycle() -> None:
    """Verify end-to-end dataset registration, schema evolution, and artifact tracking."""
    service = MetadataService()

    # 1. Register dataset metadata
    ds = service.register_dataset_metadata(
        DatasetMetadata(
            dataset_id="customer_orders",
            namespace="analytics",
            name="Customer Orders Silver",
            layer="silver",
            format="parquet",
            location_uri="/lake/silver/customer_orders",
            owner="data-platform",
            tags=["orders", "silver"],
            custom_properties={"domain": "retail"},
        )
    )
    assert str(ds.urn) == "urn:atlas:dataset:analytics:customer_orders"

    # 2. Register schema v1 and v2
    s_v1 = service.register_schema(
        "customer_orders",
        namespace="analytics",
        version=1,
        columns=[
            ColumnMetadata(name="order_id", data_type="string", primary_key=True),
            ColumnMetadata(name="customer_id", data_type="string", nullable=False),
            ColumnMetadata(name="amount", data_type="float", nullable=False),
        ],
    )
    s_v2 = service.register_schema(
        "customer_orders",
        namespace="analytics",
        version=2,
        columns=[
            ColumnMetadata(name="order_id", data_type="string", primary_key=True),
            ColumnMetadata(name="customer_id", data_type="string", nullable=False),
            ColumnMetadata(name="amount", data_type="float", nullable=False),
            ColumnMetadata(name="currency", data_type="string", default_value="USD"),
        ],
    )
    assert s_v1.version == 1
    assert s_v2.version == 2
    assert service.get_latest_schema("customer_orders") == s_v2
    assert service.get_dataset_metadata("customer_orders").latest_schema_version == 2  # type: ignore[union-attr]

    # 3. Register physical artifacts linked to dataset and schema versions
    art1 = service.register_artifact(
        ArtifactMetadata(
            artifact_id="art-orders-20261103",
            dataset_id="customer_orders",
            namespace="analytics",
            uri="/lake/silver/customer_orders/date=2026-11-03/part-0.parquet",
            format="parquet",
            layer="silver",
            execution_id="exec-101",
            schema_version=1,
            row_count=1250,
            byte_size=65536,
        ),
        require_existing_dataset=True,
    )
    art2 = service.register_artifact(
        Artifact(
            id="art-orders-20261104",
            dataset_id="customer_orders",
            path="/lake/silver/customer_orders/date=2026-11-04/part-0.parquet",
            format="parquet",
        ),
        require_existing_dataset=True,
    )

    assert service.get_artifact(art1.artifact_id) == art1
    assert service.get_artifact(art2.artifact_id) == art2
    dataset_artifacts = service.list_artifacts(dataset_id="customer_orders")
    assert len(dataset_artifacts) == 2
    exec_artifacts = service.list_artifacts(execution_id="exec-101")
    assert exec_artifacts == [art1]


def test_public_package_exports() -> None:
    """Verify all symbols in atlas_metadata.__all__ are importable and non-None."""
    assert len(atlas_metadata.__all__) >= 25
    for symbol in atlas_metadata.__all__:
        assert hasattr(atlas_metadata, symbol), f"Missing export: {symbol}"
        assert getattr(atlas_metadata, symbol) is not None


def test_core_dataset_round_trip_overwrite_preserves_catalog_fields() -> None:
    """get_dataset -> edit metadata -> register_dataset_metadata(overwrite) keeps other fields."""
    service = MetadataService()
    service.register_dataset_metadata(
        DatasetMetadata(
            dataset_id="orders",
            name="orders",
            owner="alice",
            tags=["pii"],
            description="Orders",
            upstream_urns=["urn:atlas:dataset:default:raw_orders"],
            custom_properties={"team": "data-eng"},
        )
    )
    service.register_schema("orders", [{"name": "id", "data_type": "int"}])
    before = service.get_dataset_metadata("orders")
    assert before is not None
    assert before.latest_schema_version == 1

    core = service.get_dataset("orders")
    assert core is not None
    props = dict(core.metadata)
    props["latest_quality_assessment"] = {"overall_status": "PASSED"}
    service.register_dataset_metadata(core.model_copy(update={"metadata": props}), overwrite=True)

    after = service.get_dataset_metadata("orders")
    assert after is not None
    assert after.latest_schema_version == 1
    assert after.created_at == before.created_at
    assert after.custom_properties == {
        "team": "data-eng",
        "latest_quality_assessment": {"overall_status": "PASSED"},
    }
    unchanged = DatasetMetadata.model_fields.keys() - {"custom_properties", "updated_at"}
    for field in unchanged:
        assert getattr(after, field) == getattr(before, field), field
