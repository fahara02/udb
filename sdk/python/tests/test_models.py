from __future__ import annotations

from udb_client.models import MetadataModel, SelectQuery, UpsertCommand


def test_pydantic_metadata_model_builds_sdk_metadata() -> None:
    metadata = MetadataModel(
        tenant_id="tenant-1",
        purpose="billing.test",
        correlation_id="corr-1",
        scopes=("udb:read",),
    ).to_metadata()

    assert metadata.tenant_id == "tenant-1"
    assert dict(metadata.to_grpc_metadata())["x-scopes"] == "udb:read"


def test_pydantic_request_models_build_proto_messages() -> None:
    metadata = MetadataModel(
        tenant_id="tenant-1",
        purpose="billing.test",
        correlation_id="corr-1",
    ).to_metadata()

    select = SelectQuery(
        message_type="acme.billing.v1.Customer",
        filter={"customer_id": "cus_1"},
        limit=1,
    ).to_proto(metadata)
    upsert = UpsertCommand(
        message_type="acme.billing.v1.Customer",
        record={"customer_id": "cus_1", "tenant_id": "tenant-1"},
        conflict_fields=("customer_id",),
    ).to_proto(metadata)

    assert select.context.tenant_id == "tenant-1"
    assert select.filter.fields["customer_id"].string_value == "cus_1"
    assert upsert.record_json == b'{"customer_id":"cus_1","tenant_id":"tenant-1"}'
