from __future__ import annotations

from google.protobuf.json_format import MessageToDict

from gen.acme.billing.v1 import acme_billing_v1_pb2 as acme_billing
from udb_client.models import MetadataModel


def test_generated_product_model_uses_proto_field_names() -> None:
    product = acme_billing.Product(
        product_id="prod-test-1",
        name="Generated Python model",
        description="Generated from the example proto",
        price_cents=12900,
        sku="PY-GEN-001",
    )

    payload = MessageToDict(product, preserving_proto_field_name=True)

    assert payload["product_id"] == "prod-test-1"
    assert payload["price_cents"] == "12900"


def test_pydantic_metadata_model_builds_sdk_metadata() -> None:
    metadata = MetadataModel(
        tenant_id="acme-org-1",
        purpose="billing.example",
        correlation_id="test-correlation",
        scopes=("udb:read", "udb:write"),
    ).to_metadata()

    headers = dict(metadata.to_grpc_metadata())

    assert headers["x-tenant-id"] == "acme-org-1"
    assert headers["x-scopes"] == "udb:read,udb:write"
