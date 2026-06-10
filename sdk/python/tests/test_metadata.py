from __future__ import annotations

from udb.entity.v1 import types_pb2
from udb_client import Metadata, to_record_json, to_struct


def test_metadata_headers_include_required_and_routing_values() -> None:
    meta = Metadata(
        tenant_id="tenant-1",
        user_id="user-1",
        purpose="billing.test",
        correlation_id="corr-1",
        scopes=("udb:read", "udb:write"),
        service_identity="pytest",
        project_id="billing",
        target_backend="postgres",
        primary_read=True,
    )

    headers = dict(meta.to_grpc_metadata())

    assert headers["x-tenant-id"] == "tenant-1"
    assert headers["x-scopes"] == "udb:read,udb:write"
    assert headers["x-udb-project-id"] == "billing"
    assert headers["x-udb-target-backend"] == "postgres"
    assert headers["x-udb-primary-read"] == "true"


def test_metadata_headers_include_credentials() -> None:
    headers = dict(
        Metadata(
            tenant_id="tenant-1",
            purpose="billing.test",
            correlation_id="corr-1",
            bearer_token="bearer-1",
            api_key="api-key-1",
        ).to_grpc_metadata()
    )

    assert headers["authorization"] == "Bearer bearer-1"
    assert headers["x-api-key"] == "api-key-1"


def test_metadata_builds_request_context() -> None:
    meta = Metadata(
        tenant_id="tenant-1",
        purpose="billing.test",
        correlation_id="corr-1",
        scopes=("udb:read",),
        service_identity="pytest",
    )

    context = meta.to_request_context()

    assert context.tenant_id == "tenant-1"
    assert context.scopes == ["udb:read"]
    assert context.service_identity == "pytest"


def test_helpers_build_protobuf_payloads() -> None:
    assert to_struct({"status": "paid"}).fields["status"].string_value == "paid"
    assert to_record_json({"b": 2, "a": 1}) == b'{"a":1,"b":2}'

    request = types_pb2.SelectRequest(filter=to_struct({"customer_id": "cus_1"}))
    assert request.filter.fields["customer_id"].string_value == "cus_1"
