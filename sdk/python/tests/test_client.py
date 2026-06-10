from __future__ import annotations

import pytest

from udb.entity.v1 import types_pb2
from udb_client import Metadata, UdbClient
from udb_client.exceptions import UdbConfigurationError
from udb_client.generated_client import DataBrokerClient


def test_client_injects_context_without_mutating_original_request() -> None:
    client = UdbClient(
        "unused",
        Metadata(
            tenant_id="tenant-1",
            purpose="billing.test",
            correlation_id="corr-1",
            scopes=("udb:read",),
        ),
    )
    request = types_pb2.SelectRequest(message_type="acme.billing.v1.Customer")

    cloned = client._with_context(request, None)

    assert request.context.tenant_id == ""
    assert cloned.context.tenant_id == "tenant-1"
    assert cloned.context.scopes == ["udb:read"]
    client.close()


def test_client_requires_metadata_before_rpc_context_injection() -> None:
    client = UdbClient("unused")

    with pytest.raises(UdbConfigurationError):
        client._with_context(types_pb2.SelectRequest(), None)

    client.close()


def test_generated_client_uses_canonical_api_key_header() -> None:
    client = DataBrokerClient(
        "unused",
        bearer_token="bearer-1",
        api_key="api-key-1",
    )

    try:
        headers = dict(client._call_metadata(None, "request-1"))
    finally:
        client.close()

    assert headers["authorization"] == "Bearer bearer-1"
    assert headers["x-api-key"] == "api-key-1"
    assert headers["x-request-id"] == "request-1"
    assert "x-udb-api-key" not in headers
