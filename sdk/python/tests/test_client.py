from __future__ import annotations

import pytest

from udb.entity.v1 import types_pb2
from udb_client import Metadata, UdbClient
from udb_client.exceptions import UdbConfigurationError


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
