# UDB Python SDK

<!-- UDB_BRAND_HEADER_START -->
<p align="center">
  <img src="../../docs/assets/udb_logo.svg" alt="UDB logo" width="96">
</p>

<p align="center">
  <strong>UDB :: Universal Data Broker</strong><br>
  <sub>gRPC data plane | native control plane | tenant/project scope guard<br>crate v0.3.5 | protocol v1.0.0</sub>
</p>
<!-- UDB_BRAND_HEADER_END -->

`udb-client` is the Python client for UDB. It gives you sync and async clients,
metadata handling, common CRUD helpers, optional Pydantic models, raw access to
every broker RPC, and a version-matched `udb` CLI launcher.

## Install

```bash
pip install udb-client==0.3.5
```

Optional validated command models:

```bash
pip install "udb-client[pydantic]==0.3.5"
```

Runtime: Python 3.10+

## Export UDB Protos For Your App

After installation, the `udb` command is available. Use it from your app project
to copy UDB's annotation and broker protos into your own proto tree:

```bash
udb proto export --fmt
```

Then your project schemas can import:

```proto
import "udb/core/common/v1/db.proto";
```

Run `udb proto export` again whenever you upgrade the SDK/CLI. It keeps the UDB
annotation protos and your installed CLI version together.

Run `udb proto fmt` after export or schema edits to keep long UDB field
annotations on one line for easier review.

## Basic CRUD

```python
from udb_client import Metadata, UdbClient, decode_records

meta = Metadata(
    tenant_id="acme",
    user_id="user-1",
    purpose="billing.api",
    correlation_id="request-001",
    scopes=("udb:read", "udb:write"),
    service_identity="billing-service",
    project_id="billing",
)

with UdbClient("127.0.0.1:50051", meta) as udb:
    udb.upsert(
        message_type="acme.billing.v1.Customer",
        record={
            "customer_id": "cus_001",
            "tenant_id": "acme",
            "name": "Ada Lovelace",
            "email": "ada@example.com",
        },
        conflict_fields=("customer_id",),
        return_record=True,
    )

    rows = udb.select(
        message_type="acme.billing.v1.Customer",
        filter={"customer_id": "cus_001"},
        limit=1,
    )
    print(decode_records(rows))
```

## Async

```python
from udb_client import Metadata, UdbAsyncClient

meta = Metadata(
    tenant_id="acme",
    purpose="billing.worker",
    correlation_id="job-42",
    scopes=("udb:read", "udb:write"),
    service_identity="billing-worker",
    project_id="billing",
)

async with UdbAsyncClient("127.0.0.1:50051", meta) as udb:
    await udb.upsert(
        message_type="acme.billing.v1.Customer",
        record={"customer_id": "cus_002", "tenant_id": "acme"},
        conflict_fields=("customer_id",),
    )
```

## Authz Check

```python
from udb_client.auth import UdbAuthClient
from udb.core.authz.services.v1 import core_pb2 as authz

with UdbAuthClient("127.0.0.1:50051", meta) as auth:
    allowed, decision = auth.can(
        authz.ResourceRef(message_type="acme.billing.v1.Customer"),
        "read",
    )
```

## Full API Access

For broker APIs without a convenience method, use `client.call("RpcName",
request)` for unary RPCs with metadata/error handling, or `client.stub` for raw
streaming and advanced calls.

The generated protobuf modules are included for request/response types:

```python
from udb.entity.v1 import types_pb2
from udb.services.v1 import data_broker_pb2_grpc
```

## Local SDK Development

Consumers do not need this. Use it only when editing the SDK itself:

```bash
cd sdk/python
python -m pip install -e ".[dev]"
python scripts/generate_protos.py
pytest
pyrefly check
```
