# UDB Python SDK

`udb-client` is the Python SDK for the UDB DataBroker gRPC API. It ships the generated protobuf bindings plus a small sync/async client that injects UDB metadata, builds common CRUD/vector/blob requests, and exposes the raw generated stub for every broker RPC.

## Install

```bash
pip install udb-client==0.3.0
```

For local development from this repo, prefer `uv`:

```bash
cd sdk/python
uv sync --extra dev
uv run python scripts/generate_protos.py
uv run pytest
uv run pyrefly check
```

The same flow with pip:

```bash
cd sdk/python
python -m pip install -e ".[dev]"
python scripts/generate_protos.py
pytest
pyrefly check
```

## Basic CRUD

```python
from udb_client import Metadata, UdbClient, decode_records

meta = Metadata(
    tenant_id="tenant-1",
    user_id="user-1",
    purpose="billing.demo",
    correlation_id="demo-001",
    scopes=("udb:read", "udb:write"),
    service_identity="python.example",
    project_id="billing",
)

with UdbClient("127.0.0.1:50051", meta) as udb:
    udb.warmup()

    udb.upsert(
        message_type="acme.billing.v1.Customer",
        record={
            "customer_id": "cus_001",
            "tenant_id": "tenant-1",
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

    udb.delete(
        message_type="acme.billing.v1.Customer",
        filter={"customer_id": "cus_001"},
    )
```

## Async

```python
from udb_client import Metadata, UdbAsyncClient

async with UdbAsyncClient("127.0.0.1:50051", Metadata(
    tenant_id="tenant-1",
    purpose="billing.worker",
    correlation_id="job-42",
    scopes=("udb:read", "udb:write"),
    service_identity="python.worker",
    project_id="billing",
)) as udb:
    await udb.upsert(
        message_type="acme.billing.v1.Customer",
        record={"customer_id": "cus_002", "tenant_id": "tenant-1"},
        conflict_fields=("customer_id",),
    )
```

## Full API Access

The generated protobuf modules are included:

```python
from udb.entity.v1 import types_pb2
from udb.services.v1 import data_broker_pb2_grpc
```

For broker APIs without a convenience method, use `client.call("RpcName", request)` for unary RPCs with metadata/error handling, or `client.stub` for raw streaming and advanced calls.

## Optional Pydantic Models

Install the pydantic extra when you want request validation and editor-friendly models:

```bash
pip install "udb-client[pydantic]==0.3.0"
```

```python
from udb_client import UdbClient
from udb_client.models import MetadataModel, UpsertCommand

meta = MetadataModel(
    tenant_id="tenant-1",
    purpose="billing.demo",
    correlation_id="demo-001",
    scopes=("udb:write",),
    project_id="billing",
).to_metadata()

command = UpsertCommand(
    message_type="acme.billing.v1.Customer",
    record={"customer_id": "cus_001", "tenant_id": "tenant-1"},
    conflict_fields=("customer_id",),
)

with UdbClient("127.0.0.1:50051", meta) as udb:
    udb.upsert(command.to_proto())
```

## Regenerate Protobuf Bindings

Run this after changing files under `proto/udb`:

```bash
cd sdk/python
python -m pip install -e ".[dev]"
python scripts/generate_protos.py
```

Generated `udb/.../*_pb2.py`, `*_pb2_grpc.py`, and `*.pyi` files are committed so pip users do not need `grpcio-tools`.
