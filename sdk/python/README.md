# UDB Python SDK

<!-- UDB_BRAND_HEADER_START -->
<p align="center">
  <img src="../../docs/assets/udb_logo.svg" alt="UDB logo" width="96">
</p>

<p align="center">
  <strong>UDB :: Universal Data Broker</strong><br>
  <sub>gRPC data plane | native control plane | tenant/project scope guard<br>crate v0.4.37 | protocol v1.0.0</sub>
</p>
<!-- UDB_BRAND_HEADER_END -->

`udb-client` is the Python client for UDB. It gives you sync and async clients,
metadata handling, common CRUD helpers, optional Pydantic models, raw access to
every broker RPC, and a version-matched `udb` CLI launcher.

## Install

```bash
pip install udb-client==0.4.37
```

Optional validated command models:

```bash
pip install "udb-client[pydantic]==0.4.37"
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

The async client is faster under concurrency than the sync client (it shares one
HTTP/2 channel across many in-flight coroutines), so prefer `UdbAsyncClient` /
`UdbAsyncProject` (`create_udb_async`) for high-throughput or fan-out workloads.

## File Storage

`UdbProject` exposes the `StorageService` file lifecycle on `project.storage`.
The one-call `upload_file` does `RegisterUpload` → HTTP PUT the bytes to the
presigned URL → `FinalizeUpload` (exactly three RPCs, no proof Get/List):

```python
from udb_client import UdbConfig, UdbProject

with UdbProject.connect(
    UdbConfig(target="127.0.0.1:50051", tenant_id="acme", project_id="default")
) as project:
    project.login_and_adopt_tenant("svc@acme", "password")  # bearer + canonical tenant

    finalized = project.storage.upload_file(
        "report.pdf",
        b"...file bytes...",
        {"content_type": "application/pdf", "file_type": "report"},
    )
    file_id = finalized.file_id
```

### Download

Downloads prefer the presigned `GetDownloadUrl` path so file bytes never travel
through the broker. `download_file` returns the `GetDownloadUrlResponse` by
default; pass `fetch=True` to also HTTP GET the presigned URL and return the raw
bytes:

```python
# Presigned URL only (default): no bytes through the broker
resp = project.storage.download_file(file_id)
print(resp.download_url)

# Resolve the presigned URL and HTTP GET the bytes back
data = project.storage.download_file(file_id, fetch=True)
```

New in 0.3.6: when a client cannot egress to the object store (no public
download URL, corporate proxy, etc.), stream the bytes **through the broker**
with the server-streaming `StorageService.DownloadFile` RPC. Pass `stream=True`
to `download_file`, or call `download_stream` directly — both issue exactly one
`DownloadFile` RPC and reassemble every `DownloadFileChunk` into the full body:

```python
# Server-streaming fallback through the broker
data = project.storage.download_file(file_id, stream=True)
# equivalently:
data = project.storage.download_stream(file_id)
```

## Performance

Every UDB client holds a **single long-lived gRPC channel** — construct the
client once and reuse it across all RPCs (the `with`/`async with` block above keeps
it open for its body). Never create a client per call: a fresh channel forces a
TCP+TLS+HTTP/2 handshake every time, which dominates per-RPC latency.

By default the channel is built with `udb_client.default_channel_options()`, which adds:

- **Keepalive** (`keepalive_time_ms=30000`, `keepalive_timeout_ms=10000`,
  `keepalive_permit_without_calls=1`) so an idle connection stays warm instead of
  dropping to IDLE and re-handshaking.
- No channel-wide retry policy. Generated wrappers retry only read-only unary RPCs
  using proto-derived `operation_kind`; mutating RPCs are never replayed by default.

Pass your own `channel_options=` to override any of these (your options win on
conflicting keys); `merge_channel_options(...)` is exposed if you want to layer
yours on top of the defaults yourself.

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

## Consistency, Idempotency Replay, And Typed Errors

A mutation returns a `WriteReceipt`; a follow-up read can carry a read fence
built from it, or request an explicit consistency mode. A keyed replay-safe
mutation the broker deduplicated reports `was_duplicate` (full contract:
[docs/native-services.md](../../docs/native-services.md)).

```python
from udb_client import ConsistencyMode, was_duplicate

resp, receipt = udb.upsert_with_receipt(
    message_type="acme.billing.v1.Invoice",
    record={"invoice_id": "inv-1", "total_cents": 100},
    conflict_fields=("invoice_id",),
    idempotency_key="inv-1-create",
)
if was_duplicate(resp):
    ...  # broker replayed a prior identical keyed write

# Read-your-writes: metadata carrying a fence derived from the receipt.
fenced = meta.after_write(receipt)
rows = udb.select(message_type="acme.billing.v1.Invoice", metadata=fenced)

# Or pin a typed consistency mode (strong / read-your-writes / bounded / eventual …).
eventual = meta.with_consistency(ConsistencyMode.EVENTUAL)
```

Retry contract: a replay-safe mutation is retried on transient errors **only
when the request carries a non-empty `idempotency_key`** — keyless mutations
fail closed rather than risk a double apply. Typed error details decode from
the `udb-error-detail-bin` trailer onto `UdbDetailedError`
(`kind` / `retryable` / `field_violations`).

## Platform Services (Vault, Metering, Scheduler, …)

Vault, Metering, Scheduler, Search, Webhook, Workflow, Lock, LiveQuery, Config,
Backup, and Embedding have no workflow facade yet — call them through the
generated per-service robustness clients (native services ride the native
listener when your deployment separates it):

```python
from udb_client.generated_client import GeneratedClient
from udb.core.vault.services.v1 import vault_service_pb2 as vault

gen = GeneratedClient("127.0.0.1:50051", meta)
key = gen.VaultService.create_transit_key(
    vault.CreateTransitKeyRequest(tenant_id=tenant, key_name="docs")
)
enc = gen.VaultService.encrypt(
    vault.EncryptRequest(tenant_id=tenant, key_name="docs", plaintext="secret")
)
```

The per-RPC retry class and idempotency contract live in
[docs/generated/udb-native-contract.json](../../docs/generated/udb-native-contract.json).

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
