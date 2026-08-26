# UDB Go SDK

<!-- UDB_BRAND_HEADER_START -->
<p align="center">
  <img src="../../docs/assets/udb_logo.svg" alt="UDB logo" width="96">
</p>

<p align="center">
  <strong>UDB :: Universal Data Broker</strong><br>
  <sub>gRPC data plane | native control plane | tenant/project scope guard<br>crate v0.5.22 | protocol v1.0.0</sub>
</p>
<!-- UDB_BRAND_HEADER_END -->

The Go SDK gives Go services a small client for a running UDB broker. It attaches
UDB metadata for you, exposes DataBroker and auth helpers, and includes a
version-matched CLI launcher.

## Install

```bash
go get github.com/fahara02/udb/sdk/go@v0.5.22
```

Install the `udb` CLI launcher:

```bash
go install github.com/fahara02/udb/sdk/go/cmd/udb@v0.5.22
```

The launcher finds or downloads the matching UDB release binary, then forwards
all arguments to it.

## Export UDB Protos For Your App

From your application repo:

```bash
udb proto export --fmt
```

Now your app protos can import UDB annotations:

```proto
import "udb/core/common/v1/db.proto";
```

Run `udb proto fmt` after export or edits to keep long UDB field annotations on
one line for easier review.

## Connect And Query

```go
import (
    "context"

    entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
    authzv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/services/v1"
    "github.com/fahara02/udb/sdk/go/udbclient"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

ctx := context.Background()
conn, err := grpc.NewClient(
    "localhost:50051",
    grpc.WithTransportCredentials(insecure.NewCredentials()),
)
if err != nil {
    panic(err)
}
defer conn.Close()

meta := udbclient.Metadata{
    TenantID:             "acme",
    UserID:               "user-1",
    Purpose:              "web.request",
    CorrelationID:        "request-001",
    Scopes:               []string{"udb:read", "udb:write"},
    ServiceIdentity:      "billing.api",
    ProjectID:            "billing",
    ClientCatalogVersion: udbclient.ProtocolVersion,
}

udb := udbclient.New(conn, meta)
rows, err := udb.Select(ctx, &entityv1.SelectRequest{
    MessageType: "acme.billing.v1.Invoice",
    Limit:       50,
})
if err != nil {
    panic(err)
}
_ = rows

auth := udbclient.NewAuthClient(conn, meta)
allowed, decision, err := auth.Can(
    ctx,
    &authzv1.ResourceRef{MessageType: "acme.billing.v1.Invoice"},
    "read",
    "",
)
_, _ = allowed, decision
if err != nil {
    panic(err)
}
```

## Storage (Upload And Download Files)

The `Udb` facade (`udbclient.NewUdb`) exposes a `Storage` client. `UploadFile`
runs the register → presigned PUT → finalize sequence in one call; downloads come
in two flavours — a presigned URL (the happy path, bytes never transit the broker)
and a server-streaming byte fetch (the fallback for callers that cannot use a
presigned HTTP URL).

```go
udb, err := udbclient.NewUdb(ctx, udbclient.Config{Target: "localhost:50051"})
if err != nil {
    panic(err)
}
defer udb.Close()

// Log in and adopt the verified principal's canonical tenant/project.
_, err = udb.LoginAndAdoptTenant(ctx, &authnv1.LoginRequest{
    Username: "admin",
    Password: "admin",
})

// Upload bytes (register -> presigned PUT -> finalize).
up, err := udb.Storage.UploadFile(ctx, "greeting.txt", []byte("hello"),
    udbclient.WithContentType("text/plain"))
fileID := up.GetFile().GetFileId()

// Preferred: mint a time-limited presigned download URL (bytes stay out of the broker).
url, err := udb.Storage.DownloadFile(ctx, fileID, 15) // valid 15 minutes; 0 = server default

// Streaming fallback: stream the bytes back through the broker when a presigned
// URL can't be used. Reassembles the server-streaming DownloadFile chunk stream.
res, err := udb.Storage.DownloadFileBytes(ctx, fileID)
_ = res.Data // full file bytes; res.ContentType / res.TotalSize / res.ETag carry metadata
```

The facade signatures are:

```go
func (f *StorageFacade) UploadFile(ctx context.Context, filename string, data []byte, opts ...UploadOption) (*storagev1.FinalizeUploadResponse, error)
func (f *StorageFacade) DownloadFile(ctx context.Context, fileID string, expiresInMinutes int32) (*storagev1.GetDownloadUrlResponse, error)
func (f *StorageFacade) DownloadFileBytes(ctx context.Context, fileID string, opts ...DownloadOption) (*DownloadResult, error)
```

Upload options: `WithContentType`, `WithFileType`, `WithChecksum`, `WithETag`.
Download options: `WithDownloadChunkSize` (advisory server chunk size) and
`WithMaxDownloadBytes` (caps the reassembled payload, failing closed before the
buffer can grow past it). A full runnable program is in
[`examples/storage`](examples/storage/main.go).

## Consistency, Idempotency Replay, And Typed Errors

A mutation returns a `WriteReceipt`; a follow-up read can carry a read fence
built from it, or request an explicit consistency mode. A keyed replay-safe
mutation that the broker deduplicated reports `WasDuplicate` (see
[docs/native-services.md](../../docs/native-services.md) for the full contract).

```go
ent := udb.Entity("acme.billing.v1.Invoice", udbclient.EntityKey{"invoice_id"})

// Upsert surfaces the durable-idempotency replay flag on the result.
res, err := ent.Upsert(ctx, map[string]any{"invoice_id": "inv-1", "total_cents": 100})
if res.WasDuplicate { /* broker replayed a prior identical keyed write */ }

// Read-your-writes: fence the next read on the write's receipt.
receipt, _ := udbclient.ReceiptFromMutation(res.Response)
rc := &entityv1.RequestContext{TenantId: tenant, ProjectId: project}
udb.Metadata().AfterWrite(rc, receipt, 5000) // or package-level udbclient.AfterWrite

// Or pick an explicit consistency mode for an entity's reads.
rows, err := ent.WithConsistency(udbclient.ConsistencyReadYourWrites).
    Select(ctx, map[string]any{"invoice_id": "inv-1"})
```

Modes: `ConsistencyStrong`, `ConsistencyReadYourWrites`,
`ConsistencyBoundedStaleness`, `ConsistencyReplicaBounded`,
`ConsistencyEventual`, `ConsistencyProjectionOk`, `ConsistencyCacheOk`.

Retry contract: a replay-safe mutation is retried on transient errors **only
when the request carries a caller-supplied idempotency key** — keyless mutations
fail closed instead of risking a double apply. Typed error details decode from
the `udb-error-detail-bin` trailer via `Error.Detail()` in this package.

## Platform Services (Vault, Metering, Scheduler, …)

The control plane also ships Vault, Metering, Scheduler, Search, Webhook,
Workflow, Lock, LiveQuery, Config, Backup, and Embedding services. They have no
workflow facade yet — call them through the generated robustness layer:

```go
import vaultv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/vault/services/v1"

var reply vaultv1.EncryptResponse
err := udb.Generated.InvokeUnary(ctx,
    "/udb.core.vault.services.v1.VaultService/Encrypt",
    &vaultv1.EncryptRequest{TenantId: tenant, KeyName: "docs", Plaintext: "secret"},
    &reply)
```

Every RPC's full method path, retry class, and idempotency contract is listed in
[docs/generated/udb-native-contract.json](../../docs/generated/udb-native-contract.json).

## Notes For Users

Use the `udbclient` package for normal app code. The `gen/` packages contain the
protobuf request and response types you pass to UDB; you do not need to run
`buf` or `protoc` to install the SDK.

`udbclient.ProtocolVersion` is sent as `x-udb-client-catalog-version` so the
broker can reject incompatible clients early.

## Performance

The SDK uses a **single long-lived gRPC channel** — construct the `*grpc.ClientConn`
(or use `udbclient.NewUdb`) **once and reuse it** across every RPC.
Never open a new channel per call: a fresh channel forces a TCP+TLS+HTTP/2 handshake
on every request, which dominates per-RPC latency.

For the best behaviour, append the generated layer's dial options to your own
security options when you dial. They add:

- **Keepalive** (`Time: 30s`, `Timeout: 10s`, `PermitWithoutStream: true`) so an idle
  connection stays warm instead of dropping to IDLE and re-handshaking.
- **Jittered exponential-backoff retry** on `UNAVAILABLE` / `RESOURCE_EXHAUSTED`
  (4 attempts, 100ms base, full jitter, bounded by the call deadline), applied only to
  read-safe RPCs per the proto `operation_kind`.
- Metadata injection and typed error mapping.

```go
gen := udbclient.NewGenerated(nil, udbclient.Options{})
conn, err := grpc.NewClient(
    "localhost:50051",
    append(
        []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
        gen.DialOptions()...,
    )...,
)
```

`udbclient.NewUdb(ctx, cfg)` wires these dial options in for you automatically.
