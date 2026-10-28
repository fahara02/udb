# UDB Go SDK

<!-- UDB_BRAND_HEADER_START -->
<p align="center">
  <img src="../../docs/assets/udb_logo.svg" alt="UDB logo" width="96">
</p>

<p align="center">
  <strong>UDB :: Universal Data Broker</strong><br>
  <sub>gRPC data plane | native control plane | tenant/project scope guard<br>crate v0.6.4 | protocol v1.0.0</sub>
</p>
<!-- UDB_BRAND_HEADER_END -->

The Go SDK gives Go services a small client for a running UDB broker. It attaches
UDB metadata for you, exposes DataBroker and auth helpers, and includes a
version-matched CLI launcher.

## Install

```bash
go get github.com/fahara02/udb/sdk/go@v0.6.4
```

Supported Go versions: 1.22 (the `go.mod` floor) through 1.27. CI vets, builds
and tests the SDK on both ends of that range.

Install the `udb` CLI launcher:

```bash
go install github.com/fahara02/udb/sdk/go/cmd/udb@v0.6.4
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

## Timestamps: One Text Form

udb returns every TIMESTAMPTZ value in one text form: RFC 3339 in UTC with
exactly six fractional digits and a `Z` (`2026-10-15T08:20:00.800000Z`).
PostgreSQL stores microseconds, so six digits are exact, and a fixed width means
one instant always renders the same way.

Format with `udbclient.FormatTimestamp(t)` (layout `udbclient.TimestampLayout`)
whenever you compare a timestamp you wrote with the broker's echo or a re-read
as text. `time.RFC3339Nano` trims trailing zeros (`.8Z`, `.80724Z`) and will not
match. Comparing parsed `time.Time` values (`Equal`) is always safe.

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

Generated typed RPC client methods include the listener, required scopes,
credential types, operation kind and retry contract in their Go documentation.

`Table[T].Insert` creates a complete row and returns `CodeUnique` for any primary
or unique-key collision. `TxScope.Insert` gives the same behavior inside an
atomic transaction; a duplicate rolls back earlier writes and outbox events.
The dedicated Insert RPC returns `Unimplemented` on brokers without this API.

Inside a transaction, relational upserts, updates and deletes and vector upserts
accept a per-mutation idempotency key (a relational key gets a durable replay
receipt in the same transaction); any other mutation, such as an object or
outbox write, refuses one and nothing is written. Do not replay a partially
submitted transaction automatically.

## Durable Consumers

`udbclient.Consume` owns reconnects, credential refresh and a named,
principal-owned server cursor: the broker stores how far the consumer got
(`AckCdcEvents`) and resumes there after a restart, a reconnect or a credential
refresh. You no longer keep a cursor table, CAS-commit it, reconnect by hand and
handle expired credentials in the loop — the pattern where one loop that stopped
retrying left every consumer idle for hours without an error.

```go
err := udbclient.Consume[*myservice.OrderEvent](ctx, u, "order-worker", "orders.*",
    func(ctx context.Context, ev udbclient.ConsumedEvent[*myservice.OrderEvent]) error {
        return applyOrder(ctx, ev.Payload)
    })
```

By default (`AckBatchSize` 1) each event is acknowledged before the next one is
read, so a failed acknowledgement stops the stream before later events can
advance the cursor and a redelivery is acknowledged again without re-running its
handler. Raising `AckBatchSize` acknowledges a batch as a single watermark
(the newest id in it) for throughput; `AckInterval` flushes a partial batch on an
idle stream and `AckTimeout` bounds each settlement, including a shutdown flush.
Batching trades the per-event guarantee for throughput: events handled inside an
unflushed batch are read and handled before that watermark settles, so a refusal
or crash in between redelivers the whole batch. Delivery is at-least-once either
way — handlers must use event IDs for idempotency where needed.

Cancellation never settles an interrupted handler (it stays unacknowledged and
is redelivered), but it flushes handlers that already completed within
`AckTimeout` and preserves any flush error in the returned error.

After `MaxAttempts` handler failures (default 5) the broker atomically stores the
event in its ordinary DLQ and advances that consumer, so later events keep
flowing; a broker without `NackCdcEvents` refuses rather than discarding it.
Envelopes newer than this SDK understands stop the consumer rather than being
misread.

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

## Testing stores and policies

`udbtest.NewFake(descriptorSet, policyFile)` serves descriptor-defined tables
over loopback gRPC. Pass a `*descriptorpb.FileDescriptorSet` containing imports
and the same JSON/YAML policy file used by `udb policy apply`; close the fake
when the test ends. It denies data requests when no policy allows them.

Provision keys with `AddIdentityAPIKey(key, udbtest.Identity{...})` and connect
with `ClientForKey(t, key)`. Tenant, project, scopes and roles come from the
verified identity. Policy deny overrides allow, including denies whose
`required_scopes` the caller lacks. PII masking, codec coercion, CAS, primary
and secondary uniqueness, atomic writes, soft deletes and paging use the
descriptor rules. `New(t, messages...)` is the convenience fixture with an
explicit allow-all test policy.

`InjectFault` fails a bounded number of requests before writes apply. It also
targets `Authenticate` to exercise credential refresh failures. `SetRateLimit`
returns typed rate-limit errors, and `SetTokenLifetime` controls newly issued
bearers. Named CDC consumers retain verified ownership and monotonic cursors;
failed events use the ordinary DLQ operator APIs. Use the embedded tier for
SQL default expressions or partial unique-index predicates beyond `IS NULL`
and `IS NOT NULL`; the fake refuses unsupported schema behavior explicitly.

## Testing with the real embedded broker

`udbtest.Start(t, protoRoot, policyFile)` starts an installed embedded-capable UDB
binary with PostgreSQL, logs in as its bootstrapped administrator and returns an
authenticated `*udbclient.Udb`. All child processes and the client stop when the
test ends. Each call uses a temporary project and separate loopback ports.

```go
u := udbtest.Start(t, "./proto", "./policies.yaml")
files := udbclient.TableOf[*storagev1.File](u)
```

Use an empty proto root argument for native models only, and an empty policy
argument when no policy file is needed. A supplied policy file goes through the
production `udb policy apply` command. Set `UDB_TEST_BINARY` to select a CI build;
otherwise the helper uses `udb` on PATH. It shares PostgreSQL's normal download
cache; `UDB_HOME`, mirror and offline archive settings remain available. It
does not build a broker binary. The Linux and Windows embedded CI lane runs the
same table conformance suite used by `NewFake` and the external live broker.
`StartPair` provisions two real tenant identities on one isolated broker so
the suite includes cross-tenant isolation in all three modes.

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
