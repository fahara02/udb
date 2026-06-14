# UDB Go SDK

<!-- UDB_BRAND_HEADER_START -->
<p align="center">
  <img src="../../docs/assets/udb_logo.svg" alt="UDB logo" width="96">
</p>

<p align="center">
  <strong>UDB :: Universal Data Broker</strong><br>
  <sub>gRPC data plane | native control plane | tenant/project scope guard<br>crate v0.3.5 | protocol v1.0.0</sub>
</p>
<!-- UDB_BRAND_HEADER_END -->

The Go SDK gives Go services a small client for a running UDB broker. It attaches
UDB metadata for you, exposes DataBroker and auth helpers, and includes a
version-matched CLI launcher.

## Install

```bash
go get github.com/fahara02/udb/sdk/go@v0.3.5
```

Install the `udb` CLI launcher:

```bash
go install github.com/fahara02/udb/sdk/go/cmd/udb@v0.3.5
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
