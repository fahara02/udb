# UDB Go SDK

Thin wrapper over the generated UDB gRPC client. Centralises the
8 required metadata headers (`x-tenant-id`, `x-user-id`,
`x-purpose`, …) so application code doesn't hand-build them on
every call.

## Install

```bash
go get github.com/fahara02/udb/sdk/go@v0.3.0
```

Generated protobuf stubs are committed under `sdk/go/gen/`, so
`go get` works without `buf` installed on the consumer side.

## Usage

```go
import (
    entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
    "github.com/fahara02/udb/sdk/go/udbclient"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

conn, _ := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
client := udbclient.New(conn, udbclient.Metadata{
    TenantID:             "acme",
    Purpose:              "web.request",
    CorrelationID:        "corr-123",
    Scopes:               []string{"udb:read"},
    ServiceIdentity:      "billing.api",
    ProjectID:            "default",
    ClientCatalogVersion: "1.0.0",
})

resp, err := client.Select(ctx, &entityv1.SelectRequest{
    MessageType: "acme.billing.v1.Invoice",
    Limit:       50,
})
```

See the `examples/` directory for `select`, `upsert`, and
`publish_cdc` end-to-end programs.

## Generated package layout

`buf generate` (from the repo root) emits stubs into:

- `github.com/fahara02/udb/sdk/go/gen/udb/entity/v1`
- `github.com/fahara02/udb/sdk/go/gen/udb/events/v1`
- `github.com/fahara02/udb/sdk/go/gen/udb/services/v1`

The `go_package_prefix` in `buf.gen.yaml` controls this mapping;
don't override it in individual `.proto` files.

## Wire-protocol version

`udbclient.ProtocolVersion` mirrors `sdk/UDB_PROTOCOL_VERSION` —
the broker rejects requests whose `x-udb-client-catalog-version`
is outside the supported range.
