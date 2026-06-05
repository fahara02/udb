# UDB Go SDK

The Go SDK gives Go services a small client for a running UDB broker. It attaches
UDB metadata for you, exposes DataBroker and auth helpers, and includes a
version-matched CLI launcher.

## Install

```bash
go get github.com/fahara02/udb/sdk/go@v0.3.1
```

Install the `udb` CLI launcher:

```bash
go install github.com/fahara02/udb/sdk/go/cmd/udb@v0.3.1
```

The launcher finds or downloads the matching UDB release binary, then forwards
all arguments to it.

## Export UDB Protos For Your App

From your application repo:

```bash
udb proto export
```

Now your app protos can import UDB annotations:

```proto
import "udb/core/common/v1/db.proto";
```

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
