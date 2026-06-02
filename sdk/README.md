# UDB SDKs

This directory contains the generated stubs and thin language wrappers for the
UDB gRPC protocol.

Current protocol version: [`UDB_PROTOCOL_VERSION`](UDB_PROTOCOL_VERSION) =
`1.0.0`.

Current crate/SDK release from [`../versions.json`](../versions.json): `0.2.1`.

## Release Matrix

| SDK | Package | Version | Install | Guide |
|---|---|---:|---|---|
| Go | `github.com/fahara02/udb/sdk/go` | `0.2.1` | `go get github.com/fahara02/udb/sdk/go@v0.2.1` | [`go/README.md`](go/README.md) |
| Python | `udb-client` | `0.2.1` | `pip install udb-client==0.2.1` | [`python/README.md`](python/README.md) |
| TypeScript / Node | `@udb_plus/sdk` | `0.2.1` | `npm i @udb_plus/sdk@0.2.1` | [`typescript/README.md`](typescript/README.md) |
| PHP / Laravel | `fahara02/udb-laravel` | `0.2.1` | `composer require fahara02/udb-laravel:^0.2.1` | [`php/README.md`](php/README.md) |
| C# | `Udb.Client` | `0.2.1` | `dotnet add package Udb.Client --version 0.2.1` | [`csharp/README.md`](csharp/README.md) |
| Java | `dev.udb:udb-java-client` | `0.2.1-SNAPSHOT` today | build with `mvn -f sdk/java/pom.xml test` until Maven Central release wiring is complete | [`java/README.md`](java/README.md) |

Go, Python, TypeScript, and PHP are the published-first SDKs. C# and Java have
versioned manifests and committed stubs; their public package publishing is
tracked as release pipeline work.

## Protocol Sources

The generated clients come from:

- [`../proto/udb/entity/v1/types.proto`](../proto/udb/entity/v1/types.proto)
- [`../proto/udb/events/v1/udb_events.proto`](../proto/udb/events/v1/udb_events.proto)
- [`../proto/udb/services/v1/data_broker.proto`](../proto/udb/services/v1/data_broker.proto)
- [`../proto/udb/core/**`](../proto/udb/core)

Regenerate after proto changes:

```powershell
.\scripts\gen_sdk.ps1
```

```bash
./scripts/gen_sdk.sh
```

Consumers do not need generation tools for normal use; each SDK commits its
generated `gen/` surface, except the TypeScript runtime package, which loads the
bundled `.proto` files dynamically through `@grpc/proto-loader`.

## Required Metadata

Every client should send:

- `x-tenant-id`
- `x-user-id`
- `x-purpose`
- `x-correlation-id`
- `x-scopes`
- `x-service-identity`
- `x-udb-project-id`
- `x-udb-client-catalog-version`

The wrapper clients centralize these headers. Use raw generated stubs only when
you need an RPC that does not yet have a convenience method, and still attach the
same metadata.
