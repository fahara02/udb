# Service And SDK Integration

Services should talk to UDB through the UDB-owned gRPC contract, not by reaching
behind the broker into backend-specific databases.

## Endpoint

The broker serves gRPC on `UDB_GRPC_ADDR` or the CLI positional serve address.
The local default is:

```text
127.0.0.1:50051
```

gRPC reflection is registered by the server, so standard reflection-aware tools
can inspect the compiled descriptor set.

## Required Request Metadata

Every non-health request should carry these headers (exact names — see
`context_from_metadata` in `../src/runtime/service/mod.rs`):

| Metadata | Meaning |
|---|---|
| `x-tenant-id` | Tenant boundary |
| `x-user-id` | Subject / user id |
| `x-purpose` | Purpose for ABAC/audit |
| `x-correlation-id` | Request correlation id |
| `x-scopes` | Comma-separated scopes |
| `x-service-identity` | Calling service identity |
| `x-udb-project-id` | Project/catalog routing boundary |
| `x-udb-client-catalog-version` | Client-side catalog expectation |

All wrapper SDKs attach these for you from their `Metadata`/`UdbMetadata` object.
Header-scoped permissions should stay disabled in production unless the trust
boundary is explicit (`UDB_ALLOW_HEADER_SCOPES=false`).

The native control-plane services (Authn/Authz/ApiKey/Tenant/Notification/
Analytics) are served on a **separate internal listener** (`UDB_AUTH_GRPC_ADDR`),
not the `DataBroker` endpoint above — see [native-services.md](native-services.md).

## Client Packages

- Go SDK: [../sdk/go](../sdk/go)
- Python SDK: [../sdk/python](../sdk/python)
- TypeScript client: [../sdk/typescript](../sdk/typescript)
- Java SDK: [../sdk/java](../sdk/java)
- C# SDK: [../sdk/csharp](../sdk/csharp)
- PHP/Laravel SDK: [../sdk/php](../sdk/php)

Per-language quickstarts: [root README](../README.md#-quickstart-per-language).

Common client env:

```env
UDB_ENDPOINT=udb.internal:50051
UDB_DEADLINE_MS=30000
UDB_SERVICE_IDENTITY=my.service
UDB_PROJECT_ID=my-project
UDB_DEFAULT_PURPOSE=service.request
UDB_DEFAULT_SCOPES=udb:read,udb:write
```

## Local Development

Use the playground when you need backing services:

```powershell
cargo run --bin udb-proto-parser -- dev up
cargo run --bin udb-proto-parser -- dev smoke
cargo run --bin udb-proto-parser -- dev down
```

Use `cargo run --bin udb-proto-parser -- init` to emit a small project scaffold
with proto, compose, and client examples.
