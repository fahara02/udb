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

Every non-health request should carry:

| Metadata | Meaning |
|---|---|
| `x-udb-service-identity` | Calling service identity |
| `x-udb-tenant-id` | Tenant boundary |
| `x-udb-project-id` | Project/catalog routing boundary |
| `x-udb-purpose` | Purpose for ABAC/audit |
| `x-udb-scopes` | Comma-separated scopes |
| `x-udb-client-catalog-version` | Client-side catalog expectation |

Header-scoped permissions should stay disabled in production unless the trust
boundary is explicit (`UDB_ALLOW_HEADER_SCOPES=false`).

## Client Packages

- PHP/Laravel SDK: [../sdk/php](../sdk/php)
- Go SDK/examples: [../sdk/go](../sdk/go)
- Python SDK: [../sdk/python](../sdk/python)
- TypeScript client: [../sdk/typescript](../sdk/typescript)

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
