# UDB Generated SDKs

This folder contains language-specific wrappers and examples for generated UDB gRPC clients.

Current wrapper coverage:

- Go
- Python
- TypeScript
- C#
- Java
- **PHP / Laravel** — full Laravel SDK with ServiceProvider, Facade, ContextMiddleware, typed exceptions, and Pest tests. See [`php/README.md`](php/README.md). Installable via `composer require fahara02/udb-laravel`.

Canonical protos live at:

- `proto/udb/entity/v1/types.proto`
- `proto/udb/events/v1/udb_events.proto`
- `proto/udb/services/v1/data_broker.proto`

Generate all clients from `udb`:

```powershell
./scripts/gen_sdk.ps1
```

```bash
./scripts/gen_sdk.sh
```

Every client should send these metadata headers:

- `x-tenant-id`
- `x-user-id`
- `x-purpose`
- `x-correlation-id`
- `x-scopes`
- `x-service-identity`
- `x-udb-project-id`
- `x-udb-client-catalog-version`

The current UDB wire protocol version is stored in `UDB_PROTOCOL_VERSION`.
