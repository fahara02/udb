# Universal Data Broker (UDB)

[![Rust 2024](https://img.shields.io/badge/Rust-2024-b7410e?logo=rust)](Cargo.toml)
[![gRPC + Protobuf](https://img.shields.io/badge/API-gRPC%20%2B%20Protobuf-244c5a)](proto/README.md)
[![Protocol 1.0.0](https://img.shields.io/badge/protocol-1.0.0-2f855a)](sdk/UDB_PROTOCOL_VERSION)
[![Backends](https://img.shields.io/badge/backend%20kinds-18-4c51bf)](#backend-model)

UDB is a Rust implementation of a proto-driven data broker. It reads project
owned `.proto` schemas, extracts storage annotations, builds a catalog manifest,
generates migration/bootstrap artifacts, and can serve those schemas through a
neutral gRPC `DataBroker` API.

The important idea is this:

```text
project .proto files
        |
        v
parser -> ProtoSchema AST -> CatalogManifest/checksum
        |                     |
        |                     +-> lint, drift, migration plans, SQL/artifacts
        |
        v
DataBroker runtime -> auth/ABAC -> channel admission -> IR -> backend executor
```

This repo is not only a parser and not only a gRPC server. It is a crate,
binary, runtime, protocol module, SDK workspace, backend plugin inventory,
operation IR, migration engine, and a set of operational runbooks. It also has
visible architectural history: early UDB was Postgres anchored, and the current
codebase is in a peer-to-peer transition where canonical stores are explicit
traits instead of implicit `PgPool` access.

## What This Project Is

UDB tries to solve a specific problem: many services want to read and write
business data, vectors, blobs, cache entries, CDC events, and admin/catalog
state, but every service talking directly to every database creates drift in
authorization, migrations, tenant isolation, observability, and retry behavior.

UDB centralizes those concerns:

- Project schemas stay in normal project-owned proto packages.
- UDB annotations describe relational tables, object fields, vector stores,
  caches, document stores, graph stores, time-series/column stores, and security.
- The broker exposes one UDB-owned gRPC contract under `proto/udb/...`.
- Runtime requests carry tenant, purpose, scopes, service identity, project id,
  and catalog version metadata.
- Backends are reached through a neutral logical IR and feature-gated plugin
  modules instead of service code hand-writing each database dialect.

## Honest Status

The maintained docs now live under [`docs/README.md`](docs/README.md). The old
older notes and duplicate runbooks were consolidated so current status is easier
to verify. The source of truth for backend inventory is:

- [`src/backend/mod.rs`](src/backend/mod.rs): `BackendKind`, tier, role,
  capability matrix, operation support.
- [`src/backend/plugins/mod.rs`](src/backend/plugins/mod.rs): compiled plugin
  inventory.
- [`src/runtime/executors/`](src/runtime/executors): runtime executor modules.
- [`src/ir/compile/`](src/ir/compile): backend-specific IR compilers.
- [`Cargo.toml`](Cargo.toml): default and optional feature graph.

The current code recognizes 18 backend kinds and the default feature set enables
their plugin modules. A slim build can compile only the core pieces, for example
`--no-default-features --features postgres`.

Also note that the Docker files still show some historical path assumptions in
places. The Rust crate and CLI are the most reliable entry points while the repo
split/packaging work settles.

## Codebase Map

Approximate source shape at the time this README was written:

| Area | Files | Purpose |
|---|---:|---|
| [`src/runtime`](src/runtime) | 102 | Broker orchestration, service handlers, backend clients, CDC, catalog, system stores, security, metrics |
| [`src/ir`](src/ir) | 28 | Neutral logical operations and backend compilers |
| [`src/generation`](src/generation) | 17 | Manifest, SQL, DSN, drift, lint, and backend artifact generation |
| [`src/migration`](src/migration) | 7 | Diffing, plans, audited apply, phase runner, db_ops sync |
| [`src/control`](src/control) | 11 | Startup lifecycle, FSM, hooks, notifications, approval workflow |
| [`src/parser`](src/parser) | 10 | Hand-written proto lexer/parser and annotation extraction |
| [`src/backend`](src/backend) | 21 | Backend identity, capabilities, plugin contract, plugin inventory |
| [`src/cli`](src/cli) | 7 | `udb-proto-parser` command implementation |
| [`src/planning`](src/planning) | 4 | Request planning helpers for broker operations |
| [`src/schema`](src/schema) | 3 | Proto AST structs and deterministic checksums |
| [`crates/udb-portable`](crates/udb-portable) | 2 | WASM/edge-safe parser/checksum/schema-cache subset |

The public crate surface is collected in [`src/lib.rs`](src/lib.rs). The binary
entry point is tiny by design: [`src/main.rs`](src/main.rs) calls the CLI module.

## Request Flow

For a normal gRPC call:

1. `DataBrokerService` receives the RPC in [`src/runtime/service/mod.rs`](src/runtime/service/mod.rs).
2. The handler extracts metadata into a `SecurityContext` and request context.
3. `ensure_ready()` checks the startup lifecycle FSM has reached `Completed`.
4. Catalog compatibility is checked against `x-udb-client-catalog-version`.
5. ABAC policies evaluate service identity, tenant, purpose, operation, scopes,
   and message type.
6. A channel permit is acquired through [`src/runtime/channels.rs`](src/runtime/channels.rs);
   this is where per-operation limits, fairness, and backpressure live.
7. The request is planned or lowered to neutral IR.
8. A backend target is resolved from project routing, target backend/instance,
   circuit breaker state, and plugin registry.
9. The backend executor runs the operation.
10. Responses include catalog/consistency headers; mutations also include a
    write receipt when possible.
11. Metrics, audit, CDC, projection, saga, or DLQ paths record side effects as
    configured.

The gRPC protocol currently defines 60 RPCs in
[`proto/udb/services/v1/data_broker.proto`](proto/udb/services/v1/data_broker.proto).
They cover data, vector, object, transaction, CDC, resource admin, catalog,
migration, DLQ, saga, policy, project, health, and admin/audit surfaces.

## Main Concepts

### Project Protos

Project/application protos are schema input. They do not need to import or
define the UDB `DataBroker` service. UDB parses annotations by suffix, so an
annotation may be canonical like `(udb.table)` or project-qualified like
`(acme.billing.v1.table)`.

The parser supports:

- table and column projections
- primary keys, indexes, foreign keys, checks
- RLS and tenant columns
- vector/cache/object/document/graph/time-series/column/model-registry stores
- proto3 reserved field names and ranges for drift safety
- language options propagated into the manifest
- annotation modes: compat, warn, strict

Key files:

- [`src/parser/mod.rs`](src/parser/mod.rs)
- [`src/parser/options.rs`](src/parser/options.rs)
- [`src/parser/db_parser.rs`](src/parser/db_parser.rs)
- [`src/schema/ast.rs`](src/schema/ast.rs)
- [`docs/annotations.md`](docs/annotations.md)

### Catalog Manifest

The catalog manifest is the broker's normalized view of parsed schemas. It is
where proto messages become tables, columns, stores, projections, security
metadata, language class names, checksums, warnings, and validation errors.

Key files:

- [`src/generation/manifest/mod.rs`](src/generation/manifest/mod.rs)
- [`src/generation/lint.rs`](src/generation/lint.rs)
- [`src/generation/sql/`](src/generation/sql)
- [`src/migration/diff.rs`](src/migration/diff.rs)

### Neutral IR

Data-plane operations lower into backend-neutral structs before compiler modules
turn them into SQL, JSON HTTP payloads, key/value operations, object operations,
or CQL/Cypher/etc.

The main IR operations are:

- `LogicalRead`
- `LogicalWrite`
- `LogicalDelete`
- `LogicalSearch`
- `LogicalAggregate`
- `LogicalResourceOp`

Key files:

- [`src/ir/operations.rs`](src/ir/operations.rs)
- [`src/ir/filter.rs`](src/ir/filter.rs)
- [`src/ir/compile/`](src/ir/compile)
- [`src/ir/cross_backend_tests.rs`](src/ir/cross_backend_tests.rs)

### Backend Model

UDB separates backend identity from runtime availability:

- `BackendKind` is the known backend enum.
- `BackendTier` groups SQL/cache/vector/object/document/graph/column stores.
- `BackendRole` says whether a backend can be canonical, projection-only, or
  both.
- `BackendCapability` declares operation and consistency properties.
- `Backend` plugin structs register backend-specific setup, generation, and
  conformance contracts.

Backend kinds currently present in code:

| Tier | Backends |
|---|---|
| SQL / relational | Postgres, MySQL, SQLite, SQL Server, ClickHouse |
| Cache | Redis, Memcached |
| Vector | Qdrant, Weaviate, Pinecone |
| Object | MinIO, S3, Azure Blob, Google Cloud Storage |
| Document/search/graph/column | MongoDB, Elasticsearch, Neo4j, Cassandra/ScyllaDB |

Core backend defaults from the old spec remain Postgres, Redis, Qdrant, and
MinIO. The newer plugin inventory is broader.

### Canonical Stores

This is the most important architectural transition in the repo.

Older UDB paths assumed Postgres was the canonical store for system tables,
CDC, saga state, projection task state, migration audit, and consistency fences.
The newer peer-to-peer work introduces:

- `CanonicalStore`
- `DurabilityToken`
- `SystemStores`
- `CanonicalStoreRegistry`
- Postgres, MySQL, and SQLite implementations for system-store traits

Key files:

- [`src/runtime/canonical_store/mod.rs`](src/runtime/canonical_store/mod.rs)
- [`src/runtime/canonical_store/system_store.rs`](src/runtime/canonical_store/system_store.rs)
- [`src/runtime/canonical_store/postgres.rs`](src/runtime/canonical_store/postgres.rs)
- [`src/runtime/canonical_store/mysql.rs`](src/runtime/canonical_store/mysql.rs)
- [`src/runtime/canonical_store/sqlite.rs`](src/runtime/canonical_store/sqlite.rs)
- [`docs/architecture.md`](docs/architecture.md)

Do not read "universal DB layer" as "every backend has identical semantics."
The code tries to be explicit about what compiles, what is unsupported, and what
is eventually consistent or projection-only.

### Runtime System Tables

UDB owns internal catalog/system tables for:

- catalog versions and activation logs
- project catalog bindings
- migration runs and operation ledgers
- CDC event journal, offsets, lock log, control table, topic policy, DLQ
- saga coordinator
- projection tasks
- ABAC policies
- admin audit log

Preview the DDL:

```powershell
cargo run --bin udb-proto-parser -- system-ddl
```

Related files:

- [`src/runtime/system.rs`](src/runtime/system.rs)
- [`src/control/lifecycle.rs`](src/control/lifecycle.rs)
- [`src/runtime/core/catalog_sql.rs`](src/runtime/core/catalog_sql.rs)
- [`src/runtime/core/catalog_admin.rs`](src/runtime/core/catalog_admin.rs)

## Repository Layout

| Path | What lives there |
|---|---|
| [`src/lib.rs`](src/lib.rs) | Public library surface and compatibility re-exports |
| [`src/main.rs`](src/main.rs) | Binary entry point |
| [`src/cli`](src/cli) | CLI parsing and command handlers |
| [`src/parser`](src/parser) | Proto lexer/parser and annotation extraction |
| [`src/schema`](src/schema) | AST and checksum types |
| [`src/generation`](src/generation) | Manifest/SQL/DSN/drift/lint generation |
| [`src/ir`](src/ir) | Backend-neutral operation model and compilers |
| [`src/backend`](src/backend) | Backend inventory, plugin trait, capability matrix |
| [`src/runtime`](src/runtime) | Broker runtime, service handlers, backend executors, CDC, security, metrics |
| [`src/migration`](src/migration) | Migration diff/apply/sync/phase-runner |
| [`src/control`](src/control) | Startup lifecycle, FSM, approval, hooks, notifications |
| [`proto`](proto/README.md) | UDB-owned gRPC/protobuf contract |
| [`sdk`](sdk/README.md) | Generated/wrapped clients |
| [`examples`](examples) | Arbitrary project, multi-project, and toy plugin examples |
| [`configs`](configs) | YAML config examples |
| [`docs`](docs) | Operational docs, security, upgrade history, runbooks |
| [`crates/udb-portable`](crates/udb-portable) | WASM/edge parser/checksum/schema-cache subset |

## Quick Start For Developers

The fastest meaningful flow is to use the arbitrary project example, because
the UDB-owned protocol protos are service definitions, not domain schemas.

```powershell
cargo test --lib
```

```powershell
cargo run --bin udb-proto-parser -- lint examples/arbitrary_project/proto --human
cargo run --bin udb-proto-parser -- catalog examples/arbitrary_project/proto
cargo run --bin udb-proto-parser -- sql examples/arbitrary_project/proto
cargo run --bin udb-proto-parser -- plan examples/arbitrary_project/proto
```

Run a Postgres-backed broker locally:

```powershell
Copy-Item .env.example .env.local
$env:UDB_PG_DSN = "postgresql://udb:udb@localhost:5432/udb?sslmode=prefer"
$env:UDB_ABAC_DEFAULT_ALLOW = "true"
cargo run --bin udb-proto-parser -- serve examples/arbitrary_project/proto "" 0.0.0.0:50051
```

Run local readiness checks:

```powershell
cargo run --bin udb-proto-parser -- doctor --human
cargo run --bin udb-proto-parser -- doctor --probe --human
```

## CLI

The binary is `udb-proto-parser`. Its name is older than its current scope; it
now drives parsing, generation, runtime serving, migration/admin checks, and the
local playground.

Schema and planning:

```powershell
cargo run --bin udb-proto-parser -- catalog <proto-root> [namespace]
cargo run --bin udb-proto-parser -- dsn <proto-root>
cargo run --bin udb-proto-parser -- sql <proto-root>
cargo run --bin udb-proto-parser -- plan <proto-root>
cargo run --bin udb-proto-parser -- lint <proto-root> --human
cargo run --bin udb-proto-parser -- drift <proto-root> --prior old_manifest.json
cargo run --bin udb-proto-parser -- explain <proto-root>
cargo run --bin udb-proto-parser -- manifest-export <proto-root>
cargo run --bin udb-proto-parser -- field-mask-preview <proto-root>
```

Runtime/admin:

```powershell
cargo run --bin udb-proto-parser -- serve <proto-root> "" 0.0.0.0:50051
cargo run --bin udb-proto-parser -- doctor --probe --human
cargo run --bin udb-proto-parser -- health-check
cargo run --bin udb-proto-parser -- system-ddl
cargo run --bin udb-proto-parser -- tracker-ddl
cargo run --bin udb-proto-parser -- admin dry-run <proto-root>
cargo run --bin udb-proto-parser -- admin force-sync <proto-root>
cargo run --bin udb-proto-parser -- admin verify-audit --limit 250
cargo run --bin udb-proto-parser -- admin release-lock
```

Policy and compatibility:

```powershell
$env:UDB_ABAC_POLICY_FILE = "docs/abac_seed.json"
cargo run --bin udb-proto-parser -- policy-lint
cargo run --bin udb-proto-parser -- policy-seed
cargo run --bin udb-proto-parser -- compat-matrix
cargo run --bin udb-proto-parser -- config-skeleton
```

Playground wrapper:

```powershell
cargo run --bin udb-proto-parser -- dev up
cargo run --bin udb-proto-parser -- dev status
cargo run --bin udb-proto-parser -- dev logs udb
cargo run --bin udb-proto-parser -- dev smoke
cargo run --bin udb-proto-parser -- dev down
```

## Configuration

Configuration is loaded as defaults plus optional file plus environment overlay.
The standard config path is `UDB_CONFIG_PATH`; the complete operator template is
[`.env.example`](.env.example). Env files are loaded in this order:

1. OS environment
2. `.env.<APP_ENV>`
3. `.env.local`
4. `.env.prod`
5. `.env`

Minimum required env for a normal Postgres-backed broker:

| Variable | Meaning |
|---|---|
| `APP_ENV` | Selects `.env.<APP_ENV>` and labels the runtime environment |
| `UDB_ENV` | Security-mode switch; `production`/`prod` enables stricter defaults |
| `UDB_APP_NAME` | Broker/application identity |
| `UDB_PG_INSTANCES` | Named Postgres instances, usually `primary` |
| `UDB_PG_DSN_PRIMARY` | DSN for the named `primary` instance |
| `UDB_PG_DSN` or `DATABASE_URL` | Canonical primary Postgres DSN |
| `UDB_2PC_ENABLED` | Enables real Postgres prepared-transaction 2PC when `true` |

Common optional env variables:

| Variable | Meaning |
|---|---|
| `UDB_CONFIG_PATH` | YAML/JSON/TOML runtime config path |
| `UDB_BACKEND_INSTANCES` | Named backend instance descriptor list |
| `UDB_REDIS_DSN` | Redis cache/rate-limit/idempotency |
| `UDB_QDRANT_URL` | Qdrant vector backend |
| `UDB_MINIO_ENDPOINT`, `UDB_MINIO_ACCESS_KEY`, `UDB_MINIO_SECRET_KEY` | MinIO/S3-compatible object storage |
| `UDB_NOSQL_DSN`, `UDB_NOSQL_API_URL` | MongoDB/Atlas Data API backend |
| `UDB_GRAPH_DSN`, `UDB_GRAPH_HTTP_URL` | Neo4j graph backend |
| `UDB_COLUMN_DSN`, `UDB_COLUMN_HTTP_URL` | ClickHouse column backend |
| `UDB_KAFKA_BROKERS` | Kafka brokers for CDC |
| `UDB_ABAC_DEFAULT_ALLOW` | Development-only relaxed authorization |
| `UDB_ALLOW_DEGRADED_BACKENDS` | Allow startup with optional backend failures |
| `UDB_METRICS_ADDR` | Prometheus scrape address, default `0.0.0.0:50052` |
| `UDB_GRPC_ADDR` | Default serve address when not supplied positionally |
| `UDB_TLS_*`, `UDB_MTLS_*` | Server TLS and client CA config |

See:

- [`.env.example`](.env.example)
- [`configs/database.yaml`](configs/database.yaml)
- [`configs/backends.yaml`](configs/backends.yaml)
- [`configs/services.yaml`](configs/services.yaml)
- [`src/runtime/config/mod.rs`](src/runtime/config/mod.rs)

## Security Model

UDB authorization is request-context based. Every non-health request should carry:

- `x-tenant-id`
- `x-user-id`
- `x-purpose`
- `x-correlation-id`
- `x-scopes`
- `x-service-identity`
- `x-udb-project-id`
- `x-udb-client-catalog-version`

The runtime supports:

- JWT service identity
- mTLS service identity
- dev-only header fallback
- ABAC policy evaluation
- PII masking
- field-level encryption
- tenant-aware request context injection
- audit logging
- admin audit hash-chain verification
- topic-policy enforcement for CDC

Start here:

- [`src/runtime/security.rs`](src/runtime/security.rs)
- [`src/runtime/service/mod.rs`](src/runtime/service/mod.rs)
- [`docs/security.md`](docs/security.md)
- [`docs/integration.md`](docs/integration.md)

## Protocol And SDKs

The UDB-owned broker contract is:

- [`proto/udb/entity/v1/types.proto`](proto/udb/entity/v1/types.proto)
- [`proto/udb/events/v1/udb_events.proto`](proto/udb/events/v1/udb_events.proto)
- [`proto/udb/services/v1/data_broker.proto`](proto/udb/services/v1/data_broker.proto)

The build script compiles those with `tonic-build` and writes a generated
`protocol.rs` include under Cargo's `OUT_DIR`.

Generate SDKs:

```powershell
.\scripts\gen_sdk.ps1
```

```bash
./scripts/gen_sdk.sh
```

SDK folders:

| SDK | Path |
|---|---|
| Go | [`sdk/go`](sdk/go/README.md) |
| Python | [`sdk/python`](sdk/python/README.md) |
| TypeScript | [`sdk/typescript`](sdk/typescript/README.md) |
| C# | [`sdk/csharp`](sdk/csharp/README.md) |
| Java | [`sdk/java`](sdk/java/README.md) |
| PHP / Laravel | [`sdk/php`](sdk/php/README.md) |

Protocol version: [`sdk/UDB_PROTOCOL_VERSION`](sdk/UDB_PROTOCOL_VERSION).

## Testing

Fast local tests:

```powershell
cargo test --lib
```

Backend feature sweeps:

```powershell
cargo test --all-features --lib
cargo test --no-default-features --features postgres --lib
cargo test --features clickhouse,mssql,cassandra --lib
```

Proto contract:

```powershell
buf lint
buf build
buf generate
```

Integration tests are opt-in:

```powershell
docker compose -f docker-compose.integration.yml up -d --wait
$env:UDB_INTEGRATION_TESTS = "1"
cargo test --test integration_tests -- --nocapture
docker compose -f docker-compose.integration.yml down -v --remove-orphans
```

The full default Rust suite is meant to run without external services. Live
Docker/infrastructure tests are guarded by env variables or `#[ignore]`.

See:

- [`TESTING.md`](TESTING.md)
- [`docs/testing.md`](docs/testing.md)
- [`tests/integration_tests.rs`](tests/integration_tests.rs)
- [`tests/parser_tests.rs`](tests/parser_tests.rs)

## Load, Soak, And Operations

Load profiles are scripted through `ghz`:

```powershell
$env:UDB_HOST = "localhost:50051"
$env:CONCURRENCY = "50"
$env:TOTAL_REQUESTS = "10000"
$env:PROFILE = "read-heavy"
.\scripts\load_test.ps1
```

Profiles include:

- `read-heavy`
- `write-heavy`
- `mixed-projection`
- `tenant-noisy-neighbor`
- `backend-outage`
- `reload-during-traffic`
- `multi-project-smoke`

Operational docs:

| Topic | Document |
|---|---|
| Docs index | [`docs/README.md`](docs/README.md) |
| Architecture and backend inventory | [`docs/architecture.md`](docs/architecture.md) |
| Operations, topology, reload, backup, and load profiles | [`docs/operations.md`](docs/operations.md) |
| Security, audit, encryption, and supply chain | [`docs/security.md`](docs/security.md) |
| Testing and live acceptance | [`docs/testing.md`](docs/testing.md) |

## Examples

| Example | What to look at |
|---|---|
| [`examples/arbitrary_project`](examples/arbitrary_project/README.md) | A project namespace that UDB does not own; shows table, cache, vector, object, PII, encryption |
| [`examples/multi_project`](examples/multi_project/README.md) | One broker serving unrelated projects with separate proto roots/catalogs |
| [`examples/toy_backend_plugin`](examples/toy_backend_plugin/README.md) | Minimal external backend plugin contract |

## Portable Crate

[`crates/udb-portable`](crates/udb-portable) is the browser/edge-safe subset.
It path-includes the same AST, checksum, lexer, and parser source files used by
the main crate. It deliberately excludes `tokio`, `sqlx`, `tonic`, cloud SDKs,
Kafka, Redis, and filesystem directory parsing.

Use it when a client or edge worker needs to parse proto source, compute the
same schema checksum as the server, or track catalog/schema compatibility
without embedding the whole broker.

## Kubernetes

[`deploy/kubernetes`](deploy/kubernetes/README.md) contains CRD contracts for:

- `UdbBroker`
- `UdbProjectCatalog`
- `UdbBackendInstance`
- `UdbMigrationRun`
- `UdbCdcStream`
- `UdbProjectionWorker`

Apply contracts:

```bash
kubectl apply -f deploy/kubernetes/crds/udb.io_crds.yaml
```

These are controller-neutral contracts. The repo contains CRDs, not a complete
operator implementation.

## Supply Chain

The intended gate is:

```powershell
cargo deny check advisories bans licenses sources
```

The policy denies unknown registries, git dependencies, and undocumented source
exceptions. See [`docs/security.md`](docs/security.md).

## Known Rough Edges

- Some newer backend plugins are still plugin-owned rather than fully covered by
  one universal connection lifecycle.
- Disabled-feature reporting should be aligned for every backend plugin.
- Some Docker/package paths still reflect older monorepo layouts.
- The default build intentionally pulls many backend SDKs; use slim feature
  builds to check dependency hygiene.
- Several live acceptance gates in the docs require real infrastructure and are
  not satisfied by code-only tests.
- The crate currently warns on unused/dead code during build; the warnings are
  tracked by the refactor history and are not treated as fatal yet.

## Where To Start When Changing Code

| Task | Start here |
|---|---|
| Add or change proto annotation parsing | [`src/parser/options.rs`](src/parser/options.rs), [`src/parser/db_parser.rs`](src/parser/db_parser.rs), [`src/schema/ast.rs`](src/schema/ast.rs) |
| Add a backend operation | [`src/ir/operations.rs`](src/ir/operations.rs), [`src/ir/compile`](src/ir/compile), [`src/runtime/executors`](src/runtime/executors) |
| Add a backend plugin | [`src/backend/plugin.rs`](src/backend/plugin.rs), [`src/backend/plugins`](src/backend/plugins), [`examples/toy_backend_plugin`](examples/toy_backend_plugin/README.md) |
| Change gRPC behavior | [`proto/udb/services/v1/data_broker.proto`](proto/udb/services/v1/data_broker.proto), [`src/runtime/service`](src/runtime/service) |
| Change auth or metadata | [`src/runtime/security.rs`](src/runtime/security.rs), [`src/runtime/service/mod.rs`](src/runtime/service/mod.rs), [`src/embedded.rs`](src/embedded.rs) |
| Change catalog/migration behavior | [`src/generation/manifest`](src/generation/manifest), [`src/migration`](src/migration), [`src/control/lifecycle.rs`](src/control/lifecycle.rs) |
| Change system-store behavior | [`src/runtime/canonical_store`](src/runtime/canonical_store), [`src/runtime/system.rs`](src/runtime/system.rs) |
| Change config loading | [`src/runtime/config`](src/runtime/config), [`src/cli/env_setup.rs`](src/cli/env_setup.rs), [`build.rs`](build.rs) |
