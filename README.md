<h1 align="center">🛰️ Universal Data Broker (UDB)</h1>

<p align="center"><em>One proto-driven gRPC control point in front of 18 databases — with built-in auth, multi-tenant RLS, migrations, CDC, and SDKs for 6 languages.</em></p>

<p align="center">
<a href="Cargo.toml"><img alt="Rust 2024" src="https://img.shields.io/badge/Rust-2024-b7410e?logo=rust&logoColor=white"></a>
<a href="proto/README.md"><img alt="gRPC + Protobuf" src="https://img.shields.io/badge/API-gRPC%20%2B%20Protobuf-2563eb?logo=grpc&logoColor=white"></a>
<a href="sdk/UDB_PROTOCOL_VERSION"><img alt="Protocol 1.0.0" src="https://img.shields.io/badge/protocol-1.0.0-059669"></a>
<a href="#-backend-matrix"><img alt="18 backend kinds" src="https://img.shields.io/badge/backends-18-7c3aed"></a>
<a href="#-supported-features"><img alt="76 DataBroker RPCs" src="https://img.shields.io/badge/DataBroker-76%20RPCs-0f766e"></a>
<a href="#native-control-plane"><img alt="77 native control-plane RPCs" src="https://img.shields.io/badge/control%20plane-77%20RPCs-c2410c"></a>
<a href="#-quickstart-per-language"><img alt="SDKs for six languages" src="https://img.shields.io/badge/SDKs-Go%20%C2%B7%20Python%20%C2%B7%20TS%20%C2%B7%20Java%20%C2%B7%20C%23%20%C2%B7%20PHP-334155"></a>
<a href="LICENSE"><img alt="License MIT" src="https://img.shields.io/badge/license-MIT-555"></a>
</p>

UDB is a data broker for teams that already think in schemas.

You describe your domain in normal `.proto` files, add UDB annotations for where
the data should live, and run one broker in front of your databases, object
stores, caches, vector stores, and graph/document systems. Your application calls
one gRPC API. UDB handles routing, tenant context, authorization, migrations,
CDC, and backend-specific details.

<p align="center">
  <img src="docs/assets/architecture-pipeline.svg" alt="UDB architecture: project protos are parsed into a catalog manifest that drives build-time generation and the per-request DataBroker runtime pipeline (authn/authz → admission → neutral IR → executor → 18 backends)." width="900">
</p>

## Why UDB Exists

Most growing products end up with the same shape of problem:

- user data in Postgres or MySQL;
- search in Elasticsearch;
- embeddings in Qdrant, Weaviate, or Pinecone;
- files in S3, MinIO, Azure Blob, or GCS;
- cache/session data in Redis;
- analytics in ClickHouse;
- project-specific auth, tenant, audit, and migration rules scattered through
  every service.

That works for a while. Then every service has to remember the same security
headers, retry rules, schema version, RLS policy, object naming convention,
audit trail, and backend quirks.

UDB puts that contract in one place. Your app sends "select invoices", "upsert
this customer", "store this object", "search these vectors", or "can this user
read this resource?" The broker checks the request, resolves the active catalog,
talks to the right backend, and returns a typed response.

## How It Feels

Your project owns the domain model:

```proto
syntax = "proto3";
package acme.billing.v1;

import "udb/core/common/v1/db.proto";

message Invoice {
  option (udb.core.common.v1.pg_table) = {
    table: "invoices"
    schema: "billing"
  };

  string invoice_id = 1 [(udb.core.common.v1.pg_column) = {
    primary_key: true
    sql_type: "text"
  }];

  string customer_id = 2;
  int64 total_cents = 3;
}
```

Then application code uses the SDK for its language:

```python
from udb_client import Metadata, UdbClient, decode_records

meta = Metadata(
    tenant_id="acme",
    user_id="user-1",
    purpose="billing.api",
    scopes=("udb:read", "udb:write"),
    service_identity="billing-service",
    project_id="billing",
)

with UdbClient("127.0.0.1:50051", meta) as udb:
    rows = udb.select(message_type="acme.billing.v1.Invoice", limit=50)
    print(decode_records(rows))
```

The same model can route relational data, objects, vectors, cache entries, graph
edges, document records, and analytics operations without each service learning
every backend dialect.

## ⚡ Supported Features

Data plane (`DataBroker`, 76 RPCs):

- **Relational** CRUD + batch (`Select`/`BatchSelect`/`Upsert`/`BatchUpsert`/`Delete`).
- **Vector** search / hybrid search / upsert (Qdrant, Weaviate, Pinecone, Elasticsearch knn).
- **Object/blob** put/get, presigned URLs, multipart (S3, MinIO, Azure Blob, GCS).
- **Cache** get/set/delete/scan (Redis, Memcached).
- **Document / graph / time-series / analytical** ops (MongoDB, Neo4j, ClickHouse, Cassandra).
- **Transactions**: per-request transactionality, real Postgres **2PC** and MySQL **XA** (`UDB_2PC_ENABLED`), **sagas** with recovery/compensation.
- **CDC → Kafka** via a transactional outbox relay, with DLQ, topic policy, and a CDC control plane.
- **Catalog & migrations**: staged/activate/rollback catalogs, proto-driven migration plan/apply with an audited op ledger.
- **Projections / materialized views**, **per-tenant RLS**, **field-level encryption** (AES-256-GCM-SIV), rate limiting / fair channels / backpressure, Prometheus metrics.

Control plane (`proto/udb/core/**`, isolated listener — see [Native Control Plane](#native-control-plane)):

- **Authn**: native JWT validation (JWKS/`kid`), **UDB-issued RS256 JWT signing + refresh tokens**, **Argon2id** passwords, **RFC 6238 TOTP MFA**, server-side sessions, **CSRF**, OTP, full user admin, mTLS + hybrid external identity (OIDC/Better Auth bridge).
- **Authz**: RBAC + ABAC + simple ReBAC over a Casbin enforcer, role/policy/relationship CRUD, audit decisions, **`GetNativeAccess`** (restricted role + scoped DSN + RLS session vars), **signed policy bundles** for offline SDK caches.
- **ApiKey**: hashed keys, scopes, rotation, revocation, usage stats.
- **Tenant / Notification / Analytics**: tenant + config management, notification logs/templates/preferences/delivery-stats (with Kafka emit), and pipeline/executor/reconciliation/throughput/SLA analytics.

All native control-plane CRUD is **proto-driven** (table + column shape resolved from the embedded `proto/udb/core/**` manifest via `NativeModel`) and **Postgres-backed, fail-closed** — no in-memory stores.

## Project Status

UDB is usable today as a broker, CLI, protocol, and SDK set. It is also a young
open-source project: the core data plane, auth/control plane, migrations, and
published SDKs are the focus; packaging and production polish are still moving.

**Maturity at a glance**

| Area | Status | Notes |
|---|---|---|
| Proto parser, catalog, drift, migrations | 🟢 Stable | Hand-written parser, deterministic checksums, audited apply ledger |
| DataBroker data plane (76 RPCs) | 🟢 Stable | Relational, vector, object, cache, document, graph, column |
| Native control plane (Authn/Authz/ApiKey/Tenant/Notification/Analytics) | 🟢 Stable | Proto-driven, Postgres-backed, fail-closed, protected by bearer auth on its own listener |
| Canonical `SystemStores` | 🟢 Stable | SQL core plus feature-gated MSSQL, Redis, Cassandra, Neo4j, Qdrant, ClickHouse, vector/search adapters, and native MongoDB |
| CDC to Kafka (transactional outbox, DLQ, topic policy) | 🟢 Stable | At-least-once and exactly-once (Kafka transactions) modes |
| 2PC / XA, sagas with recovery | 🟡 Beta | Postgres 2PC and MySQL XA behind `UDB_2PC_ENABLED` |
| Vector / document / graph / column backends | 🟢 Stable | Canonical where `SystemStores` evidence exists; otherwise projection-only and fail-closed |
| Object stores and Memcached | 🟡 Beta | Full data-plane targets; object-store canonical profile remains conditional-write gated, Memcached is explicitly projection-only |
| SDKs (Go, Python, TypeScript, Java, C#, PHP) | 🟡 Beta | Go/Python/TS/PHP publish today; C#/Java version-checked, publish wiring in progress |

UDB is intentionally honest about backend capability. A backend is only offered
for an operation when the broker knows how to execute it. If a backend cannot
provide a required guarantee, the broker refuses the call instead of guessing.
The full backend table lives in [Backend Matrix](#-backend-matrix).

## Codebase Map

Current source shape (Rust files per area):

| Area | Files | Purpose |
|---|---:|---|
| [`src/runtime`](src/runtime) | 143 | Broker orchestration, service handlers, backend clients, CDC, catalog, system stores, security, metrics |
| [`src/ir`](src/ir) | 29 | Neutral logical operations and backend compilers |
| [`src/generation`](src/generation) | 18 | Manifest, SQL, DSN, drift, lint, and backend artifact generation |
| [`src/migration`](src/migration) | 7 | Diffing, plans, audited apply, phase runner, db_ops sync |
| [`src/control`](src/control) | 10 | Startup lifecycle, FSM, hooks, notifications, approval workflow |
| [`src/parser`](src/parser) | 10 | Hand-written proto lexer/parser and annotation extraction |
| [`src/backend`](src/backend) | 21 | Backend identity, capabilities, plugin contract, plugin inventory |
| [`src/cli`](src/cli) | 8 | `udb-proto-parser` command implementation |
| [`src/planning`](src/planning) | 4 | Request planning helpers for broker operations |
| [`src/schema`](src/schema) | 3 | Proto AST structs and deterministic checksums |
| [`crates/udb-portable`](crates/udb-portable) | 2 | WASM/edge-safe parser/checksum/schema-cache subset |

The public crate surface is collected in [`src/lib.rs`](src/lib.rs). The binary
entry point is tiny by design: [`src/main.rs`](src/main.rs) calls the CLI module.

## Request Flow

<p align="center">
  <img src="docs/assets/request-flow.svg" alt="UDB request flow: client → security context → authorization → channel admission → neutral IR → backend execution with RLS → response, with metrics/audit/CDC/projection/saga/DLQ side effects." width="900">
</p>

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

The `DataBroker` data-plane contract defines 76 RPCs in
[`proto/udb/services/v1/data_broker.proto`](proto/udb/services/v1/data_broker.proto).
They cover relational, vector, object, cache, document, graph, time-series,
analytical, transaction/2PC, CDC, resource admin, catalog, migration, DLQ, saga,
policy, project, health, and admin/audit surfaces.

Alongside the data plane, UDB now ships a **native control plane** under
`proto/udb/core/**` — six services (Authn, Authz, ApiKey, Tenant, Notification,
Analytics, 77 RPCs total) that run on a **separate, network-isolated listener**
(`UDB_AUTH_GRPC_ADDR`). See [Native Control Plane](#native-control-plane).

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

### 🗄️ Backend Matrix

UDB separates backend identity from runtime availability:

- `BackendKind` is the known backend enum.
- `BackendTier` groups SQL/cache/vector/object/document/graph/column stores.
- `BackendRole` says whether a backend is **canonical** (can host UDB's system
  tables and anchor durability) or a **projection** target (read/write only).
- `BackendCapability` declares operation and consistency properties.
- `Backend` plugin structs register backend-specific setup, generation, and
  conformance contracts.

The code declares **18 `BackendKind` variants** (`src/backend/mod.rs`). Default
features compile the full broker surface except `mongodb-native`; slim builds
compile a subset, e.g.
`--no-default-features --features postgres`.

| Backend | Tier | Feature flag | Role in matching build | Lifecycle | Operations / notes |
|---|---|---|---|---|---|
| Postgres | SQL | `postgres` (always on) | 🟢 canonical | catalog migration | relational CRUD, tx, RLS, 2PC |
| MySQL | SQL | `mysql` | 🟢 canonical | catalog migration | relational CRUD, tx, XA/2PC |
| SQLite | SQL | `sqlite` | 🟢 canonical | catalog migration | embedded relational CRUD, tx, dev/test canonical store |
| SQL Server | SQL | `mssql` | 🟢 canonical | compiler-mediated | Tiberius canonical store, relational CRUD, tx, `SESSION_CONTEXT` |
| MongoDB | document | `mongodb` / `mongodb-native` | projection; 🟢 canonical with `mongodb-native` | native when native driver is compiled | Data API/scalar projection by default; replica-set/sharded native store with majority semantics |
| ClickHouse | SQL/column | `clickhouse` | 🟢 canonical | native | analytical query/mutate, ReplacingMergeTree versioned-CAS store, eventual consistency |
| Neo4j | graph | `neo4j` | 🟢 canonical | native | graph query/mutate, transactional Cypher system store |
| Cassandra / ScyllaDB | column | `cassandra` | 🟢 canonical | compiler-mediated | wide-column query/mutate, LWT-backed system store |
| Qdrant | vector | `qdrant` | 🟢 canonical | native | vector search/upsert, dedicated system collection, strongly ordered writes |
| Weaviate | vector | `weaviate` | 🟢 canonical | native | vector + hybrid search, shared vector-system store adapter |
| Pinecone | vector | `pinecone` | 🟢 canonical | native | vector + hybrid search, shared vector-system store adapter |
| Elasticsearch | document/search | `elasticsearch` | 🟢 canonical | native | search + hybrid/knn, shared vector/search system-store adapter |
| Redis | cache | `redis` | 🟢 canonical when durable AOF profile passes startup checks | none for resource lifecycle | cache get/set/del/scan plus native Redis `SystemStores`; runtime refuses unsafe durability profiles |
| Memcached | cache | `memcached` | projection only | none | cache get/set; explicitly unsupported for canonical state |
| S3 | object | `s3` | projection; canonical candidate | native | streaming put/get, presigned URL, multipart; canonical profile gated on conditional writes/fencing |
| MinIO | object | `s3` | projection; canonical candidate | native | S3-compatible streaming put/get/presign/multipart |
| Azure Blob | object | `azureblob` | projection; canonical candidate | native | streaming put/get with staged blocks |
| Google Cloud Storage | object | `gcs` | projection; canonical candidate | native | streaming put/get |

> **Role legend** — 🟢 **canonical**: implements the full `SystemStores` trait set
> and can anchor a deployment (host UDB's system tables, outbox, saga/audit/lease
> state). **projection**: a first-class read/write target reached via typed RPCs
> and/or generic dispatch. **canonical candidate**: the feasibility profile is
> known, but the backend is not accepted as a system-state anchor yet.

Postgres is always compiled (never feature-gated); the other 17 backend surfaces
are gated. MinIO and S3 share the `s3` feature. `mongodb-native` is intentionally
separate from `mongodb`: the ordinary HTTP/Data-API build remains projection,
while the native driver build can host `SystemStores` after topology checks.
To inspect the exact capability report for the current binary, run
`udb compat-matrix` or `cargo run --bin udb-proto-parser -- compat-matrix`.

### Canonical Stores

This is the most important architectural transition in the repo.

Older UDB paths assumed Postgres was the canonical store for system tables,
CDC, saga state, projection task state, migration audit, and consistency fences.
The newer peer-to-peer work introduces:

- `CanonicalStore`
- `DurabilityToken`
- `SystemStores`
- `CanonicalStoreRegistry`
- SQL canonical stores: Postgres, MySQL, SQLite, SQL Server
- Native non-SQL canonical stores/adapters: Redis, Cassandra, Neo4j, Qdrant,
  ClickHouse, Weaviate, Pinecone, Elasticsearch, and native MongoDB

Key files:

- [`src/runtime/canonical_store/mod.rs`](src/runtime/canonical_store/mod.rs)
- [`src/runtime/canonical_store/system_store.rs`](src/runtime/canonical_store/system_store.rs)
- [`src/runtime/canonical_store/postgres.rs`](src/runtime/canonical_store/postgres.rs)
- [`src/runtime/canonical_store/mysql.rs`](src/runtime/canonical_store/mysql.rs)
- [`src/runtime/canonical_store/sqlite.rs`](src/runtime/canonical_store/sqlite.rs)
- [`src/runtime/canonical_store/mssql.rs`](src/runtime/canonical_store/mssql.rs)
- [`src/runtime/canonical_store/qdrant.rs`](src/runtime/canonical_store/qdrant.rs)
- [`src/runtime/canonical_store/vector_system.rs`](src/runtime/canonical_store/vector_system.rs)
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
| [`sdk`](sdk/README.md) | Language clients and CLI launchers |
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
cargo run --bin udb-proto-parser -- lint examples/go_arbitary_project/proto --human
cargo run --bin udb-proto-parser -- catalog examples/go_arbitary_project/proto
cargo run --bin udb-proto-parser -- sql examples/go_arbitary_project/proto
cargo run --bin udb-proto-parser -- plan examples/go_arbitary_project/proto
```

Run a Postgres-backed broker locally:

```powershell
Copy-Item .env.example .env.local
$env:UDB_PG_DSN = "postgresql://udb:udb@localhost:5432/udb?sslmode=prefer"
$env:UDB_ABAC_DEFAULT_ALLOW = "true"
cargo run --bin udb-proto-parser -- serve examples/go_arbitary_project/proto "" 0.0.0.0:50051
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

Published CLI/broker binaries are built with the portable backend set plus real
OIDC and WebAuthn support. To reproduce that shipped binary feature set locally:

```powershell
cargo build --bin udb-proto-parser --no-default-features --features "postgres,mysql,sqlite,qdrant,s3,mongodb,neo4j,clickhouse,redis,elasticsearch,weaviate,pinecone,azureblob,gcs,otel,runtime-logging,http-client,oidc,webauthn"
```

To verify the normal crate feature behavior on Windows, keep default features on
and add OIDC/WebAuthn:

```powershell
cargo check --features oidc,webauthn
```

WebAuthn uses `webauthn-rs`, which builds vendored OpenSSL. On Windows/MSVC,
install Visual Studio Build Tools, NASM, and Strawberry Perl; keep
`C:\Strawberry\perl\bin` ahead of Git/MSYS Perl in `PATH`. On Linux install
`build-essential`, `perl`, `nasm`, `cmake`, `clang`, `ninja`, and `pkg-config`.
On macOS, including Apple Silicon, install Xcode command-line tools plus
`cmake`, `nasm`, and `ninja`.

Windows bootstrap:

```powershell
.\scripts\bootstrap-webauthn.ps1 -CheckOnly
.\scripts\bootstrap-webauthn.ps1 -InstallChocolatey -FetchCargo
.\scripts\bootstrap-webauthn.ps1 -InstallChocolatey -RepairChocolateyLocks -FetchCargo
.\scripts\bootstrap-webauthn.ps1 -CleanNativeBuildCache
```

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
| `UDB_WEAVIATE_DSN`, `UDB_PINECONE_DSN`, `UDB_ELASTIC_DSN` | Vector/search backends and vector-system canonical adapters |
| `UDB_MSSQL_DSN` | SQL Server relational backend and canonical store |
| `UDB_CASSANDRA_DSN` | Cassandra/ScyllaDB wide-column backend and LWT-backed canonical store |
| `UDB_MINIO_ENDPOINT`, `UDB_MINIO_ACCESS_KEY`, `UDB_MINIO_SECRET_KEY` | MinIO/S3-compatible object storage |
| `UDB_AZUREBLOB_DSN`, `UDB_GCS_DSN` | Azure Blob / GCS object storage |
| `UDB_NOSQL_DSN`, `UDB_NOSQL_API_URL` | MongoDB/Atlas Data API backend |
| `UDB_MONGODB_DSN` or `UDB_NOSQL_DSN` | Native MongoDB canonical profile (`mongodb-native`) |
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

## Native Control Plane

Beyond the data-plane `DataBroker`, UDB serves a UDB-owned **auth/admin control
plane** defined under `proto/udb/core/**`. These six services are **network-isolated
on a separate listener** (`UDB_AUTH_GRPC_ADDR`, default loopback `port+10`) and
protected by a tonic interceptor that requires a verified bearer token with
`udb:admin`, `udb:auth:admin`, `udb:*`, or `*`. The interceptor also binds
`x-tenant-id` to the token tenant when that metadata is present. The listener
still must not sit on the public `DataBroker` port, because these services are a
policy decision point that accepts the subject principal as input.
All of them are proto-driven (`NativeModel`) and Postgres-backed, failing closed
when no PG pool is configured; their tables are generated from the embedded
`proto/udb/core/**` manifest through the normal migration path.

<p align="center">
  <img src="docs/assets/control-plane.svg" alt="UDB topology: apps reach the public DataBroker listener (76 RPCs); a trusted PEP reaches the isolated internal control-plane listener (6 services, 77 RPCs); both reach the 18 backends and emit events to Kafka." width="940">
</p>

| Service | Proto | RPCs | What it does |
|---|---|---:|---|
| `AuthnService` | `core/authn` | 23 | Authenticate (JWT / session / API key / external), login/logout, **RS256 JWT signing + refresh**, sessions, **TOTP MFA**, **CSRF**, OTP, user admin |
| `AuthzService` | `core/authz` | 23 | `Authorize`/`CheckAccess`/batch over RBAC+ABAC+ReBAC (Casbin), role/policy/relationship CRUD, audit decisions, **`GetNativeAccess`**, **`GetPolicyBundle`** |
| `ApiKeyService` | `core/apikey` | 7 | Create/get/list/update/revoke/validate API keys + usage stats |
| `TenantService` | `core/tenant` | 6 | Tenant + tenant-config CRUD |
| `NotificationService` | `core/notification` | 11 | Notifications, templates, preferences, delivery stats (emits `udb.notification.sent.v1` to Kafka) |
| `AnalyticsService` | `core/analytics` | 7 | Pipeline metrics, executor performance, reconciliation, throughput, SLA compliance |

Key capabilities:

- **Identity**: native JWT (static PEM or JWKS URL with `kid` rotation), UDB-issued
  RS256 access tokens + refresh tokens (`UDB_JWT_PRIVATE_KEY`), Argon2id passwords
  (legacy keyed-HMAC auto-upgraded on login), RFC 6238 TOTP MFA, server-side sessions
  with idle/absolute TTL + revocation, mTLS SAN identity, and a hybrid external-identity
  bridge. External-provider authentication now requires a signed JWT verified by
  UDB before claims are mapped; raw JSON claims are rejected.
- **Authorization**: one engine for RBAC (roles + bindings), ABAC (attribute
  conditions), and simple ReBAC (relationship tuples) with tenant/project domains,
  explicit-deny-wins, priority, and deterministic `decision_id` + audit records.
  `UDB_AUTHZ_V2` (default **on**) routes broker enforcement through it.
- **Native fast path**: `GetNativeAccess` authorizes a request and, when allowed,
  mints a short-lived restricted-role DSN plus the exact `app.current_*` session
  variables to `SET LOCAL`, so an SDK can talk to Postgres directly while the
  broker-generated RLS still applies.
- **Offline SDK authz**: `GetPolicyBundle` returns an HMAC-signed, time-boxed
  snapshot the SDK caches to answer `can()` locally.

Source: [`src/runtime/authn/`](src/runtime/authn), [`src/runtime/authz/`](src/runtime/authz),
[`src/runtime/service/auth_service/`](src/runtime/service/auth_service),
[`docs/native-services.md`](docs/native-services.md).

## Protocol And SDKs

The UDB-owned protocol is versioned separately from the crate and SDK package
versions. The current wire protocol is
[`1.0.0`](sdk/UDB_PROTOCOL_VERSION), and the current crate/SDK release tracked
by [`versions.json`](versions.json) is `0.3.1`.

The SDKs are the easiest way to talk to UDB. They do three useful things:

- attach the required tenant/user/purpose/scope metadata on every gRPC call;
- expose convenient `select`, `upsert`, auth, and raw broker clients;
- install or wrap a version-matched `udb` CLI so you can use broker tools from
  your app project.

Install one SDK:

| SDK | Current release | Install | Runtime requirements | Notes |
|---|---:|---|---|---|
| Go | `0.3.1` | `go get github.com/fahara02/udb/sdk/go@v0.3.1` | Go 1.22+, `grpc`, `protobuf` | Use `udbclient`; install CLI with `go install github.com/fahara02/udb/sdk/go/cmd/udb@v0.3.1` |
| Python | `0.3.1` | `pip install udb-client==0.3.1` | Python 3.10+, `grpcio`, `protobuf` | Sync/async clients, optional `pydantic`, `udb` CLI entry point |
| TypeScript / Node | `0.3.1` | `npm i @udb_plus/sdk@0.3.1` | Node 18+, `@grpc/grpc-js` | Import `@udb_plus/sdk/client` and `/auth`; use `npx udb ...` for the CLI |
| PHP / Laravel | `0.3.1` | `composer require fahara02/udb-laravel:^0.3.1` | PHP 8.1+, `ext-grpc`, Laravel 10/11/12 | ServiceProvider, Facade, middleware, typed exceptions, `vendor/bin/udb` |
| C# | `0.3.1` | `dotnet add package Udb.Client --version 0.3.1` | .NET 8, `Grpc.Net.Client` | Client package plus companion `Udb.Cli` tool |
| Java | `0.3.1-SNAPSHOT` today, `0.3.1` target | `dev.udb:udb-java-client` | Java 17, gRPC Java | Build from checkout until Maven Central publishing lands |

To write application protos with UDB annotations, export the shared UDB proto
contract into your project:

```bash
udb proto export
```

That creates or refreshes `proto/udb/**`, vendors the small `google/api/**`
imports needed for offline `protoc`, and can merge the required `buf.yaml`
entries. Your project can then import:

```proto
import "udb/core/common/v1/db.proto";
```

Use the exported annotation protos in your app schemas, then start the broker
against your proto root:

```bash
udb serve proto "" 0.0.0.0:50051
```

Every SDK sends the same request metadata:

- `x-tenant-id`
- `x-user-id`
- `x-purpose`
- `x-correlation-id`
- `x-scopes`
- `x-service-identity`
- `x-udb-project-id`
- `x-udb-client-catalog-version`

For normal use you do not run SDK generation. Install the package, export the
UDB protos when you need annotations, write your project protos, and call the
broker.

## Quickstart Per Language

<details open>
<summary><b>Go</b> - <code>go get github.com/fahara02/udb/sdk/go@v0.3.1</code></summary>

```go
import (
    entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
    authzv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/services/v1"
    "github.com/fahara02/udb/sdk/go/udbclient"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

conn, _ := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
meta := udbclient.Metadata{
    TenantID: "acme", UserID: "user-1", Purpose: "web.request",
    Scopes: []string{"udb:read", "udb:write"},
    ServiceIdentity: "billing.api", ProjectID: "default",
    ClientCatalogVersion: udbclient.ProtocolVersion,
}

udb := udbclient.New(conn, meta)
rs, _ := udb.Select(ctx, &entityv1.SelectRequest{MessageType: "acme.billing.v1.Invoice", Limit: 50})

auth := udbclient.NewAuthClient(conn, meta)
allowed, decision, _ := auth.Can(ctx, &authzv1.ResourceRef{MessageType: "acme.billing.v1.Invoice"}, "read", "")
```

Guide: [`sdk/go/README.md`](sdk/go/README.md).
</details>

<details>
<summary><b>Python</b> - <code>pip install udb-client==0.3.1</code></summary>

```python
from udb_client import Metadata, UdbClient, decode_records
from udb_client.auth import UdbAuthClient
from udb.core.authz.services.v1 import core_pb2 as authz

meta = Metadata(tenant_id="acme", user_id="user-1", purpose="billing.demo",
                correlation_id="demo-001", scopes=("udb:read", "udb:write"))

with UdbClient("127.0.0.1:50051", meta) as udb:
    udb.upsert(message_type="acme.billing.v1.Customer",
               record={"customer_id": "cus_001", "tenant_id": "acme"},
               conflict_fields=("customer_id",))
    rs = udb.select(message_type="acme.billing.v1.Customer", limit=10)
    print(decode_records(rs))

with UdbAuthClient("127.0.0.1:50051", meta) as auth:
    allowed, decision = auth.can(authz.ResourceRef(message_type="acme.billing.v1.Customer"), "read")
```

Install `pip install "udb-client[pydantic]==0.3.1"` when you want the optional
validated command models. Guide: [`sdk/python/README.md`](sdk/python/README.md).
</details>

<details>
<summary><b>TypeScript / Node</b> - <code>npm i @udb_plus/sdk@0.3.1</code></summary>

```ts
import { dataBrokerClient, metadata, UdbMetadata } from "@udb_plus/sdk/client";
import { UdbAuthClient } from "@udb_plus/sdk/auth";

const meta: UdbMetadata = { tenantId: "acme", userId: "user-1", purpose: "web.request",
    scopes: ["udb:read", "udb:write"], serviceIdentity: "billing.api" };

const broker = dataBrokerClient("localhost:50051");
broker.Select({ message_type: "acme.billing.v1.Invoice", limit: 50 }, metadata(meta),
    (err: any, rs: any) => console.log(rs?.records));

const auth = new UdbAuthClient("localhost:50051", meta);
const [allowed, decision] = await auth.can({ message_type: "acme.billing.v1.Invoice" }, "read");
```

Guide: [`sdk/typescript/README.md`](sdk/typescript/README.md).
</details>

<details>
<summary><b>Java</b> - Maven <code>dev.udb:udb-java-client</code>, Java 17</summary>

Current manifest version is `0.3.1-SNAPSHOT`; the release target is `0.3.1`.
Until Maven Central publish wiring is complete, build from the repo checkout:

```bash
mvn -f sdk/java/pom.xml test
```

```java
import dev.udb.client.*;
import com.udb.entity.v1.Types.*;
import com.udb.core.authz.services.v1.ResourceRef;

var meta = new UdbMetadata("acme", "web.request", "corr-123",
    java.util.List.of("udb:read", "udb:write"), "billing.api", "user-1", "default", UdbClient.PROTOCOL_VERSION);

try (UdbClient udb = new UdbClient("localhost:50051", meta)) {
    RecordSet rs = udb.select(SelectRequest.newBuilder()
        .setMessageType("acme.billing.v1.Invoice").setLimit(50).build());
}
try (UdbAuthClient auth = new UdbAuthClient("localhost:50051", meta)) {
    var d = auth.can(ResourceRef.newBuilder().setMessageType("acme.billing.v1.Invoice").build(), "read", "");
}
```

Guide: [`sdk/java/README.md`](sdk/java/README.md).
</details>

<details>
<summary><b>C#</b> - NuGet package <code>Udb.Client</code>, target <code>net8.0</code></summary>

```csharp
using Udb.Client; using Udb.Entity.V1;
using AuthzV1 = udb.core.Authz.Services.V1;

await using var udb = new UdbClient("http://localhost:50051", new UdbMetadata(
    TenantId: "acme", Purpose: "web.request", CorrelationId: "corr-123",
    Scopes: new[] { "udb:read", "udb:write" }, ServiceIdentity: "billing.api", UserId: "user-1"));
RecordSet rs = await udb.SelectAsync(new SelectRequest { MessageType = "acme.billing.v1.Invoice", Limit = 50 });

await using var auth = new UdbAuthClient("http://localhost:50051", /* same meta */ default!);
var (allowed, decision) = await auth.CanAsync(new AuthzV1.ResourceRef { MessageType = "acme.billing.v1.Invoice" }, "read");
```

Guide: [`sdk/csharp/README.md`](sdk/csharp/README.md).
</details>

<details>
<summary><b>PHP / Laravel</b> - <code>composer require fahara02/udb-laravel:^0.3.1</code></summary>

```php
use Fahara02\UdbLaravel\Facades\Udb;
use Udb\Entity\V1\SelectRequest;
use Udb\Core\Authz\Services\V1\ResourceRef;

// request context auto-bound by middleware; pass UdbMetadata explicitly off-request
$rs = Udb::select((new SelectRequest())->setMessageType('acme.billing.v1.Invoice')->setLimit(50));

[$allowed, $decision] = app(\Fahara02\UdbLaravel\UdbAuthClient::class)
    ->can((new ResourceRef())->setMessageType('acme.billing.v1.Invoice'), 'read');
```

Requires PHP 8.1+, `ext-grpc`, and Laravel 10/11/12. Guide:
[`sdk/php/README.md`](sdk/php/README.md).
</details>

Each SDK has the same basic job: carry request metadata, call the broker, and
make the native auth/authz APIs reachable in that language. Some languages have
more convenience helpers than others, but every SDK can still call the full gRPC
surface when you need an RPC that does not yet have a wrapper method.

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
| [`examples/go_arbitary_project`](examples/go_arbitary_project/README.md) | A Go project namespace UDB does not own; shows table, cache, vector, object, PII, encryption end-to-end |
| [`examples/python_arbitary_project`](examples/python_arbitary_project/README.md) | The same arbitrary-project flow driven from the Python SDK |
| [`examples/php_arbitary_project`](examples/php_arbitary_project/README.md) | The same flow from the PHP/Laravel SDK |
| [`examples/native-services/go`](examples/native-services/go) | Using the native control plane (Authn/Authz/ApiKey/Tenant/Notification/Analytics) from Go |
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
