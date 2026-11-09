<p align="center">
  <img src="docs/assets/udb_logo.svg" alt="UDB logo" width="160">
</p>

<h1 align="center">Universal Data Broker</h1>

<p align="center">
  <strong>UDB :: Universal Data Broker</strong><br>
  <sub>gRPC data plane | native control plane | tenant/project scope guard<br>crate v0.6.13 | protocol v1.0.0</sub>
</p>

<p align="center">
  <a href="Cargo.toml"><img alt="Rust 2024" src="https://img.shields.io/badge/Rust-2024-b7410e?logo=rust&logoColor=white"></a>
  <a href="proto/README.md"><img alt="gRPC + Protobuf" src="https://img.shields.io/badge/API-gRPC%20%2B%20Protobuf-2563eb?logo=grpc&logoColor=white"></a>
  <a href="sdk/UDB_PROTOCOL_VERSION"><img alt="Protocol 1.0.0" src="https://img.shields.io/badge/protocol-1.0.0-059669"></a>
  <a href="#sdks"><img alt="SDKs" src="https://img.shields.io/badge/SDKs-Go%20%C2%B7%20Python%20%C2%B7%20TypeScript%20%C2%B7%20Java%20%C2%B7%20C%23%20%C2%B7%20PHP%20%C2%B7%20Rust-334155"></a>
  <a href="LICENSE"><img alt="License MIT" src="https://img.shields.io/badge/license-MIT-555"></a>
</p>

UDB is a public, schema-driven knowledge graph and data broker for applications
that need one typed API in front of many data systems.

You describe your domain in normal `.proto` files, add UDB annotations for
storage and security behavior, then run one broker in front of relational
databases, object stores, caches, vector stores, document stores, graph stores,
analytics systems, and native identity services. Application code calls UDB
through gRPC or an SDK; UDB handles routing, metadata, authorization, migrations,
CDC, and backend-specific execution.

<p align="center">
  <img src="docs/assets/architecture-pipeline.svg" alt="UDB architecture pipeline from proto files to catalog generation, runtime authorization, routing, and backend execution" width="940">
</p>

## What UDB Gives You

- One `DataBroker` API for relational records, objects, vectors, cache entries,
  documents, graphs, analytics, catalog operations, migrations, transactions,
  CDC, and admin health.
- A native control plane for authentication, authorization, API keys, identity
  providers, tenants, notifications, analytics, storage, assets, WebRTC, and
  versioned policy distribution.
- A descriptor-driven contract: protos define services, tables, field security,
  endpoint security, SDK surfaces, event contracts, and generated docs.
- Multi-tenant request context on every call: tenant, project, user, purpose,
  service identity, scopes, correlation id, and client catalog version.
- SDKs for Go, Python, TypeScript/Node, Java, C#, PHP/Laravel, and Rust.
- A single CLI named `udb` for proto export, formatting, SDK generation, native
  service setup, app integration, local dev, broker serving, and diagnostics.

## Current Surface

The service surface below is generated from the embedded descriptor — do not
edit the counts by hand. Run `udb native docs` (which also regenerates
[docs/generated/native-services.md](docs/generated/native-services.md)) and
commit the refreshed block; the `readme_services_block_matches_embedded_descriptor`
staleness test fails if it drifts.

<!-- BEGIN GENERATED:services -->
| Area | Surface |
|---|---|
| Data plane | 84 `DataBroker` RPCs |
| Native control plane | 28 services, 342 RPCs |

Per-service RPC counts (native control plane):

| Service | RPCs |
|---|---|
| `analytics` | 7 |
| `apikey` | 9 |
| `asset` | 8 |
| `authn` | 81 |
| `authz` | 41 |
| `backup` | 8 |
| `cache` | 8 |
| `config` | 5 |
| `control` | 6 |
| `embedding` | 19 |
| `idp` | 27 |
| `livequery` | 1 |
| `lock` | 5 |
| `messaging` | 15 |
| `metering` | 6 |
| `notification` | 12 |
| `scheduler` | 6 |
| `search` | 5 |
| `storage` | 10 |
| `tenant` | 8 |
| `vault` | 23 |
| `webhook` | 6 |
| `webrtc_peer` | 5 |
| `webrtc_room` | 9 |
| `webrtc_signaling` | 1 |
| `webrtc_track` | 4 |
| `webrtc_turn` | 1 |
| `workflow` | 6 |
<!-- END GENERATED:services -->

| Area | Surface |
|---|---|
| Contract manifest | 733 messages, 49 table-backed models, 192 event contracts |
| Backends | 18 backend kinds across SQL, cache, vector, object, document, graph, and column stores |
| SDKs | Go, Python, TypeScript/Node, Java, C#, PHP/Laravel, Rust |
| Release | crate/SDK version `0.6.13`, wire protocol `1.0.0` |

## Why UDB

Any app that outgrows a single database ends up hand-writing the same plumbing:
a client per datastore, a tenant filter on every query, retry and idempotency
logic, an auth layer, a secrets store, a search index, a job runner. UDB is that
plumbing — built once, behind one typed API — so your code stays about your
product, not the seams between systems.

- **Tenant isolation you can't forget to add.** Every row is scoped by a
  canonical tenant UUID, enforced in the database — not by a `WHERE tenant = …`
  you might leave off one query. Connect with an API key, or log in with
  `connectEnterprise` / `loginAndAdoptTenant` and your human tenant code
  (`"acme"`), and the SDK adopts the canonical UUID from the verified principal,
  so the classic first-week bug — a code/UUID mismatch that silently returns zero
  rows — can't happen on those paths. A tenant id you set by hand is sent as-is.
- **Writes that behave, so you delete code.** Read-after-write consistency,
  idempotent `Upsert`/`Delete` (a retried mutation replays safely and reports
  `was_duplicate`), and typed errors on a stable wire contract are built in —
  correctness glue you'd otherwise re-write in every service.
- **The rest of the backend, already built.** Auth with SSO, secrets, search,
  cache, schedulers and durable workflows, webhooks, metering, embeddings, file
  storage — the [control plane](#native-control-plane) you'd normally assemble
  from a dozen vendors, all tenant-scoped and reached through the same SDK as your
  data.
- **Swap the database without a rewrite.** The same `Select`/`Upsert`/`Delete`
  fronts [18 backend kinds](#backends) — relational, document, vector, object,
  cache, graph, wide-column, analytics, and search — so choosing or changing a
  store is a config decision, not a rewrite of your app.
- **Identical behavior in every language.** The Go, Python, TypeScript, Java, C#,
  PHP and Rust SDKs are generated from one proto contract, so an `Invoice` behaves
  the same in all seven.

None of this is aspirational: every backend and RPC is exercised by live
conformance tests against a running broker in each language — the surface is
proven, not just declared.

## How It Feels

Your project owns the model:

```proto
syntax = "proto3";
package acme.billing.v1;

import "udb/core/common/v1/db.proto";

message Invoice {
  option (udb.core.common.v1.pg_table) = {
    schema: "billing"
    table: "invoices"
  };

  string invoice_id = 1 [(udb.core.common.v1.pg_column) = {
    primary_key: true
    sql_type: "text"
  }];

  string tenant_id = 2;
  string customer_id = 3;
  int64 total_cents = 4;
}
```

Application code calls the broker through an SDK:

```python
import os
from udb_client import UdbProject

# A service authenticates with its API key. The SDK exchanges the key for a
# self-refreshing bearer, and tenant and project come from the key itself.
with UdbProject.connect(
    target="127.0.0.1:50051",       # data plane (DataBroker)
    auth_target="127.0.0.1:50061",  # control plane: data port + 10
    api_key=os.environ["UDB_API_KEY"],
    purpose="billing.api",
) as udb:
    rows = udb.data.table("acme.billing.v1.Invoice").select(limit=50)
```

A person signs in with `connect_enterprise(target=..., auth_target=...,
tenant_code="acme", username=..., password=...)` instead; the SDK swaps the human
tenant code for the canonical tenant UUID that row-level security checks. See
[`examples/python_enterprise`](examples/python_enterprise).

## Quick Start

The whole idea: **you describe your data model in protobuf, and UDB turns it into
a running, multi-tenant, RLS-protected API over your database — no ORM, no
hand-written SQL, and no migration files to write by hand.** Here's the path from
an empty folder to querying your own tables.

Steps 1–10 are the first run against an empty database. **Step 11 is the one you
will repeat for the rest of the project** — changing a model that already holds
data is a different job, and UDB gives you commands for it rather than leaving
you to guess.

**1. Get the `udb` CLI** — from a release binary, an SDK launcher, or source:

```bash
cargo install --path .
```

**2. Pull in UDB's shared protobuf annotations** so your own protos can use them:

```bash
udb proto export --fmt      # writes udb/core/common/v1/*.proto into your project
```

**3. Describe your data model.** Write normal protos and annotate the fields UDB
should turn into tables/columns — that annotation file is the one you just
exported:

```proto
import "udb/core/common/v1/db.proto";   // gives you pg_table / pg_column, RLS, tenancy
```

**4. See exactly what UDB will build** before you run anything — the catalog, the
SQL it generates, and a human-readable lint of your model:

```bash
udb catalog proto            # the tables/columns UDB derived from your protos
udb sql proto                # the DDL it will run
udb lint proto --human       # plain-English warnings (missing tenant column, etc.)
```

**5. Write a working `.env`** with freshly generated secrets, then check which
backends your protos need (the CLI and the broker read `./.env`):

```bash
udb env --profile dev --out .env --dsn "postgresql://user:pass@localhost:5432/app"
udb requirements             # each backend your manifest declares, and whether it is configured
```

**6. Start the broker.** It generates the schema and serves your model over gRPC.
The data plane listens on `50051`; the native control plane (auth, API keys,
storage, …) on loopback `127.0.0.1:50061`:

```bash
udb serve proto "" 0.0.0.0:50051
```

**7. Mint the first admin** (offline, straight into Postgres). Keep the printed
`tenant_id` — the canonical tenant UUID — for the next steps:

```bash
udb auth bootstrap user --username admin --email admin@example.com \
  --password "$ADMIN_PASSWORD" --tenant acme --project billing
```

**8. Give the project its catalog and policies.** Authorization is default-deny,
so nothing reads data until policies exist. `udb up` does both and is safe to
re-run; it reads `udb.yaml` (`tenant`, `project`, `policies`, `service_accounts`,
…) when present:

```bash
udb up --project billing --tenant "$TENANT_UUID"
# or step by step:
udb catalog bootstrap --project billing
udb policy apply -f policies.yaml --tenant "$TENANT_UUID"
```

**9. Confirm it's healthy:**

```bash
udb doctor --human           # readiness in plain English
udb compat-matrix            # which backends/features are live
```

**10. Connect your application.** Point an SDK at the broker (`target` = data
plane, `authTarget` = control plane), authenticate, and do tenant-scoped CRUD.
Services use an API key (`udb auth api-key create`, or a service account — see
[docs/service-accounts.md](docs/service-accounts.md)); people log in with
`connectEnterprise`. The shortest correct path is in
[`examples/go_enterprise`](examples/go_enterprise) (Go), with equivalents in
[`examples/python_enterprise`](examples/python_enterprise) and
[`examples/ts_enterprise`](examples/ts_enterprise). Start there rather than
wiring the raw gRPC clients yourself.

**11. Change your model — the day-2 loop.** Editing a proto once the table holds
rows is where care is needed, so it has its own commands. Never point a broker at
a changed model and hope:

```bash
udb verify --live --dsn "$UDB_PG_DSN"   # does the database still match the protos?
udb drift --prior prior-manifest.json   # what will this change do?
udb plan  --prior prior-manifest.json           --emit-approval-plan plan.json  # the file `serve` will accept
```

`drift` classifies every operation `SafeAuto`, `RequiresReview` or `Blocked`, and
each carries a `data_destructive` flag — gate your pipeline on that flag, never on
whether the operation's name starts with `Drop`. Rehearse against a `pg_dump`
clone before production.

Worked end-to-end walkthrough: [`examples/schema_change`](examples/schema_change).
Full guide: [docs/upgrading.md](docs/upgrading.md).

## CLI

All public commands use the short binary name `udb`.

| Command | Purpose |
|---|---|
| `udb init` | Build or preview a project-aware UDB setup plan |
| `udb proto export --fmt` | Export UDB annotation and broker protos into an app |
| `udb proto fmt --check` | Check annotation formatting |
| `udb catalog`, `udb sql` | Inspect the schema and DDL UDB derives from your protos |
| `udb drift`, `udb plan` | **Changing a live model:** what a change does, and the approval plan `serve` accepts |
| `udb verify --live` | Compare protos against a LIVE database, read-only, before applying anything; also checks delivery health and exits 1 on a finding or an undelivered outbox |
| `udb up` | Take a project from its protos to a served catalog (bootstrap, or the reviewed transition when the ACTIVE catalog differs), then apply its declared policies, identities and seed data; safe to re-run |
| `udb catalog bootstrap` | Give a project its first ACTIVE catalog; exits 3 when an ACTIVE catalog already differs from the protos |
| `udb catalog stage`, `udb catalog activate`, `udb catalog status` | Stage a catalog built from the protos, make it ACTIVE (running brokers reload it), show the ACTIVE one |
| `udb catalog transition plan\|approve\|apply\|stage\|activate\|status` | Review and apply a catalog change that needs review, through authorized broker RPCs |
| `udb catalog restore-receipts --run <id>` | Rebuild a reviewed transition's cleared receipts so the broker starts again |
| `udb catalog rebind-target --run <id> --actor <who> --reason <why>` | Move a reviewed transition to a restored or relocated database |
| `udb env`, `udb env fill` | Generate a working `.env` for dev or enterprise, with real secrets, or complete an existing one |
| `udb requirements` | Print the backends this project's manifest needs and whether each is configured |
| `udb sdk generate` | Generate descriptor-aware SDK surfaces from templates |
| `udb native list`, `udb native manifest`, `udb native docs` | Inspect native-service contracts |
| `udb app init` | Add a language/framework integration scaffold |
| `udb dev up`, `udb dev smoke` | Run and test the local playground |
| `udb serve` | Start the gRPC broker |
| `udb doctor`, `udb health-check` | Check runtime and dependency readiness |
| `udb auth bootstrap user` | Mint the first admin offline and print the canonical tenant UUID |
| `udb auth service-account create\|activate\|set-password` | Manage service accounts |
| `udb auth grant create\|get\|list\|replace\|rotate-identity\|transfer\|revoke` | Manage service grants |
| `udb auth api-key create\|list\|revoke` | Mint, list and revoke API keys |
| `udb auth ...` | Other control-plane helpers: roles, relations, policies, certificate bindings |
| `udb policy diff\|apply -f <file> --tenant <uuid>` | Compare or apply a declared policy file |
| `udb identity diff\|apply -f <identities.yaml>` | Compare or apply declared service identities |
| `udb sync-migrations` (alias `udb dbops sync`) | Sync generated database-operation artifacts |

## Backends

UDB knows these backend kinds:

`postgres`, `mysql`, `sqlite`, `sqlserver`, `clickhouse`, `redis`, `memcached`,
`qdrant`, `weaviate`, `pinecone`, `minio`, `s3`, `azureblob`, `gcs`, `mongodb`,
`elasticsearch`, `neo4j`, and `cassandra`.

The live capability matrix distinguishes compiled support from configured
runtime availability:

```bash
udb compat-matrix
```

## Native Control Plane

UDB ships a separate internal control-plane listener for native services:
loopback `127.0.0.1:50061` (the data port + 10) by default, moved with
`UDB_AUTH_GRPC_ADDR`. Bind it to an internal interface or place it behind a
trusted gateway. The data-plane port answers `UNIMPLEMENTED` for native RPCs, so
SDKs set `authTarget` to this listener.

| Service family | What it covers |
|---|---|
| Authn | Login, sessions, refresh tokens, JWT/JWKS, MFA, OTP, devices, WebAuthn |
| Authz | RBAC, ABAC, ReBAC, decisions, policy bundles, native access, governance |
| API keys | Hashed keys, scopes, rotation, revocation, usage stats |
| Identity providers | OIDC, SAML, SCIM, JIT, external identity links |
| Control distribution | Versioned policy/resource distribution with ACK/NACK |
| Tenant, notification, analytics | Tenant config, messages, templates, metrics, SLA views |
| Storage and asset | Object metadata, presigned URLs, asset pipelines, vector-ready workflows |
| WebRTC | Rooms, peers, tracks, TURN credentials, signaling, egress/SFU |
| Vault | Secrets, transit encrypt/sign/HMAC, dynamic database credentials |
| Metering | Usage recording and per-tenant quotas |
| Scheduler, workflow, lock | Cron/one-shot jobs, durable sagas, distributed locks with fencing tokens |
| Search, embedding | Managed search indexes, vector retrieval, and backfill enumeration |
| Webhook | SSRF-guarded outbound webhook endpoints and delivery tracking |
| Messaging | End-to-end-encrypted (MLS) messaging: devices, key packages, groups, inbox streaming, encrypted backups |
| Cache | Tenant-scoped namespaces with get/set/increment/delete/scan and namespace stats |
| Config, live query | Feature flags and server-streaming live-query subscriptions |
| Backup | Tenant-scoped backup and restore |

Every service, RPC, required scope, credential kind and tenant rule is listed in
the generated [docs/generated/native-services.md](docs/generated/native-services.md)
(`udb native docs`). Details, including the read-your-writes consistency contract (write receipts,
read fences, consistency modes) and idempotency `was_duplicate` replay:
[docs/native-services.md](docs/native-services.md).

## SDKs

| Language | Install |
|---|---|
| Go | `go get github.com/fahara02/udb/sdk/go@v0.6.13` |
| Python | `pip install udb-client==0.6.13` |
| TypeScript / Node | `npm i @udb_plus/sdk@0.6.13` |
| PHP / Laravel | `composer require fahara02/udb-laravel:^0.6.13` |
| C# | `dotnet add package Udb.Client --version 0.6.13` |
| Rust | `cargo add udb-client@0.6.13` |
| Java | `io.github.fahara02:udb-java-client` (`0.6.13` target; build from checkout until publishing lands) |

Start here: [sdk/README.md](sdk/README.md).

## Examples

| Example | Focus |
|---|---|
| [examples/go_arbitary_project](examples/go_arbitary_project/README.md) | Go app with app-owned protos and UDB broker calls |
| [examples/python_arbitary_project](examples/python_arbitary_project/README.md) | Python app flow with generated models and SDK calls |
| [examples/php_arbitary_project](examples/php_arbitary_project/README.md) | PHP/Laravel app flow |
| [examples/php_quickstart](examples/php_quickstart/README.md) | CRUD, authz scopes, and relationships |
| [examples/native-services](examples/native-services/README.md) | Native auth/authz/API-key/native-access examples |
| [examples/multi_project](examples/multi_project/README.md) | One broker serving separate project catalogs |
| [examples/toy_backend_plugin](examples/toy_backend_plugin/README.md) | Minimal external backend plugin |

## Documentation

- [docs/README.md](docs/README.md) - documentation index
- [docs/architecture.md](docs/architecture.md) - architecture, request flow, routing, pooling, events, and backend capability
- [docs/api-rules.md](docs/api-rules.md) - API route rules, OpenAPI operation IDs, and SDK alias policy
- [docs/annotations.md](docs/annotations.md) - proto annotations
- [docs/integration.md](docs/integration.md) - application integration
- [docs/native-services.md](docs/native-services.md) - native auth, authz, IdP, storage, assets, WebRTC, and SDK facades
- [docs/operations.md](docs/operations.md) - production readiness, config, runbooks, SLOs, and validation
- [docs/security.md](docs/security.md) - request context, identity, authorization, sensitive data, and compliance profiles
- [docs/testing.md](docs/testing.md) - test commands and live checks
- [VERSIONING.md](VERSIONING.md) - release and protocol versioning
- [TESTING.md](TESTING.md) - top-level test guide

## Development

Fast local checks:

```bash
cargo test --lib
buf lint
buf build
node scripts/check-versions.mjs
```

SDK conformance:

```bash
node sdk-conformance/run.mjs
```

The default Rust suite is designed to run without external services. Live
backend, HA, and load checks are documented separately and require the matching
infrastructure.

## License

MIT. See [LICENSE](LICENSE).
