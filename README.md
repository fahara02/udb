<p align="center">
  <img src="docs/assets/udb_logo.svg" alt="UDB logo" width="160">
</p>

<h1 align="center">Universal Data Broker</h1>

<p align="center">
  <strong>UDB :: Universal Data Broker</strong><br>
  <sub>gRPC data plane | native control plane | tenant/project scope guard<br>crate v0.3.2 | protocol v1.0.0</sub>
</p>

<p align="center">
  <a href="Cargo.toml"><img alt="Rust 2024" src="https://img.shields.io/badge/Rust-2024-b7410e?logo=rust&logoColor=white"></a>
  <a href="proto/README.md"><img alt="gRPC + Protobuf" src="https://img.shields.io/badge/API-gRPC%20%2B%20Protobuf-2563eb?logo=grpc&logoColor=white"></a>
  <a href="sdk/UDB_PROTOCOL_VERSION"><img alt="Protocol 1.0.0" src="https://img.shields.io/badge/protocol-1.0.0-059669"></a>
  <a href="#sdks"><img alt="SDKs" src="https://img.shields.io/badge/SDKs-Go%20%C2%B7%20Python%20%C2%B7%20TypeScript%20%C2%B7%20Java%20%C2%B7%20C%23%20%C2%B7%20PHP-334155"></a>
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
- SDKs for Go, Python, TypeScript/Node, Java, C#, and PHP/Laravel.
- A single CLI named `udb` for proto export, formatting, SDK generation, native
  service setup, app integration, local dev, broker serving, and diagnostics.

## Current Surface

| Area | Surface |
|---|---|
| Data plane | 76 `DataBroker` RPCs |
| Native control plane | 15 services, 186 RPCs |
| Contract manifest | 733 messages, 49 table-backed models, 192 event contracts |
| Backends | 18 backend kinds across SQL, cache, vector, object, document, graph, and column stores |
| SDKs | Go, Python, TypeScript/Node, Java, C#, PHP/Laravel |
| Release | crate/SDK version `0.3.2`, wire protocol `1.0.0` |

The native-service table is generated from the embedded descriptor:
[docs/generated/native-services.md](docs/generated/native-services.md).

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

## Quick Start

Install the CLI from a release binary, through an SDK launcher, or from source:

```bash
cargo install --path .
```

Export UDB's shared protos into an application project:

```bash
udb proto export --fmt
```

Write app-owned protos that import UDB annotations:

```proto
import "udb/core/common/v1/db.proto";
```

Inspect the catalog and generated SQL:

```bash
udb catalog proto
udb sql proto
udb lint proto --human
```

Start a local broker:

```bash
udb serve proto "" 0.0.0.0:50051
```

Check runtime readiness:

```bash
udb doctor --human
udb compat-matrix
```

## CLI

All public commands use the short binary name `udb`.

| Command | Purpose |
|---|---|
| `udb init` | Build or preview a project-aware UDB setup plan |
| `udb proto export --fmt` | Export UDB annotation and broker protos into an app |
| `udb proto fmt --check` | Check annotation formatting |
| `udb catalog`, `udb sql`, `udb plan`, `udb drift` | Inspect schemas, SQL, migrations, and drift |
| `udb sdk generate` | Generate descriptor-aware SDK surfaces from templates |
| `udb native list`, `udb native manifest`, `udb native docs` | Inspect native-service contracts |
| `udb app init` | Add a language/framework integration scaffold |
| `udb dev up`, `udb dev smoke` | Run and test the local playground |
| `udb serve` | Start the gRPC broker |
| `udb doctor`, `udb health-check` | Check runtime and dependency readiness |
| `udb auth ...` | Use control-plane helpers for principals, sessions, API keys, roles, relations, and policies |
| `udb dbops sync` | Sync generated database-operation artifacts |

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

UDB ships a separate internal control-plane listener for native services. Bind
it to an internal interface or place it behind a trusted gateway.

| Service family | What it covers |
|---|---|
| Authn | Login, sessions, refresh tokens, JWT/JWKS, MFA, OTP, devices, WebAuthn |
| Authz | RBAC, ABAC, ReBAC, decisions, policy bundles, native access, governance |
| API keys | Hashed keys, scopes, rotation, revocation, usage stats |
| Identity providers | OIDC, SAML, SCIM, JIT, external identity links |
| Control distribution | Versioned policy/resource distribution with ACK/NACK |
| Tenant, notification, analytics | Tenant config, messages, templates, metrics, SLA views |
| Storage and asset | Object metadata, presigned URLs, asset pipelines, vector-ready workflows |
| WebRTC | Rooms, peers, tracks, TURN credentials, signaling |

Details: [docs/native-services.md](docs/native-services.md).

## SDKs

| Language | Install |
|---|---|
| Go | `go get github.com/fahara02/udb/sdk/go@v0.3.2` |
| Python | `pip install udb-client==0.3.2` |
| TypeScript / Node | `npm i @udb_plus/sdk@0.3.2` |
| PHP / Laravel | `composer require fahara02/udb-laravel:^0.3.2` |
| C# | `dotnet add package Udb.Client --version 0.3.2` |
| Java | `dev.udb:udb-java-client` (`0.3.2` target; build from checkout until publishing lands) |

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
