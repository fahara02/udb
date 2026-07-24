# UDB SDKs

```text
┌────────────────────────────────────────────────────────────────────────────┐
│                                                                            │
│    ██    ██  ██████   ██████                                               │
│    ██    ██  ██   ██  ██   ██                                              │
│    ██    ██  ██   ██  ██████                                               │
│    ██    ██  ██   ██  ██   ██                                              │
│     ██████   ██████   ██████                                               │
│                                                                            │
│    UNIVERSAL DATA BROKER                                                   │
│    gRPC data plane | native control plane | tenant/project scope guard     │
│                                                                            │
│    crate v0.4.24 | protocol v1.0.0                                          │
└────────────────────────────────────────────────────────────────────────────┘
```

A UDB SDK is the client library your application uses to talk to a running UDB
broker. If you're building an app in Go, Python, TypeScript, PHP, C#, or Java and
want to read and write data through UDB, this is where you start.

Each SDK does four jobs for you: it attaches the request metadata every call
needs, wraps the common DataBroker operations in short helper methods, gives you
typed clients for the native auth and authz services, and ships a version-matched
`udb` CLI launcher so the tooling always matches your library.

Current SDK release: `0.4.24`

Current wire protocol: [`1.0.0`](UDB_PROTOCOL_VERSION)

## How You Actually Use It

Every SDK puts a small, high-level layer over the generated RPCs (the low-level
remote procedure calls the broker exposes). Your application code stays short,
but the layer never hides the rules that keep multi-tenant data correct:

- **One connect and login that adopts your tenant.** You log in with a username
  and password (or an API key) and name your tenant by its human *code*. The SDK
  resolves that code to the canonical tenant UUID that row-level security (the
  database's per-tenant access rule) actually checks. This one step prevents the
  most common first-integration bug: passing a tenant code where a UUID is
  expected and then silently reading zero rows.
- **Typed, tenant-scoped data calls.** Read and write your own proto-defined
  records with `select`, `upsert`, and `delete`. Read-after-write consistency,
  replay-safe retries, and idempotency keys are handled for you.
- **The native control plane, from the same client.** Authorization checks, API
  keys, storage, vault, and the other native services are one call away.
- **Raw generated RPCs stay available.** The high-level layer is a thin typed
  workflow over the same served RPCs — not a second protocol — so admin tools and
  advanced callers can drop down to the full generated surface any time.

Every language behaves the same way, and that's enforced. Static cross-language
conformance covers all six SDKs (`node sdk-conformance/run.mjs`), and live broker
coverage runs the Go, TypeScript, Python, and PHP harnesses against a real
server — DataBroker RPCs, native auth, tenant/authz/API-key flows, and the
storage/asset/notification/WebRTC facades. Every RPC is measured, so the gates
track the current generated surface instead of a hand-maintained count.

## Install

| Language | Package | Install |
|---|---|---|
| Go | `github.com/fahara02/udb/sdk/go` | `go get github.com/fahara02/udb/sdk/go@v0.4.24` |
| Python | `udb-client` | `pip install udb-client==0.4.24` |
| TypeScript / Node | `@udb_plus/sdk` | `npm i @udb_plus/sdk@0.4.24` |
| PHP / Laravel | `fahara02/udb-laravel` | `composer require fahara02/udb-laravel:^0.4.24` |
| C# | `Udb.Client` | `dotnet add package Udb.Client --version 0.4.24` |
| Java | `dev.udb:udb-java-client` | build from checkout until Maven Central publishing lands |

## What Every SDK Provides

| Surface | Purpose |
|---|---|
| Metadata helpers | Attach tenant, project, purpose, scopes, service identity, user id, correlation id, and catalog version |
| DataBroker client | Call record, object, vector, cache, document, graph, analytics, catalog, migration, transaction, CDC, and admin RPCs |
| Native clients | Call authn, authz, API key, IdP, tenant, notification, analytics, storage, asset, and WebRTC services |
| Framework adapters | Bind request context in common web frameworks |
| CLI launcher | Run a version-matched `udb` command from the package ecosystem |
| Generated protos | Access the full descriptor-derived gRPC surface when a helper method is not enough |

## Shared Flow

1. Install the SDK for your language.
2. Export UDB's shared protos into your application:

```bash
udb proto export --fmt
```

3. Import UDB annotations from your app protos:

```proto
import "udb/core/common/v1/db.proto";
```

4. Start the broker:

```bash
udb serve proto "" 0.0.0.0:50051
```

5. Use the SDK to call `Select`, `Upsert`, authz checks, object APIs, vector
   search, native services, and raw generated RPCs when needed.

## Metadata

SDK metadata objects carry:

- tenant id;
- user id when there is an end user;
- purpose;
- correlation id;
- scopes;
- service identity;
- project id;
- client catalog/protocol version;
- optional bearer token or API key.

Bearer credentials use `authorization: Bearer ...`. API keys use `x-api-key`.

## Framework Adapters

Adapters keep request metadata consistent in application code.

| Language | Adapter examples |
|---|---|
| TypeScript / Node | Express, Fastify, Next.js |
| Python | FastAPI, Starlette |
| Go | HTTP and gRPC middleware helpers |
| Java | Spring-oriented client helpers |
| C# | ASP.NET Core middleware |
| PHP / Laravel | Service provider, facade, middleware |

Use adapters at the edge of an application. Business code should receive a
request-scoped client or metadata object instead of rebuilding headers by hand.

## Language Guides

- [Go](go/README.md)
- [Python](python/README.md)
- [TypeScript / Node](typescript/README.md)
- [PHP / Laravel](php/README.md)
- [C#](csharp/README.md)
- [Java](java/README.md)

## Conformance

Cross-language behavior is checked by:

```bash
node sdk-conformance/run.mjs
```

See [../sdk-conformance/README.md](../sdk-conformance/README.md).

## Generation And Manifests

SDKs are generated from UDB's embedded descriptor and templates:

```bash
udb sdk list-langs
udb sdk manifest
udb sdk generate --lang typescript
udb sdk generate --lang all
```

Generated code should stay tied to:

- crate/package version `0.4.24`;
- protocol version `1.0.0`;
- descriptor-derived RPC and service metadata;
- the shared metadata contract used by every SDK.

## PHP / Laravel Publishing Model

The PHP/Laravel SDK lives in `sdk/php/` in this monorepo. Composer package
registries expect `composer.json` at the indexed repository root, so the public
package is published from a read-only satellite repository containing the
`sdk/php` subtree.

Release flow:

1. Tag the monorepo release.
2. Split `sdk/php` into the Laravel package repository.
3. Tag the satellite repository with the same version.
4. Let Packagist index `fahara02/udb-laravel`.

Consumer install command:

```bash
composer require fahara02/udb-laravel:^0.4.24
```

The monorepo remains the source of truth for generated PHP code, tests, and
release versioning.
