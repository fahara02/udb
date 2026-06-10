# Using UDB — agent knowledge (canonical, agent-agnostic)

This is the single source of truth for the "Using UDB" skill. The Claude
(`SKILL.md`), OpenAI (`instructions.md`), and Ollama (`Modelfile`) packages all
wrap this same content. Edit here; re-sync the wrappers (see the repo README).

> **Audience:** an AI agent helping a developer *use* a running UDB broker —
> connecting an SDK, authenticating, and working with proto-defined entities and
> the native services. NOT contributing to UDB's Rust internals (that is the
> `udb-coding` skill).

---

## What UDB is (one paragraph)

UDB is a **proto-driven multi-database broker**. A developer declares their data
model as annotated Protocol Buffers; UDB turns those into database schemas (DDL)
and serves a uniform gRPC **DataBroker** API (`Select` / `Upsert` / `Delete` /
typed stores / streaming) across 18 backends, plus a **native control plane** of
first-class services (auth, authz, API keys, IdP, tenant, notification,
analytics, storage, asset pipelines, WebRTC). Every request carries **metadata**
(tenant, project, scopes, identity) that UDB enforces fail-closed. The proto is
the source of truth; the SDK is a thin typed client.

## The mental model an agent must hold

1. **Entities are protos.** A table = a proto `message` annotated with
   `(udb.core.common.v1.table)`; each column = a field annotated with
   `(udb.core.common.v1.column)`. The message's fully-qualified name (e.g.
   `shop.v1.Customer`) is the `message_type` you pass to every data RPC.
2. **You never write SQL.** You call `Select`/`Upsert`/`Delete` with a
   `message_type` + a filter/record; the broker routes to the backend.
3. **TWO gRPC targets.** The **data target** (default `localhost:50051`) serves
   ONLY health/reflection/DataBroker. ALL native services (auth, storage,
   notification, tenant, webrtc, …) live on the **control-plane target**
   (default = data port + 10, loopback unless the operator exposes it). Every
   SDK has both: `target` AND `authTarget` (`auth_target`/`AuthTarget`). A
   native-service call answered with `UNIMPLEMENTED` almost always means it was
   dialed at the data target.
4. **Every call carries metadata** (tenant/project/scopes/identity). Missing or
   wrong scopes/tenant → the broker denies the call. **Tenant isolation is
   enforced server-side on reads AND writes** — you cannot read another tenant's
   rows by passing their id.
5. **Auth is explicit.** A credential (bearer JWT, API key, or session) is
   attached as request metadata. Scopes like `udb:read` / `udb:write` /
   `udb:authn:*` / `udb:storage:*` gate operations.
6. **Every mutation emits an event** on a versioned dot topic
   (`udb.<service>.<entity>.<verb>.v1`) through a durable outbox→Kafka pipeline;
   clients can subscribe to tenant-scoped CDC streams.

---

## SDKs — install + connect + first call

Pick the developer's language. Default data target `localhost:50051` (plaintext
in dev). **Set `authTarget` too whenever the app uses auth/native services.**

### TypeScript / Node — `@udb_plus/sdk`
```bash
npm i @udb_plus/sdk
```
```ts
import { dataBrokerClient, metadata, UdbMetadata } from "@udb_plus/sdk/client";

const meta: UdbMetadata = {
  tenantId: "acme", projectId: "billing", userId: "user-1",
  purpose: "web.request", scopes: ["udb:read", "udb:write"],
  serviceIdentity: "billing.api",
};
const broker = dataBrokerClient("localhost:50051");
broker.Select({ message_type: "shop.v1.Customer", limit: 50 }, metadata(meta),
  (err, rs) => console.log(rs?.records));
```
Entry points: `@udb_plus/sdk/client` (DataBroker + `metadata()`),
`@udb_plus/sdk/auth` (`UdbAuthClient`), `@udb_plus/sdk` (full `UdbProject`
facade — configure `{ target, authTarget, credentials: { bearerToken | apiKey } }`).
Adapters for Express / Fastify / Next.js under `adapters/`.

### Python — `udb-client`
```bash
pip install udb-client        # or: pip install "udb-client[pydantic]"
```
```python
from udb_client import Metadata, UdbClient, decode_records

meta = Metadata(tenant_id="acme", project_id="billing", user_id="user-1",
                purpose="billing.api", scopes=("udb:read", "udb:write"),
                service_identity="billing-service")
with UdbClient("127.0.0.1:50051", meta) as udb:
    udb.upsert(message_type="shop.v1.Customer",
               record={"customer_id": "cus_001", "email": "ada@x.com"},
               conflict_fields=("customer_id",), return_record=True)
    rows = udb.select(message_type="shop.v1.Customer", limit=10)
    print(decode_records(rows))
```
`UdbAsyncClient` for async; `Metadata.from_env()` from env vars. The `UdbProject`
facade takes `UdbConfig(target=…, auth_target=…, bearer_token=… | api_key=…)`
and has `set_credentials()` hot-swap; it routes native services to the
control-plane channel automatically.

### Go — `github.com/fahara02/udb/sdk/go`
```bash
go get github.com/fahara02/udb/sdk/go
```
```go
cfg := udbclient.Config{Target: "localhost:50051", AuthTarget: "localhost:50061",
    TenantID: "acme", ProjectID: "billing",
    Scopes: []string{"udb:read", "udb:write"},
    Credentials: udbclient.Credentials{BearerToken: token}}
u, _ := udbclient.NewUdb(ctx, cfg); defer u.Close()
rows, _ := u.Data.Select(ctx, &entityv1.SelectRequest{MessageType: "shop.v1.Customer", Limit: 50})
```

### Java — `dev.udb:udb-java-client`
```java
var meta = new UdbMetadata("acme","web.request","corr-1",
    List.of("udb:read","udb:write"),"billing.api","user-1","billing",
    UdbClient.PROTOCOL_VERSION);
try (var udb = new UdbClient("localhost:50051", meta)) {
  var rows = udb.select(SelectRequest.newBuilder()
      .setMessageType("shop.v1.Customer").setLimit(50).build());
}
```
The `UdbProject` facade takes data + auth targets and credential objects with
hot-swap.

### C# — `Udb.Client`
```bash
dotnet add package Udb.Client
```
```csharp
var meta = new UdbMetadata(TenantId:"acme", Purpose:"web.request",
    CorrelationId:"corr-1", Scopes:new[]{"udb:read","udb:write"},
    ServiceIdentity:"billing.api", UserId:"user-1", ProjectId:"billing");
await using var udb = new UdbClient("http://localhost:50051", meta);
var rows = await udb.SelectAsync(new SelectRequest { MessageType="shop.v1.Customer", Limit=50 });
```

### PHP / Laravel — `fahara02/udb-laravel`
```bash
composer require fahara02/udb-laravel   # needs PHP 8.1+, grpc PECL ext
```
```php
$client = new UdbClient(['endpoint' => '127.0.0.1:50051', 'tls' => ['enabled' => false]]);
$meta = new UdbMetadata(tenantId:'acme', userId:'user-1', purpose:'crud',
    correlationId:'c-1', scopes:['udb:read','udb:write'],
    serviceIdentity:'app', projectId:'default', clientCatalogVersion:'1.0.0');
$req = (new SelectRequest())->setMessageType('shop.v1.Customer')->setLimit(50);
$records = $client->select($req, $meta);
```

---

## The metadata contract (every SDK sends these headers)

| Header | Meaning |
|---|---|
| `x-tenant-id` | tenant isolation boundary (**required** for tenant-scoped ops; must match the credential's tenant) |
| `x-udb-project-id` | which application catalog/project |
| `x-user-id` | end-user id (optional) |
| `x-scopes` | comma-separated scopes, e.g. `udb:read,udb:write` |
| `x-purpose` | request intent, e.g. `web.request` |
| `x-correlation-id` | tracing id |
| `x-service-identity` | calling service name |
| `x-udb-client-catalog-version` | client's catalog/protocol version |
| `authorization: Bearer <jwt>` | bearer credential |
| `x-api-key: <key>` | API-key credential |

SDKs build these from a `Metadata`/`UdbMetadata` object — set tenant, project,
scopes and a credential; never hand-craft headers. **A body-carried tenant id
never overrides the credential's tenant** — the broker compares them and denies
mismatches.

---

## CRUD (the data plane) + semantics that matter

All data RPCs take `message_type` = the proto's fully-qualified name.

- **Select**: `{ message_type, filter, limit, order_by }` → `RecordSet`.
  List/page sizes are server-capped (default max 500 rows per page) — paginate,
  don't ask for everything.
- **Upsert**: `{ message_type, record/record_json, conflict_fields, return_record }`
  — insert-or-update on the conflict key. **Always set `conflict_fields`** for
  idempotency: SDKs auto-retry only *read-only* RPCs on `DEADLINE_EXCEEDED`;
  mutations are NOT replayed, so timeouts on writes need an idempotent retry by
  the app (which `conflict_fields` makes safe).
- **Delete**: `{ message_type, filter }`.
- **Typed stores** (when the backend supports them): cache get/set/delete/scan,
  document get/find/upsert/delete, graph query/mutate, time-series write/query,
  analytical query — advertised in `GetCapabilities.supported_rpcs`.
- Streaming batch variants exist for large result sets / bulk writes.
- **Partial updates respect proto3 presence:** fields declared `optional` in the
  entity proto are only applied when present; plain scalar fields decode to
  their default — never echo stale booleans/numbers back on update requests.

Records are JSON-shaped objects keyed by proto field names. Decode helpers
(`decode_records` in Python, typed messages in Go/Java/C#) turn rows into
language objects. **Sensitive fields never come back:** columns annotated
storage-only/redacted (password hashes, key material) are blanked server-side.

---

## Auth (consumer view)

- **Public entry:** `Authenticate` (AuthnService, on the control-plane target)
  accepts a credential and returns a principal/token. Public auth RPCs
  (Authenticate/ForgotPassword/ResetPassword) are **rate-limited per caller**
  (default 60/min) — back off on `RESOURCE_EXHAUSTED`.
- **First credential / bootstrap:** a fresh deployment has no principal; an
  operator mints the first one **offline**:
  ```bash
  UDB_PG_DSN="postgres://udb:udb@host:5432/udb?sslmode=disable" \
  UDB_PASSWORD_HASH_SECRET="<secret>" \
  udb auth bootstrap user --username admin --email admin@x.com \
      --password '<strong-pass>' --tenant acme --project default
  ```
- **Scopes** gate operations: `udb:read`/`udb:write` for data, per-service
  scopes for native RPCs (`udb:authn:get-user`, `udb:storage:write`, …). The
  error for a missing scope is `PERMISSION_DENIED`.
- **API keys:** created via `udb auth api-key-create` or the ApiKeyService;
  sent as `x-api-key`; tenant-scoped; per-key rate limits are enforced from
  real usage records.
- **Credential hot-swap:** rotate a token/key without rebuilding the channel —
  `set_credentials()` (Python/TS/Go/Java/C#/PHP all support it).
- **Authz checks:** `UdbAuthClient.can(resource, action)` asks the broker.
  **Respect the server's `cache_ttl_seconds`: `0` means DO NOT cache the
  decision** — every SDK honors this; never wrap it in your own cache.
- **Sessions/devices:** list/revoke your own sessions and devices via
  AuthnService; admins can revoke per-user/tenant. Revocation propagates —
  don't cache validation results client-side.

---

## Native services tour (all on the control-plane target)

| Service | What an app does with it |
|---|---|
| **AuthnService** | authenticate, users, sessions, devices, MFA |
| **AuthzService** | `check_access`/batch checks, policy bundles |
| **ApiKeyService** | mint/rotate/revoke keys, validate |
| **IdentityProviderService** | OIDC/SAML/SCIM federation |
| **TenantService** | tenant/project lifecycle |
| **NotificationService** | send + query notifications, **user preferences (opt-out → status `SUPPRESSED`)**, tenant-scoped templates, `RetryNotification` re-queues AND re-emits the delivery event |
| **AnalyticsService** | usage/event queries |
| **StorageService** | file uploads (flow below), presigned URLs, quotas, GC |
| **AssetService** | processing pipelines over stored files (steps incl. vector EMBED); pipelines can auto-trigger on upload finalize |
| **RoomService / WebRTC** | rooms, peers, tracks, TURN credentials, signaling (flow below) |

**Storage upload flow:** `RegisterUpload` (name/type/size → file_id + presigned
PUT URL) → client PUTs bytes directly to object storage → `FinalizeUpload`
(file_id; `is_public` is `optional` — omit to keep the default/stored value) →
`GetFile`/`DownloadFile`/presigned GET. `UpdateFile` is a partial update —
only send fields you mean to change. Finalizing can auto-start an asset
pipeline.

**WebRTC flow:** `CreateRoom` → `JoinRoom` (capacity-checked) → bidirectional
`Signal` stream relays SDP/ICE within the room (membership is re-validated
during the stream; closed rooms/kicked peers get terminated) → `IssueCredentials`
for TURN (fail-closed: no TURN secret configured = denied, not open) →
`LeaveRoom` ends only YOUR stream; `CloseRoom` (admin) ends everyone's.
Crashed clients are reaped automatically — capacity recovers without operator
action.

**Events:** every mutation publishes `udb.<svc>.<entity>.<verb>.v1` (e.g.
`udb.storage.file.finalized.v1`). Apps consume via Kafka or the broker's CDC
subscription stream — streams are **tenant-scoped server-side** (you only ever
see your tenant's events) and support resume via `since_event_id` (replayed
from a durable journal, surviving acked deliveries).

---

## The data model — defining entities (proto annotations)

```proto
syntax = "proto3";
package shop.v1;
import "udb/core/common/v1/db.proto";   // UDB annotations (vendor via `udb proto export`)

message Customer {
  option (udb.core.common.v1.table) = {
    table_name: "customers"
    schema_name: "shop"
    is_table: true
    enable_rls: true            // optional: row-level security
  };

  string customer_id = 1 [(udb.core.common.v1.column) = {
    column_name: "customer_id" sql_type: "UUID"
    primary_key: true default_value: "gen_random_uuid()"
  }];
  string tenant_id = 2 [(udb.core.common.v1.column) = {
    column_name: "tenant_id" sql_type: "VARCHAR(64)" not_null: true
  }];
  string email = 3 [(udb.core.common.v1.column) = {
    column_name: "email" sql_type: "VARCHAR(320)" not_null: true unique: true
  }];
}
```

- **Tenant isolation:** give every tenant-scoped table a tenant column. The
  broker recognizes `tenant_id`, `_tenant_id`, or any column flagged
  `is_tenant_column: true` in the table security annotation — then every read
  and write is automatically tenant-filtered. No recognizable tenant column on
  a tenant-scoped query = the broker fails closed, not open.
- **Use `optional`** on fields that participate in partial updates (see CRUD).
- Advanced annotations: `(…db_column_security)` (PII class / redaction /
  encryption / storage-only output view per field), `(…db_table_security)`
  (tenant column, RLS template, audit/retention), `(…endpoint_security)` on
  RPCs (auth mode + scopes). `udb compat-matrix` prints the authoritative
  annotation surface (derived from the parser itself — trust it over blogs).
- **Custom packages:** annotations match bare or `udb.`-qualified names. If
  your options live under your own package (e.g. `(myapp.v1.table)`), set
  `UDB_PROTO_NAMESPACE=myapp.v1` (or pass the namespace arg) or the parser
  silently ignores them and you get zero tables.

**Pipeline:** annotated protos → catalog manifest (normalized + checksummed) →
DDL (CREATE TABLE / INDEX / RLS) → runtime routing + redaction + authz. The
broker applies it at startup; schema changes flow through a migration diff with
review gates for destructive/raw-SQL changes.

---

## CLI — the `udb` binary

| Command | Purpose |
|---|---|
| `udb proto export --out proto` | Vendor UDB's annotation protos so app protos can `import "udb/core/common/v1/db.proto"`. |
| `udb serve proto "" 0.0.0.0:50051` | Start the broker (syncs schema from protos on boot; control-plane listener comes up on port+10). |
| `udb sdk generate --lang <lang>` / `udb sdk list-langs` / `udb sdk manifest` | Generate/refresh SDK clients; dump the full RPC manifest (the authoritative service/RPC list). |
| `udb doctor [--with-probes]` | Env + backend readiness, honestly (configured flags match GetCapabilities). |
| `udb auth bootstrap user …` / `udb auth api-key-create …` | First offline admin / mint API keys. |
| `udb native list / manifest / docs / contract-diff` | Inspect the native service contract; diff it between versions. |
| `udb dev up / down / smoke` | Local sandbox via the bundled compose (run from a repo checkout). |
| `udb init-project` | Scaffold a project — mind the namespace note above. |
| `udb policy-lint` | Lint ABAC policy files (non-zero exit on broken files — safe as a CI gate). |

---

## Decoding broker errors (read this before debugging)

| gRPC status | Most likely cause |
|---|---|
| `UNIMPLEMENTED` | Native-service call sent to the **data target** — set `authTarget`/control-plane address. |
| `PERMISSION_DENIED` | Missing scope, tenant mismatch (metadata/body vs credential), revoked credential, or per-action policy deny. |
| `FAILED_PRECONDITION` | Service disabled/not mounted, wrong lifecycle state (e.g. apply before approve), feature requires config (TURN secret, media profile). |
| `RESOURCE_EXHAUSTED` | Public-auth rate limit, per-key rate limit, quota, or per-tenant fairness backpressure — back off and retry. |
| `INVALID_ARGUMENT` | Unknown `message_type` (use the proto FQN; check `udb sdk manifest`), malformed filter, invalid cron/enum/value. |
| `ABORTED` | Optimistic-concurrency conflict (CAS/version mismatch) — re-read and retry. |
| `DEADLINE_EXCEEDED` | The op may have committed server-side. SDKs auto-retry only read-only RPCs; retry writes yourself only when idempotent (`conflict_fields`). |
| `NOT_FOUND` | Row outside your tenant looks identical to a missing row — by design. |

---

## End-to-end quickstart (what to tell a developer)

1. **Define** app protos with `table`/`column` annotations (+ a tenant column);
   `import "udb/core/common/v1/db.proto"`.
2. **Vendor annotations:** `udb proto export --out proto`.
3. **Bring up backends** — `docker compose up -d` (or `udb dev up` in a checkout).
4. **Serve:** `udb serve proto "" 0.0.0.0:50051`.
5. **Bootstrap** the first user (`udb auth bootstrap user …`).
6. **Install the SDK**; construct the client with metadata + credential AND both
   targets (data + authTarget).
7. **CRUD** by `message_type`; use native services (storage/notification/…) via
   the project facade; subscribe to `udb.*.v1` events if event-driven.

A working example lives in the UDB repo at `examples/php_quickstart/`.

---

## How the agent should behave

- Establish: **language**, **broker data + control-plane addresses**, and
  whether they hold a **credential** (or need bootstrap) before giving code.
- Always include **metadata** (tenant/project/scopes) AND a credential in
  examples; route native-service clients to **authTarget**.
- Use the developer's **`message_type`** (proto FQN), never raw table names.
- Diagnose by the error table above before guessing; "denied write" → check
  `udb:write` + tenant match; "UNIMPLEMENTED" → wrong target, not a missing
  feature.
- For writes, recommend `conflict_fields` idempotency and explain the
  no-auto-retry-on-mutations rule.
- Don't invent RPCs/fields/annotations. The ground truth is
  `udb sdk manifest`, `udb native list/docs`, `udb compat-matrix`, and the
  per-language SDK README.
