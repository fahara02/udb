---
name: using-udb
description: Help a developer USE a running UDB broker — connect a language SDK, authenticate with scopes/credentials, CRUD proto-defined entities over the gRPC DataBroker API, and use the native services (storage uploads, notifications, WebRTC rooms, events/CDC). Use when the user is building an app against UDB, asks about the UDB SDK (TypeScript/Python/Go/Java/C#/PHP), UDB metadata/tenant/scopes/auth, Select/Upsert/Delete, file upload/presigned URLs, UDB events/topics, defining UDB entity protos (table/column annotations), debugging UDB gRPC errors, or the `udb` CLI (serve, sdk generate, proto export, auth bootstrap, doctor).
allowed-tools: Read, Grep, Bash, WebFetch
---

# Using UDB

UDB is a **proto-driven multi-database broker**. Developers declare their data
model as annotated Protocol Buffers; UDB generates the DB schema and serves a
uniform gRPC **DataBroker** API (`Select`/`Upsert`/`Delete`) plus a native
control plane (auth/authz/api-keys/storage/asset/notification/webrtc). Every
request carries **metadata** (tenant, project, scopes, identity) enforced
fail-closed server-side.

**Full reference (read on demand): [references/using-udb.md](references/using-udb.md)** —
per-language SDK install+connect snippets, the metadata header table, CRUD
semantics (idempotency/retry/pagination/proto3-presence), auth + bootstrap, the
native-services tour (storage upload flow, notifications, WebRTC, events/CDC),
proto annotation authoring with tenant columns, the CLI, and the error-decode
table.

## Mental model (hold this)
1. **Entities are protos.** Table = annotated `message`; its fully-qualified
   name (e.g. `shop.v1.Customer`) is the `message_type` for every data RPC.
2. **TWO gRPC targets.** Data target (default `:50051`) serves ONLY
   health/reflection/DataBroker. ALL native services live on the control-plane
   target (default port+10) — every SDK has `target` AND `authTarget`.
   `UNIMPLEMENTED` ≈ you dialed the wrong one.
3. **Every call carries metadata** (tenant/project/scopes + a credential);
   tenant isolation is enforced server-side on reads AND writes, and a body
   tenant can never override the credential's tenant.
4. **Mutations are not auto-retried** (only read-only RPCs retry on
   DEADLINE_EXCEEDED) — recommend `conflict_fields` idempotency on Upsert.
5. **Every mutation emits an event** (`udb.<svc>.<entity>.<verb>.v1`);
   CDC subscription streams are tenant-scoped with `since_event_id` replay.
6. **TWO authz surfaces, different engines.** Data RPCs are gated by a **data-plane
   ABAC snapshot** (default-DENY); `udb.authz.can/require` query a SEPARATE
   control-plane **Casbin** engine (roles/`policy_rules`). They can disagree — the
   data-plane ABAC is what actually protects your data. (UDB_FRICTION §7.)

## Before giving code, establish
- **Language** (TS / Python / Go / Java / C# / PHP) → that SDK's snippet from
  the reference.
- **Both addresses** — data target + control-plane/authTarget.
- **Credential** — bearer/API key in hand, or bootstrap needed?

## Quick reference
**SDK packages:** TS `@udb_plus/sdk` · Python `udb-client` · Go
`github.com/fahara02/udb/sdk/go` · Java `dev.udb:udb-java-client` · C#
`Udb.Client` · PHP `fahara02/udb-laravel`.

**CRUD** (by `message_type` = proto FQN): `Select {filter, limit ≤ ~500/page}` ·
`Upsert {record, conflict_fields, return_record}` · `Delete {filter}` · typed
cache/document/graph/timeseries RPCs per `GetCapabilities`.

**TS ergonomics:** `udb.data.table("pkg.v1.Entity",{key:[…]})` packs/decodes
`record_json` itself — your own entities need **no per-entity codegen**. For a
typed row, generate a plain snake_case interface with **ts-proto** (`onlyTypes=true,
snakeToCamel=false`) — protobuf-es (the repo's own SDK codegen) is camelCase and
won't match `record_json`. Serving a custom proto needs neither `udb proto export`
nor `buf` (the broker embeds the annotation contract).

**Storage upload:** `RegisterUpload` → presigned PUT → `FinalizeUpload`
(`is_public` is `optional` — omit to preserve) → presigned GET. **Authz:**
`can(resource, action)`; server `cache_ttl_seconds=0` = never cache.

**Real enterprise authn + authz flow** (worked end-to-end in `examples/ts_enterprise/`):
1. **Bootstrap the admin OFFLINE** (Postgres-direct; also binds `organization_owner`
   → `udb:*` at login):
   ```bash
   UDB_PG_DSN=… UDB_PASSWORD_HASH_SECRET=… \
   udb auth bootstrap user --username admin --email admin@x.com \
       --password '<strong>' --tenant acme --project default   # prints canonical tenant_id (UUID)
   ```
2. **Login → adopt the canonical tenant UUID** (the JWT tenant claim is the UUID,
   NOT the code "acme"): `login()` → `auth.authenticateBearer(token)` →
   `setTenant(principal.tenant_id)` (or `loginAndAdoptTenant()`).
3. **SEED ABAC or every data RPC is `PERMISSION_DENIED`** — the org-owner role +
   `udb:*` scopes are NOT enough; the data-plane reads an ABAC policy snapshot.
   Set `UDB_ABAC_POLICIES_JSON` (or rows in `udb_system.udb_abac_policies`):
   `{effect,service_identity,tenant_id,purpose,message_type,operation,required_scope}`
   (`*`/empty = wildcard). Dev shortcut: `UDB_ABAC_DEFAULT_ALLOW=true`.
4. The broker needs JWT keys (`UDB_JWT_PRIVATE_KEY`/`_PUBLIC_KEY`, RS256), sessions
   (`UDB_SESSION_ENABLED=true` + `UDB_SESSION_HASH_SECRET`), and the auth plane
   exposed (`UDB_AUTH_GRPC_ADDR=0.0.0.0:<port+10>`). `udb doctor --enterprise`
   lists every unmet prereq at once.

**mTLS from an SDK:** `UdbProject.connect({ tls: { rootCerts, privateKey, certChain } })`
(`secure:true` alone = system roots + no client cert; can't reach a private-CA/mTLS broker).

**CLI:** `udb proto export --out proto` · `udb serve proto "" 0.0.0.0:50051` ·
`udb sdk generate --lang <l>` · `udb sdk manifest` · `udb doctor` ·
`udb native list/docs` · `udb compat-matrix` (authoritative annotations).

## Error decode (first response to any failure)
`UNIMPLEMENTED`→wrong target (set authTarget) · `PERMISSION_DENIED`→scope or
tenant mismatch · `FAILED_PRECONDITION`→service disabled / wrong state /
missing config · `RESOURCE_EXHAUSTED`→rate limit or backpressure (back off) ·
`INVALID_ARGUMENT`→unknown message_type (use the FQN; `udb sdk manifest`), OR
"tenant isolation requires filter on tenant_id" → a tenant-scoped read/delete MUST
put `tenant_id` IN THE FILTER (`select({where:{…, tenant_id}})`) ·
`ABORTED`→version/CAS conflict (re-read, retry) · `NOT_FOUND` can mean
"exists, but not your tenant" — by design ·
`PERMISSION_DENIED` on a data RPC with valid scopes → no ABAC policy seeded
(default-deny); seed `UDB_ABAC_POLICIES_JSON` or set `UDB_ABAC_DEFAULT_ALLOW=true`.

## Guardrails
- Always include metadata (tenant/project/scopes) AND a credential in examples;
  route native-service clients to authTarget. Use `message_type` FQNs, never
  table names.
- Tenant-scoped entity protos need a recognizable tenant column (`tenant_id`,
  `_tenant_id`, or `is_tenant_column: true`); custom proto packages need
  `UDB_PROTO_NAMESPACE` or annotations are silently ignored.
- Don't invent RPCs/fields/annotations — ground truth is `udb sdk manifest`,
  `udb native list/docs`, `udb compat-matrix`, and the reference file above.
