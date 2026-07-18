---
name: using-udb
description: Help a developer USE a running UDB broker — connect a language SDK, authenticate with scopes/credentials, CRUD proto-defined entities over the gRPC DataBroker API, and use the native services (storage uploads, notifications, WebRTC rooms, events/CDC, vault secrets/transit crypto, metering/quotas, scheduler jobs, search indexes, webhooks, workflows/sagas, distributed locks, live-query streams, feature flags, backup/restore, embeddings). Use when the user is building an app against UDB, asks about the UDB SDK (TypeScript/Python/Go/Java/C#/PHP), UDB metadata/tenant/scopes/auth, Select/Upsert/Delete, idempotency keys/was_duplicate, write receipts/read fences/consistency modes, file upload/presigned URLs, UDB events/topics, defining UDB entity protos (table/column annotations), debugging UDB gRPC errors, or the `udb` CLI (serve, sdk generate, proto export, auth bootstrap, doctor).
allowed-tools: Read, Grep, Bash, WebFetch
---

# Using UDB

UDB is a **proto-driven multi-database broker**. Developers declare their data
model as annotated Protocol Buffers; UDB generates the DB schema and serves a
uniform gRPC **DataBroker** API (`Select`/`Upsert`/`Delete`) plus a native
control plane (auth/authz/api-keys/storage/asset/notification/webrtc — and the
platform wave: vault, metering, scheduler, search, webhook, workflow, lock,
livequery, config flags, backup, embedding). Every request carries **metadata**
(tenant, project, scopes, identity) enforced fail-closed server-side.

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
4. **Mutation retry is keyed.** Read-only RPCs auto-retry; a replay-safe
   mutation (Upsert/Delete) auto-retries ONLY with a non-empty
   `idempotency_key` (broker dedups durably; a replay returns
   `was_duplicate=true`). Keyless mutations fail closed — recommend
   `conflict_fields` + an `idempotency_key` on writes.
5. **Every mutation emits an event** (`udb.<svc>.<entity>.<verb>.v1`);
   CDC subscription streams are tenant-scoped with `since_event_id` replay.
6. **ONE authz engine (Casbin), default-DENY.** Data RPCs and
   `udb.authz.can/require` both decide via Casbin over roles/`policy_rules`. The
   data plane reads a PG-warmed snapshot of `policy_rules` — what an operator
   configures via `AuthzService.CreatePolicyRule` is exactly what protects your
   data (no separate env-JSON ABAC lane).

## Before giving code, establish
- **Language** (TS / Python / Go / Java / C# / PHP) → that SDK's snippet from
  the reference.
- **Both addresses** — data target + control-plane/authTarget.
- **Credential** — bearer/API key in hand, or bootstrap needed?

## Quick reference
**Current baseline:** UDB `0.4.0`, wire protocol `1.0.0` (release tag
`v0.4.0`). Pin SDKs to the same product version unless intentionally testing a
mixed-client upgrade: TS `@udb_plus/sdk@0.4.0` · Python `udb-client==0.4.0` · Go
`github.com/fahara02/udb/sdk/go@v0.4.0` · Java `dev.udb:udb-java-client`
`0.4.0` · C# `Udb.Client` `0.4.0` · PHP `fahara02/udb-laravel:^0.4.0`.

**Go SDK 0.3.7+ enterprise path:** for long-running services, use the SDK's
native session helpers instead of hand-rolled bearer refresh. Dial with
`udbclient.NewUdb`, then authenticate with
`u.Auth.LoginSession(store).LoginWithDevice(ctx, loginReq)`; call
`u.Auth.AuthenticateBearer(ctx, token.AccessToken)` and adopt the verified
principal's canonical tenant/project before any tenant-scoped CRUD. Keep the
returned `TokenManager` and call `Token(ctx)` before each DataBroker/native
operation, appending `authorization: Bearer <access_token>` to the SDK
metadata context. `LoginAndAdoptTenant` is valid for one-shot login/adopt and
updates generated-client authorization, but do not freeze its initial access
token inside a long-running server. If an API key is configured, authenticate it
once and let the SDK connection carry `x-api-key`.

**CRUD** (by `message_type` = proto FQN): `Select {filter, limit ≤ ~500/page}` ·
`Upsert {record, conflict_fields, return_record}` · `Delete {filter}` · typed
cache/document/graph/timeseries RPCs per `GetCapabilities`.

**TS ergonomics:** `udb.data.table("pkg.v1.Entity",{key:[…]})` packs/decodes
`record_json` itself — your own entities need **no per-entity codegen**. For a
typed row, generate a plain snake_case interface with **ts-proto** (`onlyTypes=true,
snakeToCamel=false`) — protobuf-es (the repo's own SDK codegen) is camelCase and
won't match `record_json`. Serving a custom proto needs neither `udb proto export`
nor `buf` (the broker embeds the annotation contract).

**Storage upload:** prefer SDK helpers (`Storage.UploadFile` / `RegisterUpload`
→ presigned PUT → `FinalizeUpload`) over direct DB/object-store access.
`project_id` is a UUID column; Go SDK 0.3.7+ only sends it when metadata holds a
canonical UUID, so human project codes like `private` are safe. `is_public` is
`optional` — omit to preserve. **Authz:** `can(resource, action)`; server
`cache_ttl_seconds=0` = never cache.

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
3. **SEED authz or every data RPC is `PERMISSION_DENIED`** — the org-owner role +
   `udb:*` scopes are NOT enough; the data plane enforces from `policy_rules`.
   Seed it via `AuthzService.CreatePolicyRule` (per-tenant, at runtime, from your
   code) or `udb policy-seed`, with the real `Select`/`Upsert`/`Delete` action:
   `{effect,subject,tenant,resource,action,required_scopes}` (`*`/empty =
   wildcard). Dev shortcut: `UDB_ABAC_DEFAULT_ALLOW=true`.
4. The broker needs JWT keys (`UDB_JWT_PRIVATE_KEY`/`_PUBLIC_KEY`, RS256), sessions
   (`UDB_SESSION_ENABLED=true` + `UDB_SESSION_HASH_SECRET`), and the auth plane
   exposed (`UDB_AUTH_GRPC_ADDR=0.0.0.0:<port+10>`). `udb doctor --enterprise`
   lists every unmet prereq at once.

**mTLS from an SDK:** `UdbProject.connect({ tls: { rootCerts, privateKey, certChain } })`
(`secure:true` alone = system roots + no client cert; can't reach a private-CA/mTLS broker).

**CLI:** `udb proto export --out proto` · `udb serve proto "" 0.0.0.0:50051` ·
`udb sdk generate --lang <l>` · `udb sdk manifest` · `udb requirements` (backend
contract; run BEFORE first start) · `udb doctor --enterprise` (manifest-aware
preflight — lists every unmet prereq + missing required backend at once) ·
`udb native list/docs` · `udb compat-matrix` (authoritative annotations). Since
0.3.7, `udb --help`, `udb help <cmd>`, command `--help`, `udb --version`, and
near-miss "did you mean" suggestions are supported.

## Error decode (first response to any failure)
`UNIMPLEMENTED`→wrong target (set authTarget) · `PERMISSION_DENIED`→scope or
tenant mismatch · `FAILED_PRECONDITION`→service disabled / wrong state /
missing config · `RESOURCE_EXHAUSTED`→rate limit or backpressure (back off) ·
`UNAUTHENTICATED` with only `x-api-key`→DataBroker data plane requires Bearer
JWT or mTLS; log in for an access token and send `authorization: Bearer <jwt>` ·
`INVALID_ARGUMENT`→unknown message_type (use the FQN; `udb sdk manifest`), OR
"tenant isolation requires filter on tenant_id" → a tenant-scoped read/delete MUST
put `tenant_id` IN THE FILTER (`select({where:{…, tenant_id}})`) ·
`ABORTED`→version/CAS conflict (re-read, retry) · `NOT_FOUND` can mean
"exists, but not your tenant" — by design ·
`PERMISSION_DENIED` on a data RPC with valid scopes → no policy seeded
(default-deny); seed `policy_rules` (`AuthzService.CreatePolicyRule` / `udb
policy-seed`) or set `UDB_ABAC_DEFAULT_ALLOW=true`.

## Guardrails
- Always include metadata (tenant/project/scopes) AND a credential in examples;
  route native-service clients to authTarget. Use `message_type` FQNs, never
  table names.
- Do not bypass UDB SDK/native services for storage or auth because a helper is
  awkward; fix/wrap the SDK path locally and report any missing helper back to
  UDB. No raw `udb_storage` writes from apps.
- Tenant-scoped entity protos need a recognizable tenant column (`tenant_id`,
  `_tenant_id`, or `is_tenant_column: true`); custom proto packages need
  `UDB_PROTO_NAMESPACE` or annotations are silently ignored.
- Don't invent RPCs/fields/annotations — ground truth is `udb sdk manifest`,
  `udb native list/docs`, `udb compat-matrix`, and the reference file above.
