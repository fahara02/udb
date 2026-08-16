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

**More references (read on demand, authoritative/generated):**
- [references/rpc-inventory.md](references/rpc-inventory.md) — every native RPC
  with its required scope, tenant fields, request/response types, and
  handler. Ground truth for "which scope does this call need" and
  `PERMISSION_DENIED` on `:50061`.
- [references/sensitive-fields.md](references/sensitive-fields.md) — which
  fields across the API are treated as sensitive (redacted from logs/audit,
  never echoed). Guard when building requests or reading responses.

## Mental model (hold this)
1. **Entities are protos.** Table = annotated `message`; its fully-qualified
   name (e.g. `shop.v1.Customer`) is the `message_type` for every data RPC.
2. **THREE planes on THREE listeners.** Data plane (`DataBroker` CRUD) on the
   public port (default `:50051`); native/control plane (all 27 services) on a
   **loopback-by-default** listener (`UDB_AUTH_GRPC_ADDR`, default `:50061`);
   WebRTC peer plane (`:50071`). Every SDK has `target` AND `authTarget`.
   `UNIMPLEMENTED` = native call sent to the data port; `ECONNREFUSED` = the
   loopback auth listener isn't exposed.
3. **THREE authorization surfaces, cured differently.** Data CRUD → **Casbin
   policy rows** (`udb_authz.policy_rules`); native RPCs → **token scopes**
   (`endpoint_security`, `udb:<service>:<method>`); the old ABAC table is dead.
   A data deny needs a *policy row*; a native deny needs a *scope*. Don't confuse
   them.
4. **Two kinds of caller.** A **human** logs in (username/password → JWT). A
   **service** is a `SERVICE_ACCOUNT` user + a **mandatory `ServiceAccountGrant`**
   (no grant = can't authenticate — the #1 "works as a user, fails as a service"
   trap) + an API key or password login (services get NO refresh token; no
   client-credentials grant exists).
5. **Every call carries metadata** (tenant UUID / project / scopes / **purpose**
   + a credential); tenant isolation is enforced on reads AND writes; a
   tenant-scoped op with no `purpose` is denied before authz.
6. **Mutation retry is keyed.** Replay-safe mutations auto-retry ONLY with a
   non-empty `idempotency_key` (durable dedup; replay → `was_duplicate=true`).
   Keyless fail closed. There is no `Insert` RPC — `Upsert` with empty
   `conflict_fields` inserts.
7. **Casbin data-plane action = the RPC method name.** `Select`/`Upsert`/
   `Delete`/`Update`/`BulkCas` — **NOT** `data.select`. Wrong action / human
   tenant-code / wrong `object` = silent deny. `UDB_ABAC_DEFAULT_ALLOW=true`
   bypasses ONLY while zero policy rows exist.

## Before giving code, establish
- **Language** (TS / Python / Go / Java / C# / PHP) → that SDK's snippet from
  the reference.
- **Both addresses** — data target + control-plane/authTarget.
- **Credential** — bearer/API key in hand, or bootstrap needed?

## Quick reference
**Current baseline:** UDB `0.5.10`, wire protocol `1.0.0`. Pin SDKs to the same
product version: TS `@udb_plus/sdk@0.5.10` · Python `udb-client==0.5.10` · Go
`github.com/fahara02/udb/sdk/go@v0.5.10` · Java `dev.udb:udb-java-client`
`0.5.10` · C# `Udb.Client` `0.5.10` · PHP `fahara02/udb-laravel:^0.5.10`.

**Enterprise session (human):** Go `udbclient.ConnectEnterprise(ctx,
EnterpriseConfig{Target, AuthTarget, Username, Password, TenantCode})` → an
`EnterpriseSession` that logged in, adopted the canonical tenant UUID, and
background-refreshes the bearer; use `session.DataContext(ctx)` /
`session.NativeContext(ctx)` + `session.CanonicalTenantID` in filters. TS:
`UdbProject.connectEnterprise(config)`. **Service (machine):** provision
`CreateUser{account_kind:SERVICE_ACCOUNT}` → `CreateServiceAccountGrant{scopes}`
→ `CreateApiKey` (save the one-time `udbk_…`) → bind it to your data role
(`AssignRole`); connect with the STATIC `Credentials.APIKey`/`Bearer` path
(no auto-refresh — re-exchange the key on expiry).

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
   Straightforward: `udb authz seed --tenant <T-UUID> --role app_rw` (offline,
   Postgres-direct, validated action tokens, idempotent + atomic; `--emit <path>`
   for a version-controlled JSON) — or runtime `AuthzService.CreatePolicyRule`,
   or offline `udb policy-seed`. Real actions only (`Select`/`Upsert`/`Delete`/
   `Update`/`BulkCas` — never `data.*`), then bind principals to the role
   (`udb auth role bind`). Dev shortcut: `UDB_ABAC_DEFAULT_ALLOW=true`.
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
`udb authz seed --tenant <UUID> --role app_rw` (offline data-plane policy
seeding) · `udb native list/docs` · `udb compat-matrix` (authoritative
annotations). Since 0.3.7, `udb --help`, `udb help <cmd>`, command `--help`,
`udb --version`, and near-miss "did you mean" suggestions are supported.

## Error decode (first response to any failure)
`UNIMPLEMENTED`→native call sent to the data port (use authTarget/`:50061`) ·
`FAILED_PRECONDITION`→native service disabled (backend missing: Vault unseal /
Redis / Kafka / object store / ledger), wrong lifecycle, or missing config ·
`RESOURCE_EXHAUSTED`→rate limit/quota (read `retry_after_ms`, back off) ·
`UNAUTHENTICATED`→missing bearer, or empty tenant; on the data plane exchange an
API key for a JWT if `x-api-key` isn't accepted · `INVALID_ARGUMENT`→unknown
message_type (`udb sdk manifest`), or "tenant isolation requires filter on
tenant_id" (put the tenant UUID in the filter), or non-UUID `project_id` on
storage · `ABORTED`→CAS/revision conflict (re-read, retry) · `NOT_FOUND`→may be
"exists but not your tenant" (by design).

**`PERMISSION_DENIED` decision tree:** (1) "purpose is required" → send
`x-purpose`. (2) NATIVE call → token missing scope `udb:<service>:<method>` (for
a service, add it to its grant first). (3) DATA call → Casbin: "no authz policy"
= zero rows → `udb authz seed --tenant <T-UUID> --role app_rw` then bind the
principal; "no applicable allow policy" = one of the three token traps —
`action` must be `Select`/`Upsert`/… (not `data.select`), `tenant_id`=caller
UUID, `object`=message_type, subject matches. (4) "tenant mismatch" → use the
credential's UUID.
(5) `udb doctor --consumer --key <k> --entity <fqn>` prints the exact reason.

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
