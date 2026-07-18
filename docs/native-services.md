# Native Control Plane


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
│    crate v0.4.14 | protocol v1.0.0                                          │
└────────────────────────────────────────────────────────────────────────────┘
```
Alongside the data plane that reads and writes your app's tables, UDB 0.4.14 ships
a **native control plane**: a set of built-in gRPC services that handle the plumbing
most applications end up building anyway. If you need login and access control, file
storage, asset pipelines, realtime coordination, multi-tenancy, notifications,
analytics, or policy distribution, these services are already here — you call them
instead of standing up your own.

You don't have to memorize the service list. The exact native service surface is
descriptor-owned, so the source of truth is always the generated native-service
docs rather than this page.

<p align="center">
  <img src="assets/control-plane.svg" alt="UDB public data-plane listener and separate native control-plane listener" width="940">
</p>

The native control plane runs on its own listener, separate from the public
`DataBroker` listener your app clients talk to. Treat it as internal: bind it to a
private network or put it behind a trusted gateway.

## Client layers

Every UDB SDK gives you three ways to call these services, from most convenient to
most raw. Reach for the highest layer that fits your task — most app code should
never hand-build a raw RPC request body.

1. **Workflow client (recommended).** The `UdbProject` facade does the stitching for
   you. It bundles the multi-step flows most apps need — login + tenant adoption,
   file upload (register → HTTP PUT → finalize), asset pipelines, WebRTC join,
   notification templates, read-after-write fences — into single calls, so you never
   wire the RPCs together by hand or re-send body authority. Identity always flows
   from the verified token, never from the request body.
   Per language: [`sdk/go/udbclient/project.go`](../sdk/go/udbclient/project.go),
   [`sdk/python/udb_client/project.py`](../sdk/python/udb_client/project.py),
   [`sdk/typescript/project.ts`](../sdk/typescript/project.ts),
   [`sdk/php/src/UdbProject.php`](../sdk/php/src/UdbProject.php).

   ```text
   // First-page example — workflow helper, never a raw GenericDispatch body:
   project := udb.LoginAndAdoptTenant(ctx, username, password)   // [Login, AuthenticateBearer]
   file    := project.Storage().UploadFile(ctx, name, bytes)     // [RegisterUpload, PUT, FinalizeUpload]
   room    := project.WebRTC().JoinSession(ctx, roomID)          // [JoinSession]
   ```

2. **Thin generated client (advanced / admin / bench).** This is the raw generated
   robustness client (`generatedClient.ts`, `generated_client.go`,
   `generated_client.py`, `Generated/GeneratedClient.php`). It maps every RPC
   one-to-one, including `GenericDispatch` for arbitrary data-plane SQL. Reach for it
   when you need a one-off admin call, a conformance or bench harness, or an RPC the
   workflow layer hasn't wrapped yet. When you do, watch the per-RPC **lifecycle
   preconditions** spelled out in the contract — for example, an approve→apply token,
   or an EnsureBaseline before a migration.

   One firm rule: Do **not** point `GenericDispatch` at the broker's internal `udb_*` schemas
   to repair native state. This is the No-Internal-Tables rule, enforced by
   `scripts/check-no-internal-tables.py`.

3. **Broker contracts (post-mutation guarantees).** This layer isn't code you call
   directly — it's the machine-readable per-RPC contract the other two layers rely
   on. The generated native-service docs describe each RPC's `operation_kind`/`read_only`
   (retry safety), its readback and lifecycle preconditions, its idempotency contract
   (`request_key_field`/`replay_safe`), and its typed error detail, all sourced from
   [`docs/generated/udb-native-contract.json`](generated/udb-native-contract.json).
   The workflow helpers read these contracts so they can honor write receipts and read
   fences for you — which is how a create or update becomes durably visible without a
   hot-path proof `Get`/`List`.

## Service Table

The native service surface is descriptor-rendered. The table below is a map: it
names each service and what it covers, so you can find the right one at a glance.
For the exact per-service RPC counts and listeners, read the generated table — those
numbers are descriptor-owned, not hand-maintained here.

| Service | Purpose |
|---|---|
| `AuthnService` | Login, sessions, JWT/JWKS, refresh tokens, MFA, OTP, devices, WebAuthn, user admin |
| `AuthzService` | RBAC, ABAC, ReBAC, access checks, policy bundles, native access, governance |
| `ApiKeyService` | API key creation, validation, rotation, revocation, usage stats |
| `IdentityProviderService` | OIDC, SAML, SCIM, JIT, external identity links |
| `ControlPlaneService` | Versioned policy/resource distribution with ACK/NACK |
| `TenantService` | Tenant and tenant config CRUD |
| `NotificationService` | Notifications, templates, preferences, delivery stats |
| `AnalyticsService` | Pipeline, executor, reconciliation, throughput, and SLA metrics |
| `StorageService` | Upload registration, finalize, download URLs, file metadata and lifecycle |
| `AssetService` | Asset records, pipeline definitions, pipeline runs, step completion |
| `VaultService` | Secret storage, transit encrypt/decrypt/sign/verify/HMAC, dynamic DB credential leases, seal status |
| `MeteringService` | Usage recording, quota policy, and usage/quota queries |
| `SchedulerService` | Cron and one-shot scheduled jobs (create, pause, resume, delete) |
| `SearchService` | Search index lifecycle, reindex, and query |
| `WebhookService` | SSRF-guarded outbound webhook endpoints and delivery tracking |
| `WorkflowService` | Durable workflows/sagas: start, signal, cancel, compensation |
| `LockService` | Distributed advisory locks with fencing tokens (acquire, renew, release) |
| `LiveQueryService` | Server-streaming subscriptions to live data-plane changes |
| `ConfigService` | Feature flags and flag evaluation |
| `BackupService` | Tenant backup/restore and backup policies |
| `EmbeddingService` | Embedding sources, backfill enumeration, and vector retrieval |
| `RoomService` / `PeerService` / `TrackService` | WebRTC room, peer, and track lifecycle (incl. egress/SFU) |
| `TurnService` / `SignalingService` | TURN credential issuance and the bidirectional signaling bridge |

Generated table with exact counts: [generated/native-services.md](generated/native-services.md).

## Platform Services

Beyond auth, identity, storage, and WebRTC, the native control plane ships a set of
platform services that cover the infrastructure many applications reach for. Each is
a first-class gRPC service on the native listener, and each gets the same tenant and
project scope guard, typed errors, and native-store persistence as the rest of the
control plane.

| Service | What it does | Representative RPCs |
|---|---|---|
| `VaultService` | Encrypted secrets, transit crypto, and short-lived database credentials | `PutSecret`, `GetSecret`, `Encrypt`/`Decrypt`, `Sign`/`Verify`, `Hmac`, `GenerateDatabaseCredentials`, `SealStatus` |
| `MeteringService` | Records usage events and enforces per-tenant quotas | `RecordUsage`, `QueryUsage`, `PutQuota`, `GetQuota`, `CheckQuota` |
| `SchedulerService` | Durable cron / one-shot job scheduling | `CreateJob`, `GetJob`, `ListJobs`, `PauseJob`, `ResumeJob`, `DeleteJob` |
| `SearchService` | Managed search indexes over app data | `CreateIndex`, `Reindex`, `Search`, `ListIndexes`, `DeleteIndex` |
| `WebhookService` | Outbound webhooks with SSRF-guarded endpoints and delivery logs | `CreateEndpoint`, `UpdateEndpoint`, `DeleteEndpoint`, `ListDeliveries` |
| `WorkflowService` | Long-running workflows / sagas with signals and compensation | `StartWorkflow`, `SignalWorkflow`, `CancelWorkflow`, `GetWorkflow`, `ListWorkflows` |
| `LockService` | Distributed advisory locks with monotonic fencing tokens | `AcquireLock`, `RenewLock`, `ReleaseLock` |
| `LiveQueryService` | Streams live changes to a subscribed query | `Subscribe` (server-streaming) |
| `ConfigService` | Feature flags and evaluation | `PutFlag`, `GetFlag`, `ListFlags`, `EvaluateFlags`, `DeleteFlag` |
| `BackupService` | Tenant-scoped backup and restore | `StartTenantBackup`, `RestoreTenant`, `ListBackups`, `PutBackupPolicy` |
| `EmbeddingService` | Embedding sources and vector retrieval, with a leader-driven backfill enumerator | `RegisterSource`, `ReportEmbedding`, `Backfill`, `Retrieve`, `ListSources` |

These services don't have `UdbProject` workflow facades yet. Until they do, call them
through the **thin generated client** — the generated robustness layer
(`generatedClient.ts`, `generated_client.go`, `generated_client.py`,
`Generated/GeneratedClient.php`) reaches every RPC. Python also exposes a per-service
stub (`VaultServiceClient`, `MeteringServiceClient`, …), and every language offers a
generic unary-by-full-method-path escape hatch. For example:

```python
# Python — per-service generated stub, verified identity flows from the token.
vault = VaultServiceClient(channel)
vault.create_transit_key(CreateTransitKeyRequest(tenant_id=tid, key_name="docs"))
enc = vault.encrypt(EncryptRequest(tenant_id=tid, key_name="docs", plaintext=b"..."))
```

```ts
// TypeScript / Go — generic unary by full method path (same auth + retry path):
core.unary("udb.core.vault.services.v1.VaultService", "Encrypt", request, call);
// Go: gc.InvokeUnary(ctx, "/udb.core.vault.services.v1.VaultService/Encrypt", req, &reply)
```

The generated per-RPC contract for each of these — `operation_kind`/`read_only`,
idempotency key field, replay safety, and typed error detail — lives in
[`docs/generated/udb-native-contract.json`](generated/udb-native-contract.json).
To be clear, these are native-service RPCs: calling them is **not** the banned
`GenericDispatch` against internal `udb_*` schemas. You are calling each service's
own public API.

## Native Store Path

The native-store control-plane work (introduced in 0.3.5) aligns native services with
UDB's canonical descriptor pipeline. In practice, native services persist their state
through typed native entity stores and native runtime bindings — not through one-off
SQL strings or a separate KV-only shortcut.

Why go to that trouble? Because service state then inherits the same contract that
your app entities do: generated table metadata, tenant/project scope checks, conflict
and return-field semantics, CDC/outbox behavior, native-service event contracts, and
manifest drift gates. Notification and analytics have already moved to this model;
storage and asset flows follow the same runtime direction, while the object bytes
themselves stay in the configured object backend.

0.3.6 adds `StorageService.DownloadFile`, a server-streaming RPC that returns a
finalized file's bytes in `DownloadFileChunk` frames. On the happy path, SDK clients
prefer the presigned `GetDownloadUrl` and only fall back to `DownloadFile` streaming
when presigned-HTTP access isn't available — so on the common path, file bytes never
have to transit the broker. The storage service resolves the object bucket it
reads and writes from `UDB_STORAGE_BUCKET` / `UDB_STORAGE_OBJECT_BACKEND` (defaulting
to `minio` / `udb-storage`), which is independent of the data-plane object module's
`UDB_OBJECT_BUCKET`.

For operators, two things follow: native-service startup should fail closed when a
declared backend is missing, and release branches should keep
`docs/generated/udb-native-contract.json` synchronized with `udb native manifest`.

## Consistency, Write Receipts, And Read Fences

Ever written a row, read it back immediately, and gotten stale data? UDB makes
read-after-write correctness explicit instead of hoping a replica or projection has
caught up. Every mutation returns a **write receipt**; a later read can carry a
**read fence** built from that receipt, request a specific **consistency mode**, or
both. The SDK workflow helpers wire all of this for you, but the contract is public,
so advanced callers can drive it directly.

**Consistency mode** (`RequestContext.consistency_mode`, enum `ConsistencyMode`)
tells the broker how fresh a read must be:

| Mode | Meaning |
|---|---|
| `STRONG` | Read the primary; never a replica or projection |
| `READ_YOUR_WRITES` | Honor a read fence so the caller sees its own prior write |
| `BOUNDED_STALENESS` | Replica read within a bounded lag |
| `REPLICA_BOUNDED` | Replica read with an explicit max-lag budget |
| `EVENTUAL` | Any replica |
| `PROJECTION_OK` / `CACHE_OK` | A projection or cache may serve the read |

**Write receipt** (`MutationResponse.write_receipt`, message `WriteReceipt`) is the
durability proof a mutation returns:

| Field | Meaning |
|---|---|
| `source_lsn` | Backend log position of the commit |
| `outbox_seq` | Monotonic outbox sequence for the emitted event |
| `projection_task_ids` | Projection tasks that must finish for the write to be visible in a projection |
| `manifest_checksum` | Catalog manifest the write was validated against |
| `written_at_unix_ms` | Commit timestamp |

**Read fence** (`RequestContext.read_fence`, message `ReadFence`) is what a
follow-up read carries so the broker waits for the write to be visible:

| Field | Meaning |
|---|---|
| `min_outbox_lsn` | Minimum log position the read must observe |
| `projection_task_ids` | Projection tasks that must complete before the read returns |
| `max_wait_ms` | How long the read may block waiting for the fence |

**Durable idempotency and `was_duplicate`.** When you give a `Upsert`/`Delete` an
idempotency key (a non-empty key on a replay-safe mutation), UDB deduplicates it in
the same transaction as the write. If the same key arrives again, the replay returns
the stored first-writer response with `MutationResponse.was_duplicate = true` — so a
client retry or an accidental duplicate send never double-applies the write. A keyed
BatchUpsert dedups per item the same way.

**SDK helpers.** You rarely touch these fields by hand. The canonical ergonomic
surface (method names follow each language's conventions) is:

- `metadata.afterWrite(receipt)` — stamp the next request's read fence from a
  receipt (alias `withReadFence`), so the following read is read-your-writes.
- `readFenceFromReceipt(receipt)` — build a `ReadFence` without mutating metadata.
- the mutation result exposes `was_duplicate` so a caller can tell a replay from a
  fresh write.
- a replay-safe mutation is retried on a transient error **only when the caller
  supplies an idempotency key**. Without a key the SDK fails closed (no retry)
  rather than risk a double-apply — supply an idempotency key to make a mutation
  safely retryable.

## Authn And Authz

Authn covers login, logout, server-side sessions, refresh tokens, JWT validation,
UDB-issued JWTs, JWKS, password login, MFA, OTP, recovery codes, devices,
WebAuthn/passkeys, and user lifecycle APIs.

Authz covers RBAC, ABAC, simple ReBAC, role and relationship management, access
decision audits, policy bundles, native access grants, policy drafts, approvals,
activation, rollback, canaries, simulation, explanation, revisions, and bundle
invalidation.

`GetNativeAccess` lets a trusted server-side caller request short-lived, restricted
database access after an authz decision has been made. SDKs expose this as a native
access helper — keep it server-side, and never hand it to a browser or mobile client.

Authn capabilities include:

- static public-key or JWKS-based JWT validation;
- UDB-issued access tokens and refresh tokens;
- server-side session lifecycle;
- password login with MFA-ready account state;
- OTP and recovery-code workflows;
- device inventory and WebAuthn/passkey state;
- user lifecycle administration.

Authz capabilities include:

- role and role-binding management;
- attribute and relationship-aware checks;
- batch access decisions;
- access decision audit records;
- signed policy bundles for SDK-side caching;
- policy draft, approval, activation, rollback, simulation, and explanation
  workflows;
- native access grants after a successful decision.

Native access is for trusted server-side use. Browser or mobile clients should
call application APIs or the broker rather than receiving direct backend access.

## API Keys, Tenants, Notifications, And Analytics

`ApiKeyService` manages hashed API keys, scopes, validation, rotation,
revocation, and usage statistics.

`TenantService` manages tenant records and tenant configuration. Tenant status
and configuration are part of runtime admission and native-service checks.

`NotificationService` manages notification records, templates, preferences, and
delivery statistics. Notification events can flow through the same event/outbox
path as other UDB events.

`AnalyticsService` exposes operational summaries for pipelines, executors,
reconciliation, throughput, and SLA-style views. It's built for control-plane and
operator-facing dashboards, not for application-domain analytics.

## Identity Providers And SCIM

`IdentityProviderService` covers:

- OIDC provider registry and discovery;
- SAML metadata, login, and ACS flows;
- SCIM users and groups;
- JIT provisioning;
- external identity linking;
- group-to-role mapping previews.

Typical SCIM flow:

1. Create an IdP provider for the tenant.
2. Configure SCIM credentials and mapping rules.
3. Provision users and groups.
4. Map external groups to UDB roles through configured mappings.
5. Verify decisions with `CheckAccess` or SDK `can()`.

## Storage And Asset Workflows

`StorageService` manages object metadata and presigned access:

- register upload;
- finalize upload;
- get download URL;
- get/update/delete/list file metadata;
- enforce tenant scope and optional quotas.

`AssetService` builds on storage metadata:

- asset registration;
- reusable pipeline definitions;
- pipeline instances;
- step completion;
- asset listing and fetch;
- embedding/vector-ready asset workflows.

## WebRTC And Signalling

WebRTC services cover rooms, peers, tracks, TURN credentials, and bidirectional
signaling. Durable room/peer/track data is stored through the native control
plane; signaling fan-out is transient.

Optional WebSocket signalling is available for clients that cannot use gRPC
signaling directly.

| Setting | Purpose |
|---|---|
| `UDB_WEBRTC_GRPC_ADDR` | Optional peer-facing WebRTC listener |
| `UDB_TURN_URLS` | TURN servers advertised to clients |
| `UDB_TURN_SECRET` | TURN credential secret |
| `UDB_WS_SIGNALLING_ADDR` | Enables the WebSocket bridge |
| `UDB_WS_SIGNALLING_PROTOCOL` | `pixelstreaming` or `json-relay` |
| `UDB_WS_SIGNALLING_TOKEN` | Optional shared handshake token |

TLS for `wss://` is normally terminated by a gateway such as nginx or Envoy.

## Control Distribution

`ControlPlaneService` provides a versioned distribution API for policy and
resource state. It supports state-of-the-world and delta-style resource delivery
with ACK/NACK status.

Reach for it when gateways, SDK caches, or sidecars need to receive a known
policy/resource version and report back whether they accepted or rejected it.

## Event Model

Native services emit descriptor-declared events for lifecycle, audit, and operational
workflows. Every event envelope preserves the tenant id, project id, correlation id,
actor/service identity, operation, resource, schema version, and redaction context —
so downstream consumers always know who did what, to which resource, on whose behalf.

To inspect the events in machine-readable form, use the generated contract:

```bash
udb native manifest
```

## Configuration

| Setting | Purpose |
|---|---|
| `UDB_AUTH_GRPC_ADDR` | Native control-plane listener |
| `UDB_WEBRTC_GRPC_ADDR` | Optional peer-facing WebRTC listener |
| `UDB_JWT_PUBLIC_KEY` / `UDB_JWT_JWKS_URL` | JWT validation |
| `UDB_JWT_PRIVATE_KEY` | UDB-issued token signing |
| `UDB_POLICY_BUNDLE_SECRET` | Signed SDK policy bundles |
| `UDB_STORAGE_OBJECT_BACKEND` | Object backend used by storage service |
| `UDB_TURN_URLS` / `UDB_TURN_SECRET` | TURN credential configuration |

## SDK Facades

SDKs provide generated clients plus ergonomic facades.

| Facade | Examples |
|---|---|
| Auth | login, refresh, token validation |
| Authz | `can`, `require`, batch checks, native access |
| API keys | create, rotate, revoke |
| Tenant | tenant and config helpers |
| Notification | send, templates, preferences |
| Analytics | pipeline and SLA views |
| Storage | upload registration, finalize, download URLs, metadata |
| Asset | pipeline definitions, runs, step completion |
| WebRTC | rooms, peers, tracks, TURN, signaling |

Framework adapters are available for common server frameworks, including
Express, Fastify, Next.js, FastAPI, Starlette, Go HTTP/gRPC middleware, Spring,
ASP.NET Core, and Laravel.

## Inspect The Contract

```bash
udb native list --json
udb native manifest
udb native docs
```
