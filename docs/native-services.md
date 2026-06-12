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
│    crate v0.3.5 | protocol v1.0.0                                          │
└────────────────────────────────────────────────────────────────────────────┘
```
UDB includes a native control plane for identity, access, storage metadata,
asset workflows, realtime coordination, tenancy, notifications, analytics, and
policy distribution.

<p align="center">
  <img src="assets/control-plane.svg" alt="UDB public data-plane listener and separate native control-plane listener" width="940">
</p>

The native control plane is separate from the public `DataBroker` listener. Bind
it to an internal network or place it behind a trusted gateway.

## Service Table

UDB 0.3.5 exposes 15 native services with 186 native RPCs.

| Service | RPCs | Purpose |
|---|---:|---|
| `AuthnService` | 50 | Login, sessions, JWT/JWKS, refresh tokens, MFA, OTP, devices, WebAuthn, user admin |
| `AuthzService` | 41 | RBAC, ABAC, ReBAC, access checks, policy bundles, native access, governance |
| `ApiKeyService` | 9 | API key creation, validation, rotation, revocation, usage stats |
| `IdentityProviderService` | 27 | OIDC, SAML, SCIM, JIT, external identity links |
| `ControlPlaneService` | 5 | Versioned policy/resource distribution with ACK/NACK |
| `TenantService` | 6 | Tenant and tenant config CRUD |
| `NotificationService` | 11 | Notifications, templates, preferences, delivery stats |
| `AnalyticsService` | 7 | Pipeline, executor, reconciliation, throughput, and SLA metrics |
| `StorageService` | 7 | Upload registration, finalize, download URLs, file metadata and lifecycle |
| `AssetService` | 8 | Asset records, pipeline definitions, pipeline runs, step completion |
| `RoomService` | 5 | WebRTC room lifecycle |
| `PeerService` | 4 | WebRTC peer lifecycle |
| `TrackService` | 4 | WebRTC track lifecycle |
| `TurnService` | 1 | TURN credential issuance |
| `SignalingService` | 1 | Bidirectional WebRTC signaling bridge |

Generated table: [generated/native-services.md](generated/native-services.md).

## 0.3.5 Native Store Path

The 0.3.5 control-plane work aligns native services with UDB's canonical
descriptor pipeline. Native services should persist through typed native entity
stores and native runtime bindings, not through one-off SQL strings or a
separate KV-only shortcut.

That matters because service state then inherits the same contract that app
entities do: generated table metadata, tenant/project scope checks, conflict and
return-field semantics, CDC/outbox behavior, native-service event contracts, and
manifest drift gates. Notification and analytics are part of this move in
0.3.5; storage and asset flows use the same runtime direction while object bytes
remain in the configured object backend.

For operators, this means native-service startup should fail closed when a
declared backend is missing, and release branches should keep
`docs/generated/udb-native-contract.json` synchronized with
`udb native manifest`.

## Authn And Authz

Authn covers login, logout, server-side sessions, refresh tokens, JWT validation,
UDB-issued JWTs, JWKS, password login, MFA, OTP, recovery codes, devices,
WebAuthn/passkeys, and user lifecycle APIs.

Authz covers RBAC, ABAC, simple ReBAC, role and relationship management, access
decision audits, policy bundles, native access grants, policy drafts, approvals,
activation, rollback, canaries, simulation, explanation, revisions, and bundle
invalidation.

`GetNativeAccess` lets a trusted server-side caller request short-lived,
restricted database access after an authz decision. SDKs expose this as a native
access helper; applications should keep it server-side.

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
reconciliation, throughput, and SLA-style views. It is intended for control
plane and operator-facing views rather than application-domain analytics.

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

This service is useful when gateways, SDK caches, or sidecars need to receive a
known policy/resource version and report whether they accepted or rejected it.

## Event Model

Native services emit descriptor-declared events for lifecycle, audit, and
operational workflows. Event envelopes preserve tenant id, project id,
correlation id, actor/service identity, operation, resource, schema version, and
redaction context.

Use the generated contract for machine-readable inspection:

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
