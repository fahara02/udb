# UDB SDK live-test coverage — four live harnesses (Go · TypeScript · Python · PHP)

**COVERAGE: 264/264 (100%) RPCs exercised in every SDK** — the full generated surface is probed, not one RPC and a guess. Of the 264: 243 non-destructive RPCs receive a **field-populated** typed request (real decode + tenant/validation + handler-entry across the whole message), and the 21 destructive RPCs (abac/catalog/revoke-all/emergency/reset family) are sent a **typed-empty** request so the handler's validation runs but the destructive action never executes (populating them would corrupt shared broker state).

This is not just a mount ping. Beyond the full-surface probe, each SDK drives real **create→read→assert** lifecycles against every backend and native service, a full **session lifecycle** (logout must invalidate the token + refresh family), and **fail-closed edge cases** (wrong password mints no token; a forged bearer never validates/introspects-active).

✅ deep = explicit CRUD/lifecycle/decision with value assertions; 🟢 populated = field-populated typed request via the descriptor probe; 🟠 = destructive family sent a typed-empty request (validation exercised, action suppressed).

## SDK Coverage Split

UDB 0.3.5 ships six generated SDKs. Static conformance runs across Go,
TypeScript, Python, C#, Java, and PHP; the deep live broker matrix below covers
the four SDKs that currently own live broker harnesses: Go, TypeScript, Python,
and PHP.

## Live-SDK parity matrix

The four SDKs are at parity — each runs the same five layers against the same 264-RPC surface. The probe is **descriptor-driven** (Go/Python via proto reflection, TS via proto-loader field-stripping, PHP via setter reflection — never a hardcoded field/RPC list).

| Layer | Go | TypeScript | Python | PHP |
|---|---|---|---|---|
| Full-surface probe (264/264) | ✅ | ✅ | ✅ | ✅ |
| Populated-request floor asserted | ≥230 | ≥200 | ≥230 | ≥230 |
| Descriptor-equivalent field population (every scalar + context) | ✅ | ✅ | ✅ | ✅ (reflection over `set*`) |
| Backend E2E round-trips (postgres/mongo/minio) | ✅ | ✅ | ✅ | ✅ |
| Native-service E2E (tenant/authz/apikey/analytics/notification/storage/asset/webrtc) | ✅ | ✅ | ✅ | ✅ |
| Capability-claim check (every advertised backend reachable) | ✅ | ✅ | ✅ | ✅ |
| Backend-capability **claim challenge** (claimed ops admitted, unclaimed ops refused with the declared code) | ✅ | ✅ | ✅ | ✅ |
| **All-backend-kinds matrix** (real round-trip per advertised backend KIND) | ✅ | ✅ | ✅ | ✅¹ |
| Session lifecycle (logout invalidates token+refresh+session-refresh) | ✅ | ✅ | ✅ | ✅ |
| Auth-negative edge cases (bad password, forged bearer fail closed) | ✅ | ✅ | ✅ | ✅ |

¹ PHP exercises the object kind as a bucket-lifecycle reachability check (`ensure_resource`+`list_resources`); Go/Python/TS assert the full streaming `PutObject`→`GetObject` body round-trip. All four assert relational/document/cache/vector/graph round-trips identically.

PHP note: previously the PHP probe populated only 8 hardcoded fields per request (`tenant/project/domain/message_type/purpose/page/page_size/limit`) and asserted a loose `populated ≥ 80` floor — a shallow "one field and done" probe that left most of each request empty. It now populates **every** scalar field on every request via reflection over the generated `set*` setters (verified: e.g. CreateRole 3→9, SendNotification 2→10, CreateApiKey 0→5 non-default scalar fields) and asserts the same `≥230` floor as Go/Python.

## Edge-case / negative coverage

Added across all four live harnesses (the suites were previously happy-path + mount-probe only):

- **Wrong-password login fails closed** — the auth plane must return an error or mint no access token; it must never hand back a usable token for a bad credential.
- **Forged/garbage bearer is rejected** — `ValidateToken` returns `valid=false` and `IntrospectToken` returns `active=false` for a non-JWT token; neither may report the token usable.
- **Logout truly revokes** (session lifecycle) — post-logout the access token must stop validating, the refresh token's family must be revoked, and session-refresh must fail.
- **Capability honesty** — every backend `GetCapabilities` advertises must answer a real `ListResources` (a mount/unavailable failure is a capability lie, not tolerated).
- **Destructive-suppression safety** — the 21 destructive RPCs are sent typed-empty so validation runs without executing the mutation.

Each negative call still must reach real handler logic — a mount failure (UNIMPLEMENTED/UNAVAILABLE/UNKNOWN) is fatal, proving the negative paths are wired, not just the positive ones.

## Backend-kind coverage — challenging ALL backend kinds, not just postgres/mongodb/minio

UDB's DataBroker supports **18 backend kinds** across 7 store categories (`src/backend/mod.rs`). The original suites round-tripped only **postgres / mongodb / minio** — one backend in three categories, nothing for cache, vector, or graph. Two layers now challenge the **full** backend surface, driven entirely by what `GetCapabilities` advertises (no hardcoded backend list):

### Layer 1 — capability claim challenge (`runLiveBackendCapabilityChallenge`)

For every `BackendCapabilityDescriptor` the broker publishes, in **both directions** through `GenericDispatch` (the single op-gated entry point all backends share):
- **Shape:** non-empty backend + tier + operations list; `unsupported_error_code == "UDB_UNSUPPORTED_OPERATION"`.
- **Positive:** each claimed side-effect-free op (`ping`/`probe`/`list_resources`) must be **admitted** (never refused as unsupported, never a mount failure).
- **Negative:** the first canonical op the backend does **not** claim must be **refused** carrying the declared `UDB_UNSUPPORTED_OPERATION` code — catching silent over-claim (admitting an unadvertised op) and false-deny alike.
- **No orphans:** every `enabled_backends` entry must publish a capability descriptor.

The op vocabulary (`ping, probe, list_resources, search, query, transaction, get_object, put_object, mutate, ensure_resource, drop_resource`) is taken verbatim from `src/runtime/service/mod.rs check_generic_dispatch_operation`.

### Layer 2 — all-backend-kinds round-trip matrix (`runLiveAllBackendKindsMatrix`)

For **every advertised backend**, the suite reads its store category from the capability tier+ops and drives the matching DataBroker RPC family with a real round-trip:

| Store category (tier) | Backends (when enabled) | RPC round-trip exercised |
|---|---|---|
| relational (`sql`, `column`) | postgres, mysql, sqlite, sqlserver, clickhouse, cassandra | `GenericDispatch` `query` (`SELECT 1`) — portable, message-type-agnostic |
| object | minio, s3, azureblob, gcs | `EnsureResource` → `PutObject` → `GetObject` (body asserted; PHP: lifecycle reachability) |
| document | mongodb | `EnsureResource` → `DocumentUpsert` → `DocumentGet` → `DocumentDelete` |
| cache | redis, memcached | `CacheSet` → `CacheGet` (value asserted) → `CacheScan` → `CacheDelete` |
| vector | qdrant, weaviate, pinecone, elasticsearch | `EnsureResource` → `VectorUpsert` → `VectorSearch` |
| graph | neo4j | `GraphMutate` (Cypher CREATE) → `GraphQuery` (MATCH) |

**Capability-driven, so it scales for free:** the default CI broker (postgres+mongodb+minio) runs the relational/document/object arms — which validates the harness itself — and the moment a richer broker enables mysql/mssql/clickhouse/cassandra, redis/memcached, qdrant/weaviate/pinecone/elasticsearch or neo4j, the **same** suite automatically extends coverage to them with **no test change**. Each arm fails closed on a wiring gap (a claimed RPC that returns a mount failure), tolerates genuine per-backend business quirks, and asserts values on success.

> **Native-service persistence is moving through the 0.3.5 native-store path.** This matrix still challenges the **data plane** across all backend kinds; the acceptance test for backend-agnostic native-service persistence is the same matrix run with `UDB_NATIVE_STORE=<backend>` against a broker whose native services are bound to that backend.

Static verification (no broker required): `go vet` 0 · `python -m py_compile` clean · `php -l` clean · `tsc -p tsconfig.test.json --noEmit` 0. Live verification runs via `scripts/run-{go,ts,python,php}-live.ps1` against a bootstrapped broker.

---

## The 264-RPC surface every SDK probes (Go-side classification)

The list below is the canonical surface. The ✅/🟢/🟠 marks are the Go suite's per-RPC classification; the TS/Python/PHP suites probe the identical surface with the same destructive-suppression set (resolved from each SDK's proto-derived `operation_kind`, never a hardcoded name list).


## DataBroker (77/77 covered)

- [x] `ActivateCatalog` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `AnalyticalQuery` — 🟢 populated (typed request, handler exercised)
- [x] `ApplyMigration` — 🟢 populated (typed request, handler exercised)
- [x] `ApproveMigrationPlan` — 🟢 populated (typed request, handler exercised)
- [x] `BatchSelect` — ✅ deep (value asserted)
- [x] `BatchUpsert` — ✅ deep (value asserted)
- [x] `BeginTx` — 🟢 populated (typed request, handler exercised)
- [x] `CacheDelete` — 🟢 populated (typed request, handler exercised)
- [x] `CacheGet` — 🟢 populated (typed request, handler exercised)
- [x] `CacheScan` — 🟢 populated (typed request, handler exercised)
- [x] `CacheSet` — 🟢 populated (typed request, handler exercised)
- [x] `CreateMaterializedView` — 🟢 populated (typed request, handler exercised)
- [x] `Delete` — ✅ deep (value asserted)
- [x] `DeletePolicy` — 🟢 populated (typed request, handler exercised)
- [x] `DismissDlqEvent` — 🟢 populated (typed request, handler exercised)
- [x] `DocumentDelete` — ✅ deep (value asserted)
- [x] `DocumentFind` — ✅ deep (value asserted)
- [x] `DocumentGet` — ✅ deep (value asserted)
- [x] `DocumentUpsert` — ✅ deep (value asserted)
- [x] `DropResource` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `EnqueueOutboxEvent` — 🟢 populated (typed request, handler exercised)
- [x] `EnsureBaseline` — 🟢 populated (typed request, handler exercised)
- [x] `EnsureProject` — ✅ deep (value asserted)
- [x] `EnsureResource` — ✅ deep (value asserted)
- [x] `GeneratePresignedUrl` — ✅ deep (value asserted)
- [x] `GenericDispatch` — ✅ deep (value asserted)
- [x] `GetAdminSummary` — ✅ deep (value asserted)
- [x] `GetCapabilities` — ✅ deep (value asserted)
- [x] `GetCatalogManifest` — ✅ deep (value asserted)
- [x] `GetCatalogVersion` — 🟢 populated (typed request, handler exercised)
- [x] `GetCatalogVersions` — ✅ deep (value asserted)
- [x] `GetCdcStatus` — ✅ deep (value asserted)
- [x] `GetDlqEvent` — 🟢 populated (typed request, handler exercised)
- [x] `GetHealthReport` — ✅ deep (value asserted)
- [x] `GetMigrationStatus` — 🟢 populated (typed request, handler exercised)
- [x] `GetObject` — ✅ deep (value asserted)
- [x] `GetSaga` — 🟢 populated (typed request, handler exercised)
- [x] `GraphMutate` — 🟢 populated (typed request, handler exercised)
- [x] `GraphQuery` — 🟢 populated (typed request, handler exercised)
- [x] `InitiateMultipartUpload` — 🟢 populated (typed request, handler exercised)
- [x] `LintPolicies` — ✅ deep (value asserted)
- [x] `ListAdminAuditLogs` — ✅ deep (value asserted)
- [x] `ListDlqEvents` — ✅ deep (value asserted)
- [x] `ListMessageSchemas` — ✅ deep (value asserted)
- [x] `ListMigrationRuns` — ✅ deep (value asserted)
- [x] `ListPolicies` — ✅ deep (value asserted)
- [x] `ListProjects` — ✅ deep (value asserted)
- [x] `ListResources` — ✅ deep (value asserted)
- [x] `ListSagas` — ✅ deep (value asserted)
- [x] `LookupMessageSchema` — ✅ deep (value asserted)
- [x] `MarkSagaReviewed` — 🟢 populated (typed request, handler exercised)
- [x] `PauseCdc` — 🟢 populated (typed request, handler exercised)
- [x] `PlanMigration` — 🟢 populated (typed request, handler exercised)
- [x] `PreviewCdcRedaction` — 🟢 populated (typed request, handler exercised)
- [x] `PublishCDC` — 🟢 populated (typed request, handler exercised)
- [x] `PutObject` — ✅ deep (value asserted)
- [x] `PutPolicy` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `QuarantineDlqEvent` — 🟢 populated (typed request, handler exercised)
- [x] `ReloadPolicies` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `ReplayDlqEvent` — 🟢 populated (typed request, handler exercised)
- [x] `ResumeCdc` — 🟢 populated (typed request, handler exercised)
- [x] `RetrySagaCompensation` — 🟢 populated (typed request, handler exercised)
- [x] `RollbackCatalog` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `ScanProjectionDrift` — 🟢 populated (typed request, handler exercised)
- [x] `Select` — ✅ deep (value asserted)
- [x] `SelectV2` — ✅ deep (value asserted)
- [x] `StageCatalog` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `StepDownCdcLeader` — 🟢 populated (typed request, handler exercised)
- [x] `TimeSeriesQuery` — 🟢 populated (typed request, handler exercised)
- [x] `TimeSeriesWrite` — 🟢 populated (typed request, handler exercised)
- [x] `Upsert` — ✅ deep (value asserted)
- [x] `ValidateCatalog` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `VectorBatchUpsert` — 🟢 populated (typed request, handler exercised)
- [x] `VectorHybridSearch` — 🟢 populated (typed request, handler exercised)
- [x] `VectorSearch` — 🟢 populated (typed request, handler exercised)
- [x] `VectorUpsert` — 🟢 populated (typed request, handler exercised)
- [x] `VerifyAdminAuditLog` — 🟢 populated (typed request, handler exercised)

## AuthnService (50/50 covered)

- [x] `AdminResetMfa` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `AdminResetPassword` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `AdminRevokeAllTenantSessions` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `AdminRevokeAllUserSessions` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `AdminRevokeSession` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `Authenticate` — ✅ deep (value asserted)
- [x] `ChangePassword` — ✅ deep (value asserted)
- [x] `ChangeUserStatus` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `ConfirmMFAEnrollment` — 🟢 populated (typed request, handler exercised)
- [x] `CreateSession` — 🟢 populated (typed request, handler exercised)
- [x] `CreateUser` — ✅ deep (value asserted)
- [x] `DeleteWebAuthnCredential` — 🟢 populated (typed request, handler exercised)
- [x] `DisableMfaFactor` — 🟢 populated (typed request, handler exercised)
- [x] `EmergencyRevoke` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `EnrollMFA` — 🟢 populated (typed request, handler exercised)
- [x] `FinishWebAuthnAuthentication` — 🟢 populated (typed request, handler exercised)
- [x] `FinishWebAuthnRegistration` — 🟢 populated (typed request, handler exercised)
- [x] `ForgotPassword` — 🟢 populated (typed request, handler exercised)
- [x] `GenerateRecoveryCodes` — ✅ deep (value asserted)
- [x] `GetJwks` — ✅ deep (value asserted)
- [x] `GetMfaPolicy` — ✅ deep (value asserted)
- [x] `GetSession` — ✅ deep (value asserted)
- [x] `GetUser` — ✅ deep (value asserted)
- [x] `IntrospectToken` — ✅ deep (value asserted)
- [x] `IssueMfaChallenge` — 🟢 populated (typed request, handler exercised)
- [x] `ListDevices` — ✅ deep (value asserted)
- [x] `ListMfaFactors` — ✅ deep (value asserted)
- [x] `ListSessions` — ✅ deep (value asserted)
- [x] `ListUsers` — ✅ deep (value asserted)
- [x] `ListWebAuthnCredentials` — ✅ deep (value asserted)
- [x] `Login` — ✅ deep (value asserted)
- [x] `Logout` — ✅ deep (value asserted)
- [x] `PutMfaPolicy` — 🟢 populated (typed request, handler exercised)
- [x] `RefreshSession` — ✅ deep (value asserted)
- [x] `RefreshToken` — ✅ deep (value asserted)
- [x] `RenamePasskey` — 🟢 populated (typed request, handler exercised)
- [x] `ResendOTP` — 🟢 populated (typed request, handler exercised)
- [x] `ResetPassword` — 🟢 populated (typed request, handler exercised)
- [x] `RevokeDevice` — 🟢 populated (typed request, handler exercised)
- [x] `RevokeRecoveryCodes` — 🟢 populated (typed request, handler exercised)
- [x] `RevokeSession` — ✅ deep (value asserted)
- [x] `SendOTP` — 🟢 populated (typed request, handler exercised)
- [x] `SendPhoneVerification` — 🟢 populated (typed request, handler exercised)
- [x] `StartWebAuthnAuthentication` — 🟢 populated (typed request, handler exercised)
- [x] `StartWebAuthnRegistration` — 🟢 populated (typed request, handler exercised)
- [x] `UpdateUser` — ✅ deep (value asserted)
- [x] `ValidateCSRF` — ✅ deep (value asserted)
- [x] `ValidateToken` — ✅ deep (value asserted)
- [x] `VerifyMfaChallenge` — 🟢 populated (typed request, handler exercised)
- [x] `VerifyOTP` — 🟢 populated (typed request, handler exercised)

## AuthzService (41/41 covered)

- [x] `ActivateCanary` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `ActivatePolicyVersion` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `ApprovePolicyDraft` — 🟢 populated (typed request, handler exercised)
- [x] `AssignRole` — ✅ deep (value asserted)
- [x] `Authorize` — ✅ deep (value asserted)
- [x] `BatchCheckPermissions` — ✅ deep (value asserted)
- [x] `CheckAccess` — ✅ deep (value asserted)
- [x] `CreatePolicyDraft` — 🟢 populated (typed request, handler exercised)
- [x] `CreatePolicyRule` — ✅ deep (value asserted)
- [x] `CreateRole` — ✅ deep (value asserted)
- [x] `DeletePolicyRule` — ✅ deep (value asserted)
- [x] `DeleteRole` — ✅ deep (value asserted)
- [x] `DiffPolicyDraft` — 🟢 populated (typed request, handler exercised)
- [x] `ExplainPolicy` — 🟢 populated (typed request, handler exercised)
- [x] `GetAuthzRevision` — ✅ deep (value asserted)
- [x] `GetCanaryStatus` — 🟢 populated (typed request, handler exercised)
- [x] `GetNativeAccess` — 🟢 populated (typed request, handler exercised)
- [x] `GetPolicyBundle` — ✅ deep (value asserted)
- [x] `GetPolicyRule` — ✅ deep (value asserted)
- [x] `GetRole` — ✅ deep (value asserted)
- [x] `InvalidatePolicyBundles` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `LintAuthzPolicies` — ✅ deep (value asserted)
- [x] `ListAccessDecisionAudits` — ✅ deep (value asserted)
- [x] `ListPolicyRules` — ✅ deep (value asserted)
- [x] `ListPolicyVersions` — 🟢 populated (typed request, handler exercised)
- [x] `ListRoles` — ✅ deep (value asserted)
- [x] `ListUserPermissions` — ✅ deep (value asserted)
- [x] `ListUserRoles` — ✅ deep (value asserted)
- [x] `MigrateLegacyPolicies` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `PromoteCanary` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `PutAuthzPolicy` — ✅ deep (value asserted)
- [x] `PutRelationship` — ✅ deep (value asserted)
- [x] `PutRoleBinding` — ✅ deep (value asserted)
- [x] `RejectPolicyDraft` — 🟢 populated (typed request, handler exercised)
- [x] `RevokeRole` — ✅ deep (value asserted)
- [x] `RollbackPolicyVersion` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `SeedBuiltinRoles` — 🟢 populated (typed request, handler exercised)
- [x] `SimulatePolicy` — 🟢 populated (typed request, handler exercised)
- [x] `SubmitPolicyDraft` — 🟢 populated (typed request, handler exercised)
- [x] `UpdatePolicyDraft` — 🟢 populated (typed request, handler exercised)
- [x] `UpdateRole` — ✅ deep (value asserted)

## IdentityProviderService (27/27 covered)

- [x] `CreateProvider` — ✅ deep (value asserted)
- [x] `DisableProvider` — ✅ deep (value asserted)
- [x] `ForceJwksRefresh` — 🟢 populated (typed request, handler exercised)
- [x] `GetProvider` — ✅ deep (value asserted)
- [x] `ImportSamlMetadata` — 🟢 populated (typed request, handler exercised)
- [x] `LinkIdentity` — 🟢 populated (typed request, handler exercised)
- [x] `ListExternalIdentities` — 🟢 populated (typed request, handler exercised)
- [x] `ListProviders` — ✅ deep (value asserted)
- [x] `PreviewClaimMapping` — 🟢 populated (typed request, handler exercised)
- [x] `PreviewGroupMapping` — 🟢 populated (typed request, handler exercised)
- [x] `ResolveExternalIdentity` — 🟢 populated (typed request, handler exercised)
- [x] `SamlAcs` — 🟢 populated (typed request, handler exercised)
- [x] `ScimCreateGroup` — 🟢 populated (typed request, handler exercised)
- [x] `ScimCreateUser` — 🟢 populated (typed request, handler exercised)
- [x] `ScimDeleteGroup` — 🟢 populated (typed request, handler exercised)
- [x] `ScimDeleteUser` — 🟢 populated (typed request, handler exercised)
- [x] `ScimGetGroup` — 🟢 populated (typed request, handler exercised)
- [x] `ScimGetUser` — 🟢 populated (typed request, handler exercised)
- [x] `ScimListGroups` — 🟢 populated (typed request, handler exercised)
- [x] `ScimListUsers` — 🟢 populated (typed request, handler exercised)
- [x] `ScimPatchGroup` — 🟢 populated (typed request, handler exercised)
- [x] `ScimPatchUser` — 🟢 populated (typed request, handler exercised)
- [x] `ScimReplaceUser` — 🟢 populated (typed request, handler exercised)
- [x] `StartSamlLogin` — 🟢 populated (typed request, handler exercised)
- [x] `TestProviderDiscovery` — 🟢 populated (typed request, handler exercised)
- [x] `UnlinkIdentity` — 🟢 populated (typed request, handler exercised)
- [x] `UpdateProvider` — 🟢 populated (typed request, handler exercised)

## NotificationService (11/11 covered)

- [x] `GetDeliveryStats` — ✅ deep (value asserted)
- [x] `GetNotification` — ✅ deep (value asserted)
- [x] `GetPreference` — ✅ deep (value asserted)
- [x] `GetTemplate` — ✅ deep (value asserted)
- [x] `ListNotifications` — ✅ deep (value asserted)
- [x] `ListPreferences` — ✅ deep (value asserted)
- [x] `ListTemplates` — 🟢 populated (typed request, handler exercised)
- [x] `RetryNotification` — 🟢 populated (typed request, handler exercised)
- [x] `SendNotification` — ✅ deep (value asserted)
- [x] `SetPreference` — ✅ deep (value asserted)
- [x] `UpsertTemplate` — ✅ deep (value asserted)

## ApiKeyService (9/9 covered)

- [x] `CreateApiKey` — ✅ deep (value asserted)
- [x] `EmergencyRevokeApiKeys` — 🟠 safe-typed-empty (dangerous mutation — validation exercised)
- [x] `GetApiKey` — ✅ deep (value asserted)
- [x] `GetApiKeyUsageStats` — 🟢 populated (typed request, handler exercised)
- [x] `ListApiKeys` — ✅ deep (value asserted)
- [x] `RevokeApiKey` — ✅ deep (value asserted)
- [x] `RotateApiKey` — 🟢 populated (typed request, handler exercised)
- [x] `UpdateApiKey` — ✅ deep (value asserted)
- [x] `ValidateApiKey` — ✅ deep (value asserted)

## AssetService (8/8 covered)

- [x] `CompleteStep` — 🟢 populated (typed request, handler exercised)
- [x] `CreatePipelineDefinition` — ✅ deep (value asserted)
- [x] `GetAsset` — ✅ deep (value asserted)
- [x] `GetPipeline` — ✅ deep (value asserted)
- [x] `GetPipelineDefinition` — ✅ deep (value asserted)
- [x] `ListAssets` — ✅ deep (value asserted)
- [x] `RegisterAsset` — ✅ deep (value asserted)
- [x] `StartPipeline` — ✅ deep (value asserted)

## StorageService (7/7 covered)

- [x] `DeleteFile` — ✅ deep (value asserted)
- [x] `FinalizeUpload` — 🟢 populated (typed request, handler exercised)
- [x] `GetDownloadUrl` — ✅ deep (value asserted)
- [x] `GetFile` — ✅ deep (value asserted)
- [x] `ListFiles` — ✅ deep (value asserted)
- [x] `RegisterUpload` — ✅ deep (value asserted)
- [x] `UpdateFile` — ✅ deep (value asserted)

## AnalyticsService (7/7 covered)

- [x] `GetExecutorPerformance` — 🟢 populated (typed request, handler exercised)
- [x] `GetPipelineSummary` — ✅ deep (value asserted)
- [x] `GetReconciliationAnalytics` — 🟢 populated (typed request, handler exercised)
- [x] `GetSlaCompliance` — 🟢 populated (typed request, handler exercised)
- [x] `GetThroughput` — ✅ deep (value asserted)
- [x] `RecordPipelineMetric` — ✅ deep (value asserted)
- [x] `TriggerSnapshot` — ✅ deep (value asserted)

## TenantService (6/6 covered)

- [x] `CreateTenant` — ✅ deep (value asserted)
- [x] `GetTenant` — 🟢 populated (typed request, handler exercised)
- [x] `GetTenantConfig` — ✅ deep (value asserted)
- [x] `ListTenants` — 🟢 populated (typed request, handler exercised)
- [x] `UpdateTenant` — 🟢 populated (typed request, handler exercised)
- [x] `UpdateTenantConfig` — ✅ deep (value asserted)

## RoomService (5/5 covered)

- [x] `CloseRoom` — ✅ deep (value asserted)
- [x] `CreateRoom` — ✅ deep (value asserted)
- [x] `GetRoom` — ✅ deep (value asserted)
- [x] `ListRooms` — ✅ deep (value asserted)
- [x] `UpdateRoom` — ✅ deep (value asserted)

## PeerService (5/5 covered)

- [x] `GetPeer` — ✅ deep (value asserted)
- [x] `JoinRoom` — ✅ deep (value asserted)
- [x] `JoinSession` — 🟢 populated (typed request, handler exercised)
- [x] `LeaveRoom` — ✅ deep (value asserted)
- [x] `ListPeers` — ✅ deep (value asserted)

## TrackService (4/4 covered)

- [x] `ListTracks` — ✅ deep (value asserted)
- [x] `MuteTrack` — ✅ deep (value asserted)
- [x] `PublishTrack` — ✅ deep (value asserted)
- [x] `UnpublishTrack` — ✅ deep (value asserted)

## TurnService (1/1 covered)

- [x] `IssueCredentials` — ✅ deep (value asserted)

## ControlPlaneService (5/5 covered)

- [x] `AckStatus` — 🟢 populated (typed request, handler exercised)
- [x] `DeltaResources` — 🟢 populated (typed request, handler exercised)
- [x] `GetResources` — 🟢 populated (typed request, handler exercised)
- [x] `ListNodeStates` — 🟢 populated (typed request, handler exercised)
- [x] `StreamResources` — 🟢 populated (typed request, handler exercised)

## SignalingService (1/1 covered)

- [x] `Signal` — 🟢 populated (typed request, handler exercised)
