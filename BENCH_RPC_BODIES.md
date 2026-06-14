# UDB benchmark — per-RPC valid request bodies

Grounded checklist of **all 262 RPCs** and the **valid request body** each needs so the
live perf bench drives its real success path (not a placeholder that the broker rejects
with `INVALID_ARGUMENT`). Every field below is read from the actual proto request message
— not guessed. Drive the harness from this; tick `done` as each RPC goes green.

## Why this exists
The published bench was ~70% red because requests were generically populated (every scalar
= a placeholder, IDs = `"1"`). The broker rejects those: the failure histogram was
**INVALID_ARGUMENT-dominant** (~65 Go / ~90 Python), then INTERNAL, PERMISSION_DENIED,
NOT_FOUND. Seeding IDs alone is not enough — each RPC needs a semantically-valid body.
This file is the source of truth for those bodies.

## How to read the tables
`| done | RPC | op_kind | request msg | valid body | seed refs / notes |`
- **valid body** — the concrete fields to send. Scalars are literals; enums are a valid
  non-`*_UNSPECIFIED` value; ID/reference fields are `<seed:KEY>` (resolved from the seed
  fixtures below); nested messages are expanded; `repeated` shows one valid element.
- **op_kind** — `read_only` / `mutation` / `destructive` (from the proto `operation_kind`).
- **seed refs / notes** — the seeded entity the body depends on, and any caveat
  (needs scope X → else PERMISSION_DENIED; needs a real prior-created id → else NOT_FOUND;
  needs an external peer/IdP/warm-Kafka → currently un-seedable).

## Seed-fixture keys (created once, before measuring; IDs threaded into bodies)
`tenant_id`, `project`, `message_type`, `record_id` (a seeded data row PK),
`bucket`, `object_key`, `document_id`, `mongo_collection`,
`user_id` (+ aliases `created_by`/`assigned_by`/`revoked_by`/`owner_id`/`subject`),
`session_id`, `token`, `refresh_token`, `csrf_token`, `code` (recovery),
`role`/`role_id`/`role_code`, `user_role_id`, `policy_id`, `policy_draft_id`,
`relation`/`object`/`resource`/`action`, `key_id`, `plain_key`, `stage_name`,
`template_id`/`log_id`/`event_type`, `file_id`,
`definition_id`/`asset_id`/`instance_id`, `room_id`/`peer_id`/`track_id`,
`migration_id`, `saga_id`, `provider_id`.

## Known un-seedable / broker-bug RPCs (call out, don't fake)
- `DataBroker/PublishCDC` first-event needs a **warm Kafka + CDC leader** (`UDB_KAFKA_BROKERS`).
- WebRTC `Signal`, ControlPlane `StreamResources`/`DeltaResources` (bidi) need a live peer.
- IdP SAML/SCIM/external-IdP RPCs need an external provider.
- `IdentityProviderService/CreateProvider` — broker `varchar(24)` overflow (use a ≤24-char id).
- DLQ/saga mutators need a real failed-pipeline DLQ event / saga id.

---

## Execution order — the auth route (MANDATORY sequencing)

The bench runs as ONE authenticated principal (the admin) plus a seeded disposable
user. Auth RPCs that end a session / invalidate a principal MUST run **last**, or they
kill the session mid-run and everything after fails. Drive the harness in three phases:

### Phase 1 — establish the session (run FIRST, in this order)
1. `AuthnService/Login` — get `token`, `session_id`, `refresh_token`, `csrf_token` → seed fixtures.
2. `AuthnService/RefreshToken` — exercise the token-family refresh.
3. `AuthnService/RefreshSession` — refresh the live session.
4. `AuthnService/Authenticate` + `ValidateToken` + `IntrospectToken` + `GetJwks` — validate (read-only).
(Also do the rest of the **seed phase** here: CreateUser→disposable user, Upsert rows, roles/policies/keys/templates/files/etc. — see seed-fixture legend.)

### Phase 2 — measure everything under the live session
ALL other RPCs across every service, **plus** the non-terminal AuthnService RPCs
(GetUser, ListUsers, UpdateUser, sessions read, MFA enroll/list, devices list,
recovery-code generate, WebAuthn start/list, phone/OTP send+verify, GetMfaPolicy,
ListMfaFactors, RenamePasskey, etc.). Destructive ops here MUST target the **seeded
disposable user** (`<seed:user_id>`) / its session — never the admin's own.

### Phase 3 — tear the session down (run LAST, after all measurement)
These end a session / invalidate a principal or credentials — run only at the end,
preferring the disposable seeded session, and the admin's own session truly last:
`Logout`, `RevokeSession`, `RevokeDevice`, `DisableMfaFactor`, `RevokeRecoveryCodes`,
`DeleteWebAuthnCredential`, `ChangePassword`, `ResetPassword`, `AdminResetPassword`,
`ChangeUserStatus`, `AdminResetMfa`, `AdminRevokeSession`, `AdminRevokeAllUserSessions`,
`AdminRevokeAllTenantSessions`, `EmergencyRevoke`.
(In each AuthnService row below, the `seed refs / notes` column marks the phase where it isn't Phase 2.)

<!-- SECTIONS ASSEMBLED BELOW (one per service proto) -->


## DataBroker
_proto: services/v1/data_broker.proto · 76 RPCs_

Every request type embeds `RequestContext context` (field 1) — defined in `udb/entity/v1/context.proto`. The
load-bearing context fields for a successful call are `tenant_id` = `<seed:tenant_id>`, `scopes` = the scope(s)
the method requires (e.g. `["udb:admin"]`), and optionally `project_id` = `<seed:project>`. In the table below
`ctx` is shorthand for `context{tenant_id:<seed:tenant_id>, scopes:[...]}`. All other context fields
(correlation_id, purpose, routing_policy, consistency, etc.) are optional and may be omitted.

`op_kind` enum (from `udb/core/common/v1/security.proto` `operation_kind` method option):
`OPERATION_KIND_READ_ONLY` = RO, `OPERATION_KIND_MUTATION` = MUT, `OPERATION_KIND_DESTRUCTIVE` = DEST.

Reusable nested message: **StoreResource** (`operation.proto`) = `{backend, instance, resource_kind, resource_name, resource_uri, message_type, schema, labels<map>}`. For a store-backed call set at minimum `message_type:<seed:message_type>` (or `backend`+`resource_name`).

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
|------|-----|---------|-------------|------------|-------------------|
| [ ] | Select | RO | SelectRequest | `ctx`, `message_type:"<seed:message_type>"`, `filter:{record_id:"<seed:record_id>"}` (Struct), `limit:10` | filter/fields/sort/page_token/cache all optional; empty filter selects all. message_type must be a seeded entity. |
| [ ] | BatchSelect | MUT | stream SelectRequest | one stream element = same as Select body | client-streaming; send ≥1 SelectRequest. op_kind=MUT in proto despite read. |
| [ ] | SelectV2 | RO | SelectRequest | same as Select | server-streams RecordBatchV2; only call when ProtocolSupport.encodings advertises `record_batch_v2`. |
| [ ] | Upsert | MUT | UpsertRequest | `ctx`, `message_type:"<seed:message_type>"`, `payload:{<entity fields incl id>}` (Struct) OR `record_json:<bytes JSON>`, `return_record:true` | supply either `payload` (Struct) or `record_json` (bytes), not both required; `conflict_fields`,`idempotency_key`,`cache` optional. id field in payload → `<seed:record_id>`. |
| [ ] | BatchUpsert | MUT | stream UpsertRequest | one stream element = same as Upsert body | client-streaming; send ≥1. |
| [ ] | Delete | MUT | DeleteRequest | `ctx`, `message_type:"<seed:message_type>"`, `filter:{id:"<seed:record_id>"}` (Struct) | `idempotency_key` optional. record_id must exist for affected_rows>0. |
| [ ] | VectorSearch | RO | VectorSearchRequest | `ctx`, `collection:"<seed:message_type>"`, `vector:[0.1,0.2,0.3]` (repeated float, len=collection dim), `limit:5`, `with_payload:true` | vector length MUST equal the collection's configured dimension. `filter`,`score_threshold` optional. Needs a seeded vector collection. |
| [ ] | VectorHybridSearch | RO | VectorHybridSearchRequest | `ctx`, `collection:"<seed:message_type>"`, `vector:[0.1,0.2,0.3]`, `text_query:"hello"`, `limit:5`, `with_payload:true` | `fusion_weights` (repeated float) & `filter` optional. dim must match collection. |
| [ ] | VectorUpsert | MUT | VectorUpsertRequest | `ctx`, `collection:"<seed:message_type>"`, `points:[{id:"<seed:record_id>", vector:[0.1,0.2,0.3], payload:{}}]` | each VectorPointMutation = `{id, vector(repeated float), payload(Struct)}`; vector dim must match collection. `idempotency_key` optional. |
| [ ] | VectorBatchUpsert | MUT | stream VectorUpsertRequest | one stream element = same as VectorUpsert | client-streaming; send ≥1. |
| [ ] | PutObject | MUT | stream Chunk | stream of Chunk = `{ctx, bucket:"<seed:bucket>", object_key:"<seed:object_key>", data:<bytes>, content_type:"application/octet-stream", final_chunk:true(on last)}` | client-streaming; set `final_chunk:true` on terminal chunk. `idempotency_key` optional. bucket must exist. |
| [ ] | GetObject | RO | ObjectRequest | `ctx`, `bucket:"<seed:bucket>"`, `object_key:"<seed:object_key>"` | object must have been Put first. server-streams Chunk. |
| [ ] | GeneratePresignedUrl | MUT | UrlRequest | `ctx`, `bucket:"<seed:bucket>"`, `object_key:"<seed:object_key>"`, `method:"GET"`, `ttl_seconds:300` | `method` is HTTP verb string (GET/PUT). `content_type` optional. |
| [ ] | InitiateMultipartUpload | MUT | MultipartUploadRequest | `ctx`, `bucket:"<seed:bucket>"`, `object_key:"<seed:object_key>"`, `content_type:"application/octet-stream"`, `part_count:1`, `ttl_seconds:300` | `idempotency_key` optional. |
| [ ] | CacheGet | RO | CacheGetRequest | `ctx`, `resource:StoreResource{backend:"redis"}`, `key:"<seed:object_key>"`, `touch:false` | key must have been CacheSet first for found=true. |
| [ ] | CacheSet | MUT | CacheSetRequest | `ctx`, `resource:StoreResource{backend:"redis"}`, `key:"<seed:object_key>"`, `value:<bytes>`, `content_type:"text/plain"`, `ttl_seconds:60` | `only_if_absent`/`only_if_present`/`idempotency_key`/`metadata` optional. |
| [ ] | CacheDelete | MUT | CacheDeleteRequest | `ctx`, `resource:StoreResource{backend:"redis"}`, `key:"<seed:object_key>"` | `idempotency_key` optional. |
| [ ] | CacheScan | RO | CacheScanRequest | `ctx`, `resource:StoreResource{backend:"redis"}`, `key_pattern:"*"`, `limit:50` | `page_token` optional. |
| [ ] | DocumentGet | RO | DocumentGetRequest | `ctx`, `resource:StoreResource{backend:"mongodb", resource_name:"<seed:mongo_collection>"}`, `document_id:"<seed:document_id>"` | `fields` optional. document must exist. |
| [ ] | DocumentFind | RO | DocumentFindRequest | `ctx`, `resource:StoreResource{backend:"mongodb", resource_name:"<seed:mongo_collection>"}`, `filter:{}` (Struct), `limit:10` | empty filter matches all. `fields`,`sort`,`page_token` optional. |
| [ ] | DocumentUpsert | MUT | DocumentUpsertRequest | `ctx`, `resource:StoreResource{backend:"mongodb", resource_name:"<seed:mongo_collection>"}`, `document_id:"<seed:document_id>"`, `document:{name:"x"}` (Struct) | `merge_fields`,`replace`,`idempotency_key` optional. |
| [ ] | DocumentDelete | MUT | DocumentDeleteRequest | `ctx`, `resource:StoreResource{backend:"mongodb", resource_name:"<seed:mongo_collection>"}`, `document_id:"<seed:document_id>"` | `filter`(Struct) alternative to document_id. `idempotency_key` optional. |
| [ ] | GraphQuery | RO | GraphQueryRequest | `ctx`, `resource:StoreResource{backend:"neo4j"}`, `query:"MATCH (n) RETURN n LIMIT 1"`, `read_only:true`, `limit:10` | `parameters`(Struct),`page_token` optional. query syntax = backend (Cypher). |
| [ ] | GraphMutate | MUT | GraphMutationRequest | `ctx`, `resource:StoreResource{backend:"neo4j"}`, `query:"CREATE (n:Node {id:$id})"`, `parameters:{id:"<seed:record_id>"}` (Struct) | `idempotency_key` optional. |
| [ ] | TimeSeriesWrite | MUT | TimeSeriesWriteRequest | `ctx`, `resource:StoreResource{backend:"clickhouse"}`, `points:[{timestamp:<now Timestamp>, tags:{host:"a"}, values:{cpu:0.5}}]` | TimeSeriesPoint = `{timestamp(Timestamp), tags<map str,str>, values<map str,double>, fields(Struct)}`. `idempotency_key` optional. |
| [ ] | TimeSeriesQuery | RO | TimeSeriesQueryRequest | `ctx`, `resource:StoreResource{backend:"clickhouse"}`, `from:<Timestamp>`, `to:<Timestamp>`, `limit:100` | `filter`,`fields`,`group_by`,`aggregate`,`window`,`page_token` optional. |
| [ ] | AnalyticalQuery | RO | AnalyticalQueryRequest | `ctx`, `resource:StoreResource{backend:"clickhouse"}`, `query:"SELECT 1"`, `limit:100` | `parameters`(Struct),`dry_run`,`page_token` optional. |
| [ ] | BeginTx | MUT | stream Mutation | stream of Mutation = `{ctx, operation:"upsert", message_type:"<seed:message_type>", payload:{...} }` then final `{commit:true}` | bidi-stream. Mutation fields: operation/message_type/record_json/payload/filter/collection/vector_points/commit/rollback/bucket/object_key/object_data/content_type/idempotency_key/tx_id. Send commit:true to finalize. |
| [ ] | PublishCDC | MUT | CDCSubscriptionRequest | `ctx`, `topic_pattern:"<seed:project>.*"` | `since_event_id` optional (resume). server-streams CDCEnvelope. |
| [ ] | CreateMaterializedView | MUT | ViewDefinition | `ctx`, `schema:"public"`, `name:"mv_test"`, `query:"SELECT 1"`, `with_data:true` | `ttl_days` optional. |
| [ ] | EnqueueOutboxEvent | MUT | EnqueueOutboxEventRequest | `ctx`, `topic:"<seed:event_type>"`, `partition_key:"<seed:document_id>"`, `payload:{event_id:"<uuid>", event_type:"<seed:event_type>", correlation_id:"<uuid>", document_id:"<seed:document_id>"}` (Struct) | payload Struct MUST contain event_id/event_type/correlation_id/document_id (per proto comment). `schema_uri`,`idempotency_key` optional. |
| [ ] | GenericDispatch | MUT | GenericDispatchRequest | `ctx (scopes:[udb:dispatch])`, `backend:"mongodb"`, `operation:"ping"` | operation ∈ {ping, ensure_resource, drop_resource, list_resources}. resource_kind/resource_name/resource_uri/spec_json/idempotency_key/dry_run optional depending on op. |
| [ ] | EnsureResource | MUT | ResourceAdminRequest | `ctx (scopes:[udb:admin])`, `backend:"mongodb"`, `resource_name:"<seed:mongo_collection>"` | `spec_json`(JSON),`idempotency_key`,`dry_run` optional. |
| [ ] | DropResource | DEST | ResourceAdminRequest | `ctx (scopes:[udb:admin])`, `backend:"mongodb"`, `resource_name:"<seed:mongo_collection>"` | destructive; resource must exist. |
| [ ] | ListResources | RO | ResourceAdminRequest | `ctx (scopes:[udb:admin])`, `backend:"mongodb"` | `resource_name` ignored for list. |
| [ ] | StageCatalog | DEST | StageCatalogRequest | `ctx (scopes:[udb:admin])`, `manifest_json:<bytes valid CatalogManifest JSON>` (field 1000), `project_id:"<seed:project>"`, `reason:"stage"` | manifest_json is field 1000 (not 2). Needs a real serialized CatalogManifest. `idempotency_key` optional. |
| [ ] | ActivateCatalog | DEST | CatalogVersionRequest | `ctx (scopes:[udb:admin])`, `project_id:"<seed:project>"`, `version:"<staged version>"` | version from a prior StageCatalog (empty = latest STAGED). `reason`,`idempotency_key` optional. |
| [ ] | RollbackCatalog | DEST | CatalogVersionRequest | `ctx (scopes:[udb:admin])`, `project_id:"<seed:project>"` | rolls back to previous ACTIVE. `version`/`reason`/`idempotency_key` optional. Needs ≥2 catalog versions. |
| [ ] | ValidateCatalog | DEST | StageCatalogRequest | `ctx (scopes:[udb:admin])`, `manifest_json:<bytes CatalogManifest JSON>`, `project_id:"<seed:project>"` | same shape as StageCatalog; validates only. |
| [ ] | GetCatalogVersions | RO | CatalogManifestRequest | `ctx (scopes:[udb:admin])`, `redact:false` | note: uses CatalogManifestRequest `{context, redact}` — no project field; project from context. |
| [ ] | GetCatalogVersion | RO | CatalogVersionRequest | `ctx (scopes:[udb:admin])`, `project_id:"<seed:project>"`, `version:""` | empty version = active. |
| [ ] | PlanMigration | MUT | MigrationPlanRequest | `ctx (scopes:[udb:admin])`, `project_id:"<seed:project>"`, `dry_run:true` | returns run_id. |
| [ ] | ApplyMigration | MUT | MigrationApplyRequest | `ctx (scopes:[udb:admin])`, `run_id:"<seed:migration_id>"`, `project_id:"<seed:project>"` | run_id from PlanMigration. `approval_token` from ApproveMigrationPlan (required for blocked ops). `idempotency_key` optional. |
| [ ] | GetMigrationStatus | RO | MigrationRunRequest | `ctx (scopes:[udb:admin])`, `run_id:"<seed:migration_id>"`, `project_id:"<seed:project>"` | run_id must exist. `idempotency_key` optional. |
| [ ] | ListMigrationRuns | RO | MigrationRunListRequest | `ctx (scopes:[udb:admin or udb:admin:viewer])`, `project_id:"<seed:project>"`, `limit:50` | `state_filter`,`page_token` optional. |
| [ ] | ApproveMigrationPlan | MUT | MigrationRunRequest | `ctx (scopes:[udb:admin])`, `run_id:"<seed:migration_id>"`, `project_id:"<seed:project>"` | run_id must be a plan awaiting review. |
| [ ] | ListDlqEvents | RO | DlqListRequest | `ctx`, `limit:50` | `topic`,`status_filter`(OPEN/REPLAYED/DISMISSED/QUARANTINED),`page_token` optional. |
| [ ] | GetDlqEvent | RO | DlqEventRequest | `ctx`, `dlq_id:"<seed:record_id>"` | dlq_id = an existing DLQ row id (no dedicated seed key; reuse record_id). |
| [ ] | ReplayDlqEvent | MUT | DlqActionRequest | `ctx`, `dlq_id:"<seed:record_id>"`, `preserve_event_id:false` | `reason` optional. dlq_id must exist. |
| [ ] | DismissDlqEvent | MUT | DlqActionRequest | `ctx`, `dlq_id:"<seed:record_id>"` | `reason` optional. |
| [ ] | QuarantineDlqEvent | MUT | DlqActionRequest | `ctx`, `dlq_id:"<seed:record_id>"` | `reason` optional. |
| [ ] | GetCdcStatus | RO | CdcControlRequest | `ctx`, `slot_name:"<cdc slot>"` | slot_name = a configured CDC slot name (no dedicated seed key). `reason` optional. |
| [ ] | PauseCdc | MUT | CdcControlRequest | `ctx`, `slot_name:"<cdc slot>"`, `reason:"maintenance"` | slot_name must be a live slot. |
| [ ] | ResumeCdc | MUT | CdcControlRequest | `ctx`, `slot_name:"<cdc slot>"`, `reason:"resume"` | slot must be paused. |
| [ ] | StepDownCdcLeader | MUT | CdcControlRequest | `ctx`, `slot_name:"<cdc slot>"`, `reason:"failover"` | only effective on the leader node. |
| [ ] | PreviewCdcRedaction | RO | CdcRedactionPreviewRequest | `ctx`, `message_type:"<seed:message_type>"`, `topic:"<seed:event_type>"`, `payload_json:<bytes JSON>`, `redaction_mode:"mask"`, `redaction_version:1` | `schema_uri` optional. payload_json = sample event JSON. |
| [ ] | ScanProjectionDrift | RO | ProjectionDriftScanRequest | `ctx`, `project_id:"<seed:project>"`, `message_type:"<seed:message_type>"`, `scan_mode:"sample"`, `rows_per_target:100`, `limit:10` | `repair:false` to scan only. |
| [ ] | ListSagas | RO | SagaListRequest | `ctx`, `limit:50` | `tenant_id_filter`,`status_filter`(pending/in_progress/committed/compensated/failed_compensation/manual_review),`tx_id_filter`,`correlation_id_filter`,`page_token` optional. |
| [ ] | GetSaga | RO | SagaRequest | `ctx`, `saga_id:"<seed:saga_id>"` | saga_id must exist. `reason`,`idempotency_key` optional. |
| [ ] | RetrySagaCompensation | MUT | SagaRequest | `ctx`, `saga_id:"<seed:saga_id>"`, `reason:"retry"` | saga must be in failed_compensation. |
| [ ] | MarkSagaReviewed | MUT | SagaRequest | `ctx`, `saga_id:"<seed:saga_id>"`, `reason:"reviewed"` | saga must be in manual_review. |
| [ ] | ListPolicies | RO | PolicyListRequest | `ctx`, `include_disabled:false`, `limit:50` | `page_token` optional. |
| [ ] | PutPolicy | DEST | PutPolicyRequest | `ctx`, `policy:PolicyRecord{effect:"allow", service_identity:"<seed:user_id>", tenant_id:"<seed:tenant_id>", message_type:"<seed:message_type>", operation:"read", required_scope:"udb:read", priority:100, enabled:true}` | PolicyRecord.policy_id omit/0 for create; set `<seed:policy_id>` to update. effect=allow/deny. |
| [ ] | DeletePolicy | MUT | PolicyRequest | `ctx`, `policy_id:<seed:policy_id>` (int64) | policy_id must exist. |
| [ ] | ReloadPolicies | DEST | CapabilitiesRequest | `ctx`, `project_id:"<seed:project>"` | reuses CapabilitiesRequest `{context, project_id}`. |
| [ ] | LintPolicies | RO | CapabilitiesRequest | `ctx`, `project_id:"<seed:project>"` | reuses CapabilitiesRequest. |
| [ ] | GetCapabilities | RO | CapabilitiesRequest | `ctx`, `project_id:"<seed:project>"` | project_id optional (empty = default). |
| [ ] | GetCatalogManifest | RO | CatalogManifestRequest | `ctx`, `redact:false` | project from context. |
| [ ] | LookupMessageSchema | RO | MessageSchemaLookupRequest | `ctx`, `project_id:"<seed:project>"`, `message_type:"<seed:message_type>"` | `client_catalog_version` optional (empty = active). |
| [ ] | ListMessageSchemas | RO | MessageSchemaListRequest | `ctx`, `project_id:"<seed:project>"` | `client_catalog_version` optional. |
| [ ] | GetHealthReport | RO | HealthReportRequest | `ctx`, `with_probes:false`, `project_id:"<seed:project>"` | with_probes:true sends live backend probes. |
| [ ] | EnsureProject | MUT | EnsureProjectRequest | `ctx (scopes:[udb:admin])`, `project_id:"<seed:project>"`, `name:"My Project"`, `cdc_topic_prefix:"<seed:project>."` | idempotent. cdc_topic_prefix dot-terminated. |
| [ ] | ListProjects | RO | ProjectListRequest | `ctx (scopes:[udb:admin])`, `limit:50` | `page_token` optional. |
| [ ] | GetAdminSummary | RO | AdminSummaryRequest | `ctx (scopes:[udb:admin])`, `project_id:"<seed:project>"`, `with_probes:false`, `redact:false` | project_id optional (empty = all). |
| [ ] | ListAdminAuditLogs | RO | AdminAuditLogRequest | `ctx (scopes:[udb:admin or udb:admin:viewer])`, `limit:50`, `redact:false` | `operation_filter`,`actor_filter`,`tenant_id_filter`,`project_id_filter`,`page_token` optional. |
| [ ] | VerifyAdminAuditLog | RO | AdminAuditVerifyRequest | `ctx (scopes:[udb:admin or udb:admin:viewer])`, `limit:0` | limit<=0 verifies full local chain. |


## AuthnService

_proto: core/authn/services/v1/authn_service.proto · 50 RPCs_

All request/response message bodies live in `core/authn/services/v1/core.proto`. Enum fields resolve against `core/authn/entity/v1/enums.proto`; `context` is `udb.core.common.v1.RequestContext` (from `common/v1/types.proto`, fields: `tenant{TenantContext}`, `request_id`, `correlation_id`, `user_id`, `headers`, `trace_id`, `ip_address`, `user_agent`, `idempotency_key`, `scopes`, `roles`, ...). Most authn fields read from `RequestContext` are populated server-side from the bearer/session, so the column below lists only the *message body* fields you set.

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
|------|-----|---------|-------------|------------|-------------------|
| [ ] | CreateUser | MUTATION | CreateUserRequest | `username: "alice"`, `email: "alice@acme.test"`, `password: "Str0ng!Passw0rd"`, `tenant_id: "<seed:tenant_id>"`, `full_name: "Alice A"`, `account_kind: ACCOUNT_KIND_PERSON` | password policy: min 10, 1 upper/lower/digit/special. `account_kind` enum (AccountKind). Optional: `project_id`, `external_provider_id`, `external_subject`, `profile_attributes` map, `context`. |
| [ ] | GetUser | READ_ONLY | GetUserRequest | `user_id: "<seed:user_id>"` | Lookup-by oneof-style: provide ONE of `user_id` / `username` / `email` (not a proto oneof, but only one needed). |
| [ ] | ListUsers | READ_ONLY | ListUsersRequest | `tenant_id: "<seed:tenant_id>"` | Optional filters: `account_kind` (AccountKind), `status` (UserStatus), `page` (PageRequest). |
| [ ] | UpdateUser | MUTATION | UpdateUserRequest | `user_id: "<seed:user_id>"`, `full_name: "Alice B"`, `email: "alice2@acme.test"`, `tenant_id: "<seed:tenant_id>"` | Optional: `account_kind` (AccountKind), `project_id`, `profile_attributes`, `external_provider_id`, `external_subject`, `context`. |
| [ ] | ChangeUserStatus | DESTRUCTIVE | ChangeUserStatusRequest | `user_id: "<seed:user_id>"`, `new_status: USER_STATUS_SUSPENDED`, `reason: "admin action"` | `new_status` enum (UserStatus, non-UNSPECIFIED). Optional `context`. |
| [ ] | AdminResetPassword | DESTRUCTIVE | AdminResetPasswordRequest | `user_id: "<seed:user_id>"` | Sends email OTP; response returns `otp_id`. Optional `context`. |
| [ ] | SendOTP | MUTATION | SendOTPRequest | `user_id: "<seed:user_id>"`, `otp_type: OTP_TYPE_EMAIL_VERIFICATION` | `otp_type` enum (OTPType, non-UNSPECIFIED). Optional `correlation_id`, `context`. |
| [ ] | VerifyOTP | READ_ONLY | VerifyOTPRequest | `otp_id: "<seed:code>"` (the OTP id from SendOTP), `code: "123456"` | `otp_id` is an OTP-handle ref (depends on a prior SendOTP); `code` is the 6-digit plaintext. NOTE: a *correct* code cannot be grounded from proto alone — depends on the OTP issued at runtime. |
| [ ] | ResendOTP | MUTATION | ResendOTPRequest | `original_otp_id: "<seed:code>"`, `reason: "not_received"` | `original_otp_id` refs a prior OTP id. `reason` is free string (suggested: not_received \| expired \| delivery_failed). |
| [ ] | Authenticate | READ_ONLY | AuthnRequest | `bearer_token: "<seed:token>"`, `credential_type: AUTH_CREDENTIAL_TYPE_BEARER_TOKEN` | PUBLIC. Provide ONE proof: `bearer_token` / `session_id` / `api_key` / (`external_provider_id`+`external_token`). `credential_type` enum (AuthCredentialType). Optional `tenant_hint`, `project_hint`, `requested_scopes`, `client_id`, `audience`, `issuer`, `attributes`. |
| [ ] | Login | MUTATION | LoginRequest | `username: "alice"`, `password: "Str0ng!Passw0rd"`, `device_type: DEVICE_TYPE_API`, `device_name: "cli"` | PUBLIC. `device_type` enum (DeviceType, non-UNSPECIFIED). MFA step-2 fields (`mfa_otp_id`,`totp_code`,`recovery_code`) only on second call after `mfa_required=true`. Optional `ip_address`,`user_agent`,`device_id`,`tenant_hint`,`project_hint`,`access_surface`. |
| [ ] | RefreshToken | MUTATION | RefreshTokenRequest | `refresh_token: "<seed:refresh_token>"` | PUBLIC. Provide `refresh_token` (token-family credential) OR legacy `session_id`. |
| [ ] | Logout | MUTATION | LogoutRequest | `session_id: "<seed:session_id>"` | Optional `all_sessions: true` to revoke all, `revoke_reason`, `context`. |
| [ ] | ChangePassword | MUTATION | ChangePasswordRequest | `user_id: "<seed:user_id>"`, `current_password: "Str0ng!Passw0rd"`, `new_password: "N3w!Passw0rd9"`, `otp_id: "<seed:code>"` | `otp_id` = 2FA OTP confirming the change (runtime-issued). Optional `context`. |
| [ ] | ValidateToken | READ_ONLY | ValidateTokenRequest | `token: "<seed:token>"`, `token_type: TOKEN_TYPE_JWT_ACCESS` | `token_type` enum (TokenType, non-UNSPECIFIED). `token` = raw JWT or session_token. |
| [ ] | CreateSession | MUTATION | CreateSessionRequest | `principal: { principal_id: "<seed:user_id>", subject: "<seed:subject>", user_id: "<seed:user_id>", tenant_id: "<seed:tenant_id>" }`, `ttl_seconds: 3600` | Nested `Principal` message (expand required ids). Optional `client_fingerprint`. |
| [ ] | RefreshSession | MUTATION | RefreshSessionRequest | `session_id: "<seed:session_id>"`, `ttl_seconds: 3600` | session_id ref. |
| [ ] | GetSession | READ_ONLY | GetSessionRequest | `session_id: "<seed:session_id>"` | session_id ref. |
| [ ] | ListSessions | READ_ONLY | ListSessionsRequest | `user_id: "<seed:user_id>"` | Optional `active_only: true`, `page` (PageRequest). |
| [ ] | RevokeSession | MUTATION | RevokeSessionRequest | `session_id: "<seed:session_id>"`, `revoke_reason: "user logout"` | Or revoke all for a principal: `principal_id: "<seed:subject>"`, `all_for_principal: true`. Optional `context`. |
| [ ] | ValidateCSRF | READ_ONLY | ValidateCSRFRequest | `session_id: "<seed:session_id>"`, `csrf_token: "<seed:csrf_token>"` | Server-side sessions only. csrf_token = value from csrf cookie/header (runtime-issued at Login). |
| [ ] | EnrollMFA | MUTATION | EnrollMFARequest | `user_id: "<seed:user_id>"`, `mfa_type: AUTH_FACTOR_KIND_TOTP` | `mfa_type` enum (AuthFactorKind, non-UNSPECIFIED). Optional `context`. |
| [ ] | ConfirmMFAEnrollment | MUTATION | ConfirmMFAEnrollmentRequest | `user_id: "<seed:user_id>"`, `otp_id: "<seed:code>"`, `code: "123456"` | `otp_id` = verify_otp_id from EnrollMFA; `code` = TOTP/email code. NOTE: correct TOTP code not groundable from proto (computed from totp_secret at runtime). |
| [ ] | GenerateRecoveryCodes | MUTATION | GenerateRecoveryCodesRequest | `user_id: "<seed:user_id>"`, `count: 10` | `count` clamped server-side (default 10). Optional `context`. |
| [ ] | PutMfaPolicy | MUTATION | PutMfaPolicyRequest | `tenant_id: "<seed:tenant_id>"`, `require_mfa: true` | Optional `context`. |
| [ ] | GetMfaPolicy | READ_ONLY | GetMfaPolicyRequest | `tenant_id: "<seed:tenant_id>"` | Optional `context`. |
| [ ] | ForgotPassword | MUTATION | ForgotPasswordRequest | `identifier: "alice@acme.test"` | PUBLIC. `identifier` = username or email. Optional `context`. |
| [ ] | ResetPassword | MUTATION | ResetPasswordRequest | `otp_id: "<seed:code>"`, `code: "123456"`, `new_password: "N3w!Passw0rd9"` | PUBLIC. `otp_id`/`code` from ForgotPassword (runtime-issued PASSWORD_RESET OTP). Optional `context`. |
| [ ] | IntrospectToken | READ_ONLY | IntrospectTokenRequest | `token: "<seed:token>"` | Optional `context`. |
| [ ] | SendPhoneVerification | MUTATION | SendPhoneVerificationRequest | `user_id: "<seed:user_id>"`, `phone: "+15551234567"` | `phone` = E.164. Complete with VerifyOTP. Optional `context`. |
| [ ] | GetJwks | READ_ONLY | GetJwksRequest | `{}` (empty) | PUBLIC. Only optional `context`; no required fields. |
| [ ] | StartWebAuthnRegistration | MUTATION | StartWebAuthnRegistrationRequest | `user_id: "<seed:user_id>"`, `label: "yubikey"`, `tenant_id: "<seed:tenant_id>"` | Optional `project_id`, `context`. |
| [ ] | FinishWebAuthnRegistration | MUTATION | FinishWebAuthnRegistrationRequest | `challenge_id: "<seed:code>"`, `public_key_credential_json: "{...}"`, `label: "yubikey"` | `challenge_id` from Start...; `public_key_credential_json` = WebAuthn attestation JSON. NOTE: a valid credential JSON requires a real authenticator/browser — not groundable from proto. Optional `context`. |
| [ ] | StartWebAuthnAuthentication | MUTATION | StartWebAuthnAuthenticationRequest | `user_id: "<seed:user_id>"`, `tenant_id: "<seed:tenant_id>"` | PUBLIC. Optional `project_id`, `context`. |
| [ ] | FinishWebAuthnAuthentication | MUTATION | FinishWebAuthnAuthenticationRequest | `challenge_id: "<seed:code>"`, `public_key_credential_json: "{...}"` | PUBLIC. `challenge_id` from Start...; assertion JSON requires a real authenticator — not groundable from proto. Optional `context`. |
| [ ] | ListDevices | READ_ONLY | ListDevicesRequest | `user_id: "<seed:user_id>"` | Optional `page` (PageRequest), `context`. |
| [ ] | RevokeDevice | MUTATION | RevokeDeviceRequest | `device_id: "<seed:record_id>"`, `reason: "lost device"` | `device_id` ref (a Device id; no dedicated seed key — use record_id). Optional `context`. |
| [ ] | AdminRevokeSession | DESTRUCTIVE | AdminRevokeSessionRequest | `user_id: "<seed:user_id>"`, `session_id: "<seed:session_id>"`, `reason: "compromised"` | Optional `context`. |
| [ ] | AdminRevokeAllUserSessions | DESTRUCTIVE | AdminRevokeAllUserSessionsRequest | `user_id: "<seed:user_id>"`, `reason: "compromised"` | Optional `context`. |
| [ ] | AdminRevokeAllTenantSessions | DESTRUCTIVE | AdminRevokeAllTenantSessionsRequest | `tenant_id: "<seed:tenant_id>"`, `reason: "incident"` | Optional `context`. |
| [ ] | EmergencyRevoke | DESTRUCTIVE | EmergencyRevokeRequest | `tenant_id: "<seed:tenant_id>"`, `reason: "incident"` | Provide at least one revoke target: `signing_key_id` (<seed:key_id>) / `token_family_id` / `tenant_id` / `principal_id` (<seed:subject>). Optional `context`. |
| [ ] | IssueMfaChallenge | MUTATION | IssueMfaChallengeRequest | `user_id: "<seed:user_id>"`, `factor_kind: AUTH_FACTOR_KIND_TOTP`, `purpose: MFA_CHALLENGE_PURPOSE_SENSITIVE_OPERATION` | `factor_kind` enum (AuthFactorKind), `purpose` enum (MfaChallengePurpose), both non-UNSPECIFIED. Optional `device_fingerprint`, `ip_address`, `context`. |
| [ ] | VerifyMfaChallenge | READ_ONLY | VerifyMfaChallengeRequest | `challenge_id: "<seed:code>"`, `code: "123456"` | `challenge_id` from IssueMfaChallenge; `code` = TOTP/OTP/recovery proof (runtime-dependent). Optional `device_fingerprint`, `context`. |
| [ ] | ListMfaFactors | READ_ONLY | ListMfaFactorsRequest | `user_id: "<seed:user_id>"` | Optional `context`. |
| [ ] | DisableMfaFactor | MUTATION | DisableMfaFactorRequest | `user_id: "<seed:user_id>"`, `factor_kind: AUTH_FACTOR_KIND_TOTP` | `factor_kind` enum (AuthFactorKind, non-UNSPECIFIED). Optional `context`. |
| [ ] | RenamePasskey | MUTATION | RenamePasskeyRequest | `user_id: "<seed:user_id>"`, `credential_id: "<seed:record_id>"`, `new_label: "work key"` | `credential_id` ref (WebAuthn credential id; no dedicated seed key — use record_id). Optional `context`. |
| [ ] | RevokeRecoveryCodes | MUTATION | RevokeRecoveryCodesRequest | `user_id: "<seed:user_id>"` | Optional `context`. |
| [ ] | AdminResetMfa | DESTRUCTIVE | AdminResetMfaRequest | `user_id: "<seed:user_id>"`, `reason: "lost device"` | Optional `context`. |
| [ ] | ListWebAuthnCredentials | READ_ONLY | ListWebAuthnCredentialsRequest | `user_id: "<seed:user_id>"` | Optional `context`. |
| [ ] | DeleteWebAuthnCredential | MUTATION | DeleteWebAuthnCredentialRequest | `user_id: "<seed:user_id>"`, `credential_id: "<seed:record_id>"` | `credential_id` ref (WebAuthn credential id; no dedicated seed key — use record_id). Optional `context`. |


## AuthzService

_proto: core/authz/services/v1/authz_service.proto · 41 RPCs_

All RPCs require `mode: AUTH_MODE_BEARER`, `request_context_required: true`, `tenant_required: true`, and credential type `CREDENTIAL_TYPE_BEARER_JWT` or `CREDENTIAL_TYPE_SESSION`. Each RPC also carries its own `scopes:` (the "scope" column below) — the calling principal's JWT/session must carry that scope or the call fails with PERMISSION_DENIED. Governance RPCs additionally pass a `GovernanceActor actor` whose `scopes`/`roles` are re-checked under `native.authz.governance` (and `break_glass` is the emergency bypass), so the actor must carry `authz:admin` / `authz:policy:write` / `authz:policy:approve` / `authz:policy:read` matching the RPC.

Enum valid NAMEs:
- `PolicyEffect`: `POLICY_EFFECT_ALLOW`, `POLICY_EFFECT_DENY` (avoid `*_UNSPECIFIED`).
- `RoleScopeType`: `ROLE_SCOPE_TYPE_GLOBAL` / `_TENANT` / `_PROJECT` / `_RESOURCE` / `_EXTERNAL`.
- `PrincipalKind`: `PRINCIPAL_KIND_USER` / `_SERVICE_ACCOUNT` / `_WORKLOAD` / `_GROUP` / `_ROLE` / `_EXTERNAL_SUBJECT`.
- `CanaryScopeKind`: `CANARY_SCOPE_KIND_NODE` / `_TENANT` / `_PERCENT`.
- `PolicyVersionState`: `POLICY_VERSION_STATE_DRAFT` / `_PENDING_REVIEW` / `_APPROVED` / `_ACTIVE` / `_SUPERSEDED` / `_REJECTED` / `_ROLLED_BACK`.

Shared nested messages (real fields):
- `Principal`: principal_id, subject, user_id, service_identity, tenant_id, project_id, repeated scopes, repeated roles, provider_id, auth_method, expires_at_unix(int64), account_kind, domain, map attributes.
- `ResourceRef`: resource_type, resource_name, message_type, schema, table, backend, instance, resource_id, collection, bucket, path, service, api, tenant_id, project_id, map attributes.
- `AccessContext`: ip_address, user_agent, device_id, token_id, session_id, map attributes.
- `RoleBinding`: subject, role, tenant, project, expires_at_unix(int64), source.
- `RelationshipTuple`: subject, relation, object, tenant, project, version(int64), expires_at_unix(int64), source.
- `AuthzPolicyRecord`: id, priority(int32), enabled(bool), effect(string), tenant, project, subject, role, action, resource, purpose, relationship, map conditions, repeated required_scopes.
- `GovernanceActor`: subject, tenant_id, project_id, repeated scopes, repeated roles, break_glass(bool), break_glass_reason, break_glass_expires_at_unix(int64).
- `PolicyDocument`: repeated AuthzPolicyRecord policies, repeated RoleBinding role_bindings, repeated RelationshipTuple relationship_tuples.
- `SimulationCase`: Principal principal, ResourceRef resource, action, purpose, map attributes, label.

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | Authorize | READ_ONLY | AuthzRequest | `{principal:{subject:"<seed:subject>",user_id:"<seed:user_id>",tenant_id:"<seed:tenant_id>",scopes:[...]}, tenant_id:"<seed:tenant_id>", project_id:"<seed:project>", resource:{resource_type:"<seed:resource>",table:"<table>"}, action:"<seed:action>", domain:"<seed:tenant_id>", requested_scopes:["<scope>"]}` | scope `udb:authz:authorize`. All fields proto-optional; principal+resource+action+domain are the decision inputs. |
| [ ] | CheckAccess | READ_ONLY | CheckAccessRequest | `{user_id:"<seed:user_id>", domain:"<seed:tenant_id>", object:"<seed:object>", action:"<seed:action>"}` | scope `udb:authz:check-access`. REQUIRED: user_id, domain, object, action. optional: context, principal, resource, purpose, tenant_id, project_id, attributes. |
| [ ] | CreateRole | MUTATION | CreateRoleRequest | `{name:"reader", created_by:"<seed:subject>", role_code:"<seed:role_code>", domain:"<seed:tenant_id>", tenant_id:"<seed:tenant_id>", scope_type:"ROLE_SCOPE_TYPE_TENANT"}` | scope `udb:authz:create-role`. REQUIRED: name, created_by. scope_type enum→NAME. |
| [ ] | AssignRole | MUTATION | AssignRoleRequest | `{user_id:"<seed:user_id>", role_id:"<seed:role_id>", domain:"<seed:tenant_id>", assigned_by:"<seed:subject>", principal_kind:"PRINCIPAL_KIND_USER", tenant_id:"<seed:tenant_id>"}` | scope `udb:authz:assign-role`. REQUIRED: user_id, role_id, domain, assigned_by. expires_at = google.protobuf.Timestamp (optional). role_id must reference an existing role. |
| [ ] | CreatePolicyRule | MUTATION | CreatePolicyRuleRequest | `{subject:"<seed:subject>", domain:"<seed:tenant_id>", object:"<seed:object>", action:"<seed:action>", effect:"POLICY_EFFECT_ALLOW", created_by:"<seed:subject>", tenant_id:"<seed:tenant_id>"}` | scope `udb:authz:create-policy-rule`. REQUIRED: subject, domain, object, action, effect, created_by. |
| [ ] | ListUserPermissions | READ_ONLY | ListUserPermissionsRequest | `{user_id:"<seed:user_id>", domain:"<seed:tenant_id>"}` | scope `udb:authz:list-user-permissions`. Both REQUIRED. |
| [ ] | ListAccessDecisionAudits | READ_ONLY | ListAccessDecisionAuditsRequest | `{user_id:"<seed:user_id>", domain:"<seed:tenant_id>", page:{page_size:50}}` | scope `udb:authz:list-access-decision-audits`. all OPTIONAL: user_id, domain, correlation_id, page (PageRequest). |
| [ ] | RevokeRole | MUTATION | RevokeRoleRequest | `{user_id:"<seed:user_id>", user_role_id:"<seed:user_role_id>", reason:"rotation", revoked_by:"<seed:subject>"}` | scope `udb:authz:revoke-role`. user_role_id is the assignment id (from AssignRole/ListUserRoles), not role_id. |
| [ ] | ListUserRoles | READ_ONLY | ListUserRolesRequest | `{user_id:"<seed:user_id>", domain:"<seed:tenant_id>", active_only:true}` | scope `udb:authz:list-user-roles`. |
| [ ] | GetRole | READ_ONLY | GetRoleRequest | `{role_id:"<seed:role_id>"}` or `{role_code:"<seed:role_code>", domain:"<seed:tenant_id>"}` | scope `udb:authz:get-role`. role_code is alternative lookup. |
| [ ] | ListRoles | READ_ONLY | ListRolesRequest | `{domain:"<seed:tenant_id>", active_only:true, page:{page_size:50}}` | scope `udb:authz:list-roles`. |
| [ ] | BatchCheckPermissions | READ_ONLY | BatchCheckPermissionsRequest | `{user_id:"<seed:user_id>", domain:"<seed:tenant_id>", checks:[{object:"<seed:object>",action:"<seed:action>"}], context:{ip_address:"127.0.0.1"}}` | scope `udb:authz:batch-check-permissions`. checks = repeated PermissionCheck{object,action}. |
| [ ] | UpdateRole | MUTATION | UpdateRoleRequest | `{role_id:"<seed:role_id>", updated_by:"<seed:subject>", name:"reader-2", description:"...", is_active:true}` | scope `udb:authz:update-role`. REQUIRED: role_id, updated_by. is_active is `optional bool`. |
| [ ] | DeleteRole | MUTATION | DeleteRoleRequest | `{role_id:"<seed:role_id>", deleted_by:"<seed:subject>"}` | scope `udb:authz:delete-role`. Both REQUIRED. Soft-delete; assignments revoked. |
| [ ] | GetPolicyRule | READ_ONLY | GetPolicyRuleRequest | `{policy_id:"<seed:policy_id>"}` | scope `udb:authz:get-policy-rule`. |
| [ ] | ListPolicyRules | READ_ONLY | ListPolicyRulesRequest | `{domain:"<seed:tenant_id>", subject:"<seed:subject>", object:"<seed:object>", active_only:true, page:{page_size:50}}` | scope `udb:authz:list-policy-rules`. all filters optional. |
| [ ] | DeletePolicyRule | MUTATION | DeletePolicyRuleRequest | `{policy_id:"<seed:policy_id>", deleted_by:"<seed:subject>"}` | scope `udb:authz:delete-policy-rule`. |
| [ ] | PutRoleBinding | MUTATION | PutRoleBindingRequest | `{binding:{subject:"<seed:subject>", role:"<seed:role>", tenant:"<seed:tenant_id>", project:"<seed:project>", source:"manual"}}` | scope `udb:authz:put-role-binding`. binding = RoleBinding. Returns AuthMutationResponse. |
| [ ] | PutRelationship | MUTATION | PutRelationshipRequest | `{tuple:{subject:"<seed:subject>", relation:"<seed:relation>", object:"<seed:object>", tenant:"<seed:tenant_id>", project:"<seed:project>", source:"manual"}}` | scope `udb:authz:put-relationship`. tuple = RelationshipTuple. |
| [ ] | PutAuthzPolicy | MUTATION | PutAuthzPolicyRequest | `{policy:{id:"<seed:policy_id>", priority:100, enabled:true, effect:"allow", tenant:"<seed:tenant_id>", subject:"<seed:subject>", action:"<seed:action>", resource:"<seed:resource>", required_scopes:["<scope>"]}}` | scope `udb:authz:put-authz-policy`. policy = AuthzPolicyRecord; effect is a string field here ("allow"/"deny"). |
| [ ] | LintAuthzPolicies | READ_ONLY | LintAuthzPoliciesRequest | `{}` (no fields) | scope `udb:authz:lint-authz-policies`. Empty request message. |
| [ ] | GetNativeAccess | READ_ONLY | NativeAccessRequest | `{principal:{subject:"<seed:subject>",user_id:"<seed:user_id>",tenant_id:"<seed:tenant_id>"}, tenant_id:"<seed:tenant_id>", project_id:"<seed:project>", resource:{resource_type:"<seed:resource>",table:"<table>"}, action:"<seed:action>", backend:"postgres", requested_scopes:["<scope>"]}` | scope `udb:authz:get-native-access`. Mints DSN only when decision allows; backend defaults "postgres". |
| [ ] | GetPolicyBundle | READ_ONLY | PolicyBundleRequest | `{tenant_id:"<seed:tenant_id>", project_id:"<seed:project>", domain:"<seed:tenant_id>"}` | scope `udb:authz:get-policy-bundle`. |
| [ ] | CreatePolicyDraft | MUTATION | CreatePolicyDraftRequest | `{actor:{subject:"<seed:subject>",tenant_id:"<seed:tenant_id>",scopes:["udb:authz:policy:write"]}, tenant_id:"<seed:tenant_id>", project_id:"<seed:project>", policy_set_name:"default", title:"draft 1", change_reason:"init", document:{policies:[...]}}` or `{...,branch_from_active:true}` to clone active. | scope `udb:authz:policy:write`. actor must carry `authz:policy:write`. document = PolicyDocument. |
| [ ] | UpdatePolicyDraft | MUTATION | UpdatePolicyDraftRequest | `{actor:{subject:"<seed:subject>",scopes:["udb:authz:policy:write"]}, draft_id:"<seed:policy_draft_id>", document:{...}, change_reason:"edit", expected_updated_at_unix:<epoch>, title:"draft 1"}` | scope `udb:authz:policy:write`. expected_updated_at_unix = optimistic-concurrency token (must equal draft's current updated_at). |
| [ ] | DiffPolicyDraft | READ_ONLY | DiffPolicyDraftRequest | `{actor:{subject:"<seed:subject>",scopes:["udb:authz:policy:read"]}, draft_id:"<seed:policy_draft_id>"}` (optional `against_version_id`) | scope `udb:authz:policy:read`. |
| [ ] | SubmitPolicyDraft | MUTATION | SubmitPolicyDraftRequest | `{actor:{subject:"<seed:subject>",scopes:["udb:authz:policy:write"]}, draft_id:"<seed:policy_draft_id>", expected_updated_at_unix:<epoch>}` | scope `udb:authz:policy:write`. |
| [ ] | ApprovePolicyDraft | MUTATION | ApprovePolicyDraftRequest | `{actor:{subject:"<seed:subject>",scopes:["udb:authz:policy:approve"]}, draft_id:"<seed:policy_draft_id>", reviewer:"<seed:subject>", reason:"ok"}` | scope `udb:authz:policy:approve`. Reviewer should differ from submitter (separation of duties). |
| [ ] | RejectPolicyDraft | MUTATION | RejectPolicyDraftRequest | `{actor:{subject:"<seed:subject>",scopes:["udb:authz:policy:approve"]}, draft_id:"<seed:policy_draft_id>", reviewer:"<seed:subject>", reason:"nack"}` | scope `udb:authz:policy:approve`. |
| [ ] | ActivatePolicyVersion | DESTRUCTIVE | ActivatePolicyVersionRequest | `{actor:{subject:"<seed:subject>",scopes:["udb:authz:admin"]}, policy_version_id:"<seed:policy_id>", expected_revision:<n>, expected_policy_revision:<n>, expected_relationship_revision:<n>}` | scope `udb:authz:admin`. Mutates live snapshot. revisions = optimistic concurrency (use values from GetAuthzRevision). |
| [ ] | RollbackPolicyVersion | DESTRUCTIVE | RollbackPolicyVersionRequest | `{actor:{subject:"<seed:subject>",scopes:["udb:authz:admin"]}, policy_set_id:"<seed:policy_id>", target_version_id:"<seed:policy_id>", change_reason:"revert"}` | scope `udb:authz:admin`. target_version_id empty = policy set's rollback_version_id. |
| [ ] | ActivateCanary | DESTRUCTIVE | ActivateCanaryRequest | `{actor:{subject:"<seed:subject>",scopes:["udb:authz:admin"]}, policy_version_id:"<seed:policy_id>", scope_kind:"CANARY_SCOPE_KIND_PERCENT", scope_values:["10"], success_window_secs:300, metric_threshold:0.99, min_samples:100, expected_revision:<n>}` | scope `udb:authz:admin`. scope_kind enum→NAME; PERCENT → scope_values=[1..100]; NODE/TENANT → id list. |
| [ ] | PromoteCanary | DESTRUCTIVE | PromoteCanaryRequest | `{actor:{subject:"<seed:subject>",scopes:["udb:authz:admin"]}, canary_id:"<seed:policy_id>", expected_revision:<n>}` | scope `udb:authz:admin`. Fails unless canary promote-eligible. |
| [ ] | GetCanaryStatus | READ_ONLY | GetCanaryStatusRequest | `{actor:{subject:"<seed:subject>",scopes:["udb:authz:policy:read"]}, canary_id:"<seed:policy_id>"}` | scope `udb:authz:policy:read`. |
| [ ] | ListPolicyVersions | READ_ONLY | ListPolicyVersionsRequest | `{actor:{subject:"<seed:subject>",scopes:["udb:authz:policy:read"]}, tenant_id:"<seed:tenant_id>", project_id:"<seed:project>", policy_set_id:"<seed:policy_id>", state:"POLICY_VERSION_STATE_ACTIVE", page:{page_size:50}}` | scope `udb:authz:policy:read`. state enum→NAME. |
| [ ] | SimulatePolicy | MUTATION | SimulatePolicyRequest | `{actor:{subject:"<seed:subject>",scopes:["udb:authz:policy:read"]}, tenant_id:"<seed:tenant_id>", project_id:"<seed:project>", draft_id:"<seed:policy_draft_id>", cases:[{principal:{subject:"<seed:subject>"}, resource:{resource_type:"<seed:resource>"}, action:"<seed:action>", label:"c1"}], persist:false}` | scope `udb:authz:policy:read` (note op_kind=MUTATION but scope is read). cases = repeated SimulationCase. Empty draft_id + candidate = ad-hoc PolicyDocument. Never mutates durable state. |
| [ ] | ExplainPolicy | READ_ONLY | ExplainPolicyRequest | `{actor:{subject:"<seed:subject>",scopes:["udb:authz:policy:read"]}, tenant_id:"<seed:tenant_id>", project_id:"<seed:project>", test_case:{principal:{subject:"<seed:subject>"}, resource:{resource_type:"<seed:resource>"}, action:"<seed:action>"}}` (empty draft_id = active snapshot) | scope `udb:authz:policy:read`. test_case = single SimulationCase. |
| [ ] | GetAuthzRevision | READ_ONLY | GetAuthzRevisionRequest | `{tenant_id:"<seed:tenant_id>", project_id:"<seed:project>"}` | scope `udb:authz:policy:read`. No actor field. |
| [ ] | InvalidatePolicyBundles | DESTRUCTIVE | InvalidatePolicyBundlesRequest | `{actor:{subject:"<seed:subject>",scopes:["udb:authz:admin"]}, tenant_id:"<seed:tenant_id>", project_id:"<seed:project>", reason:"rotate"}` | scope `udb:authz:admin`. |
| [ ] | SeedBuiltinRoles | MUTATION | SeedBuiltinRolesRequest | `{actor:{subject:"<seed:subject>",scopes:["udb:authz:admin"]}, tenant_id:"<seed:tenant_id>", project_id:"<seed:project>"}` | scope `udb:authz:admin`. |
| [ ] | MigrateLegacyPolicies | DESTRUCTIVE | MigrateLegacyPoliciesRequest | `{actor:{subject:"<seed:subject>",scopes:["udb:authz:admin"]}, tenant_id:"<seed:tenant_id>", project_id:"<seed:project>", apply:false, policy_set_name:"default"}` | scope `udb:authz:admin`. apply=false → report only; apply=true → writes governed draft. |


## ApiKeyService

_proto: core/apikey/services/v1/apikey_service.proto · 9 RPCs_

Request messages live in `core/apikey/services/v1/core.proto`. Enums (`ApiKeyOwnerType`, `ApiKeyStatus`) in `core/apikey/entity/v1/enums.proto`. `RequestContext` / `TenantContext` / `PageRequest` in `core/common/v1/{types,dto}.proto`.

Common shapes:
- `context` (RequestContext) — `tenant` (TenantContext: `tenant_id`, `organization_id`, `project_id`, `environment`, `region`, ...), `request_id`, `correlation_id`, `user_id`, `idempotency_key`, `scopes`, `roles`, ... Provide `context.tenant.tenant_id` = `<seed:tenant_id>` since every RPC has `tenant_required: true`.
- `ApiKeyOwnerType` enum: `API_KEY_OWNER_TYPE_UNSPECIFIED|INTEGRATION|CICD|ANALYTICS|TENANT|PROJECT|SERVICE_ACCOUNT|WORKLOAD`.
- `ApiKeyStatus` enum: `API_KEY_STATUS_UNSPECIFIED|ACTIVE|REVOKED|EXPIRED`.
- `PageRequest`: `page` (int32), `page_size` (int32), `page_token` (string).

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
|---|---|---|---|---|---|
| [ ] | CreateApiKey | MUTATION | CreateApiKeyRequest | `name`="bench-key" (string), `description`="bench" (string), `owner_type`=`API_KEY_OWNER_TYPE_SERVICE_ACCOUNT` (enum), `owner_id`=`<seed:owner_id>` (string), `scopes`=["resource:read"] (repeated string), `ip_allowlist`=[] (repeated string CIDR; empty=unrestricted), `rate_limit_per_minute`=0 (int32; 0=default 60), `rate_limit_per_day`=0 (int64; 0=default 10000), `expires_at`=null (Timestamp; null=never), `context.tenant.tenant_id`=`<seed:tenant_id>` (RequestContext) | Returns `plain_key` ONCE — capture it as `<seed:plain_key>` and the returned `key.key_id` as `<seed:key_id>` for downstream RPCs. |
| [ ] | GetApiKey | READ_ONLY | GetApiKeyRequest | `key_id`=`<seed:key_id>` (string) | No `context` field on this msg. NOT_FOUND without a real seeded key_id. |
| [ ] | ListApiKeys | READ_ONLY | ListApiKeysRequest | `owner_id`=`<seed:owner_id>` (string), `owner_type`=`API_KEY_OWNER_TYPE_SERVICE_ACCOUNT` (enum), `status`=`API_KEY_STATUS_ACTIVE` (enum), `page.page`=1, `page.page_size`=50 (PageRequest) | All fields optional filters; empty body also valid. No `context` field. |
| [ ] | UpdateApiKey | MUTATION | UpdateApiKeyRequest | `key_id`=`<seed:key_id>` (string), `name`="bench-key-2" (string), `description`="updated" (string), `scopes`=["resource:read"] (repeated string), `ip_allowlist`=[] (repeated string), `rate_limit_per_minute`=0 (int32), `rate_limit_per_day`=0 (int64), `expires_at`=null (Timestamp), `context.tenant.tenant_id`=`<seed:tenant_id>` | NOT_FOUND without real seeded key_id. |
| [ ] | RevokeApiKey | MUTATION | RevokeApiKeyRequest | `key_id`=`<seed:key_id>` (string), `revoke_reason`="bench cleanup" (string), `context.tenant.tenant_id`=`<seed:tenant_id>` | Needs a real seeded key_id (NOT_FOUND otherwise). Destructive — run after Get/List/Stats. |
| [ ] | RotateApiKey | MUTATION | RotateApiKeyRequest | `key_id`=`<seed:key_id>` (string), `rotation_reason`="bench rotate" (string), `context.tenant.tenant_id`=`<seed:tenant_id>` | Needs a real seeded key_id. Returns a NEW `plain_key` ONCE; old secret invalidated. |
| [ ] | EmergencyRevokeApiKeys | DESTRUCTIVE | EmergencyRevokeApiKeysRequest | At least one selector required: `key_prefix`="" (string), `owner_id`=`<seed:owner_id>` (string), `tenant_id`=`<seed:tenant_id>` (string), `project_id`=`<seed:project>` (string), `scope`="resource:read" (string), `created_before`=null (Timestamp), `reason`="bench emergency" (string), `context.tenant.tenant_id`=`<seed:tenant_id>` | Per-record tenant/owner authority enforced. Set `owner_id`=`<seed:owner_id>` to scope to bench-created keys only. Truly destructive — run last. |
| [ ] | ValidateApiKey | READ_ONLY | ValidateApiKeyRequest | `plain_key`=`<seed:plain_key>` (string, raw key from CreateApiKey), `endpoint`="/v1/test" (string), `required_scope`="resource:read" (string), `ip_address`="127.0.0.1" (string) | No `context` field. Needs the real plain_key captured at create time; returns `valid=false` (not error) for an unknown key. |
| [ ] | GetApiKeyUsageStats | READ_ONLY | GetApiKeyUsageStatsRequest | `key_id`=`<seed:key_id>` (string), `from`=null (Timestamp), `to`=null (Timestamp) | No `context` field. Needs real seeded key_id. |


## IdentityProviderService

_proto: core/idp/services/v1/identity_provider_service.proto · 27 RPCs_

All RPCs are tenant-scoped and server-only (control-plane, native auth listener). Every request requires Bearer JWT / session auth with the per-RPC scope and a resolvable tenant. Seed legend: `<seed:tenant_id>`, `<seed:project>`, `<seed:user_id>`, `<seed:provider_id>` (a seeded IdP provider), `<seed:session_id>`.

### Shared nested types (grounded)

- `RequestContext context` (common/v1/types.proto): `tenant{ tenant_id, organization_id, project_id, environment, region, partition_id }`, `request_id`, `correlation_id`, `user_id`, `headers map<string,string>`, `trace_id`, `span_id`, `ip_address`, `user_agent`, `timestamp`, `principal_id`, `service_identity`, `scopes []`, `roles []`, `purpose`, `idempotency_key`, `client_catalog_version`, `consistency`, `attributes map<string,string>`, `traceparent`. Minimal valid: `{ tenant: { tenant_id: "<seed:tenant_id>" } }`.
- `PageRequest page` (common/v1/dto.proto): `page int32`, `page_size int32`, `page_token string`. Minimal valid: `{ page: 1, page_size: 20 }`.
- Enum `IdpKind` (idp/entity/v1/enums.proto): `IDP_KIND_UNSPECIFIED`, `IDP_KIND_NATIVE`, `IDP_KIND_OIDC`, `IDP_KIND_SAML`, `IDP_KIND_LDAP`, `IDP_KIND_CUSTOM_JWT`, `IDP_KIND_EXTERNAL_SESSION`.
- Enum `AssuranceLevel`: `..._UNSPECIFIED|NONE|LOW|SINGLE_FACTOR|MULTI_FACTOR|HARDWARE` (response-only).
- Enum `ProviderHealth`: `..._UNSPECIFIED|HEALTHY|DEGRADED|UNREACHABLE` (response-only).

### CreateProvider varchar(24) overflow (record this)

`IdentityProvider.kind` (entity proto line 72) is persisted to column `kind VARCHAR(24)`, but the runtime stores the full enum NAME. The longest name `IDP_KIND_EXTERNAL_SESSION` is **25 chars → overflows VARCHAR(24)**. `IDP_KIND_CUSTOM_JWT` (19), `IDP_KIND_OIDC` (13), `IDP_KIND_SAML` (13), `IDP_KIND_NATIVE` (15), `IDP_KIND_LDAP` (13) all fit. For a benchable success body use a kind ≤24 chars, e.g. **`kind: IDP_KIND_OIDC`** (13 chars). Avoid `IDP_KIND_EXTERNAL_SESSION` until the column is widened.

### Checklist

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
|------|-----|---------|-------------|------------|-------------------|
| [ ] | CreateProvider | MUTATION | CreateProviderRequest | `{ tenant_id, kind: IDP_KIND_OIDC, display_name: "Acme OIDC", issuer: "https://idp.example.com", jwks_url: "https://idp.example.com/jwks", client_ids: ["client-1"], audiences: ["udb"], claim_mapping_json: "{}", group_mapping_json: "{}", jit_policy_json: "{}", account_linking_policy: "explicit", enabled: true, created_by: "<seed:user_id>", context: {tenant:{tenant_id}} }` | tenant_id=`<seed:tenant_id>`. **kind must be ≤24-char name (use IDP_KIND_OIDC, NOT IDP_KIND_EXTERNAL_SESSION=25ch → VARCHAR(24) overflow).** display_name unique per tenant. client_secret/saml_signing_key_pem write-only optional. |
| [ ] | UpdateProvider | MUTATION | UpdateProviderRequest | `{ provider_id, tenant_id, display_name: "Acme OIDC v2", claim_mapping_json: "{}", group_mapping_json: "{}", jit_policy_json: "{}", account_linking_policy: "explicit", updated_by: "<seed:user_id>", context: {tenant:{tenant_id}} }` | provider_id=`<seed:provider_id>`, tenant_id=`<seed:tenant_id>`. client_secret/saml_signing_key_pem empty = unchanged. |
| [ ] | DisableProvider | MUTATION | DisableProviderRequest | `{ provider_id, tenant_id, updated_by: "<seed:user_id>", context: {tenant:{tenant_id}} }` | provider_id=`<seed:provider_id>`, tenant_id=`<seed:tenant_id>`. |
| [ ] | GetProvider | READ_ONLY | GetProviderRequest | `{ provider_id, tenant_id }` | provider_id=`<seed:provider_id>`, tenant_id=`<seed:tenant_id>`. No context field. |
| [ ] | ListProviders | READ_ONLY | ListProvidersRequest | `{ tenant_id, kind: IDP_KIND_UNSPECIFIED, enabled_only: false, page: {page:1,page_size:20} }` | tenant_id=`<seed:tenant_id>`. kind unspecified = all. |
| [ ] | TestProviderDiscovery | READ_ONLY | TestProviderDiscoveryRequest | `{ provider_id, tenant_id }` | provider_id=`<seed:provider_id>`, tenant_id=`<seed:tenant_id>`. Needs reachable external OIDC/SAML discovery endpoint to return reachable=true. |
| [ ] | ForceJwksRefresh | MUTATION | ForceJwksRefreshRequest | `{ provider_id, tenant_id }` | provider_id=`<seed:provider_id>`, tenant_id=`<seed:tenant_id>`. Needs the provider's external jwks_url reachable. |
| [ ] | PreviewClaimMapping | READ_ONLY | PreviewClaimMappingRequest | `{ provider_id, tenant_id, claims_json: "{\"sub\":\"abc\",\"email\":\"a@x.com\"}", claim_mapping_json: "" }` | provider_id=`<seed:provider_id>`, tenant_id=`<seed:tenant_id>`. claims_json = raw IdP token payload JSON object. claim_mapping_json empty = use stored mapping. No external call. |
| [ ] | PreviewGroupMapping | READ_ONLY | PreviewGroupMappingRequest | `{ provider_id, tenant_id, groups: ["admins"], group_mapping_json: "" }` | provider_id=`<seed:provider_id>`, tenant_id=`<seed:tenant_id>`. group_mapping_json empty = use stored mapping. No external call. |
| [ ] | ListExternalIdentities | READ_ONLY | ListExternalIdentitiesRequest | `{ tenant_id, provider_id: "", user_id: "", page: {page:1,page_size:20} }` | tenant_id=`<seed:tenant_id>`. provider_id/user_id empty = all; optionally `<seed:provider_id>` / `<seed:user_id>`. |
| [ ] | LinkIdentity | MUTATION | LinkIdentityRequest | `{ tenant_id, provider_id, subject: "ext-subject-1", user_id, email: "a@x.com", email_verified: true, context: {tenant:{tenant_id}} }` | tenant_id=`<seed:tenant_id>`, provider_id=`<seed:provider_id>`, user_id=`<seed:user_id>`. |
| [ ] | UnlinkIdentity | MUTATION | UnlinkIdentityRequest | `{ tenant_id, external_identity_id, context: {tenant:{tenant_id}} }` | tenant_id=`<seed:tenant_id>`. external_identity_id = id from a prior LinkIdentity/ListExternalIdentities (no dedicated seed; chain from LinkIdentity). |
| [ ] | ImportSamlMetadata | MUTATION | ImportSamlMetadataRequest | `{ provider_id, tenant_id, metadata_xml: "<EntityDescriptor ...>...</EntityDescriptor>", updated_by: "<seed:user_id>", context: {tenant:{tenant_id}} }` | provider_id=`<seed:provider_id>` (must be a SAML provider, IDP_KIND_SAML). metadata_xml = real SAML 2.0 IdP metadata XML; if empty, stored saml_metadata_url is fetched (needs external IdP). **Needs external SAML IdP metadata.** |
| [ ] | StartSamlLogin | MUTATION | StartSamlLoginRequest | `{ provider_id, tenant_id, relay_state: "state-1" }` | provider_id=`<seed:provider_id>` (SAML provider with imported metadata/SSO URL). **Needs a configured SAML provider (entity_id + SSO URL imported).** |
| [ ] | SamlAcs | MUTATION | SamlAcsRequest | `{ provider_id, tenant_id, saml_response: "<base64 SAMLResponse>", relay_state: "state-1", context: {tenant:{tenant_id}} }` | provider_id=`<seed:provider_id>`. saml_response = base64 IdP-signed SAMLResponse. **Needs a real signed SAMLResponse from an external SAML IdP; ungroundable without external IdP round-trip.** |
| [ ] | ResolveExternalIdentity | MUTATION | ResolveExternalIdentityRequest | `{ provider_id, tenant_id, claims_json: "{\"sub\":\"abc\",\"email\":\"a@x.com\",\"email_verified\":true}" }` | provider_id=`<seed:provider_id>`, tenant_id=`<seed:tenant_id>`. claims_json = already-verified external claims JSON. JIT-provisions/links a user (depends on provider jit_policy). |
| [ ] | ScimCreateUser | MUTATION | ScimCreateUserRequest | `{ tenant_id, provider_id, scim_user_json: "{\"userName\":\"a@x.com\",\"active\":true}", context: {tenant:{tenant_id}} }` | tenant_id=`<seed:tenant_id>`, provider_id=`<seed:provider_id>`. scim_user_json = SCIM 2.0 User JSON. Provider should be SCIM-capable (external provisioning connector). |
| [ ] | ScimGetUser | MUTATION | ScimGetUserRequest | `{ tenant_id, provider_id, scim_user_id }` | tenant_id=`<seed:tenant_id>`, provider_id=`<seed:provider_id>`. scim_user_id from a prior ScimCreateUser (chain; no dedicated seed). op_kind=MUTATION (proto declares MUTATION despite GET verb). |
| [ ] | ScimListUsers | MUTATION | ScimListUsersRequest | `{ tenant_id, provider_id, filter: "", page: {page:1,page_size:20} }` | tenant_id=`<seed:tenant_id>`, provider_id=`<seed:provider_id>`. filter = SCIM filter, e.g. `userName eq "x"`. op_kind=MUTATION per proto. |
| [ ] | ScimReplaceUser | MUTATION | ScimReplaceUserRequest | `{ tenant_id, provider_id, scim_user_id, scim_user_json: "{\"userName\":\"a@x.com\",\"active\":true}", context: {tenant:{tenant_id}} }` | tenant_id=`<seed:tenant_id>`, provider_id=`<seed:provider_id>`. scim_user_id chained from ScimCreateUser. |
| [ ] | ScimPatchUser | MUTATION | ScimPatchUserRequest | `{ tenant_id, provider_id, scim_user_id, operations: [{op:"replace", path:"active", value_json:"false"}], context: {tenant:{tenant_id}} }` | tenant_id=`<seed:tenant_id>`, provider_id=`<seed:provider_id>`. operations = repeated ScimPatchOp{op(add|replace|remove), path, value_json}. scim_user_id chained. |
| [ ] | ScimDeleteUser | MUTATION | ScimDeleteUserRequest | `{ tenant_id, provider_id, scim_user_id, context: {tenant:{tenant_id}} }` | tenant_id=`<seed:tenant_id>`, provider_id=`<seed:provider_id>`. scim_user_id chained. Maps to deactivate + session revoke. |
| [ ] | ScimCreateGroup | MUTATION | ScimCreateGroupRequest | `{ tenant_id, provider_id, scim_group_json: "{\"displayName\":\"admins\"}", context: {tenant:{tenant_id}} }` | tenant_id=`<seed:tenant_id>`, provider_id=`<seed:provider_id>`. scim_group_json = SCIM 2.0 Group JSON. |
| [ ] | ScimGetGroup | MUTATION | ScimGetGroupRequest | `{ tenant_id, provider_id, scim_group_id }` | tenant_id=`<seed:tenant_id>`, provider_id=`<seed:provider_id>`. scim_group_id chained from ScimCreateGroup. op_kind=MUTATION per proto. |
| [ ] | ScimListGroups | MUTATION | ScimListGroupsRequest | `{ tenant_id, provider_id, filter: "", page: {page:1,page_size:20} }` | tenant_id=`<seed:tenant_id>`, provider_id=`<seed:provider_id>`. op_kind=MUTATION per proto. |
| [ ] | ScimPatchGroup | MUTATION | ScimPatchGroupRequest | `{ tenant_id, provider_id, scim_group_id, operations: [{op:"add", path:"members", value_json:"[\"scim-user-id\"]"}], context: {tenant:{tenant_id}} }` | tenant_id=`<seed:tenant_id>`, provider_id=`<seed:provider_id>`. scim_group_id chained. |
| [ ] | ScimDeleteGroup | MUTATION | ScimDeleteGroupRequest | `{ tenant_id, provider_id, scim_group_id, context: {tenant:{tenant_id}} }` | tenant_id=`<seed:tenant_id>`, provider_id=`<seed:provider_id>`. scim_group_id chained. |


## TenantService

_proto: core/tenant/services/v1/tenant_service.proto · 6 RPCs_

All request messages are defined inline in `tenant_service.proto`; every request field is a proto3 scalar `string`/`int32` (no enums, nested messages, oneofs, or repeated fields on the request side). `type`/`status` are free-form `string` columns, NOT proto enums. All 6 RPCs carry `tenant_required: true` + `request_context_required: true` (must send tenant context + bearer JWT/session).

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
|------|-----|---------|-------------|------------|-------------------|
| [ ] | CreateTenant | MUTATION | CreateTenantRequest | `code` (string, e.g. `"acme-bench"`), `name` (string, e.g. `"Acme Bench"`), `type` (string, e.g. `"organization"`), `parent_tenant_id` (string, optional — `<seed:tenant_id>` or empty for root), `config` (string JSON, e.g. `"{}"`), `branding` (string JSON, e.g. `"{}"`) | Only `code`+`name` are realistically load-bearing; `config`/`branding` are JSON strings — send `"{}"`, not bare `""`, to be safe. `parent_tenant_id` must reference an existing tenant if set. Creates a new tenant row each call → use a unique `code` per bench iteration to avoid uniqueness conflicts. |
| [ ] | GetTenant | READ_ONLY | GetTenantRequest | `tenant_id` (string) = `<seed:tenant_id>` | Single lookup by id. |
| [ ] | ListTenants | READ_ONLY | ListTenantsRequest | `type` (string filter, optional), `status` (string filter, optional), `page` (int32, e.g. `1`), `page_size` (int32, e.g. `20`) | All fields optional filters/pagination; empty `type`/`status` = no filter. Keep `page_size` modest (e.g. 20) for bench stability. |
| [ ] | UpdateTenant | MUTATION | UpdateTenantRequest | `tenant_id` (string) = `<seed:tenant_id>`, `name` (string), `status` (string, e.g. `"active"`), `config` (string JSON `"{}"`), `branding` (string JSON `"{}"`) | `tenant_id` must reference an existing tenant. `status` is a free-form string column (no enum). |
| [ ] | GetTenantConfig | READ_ONLY | GetTenantConfigRequest | `tenant_id` (string) = `<seed:tenant_id>` | Returns repeated TenantConfig rows for the tenant. |
| [ ] | UpdateTenantConfig | MUTATION | UpdateTenantConfigRequest | `tenant_id` (string) = `<seed:tenant_id>`, `config_key` (string, e.g. `"feature.flag"`), `config_value` (string, e.g. `"on"`), `type` (string, e.g. `"string"`) | Upserts one config key/value for the tenant. `tenant_id` must reference an existing tenant. |

### Quota / RESOURCE_EXHAUSTED notes
- **CreateTenant** is the only growth RPC: each call inserts a tenant. Repeated bench calls can hit a tenant-count quota or unique-`code` collisions. Avoid by (a) using a unique `code` per iteration and (b) cleaning up created tenants, or running CreateTenant with low iteration counts. The other 5 RPCs are idempotent reads/updates against a seeded `<seed:tenant_id>` and are not quota-prone.
- All RPCs are `requires_postgres: true` and `tenant_required: true`; an unseeded/missing tenant context yields auth/NOT_FOUND, not a valid-body failure.


## AnalyticsService

_proto: core/analytics/services/v1/analytics_service.proto · 7 RPCs_

All request/response messages live in `core/analytics/services/v1/core.proto` (imported by the service proto). `RequestContext` resolves to `udb.core.common.v1.RequestContext` (`core/common/v1/types.proto`, has nested `TenantContext tenant`), NOT the `udb.entity.v1` one. `PageRequest`/`PageResponse` are from `core/common/v1/dto.proto`. All 7 RPCs require auth (`AUTH_MODE_BEARER`), `request_context_required: true`, and `tenant_required: true`.

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | RecordPipelineMetric | MUTATION | RecordPipelineMetricRequest | `{ "stage_name": "<seed:stage_name>", "tenant_id": "<seed:tenant_id>", "latency_ms": 12.5, "is_success": true, "context": { "tenant": { "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>" }, "request_id": "..." } }` | Fields: `stage_name` string(1), `tenant_id` string(2), `latency_ms` double(3), `is_success` bool(4), `context` RequestContext(5). Set `stage_name`+`tenant_id` non-empty to avoid INTERNAL on empty input. `context.tenant.tenant_id` should match `tenant_id`. POST body `*`. |
| [ ] | GetPipelineSummary | READ_ONLY | GetPipelineSummaryRequest | `{ "stage_name": "<seed:stage_name>", "tenant_id": "<seed:tenant_id>", "hour_from": "2026-06-01T00", "hour_to": "2026-06-14T23", "page": { "page": 1, "page_size": 50 } }` | Fields: `stage_name` string(1, empty=all stages), `tenant_id` string(2), `hour_from` string(3, ISO-8601 hour), `hour_to` string(4), `page` PageRequest(5){page int32, page_size int32, page_token string}. Provide `tenant_id` + ISO hour range to avoid INTERNAL; `stage_name` may be empty. |
| [ ] | GetExecutorPerformance | READ_ONLY | GetExecutorPerformanceRequest | `{ "executor_identity": "", "workload_kind": "", "date_from": "2026-06-01", "date_to": "2026-06-14" }` | Fields: `executor_identity` string(1, empty=all executors), `workload_kind` string(2), `date_from` string(3, ISO date), `date_to` string(4). No tenant_id field on body (tenant comes from auth/RequestContext metadata). Provide `date_from`+`date_to` to avoid INTERNAL on empty/unparseable date input. |
| [ ] | GetReconciliationAnalytics | READ_ONLY | GetReconciliationAnalyticsRequest | `{ "date_from": "2026-06-01", "date_to": "2026-06-14" }` | Fields: `date_from` string(1, ISO date), `date_to` string(2). Only two fields; tenant from auth context. Provide valid date strings to avoid INTERNAL. |
| [ ] | GetThroughput | READ_ONLY | GetThroughputRequest | `{ "tenant_id": "<seed:tenant_id>", "hour_from": "2026-06-01T00", "hour_to": "2026-06-14T23" }` | Fields: `tenant_id` string(1), `hour_from` string(2, ISO-8601 hour), `hour_to` string(3). Provide `tenant_id` + ISO hour range to avoid INTERNAL. |
| [ ] | GetSlaCompliance | READ_ONLY | GetSlaComplianceRequest | `{ "stage_name": "<seed:stage_name>", "date_from": "2026-06-01", "date_to": "2026-06-14", "p99_threshold_ms": 250.0, "error_rate_threshold": 0.01 }` | Fields: `stage_name` string(1), `date_from` string(2, ISO date), `date_to` string(3), `p99_threshold_ms` double(4), `error_rate_threshold` double(5). Provide `stage_name`+date range+thresholds to avoid INTERNAL; thresholds default 0 if omitted (everything fails SLA). |
| [ ] | TriggerSnapshot | MUTATION | TriggerSnapshotRequest | `{ "stage_name": "<seed:stage_name>", "hour": "2026-06-14T10", "context": { "tenant": { "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>" }, "request_id": "..." } }` | Fields: `stage_name` string(1, empty=all stages), `hour` string(2, ISO-8601 hour; empty=previous complete hour), `context` RequestContext(3). Both `stage_name` and `hour` may be empty (defaults), but `context.tenant.tenant_id` should be set. POST body `*`. |

### Field reference (grounded)

- **RecordPipelineMetricRequest** (`core.proto:22`): `stage_name` string, `tenant_id` string, `latency_ms` double, `is_success` bool, `context` `udb.core.common.v1.RequestContext`.
- **GetPipelineSummaryRequest** (`core.proto:57`): `stage_name` string, `tenant_id` string, `hour_from` string, `hour_to` string, `page` `udb.core.common.v1.PageRequest`.
- **GetExecutorPerformanceRequest** (`core.proto:92`): `executor_identity` string, `workload_kind` string, `date_from` string, `date_to` string.
- **GetReconciliationAnalyticsRequest** (`core.proto:125`): `date_from` string, `date_to` string.
- **GetThroughputRequest** (`core.proto:158`): `tenant_id` string, `hour_from` string, `hour_to` string.
- **GetSlaComplianceRequest** (`core.proto:189`): `stage_name` string, `date_from` string, `date_to` string, `p99_threshold_ms` double, `error_rate_threshold` double.
- **TriggerSnapshotRequest** (`core.proto:243`): `stage_name` string, `hour` string, `context` `udb.core.common.v1.RequestContext`.

Nested types:

- **RequestContext** (`core/common/v1/types.proto:33`): `tenant` `TenantContext`, `request_id` string, `correlation_id` string, `user_id` string, `headers` map<string,string>, `trace_id` string, `span_id` string, `ip_address` string, `user_agent` string, `timestamp` Timestamp, `principal_id` string, `service_identity` string, `scopes` repeated string, `roles` repeated string, `purpose` string, `idempotency_key` string, `client_catalog_version` string, `consistency` string, `attributes` map<string,string>, `traceparent` string.
- **TenantContext** (`core/common/v1/types.proto:14`): `tenant_id` string, `organization_id` string, `project_id` string, `environment` string, `region` string, ...
- **PageRequest** (`core/common/v1/dto.proto:17`): `page` int32, `page_size` int32, `page_token` string.

No enums, oneofs, or repeated scalars appear in any request body. `<seed:message_type>` and `<seed:record_id>` legend keys are not used by AnalyticsService request bodies.


## AssetService

_proto: core/asset/services/v1/asset_service.proto · 8 RPCs_

All request messages are defined inline in `asset_service.proto`; every request field is a flat scalar (`string` / `int32`) — no enums, nested messages, `oneof`, or `repeated` appear in any request type (entity messages `PipelineDefinition` / `PipelineInstance` / `PipelineStep` / `Asset` are used in responses only). `tenant_required: true` on every RPC, so `tenant_id` is always needed.

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
|---|---|---|---|---|---|
| [ ] | CreatePipelineDefinition | MUTATION | CreatePipelineDefinitionRequest | `tenant_id=<seed:tenant_id>`, `name="thumbnail-pipeline"`, `description="Generate thumbnails"`, `media_type="image/png"`, `steps="[{\"name\":\"resize\",\"type\":\"TRANSFORM\"}]"` (JSON string), `version=1` (int32) | `steps` is a JSON string (field 5), not a list. Returns `definition_id` — capture as `<seed:definition_id>`. |
| [ ] | GetPipelineDefinition | READ_ONLY | GetPipelineDefinitionRequest | `tenant_id=<seed:tenant_id>`, `definition_id=<seed:definition_id>` | definition_id must exist (from CreatePipelineDefinition) or NOT_FOUND. |
| [ ] | RegisterAsset | MUTATION | RegisterAssetRequest | `tenant_id=<seed:tenant_id>`, `project_id=<seed:project>`, `file_id=<seed:file_id>`, `name="logo.png"`, `media_type="image/png"`, `metadata="{\"source\":\"upload\"}"` (JSON string) | `file_id` references an existing StorageService file (`<seed:file_id>`). Returns `asset_id` — capture as `<seed:asset_id>`. |
| [ ] | StartPipeline | MUTATION | StartPipelineRequest | `tenant_id=<seed:tenant_id>`, `definition_id=<seed:definition_id>`, `asset_id=<seed:asset_id>`, `context="{}"` (JSON string), `correlation_id="run-001"` | Needs a real `definition_id` AND `asset_id` or INVALID_ARGUMENT/NOT_FOUND. Returns `instance_id` — capture as `<seed:instance_id>`. |
| [ ] | GetPipeline | READ_ONLY | GetPipelineRequest | `tenant_id=<seed:tenant_id>`, `instance_id=<seed:instance_id>` | instance_id from StartPipeline. Response carries instance + repeated steps. |
| [ ] | CompleteStep | MUTATION | CompleteStepRequest | `tenant_id=<seed:tenant_id>`, `step_id=<seed:step_id>`, `status="COMPLETED"`, `result="{}"` (JSON string), `error_message=""` | `status` is a STRING field (not a proto enum); valid values per proto comment: `COMPLETED` \| `SKIPPED` \| `FAILED`. `step_id` must be a real step from a started pipeline (GetPipeline response `steps[].id`) or INVALID_ARGUMENT. `error_message` only meaningful when status=FAILED. |
| [ ] | ListAssets | READ_ONLY | ListAssetsRequest | `tenant_id=<seed:tenant_id>`, `media_type="image/png"` (optional filter), `status=""` (optional filter), `page=1` (int32), `page_size=20` (int32) | `media_type`/`status` are optional string filters; omit (empty) to list all. |
| [ ] | GetAsset | READ_ONLY | GetAssetRequest | `tenant_id=<seed:tenant_id>`, `asset_id=<seed:asset_id>` | asset_id from RegisterAsset or NOT_FOUND. |

Seed legend: `tenant_id`, `project` (→ `project_id`), `definition_id`, `asset_id`, `instance_id`, `file_id`, `object_key`, `bucket`. Note: `object_key` and `bucket` are not used by any AssetService request (they belong to StorageService); AssetService references storage only via `file_id`.


## StorageService

_proto: core/storage/services/v1/storage_service.proto · 7 RPCs_

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | RegisterUpload | MUTATION | RegisterUploadRequest | `{ tenant_id: <seed:tenant_id>, project_id: <seed:project>, filename: "report.pdf", content_type: "application/pdf", file_type: "document", reference_id: <seed:file_id>, reference_type: "document", is_public: false, expires_in_minutes: 15, size_bytes: 1024 }` | fields: tenant_id(str), project_id(str), filename(str), content_type(str), file_type(str), reference_id(str), reference_type(str), is_public(optional bool), expires_in_minutes(int32), size_bytes(int64). All plain strings/scalars; is_public is proto3 optional (absent → defaults private). reference_id/reference_type are free-form app linkage, not a storage seed. size_bytes drives pre-upload tenant quota check. |
| [ ] | FinalizeUpload | MUTATION | FinalizeUploadRequest | `{ tenant_id: <seed:tenant_id>, file_id: <seed:file_id>, content_type: "application/pdf", file_type: "document", reference_id: <seed:file_id>, reference_type: "document", is_public: false, size_bytes: 1024 }` | fields: tenant_id(str), file_id(str), content_type(str), file_type(str), reference_id(str), reference_type(str), is_public(optional bool), size_bytes(int64). file_id must reference the upload registered by RegisterUpload. is_public optional (absent → leaves stored visibility unchanged). size_bytes = actual uploaded size, persisted on finalize. |
| [ ] | GetDownloadUrl | READ_ONLY | GetDownloadUrlRequest | `{ tenant_id: <seed:tenant_id>, file_id: <seed:file_id>, expires_in_minutes: 15 }` | fields: tenant_id(str), file_id(str), expires_in_minutes(int32). file_id must be a finalized file. |
| [ ] | GetFile | READ_ONLY | GetFileRequest | `{ tenant_id: <seed:tenant_id>, file_id: <seed:file_id> }` | fields: tenant_id(str), file_id(str). |
| [ ] | UpdateFile | MUTATION | UpdateFileRequest | `{ tenant_id: <seed:tenant_id>, file_id: <seed:file_id>, filename: "renamed.pdf", content_type: "application/pdf", file_type: "document", reference_id: <seed:file_id>, reference_type: "document", is_public: true }` | fields: tenant_id(str), file_id(str), filename(str), content_type(str), file_type(str), reference_id(str), reference_type(str), is_public(optional bool). Partial update: empty string fields leave value unchanged; is_public absent leaves visibility unchanged (never silently flips public/private). |
| [ ] | DeleteFile | MUTATION | DeleteFileRequest | `{ tenant_id: <seed:tenant_id>, file_id: <seed:file_id> }` | fields: tenant_id(str), file_id(str). Soft delete. |
| [ ] | ListFiles | READ_ONLY | ListFilesRequest | `{ tenant_id: <seed:tenant_id>, file_type: "document", reference_id: <seed:file_id>, reference_type: "document", uploaded_by: <seed:user_id>, page: 1, page_size: 20 }` | fields: tenant_id(str), file_type(str), reference_id(str), reference_type(str), uploaded_by(str), page(int32), page_size(int32). file_type/reference_id/reference_type/uploaded_by are optional filters (empty → no filter). uploaded_by filters by uploader user id. |


## NotificationService

_proto: core/notification/services/v1/notification_service.proto · 11 RPCs_

Enums (entity/v1/enums.proto):
- `NotificationChannel`: `NOTIFICATION_CHANNEL_UNSPECIFIED|EMAIL|SMS|PUSH|IN_APP|WEBHOOK`
- `NotificationStatus`: `NOTIFICATION_STATUS_UNSPECIFIED|PENDING|SENT|DELIVERED|FAILED|SUPPRESSED`

Shared: `RequestContext` (common.v1) carries tenant/credential context; `PageRequest` = `{page:int32, page_size:int32, page_token:string}`. All RPCs are `tenant_required` + bearer JWT/session.

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | SendNotification | MUTATION | SendNotificationRequest | `event_type:"<seed:event_type>"`, `recipient_id:"<seed:user_id>"`, `recipient_address:"user@example.com"`, `tenant_id:"<seed:tenant_id>"`, `project_id:"<seed:project>"`, `locale:"en"`, `variables:{}`, `channels:["NOTIFICATION_CHANNEL_EMAIL"]` | event_type must match a template. channels empty = template-default. Optional: resource_type/resource_id/resource_name/correlation_id. Field 13 = RequestContext context. |
| [ ] | GetNotification | READ_ONLY | GetNotificationRequest | `log_id:"<seed:log_id>"` | only field is log_id (1). |
| [ ] | ListNotifications | READ_ONLY | ListNotificationsRequest | `tenant_id:"<seed:tenant_id>"`, `page:{page:1,page_size:20}` | all filters optional: recipient_id, project_id, resource_type, resource_id, event_type, channel(enum), status(NotificationStatus enum). |
| [ ] | RetryNotification | MUTATION | RetryNotificationRequest | `log_id:"<seed:log_id>"` | log_id (1) must reference a FAILED log; field 2 = RequestContext context. |
| [ ] | UpsertTemplate | MUTATION | UpsertTemplateRequest | `event_type:"<seed:event_type>"`, `channel:"NOTIFICATION_CHANNEL_EMAIL"`, `locale:"en"`, `subject_template:"Hello {name}"`, `body_template:"Body {name}"`, `is_active:true` | field 7 = RequestContext context. |
| [ ] | GetTemplate | READ_ONLY | GetTemplateRequest | `event_type:"<seed:event_type>"`, `channel:"NOTIFICATION_CHANNEL_EMAIL"`, `locale:"en"` | keyed by event_type+channel+locale. |
| [ ] | ListTemplates | READ_ONLY | ListTemplatesRequest | `page:{page:1,page_size:20}` | all optional: event_type, channel(enum), active_only(bool). |
| [ ] | GetDeliveryStats | READ_ONLY | GetDeliveryStatsRequest | `tenant_id:"<seed:tenant_id>"`, `event_type:"<seed:event_type>"`, `date_from:"2026-01-01"`, `date_to:"2026-12-31"` | date_from/date_to format YYYY-MM-DD; event_type optional. |
| [ ] | SetPreference | MUTATION | SetPreferenceRequest | `user_id:"<seed:user_id>"`, `tenant_id:"<seed:tenant_id>"`, `channel:"NOTIFICATION_CHANNEL_EMAIL"`, `event_type:""`, `is_opted_out:true` | event_type empty = channel-wide opt-out; field 6 = RequestContext context. |
| [ ] | GetPreference | READ_ONLY | GetPreferenceRequest | `user_id:"<seed:user_id>"`, `tenant_id:"<seed:tenant_id>"`, `channel:"NOTIFICATION_CHANNEL_EMAIL"`, `event_type:""` | keyed by user_id+tenant_id+channel+event_type. |
| [ ] | ListPreferences | READ_ONLY | ListPreferencesRequest | `user_id:"<seed:user_id>"`, `tenant_id:"<seed:tenant_id>"`, `page:{page:1,page_size:20}` | lists all preferences for a user. |


## WebRTC (Room/Peer/Track/Turn/Signaling)

_proto: core/webrtc/services/v1/webrtc_service.proto · 16 RPCs_

Seed legend (substitute real values for `<seed:KEY>`): `tenant_id`, `project`, `room_id`, `peer_id`, `track_id`, `user_id`.

All RPCs require `endpoint_security` bearer auth with `tenant_required: true`; `tenant_id` is a real request field on every request message (verified in proto) and must match the authenticated tenant. `config`/`metadata`/`settings` are free-form JSON strings.

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | RoomService.CreateRoom | MUTATION | CreateRoomRequest | `tenant_id`=`<seed:tenant_id>`, `name`="bench-room", `max_participants`=10, `config`=`{}`, `created_by`=`<seed:user_id>` | name free text; max_participants int32; config JSON string; created_by user ref. Returns new `room_id`. |
| [ ] | RoomService.GetRoom | READ_ONLY | GetRoomRequest | `tenant_id`=`<seed:tenant_id>`, `room_id`=`<seed:room_id>` | room must exist. |
| [ ] | RoomService.UpdateRoom | MUTATION | UpdateRoomRequest | `tenant_id`=`<seed:tenant_id>`, `room_id`=`<seed:room_id>`, `name`="bench-room-2", `state`="active", `config`=`{}` | all fields string; `state` is a free-form string column (not a proto enum); config JSON. |
| [ ] | RoomService.CloseRoom | MUTATION | CloseRoomRequest | `tenant_id`=`<seed:tenant_id>`, `room_id`=`<seed:room_id>` | room must exist; cascades peer.left/track.ended emits. |
| [ ] | RoomService.ListRooms | READ_ONLY | ListRoomsRequest | `tenant_id`=`<seed:tenant_id>`, `state`="active", `page`=1, `page_size`=20 | `state` filter is optional free-form string; page/page_size int32. |
| [ ] | PeerService.JoinRoom | MUTATION | JoinRoomRequest | `tenant_id`=`<seed:tenant_id>`, `room_id`=`<seed:room_id>`, `display_name`="Bench User", `metadata`=`{}`, `user_agent`="bench/1.0" | room must exist+open. metadata JSON string. Returns new `peer_id` + existing_peers. |
| [ ] | PeerService.LeaveRoom | MUTATION | LeaveRoomRequest | `tenant_id`=`<seed:tenant_id>`, `room_id`=`<seed:room_id>`, `peer_id`=`<seed:peer_id>` | peer must be in room. |
| [ ] | PeerService.GetPeer | READ_ONLY | GetPeerRequest | `tenant_id`=`<seed:tenant_id>`, `peer_id`=`<seed:peer_id>` | no room_id; peer looked up by id within tenant. |
| [ ] | PeerService.ListPeers | READ_ONLY | ListPeersRequest | `tenant_id`=`<seed:tenant_id>`, `room_id`=`<seed:room_id>`, `state`="connected" | `state` optional free-form string filter. |
| [ ] | TrackService.PublishTrack | MUTATION | PublishTrackRequest | `tenant_id`=`<seed:tenant_id>`, `room_id`=`<seed:room_id>`, `peer_id`=`<seed:peer_id>`, `kind`="audio", `label`="mic", `settings`=`{}`, `metadata`=`{}` | `kind` free-form string (e.g. "audio"/"video"); settings+metadata JSON. peer must exist. Returns new `track_id`. |
| [ ] | TrackService.UnpublishTrack | MUTATION | UnpublishTrackRequest | `tenant_id`=`<seed:tenant_id>`, `track_id`=`<seed:track_id>` | track must exist (no room_id needed). |
| [ ] | TrackService.MuteTrack | MUTATION | MuteTrackRequest | `tenant_id`=`<seed:tenant_id>`, `track_id`=`<seed:track_id>`, `muted`=true | `muted` bool toggles mute state. |
| [ ] | TrackService.ListTracks | READ_ONLY | ListTracksRequest | `tenant_id`=`<seed:tenant_id>`, `room_id`=`<seed:room_id>`, `peer_id`=`<seed:peer_id>`, `kind`="audio" | peer_id + kind are optional free-form filters. |
| [ ] | TurnService.IssueCredentials | MUTATION | IssueCredentialsRequest | `tenant_id`=`<seed:tenant_id>`, `room_id`=`<seed:room_id>`, `peer_id`=`<seed:peer_id>`, `ttl_seconds`=3600 | ttl_seconds int32. Returns ephemeral ICE servers + signed username/credential. TURN config must be present (fail-closed). |
| [ ] | SignalingService.Signal | MUTATION | SignalRequest (stream) | per-message: `room_id`=`<seed:room_id>`, `peer_id`=`<seed:peer_id>`, `tenant_id`=`<seed:tenant_id>` + ONE oneof payload: `ping`=true \| `offer_sdp`="<sdp>" \| `answer_sdp`="<sdp>" \| `ice_candidate`="<candidate>" | BIDI stream — needs a live joined peer (room_id+peer_id from a prior JoinRoom). Simplest valid frame: set `ping`=true. Server replies SignalResponse oneof (offer_sdp/answer_sdp/ice_candidate/peer_joined/peer_left/track_published/pong). |


## ControlPlaneService

_proto: core/control/services/v1/control_plane_service.proto · 5 RPCs_

xDS-style versioned, ACK/NACK, nonce-paired control-plane resource distribution. Server-only: runs on the isolated native auth/control listener with an admin/service-account credential; never on the public DataBroker port. All RPCs `request_context_required: true`, `tenant_required: false`.

Grounded message defs in `core/control/services/v1/core.proto`; `ResourceType` enum in `core/control/entity/v1/enums.proto`; `RequestContext`/`TenantContext` in `core/common/v1/types.proto`; `PageRequest` in `core/common/v1/dto.proto`.

`ResourceType` valid NAMEs: `RESOURCE_TYPE_UNSPECIFIED` (0), `RESOURCE_TYPE_ROUTING_POLICY` (1), `RESOURCE_TYPE_METHOD_SECURITY_POLICY` (2), `RESOURCE_TYPE_RLS_TENANT_POLICY` (3), `RESOURCE_TYPE_NATIVE_SERVICE_ENABLEMENT` (4), `RESOURCE_TYPE_BACKEND_TARGET_DEFINITION` (5).

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
|---|---|---|---|---|---|
| [ ] | StreamResources | MUTATION | `stream DiscoveryRequest` | First stream message (subscribe, not yet ACKing): `{ node_id: "<seed:node_id>", resource_type: "RESOURCE_TYPE_BACKEND_TARGET_DEFINITION", version_info: "", response_nonce: "", resource_names: [], context: { tenant: { tenant_id: "<seed:tenant_id>" } } }` | **Bidi stream.** Valid first request = a subscription: `version_info` and `response_nonce` empty, `error_detail` absent (empty == not an ACK/NACK yet, just open the SotW subscription). Push definitions (`RESOURCE_TYPE_BACKEND_TARGET_DEFINITION`) before referencing policies (make-before-break). Subsequent ACK echoes server `version_info`+`response_nonce`; NACK sets `error_detail`. Fields: node_id(string), resource_type(enum), version_info(string), response_nonce(string), resource_names(repeated string), error_detail(ErrorDetail{code:int32,message:string}), context(RequestContext). |
| [ ] | DeltaResources | MUTATION | `stream DeltaDiscoveryRequest` | First stream message (initial subscribe): `{ node_id: "<seed:node_id>", resource_type: "RESOURCE_TYPE_BACKEND_TARGET_DEFINITION", response_nonce: "", resource_names_subscribe: ["<seed:resource_name>"], resource_names_unsubscribe: [], initial_resource_versions: {}, context: { tenant: { tenant_id: "<seed:tenant_id>" } } }` | **Bidi stream.** Valid first request = initial delta subscription: `response_nonce` empty, `initial_resource_versions` empty (node holds nothing yet), `error_detail` absent. `resource_names_subscribe` empty == wildcard/all. Later ACK echoes server `nonce` in `response_nonce`; NACK sets `error_detail`. Fields: node_id(string), resource_type(enum), response_nonce(string), resource_names_subscribe(repeated string), resource_names_unsubscribe(repeated string), initial_resource_versions(map<string,string>), error_detail(ErrorDetail), context(RequestContext). |
| [ ] | GetResources | READ_ONLY | `GetResourcesRequest` | `{ resource_type: "RESOURCE_TYPE_BACKEND_TARGET_DEFINITION", tenant_id: "<seed:tenant_id>", resource_names: [], page: { page: 1, page_size: 50 }, context: { tenant: { tenant_id: "<seed:tenant_id>" } } }` | Unary on-demand fetch. `tenant_id` empty == fleet-wide (NULL-tenant) rows only; set == fleet-wide + that tenant's rows. `resource_names` empty == all matching type/tenant. Fields: resource_type(enum), tenant_id(string), resource_names(repeated string), page(PageRequest{page:int32,page_size:int32,page_token:string}), context(RequestContext). HTTP GET /v1/control/resources. |
| [ ] | ListNodeStates | READ_ONLY | `ListNodeStatesRequest` | `{ node_id: "", resource_type: "RESOURCE_TYPE_UNSPECIFIED", page: { page: 1, page_size: 50 }, context: { tenant: { tenant_id: "<seed:tenant_id>" } } }` | Admin visibility. `node_id` empty == all nodes; `resource_type` UNSPECIFIED == all types. To filter: node_id "<seed:node_id>". Fields: node_id(string), resource_type(enum), page(PageRequest), context(RequestContext). HTTP GET /v1/control/node-states. |
| [ ] | AckStatus | MUTATION | `AckStatusRequest` | `{ node_id: "<seed:node_id>", resource_type: "RESOURCE_TYPE_BACKEND_TARGET_DEFINITION", context: { tenant: { tenant_id: "<seed:tenant_id>" } } }` | Per-node ack visibility (named MUTATION but returns read-only status). Requires a real `node_id` (a node that has connected) + a concrete `resource_type` (not UNSPECIFIED). Fields: node_id(string), resource_type(enum), context(RequestContext). HTTP GET /v1/control/node-states/{node_id}:ack-status. |

Seed legend:
- `<seed:node_id>` — a data-plane PEP node identifier that has opened a Stream/Delta session (DiscoveryRequest.node_id / NodeAckState.node_id).
- `<seed:tenant_id>` — canonical tenant UUID, set in `context.tenant.tenant_id` (RequestContext → TenantContext.tenant_id) and, for GetResources, the top-level `tenant_id` filter.
- `<seed:resource_name>` — a `Resource.name` previously distributed for the chosen `resource_type` (used in resource_names / resource_names_subscribe).
