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
