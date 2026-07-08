## AnalyticsService

_proto: core/analytics/services/v1/analytics_service.proto · 7 RPCs_

All request/response messages live in `core/analytics/services/v1/core.proto` (imported by the service proto). `RequestContext` resolves to `udb.core.common.v1.RequestContext` (`core/common/v1/types.proto`, has nested `TenantContext tenant`), NOT the `udb.entity.v1` one. `PageRequest`/`PageResponse` are from `core/common/v1/dto.proto`. All 7 RPCs require auth (`AUTH_MODE_BEARER`), `request_context_required: true`, and `tenant_required: true`.

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | GetExecutorPerformance | READ_ONLY | GetExecutorPerformanceRequest | `{ "executor_identity": "", "workload_kind": "", "date_from": "2026-06-01", "date_to": "2026-06-14" }` | Fields: `executor_identity` string(1, empty=all executors), `workload_kind` string(2), `date_from` string(3, ISO date), `date_to` string(4). No tenant_id field on body (tenant comes from auth/RequestContext metadata). Provide `date_from`+`date_to` to avoid INTERNAL on empty/unparseable date input. |
| [ ] | GetPipelineSummary | READ_ONLY | GetPipelineSummaryRequest | `{ "stage_name": "<seed:stage_name>", "tenant_id": "<seed:tenant_id>", "hour_from": "2026-06-01T00:00:00Z", "hour_to": "2026-06-14T23:00:00Z", "page": { "page": 1, "page_size": 50 } }` | Fields: `stage_name` string(1, empty=all stages), `tenant_id` string(2), `hour_from` string(3, ISO-8601 hour), `hour_to` string(4), `page` PageRequest(5){page int32, page_size int32, page_token string}. Provide `tenant_id` + full RFC3339 hour range to avoid PostgreSQL timestamptz parse failures; `stage_name` may be empty. |
| [ ] | GetReconciliationAnalytics | READ_ONLY | GetReconciliationAnalyticsRequest | `{ "date_from": "2026-06-01", "date_to": "2026-06-14" }` | Fields: `date_from` string(1, ISO date), `date_to` string(2). Only two fields; tenant from auth context. Provide valid date strings to avoid INTERNAL. |
| [ ] | GetSlaCompliance | READ_ONLY | GetSlaComplianceRequest | `{ "stage_name": "<seed:stage_name>", "date_from": "2026-06-01", "date_to": "2026-06-14", "p99_threshold_ms": 250.0, "error_rate_threshold": 0.01 }` | Fields: `stage_name` string(1), `date_from` string(2, ISO date), `date_to` string(3), `p99_threshold_ms` double(4), `error_rate_threshold` double(5). Provide `stage_name`+date range+thresholds to avoid INTERNAL; thresholds default 0 if omitted (everything fails SLA). |
| [ ] | GetThroughput | READ_ONLY | GetThroughputRequest | `{ "tenant_id": "<seed:tenant_id>", "hour_from": "2026-06-01T00:00:00Z", "hour_to": "2026-06-14T23:00:00Z" }` | Fields: `tenant_id` string(1), `hour_from` string(2, ISO-8601 hour), `hour_to` string(3). Provide `tenant_id` + full RFC3339 hour range to avoid PostgreSQL timestamptz parse failures. |
| [ ] | RecordPipelineMetric | MUTATION | RecordPipelineMetricRequest | `{ "stage_name": "<seed:stage_name>", "tenant_id": "<seed:tenant_id>", "latency_ms": 12.5, "is_success": true, "context": { "tenant": { "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>" }, "request_id": "..." } }` | Fields: `stage_name` string(1), `tenant_id` string(2), `latency_ms` double(3), `is_success` bool(4), `context` RequestContext(5). Set `stage_name`+`tenant_id` non-empty to avoid INTERNAL on empty input. `context.tenant.tenant_id` should match `tenant_id`. POST body `*`. |
| [ ] | TriggerSnapshot | MUTATION | TriggerSnapshotRequest | `{ "stage_name": "<seed:stage_name>", "hour": "2026-06-14T10:00:00Z", "context": { "tenant": { "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>" }, "request_id": "..." } }` | Fields: `stage_name` string(1, empty=all stages), `hour` string(2, ISO-8601 hour; empty=previous complete hour), `context` RequestContext(3). Both `stage_name` and `hour` may be empty (defaults), but `context.tenant.tenant_id` should be set. POST body `*`. |

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
