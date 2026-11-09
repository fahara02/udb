## MeteringService
_proto: core/metering/services/v1/metering_service.proto_

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [x] | CheckQuota | READ_ONLY | CheckQuotaRequest | `{ "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>", "metric": "sdk.perf.request" }` | checks current usage against the seeded quota metric. |
| [x] | GetQuota | READ_ONLY | GetQuotaRequest | `{ "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>", "metric": "sdk.perf.request" }` | reads the seeded quota rule. |
| [x] | ListQuotas | READ_ONLY | ListQuotasRequest | `{ "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>", "limit": 50, "page_size": 50 }` | lists quota rules for the tenant/project. |
| [x] | PutQuota | MUTATION | PutQuotaRequest | `{ "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>", "metric": "sdk.perf.request", "limit_value": 1000000, "window_seconds": 86400, "enabled": true, "metadata_json": "{}" }` | upserts the seeded quota rule. |
| [x] | QueryUsage | READ_ONLY | QueryUsageRequest | `{ "tenant_id": "<seed:tenant_id>", "metric": "sdk.perf.request", "window_seconds": 86400 }` | queries the 24h usage window for the seeded metric. |
| [x] | RecordUsage | MUTATION | RecordUsageRequest | `{ "tenant_id": "<seed:tenant_id>", "principal_id": "<seed:user_id>", "method": "sdk.perf.request", "unit": "request", "quantity": 1, "metadata_json": "{}" }` | records one usage unit for the seeded metric. |
