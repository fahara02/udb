## WebhookService
_proto: core/webhook/services/v1/webhook_service.proto_

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | CreateEndpoint | MUTATION | CreateEndpointRequest | `{ "tenant_id": "<seed:tenant_id>", "url": "https://example.com/udb-webhook", "topic_pattern": "<seed:topic_pattern>", "description": "sdk perf webhook", "max_attempts": 3, "metadata_json": "{}" }` | creates a webhook endpoint for the seeded topic pattern. |
| [ ] | DeleteEndpoint | DESTRUCTIVE | DeleteEndpointRequest | `{ "tenant_id": "<seed:tenant_id>", "endpoint_id": "<seed:delete_endpoint_id>" }` | deletes a disposable seeded endpoint. |
| [ ] | GetEndpoint | READ_ONLY | GetEndpointRequest | `{ "tenant_id": "<seed:tenant_id>", "endpoint_id": "<seed:endpoint_id>" }` | reads the seeded webhook endpoint. |
| [ ] | ListDeliveries | READ_ONLY | ListDeliveriesRequest | `{ "tenant_id": "<seed:tenant_id>", "page": 1, "page_size": 20 }` | lists recent webhook deliveries for the tenant. |
| [ ] | ListEndpoints | READ_ONLY | ListEndpointsRequest | `{ "tenant_id": "<seed:tenant_id>", "page": 1, "page_size": 20, "active_only": true }` | lists active webhook endpoints for the tenant. |
| [ ] | UpdateEndpoint | MUTATION | UpdateEndpointRequest | `{ "tenant_id": "<seed:tenant_id>", "endpoint_id": "<seed:endpoint_id>", "url": "https://example.com/udb-webhook-updated", "topic_pattern": "<seed:topic_pattern>", "description": "sdk perf webhook updated", "active": true, "max_attempts": 3 }` | updates the seeded endpoint and keeps it active. |
