## ApiKeyService

_proto: core/apikey/services/v1/apikey_service.proto · 9 RPCs_

Request messages live in `core/apikey/services/v1/core.proto`. Enums (`ApiKeyOwnerType`, `ApiKeyStatus`) in `core/apikey/entity/v1/enums.proto`. `RequestContext` / `TenantContext` / `PageRequest` in `core/common/v1/{types,dto}.proto`.

Common shapes:
- `context` (RequestContext) — `tenant` (TenantContext: `tenant_id`, `organization_id`, `project_id`, `environment`, `region`, ...), `request_id`, `correlation_id`, `user_id`, `idempotency_key`, `scopes`, `roles`, ... Provide `context.tenant.tenant_id` = `<seed:tenant_id>` since every RPC has `tenant_required: true`.
- `ApiKeyOwnerType` enum: `API_KEY_OWNER_TYPE_UNSPECIFIED|INTEGRATION|CICD|ANALYTICS|TENANT|PROJECT|SERVICE_ACCOUNT|WORKLOAD`.
- `ApiKeyStatus` enum: `API_KEY_STATUS_UNSPECIFIED|ACTIVE|REVOKED|EXPIRED`.
- `PageRequest`: `page` (int32), `page_size` (int32), `page_token` (string).

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | CreateApiKey | MUTATION | CreateApiKeyRequest | `{ "name": "bench-key", "description": "bench", "owner_type": "API_KEY_OWNER_TYPE_SERVICE_ACCOUNT", "owner_id": "<seed:owner_id>", "scopes": ["resource:read"], "ip_allowlist": [], "rate_limit_per_minute": 0, "rate_limit_per_day": 0, "context": { "tenant": { "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>" }, "user_id": "<seed:owner_id>" } }` | Returns `plain_key` ONCE — capture it as `<seed:plain_key>` and the returned public `key.key_id`/prefix as `<seed:key_id>` for downstream RPCs. |
| [ ] | EmergencyRevokeApiKeys | DESTRUCTIVE | EmergencyRevokeApiKeysRequest | `{ "owner_id": "<seed:owner_id>", "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>", "scope": "resource:read", "reason": "bench emergency", "context": { "tenant": { "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>" }, "user_id": "<seed:owner_id>" } }` | Per-record tenant/owner authority enforced. Set `owner_id`=`<seed:owner_id>` to scope to bench-created keys only. Truly destructive — run last. |
| [ ] | GetApiKey | READ_ONLY | GetApiKeyRequest | `{ "key_id": "<seed:key_id>" }` | No `context` field on this msg. NOT_FOUND without a real seeded key_id. |
| [ ] | GetApiKeyUsageStats | READ_ONLY | GetApiKeyUsageStatsRequest | `{ "key_id": "<seed:key_id>" }` | No `context` field. Needs real seeded key_id. Optional `from`/`to` timestamp filters are intentionally omitted for the strict-JSON manifest slice. |
| [ ] | ListApiKeys | READ_ONLY | ListApiKeysRequest | `{ "owner_id": "<seed:owner_id>", "owner_type": "API_KEY_OWNER_TYPE_SERVICE_ACCOUNT", "status": "API_KEY_STATUS_ACTIVE", "page": { "page": 1, "page_size": 50 } }` | All fields optional filters; empty body also valid. No `context` field. |
| [ ] | RevokeApiKey | MUTATION | RevokeApiKeyRequest | `{ "key_id": "<seed:revoke_key_id>", "revoke_reason": "bench cleanup", "context": { "tenant": { "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>" }, "user_id": "<seed:owner_id>" } }` | Needs a real seeded key_id/prefix (NOT_FOUND otherwise). Destructive — use a disposable revoke key and run after Get/List/Stats. |
| [ ] | RotateApiKey | MUTATION | RotateApiKeyRequest | `{ "key_id": "<seed:key_id>", "rotation_reason": "bench rotate", "context": { "tenant": { "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>" }, "user_id": "<seed:owner_id>" } }` | Needs a real seeded key_id/prefix. Returns a NEW `plain_key` ONCE; old secret invalidated. |
| [ ] | UpdateApiKey | MUTATION | UpdateApiKeyRequest | `{ "key_id": "<seed:update_key_id>", "name": "bench-key-2", "description": "updated", "scopes": ["resource:read"], "ip_allowlist": [], "rate_limit_per_minute": 0, "rate_limit_per_day": 0, "context": { "tenant": { "tenant_id": "<seed:tenant_id>", "project_id": "<seed:project>" }, "user_id": "<seed:owner_id>" } }` | NOT_FOUND without real seeded key_id/prefix; target a disposable update key so RotateApiKey cannot invalidate it first. |
| [ ] | ValidateApiKey | READ_ONLY | ValidateApiKeyRequest | `{ "plain_key": "<seed:plain_key>", "endpoint": "/v1/test", "required_scope": "resource:read", "ip_address": "127.0.0.1" }` | No `context` field. Needs the real plain_key captured at create time; returns `valid=false` (not error) for an unknown key. |
