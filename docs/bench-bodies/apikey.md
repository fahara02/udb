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
