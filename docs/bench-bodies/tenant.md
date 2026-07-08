## TenantService

_proto: core/tenant/services/v1/tenant_service.proto · 7 RPCs_

All request messages are defined inline in `tenant_service.proto`; every request field is a proto3 scalar `string`/`int32` except `UpdateTenantRequest.update_mask` (omitted here for legacy full update semantics). `type`/`status` are free-form `string` columns, NOT proto enums. All 7 RPCs carry `tenant_required: true` + `request_context_required: true` (must send tenant context + bearer JWT/session).

| done | RPC | op_kind | request msg | valid body | seed refs / notes |
| --- | --- | --- | --- | --- | --- |
| [ ] | CreateTenant | MUTATION | CreateTenantRequest | `{ "code": "<seed:tenant_code>", "name": "Acme Bench", "type": "organization", "parent_tenant_id": "", "config": "{}", "branding": "{}" }` | Only `code`+`name` are realistically load-bearing; `config`/`branding` are JSON strings — send `"{}"`, not bare `""`, to be safe. `parent_tenant_id` must reference an existing tenant if set. Creates a new tenant row each call → use a unique `code` per bench iteration to avoid uniqueness conflicts. |
| [ ] | GetTenant | READ_ONLY | GetTenantRequest | `{ "tenant_id": "<seed:tenant_id>" }` | Single lookup by id. |
| [ ] | GetTenantConfig | READ_ONLY | GetTenantConfigRequest | `{ "tenant_id": "<seed:tenant_id>" }` | Returns repeated TenantConfig rows for the tenant. |
| [ ] | ListTenants | READ_ONLY | ListTenantsRequest | `{ "type": "", "status": "", "page": 1, "page_size": 20 }` | All fields optional filters/pagination; empty `type`/`status` = no filter. Keep `page_size` modest (e.g. 20) for bench stability. |
| [ ] | PurgeTenant | DESTRUCTIVE | PurgeTenantRequest | `{ "tenant_id": "<seed:purge_tenant_id>", "confirmation_token": "sdk-perf-confirm-purge" }` | Destructive; target must be a disposable tenant. |
| [ ] | UpdateTenant | MUTATION | UpdateTenantRequest | `{ "tenant_id": "<seed:tenant_id>", "name": "Acme Bench", "status": "active", "config": "{}", "branding": "{}" }` | `tenant_id` must reference an existing tenant. `status` is a free-form string column (no enum). |
| [ ] | UpdateTenantConfig | MUTATION | UpdateTenantConfigRequest | `{ "tenant_id": "<seed:tenant_id>", "config_key": "feature.flag", "config_value": "on", "type": "string" }` | Upserts one config key/value for the tenant. `tenant_id` must reference an existing tenant. |

### Quota / RESOURCE_EXHAUSTED notes
- **CreateTenant** is the growth RPC: each call inserts a tenant. Repeated bench calls can hit a tenant-count quota or unique-`code` collisions. Avoid by (a) using a unique `code` per iteration and (b) cleaning up created tenants, or running CreateTenant with low iteration counts. The read/update/config rows are idempotent against a seeded `<seed:tenant_id>` and are not quota-prone. **PurgeTenant** must target only a disposable tenant under matching tenant credentials; never point it at the active benchmark tenant.
- All RPCs are `requires_postgres: true` and `tenant_required: true`; an unseeded/missing tenant context yields auth/NOT_FOUND, not a valid-body failure.
