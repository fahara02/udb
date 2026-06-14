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
