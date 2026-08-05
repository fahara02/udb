//! Static identifiers for the native `TenantService`: the entity message names,
//! the versioned runtime outbox topics, the proto-declared event-type strings,
//! and the stored-token / page-size defaults. Extracted verbatim from the former
//! god file; every value is byte-stable for downstream audit/CDC consumers.

pub(crate) const TENANT_MSG: &str = "udb.core.tenant.entity.v1.Tenant";
pub(crate) const TENANT_CONFIG_MSG: &str = "udb.core.tenant.entity.v1.TenantConfig";
pub(crate) const TOPIC_TENANT_PURGED: &str = "udb.tenant.purged.v1";
/// Versioned runtime outbox topics for the tenant lifecycle mutations. Each RPC
/// below declares a `method_event_contract` in `tenant_service.proto`
/// (CreateTenant / UpdateTenant / UpdateTenantConfig, partition key
/// `tenant_id`); these are their canonical versioned dot topics, following the
/// same runtime event model as [`TOPIC_TENANT_PURGED`].
pub(crate) const TOPIC_TENANT_CREATED: &str = "udb.tenant.created.v1";
pub(crate) const TOPIC_TENANT_UPDATED: &str = "udb.tenant.updated.v1";
pub(crate) const TOPIC_TENANT_CONFIG_UPDATED: &str = "udb.tenant.config-updated.v1";
/// Proto-declared `method_event_contract.event_type` strings, threaded into the
/// compliance envelope `operation` so every emitted event is traceable to the
/// exact RPC contract that declared it (never invented at the emit site).
pub(crate) const EVENT_TYPE_TENANT_CREATED: &str = "tenant.CreateTenant";
pub(crate) const EVENT_TYPE_TENANT_UPDATED: &str = "tenant.UpdateTenant";
pub(crate) const EVENT_TYPE_TENANT_CONFIG_UPDATED: &str = "tenant.UpdateTenantConfig";
/// Operation recorded on the purge audit event (pre-existing envelope shape;
/// kept byte-stable for downstream audit consumers).
pub(crate) const EVENT_OP_TENANT_PURGE: &str = "tenant.purge";
/// Versioned runtime outbox topic for the PRIVILEGED cross-tenant admin purge
/// (Bug #2). Distinct from [`TOPIC_TENANT_PURGED`] so audit consumers can tell a
/// delegated cross-tenant purge apart from a tenant's own self-purge. Tenant-
/// scoped dot topic under the security-sensitive `udb.tenant.` compliance prefix.
pub(crate) const TOPIC_TENANT_ADMIN_PURGED: &str = "udb.tenant.admin-purged.v1";
/// Proto-declared `method_event_contract.event_type` for `AdminPurgeTenant`,
/// threaded into the compliance envelope `operation` (never invented at emit).
pub(crate) const EVENT_TYPE_TENANT_ADMIN_PURGE: &str = "tenant.AdminPurgeTenant";
/// DISTINCT, default-deny scope that authorizes the privileged cross-tenant purge
/// — SEPARATE from the self-purge `udb:tenant:purge-tenant`. Mirrors the RPC's
/// `endpoint_security.scopes`; the handler re-checks it (D2 defense-in-depth) so
/// a caller without it is rejected even past the coarse transport gate.
pub(crate) const SCOPE_TENANT_ADMIN_PURGE: &str = "udb:tenant:admin-purge";
/// Logical action recorded for the per-action authz decision on the admin purge.
pub(crate) const ACTION_TENANT_ADMIN_PURGE: &str = "tenant.admin-purge";
/// Canonical stored INACTIVE status token (a valid `tenant_status_to_db` token) —
/// the terminal state a SOFT admin purge moves the tenant control record to.
pub(crate) const TENANT_STATUS_INACTIVE_DB: &str = "INACTIVE";
/// Durable idempotency + immutable audit/outcome ledger for `AdminPurgeTenant`,
/// co-located in the UDB-owned tenant schema. Created idempotently on first use
/// (same `CREATE TABLE IF NOT EXISTS` pattern the platform system tables use).
pub(crate) const ADMIN_PURGE_LEDGER_SCHEMA: &str = "udb_tenant";
pub(crate) const ADMIN_PURGE_LEDGER_TABLE: &str = "tenant_admin_purge_outcomes";
/// Stored tenant type when `CreateTenant` supplies none (short DB token).
pub(crate) const DEFAULT_TENANT_TYPE_DB: &str = "ORGANIZATION";
/// Canonical stored ACTIVE status token — matches the entity proto column
/// default (`status ... default_value: "'ACTIVE'"`) and `tenant_status_to_db`.
pub(crate) const TENANT_STATUS_ACTIVE_DB: &str = "ACTIVE";
/// Default `ListTenants` page size when the request supplies none.
pub(crate) const DEFAULT_TENANT_LIST_PAGE_SIZE: i32 = 50;
