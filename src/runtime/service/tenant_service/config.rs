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
/// Stored tenant type when `CreateTenant` supplies none (short DB token).
pub(crate) const DEFAULT_TENANT_TYPE_DB: &str = "ORGANIZATION";
/// Canonical stored ACTIVE status token — matches the entity proto column
/// default (`status ... default_value: "'ACTIVE'"`) and `tenant_status_to_db`.
pub(crate) const TENANT_STATUS_ACTIVE_DB: &str = "ACTIVE";
/// Default `ListTenants` page size when the request supplies none.
pub(crate) const DEFAULT_TENANT_LIST_PAGE_SIZE: i32 = 50;
