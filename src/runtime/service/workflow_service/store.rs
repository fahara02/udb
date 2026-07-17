//! Query builders for the workflow store: the shared tenant+project scope
//! predicate and the SELECT projection, both rendered from the manifest model so
//! column identifiers stay single-sourced.

use crate::runtime::native_catalog::NativeModel;
use uuid::Uuid;

/// Normalize a project scope value before it is bound into the UUID-typed
/// `project_id` predicate. The workflow schema stores `project_id` as a UUID, but
/// the scope is resolved from request metadata / the verified claim, which may
/// carry a human project CODE (e.g. `default`) rather than a UUID. A non-UUID code
/// can never equal a stored UUID, so binding it verbatim makes Postgres reject the
/// whole query with `invalid input syntax for type uuid`. Treat any non-UUID (or
/// empty) scope as tenant-wide — the `($n = '' OR ...)` branch the predicate
/// already supports — so the tenant boundary still gates the read/mutation while a
/// non-resolvable project code degrades to unscoped instead of a 500.
pub(crate) fn workflow_project_bind(project_id: &str) -> String {
    let trimmed = project_id.trim();
    if trimmed.is_empty() || Uuid::parse_str(trimmed).is_err() {
        String::new()
    } else {
        trimmed.to_string()
    }
}

/// Tenant + project scope predicate shared by get/list/cancel/signal (16.3.1).
/// An empty `project_bind` value keeps the query tenant-wide — the same
/// backward-compatible `($n = '' OR col = $n)` semantic the api-key list uses —
/// while a non-empty project (resolved from request metadata / the verified
/// claim; the request protos carry no project field) restricts to rows in that
/// exact project, so a project-scoped caller can never read or mutate another
/// project's (or a project-less) instance. `NULLIF(..)::UUID` keeps the empty
/// bind castable against the UUID column.
pub(crate) fn workflow_scope_predicate(
    m: &NativeModel,
    tenant_bind: &str,
    project_bind: &str,
) -> String {
    format!(
        "{tenant_id} = {tenant_bind}::UUID AND \
         ({project_bind} = '' OR {project_id} = NULLIF({project_bind}, '')::UUID)",
        tenant_id = m.q("tenant_id"),
        project_id = m.q("project_id"),
    )
}

pub(crate) fn workflow_select_projection(m: &NativeModel) -> String {
    [
        m.text("workflow_id"),
        m.text("tenant_id"),
        m.text_or_empty("project_id"),
        m.select("workflow_type"),
        m.select("status"),
        m.select("current_step"),
        m.select("total_steps"),
        m.text_or_empty("payload"),
        m.text_or_empty("compensations"),
        m.text_or_empty("correlation_id"),
        m.text_or_empty("saga_id"),
        m.text_or_empty("pending_signal"),
        m.text_or_empty("last_error"),
        format!(
            "EXTRACT(EPOCH FROM {})::BIGINT AS next_run_at_epoch",
            m.q("next_run_at")
        ),
        format!(
            "EXTRACT(EPOCH FROM {})::BIGINT AS last_transition_at_epoch",
            m.q("last_transition_at")
        ),
    ]
    .join(", ")
}
