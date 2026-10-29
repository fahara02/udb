//! Role binding by role CODE for flows that create or link an account and grant
//! its roles in the same step (invitation acceptance, identity-provider JIT
//! provisioning). One implementation of the `user_roles` row the bootstrap and
//! `AssignRole` paths write, plus the role-assignment authz revision that makes
//! every node reload its snapshot, so the flows cannot drift apart.

use sqlx::PgConnection;
use tonic::Status;
use uuid::Uuid;

use crate::runtime::native_catalog::native_model;

const ROLE_MSG: &str = "udb.core.authz.entity.v1.Role";
const USER_ROLE_MSG: &str = "udb.core.authz.entity.v1.UserRole";
const AUTHZ_REVISION_MSG: &str = "udb.core.authz.entity.v1.AuthzRevision";

fn store_status(context: &'static str) -> impl Fn(sqlx::Error) -> Status {
    move |err| crate::runtime::executor_utils::sqlx_error_to_status(context, &err)
}

/// What to do when the account already holds the role.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExistingBinding {
    /// Refresh `assigned_by` and `tenant_id` (an explicit grant, such as an
    /// accepted invitation, re-asserts the binding).
    Refresh,
    /// Leave it exactly as it is (an identity-provider mapping never rewrites
    /// a binding an administrator made).
    Keep,
}

/// The role row each CODE binds to: the tenant's own active role first, else a
/// global system role (tenant `*`), unscoped or scoped to `project_id`.
/// Returns the resolved `(code, role_id)` pairs and the codes with no such role.
pub(crate) async fn resolve_role_codes(
    conn: &mut PgConnection,
    tenant_id: &str,
    project_id: &str,
    codes: &[String],
) -> Result<(Vec<(String, Uuid)>, Vec<String>), Status> {
    let rm = native_model(
        ROLE_MSG,
        &[
            "role_id",
            "role_code",
            "tenant_id",
            "project_id",
            "is_active",
            "deleted_at",
        ],
    );
    let sql = format!(
        "SELECT {rid}::TEXT AS role_id FROM {rel} \
         WHERE {code} = $1 AND {del} IS NULL AND {active} = TRUE \
           AND ({tenant} = $2 OR {tenant} = '*') \
           AND (COALESCE({project}, '') = '' OR {project} = $3) \
         ORDER BY ({tenant} = $2) DESC, (COALESCE({project}, '') = $3) DESC \
         LIMIT 1",
        rid = rm.q("role_id"),
        rel = rm.relation,
        code = rm.q("role_code"),
        del = rm.q("deleted_at"),
        active = rm.q("is_active"),
        tenant = rm.q("tenant_id"),
        project = rm.q("project_id"),
    );
    let mut resolved = Vec::with_capacity(codes.len());
    let mut missing = Vec::new();
    for code in codes {
        let role_id: Option<String> = sqlx::query_scalar(&sql)
            .bind(code)
            .bind(tenant_id)
            .bind(project_id)
            .fetch_optional(&mut *conn)
            .await
            .map_err(store_status("role code lookup failed"))?;
        match role_id.as_deref().map(Uuid::parse_str) {
            Some(Ok(role_id)) => resolved.push((code.clone(), role_id)),
            _ => missing.push(code.clone()),
        }
    }
    Ok((resolved, missing))
}

/// Bind `user_id` to each resolved role in `tenant_id` (domain `''`, like the
/// bootstrap and `AssignRole` rows). Returns the codes this call wrote: every
/// code under [`ExistingBinding::Refresh`], only the newly created bindings
/// under [`ExistingBinding::Keep`]. Run [`append_role_assignment_revision`] in
/// the same transaction when the result is non-empty.
pub(crate) async fn bind_role_ids(
    conn: &mut PgConnection,
    user_id: &Uuid,
    tenant_id: &str,
    roles: &[(String, Uuid)],
    assigned_by: &Uuid,
    created_by: &str,
    existing: ExistingBinding,
) -> Result<Vec<String>, Status> {
    let ur = native_model(
        USER_ROLE_MSG,
        &[
            "user_role_id",
            "user_id",
            "role_id",
            "domain",
            "assigned_by",
            "tenant_id",
            "created_by",
        ],
    );
    let conflict = match existing {
        ExistingBinding::Refresh => format!(
            "DO UPDATE SET {assigned_by} = EXCLUDED.{assigned_by}, {tenant} = EXCLUDED.{tenant}",
            assigned_by = ur.q("assigned_by"),
            tenant = ur.q("tenant_id"),
        ),
        ExistingBinding::Keep => "DO NOTHING".to_string(),
    };
    let sql = format!(
        "INSERT INTO {rel} ({urid}, {uid}, {rid}, {domain}, {assigned_by}, {tenant}, {created_by}) \
         VALUES (gen_random_uuid(), $1, $2, '', $3, $4, $5) \
         ON CONFLICT ({uid}, {rid}, {domain}) {conflict} \
         RETURNING {urid}::TEXT",
        rel = ur.relation,
        urid = ur.q("user_role_id"),
        uid = ur.q("user_id"),
        rid = ur.q("role_id"),
        domain = ur.q("domain"),
        assigned_by = ur.q("assigned_by"),
        tenant = ur.q("tenant_id"),
        created_by = ur.q("created_by"),
    );
    let mut written = Vec::new();
    for (code, role_id) in roles {
        let row: Option<String> = sqlx::query_scalar(&sql)
            .bind(user_id)
            .bind(role_id)
            .bind(assigned_by)
            .bind(tenant_id)
            .bind(created_by)
            .fetch_optional(&mut *conn)
            .await
            .map_err(store_status("role binding failed"))?;
        if row.is_some() {
            written.push(code.clone());
        }
    }
    Ok(written)
}

/// Append the next policy revision of `(tenant_id, project_id)` as a role
/// assignment (relationship revision unchanged), so every node's authz
/// snapshot reloads. Append-only, like every authz mutation.
pub(crate) async fn append_role_assignment_revision(
    conn: &mut PgConnection,
    tenant_id: &str,
    project_id: &str,
    changed_by: &str,
) -> Result<(), Status> {
    let rv = native_model(
        AUTHZ_REVISION_MSG,
        &[
            "revision_id",
            "tenant_id",
            "project_id",
            "policy_revision",
            "relationship_revision",
            "content_hash",
            "changed_by",
            "change_type",
        ],
    );
    let sql = format!(
        "INSERT INTO {rel} ({rid}, {tenant}, {project}, {policy}, {relationship}, {hash}, {by}, {kind}) \
         SELECT gen_random_uuid(), $1, $2, COALESCE(MAX({policy}), 0) + 1, \
                COALESCE(MAX({relationship}), 0), 'role-assignment', $3, \
                'AUTHZ_CHANGE_TYPE_ROLE_ASSIGNMENT' \
         FROM {rel} WHERE {tenant} = $1 AND {project} = $2",
        rel = rv.relation,
        rid = rv.q("revision_id"),
        tenant = rv.q("tenant_id"),
        project = rv.q("project_id"),
        policy = rv.q("policy_revision"),
        relationship = rv.q("relationship_revision"),
        hash = rv.q("content_hash"),
        by = rv.q("changed_by"),
        kind = rv.q("change_type"),
    );
    sqlx::query(&sql)
        .bind(tenant_id)
        .bind(project_id)
        .bind(changed_by)
        .execute(&mut *conn)
        .await
        .map_err(store_status("role assignment revision failed"))?;
    Ok(())
}
