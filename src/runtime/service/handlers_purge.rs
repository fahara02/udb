//! PurgeSubject (D4): erase one data subject inside the caller's tenant and
//! return a signed receipt.
//!
//! The rows go through the normal data-plane delete (`DataBrokerRuntime::delete`),
//! so vector points, graph nodes, search documents, caches and CDC consumers
//! follow exactly as they do for any delete; soft-delete tables then lose their
//! tombstones too, since an erased subject must not survive as a "deleted" row.
//! Tables are found by their `owner_field` columns. udb's own entities (users,
//! files) have dedicated erasure RPCs and are listed in `not_covered`.

use super::*;
use crate::proto::{PurgeSubjectRequest, PurgeSubjectResponse, PurgedSubjectTable};

/// Stores this purge does not reach, and how to erase the subject there.
const NOT_COVERED: &[&str] = &[
    "udb user accounts: delete the account with AuthnService.DeleteUser (sessions, MFA and API keys follow)",
    "stored files: delete them with StorageService.DeleteFile mode HARD (bytes are collected)",
    "event history: CDC journal and Kafka retention expire on their configured schedules",
    "backups: expire on the backup retention schedule",
];

impl DataBrokerService {
    pub(crate) async fn purge_subject_inner(
        &self,
        request: Request<PurgeSubjectRequest>,
    ) -> Result<Response<PurgeSubjectResponse>, Status> {
        let (started, security) = authorized_call!(self, request, "PurgeSubject");
        if let Err(err) = require_admin_scope(&security) {
            return self.record_grpc("PurgeSubject", started, Err(err));
        }
        let req = request.into_inner();
        let result = self.purge_subject_run(&security, req).await;
        self.record_grpc("PurgeSubject", started, result.map(Response::new))
    }

    async fn purge_subject_run(
        &self,
        security: &SecurityContext,
        req: PurgeSubjectRequest,
    ) -> Result<PurgeSubjectResponse, Status> {
        let subject = req.subject_id.trim().to_string();
        if subject.is_empty() {
            return Err(crate::runtime::executor_utils::invalid_argument_fields(
                "subject_id is required",
                [(
                    "subject_id",
                    "the value the subject's rows carry in their owner_field columns",
                )],
            ));
        }
        if req.confirm_subject_id.trim() != subject {
            return Err(crate::runtime::executor_utils::invalid_argument_fields(
                "confirm_subject_id must repeat subject_id: an erasure cannot be undone",
                [("confirm_subject_id", "must equal subject_id")],
            ));
        }
        let tenant = security.tenant_id.trim().to_string();
        if tenant.is_empty() {
            return Err(crate::runtime::executor_utils::invalid_argument_fields(
                "PurgeSubject runs inside one tenant; the caller has none",
                [("tenant_id", "authenticate with a tenant-scoped credential")],
            ));
        }
        let catalog = self.catalog.active_for(&security.project_id);
        let manifest = &catalog.manifest;
        let runtime = self.runtime_snapshot();
        let context = security.request_context_with_decision("");
        let signer =
            crate::runtime::security::ErasureReceiptSigner::configured().map_err(|err| {
                crate::runtime::executor_utils::failed_precondition_fields(
                    err,
                    [(
                        "receipt_jws",
                        "a valid signing key is required before erasure",
                    )],
                )
            })?;

        let mut tables = Vec::new();
        let mut total = 0i64;
        let mut not_covered: Vec<String> = NOT_COVERED.iter().map(|s| s.to_string()).collect();
        for table in &manifest.tables {
            let owners: Vec<&crate::generation::ManifestColumn> = table
                .columns
                .iter()
                .filter(|column| column.security.owner_field)
                .collect();
            if !owners.is_empty()
                && crate::runtime::postgres_helpers::tenant_column_ref(table).is_none()
            {
                not_covered.push(format!(
                    "{}: no tenant column; the caller's ownership cannot be established",
                    table.message_fqn()
                ));
                continue;
            }
            for owner in owners {
                let mut deleted = 0i64;
                if !req.dry_run {
                    let filter = serde_json::json!({ owner.field_name.clone(): subject.clone() });
                    let response = runtime
                        .delete(
                            manifest,
                            &table.message_fqn(),
                            filter,
                            context.clone(),
                            String::new(),
                            None,
                            crate::runtime::core::setup_data::MutationGuards::default(),
                        )
                        .await?;
                    deleted = response.affected_rows;
                    if table.soft_delete {
                        deleted = hard_delete_tombstones(
                            &runtime, &context, table, owner, &tenant, &subject,
                        )
                        .await?;
                    }
                }
                total += deleted;
                tables.push(PurgedSubjectTable {
                    message_type: table.message_fqn(),
                    column: owner.column_name.clone(),
                    deleted,
                });
            }
        }

        let receipt = serde_json::json!({
            "typ": "udb.erasure-receipt.v1",
            "receipt_id": uuid::Uuid::new_v4().to_string(),
            "tenant_id": tenant,
            "project_id": security.project_id,
            // The subject is identified by a digest: the receipt must not carry
            // the very identifier the erasure removed.
            "subject_sha256": format!("{:x}", <sha2::Sha256 as sha2::Digest>::digest(subject.as_bytes())),
            "reason": req.reason,
            "requested_by": security.user_id,
            "dry_run": req.dry_run,
            "tables": tables.iter().map(|t| serde_json::json!({
                "message_type": t.message_type,
                "column": t.column,
                "deleted": t.deleted,
            })).collect::<Vec<_>>(),
            "total_deleted": total,
            "not_covered": not_covered,
            "purged_at": chrono::Utc::now().to_rfc3339(),
        });
        let receipt_jws = signer.sign(&receipt).map_err(|err| {
            crate::runtime::executor_utils::internal_status("PurgeSubject", "sign_receipt", err)
        })?;
        Ok(PurgeSubjectResponse {
            tables,
            not_covered,
            total_deleted: total,
            dry_run: req.dry_run,
            receipt_jws,
            receipt_json: receipt.to_string(),
        })
    }
}

/// Remove soft-deleted rows of the subject that the data-plane delete left as
/// tombstones, inside the tenant's session settings.
async fn hard_delete_tombstones(
    runtime: &crate::runtime::DataBrokerRuntime,
    context: &crate::RequestContext,
    table: &crate::generation::ManifestTable,
    owner: &crate::generation::ManifestColumn,
    tenant: &str,
    subject: &str,
) -> Result<i64, Status> {
    let Some(pool) = runtime.pg_pool_clone() else {
        return Ok(0);
    };
    let Some(tenant_column) = crate::runtime::postgres_helpers::tenant_column_ref(table) else {
        return Ok(0);
    };
    let quote = |ident: &str| format!("\"{}\"", ident.replace('"', "\"\""));
    let relation = format!("{}.{}", quote(&table.schema), quote(&table.table));
    let project_filter = crate::generation::sql::resolve_project_column(table)
        .map(|column| format!(" AND {}::TEXT = $3", quote(column)))
        .unwrap_or_default();
    let mut tx = pool.begin().await.map_err(|err| {
        crate::runtime::executor_utils::internal_status("PurgeSubject", "begin", err.to_string())
    })?;
    crate::runtime::core::set_request_local_settings(&mut tx, context)
        .await
        .map_err(|err| {
            crate::runtime::executor_utils::internal_status(
                "PurgeSubject",
                "session",
                err.to_string(),
            )
        })?;
    let sql = format!(
        "DELETE FROM {relation} WHERE {tenant}::TEXT = $1 AND {owner}::TEXT = $2{project_filter} AND {tombstone} IS NOT NULL",
        tenant = quote(&tenant_column.column_name),
        owner = quote(&owner.column_name),
        tombstone = quote(&table.soft_delete_column),
    );
    let mut query = sqlx::query(&sql).bind(tenant).bind(subject);
    if !project_filter.is_empty() {
        query = query.bind(&context.project_id);
    }
    let result = query.execute(&mut *tx).await.map_err(|err| {
        crate::runtime::executor_utils::internal_status(
            "PurgeSubject",
            "hard_delete_tombstones",
            err.to_string(),
        )
    })?;
    tx.commit().await.map_err(|err| {
        crate::runtime::executor_utils::internal_status("PurgeSubject", "commit", err.to_string())
    })?;
    Ok(result.rows_affected() as i64)
}
