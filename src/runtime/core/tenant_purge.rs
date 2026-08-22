//! Tenant/account HARD-DELETE ripple (GDPR right-to-be-forgotten).
//!
//! Two pieces:
//!   * [`plan_tenant_purge`] — a PURE planner that walks the active
//!     [`CatalogManifest`], resolves each table's tenant-isolation column via the
//!     SHARED resolver (`crate::generation::sql::resolve_tenant_column_ref` — the
//!     same one generation/planning/IR compilers use; NO second resolver here),
//!     and returns the ordered list of `(schema, table, tenant_col)` to purge
//!     plus the EXCLUDED tables (no resolvable tenant column). Excluded tables are
//!     REPORTED, never silently dropped (capability honesty).
//!   * [`purge_tenant`] — runs the plan: one transaction issuing a real
//!     `DELETE … WHERE <tenant_col>::text = $1` per planned table (HARD delete, no
//!     `deleted_at` soft path), commits, then best-effort tenant/principal token
//!     revocation via the existing [`crate::runtime::authn::revocation`] foundation
//!     (`deny_tenant_after` / `deny_principal_after`). Revocation is an accelerator
//!     over the durable Postgres source of truth, so a Redis outage NEVER fails the
//!     already-committed purge.
//!
//! Ordering: when the manifest carries foreign-key metadata we delete
//! children→parents (a child that references another included table is deleted
//! first) via a stable topological sort, so FK constraints don't reject the
//! delete. When NO FK metadata links the included tables we fall back to manifest
//! order and rely on declared `ON DELETE CASCADE`; [`TenantPurgePlan::fk_ordered`]
//! reports which case applied so callers/operators know the assumption in force.
//!
//! The PurgeTenant gRPC handler is wired from `tenant_service`; generated client
//! artifacts still ride the normal proto/codegen gate, but the served handler and
//! this executor share the same planner.
#![allow(dead_code)]

use crate::generation::sql::resolve_tenant_column_ref;
use crate::generation::{CatalogManifest, ManifestTable};
use crate::runtime::executor_utils::qi_runtime;

/// One table the purge will hard-delete the tenant's rows from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurgeTarget {
    pub schema: String,
    pub table: String,
    /// Resolved tenant-isolation column (the `WHERE` discriminator).
    pub tenant_column: String,
}

/// A table the purge SKIPS because it has no resolvable tenant column. Reported,
/// never silently dropped — an operator can see exactly what the ripple does not
/// cover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExcludedPurgeTable {
    pub schema: String,
    pub table: String,
    pub reason: String,
}

/// Pure result of [`plan_tenant_purge`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantPurgePlan {
    /// Tables to purge, ordered children→parents when `fk_ordered` is true.
    pub targets: Vec<PurgeTarget>,
    /// Tables with no resolvable tenant column (reported, not purged).
    pub excluded: Vec<ExcludedPurgeTable>,
    /// `true` when foreign-key metadata produced a child→parent (acyclic) order;
    /// `false` when no FK edges linked the included tables (manifest order +
    /// `ON DELETE CASCADE` assumption) or a FK cycle forced a partial order.
    pub fk_ordered: bool,
}

/// Per-table delete count produced by [`purge_tenant`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurgedTableCount {
    pub schema: String,
    pub table: String,
    pub tenant_column: String,
    pub deleted: u64,
}

/// Outcome of a committed [`purge_tenant`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantPurgeReport {
    pub tenant_id: String,
    /// Per-table hard-delete counts, in execution (children→parents) order.
    pub purged: Vec<PurgedTableCount>,
    /// Tables skipped for lack of a tenant column (carried through from the plan).
    pub excluded: Vec<ExcludedPurgeTable>,
    /// Sum of `purged[*].deleted`.
    pub total_deleted: u64,
    /// Whether the tenant-level cluster denylist cutoff was recorded (best-effort).
    pub tenant_denylisted: bool,
    /// How many principals had a denylist cutoff recorded (best-effort).
    pub principals_denylisted: usize,
}

/// PURE planner: enumerate every tenant-owned table in the manifest and the
/// ordered hard-delete plan. No I/O, no clock — fully unit-testable.
pub(crate) fn plan_tenant_purge(manifest: &CatalogManifest) -> TenantPurgePlan {
    let mut included: Vec<&ManifestTable> = Vec::new();
    let mut tenant_columns: Vec<String> = Vec::new();
    let mut excluded: Vec<ExcludedPurgeTable> = Vec::new();

    for table in &manifest.tables {
        // SHARED resolver — identical tenant-column resolution to generation,
        // planning, and the IR compilers. A duplicate resolver here would be the
        // exact drift the duplicate-resolver doctrine forbids.
        match resolve_tenant_column_ref(table) {
            Some(column) => {
                included.push(table);
                tenant_columns.push(column.column_name.clone());
            }
            None => excluded.push(ExcludedPurgeTable {
                schema: table.schema.clone(),
                table: table.table.clone(),
                reason: "no resolvable tenant-isolation column".to_string(),
            }),
        }
    }

    let (order, fk_ordered) = order_children_first(&included);
    let targets = order
        .into_iter()
        .map(|i| PurgeTarget {
            schema: included[i].schema.clone(),
            table: included[i].table.clone(),
            tenant_column: tenant_columns[i].clone(),
        })
        .collect();

    TenantPurgePlan {
        targets,
        excluded,
        fk_ordered,
    }
}

/// Stable topological sort of the included tables so children (tables that
/// reference another included table via a foreign key) are deleted BEFORE the
/// tables they point at. Ties break by manifest position for determinism.
///
/// Returns the index order into `tables` plus whether the order is FK-derived and
/// acyclic. On a FK cycle (or no FK edges) the remaining/all tables are appended
/// in manifest order and `fk_ordered` is `false`.
fn order_children_first(tables: &[&ManifestTable]) -> (Vec<usize>, bool) {
    use std::collections::HashMap;

    let n = tables.len();
    // `\u{1}` is not a legal SQL identifier char, so it cannot collide a schema
    // with a table name when forming the lookup key.
    let key = |schema: &str, table: &str| format!("{schema}\u{1}{table}");
    let index: HashMap<String, usize> = tables
        .iter()
        .enumerate()
        .map(|(i, t)| (key(&t.schema, &t.table), i))
        .collect();

    // Edge child→parent: child must be deleted first. `parents_of[child]` lists
    // the parents; `indeg[parent]` counts children still pending.
    let mut parents_of: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut indeg = vec![0usize; n];
    let mut had_edge = false;
    for (i, table) in tables.iter().enumerate() {
        for fk in &table.foreign_keys {
            if let Some(&parent) = index.get(&key(&fk.ref_schema, &fk.ref_table)) {
                if parent == i {
                    continue; // self-reference: no ordering constraint
                }
                had_edge = true;
                parents_of[i].push(parent);
                indeg[parent] += 1;
            }
        }
    }

    // Kahn's algorithm, emitting the lowest manifest index among the currently
    // unblocked nodes for a deterministic, manifest-stable order.
    let mut order = Vec::with_capacity(n);
    let mut visited = vec![false; n];
    while let Some(i) = (0..n).find(|&i| !visited[i] && indeg[i] == 0) {
        visited[i] = true;
        order.push(i);
        for &parent in &parents_of[i] {
            if indeg[parent] > 0 {
                indeg[parent] -= 1;
            }
        }
    }

    let acyclic = order.len() == n;
    if !acyclic {
        // FK cycle: append the still-blocked nodes in manifest order so the plan
        // is still complete (no table dropped); the caller learns the order is
        // not fully FK-safe via the returned flag.
        for i in 0..n {
            if !visited[i] {
                order.push(i);
            }
        }
    }

    (order, had_edge && acyclic)
}

/// Quoted `"schema"."table"` for the DELETE target.
fn qualified_relation(schema: &str, table: &str) -> String {
    format!("{}.{}", qi_runtime(schema), qi_runtime(table))
}

fn validate_purge_tenant_id(tenant_id: &str) -> Result<&str, tonic::Status> {
    let tenant_id = tenant_id.trim();
    if tenant_id.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "tenant_id is required",
            [("tenant_id", "must be a non-empty tenant id")],
        ));
    }
    Ok(tenant_id)
}

fn tenant_purge_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::internal_status("tenant_purge", operation, message)
}

/// Builds the tenant-purge outbox row from the per-table deleted counts and the
/// excluded tables, both of which only exist once the deletes have run inside the
/// purge transaction. Returning `None` means there is nothing to enqueue.
pub(crate) type TenantPurgeEventBuilder<'a> = &'a (
        dyn Fn(
    &[PurgedTableCount],
    &[ExcludedPurgeTable],
) -> Option<crate::runtime::core::native_store::NativeOutboxTransactionWrite>
            + Send
            + Sync
    );

/// Execute the tenant hard-delete ripple.
///
/// In ONE transaction it issues `DELETE FROM "<schema>"."<table>" WHERE
/// "<tenant_col>"::text = $1` for every planned table (children→parents) and
/// commits — so there is no half-purge: either every tenant-owned row is gone or
/// nothing is. After the commit it records the tenant-level and per-principal
/// cluster denylist cutoffs so pre-delete tokens fail validation immediately
/// instead of at TTL. Denylisting is best-effort (Redis is an accelerator over
/// the durable Postgres revocation source of truth) and NEVER fails the committed
/// purge.
///
/// The `outbox` builder, when supplied, is invoked just before the commit and its
/// row is written INSIDE the same transaction, so the purge and the event
/// announcing it are one atomic unit.
pub(crate) async fn purge_tenant(
    pool: &sqlx::PgPool,
    manifest: &CatalogManifest,
    tenant_id: &str,
    principal_ids: &[String],
    // The denylist type only exists under the `redis` feature; gate the parameter
    // so the slim build (no Redis) still compiles this module.
    #[cfg(feature = "redis")] denylist: Option<&crate::runtime::authn::revocation::JtiDenylist>,
    now_unix: u64,
    // Builds the purge event, which is then committed INSIDE the purge
    // transaction below. A tenant purge is destructive and irreversible, so a
    // committed purge whose event was lost leaves downstream systems serving a
    // tenant this broker has erased, with nothing to re-derive the fact from.
    //
    // A CLOSURE rather than a prebuilt row because the event payload reports the
    // per-table deleted counts, which do not exist until the deletes have run —
    // and they only run inside this transaction. It is handed the counts and the
    // excluded tables at the last point before commit. Returning `None` (no
    // outbox relation configured, or a rejected envelope) is a no-op.
    outbox: Option<TenantPurgeEventBuilder<'_>>,
) -> Result<TenantPurgeReport, tonic::Status> {
    let tenant_id = validate_purge_tenant_id(tenant_id)?;

    let plan = plan_tenant_purge(manifest);

    let mut tx = pool.begin().await.map_err(|err| {
        tenant_purge_internal_status(
            "begin_tenant_purge",
            format!("failed to begin tenant-purge transaction: {err}"),
        )
    })?;

    let mut purged = Vec::with_capacity(plan.targets.len());
    let mut total_deleted = 0u64;
    for target in &plan.targets {
        let sql = format!(
            "DELETE FROM {} WHERE {}::text = $1",
            qualified_relation(&target.schema, &target.table),
            qi_runtime(&target.tenant_column),
        );
        let result = sqlx::query(&sql)
            .bind(tenant_id)
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                tenant_purge_internal_status(
                    "delete_tenant_table",
                    format!(
                        "tenant purge failed deleting from {}.{}: {err}",
                        target.schema, target.table
                    ),
                )
            })?;
        let deleted = result.rows_affected();
        total_deleted += deleted;
        purged.push(PurgedTableCount {
            schema: target.schema.clone(),
            table: target.table.clone(),
            tenant_column: target.tenant_column.clone(),
            deleted,
        });
    }

    // Inside the transaction, so the purge and its event are one atomic unit: the
    // event cannot be lost after the rows are gone, and a failed enqueue aborts
    // the purge rather than erasing a tenant silently.
    if let Some(outbox) = outbox.and_then(|build| build(&purged, &plan.excluded)) {
        crate::runtime::cdc::insert_outbox_row(
            &mut *tx,
            &outbox.relation,
            outbox.event_id,
            &outbox.topic,
            &outbox.partition_key,
            &outbox.envelope,
        )
        .await
        .map_err(|err| {
            tenant_purge_internal_status(
                "tenant_purge_outbox",
                format!("failed to enqueue tenant-purge event: {err}"),
            )
        })?;
    }

    tx.commit().await.map_err(|err| {
        tenant_purge_internal_status(
            "commit_tenant_purge",
            format!("failed to commit tenant purge: {err}"),
        )
    })?;

    // ── Post-commit, best-effort token revocation ────────────────────────────
    // The purge is durably committed above; from here failures are logged, never
    // returned, so a Redis hiccup can't strand a half-revoked-but-deleted tenant.
    #[cfg(feature = "redis")]
    let (tenant_denylisted, principals_denylisted) = {
        let mut tenant_denylisted = false;
        let mut principals_denylisted = 0usize;
        if let Some(denylist) = denylist {
            match denylist.deny_tenant_after(tenant_id, now_unix).await {
                Ok(()) => tenant_denylisted = true,
                Err(err) => tracing::warn!(
                    tenant_id = %tenant_id,
                    error = %err,
                    "tenant purge committed but tenant denylist cutoff was not recorded \
                     (tokens will still fail at the durable revocation read / natural TTL)"
                ),
            }
            for principal_id in principal_ids {
                match denylist.deny_principal_after(principal_id, now_unix).await {
                    Ok(()) => principals_denylisted += 1,
                    Err(err) => tracing::warn!(
                        principal_id = %principal_id,
                        error = %err,
                        "tenant purge committed but principal denylist cutoff was not recorded"
                    ),
                }
            }
        }
        (tenant_denylisted, principals_denylisted)
    };
    // Slim build (no Redis accelerator): the durable Postgres revocation read
    // stays the source of truth, so there is no cluster-cutoff to record here.
    #[cfg(not(feature = "redis"))]
    let (tenant_denylisted, principals_denylisted) = {
        let _ = (principal_ids, now_unix);
        (false, 0usize)
    };

    Ok(TenantPurgeReport {
        tenant_id: tenant_id.to_string(),
        purged,
        excluded: plan.excluded,
        total_deleted,
        tenant_denylisted,
        principals_denylisted,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    fn decode_detail(status: &tonic::Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("error-detail trailer present")
            .to_bytes()
            .expect("trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_internal_detail(
        status: &tonic::Status,
        backend: &str,
        operation: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, backend);
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
    }

    fn column(name: &str) -> ManifestColumn {
        ManifestColumn {
            field_name: name.to_string(),
            column_name: name.to_string(),
            ..ManifestColumn::default()
        }
    }

    fn table(schema: &str, name: &str, columns: Vec<ManifestColumn>) -> ManifestTable {
        ManifestTable {
            schema: schema.to_string(),
            table: name.to_string(),
            columns,
            ..ManifestTable::default()
        }
    }

    #[test]
    fn purge_tenant_missing_tenant_id_carries_field_violation() {
        let err = validate_purge_tenant_id("  ")
            .expect_err("missing tenant_id must fail before planning or DB access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "tenant_id is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "tenant_id");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty tenant id"
        );
    }

    #[test]
    fn tenant_purge_internal_status_carries_typed_detail() {
        let status = tenant_purge_internal_status(
            "begin_tenant_purge",
            "failed to begin tenant-purge transaction",
        );
        assert_internal_detail(
            &status,
            "tenant_purge",
            "begin_tenant_purge",
            "failed to begin tenant-purge transaction",
        );
    }

    /// The PURE planner must INCLUDE every table whose tenant column resolves via
    /// the shared resolver — through any of its three signals — with the correct
    /// `tenant_column`, and REPORT (not drop) tenant-less tables in `excluded`.
    #[test]
    fn plan_includes_tenant_tables_and_excludes_tenantless() {
        // (a) tenant column declared via table_security.tenant_column.
        let mut declared = table("app", "invoice", vec![column("id"), column("owner_tenant")]);
        declared.table_security.tenant_column = "owner_tenant".to_string();

        // (b) tenant column flagged on the column itself (is_tenant_column).
        let mut flagged_col = column("tenant_ref");
        flagged_col.is_tenant_column = true;
        let flagged = table("app", "ledger", vec![column("id"), flagged_col]);

        // (c) conventional `tenant_id` name (the resolver's last-resort fallback).
        let conventional = table("app", "note", vec![column("id"), column("tenant_id")]);

        // (d) NO tenant signal: no declared column, no flag, no candidate name.
        let tenantless = table("app", "currency_rate", vec![column("id"), column("rate")]);

        let manifest = CatalogManifest {
            tables: vec![declared, flagged, conventional, tenantless],
            ..CatalogManifest::default()
        };

        let plan = plan_tenant_purge(&manifest);

        let included: std::collections::HashMap<&str, &str> = plan
            .targets
            .iter()
            .map(|t| (t.table.as_str(), t.tenant_column.as_str()))
            .collect();
        assert_eq!(
            included.len(),
            3,
            "three tenant-owned tables must be planned"
        );
        assert_eq!(included.get("invoice"), Some(&"owner_tenant"));
        assert_eq!(included.get("ledger"), Some(&"tenant_ref"));
        assert_eq!(included.get("note"), Some(&"tenant_id"));
        assert!(
            !included.contains_key("currency_rate"),
            "a tenant-less table must never be a purge target"
        );

        assert_eq!(plan.excluded.len(), 1, "the tenant-less table is reported");
        assert_eq!(plan.excluded[0].table, "currency_rate");
        assert!(
            !plan.excluded[0].reason.trim().is_empty(),
            "excluded tables must carry a human reason, not be silently skipped"
        );

        // No foreign keys link the included tables → manifest order, not FK-safe.
        assert!(!plan.fk_ordered);
    }

    /// Foreign-key metadata must order children before parents so the in-tx
    /// DELETE cannot trip a FK constraint. `line_item` → `invoice` means
    /// `line_item` is purged first; `fk_ordered` reflects the FK-derived order.
    #[test]
    fn plan_orders_children_before_parents_via_fk() {
        let parent = table("app", "invoice", vec![column("id"), column("tenant_id")]);
        let mut child = table("app", "line_item", vec![column("id"), column("tenant_id")]);
        child
            .foreign_keys
            .push(crate::generation::ManifestForeignKey {
                name: "line_item_invoice_fk".to_string(),
                columns: vec!["invoice_id".to_string()],
                ref_schema: "app".to_string(),
                ref_table: "invoice".to_string(),
                ref_columns: vec!["id".to_string()],
                ..crate::generation::ManifestForeignKey::default()
            });

        // Parent declared first in the manifest; the sort must still emit the
        // child (line_item) before the parent (invoice).
        let manifest = CatalogManifest {
            tables: vec![parent, child],
            ..CatalogManifest::default()
        };

        let plan = plan_tenant_purge(&manifest);
        let order: Vec<&str> = plan.targets.iter().map(|t| t.table.as_str()).collect();
        assert_eq!(order, vec!["line_item", "invoice"], "children purge first");
        assert!(
            plan.fk_ordered,
            "FK metadata yielded an acyclic child→parent order"
        );
    }

    // NOTE: the live `DELETE` + commit + denylist behavior of `purge_tenant` is
    // exercised by the env-gated live PG suite (it needs a real database and
    // Redis), not by a unit test — the pure planner above is the unit-level
    // contract.
}
