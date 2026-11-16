//! One physical relation per project.
//!
//! Physical tables carry no project prefix, and under the default Permissive
//! routing mode an unlabeled backend instance serves every project. Two
//! projects that declare the same `schema.table` therefore land on the same
//! physical table and silently read and write each other's rows.
//!
//! Catalog staging and activation refuse that: a relation another project's
//! ACTIVE catalog already declares on a backend instance this project can also
//! reach is refused, unless
//!
//! * both declarations set `pg_table.cross_project_shared: true` (sharing is
//!   deliberate on both sides), or
//! * both declarations scope rows by the same project column, which the data
//!   plane ANDs into every statement for a project-bound request (so the rows
//!   really are isolated per project).
//!
//! UDB's own native-service tables are platform-owned (every project catalog
//! carries them by design and they are tenant/project scoped by the native
//! services) and are not claims.
//!
//! The registry is the existing catalog-version table: the claims of every
//! other project are read from its ACTIVE catalog's manifest. No new table.

use std::collections::{BTreeMap, BTreeSet};

use crate::generation::manifest::{CatalogManifest, ManifestTable};

use super::DataBrokerRuntime;
use super::catalog_admin::{catalog_admin_internal_status, catalog_admin_schema_status};

/// Reason code carried by the refusal.
pub(crate) const RELATION_CLAIMED_BY_OTHER_PROJECT: &str =
    "catalog_relation_claimed_by_other_project";

/// Advisory-lock key that serializes every relation-claim check, so two
/// projects activating the same table concurrently cannot both pass. Always
/// taken AFTER the per-project catalog lock, in every caller.
const RELATION_CLAIM_LOCK_KEY: &str = "udb.catalog.relation_claims";
const RELATION_CLAIM_LOCK_SEED: i64 = 534_155;

/// One physical relation a catalog declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelationClaim {
    /// Backend token, lowercase (`postgres`, `mysql`, ...).
    pub backend: String,
    /// Instance the declaration pins, empty when it lets routing choose.
    pub instance: String,
    /// Physical relation name, lowercase (`schema.table`).
    pub relation: String,
    /// Fully qualified proto message that declares it.
    pub message_fqn: String,
    pub cross_project_shared: bool,
    /// The project column rows are scoped by, when the table has one.
    pub project_column: Option<String>,
}

/// A relation the candidate catalog would share with another project.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RelationClaimConflict {
    pub relation: String,
    pub backend: String,
    pub message_fqn: String,
    pub other_project: String,
    pub other_message_fqn: String,
    /// Backend instances both projects reach.
    pub instances: Vec<String>,
    pub candidate_shared: bool,
    pub other_shared: bool,
}

/// UDB-owned relations: the embedded native-service manifest.
pub(crate) fn platform_relations(native: &CatalogManifest) -> BTreeSet<String> {
    native
        .tables
        .iter()
        .map(|table| relation_key(&table.schema, &table.table))
        .collect()
}

fn relation_key(schema: &str, table: &str) -> String {
    let schema = schema.trim();
    let table = table.trim();
    if schema.is_empty() {
        table.to_ascii_lowercase()
    } else {
        format!("{schema}.{table}").to_ascii_lowercase()
    }
}

fn platform_owned(table: &ManifestTable, platform: &BTreeSet<String>) -> bool {
    platform.contains(&relation_key(&table.schema, &table.table))
}

/// The relational resources a manifest declares, excluding UDB-owned ones.
/// Every table owns its canonical relation (`schema.table` on PostgreSQL);
/// relational projections onto other SQL backends are claims too.
pub(crate) fn relation_claims(
    manifest: &CatalogManifest,
    platform: &BTreeSet<String>,
) -> Vec<RelationClaim> {
    let mut claims = Vec::new();
    for table in &manifest.tables {
        if platform_owned(table, platform) {
            continue;
        }
        let project_column = crate::generation::sql::resolve_project_column(table)
            .map(|column| column.to_ascii_lowercase());
        let message_fqn = table.message_fqn();
        let mut targets: Vec<(String, String, String)> = table
            .projections
            .iter()
            .filter(|projection| projection.projection_kind == "relational")
            .filter(|projection| !projection.resource_name.trim().is_empty())
            .map(|projection| {
                (
                    projection.backend.trim().to_ascii_lowercase(),
                    projection.instance.trim().to_string(),
                    projection.resource_name.trim().to_ascii_lowercase(),
                )
            })
            .collect();
        let canonical = (
            "postgres".to_string(),
            String::new(),
            relation_key(&table.schema, &table.table),
        );
        if !targets.contains(&canonical) {
            targets.push(canonical);
        }
        for (backend, instance, relation) in targets {
            let claim = RelationClaim {
                backend,
                instance,
                relation,
                message_fqn: message_fqn.clone(),
                cross_project_shared: table.cross_project_shared,
                project_column: project_column.clone(),
            };
            if !claims.contains(&claim) {
                claims.push(claim);
            }
        }
    }
    claims
}

/// Whether two declarations of one physical relation may coexist.
fn claims_may_share(candidate: &RelationClaim, other: &RelationClaim) -> bool {
    if candidate.cross_project_shared && other.cross_project_shared {
        return true;
    }
    matches!(
        (&candidate.project_column, &other.project_column),
        (Some(a), Some(b)) if a == b
    )
}

/// The collision decision. `reachable(project, claim)` returns the backend
/// instances the claim can land on for that project under the current routing.
/// Claims of the candidate's own project are never conflicts (re-applying or
/// evolving one project's catalog).
pub(crate) fn relation_claim_conflicts<F>(
    candidate_project: &str,
    candidate: &[RelationClaim],
    others: &[(String, Vec<RelationClaim>)],
    reachable: F,
) -> Vec<RelationClaimConflict>
where
    F: Fn(&str, &RelationClaim) -> BTreeSet<String>,
{
    let candidate_project = candidate_project.trim();
    let mut conflicts: BTreeMap<(String, String, String), RelationClaimConflict> = BTreeMap::new();
    for claim in candidate {
        let mine = reachable(candidate_project, claim);
        if mine.is_empty() {
            continue;
        }
        for (other_project, other_claims) in others {
            let other_project = other_project.trim();
            if other_project == candidate_project {
                continue;
            }
            for other in other_claims {
                if other.backend != claim.backend || other.relation != claim.relation {
                    continue;
                }
                if claims_may_share(claim, other) {
                    continue;
                }
                let theirs = reachable(other_project, other);
                let shared: Vec<String> = mine.intersection(&theirs).cloned().collect();
                if shared.is_empty() {
                    continue;
                }
                conflicts
                    .entry((
                        claim.relation.clone(),
                        claim.backend.clone(),
                        other_project.to_string(),
                    ))
                    .or_insert_with(|| RelationClaimConflict {
                        relation: claim.relation.clone(),
                        backend: claim.backend.clone(),
                        message_fqn: claim.message_fqn.clone(),
                        other_project: other_project.to_string(),
                        other_message_fqn: other.message_fqn.clone(),
                        instances: shared,
                        candidate_shared: claim.cross_project_shared,
                        other_shared: other.cross_project_shared,
                    });
            }
        }
    }
    conflicts.into_values().collect()
}

/// The operator-facing refusal: names each table, the other project, the
/// shared backend instance(s), and every way to resolve it.
pub(crate) fn relation_claim_conflict_message(
    candidate_project: &str,
    conflicts: &[RelationClaimConflict],
) -> String {
    let details: Vec<String> = conflicts
        .iter()
        .map(|conflict| {
            let sharing = match (conflict.candidate_shared, conflict.other_shared) {
                (true, false) => format!(
                    "; this catalog marks it cross_project_shared but project '{}' does not",
                    conflict.other_project
                ),
                (false, true) => format!(
                    "; project '{}' marks it cross_project_shared but this catalog does not",
                    conflict.other_project
                ),
                _ => String::new(),
            };
            format!(
                "table {} ({}, message {}) is already declared by project '{}' (message {}) on {} instance(s) [{}] that project '{}' also reaches{}",
                conflict.relation,
                conflict.backend,
                conflict.message_fqn,
                conflict.other_project,
                conflict.other_message_fqn,
                conflict.backend,
                conflict.instances.join(", "),
                candidate_project,
                sharing,
            )
        })
        .collect();
    format!(
        "refused: two projects would share one physical table and read and write each other's rows: {}. \
         Resolve one of these ways: rename the table (pg_table.table_name) or move it to another schema \
         (pg_table.schema_name) in one project; scope its rows by project in BOTH projects \
         (db_table_security project_isolation_mode with a project_column); give each project its own \
         backend instance (label the instance project_id=<project>, or set UDB_PROJECT_ROUTING_MODE=strict); \
         or, when sharing is intended, set cross_project_shared: true in pg_table for this table in BOTH projects",
        details.join("; ")
    )
}

/// Storage-isolation advisories: unlabeled backend instances under Permissive
/// routing while more than one project is registered.
pub(crate) fn unlabeled_backend_warnings(
    project_count: usize,
    unlabeled: &[(String, String)],
) -> Vec<String> {
    if project_count < 2 {
        return Vec::new();
    }
    unlabeled
        .iter()
        .map(|(backend, instance)| {
            format!(
                "project storage isolation: {backend} instance '{instance}' has no project label and \
                 UDB_PROJECT_ROUTING_MODE is permissive, so it serves all {project_count} registered \
                 projects; physical tables have no project prefix, so projects that declare the same \
                 table share its rows unless it is project-scoped or marked cross_project_shared. \
                 Label the instance (project_id=<project> or projects=<a,b>) or set \
                 UDB_PROJECT_ROUTING_MODE=strict"
            )
        })
        .collect()
}

impl DataBrokerRuntime {
    /// Refuse `manifest` for `project_id` when one of its relations is already
    /// declared by another project's ACTIVE catalog on a backend instance both
    /// projects reach (see the module docs for the exemptions). Runs inside the
    /// caller's catalog transaction, after its per-project lock.
    pub(super) async fn refuse_cross_project_relation_claims(
        &self,
        connection: &mut sqlx::PgConnection,
        operation: &'static str,
        project_id: &str,
        manifest: &CatalogManifest,
    ) -> Result<(), tonic::Status> {
        let platform = platform_relations(crate::runtime::native_catalog::native_manifest());
        let candidate = relation_claims(manifest, &platform);
        if candidate.is_empty() {
            return Ok(());
        }
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, $2))")
            .bind(RELATION_CLAIM_LOCK_KEY)
            .bind(RELATION_CLAIM_LOCK_SEED)
            .execute(&mut *connection)
            .await
            .map_err(|err| {
                catalog_admin_internal_status(
                    "catalog_relation_claims_lock",
                    format!("catalog relation-claim lock failed: {err}"),
                )
            })?;
        let others = self
            .active_relation_claims_on_connection(
                connection,
                operation,
                Some(project_id),
                &platform,
            )
            .await?;
        let conflicts =
            relation_claim_conflicts(project_id, &candidate, &others, |owner, claim| {
                self.relation_claim_instances_for_project(&claim.backend, &claim.instance, owner)
            });
        if conflicts.is_empty() {
            return Ok(());
        }
        Err(catalog_admin_schema_status(
            operation,
            RELATION_CLAIMED_BY_OTHER_PROJECT,
            relation_claim_conflict_message(project_id, &conflicts),
        ))
    }

    /// The relation claims of every ACTIVE project catalog except `exclude`.
    /// A stored catalog that does not decode is a refusal: ownership cannot be
    /// proven without it.
    async fn active_relation_claims_on_connection(
        &self,
        connection: &mut sqlx::PgConnection,
        operation: &'static str,
        exclude: Option<&str>,
        platform: &BTreeSet<String>,
    ) -> Result<Vec<(String, Vec<RelationClaim>)>, tonic::Status> {
        let cat_rel =
            crate::runtime::system::SystemCatalogConfig::default().catalog_versions_relation();
        let rows: Vec<(String, String)> = sqlx::query_as(&format!(
            "SELECT project_id, manifest_json::TEXT FROM {cat_rel}
             WHERE status = 'ACTIVE' AND project_id <> $1
             ORDER BY project_id"
        ))
        .bind(exclude.unwrap_or_default())
        .fetch_all(&mut *connection)
        .await
        .map_err(|err| {
            catalog_admin_internal_status(
                "catalog_relation_claims_lookup",
                format!("active catalog relation-claim lookup failed: {err}"),
            )
        })?;
        let mut out = Vec::with_capacity(rows.len());
        for (other_project, manifest_json) in rows {
            let other: CatalogManifest = serde_json::from_str(&manifest_json).map_err(|err| {
                catalog_admin_schema_status(
                    operation,
                    "catalog_relation_claims_unreadable",
                    format!(
                        "cannot prove table ownership: the ACTIVE catalog of project '{other_project}' does not decode as a catalog manifest ({err})"
                    ),
                )
            })?;
            out.push((other_project, relation_claims(&other, platform)));
        }
        Ok(out)
    }

    /// Storage-isolation findings for startup logs and `udb doctor`:
    /// unlabeled backend instances under Permissive routing with more than one
    /// registered project, and relations that ACTIVE catalogs of different
    /// projects already share (activated before this check existed).
    pub async fn project_storage_isolation_findings(&self) -> Result<Vec<String>, tonic::Status> {
        let pool = self.pg_pool()?;
        let mut connection = pool.acquire().await.map_err(|err| {
            catalog_admin_internal_status("project_storage_isolation_findings", err.to_string())
        })?;
        let platform = platform_relations(crate::runtime::native_catalog::native_manifest());
        let catalogs = self
            .active_relation_claims_on_connection(
                &mut connection,
                "project_storage_isolation_findings",
                None,
                &platform,
            )
            .await?;
        let mut findings = unlabeled_backend_warnings(
            catalogs.len(),
            &self.unlabeled_permissive_backend_instances(),
        );
        for (index, (project, claims)) in catalogs.iter().enumerate() {
            let later = &catalogs[index + 1..];
            let conflicts = relation_claim_conflicts(project, claims, later, |owner, claim| {
                self.relation_claim_instances_for_project(&claim.backend, &claim.instance, owner)
            });
            for conflict in conflicts {
                findings.push(format!(
                    "project storage isolation: ACTIVE catalogs of projects '{}' and '{}' both declare table {} on {} instance(s) [{}] without project scoping or cross_project_shared on both, so they share its rows; rename it, scope it by project, label the backends, or mark it cross_project_shared in both",
                    project,
                    conflict.other_project,
                    conflict.relation,
                    conflict.backend,
                    conflict.instances.join(", "),
                ));
            }
        }
        Ok(findings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::manifest::ManifestColumn;

    fn table(schema: &str, name: &str) -> ManifestTable {
        ManifestTable {
            message_name: "Order".to_string(),
            proto_package: "acme.shop.v1".to_string(),
            schema: schema.to_string(),
            table: name.to_string(),
            columns: vec![ManifestColumn {
                field_name: "id".to_string(),
                column_name: "id".to_string(),
                sql_type: "TEXT".to_string(),
                is_primary: true,
                ..ManifestColumn::default()
            }],
            ..ManifestTable::default()
        }
    }

    fn project_scoped(mut table: ManifestTable) -> ManifestTable {
        table.columns.push(ManifestColumn {
            field_name: "project_id".to_string(),
            column_name: "project_id".to_string(),
            sql_type: "TEXT".to_string(),
            ..ManifestColumn::default()
        });
        table.table_security.project_isolation_mode = "column".to_string();
        table.table_security.project_column = "project_id".to_string();
        table
    }

    fn shared(mut table: ManifestTable) -> ManifestTable {
        table.cross_project_shared = true;
        table
    }

    fn manifest(tables: Vec<ManifestTable>) -> CatalogManifest {
        CatalogManifest {
            tables,
            ..CatalogManifest::default()
        }
    }

    fn claims(tables: Vec<ManifestTable>) -> Vec<RelationClaim> {
        relation_claims(&manifest(tables), &BTreeSet::new())
    }

    /// Permissive routing over one unlabeled primary: every project reaches it.
    fn one_unlabeled_primary(_project: &str, _claim: &RelationClaim) -> BTreeSet<String> {
        BTreeSet::from(["primary".to_string()])
    }

    fn decide(
        candidate: Vec<ManifestTable>,
        other_project: &str,
        other: Vec<ManifestTable>,
    ) -> Vec<RelationClaimConflict> {
        relation_claim_conflicts(
            "alpha",
            &claims(candidate),
            &[(other_project.to_string(), claims(other))],
            one_unlabeled_primary,
        )
    }

    #[test]
    fn same_table_in_another_project_on_the_same_backend_is_refused() {
        let conflicts = decide(
            vec![table("shop", "orders")],
            "beta",
            vec![table("shop", "orders")],
        );
        assert_eq!(conflicts.len(), 1, "{conflicts:?}");
        let conflict = &conflicts[0];
        assert_eq!(conflict.relation, "shop.orders");
        assert_eq!(conflict.backend, "postgres");
        assert_eq!(conflict.other_project, "beta");
        assert_eq!(conflict.instances, vec!["primary".to_string()]);
        let message = relation_claim_conflict_message("alpha", &conflicts);
        for needle in [
            "shop.orders",
            "project 'beta'",
            "cross_project_shared",
            "project_isolation_mode",
            "table_name",
            "UDB_PROJECT_ROUTING_MODE=strict",
        ] {
            assert!(message.contains(needle), "{needle} missing from: {message}");
        }
    }

    #[test]
    fn relation_names_compare_case_insensitively() {
        let conflicts = decide(
            vec![table("Shop", "Orders")],
            "beta",
            vec![table("shop", "orders")],
        );
        assert_eq!(conflicts.len(), 1);
    }

    #[test]
    fn both_marked_shared_is_allowed() {
        assert!(
            decide(
                vec![shared(table("shop", "orders"))],
                "beta",
                vec![shared(table("shop", "orders"))],
            )
            .is_empty()
        );
    }

    #[test]
    fn only_one_side_marked_shared_is_refused_and_says_which() {
        let conflicts = decide(
            vec![shared(table("shop", "orders"))],
            "beta",
            vec![table("shop", "orders")],
        );
        assert_eq!(conflicts.len(), 1);
        assert!(conflicts[0].candidate_shared && !conflicts[0].other_shared);
        let message = relation_claim_conflict_message("alpha", &conflicts);
        assert!(
            message
                .contains("this catalog marks it cross_project_shared but project 'beta' does not"),
            "{message}"
        );
    }

    #[test]
    fn project_column_isolation_on_both_sides_is_allowed() {
        assert!(
            decide(
                vec![project_scoped(table("shop", "orders"))],
                "beta",
                vec![project_scoped(table("shop", "orders"))],
            )
            .is_empty()
        );
    }

    #[test]
    fn project_column_on_one_side_only_is_refused() {
        let conflicts = decide(
            vec![project_scoped(table("shop", "orders"))],
            "beta",
            vec![table("shop", "orders")],
        );
        assert_eq!(conflicts.len(), 1);
    }

    #[test]
    fn same_project_reapply_is_allowed() {
        assert!(
            decide(
                vec![table("shop", "orders")],
                "alpha",
                vec![table("shop", "orders")],
            )
            .is_empty()
        );
    }

    #[test]
    fn different_tables_do_not_collide() {
        assert!(
            decide(
                vec![table("shop", "orders")],
                "beta",
                vec![table("shop", "invoices"), table("billing", "orders")],
            )
            .is_empty()
        );
    }

    #[test]
    fn projects_on_separately_labelled_instances_do_not_collide() {
        let by_label =
            |project: &str, _claim: &RelationClaim| BTreeSet::from([format!("pg-{project}")]);
        let conflicts = relation_claim_conflicts(
            "alpha",
            &claims(vec![table("shop", "orders")]),
            &[("beta".to_string(), claims(vec![table("shop", "orders")]))],
            by_label,
        );
        assert!(conflicts.is_empty(), "{conflicts:?}");
    }

    #[test]
    fn platform_owned_native_tables_are_not_claims() {
        let platform = BTreeSet::from(["udb_authn.users".to_string()]);
        let native = table("udb_authn", "users");
        assert!(relation_claims(&manifest(vec![native]), &platform).is_empty());
        let embedded = crate::runtime::native_catalog::native_manifest();
        assert!(
            relation_claims(embedded, &platform_relations(embedded)).is_empty(),
            "every embedded native table must be platform-owned"
        );
    }

    #[test]
    fn relational_projections_onto_other_sql_backends_are_claims() {
        let mut orders = table("shop", "orders");
        orders.projections = vec![crate::generation::manifest::ManifestProjection {
            projection_kind: "relational".to_string(),
            backend: "MySQL".to_string(),
            resource_name: "shop.orders".to_string(),
            ..Default::default()
        }];
        let found = claims(vec![orders]);
        assert!(found.iter().any(|claim| claim.backend == "mysql"));
        assert!(found.iter().any(|claim| claim.backend == "postgres"));
    }

    #[test]
    fn unlabeled_backend_warning_needs_more_than_one_project() {
        let unlabeled = vec![("postgres".to_string(), "primary".to_string())];
        assert!(unlabeled_backend_warnings(1, &unlabeled).is_empty());
        let warnings = unlabeled_backend_warnings(2, &unlabeled);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("postgres instance 'primary'"));
        assert!(warnings[0].contains("UDB_PROJECT_ROUTING_MODE=strict"));
    }
}
