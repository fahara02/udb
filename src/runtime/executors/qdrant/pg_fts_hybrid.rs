//! Postgres full-text + Qdrant dense hybrid vector search.
//!
//! `VectorHybridSearch` on a collection that a manifest projection fills from a
//! Postgres table, where that projection declares `fts_columns`, runs two legs:
//!
//! * the TEXT leg is Postgres full-text search (`to_tsvector` /
//!   `plainto_tsquery`, ranked by `ts_rank`) over the projection's SOURCE table,
//!   scoped by the verified tenant/project and the soft-delete tombstone, with
//!   the request's tenant GUCs installed on the connection;
//! * the DENSE leg is the Qdrant collection's kNN, scoped by the same
//!   tenant/project filter as every other vector read.
//!
//! Each matching source row is mapped to the point id the projection worker
//! writes it under (`qdrant_projection_point_key` + the executor's id hashing),
//! so both legs rank the same ids. The legs are fused with reciprocal-rank
//! fusion and the fused top-k is returned with its Qdrant payloads: a text-only
//! hit is fetched from Qdrant by id under the scoped (and caller) filter, so it
//! can never surface a point the dense leg's filter would have hidden.
//!
//! Projection options: `fts_columns` (comma list of field or column names) turns
//! the mode on; `fts_config` picks the text-search configuration (default
//! `simple`). Without `fts_columns` hybrid search is unchanged.

use std::collections::{HashMap, HashSet};

use serde_json::Value as JsonValue;

use crate::broker::RequestContext;
use crate::generation::CatalogManifest;
use crate::generation::manifest::{ManifestStoreOption, ManifestTable};
use crate::proto::{VectorHybridSearchRequest, VectorPoint, VectorSearchRequest, VectorSet};
use crate::runtime::executor_utils::{
    failed_precondition_fields, internal_status, is_encrypted_column, reject_plan,
};

use super::{QDRANT_DEFAULT_SEARCH_LIMIT, QdrantHttpClient, qdrant_point_id};

/// Text-search configuration used when the projection declares none.
pub(crate) const DEFAULT_FTS_CONFIG: &str = "simple";

/// The reciprocal-rank-fusion constant (the canonical Cormack et al. value,
/// the same default the native SearchService and Qdrant's own RRF use).
const RRF_K: f64 = 60.0;

/// The full-text source of a Qdrant collection: the projection's source table
/// and the options the text leg needs.
#[derive(Debug, Clone)]
pub(crate) struct PgFtsHybridSource {
    pub(crate) table: ManifestTable,
    /// Source primary-key columns, in declaration order (the task row key).
    pub(crate) primary_key_columns: Vec<String>,
    /// The projection target's options as the task ledger carries them.
    pub(crate) target_options: JsonValue,
    /// Physical columns concatenated into the searched document.
    pub(crate) fts_columns: Vec<String>,
    pub(crate) fts_config: String,
}

fn store_option_value(options: &[ManifestStoreOption], key: &str) -> Option<String> {
    options
        .iter()
        .find(|option| option.key.eq_ignore_ascii_case(key))
        .map(|option| option.value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn store_option_list(options: &[ManifestStoreOption], key: &str) -> Vec<String> {
    store_option_value(options, key)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

impl PgFtsHybridSource {
    /// The full-text source of `collection`: `Ok(None)` when no Qdrant
    /// projection into it declares `fts_columns` (hybrid search keeps its
    /// default behaviour), an error when the declaration cannot be served.
    pub(crate) fn for_collection(
        manifest: &CatalogManifest,
        collection: &str,
    ) -> Result<Option<Self>, String> {
        let plans = crate::runtime::projection::ProjectionPlan::from_manifest(manifest);
        let declared = plans
            .iter()
            .flat_map(|plan| plan.targets.iter().map(move |target| (plan, target)))
            .filter(|(_, target)| {
                target.backend.trim().eq_ignore_ascii_case("qdrant")
                    && target.resource_name == collection
                    && !store_option_list(&target.options, "fts_columns").is_empty()
            })
            .collect::<Vec<_>>();
        let (plan, target) = match declared.as_slice() {
            [] => return Ok(None),
            [one] => *one,
            _ => {
                return Err(format!(
                    "collection '{collection}' has more than one projection declaring fts_columns; full-text hybrid search needs exactly one source table"
                ));
            }
        };
        let table = manifest
            .tables
            .iter()
            .find(|table| table.schema == plan.source_schema && table.table == plan.source_table)
            .ok_or_else(|| {
                format!(
                    "collection '{collection}': projection source table {}.{} is not in the catalog",
                    plan.source_schema, plan.source_table
                )
            })?;
        if plan.primary_key_columns.is_empty() {
            return Err(format!(
                "collection '{collection}': full-text hybrid search needs a primary key on {}.{} to map rows to points",
                table.schema, table.table
            ));
        }
        let resolver = crate::planning::broker::column_resolver(table);
        let mut fts_columns: Vec<String> = Vec::new();
        for name in store_option_list(&target.options, "fts_columns") {
            let column = resolver
                .get(&name.to_ascii_lowercase())
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "collection '{collection}': fts_columns names '{name}', which is not a column of {}.{}",
                        table.schema, table.table
                    )
                })?;
            if table
                .columns
                .iter()
                .any(|def| def.column_name == column && is_encrypted_column(def))
            {
                return Err(format!(
                    "collection '{collection}': fts_columns names encrypted column '{column}', which cannot be full-text searched"
                ));
            }
            if !fts_columns.contains(&column) {
                fts_columns.push(column);
            }
        }
        let fts_config = store_option_value(&target.options, "fts_config")
            .unwrap_or_else(|| DEFAULT_FTS_CONFIG.to_string());
        let target_options = serde_json::to_value(&target.options)
            .map_err(|err| format!("collection '{collection}': projection options: {err}"))?;
        Ok(Some(Self {
            table: table.clone(),
            primary_key_columns: plan.primary_key_columns.clone(),
            target_options,
            fts_columns,
            fts_config,
        }))
    }

    /// Columns the row-to-point mapping reads: the primary key, the tenant
    /// field the projection stamps from, and any declared id field. Only real
    /// columns are selected (an id option naming no column is ignored by the
    /// projection's identity lookup as well).
    fn identity_columns(&self) -> Vec<String> {
        let mut columns = self.primary_key_columns.clone();
        let declared = crate::runtime::projection::ROW_IDENTITY_OPTION_KEYS
            .iter()
            .chain(["tenant_field"].iter())
            .filter_map(|key| {
                self.target_options.as_array().and_then(|entries| {
                    entries.iter().find_map(|entry| {
                        let entry_key = entry.get("key").and_then(JsonValue::as_str)?;
                        if entry_key.eq_ignore_ascii_case(key) {
                            entry.get("value").and_then(JsonValue::as_str)
                        } else {
                            None
                        }
                    })
                })
            })
            .map(|value| value.trim().to_string())
            .collect::<Vec<_>>();
        for field in declared {
            if !field.is_empty()
                && !columns.contains(&field)
                && self.table.columns.iter().any(|c| c.column_name == field)
            {
                columns.push(field);
            }
        }
        columns
    }
}

/// The text-leg statement and its positional binds (all bound as text).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TextLegQuery {
    pub(crate) sql: String,
    pub(crate) binds: Vec<String>,
}

fn qi(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn jsonb_object(columns: &[String]) -> String {
    let pairs = columns
        .iter()
        .map(|column| format!("{}, t.{}", sql_literal(column), qi(column)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("jsonb_build_object({pairs})")
}

/// `t."<column>" = $n` with the text-bound parameter cast to the column's
/// declared type (never the column to text), so an index on the tenant or
/// project column stays usable. Types the shared placeholder-cast helper
/// leaves bare are text-like, or integer/numeric columns cast here explicitly
/// (a text parameter has no implicit cast to them).
fn scope_predicate(column: &crate::generation::manifest::ManifestColumn, index: usize) -> String {
    let placeholder = format!("${index}");
    let mut value = crate::ir::compile::postgres::cast_placeholder_to_column_type(
        &column.sql_type,
        &placeholder,
    );
    if value == placeholder {
        let base = column
            .sql_type
            .split('(')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        if matches!(
            base.as_str(),
            "bigint"
                | "int8"
                | "integer"
                | "int"
                | "int4"
                | "smallint"
                | "int2"
                | "numeric"
                | "decimal"
        ) {
            value = format!("{placeholder}::{}", base.to_ascii_uppercase());
        }
    }
    format!("t.{} = {value}", qi(&column.column_name))
}

/// The Postgres full-text statement of the text leg. `$1` is the text-search
/// configuration, `$2` the query text; the tenant (and project, when the table
/// has a project column and the request names one) predicates are bound from
/// the VERIFIED request context, never from the caller filter. A tenant-scoped
/// table without a tenant in the context is refused, and a soft-delete table
/// excludes tombstoned rows.
pub(crate) fn build_text_leg_query(
    source: &PgFtsHybridSource,
    context: &RequestContext,
    text_query: &str,
    prefetch: usize,
) -> Result<TextLegQuery, String> {
    let table = &source.table;
    if source.fts_columns.is_empty() {
        return Err("full-text hybrid search needs at least one fts_columns entry".to_string());
    }
    let document = source
        .fts_columns
        .iter()
        .map(|column| format!("t.{}::text", qi(column)))
        .collect::<Vec<_>>()
        .join(", ");
    let tsvector = format!("to_tsvector($1::text::regconfig, concat_ws(' ', {document}))");
    let tsquery = "plainto_tsquery($1::text::regconfig, $2::text)";
    let mut binds = vec![source.fts_config.clone(), text_query.to_string()];
    let mut predicates = vec![format!("{tsvector} @@ {tsquery}")];

    match crate::generation::sql::resolve_tenant_column_ref(table) {
        Some(column) => {
            let tenant = context.tenant_id.trim();
            if tenant.is_empty() {
                return Err(format!(
                    "tenant-scoped table {}.{} needs a verified tenant for full-text search",
                    table.schema, table.table
                ));
            }
            binds.push(tenant.to_string());
            predicates.push(scope_predicate(column, binds.len()));
        }
        None if crate::generation::sql::table_requires_tenant_column(table) => {
            return Err(format!(
                "tenant-scoped table {}.{} has no resolvable tenant column",
                table.schema, table.table
            ));
        }
        None => {}
    }
    if let Some(column) = crate::generation::sql::resolve_project_column_ref(table)
        && !context.project_id.trim().is_empty()
    {
        binds.push(context.project_id.trim().to_string());
        predicates.push(scope_predicate(column, binds.len()));
    }
    if table.soft_delete {
        let declared = table.soft_delete_column.trim();
        let column = table
            .columns
            .iter()
            .find(|c| !declared.is_empty() && c.column_name.eq_ignore_ascii_case(declared))
            .ok_or_else(|| {
                format!(
                    "soft-delete table {}.{} soft_delete_column '{}' is not a declared column",
                    table.schema, table.table, table.soft_delete_column
                )
            })?;
        predicates.push(format!("t.{} IS NULL", qi(&column.column_name)));
    }

    let sql = format!(
        "SELECT {} AS row_key, {} AS identity, ts_rank({tsvector}, {tsquery})::float8 AS rank FROM {}.{} AS t WHERE {} ORDER BY 3 DESC, 1 ASC LIMIT {}",
        jsonb_object(&source.primary_key_columns),
        jsonb_object(&source.identity_columns()),
        qi(&table.schema),
        qi(&table.table),
        predicates.join(" AND "),
        prefetch.max(1)
    );
    Ok(TextLegQuery { sql, binds })
}

/// The id Qdrant reports for a point written under `key`: the executor's
/// hashing ([`qdrant_point_id`]) in Qdrant's canonical text form.
pub(crate) fn qdrant_point_id_text(key: &str) -> String {
    match qdrant_point_id(key) {
        JsonValue::String(id) => uuid::Uuid::parse_str(&id)
            .map(|uuid| uuid.to_string())
            .unwrap_or(id),
        other => crate::runtime::executor_utils::json_scalar_to_string(&other),
    }
}

/// Map ranked text-leg rows (`row_key`, `identity`) to point ids, in rank
/// order, first occurrence wins.
pub(crate) fn text_leg_point_ids(
    source: &PgFtsHybridSource,
    project_id: &str,
    rows: &[(JsonValue, JsonValue)],
) -> Result<Vec<String>, String> {
    let mut seen = HashSet::new();
    let mut ids = Vec::with_capacity(rows.len());
    for (row_key, identity) in rows {
        let key = crate::runtime::projection::qdrant_projection_point_key(
            project_id,
            row_key,
            &source.target_options,
            identity,
        )?;
        let id = qdrant_point_id_text(&key);
        if seen.insert(id.clone()) {
            ids.push(id);
        }
    }
    Ok(ids)
}

/// Weighted reciprocal-rank fusion of the dense and text rankings:
/// `score = w_dense / (k + rank_dense) + w_text / (k + rank_text)` with 0-based
/// ranks. `weights` is the request's `fusion_weights` (`[dense, text]`); a
/// missing, negative or non-finite weight is `1.0`. Sorted by score desc, then
/// id asc for determinism.
pub(crate) fn fuse_ranked_ids(
    dense: &[String],
    text: &[String],
    weights: &[f32],
) -> Vec<(String, f64)> {
    let weight = |index: usize| {
        weights
            .get(index)
            .copied()
            .map(f64::from)
            .filter(|w| w.is_finite() && *w >= 0.0)
            .unwrap_or(1.0)
    };
    let mut scores: HashMap<String, f64> = HashMap::new();
    for (list_index, list) in [dense, text].into_iter().enumerate() {
        let w = weight(list_index);
        for (rank, id) in list.iter().enumerate() {
            *scores.entry(id.clone()).or_insert(0.0) += w / (RRF_K + rank as f64);
        }
    }
    let mut fused = scores.into_iter().collect::<Vec<_>>();
    fused.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    fused
}

/// Order the fused ids' points: points the legs did not return (filtered out
/// or not yet projected) are dropped, each kept point carries its fused score.
pub(crate) fn assemble_fused_points(
    fused: Vec<(String, f64)>,
    mut points: HashMap<String, VectorPoint>,
    limit: usize,
) -> Vec<VectorPoint> {
    fused
        .into_iter()
        .filter_map(|(id, score)| {
            points.remove(&id).map(|mut point| {
                point.score = score as f32;
                point
            })
        })
        .take(limit)
        .collect()
}

fn hybrid_text_leg_status(message: String) -> tonic::Status {
    failed_precondition_fields(
        format!("vector hybrid full-text leg refused: {message}"),
        [("collection", message)],
    )
}

impl crate::runtime::DataBrokerRuntime {
    /// Hybrid search whose text leg is Postgres full-text search over the
    /// collection's projection source (see the module docs).
    pub(crate) async fn vector_hybrid_search_pg_fts(
        &self,
        source: &PgFtsHybridSource,
        request: &VectorHybridSearchRequest,
        context: &RequestContext,
        qdrant: &QdrantHttpClient,
        scoped_filter: JsonValue,
    ) -> Result<VectorSet, tonic::Status> {
        // C22: a table declaring `required_scope` demands it of every read.
        reject_plan(
            &crate::planning::broker::table_required_scope_error(context, &source.table)
                .into_iter()
                .collect::<Vec<_>>(),
        )?;
        let limit = if request.limit > 0 {
            request.limit as usize
        } else {
            QDRANT_DEFAULT_SEARCH_LIMIT as usize
        };
        let prefetch = if request.prefetch_limit > 0 {
            request.prefetch_limit as usize
        } else {
            (limit * 4).max(50)
        };

        let dense_points = if request.vector.is_empty() {
            Vec::new()
        } else {
            let dense = VectorSearchRequest {
                context: request.context.clone(),
                collection: request.collection.clone(),
                vector: request.vector.clone(),
                filter: request.filter.clone(),
                limit: prefetch as i32,
                score_threshold: 0.0,
                with_payload: request.with_payload,
                with_vector: request.with_vector,
                vector_name: request.vector_name.clone(),
                quantization_rescore: request.quantization_rescore,
            };
            qdrant.search(&dense, scoped_filter.clone()).await?.points
        };

        let query = build_text_leg_query(source, context, request.text_query.trim(), prefetch)
            .map_err(hybrid_text_leg_status)?;
        let rows = self
            .pg_fts_text_leg_rows(&source.table, context, &query)
            .await?;
        let text_ids = text_leg_point_ids(source, &context.project_id, &rows)
            .map_err(hybrid_text_leg_status)?;

        let dense_ids = dense_points
            .iter()
            .map(|point| point.id.clone())
            .collect::<Vec<_>>();
        let fused = fuse_ranked_ids(&dense_ids, &text_ids, &request.fusion_weights);
        let mut points = dense_points
            .into_iter()
            .map(|point| (point.id.clone(), point))
            .collect::<HashMap<_, _>>();
        let missing = fused
            .iter()
            .map(|(id, _)| id)
            .filter(|id| !points.contains_key(*id))
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            for point in qdrant
                .points_by_ids(
                    &request.collection,
                    &missing,
                    scoped_filter,
                    request.with_payload,
                    request.with_vector,
                )
                .await?
            {
                points.insert(point.id.clone(), point);
            }
        }
        Ok(VectorSet {
            points: assemble_fused_points(fused, points, limit),
        })
    }

    /// Run the text leg on a routed read connection with the request's tenant
    /// GUCs installed (and always reset before the connection is reused).
    async fn pg_fts_text_leg_rows(
        &self,
        table: &ManifestTable,
        context: &RequestContext,
        query: &TextLegQuery,
    ) -> Result<Vec<(JsonValue, JsonValue)>, tonic::Status> {
        let routed = self.pg_select_pool_for_table_routed(table, context).await?;
        let pool = routed.pool();
        let mut conn = pool.acquire().await.map_err(|err| {
            internal_status(
                "postgres",
                "vector_hybrid_text_leg",
                format!("PG connection acquire failed: {err}"),
            )
        })?;
        crate::runtime::core::set_request_local_settings_conn(&mut conn, context).await?;
        let mut statement = sqlx::query(&query.sql);
        for bind in &query.binds {
            statement = statement.bind(bind.clone());
        }
        let rows_result = statement.fetch_all(&mut *conn).await.map_err(|err| {
            internal_status(
                "postgres",
                "vector_hybrid_text_leg",
                format!("PostgreSQL full-text query failed: {err}"),
            )
        });
        let reset_result =
            crate::runtime::core::reset_request_local_settings_conn(&mut conn, context).await;
        // A connection whose GUC reset failed may still carry this tenant's
        // settings: close it instead of recycling it.
        if reset_result.is_ok() {
            drop(conn);
        } else {
            drop(conn.detach());
        }
        let rows = rows_result?;
        reset_result?;
        drop(routed);
        rows.into_iter()
            .map(|row| {
                use sqlx::Row;
                let row_key: JsonValue = row.try_get("row_key").map_err(|err| {
                    internal_status(
                        "postgres",
                        "vector_hybrid_text_leg",
                        format!("full-text row key decode failed: {err}"),
                    )
                })?;
                let identity: JsonValue = row.try_get("identity").map_err(|err| {
                    internal_status(
                        "postgres",
                        "vector_hybrid_text_leg",
                        format!("full-text row identity decode failed: {err}"),
                    )
                })?;
                Ok((row_key, identity))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::manifest::{ManifestColumn, ManifestProjection, ManifestTableSecurity};
    use serde_json::json;

    fn opt(key: &str, value: &str) -> ManifestStoreOption {
        ManifestStoreOption {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    fn col(name: &str, field: &str) -> ManifestColumn {
        ManifestColumn {
            field_name: field.to_string(),
            column_name: name.to_string(),
            sql_type: "TEXT".to_string(),
            ..ManifestColumn::default()
        }
    }

    fn manifest(options: Vec<ManifestStoreOption>) -> CatalogManifest {
        let mut secret = col("secret", "secret");
        secret.encrypted = true;
        CatalogManifest {
            checksum_sha256: format!("fts-{}", uuid::Uuid::new_v4().simple()),
            tables: vec![ManifestTable {
                message_name: "Doc".to_string(),
                schema: "app".to_string(),
                table: "docs".to_string(),
                primary_key: vec!["id".to_string()],
                table_security: ManifestTableSecurity {
                    tenant_column: "tenant_id".to_string(),
                    ..ManifestTableSecurity::default()
                },
                columns: vec![
                    col("id", "id"),
                    col("tenant_id", "tenantId"),
                    col("title", "title"),
                    col("body_text", "bodyText"),
                    secret,
                ],
                ..ManifestTable::default()
            }],
            projections: vec![ManifestProjection {
                message_type: "Doc".to_string(),
                projection_kind: "vector".to_string(),
                backend: "qdrant".to_string(),
                resource_name: "docs_vec".to_string(),
                write_policy: "projection".to_string(),
                fanout_policy: "async_projection".to_string(),
                options,
                ..ManifestProjection::default()
            }],
            ..CatalogManifest::default()
        }
    }

    fn context(tenant: &str, project: &str) -> RequestContext {
        RequestContext {
            tenant_id: tenant.to_string(),
            project_id: project.to_string(),
            ..RequestContext::default()
        }
    }

    fn source() -> PgFtsHybridSource {
        PgFtsHybridSource::for_collection(
            &manifest(vec![
                opt("vector_field", "vector"),
                opt("fts_columns", "title, bodyText"),
            ]),
            "docs_vec",
        )
        .unwrap()
        .expect("fts source")
    }

    #[test]
    fn mode_is_off_without_fts_columns_or_for_another_collection() {
        let plain = manifest(vec![opt("vector_field", "vector")]);
        assert!(
            PgFtsHybridSource::for_collection(&plain, "docs_vec")
                .unwrap()
                .is_none()
        );
        let fts = manifest(vec![opt("fts_columns", "title")]);
        assert!(
            PgFtsHybridSource::for_collection(&fts, "other")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn fts_columns_resolve_field_names_and_refuse_unknown_or_encrypted() {
        let source = source();
        assert_eq!(source.fts_columns, vec!["title", "body_text"]);
        assert_eq!(source.fts_config, DEFAULT_FTS_CONFIG);
        assert_eq!(source.primary_key_columns, vec!["id"]);
        let unknown = manifest(vec![opt("fts_columns", "nope")]);
        assert!(PgFtsHybridSource::for_collection(&unknown, "docs_vec").is_err());
        let encrypted = manifest(vec![opt("fts_columns", "title,secret")]);
        assert!(PgFtsHybridSource::for_collection(&encrypted, "docs_vec").is_err());
        let english = manifest(vec![
            opt("fts_columns", "title"),
            opt("fts_config", "english"),
        ]);
        assert_eq!(
            PgFtsHybridSource::for_collection(&english, "docs_vec")
                .unwrap()
                .unwrap()
                .fts_config,
            "english"
        );
    }

    #[test]
    fn text_leg_binds_the_verified_tenant_and_quotes_identifiers() {
        let query =
            build_text_leg_query(&source(), &context("t1", ""), "zebra stripes", 25).unwrap();
        assert_eq!(query.binds, vec!["simple", "zebra stripes", "t1"]);
        assert_eq!(
            query.sql,
            "SELECT jsonb_build_object('id', t.\"id\") AS row_key, jsonb_build_object('id', t.\"id\", 'tenant_id', t.\"tenant_id\") AS identity, ts_rank(to_tsvector($1::text::regconfig, concat_ws(' ', t.\"title\"::text, t.\"body_text\"::text)), plainto_tsquery($1::text::regconfig, $2::text))::float8 AS rank FROM \"app\".\"docs\" AS t WHERE to_tsvector($1::text::regconfig, concat_ws(' ', t.\"title\"::text, t.\"body_text\"::text)) @@ plainto_tsquery($1::text::regconfig, $2::text) AND t.\"tenant_id\" = $3 ORDER BY 3 DESC, 1 ASC LIMIT 25"
        );
        // The query text is a bind, never spliced into the statement.
        assert!(!query.sql.contains("zebra"));
    }

    #[test]
    fn text_leg_casts_the_parameter_not_the_column() {
        let mut source = source();
        for column in &mut source.table.columns {
            if column.column_name == "tenant_id" {
                column.sql_type = "UUID".to_string();
            }
        }
        source.table.columns.push(ManifestColumn {
            sql_type: "BIGINT".to_string(),
            ..col("project_id", "projectId")
        });
        let query = build_text_leg_query(&source, &context("t1", "7"), "q", 5).unwrap();
        assert!(
            query.sql.contains("AND t.\"tenant_id\" = $3::UUID"),
            "{}",
            query.sql
        );
        assert!(
            query.sql.contains("AND t.\"project_id\" = $4::BIGINT"),
            "{}",
            query.sql
        );
        assert!(!query.sql.contains("::text = $"), "{}", query.sql);
    }

    #[test]
    fn text_leg_refuses_a_tenant_table_without_a_verified_tenant() {
        assert!(build_text_leg_query(&source(), &context("  ", ""), "q", 10).is_err());
    }

    #[test]
    fn text_leg_scopes_project_and_soft_delete_and_escapes_identifiers() {
        let mut source = source();
        source.table.columns.push(col("project_id", "projectId"));
        source.table.columns.push(col("deleted_at", "deletedAt"));
        source.table.soft_delete = true;
        source.table.soft_delete_column = "deleted_at".to_string();
        source.table.table = "we\"ird".to_string();
        let query = build_text_leg_query(&source, &context("t1", "p1"), "q", 0).unwrap();
        assert_eq!(query.binds, vec!["simple", "q", "t1", "p1"]);
        assert!(
            query.sql.contains("AND t.\"project_id\" = $4"),
            "{}",
            query.sql
        );
        assert!(
            query.sql.contains("AND t.\"deleted_at\" IS NULL"),
            "{}",
            query.sql
        );
        assert!(
            query.sql.contains("FROM \"app\".\"we\"\"ird\" AS t"),
            "{}",
            query.sql
        );
        assert!(query.sql.ends_with("LIMIT 1"), "{}", query.sql);
        source.table.soft_delete_column = "missing".to_string();
        assert!(build_text_leg_query(&source, &context("t1", "p1"), "q", 5).is_err());
    }

    #[test]
    fn text_rows_map_to_the_projected_point_ids() {
        let source = source();
        let rows = vec![
            (json!({"id":"d1"}), json!({"id":"d1","tenant_id":"t1"})),
            (json!({"id":"d1"}), json!({"id":"d1","tenant_id":"t1"})),
            (json!({"id":"d2"}), json!({"id":"d2","tenant_id":"t1"})),
        ];
        let ids = text_leg_point_ids(&source, "proj-a", &rows).unwrap();
        assert_eq!(
            ids,
            vec![
                qdrant_point_id_text("t:t1/p:proj-a/d1"),
                qdrant_point_id_text("t:t1/p:proj-a/d2"),
            ],
            "rank order, duplicates collapsed, ids scoped like the projection's"
        );
        // Another tenant's row with the same primary key is a different point.
        let other = text_leg_point_ids(
            &source,
            "proj-a",
            &[(json!({"id":"d1"}), json!({"id":"d1","tenant_id":"t2"}))],
        )
        .unwrap();
        assert_ne!(other[0], ids[0]);
        assert!(uuid::Uuid::parse_str(&ids[0]).is_ok());
    }

    #[test]
    fn rrf_fuses_both_legs_and_honours_weights() {
        let dense = vec!["a".to_string(), "b".to_string()];
        let text = vec!["c".to_string(), "a".to_string()];
        let fused = fuse_ranked_ids(&dense, &text, &[]);
        assert_eq!(fused[0].0, "a", "in both legs ranks first");
        assert!((fused[0].1 - (1.0 / 60.0 + 1.0 / 61.0)).abs() < 1e-12);
        // c (text rank 0) = 1/60 beats b (dense rank 1) = 1/61.
        assert_eq!(fused[1].0, "c");
        assert_eq!(fused[2].0, "b");
        // A heavy text weight lifts the text-only hit above the shared one.
        let weighted = fuse_ranked_ids(&dense, &text, &[0.0, 5.0]);
        assert_eq!(weighted[0].0, "c");
        // Invalid weights fall back to 1.0.
        assert_eq!(fuse_ranked_ids(&dense, &text, &[f32::NAN, -1.0]), fused);
    }

    #[test]
    fn assembly_keeps_fused_order_drops_unfetched_and_truncates() {
        let point = |id: &str| VectorPoint {
            id: id.to_string(),
            ..VectorPoint::default()
        };
        let fused = vec![
            ("a".to_string(), 0.3),
            ("gone".to_string(), 0.2),
            ("b".to_string(), 0.1),
            ("c".to_string(), 0.05),
        ];
        let points = ["a", "b", "c"]
            .into_iter()
            .map(|id| (id.to_string(), point(id)))
            .collect::<HashMap<_, _>>();
        let out = assemble_fused_points(fused, points, 2);
        assert_eq!(
            out.iter().map(|p| p.id.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert!((out[0].score - 0.3).abs() < 1e-6);
    }
}
