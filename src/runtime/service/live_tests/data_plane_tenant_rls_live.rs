//! RLS1 / F6 — LIVE tenant-isolation of the SERVED data-plane CRUD path.
//!
//! Reproduces the customer-reported cross-tenant leak on the broker's own
//! `Select` / `Update` / `Delete` verbs against a REAL Postgres. A caller
//! authenticated as tenant **A** presents a body filter that names a FOREIGN
//! tenant **B** (`{tenant_id: B}` — the attack), or omits the tenant predicate
//! entirely; the compiler ANDs the VERIFIED-claim tenant into every WHERE, so
//! the attack matches ZERO rows and a scan never returns B's rows.
//!
//! These drive the exact served entry points a gRPC client's Select/Update/
//! Delete land on — `DataBrokerRuntime::{select,update,delete}` — whose Postgres
//! path lowers the request to neutral IR and compiles it through
//! `compile_bridged_postgres_statement` with `CompileContext.with_tenant(claim)`.
//! The fix under test is the `context_predicates` backstop in
//! `src/ir/compile/postgres.rs` (`compile_read`/`compile_update`/`compile_delete`).
//!
//! The throwaway table is created with NO row-level-security policy on purpose:
//! Postgres RLS is bypassed at runtime for an owner connection without
//! `FORCE ROW LEVEL SECURITY`, which is precisely the deployment the fix
//! defends. The compiler predicate is therefore the ONLY isolation boundary, so
//! reverting it makes every assertion below fail (the attack starts affecting /
//! returning tenant B's rows).
//!
//! Run with a live Postgres (see the session runbook):
//!   UDB_LIVE_NATIVE_PG_DSN=postgres://udb:udb@127.0.0.1:55432/udb \
//!     cargo test --lib data_plane_ -- --ignored --nocapture

use super::support::{live_pg_pool, live_runtime};
use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
use crate::proto::SelectRequest;
use crate::runtime::core::setup_data::MutationGuards;
use uuid::Uuid;

const MESSAGE: &str = "udb.rlsiso.v1.Doc";

/// Ad-hoc single-table manifest whose `tenant_id` column is the isolation
/// boundary (`is_tenant_column: true` makes `resolve_tenant_column` return it,
/// so the compiler's `context_predicates` AND-scopes on it).
fn iso_manifest(schema: &str) -> CatalogManifest {
    CatalogManifest {
        // A non-empty checksum keys the manifest lookup index; unique per run.
        checksum_sha256: format!("rls-iso-{schema}"),
        tables: vec![ManifestTable {
            message_name: "Doc".to_string(),
            proto_package: "udb.rlsiso.v1".to_string(),
            schema: schema.to_string(),
            table: "docs".to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec![
                ManifestColumn {
                    field_name: "id".to_string(),
                    column_name: "id".to_string(),
                    proto_type: "string".to_string(),
                    sql_type: "TEXT".to_string(),
                    is_primary: true,
                    not_null: true,
                    ..ManifestColumn::default()
                },
                ManifestColumn {
                    field_name: "tenant_id".to_string(),
                    column_name: "tenant_id".to_string(),
                    proto_type: "string".to_string(),
                    sql_type: "TEXT".to_string(),
                    not_null: true,
                    is_tenant_column: true,
                    ..ManifestColumn::default()
                },
                ManifestColumn {
                    field_name: "label".to_string(),
                    column_name: "label".to_string(),
                    proto_type: "string".to_string(),
                    sql_type: "TEXT".to_string(),
                    not_null: true,
                    ..ManifestColumn::default()
                },
            ],
            ..ManifestTable::default()
        }],
        ..CatalogManifest::default()
    }
}

/// Verified-claim context for a NON-admin caller of `tenant`. `purpose` +
/// `udb:read`/`udb:write` scopes are required or the planner rejects the request
/// before it reaches the compiler.
fn tenant_context(tenant: &str) -> crate::RequestContext {
    crate::RequestContext {
        tenant_id: tenant.to_string(),
        project_id: String::new(),
        purpose: "rls-iso-test".to_string(),
        scopes: vec!["udb:read".to_string(), "udb:write".to_string()],
        ..Default::default()
    }
}

async fn create_docs_table(pool: &sqlx::PgPool, schema: &str) {
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(pool)
        .await
        .expect("create throwaway schema");
    // Plain table, NO RLS policy: the compiler predicate is the sole boundary.
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".docs (\
            id text PRIMARY KEY, tenant_id text NOT NULL, label text NOT NULL)"
    ))
    .execute(pool)
    .await
    .expect("create docs table");
}

async fn seed(pool: &sqlx::PgPool, schema: &str, tenant_a: &str, tenant_b: &str) {
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".docs (id, tenant_id, label) VALUES \
         ($1,$2,$3),($4,$5,$6),($7,$8,$9),($10,$11,$12)"
    ))
    .bind("a1")
    .bind(tenant_a)
    .bind("alice-doc")
    .bind("a2")
    .bind(tenant_a)
    .bind("ann-doc")
    .bind("b1")
    .bind(tenant_b)
    .bind("bob-doc")
    .bind("b2")
    .bind(tenant_b)
    .bind("ben-doc")
    .execute(pool)
    .await
    .expect("seed cross-tenant docs");
}

async fn count_tenant_rows(pool: &sqlx::PgPool, schema: &str, tenant: &str) -> i64 {
    use sqlx::Row;
    sqlx::query(&format!(
        "SELECT count(*) AS n FROM \"{schema}\".docs WHERE tenant_id = $1"
    ))
    .bind(tenant)
    .fetch_one(pool)
    .await
    .expect("count tenant rows")
    .try_get::<i64, _>("n")
    .expect("count column")
}

async fn label_of(pool: &sqlx::PgPool, schema: &str, id: &str) -> String {
    use sqlx::Row;
    sqlx::query(&format!("SELECT label FROM \"{schema}\".docs WHERE id = $1"))
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("select label")
        .try_get::<String, _>("label")
        .expect("label column")
}

/// RLS1: a served `Select` authenticated as tenant A must never return tenant
/// B's rows — whether the caller FORGES a `{tenant_id: B}` predicate or OMITS
/// the tenant predicate entirely. Revert of the `compile_read` context predicate
/// ⇒ the forged select returns B's rows / the un-scoped select returns all four.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_NATIVE_PG_DSN=... -- --ignored"]
async fn data_plane_select_never_returns_foreign_tenant_rows_live() {
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");
    let schema = format!("udb_rls_iso_{}", Uuid::new_v4().simple());
    let tenant_a = format!("tnt-a-{}", Uuid::new_v4().simple());
    let tenant_b = format!("tnt-b-{}", Uuid::new_v4().simple());
    create_docs_table(&pool, &schema).await;
    seed(&pool, &schema, &tenant_a, &tenant_b).await;

    let runtime = live_runtime().await;
    let manifest = iso_manifest(&schema);
    let ctx_a = tenant_context(&tenant_a);

    // Attack 1 — FORGED foreign-tenant predicate: caller (tenant A) asks for
    // {tenant_id: B}. Compiler ANDs tenant_id = A ⇒ zero rows.
    let forged = SelectRequest {
        context: None,
        message_type: MESSAGE.to_string(),
        filter: crate::runtime::executor_utils::json_to_struct(
            &serde_json::json!({ "tenant_id": tenant_b }),
        ),
        limit: 100,
        ..Default::default()
    };
    let (rows, _) = runtime
        .select(&manifest, forged, ctx_a.clone())
        .await
        .expect("served select (forged tenant filter) must succeed");
    assert!(
        rows.records_json.is_empty(),
        "forged {{tenant_id: B}} select as tenant A must return 0 rows, got {}",
        rows.records_json.len()
    );

    // Attack 2 — OMITTED tenant predicate: a bare list. Either the planner
    // fail-closes on an un-scoped read, or the compiler still ANDs tenant_id = A
    // so only A's two rows come back — never B's. A revert that drops the compiler
    // scope AND lets the read run would surface all four rows here.
    let unscoped = SelectRequest {
        context: None,
        message_type: MESSAGE.to_string(),
        filter: None,
        limit: 100,
        ..Default::default()
    };
    // A fail-closed planner rejection of an un-scoped read is also correct; only a
    // SUCCESSFUL read must be tenant-A-only.
    if let Ok((rows, _)) = runtime.select(&manifest, unscoped, ctx_a).await {
        assert_eq!(
            rows.records_json.len(),
            2,
            "un-scoped select as tenant A must return ONLY A's 2 rows (never B's); \
             got {} — a >2 count is the cross-tenant leak",
            rows.records_json.len()
        );
    }

    let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(&pool)
        .await;
    pool.close().await;
}

/// RLS1: a served `Delete` authenticated as tenant A that names a FOREIGN
/// tenant's row must affect ZERO rows and leave tenant B's data intact; a
/// same-tenant delete still works (the scope AND does not over-block). Revert of
/// the `compile_delete` context predicate ⇒ the attack deletes B's row.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_NATIVE_PG_DSN=... -- --ignored"]
async fn data_plane_delete_cannot_touch_foreign_tenant_rows_live() {
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");
    let schema = format!("udb_rls_iso_{}", Uuid::new_v4().simple());
    let tenant_a = format!("tnt-a-{}", Uuid::new_v4().simple());
    let tenant_b = format!("tnt-b-{}", Uuid::new_v4().simple());
    create_docs_table(&pool, &schema).await;
    seed(&pool, &schema, &tenant_a, &tenant_b).await;

    let runtime = live_runtime().await;
    let manifest = iso_manifest(&schema);
    let ctx_a = tenant_context(&tenant_a);

    // Attack — tenant A tries to delete tenant B's row b1 by naming it exactly.
    let resp = runtime
        .delete(
            &manifest,
            MESSAGE,
            serde_json::json!({ "tenant_id": tenant_b, "id": "b1" }),
            ctx_a.clone(),
            String::new(),
            None,
            MutationGuards::default(),
        )
        .await
        .expect("served delete must succeed (0 rows), not error");
    assert_eq!(
        resp.affected_rows, 0,
        "cross-tenant delete as tenant A must affect 0 rows"
    );
    assert_eq!(
        count_tenant_rows(&pool, &schema, &tenant_b).await,
        2,
        "tenant B's rows must be untouched by tenant A's delete (revert ⇒ b1 gone)"
    );

    // Positive control — A deletes its OWN row a1: exactly 1 row.
    let resp = runtime
        .delete(
            &manifest,
            MESSAGE,
            serde_json::json!({ "tenant_id": tenant_a, "id": "a1" }),
            ctx_a,
            String::new(),
            None,
            MutationGuards::default(),
        )
        .await
        .expect("same-tenant delete must succeed");
    assert_eq!(
        resp.affected_rows, 1,
        "same-tenant delete must still affect the caller's own row"
    );
    assert_eq!(
        count_tenant_rows(&pool, &schema, &tenant_a).await,
        1,
        "A's own delete removed exactly one of A's rows"
    );

    let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(&pool)
        .await;
    pool.close().await;
}

/// RLS1: a served `Update` authenticated as tenant A that names a FOREIGN
/// tenant (`{tenant_id: B}`) must affect ZERO rows and never mutate B's data.
/// Revert of the `compile_update` context predicate ⇒ the attack mass-updates
/// B's rows.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_NATIVE_PG_DSN=... -- --ignored"]
async fn data_plane_update_cannot_touch_foreign_tenant_rows_live() {
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");
    let schema = format!("udb_rls_iso_{}", Uuid::new_v4().simple());
    let tenant_a = format!("tnt-a-{}", Uuid::new_v4().simple());
    let tenant_b = format!("tnt-b-{}", Uuid::new_v4().simple());
    create_docs_table(&pool, &schema).await;
    seed(&pool, &schema, &tenant_a, &tenant_b).await;

    let runtime = live_runtime().await;
    let manifest = iso_manifest(&schema);
    let ctx_a = tenant_context(&tenant_a);

    // Attack — tenant A tries to overwrite EVERY tenant-B row's label.
    let resp = runtime
        .update(
            &manifest,
            MESSAGE,
            serde_json::json!({ "tenant_id": tenant_b }),
            serde_json::json!({ "label": "HACKED" }),
            Vec::new(),
            ctx_a.clone(),
            String::new(),
            None,
            false,
            MutationGuards::default(),
        )
        .await
        .expect("served update must succeed (0 rows), not error");
    assert_eq!(
        resp.affected_rows, 0,
        "cross-tenant update as tenant A must affect 0 rows"
    );
    assert_eq!(
        label_of(&pool, &schema, "b1").await,
        "bob-doc",
        "tenant B's row must keep its original label (revert ⇒ 'HACKED')"
    );
    assert_eq!(
        label_of(&pool, &schema, "b2").await,
        "ben-doc",
        "tenant B's second row must be untouched too"
    );

    // Positive control — A updates its OWN row a1: exactly 1 row.
    let resp = runtime
        .update(
            &manifest,
            MESSAGE,
            serde_json::json!({ "tenant_id": tenant_a, "id": "a1" }),
            serde_json::json!({ "label": "own-edit" }),
            Vec::new(),
            ctx_a,
            String::new(),
            None,
            false,
            MutationGuards::default(),
        )
        .await
        .expect("same-tenant update must succeed");
    assert_eq!(
        resp.affected_rows, 1,
        "same-tenant update must still affect the caller's own row"
    );
    assert_eq!(label_of(&pool, &schema, "a1").await, "own-edit");

    let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(&pool)
        .await;
    pool.close().await;
}
