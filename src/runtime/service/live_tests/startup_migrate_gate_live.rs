//! `UDB_MIGRATE_ENABLED=false` holds the project's schema on start. The setting was written by
//! the enterprise env template and read by nothing, so every restart applied the protos'
//! safe changes anyway. Off now means: a start changes no project table unless an approved
//! plan covers it, refuses while a change is pending, and still starts when nothing changed.

use super::support::{live_native_service_db_lock, require_live_dsn_any};
use crate::generation::CatalogManifest;
use crate::lifecycle::run_startup_lifecycle;
use crate::parser::{ParserConfig, parse_proto_source};
use crate::runtime::DataBrokerRuntime;
use crate::runtime::config::UdbConfig;
use futures::FutureExt;
use std::time::Duration;
use uuid::Uuid;

fn database_dsn(base: &str, database: &str) -> String {
    let (base, query) = base
        .split_once('?')
        .map_or((base, None), |(base, query)| (base, Some(query)));
    let slash = base
        .rfind('/')
        .expect("CI Postgres DSN has a database path");
    let mut dsn = format!("{}{database}", &base[..=slash]);
    if let Some(query) = query {
        dsn.push('?');
        dsn.push_str(query);
    }
    dsn
}

async fn pool(dsn: &str) -> sqlx::PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(dsn)
        .await
        .expect("connect the startup gate database")
}

fn protos(schema: &str, with_note: bool) -> (CatalogManifest, Vec<crate::ast::ProtoSchema>) {
    let note = if with_note {
        "\n  string note = 3 [(udb.core.common.v1.pg_column) = {sql_type:\"TEXT\"}];"
    } else {
        ""
    };
    let source = format!(
        r#"syntax = "proto3";
package startup.gate.live.v1;
message Item {{
  option (udb.core.common.v1.pg_table) = {{
    table_name: "items" schema_name: "{schema}" is_table: true audit_fields: false
  }};
  string item_id = 1 [(udb.core.common.v1.pg_column) = {{sql_type:"VARCHAR(80)" primary_key:true not_null:true}}];
  string name = 2 [(udb.core.common.v1.pg_column) = {{sql_type:"VARCHAR(80)"}}];{note}
}}
"#
    );
    let report = parse_proto_source(
        source.as_bytes(),
        "startup-gate-live.proto",
        &ParserConfig::default(),
    )
    .expect("parse the startup gate protos");
    let manifest = CatalogManifest::from_schemas(&report.schemas).expect("build the manifest");
    (manifest, report.schemas)
}

#[tokio::test]
#[ignore = "requires actual Postgres/CREATE DATABASE; unfiltered native CI live lane"]
async fn migrate_disabled_holds_project_schema_on_start_live() {
    let Some(dsn) = require_live_dsn_any(&[
        "UDB_LIVE_NATIVE_PG_DSN",
        "UDB_LIVE_AUTH_PG_DSN",
        "UDB_INTEGRATION_PG_DSN",
        "UDB_PG_DSN",
    ]) else {
        return;
    };
    let _lock = live_native_service_db_lock().lock().await;
    let administration = pool(&dsn).await;
    let database = format!("udb_startup_gate_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE \"{database}\""))
        .execute(&administration)
        .await
        .expect("create the startup gate database");
    let gate_dsn = database_dsn(&dsn, &database);
    let gate = pool(&gate_dsn).await;
    let schema = format!("gate_{}", Uuid::new_v4().simple());

    let start = async |apply_on_start: bool, with_note: bool| -> Result<(), String> {
        let mut config = UdbConfig::from_env();
        config.primary.direct_dsn = gate_dsn.clone();
        config.migration.apply_on_start = apply_on_start;
        config.migration.require_approval_plan = String::new();
        config.migration.skip_if_unchanged = false;
        let runtime = DataBrokerRuntime::from_config(config).await;
        let (manifest, schemas) = protos(&schema, with_note);
        run_startup_lifecycle(&runtime, &manifest, &schemas, false, false)
            .await
            .map(|_| ())
    };
    let table_exists = async || -> bool {
        sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(format!("\"{schema}\".items"))
            .fetch_one(&gate)
            .await
            .expect("look up the project table")
    };
    let note_exists = async || -> bool {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = 'items' AND column_name = 'note')",
        )
        .bind(&schema)
        .fetch_one(&gate)
        .await
        .expect("look up the new column")
    };
    let refused_naming = |result: Result<(), String>, what: &str| {
        let err = result.expect_err("a start with migration off and a pending change is refused");
        assert!(
            err.contains("UDB_MIGRATE_ENABLED=false") && err.contains(what),
            "the refusal names the switch and the change: {err}"
        );
    };

    let outcome = std::panic::AssertUnwindSafe(async {
        // A new project table is a pending change: off refuses, every time, and creates nothing.
        for _ in 0..2 {
            refused_naming(start(false, false).await, &schema);
            assert!(
                !table_exists().await,
                "migration off created a project table"
            );
        }
        // UDB's own system catalog still bootstrapped, so the broker's tables are there.
        let system: bool =
            sqlx::query_scalar("SELECT to_regclass('public.schema_migrations') IS NOT NULL")
                .fetch_one(&gate)
                .await
                .expect("look up the ledger");
        assert!(system, "migration off skipped UDB's own bootstrap");
        // On applies it.
        start(true, false)
            .await
            .expect("migration on applies the new table");
        assert!(table_exists().await);
        // Nothing pending: off starts, repeatedly.
        for _ in 0..2 {
            start(false, false)
                .await
                .expect("migration off starts when nothing changed");
        }
        // A new column is pending: off refuses and leaves the table as it was.
        for _ in 0..2 {
            refused_naming(start(false, true).await, "note");
            assert!(!note_exists().await, "migration off added a column");
        }
        start(true, true)
            .await
            .expect("migration on applies the new column");
        assert!(note_exists().await);
        start(false, true)
            .await
            .expect("migration off starts once the change is applied");
    })
    .catch_unwind()
    .await;

    gate.close().await;
    let _ = sqlx::query(&format!(
        "DROP DATABASE IF EXISTS \"{database}\" WITH (FORCE)"
    ))
    .execute(&administration)
    .await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
