//! One physical table per project, proven on a live Postgres.
//!
//! Physical tables carry no project prefix and, under the default Permissive
//! routing, the unlabeled primary serves every project. Two projects that
//! declare the same table would silently share its rows. Catalog staging and
//! activation must refuse the second project, naming the table and the project
//! that already holds it, unless both declarations mark the table
//! `cross_project_shared`.

use std::sync::Arc;

use tonic::Code;
use uuid::Uuid;

use super::support::*;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::config::UdbConfig;

async fn permissive_runtime() -> Arc<DataBrokerRuntime> {
    let mut config = UdbConfig::from_env();
    config.primary.direct_dsn = live_pg_dsn();
    config.project_routing_mode = "permissive".to_string();
    Arc::new(DataBrokerRuntime::from_config(config).await)
}

/// A project catalog (native tables merged, as `serve` and the CLI build it)
/// declaring `schema.table`, tenant-scoped but NOT project-scoped.
fn catalog_bytes(schema: &str, table: &str, shared: bool) -> Vec<u8> {
    let shared_option = if shared {
        " cross_project_shared: true"
    } else {
        ""
    };
    let source = format!(
        r#"syntax = "proto3";
package acme.claims.v1;
message Order {{
  option (udb.core.common.v1.pg_table) = {{
    table_name: "{table}" schema_name: "{schema}" is_table: true audit_fields: false{shared_option}
  }};
  option (udb.core.common.v1.db_table_security) = {{
    tenant_isolation_mode: "row" tenant_column: "tenant_id"
    soft_delete_mode: "none" audit_mode: AUDIT_MODE_NONE
    encryption_profile: "none" pii_profile: "none" export_eligible: false
  }};
  string id = 1 [(udb.core.common.v1.pg_column) = {{sql_type:"TEXT" primary_key:true not_null:true}}];
  string tenant_id = 2 [(udb.core.common.v1.pg_column) = {{sql_type:"TEXT" tenant_column:true not_null:true}}];
  string status = 3 [(udb.core.common.v1.pg_column) = {{sql_type:"TEXT"}}];
}}
"#
    );
    let report = crate::parser::parse_proto_source(
        source.as_bytes(),
        "project-relation-claims-live.proto",
        &crate::parser::ParserConfig::default(),
    )
    .expect("the fixture parses");
    let manifest = crate::generation::CatalogManifest::from_schemas(&report.schemas)
        .expect("the fixture builds a manifest");
    assert_eq!(
        manifest
            .tables
            .iter()
            .find(|candidate| candidate.table == table)
            .map(|candidate| candidate.cross_project_shared),
        Some(shared),
        "pg_table.cross_project_shared reaches the manifest"
    );
    let (manifest, _) = crate::runtime::native_catalog::merge_native(&manifest, &report.schemas);
    serde_json::to_vec(&manifest).expect("serialize manifest")
}

async fn register(
    runtime: &DataBrokerRuntime,
    project: &str,
    bytes: &[u8],
) -> Result<String, tonic::Status> {
    let (catalog_id, _, _) = runtime
        .stage_project_catalog(project, bytes, "relation claim live test", "live-test")
        .await?;
    runtime
        .activate_project_catalog(
            project,
            &catalog_id,
            "relation claim live test",
            "live-test",
        )
        .await?;
    Ok(catalog_id)
}

#[tokio::test]
#[ignore = "live: requires UDB_LIVE_NATIVE_PG_DSN"]
async fn live_second_project_declaring_the_same_table_is_refused_with_the_reason() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let runtime = permissive_runtime().await;
    let schema = format!("udb_claims_{}", Uuid::new_v4().simple());
    let alpha = format!("claims-alpha-{}", Uuid::new_v4().simple());
    let beta = format!("claims-beta-{}", Uuid::new_v4().simple());

    // The first project claims shop.orders on the shared primary.
    register(&runtime, &alpha, &catalog_bytes(&schema, "orders", false))
        .await
        .expect("the first project registers its table");

    // Re-staging the same catalog for the same project is not a collision.
    runtime
        .stage_project_catalog(
            &alpha,
            &catalog_bytes(&schema, "orders", false),
            "relation claim live test: re-apply",
            "live-test",
        )
        .await
        .expect("the owning project may re-apply its own table");

    // A second project declaring the same table is refused, by name.
    let refused = register(&runtime, &beta, &catalog_bytes(&schema, "orders", false))
        .await
        .expect_err("a second project must not silently share the table");
    assert_eq!(refused.code(), Code::FailedPrecondition, "{refused:?}");
    let message = refused.message();
    for needle in [
        format!("{schema}.orders"),
        format!("project '{alpha}'"),
        "cross_project_shared".to_string(),
        "project_isolation_mode".to_string(),
    ] {
        assert!(
            message.contains(&needle),
            "refusal must name `{needle}`: {message}"
        );
    }
    assert!(
        runtime
            .active_project_catalog_summary(&beta)
            .await
            .expect("read beta's catalog")
            .is_none(),
        "the refused project has no ACTIVE catalog"
    );

    // Marking the table shared on one side only is still refused.
    let one_sided = register(&runtime, &beta, &catalog_bytes(&schema, "orders", true))
        .await
        .expect_err("sharing must be declared by both projects");
    assert_eq!(one_sided.code(), Code::FailedPrecondition);
    assert!(
        one_sided
            .message()
            .contains("marks it cross_project_shared but project"),
        "{}",
        one_sided.message()
    );

    // A table both projects mark cross_project_shared may be shared.
    let gamma = format!("claims-gamma-{}", Uuid::new_v4().simple());
    let delta = format!("claims-delta-{}", Uuid::new_v4().simple());
    register(&runtime, &gamma, &catalog_bytes(&schema, "rates", true))
        .await
        .expect("the first project registers a shared table");
    register(&runtime, &delta, &catalog_bytes(&schema, "rates", true))
        .await
        .expect("a table both projects mark cross_project_shared is allowed");

    // Startup and `udb doctor` report the unlabeled primary serving several
    // projects under Permissive routing.
    let findings = runtime
        .project_storage_isolation_findings()
        .await
        .expect("storage isolation findings");
    assert!(
        findings
            .iter()
            .any(|finding| finding.contains("has no project label")),
        "{findings:?}"
    );
}
