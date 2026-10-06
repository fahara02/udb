//! LIVE hard-purge graph erasure on a real Neo4j + Postgres.
//!
//! Tenants A and B both hold nodes with the SAME ids and a relationship each.
//! The purge's graph leg for A must delete A's nodes and relationships and
//! leave B's intact, reporting the counts under `purged`.
//!
//! Run with a live Neo4j + Postgres:
//!   UDB_GRAPH_HTTP_URL=http://127.0.0.1:7474 cargo test --lib tenant_graph_purge_ \
//!     -- --ignored --nocapture

use std::collections::BTreeMap;

use serde_json::json;
use uuid::Uuid;

use crate::generation::CatalogManifest;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::config::{BackendInstance, UdbConfig};
use crate::runtime::executors::neo4j::Neo4jExecutor;
use crate::runtime::service::DataBrokerService;
use crate::runtime::service::live_tests::support::{
    export_live_env, live_env, live_native_service_db_lock, live_pg_dsn, require_live_dsn,
};

const PROJECT: &str = "default";
const INSTANCE: &str = "graph_purge";

fn graph_executor() -> Neo4jExecutor {
    export_live_env(&["UDB_GRAPH_HTTP_URL", "UDB_GRAPH_USER", "UDB_GRAPH_PASSWORD"]);
    Neo4jExecutor::from_env().expect("UDB_GRAPH_HTTP_URL configures the seeding Neo4j executor")
}

/// `(nodes, relationships)` of `tenant` under `label`.
async fn tenant_graph_counts(label: &str, tenant: &str) -> (u64, u64) {
    let executor = graph_executor();
    let count = |rows: Vec<serde_json::Value>| {
        rows.first()
            .and_then(|row| row.get("c"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
    };
    let nodes = executor
        .cypher_rows(
            &format!("MATCH (n:{label}) WHERE n._tenant_id = $tenant RETURN count(n) AS c"),
            json!({ "tenant": tenant }),
        )
        .await
        .expect("count nodes");
    let rels = executor
        .cypher_rows(
            &format!(
                "MATCH (:{label})-[r]->(:{label}) WHERE r._tenant_id = $tenant RETURN count(r) AS c"
            ),
            json!({ "tenant": tenant }),
        )
        .await
        .expect("count relationships");
    (count(nodes), count(rels))
}

#[tokio::test]
#[ignore = "requires live Neo4j+Postgres (UDB_GRAPH_HTTP_URL); runs in the CI live lane (-- --ignored)"]
async fn tenant_graph_purge_erases_only_the_tenants_graph_live() {
    let Some(url) = require_live_dsn("UDB_GRAPH_HTTP_URL") else {
        return;
    };
    let _guard = live_native_service_db_lock().lock().await;
    let user = live_env("UDB_GRAPH_USER").unwrap_or_else(|| "neo4j".to_string());
    let password = live_env("UDB_GRAPH_PASSWORD").unwrap_or_default();

    let label = format!("SrPurge{}", Uuid::new_v4().simple());
    let (tenant_a, tenant_b) = (Uuid::new_v4().to_string(), Uuid::new_v4().to_string());
    let seed = graph_executor();
    for tenant in [&tenant_a, &tenant_b] {
        seed.cypher_rows(
            &format!(
                "CREATE (a:{label} {{id: 'a', _tenant_id: $t, _project_id: $p}})-[:RELATED {{id: 'e1', kind: 'peer', _tenant_id: $t, _project_id: $p}}]->(b:{label} {{id: 'b', _tenant_id: $t, _project_id: $p}})"
            ),
            json!({ "t": tenant, "p": PROJECT }),
        )
        .await
        .expect("seed tenant graph");
        assert_eq!(tenant_graph_counts(&label, tenant).await, (2, 1));
    }

    let mut config = UdbConfig::from_env();
    config.primary.direct_dsn = live_pg_dsn();
    config.backend_instances.instances.push(BackendInstance {
        name: INSTANCE.to_string(),
        backend: "neo4j".to_string(),
        dsn: Some(url.clone()),
        dsn_env: None,
        labels: [
            ("http_url", url.as_str()),
            ("username", user.as_str()),
            ("password", password.as_str()),
            ("dev_mode", "true"),
        ]
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect::<BTreeMap<_, _>>(),
        ..BackendInstance::default()
    });
    let runtime = DataBrokerRuntime::from_config(config).await;
    let broker = DataBrokerService::with_runtime(CatalogManifest::default(), runtime);
    let tenant_svc = broker.build_tenant_service();
    let report = super::tenant_purge::purge_tenant_graph_stores(
        &tenant_svc,
        &CatalogManifest::default(),
        &tenant_a,
    )
    .await;

    let graph_entries: Vec<_> = report
        .purged
        .iter()
        .filter(|entry| entry["schema"] == "graph:neo4j")
        .collect();
    assert!(
        graph_entries.iter().any(|entry| entry["table"] == INSTANCE),
        "the configured instance is purged: purged={:?} excluded={:?}",
        report.purged,
        report.excluded
    );
    assert!(
        !report
            .excluded
            .iter()
            .any(|entry| entry["schema"] == "graph:neo4j" && entry["table"] == INSTANCE),
        "the configured instance must not fail: {:?}",
        report.excluded
    );
    let sum = |key: &str| -> u64 {
        graph_entries
            .iter()
            .filter_map(|entry| entry[key].as_u64())
            .sum()
    };
    assert_eq!(sum("nodes_deleted"), 2, "{graph_entries:?}");
    assert_eq!(sum("relationships_deleted"), 1, "{graph_entries:?}");
    assert_eq!(report.deleted_total(), 3);

    assert_eq!(
        tenant_graph_counts(&label, &tenant_a).await,
        (0, 0),
        "tenant A's graph is erased"
    );
    assert_eq!(
        tenant_graph_counts(&label, &tenant_b).await,
        (2, 1),
        "tenant B's graph (same ids) survives A's purge"
    );

    let _ = seed
        .cypher_rows(&format!("MATCH (n:{label}) DETACH DELETE n"), json!({}))
        .await;
}
