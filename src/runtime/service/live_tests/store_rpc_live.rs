//! Env-gated LIVE served-path tests for the typed store RPCs and raw
//! GenericDispatch (ledger section E).
//!
//! Every test drives the real `DataBroker` handlers
//! (`document_*` / `graph_*` / `time_series_write` / `cache_*` /
//! `generic_dispatch`) on the [`dp_service_with`] harness, so the request
//! crosses `security_from_request` → `authorize` → the shared dispatch core
//! (`execute_backend_operation`: IR compile or raw-dispatch hardening) → the
//! real executor → a live backend, and asserts the user-visible outcome plus a
//! tenant-scoped read-back or a deny.
//!
//! ## Env gating
//! Every test is `#[ignore]`d; the CI "Native service live tests" step runs
//! them with `--ignored` and exports the backend DSNs. A missing DSN FAILS
//! inside that lane ([`require_live_dsn`]) — a self-skip there would make the
//! test run nowhere — and skips for a local `--ignored` run.

use std::collections::BTreeMap;

use serde_json::json;
use tonic::{Code, Request};
use uuid::Uuid;

use super::data_plane_live::{dp_live_pg_dsn, dp_pool, dp_service_with, install_dp_security};
use super::support::{live_env, require_live_dsn};
use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
use crate::proto::data_broker_server::DataBroker;
use crate::proto::{
    CacheDeleteRequest, CacheGetRequest, CacheGetResponse, CacheSetRequest, DocumentDeleteRequest,
    DocumentFindRequest, DocumentUpsertRequest, GenericDispatchRequest, StoreResource,
};
use crate::runtime::config::{BackendInstance, UdbConfig};
use crate::runtime::executor_utils::json_to_struct;
use crate::runtime::service::DataBrokerService;
use crate::runtime::system::ensure_system_catalog;

const PROJECT: &str = "default";

/// The Postgres system-catalog DSN every served harness needs.
fn store_live_pg_dsn() -> Option<String> {
    if let Some(dsn) = dp_live_pg_dsn() {
        return Some(dsn);
    }
    require_live_dsn("UDB_INTEGRATION_PG_DSN")
}

/// Caller identity as gRPC metadata. An empty `tenant` omits the tenant header.
fn store_ctx<T>(message: T, tenant: &str) -> Request<T> {
    let mut req = Request::new(message);
    let md = req.metadata_mut();
    if !tenant.is_empty() {
        md.insert("x-tenant-id", tenant.parse().unwrap());
    }
    md.insert("x-udb-project-id", PROJECT.parse().unwrap());
    md.insert("x-purpose", "admin".parse().unwrap());
    md.insert(
        "x-scopes",
        "udb:admin,udb:read,udb:write,udb:dispatch".parse().unwrap(),
    );
    req
}

/// Add a `backend` instance named `store_rpc` (never `primary`, which would
/// register it as a canonical system store) unless the env already declared one.
fn ensure_instance(config: &mut UdbConfig, backend: &str, dsn: &str, labels: &[(&str, &str)]) {
    if config
        .backend_instances
        .instances
        .iter()
        .any(|instance| instance.backend.eq_ignore_ascii_case(backend))
    {
        return;
    }
    config.backend_instances.instances.push(BackendInstance {
        name: "store_rpc".to_string(),
        backend: backend.to_string(),
        dsn: Some(dsn.to_string()),
        dsn_env: None,
        labels: labels
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect::<BTreeMap<_, _>>(),
        ..BackendInstance::default()
    });
}

/// A served broker whose runtime reaches the store backends under test.
async fn store_service(pg_dsn: &str, manifest: CatalogManifest) -> DataBrokerService {
    install_dp_security();
    let pool = dp_pool(pg_dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");
    let mongo = live_env("UDB_MONGODB_DSN");
    let graph = live_env("UDB_GRAPH_HTTP_URL");
    let graph_user = live_env("UDB_GRAPH_USER").unwrap_or_else(|| "neo4j".to_string());
    let graph_password = live_env("UDB_GRAPH_PASSWORD").unwrap_or_default();
    let cassandra = live_env("UDB_CASSANDRA_DSN");
    let redis = live_env("UDB_REDIS_DSN");
    dp_service_with(pg_dsn, manifest, move |config| {
        if let Some(dsn) = mongo.as_deref() {
            ensure_instance(config, "mongodb", dsn, &[("transport", "native")]);
        }
        if let Some(url) = graph.as_deref() {
            ensure_instance(
                config,
                "neo4j",
                url,
                &[
                    ("http_url", url),
                    ("username", graph_user.as_str()),
                    ("password", graph_password.as_str()),
                    ("dev_mode", "true"),
                ],
            );
        }
        if let Some(dsn) = cassandra.as_deref() {
            ensure_instance(config, "cassandra", dsn, &[]);
        }
        if let Some(dsn) = redis.as_deref() {
            ensure_instance(config, "redis", dsn, &[]);
        }
    })
    .await
}

fn resource(backend: &str, resource_name: &str, message_type: &str) -> Option<StoreResource> {
    Some(StoreResource {
        backend: backend.to_string(),
        resource_name: resource_name.to_string(),
        message_type: message_type.to_string(),
        ..StoreResource::default()
    })
}

fn text_col(name: &str, is_primary: bool) -> ManifestColumn {
    ManifestColumn {
        field_name: name.to_string(),
        column_name: name.to_string(),
        proto_type: "string".to_string(),
        sql_type: "TEXT".to_string(),
        is_primary,
        ..ManifestColumn::default()
    }
}

fn generic_dispatch(
    backend: &str,
    operation: &str,
    resource_name: &str,
    spec: &str,
) -> GenericDispatchRequest {
    GenericDispatchRequest {
        backend: backend.to_string(),
        operation: operation.to_string(),
        resource_name: resource_name.to_string(),
        spec_json: spec.to_string(),
        ..GenericDispatchRequest::default()
    }
}

// ── P0.6 skeleton: Document (MongoDB) ─────────────────────────────────────────

const NOTE_MESSAGE: &str = "acme.sr.v1.Note";

fn note_manifest(collection: &str) -> CatalogManifest {
    CatalogManifest {
        tables: vec![ManifestTable {
            proto_package: "acme.sr.v1".to_string(),
            message_name: "Note".to_string(),
            schema: "udb".to_string(),
            table: collection.to_string(),
            primary_key: vec!["id".to_string()],
            columns: vec![text_col("id", true), text_col("body", false)],
            ..ManifestTable::default()
        }],
        ..CatalogManifest::default()
    }
}

async fn find_notes(
    svc: &DataBrokerService,
    collection: &str,
    tenant: &str,
    filter: serde_json::Value,
) -> usize {
    svc.document_find(store_ctx(
        DocumentFindRequest {
            resource: resource("mongodb", collection, NOTE_MESSAGE),
            filter: json_to_struct(&filter),
            limit: 50,
            ..DocumentFindRequest::default()
        },
        tenant,
    ))
    .await
    .expect("served DocumentFind")
    .into_inner()
    .documents
    .len()
}

/// E1: a typed document write is tenant-scoped through the IR: tenant B's
/// DocumentFind on the same entity sees none of tenant A's documents.
#[tokio::test]
#[ignore = "requires live Postgres + MongoDB (UDB_MONGODB_DSN); runs in the CI --ignored live step"]
async fn document_rpcs_are_tenant_scoped_live() {
    let (Some(pg), Some(_)) = (store_live_pg_dsn(), require_live_dsn("UDB_MONGODB_DSN")) else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let collection = format!("sr_notes_{}", Uuid::new_v4().simple());
    let svc = store_service(&pg, note_manifest(&collection)).await;
    let (tenant_a, tenant_b) = (Uuid::new_v4().to_string(), Uuid::new_v4().to_string());
    let doc_id = format!("note-{}", Uuid::new_v4().simple());

    svc.document_upsert(store_ctx(
        DocumentUpsertRequest {
            resource: resource("mongodb", &collection, NOTE_MESSAGE),
            document_id: doc_id.clone(),
            document: json_to_struct(&json!({ "body": "tenant-a secret" })),
            ..DocumentUpsertRequest::default()
        },
        &tenant_a,
    ))
    .await
    .expect("served DocumentUpsert (tenant A)");

    assert_eq!(
        find_notes(&svc, &collection, &tenant_a, json!({})).await,
        1,
        "tenant A reads back its own document"
    );
    assert_eq!(
        find_notes(&svc, &collection, &tenant_b, json!({})).await,
        0,
        "tenant B must not see tenant A's document"
    );
    assert_eq!(
        find_notes(&svc, &collection, &tenant_b, json!({ "id": doc_id })).await,
        0,
        "tenant B must not reach tenant A's document by id"
    );

    let _ = svc
        .document_delete(store_ctx(
            DocumentDeleteRequest {
                resource: resource("mongodb", &collection, NOTE_MESSAGE),
                document_id: doc_id,
                ..DocumentDeleteRequest::default()
            },
            &tenant_a,
        ))
        .await;
}

/// E6: a served typed store RPC without a tenant is refused before any
/// backend write (the data-plane gate requires a tenant; the non-SQL IR
/// compilers additionally fail closed with `tenant_scope_required`, unit-tested
/// per compiler).
#[tokio::test]
#[ignore = "requires live Postgres + MongoDB (UDB_MONGODB_DSN); runs in the CI --ignored live step"]
async fn typed_store_rpc_without_tenant_is_refused_live() {
    let (Some(pg), Some(_)) = (store_live_pg_dsn(), require_live_dsn("UDB_MONGODB_DSN")) else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let collection = format!("sr_notes_{}", Uuid::new_v4().simple());
    let svc = store_service(&pg, note_manifest(&collection)).await;
    let doc_id = format!("note-{}", Uuid::new_v4().simple());
    let err = svc
        .document_upsert(store_ctx(
            DocumentUpsertRequest {
                resource: resource("mongodb", &collection, NOTE_MESSAGE),
                document_id: doc_id.clone(),
                document: json_to_struct(&json!({ "body": "no tenant" })),
                ..DocumentUpsertRequest::default()
            },
            "",
        ))
        .await
        .expect_err("a tenant-less typed write must be refused");
    assert_eq!(err.code(), Code::Unauthenticated, "{err:?}");
    let probe_tenant = Uuid::new_v4().to_string();
    assert_eq!(
        find_notes(&svc, &collection, &probe_tenant, json!({ "id": doc_id })).await,
        0,
        "nothing was written"
    );
    let cache_err = svc
        .cache_set(store_ctx(
            CacheSetRequest {
                resource: resource("redis", "", "sr_cache"),
                key: "k".to_string(),
                value: b"v".to_vec(),
                ..CacheSetRequest::default()
            },
            "",
        ))
        .await
        .expect_err("a tenant-less raw cache write must be refused");
    assert_eq!(cache_err.code(), Code::Unauthenticated, "{cache_err:?}");
}

// ── Graph (Neo4j) ─────────────────────────────────────────────────────────────

#[cfg(feature = "neo4j")]
fn graph_executor() -> crate::runtime::executors::neo4j::Neo4jExecutor {
    crate::runtime::executors::neo4j::Neo4jExecutor::from_env()
        .expect("UDB_GRAPH_HTTP_URL configures the read-back Neo4j executor")
}

#[cfg(feature = "neo4j")]
async fn label_count(label: &str, filter: &str) -> u64 {
    let rows = graph_executor()
        .cypher_rows(
            &format!("MATCH (n:{label}) {filter} RETURN count(n) AS c"),
            json!({}),
        )
        .await
        .expect("read back node count");
    rows.first()
        .and_then(|row| row.get("c"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
}

#[cfg(feature = "neo4j")]
async fn drop_label(label: &str) {
    let _ = graph_executor()
        .cypher_rows(&format!("MATCH (n:{label}) DETACH DELETE n"), json!({}))
        .await;
}

#[cfg(feature = "neo4j")]
async fn graph_mutate(
    svc: &DataBrokerService,
    tenant: &str,
    cypher: &str,
    parameters: serde_json::Value,
) -> Result<i64, tonic::Status> {
    svc.graph_mutate(store_ctx(
        crate::proto::GraphMutationRequest {
            resource: resource("neo4j", "", "sr_graph"),
            query: cypher.to_string(),
            parameters: json_to_struct(&parameters),
            ..crate::proto::GraphMutationRequest::default()
        },
        tenant,
    ))
    .await
    .map(|response| response.into_inner().affected_rows)
}

/// E2: GraphQuery is read-only — a served query carrying CREATE is refused
/// and the graph is unchanged.
#[cfg(feature = "neo4j")]
#[tokio::test]
#[ignore = "requires live Postgres + Neo4j (UDB_GRAPH_HTTP_URL); runs in the CI --ignored live step"]
async fn graph_query_refuses_writes_live() {
    let (Some(pg), Some(_)) = (store_live_pg_dsn(), require_live_dsn("UDB_GRAPH_HTTP_URL")) else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let svc = store_service(&pg, CatalogManifest::default()).await;
    let label = format!("SrQuery{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    let before = label_count(&label, "").await;
    let err = svc
        .graph_query(store_ctx(
            crate::proto::GraphQueryRequest {
                resource: resource("neo4j", "", "sr_graph"),
                query: format!("CREATE (n:{label} {{id: 'x'}}) RETURN n"),
                ..crate::proto::GraphQueryRequest::default()
            },
            &tenant,
        ))
        .await
        .expect_err("a write through GraphQuery must be refused");
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
    assert_eq!(label_count(&label, "").await, before, "no node was created");
    drop_label(&label).await;
}

/// E3: GraphMutate reports the server's counts — a SET that matches nothing
/// is 0, two CREATEs are 2, a SET without RETURN on a match is not 0.
#[cfg(feature = "neo4j")]
#[tokio::test]
#[ignore = "requires live Postgres + Neo4j (UDB_GRAPH_HTTP_URL); runs in the CI --ignored live step"]
async fn graph_mutate_reports_server_counts_live() {
    let (Some(pg), Some(_)) = (store_live_pg_dsn(), require_live_dsn("UDB_GRAPH_HTTP_URL")) else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let svc = store_service(&pg, CatalogManifest::default()).await;
    let label = format!("SrCount{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();

    let missing = graph_mutate(
        &svc,
        &tenant,
        &format!("MATCH (n:{label} {{id: $id}}) SET n.touched = true"),
        json!({ "id": "missing" }),
    )
    .await
    .expect("served GraphMutate on a missing id");
    assert_eq!(missing, 0, "a SET that matched nothing affected nothing");

    let created = graph_mutate(
        &svc,
        &tenant,
        &format!("CREATE (:{label} {{id: 'a'}}), (:{label} {{id: 'b'}})"),
        json!({}),
    )
    .await
    .expect("served GraphMutate with two CREATEs");
    assert_eq!(created, 2, "two nodes created");
    assert_eq!(label_count(&label, "").await, 2);

    let set_only = graph_mutate(
        &svc,
        &tenant,
        &format!("MATCH (n:{label} {{id: 'a'}}) SET n.touched = true"),
        json!({}),
    )
    .await
    .expect("served GraphMutate SET without RETURN");
    assert!(set_only >= 1, "a SET that changed a node must not report 0");
    drop_label(&label).await;
}

/// E8: graph uniqueness is tenant-composite — the same id under two tenants
/// both land, a duplicate inside one tenant violates the constraint.
#[cfg(feature = "neo4j")]
#[tokio::test]
#[ignore = "requires live Postgres + Neo4j (UDB_GRAPH_HTTP_URL); runs in the CI --ignored live step"]
async fn graph_uniqueness_is_tenant_composite_live() {
    let (Some(pg), Some(_)) = (store_live_pg_dsn(), require_live_dsn("UDB_GRAPH_HTTP_URL")) else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let svc = store_service(&pg, CatalogManifest::default()).await;
    let label = format!("SrUniq{}", Uuid::new_v4().simple());
    let (tenant_a, tenant_b) = (Uuid::new_v4().to_string(), Uuid::new_v4().to_string());

    svc.generic_dispatch(store_ctx(
        generic_dispatch("neo4j", "ensure_resource", &label, "{}"),
        &tenant_a,
    ))
    .await
    .expect("served ensure_resource creates the tenant-composite constraint");

    for tenant in [&tenant_a, &tenant_b] {
        let response = svc
            .generic_dispatch(store_ctx(
                generic_dispatch(
                    "neo4j",
                    "mutate",
                    "",
                    &json!({
                        "operation": "create_node",
                        "label": label,
                        "id": "shared-id",
                        "properties": { "name": "n" },
                        // A forged scope is replaced by the verified one.
                        "scope": { "_tenant_id": "forged" },
                    })
                    .to_string(),
                ),
                tenant,
            ))
            .await
            .unwrap_or_else(|err| panic!("served create_node for tenant {tenant}: {err:?}"))
            .into_inner();
        let result: serde_json::Value =
            serde_json::from_str(&response.result_json).expect("create_node result JSON");
        assert_eq!(result["affected_rows"], 1, "{result}");
    }
    assert_eq!(
        label_count(&label, "WHERE n.id = 'shared-id'").await,
        2,
        "the same id lands once per tenant"
    );
    assert_eq!(
        label_count(&label, "WHERE n._tenant_id = 'forged'").await,
        0,
        "a caller-chosen scope is never honoured"
    );

    let duplicate = graph_mutate(
        &svc,
        &tenant_a,
        &format!(
            "CREATE (:{label} {{id: 'shared-id', _tenant_id: $tenant, _project_id: $project}})"
        ),
        json!({ "tenant": tenant_a, "project": PROJECT }),
    )
    .await;
    assert!(
        duplicate.is_err(),
        "a duplicate (id, tenant, project) must violate the composite constraint: {duplicate:?}"
    );
    let _ = svc
        .generic_dispatch(store_ctx(
            generic_dispatch("neo4j", "drop_resource", &label, "{}"),
            &tenant_a,
        ))
        .await;
    drop_label(&label).await;
}

// ── Time series / raw dispatch (Cassandra) ────────────────────────────────────

#[cfg(feature = "cassandra")]
async fn cassandra_executor(dsn: &str) -> crate::runtime::executors::cassandra::CassandraExecutor {
    let client = crate::runtime::executors::cassandra::CassandraClient::connect(dsn)
        .await
        .expect("connect read-back Cassandra session");
    crate::runtime::executors::cassandra::CassandraExecutor::new(client)
}

/// Test-side DDL through the executor's compiler-mediated path (the broker
/// never forwards that marker from a caller; see E4).
#[cfg(feature = "cassandra")]
async fn cassandra_ddl(
    executor: &crate::runtime::executors::cassandra::CassandraExecutor,
    cql: &str,
) {
    use crate::runtime::executors::MutationExecutor as _;
    executor
        .mutate(&json!({ "sql": cql, "compiler_mediated": true }).to_string())
        .await
        .unwrap_or_else(|err| panic!("cassandra DDL `{cql}`: {err:?}"));
}

#[cfg(feature = "cassandra")]
async fn cassandra_rows(
    executor: &crate::runtime::executors::cassandra::CassandraExecutor,
    cql: &str,
    params: serde_json::Value,
) -> Vec<serde_json::Value> {
    use crate::runtime::executors::QueryExecutor as _;
    let out = executor
        .query(&json!({ "sql": cql, "params": params }).to_string())
        .await
        .unwrap_or_else(|err| panic!("cassandra read-back `{cql}`: {err:?}"));
    serde_json::from_str::<Vec<serde_json::Value>>(&out).expect("cassandra rows JSON")
}

/// E7: a typed TimeSeriesWrite of 3 points to a raw `{table, rows}` table
/// reports 3 and lands under the caller's tenant (scoped read-back = 3, the
/// other tenant = 0), with the broker-stamped tenant overriding a forged one.
#[cfg(feature = "cassandra")]
#[tokio::test]
#[ignore = "requires live Postgres + Cassandra (UDB_CASSANDRA_DSN); runs in the CI --ignored live step"]
async fn time_series_write_counts_and_scopes_rows_live() {
    let (Some(pg), Some(cassandra)) = (store_live_pg_dsn(), require_live_dsn("UDB_CASSANDRA_DSN"))
    else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let executor = cassandra_executor(&cassandra).await;
    let keyspace = format!("udb_sr_{}", Uuid::new_v4().simple());
    cassandra_ddl(
        &executor,
        &format!(
            "CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = \
             {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        ),
    )
    .await;
    cassandra_ddl(
        &executor,
        &format!(
            "CREATE TABLE IF NOT EXISTS {keyspace}.ts (tenant_id text, project_id text, \
             host text, value double, PRIMARY KEY ((tenant_id), host))"
        ),
    )
    .await;
    let svc = store_service(&pg, CatalogManifest::default()).await;
    let (tenant_a, tenant_b) = (Uuid::new_v4().to_string(), Uuid::new_v4().to_string());
    let points = ["h1", "h2", "h3"]
        .iter()
        .enumerate()
        .map(|(index, host)| crate::proto::TimeSeriesPoint {
            tags: [
                ("host".to_string(), (*host).to_string()),
                // Forged: the broker stamps the verified tenant over it.
                ("tenant_id".to_string(), tenant_b.clone()),
            ]
            .into_iter()
            .collect(),
            values: [("value".to_string(), index as f64 + 0.5)]
                .into_iter()
                .collect(),
            ..crate::proto::TimeSeriesPoint::default()
        })
        .collect::<Vec<_>>();
    let table = format!("{keyspace}.ts");
    let written = svc
        .time_series_write(store_ctx(
            crate::proto::TimeSeriesWriteRequest {
                resource: resource("cassandra", &table, ""),
                points,
                ..crate::proto::TimeSeriesWriteRequest::default()
            },
            &tenant_a,
        ))
        .await
        .expect("served TimeSeriesWrite")
        .into_inner();
    assert_eq!(written.affected_rows, 3, "three points written");

    let select = format!("SELECT host, project_id FROM {keyspace}.ts WHERE tenant_id = ?");
    let rows_a = cassandra_rows(&executor, &select, json!([tenant_a])).await;
    assert_eq!(
        rows_a.len(),
        3,
        "scoped read-back under tenant A: {rows_a:?}"
    );
    assert!(
        rows_a.iter().all(|row| row["project_id"] == PROJECT),
        "project stamped: {rows_a:?}"
    );
    assert!(
        cassandra_rows(&executor, &select, json!([tenant_b]))
            .await
            .is_empty(),
        "nothing landed under the forged tenant"
    );

    cassandra_ddl(&executor, &format!("DROP KEYSPACE IF EXISTS {keyspace}")).await;
}

/// E4: a served GenericDispatch cannot assert `compiler_mediated`, not even
/// in its JSON-escaped spelling — the DDL it would unlock is refused and no
/// table appears.
#[cfg(feature = "cassandra")]
#[tokio::test]
#[ignore = "requires live Postgres + Cassandra (UDB_CASSANDRA_DSN); runs in the CI --ignored live step"]
async fn generic_dispatch_escaped_compiler_mediated_is_rejected_live() {
    let (Some(pg), Some(cassandra)) = (store_live_pg_dsn(), require_live_dsn("UDB_CASSANDRA_DSN"))
    else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let executor = cassandra_executor(&cassandra).await;
    let keyspace = format!("udb_sr_{}", Uuid::new_v4().simple());
    cassandra_ddl(
        &executor,
        &format!(
            "CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = \
             {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        ),
    )
    .await;
    let svc = store_service(&pg, CatalogManifest::default()).await;
    let tenant = Uuid::new_v4().to_string();
    // `compiler` + JSON escape of `_` + `mediated`: the raw text never contains
    // the plain marker, but serde decodes it to the real key.
    let escaped_key = format!("compiler{}u005fmediated", '\\');
    let spec = format!(
        r#"{{"sql":"CREATE TABLE IF NOT EXISTS {keyspace}.evil (id text PRIMARY KEY)","{escaped_key}":true}}"#
    );
    assert!(!spec.contains("compiler_mediated"));
    let err = svc
        .generic_dispatch(store_ctx(
            generic_dispatch("cassandra", "mutate", "", &spec),
            &tenant,
        ))
        .await
        .expect_err("caller-asserted compiler_mediated must not unlock DDL");
    assert_eq!(err.code(), Code::InvalidArgument, "{err:?}");
    let tables = cassandra_rows(
        &executor,
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
        json!([keyspace]),
    )
    .await;
    assert!(tables.is_empty(), "no table was created: {tables:?}");
    cassandra_ddl(&executor, &format!("DROP KEYSPACE IF EXISTS {keyspace}")).await;
}

// ── Raw KV / object dispatch (Redis / MinIO) ─────────────────────────────────

async fn cache_get(svc: &DataBrokerService, tenant: &str, key: &str) -> CacheGetResponse {
    svc.cache_get(store_ctx(
        CacheGetRequest {
            resource: resource("redis", "", "sr_cache"),
            key: key.to_string(),
            ..CacheGetRequest::default()
        },
        tenant,
    ))
    .await
    .expect("served CacheGet")
    .into_inner()
}

/// E5: raw cache dispatch keys are forced under the caller's tenant
/// namespace — tenant B reads neither A's key nor A's namespaced key.
#[tokio::test]
#[ignore = "requires live Postgres + Redis (UDB_REDIS_DSN); runs in the CI --ignored live step"]
async fn raw_cache_keys_are_tenant_namespaced_live() {
    let (Some(pg), Some(_)) = (store_live_pg_dsn(), require_live_dsn("UDB_REDIS_DSN")) else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let svc = store_service(&pg, CatalogManifest::default()).await;
    let (tenant_a, tenant_b) = (Uuid::new_v4().to_string(), Uuid::new_v4().to_string());
    let key = format!("sr:{}", Uuid::new_v4().simple());
    svc.cache_set(store_ctx(
        CacheSetRequest {
            resource: resource("redis", "", "sr_cache"),
            key: key.clone(),
            value: b"tenant-a".to_vec(),
            ttl_seconds: 300,
            ..CacheSetRequest::default()
        },
        &tenant_a,
    ))
    .await
    .expect("served CacheSet (tenant A)");

    let own = cache_get(&svc, &tenant_a, &key).await;
    assert!(own.found, "tenant A reads back its own key");
    assert_eq!(own.value, b"tenant-a".to_vec());
    assert!(
        !cache_get(&svc, &tenant_b, &key).await.found,
        "tenant B must not read A's key"
    );
    let namespaced = format!("udb:{PROJECT}:{tenant_a}:{key}");
    assert!(
        !cache_get(&svc, &tenant_b, &namespaced).await.found,
        "naming A's namespace is nested under B's, never honoured"
    );
    let _ = svc
        .cache_delete(store_ctx(
            CacheDeleteRequest {
                resource: resource("redis", "", "sr_cache"),
                key,
                ..CacheDeleteRequest::default()
            },
            &tenant_a,
        ))
        .await;
}

/// E5: raw object dispatch keys are forced under `__udb_t/<tenant>/` —
/// tenant B cannot read A's object, by its key or by A's prefixed key.
#[tokio::test]
#[ignore = "requires live Postgres + MinIO (UDB_MINIO_ENDPOINT); runs in the CI --ignored live step"]
async fn raw_object_keys_are_tenant_prefixed_live() {
    let (Some(pg), Some(_)) = (store_live_pg_dsn(), require_live_dsn("UDB_MINIO_ENDPOINT")) else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    let svc = store_service(&pg, CatalogManifest::default()).await;
    let (tenant_a, tenant_b) = (Uuid::new_v4().to_string(), Uuid::new_v4().to_string());
    let key = format!("sr/{}.txt", Uuid::new_v4().simple());
    let object_spec = |key: &str| json!({ "bucket": "udb-storage", "key": key }).to_string();
    svc.generic_dispatch(store_ctx(
        generic_dispatch(
            "minio",
            "put_object",
            "",
            &json!({
                "bucket": "udb-storage",
                "key": key,
                "data_text": "tenant-a object",
                "content_type": "text/plain",
            })
            .to_string(),
        ),
        &tenant_a,
    ))
    .await
    .expect("served raw put_object (tenant A)");

    let own = svc
        .generic_dispatch(store_ctx(
            generic_dispatch("minio", "get_object", "", &object_spec(&key)),
            &tenant_a,
        ))
        .await
        .expect("tenant A reads back its own object")
        .into_inner();
    let own: serde_json::Value = serde_json::from_str(&own.result_json).unwrap();
    assert_eq!(own["bytes"], "tenant-a object".len(), "{own}");

    for probe in [key.clone(), format!("__udb_t/{tenant_a}/{key}")] {
        let read = svc
            .generic_dispatch(store_ctx(
                generic_dispatch("minio", "get_object", "", &object_spec(&probe)),
                &tenant_b,
            ))
            .await;
        assert!(
            read.is_err(),
            "tenant B must not read tenant A's object via key {probe}: {read:?}"
        );
    }
    let _ = svc
        .generic_dispatch(store_ctx(
            generic_dispatch("minio", "delete_object", "", &object_spec(&key)),
            &tenant_a,
        ))
        .await;
}
