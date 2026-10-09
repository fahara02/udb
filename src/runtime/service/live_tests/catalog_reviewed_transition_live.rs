//! Real PostgreSQL + served, credential-resolved reviewed catalog transitions.
//! The project owns a database; capacity controls also use it as the primary.
//! Review RPCs use signed operator bearers
//! backed by real ACTIVE native PERSON accounts; header scopes cannot approve.

use super::support::{live_native_service_db_lock, require_live_dsn_any};
use crate::engine::FsmState;
use crate::generation::{CatalogManifest, SqlGenerationConfig, generate_bootstrap_sql};
use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::parser::{ParserConfig, parse_proto_source};
use crate::proto::data_broker_client::DataBrokerClient;
use crate::proto::data_broker_server::DataBrokerServer;
use crate::proto::udb::core::authn::entity::v1 as authn_entity;
use crate::proto::udb::core::authn::services::v1 as authn;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::proto::{
    CatalogManifestRequest, CatalogVersionRequest, CatalogVersionResponse, MigrationApplyRequest,
    MigrationPlanRequest, MigrationPlanResponse, MigrationRunRequest, MigrationStatusResponse,
    StageCatalogRequest,
};
use crate::runtime::DataBrokerRuntime;
use crate::runtime::authn::{
    AuthnConfig, PostgresApiKeyStore, PostgresSessionStore, PostgresUserStore,
};
use crate::runtime::config::{BackendInstance, BackendInstanceConfig, UdbConfig};
use crate::runtime::credential_layer::CredentialResolveLayer;
use crate::runtime::native_catalog;
use crate::runtime::security::{SecurityConfig, sign_access_token};
use crate::runtime::service::DataBrokerService;
use crate::runtime::service::auth_service::AuthnServiceImpl;
use crate::runtime::service::method_security::{scope_claim_context_for_test, test_claim_context};
use crate::runtime::system::SystemCatalogConfig;
use futures::FutureExt;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tonic::{Code, Request, Status};
use uuid::Uuid;

struct SecurityRestore(SecurityConfig);
impl Drop for SecurityRestore {
    fn drop(&mut self) {
        SecurityConfig::install_global(self.0.clone());
    }
}

struct Serving {
    client: DataBrokerClient<tonic::transport::Channel>,
    service: DataBrokerService,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}
impl Drop for Serving {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
impl Serving {
    async fn stop(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(mut task) = self.task.take() {
            match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
                Ok(result) => result.expect("owned catalog listener failed"),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    panic!("owned catalog listener did not stop");
                }
            }
        }
    }
}

fn request<T>(message: T, bearer: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.set_timeout(Duration::from_secs(30));
    let mut authorization = format!("Bearer {bearer}")
        .parse::<tonic::metadata::MetadataValue<_>>()
        .unwrap();
    authorization.set_sensitive(true);
    request
        .metadata_mut()
        .insert("authorization", authorization);
    request
        .metadata_mut()
        .insert("x-purpose", "catalog.reviewed.ci".parse().unwrap());
    request
        .metadata_mut()
        .insert("x-request-id", Uuid::new_v4().to_string().parse().unwrap());
    request
}

fn project_dsn(base: &str, database: &str) -> String {
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
        .max_connections(6)
        .acquire_timeout(Duration::from_secs(10))
        .connect(dsn)
        .await
        .expect("connect private catalog fixture Postgres")
}

fn source(schema: &str) -> String {
    format!(
        r#"syntax = "proto3";
package reviewed.catalog.live.v1;
message Receipt {{
  option (udb.core.common.v1.pg_table) = {{
    table_name: "records" schema_name: "{schema}" is_table: true
    audit_fields: false enable_rls: true force_rls: true
    indexes: {{ index_name: "idx_records_before" index_type: "BTREE" columns: "lookup_key" }}
  }};
  option (udb.core.common.v1.db_table_security) = {{
    tenant_isolation_mode: "row" tenant_column: "tenant_id"
    project_isolation_mode: "column" project_column: "project_id"
    retention_class: "reviewed.owned" soft_delete_mode: "none"
    audit_mode: AUDIT_MODE_NONE encryption_profile: "none" pii_profile: "none"
    export_eligible: false
  }};
  string record_id = 1 [(udb.core.common.v1.pg_column) = {{sql_type:"VARCHAR(80)" primary_key:true not_null:true}}];
  string tenant_id = 2 [(udb.core.common.v1.pg_column) = {{sql_type:"VARCHAR(80)" tenant_column:true not_null:true}}];
  string project_id = 3 [(udb.core.common.v1.pg_column) = {{sql_type:"VARCHAR(80)" project_column:true not_null:true}}];
  string lookup_key = 4 [(udb.core.common.v1.pg_column) = {{sql_type:"VARCHAR(80)" not_null:true}}];
  string round_item_id = 5 [(udb.core.common.v1.pg_column) = {{sql_type:"VARCHAR(80)" not_null:true}}];
  string external_key = 6 [(udb.core.common.v1.pg_column) = {{sql_type:"VARCHAR(80)" unique:true}}];
}}
message Round {{
  option (udb.core.common.v1.pg_table) = {{
    table_name: "rounds" schema_name: "{schema}" is_table: true
    audit_fields: false enable_rls: true force_rls: true
  }};
  option (udb.core.common.v1.db_table_security) = {{
    tenant_isolation_mode: "row" tenant_column: "tenant_id"
    project_isolation_mode: "column" project_column: "project_id"
    soft_delete_mode: "none" audit_mode: AUDIT_MODE_NONE
    encryption_profile: "none" pii_profile: "none" export_eligible: false
  }};
  string round_item_id = 1 [(udb.core.common.v1.pg_column) = {{sql_type:"VARCHAR(80)" primary_key:true not_null:true}}];
  string tenant_id = 2 [(udb.core.common.v1.pg_column) = {{sql_type:"VARCHAR(80)" tenant_column:true not_null:true}}];
  string project_id = 3 [(udb.core.common.v1.pg_column) = {{sql_type:"VARCHAR(80)" project_column:true not_null:true}}];
}}
"#
    )
}

fn parsed(source: &str) -> (CatalogManifest, Vec<crate::ast::ProtoSchema>) {
    let report = parse_proto_source(
        source.as_bytes(),
        "reviewed-catalog-live.proto",
        &ParserConfig::default(),
    )
    .expect("real proto parser creates the catalog fixture");
    let manifest = CatalogManifest::from_schemas(&report.schemas).expect("real manifest producer");
    let (manifest, schemas) = native_catalog::merge_native(&manifest, &report.schemas);
    assert!(
        crate::generation::lint_catalog(&manifest).passed,
        "fixture must pass real catalog lint"
    );
    (manifest, schemas)
}

async fn build_service(config: UdbConfig) -> DataBrokerService {
    let runtime = DataBrokerRuntime::from_config(config).await;
    let metrics: Arc<dyn MetricsRecorder> = Arc::new(NoopMetrics);
    let service = DataBrokerService::with_runtime_and_state(
        native_catalog::native_manifest().clone(),
        runtime,
        Arc::new(RwLock::new(FsmState::Completed)),
        metrics,
        None,
        false,
    );
    service
        .runtime_snapshot()
        .upgrade_and_validate_catalog_provenance()
        .await
        .expect("validate native loader provenance");
    service
        .reconcile_durable_active_project_catalogs()
        .await
        .expect("hydrate durable exact catalogs");
    service
}

async fn serve(service: DataBrokerService) -> Serving {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind owned catalog listener");
    let address = listener.local_addr().unwrap();
    let incoming = futures::stream::unfold(listener, |listener| async move {
        let connection = listener.accept().await.map(|(stream, _)| stream);
        Some((connection, listener))
    });
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
    let served = service.clone();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(CredentialResolveLayer::new())
            .add_service(DataBrokerServer::new(served))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve actual credential-resolved catalog broker");
    });
    let client = DataBrokerClient::connect(format!("http://{address}"))
        .await
        .expect("connect actual catalog RPC client");
    Serving {
        client,
        service,
        shutdown: Some(shutdown),
        task: Some(task),
    }
}

async fn fixture_bearer(
    authn: &AuthnServiceImpl,
    security: &SecurityConfig,
    tenant: &str,
    project: &str,
    scopes: &[&str],
) -> String {
    let username = format!("reviewed_{}", Uuid::new_v4().simple());
    // This test-only authority provisions owned fixture PERSON accounts. The
    // catalog requests below carry no task-local claim or header-scope authority.
    let provisioner_subject = Uuid::new_v4().to_string();
    let provisioning =
        || test_claim_context(&provisioner_subject, tenant, project, &["udb:admin"], &[]);
    let user = scope_claim_context_for_test(
        provisioning(),
        authn.create_user(Request::new(authn::CreateUserRequest {
            username: username.clone(),
            email: format!("{username}@example.test"),
            password: "FixturePassword1!".into(),
            tenant_id: tenant.into(),
            project_id: project.into(),
            account_kind: authn_entity::AccountKind::Person as i32,
            ..Default::default()
        })),
    )
    .await
    .expect("provision real native PERSON operator")
    .into_inner()
    .user
    .unwrap();
    scope_claim_context_for_test(
        provisioning(),
        authn.change_user_status(Request::new(authn::ChangeUserStatusRequest {
            user_id: user.user_id.clone(),
            new_status: authn_entity::UserStatus::Active as i32,
            reason: "owned catalog CI fixture".into(),
            ..Default::default()
        })),
    )
    .await
    .expect("activate real native fixture account");
    let scopes = scopes
        .iter()
        .map(|scope| scope.to_string())
        .collect::<Vec<_>>();
    // The fixture controls this issuer's real signing key. Catalog operator
    // scopes belong to PERSON credentials; service-grant admin prohibitions
    // stay enforced. The serving resolver checks this durable ACTIVE user.
    sign_access_token(
        security,
        &user.user_id,
        tenant,
        project,
        &scopes,
        &[],
        "",
        &format!("catalog-ci-{}", Uuid::new_v4()),
        "password",
        authn_entity::AccountKind::Person as i32,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
    .expect("sign actual broker fixture bearer")
    .expect("configured fixture signing key")
    .0
}

async fn plan(
    client: &mut DataBrokerClient<tonic::transport::Channel>,
    bearer: &str,
    project: &str,
    active: &CatalogVersionResponse,
    candidate: &[u8],
    key: &str,
) -> Result<MigrationPlanResponse, Status> {
    client
        .plan_migration(request(
            MigrationPlanRequest {
                project_id: project.into(),
                candidate_manifest_json: candidate.to_vec(),
                expected_active_catalog_id: active.catalog_id.clone(),
                expected_active_manifest_integrity_sha256: active.manifest_integrity_sha256.clone(),
                idempotency_key: key.into(),
                ..Default::default()
            },
            bearer,
        ))
        .await
        .map(tonic::Response::into_inner)
}

async fn approve(
    client: &mut DataBrokerClient<tonic::transport::Channel>,
    bearer: &str,
    plan: &MigrationPlanResponse,
) -> Result<MigrationStatusResponse, Status> {
    let evidence = plan
        .reviewed_catalog_transition
        .as_ref()
        .expect("native candidate plan evidence");
    client
        .approve_migration_plan(request(
            MigrationRunRequest {
                project_id: plan.project_id.clone(),
                run_id: plan.run_id.clone(),
                expected_operations_hash: evidence.operations_hash.clone(),
                reviewed_operation_fingerprints: evidence.reviewed_operation_fingerprints.clone(),
                idempotency_key: format!("approve-{}", plan.run_id),
                ..Default::default()
            },
            bearer,
        ))
        .await
        .map(tonic::Response::into_inner)
}

async fn apply(
    client: &mut DataBrokerClient<tonic::transport::Channel>,
    bearer: &str,
    approval: &MigrationStatusResponse,
) -> Result<MigrationStatusResponse, Status> {
    client
        .apply_migration(request(
            MigrationApplyRequest {
                project_id: approval.project_id.clone(),
                run_id: approval.run_id.clone(),
                approval_token: approval.approval_token.clone().unwrap(),
                idempotency_key: format!("apply-{}", approval.run_id),
                ..Default::default()
            },
            bearer,
        ))
        .await
        .map(tonic::Response::into_inner)
}

async fn stage(
    client: &mut DataBrokerClient<tonic::transport::Channel>,
    bearer: &str,
    project: &str,
    bytes: &[u8],
    run: &str,
    key: &str,
) -> Result<CatalogVersionResponse, Status> {
    client
        .stage_catalog(request(
            StageCatalogRequest {
                project_id: project.into(),
                manifest_json: bytes.to_vec(),
                reason: "real reviewed transition fixture".into(),
                reviewed_migration_run_id: run.into(),
                idempotency_key: key.into(),
                ..Default::default()
            },
            bearer,
        ))
        .await
        .map(tonic::Response::into_inner)
}

async fn activate(
    client: &mut DataBrokerClient<tonic::transport::Channel>,
    bearer: &str,
    catalog: &CatalogVersionResponse,
    run: &str,
    key: &str,
) -> Result<CatalogVersionResponse, Status> {
    client
        .activate_catalog(request(
            CatalogVersionRequest {
                project_id: catalog.project_id.clone(),
                version: catalog.catalog_id.clone(),
                reason: "real reviewed transition fixture".into(),
                reviewed_migration_run_id: run.into(),
                idempotency_key: key.into(),
                ..Default::default()
            },
            bearer,
        ))
        .await
        .map(tonic::Response::into_inner)
}

async fn assert_active(pool: &sqlx::PgPool, project: &str, expected: &str) {
    let relation = SystemCatalogConfig::current().project_catalog_bindings_relation();
    let id: String = sqlx::query_scalar(&format!(
        "SELECT active_catalog_id::TEXT FROM {relation} WHERE project_id=$1"
    ))
    .bind(project)
    .fetch_one(pool)
    .await
    .expect("read durable native ACTIVE binding");
    assert_eq!(
        id, expected,
        "a refusal must leave exact ACTIVE authority intact"
    );
}

async fn cleanup_owned_authority(
    pool: &sqlx::PgPool,
    projects: &[String],
) -> Result<(), &'static str> {
    let config = SystemCatalogConfig::current();
    let mut tx = pool
        .begin()
        .await
        .map_err(|_| "begin owned catalog cleanup")?;
    sqlx::query("SET LOCAL lock_timeout='5s'")
        .execute(&mut *tx)
        .await
        .map_err(|_| "bound owned cleanup lock wait")?;
    let phase = native_catalog::relation(&config.cdc.system_schema, "udb_migration_phase_ledger");
    let transition = native_catalog::relation(
        &config.cdc.system_schema,
        "udb_catalog_reviewed_transitions",
    );
    for relation in [&phase, &transition] {
        let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(relation)
            .fetch_one(&mut *tx)
            .await
            .map_err(|_| "locate owned cleanup ledger")?;
        if exists {
            let predicate = if relation == &phase {
                format!(
                    "run_id IN (SELECT run_id::TEXT FROM {} WHERE project_id=ANY($1))",
                    config.migration_runs_relation()
                )
            } else {
                "project_id=ANY($1)".into()
            };
            sqlx::query(&format!("DELETE FROM {relation} WHERE {predicate}"))
                .bind(projects)
                .execute(&mut *tx)
                .await
                .map_err(|_| "remove owned reviewed ledger")?;
        }
    }
    // The operation ledger cascades with runs; transition rows must go first.
    for relation in [
        config.migration_runs_relation(),
        config.project_catalog_bindings_relation(),
        config.catalog_reload_log_relation(),
        config.catalog_activation_log_relation(),
        config.catalog_versions_relation(),
        native_catalog::relation(&config.cdc.system_schema, &config.projects_table),
    ] {
        sqlx::query(&format!("DELETE FROM {relation} WHERE project_id=ANY($1)"))
            .bind(projects)
            .execute(&mut *tx)
            .await
            .map_err(|_| "remove owned catalog authority")?;
    }
    let users = native_catalog::native_model("udb.core.authn.entity.v1.User", &["project_id"]);
    sqlx::query(&format!(
        "DELETE FROM {} WHERE \"{}\"=ANY($1)",
        users.relation,
        users.column("project_id")
    ))
    .bind(projects)
    .execute(&mut *tx)
    .await
    .map_err(|_| "remove owned native PERSON accounts")?;
    tx.commit()
        .await
        .map_err(|_| "commit owned catalog cleanup")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires actual Postgres/CREATE DATABASE; unfiltered native CI live lane"]
async fn live_reviewed_catalog_transition_requires_native_approval_and_verified_application() {
    run_reviewed_catalog_fixture(false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires actual Postgres/CREATE DATABASE; unfiltered native CI live lane"]
async fn live_reviewed_catalog_transition_single_connection_distinct_target() {
    run_reviewed_catalog_fixture(true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires actual Postgres/CREATE DATABASE; unfiltered native CI live lane"]
async fn live_reviewed_catalog_transition_single_connection_primary_target() {
    run_reviewed_catalog_fixture(true, true).await;
}

// These fixtures deliberately use only the production APIs already present in
// c6664a2e, so CI can overlay identical serving controls on the original source.
async fn run_reviewed_catalog_fixture(single_connection: bool, primary_as_target: bool) {
    let Some(dsn) = require_live_dsn_any(&[
        "UDB_LIVE_NATIVE_PG_DSN",
        "UDB_LIVE_AUTH_PG_DSN",
        "UDB_INTEGRATION_PG_DSN",
        "UDB_PG_DSN",
    ]) else {
        return;
    };
    let _lock = live_native_service_db_lock().lock().await;
    let _restore = SecurityRestore(SecurityConfig::current());
    let administration = pool(&dsn).await;
    let database = format!("udb_reviewed_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE \"{database}\""))
        .execute(&administration)
        .await
        .expect("create owned routed project database");
    let target_dsn = project_dsn(&dsn, &database);
    let target = pool(&target_dsn).await;
    // These administrative fixture pools never supply broker serving capacity.
    let control = if primary_as_target {
        pool(&target_dsn).await
    } else {
        administration.clone()
    };
    super::support::migrate_native_service_db(&control).await;
    let tenant = Uuid::new_v4().to_string();
    let project = Uuid::new_v4().to_string();
    let foreign_project = Uuid::new_v4().to_string();
    let fixture_result = std::panic::AssertUnwindSafe(async {
    for ddl in native_catalog::native_service_catalog_ddl() {
        sqlx::raw_sql(&ddl)
            .execute(&target)
            .await
            .expect("provision actual project native DDL");
    }
    crate::runtime::system::ensure_system_catalog(&target)
        .await
        .expect("project-local canonical artifact ledger");
    let schema = format!("udb_reviewed_records_{}", Uuid::new_v4().simple());
    let original_source = source(&schema);
    let (original, schemas) = parsed(&original_source);
    for artifact in generate_bootstrap_sql(&schemas, &SqlGenerationConfig::default())
        .unwrap()
        .into_iter()
        .filter(|artifact| artifact.schema == schema)
    {
        sqlx::raw_sql(&artifact.content)
            .execute(&target)
            .await
            .expect("apply actual fixture producer DDL");
    }
    let candidate_source = original_source
        .replace("retention_class: \"reviewed.owned\"", "")
        .replace("idx_records_before", "idx_records_after")
        .replace(
            "columns: \"lookup_key\"",
            "columns: \"lookup_key\" columns: \"record_id\"",
        )
        .replacen("force_rls: true", &format!("force_rls: true\n    indexes: {{ index_name: \"idx_records_partial\" index_type: \"BTREE\" unique: true columns: \"lookup_key\" where_clause: \"round_item_id IS NULL\" }}\n    foreign_keys: {{ columns: \"round_item_id\" references_table: \"rounds\" references_schema: \"{schema}\" references_column: \"round_item_id\" constraint_name: \"fk_records_round_item\" on_delete: REFERENTIAL_ACTION_RESTRICT on_update: REFERENTIAL_ACTION_CASCADE deferrable: true }}"), 1)
        .replace("string round_item_id = 5 [(udb.core.common.v1.pg_column) = {sql_type:\"VARCHAR(80)\" not_null:true}];", "string round_item_id = 5 [(udb.core.common.v1.pg_column) = {sql_type:\"VARCHAR(80)\"}];");
    let (candidate, _) = parsed(&candidate_source);
    let partial_index = candidate.table(&schema, "records").unwrap().indexes.iter()
        .find(|index| index.name == "idx_records_partial").expect("real parser-generated partial unique candidate");
    assert!(partial_index.unique && partial_index.method.eq_ignore_ascii_case("btree"));
    assert_eq!(partial_index.columns, ["lookup_key"]);
    assert_eq!(partial_index.where_clause, "round_item_id IS NULL");
    let candidate_table = candidate.table(&schema, "records").unwrap();
    let ordinary_unique = candidate_table.columns.iter().find(|column| column.column_name == "external_key").unwrap();
    assert!(ordinary_unique.unique && !ordinary_unique.not_null && !ordinary_unique.is_primary);
    let foreign_key = candidate_table.foreign_keys.iter().find(|key| key.name == "fk_records_round_item")
        .expect("real parser-generated existing-table foreign key");
    assert_eq!(foreign_key.columns, ["round_item_id"]);
    assert_eq!(foreign_key.ref_schema, schema);
    assert_eq!(foreign_key.ref_table, "rounds");
    assert_eq!(foreign_key.ref_columns, ["round_item_id"]);
    assert_eq!(foreign_key.on_delete, "RESTRICT");
    assert_eq!(foreign_key.on_update, "CASCADE");
    assert!(foreign_key.deferrable && !foreign_key.initially_deferred && !foreign_key.not_valid);
    assert!(!candidate_table.columns.iter().find(|column| column.column_name == "round_item_id").unwrap().not_null);
    let original_bytes = serde_json::to_vec(&original).unwrap();
    let candidate_bytes = serde_json::to_vec(&candidate).unwrap();
    let security = SecurityConfig {
        tls_required: false,
        mtls_required: false,
        service_identity_required: false,
        allow_header_scopes: false,
        jwt_private_key: Some(include_str!("../../testdata/jwt_rs256_private.pem").into()),
        jwt_public_key: Some(include_str!("../../testdata/jwt_rs256_public.pem").into()),
        ..SecurityConfig::default()
    };
    let mut config = UdbConfig::from_env();
    config.primary.direct_dsn = if primary_as_target { target_dsn.clone() } else { dsn.clone() };
    if single_connection {
        config.primary.max_open_conns = 1;
        config.primary.min_connections = 1;
        config.primary.acquire_timeout_secs = 2;
    }
    config.project_routing_mode = if primary_as_target { "permissive" } else { "strict" }.into();
    config.security = security.clone();
    config.service.catalog_compatibility_level = "backward".into();
    config.service.abac_default_allow = false;
    config.backend_instances = if primary_as_target {
        BackendInstanceConfig { instances: vec![] }
    } else {
        BackendInstanceConfig {
            instances: vec![BackendInstance {
                name: "reviewed-project-target".into(),
                dsn: Some(target_dsn.clone()),
                dsn_env: None,
                labels: BTreeMap::from([("project_id".into(), project.clone())]),
                ..Default::default()
            }],
        }
    };
    let service = build_service(config.clone()).await;
    let runtime = service.runtime_snapshot();
    if single_connection {
        let serving_control = runtime.pg_pool_clone().expect("actual broker control pool");
        let serving_target = runtime.pg_pool_for_instance(Some(if primary_as_target {
            "primary"
        } else {
            "reviewed-project-target"
        })).expect("actual broker project target");
        assert_eq!(serving_control.options().get_max_connections(), 1);
        assert_eq!(serving_target.options().get_max_connections(), 1);
        assert_eq!(std::ptr::eq(serving_control.options(), serving_target.options()), primary_as_target,
            "the serving target must have the claimed shared/distinct pool layout");
    }
    let staged_base = runtime
        .stage_catalog(
            &project,
            "reviewed-base",
            &original_bytes,
            "fixture initial authority",
            "fixture-provisioner",
            "backward",
            "base-stage",
        )
        .await
        .expect("initial fixture stage uses ordinary native API");
    runtime
        .activate_catalog(
            &project,
            &staged_base.catalog.catalog_id,
            "fixture initial authority",
            "fixture-provisioner",
            "base-activate",
        )
        .await
        .expect("initial fixture activation");
    service
        .reconcile_durable_active_project_catalogs()
        .await
        .expect("hydrate initial ACTIVE authority");
    let recorded_base = CatalogVersionResponse {
        catalog_id: staged_base.catalog.catalog_id,
        project_id: project.clone(),
        checksum_sha256: staged_base.catalog.checksum_sha256,
        manifest_integrity_sha256: staged_base.catalog.manifest_integrity_sha256,
        ..Default::default()
    };
    assert_ne!(
        recorded_base.manifest_integrity_sha256, original.checksum_sha256,
        "outer stored integrity is not the inner semantic checksum"
    );
    let authn_config = AuthnConfig {
        session_hash_secret: format!("catalog-ci-{}", Uuid::new_v4()),
        ..AuthnConfig::from_env()
    };
    let authn = AuthnServiceImpl::with_stores(
        authn_config.clone(),
        security.clone(),
        Arc::new(PostgresSessionStore::new(control.clone(), "")),
        Arc::new(PostgresApiKeyStore::new(control.clone(), "")),
        Arc::new(PostgresUserStore::new(control.clone(), "")),
    )
    .with_runtime(Some(runtime.clone()))
    .with_authz_snapshot(Some(service.authz_snapshot()));
    let owner = fixture_bearer(&authn, &security, &tenant, &project, &["udb:admin"]).await;
    let reader = fixture_bearer(&authn, &security, &tenant, &project, &["catalog:read"]).await;
    let foreign_tenant = fixture_bearer(
        &authn,
        &security,
        &Uuid::new_v4().to_string(),
        &project,
        &["udb:admin"],
    )
    .await;
    let foreign_project_bearer =
        fixture_bearer(&authn, &security, &tenant, &foreign_project, &["udb:admin"]).await;
    crate::runtime::service::auth_service::install_data_plane_credential_resolvers(
        control.clone(),
        &authn_config,
        // The global validator needs the real native stores. Do not retain the
        // provisioning runtime or its soon-to-be-retired routed target there.
        Arc::new(authn.with_runtime(None).with_authz_snapshot(None)),
    );
    let mut served = serve(service).await;
    let base = served
        .client
        .get_catalog_version(request(
            CatalogVersionRequest {
                project_id: project.clone(),
                ..Default::default()
            },
            &owner,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(base.catalog_id, recorded_base.catalog_id);
    assert_eq!(
        base.manifest_integrity_sha256, recorded_base.manifest_integrity_sha256,
        "authenticated discovery returns verified durable outer integrity"
    );
    let discovered = served
        .client
        .get_catalog_versions(request(CatalogManifestRequest::default(), &owner))
        .await
        .unwrap()
        .into_inner();
    let discovered_active = discovered
        .versions
        .iter()
        .filter(|catalog| catalog.status == "ACTIVE")
        .collect::<Vec<_>>();
    assert_eq!(discovered_active.len(), 1);
    assert_eq!(
        discovered_active[0].manifest_integrity_sha256,
        base.manifest_integrity_sha256
    );
    assert_ne!(base.manifest_integrity_sha256, original.checksum_sha256);
    let native_plan = plan(
        &mut served.client, &owner, &project, &base, &candidate_bytes, "candidate-plan",
    ).await.expect(if single_connection {
        "CATALOG_CONTROL_CAPACITY: served reviewed candidate planning must succeed within one configured control connection"
    } else {
        "native candidate plan before stage"
    });
    if single_connection {
        let mut first_client = served.client.clone();
        let mut second_client = served.client.clone();
        let (first, second) = tokio::join!(
            plan(&mut first_client, &owner, &project, &base, &candidate_bytes, "concurrent-plan-one"),
            plan(&mut second_client, &owner, &project, &base, &candidate_bytes, "concurrent-plan-two"),
        );
        let first = first.expect("concurrent signed candidate plan one must use bounded capacity");
        let second = second.expect("concurrent signed candidate plan two must use bounded capacity");
        assert_ne!(first.run_id, second.run_id);
        assert_eq!(first.operations_hash, native_plan.operations_hash);
        assert_eq!(second.operations_hash, native_plan.operations_hash);
    }

    let mut no_credential = Request::new(MigrationPlanRequest {
        project_id: project.clone(),
        candidate_manifest_json: candidate_bytes.clone(),
        expected_active_catalog_id: base.catalog_id.clone(),
        expected_active_manifest_integrity_sha256: base.manifest_integrity_sha256.clone(),
        idempotency_key: "header-only-plan".into(),
        ..Default::default()
    });
    no_credential.set_timeout(Duration::from_secs(5));
    no_credential
        .metadata_mut()
        .insert("x-tenant-id", tenant.parse().unwrap());
    no_credential
        .metadata_mut()
        .insert("x-udb-project-id", project.parse().unwrap());
    no_credential
        .metadata_mut()
        .insert("x-scopes", "udb:admin".parse().unwrap());
    assert_eq!(
        served
            .client
            .plan_migration(no_credential)
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated,
        "header-only scopes cannot mint native review authority"
    );

    let mut wrong_inner_base = base.clone();
    wrong_inner_base.manifest_integrity_sha256 = original.checksum_sha256.clone();
    assert_eq!(
        plan(
            &mut served.client,
            &owner,
            &project,
            &wrong_inner_base,
            &candidate_bytes,
            "inner-hash-refusal"
        )
        .await
        .unwrap_err()
        .code(),
        Code::FailedPrecondition
    );
    let mut missing_base = base.clone();
    missing_base.catalog_id.clear();
    assert_eq!(
        plan(
            &mut served.client,
            &owner,
            &project,
            &missing_base,
            &candidate_bytes,
            "missing-base-refusal"
        )
        .await
        .unwrap_err()
        .code(),
        Code::FailedPrecondition
    );

    // The shipped ordinary policy remains a real red control.
    assert_eq!(
        stage(
            &mut served.client,
            &owner,
            &project,
            &candidate_bytes,
            "",
            "ordinary-reviewed-refusal"
        )
        .await
        .unwrap_err()
        .code(),
        Code::FailedPrecondition
    );

    assert_active(&control, &project, &base.catalog_id).await;

    let evidence = native_plan.reviewed_catalog_transition.as_ref().unwrap();
    assert!(
        !evidence.reviewed_operation_fingerprints.is_empty(),
        "fixture must exercise a real RequiresReview change"
    );
    assert_eq!(evidence.tenant_id, tenant);
    assert_eq!(evidence.expected_active_catalog_id, base.catalog_id);
    assert_eq!(
        plan(
            &mut served.client,
            &owner,
            &project,
            &base,
            &candidate_bytes,
            "candidate-plan"
        )
        .await
        .unwrap()
        .run_id,
        native_plan.run_id
    );
    assert_eq!(
        stage(
            &mut served.client,
            &owner,
            &project,
            &candidate_bytes,
            &native_plan.run_id,
            "unapproved-refusal"
        )
        .await
        .unwrap_err()
        .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        approve(&mut served.client, &reader, &native_plan)
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        approve(&mut served.client, &foreign_project_bearer, &native_plan)
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        approve(&mut served.client, &foreign_tenant, &native_plan)
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let bad_review = MigrationRunRequest {
        project_id: project.clone(),
        run_id: native_plan.run_id.clone(),
        expected_operations_hash: evidence.operations_hash.clone(),
        reviewed_operation_fingerprints: vec!["filesystem-assertion".into()],
        idempotency_key: "wrong-review".into(),
        ..Default::default()
    };
    assert_eq!(
        served
            .client
            .approve_migration_plan(request(bad_review, &owner))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let missing_review = MigrationRunRequest {
        project_id: project.clone(),
        run_id: native_plan.run_id.clone(),
        idempotency_key: "missing-review".into(),
        ..Default::default()
    };
    assert_eq!(
        served
            .client
            .approve_migration_plan(request(missing_review, &owner))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let mut duplicate_fingerprints = evidence.reviewed_operation_fingerprints.clone();
    duplicate_fingerprints.push(duplicate_fingerprints[0].clone());
    assert_eq!(
        served
            .client
            .approve_migration_plan(request(
                MigrationRunRequest {
                    project_id: project.clone(),
                    run_id: native_plan.run_id.clone(),
                    expected_operations_hash: evidence.operations_hash.clone(),
                    reviewed_operation_fingerprints: duplicate_fingerprints,
                    idempotency_key: "duplicate-review".into(),
                    ..Default::default()
                },
                &owner
            ))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let approval = approve(&mut served.client, &owner, &native_plan)
        .await
        .expect("exact native reviewed operation approval");
    assert_eq!(served.client.apply_migration(request(MigrationApplyRequest { context: Some(crate::proto::RequestContext {
        tenant_id: Uuid::new_v4().to_string(), project_id: project.clone(), ..Default::default() }),
        project_id: project.clone(), run_id: approval.run_id.clone(), approval_token: approval.approval_token.clone().unwrap(),
        idempotency_key: "contradictory-tenant-apply".into() }, &owner)).await.unwrap_err().code(), Code::PermissionDenied);
    assert!(
        !approval
            .reviewed_catalog_transition
            .as_ref()
            .unwrap()
            .approved_by
            .is_empty()
    );
    assert_eq!(
        approve(&mut served.client, &owner, &native_plan)
            .await
            .unwrap()
            .approval_token,
        approval.approval_token,
        "exact approval replay returns the same durable opaque token"
    );
    assert_eq!(
        stage(
            &mut served.client,
            &owner,
            &project,
            &candidate_bytes,
            &native_plan.run_id,
            "incomplete-refusal"
        )
        .await
        .unwrap_err()
        .code(),
        Code::FailedPrecondition
    );

    // The correct object name alone is insufficient physical evidence. Native
    // application must refuse a partially applied replacement with wrong keys;
    // no completion, staging authority or verification receipt may be minted.
    let failed_plan = plan(
        &mut served.client,
        &owner,
        &project,
        &base,
        &candidate_bytes,
        "wrong-physical-plan",
    )
    .await
    .unwrap();
    let failed_approval = approve(&mut served.client, &owner, &failed_plan)
        .await
        .unwrap();
    sqlx::raw_sql(&format!(
        "CREATE INDEX idx_records_after ON \"{schema}\".records (record_id)"
    ))
    .execute(&target)
    .await
    .expect("actual partial external application with wrong key shape");
    assert_eq!(
        apply(&mut served.client, &owner, &failed_approval)
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let failed_status = served
        .client
        .get_migration_status(request(
            MigrationRunRequest {
                project_id: project.clone(),
                run_id: failed_plan.run_id.clone(),
                ..Default::default()
            },
            &owner,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(failed_status.state, "ERROR");
    assert!(
        failed_status
            .reviewed_catalog_transition
            .as_ref()
            .unwrap()
            .application_evidence_sha256
            .is_empty()
    );
    assert_eq!(
        stage(
            &mut served.client,
            &owner,
            &project,
            &candidate_bytes,
            &failed_plan.run_id,
            "failed-apply-stage-refusal"
        )
        .await
        .unwrap_err()
        .code(),
        Code::FailedPrecondition
    );
    assert_active(&control, &project, &base.catalog_id).await;
    sqlx::raw_sql(&format!("DROP INDEX \"{schema}\".idx_records_after; DROP INDEX IF EXISTS \"{schema}\".idx_records_partial; CREATE INDEX IF NOT EXISTS idx_records_before ON \"{schema}\".records (lookup_key); ALTER TABLE \"{schema}\".records DROP CONSTRAINT IF EXISTS fk_records_round_item; ALTER TABLE \"{schema}\".records ALTER COLUMN round_item_id SET NOT NULL"))
        .execute(&target).await.expect("restore only fixture-owned base index shape");

    // External tooling has physically applied the exact reviewed index changes.
    // A receipt file alone cannot prove this; native apply must inspect target.
    sqlx::raw_sql(&format!("DROP INDEX \"{schema}\".idx_records_before; CREATE INDEX idx_records_after ON \"{schema}\".records (lookup_key, record_id); CREATE UNIQUE INDEX idx_records_partial ON \"{schema}\".records (lookup_key) WHERE round_item_id IS NULL; ALTER TABLE \"{schema}\".records ALTER COLUMN round_item_id DROP NOT NULL; ALTER TABLE \"{schema}\".records ADD CONSTRAINT fk_records_round_item FOREIGN KEY (round_item_id) REFERENCES \"{schema}\".rounds (round_item_id) ON DELETE RESTRICT ON UPDATE CASCADE DEFERRABLE INITIALLY IMMEDIATE"))
        .execute(&target).await.expect("actually externally apply the reviewed physical transition");
    let primary_name: String = sqlx::query_scalar("SELECT p.conname::TEXT FROM pg_catalog.pg_constraint p JOIN pg_catalog.pg_class c ON c.oid=p.conrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname::TEXT=$1 AND c.relname='records' AND p.contype='p'")
        .bind(&schema).fetch_one(&target).await.expect("discover actual producer primary constraint");
    assert!(
        primary_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    );
    let unique_name: String = sqlx::query_scalar(
        "SELECT c.conname::TEXT FROM pg_catalog.pg_constraint c
         JOIN pg_catalog.pg_class r ON r.oid=c.conrelid
         JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
         JOIN pg_catalog.pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=c.conkey[1]
         WHERE n.nspname::TEXT=$1 AND r.relname='records' AND c.contype='u'
         AND cardinality(c.conkey)=1 AND a.attname='external_key'",
    )
    .bind(&schema).fetch_one(&target).await.expect("discover actual producer ordinary UNIQUE constraint");
    assert!(unique_name.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'));
    let alternate_collation: String = sqlx::query_scalar(
        "SELECT quote_ident(n.nspname)||'.'||quote_ident(c.collname)
         FROM pg_catalog.pg_collation c JOIN pg_catalog.pg_namespace n ON n.oid=c.collnamespace
         WHERE n.nspname='pg_catalog' AND c.collname IN ('C','POSIX') AND c.oid<>(
             SELECT a.attcollation FROM pg_catalog.pg_attribute a
             JOIN pg_catalog.pg_class r ON r.oid=a.attrelid
             JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
             WHERE n.nspname::TEXT=$1 AND r.relname='records' AND a.attname='external_key')
         ORDER BY c.collname LIMIT 1",
    )
    .bind(&schema).fetch_one(&target).await.expect("choose a real alternate index collation");
    let restore_unique = format!("ALTER TABLE \"{schema}\".records ADD CONSTRAINT \"{unique_name}\" UNIQUE (external_key)");
    let drop_unique = format!("ALTER TABLE \"{schema}\".records DROP CONSTRAINT \"{unique_name}\"");
    let restore_primary = format!("ALTER TABLE \"{schema}\".records ADD CONSTRAINT \"{primary_name}\" PRIMARY KEY (record_id)");
    let drop_primary = format!("ALTER TABLE \"{schema}\".records DROP CONSTRAINT \"{primary_name}\"");
    let mut physical_controls = vec![
        ("foreign-target", format!("ALTER TABLE \"{schema}\".records DROP CONSTRAINT fk_records_round_item; ALTER TABLE \"{schema}\".records ADD CONSTRAINT fk_records_round_item FOREIGN KEY (round_item_id) REFERENCES \"{schema}\".records (record_id) ON DELETE RESTRICT ON UPDATE CASCADE DEFERRABLE INITIALLY IMMEDIATE"),
            format!("ALTER TABLE \"{schema}\".records DROP CONSTRAINT fk_records_round_item; ALTER TABLE \"{schema}\".records ADD CONSTRAINT fk_records_round_item FOREIGN KEY (round_item_id) REFERENCES \"{schema}\".rounds (round_item_id) ON DELETE RESTRICT ON UPDATE CASCADE DEFERRABLE INITIALLY IMMEDIATE")),
        ("foreign-unvalidated", format!("ALTER TABLE \"{schema}\".records DROP CONSTRAINT fk_records_round_item; ALTER TABLE \"{schema}\".records ADD CONSTRAINT fk_records_round_item FOREIGN KEY (round_item_id) REFERENCES \"{schema}\".rounds (round_item_id) ON DELETE RESTRICT ON UPDATE CASCADE DEFERRABLE INITIALLY IMMEDIATE NOT VALID"),
            format!("ALTER TABLE \"{schema}\".records VALIDATE CONSTRAINT fk_records_round_item")),
        ("partial-predicate", format!("DROP INDEX \"{schema}\".idx_records_partial; CREATE UNIQUE INDEX idx_records_partial ON \"{schema}\".records (lookup_key) WHERE round_item_id IS NOT NULL"),
            format!("DROP INDEX \"{schema}\".idx_records_partial; CREATE UNIQUE INDEX idx_records_partial ON \"{schema}\".records (lookup_key) WHERE round_item_id IS NULL")),
        (
            "type",
            format!("ALTER TABLE \"{schema}\".records ALTER COLUMN lookup_key TYPE TEXT"),
            format!("ALTER TABLE \"{schema}\".records ALTER COLUMN lookup_key TYPE VARCHAR(80)"),
        ),
        (
            "default",
            format!(
                "ALTER TABLE \"{schema}\".records ALTER COLUMN lookup_key SET DEFAULT 'wrong-native-default'"
            ),
            format!("ALTER TABLE \"{schema}\".records ALTER COLUMN lookup_key DROP DEFAULT"),
        ),
        (
            "primary",
            format!("ALTER TABLE \"{schema}\".records DROP CONSTRAINT \"{primary_name}\""),
            format!(
                "ALTER TABLE \"{schema}\".records ADD CONSTRAINT \"{primary_name}\" PRIMARY KEY (record_id)"
            ),
        ),
        (
            "rls",
            format!("ALTER TABLE \"{schema}\".records DISABLE ROW LEVEL SECURITY"),
            format!("ALTER TABLE \"{schema}\".records ENABLE ROW LEVEL SECURITY"),
        ),
        (
            "unique-deferrable",
            format!("{drop_unique}; ALTER TABLE \"{schema}\".records ADD CONSTRAINT \"{unique_name}\" UNIQUE (external_key) DEFERRABLE INITIALLY IMMEDIATE"),
            format!("{drop_unique}; {restore_unique}"),
        ),
        (
            "primary-deferrable",
            format!("{drop_primary}; ALTER TABLE \"{schema}\".records ADD CONSTRAINT \"{primary_name}\" PRIMARY KEY (record_id) DEFERRABLE INITIALLY IMMEDIATE"),
            format!("{drop_primary}; {restore_primary}"),
        ),
        (
            "unique-opclass",
            format!("{drop_unique}; CREATE UNIQUE INDEX idx_records_unique_control ON \"{schema}\".records (external_key varchar_pattern_ops)"),
            format!("DROP INDEX \"{schema}\".idx_records_unique_control; {restore_unique}"),
        ),
        (
            "unique-collation",
            format!("{drop_unique}; CREATE UNIQUE INDEX idx_records_unique_control ON \"{schema}\".records (external_key COLLATE {alternate_collation})"),
            format!("DROP INDEX \"{schema}\".idx_records_unique_control; {restore_unique}"),
        ),
        (
            "unique-include",
            format!("{drop_unique}; ALTER TABLE \"{schema}\".records ADD CONSTRAINT \"{unique_name}\" UNIQUE (external_key) INCLUDE (record_id)"),
            format!("{drop_unique}; {restore_unique}"),
        ),
        (
            "primary-include",
            format!("{drop_primary}; ALTER TABLE \"{schema}\".records ADD CONSTRAINT \"{primary_name}\" PRIMARY KEY (record_id) INCLUDE (lookup_key)"),
            format!("{drop_primary}; {restore_primary}"),
        ),
    ];
    let server_version: i32 = sqlx::query_scalar("SELECT current_setting('server_version_num')::INTEGER")
        .fetch_one(&target).await.expect("real PostgreSQL capability for NULLS NOT DISTINCT");
    if server_version >= 150000 {
        physical_controls.push((
            "unique-nulls-not-distinct",
            format!("{drop_unique}; ALTER TABLE \"{schema}\".records ADD CONSTRAINT \"{unique_name}\" UNIQUE NULLS NOT DISTINCT (external_key)"),
            format!("{drop_unique}; {restore_unique}"),
        ));
    }
    for (name, corrupt, restore) in physical_controls {
        let bad_plan = plan(
            &mut served.client,
            &owner,
            &project,
            &base,
            &candidate_bytes,
            &format!("physical-{name}-plan"),
        )
        .await
        .unwrap();
        let bad_approval = approve(&mut served.client, &owner, &bad_plan)
            .await
            .unwrap();
        sqlx::raw_sql(&corrupt)
            .execute(&target)
            .await
            .expect("corrupt only owned target physical shape");
        assert_eq!(
            apply(&mut served.client, &owner, &bad_approval)
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition,
            "native verification must reject actual affected-table {name} drift"
        );
        let bad_status = served
            .client
            .get_migration_status(request(
                MigrationRunRequest {
                    project_id: project.clone(),
                    run_id: bad_plan.run_id.clone(),
                    ..Default::default()
                },
                &owner,
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            bad_status.state, "ERROR",
            "physical {name} drift must not complete"
        );
        assert!(
            bad_status
                .reviewed_catalog_transition
                .as_ref()
                .unwrap()
                .application_evidence_sha256
                .is_empty()
        );
        let false_receipts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM public.schema_migrations WHERE filename LIKE $1 AND operation_kind='verified_preapplied'")
            .bind(format!("catalog-transition/{}/%", bad_plan.run_id)).fetch_one(&target).await.unwrap();
        assert_eq!(
            false_receipts, 0,
            "wrong {name} shape must never mint a VERIFIED target receipt"
        );
        assert_eq!(
            stage(
                &mut served.client,
                &owner,
                &project,
                &candidate_bytes,
                &bad_plan.run_id,
                &format!("physical-{name}-stage-refusal")
            )
            .await
            .unwrap_err()
            .code(),
            Code::FailedPrecondition
        );
        assert_active(&control, &project, &base.catalog_id).await;
        sqlx::raw_sql(&restore)
            .execute(&target)
            .await
            .expect("restore only owned target physical shape");
    }
    // Physical storage tuning does not change the ordinary UNIQUE/PK promise.
    // Keep this positive alongside the semantic corruption refusal matrix.
    sqlx::raw_sql(&format!(
        "ALTER INDEX \"{schema}\".\"{unique_name}\" SET (fillfactor=70,deduplicate_items=off); \
         ALTER INDEX \"{schema}\".\"{primary_name}\" SET (fillfactor=70,deduplicate_items=off)"
    ))
    .execute(&target)
    .await
    .expect("apply only owned ordinary-constraint storage tuning");
    let control_has_table: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(format!("{schema}.records"))
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(
        control_has_table, primary_as_target,
        "physical target presence must match the actual shared/distinct database layout"
    );
    let applied = apply(&mut served.client, &owner, &approval)
        .await
        .expect("native review verifies an actually pre-applied target");
    assert_eq!(applied.state, "COMPLETED");
    let applied_evidence = applied.reviewed_catalog_transition.as_ref().unwrap();
    assert_eq!(applied_evidence.application_state, "COMPLETED");
    assert!(!applied.operations.is_empty());
    assert!(
        applied
            .operations
            .iter()
            .all(|operation| operation.status == "VERIFIED"),
        "pre-applied verification must not claim broker APPLIED DDL"
    );
    let verified_receipts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM public.schema_migrations WHERE filename LIKE $1 AND operation_kind='verified_preapplied'")
        .bind(format!("catalog-transition/{}/%", native_plan.run_id)).fetch_one(&target).await.unwrap();
    assert!(
        verified_receipts > 0,
        "only actual routed native verification issues target receipts"
    );
    assert_eq!(
        applied_evidence.applied_operations_hash,
        evidence.operations_hash
    );
    assert!(!applied_evidence.application_evidence_sha256.is_empty());
    assert_eq!(
        apply(&mut served.client, &owner, &approval)
            .await
            .unwrap()
            .state,
        "COMPLETED",
        "exact native apply replay is durable"
    );
    // Missing FK and nullable widening are normal inputs to broker DDL apply.
    // After a run completes, however, stale receipts cannot authorize a new
    // stage over missing/contradictory physical target state.
    for (name, corrupt, restore) in [
        ("missing-foreign-key", format!("ALTER TABLE \"{schema}\".records DROP CONSTRAINT fk_records_round_item"),
            format!("ALTER TABLE \"{schema}\".records ADD CONSTRAINT fk_records_round_item FOREIGN KEY (round_item_id) REFERENCES \"{schema}\".rounds (round_item_id) ON DELETE RESTRICT ON UPDATE CASCADE DEFERRABLE INITIALLY IMMEDIATE")),
        ("nullable-widening-mismatch", format!("ALTER TABLE \"{schema}\".records ALTER COLUMN round_item_id SET NOT NULL"),
            format!("ALTER TABLE \"{schema}\".records ALTER COLUMN round_item_id DROP NOT NULL")),
    ] {
        sqlx::raw_sql(&corrupt).execute(&target).await.expect("corrupt only completed owned target authority");
        assert_eq!(stage(&mut served.client, &owner, &project, &candidate_bytes, &native_plan.run_id, &format!("completed-{name}-stage-refusal")).await.unwrap_err().code(), Code::FailedPrecondition,
            "completed native receipt must not stage over actual {name}");
        let current = served.client.get_migration_status(request(MigrationRunRequest { project_id: project.clone(), run_id: native_plan.run_id.clone(),
            ..Default::default() }, &owner)).await.unwrap().into_inner();
        assert_eq!(current.reviewed_catalog_transition.as_ref().unwrap().application_evidence_sha256, applied_evidence.application_evidence_sha256,
            "refused stage must not mint new {name} application evidence");
        let receipts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM public.schema_migrations WHERE filename LIKE $1 AND operation_kind='verified_preapplied'")
            .bind(format!("catalog-transition/{}/%", native_plan.run_id)).fetch_one(&target).await.unwrap();
        assert_eq!(receipts, verified_receipts, "refused stage must not mint new {name} target receipts");
        assert_active(&control, &project, &base.catalog_id).await;
        sqlx::raw_sql(&restore).execute(&target).await.expect("restore exact completed owned target authority");
    }
    let mut mismatched = candidate.clone();
    mismatched
        .warnings
        .push("owned fixture target integrity mismatch".into());
    assert_eq!(
        stage(
            &mut served.client,
            &owner,
            &project,
            &serde_json::to_vec(&mismatched).unwrap(),
            &native_plan.run_id,
            "mismatch-refusal"
        )
        .await
        .unwrap_err()
        .code(),
        Code::FailedPrecondition
    );
    let missing_run = Uuid::new_v4().to_string();
    assert_eq!(
        stage(
            &mut served.client,
            &owner,
            &project,
            &candidate_bytes,
            &missing_run,
            "missing-run-refusal"
        )
        .await
        .unwrap_err()
        .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        stage(
            &mut served.client,
            &foreign_tenant,
            &project,
            &candidate_bytes,
            &native_plan.run_id,
            "foreign-tenant-stage-refusal"
        )
        .await
        .unwrap_err()
        .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        stage(
            &mut served.client,
            &foreign_project_bearer,
            &project,
            &candidate_bytes,
            &native_plan.run_id,
            "foreign-project-stage-refusal"
        )
        .await
        .unwrap_err()
        .code(),
        Code::PermissionDenied
    );
    let reviewed_stage = stage(
        &mut served.client,
        &owner,
        &project,
        &candidate_bytes,
        &native_plan.run_id,
        "reviewed-stage",
    )
    .await
    .expect("exact approved+verified stage");
    assert_eq!(
        stage(
            &mut served.client,
            &owner,
            &project,
            &candidate_bytes,
            &native_plan.run_id,
            "reviewed-stage"
        )
        .await
        .unwrap()
        .catalog_id,
        reviewed_stage.catalog_id
    );
    assert_eq!(
        activate(
            &mut served.client,
            &owner,
            &reviewed_stage,
            "",
            "missing-ref-activate-refusal"
        )
        .await
        .unwrap_err()
        .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        activate(
            &mut served.client,
            &foreign_tenant,
            &reviewed_stage,
            &native_plan.run_id,
            "foreign-activate-refusal"
        )
        .await
        .unwrap_err()
        .code(),
        Code::FailedPrecondition
    );

    // A new ACTIVE base invalidates the old staged approval even when physical
    // data are unchanged. Both stage and activation must revalidate under lock.
    let mut newer_base = original.clone();
    newer_base
        .warnings
        .push("owned fixture new durable ACTIVE base".into());
    let superseding = stage(
        &mut served.client,
        &owner,
        &project,
        &serde_json::to_vec(&newer_base).unwrap(),
        "",
        "supersede-stage",
    )
    .await
    .unwrap();
    activate(
        &mut served.client,
        &owner,
        &superseding,
        "",
        "supersede-activate",
    )
    .await
    .unwrap();
    assert_eq!(
        activate(
            &mut served.client,
            &owner,
            &reviewed_stage,
            &native_plan.run_id,
            "stale-activate-refusal"
        )
        .await
        .unwrap_err()
        .code(),
        Code::FailedPrecondition
    );
    assert_eq!(
        stage(
            &mut served.client,
            &owner,
            &project,
            &candidate_bytes,
            &native_plan.run_id,
            "stale-stage-refusal"
        )
        .await
        .unwrap_err()
        .code(),
        Code::FailedPrecondition
    );
    assert_active(&control, &project, &superseding.catalog_id).await;

    // The renewed base can also take the ordinary broker-applied DDL path.
    // Restore actual physical base objects, so this new immutable run cannot
    // claim that an externally pre-applied receipt performed the migration.
    sqlx::raw_sql(&format!("DROP INDEX \"{schema}\".idx_records_after; DROP INDEX \"{schema}\".idx_records_partial; CREATE INDEX idx_records_before ON \"{schema}\".records (lookup_key); ALTER TABLE \"{schema}\".records DROP CONSTRAINT fk_records_round_item; ALTER TABLE \"{schema}\".records ALTER COLUMN round_item_id SET NOT NULL"))
        .execute(&target).await.expect("restore owned base for actual native DDL application");
    let fresh_plan = plan(
        &mut served.client,
        &owner,
        &project,
        &superseding,
        &candidate_bytes,
        "fresh-plan",
    )
    .await
    .unwrap();
    let fresh_approval = approve(&mut served.client, &owner, &fresh_plan)
        .await
        .unwrap();
    if single_connection {
        // Hold only physical target CREATE INDEX after its command executes.
        // PostgreSQL's TEMP expected-shape work uses another schema, so this
        // owned-db hook cannot block preflight parse/deparse or mint receipts.
        let ddl_gate = format!("catalog-ci-ddl-{}", Uuid::new_v4());
        sqlx::raw_sql(&format!(
            "CREATE FUNCTION \"{schema}\".hold_owned_catalog_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $ci_hold$
             BEGIN IF EXISTS(SELECT 1 FROM pg_event_trigger_ddl_commands() WHERE schema_name='{schema}' AND command_tag='CREATE INDEX')
             THEN PERFORM pg_advisory_xact_lock(hashtextextended('{ddl_gate}',900731)); END IF; END $ci_hold$;
             CREATE EVENT TRIGGER udb_owned_catalog_hold ON ddl_command_end EXECUTE FUNCTION \"{schema}\".hold_owned_catalog_ddl();"
        )).execute(&target).await.expect("install only owned target physical-DDL blockade");
        let mut held_target = target.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,900731))")
            .bind(&ddl_gate).execute(&mut *held_target).await.unwrap();
        let held_target_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *held_target).await.unwrap();
        let mut timed_client = served.client.clone();
        let mut timed_request = request(MigrationApplyRequest {
            project_id: project.clone(),
            run_id: fresh_approval.run_id.clone(),
            approval_token: fresh_approval.approval_token.clone().unwrap(),
            idempotency_key: format!("apply-{}", fresh_approval.run_id),
            ..Default::default()
        }, &owner);
        // Native artifact lock_timeout is 5s; expiry at 4s cancels the served
        // request before the intentional DDL gate can become an SQL failure.
        timed_request.set_timeout(Duration::from_secs(4));
        let pending_apply = tokio::spawn(async move {
            timed_client.apply_migration(timed_request).await
        });
        let runs = SystemCatalogConfig::default().migration_runs_relation();
        let active_run_id = Uuid::parse_str(&fresh_approval.run_id).unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let state: String = sqlx::query_scalar(&format!("SELECT state FROM {runs} WHERE run_id=$1"))
                    .bind(active_run_id).fetch_one(&control).await.unwrap();
                if state == "APPLYING" {
                    break;
                }
                assert_eq!(state, "APPROVED", "blocked Apply must enter its durable running state");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("actual served Apply must begin before its client deadline");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let physical_ddl_waiting: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM pg_locks waiting JOIN pg_locks held
                     ON waiting.locktype=held.locktype AND waiting.database=held.database
                     AND waiting.classid=held.classid AND waiting.objid=held.objid
                     AND waiting.objsubid=held.objsubid
                     WHERE held.pid=$1 AND held.locktype='advisory' AND held.granted
                     AND NOT waiting.granted AND waiting.pid<>held.pid)"
                ).bind(held_target_pid).fetch_one(&target).await.unwrap();
                if physical_ddl_waiting { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("actual target CREATE INDEX must wait in the owned DDL gate");
        let mut observer = control.begin().await.unwrap();
        let observer_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *observer).await.unwrap();
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1,534154))")
            .bind(&project).fetch_one(&mut *observer).await.unwrap();
        assert!(!acquired, "Apply must retain the project lock across its preflight COMMIT");
        let waiter_project = project.clone();
        let waiter = tokio::spawn(async move {
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,534154))")
                .bind(&waiter_project).execute(&mut *observer).await
                .expect("independent project waiter query");
            observer.commit().await.expect("release only observer project lock");
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND NOT granted)")
                    .bind(observer_pid).fetch_one(&control).await.unwrap();
                if blocked { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("real independent observer must be waiting on the project authority lock");
        let refused = tokio::time::timeout(Duration::from_secs(6), pending_apply)
            .await.expect("bounded served transport cancellation")
            .expect("owned client task must finish")
            .expect_err("target DDL gate must prevent Apply success before the deadline");
        // Pinned tonic 0.12 maps transport TimeoutExpired to Cancelled.
        assert_eq!(refused.code(), Code::Cancelled, "the actual transport deadline must cancel Apply");
        tokio::time::timeout(Duration::from_secs(5), waiter).await
            .expect("cancelled served Apply must release its owned project lock")
            .expect("independent observer must finish");
        // Remove the hook while its gate is still held: cancelled work cannot
        // turn an uncommitted artifact into a successful continuation.
        sqlx::query("DROP EVENT TRIGGER udb_owned_catalog_hold")
            .execute(&target).await.expect("remove only owned target DDL hook");
        held_target.rollback().await.unwrap();
    }
    let fresh_applied = apply(&mut served.client, &owner, &fresh_approval)
        .await
        .expect("actual native apply must resume after a cancelled, uncommitted artifact");
    assert_eq!(
        fresh_applied
            .reviewed_catalog_transition
            .as_ref()
            .unwrap()
            .application_state,
        "COMPLETED"
    );
    assert!(
        fresh_applied
            .operations
            .iter()
            .any(|operation| operation.status == "APPLIED"),
        "this run must actually apply broker DDL rather than only verify external work"
    );
    let fresh_stage = stage(
        &mut served.client,
        &owner,
        &project,
        &candidate_bytes,
        &fresh_plan.run_id,
        "fresh-stage",
    )
    .await
    .unwrap();

    // Drop the actual listener and create an independent runtime/service. No
    // previous serving cache or file receipt is consulted by the new runtime.
    served.stop().await;
    let mut restarted = serve(build_service(config.clone()).await).await;
    let after_restart = restarted
        .client
        .get_migration_status(request(
            MigrationRunRequest {
                project_id: project.clone(),
                run_id: fresh_plan.run_id.clone(),
                ..Default::default()
            },
            &owner,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        after_restart.reviewed_catalog_transition,
        fresh_applied.reviewed_catalog_transition
    );
    let active = activate(
        &mut restarted.client,
        &owner,
        &fresh_stage,
        &fresh_plan.run_id,
        "fresh-activate",
    )
    .await
    .expect("durable verified approval survives native restart");
    assert_eq!(active.status, "ACTIVE");
    assert_eq!(
        activate(
            &mut restarted.client,
            &owner,
            &fresh_stage,
            &fresh_plan.run_id,
            "fresh-activate"
        )
        .await
        .unwrap()
        .catalog_id,
        active.catalog_id
    );
    assert_active(&control, &project, &active.catalog_id).await;
    assert_eq!(
        restarted
            .service
            .catalog
            .active_exact_for(&project)
            .unwrap()
            .metadata
            .checksum,
        active.checksum_sha256
    );

    // Drop a declared column: real producer diff must remain blocked/destructive
    // even with an admin and an otherwise legitimate reviewed workflow.
    let blocked_source = candidate_source.lines().filter(|line| !line.contains("string lookup_key = 4") && !line.contains("idx_records_partial")).collect::<Vec<_>>().join("\n")
        .replace("indexes: { index_name: \"idx_records_after\" index_type: \"BTREE\" columns: \"lookup_key\" columns: \"record_id\" }", "");
    let report = parse_proto_source(
        blocked_source.as_bytes(),
        "blocked-reviewed.proto",
        &ParserConfig::default(),
    )
    .unwrap();
    let blocked = CatalogManifest::from_schemas(&report.schemas).unwrap();
    let (blocked, _) = native_catalog::merge_native(&blocked, &report.schemas);
    assert_eq!(
        plan(
            &mut restarted.client,
            &owner,
            &project,
            &active,
            &serde_json::to_vec(&blocked).unwrap(),
            "blocked-plan"
        )
        .await
        .unwrap_err()
        .code(),
        Code::FailedPrecondition
    );
    assert_active(&control, &project, &active.catalog_id).await;
    restarted.stop().await;
    drop(runtime);
    }).catch_unwind().await;
    // Unwind drops/aborts this test's listener before cleanup. Remove only the
    // two UUID projects owned here, so later all-project loader scans cannot
    // resolve durable references into the soon-to-be-deleted routed database.
    let cleanup = cleanup_owned_authority(&control, &[project, foreign_project]).await;
    let closed = tokio::time::timeout(Duration::from_secs(10), target.close()).await;
    if primary_as_target {
        tokio::time::timeout(Duration::from_secs(10), control.close())
            .await
            .expect("owned primary fixture pool must close");
    }
    let removed = sqlx::query(&format!("DROP DATABASE \"{database}\" WITH (FORCE)"))
        .execute(&administration)
        .await;
    if let Err(payload) = fixture_result {
        if cleanup.is_err() || closed.is_err() || removed.is_err() {
            eprintln!("owned catalog fixture cleanup incomplete after assertion failure");
        }
        std::panic::resume_unwind(payload);
    }
    cleanup.expect("clean only owned native catalog authority");
    assert!(closed.is_ok(), "owned routed project pool failed to close");
    removed.expect("remove only the owned routed database");
}
