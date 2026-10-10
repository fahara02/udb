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
    pool: &sqlx::PgPool,
    authn: &AuthnServiceImpl,
    security: &SecurityConfig,
    tenant: &str,
    project: &str,
    scopes: &[&str],
) -> String {
    let username = format!("reviewed_{}", Uuid::new_v4().simple());
    // Bootstrap an actual owned PERSON through the existing trusted in-process
    // native API. No claim is installed, so optional created_by remains SQL NULL;
    // scoped provisioning below attributes every operator to this durable user.
    // Catalog requests still use signed credentials through the TCP resolver.
    assert!(
        !crate::runtime::service::method_security::claim_context_present(),
        "owned PERSON bootstrap must precede any task-local claim"
    );
    let provisioner_username = format!("reviewed_provisioner_{}", Uuid::new_v4().simple());
    let provisioner = authn
        .create_user(Request::new(authn::CreateUserRequest {
            username: provisioner_username.clone(),
            email: format!("{provisioner_username}@example.test"),
            password: "FixturePassword1!".into(),
            tenant_id: tenant.into(),
            project_id: project.into(),
            account_kind: authn_entity::AccountKind::Person as i32,
            ..Default::default()
        }))
        .await
        .expect("bootstrap owned native PERSON provisioner without attribution")
        .into_inner()
        .user
        .expect("bootstrap returns the persisted PERSON provisioner");
    assert!(provisioner.created_by.is_empty());
    assert_eq!(provisioner.tenant_id, tenant);
    assert_eq!(provisioner.project_id, project);
    assert_eq!(
        provisioner.account_kind,
        authn_entity::AccountKind::Person as i32
    );
    let provisioner_subject = provisioner.user_id;
    assert!(Uuid::parse_str(&provisioner_subject).is_ok());
    // This administrative pool verifies fixture state only; catalog serving
    // capacity remains the configured runtime pool, including both max1 layouts.
    let users =
        native_catalog::native_model("udb.core.authn.entity.v1.User", &["user_id", "created_by"]);
    let unattributed: bool = sqlx::query_scalar(&format!(
        "SELECT {} IS NULL FROM {} WHERE {}=$1::UUID",
        users.q("created_by"),
        users.relation,
        users.q("user_id")
    ))
    .bind(&provisioner_subject)
    .fetch_one(pool)
    .await
    .expect("read durable native PERSON provisioner attribution");
    assert!(
        unattributed,
        "bootstrap provisioner created_by must be SQL NULL"
    );
    let provisioning =
        || test_claim_context(&provisioner_subject, tenant, project, &["udb:admin"], &[]);
    scope_claim_context_for_test(
        provisioning(),
        authn.change_user_status(Request::new(authn::ChangeUserStatusRequest {
            user_id: provisioner_subject.clone(),
            new_status: authn_entity::UserStatus::Active as i32,
            reason: "owned native catalog provisioner".into(),
            ..Default::default()
        })),
    )
    .await
    .expect("activate durable native PERSON provisioner");
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

async fn assert_native_unique_base(
    client: &mut DataBrokerClient<tonic::transport::Channel>,
    bearer: &str,
    base: &CatalogVersionResponse,
    bytes: &[u8],
    target: &sqlx::PgPool,
    key: &str,
) {
    let verified_plan = plan(client, bearer, &base.project_id, base, bytes, key)
        .await
        .expect("plan native verification of the actual owned UNIQUE base");
    let verified_approval = approve(client, bearer, &verified_plan)
        .await
        .expect("authorize exact native UNIQUE-base verification");
    let verified = apply(client, bearer, &verified_approval)
        .await
        .expect("verify actual owned UNIQUE base before semantic corruption");
    assert_eq!(verified.state, "COMPLETED");
    assert!(!verified.operations.is_empty());
    assert!(
        verified
            .operations
            .iter()
            .all(|operation| operation.status == "VERIFIED"),
        "the already-applied UNIQUE base must produce real VERIFIED, rather than APPLIED, evidence"
    );
    let evidence = verified.reviewed_catalog_transition.as_ref().unwrap();
    assert_eq!(evidence.application_state, "COMPLETED");
    assert_eq!(
        evidence.applied_operations_hash,
        verified_plan.operations_hash
    );
    assert!(!evidence.application_evidence_sha256.is_empty());
    let receipts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM public.schema_migrations WHERE filename LIKE $1 AND operation_kind='verified_preapplied'",
    )
    .bind(format!("catalog-transition/{}/%", verified_plan.run_id))
    .fetch_one(target)
    .await
    .expect("read actual target UNIQUE-base verification receipts");
    assert!(
        receipts > 0,
        "native UNIQUE-base verification must issue actual routed target receipts"
    );
    println!("CATALOG_UNIQUE_BASE phase={key} native_verified_receipts={receipts}");
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
        // Admit both concurrent migration RPCs; their control/target database
        // pools deliberately remain max1. The production admission default is 1.
        config.channels.migration_max_concurrent = 2;
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
    let owner = fixture_bearer(&control, &authn, &security, &tenant, &project, &["udb:admin"]).await;
    let reader = fixture_bearer(&control, &authn, &security, &tenant, &project, &["catalog:read"]).await;
    let foreign_tenant = fixture_bearer(
        &control,
        &authn,
        &security,
        &Uuid::new_v4().to_string(),
        &project,
        &["udb:admin"],
    )
    .await;
    let foreign_project_bearer =
        fixture_bearer(&control, &authn, &security, &tenant, &foreign_project, &["udb:admin"]).await;
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
    // The canonical unique:true producer emits a standalone UNIQUE index.
    // Verify that untouched producer shape through the real signed workflow
    // before deliberately adapting this owned fixture for constraint-only cases.
    let mut producer_unique_indexes: Vec<(String, i64)> = sqlx::query_as(
        "SELECT x.relname::TEXT, x.oid::BIGINT FROM pg_catalog.pg_index i
         JOIN pg_catalog.pg_class r ON r.oid=i.indrelid
         JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
         JOIN pg_catalog.pg_class x ON x.oid=i.indexrelid
         JOIN pg_catalog.pg_am am ON am.oid=x.relam
         JOIN pg_catalog.pg_attribute a ON a.attrelid=r.oid AND a.attnum=i.indkey[0]
         WHERE n.nspname::TEXT=$1 AND r.relname::TEXT='records' AND a.attname='external_key'
         AND am.amname='btree' AND i.indnkeyatts=1 AND i.indnatts=1
         AND i.indisunique AND NOT i.indisprimary AND i.indimmediate
         AND i.indisvalid AND i.indisready AND i.indislive
         AND i.indpred IS NULL AND i.indexprs IS NULL
         AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint c WHERE c.conindid=i.indexrelid)",
    )
    .bind(&schema).fetch_all(&target).await.expect("discover actual producer standalone UNIQUE index");
    assert_eq!(producer_unique_indexes.len(), 1, "owned external_key must have exactly one standalone producer UNIQUE index");
    let (unique_name, producer_unique_oid) = producer_unique_indexes.pop().unwrap();
    assert!(unique_name.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'));
    assert_native_unique_base(&mut served.client, &owner, &base, &candidate_bytes, &target,
        "standalone-producer-unique").await;
    sqlx::raw_sql(&format!(
        "ALTER TABLE \"{schema}\".records ADD CONSTRAINT \"{unique_name}\" UNIQUE USING INDEX \"{unique_name}\""
    ))
    .execute(&target).await.expect("deliberately promote only the owned producer UNIQUE index for constraint controls");
    let promoted: (i64, bool, bool, bool, bool) = sqlx::query_as(
        "SELECT c.conindid::BIGINT, c.condeferrable, c.condeferred, c.convalidated, i.indimmediate
         FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_class r ON r.oid=c.conrelid
         JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace
         JOIN pg_catalog.pg_index i ON i.indexrelid=c.conindid
         WHERE n.nspname::TEXT=$1 AND r.relname::TEXT='records' AND c.contype='u' AND c.conname::TEXT=$2",
    )
    .bind(&schema).bind(&unique_name).fetch_one(&target).await.expect("read actual owned UNIQUE promotion authority");
    assert_eq!(promoted, (producer_unique_oid, false, false, true, true),
        "deliberate fixture promotion must retain the producer index OID and immediate validated uniqueness");
    assert_native_unique_base(&mut served.client, &owner, &base, &candidate_bytes, &target,
        "owned-promoted-unique").await;
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

// BEGIN CAPABILITIES_HISTORY_CI_PROFILE
// Uses only production APIs available at 6983dce4. CI overlays these same bytes
// on A/B/A, restores the same named DB from its prepared template, and compares
// exact row receipts; physical database OIDs are recorded after every restore.
// This separate test name is outside the reviewed-transition capacity filters.
fn capabilities_profile_project(namespace: Uuid, histories: u8) -> String {
    let mut bytes = *namespace.as_bytes();
    bytes[15] ^= histories;
    Uuid::from_bytes(bytes).to_string()
}

fn capabilities_profile_source(tables: usize) -> String {
    assert!(
        (2..=512).contains(&tables),
        "profile table count must be 2..512"
    );
    let mut proto = source("udb_capabilities_history");
    for table in 2..tables {
        proto.push_str(&format!(r#"
message HistoryTable{table} {{
  option (udb.core.common.v1.pg_table) = {{
    table_name: "history_{table}" schema_name: "udb_capabilities_history" is_table: true
    audit_fields: false enable_rls: true force_rls: true
  }};
  option (udb.core.common.v1.db_table_security) = {{
    tenant_isolation_mode: "row" tenant_column: "tenant_id"
    project_isolation_mode: "column" project_column: "project_id"
    soft_delete_mode: "none" audit_mode: AUDIT_MODE_NONE
    encryption_profile: "none" pii_profile: "none" export_eligible: false
  }};
  string record_id = 1 [(udb.core.common.v1.pg_column) = {{sql_type:"VARCHAR(80)" primary_key:true not_null:true}}];
  string tenant_id = 2 [(udb.core.common.v1.pg_column) = {{sql_type:"VARCHAR(80)" tenant_column:true not_null:true}}];
  string project_id = 3 [(udb.core.common.v1.pg_column) = {{sql_type:"VARCHAR(80)" project_column:true not_null:true}}];
}}
"#));
    }
    proto
}

fn capabilities_profile_security() -> SecurityConfig {
    SecurityConfig {
        tls_required: false,
        mtls_required: false,
        service_identity_required: false,
        allow_header_scopes: false,
        jwt_private_key: Some(include_str!("../../testdata/jwt_rs256_private.pem").into()),
        jwt_public_key: Some(include_str!("../../testdata/jwt_rs256_public.pem").into()),
        ..SecurityConfig::default()
    }
}

fn capabilities_profile_config(dsn: &str, security: &SecurityConfig) -> UdbConfig {
    let mut config = UdbConfig::from_env();
    config.primary.direct_dsn = dsn.into();
    config.primary.max_open_conns = 4;
    config.primary.min_connections = 1;
    config.primary.acquire_timeout_secs = 5;
    config.project_routing_mode = "permissive".into();
    config.security = security.clone();
    config.service.catalog_compatibility_level = "backward".into();
    config.service.abac_default_allow = false;
    config.backend_instances = BackendInstanceConfig { instances: vec![] };
    config
}

fn capabilities_profile_authn(
    serving_pool: &sqlx::PgPool,
    service: &DataBrokerService,
    security: &SecurityConfig,
    namespace: Uuid,
) -> (AuthnServiceImpl, AuthnConfig) {
    let authn_config = AuthnConfig {
        session_hash_secret: format!("owned-capabilities-profile-{namespace}"),
        ..AuthnConfig::from_env()
    };
    let authn = AuthnServiceImpl::with_stores(
        authn_config.clone(),
        security.clone(),
        Arc::new(PostgresSessionStore::new(serving_pool.clone(), "")),
        Arc::new(PostgresApiKeyStore::new(serving_pool.clone(), "")),
        Arc::new(PostgresUserStore::new(serving_pool.clone(), "")),
    )
    .with_runtime(Some(service.runtime_snapshot()))
    .with_authz_snapshot(Some(service.authz_snapshot()));
    (authn, authn_config)
}

async fn capabilities_profile_serve(
    service: DataBrokerService,
    authn: AuthnServiceImpl,
    message_limit: usize,
) -> (
    Serving,
    authn::authn_service_client::AuthnServiceClient<tonic::transport::Channel>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let incoming = futures::stream::unfold(listener, |listener| async move {
        Some((listener.accept().await.map(|(stream, _)| stream), listener))
    });
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
    let broker = service.clone();
    let native = crate::runtime::service::method_security::MethodSecurityLayer::new()
        .wrap(authn::authn_service_server::AuthnServiceServer::new(authn));
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(CredentialResolveLayer::new())
            .add_service(
                DataBrokerServer::new(broker)
                    .max_decoding_message_size(message_limit)
                    .max_encoding_message_size(message_limit),
            )
            .add_service(native)
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve actual matched broker and native Authn routes");
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let login = authn::authn_service_client::AuthnServiceClient::new(channel.clone())
        .max_decoding_message_size(message_limit)
        .max_encoding_message_size(message_limit);
    (
        Serving {
            client: DataBrokerClient::new(channel)
                .max_decoding_message_size(message_limit)
                .max_encoding_message_size(message_limit),
            service,
            shutdown: Some(shutdown),
            task: Some(task),
        },
        login,
    )
}

async fn capabilities_profile_username(
    control: &sqlx::PgPool,
    namespace: Uuid,
    project: &str,
    actor: &str,
) -> String {
    let users = native_catalog::native_model(
        "udb.core.authn.entity.v1.User",
        &[
            "user_id",
            "username",
            "created_by",
            "project_id",
            "tenant_id",
            "status",
        ],
    );
    sqlx::query_scalar(&format!(
        "SELECT {} FROM {} WHERE {}=$1::UUID AND {}=$2 AND {}=$3 AND {} IS NOT NULL AND {}='ACTIVE'",
        users.q("username"), users.relation, users.q("user_id"), users.q("tenant_id"),
        users.q("project_id"), users.q("created_by"), users.q("status"),
    )).bind(actor).bind(namespace.to_string()).bind(project).fetch_one(control)
        .await.expect("read username of the exact prepared ACTIVE native PERSON")
}

// In-flight intervals cover actual client RPC await, including serving admission
// and SQL/KDF queueing. They do not establish that CPU work runs throughout.
struct CapabilitiesProfileTraffic {
    origin: std::time::Instant,
    active: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
    login_active: std::sync::atomic::AtomicUsize,
    login_peak: std::sync::atomic::AtomicUsize,
}
// A failed workload must not detach its pool monitor. Aborting on unwind is
// scoped to this fixture's owned task; normal completion still joins it.
struct CapabilitiesProfileAbortOnDrop(tokio::task::AbortHandle);
impl Drop for CapabilitiesProfileAbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl CapabilitiesProfileTraffic {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            origin: std::time::Instant::now(),
            active: 0.into(),
            peak: 0.into(),
            login_active: 0.into(),
            login_peak: 0.into(),
        })
    }
    fn begin(
        self: &Arc<Self>,
        rpc: &'static str,
        worker: usize,
        attempt: usize,
    ) -> CapabilitiesProfileFlight {
        use std::sync::atomic::Ordering;
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        assert!(
            active <= 4,
            "matched workload must cap actual total client in-flight calls at four"
        );
        if rpc == "Login" {
            let active = self.login_active.fetch_add(1, Ordering::SeqCst) + 1;
            self.login_peak.fetch_max(active, Ordering::SeqCst);
            assert!(
                active <= 2,
                "matched workload must cap actual Login in-flight calls at two"
            );
        }
        CapabilitiesProfileFlight {
            traffic: self.clone(),
            rpc,
            worker,
            attempt,
            start_us: u64::try_from(self.origin.elapsed().as_micros()).unwrap(),
            login_at_start: self.login_active.load(Ordering::SeqCst),
        }
    }
}
struct CapabilitiesProfileFlight {
    traffic: Arc<CapabilitiesProfileTraffic>,
    rpc: &'static str,
    worker: usize,
    attempt: usize,
    start_us: u64,
    login_at_start: usize,
}
impl CapabilitiesProfileFlight {
    fn finish(self, code: Option<Code>) -> serde_json::Value {
        use std::sync::atomic::Ordering;
        let end_us = u64::try_from(self.traffic.origin.elapsed().as_micros()).unwrap();
        serde_json::json!({
            "rpc":self.rpc,"worker":self.worker,"attempt":self.attempt,
            "start_us":self.start_us,"end_us":end_us,"latency_us":end_us-self.start_us,
            "ok":code.is_none(),"code":code.map(|code|format!("{code:?}")),
            "login_inflight_at_start":self.login_at_start,
            "login_inflight_at_end":self.traffic.login_active.load(Ordering::SeqCst),
        })
    }
}
impl Drop for CapabilitiesProfileFlight {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering;
        if self.rpc == "Login" {
            self.traffic.login_active.fetch_sub(1, Ordering::SeqCst);
        }
        self.traffic.active.fetch_sub(1, Ordering::SeqCst);
    }
}

fn capabilities_profile_emit(value: serde_json::Value) {
    use sha2::Digest;
    let encoded = serde_json::to_vec(&value).unwrap();
    if let Ok(directory) = std::env::var("UDB_CAPABILITIES_PROFILE_RECEIPTS_DIR") {
        std::fs::create_dir_all(&directory).unwrap();
        let file = format!("history-{}.json", value["history_count"].as_u64().unwrap());
        std::fs::write(std::path::Path::new(&directory).join(&file), &encoded).unwrap();
        println!(
            "CAPABILITIES_HISTORY_PROFILE {}",
            serde_json::json!({
                "history_count":value["history_count"],"receipt_file":file,
                "receipt_sha256":format!("{:x}",sha2::Sha256::digest(&encoded)),
            })
        );
    } else {
        println!("CAPABILITIES_HISTORY_PROFILE {value}");
    }
}

async fn capabilities_profile_actor(
    control: &sqlx::PgPool,
    namespace: Uuid,
    project: &str,
) -> String {
    let users = native_catalog::native_model(
        "udb.core.authn.entity.v1.User",
        &["user_id", "created_by", "project_id", "tenant_id", "status"],
    );
    let actors: Vec<Uuid> = sqlx::query_scalar(&format!(
        "SELECT {} FROM {} WHERE {}=$1 AND {}=$2 AND {} IS NOT NULL AND {}='ACTIVE'",
        users.q("user_id"),
        users.relation,
        users.q("project_id"),
        users.q("tenant_id"),
        users.q("created_by"),
        users.q("status"),
    ))
    .bind(project)
    .bind(namespace.to_string())
    .fetch_all(control)
    .await
    .expect("read exact durable active profile operator");
    assert_eq!(
        actors.len(),
        1,
        "profile must use the one prepared PERSON operator"
    );
    actors[0].to_string()
}

fn capabilities_profile_bearer(
    security: &SecurityConfig,
    namespace: Uuid,
    project: &str,
    actor: &str,
    scopes: &[&str],
) -> String {
    let scopes = scopes
        .iter()
        .map(|scope| scope.to_string())
        .collect::<Vec<_>>();
    sign_access_token(
        security,
        actor,
        &namespace.to_string(),
        project,
        &scopes,
        &[],
        "",
        &format!("capabilities-history-{}", Uuid::new_v4()),
        "password",
        authn_entity::AccountKind::Person as i32,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
    .expect("sign the prepared native PERSON profile operator")
    .expect("profile signing key is configured")
    .0
}

async fn capabilities_profile_history_receipt(
    control: &sqlx::PgPool,
    namespace: Uuid,
    project: &str,
    histories: usize,
) -> serde_json::Value {
    use sha2::Digest;
    let relation = SystemCatalogConfig::default().catalog_versions_relation();
    let rows: Vec<(
        String,
        String,
        String,
        String,
        i64,
        i64,
        i64,
        i64,
        String,
        String,
    )> = sqlx::query_as(&format!(
        "SELECT catalog_id::TEXT, version, status, manifest_integrity_sha256,
                octet_length(manifest_json::TEXT)::BIGINT,
                jsonb_array_length(manifest_json->'tables')::BIGINT,
                jsonb_array_length(manifest_json->'stores')::BIGINT,
                COALESCE((SELECT SUM(jsonb_array_length(t->'columns'))
                  FROM jsonb_array_elements(manifest_json->'tables') AS t),0)::BIGINT,
                encode(sha256(convert_to(manifest_json::TEXT,'UTF8')),'hex'), created_at::TEXT
           FROM {relation} WHERE project_id=$1 ORDER BY created_at DESC"
    ))
    .bind(project)
    .fetch_all(control)
    .await
    .expect("read actual profile history rows and payload bytes");
    assert_eq!(
        rows.len(),
        histories,
        "profile history count must match the prepared dataset"
    );
    assert_eq!(rows.iter().filter(|row| row.2 == "ACTIVE").count(), 1);
    assert!(rows.iter().all(|row| row.3.len() == 64 && row.4 > 0));
    let labels = rows
        .iter()
        .map(|row| format!("project:{project}:catalog:{}", row.1))
        .collect::<Vec<_>>();
    serde_json::json!({
        "fixture_tenant_id": namespace.to_string(),
        "project_id":project,"row_count": rows.len(), "manifest_json_text_bytes": rows.iter().map(|row| row.4).sum::<i64>(),
        "version_text_bytes":rows.iter().map(|row|row.1.len()).sum::<usize>(),
        "retained_table_definitions": rows.iter().map(|row| row.5).sum::<i64>(),
        "retained_store_definitions": rows.iter().map(|row| row.6).sum::<i64>(),
        "retained_column_definitions": rows.iter().map(|row| row.7).sum::<i64>(),
        "rows_sha256": format!("{:x}", sha2::Sha256::digest(serde_json::to_vec(&rows).unwrap())),
        "ordered_labels": labels,
    })
}

async fn capabilities_profile_read(
    client: &mut DataBrokerClient<tonic::transport::Channel>,
    service: &DataBrokerService,
    bearer: &str,
    project: &str,
    receipt: &serde_json::Value,
    rpc: &'static str,
    traffic: &Arc<CapabilitiesProfileTraffic>,
    attempt: usize,
) -> serde_json::Value {
    let flight = traffic.begin(rpc, usize::from(rpc == "Select"), attempt);
    if rpc == "GetCapabilities" {
        let result = client
            .get_capabilities(request(
                crate::proto::CapabilitiesRequest {
                    project_id: project.into(),
                    ..Default::default()
                },
                bearer,
            ))
            .await;
        let mut sample = flight.finish(result.as_ref().err().map(Status::code));
        if let Ok(response) = result {
            let response = response.into_inner();
            sample["response_protobuf_bytes"] =
                serde_json::json!(prost::Message::encoded_len(&response));
            let expected = receipt["ordered_labels"]
                .as_array()
                .unwrap()
                .iter()
                .map(|label| label.as_str().unwrap().to_string())
                .collect::<Vec<_>>();
            let actual = response
                .system_catalog_relations
                .iter()
                .filter(|label| label.starts_with("project:"))
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(
                actual, expected,
                "capabilities preserves exact-project history labels and order"
            );
            let active = service
                .catalog
                .active_exact_for(project)
                .expect("fresh exact ACTIVE profile catalog");
            assert_eq!(response.schema_checksum, active.manifest.checksum_sha256);
            assert!(service.catalog.authority_is_fresh());
            let expected = service
                .runtime_snapshot()
                .backend_instances_for_project(project)
                .into_iter()
                .map(super::super::backend_instance_status)
                .collect::<Vec<_>>();
            assert_eq!(
                response.backend_instances, expected,
                "capabilities must retain current project-routed backend state"
            );
        }
        sample
    } else {
        assert_eq!(rpc, "Select");
        let result = client
            .select(request(
                crate::proto::SelectRequest {
                    message_type: "reviewed.catalog.live.v1.Receipt".into(),
                    fields: vec!["record_id".into(), "tenant_id".into(), "project_id".into()],
                    limit: 1,
                    cache: Some(crate::proto::CacheOptions {
                        bypass_read: true,
                        bypass_write: true,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                bearer,
            ))
            .await;
        let mut sample = flight.finish(result.as_ref().err().map(Status::code));
        if let Ok(response) = result {
            let response = response.into_inner();
            sample["response_protobuf_bytes"] =
                serde_json::json!(prost::Message::encoded_len(&response));
            assert_eq!(
                response.records_json.len(),
                1,
                "matched Select must read its real scoped fixture row"
            );
            let row: serde_json::Value = serde_json::from_slice(&response.records_json[0]).unwrap();
            assert_eq!(
                row["tenant_id"].as_str(),
                Some(receipt["fixture_tenant_id"].as_str().unwrap())
            );
            assert_eq!(
                row["project_id"].as_str(),
                Some(project),
                "matched Select retains exact project scope"
            );
        }
        sample
    }
}

async fn capabilities_profile_login(
    client: &mut authn::authn_service_client::AuthnServiceClient<tonic::transport::Channel>,
    security: &SecurityConfig,
    namespace: Uuid,
    actor: &str,
    username: &str,
    project: &str,
    traffic: &Arc<CapabilitiesProfileTraffic>,
    worker: usize,
    attempt: usize,
    ready: Option<&tokio::sync::watch::Sender<usize>>,
) -> serde_json::Value {
    let flight = traffic.begin("Login", worker, attempt);
    if let Some(ready) = ready {
        ready.send_modify(|count| *count += 1);
    }
    let mut request = Request::new(authn::LoginRequest {
        username: username.into(),
        password: "FixturePassword1!".into(),
        ..Default::default()
    });
    request.set_timeout(Duration::from_secs(30));
    let result = client.login(request).await;
    let sample = flight.finish(result.as_ref().err().map(Status::code));
    if let Ok(response) = result {
        let response = response.into_inner();
        assert_eq!(
            response.user_id, actor,
            "real Login must authenticate the exact prepared PERSON"
        );
        assert!(!response.session_id.is_empty());
        let claims =
            crate::runtime::security::validate_bearer_token(security, &response.access_token)
                .expect("actual native Login access token verifies");
        assert_eq!(claims.sub.as_deref(), Some(actor));
        assert_eq!(
            claims.tenant_id.as_deref(),
            Some(namespace.to_string().as_str())
        );
        assert_eq!(claims.project_id.as_deref(), Some(project));
        assert_eq!(
            claims.account_kind,
            Some(authn_entity::AccountKind::Person as i32)
        );
    }
    sample
}

async fn capabilities_profile_pair(
    served: &mut Serving,
    bearer: &str,
    project: &str,
    receipt: &serde_json::Value,
    first: bool,
    traffic: &Arc<CapabilitiesProfileTraffic>,
    attempt: usize,
) -> Vec<serde_json::Value> {
    let order = if first {
        ["GetCapabilities", "Select"]
    } else {
        ["Select", "GetCapabilities"]
    };
    let mut rows = Vec::with_capacity(2);
    for rpc in order {
        rows.push(
            capabilities_profile_read(
                &mut served.client,
                &served.service,
                bearer,
                project,
                receipt,
                rpc,
                traffic,
                attempt,
            )
            .await,
        );
    }
    rows
}

async fn capabilities_profile_burst(
    served: &Serving,
    login: &authn::authn_service_client::AuthnServiceClient<tonic::transport::Channel>,
    security: &SecurityConfig,
    namespace: Uuid,
    actor: &str,
    username: &str,
    project: &str,
    receipt: &serde_json::Value,
    attempts: usize,
    login_attempts: usize,
) -> serde_json::Value {
    use std::sync::atomic::Ordering;
    let traffic = CapabilitiesProfileTraffic::new();
    let barrier = Arc::new(tokio::sync::Barrier::new(5));
    let (ready_tx, ready_rx) = tokio::sync::watch::channel(0usize);
    let mut tasks = tokio::task::JoinSet::new();
    for worker in 0..2 {
        let mut client = login.clone();
        let traffic = traffic.clone();
        let barrier = barrier.clone();
        let security = security.clone();
        let actor = actor.to_string();
        let username = username.to_string();
        let project = project.to_string();
        let ready = ready_tx.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            let mut rows = Vec::with_capacity(login_attempts);
            for attempt in 0..login_attempts {
                rows.push(
                    capabilities_profile_login(
                        &mut client,
                        &security,
                        namespace,
                        &actor,
                        &username,
                        &project,
                        &traffic,
                        worker,
                        attempt,
                        if attempt == 0 { Some(&ready) } else { None },
                    )
                    .await,
                );
            }
            rows
        });
    }
    for rpc in ["GetCapabilities", "Select"] {
        let mut client = served.client.clone();
        let service = served.service.clone();
        let barrier = barrier.clone();
        let traffic = traffic.clone();
        let bearer = capabilities_profile_bearer(
            security,
            namespace,
            project,
            actor,
            &["udb:admin", "udb:read"],
        );
        let project = project.to_string();
        let receipt = receipt.clone();
        let mut ready = ready_rx.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            while *ready.borrow() < 2 {
                ready
                    .changed()
                    .await
                    .expect("both real Login workers must begin");
            }
            let mut rows = Vec::with_capacity(attempts);
            for attempt in 0..attempts {
                rows.push(
                    capabilities_profile_read(
                        &mut client,
                        &service,
                        &bearer,
                        &project,
                        &receipt,
                        rpc,
                        &traffic,
                        attempt,
                    )
                    .await,
                );
            }
            rows
        });
    }
    let serving_pool = served.service.runtime_snapshot().pg_pool_clone().unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let done = stop.clone();
    let observed_traffic = traffic.clone();
    let monitor = tokio::spawn(async move {
        let mut snapshots = Vec::new();
        while !done.load(Ordering::Acquire) {
            snapshots.push(
                serde_json::json!({"at_us":u64::try_from(observed_traffic.origin.elapsed().as_micros()).unwrap(),
                    "size":serving_pool.size(),"idle":serving_pool.num_idle()}),
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        snapshots
    });
    let _monitor_guard = CapabilitiesProfileAbortOnDrop(monitor.abort_handle());
    barrier.wait().await;
    let process_before = capabilities_profile_process_receipt();
    let work = async {
        let mut rows = Vec::with_capacity(2 * attempts + 2 * login_attempts);
        while let Some(result) = tasks.join_next().await {
            rows.extend(result.expect("real capped workload worker"));
        }
        rows
    };
    let samples = tokio::time::timeout(Duration::from_secs(600), work).await;
    stop.store(true, Ordering::Release);
    let snapshots = monitor.await.unwrap();
    let samples =
        samples.expect("fixed four-worker workload must finish within its explicit bound");
    assert_eq!(traffic.active.load(Ordering::SeqCst), 0);
    assert_eq!(traffic.login_active.load(Ordering::SeqCst), 0);
    let overlap = |rpc| {
        samples
            .iter()
            .filter(|row| {
                if row["rpc"] != rpc || row["ok"] != true {
                    return false;
                }
                let start = row["start_us"].as_u64().unwrap();
                let end = row["end_us"].as_u64().unwrap();
                let contains = |point, end_point| {
                    samples.iter().any(|login| {
                        if login["rpc"] != "Login" {
                            return false;
                        }
                        let first = login["start_us"].as_u64().unwrap();
                        let last = login["end_us"].as_u64().unwrap();
                        if end_point {
                            first < point && point <= last
                        } else {
                            first <= point && point < last
                        }
                    })
                };
                contains(start, false) && contains(end, true)
            })
            .count()
    };
    serde_json::json!({
        "samples":samples,"read_attempts_per_rpc":attempts,"login_workers":2,
        "login_attempts_per_worker":login_attempts,"closed_loop_workers":4,
        "maximum_client_inflight":traffic.peak.load(Ordering::SeqCst),
        "maximum_login_inflight":traffic.login_peak.load(Ordering::SeqCst),
        "overlapping_capabilities_successes":overlap("GetCapabilities"),
        "overlapping_select_successes":overlap("Select"),"pool_snapshots":snapshots,
        "process_before":process_before,"process_after":capabilities_profile_process_receipt(),
    })
}

fn capabilities_profile_process_receipt() -> serde_json::Value {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let fields = stat
        .rsplit_once(')')
        .map(|(_, rest)| rest.split_whitespace().collect::<Vec<_>>())
        .unwrap_or_default();
    let value = |index| {
        fields
            .get(index)
            .and_then(|value: &&str| value.parse::<u64>().ok())
    };
    serde_json::json!({
        "pid": std::process::id(), "available_parallelism": std::thread::available_parallelism().map(|count| count.get()).ok(),
        "user_cpu_ticks": value(11), "system_cpu_ticks": value(12), "process_start_ticks": value(19),
        "threads": status.lines().find_map(|line| line.strip_prefix("Threads:")).map(str::trim),
        "rss": status.lines().find_map(|line| line.strip_prefix("VmRSS:")).map(str::trim),
    })
}

// A/B/A compiles once per source variant. prepare retains one immutable dataset;
// the CI runner snapshots/restores its owned DB before each saved binary runs.
// The separate sample artifacts never enter canonical SDK benchmark accounting.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires actual Postgres/CREATE DATABASE; matched history proof is CI only"]
async fn live_capabilities_catalog_history_profile() {
    let dsn = require_live_dsn_any(&[
        "UDB_LIVE_NATIVE_PG_DSN",
        "UDB_LIVE_AUTH_PG_DSN",
        "UDB_INTEGRATION_PG_DSN",
        "UDB_PG_DSN",
    ])
    .expect("CAPABILITIES_HISTORY_PROFILE requires an actual PostgreSQL fixture");
    let mode =
        std::env::var("UDB_CAPABILITIES_PROFILE_MODE").unwrap_or_else(|_| "correctness".into());
    assert!(matches!(
        mode.as_str(),
        "full" | "prepare" | "sample" | "cleanup" | "correctness"
    ));
    let namespace = match std::env::var("UDB_CAPABILITIES_PROFILE_NAMESPACE") {
        Ok(value) => Uuid::parse_str(&value).expect("profile namespace must be a UUID"),
        Err(_) if mode == "full" || mode == "correctness" => Uuid::new_v4(),
        Err(_) => panic!("A/B/A requires the same explicit owned profile namespace"),
    };
    let table_count = std::env::var("UDB_CAPABILITIES_PROFILE_TABLES")
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(2);
    let attempts = std::env::var("UDB_CAPABILITIES_PROFILE_ATTEMPTS")
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(50);
    let login_attempts = std::env::var("UDB_CAPABILITIES_PROFILE_LOGIN_ATTEMPTS")
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(64);
    assert!((50..=1000).contains(&attempts));
    assert!((64..=2048).contains(&login_attempts));
    let _lock = live_native_service_db_lock().lock().await;
    let _restore = SecurityRestore(SecurityConfig::current());
    let administration = pool(&dsn).await;
    let database = format!("udb_cap_hist_{}", namespace.simple());
    if mode == "cleanup" {
        sqlx::query(&format!("DROP DATABASE \"{database}\" WITH (FORCE)"))
            .execute(&administration)
            .await
            .expect("remove only the owned profile DB");
        administration.close().await;
        return;
    }
    let prepare = matches!(mode.as_str(), "full" | "prepare" | "correctness");
    if prepare {
        sqlx::query(&format!("CREATE DATABASE \"{database}\""))
            .execute(&administration)
            .await
            .expect("create the owned immutable profile DB");
    }
    let owned_dsn = project_dsn(&dsn, &database);
    let control = pool(&owned_dsn).await;
    let result=std::panic::AssertUnwindSafe(async {
        let (manifest,schemas)=parsed(&capabilities_profile_source(table_count));
        if prepare {
            super::support::migrate_native_service_db(&control).await;
            for ddl in generate_bootstrap_sql(&schemas,&SqlGenerationConfig::default()).unwrap() {
                if ddl.schema=="udb_capabilities_history" {
                    sqlx::raw_sql(&ddl.content).execute(&control).await.expect("apply real profile producer DDL");
                }
            }
        }
        let security=capabilities_profile_security();
        let config=capabilities_profile_config(&owned_dsn,&security);
        let message_limit=config.service.grpc_max_message_bytes;
        let budgets=serde_json::json!({
            "pg_pool_max":config.primary.max_open_conns,"pg_pool_min":config.primary.min_connections,
            "pg_acquire_timeout_secs":config.primary.acquire_timeout_secs,"tokio_worker_threads":2,
            "channels":config.channels,"grpc_timeout_secs":config.service.grpc_timeout_secs,
            "grpc_max_concurrent":config.service.grpc_max_concurrent,"grpc_max_message_bytes":message_limit,
            "rate_limit_enabled":config.service.rate_limit_enabled,
            "rate_limit_window_secs":config.service.rate_limit_window_secs,
            "rate_limit_max_per_window":config.service.rate_limit_max_per_window,
            "login_rate_limit_per_minute":std::env::var("UDB_RATE_LIMIT_POLICY_AUTHN_LOGIN_PUBLIC").ok(),
            "login_abuse_limit_per_minute":std::env::var("UDB_ABUSE_POLICY_AUTHN_LOGIN_ABUSE").ok(),
            "password_kdf_operator_cap":std::env::var("UDB_PASSWORD_KDF_MAX_CONCURRENCY").ok(),
        });
        let service=build_service(config).await;
        let runtime=service.runtime_snapshot();
        let serving_pool=runtime.pg_pool_clone().unwrap();
        assert_eq!(serving_pool.options().get_max_connections(),4,
            "all native authn and DataBroker requests share the actual four-connection pool");
        let (authn,authn_config)=capabilities_profile_authn(&serving_pool,&service,&security,namespace);
        if prepare {
            for histories in [1u8,8,32] {
                let project=capabilities_profile_project(namespace,histories);
                let bearer=fixture_bearer(&control,&authn,&security,&namespace.to_string(),&project,
                    &["udb:admin","udb:read"]).await;
                let actor=crate::runtime::security::validate_bearer_token(&security,&bearer).unwrap().sub.unwrap();
                super::authz_deny_path_live::insert_allow_rule(&control,&namespace.to_string(),&project,
                    &actor,"reviewed.catalog.live.v1.Receipt","Select").await;
                sqlx::query("INSERT INTO udb_capabilities_history.records \
                    (record_id,tenant_id,project_id,lookup_key,round_item_id) VALUES($1,$2,$3,'owned','owned')")
                    .bind(format!("profile-{histories}")).bind(namespace.to_string()).bind(&project)
                    .execute(&control).await.expect("seed scoped matched Select row");
            }
        }
        crate::runtime::service::auth_service::install_data_plane_credential_resolvers(
            serving_pool.clone(),&authn_config,
            Arc::new(authn.clone().with_runtime(None).with_authz_snapshot(None)),
        );
        let (_,authz,_)=service.build_auth_services();authz.warm_shared_snapshot().await;
        let (mut served,mut login)=capabilities_profile_serve(service,authn,message_limit).await;
        if prepare {
            for histories in [1u8,8,32] {
                let project=capabilities_profile_project(namespace,histories);
                let actor=capabilities_profile_actor(&control,namespace,&project).await;
                let bearer=capabilities_profile_bearer(&security,namespace,&project,&actor,&["udb:admin","udb:read"]);
                for history in 0..histories {
                    let mut value=serde_json::to_value(&manifest).unwrap();
                    value["version"]=serde_json::json!(format!("1.0.{history}"));
                    let staged=stage(&mut served.client,&bearer,&project,&serde_json::to_vec(&value).unwrap(),"",
                        &format!("profile-stage-{history}")).await.expect("prepare real served catalog history");
                    if history==0 { activate(&mut served.client,&bearer,&staged,"","profile-base-activate")
                        .await.expect("prepare real served exact ACTIVE profile catalog"); }
                }
                let dataset=capabilities_profile_history_receipt(&control,namespace,&project,histories as usize).await;
                capabilities_profile_emit(serde_json::json!({"mode":"prepare","history_count":histories,
                    "customer_table_count":table_count,"dataset":dataset,"pool_max":4,"budgets":budgets}));
            }
        }
        if mode=="sample" || mode=="full" {
            for histories in [1usize,8,32] {
                let project=capabilities_profile_project(namespace,histories as u8);
                let actor=capabilities_profile_actor(&control,namespace,&project).await;
                let username=capabilities_profile_username(&control,namespace,&project,&actor).await;
                let bearer=capabilities_profile_bearer(&security,namespace,&project,&actor,&["udb:admin","udb:read"]);
                let before=capabilities_profile_history_receipt(&control,namespace,&project,histories).await;
                let warm=CapabilitiesProfileTraffic::new();
                for attempt in 0..5 {
                    let rows=capabilities_profile_pair(&mut served,&bearer,&project,&before,attempt%2==0,&warm,attempt).await;
                    assert!(rows.iter().all(|row|row["ok"]==true),"warm both real read routes");
                }
                for attempt in 0..2 {
                    let row=capabilities_profile_login(&mut login,&security,namespace,&actor,&username,&project,
                        &warm,0,attempt,None).await;
                    assert_eq!(row["ok"],true,"warm real Argon2 Login before timing");
                }
                let idle=CapabilitiesProfileTraffic::new();
                let process_before=capabilities_profile_process_receipt();
                let mut idle_samples=Vec::with_capacity(attempts*2);
                for attempt in 0..attempts {
                    idle_samples.extend(capabilities_profile_pair(&mut served,&bearer,&project,&before,
                        attempt%2==0,&idle,attempt).await);
                }
                let idle_process_after=capabilities_profile_process_receipt();
                let burst=capabilities_profile_burst(&served,&login,&security,namespace,&actor,&username,
                    &project,&before,attempts,login_attempts).await;
                let after=capabilities_profile_history_receipt(&control,namespace,&project,histories).await;
                assert_eq!(before,after,"measured history rows and bytes must remain identical");
                let value=serde_json::json!({"mode":"sample","history_count":histories,
                    "customer_table_count":table_count,"attempts_per_rpc":attempts,"dataset":before,
                    "actor_id":actor,"budgets":budgets,"idle":{"samples":idle_samples,"process_before":process_before,
                        "process_after":idle_process_after},"burst":burst,
                    "pool_max":serving_pool.options().get_max_connections(),"pool_size":serving_pool.size(),
                    "pool_idle":serving_pool.num_idle()});
                capabilities_profile_emit(value.clone());
                assert!(idle_samples.iter().all(|row|row["ok"]==true),"all fixed idle RPC attempts must succeed");
                let burst_samples=value["burst"]["samples"].as_array().unwrap();
                assert_eq!(burst_samples.len(),2*attempts+2*login_attempts);
                assert!(burst_samples.iter().all(|row|row["ok"]==true),"all fixed burst RPC attempts must succeed");
                assert_eq!(value["burst"]["overlapping_capabilities_successes"],serde_json::json!(attempts),
                    "all measured GetCapabilities calls must begin and finish while real Login RPCs are in flight");
                assert_eq!(value["burst"]["overlapping_select_successes"],serde_json::json!(attempts),
                    "all measured Select calls must begin and finish while real Login RPCs are in flight");
            }
        }
        if mode=="full" || mode=="correctness" {
            capabilities_history_correctness(&mut served,&control,namespace,&security,&manifest).await;
        }
        drop(login);served.stop().await;
        tokio::time::timeout(Duration::from_secs(10),serving_pool.close()).await.expect("close actual serving pool");
    }).catch_unwind().await;
    control.close().await;
    if mode == "full" || mode == "correctness" || result.is_err() {
        let removed = sqlx::query(&format!("DROP DATABASE \"{database}\" WITH (FORCE)"))
            .execute(&administration)
            .await;
        if result.is_ok() {
            removed.expect("remove only the completed owned profile DB");
        }
    }
    administration.close().await;
    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}

async fn capabilities_history_correctness(
    served: &mut Serving,
    control: &sqlx::PgPool,
    namespace: Uuid,
    security: &SecurityConfig,
    manifest: &CatalogManifest,
) {
    let project = capabilities_profile_project(namespace, 1);
    let actor = capabilities_profile_actor(control, namespace, &project).await;
    let owner = capabilities_profile_bearer(
        security,
        namespace,
        &project,
        &actor,
        &["udb:admin", "udb:read"],
    );
    let reader = capabilities_profile_bearer(security, namespace, &project, &actor, &["udb:read"]);
    assert_eq!(
        served
            .client
            .get_capabilities(request(
                crate::proto::CapabilitiesRequest {
                    project_id: project.clone(),
                    ..Default::default()
                },
                &reader
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied,
        "capabilities must perform its admin authorization on each call"
    );
    assert_eq!(
        served
            .client
            .get_capabilities(request(
                crate::proto::CapabilitiesRequest {
                    project_id: capabilities_profile_project(namespace, 8),
                    ..Default::default()
                },
                &owner
            ))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied,
        "history cannot widen project authority"
    );
    let active_version = served
        .service
        .catalog
        .active_exact_for(&project)
        .unwrap()
        .metadata
        .version
        .clone();
    let mut bytes = serde_json::to_value(manifest).unwrap();
    bytes["version"] = serde_json::json!("1.0.99");
    let staged = stage(
        &mut served.client,
        &owner,
        &project,
        &serde_json::to_vec(&bytes).unwrap(),
        "",
        "fresh-history",
    )
    .await
    .expect("new history becomes visible without changing ACTIVE");
    assert_eq!(
        served
            .service
            .catalog
            .active_exact_for(&project)
            .unwrap()
            .metadata
            .version,
        active_version
    );
    let expected = capabilities_profile_history_receipt(control, namespace, &project, 2).await;
    let checked = capabilities_profile_pair(
        served,
        &owner,
        &project,
        &expected,
        true,
        &CapabilitiesProfileTraffic::new(),
        0,
    )
    .await;
    assert!(checked.iter().all(|row| row["ok"] == true));
    served.service.catalog.set_authority_fresh(false);
    let stale = served
        .client
        .get_capabilities(request(
            crate::proto::CapabilitiesRequest {
                project_id: project.clone(),
                ..Default::default()
            },
            &owner,
        ))
        .await;
    served.service.catalog.set_authority_fresh(true);
    assert_eq!(
        stale.unwrap_err().code(),
        Code::Unavailable,
        "diagnostic labels never bypass fresh ACTIVE authority"
    );
    let relation = SystemCatalogConfig::default().catalog_versions_relation();
    let original: serde_json::Value = sqlx::query_scalar(&format!(
        "SELECT manifest_json FROM {relation} WHERE catalog_id=$1::UUID"
    ))
    .bind(&staged.catalog_id)
    .fetch_one(control)
    .await
    .unwrap();
    sqlx::query(&format!("UPDATE {relation} SET manifest_json=jsonb_set(manifest_json,'{{tables}}','[]'::JSONB) WHERE catalog_id=$1::UUID"))
        .bind(&staged.catalog_id).execute(control).await.unwrap();
    let public = served
        .client
        .get_catalog_versions(request(CatalogManifestRequest::default(), &owner))
        .await;
    assert_eq!(
        public.unwrap_err().code(),
        Code::FailedPrecondition,
        "public history retains full provenance validation"
    );
    let diagnostic = served
        .client
        .get_capabilities(request(
            crate::proto::CapabilitiesRequest {
                project_id: project.clone(),
                ..Default::default()
            },
            &owner,
        ))
        .await
        .expect("corrupt historical metadata remains diagnostic only")
        .into_inner();
    assert_eq!(
        diagnostic
            .system_catalog_relations
            .iter()
            .filter(|label| label.starts_with("project:"))
            .count(),
        2,
        "opaque corrupted-history labels must not be confused with verified catalog authority"
    );
    sqlx::query(&format!(
        "UPDATE {relation} SET status='REJECTED' WHERE catalog_id=$1::UUID"
    ))
    .bind(&staged.catalog_id)
    .execute(control)
    .await
    .unwrap();
    let rejected = served
        .client
        .get_catalog_versions(request(CatalogManifestRequest::default(), &owner))
        .await
        .expect("rejected corrupt history stays a diagnostic public entry")
        .into_inner();
    assert!(
        rejected
            .versions
            .iter()
            .any(|row| row.catalog_id == staged.catalog_id
                && row.manifest_integrity_sha256.is_empty())
    );
    sqlx::query(&format!(
        "UPDATE {relation} SET manifest_json=$2,status='STAGED' WHERE catalog_id=$1::UUID"
    ))
    .bind(&staged.catalog_id)
    .bind(original)
    .execute(control)
    .await
    .unwrap();
    let config = SystemCatalogConfig::default();
    sqlx::query(&format!(
        "ALTER TABLE {relation} RENAME TO udb_capabilities_hidden_versions"
    ))
    .execute(control)
    .await
    .unwrap();
    let omitted = served
        .client
        .get_capabilities(request(
            crate::proto::CapabilitiesRequest {
                project_id: project.clone(),
                ..Default::default()
            },
            &owner,
        ))
        .await;
    let hidden = native_catalog::relation(
        &config.cdc.system_schema,
        "udb_capabilities_hidden_versions",
    );
    sqlx::query(&format!(
        "ALTER TABLE {hidden} RENAME TO \"{}\"",
        config.catalog_versions_table.replace('"', "\"\"")
    ))
    .execute(control)
    .await
    .expect("restore owned catalog relation after actual query-error control");
    assert!(
        !omitted
            .expect("history query failure keeps the existing capabilities omission behavior")
            .into_inner()
            .system_catalog_relations
            .iter()
            .any(|label| label.starts_with("project:"))
    );
    let users =
        native_catalog::native_model("udb.core.authn.entity.v1.User", &["user_id", "status"]);
    sqlx::query(&format!(
        "UPDATE {} SET {}='SUSPENDED' WHERE {}=$1::UUID",
        users.relation,
        users.q("status"),
        users.q("user_id")
    ))
    .bind(&actor)
    .execute(control)
    .await
    .unwrap();
    assert_eq!(
        served
            .client
            .get_capabilities(request(
                crate::proto::CapabilitiesRequest {
                    project_id: project.clone(),
                    ..Default::default()
                },
                &owner
            ))
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated,
        "history optimization must not cache credential authority"
    );
}
// END CAPABILITIES_HISTORY_CI_PROFILE
