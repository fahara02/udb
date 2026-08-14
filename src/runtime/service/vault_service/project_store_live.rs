//! Live project-store regression for Vault's served gRPC path.
//!
//! The test provisions two real PostgreSQL databases and binds one named runtime
//! instance to each active project. It then drives the generated Vault client
//! over a real tonic server, proving typed entity operations, raw SQL operations,
//! and audit outbox writes stay on the same project-selected physical authority.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use uuid::Uuid;

use crate::proto::udb::core::vault::services::v1 as vault_pb;
use crate::proto::udb::core::vault::services::v1::vault_service_client::VaultServiceClient;
use crate::runtime::config::{
    BackendInstance, BackendInstanceConfig, BackendInstanceRole, UdbConfig,
};
use crate::runtime::service::DataBrokerService;
use crate::runtime::{DataBrokerRuntime, native_catalog};

use super::VaultServiceServer;
use super::config::{TOPIC_KEY_ROTATED, TOPIC_SECRET_DESTROYED};

const PROJECT_A: &str = "vault-project-a";
const PROJECT_B: &str = "vault-project-b";
const TENANT: &str = "vault-project-store-tenant";
const SECRET_PATH: &str = "app/db/password";
const TRANSIT_KEY: &str = "app-key";

fn live_pg_dsn() -> String {
    std::env::var("UDB_LIVE_NATIVE_PG_DSN")
        .or_else(|_| std::env::var("UDB_LIVE_AUTH_PG_DSN"))
        .or_else(|_| std::env::var("UDB_INTEGRATION_PG_DSN"))
        .unwrap_or_else(|_| "postgres://udb:udb@127.0.0.1:55432/udb".to_string())
}

fn live_enabled() -> bool {
    std::env::var("UDB_LIVE_AUTH_TESTS")
        .map(|value| matches!(value.trim(), "1" | "true" | "TRUE"))
        .unwrap_or(false)
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn dsn_for_database(base_dsn: &str, database: &str) -> String {
    let (without_query, query) = base_dsn
        .split_once('?')
        .map_or((base_dsn, None), |(base, query)| (base, Some(query)));
    let slash = without_query
        .rfind('/')
        .expect("live PostgreSQL DSN must contain a database path");
    let mut dsn = format!("{}{database}", &without_query[..=slash]);
    if let Some(query) = query {
        dsn.push('?');
        dsn.push_str(query);
    }
    dsn
}

fn project_instance(name: &str, project_id: &str, dsn: String) -> BackendInstance {
    BackendInstance {
        name: name.to_string(),
        backend: "postgres".to_string(),
        role: BackendInstanceRole::ReadWrite,
        dsn: Some(dsn),
        dsn_env: None,
        enabled: true,
        read_weight: 1,
        write_weight: 1,
        labels: BTreeMap::from([("project_id".to_string(), project_id.to_string())]),
        capabilities: BTreeSet::new(),
    }
}

fn scoped_request<T>(message: T, project_id: &'static str) -> Request<T> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static(TENANT));
    request
        .metadata_mut()
        .insert("x-udb-project-id", MetadataValue::from_static(project_id));
    request
}

async fn provision_native_database(dsn: &str) {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(dsn)
        .await
        .unwrap_or_else(|err| panic!("connect Vault project database at {dsn}: {err}"));
    for stmt in native_catalog::native_service_catalog_ddl() {
        sqlx::raw_sql(&stmt)
            .execute(&pool)
            .await
            .unwrap_or_else(|err| panic!("Vault project native DDL failed: {err}\nSQL:\n{stmt}"));
    }
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("bootstrap project-local UDB system catalog");
    pool.close().await;
}

async fn activate_project_catalogs(service: &DataBrokerService) {
    for project_id in [PROJECT_A, PROJECT_B] {
        service
            .catalog
            .stage_catalog(
                native_catalog::native_manifest().clone(),
                project_id.to_string(),
                "1.0.0".to_string(),
                "backward".to_string(),
            )
            .await
            .unwrap_or_else(|err| panic!("stage Vault catalog for {project_id}: {err}"));
        service
            .catalog
            .activate_catalog_for(project_id, "1.0.0")
            .await
            .unwrap_or_else(|err| panic!("activate Vault catalog for {project_id}: {err}"));
    }
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with CREATE DATABASE privilege"]
async fn served_vault_pins_typed_raw_and_outbox_paths_to_each_project_instance() {
    if !live_enabled() {
        eprintln!("set UDB_LIVE_AUTH_TESTS=1 to run the live Vault project-store regression");
        return;
    }

    let base_dsn = live_pg_dsn();
    let admin_pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&base_dsn)
        .await
        .unwrap_or_else(|err| panic!("connect live PostgreSQL admin database: {err}"));
    let suffix = Uuid::new_v4().simple().to_string();
    let database_a = format!("udb_vault_a_{suffix}");
    let database_b = format!("udb_vault_b_{suffix}");
    for database in [&database_a, &database_b] {
        sqlx::query(&format!("CREATE DATABASE {}", quote_ident(database)))
            .execute(&admin_pool)
            .await
            .unwrap_or_else(|err| panic!("create live Vault database {database}: {err}"));
    }
    let dsn_a = dsn_for_database(&base_dsn, &database_a);
    let dsn_b = dsn_for_database(&base_dsn, &database_b);
    provision_native_database(&dsn_a).await;
    provision_native_database(&dsn_b).await;

    let mut config = UdbConfig {
        project_routing_mode: "strict".to_string(),
        backend_instances: BackendInstanceConfig {
            instances: vec![
                project_instance("vault-a", PROJECT_A, dsn_a.clone()),
                project_instance("vault-b", PROJECT_B, dsn_b.clone()),
            ],
        },
        ..UdbConfig::default()
    };
    config
        .encryption
        .keys
        .insert(1, "0123456789abcdef0123456789abcdef".to_string());
    config.encryption.active_version = Some(1);

    let runtime = DataBrokerRuntime::from_config(config).await;
    let service =
        DataBrokerService::with_runtime(native_catalog::native_manifest().clone(), runtime);
    activate_project_catalogs(&service).await;
    let vault = service.build_vault_service();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind Vault live server");
    let address = listener.local_addr().expect("Vault live server address");
    let incoming = TcpListenerStream::new(listener);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(VaultServiceServer::new(vault))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve live Vault project-store regression");
    });
    let mut client = VaultServiceClient::connect(format!("http://{address}"))
        .await
        .expect("connect generated Vault client");

    for (project, value) in [(PROJECT_A, "alpha-secret"), (PROJECT_B, "beta-secret")] {
        let response = client
            .put_secret(scoped_request(
                vault_pb::PutSecretRequest {
                    tenant_id: TENANT.to_string(),
                    secret_path: SECRET_PATH.to_string(),
                    secret_value: value.to_string(),
                    expected_version: 0,
                    metadata_json: "{}".to_string(),
                },
                project,
            ))
            .await
            .unwrap_or_else(|err| panic!("put secret for {project}: {err}"))
            .into_inner();
        assert_eq!(response.version, 1);

        let listed = client
            .list_secrets(scoped_request(
                vault_pb::ListSecretsRequest {
                    tenant_id: TENANT.to_string(),
                    page_size: 10,
                    ..Default::default()
                },
                project,
            ))
            .await
            .unwrap_or_else(|err| panic!("list secrets for {project}: {err}"))
            .into_inner();
        assert_eq!(listed.total_count, 1);
        assert_eq!(listed.secrets[0].secret_path, SECRET_PATH);

        client
            .create_transit_key(scoped_request(
                vault_pb::CreateTransitKeyRequest {
                    tenant_id: TENANT.to_string(),
                    key_name: TRANSIT_KEY.to_string(),
                    algorithm: "aes256-gcm-siv".to_string(),
                },
                project,
            ))
            .await
            .unwrap_or_else(|err| panic!("create transit key for {project}: {err}"));
    }

    let get_a = client
        .get_secret(scoped_request(
            vault_pb::GetSecretRequest {
                tenant_id: TENANT.to_string(),
                secret_path: SECRET_PATH.to_string(),
                version: 0,
            },
            PROJECT_A,
        ))
        .await
        .expect("get project A secret")
        .into_inner();
    let get_b = client
        .get_secret(scoped_request(
            vault_pb::GetSecretRequest {
                tenant_id: TENANT.to_string(),
                secret_path: SECRET_PATH.to_string(),
                version: 0,
            },
            PROJECT_B,
        ))
        .await
        .expect("get project B secret")
        .into_inner();
    assert_eq!(get_a.secret_value, "alpha-secret");
    assert_eq!(get_b.secret_value, "beta-secret");

    let rotated = client
        .rotate_transit_key(scoped_request(
            vault_pb::RotateTransitKeyRequest {
                tenant_id: TENANT.to_string(),
                key_name: TRANSIT_KEY.to_string(),
            },
            PROJECT_A,
        ))
        .await
        .expect("rotate only project A transit key")
        .into_inner();
    assert_eq!(rotated.version, 2);
    let encrypted_a = client
        .encrypt(scoped_request(
            vault_pb::EncryptRequest {
                tenant_id: TENANT.to_string(),
                key_name: TRANSIT_KEY.to_string(),
                plaintext: "a".to_string(),
            },
            PROJECT_A,
        ))
        .await
        .expect("encrypt with project A key")
        .into_inner();
    let encrypted_b = client
        .encrypt(scoped_request(
            vault_pb::EncryptRequest {
                tenant_id: TENANT.to_string(),
                key_name: TRANSIT_KEY.to_string(),
                plaintext: "b".to_string(),
            },
            PROJECT_B,
        ))
        .await
        .expect("encrypt with project B key")
        .into_inner();
    assert_eq!(encrypted_a.key_version, 2);
    assert_eq!(encrypted_b.key_version, 1);

    let destroyed = client
        .destroy_secret(scoped_request(
            vault_pb::DestroySecretRequest {
                tenant_id: TENANT.to_string(),
                secret_path: SECRET_PATH.to_string(),
                confirmation_token: SECRET_PATH.to_string(),
            },
            PROJECT_A,
        ))
        .await
        .expect("destroy project A secret")
        .into_inner();
    assert_eq!(destroyed.destroyed_versions, 1);
    let missing_a = client
        .get_secret(scoped_request(
            vault_pb::GetSecretRequest {
                tenant_id: TENANT.to_string(),
                secret_path: SECRET_PATH.to_string(),
                version: 0,
            },
            PROJECT_A,
        ))
        .await
        .expect_err("destroyed project A secret must be unreadable");
    assert_eq!(missing_a.code(), tonic::Code::NotFound);
    let surviving_b = client
        .get_secret(scoped_request(
            vault_pb::GetSecretRequest {
                tenant_id: TENANT.to_string(),
                secret_path: SECRET_PATH.to_string(),
                version: 0,
            },
            PROJECT_B,
        ))
        .await
        .expect("project B secret must survive project A destroy")
        .into_inner();
    assert_eq!(surviving_b.secret_value, "beta-secret");

    // The outbox is part of the same physical-authority contract. A previous
    // implementation re-ran weighted selection during emit, which could place
    // the mutation in one instance and its event in another. Project A's unique
    // rotate/destroy topics must exist only in A's database.
    let inspect_a = PgPoolOptions::new()
        .max_connections(1)
        .connect(&dsn_a)
        .await
        .expect("connect project A database for outbox inspection");
    let inspect_b = PgPoolOptions::new()
        .max_connections(1)
        .connect(&dsn_b)
        .await
        .expect("connect project B database for outbox inspection");
    let topics_a: Vec<String> = sqlx::query_scalar(
        "SELECT topic FROM udb_system.outbox_events WHERE partition_key IN ($1, $2)",
    )
    .bind(SECRET_PATH)
    .bind(TRANSIT_KEY)
    .fetch_all(&inspect_a)
    .await
    .expect("read project A Vault outbox topics");
    let topics_b: Vec<String> = sqlx::query_scalar(
        "SELECT topic FROM udb_system.outbox_events WHERE partition_key IN ($1, $2)",
    )
    .bind(SECRET_PATH)
    .bind(TRANSIT_KEY)
    .fetch_all(&inspect_b)
    .await
    .expect("read project B Vault outbox topics");
    assert!(topics_a.iter().any(|topic| topic == TOPIC_KEY_ROTATED));
    assert!(topics_a.iter().any(|topic| topic == TOPIC_SECRET_DESTROYED));
    assert!(!topics_b.iter().any(|topic| topic == TOPIC_KEY_ROTATED));
    assert!(!topics_b.iter().any(|topic| topic == TOPIC_SECRET_DESTROYED));
    inspect_a.close().await;
    inspect_b.close().await;

    drop(client);
    let _ = shutdown_tx.send(());
    server.await.expect("join Vault live server");
    drop(service);

    for database in [&database_a, &database_b] {
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            quote_ident(database)
        ))
        .execute(&admin_pool)
        .await
        .unwrap_or_else(|err| panic!("drop live Vault database {database}: {err}"));
    }
    admin_pool.close().await;
}
