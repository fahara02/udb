// BEGIN PASSWORD_CPU_CI_REPRO
//! This entire module uses only APIs present before the CPU offload. CI can
//! transplant it unchanged onto the previous source and require the named
//! starvation assertion to fail, then exercise the corrected serving path.

use super::support::*;
use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable, ManifestTableSecurity};
use crate::proto::data_broker_client::DataBrokerClient;
use crate::proto::data_broker_server::DataBrokerServer;
use crate::proto::udb::core::authn::entity::v1 as authn_entity;
use crate::proto::udb::core::authn::services::v1 as authn;
use crate::proto::udb::core::authn::services::v1::authn_service_client::AuthnServiceClient;
use crate::proto::udb::core::authn::services::v1::authn_service_server::{
    AuthnService, AuthnServiceServer,
};
use crate::proto::{RequestContext, SelectRequest};
use crate::runtime::credential_layer::CredentialResolveLayer;
use crate::runtime::security::{SecurityConfig, sign_access_token};
use crate::runtime::service::method_security::MethodSecurityLayer;
use sha2::Digest;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tonic::Request;

const PASSWORD: &str = "CorrectHorse1!";
const MESSAGE: &str = "cpu.fixture.v1.Item";
const BURST: usize = 8;
const READ_DEADLINE: Duration = Duration::from_secs(3);
const MAX_TIMER_GAP: Duration = Duration::from_millis(500);

struct SecurityRestore(SecurityConfig);
impl Drop for SecurityRestore {
    fn drop(&mut self) {
        SecurityConfig::install_global(self.0.clone());
    }
}

struct LoginInFlight(Arc<AtomicUsize>);
impl Drop for LoginInFlight {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn read_request(token: &str) -> Request<SelectRequest> {
    let mut request = Request::new(SelectRequest {
        context: Some(RequestContext {
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            purpose: "password-cpu-ci".to_string(),
            ..Default::default()
        }),
        message_type: MESSAGE.to_string(),
        limit: 1,
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
        .metadata_mut()
        .insert("x-purpose", "password-cpu-ci".parse().unwrap());
    request
}

fn fixture_manifest(schema: &str) -> CatalogManifest {
    let columns = ["id", "tenant_id", "project_id"]
        .into_iter()
        .map(|name| ManifestColumn {
            field_name: name.to_string(),
            column_name: name.to_string(),
            proto_type: "string".to_string(),
            sql_type: "TEXT".to_string(),
            is_primary: name == "id",
            not_null: true,
            ..Default::default()
        })
        .collect();
    let mut manifest = CatalogManifest {
        tables: vec![ManifestTable {
            proto_package: "cpu.fixture.v1".to_string(),
            message_name: "Item".to_string(),
            schema: schema.to_string(),
            table: "items".to_string(),
            primary_key: vec!["id".to_string()],
            columns,
            table_security: ManifestTableSecurity {
                tenant_column: "tenant_id".to_string(),
                project_column: "project_id".to_string(),
                ..Default::default()
            },
            ..Default::default()
        }],
        ..Default::default()
    };
    manifest.checksum_sha256 = format!(
        "{:x}",
        sha2::Sha256::digest(serde_json::to_vec(&manifest).unwrap())
    );
    manifest
}

async fn fixture_policy(pool: &sqlx::PgPool, subject: &str) -> String {
    let model = crate::runtime::native_catalog::native_model(
        "udb.core.authz.entity.v1.PolicyRule",
        &[
            "policy_id",
            "subject",
            "domain",
            "object",
            "action",
            "effect",
            "condition",
            "description",
            "is_active",
            "tenant_id",
            "project_id",
            "attributes_json",
        ],
    );
    let id = uuid::Uuid::new_v4();
    let sql = format!(
        "INSERT INTO {rel} \
         ({id}, {subject}, {domain}, {object}, {action}, {effect}, {condition}, \
          {description}, {active}, {tenant}, {project}, {attributes}) \
         VALUES ($1, $2, 'acme', $3, 'Select', 'ALLOW', '', \
                 'password CPU CI narrow read', TRUE, 'acme', 'billing', '{{}}'::JSONB)",
        rel = model.relation,
        id = model.q("policy_id"),
        subject = model.q("subject"),
        domain = model.q("domain"),
        object = model.q("object"),
        action = model.q("action"),
        effect = model.q("effect"),
        condition = model.q("condition"),
        description = model.q("description"),
        active = model.q("is_active"),
        tenant = model.q("tenant_id"),
        project = model.q("project_id"),
        attributes = model.q("attributes_json"),
    );
    sqlx::query(&sql)
        .bind(id)
        .bind(subject)
        .bind(MESSAGE)
        .execute(pool)
        .await
        .expect("persist exact read policy");
    id.to_string()
}

async fn run_served_burst(workers: usize) {
    // Fail closed when the live job accidentally omits its database fixture.
    assert!(
        ["UDB_LIVE_AUTH_PG_DSN", "UDB_INTEGRATION_PG_DSN"]
            .iter()
            .any(|name| std::env::var(name).is_ok_and(|dsn| !dsn.trim().is_empty())),
        "password CPU live proof requires the CI PostgreSQL fixture"
    );
    let _db_guard = live_auth_db_lock().lock().await;
    let _restore = SecurityRestore(SecurityConfig::current());
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service_with_jwt(pool.clone());
    let mut users = Vec::new();
    for index in 0..BURST {
        users.push(create_verified_user(&authn, &format!("cpu-{workers}-{index}"), PASSWORD).await);
    }
    // Pin the real persisted workload, rather than assuming a successful login
    // went through an expensive password algorithm. Both CI variants see the
    // same Argon2 version, memory, passes and lanes, with no legacy rehash.
    let user_store = crate::runtime::authn::PostgresUserStore::new(pool.clone(), "");
    for user in &users {
        let stored = crate::runtime::authn::UserStore::get_user_by_id(&user_store, &user.user_id)
            .await
            .unwrap()
            .expect("canonical fixture user remains durable");
        assert!(
            stored
                .password_hash
                .starts_with("$argon2id$v=19$m=19456,t=2,p=1$")
        );
        assert!(!crate::runtime::authn::password_hash_needs_upgrade(
            &stored.password_hash
        ));
    }
    let schema = format!("udb_password_cpu_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".items \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, project_id TEXT NOT NULL)"
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".items VALUES ('cpu-read-row', 'acme', 'billing')"
    ))
    .execute(&pool)
    .await
    .unwrap();

    let runtime =
        crate::runtime::DataBrokerRuntime::from_config(crate::runtime::config::UdbConfig {
            primary: crate::runtime::config::DbConfig {
                direct_dsn: live_pg_dsn(),
                ..Default::default()
            },
            ..Default::default()
        })
        .await;
    let broker = crate::runtime::service::DataBrokerService::with_runtime_and_state(
        fixture_manifest(&schema),
        runtime,
        Arc::new(RwLock::new(crate::FsmState::Completed)),
        Arc::new(crate::metrics::NoopMetrics),
        None,
        false,
    );
    activate_live_project_catalog(&broker, "billing", "password-cpu-live").await;
    let policy_id = fixture_policy(&pool, &users[0].user_id).await;
    let (_, authz, _) = broker.build_auth_services();
    authz.warm_shared_snapshot().await;
    assert!(
        broker
            .authz_snapshot()
            .load_full()
            .policies
            .iter()
            .any(|policy| policy.id == policy_id),
        "the actual warmed policy snapshot must grant only the fixture read"
    );
    let security = SecurityConfig {
        tls_required: false,
        service_identity_required: false,
        mtls_required: false,
        allow_header_scopes: false,
        jwt_private_key: Some(include_str!("../../../testdata/jwt_rs256_private.pem").to_string()),
        jwt_public_key: Some(include_str!("../../../testdata/jwt_rs256_public.pem").to_string()),
        ..Default::default()
    };
    SecurityConfig::install_global(security.clone());
    super::super::install_data_plane_credential_resolvers(
        pool.clone(),
        &crate::runtime::authn::AuthnConfig {
            session_enabled: true,
            session_hash_secret: "live-auth-test-secret".to_string(),
            ..Default::default()
        },
        Arc::new(authn.clone()),
    );
    let read_token = sign_access_token(
        &security,
        &users[0].user_id,
        "acme",
        "billing",
        &["udb:read".to_string()],
        &[],
        "",
        &format!("cpu-read-{}", uuid::Uuid::new_v4()),
        "pwd",
        authn_entity::AccountKind::Person as i32,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
    .unwrap()
    .expect("fixture RSA key configured")
    .0;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let incoming = futures::stream::unfold(listener, |listener| async move {
        Some((listener.accept().await.map(|(stream, _)| stream), listener))
    });
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let native = MethodSecurityLayer::new().wrap(AuthnServiceServer::new(authn.clone()));
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .layer(CredentialResolveLayer::new())
            .add_service(DataBrokerServer::new(broker))
            .add_service(native)
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve actual authn and DataBroker routes");
    });
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut reader = DataBrokerClient::new(channel.clone());
    let mut login_client = AuthnServiceClient::new(channel.clone());
    // Warm the actual KDF, credential resolver, statement cache and Casbin
    // enforcer before measuring concurrent steady-state serving work.
    let warmed_login = login_client
        .login(authn::LoginRequest {
            username: users[0].username.clone(),
            password: PASSWORD.to_string(),
            ..Default::default()
        })
        .await
        .expect("warm real password login")
        .into_inner();
    assert_eq!(warmed_login.user_id, users[0].user_id);
    for _ in 0..3 {
        let rows = reader
            .select(read_request(&read_token))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(rows.records_json.len(), 1);
    }

    let stop = Arc::new(AtomicBool::new(false));
    let timer_stop = stop.clone();
    let (timer_ready_tx, timer_ready_rx) = tokio::sync::oneshot::channel();
    let timer = tokio::spawn(async move {
        let mut previous = Instant::now();
        let mut maximum = Duration::ZERO;
        let mut ticks = 0;
        timer_ready_tx.send(()).unwrap();
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let now = Instant::now();
            maximum = maximum.max(now.duration_since(previous));
            previous = now;
            ticks += 1;
            if timer_stop.load(Ordering::Acquire) {
                return (maximum, ticks);
            }
        }
    });
    timer_ready_rx.await.unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(BURST + 1));
    let active_logins = Arc::new(AtomicUsize::new(0));
    let mut logins = tokio::task::JoinSet::new();
    let burst_started = Instant::now();
    for user in &users {
        let mut client = login_client.clone();
        let barrier = barrier.clone();
        let active_logins = active_logins.clone();
        let username = user.username.clone();
        let expected_user = user.user_id.clone();
        logins.spawn(async move {
            barrier.wait().await;
            active_logins.fetch_add(1, Ordering::AcqRel);
            let in_flight = LoginInFlight(active_logins);
            let response = tokio::time::timeout(
                Duration::from_secs(90),
                client.login(authn::LoginRequest {
                    username,
                    password: PASSWORD.to_string(),
                    ..Default::default()
                }),
            )
            .await;
            drop(in_flight);
            response.is_ok_and(|result| {
                result.is_ok_and(|response| response.into_inner().user_id == expected_user)
            })
        });
    }
    let reads_stop = stop.clone();
    let reads_active_logins = active_logins.clone();
    let token = read_token.clone();
    let reads = tokio::spawn(async move {
        let mut maximum = Duration::ZERO;
        let mut successes = 0;
        let mut failures = 0;
        let mut overlapping_successes = 0;
        loop {
            let began_during_login = reads_active_logins.load(Ordering::Acquire) > 0;
            let started = Instant::now();
            let result =
                tokio::time::timeout(READ_DEADLINE, reader.select(read_request(&token))).await;
            let ended_during_login = reads_active_logins.load(Ordering::Acquire) > 0;
            maximum = maximum.max(started.elapsed());
            if result.is_ok_and(|response| {
                response.is_ok_and(|response| {
                    let rows = response.into_inner();
                    rows.records_json.len() == 1
                        && serde_json::from_slice::<serde_json::Value>(&rows.records_json[0])
                            .is_ok_and(|row| row["id"] == "cpu-read-row")
                })
            }) {
                successes += 1;
                overlapping_successes += usize::from(began_during_login && ended_during_login);
            } else {
                failures += 1;
            }
            if reads_stop.load(Ordering::Acquire) {
                return (maximum, successes, failures, overlapping_successes);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    });
    barrier.wait().await;
    let mut login_successes = 0;
    while let Some(result) = logins.join_next().await {
        login_successes += usize::from(result.expect("served login task must not panic"));
    }
    let burst_elapsed = burst_started.elapsed();
    stop.store(true, Ordering::Release);
    let (maximum_read, read_successes, read_failures, overlapping_read_successes) =
        reads.await.unwrap();
    let (maximum_gap, ticks) = timer.await.unwrap();

    // Cached signature validation must still resolve current user state after
    // the CPU optimization. The same signed token dies when its owner suspends.
    authn
        .change_user_status(Request::new(authn::ChangeUserStatusRequest {
            user_id: users[0].user_id.clone(),
            new_status: authn_entity::UserStatus::Suspended as i32,
            reason: "password CPU fixture revocation".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap();
    let mut reader = DataBrokerClient::new(channel.clone());
    let refusal = reader
        .select(read_request(&read_token))
        .await
        .expect_err("CPU admission must not cache a suspended principal's authority");
    assert_eq!(refusal.code(), tonic::Code::Unauthenticated);
    drop(reader);
    drop(login_client);
    drop(channel);
    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("actual serving routes must shut down without leaked work")
        .unwrap();
    cleanup_native_auth_db(&pool).await;
    println!(
        "password CPU serving workers={workers} burst={BURST} argon2id_v=19 m_kib=19456 passes=2 lanes=1 logins_ok={login_successes} reads_ok={read_successes} reads_failed={read_failures} overlapping_reads_ok={overlapping_read_successes} burst_ms={} maximum_read_ms={} maximum_timer_gap_ms={} timer_ticks={ticks}",
        burst_elapsed.as_millis(),
        maximum_read.as_millis(),
        maximum_gap.as_millis(),
    );
    assert!(
        maximum_gap <= MAX_TIMER_GAP,
        "password-cpu: served Login burst must not starve the runtime timer; maximum gap {maximum_gap:?}"
    );
    assert!(
        overlapping_read_successes > 0,
        "actual Select must begin and finish successfully while Login RPCs remain in flight"
    );
    assert!(
        maximum_read <= READ_DEADLINE,
        "password-cpu: authenticated Select must finish within its existing three-second deadline during password work; maximum wall time {maximum_read:?}"
    );
    assert_eq!(
        login_successes, BURST,
        "every real password must still authenticate"
    );
    assert!(
        read_successes > 0,
        "actual Select must execute during the burst"
    );
    assert_eq!(
        read_failures, 0,
        "all narrow authenticated reads must succeed"
    );
    assert!(ticks > 0);
}

#[test]
#[ignore = "requires live CI PostgreSQL; included in the unfiltered native live lane"]
fn live_password_cpu_burst_preserves_served_read_deadline_and_timer() {
    // Exercise both minimal and small multicore executor budgets. Production
    // admission independently uses detected CPU availability, never these
    // fixture worker counts or a new thread pool.
    for workers in [1, 2] {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .enable_all()
            .build()
            .expect("build actual serving runtime")
            .block_on(run_served_burst(workers));
    }
}
// END PASSWORD_CPU_CI_REPRO
