//! Live Postgres suite for the native `MessagingService`, driven over a real
//! tonic transport (the service is mounted on a TCP listener and called through
//! the generated client), with OpenMLS clients playing the devices.
//!
//! Run with a live Postgres:
//!   UDB_LIVE_NATIVE_PG_DSN=postgres://udb:udb@127.0.0.1:55432/udb \
//!     cargo test --lib messaging_service::tests::live -- --ignored --test-threads=1

use std::collections::{BTreeMap, BTreeSet};

use openmls::prelude::OpenMlsProvider as _;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

use crate::proto::udb::core::messaging::services::v1 as msg_pb;
use crate::proto::udb::core::messaging::services::v1::messaging_service_client::MessagingServiceClient;
use crate::runtime::service::live_tests::support::{
    live_native_service_db_lock, live_pg_pool, messaging_service, migrate_native_service_db,
};

use super::super::MessagingServiceServer;
use super::super::config::TOPIC_INBOX_APPENDED;
use super::super::workers::run_inbox_retention_once;
use super::client::Device;
use super::decode_detail;

pub(super) type Client = MessagingServiceClient<Channel>;

pub(super) async fn serve() -> (Client, tokio::sync::oneshot::Sender<()>) {
    let svc = messaging_service().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind messaging listener");
    let address = listener.local_addr().expect("listener address");
    let incoming = futures::stream::unfold(listener, |listener| async move {
        let connection = listener.accept().await.map(|(stream, _)| stream);
        Some((connection, listener))
    });
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(MessagingServiceServer::new(svc))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve MessagingService");
    });
    let client = MessagingServiceClient::connect(format!("http://{address}"))
        .await
        .expect("connect MessagingService client");
    (client, shutdown_tx)
}

pub(super) fn req<T>(tenant: &str, message: T) -> Request<T> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("x-tenant-id", tenant.parse().expect("tenant header"));
    request
}

pub(super) async fn register(client: &mut Client, tenant: &str, subject: &str, device: &Device) {
    let response = client
        .register_messaging_device(req(
            tenant,
            msg_pb::RegisterMessagingDeviceRequest {
                tenant_id: tenant.to_string(),
                subject_id: subject.to_string(),
                device_id: device.device_id.clone(),
                signature_public_key: device.signature_key(),
                credential: device.credential_bytes(),
            },
        ))
        .await
        .expect("register device")
        .into_inner();
    assert!(response.created, "{} registers fresh", device.device_id);
}

pub(super) async fn publish(
    client: &mut Client,
    tenant: &str,
    device: &Device,
    count: usize,
    last_resort: bool,
) {
    let response = client
        .publish_key_packages(req(
            tenant,
            msg_pb::PublishKeyPackagesRequest {
                tenant_id: tenant.to_string(),
                device_id: device.device_id.clone(),
                key_packages: (0..count).map(|_| device.key_package()).collect(),
                last_resort_key_package: if last_resort {
                    device.key_package()
                } else {
                    Vec::new()
                },
            },
        ))
        .await
        .expect("publish key packages")
        .into_inner();
    assert_eq!(response.accepted as usize, count);
}

pub(super) async fn claim(
    client: &mut Client,
    tenant: &str,
    requester: &str,
    target: &str,
) -> msg_pb::ClaimedKeyPackage {
    // Fair admission may shed a burst with a retryable RESOURCE_EXHAUSTED; a
    // client honours it by retrying, which is what this does.
    let mut attempts = 0;
    let mut response = loop {
        let result = client
            .claim_key_packages(req(
                tenant,
                msg_pb::ClaimKeyPackagesRequest {
                    tenant_id: tenant.to_string(),
                    requester_device_id: requester.to_string(),
                    target_device_ids: vec![target.to_string()],
                    target_subject_ids: Vec::new(),
                },
            ))
            .await;
        match result {
            Err(status) if status.code() == Code::ResourceExhausted && attempts < 50 => {
                attempts += 1;
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            other => break other.expect("claim key package").into_inner(),
        }
    };
    assert_eq!(response.key_packages.len(), 1, "one package for {target}");
    response.key_packages.remove(0)
}

pub(super) async fn drain(
    client: &mut Client,
    tenant: &str,
    device_id: &str,
    after_seq: i64,
) -> Vec<msg_pb::InboxEntry> {
    let mut stream = client
        .stream_inbox(req(
            tenant,
            msg_pb::StreamInboxRequest {
                tenant_id: tenant.to_string(),
                device_id: device_id.to_string(),
                after_seq,
                follow: false,
                max_entries: 0,
            },
        ))
        .await
        .expect("open inbox stream")
        .into_inner();
    let mut entries = Vec::new();
    while let Some(item) = stream.message().await.expect("inbox stream item") {
        entries.push(item.entry.expect("inbox entry"));
    }
    entries
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn submit(
    client: &mut Client,
    tenant: &str,
    group_id: &str,
    sender: &str,
    epoch: i64,
    commit: Vec<u8>,
    welcome: Vec<u8>,
    client_message_id: &str,
) -> Result<msg_pb::SubmitCommitResponse, Status> {
    client
        .submit_commit(req(
            tenant,
            msg_pb::SubmitCommitRequest {
                tenant_id: tenant.to_string(),
                group_id: group_id.to_string(),
                sender_device_id: sender.to_string(),
                epoch,
                commit,
                welcome,
                group_info: Vec::new(),
                client_message_id: client_message_id.to_string(),
            },
        ))
        .await
        .map(tonic::Response::into_inner)
}

pub(super) async fn send(
    client: &mut Client,
    tenant: &str,
    group_id: &str,
    sender: &str,
    ciphertext: Vec<u8>,
    client_message_id: &str,
) -> Result<msg_pb::SendMessageResponse, Status> {
    client
        .send_message(req(
            tenant,
            msg_pb::SendMessageRequest {
                tenant_id: tenant.to_string(),
                group_id: group_id.to_string(),
                sender_device_id: sender.to_string(),
                ciphertext,
                client_message_id: client_message_id.to_string(),
            },
        ))
        .await
        .map(tonic::Response::into_inner)
}

async fn outbox_events_for(pool: &sqlx::PgPool, tenant: &str) -> Vec<serde_json::Value> {
    sqlx::query_scalar(
        "SELECT payload FROM udb_system.outbox_events \
         WHERE topic = $1 AND payload->'payload'->>'tenant_id' = $2 ORDER BY event_seq",
    )
    .bind(TOPIC_INBOX_APPENDED)
    .bind(tenant)
    .fetch_all(pool)
    .await
    .expect("read inbox events")
}

async fn inbox_rows(pool: &sqlx::PgPool, tenant: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM udb_messaging.inbox WHERE tenant_id = $1")
        .bind(tenant)
        .fetch_one(pool)
        .await
        .expect("count inbox rows")
}

/// M1: one-time KeyPackages are consumed exactly once under concurrent claims,
/// the last-resort package backs an exhausted device without being consumed,
/// and tenants are isolated both by the handlers and by Postgres RLS.
#[tokio::test]
#[ignore = "requires live Postgres; run with cargo test --lib messaging_service::tests::live -- --ignored --test-threads=1"]
async fn live_messaging_key_packages_are_claimed_once_and_tenants_are_isolated() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let (client, _shutdown) = serve().await;
    let tenant_a = uuid::Uuid::new_v4().to_string();
    let tenant_b = uuid::Uuid::new_v4().to_string();

    let alice = Device::new(&tenant_a, "alice", "alice-phone");
    let bob = Device::new(&tenant_a, "bob", "bob-phone");
    let mut c = client.clone();
    register(&mut c, &tenant_a, "alice", &alice).await;
    register(&mut c, &tenant_a, "bob", &bob).await;
    // Re-registering the identical device is an idempotent replay.
    let replay = c
        .register_messaging_device(req(
            &tenant_a,
            msg_pb::RegisterMessagingDeviceRequest {
                tenant_id: tenant_a.clone(),
                subject_id: "bob".to_string(),
                device_id: "bob-phone".to_string(),
                signature_public_key: bob.signature_key(),
                credential: bob.credential_bytes(),
            },
        ))
        .await
        .expect("replay register")
        .into_inner();
    assert!(!replay.created);
    // A different key for the same device id is refused.
    let impostor = Device::new(&tenant_a, "bob", "bob-phone");
    let err = c
        .register_messaging_device(req(
            &tenant_a,
            msg_pb::RegisterMessagingDeviceRequest {
                tenant_id: tenant_a.clone(),
                subject_id: "bob".to_string(),
                device_id: "bob-phone".to_string(),
                signature_public_key: impostor.signature_key(),
                credential: impostor.credential_bytes(),
            },
        ))
        .await
        .expect_err("a key change for an existing device is refused");
    assert_eq!(err.code(), Code::AlreadyExists);

    // A package signed by a different key than bob's registered one is refused.
    let err = c
        .publish_key_packages(req(
            &tenant_a,
            msg_pb::PublishKeyPackagesRequest {
                tenant_id: tenant_a.clone(),
                device_id: "bob-phone".to_string(),
                key_packages: vec![impostor.key_package()],
                last_resort_key_package: Vec::new(),
            },
        ))
        .await
        .expect_err("foreign-key package refused");
    assert_eq!(err.code(), Code::InvalidArgument);

    const ONE_TIME: usize = 5;
    const CLAIMERS: usize = 24;
    publish(&mut c, &tenant_a, &bob, ONE_TIME, true).await;
    let stock = c
        .get_key_package_stock(req(
            &tenant_a,
            msg_pb::GetKeyPackageStockRequest {
                tenant_id: tenant_a.clone(),
                device_id: "bob-phone".to_string(),
            },
        ))
        .await
        .expect("stock")
        .into_inner();
    assert_eq!(stock.one_time_remaining, ONE_TIME as i64);
    assert!(stock.has_last_resort);

    // CLAIMERS concurrent claims for bob's device.
    let claims = futures::future::join_all((0..CLAIMERS).map(|_| {
        let mut client = client.clone();
        let tenant = tenant_a.clone();
        async move { claim(&mut client, &tenant, "alice-phone", "bob-phone").await }
    }))
    .await;
    let one_time: Vec<&str> = claims
        .iter()
        .filter(|claimed| !claimed.last_resort)
        .map(|claimed| claimed.key_package_ref.as_str())
        .collect();
    let distinct: BTreeSet<&str> = one_time.iter().copied().collect();
    assert_eq!(
        one_time.len(),
        ONE_TIME,
        "every one-time package is handed out"
    );
    assert_eq!(
        distinct.len(),
        ONE_TIME,
        "no one-time package is handed out twice"
    );
    let last_resort: BTreeSet<&str> = claims
        .iter()
        .filter(|claimed| claimed.last_resort)
        .map(|claimed| claimed.key_package_ref.as_str())
        .collect();
    assert_eq!(
        last_resort.len(),
        1,
        "the exhausted device falls back to one last-resort package"
    );
    assert!(last_resort.is_disjoint(&distinct));
    let claimed_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM udb_messaging.key_packages \
         WHERE tenant_id = $1 AND device_id = 'bob-phone' AND claimed_at IS NOT NULL",
    )
    .bind(&tenant_a)
    .fetch_one(&pool)
    .await
    .expect("count claimed");
    assert_eq!(claimed_rows, ONE_TIME as i64);
    for claimed in &claims {
        assert_eq!(claimed.subject_id, "bob");
        super::super::mls::validate_key_package("key_package", &claimed.key_package)
            .expect("a claimed package is the published MLS KeyPackage");
    }

    // Tenant B sees nothing of tenant A through the service.
    let carol = Device::new(&tenant_b, "carol", "carol-phone");
    register(&mut c, &tenant_b, "carol", &carol).await;
    let listed = c
        .list_messaging_devices(req(
            &tenant_b,
            msg_pb::ListMessagingDevicesRequest {
                tenant_id: tenant_b.clone(),
                subject_id: "bob".to_string(),
                include_revoked: true,
                ..Default::default()
            },
        ))
        .await
        .expect("list as tenant b")
        .into_inner();
    assert!(
        listed.devices.is_empty(),
        "tenant B cannot list tenant A's devices"
    );
    let cross = c
        .claim_key_packages(req(
            &tenant_b,
            msg_pb::ClaimKeyPackagesRequest {
                tenant_id: tenant_b.clone(),
                requester_device_id: "carol-phone".to_string(),
                target_device_ids: vec!["bob-phone".to_string()],
                target_subject_ids: vec!["bob".to_string()],
            },
        ))
        .await
        .expect("cross-tenant claim answers")
        .into_inner();
    assert!(cross.key_packages.is_empty());
    assert_eq!(cross.missing_device_ids, ["bob-phone"]);
    let err = c
        .list_messaging_devices(req(
            &tenant_b,
            msg_pb::ListMessagingDevicesRequest {
                tenant_id: tenant_a.clone(),
                subject_id: "bob".to_string(),
                include_revoked: true,
                ..Default::default()
            },
        ))
        .await
        .expect_err("a body tenant different from the header tenant is refused");
    assert_eq!(err.code(), Code::PermissionDenied);
    let listed = c
        .list_messaging_devices(req(
            &tenant_a,
            msg_pb::ListMessagingDevicesRequest {
                tenant_id: tenant_a.clone(),
                subject_id: "bob".to_string(),
                include_revoked: false,
                ..Default::default()
            },
        ))
        .await
        .expect("list as tenant a")
        .into_inner();
    assert_eq!(listed.devices.len(), 1);
    assert_eq!(
        listed.devices[0].key_fingerprint,
        super::super::model::key_fingerprint(&bob.signature_key())
    );

    // Postgres RLS, through a role that cannot bypass it.
    let role = format!("udb_messaging_rls_{}", uuid::Uuid::new_v4().simple());
    sqlx::raw_sql(&format!(
        "CREATE ROLE \"{role}\" NOLOGIN NOSUPERUSER NOBYPASSRLS; \
         GRANT USAGE ON SCHEMA udb_messaging TO \"{role}\"; \
         GRANT SELECT ON ALL TABLES IN SCHEMA udb_messaging TO \"{role}\""
    ))
    .execute(&pool)
    .await
    .expect("create RLS inspection role");
    for table in [
        "devices",
        "key_packages",
        "mls_groups",
        "group_members",
        "inbox",
    ] {
        let (enabled, forced): (bool, bool) = sqlx::query_as(
            "SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE oid = to_regclass($1)",
        )
        .bind(format!("udb_messaging.{table}"))
        .fetch_one(&pool)
        .await
        .expect("RLS posture");
        assert!(
            enabled && forced,
            "udb_messaging.{table} must enable and force RLS"
        );
    }
    let visible = |tenant: Option<String>| {
        let pool = pool.clone();
        let role = role.clone();
        async move {
            let mut tx = pool.begin().await.expect("begin RLS probe");
            sqlx::query(&format!("SET LOCAL ROLE \"{role}\""))
                .execute(&mut *tx)
                .await
                .expect("assume RLS role");
            if let Some(tenant) = tenant {
                sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)")
                    .bind(tenant)
                    .execute(&mut *tx)
                    .await
                    .expect("tenant scope");
            }
            let rows: Vec<(String, i64)> = sqlx::query_as(
                "SELECT tenant_id::text, COUNT(*) FROM udb_messaging.devices GROUP BY 1 ORDER BY 1",
            )
            .fetch_all(&mut *tx)
            .await
            .expect("RLS probe read");
            let packages: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM udb_messaging.key_packages")
                    .fetch_one(&mut *tx)
                    .await
                    .expect("RLS probe packages");
            tx.rollback().await.expect("rollback RLS probe");
            (rows, packages)
        }
    };
    let (rows_a, packages_a) = visible(Some(tenant_a.clone())).await;
    assert_eq!(rows_a, vec![(tenant_a.clone(), 2)]);
    assert_eq!(packages_a, (ONE_TIME + 1) as i64);
    let (rows_b, packages_b) = visible(Some(tenant_b.clone())).await;
    assert_eq!(rows_b, vec![(tenant_b.clone(), 1)]);
    assert_eq!(packages_b, 0);
    let (rows_none, packages_none) = visible(None).await;
    assert!(
        rows_none.is_empty() && packages_none == 0,
        "no tenant scope, no rows"
    );
    sqlx::raw_sql(&format!(
        "REVOKE ALL ON ALL TABLES IN SCHEMA udb_messaging FROM \"{role}\"; \
         REVOKE USAGE ON SCHEMA udb_messaging FROM \"{role}\"; DROP ROLE \"{role}\""
    ))
    .execute(&pool)
    .await
    .expect("drop RLS inspection role");
}

/// M2 + M3: group creation, epoch linearization of concurrent commits, the
/// 3-device fan-out, inbox streaming/acknowledgement, idempotent sends, the
/// outbox events, revocation hints, retention, and a plaintext scan of the
/// broker database.
#[tokio::test]
#[ignore = "requires live Postgres; run with cargo test --lib messaging_service::tests::live -- --ignored --test-threads=1"]
async fn live_messaging_group_commits_linearize_and_messages_fan_out() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let (client, _shutdown) = serve().await;
    let mut c = client.clone();
    let tenant = uuid::Uuid::new_v4().to_string();
    let group_id = "team-room";

    let alice = Device::new(&tenant, "alice", "alice-phone");
    let bob = Device::new(&tenant, "bob", "bob-phone");
    let carol = Device::new(&tenant, "carol", "carol-laptop");
    for (subject, device) in [("alice", &alice), ("bob", &bob), ("carol", &carol)] {
        register(&mut c, &tenant, subject, device).await;
        publish(&mut c, &tenant, device, 2, false).await;
    }

    // Alice creates the group (epoch 0); a replay is idempotent.
    let (mut alice_group, info, tree) = alice.create_group(group_id);
    for expect_created in [true, false] {
        let created = c
            .create_group(req(
                &tenant,
                msg_pb::CreateGroupRequest {
                    tenant_id: tenant.clone(),
                    group_id: group_id.to_string(),
                    creator_device_id: "alice-phone".to_string(),
                    group_info: info.clone(),
                    ratchet_tree: tree.clone(),
                    ..Default::default()
                },
            ))
            .await
            .expect("create group")
            .into_inner();
        assert_eq!((created.created, created.epoch), (expect_created, 0));
    }

    // Alice adds bob (epoch 0 -> 1); bob joins from the Welcome in his inbox.
    let bob_kp = claim(&mut c, &tenant, "alice-phone", "bob-phone").await;
    let (commit, welcome) = alice.add(&mut alice_group, &[bob_kp.key_package]);
    let accepted = submit(
        &mut c,
        &tenant,
        group_id,
        "alice-phone",
        0,
        commit.clone(),
        welcome,
        "add-bob",
    )
    .await
    .expect("add bob");
    alice.merge(&mut alice_group);
    assert_eq!(accepted.epoch, 1);
    assert_eq!(accepted.added_device_ids, ["bob-phone"]);
    let retried = submit(
        &mut c,
        &tenant,
        group_id,
        "alice-phone",
        0,
        commit.clone(),
        Vec::new(),
        "add-bob",
    )
    .await
    .expect("retried commit is answered from the idempotency key");
    assert!(retried.duplicate);
    assert_eq!(retried.epoch, 1);
    let bob_inbox = drain(&mut c, &tenant, "bob-phone", 0).await;
    assert_eq!(bob_inbox.len(), 1);
    assert_eq!(bob_inbox[0].kind, "WELCOME");
    let mut bob_group = bob.join(&bob_inbox[0].payload);

    // Two concurrent commits at epoch 1: exactly one is accepted; the other is
    // refused with the current epoch so it can re-base.
    let alice_update = alice.self_update(&mut alice_group);
    let bob_update = bob.self_update(&mut bob_group);
    let (mut c1, mut c2) = (client.clone(), client.clone());
    let (alice_result, bob_result) = tokio::join!(
        submit(
            &mut c1,
            &tenant,
            group_id,
            "alice-phone",
            1,
            alice_update,
            Vec::new(),
            ""
        ),
        submit(
            &mut c2,
            &tenant,
            group_id,
            "bob-phone",
            1,
            bob_update,
            Vec::new(),
            ""
        ),
    );
    let outcomes = [alice_result.is_ok(), bob_result.is_ok()];
    assert_eq!(
        outcomes.iter().filter(|ok| **ok).count(),
        1,
        "exactly one concurrent commit at an epoch is accepted: {alice_result:?} / {bob_result:?}"
    );
    let loser = alice_result
        .as_ref()
        .err()
        .or(bob_result.as_ref().err())
        .expect("a loser");
    assert_eq!(loser.code(), Code::FailedPrecondition);
    let detail = decode_detail(loser);
    assert_eq!(
        detail.missing.get("current_epoch").map(String::as_str),
        Some("2")
    );
    // The loser drops its pending commit and applies the winner's from its inbox.
    if alice_result.is_ok() {
        alice.merge(&mut alice_group);
        bob_group
            .clear_pending_commit(bob.provider.storage())
            .expect("bob drops his pending commit");
        let entries = drain(&mut c, &tenant, "bob-phone", bob_inbox[0].seq).await;
        assert_eq!(entries.last().map(|e| e.kind.as_str()), Some("COMMIT"));
        bob.apply_commit(&mut bob_group, &entries.last().unwrap().payload);
    } else {
        bob.merge(&mut bob_group);
        alice_group
            .clear_pending_commit(alice.provider.storage())
            .expect("alice drops her pending commit");
        let entries = drain(&mut c, &tenant, "alice-phone", 0).await;
        assert_eq!(entries.last().map(|e| e.kind.as_str()), Some("COMMIT"));
        alice.apply_commit(&mut alice_group, &entries.last().unwrap().payload);
    }
    assert_eq!(alice_group.epoch().as_u64(), 2);
    assert_eq!(bob_group.epoch().as_u64(), 2);
    // Replaying the retired epoch-0 commit is refused as stale.
    let stale = submit(
        &mut c,
        &tenant,
        group_id,
        "alice-phone",
        0,
        commit,
        Vec::new(),
        "",
    )
    .await
    .expect_err("stale commit");
    assert_eq!(stale.code(), Code::FailedPrecondition);

    // Alice adds carol (epoch 2 -> 3), uploading the new GroupInfo.
    let carol_kp = claim(&mut c, &tenant, "alice-phone", "carol-laptop").await;
    let (commit, welcome) = alice.add(&mut alice_group, &[carol_kp.key_package]);
    alice.merge(&mut alice_group);
    let added = c
        .submit_commit(req(
            &tenant,
            msg_pb::SubmitCommitRequest {
                tenant_id: tenant.clone(),
                group_id: group_id.to_string(),
                sender_device_id: "alice-phone".to_string(),
                epoch: 2,
                commit: commit.clone(),
                welcome,
                group_info: alice.group_info(&alice_group),
                client_message_id: String::new(),
            },
        ))
        .await
        .expect("add carol")
        .into_inner();
    assert_eq!(added.epoch, 3);
    assert_eq!(
        added.member_device_ids,
        ["alice-phone", "bob-phone", "carol-laptop"]
    );
    let bob_commit = drain(&mut c, &tenant, "bob-phone", 0).await;
    bob.apply_commit(
        &mut bob_group,
        &bob_commit.last().expect("bob commit").payload,
    );
    let carol_inbox = drain(&mut c, &tenant, "carol-laptop", 0).await;
    assert_eq!(carol_inbox.len(), 1);
    assert_eq!(carol_inbox[0].kind, "WELCOME");
    let mut carol_group = carol.join(&carol_inbox[0].payload);
    let info = c
        .get_group_info(req(
            &tenant,
            msg_pb::GetGroupInfoRequest {
                tenant_id: tenant.clone(),
                group_id: group_id.to_string(),
            },
        ))
        .await
        .expect("group info")
        .into_inner();
    assert_eq!((info.epoch, info.group_info_epoch), (3, 3));
    assert!(!info.ratchet_tree.is_empty() && !info.group_info.is_empty());

    // Carol follows her inbox live while alice sends.
    let mut follower = client
        .clone()
        .stream_inbox(req(
            &tenant,
            msg_pb::StreamInboxRequest {
                tenant_id: tenant.clone(),
                device_id: "carol-laptop".to_string(),
                after_seq: carol_inbox[0].seq,
                follow: true,
                max_entries: 1,
            },
        ))
        .await
        .expect("follow carol inbox")
        .into_inner();

    // SendMessage fans out to all three member devices.
    const SECRET: &[u8] = b"udb-e2ee-plaintext-canary";
    let ciphertext = alice.encrypt(&mut alice_group, SECRET);
    let sent = send(
        &mut c,
        &tenant,
        group_id,
        "alice-phone",
        ciphertext.clone(),
        "msg-1",
    )
    .await
    .expect("send");
    assert_eq!((sent.fanout, sent.epoch, sent.duplicate), (3, 3, false));
    let replay = send(
        &mut c,
        &tenant,
        group_id,
        "alice-phone",
        ciphertext.clone(),
        "msg-1",
    )
    .await
    .expect("replayed send");
    assert!(replay.duplicate, "a retried send is not fanned out twice");
    assert_eq!(replay.fanout, 3);

    let live = tokio::time::timeout(std::time::Duration::from_secs(10), follower.message())
        .await
        .expect("the follower wakes on the append")
        .expect("follower item")
        .expect("an entry");
    let live = live.entry.expect("entry");
    assert_eq!(live.kind, "MESSAGE");
    assert_eq!(carol.decrypt(&mut carol_group, &live.payload), SECRET);

    let mut delivered = BTreeMap::new();
    for device in ["alice-phone", "bob-phone", "carol-laptop"] {
        let messages: Vec<_> = drain(&mut c, &tenant, device, 0)
            .await
            .into_iter()
            .filter(|entry| entry.kind == "MESSAGE")
            .collect();
        assert_eq!(
            messages.len(),
            1,
            "{device} receives the message exactly once"
        );
        assert_eq!(messages[0].sender_device_id, "alice-phone");
        assert_eq!(messages[0].client_message_id, "msg-1");
        delivered.insert(device, messages[0].clone());
    }
    assert_eq!(
        bob.decrypt(&mut bob_group, &delivered["bob-phone"].payload),
        SECRET
    );

    let bad = send(
        &mut c,
        &tenant,
        group_id,
        "alice-phone",
        b"not mls".to_vec(),
        "",
    )
    .await
    .expect_err("garbage is not an MLS message");
    assert_eq!(bad.code(), Code::InvalidArgument);

    // Ack: bob's cursor advances, the stream resumes after it, and an ack past
    // the head is clamped instead of skipping future entries.
    let bob_all = drain(&mut c, &tenant, "bob-phone", 0).await;
    let bob_head = bob_all.last().expect("bob entries").seq;
    let acked = c
        .ack_inbox(req(
            &tenant,
            msg_pb::AckInboxRequest {
                tenant_id: tenant.clone(),
                device_id: "bob-phone".to_string(),
                up_to_seq: bob_head + 100,
            },
        ))
        .await
        .expect("ack")
        .into_inner();
    assert_eq!(
        acked.acked_seq, bob_head,
        "ack is clamped to the allocated head"
    );
    assert_eq!(acked.acknowledged, bob_all.len() as i64);
    assert!(
        drain(&mut c, &tenant, "bob-phone", 0).await.is_empty(),
        "the cursor resumes after the ack"
    );

    // One inbox.appended event per inbox row, naming only tenant/device/seq.
    let rows = inbox_rows(&pool, &tenant).await;
    let events = outbox_events_for(&pool, &tenant).await;
    assert_eq!(events.len() as i64, rows, "every inbox row has its event");
    for event in &events {
        let payload = event["payload"].as_object().expect("domain payload");
        let mut keys: Vec<&str> = payload.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, ["device_id", "seq", "tenant_id"]);
    }

    // The broker database holds no plaintext anywhere.
    let needle = SECRET.to_vec();
    let bytea_columns: Vec<(String, String)> = sqlx::query_as(
        "SELECT table_name::text, column_name::text FROM information_schema.columns \
         WHERE table_schema = 'udb_messaging' AND data_type = 'bytea'",
    )
    .fetch_all(&pool)
    .await
    .expect("list bytea columns");
    assert!(bytea_columns.len() >= 6);
    for (table, column) in &bytea_columns {
        let hits: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM udb_messaging.\"{table}\" WHERE position($1 IN \"{column}\") > 0"
        ))
        .bind(&needle)
        .fetch_one(&pool)
        .await
        .expect("plaintext scan");
        assert_eq!(hits, 0, "plaintext found in udb_messaging.{table}.{column}");
    }
    let text_hits: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM udb_system.outbox_events WHERE payload::text LIKE '%' || $1 || '%'",
    )
    .bind(String::from_utf8_lossy(SECRET).to_string())
    .fetch_one(&pool)
    .await
    .expect("outbox plaintext scan");
    assert_eq!(text_hits, 0);

    // Revoking carol: alice and bob get a removal hint; carol stops receiving.
    let revoked = c
        .revoke_messaging_device(req(
            &tenant,
            msg_pb::RevokeMessagingDeviceRequest {
                tenant_id: tenant.clone(),
                device_id: "carol-laptop".to_string(),
                reason: "lost".to_string(),
            },
        ))
        .await
        .expect("revoke")
        .into_inner();
    assert_eq!(revoked.group_ids, [group_id]);
    let hint = drain(&mut c, &tenant, "bob-phone", 0).await;
    assert_eq!(hint.len(), 1);
    assert_eq!(hint[0].kind, "REMOVAL_HINT");
    let hint_json: serde_json::Value = serde_json::from_slice(&hint[0].payload).expect("hint json");
    assert_eq!(hint_json["device_id"], "carol-laptop");
    let after_revoke = send(
        &mut c,
        &tenant,
        group_id,
        "alice-phone",
        alice.encrypt(&mut alice_group, b"after revoke"),
        "msg-2",
    )
    .await
    .expect("send after revoke");
    assert_eq!(
        after_revoke.fanout, 2,
        "a revoked device receives no fan-out"
    );

    // Retention: acknowledged rows go first; expired rows go after the TTL.
    let ttl = std::time::Duration::from_secs(30 * 24 * 3600);
    let before = inbox_rows(&pool, &tenant).await;
    let deleted = run_inbox_retention_once(&pool, ttl, 1000)
        .await
        .expect("retention pass");
    let after = inbox_rows(&pool, &tenant).await;
    assert!(
        deleted >= acked.acknowledged,
        "acknowledged rows are collected"
    );
    assert_eq!(
        before - after,
        acked.acknowledged,
        "only bob's acknowledged rows were collected"
    );
    sqlx::query(
        "UPDATE udb_messaging.inbox SET created_at = NOW() - INTERVAL '31 days' \
         WHERE tenant_id = $1 AND device_id = 'carol-laptop'",
    )
    .bind(&tenant)
    .execute(&pool)
    .await
    .expect("age carol's rows");
    run_inbox_retention_once(&pool, ttl, 1000)
        .await
        .expect("second retention pass");
    let carol_left: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM udb_messaging.inbox WHERE tenant_id = $1 AND device_id = 'carol-laptop'",
    )
    .bind(&tenant)
    .fetch_one(&pool)
    .await
    .expect("count carol rows");
    assert_eq!(carol_left, 0, "rows older than the TTL are collected");
    assert!(
        inbox_rows(&pool, &tenant).await > 0,
        "unacknowledged fresh rows survive"
    );
}

#[allow(clippy::too_many_arguments)]
async fn submit_with_info(
    client: &mut Client,
    tenant: &str,
    group_id: &str,
    sender: &str,
    epoch: i64,
    commit: Vec<u8>,
    welcome: Vec<u8>,
    group_info: Vec<u8>,
) -> Result<msg_pb::SubmitCommitResponse, Status> {
    client
        .submit_commit(req(
            tenant,
            msg_pb::SubmitCommitRequest {
                tenant_id: tenant.to_string(),
                group_id: group_id.to_string(),
                sender_device_id: sender.to_string(),
                epoch,
                commit,
                welcome,
                group_info,
                client_message_id: String::new(),
            },
        ))
        .await
        .map(tonic::Response::into_inner)
}

async fn create_with_policy(
    client: &mut Client,
    tenant: &str,
    group_id: &str,
    creator: &str,
    info: Vec<u8>,
    tree: Vec<u8>,
    policy: msg_pb::ExternalJoinPolicy,
) -> Result<msg_pb::CreateGroupResponse, Status> {
    client
        .create_group(req(
            tenant,
            msg_pb::CreateGroupRequest {
                tenant_id: tenant.to_string(),
                group_id: group_id.to_string(),
                creator_device_id: creator.to_string(),
                group_info: info,
                ratchet_tree: tree,
                external_join_policy: policy as i32,
            },
        ))
        .await
        .map(tonic::Response::into_inner)
}

async fn group_info(
    client: &mut Client,
    tenant: &str,
    group_id: &str,
) -> msg_pb::GetGroupInfoResponse {
    client
        .get_group_info(req(
            tenant,
            msg_pb::GetGroupInfoRequest {
                tenant_id: tenant.to_string(),
                group_id: group_id.to_string(),
            },
        ))
        .await
        .expect("group info")
        .into_inner()
}

/// GetGroupInfo as the end user `subject` calls it (the verified claim scopes
/// the handler), straight on the service.
async fn group_info_as(
    tenant: &str,
    subject: &str,
    group_id: &str,
) -> Result<msg_pb::GetGroupInfoResponse, Status> {
    use crate::proto::udb::core::messaging::services::v1::messaging_service_server::MessagingService as _;
    use crate::runtime::service::method_security::{
        VerifiedClaimContext, scope_claim_context_for_test,
    };
    let svc = messaging_service().await;
    let request = req(
        tenant,
        msg_pb::GetGroupInfoRequest {
            tenant_id: tenant.to_string(),
            group_id: group_id.to_string(),
        },
    );
    let claim = VerifiedClaimContext {
        authenticated: true,
        subject: subject.to_string(),
        tenant_id: tenant.to_string(),
        ..Default::default()
    };
    scope_claim_context_for_test(claim, svc.get_group_info(request))
        .await
        .map(tonic::Response::into_inner)
}

/// External joins: a SAME_PRINCIPAL group admits another device of a member's
/// principal (it gets the COMMIT echo, not a Welcome, and everyone reads its
/// messages) and refuses a stranger; a GroupInfo from a past epoch is stale; a
/// NONE group refuses every external join; a group open to joins requires the
/// GroupInfo on every commit; and an end user cannot read the GroupInfo of a
/// group it is not in.
#[tokio::test]
#[ignore = "requires live Postgres; run with cargo test --lib messaging_service::tests::live -- --ignored --test-threads=1"]
async fn live_messaging_external_join_follows_the_group_policy() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let (client, _shutdown) = serve().await;
    let mut c = client.clone();
    let tenant = uuid::Uuid::new_v4().to_string();
    let group_id = "family-room";

    let phone = Device::new(&tenant, "alice", "alice-phone");
    let laptop = Device::new(&tenant, "alice", "alice-laptop");
    let tablet = Device::new(&tenant, "alice", "alice-tablet");
    let bob = Device::new(&tenant, "bob", "bob-phone");
    let mallory = Device::new(&tenant, "mallory", "mallory-phone");
    for (subject, device) in [
        ("alice", &phone),
        ("alice", &laptop),
        ("alice", &tablet),
        ("bob", &bob),
        ("mallory", &mallory),
    ] {
        register(&mut c, &tenant, subject, device).await;
    }
    publish(&mut c, &tenant, &bob, 1, false).await;

    let (mut phone_group, info, tree) = phone.create_group(group_id);
    let created = create_with_policy(
        &mut c,
        &tenant,
        group_id,
        "alice-phone",
        info,
        tree,
        msg_pb::ExternalJoinPolicy::SamePrincipal,
    )
    .await
    .expect("create a SAME_PRINCIPAL group");
    assert_eq!(created.epoch, 0);

    // (e) A group open to external joins needs the GroupInfo on every commit.
    let bob_kp = claim(&mut c, &tenant, "alice-phone", "bob-phone").await;
    let (commit, welcome) = phone.add(&mut phone_group, &[bob_kp.key_package]);
    let missing = submit(
        &mut c,
        &tenant,
        group_id,
        "alice-phone",
        0,
        commit.clone(),
        welcome.clone(),
        "",
    )
    .await
    .expect_err("a commit without the GroupInfo is refused");
    assert_eq!(missing.code(), Code::InvalidArgument);
    phone.merge(&mut phone_group);
    let added = submit_with_info(
        &mut c,
        &tenant,
        group_id,
        "alice-phone",
        0,
        commit,
        welcome,
        phone.group_info(&phone_group),
    )
    .await
    .expect("add bob with the GroupInfo");
    assert_eq!(added.epoch, 1);
    let bob_inbox = drain(&mut c, &tenant, "bob-phone", 0).await;
    let mut bob_group = bob.join(&bob_inbox[0].payload);
    let phone_seq = drain(&mut c, &tenant, "alice-phone", 0)
        .await
        .last()
        .map_or(0, |entry| entry.seq);

    let served = group_info(&mut c, &tenant, group_id).await;
    assert_eq!((served.epoch, served.group_info_epoch), (1, 1));
    assert_eq!(
        served.external_join_policy,
        msg_pb::ExternalJoinPolicy::SamePrincipal as i32
    );

    // (b) A device of a principal with no member device is refused.
    let (_mallory_group, mallory_commit, mallory_info) =
        mallory.external_join(&served.group_info, &served.ratchet_tree);
    let refused = submit_with_info(
        &mut c,
        &tenant,
        group_id,
        "mallory-phone",
        1,
        mallory_commit,
        Vec::new(),
        mallory_info,
    )
    .await
    .expect_err("a stranger cannot join externally");
    assert_eq!(refused.code(), Code::PermissionDenied);
    assert_eq!(
        decode_detail(&refused).policy_decision_id,
        "messaging_external_join_not_permitted"
    );

    // (a) The laptop of alice joins: it gets the COMMIT echo, the members
    // apply it.
    let (mut laptop_group, laptop_commit, laptop_info) =
        laptop.external_join(&served.group_info, &served.ratchet_tree);
    let joined = submit_with_info(
        &mut c,
        &tenant,
        group_id,
        "alice-laptop",
        1,
        laptop_commit,
        Vec::new(),
        laptop_info,
    )
    .await
    .expect("the laptop joins externally");
    assert_eq!(joined.epoch, 2);
    assert_eq!(joined.added_device_ids, ["alice-laptop"]);
    assert_eq!(
        joined.member_device_ids,
        ["alice-laptop", "alice-phone", "bob-phone"]
    );
    let laptop_inbox = drain(&mut c, &tenant, "alice-laptop", 0).await;
    assert_eq!(laptop_inbox.len(), 1);
    assert_eq!(laptop_inbox[0].kind, "COMMIT", "a joiner gets no Welcome");
    assert_eq!(laptop_inbox[0].sender_device_id, "alice-laptop");
    let phone_entries = drain(&mut c, &tenant, "alice-phone", phone_seq).await;
    let phone_seq = phone_entries.last().expect("commit").seq;
    phone.apply_commit(
        &mut phone_group,
        &phone_entries.last().expect("commit").payload,
    );
    let bob_entries = drain(&mut c, &tenant, "bob-phone", bob_inbox[0].seq).await;
    let bob_seq = bob_entries.last().expect("commit").seq;
    bob.apply_commit(&mut bob_group, &bob_entries.last().expect("commit").payload);
    let refreshed = group_info(&mut c, &tenant, group_id).await;
    assert_eq!((refreshed.epoch, refreshed.group_info_epoch), (2, 2));

    let ciphertext = laptop.encrypt(&mut laptop_group, b"hello from the laptop");
    let sent = send(
        &mut c,
        &tenant,
        group_id,
        "alice-laptop",
        ciphertext,
        "laptop-1",
    )
    .await
    .expect("the joined device sends");
    assert_eq!(sent.fanout, 3);
    for (device, group, after) in [
        (&phone, &mut phone_group, phone_seq),
        (&bob, &mut bob_group, bob_seq),
    ] {
        let entries = drain(&mut c, &tenant, &device.device_id, after).await;
        let message = entries.last().expect("message");
        assert_eq!(message.kind, "MESSAGE");
        assert_eq!(
            device.decrypt(group, &message.payload),
            b"hello from the laptop"
        );
    }

    // (c) The epoch-1 GroupInfo is stale now.
    let (_tablet_group, stale_commit, stale_info) =
        tablet.external_join(&served.group_info, &served.ratchet_tree);
    let stale = submit_with_info(
        &mut c,
        &tenant,
        group_id,
        "alice-tablet",
        1,
        stale_commit,
        Vec::new(),
        stale_info,
    )
    .await
    .expect_err("a join from a past GroupInfo is stale");
    assert_eq!(stale.code(), Code::FailedPrecondition);

    // (d) A group created without a policy refuses every external join.
    let closed = "closed-room";
    let (_closed_group, closed_info, closed_tree) = phone.create_group(closed);
    create_with_policy(
        &mut c,
        &tenant,
        closed,
        "alice-phone",
        closed_info,
        closed_tree,
        msg_pb::ExternalJoinPolicy::Unspecified,
    )
    .await
    .expect("create a closed group");
    let closed_served = group_info(&mut c, &tenant, closed).await;
    assert_eq!(
        closed_served.external_join_policy,
        msg_pb::ExternalJoinPolicy::None as i32
    );
    let (_closed_join, closed_commit, closed_join_info) =
        tablet.external_join(&closed_served.group_info, &closed_served.ratchet_tree);
    let disabled = submit_with_info(
        &mut c,
        &tenant,
        closed,
        "alice-tablet",
        0,
        closed_commit,
        Vec::new(),
        closed_join_info,
    )
    .await
    .expect_err("a closed group refuses external joins");
    assert_eq!(disabled.code(), Code::PermissionDenied);
    assert_eq!(
        decode_detail(&disabled).policy_decision_id,
        "messaging_external_join_disabled"
    );

    // (f) An end user reads only the GroupInfo of a group it is in.
    let hidden = group_info_as(&tenant, "mallory", group_id)
        .await
        .expect_err("a non-member end user sees no GroupInfo");
    assert_eq!(hidden.code(), Code::NotFound);
    let visible = group_info_as(&tenant, "bob", group_id)
        .await
        .expect("a member end user reads the GroupInfo");
    assert_eq!(visible.epoch, 2);
}
