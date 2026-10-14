//! Served end-to-end encrypted messaging through the Rust SDK (feature `mls`).
//!
//! Three devices of the live user (each a `MessagingClient` with its own MLS
//! state) talk to a real broker + Postgres over gRPC: register, publish and top
//! up KeyPackages, create a group, add by claimed KeyPackages, join from the
//! Welcome, exchange messages, rotate keys (one commit loses the epoch race and
//! re-bases on the winner), remove a member, and follow the inbox stream.
//!
//! Gated on `UDB_LIVE_SDK_TESTS=1`; without it the test passes without touching
//! the network. Needs `UDB_AUTH_GRPC_TARGET` (the native listener that serves
//! AuthnService and MessagingService), `UDB_LIVE_USERNAME`, `UDB_LIVE_PASSWORD`,
//! `UDB_LIVE_TENANT`, and optionally `UDB_LIVE_PROJECT`.
//!
//! Run: `cargo test --features mls --test live_messaging -- --nocapture`

// Compiled only with the `mls` feature (an empty test crate otherwise).
#![cfg(feature = "mls")]

use std::time::Duration;

use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use udb_client::messaging::mls::{DeviceIdentity, InboxEvent, MemoryStore, MlsClient};
use udb_client::messaging::{MessagingClient, MessagingError};
use udb_client::proto::udb::core::authn::services::v1 as authn;
use udb_client::proto::udb::core::authn::services::v1::authn_service_client::AuthnServiceClient;
use udb_client::Metadata;

type Device = MessagingClient<MemoryStore>;

fn required(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| panic!("{name} is required when UDB_LIVE_SDK_TESTS=1"))
}

fn endpoint(address: &str) -> String {
    if address.contains("://") {
        address.to_string()
    } else {
        format!("http://{address}")
    }
}

fn request<T>(meta: &Metadata, message: T) -> Request<T> {
    let mut request = Request::new(message);
    meta.apply(&mut request).expect("metadata");
    request
        .metadata_mut()
        .insert("x-request-id", "rust-live-messaging".parse().unwrap());
    request
}

/// Log in and resolve the canonical tenant id and subject behind the token.
async fn login(channel: Channel) -> (Metadata, String) {
    let mut base = Metadata::new(required("UDB_LIVE_TENANT"))
        .with_project(std::env::var("UDB_LIVE_PROJECT").unwrap_or_else(|_| "default".into()));
    base.purpose = "rust.live.messaging".into();
    let mut authn = AuthnServiceClient::new(channel);
    let login = authn
        .login(request(
            &base,
            authn::LoginRequest {
                username: required("UDB_LIVE_USERNAME"),
                password: required("UDB_LIVE_PASSWORD"),
                device_name: "rust-sdk-live-messaging".into(),
                tenant_hint: base.tenant_id.clone(),
                project_hint: base.project_id.clone(),
                ..Default::default()
            },
        ))
        .await
        .expect("login")
        .into_inner();
    assert!(!login.access_token.is_empty(), "login issued no token");
    let principal = authn
        .authenticate(request(
            &base,
            authn::AuthnRequest {
                bearer_token: login.access_token.clone(),
                tenant_hint: base.tenant_id.clone(),
                project_hint: base.project_id.clone(),
                ..Default::default()
            },
        ))
        .await
        .expect("authenticate")
        .into_inner()
        .principal
        .expect("principal");
    base.tenant_id = principal.tenant_id.clone();
    (
        base.with_bearer_token(login.access_token),
        principal.subject,
    )
}

async fn device(channel: &Channel, meta: &Metadata, subject: &str, device_id: &str) -> Device {
    let mls = MlsClient::create(
        DeviceIdentity::new(&meta.tenant_id, subject, device_id).expect("identity"),
        MemoryStore::new(),
    )
    .expect("device keys");
    let mut client = MessagingClient::with_channel(channel.clone(), meta.clone(), mls)
        .with_key_package_stock(3, 3);
    let registered = client.register_device().await.expect("register device");
    assert_eq!(registered.device_id, device_id);
    assert_eq!(registered.status, "ACTIVE");
    let stock = client
        .ensure_key_packages()
        .await
        .expect("publish key packages");
    assert_eq!(stock.published, 3);
    assert_eq!(stock.one_time_remaining, 3);
    assert!(stock.has_last_resort);
    client
}

fn messages(events: &[InboxEvent]) -> Vec<(String, String)> {
    events
        .iter()
        .filter_map(|event| match event {
            InboxEvent::Message {
                sender_device_id,
                plaintext,
                ..
            } => Some((
                sender_device_id.clone(),
                String::from_utf8_lossy(plaintext).into_owned(),
            )),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn live_e2ee_group_chat_through_the_rust_sdk() {
    if std::env::var("UDB_LIVE_SDK_TESTS").as_deref() != Ok("1") {
        eprintln!("skipped: the live messaging test needs UDB_LIVE_SDK_TESTS=1");
        return;
    }
    let channel = Endpoint::from_shared(endpoint(&required("UDB_AUTH_GRPC_TARGET")))
        .expect("endpoint")
        .connect()
        .await
        .expect("connect the native listener");
    let (meta, subject) = login(channel.clone()).await;
    let run = format!(
        "{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let (a_id, b_id, c_id) = (
        format!("rs-a-{run}"),
        format!("rs-b-{run}"),
        format!("rs-c-{run}"),
    );
    let group = format!("rs-room-{run}");
    let mut a = device(&channel, &meta, &subject, &a_id).await;
    let mut b = device(&channel, &meta, &subject, &b_id).await;
    let mut c = device(&channel, &meta, &subject, &c_id).await;

    // The directory publishes the key each device holds.
    let listed = a.list_devices("", false).await.expect("list devices");
    let b_registration = b.mls().registration().unwrap();
    assert!(listed.iter().any(|d| d.device_id == b_id
        && d.key_fingerprint == b_registration.key_fingerprint
        && d.signature_public_key == b_registration.signature_public_key));

    // Create, add two devices by claimed KeyPackage, join from the Welcome.
    assert_eq!(a.create_group(&group).await.expect("create group"), 0);
    let added = a
        .add_members(&group, &[], &[b_id.as_str(), c_id.as_str()])
        .await
        .expect("add members");
    assert!(added.committed);
    assert_eq!(added.epoch, 1);
    assert_eq!(added.added_device_ids, [b_id.clone(), c_id.clone()]);
    assert!(added.missing_device_ids.is_empty());
    for (client, id) in [(&mut b, &b_id), (&mut c, &c_id)] {
        let events = client.sync().await.expect("sync");
        assert!(
            matches!(&events[..], [InboxEvent::Joined { epoch: 1, members, added_by, .. }]
                if members.len() == 3 && added_by == &a_id),
            "{id}: {events:?}"
        );
    }
    // A claim consumed one package: the next top-up republishes exactly one.
    let topped = b.ensure_key_packages().await.expect("top up");
    assert_eq!((topped.published, topped.one_time_remaining), (1, 3));

    // Messages: everyone else decrypts; the sender skips its own echo.
    a.send(&group, b"hello from a").await.expect("send");
    b.send(&group, b"hello from b").await.expect("send");
    assert_eq!(
        messages(&c.sync().await.unwrap()),
        [
            (a_id.clone(), "hello from a".to_string()),
            (b_id.clone(), "hello from b".to_string())
        ]
    );
    assert_eq!(
        messages(&b.sync().await.unwrap()),
        [(a_id.clone(), "hello from a".to_string())]
    );
    let a_events = a.sync().await.unwrap();
    assert_eq!(
        messages(&a_events),
        [(b_id.clone(), "hello from b".to_string())]
    );
    assert!(a_events
        .iter()
        .any(|e| matches!(e, InboxEvent::OwnCommit { epoch: 1, .. })));

    // Key rotation with a lost race: b's update lands at epoch 1; c, still at
    // epoch 1, is refused with the current epoch and re-bases on b's commit.
    let b_update = b.update_keys(&group).await.expect("b rotates keys");
    assert_eq!((b_update.epoch, b_update.rebases), (2, 0));
    let c_update = c.update_keys(&group).await.expect("c rotates keys");
    assert_eq!(
        (c_update.epoch, c_update.rebases),
        (3, 1),
        "c's stale commit re-based once"
    );
    assert_eq!(c.mls().epoch(&group).unwrap(), 3);
    // The commits it caught up on are still delivered as events.
    let c_events = c.sync().await.unwrap();
    assert!(
        c_events.iter().any(
            |e| matches!(e, InboxEvent::Commit { epoch: 2, sender_device_id, .. } if sender_device_id == &b_id)
        ),
        "{c_events:?}"
    );
    for client in [&mut a, &mut b] {
        client.sync().await.unwrap();
        assert_eq!(client.mls().epoch(&group).unwrap(), 3);
    }

    // Remove c: the others move on, c learns it was removed and gets nothing new.
    let removed = a
        .remove_devices(&group, &[c_id.as_str()])
        .await
        .expect("remove c");
    assert_eq!(
        (removed.epoch, removed.removed_device_ids.clone()),
        (4, vec![c_id.clone()])
    );
    let c_events = c.sync().await.unwrap();
    assert!(
        c_events
            .iter()
            .any(|e| matches!(e, InboxEvent::Removed { removed_by, .. } if removed_by == &a_id)),
        "{c_events:?}"
    );
    assert!(c.mls().group_ids().is_empty());
    let b_events = b.sync().await.unwrap();
    assert!(
        b_events.iter().any(|e| matches!(e, InboxEvent::Commit { epoch: 4, removed, .. } if removed == std::slice::from_ref(&c_id))),
        "{b_events:?}"
    );

    // Follow the inbox stream: b waits, a sends after the removal.
    let sent = a.send(&group, b"after c left").await.expect("send");
    assert_eq!(sent.fanout, 2, "a and b only");
    let mut received = None;
    for _ in 0..5 {
        match b
            .next_event_timeout(Duration::from_secs(10))
            .await
            .expect("follow inbox")
        {
            Some(event @ InboxEvent::Message { .. }) => {
                received = Some(event);
                break;
            }
            Some(_) => continue,
            None => break,
        }
    }
    assert_eq!(
        messages(&received.into_iter().collect::<Vec<_>>()),
        [(a_id.clone(), "after c left".to_string())]
    );
    assert!(messages(&c.sync().await.unwrap()).is_empty());

    // Clean up: revoke the run's devices.
    for (client, id) in [(&mut a, &a_id), (&mut b, &b_id), (&mut c, &c_id)] {
        let revoked = client.revoke_device(id, "live test cleanup").await;
        match revoked {
            Ok(response) => assert!(response.revoked),
            Err(MessagingError::Rpc(err)) => panic!("revoke {id}: {err}"),
            Err(other) => panic!("revoke {id}: {other}"),
        }
    }
}

/// Opt-in encrypted backup: a device backs its state up under a recovery
/// secret, is lost, and is restored on a new machine from the broker-held
/// envelope; the restored device reads what arrived while it was gone and
/// keeps decrypting new messages. A wrong secret does not open the backup and
/// a deleted backup is gone.
#[tokio::test(flavor = "multi_thread")]
async fn live_backup_restores_a_device_on_a_new_machine() {
    if std::env::var("UDB_LIVE_SDK_TESTS").as_deref() != Ok("1") {
        eprintln!("skipped: the live messaging test needs UDB_LIVE_SDK_TESTS=1");
        return;
    }
    let channel = Endpoint::from_shared(endpoint(&required("UDB_AUTH_GRPC_TARGET")))
        .expect("endpoint")
        .connect()
        .await
        .expect("connect the native listener");
    let (meta, subject) = login(channel.clone()).await;
    let run = format!(
        "{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let (a_id, b_id) = (format!("rs-bk-a-{run}"), format!("rs-bk-b-{run}"));
    let group = format!("rs-backup-room-{run}");
    let backup_id = format!("rs-backup-{run}");
    let secret = b"a long recovery passphrase only the user knows";
    let mut a = device(&channel, &meta, &subject, &a_id).await;
    let mut b = device(&channel, &meta, &subject, &b_id).await;

    a.create_group(&group).await.expect("create group");
    a.add_members(&group, &[], &[b_id.as_str()])
        .await
        .expect("add b");
    b.sync().await.expect("b joins");
    a.send(&group, b"before the backup").await.expect("send");
    assert_eq!(
        messages(&b.sync().await.unwrap()),
        [(a_id.clone(), "before the backup".to_string())]
    );
    let stored = b
        .backup(secret, b"b's chat history", &backup_id)
        .await
        .expect("back up b");
    assert_eq!(
        (stored.backup_id.as_str(), stored.version),
        (backup_id.as_str(), 1)
    );

    // b's machine is lost; a keeps talking.
    drop(b);
    a.send(&group, b"while b was away").await.expect("send");

    let wrong = MessagingClient::restore(
        channel.clone(),
        meta.clone(),
        "",
        &backup_id,
        b"not the recovery passphrase",
        MemoryStore::new(),
    )
    .await;
    assert!(
        matches!(wrong, Err(MessagingError::Mls(_))),
        "a wrong secret does not open the backup: {wrong:?}"
    );
    let (mut restored, history) = MessagingClient::restore(
        channel.clone(),
        meta.clone(),
        "",
        &backup_id,
        secret,
        MemoryStore::new(),
    )
    .await
    .expect("restore b on a new machine");
    assert_eq!(history, b"b's chat history");
    assert_eq!(restored.mls().identity().device_id, b_id);
    assert_eq!(
        messages(&restored.sync().await.unwrap()),
        [(a_id.clone(), "while b was away".to_string())]
    );
    // The restored device follows a key rotation and keeps decrypting, both ways.
    a.update_keys(&group).await.expect("a rotates keys");
    a.send(&group, b"after the restore").await.expect("send");
    assert_eq!(
        messages(&restored.sync().await.unwrap()),
        [(a_id.clone(), "after the restore".to_string())]
    );
    restored
        .send(&group, b"b is back")
        .await
        .expect("restored device sends");
    assert_eq!(
        messages(&a.sync().await.unwrap()),
        [(b_id.clone(), "b is back".to_string())]
    );

    assert!(restored.delete_backup(&backup_id).await.expect("delete"));
    let gone = MessagingClient::restore(
        channel.clone(),
        meta.clone(),
        "",
        &backup_id,
        secret,
        MemoryStore::new(),
    )
    .await;
    assert!(
        matches!(&gone, Err(MessagingError::Rpc(err)) if err.code() == tonic::Code::NotFound),
        "{gone:?}"
    );
    for (client, id) in [(&mut a, &a_id), (&mut restored, &b_id)] {
        client
            .revoke_device(id, "live test cleanup")
            .await
            .expect("revoke");
    }
}
