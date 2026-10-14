//! A scripted messaging device, one command per invocation, its MLS state kept
//! in a file between runs. The TypeScript SDK's live interop test drives it as
//! the Rust member of a Rust<->TypeScript group; it is also a compact example of
//! the `MessagingClient` API with a `FileStore`.
//!
//! ```text
//! messaging_peer <state-file> init <device-id>     register + publish KeyPackages
//! messaging_peer <state-file> sync                 process the inbox, one JSON event per line
//! messaging_peer <state-file> send <group> <text>  encrypt and send
//! messaging_peer <state-file> update <group>       rotate this device's keys in the group
//! messaging_peer <state-file> revoke               revoke this device
//! ```
//!
//! Connection and login come from `UDB_AUTH_GRPC_TARGET`, `UDB_LIVE_USERNAME`,
//! `UDB_LIVE_PASSWORD`, `UDB_LIVE_TENANT` and `UDB_LIVE_PROJECT`.
//!
//! Build: `cargo build --features mls --example messaging_peer`

use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use udb_client::messaging::mls::{DeviceIdentity, FileStore, InboxEvent, MlsClient};
use udb_client::messaging::MessagingClient;
use udb_client::proto::udb::core::authn::services::v1 as authn;
use udb_client::proto::udb::core::authn::services::v1::authn_service_client::AuthnServiceClient;
use udb_client::serde_json::json;
use udb_client::Metadata;

type Failure = Box<dyn std::error::Error>;

fn env(name: &str) -> Result<String, Failure> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("{name} is required").into())
}

fn request<T>(meta: &Metadata, message: T) -> Result<Request<T>, Failure> {
    let mut request = Request::new(message);
    meta.apply(&mut request)?;
    request
        .metadata_mut()
        .insert("x-request-id", "rust-messaging-peer".parse()?);
    Ok(request)
}

/// Log in; the metadata carries the canonical tenant id and the bearer token.
async fn login(channel: Channel) -> Result<(Metadata, String), Failure> {
    let mut meta = Metadata::new(env("UDB_LIVE_TENANT")?)
        .with_project(std::env::var("UDB_LIVE_PROJECT").unwrap_or_else(|_| "default".into()));
    meta.purpose = "rust.messaging.peer".into();
    let mut client = AuthnServiceClient::new(channel);
    let login = client
        .login(request(
            &meta,
            authn::LoginRequest {
                username: env("UDB_LIVE_USERNAME")?,
                password: env("UDB_LIVE_PASSWORD")?,
                device_name: "rust-messaging-peer".into(),
                tenant_hint: meta.tenant_id.clone(),
                project_hint: meta.project_id.clone(),
                ..Default::default()
            },
        )?)
        .await?
        .into_inner();
    let principal = client
        .authenticate(request(
            &meta,
            authn::AuthnRequest {
                bearer_token: login.access_token.clone(),
                tenant_hint: meta.tenant_id.clone(),
                project_hint: meta.project_id.clone(),
                ..Default::default()
            },
        )?)
        .await?
        .into_inner()
        .principal
        .ok_or("Authenticate returned no principal")?;
    meta.tenant_id = principal.tenant_id;
    Ok((
        meta.with_bearer_token(login.access_token),
        principal.subject,
    ))
}

fn event_json(event: &InboxEvent) -> udb_client::serde_json::Value {
    match event {
        InboxEvent::Joined {
            group_id,
            epoch,
            members,
            added_by,
            ..
        } => {
            json!({"kind": "joined", "group_id": group_id, "epoch": epoch, "members": members, "device_id": added_by})
        }
        InboxEvent::Commit {
            group_id,
            epoch,
            sender_device_id,
            added,
            removed,
            ..
        } => {
            json!({"kind": "commit", "group_id": group_id, "epoch": epoch, "device_id": sender_device_id, "added": added, "removed": removed})
        }
        InboxEvent::OwnCommit {
            group_id,
            epoch,
            merged,
            ..
        } => json!({"kind": "own_commit", "group_id": group_id, "epoch": epoch, "merged": merged}),
        InboxEvent::Removed {
            group_id,
            removed_by,
            ..
        } => json!({"kind": "removed", "group_id": group_id, "device_id": removed_by}),
        InboxEvent::Message {
            group_id,
            epoch,
            sender_device_id,
            plaintext,
            ..
        } => {
            json!({"kind": "message", "group_id": group_id, "epoch": epoch, "device_id": sender_device_id, "text": String::from_utf8_lossy(plaintext)})
        }
        InboxEvent::RemovalHint {
            group_id,
            device_id,
            reason,
            ..
        } => {
            json!({"kind": "removal_hint", "group_id": group_id, "device_id": device_id, "reason": reason})
        }
        InboxEvent::Skipped { reason, .. } => json!({"kind": "skipped", "reason": reason}),
        InboxEvent::Undecryptable {
            group_id, reason, ..
        } => json!({"kind": "undecryptable", "group_id": group_id, "reason": reason}),
    }
}

#[tokio::main]
async fn main() -> Result<(), Failure> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (state, command) = match args.as_slice() {
        [state, command, ..] => (state.clone(), command.as_str()),
        _ => {
            return Err(
                "usage: messaging_peer <state-file> <init|sync|send|update|revoke> ...".into(),
            )
        }
    };
    let target = env("UDB_AUTH_GRPC_TARGET")?;
    let target = if target.contains("://") {
        target
    } else {
        format!("http://{target}")
    };
    let channel = Endpoint::from_shared(target)?.connect().await?;
    let (meta, subject) = login(channel.clone()).await?;
    let mls = if command == "init" {
        let device_id = args.get(2).ok_or("init needs a device id")?;
        MlsClient::create(
            DeviceIdentity::new(&meta.tenant_id, subject, device_id)?,
            FileStore::new(&state),
        )?
    } else {
        MlsClient::open(FileStore::new(&state))?
    };
    let mut client = MessagingClient::with_channel(channel, meta, mls).with_key_package_stock(5, 5);
    match (command, &args[2..]) {
        ("init", _) => {
            let device = client.register_device().await?;
            let stock = client.ensure_key_packages().await?;
            println!(
                "{}",
                json!({"device_id": device.device_id, "key_fingerprint": device.key_fingerprint, "published": stock.published})
            );
        }
        ("sync", _) => {
            for event in client.sync().await? {
                println!("{}", event_json(&event));
            }
        }
        ("send", [group, text]) => {
            let sent = client.send(group, text.as_bytes()).await?;
            println!(
                "{}",
                json!({"sent": true, "epoch": sent.epoch, "fanout": sent.fanout})
            );
        }
        ("update", [group]) => {
            let outcome = client.update_keys(group).await?;
            println!(
                "{}",
                json!({"epoch": outcome.epoch, "rebases": outcome.rebases})
            );
        }
        ("revoke", _) => {
            let device_id = client.mls().identity().device_id.clone();
            let revoked = client
                .revoke_device(&device_id, "interop test cleanup")
                .await?;
            println!("{}", json!({"revoked": revoked.revoked}));
        }
        _ => return Err(format!("unknown command or arguments: {args:?}").into()),
    }
    Ok(())
}
