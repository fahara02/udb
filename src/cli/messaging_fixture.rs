//! `udb messaging bench-fixture`: mint, with the real client crate (`udb-mls`),
//! every MLS object the MessagingService benchmark rows need for one tenant and
//! subject, so each measured RPC runs its success path.
//!
//! The command is offline and writes nothing to the broker. It prints one JSON
//! document with two parts:
//!
//! - `setup`: the requests the benchmark seed sends BEFORE the measurement
//!   (devices to register, KeyPackages to publish, groups to create, one commit
//!   that adds a second device to the message group so the measured device has
//!   inbox entries and the group is at epoch 1);
//! - `fixtures`: flat `<seed:KEY>` values (base64 for bytes) the bench bodies
//!   consume. Every mutation row gets a target of its own, so the rows succeed
//!   in any order and each one consumes its target exactly once.
//!
//! | row | target |
//! |---|---|
//! | RegisterMessagingDevice | a fourth device, minted but not registered |
//! | PublishKeyPackages | a fresh one-time KeyPackage of the measured device |
//! | ClaimKeyPackages | the peer device's published KeyPackages |
//! | CreateGroup | a third group, minted but not created |
//! | SubmitCommit | an Add commit + Welcome at epoch 0 of the commit group |
//! | SendMessage | a PrivateMessage for the message group's epoch 1 |
//! | RevokeMessagingDevice | a disposable registered device in no group |
//! | reads | the measured device and the message group |

use base64::Engine as _;
use serde_json::{Map, Value, json};
use udb_mls::{CommitIntent, DeviceIdentity, MemoryStore, MlsClient};

/// Device and group ids, under one prefix.
struct Names {
    device: String,
    peer: String,
    revoke: String,
    register: String,
    message_group: String,
    commit_group: String,
    new_group: String,
}

impl Names {
    fn new(prefix: &str) -> Self {
        Self {
            device: format!("{prefix}-device"),
            peer: format!("{prefix}-peer"),
            revoke: format!("{prefix}-revoke"),
            register: format!("{prefix}-register"),
            message_group: format!("{prefix}-group"),
            commit_group: format!("{prefix}-commit-group"),
            new_group: format!("{prefix}-new-group"),
        }
    }
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn device(tenant: &str, subject: &str, device_id: &str) -> Result<MlsClient<MemoryStore>, String> {
    let identity =
        DeviceIdentity::new(tenant, subject, device_id).map_err(|err| err.to_string())?;
    MlsClient::create(identity, MemoryStore::new()).map_err(|err| err.to_string())
}

fn registration(client: &MlsClient<MemoryStore>) -> Result<Value, String> {
    let r = client.registration().map_err(|err| err.to_string())?;
    Ok(json!({
        "device_id": r.device_id,
        "signature_public_key": b64(&r.signature_public_key),
        "credential": b64(&r.credential),
    }))
}

/// One-time KeyPackages the peer device publishes at setup; the claim row
/// consumes one (and the last-resort package backs any further claim).
const PEER_ONE_TIME_PACKAGES: usize = 3;

/// Build the fixture document.
pub(crate) fn build(tenant: &str, subject: &str, prefix: &str) -> Result<Value, String> {
    let tenant = tenant.trim();
    let subject = subject.trim();
    if tenant.is_empty() || subject.is_empty() {
        return Err("--tenant and --subject are required".into());
    }
    let n = Names::new(prefix.trim());
    let err = |e: udb_mls::MlsError| e.to_string();

    let mut measured = device(tenant, subject, &n.device)?;
    let mut peer = device(tenant, subject, &n.peer)?;
    let revoke = device(tenant, subject, &n.revoke)?;
    let register = device(tenant, subject, &n.register)?;

    // Setup: the peer's published stock; the measured device's last resort.
    let peer_packages = peer.key_packages(PEER_ONE_TIME_PACKAGES).map_err(err)?;
    let peer_last_resort = peer.last_resort_key_package().map_err(err)?;
    let measured_last_resort = measured.last_resort_key_package().map_err(err)?;

    // Message group: created at epoch 0, the peer added at setup (epoch 1).
    let message_group = measured.create_group(&n.message_group).map_err(err)?;
    let add_peer = measured
        .commit(
            &n.message_group,
            CommitIntent::Add {
                key_packages: peer.key_packages(1).map_err(err)?,
            },
        )
        .map_err(err)?
        .ok_or("adding the peer produced no commit")?;
    measured.confirm_commit(&n.message_group).map_err(err)?;
    let welcome = add_peer
        .welcome
        .clone()
        .ok_or("an Add commit carries a Welcome")?;
    // SendMessage row: a PrivateMessage for epoch 1.
    let message = measured
        .encrypt(&n.message_group, b"udb messaging benchmark message")
        .map_err(err)?;

    // Commit group: created at setup; the SubmitCommit row adds the peer.
    let commit_group = measured.create_group(&n.commit_group).map_err(err)?;
    let row_commit = measured
        .commit(
            &n.commit_group,
            CommitIntent::Add {
                key_packages: peer.key_packages(1).map_err(err)?,
            },
        )
        .map_err(err)?
        .ok_or("the commit row produced no commit")?;
    let row_welcome = row_commit
        .welcome
        .clone()
        .ok_or("the commit row's Add carries a Welcome")?;

    // CreateGroup row: a group nobody has created yet.
    let new_group = measured.create_group(&n.new_group).map_err(err)?;
    // PublishKeyPackages row: a fresh one-time package of the measured device.
    let publish = measured.key_packages(1).map_err(err)?.remove(0);
    let register_row = register.registration().map_err(err)?;

    let mut fixtures = Map::new();
    let mut put = |key: &str, value: String| {
        fixtures.insert(key.to_string(), Value::String(value));
    };
    put("mls_subject_id", subject.to_string());
    put("mls_device_id", n.device.clone());
    put("mls_peer_device_id", n.peer.clone());
    put("mls_revoke_device_id", n.revoke.clone());
    put("mls_register_device_id", n.register.clone());
    put(
        "mls_register_signature_key",
        b64(&register_row.signature_public_key),
    );
    put("mls_register_credential", b64(&register_row.credential));
    put("mls_key_package", b64(&publish));
    put("mls_group_id", n.message_group.clone());
    put("mls_message_ciphertext", b64(&message.ciphertext));
    put("mls_commit_group_id", n.commit_group.clone());
    put("mls_commit", b64(&row_commit.commit));
    put("mls_welcome", b64(&row_welcome));
    put("mls_new_group_id", n.new_group.clone());
    put("mls_new_group_info", b64(&new_group.group_info));
    put("mls_new_ratchet_tree", b64(&new_group.ratchet_tree));

    let group = |creation: &udb_mls::GroupCreation| {
        json!({
            "group_id": creation.group_id,
            "creator_device_id": n.device,
            "group_info": b64(&creation.group_info),
            "ratchet_tree": b64(&creation.ratchet_tree),
        })
    };
    Ok(json!({
        "schema": "udb.messaging.bench-fixture.v1",
        "tenant_id": tenant,
        "subject_id": subject,
        "setup": {
            "devices": [registration(&measured)?, registration(&peer)?, registration(&revoke)?],
            "key_packages": [
                {
                    "device_id": n.peer,
                    "key_packages": peer_packages.iter().map(|kp| b64(kp)).collect::<Vec<_>>(),
                    "last_resort_key_package": b64(&peer_last_resort),
                },
                {
                    "device_id": n.device,
                    "key_packages": Vec::<String>::new(),
                    "last_resort_key_package": b64(&measured_last_resort),
                },
            ],
            "groups": [group(&message_group), group(&commit_group)],
            "commits": [{
                "group_id": n.message_group,
                "sender_device_id": n.device,
                "epoch": add_peer.epoch,
                "commit": b64(&add_peer.commit),
                "welcome": b64(&welcome),
                "client_message_id": format!("{prefix}-setup-add-peer", prefix = prefix.trim()),
            }],
        },
        "fixtures": fixtures,
    }))
}

/// `udb messaging bench-fixture --tenant <id> --subject <id> [--prefix <p>] [--out <path>]`
pub(crate) fn run(tenant: &str, subject: &str, prefix: &str, out: Option<&str>) -> i32 {
    let document = match build(tenant, subject, prefix) {
        Ok(document) => document,
        Err(message) => {
            eprintln!("messaging bench-fixture: {message}");
            return 1;
        }
    };
    let text = match serde_json::to_string_pretty(&document) {
        Ok(text) => text,
        Err(err) => {
            eprintln!("messaging bench-fixture: encode: {err}");
            return 1;
        }
    };
    match out.filter(|path| !path.trim().is_empty() && *path != "-") {
        Some(path) => match std::fs::write(path, text + "\n") {
            Ok(()) => 0,
            Err(err) => {
                eprintln!("messaging bench-fixture: write {path}: {err}");
                1
            }
        },
        None => {
            println!("{text}");
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_fixture_names_the_subject_and_carries_every_row_input() {
        let doc = build("tenant-1", "subject-1", "sdk-perf-mls").expect("fixture");
        assert_eq!(doc["schema"], "udb.messaging.bench-fixture.v1");
        let fixtures = doc["fixtures"].as_object().expect("fixtures");
        for key in [
            "mls_device_id",
            "mls_register_credential",
            "mls_key_package",
            "mls_message_ciphertext",
            "mls_commit",
            "mls_welcome",
            "mls_new_group_info",
            "mls_new_ratchet_tree",
        ] {
            assert!(
                fixtures
                    .get(key)
                    .and_then(Value::as_str)
                    .is_some_and(|v| !v.is_empty()),
                "{key}"
            );
        }
        // The registered credential names <tenant>/<subject>/<device>.
        let credential = base64::engine::general_purpose::STANDARD
            .decode(fixtures["mls_register_credential"].as_str().unwrap())
            .unwrap();
        let identity = b"tenant-1/subject-1/sdk-perf-mls-register";
        assert!(credential.windows(identity.len()).any(|w| w == identity));
        // The setup commit moves the message group from epoch 0; the row commit
        // is for the separate commit group at epoch 0.
        assert_eq!(doc["setup"]["commits"][0]["epoch"], 0);
        assert_eq!(doc["setup"]["groups"].as_array().unwrap().len(), 2);
        assert!(build("", "s", "p").is_err());
    }
}
