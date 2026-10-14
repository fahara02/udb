//! In-process clients exercising the full client lifecycle against `Hub`, a
//! minimal stand-in for the broker's delivery rules: a linear epoch per group
//! (a commit built on a stale epoch is refused with the current one), COMMIT to
//! every member before and after a commit except the added devices, WELCOME to
//! the added devices, MESSAGE to every member including the sender. The real
//! broker (PublicGroup validation, served gRPC) is exercised by the Rust SDK's
//! live interop test.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    key_fingerprint, CommitIntent, DeviceIdentity, InboxEvent, InboxItem, InboxKind, MemoryStore,
    MlsClient, MlsError, OutgoingMessage, PendingCommit, StateStore,
};

const TENANT: &str = "tenant-mls";

type Client = MlsClient<MemoryStore>;

fn device(subject: &str, device: &str) -> Client {
    MlsClient::create(
        DeviceIdentity::new(TENANT, subject, device).expect("identity"),
        MemoryStore::new(),
    )
    .expect("create device")
}

#[derive(Default)]
struct Hub {
    inboxes: BTreeMap<String, Vec<InboxItem>>,
    next_seq: BTreeMap<String, i64>,
    groups: BTreeMap<String, (u64, BTreeSet<String>)>,
}

impl Hub {
    fn append(&mut self, device: &str, mut item: InboxItem) {
        let seq = self.next_seq.entry(device.to_string()).or_insert(0);
        *seq += 1;
        item.seq = *seq;
        self.inboxes
            .entry(device.to_string())
            .or_default()
            .push(item);
    }

    fn create(&mut self, group_id: &str, creator: &str) {
        self.groups.insert(
            group_id.to_string(),
            (0, BTreeSet::from([creator.to_string()])),
        );
    }

    fn epoch(&self, group_id: &str) -> u64 {
        self.groups[group_id].0
    }

    /// `Err(current_epoch)` for a stale commit, like FAILED_PRECONDITION
    /// `missing.current_epoch` from the broker.
    fn submit(&mut self, sender: &str, pending: &PendingCommit) -> Result<u64, u64> {
        let (epoch, members) = self.groups[&pending.group_id].clone();
        if pending.epoch != epoch {
            return Err(epoch);
        }
        let mut after = members.clone();
        for device in &pending.removed_device_ids {
            after.remove(device);
        }
        for device in &pending.added_device_ids {
            after.insert(device.clone());
        }
        for device in members.union(&after) {
            let item = if pending.added_device_ids.contains(device) {
                InboxItem::welcome(
                    0,
                    &pending.group_id,
                    epoch + 1,
                    sender,
                    pending.welcome.clone().expect("adds carry a Welcome"),
                )
            } else {
                InboxItem::commit(0, &pending.group_id, epoch, sender, pending.commit.clone())
            };
            self.append(device, item);
        }
        self.groups
            .insert(pending.group_id.clone(), (epoch + 1, after));
        Ok(epoch + 1)
    }

    fn send(&mut self, sender: &str, message: &OutgoingMessage) {
        let (epoch, members) = self.groups[&message.group_id].clone();
        assert_eq!(
            message.epoch, epoch,
            "the hub only accepts current-epoch messages"
        );
        for device in members {
            let item = InboxItem::message(
                0,
                &message.group_id,
                message.epoch,
                sender,
                message.ciphertext.clone(),
            );
            self.append(&device, item);
        }
    }

    fn take(&mut self, device: &str) -> Vec<InboxItem> {
        std::mem::take(self.inboxes.entry(device.to_string()).or_default())
    }

    /// Deliver a device's pending inbox entries in order.
    fn deliver<S: StateStore>(&mut self, client: &mut MlsClient<S>) -> Vec<InboxEvent> {
        let device = client.identity().device_id.clone();
        self.take(&device)
            .iter()
            .map(|item| client.process(item).expect("process inbox entry"))
            .collect()
    }
}

fn id(client: &Client) -> String {
    client.identity().device_id.clone()
}

/// Build, submit and confirm a commit that must be accepted.
fn commit_ok(hub: &mut Hub, client: &mut Client, group_id: &str, intent: CommitIntent) {
    let pending = client
        .commit(group_id, intent)
        .expect("build commit")
        .expect("commit has work");
    hub.submit(&id(client), &pending).expect("commit accepted");
    client.confirm_commit(group_id).expect("confirm commit");
}

fn send(hub: &mut Hub, client: &mut Client, group_id: &str, text: &str) -> OutgoingMessage {
    let sealed = client.encrypt(group_id, text.as_bytes()).expect("encrypt");
    assert!(
        !sealed
            .ciphertext
            .windows(text.len())
            .any(|w| w == text.as_bytes()),
        "the ciphertext must not contain the plaintext"
    );
    hub.send(&id(client), &sealed);
    sealed
}

fn texts(events: &[InboxEvent]) -> Vec<(String, String)> {
    events
        .iter()
        .filter_map(|event| match event {
            InboxEvent::Message {
                sender_device_id,
                plaintext,
                ..
            } => Some((
                sender_device_id.clone(),
                String::from_utf8(plaintext.clone()).expect("utf-8"),
            )),
            _ => None,
        })
        .collect()
}

/// A three-member group: alice creates and adds bob and carol.
fn three_member_group(hub: &mut Hub, group_id: &str) -> (Client, Client, Client) {
    let mut alice = device("alice", "alice-phone");
    let mut bob = device("bob", "bob-phone");
    let mut carol = device("carol", "carol-laptop");
    let created = alice.create_group(group_id).expect("create group");
    assert_eq!(created.epoch, 0);
    hub.create(group_id, &id(&alice));
    let packages = [
        bob.key_packages(1).expect("bob kp").remove(0),
        carol.key_packages(1).expect("carol kp").remove(0),
    ];
    commit_ok(
        hub,
        &mut alice,
        group_id,
        CommitIntent::Add {
            key_packages: packages.to_vec(),
        },
    );
    for client in [&mut bob, &mut carol] {
        let events = hub.deliver(client);
        assert!(
            matches!(&events[..], [InboxEvent::Joined { epoch: 1, members, added_by, .. }]
                if members.len() == 3 && added_by == "alice-phone"),
            "{events:?}"
        );
    }
    // Alice's own commit echo is recognised, not re-applied.
    let echo = hub.deliver(&mut alice);
    assert!(
        matches!(
            &echo[..],
            [InboxEvent::OwnCommit {
                merged: false,
                epoch: 1,
                ..
            }]
        ),
        "{echo:?}"
    );
    (alice, bob, carol)
}

// ── identity ───────────────────────────────────────────────────────────────

#[test]
fn identities_follow_the_broker_rules() {
    assert!(DeviceIdentity::new("t1", "alice", "phone-1.a_b").is_ok());
    assert!(DeviceIdentity::new("t1", "alice", "a/b").is_err());
    assert!(DeviceIdentity::new("t/1", "alice", "d").is_err());
    assert!(DeviceIdentity::new("t1", " ", "d").is_err());
    assert!(DeviceIdentity::new("t1", "alice", "x".repeat(129)).is_err());
    assert_eq!(
        crate::split_identity(b"t1/user/with/slash/dev-1"),
        Some(("t1".into(), "user/with/slash".into(), "dev-1".into()))
    );
    assert_eq!(crate::split_identity(b"t1/dev"), None);

    let mut alice = device("alice", "alice-phone");
    let registration = alice.registration().expect("registration");
    assert_eq!(registration.signature_public_key.len(), 32);
    assert_eq!(
        registration.key_fingerprint,
        key_fingerprint(&registration.signature_public_key)
    );
    assert_eq!(registration.key_fingerprint.len(), 64);
    assert!(alice.create_group("bad group").is_err());
    assert!(alice.create_group("room:1").is_ok());
    assert_eq!(
        alice.create_group("room:1").unwrap_err(),
        MlsError::GroupExists("room:1".into())
    );
}

// ── create / add / welcome / send / decrypt ───────────────────────────────

#[test]
fn members_join_from_the_welcome_and_exchange_messages() {
    let mut hub = Hub::default();
    let (mut alice, mut bob, mut carol) = three_member_group(&mut hub, "g-chat");
    for client in [&alice, &bob, &carol] {
        assert_eq!(client.epoch("g-chat").unwrap(), 1);
        let members: Vec<String> = client
            .members("g-chat")
            .unwrap()
            .into_iter()
            .map(|m| m.device_id)
            .collect();
        assert_eq!(members, ["alice-phone", "bob-phone", "carol-laptop"]);
    }

    send(&mut hub, &mut alice, "g-chat", "hello from alice");
    send(&mut hub, &mut bob, "g-chat", "hi alice");

    let alice_events = hub.deliver(&mut alice);
    assert_eq!(
        texts(&alice_events),
        [("bob-phone".into(), "hi alice".into())]
    );
    // Her own message echo is skipped.
    assert!(alice_events
        .iter()
        .any(|e| matches!(e, InboxEvent::Skipped { reason, .. } if reason == "own message")));
    assert_eq!(
        texts(&hub.deliver(&mut bob)),
        [("alice-phone".into(), "hello from alice".into())]
    );
    let carol_events = hub.deliver(&mut carol);
    assert_eq!(
        texts(&carol_events),
        [
            ("alice-phone".into(), "hello from alice".into()),
            ("bob-phone".into(), "hi alice".into())
        ]
    );
    match &carol_events[0] {
        InboxEvent::Message {
            sender_subject_id,
            epoch,
            ..
        } => {
            assert_eq!(sender_subject_id, "alice");
            assert_eq!(*epoch, 1);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn replayed_entries_are_skipped_by_the_cursor() {
    let mut hub = Hub::default();
    let (mut alice, mut bob, _carol) = three_member_group(&mut hub, "g-replay");
    send(&mut hub, &mut alice, "g-replay", "once");
    let items = hub.take("bob-phone");
    let first = bob.process(&items[0]).unwrap();
    assert!(matches!(first, InboxEvent::Message { .. }));
    assert_eq!(bob.inbox_cursor(), items[0].seq);
    let again = bob.process(&items[0]).unwrap();
    assert!(
        matches!(again, InboxEvent::Skipped { ref reason, .. } if reason == "already processed")
    );
}

// ── remove ─────────────────────────────────────────────────────────────────

#[test]
fn a_removed_member_loses_the_group_and_cannot_read_new_messages() {
    let mut hub = Hub::default();
    let (mut alice, mut bob, mut carol) = three_member_group(&mut hub, "g-remove");
    commit_ok(
        &mut hub,
        &mut alice,
        "g-remove",
        CommitIntent::Remove {
            device_ids: vec!["carol-laptop".into()],
        },
    );
    let bob_events = hub.deliver(&mut bob);
    assert!(
        matches!(&bob_events[..], [InboxEvent::Commit { epoch: 2, removed, added, sender_device_id, .. }]
            if removed == &["carol-laptop".to_string()] && added.is_empty() && sender_device_id == "alice-phone"),
        "{bob_events:?}"
    );
    let carol_events = hub.deliver(&mut carol);
    assert!(
        matches!(&carol_events[..], [InboxEvent::Removed { removed_by, .. }] if removed_by == "alice-phone"),
        "{carol_events:?}"
    );
    assert!(carol.group_ids().is_empty());
    assert_eq!(
        carol.epoch("g-remove").unwrap_err(),
        MlsError::UnknownGroup("g-remove".into())
    );

    // The broker no longer fans out to carol; even if her device saw the
    // ciphertext it could not read it.
    let sealed = send(&mut hub, &mut alice, "g-remove", "after carol left");
    let leaked = carol
        .process(&InboxItem::message(
            100,
            "g-remove",
            sealed.epoch,
            "alice-phone",
            sealed.ciphertext,
        ))
        .unwrap();
    assert!(
        matches!(leaked, InboxEvent::Undecryptable { .. }),
        "{leaked:?}"
    );
    assert_eq!(
        texts(&hub.deliver(&mut bob)),
        [("alice-phone".into(), "after carol left".into())]
    );

    // Removing a non-member is a no-op; removing yourself is refused.
    assert!(alice
        .commit(
            "g-remove",
            CommitIntent::Remove {
                device_ids: vec!["carol-laptop".into()]
            }
        )
        .unwrap()
        .is_none());
    assert!(matches!(
        alice.commit(
            "g-remove",
            CommitIntent::Remove {
                device_ids: vec!["alice-phone".into()]
            }
        ),
        Err(MlsError::Rejected(_))
    ));
}

// ── post-compromise security ───────────────────────────────────────────────

#[test]
fn a_key_update_heals_a_compromised_device_state() {
    let mut hub = Hub::default();
    let (mut alice, mut bob, mut carol) = three_member_group(&mut hub, "g-pcs");
    // An attacker copies bob's complete state.
    let stolen = bob.snapshot().unwrap();
    let mut attacker = MlsClient::open(MemoryStore::from_snapshot(stolen)).unwrap();

    // The copy reads current-epoch traffic: the compromise is real.
    let before = send(&mut hub, &mut alice, "g-pcs", "before the update");
    let read = attacker
        .process(&InboxItem::message(
            900,
            "g-pcs",
            before.epoch,
            "alice-phone",
            before.ciphertext.clone(),
        ))
        .unwrap();
    assert!(matches!(read, InboxEvent::Message { .. }), "{read:?}");

    // Bob rotates his leaf keys; everyone applies the commit.
    let update = bob.commit("g-pcs", CommitIntent::Update).unwrap().unwrap();
    hub.submit("bob-phone", &update).unwrap();
    bob.confirm_commit("g-pcs").unwrap();
    assert_eq!(bob.epoch("g-pcs").unwrap(), 2);
    let alice_events = hub.deliver(&mut alice);
    assert!(alice_events
        .iter()
        .any(|e| matches!(e, InboxEvent::Commit { epoch: 2, sender_device_id, .. } if sender_device_id == "bob-phone")));
    hub.deliver(&mut carol);
    hub.deliver(&mut bob);

    // A message after the update: members read it, the stale copy does not,
    // even when it is fed the update commit itself.
    let after = send(&mut hub, &mut alice, "g-pcs", "after the update");
    assert_eq!(
        texts(&hub.deliver(&mut bob)),
        [("alice-phone".into(), "after the update".into())]
    );
    assert_eq!(
        texts(&hub.deliver(&mut carol)),
        [("alice-phone".into(), "after the update".into())]
    );
    let fed = attacker
        .process(&InboxItem::commit(
            901,
            "g-pcs",
            1,
            "bob-phone",
            update.commit,
        ))
        .unwrap();
    assert!(
        matches!(
            fed,
            InboxEvent::OwnCommit {
                merged: false,
                epoch: 1,
                ..
            }
        ),
        "the copy has no pending commit to merge: {fed:?}"
    );
    let healed = attacker
        .process(&InboxItem::message(
            902,
            "g-pcs",
            after.epoch,
            "alice-phone",
            after.ciphertext,
        ))
        .unwrap();
    assert!(
        matches!(healed, InboxEvent::Undecryptable { .. }),
        "the stale copy must not decrypt post-update traffic: {healed:?}"
    );
}

// ── concurrent commits: the loser re-bases ─────────────────────────────────

#[test]
fn a_stale_commit_is_rebuilt_on_the_winning_epoch() {
    let mut hub = Hub::default();
    let mut alice = device("alice", "alice-phone");
    let mut bob = device("bob", "bob-phone");
    let mut dave = device("dave", "dave-tablet");
    alice.create_group("g-race").unwrap();
    hub.create("g-race", "alice-phone");
    commit_ok(
        &mut hub,
        &mut alice,
        "g-race",
        CommitIntent::Add {
            key_packages: bob.key_packages(1).unwrap(),
        },
    );
    hub.deliver(&mut bob);
    hub.deliver(&mut alice);

    // Both build on epoch 1. Bob's update lands first.
    let dave_package = dave.key_packages(1).unwrap();
    let alice_pending = alice
        .commit(
            "g-race",
            CommitIntent::Add {
                key_packages: dave_package.clone(),
            },
        )
        .unwrap()
        .unwrap();
    assert!(alice.has_pending_commit("g-race"));
    assert!(matches!(
        alice.encrypt("g-race", b"blocked"),
        Err(MlsError::PendingCommit(_))
    ));
    let bob_pending = bob.commit("g-race", CommitIntent::Update).unwrap().unwrap();
    assert_eq!(hub.submit("bob-phone", &bob_pending), Ok(2));
    bob.confirm_commit("g-race").unwrap();

    // Alice's commit is refused with the current epoch.
    let current = hub.submit("alice-phone", &alice_pending).unwrap_err();
    assert_eq!(current, 2);
    // Re-base: drop the commit, catch up through the inbox, rebuild, resubmit.
    let intent = alice
        .discard_commit("g-race")
        .unwrap()
        .expect("intent kept");
    assert_eq!(
        intent,
        CommitIntent::Add {
            key_packages: dave_package
        }
    );
    while alice.epoch("g-race").unwrap() < current {
        let events = hub.deliver(&mut alice);
        assert!(
            !events.is_empty(),
            "the inbox must carry the winning commit"
        );
    }
    let rebuilt = alice.commit("g-race", intent).unwrap().unwrap();
    assert_eq!(rebuilt.epoch, 2);
    assert_eq!(hub.submit("alice-phone", &rebuilt), Ok(3));
    alice.confirm_commit("g-race").unwrap();

    hub.deliver(&mut bob);
    let joined = hub.deliver(&mut dave);
    assert!(
        matches!(&joined[..], [InboxEvent::Joined { epoch: 3, .. }]),
        "{joined:?}"
    );
    send(&mut hub, &mut dave, "g-race", "dave is in");
    assert_eq!(
        texts(&hub.deliver(&mut alice)),
        [("dave-tablet".into(), "dave is in".into())]
    );
    assert_eq!(
        texts(&hub.deliver(&mut bob)),
        [("dave-tablet".into(), "dave is in".into())]
    );
}

#[test]
fn a_commit_that_loses_while_the_inbox_runs_keeps_its_intent() {
    let mut hub = Hub::default();
    let (mut alice, mut bob, mut carol) = three_member_group(&mut hub, "g-lost");
    // Alice builds a removal of carol; bob's update wins and reaches alice's
    // inbox before she learns her submit failed.
    let alice_pending = alice
        .commit(
            "g-lost",
            CommitIntent::Remove {
                device_ids: vec!["carol-laptop".into()],
            },
        )
        .unwrap()
        .unwrap();
    let bob_pending = bob.commit("g-lost", CommitIntent::Update).unwrap().unwrap();
    hub.submit("bob-phone", &bob_pending).unwrap();
    bob.confirm_commit("g-lost").unwrap();
    let events = hub.deliver(&mut alice);
    assert!(
        matches!(&events[..], [InboxEvent::Commit { epoch: 2, .. }]),
        "{events:?}"
    );
    assert!(!alice.has_pending_commit("g-lost"));
    assert_eq!(hub.submit("alice-phone", &alice_pending), Err(2));
    let intent = alice
        .discard_commit("g-lost")
        .unwrap()
        .expect("lost intent");
    commit_ok(&mut hub, &mut alice, "g-lost", intent);
    assert!(matches!(
        &hub.deliver(&mut carol)[..],
        [InboxEvent::Commit { .. }, InboxEvent::Removed { .. }]
    ));
}

#[test]
fn a_lost_submit_response_is_recovered_from_the_echo() {
    let mut hub = Hub::default();
    let (mut alice, mut bob, _carol) = three_member_group(&mut hub, "g-echo");
    let pending = alice
        .commit("g-echo", CommitIntent::Update)
        .unwrap()
        .unwrap();
    hub.submit("alice-phone", &pending).unwrap();
    // The response never arrived; the echo in her inbox merges the commit.
    let events = hub.deliver(&mut alice);
    assert!(
        matches!(
            &events[..],
            [InboxEvent::OwnCommit {
                merged: true,
                epoch: 2,
                ..
            }]
        ),
        "{events:?}"
    );
    assert!(!alice.has_pending_commit("g-echo"));
    hub.deliver(&mut bob);
    send(&mut hub, &mut alice, "g-echo", "still in sync");
    assert_eq!(
        texts(&hub.deliver(&mut bob)),
        [("alice-phone".into(), "still in sync".into())]
    );
}

// ── key packages ───────────────────────────────────────────────────────────

#[test]
fn the_last_resort_package_admits_a_device_more_than_once() {
    let mut hub = Hub::default();
    let mut alice = device("alice", "alice-phone");
    let mut bob = device("bob", "bob-phone");
    let last_resort = bob.last_resort_key_package().unwrap();
    for group_id in ["g-lr-1", "g-lr-2"] {
        alice.create_group(group_id).unwrap();
        hub.create(group_id, "alice-phone");
        commit_ok(
            &mut hub,
            &mut alice,
            group_id,
            CommitIntent::Add {
                key_packages: vec![last_resort.clone()],
            },
        );
        let joined = hub.deliver(&mut bob);
        assert!(
            matches!(&joined[..], [InboxEvent::Joined { group_id: g, .. }] if g == group_id),
            "{joined:?}"
        );
    }
    // A one-time package is consumed by its Welcome.
    let mut carol = device("carol", "carol-laptop");
    let once = carol.key_packages(1).unwrap();
    for group_id in ["g-once-1", "g-once-2"] {
        alice.create_group(group_id).unwrap();
        hub.create(group_id, "alice-phone");
        commit_ok(
            &mut hub,
            &mut alice,
            group_id,
            CommitIntent::Add {
                key_packages: once.clone(),
            },
        );
    }
    let events = hub.deliver(&mut carol);
    assert!(matches!(events[0], InboxEvent::Joined { .. }), "{events:?}");
    assert!(
        matches!(events[1], InboxEvent::Undecryptable { .. }),
        "{events:?}"
    );
}

#[test]
fn foreign_or_invalid_key_packages_are_rejected() {
    let mut alice = device("alice", "alice-phone");
    alice.create_group("g-kp").unwrap();
    let mut foreign = MlsClient::create(
        DeviceIdentity::new("other-tenant", "mallory", "m-1").unwrap(),
        MemoryStore::new(),
    )
    .unwrap();
    let err = alice
        .commit(
            "g-kp",
            CommitIntent::Add {
                key_packages: foreign.key_packages(1).unwrap(),
            },
        )
        .unwrap_err();
    assert!(
        matches!(err, MlsError::Rejected(ref m) if m.contains("another tenant")),
        "{err}"
    );
    let mut tampered = device("bob", "bob-phone")
        .key_packages(1)
        .unwrap()
        .remove(0);
    let last = tampered.len() - 1;
    tampered[last] ^= 0x01;
    assert!(matches!(
        alice.commit(
            "g-kp",
            CommitIntent::Add {
                key_packages: vec![tampered]
            }
        ),
        Err(MlsError::Rejected(_))
    ));
    // Adding a device that is already a member does nothing.
    let mut bob = device("bob", "bob-2");
    let mut hub = Hub::default();
    hub.create("g-kp", "alice-phone");
    commit_ok(
        &mut hub,
        &mut alice,
        "g-kp",
        CommitIntent::Add {
            key_packages: bob.key_packages(1).unwrap(),
        },
    );
    assert!(alice
        .commit(
            "g-kp",
            CommitIntent::Add {
                key_packages: bob.key_packages(1).unwrap()
            }
        )
        .unwrap()
        .is_none());
    assert_eq!(hub.epoch("g-kp"), 1);
}

// ── persistence ────────────────────────────────────────────────────────────

#[test]
fn state_survives_a_restart_through_the_file_store() {
    let dir = std::env::temp_dir().join(format!(
        "udb-mls-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("bob.mls");
    let mut hub = Hub::default();
    let mut alice = device("alice", "alice-phone");
    let bob_identity = DeviceIdentity::new(TENANT, "bob", "bob-phone").unwrap();
    let packages = {
        let mut bob =
            MlsClient::create(bob_identity.clone(), crate::FileStore::new(&path)).unwrap();
        bob.key_packages(2).unwrap()
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "the state file is owner-only");
    }
    alice.create_group("g-file").unwrap();
    hub.create("g-file", "alice-phone");
    commit_ok(
        &mut hub,
        &mut alice,
        "g-file",
        CommitIntent::Add {
            key_packages: packages[..1].to_vec(),
        },
    );
    {
        // A restarted process: the KeyPackage private key came back from disk.
        let mut bob = MlsClient::open(crate::FileStore::new(&path)).unwrap();
        assert!(matches!(
            &hub.deliver(&mut bob)[..],
            [InboxEvent::Joined { .. }]
        ));
    }
    send(&mut hub, &mut alice, "g-file", "persisted");
    let mut bob = MlsClient::open_or_create(bob_identity, crate::FileStore::new(&path)).unwrap();
    assert_eq!(bob.group_ids(), ["g-file"]);
    assert_eq!(
        texts(&hub.deliver(&mut bob)),
        [("alice-phone".into(), "persisted".into())]
    );
    // The same store cannot be claimed by another device or created twice.
    let other = DeviceIdentity::new(TENANT, "bob", "bob-laptop").unwrap();
    assert!(matches!(
        MlsClient::open_or_create(other.clone(), crate::FileStore::new(&path)),
        Err(MlsError::AlreadyInitialized(_))
    ));
    assert!(matches!(
        MlsClient::create(other, crate::FileStore::new(&path)),
        Err(MlsError::AlreadyInitialized(_))
    ));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_corrupt_or_empty_store_is_reported() {
    assert_eq!(
        MlsClient::open(MemoryStore::new()).unwrap_err(),
        MlsError::NotInitialized
    );
    assert!(matches!(
        MlsClient::open(MemoryStore::from_snapshot(b"garbage".to_vec())),
        Err(MlsError::Corrupt(_))
    ));
    let alice = device("alice", "alice-phone");
    let mut snapshot = alice.snapshot().unwrap();
    snapshot.truncate(snapshot.len() - 3);
    assert!(matches!(
        MlsClient::open(MemoryStore::from_snapshot(snapshot)),
        Err(MlsError::Corrupt(_))
    ));
    // The snapshot never contains a group's plaintext: encrypt, then scan.
    let mut hub = Hub::default();
    let (mut alice, _bob, _carol) = three_member_group(&mut hub, "g-scan");
    send(&mut hub, &mut alice, "g-scan", "secret-plaintext-marker");
    let state = alice.store().load().unwrap().unwrap();
    assert!(!state
        .windows(b"secret-plaintext-marker".len())
        .any(|w| w == b"secret-plaintext-marker"));
}

#[test]
fn removal_hints_and_unknown_kinds_are_surfaced() {
    let mut bob = device("bob", "bob-phone");
    let hint = InboxItem {
        seq: 1,
        kind: InboxKind::RemovalHint,
        group_id: "g-x".into(),
        epoch: 0,
        payload: br#"{"type":"device_revoked","group_id":"g-x","device_id":"carol-laptop","leaf_index":2,"reason":"lost"}"#.to_vec(),
        sender_device_id: "carol-laptop".into(),
        client_message_id: String::new(),
    };
    assert_eq!(
        bob.process(&hint).unwrap(),
        InboxEvent::RemovalHint {
            seq: 1,
            group_id: "g-x".into(),
            device_id: "carol-laptop".into(),
            reason: "lost".into()
        }
    );
    let unknown = InboxItem {
        seq: 2,
        kind: InboxKind::parse("FUTURE"),
        ..hint
    };
    assert!(matches!(
        bob.process(&unknown).unwrap(),
        InboxEvent::Skipped { .. }
    ));
    assert_eq!(bob.inbox_cursor(), 2);
}
