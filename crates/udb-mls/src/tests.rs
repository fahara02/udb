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

    /// An external commit: the joiner becomes a member and, like every member,
    /// receives the COMMIT (its echo); nobody gets a Welcome.
    fn submit_external(&mut self, sender: &str, join: &crate::ExternalJoin) -> Result<u64, u64> {
        let (epoch, members) = self.groups[&join.group_id].clone();
        if join.epoch != epoch {
            return Err(epoch);
        }
        let mut after = members;
        after.insert(sender.to_string());
        for device in &after {
            let item = InboxItem::commit(0, &join.group_id, epoch, sender, join.commit.clone());
            self.append(device, item);
        }
        self.groups
            .insert(join.group_id.clone(), (epoch + 1, after));
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

#[test]
fn key_packages_name_their_device() {
    let mut bob = device("bob", "bob-phone");
    let registration = bob.registration().unwrap();
    let one_time = crate::inspect_key_package(&bob.key_packages(1).unwrap()[0]).unwrap();
    assert_eq!(
        one_time.identity,
        DeviceIdentity::new(TENANT, "bob", "bob-phone").unwrap()
    );
    assert_eq!(
        one_time.signature_public_key,
        registration.signature_public_key
    );
    assert!(!one_time.last_resort);
    let last_resort = crate::inspect_key_package(&bob.last_resort_key_package().unwrap()).unwrap();
    assert!(last_resort.last_resort);
    assert!(crate::inspect_key_package(b"junk").is_err());
}

// ── encrypted backup ───────────────────────────────────────────────────────

const SECRET: &[u8] = b"correct horse battery staple";

/// Cheap Argon2id costs so the debug-build tests stay fast; the recommended
/// costs are exercised once below.
const FAST: crate::BackupParams = crate::BackupParams {
    memory_kib: 256,
    iterations: 1,
    parallelism: 1,
};

fn contents(snapshot: &[u8], history: &[u8]) -> crate::BackupContents {
    crate::BackupContents {
        snapshot: snapshot.to_vec(),
        history: history.to_vec(),
    }
}

#[test]
fn a_restored_device_decrypts_what_was_sent_after_the_backup_and_new_messages() {
    let mut hub = Hub::default();
    let (mut alice, mut bob, mut carol) = three_member_group(&mut hub, "room");
    send(&mut hub, &mut alice, "room", "before the backup");
    assert_eq!(
        texts(&hub.deliver(&mut bob)),
        [("alice-phone".to_string(), "before the backup".to_string())]
    );
    let cursor = bob.inbox_cursor();
    let envelope = bob
        .export_backup_with(SECRET, b"bob's chat history", FAST)
        .expect("seal backup");
    // Bob's phone is lost; the group moves on without him noticing.
    drop(bob);
    send(&mut hub, &mut alice, "room", "while bob was away");

    let (mut restored, history) =
        MlsClient::restore_backup(SECRET, &envelope, MemoryStore::new()).expect("restore");
    assert_eq!(history, b"bob's chat history");
    assert_eq!(
        restored.identity(),
        &DeviceIdentity::new(TENANT, "bob", "bob-phone").unwrap()
    );
    assert_eq!(restored.inbox_cursor(), cursor);
    assert_eq!(restored.epoch("room").unwrap(), 1);
    assert_eq!(
        texts(&hub.deliver(&mut restored)),
        [("alice-phone".to_string(), "while bob was away".to_string())]
    );

    // The restored device follows a key rotation and decrypts new traffic.
    commit_ok(&mut hub, &mut carol, "room", CommitIntent::Update);
    hub.deliver(&mut alice);
    hub.deliver(&mut carol);
    hub.deliver(&mut restored);
    assert_eq!(restored.epoch("room").unwrap(), 2);
    send(&mut hub, &mut alice, "room", "after the rotation");
    assert_eq!(
        texts(&hub.deliver(&mut restored)),
        [("alice-phone".to_string(), "after the rotation".to_string())]
    );
    // And it can still speak: carol decrypts what the restored device sends.
    hub.deliver(&mut carol);
    send(&mut hub, &mut restored, "room", "bob is back");
    assert_eq!(
        texts(&hub.deliver(&mut carol)),
        [("bob-phone".to_string(), "bob is back".to_string())]
    );
}

#[test]
fn the_recommended_costs_round_trip() {
    let bob = device("bob", "bob-phone");
    let envelope = bob.export_backup(SECRET, b"").expect("seal");
    let opened = crate::open_backup(SECRET, &envelope).expect("open");
    assert_eq!(opened.snapshot, bob.snapshot().unwrap());
    assert!(opened.history.is_empty());
    assert_eq!(
        &envelope[8..20],
        [
            &(19u32 * 1024).to_be_bytes()[..],
            &2u32.to_be_bytes()[..],
            &1u32.to_be_bytes()[..]
        ]
        .concat()
        .as_slice()
    );
}

#[test]
fn a_wrong_secret_or_any_modified_byte_does_not_open() {
    let envelope = crate::seal_backup(SECRET, &contents(b"state", b"history"), FAST).unwrap();
    assert!(crate::open_backup(SECRET, &envelope).is_ok());
    assert!(matches!(
        crate::open_backup(b"correct horse battery stapler", &envelope),
        Err(MlsError::Backup(_))
    ));
    // Every region: magic, Argon2id costs, salt, nonce, ciphertext, tag.
    for index in [0, 9, 13, 17, 25, 45, 61, envelope.len() - 1] {
        let mut modified = envelope.clone();
        modified[index] ^= 0x01;
        assert!(
            matches!(
                crate::open_backup(SECRET, &modified),
                Err(MlsError::Backup(_))
            ),
            "a flip at byte {index} must not open"
        );
    }
    assert!(crate::open_backup(SECRET, &envelope[..envelope.len() - 1]).is_err());
    assert!(crate::open_backup(SECRET, &envelope[..crate::MIN_BACKUP_LEN - 1]).is_err());
    assert!(matches!(
        crate::seal_backup(b"short", &contents(b"s", b"h"), FAST),
        Err(MlsError::Backup(_))
    ));
}

#[test]
fn crafted_argon2_costs_are_refused_before_any_work() {
    let mut envelope = crate::seal_backup(SECRET, &contents(b"s", b"h"), FAST).unwrap();
    envelope[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
    let err = crate::open_backup(SECRET, &envelope).unwrap_err();
    assert!(
        matches!(&err, MlsError::Backup(message) if message.contains("exceeds")),
        "{err:?}"
    );
}

#[test]
fn the_envelope_carries_no_plaintext_and_never_repeats() {
    let bob = device("bob", "bob-phone");
    let snapshot = bob.snapshot().unwrap();
    let history = b"meet at the north gate at nine".to_vec();
    let first = bob.export_backup_with(SECRET, &history, FAST).unwrap();
    let second = bob.export_backup_with(SECRET, &history, FAST).unwrap();
    assert!(first.starts_with(crate::BACKUP_MAGIC));
    assert_ne!(first, second, "a fresh salt and nonce per backup");
    // The metadata names the device; the snapshot is otherwise binary.
    let identity = b"\"device_id\":\"bob-phone\"";
    let signature_key = bob.registration().unwrap().signature_public_key;
    for envelope in [&first, &second] {
        for needle in [
            &history[..],
            &identity[..],
            &signature_key[..],
            &snapshot[..32],
            SECRET,
        ] {
            assert!(
                !envelope.windows(needle.len()).any(|w| w == needle),
                "the envelope must not contain plaintext"
            );
        }
    }
    assert!(
        snapshot.windows(identity.len()).any(|w| w == identity),
        "the needle is real: the snapshot names the device"
    );
}

#[test]
fn a_restore_never_replaces_a_device_in_the_store() {
    let bob = device("bob", "bob-phone");
    let envelope = bob.export_backup_with(SECRET, b"", FAST).unwrap();
    let occupied = MemoryStore::new();
    let carol = MlsClient::create(
        DeviceIdentity::new(TENANT, "carol", "carol-laptop").unwrap(),
        occupied,
    )
    .unwrap();
    let before = carol.snapshot().unwrap();
    let occupied = MemoryStore::from_snapshot(before.clone());
    let err = MlsClient::restore_backup(SECRET, &envelope, occupied).unwrap_err();
    assert!(matches!(err, MlsError::AlreadyInitialized(_)), "{err:?}");
    let wrong = MlsClient::restore_backup(b"not the secret", &envelope, MemoryStore::new());
    assert!(matches!(wrong, Err(MlsError::Backup(_))));
}

// ── key pinning ────────────────────────────────────────────────────────────

fn signature_key<S: StateStore>(client: &MlsClient<S>) -> Vec<u8> {
    client.registration().unwrap().signature_public_key
}

/// Alice creates `group_id` and adds `peer` by one of its KeyPackages.
fn add_by_key_package(hub: &mut Hub, alice: &mut Client, peer: &mut Client, group_id: &str) {
    alice.create_group(group_id).unwrap();
    hub.create(group_id, &id(alice));
    commit_ok(
        hub,
        alice,
        group_id,
        CommitIntent::Add {
            key_packages: peer.key_packages(1).unwrap(),
        },
    );
}

#[test]
fn pins_on_first_sight_from_a_key_package() {
    let mut hub = Hub::default();
    let mut alice = device("alice", "alice-phone");
    let mut bob = device("bob", "bob-phone");
    // The own key is pinned (and verified) from creation.
    let own = alice.pinned_key("alice-phone").expect("own pin");
    assert_eq!(own.signature_public_key, signature_key(&alice));
    assert!(own.verified);
    assert!(alice.pinned_key("bob-phone").is_none());

    add_by_key_package(&mut hub, &mut alice, &mut bob, "g-pin");
    let pinned = alice.pinned_devices("bob");
    assert_eq!(pinned.len(), 1);
    let (device_id, pin) = &pinned[0];
    assert_eq!(device_id, "bob-phone");
    assert_eq!(pin.subject_id, "bob");
    assert_eq!(pin.signature_public_key, signature_key(&bob));
    assert_eq!(pin.fingerprint(), key_fingerprint(&signature_key(&bob)));
    assert!(!pin.verified);
    assert!(pin.pending.is_none());
    assert!(pin.first_seen_unix > 0);
    // A first contact with a new subject is silent.
    assert!(alice.take_key_events().unwrap().is_empty());

    // Bob pinned alice from the Welcome's ratchet tree.
    hub.deliver(&mut bob);
    assert_eq!(
        bob.pinned_key("alice-phone").unwrap().signature_public_key,
        signature_key(&alice)
    );
    assert!(bob.take_key_events().unwrap().is_empty());
}

#[test]
fn a_different_key_for_a_pinned_device_is_refused() {
    let mut hub = Hub::default();
    let mut alice = device("alice", "alice-phone");
    let mut bob = device("bob", "bob-phone");
    add_by_key_package(&mut hub, &mut alice, &mut bob, "g-first");

    // Same identity, different key: an impostor a compromised directory serves.
    let mut impostor = device("bob", "bob-phone");
    alice.create_group("g-second").unwrap();
    let err = alice
        .commit(
            "g-second",
            CommitIntent::Add {
                key_packages: impostor.key_packages(1).unwrap(),
            },
        )
        .unwrap_err();
    assert_eq!(
        err,
        MlsError::KeyChanged {
            subject_id: "bob".into(),
            device_id: "bob-phone".into(),
            pinned_fingerprint: key_fingerprint(&signature_key(&bob)),
            observed_fingerprint: key_fingerprint(&signature_key(&impostor)),
        }
    );
    assert!(!alice.has_pending_commit("g-second"));
    assert_eq!(alice.members("g-second").unwrap().len(), 1);
    let pin = alice.pinned_key("bob-phone").unwrap();
    assert_eq!(
        pin.signature_public_key,
        signature_key(&bob),
        "the pin is unchanged"
    );
    assert_eq!(
        pin.pending.as_deref(),
        Some(signature_key(&impostor).as_slice())
    );
    // The refusal was saved: a restart still holds the pending key.
    let reopened = MlsClient::open(MemoryStore::from_snapshot(alice.snapshot().unwrap())).unwrap();
    assert!(reopened.pinned_key("bob-phone").unwrap().pending.is_some());

    // The directory path fails closed the same way; the real key still matches.
    let impostor_key = signature_key(&impostor);
    assert!(matches!(
        alice.observe_device_keys(
            &[("bob", "bob-phone", impostor_key.as_slice())],
            crate::KeySource::Directory
        ),
        Err(MlsError::KeyChanged { .. })
    ));
    let bob_key = signature_key(&bob);
    alice
        .observe_device_keys(
            &[("bob", "bob-phone", bob_key.as_slice())],
            crate::KeySource::Directory,
        )
        .unwrap();
    // A device id showing up under another subject is a key change too.
    assert!(matches!(
        alice.observe_device_keys(
            &[("mallory", "bob-phone", bob_key.as_slice())],
            crate::KeySource::Directory
        ),
        Err(MlsError::KeyChanged { .. })
    ));
    // Fail-closed refusals are errors, not queued events.
    assert!(alice.take_key_events().unwrap().is_empty());
}

#[test]
fn accept_key_change_repins_only_the_compared_key() {
    let mut hub = Hub::default();
    let mut alice = device("alice", "alice-phone");
    let mut bob = device("bob", "bob-phone");
    add_by_key_package(&mut hub, &mut alice, &mut bob, "g-accept");
    let real = key_fingerprint(&signature_key(&bob));

    // Verifying the pin: the right fingerprint (as a user might type it) marks
    // it verified, a wrong one is a key change.
    assert!(matches!(
        alice.verify_device("bob", "bob-phone", &"0".repeat(64)),
        Err(MlsError::KeyChanged { .. })
    ));
    alice
        .verify_device("bob", "bob-phone", &real.to_uppercase())
        .unwrap();
    assert!(alice.pinned_key("bob-phone").unwrap().verified);
    assert!(matches!(
        alice.verify_device("carol", "bob-phone", &real),
        Err(MlsError::Rejected(_))
    ));

    let mut impostor = device("bob", "bob-phone");
    let packages = impostor.key_packages(1).unwrap();
    alice.create_group("g-new").unwrap();
    hub.create("g-new", "alice-phone");
    assert!(matches!(
        alice.commit(
            "g-new",
            CommitIntent::Add {
                key_packages: packages.clone()
            }
        ),
        Err(MlsError::KeyChanged { .. })
    ));
    let observed = key_fingerprint(&signature_key(&impostor));
    // Only the compared, pending key is accepted.
    for wrong in [real.as_str(), "deadbeef"] {
        assert!(matches!(
            alice.accept_key_change("bob", "bob-phone", wrong),
            Err(MlsError::Rejected(_))
        ));
    }
    assert!(matches!(
        alice.accept_key_change("mallory", "bob-phone", &observed),
        Err(MlsError::Rejected(_))
    ));
    assert!(alice.pinned_key("bob-phone").unwrap().pending.is_some());
    assert!(matches!(
        alice.accept_key_change("alice", "alice-phone", &observed),
        Err(MlsError::Rejected(_))
    ));
    alice
        .accept_key_change("bob", "bob-phone", &observed)
        .unwrap();
    let pin = alice.pinned_key("bob-phone").unwrap();
    assert_eq!(pin.signature_public_key, signature_key(&impostor));
    assert!(!pin.verified, "a re-pinned key starts unverified");
    assert!(pin.pending.is_none());
    // Now the accepted key is the pin, and the add goes through.
    commit_ok(
        &mut hub,
        &mut alice,
        "g-new",
        CommitIntent::Add {
            key_packages: packages,
        },
    );
    // The old key is now the stranger.
    assert!(matches!(
        alice.commit(
            "g-accept",
            CommitIntent::Add {
                key_packages: bob.key_packages(1).unwrap()
            }
        ),
        Err(MlsError::KeyChanged { .. })
    ));
}

#[test]
fn a_commit_adding_an_impostor_raises_a_key_event() {
    let mut hub = Hub::default();
    let (mut alice, _bob, mut carol) = three_member_group(&mut hub, "g-swap");
    // Carol removes bob, then (never having seen his key) adds an impostor
    // that carries bob's device id.
    commit_ok(
        &mut hub,
        &mut carol,
        "g-swap",
        CommitIntent::Remove {
            device_ids: vec!["bob-phone".into()],
        },
    );
    assert!(carol.forget_device("bob", "bob-phone").unwrap());
    let mut impostor = device("bob", "bob-phone");
    commit_ok(
        &mut hub,
        &mut carol,
        "g-swap",
        CommitIntent::Add {
            key_packages: impostor.key_packages(1).unwrap(),
        },
    );
    // The group accepted it: alice applies both commits...
    let events = hub.deliver(&mut alice);
    assert!(
        matches!(
            &events[..],
            [InboxEvent::Commit { epoch: 2, .. }, InboxEvent::Commit { epoch: 3, added, .. }]
                if added == &["bob-phone".to_string()]
        ),
        "{events:?}"
    );
    // ...and is told the key changed.
    let key_events = alice.take_key_events().unwrap();
    assert_eq!(key_events.len(), 1, "{key_events:?}");
    match &key_events[0] {
        crate::KeyEvent::KeyChanged {
            subject_id,
            device_id,
            observed_fingerprint,
            source,
            group_id,
            seq,
            ..
        } => {
            assert_eq!(
                (subject_id.as_str(), device_id.as_str()),
                ("bob", "bob-phone")
            );
            assert_eq!(
                observed_fingerprint,
                &key_fingerprint(&signature_key(&impostor))
            );
            assert_eq!(*source, crate::KeySource::Commit);
            assert_eq!(group_id.as_deref(), Some("g-swap"));
            assert!(seq.is_some());
        }
        other => panic!("{other:?}"),
    }
    assert!(alice.take_key_events().unwrap().is_empty(), "taken once");
    // A later commit that keeps the impostor does not repeat the event.
    commit_ok(&mut hub, &mut carol, "g-swap", CommitIntent::Update);
    hub.deliver(&mut alice);
    assert!(alice.key_events().is_empty());
    // The impostor joined; the pin still holds the real key.
    // (The shared device id's inbox also holds the removal, which it skips.)
    assert!(hub
        .deliver(&mut impostor)
        .iter()
        .any(|event| matches!(event, InboxEvent::Joined { .. })));
    assert!(alice.pinned_key("bob-phone").unwrap().pending.is_some());
}

#[test]
fn a_new_device_of_a_known_subject_is_reported() {
    let mut hub = Hub::default();
    let mut alice = device("alice", "alice-phone");
    let mut bob = device("bob", "bob-phone");
    add_by_key_package(&mut hub, &mut alice, &mut bob, "g-devices");
    let tablet = device("bob", "bob-tablet");
    let laptop = device("alice", "alice-laptop");
    let dave_1 = device("dave", "dave-1");
    let dave_2 = device("dave", "dave-2");
    let keys = [
        signature_key(&bob),
        signature_key(&tablet),
        signature_key(&laptop),
        signature_key(&dave_1),
        signature_key(&dave_2),
    ];
    alice
        .observe_device_keys(
            &[
                ("bob", "bob-phone", keys[0].as_slice()),
                ("bob", "bob-tablet", keys[1].as_slice()),
                ("alice", "alice-laptop", keys[2].as_slice()),
                // Two devices of a subject never seen: one silent first contact.
                ("dave", "dave-1", keys[3].as_slice()),
                ("dave", "dave-2", keys[4].as_slice()),
            ],
            crate::KeySource::Directory,
        )
        .unwrap();
    let events = alice.take_key_events().unwrap();
    let new_devices: Vec<(&str, &str)> = events
        .iter()
        .map(|event| match event {
            crate::KeyEvent::NewDevice {
                subject_id,
                device_id,
                fingerprint,
                source,
                ..
            } => {
                assert_eq!(*source, crate::KeySource::Directory);
                assert_eq!(fingerprint.len(), 64);
                (subject_id.as_str(), device_id.as_str())
            }
            other => panic!("{other:?}"),
        })
        .collect();
    // A new device of a known subject, and any new device of the own subject
    // (a device added to this user's account is always worth a look).
    assert_eq!(
        new_devices,
        [("bob", "bob-tablet"), ("alice", "alice-laptop")]
    );
    assert_eq!(alice.pinned_devices("dave").len(), 2);
    // Seen again: nothing new.
    alice
        .observe_device_keys(
            &[("bob", "bob-tablet", keys[1].as_slice())],
            crate::KeySource::Directory,
        )
        .unwrap();
    assert!(alice.take_key_events().unwrap().is_empty());
    // Forgetting a pin lets the device be pinned afresh.
    assert!(alice.forget_device("bob", "bob-tablet").unwrap());
    assert!(!alice.forget_device("bob", "bob-tablet").unwrap());
    assert!(alice.forget_device("alice", "alice-phone").is_err());
}

#[test]
fn safety_numbers_match_on_both_sides_and_ignore_order() {
    let mut alice = device("alice", "alice-phone");
    let mut bob = device("bob", "bob-phone");
    let tablet = device("bob", "bob-tablet");
    let (a, b, t) = (
        signature_key(&alice),
        signature_key(&bob),
        signature_key(&tablet),
    );
    assert!(matches!(
        alice.safety_number("bob"),
        Err(MlsError::Rejected(_))
    ));
    alice
        .observe_device_keys(
            &[
                ("bob", "bob-tablet", t.as_slice()),
                ("bob", "bob-phone", b.as_slice()),
            ],
            crate::KeySource::Directory,
        )
        .unwrap();
    bob.observe_device_keys(
        &[
            ("alice", "alice-phone", a.as_slice()),
            ("bob", "bob-tablet", t.as_slice()),
        ],
        crate::KeySource::Directory,
    )
    .unwrap();
    let number = alice.safety_number("bob").unwrap();
    assert_eq!(number, bob.safety_number("alice").unwrap());
    let groups: Vec<&str> = number.split(' ').collect();
    assert_eq!(groups.len(), 12, "{number}");
    assert!(groups
        .iter()
        .all(|g| g.len() == 5 && g.bytes().all(|c| c.is_ascii_digit())));

    // A different key for one device gives a different number.
    let mut swapped =
        MlsClient::open(MemoryStore::from_snapshot(alice.snapshot().unwrap())).unwrap();
    assert_eq!(swapped.safety_number("bob").unwrap(), number);
    let other = signature_key(&device("bob", "bob-tablet"));
    assert!(matches!(
        swapped.observe_device_keys(
            &[("bob", "bob-tablet", other.as_slice())],
            crate::KeySource::Directory,
        ),
        Err(MlsError::KeyChanged { .. })
    ));
    assert_eq!(
        swapped.safety_number("bob").unwrap(),
        number,
        "pending keys do not count"
    );
    swapped
        .accept_key_change("bob", "bob-tablet", &key_fingerprint(&other))
        .unwrap();
    assert_ne!(swapped.safety_number("bob").unwrap(), number);
    assert!(matches!(
        alice.safety_number("nobody"),
        Err(MlsError::Rejected(_))
    ));
}

#[test]
fn pins_survive_a_file_store_reopen() {
    let dir = std::env::temp_dir().join(format!(
        "udb-mls-pins-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("alice.mls");
    let bob = device("bob", "bob-phone");
    let tablet = device("bob", "bob-tablet");
    let (b, t) = (signature_key(&bob), signature_key(&tablet));
    {
        let mut alice = MlsClient::create(
            DeviceIdentity::new(TENANT, "alice", "alice-phone").unwrap(),
            crate::FileStore::new(&path),
        )
        .unwrap();
        alice
            .observe_device_keys(
                &[("bob", "bob-phone", b.as_slice())],
                crate::KeySource::Directory,
            )
            .unwrap();
        alice
            .verify_device("bob", "bob-phone", &key_fingerprint(&b))
            .unwrap();
        alice
            .observe_device_keys(
                &[("bob", "bob-tablet", t.as_slice())],
                crate::KeySource::Directory,
            )
            .unwrap();
    }
    let mut alice = MlsClient::open(crate::FileStore::new(&path)).unwrap();
    let pin = alice.pinned_key("bob-phone").unwrap();
    assert_eq!(pin.signature_public_key, b);
    assert!(pin.verified);
    // The undelivered event survived too.
    assert!(matches!(
        &alice.take_key_events().unwrap()[..],
        [crate::KeyEvent::NewDevice { device_id, .. }] if device_id == "bob-tablet"
    ));
    let number = alice.safety_number("bob").unwrap();

    // An encrypted backup carries the pins to a restored device.
    let envelope = alice.export_backup_with(SECRET, b"", FAST).unwrap();
    let (restored, _) = MlsClient::restore_backup(SECRET, &envelope, MemoryStore::new()).unwrap();
    assert_eq!(restored.pinned_devices("bob"), alice.pinned_devices("bob"));
    assert_eq!(restored.safety_number("bob").unwrap(), number);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_0_6_0_snapshot_opens_with_no_pins() {
    let mut hub = Hub::default();
    let mut alice = device("alice", "alice-phone");
    let mut bob = device("bob", "bob-phone");
    add_by_key_package(&mut hub, &mut alice, &mut bob, "g-old");
    // Rewrite the metadata the way 0.6.0 wrote it: no pins, no key events.
    let (meta, values) = crate::store::decode_snapshot(&alice.snapshot().unwrap()).unwrap();
    let mut meta: serde_json::Value = serde_json::from_slice(&meta).unwrap();
    let object = meta.as_object_mut().unwrap();
    assert!(object.remove("pins").is_some());
    assert!(object.remove("key_events").is_some());
    let old = crate::store::encode_snapshot(&serde_json::to_vec(&meta).unwrap(), &values);

    let mut opened = MlsClient::open(MemoryStore::from_snapshot(old)).unwrap();
    assert_eq!(opened.group_ids(), ["g-old"]);
    // The own key is pinned on open; peers are pinned on next sight.
    let own = opened
        .pinned_key("alice-phone")
        .expect("own pin backfilled");
    assert_eq!(own.signature_public_key, signature_key(&alice));
    assert!(opened.pinned_key("bob-phone").is_none());
    let bob_key = signature_key(&bob);
    opened
        .observe_device_keys(
            &[("bob", "bob-phone", bob_key.as_slice())],
            crate::KeySource::Directory,
        )
        .unwrap();
    assert!(opened.take_key_events().unwrap().is_empty());
    assert!(opened.safety_number("bob").is_ok());
    // Installing a host clock stamps only pins recorded without one.
    let seen = opened.pinned_key("bob-phone").unwrap().first_seen_unix;
    assert!(seen > 0);
    let stamped = MlsClient::open(MemoryStore::from_snapshot(opened.snapshot().unwrap()))
        .unwrap()
        .with_clock(|| 1_700_000_000);
    assert_eq!(
        stamped.pinned_key("bob-phone").unwrap().first_seen_unix,
        seen
    );
}

// ── external joins ─────────────────────────────────────────────────────────

#[test]
fn a_device_joins_by_external_commit_from_the_group_info() {
    let mut hub = Hub::default();
    let (mut alice, mut bob, mut carol) = three_member_group(&mut hub, "g-ext");
    // Every commit carries the GroupInfo of the epoch it creates.
    let update = alice
        .commit("g-ext", CommitIntent::Update)
        .unwrap()
        .unwrap();
    let info = update
        .group_info
        .clone()
        .expect("a commit carries the new epoch's GroupInfo");
    assert_eq!(hub.submit("alice-phone", &update), Ok(2));
    alice.confirm_commit("g-ext").unwrap();
    for client in [&mut alice, &mut bob, &mut carol] {
        hub.deliver(client);
    }

    // A pinned key that differs from a member's leaf refuses the join.
    let mut wary = device("dave", "dave-1");
    let impostor = signature_key(&device("bob", "bob-phone"));
    wary.observe_device_keys(
        &[("bob", "bob-phone", impostor.as_slice())],
        crate::KeySource::Directory,
    )
    .unwrap();
    assert!(matches!(
        wary.join_external("g-ext", &info, &[]),
        Err(MlsError::KeyChanged { .. })
    ));
    assert!(wary.group_ids().is_empty());

    // Alice's second device joins from the GroupInfo (it carries the tree).
    let mut laptop = device("alice", "alice-laptop");
    let join = laptop.join_external("g-ext", &info, &[]).unwrap();
    assert_eq!(join.epoch, 2, "built on the GroupInfo's epoch");
    assert!(join.group_info.is_some());
    assert!(laptop.has_pending_commit("g-ext"));
    assert!(matches!(
        laptop.encrypt("g-ext", b"too early"),
        Err(MlsError::PendingCommit(_))
    ));
    assert!(matches!(
        laptop.join_external("g-ext", &info, &[]),
        Err(MlsError::GroupExists(_))
    ));
    assert_eq!(hub.submit_external("alice-laptop", &join), Ok(3));
    let summary = laptop.confirm_commit("g-ext").unwrap();
    assert_eq!(summary.epoch, 3);
    assert_eq!(
        summary.members,
        ["alice-laptop", "alice-phone", "bob-phone", "carol-laptop"]
    );
    assert!(!laptop.has_pending_commit("g-ext"));

    // The members apply the external commit as an add by the joiner.
    for client in [&mut bob, &mut carol] {
        let events = hub.deliver(client);
        assert!(
            matches!(&events[..], [InboxEvent::Commit { epoch: 3, added, sender_device_id, .. }]
                if added == &["alice-laptop".to_string()] && sender_device_id == "alice-laptop"),
            "{events:?}"
        );
    }
    hub.deliver(&mut alice);
    // A new device of alice's own account is worth a look.
    assert!(alice
        .take_key_events()
        .unwrap()
        .iter()
        .any(|event| matches!(
            event,
            crate::KeyEvent::NewDevice { device_id, source: crate::KeySource::Commit, .. }
                if device_id == "alice-laptop"
        )));
    // The joiner gets its own commit back, already applied.
    assert!(matches!(
        &hub.deliver(&mut laptop)[..],
        [InboxEvent::OwnCommit {
            merged: false,
            epoch: 3,
            ..
        }]
    ));
    send(&mut hub, &mut laptop, "g-ext", "joined from the laptop");
    for client in [&mut alice, &mut bob, &mut carol] {
        assert_eq!(
            texts(&hub.deliver(client)),
            [(
                "alice-laptop".to_string(),
                "joined from the laptop".to_string()
            )]
        );
    }
}

#[test]
fn an_external_join_is_dropped_on_refusal_and_confirmed_by_its_echo() {
    let mut hub = Hub::default();
    let (mut alice, mut bob, _carol) = three_member_group(&mut hub, "g-ext-2");
    let first = alice
        .commit("g-ext-2", CommitIntent::Update)
        .unwrap()
        .unwrap();
    let stale_info = first.group_info.clone().unwrap();
    hub.submit("alice-phone", &first).unwrap();
    alice.confirm_commit("g-ext-2").unwrap();
    hub.deliver(&mut alice);
    hub.deliver(&mut bob);

    // Another commit moves the group on: a join from the older GroupInfo is
    // refused stale, and discarding it deletes the joined group.
    let second = bob
        .commit("g-ext-2", CommitIntent::Update)
        .unwrap()
        .unwrap();
    let info = second.group_info.clone().unwrap();
    hub.submit("bob-phone", &second).unwrap();
    bob.confirm_commit("g-ext-2").unwrap();
    let mut tablet = device("bob", "bob-tablet");
    let stale = tablet.join_external("g-ext-2", &stale_info, &[]).unwrap();
    assert_eq!(hub.submit_external("bob-tablet", &stale), Err(3));
    assert_eq!(tablet.discard_commit("g-ext-2").unwrap(), None);
    assert!(tablet.group_ids().is_empty());
    assert!(!tablet.has_pending_commit("g-ext-2"));

    // Rejoin from the current GroupInfo; the SubmitCommit response is lost,
    // and the inbox echo confirms the join after a restart.
    let join = tablet.join_external("g-ext-2", &info, &[]).unwrap();
    assert_eq!(hub.submit_external("bob-tablet", &join), Ok(4));
    let mut restarted =
        MlsClient::open(MemoryStore::from_snapshot(tablet.snapshot().unwrap())).unwrap();
    assert!(restarted.has_pending_commit("g-ext-2"));
    assert!(matches!(
        &hub.deliver(&mut restarted)[..],
        [InboxEvent::OwnCommit {
            merged: true,
            epoch: 4,
            ..
        }]
    ));
    assert!(!restarted.has_pending_commit("g-ext-2"));
    hub.deliver(&mut bob);
    send(&mut hub, &mut restarted, "g-ext-2", "tablet is in");
    assert_eq!(
        texts(&hub.deliver(&mut bob)),
        [("bob-tablet".to_string(), "tablet is in".to_string())]
    );
}
