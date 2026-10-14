//! Unit guards for the native `MessagingService`: the MLS delivery-service rules
//! against REAL client-built MLS objects (OpenMLS clients play the devices), the
//! request validators and caller binding, the typed stale-epoch refusal, the
//! claim/retention SQL shapes, and the event payload. The live Postgres suite
//! (`live` below) drives the served gRPC surface.

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::proto::udb::core::messaging::services::v1 as msg_pb;
use crate::proto::udb::core::messaging::services::v1::messaging_service_server::MessagingService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

use super::MessagingServiceImpl;
use super::config::parse_ttl_secs;
use super::errors::stale_epoch_status;
use super::events::inbox_appended_payload;
use super::mls;
use super::model::{
    resolve_subject, validate_client_message_id, validate_device_id, validate_group_id,
};
use super::store::{claim_one_time_sql, inbox_retention_sql, models};

fn decode_detail(status: &Status) -> ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(ERROR_DETAIL_METADATA_KEY)
        .expect("error-detail trailer present")
        .to_bytes()
        .expect("trailer decodes to bytes");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
}

/// MLS clients standing in for real devices. Shared with the live suite.
pub(super) mod client {
    use openmls::prelude::tls_codec::Deserialize as _;
    use openmls::prelude::tls_codec::Serialize as _;
    use openmls::prelude::{
        BasicCredential, CredentialWithKey, GroupId, KeyPackage, LeafNodeParameters,
        MIXED_PLAINTEXT_WIRE_FORMAT_POLICY, MlsGroup, MlsGroupCreateConfig, MlsMessageBodyIn,
        MlsMessageIn, OpenMlsProvider, ProcessedMessageContent, StagedWelcome,
    };
    use openmls_basic_credential::SignatureKeyPair;
    use openmls_rust_crypto::OpenMlsRustCrypto;

    use super::super::mls::{CIPHERSUITE, device_identity};

    pub(crate) struct Device {
        pub(crate) provider: OpenMlsRustCrypto,
        pub(crate) signer: SignatureKeyPair,
        pub(crate) credential: CredentialWithKey,
        pub(crate) device_id: String,
    }

    pub(crate) fn create_config() -> MlsGroupCreateConfig {
        MlsGroupCreateConfig::builder()
            .wire_format_policy(MIXED_PLAINTEXT_WIRE_FORMAT_POLICY)
            .ciphersuite(CIPHERSUITE)
            .use_ratchet_tree_extension(true)
            .build()
    }

    impl Device {
        pub(crate) fn new(tenant_id: &str, subject_id: &str, device_id: &str) -> Self {
            Self::with_identity(
                device_id,
                &device_identity(tenant_id, subject_id, device_id),
            )
        }

        pub(crate) fn with_identity(device_id: &str, identity: &str) -> Self {
            let provider = OpenMlsRustCrypto::default();
            let signer =
                SignatureKeyPair::new(CIPHERSUITE.signature_algorithm()).expect("signature key");
            signer.store(provider.storage()).expect("store signer");
            let credential = CredentialWithKey {
                credential: BasicCredential::new(identity.as_bytes().to_vec()).into(),
                signature_key: signer.public().into(),
            };
            Self {
                provider,
                signer,
                credential,
                device_id: device_id.to_string(),
            }
        }

        pub(crate) fn credential_bytes(&self) -> Vec<u8> {
            self.credential
                .credential
                .tls_serialize_detached()
                .expect("serialize credential")
        }

        pub(crate) fn signature_key(&self) -> Vec<u8> {
            self.signer.public().to_vec()
        }

        pub(crate) fn key_package(&self) -> Vec<u8> {
            KeyPackage::builder()
                .build(
                    CIPHERSUITE,
                    &self.provider,
                    &self.signer,
                    self.credential.clone(),
                )
                .expect("key package")
                .key_package()
                .tls_serialize_detached()
                .expect("serialize key package")
        }

        /// Create a group; returns `(group, group_info bytes, ratchet tree bytes)`.
        pub(crate) fn create_group(&self, group_id: &str) -> (MlsGroup, Vec<u8>, Vec<u8>) {
            let group = MlsGroup::new_with_group_id(
                &self.provider,
                &self.signer,
                &create_config(),
                GroupId::from_slice(group_id.as_bytes()),
                self.credential.clone(),
            )
            .expect("create group");
            let info = self.group_info(&group);
            let tree = group
                .export_ratchet_tree()
                .tls_serialize_detached()
                .expect("serialize tree");
            (group, info, tree)
        }

        pub(crate) fn group_info(&self, group: &MlsGroup) -> Vec<u8> {
            group
                .export_group_info(self.provider.crypto(), &self.signer, false)
                .expect("group info")
                .tls_serialize_detached()
                .expect("serialize group info")
        }

        /// Add members from their KeyPackage bytes; returns `(commit, welcome)`.
        /// The commit stays pending until [`Self::merge`].
        pub(crate) fn add(
            &self,
            group: &mut MlsGroup,
            key_packages: &[Vec<u8>],
        ) -> (Vec<u8>, Vec<u8>) {
            use openmls::prelude::{KeyPackageIn, OpenMlsProvider as _, ProtocolVersion};
            let packages: Vec<KeyPackage> = key_packages
                .iter()
                .map(|bytes| {
                    KeyPackageIn::tls_deserialize_exact(bytes.as_slice())
                        .expect("key package decodes")
                        .validate(self.provider.crypto(), ProtocolVersion::Mls10)
                        .expect("key package validates")
                })
                .collect();
            let (commit, welcome, _info) = group
                .add_members(&self.provider, &self.signer, &packages)
                .expect("add members");
            (
                commit.tls_serialize_detached().expect("serialize commit"),
                welcome.tls_serialize_detached().expect("serialize welcome"),
            )
        }

        pub(crate) fn self_update(&self, group: &mut MlsGroup) -> Vec<u8> {
            let bundle = group
                .self_update(&self.provider, &self.signer, LeafNodeParameters::default())
                .expect("self update");
            let (commit, _welcome, _info) = bundle.into_contents();
            commit.tls_serialize_detached().expect("serialize commit")
        }

        pub(crate) fn merge(&self, group: &mut MlsGroup) {
            group
                .merge_pending_commit(&self.provider)
                .expect("merge pending commit");
        }

        pub(crate) fn join(&self, welcome: &[u8]) -> MlsGroup {
            let welcome = match MlsMessageIn::tls_deserialize_exact(welcome)
                .expect("welcome decodes")
                .extract()
            {
                MlsMessageBodyIn::Welcome(welcome) => welcome,
                _ => panic!("not a welcome"),
            };
            StagedWelcome::new_from_welcome(
                &self.provider,
                create_config().join_config(),
                welcome,
                None,
            )
            .expect("stage welcome")
            .into_group(&self.provider)
            .expect("join group")
        }

        pub(crate) fn encrypt(&self, group: &mut MlsGroup, plaintext: &[u8]) -> Vec<u8> {
            group
                .create_message(&self.provider, &self.signer, plaintext)
                .expect("create message")
                .tls_serialize_detached()
                .expect("serialize message")
        }

        /// Process a handshake from the inbox and merge it.
        pub(crate) fn apply_commit(&self, group: &mut MlsGroup, commit: &[u8]) {
            let message = MlsMessageIn::tls_deserialize_exact(commit)
                .expect("commit decodes")
                .try_into_protocol_message()
                .expect("protocol message");
            let processed = group
                .process_message(&self.provider, message)
                .expect("process commit");
            match processed.into_content() {
                ProcessedMessageContent::StagedCommitMessage(staged) => group
                    .merge_staged_commit(&self.provider, *staged)
                    .expect("merge staged commit"),
                _ => panic!("expected a commit"),
            }
        }

        pub(crate) fn decrypt(&self, group: &mut MlsGroup, ciphertext: &[u8]) -> Vec<u8> {
            let message = MlsMessageIn::tls_deserialize_exact(ciphertext)
                .expect("ciphertext decodes")
                .try_into_protocol_message()
                .expect("protocol message");
            match group
                .process_message(&self.provider, message)
                .expect("decrypt message")
                .into_content()
            {
                ProcessedMessageContent::ApplicationMessage(app) => app.into_bytes(),
                _ => panic!("expected an application message"),
            }
        }
    }
}

use client::Device;

const TENANT: &str = "tenant-msg-unit";

// ── MLS delivery-service rules ─────────────────────────────────────────────

#[test]
fn key_package_validation_returns_ref_identity_and_key() {
    let alice = Device::new(TENANT, "alice", "alice-phone");
    let validated = mls::validate_key_package("key_packages", &alice.key_package())
        .expect("a client KeyPackage validates");
    assert_eq!(validated.key_package_ref.len(), 64, "hex SHA-256 reference");
    assert_eq!(
        validated.identity,
        mls::device_identity(TENANT, "alice", "alice-phone").as_bytes()
    );
    assert_eq!(validated.signature_key, alice.signature_key());

    let mut tampered = alice.key_package();
    let last = tampered.len() - 1;
    tampered[last] ^= 0x01;
    assert!(mls::validate_key_package("key_packages", &tampered).is_err());
    assert!(mls::validate_key_package("key_packages", b"not a key package").is_err());
}

#[test]
fn group_creation_commit_and_message_follow_the_public_state() {
    let alice = Device::new(TENANT, "alice", "alice-phone");
    let bob = Device::new(TENANT, "bob", "bob-phone");
    let carol = Device::new(TENANT, "carol", "carol-laptop");
    let (mut group, info, tree) = alice.create_group("g-unit");

    let created = mls::create_public_group("g-unit", &info, &tree).expect("group registers");
    assert_eq!(created.epoch, 0);
    assert_eq!(created.members.len(), 1);
    assert!(mls::create_public_group("other-id", &info, &tree).is_err());

    // Add bob and carol at epoch 0.
    let (commit, welcome) = alice.add(&mut group, &[bob.key_package(), carol.key_package()]);
    alice.merge(&mut group);
    mls::check_welcome(&welcome).expect("welcome is well formed");
    let (gid, epoch) = mls::commit_epoch(&commit).expect("header");
    assert_eq!((gid.as_slice(), epoch), (b"g-unit".as_slice(), 0));
    let accepted = mls::apply_commit(&created.state, "g-unit", &commit).expect("commit accepted");
    assert_eq!(accepted.committed_epoch, 0);
    assert_eq!(accepted.after.epoch, 1);
    assert_eq!(
        accepted.sender_identity,
        mls::device_identity(TENANT, "alice", "alice-phone").as_bytes()
    );
    let ids: Vec<String> = accepted
        .after
        .members
        .iter()
        .map(|leaf| mls::split_identity(&leaf.identity).expect("identity").2)
        .collect();
    assert_eq!(ids, ["alice-phone", "bob-phone", "carol-laptop"]);
    // The stored state round-trips byte-for-byte through the loader.
    assert_eq!(
        mls::state_members(b"g-unit", "g-unit")
            .err()
            .map(|e| e.field),
        Some("public_group_state")
    );
    assert_eq!(
        mls::state_members(&accepted.after.state, "g-unit")
            .expect("reload")
            .len(),
        3
    );

    // Replaying the epoch-0 commit against the epoch-1 state is refused.
    assert!(mls::apply_commit(&accepted.after.state, "g-unit", &commit).is_err());

    // A GroupInfo for the new epoch verifies; the epoch-0 one does not match.
    let info_1 = alice.group_info(&group);
    assert_eq!(
        mls::verify_group_info(&accepted.after.state, "g-unit", &info_1).expect("verifies"),
        1
    );
    assert!(mls::verify_group_info(&accepted.after.state, "g-unit", &info).is_err());

    // Application messages: the header says epoch 1; the content stays sealed.
    let ciphertext = alice.encrypt(&mut group, b"hello");
    assert_eq!(
        mls::application_message_epoch("g-unit", &ciphertext).expect("header"),
        1
    );
    assert!(
        !ciphertext.windows(5).any(|w| w == b"hello"),
        "the ciphertext must not contain the plaintext"
    );
    assert!(mls::application_message_epoch("other", &ciphertext).is_err());
    // A commit is not accepted as an application message, nor the reverse.
    assert!(mls::application_message_epoch("g-unit", &commit).is_err());
    assert!(mls::apply_commit(&accepted.after.state, "g-unit", &ciphertext).is_err());

    // Bob joins from the Welcome and decrypts.
    let mut bob_group = bob.join(&welcome);
    assert_eq!(bob.decrypt(&mut bob_group, &ciphertext), b"hello");
    // Bob's self-update at epoch 1 is accepted by the broker state.
    let update = bob.self_update(&mut bob_group);
    let after = mls::apply_commit(&accepted.after.state, "g-unit", &update).expect("update");
    assert_eq!(after.after.epoch, 2);
    assert_eq!(
        after.sender_identity,
        mls::device_identity(TENANT, "bob", "bob-phone").as_bytes()
    );
}

#[test]
fn a_private_handshake_is_refused() {
    use openmls::prelude::{MlsGroupCreateConfig, PURE_CIPHERTEXT_WIRE_FORMAT_POLICY};
    let alice = Device::new(TENANT, "alice", "alice-phone");
    let bob = Device::new(TENANT, "bob", "bob-phone");
    let mut group = openmls::prelude::MlsGroup::new_with_group_id(
        &alice.provider,
        &alice.signer,
        &MlsGroupCreateConfig::builder()
            .wire_format_policy(PURE_CIPHERTEXT_WIRE_FORMAT_POLICY)
            .ciphersuite(mls::CIPHERSUITE)
            .build(),
        openmls::prelude::GroupId::from_slice(b"g-private"),
        alice.credential.clone(),
    )
    .expect("group");
    let info = alice.group_info(&group);
    let tree = {
        use openmls::prelude::tls_codec::Serialize as _;
        group
            .export_ratchet_tree()
            .tls_serialize_detached()
            .unwrap()
    };
    let created = mls::create_public_group("g-private", &info, &tree).expect("create");
    let (commit, _welcome) = alice.add(&mut group, &[bob.key_package()]);
    let err = mls::apply_commit(&created.state, "g-private", &commit).expect_err("refused");
    assert!(err.reason.contains("PublicMessage"), "{err}");
}

#[test]
fn identities_split_on_first_and_last_slash() {
    assert_eq!(
        mls::split_identity(b"t1/user/with/slash/dev-1"),
        Some(("t1".into(), "user/with/slash".into(), "dev-1".into()))
    );
    assert_eq!(mls::split_identity(b"t1/dev"), None);
    assert_eq!(mls::split_identity(b"/s/d"), None);
    assert_eq!(mls::split_identity(&[0xff, b'/', b'a', b'/', b'b']), None);
}

// ── validation, binding, typed errors ──────────────────────────────────────

#[test]
fn identifier_validators_reject_separators_and_oversize() {
    assert_eq!(
        validate_device_id("device_id", " dev-1.a_b ").unwrap(),
        "dev-1.a_b"
    );
    assert!(validate_device_id("device_id", "a/b").is_err());
    assert!(validate_device_id("device_id", "").is_err());
    assert!(validate_device_id("device_id", &"x".repeat(129)).is_err());
    assert_eq!(validate_group_id("room:42").unwrap(), "room:42");
    assert!(validate_group_id("room 42").is_err());
    assert_eq!(validate_client_message_id("  ").unwrap(), "");
    assert!(validate_client_message_id("bad id").is_err());
}

#[test]
fn an_end_user_acts_only_for_itself() {
    assert_eq!(
        resolve_subject(Some("alice"), "subject_id", "", "op").unwrap(),
        "alice"
    );
    assert_eq!(
        resolve_subject(Some("alice"), "subject_id", "alice", "op").unwrap(),
        "alice"
    );
    let err = resolve_subject(Some("alice"), "subject_id", "bob", "op").unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        resolve_subject(None, "subject_id", "bob", "op").unwrap(),
        "bob"
    );
    assert_eq!(
        resolve_subject(None, "subject_id", "", "op")
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn stale_epoch_carries_the_current_epoch() {
    let status = stale_epoch_status("submit_commit", 3, 5);
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    let detail = decode_detail(&status);
    assert_eq!(detail.kind, ErrorKind::Conflict as i32);
    assert_eq!(detail.reason, "UDB_PRECONDITION_CONFLICT");
    assert_eq!(detail.operation, "submit_commit");
    assert_eq!(
        detail.missing.get("current_epoch").map(String::as_str),
        Some("5")
    );
}

#[test]
fn ttl_accepts_seconds_and_unit_suffixes() {
    assert_eq!(parse_ttl_secs("90"), Some(90));
    assert_eq!(parse_ttl_secs("30d"), Some(30 * 86_400));
    assert_eq!(parse_ttl_secs("12h"), Some(12 * 3_600));
    assert_eq!(parse_ttl_secs("5m"), Some(300));
    assert_eq!(parse_ttl_secs("0"), None);
    assert_eq!(parse_ttl_secs("-1"), None);
    assert_eq!(parse_ttl_secs("3w"), None);
    assert_eq!(parse_ttl_secs("abc"), None);
}

#[test]
fn inbox_event_names_only_tenant_device_and_seq() {
    let payload = inbox_appended_payload("t", "d", 7);
    let mut keys: Vec<&String> = payload.as_object().expect("object").keys().collect();
    keys.sort();
    assert_eq!(keys, ["device_id", "seq", "tenant_id"]);
}

#[test]
fn claim_sql_consumes_exactly_one_unlocked_unclaimed_package() {
    let sql = claim_one_time_sql(&models().key_package);
    assert!(sql.contains("FOR UPDATE SKIP LOCKED"), "{sql}");
    assert!(sql.contains("LIMIT 1"), "{sql}");
    assert!(sql.contains("NOT \"last_resort\""), "{sql}");
    assert_eq!(sql.matches("\"claimed_at\" IS NULL").count(), 2, "{sql}");
    assert!(sql.contains("\"udb_messaging\".\"key_packages\""), "{sql}");
}

#[test]
fn retention_sql_deletes_acked_or_expired_rows_in_bounded_batches() {
    let sql = inbox_retention_sql(&models().inbox);
    assert!(
        sql.contains("\"acked_at\" IS NOT NULL OR \"created_at\" <"),
        "{sql}"
    );
    assert!(sql.contains("LIMIT $2"), "{sql}");
    assert!(sql.contains("FOR UPDATE SKIP LOCKED"), "{sql}");
}

#[tokio::test]
async fn register_device_rejects_cross_tenant_body() {
    let svc = MessagingServiceImpl::new();
    let mut request = Request::new(msg_pb::RegisterMessagingDeviceRequest {
        tenant_id: "tenant-b".to_string(),
        subject_id: "alice".to_string(),
        device_id: "d1".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .register_messaging_device(request)
        .await
        .expect_err("cross-tenant body must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn missing_store_is_a_typed_capability_refusal() {
    let svc = MessagingServiceImpl::new();
    let alice = Device::new(TENANT, "alice", "alice-phone");
    let mut request = Request::new(msg_pb::RegisterMessagingDeviceRequest {
        tenant_id: TENANT.to_string(),
        subject_id: "alice".to_string(),
        device_id: "alice-phone".to_string(),
        signature_public_key: alice.signature_key(),
        credential: alice.credential_bytes(),
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static(TENANT));
    let err = svc
        .register_messaging_device(request)
        .await
        .expect_err("no runtime");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Capability as i32);
    assert_eq!(detail.backend, "messaging");
}

#[tokio::test]
async fn register_device_requires_the_credential_to_name_the_device() {
    let svc = MessagingServiceImpl::new();
    let wrong = Device::with_identity("alice-phone", "other-tenant/alice/alice-phone");
    let mut request = Request::new(msg_pb::RegisterMessagingDeviceRequest {
        tenant_id: TENANT.to_string(),
        subject_id: "alice".to_string(),
        device_id: "alice-phone".to_string(),
        signature_public_key: wrong.signature_key(),
        credential: wrong.credential_bytes(),
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static(TENANT));
    let err = svc
        .register_messaging_device(request)
        .await
        .expect_err("mismatch");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

mod live;
