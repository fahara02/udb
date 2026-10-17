//! MLS Delivery Service validation (OpenMLS `PublicGroup`). Pure: no store
//! access, so every rule here is unit-testable with real client-built MLS
//! objects.
//!
//! The broker holds only PUBLIC group state: the ratchet tree, the group
//! context, the interim transcript hash and the confirmation tag. It parses and
//! validates handshake messages (PublicMessage commits) against that state so it
//! can enforce a linear epoch and an authoritative roster, and it reads only the
//! cleartext header (group id, epoch, content type) of application
//! PrivateMessages. It never holds a group secret and never decrypts.
//!
//! The public state is persisted as the key/value contents of an OpenMLS
//! in-memory storage provider after `PublicGroup::store`/`merge_commit`,
//! serialized deterministically (sorted entries, length-prefixed) with a format
//! tag, so a row round-trips through `PublicGroup::load` byte-for-byte.

use openmls::prelude::tls_codec::{Deserialize as _, Serialize as _};
use openmls::prelude::{
    BasicCredential, Ciphersuite, ContentType, Credential, GroupId, KeyPackageIn, MlsMessageBodyIn,
    MlsMessageIn, OpenMlsProvider, ProcessedMessageContent, ProposalStore, ProtocolMessage,
    ProtocolVersion, PublicGroup, RatchetTreeIn, Sender,
};
use openmls_rust_crypto::OpenMlsRustCrypto;

/// RFC 9420 mandatory-to-implement ciphersuite; the only one this service accepts.
pub(crate) const CIPHERSUITE: Ciphersuite =
    Ciphersuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519;

/// Format tag for a serialized public group state row.
const STATE_MAGIC: &[u8; 8] = b"UDBMLS01";

/// A rule violation, naming the request field it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MlsReject {
    pub(crate) field: &'static str,
    pub(crate) reason: String,
}

impl MlsReject {
    fn new(field: &'static str, reason: impl Into<String>) -> Self {
        Self {
            field,
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for MlsReject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.field, self.reason)
    }
}

/// Lowercase hex of `bytes`.
pub(crate) fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// The MLS identity a device credential must carry: `<tenant>/<subject>/<device>`.
pub(crate) fn device_identity(tenant_id: &str, subject_id: &str, device_id: &str) -> String {
    format!("{tenant_id}/{subject_id}/{device_id}")
}

/// Split an identity into `(tenant, subject, device)`. The tenant ends at the
/// first `/` and the device starts after the last one, so a subject may itself
/// contain `/`. `None` for anything else.
pub(crate) fn split_identity(identity: &[u8]) -> Option<(String, String, String)> {
    let text = std::str::from_utf8(identity).ok()?;
    let (tenant, rest) = text.split_once('/')?;
    let (subject, device) = rest.rsplit_once('/')?;
    if tenant.is_empty() || subject.is_empty() || device.is_empty() {
        return None;
    }
    Some((tenant.to_string(), subject.to_string(), device.to_string()))
}

/// The identity bytes of a TLS-serialized MLS BasicCredential.
pub(crate) fn basic_credential_identity(
    field: &'static str,
    credential: &[u8],
) -> Result<Vec<u8>, MlsReject> {
    let credential = Credential::tls_deserialize_exact(credential)
        .map_err(|err| MlsReject::new(field, format!("not an MLS Credential: {err}")))?;
    basic_identity(field, credential)
}

fn basic_identity(field: &'static str, credential: Credential) -> Result<Vec<u8>, MlsReject> {
    BasicCredential::try_from(credential)
        .map(|basic| basic.identity().to_vec())
        .map_err(|err| MlsReject::new(field, format!("not a BasicCredential: {err}")))
}

/// A one-time or last-resort KeyPackage that passed validation.
#[derive(Debug, Clone)]
pub(crate) struct ValidatedKeyPackage {
    pub(crate) key_package_ref: String,
    pub(crate) identity: Vec<u8>,
    pub(crate) signature_key: Vec<u8>,
}

/// Parse and verify a TLS-serialized KeyPackage: signature, lifetime, protocol
/// version and the service ciphersuite. Returns its hash reference and the
/// identity/signature key of its leaf, for the caller to bind to a device.
pub(crate) fn validate_key_package(
    field: &'static str,
    bytes: &[u8],
) -> Result<ValidatedKeyPackage, MlsReject> {
    let provider = OpenMlsRustCrypto::default();
    let key_package = KeyPackageIn::tls_deserialize_exact(bytes)
        .map_err(|err| MlsReject::new(field, format!("not an MLS KeyPackage: {err}")))?
        .validate(provider.crypto(), ProtocolVersion::Mls10)
        .map_err(|err| MlsReject::new(field, format!("KeyPackage did not verify: {err}")))?;
    if key_package.ciphersuite() != CIPHERSUITE {
        return Err(MlsReject::new(
            field,
            format!(
                "KeyPackage ciphersuite {:?} is not the service ciphersuite {CIPHERSUITE:?}",
                key_package.ciphersuite()
            ),
        ));
    }
    let key_package_ref = key_package
        .hash_ref(provider.crypto())
        .map_err(|err| MlsReject::new(field, format!("KeyPackage reference failed: {err}")))?;
    let leaf = key_package.leaf_node();
    Ok(ValidatedKeyPackage {
        key_package_ref: to_hex(key_package_ref.as_slice()),
        identity: basic_identity(field, leaf.credential().clone())?,
        signature_key: leaf.signature_key().as_slice().to_vec(),
    })
}

/// One leaf of a validated ratchet tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MemberLeaf {
    pub(crate) leaf_index: u32,
    pub(crate) identity: Vec<u8>,
    pub(crate) signature_key: Vec<u8>,
}

/// Public group state after a create or an accepted commit.
#[derive(Debug, Clone)]
pub(crate) struct GroupState {
    pub(crate) epoch: u64,
    pub(crate) state: Vec<u8>,
    pub(crate) members: Vec<MemberLeaf>,
}

fn members_of(group: &PublicGroup) -> Result<Vec<MemberLeaf>, MlsReject> {
    group
        .members()
        .map(|member| {
            Ok(MemberLeaf {
                leaf_index: member.index.u32(),
                identity: basic_identity("ratchet_tree", member.credential.clone())?,
                signature_key: member.signature_key.clone(),
            })
        })
        .collect()
}

fn dump_state(provider: &OpenMlsRustCrypto) -> Vec<u8> {
    let values = provider
        .storage()
        .values
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut entries: Vec<(&Vec<u8>, &Vec<u8>)> = values.iter().collect();
    entries.sort();
    let mut out = Vec::with_capacity(
        STATE_MAGIC.len()
            + 4
            + entries
                .iter()
                .map(|(k, v)| 8 + k.len() + v.len())
                .sum::<usize>(),
    );
    out.extend_from_slice(STATE_MAGIC);
    out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    for (key, value) in entries {
        out.extend_from_slice(&(key.len() as u32).to_be_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(&(value.len() as u32).to_be_bytes());
        out.extend_from_slice(value);
    }
    out
}

fn read_chunk<'a>(bytes: &mut &'a [u8]) -> Option<&'a [u8]> {
    if bytes.len() < 4 {
        return None;
    }
    let (len, rest) = bytes.split_at(4);
    let len = u32::from_be_bytes(len.try_into().ok()?) as usize;
    if rest.len() < len {
        return None;
    }
    let (chunk, rest) = rest.split_at(len);
    *bytes = rest;
    Some(chunk)
}

fn load_state(state: &[u8]) -> Result<OpenMlsRustCrypto, MlsReject> {
    let corrupt = || MlsReject::new("public_group_state", "stored public group state is corrupt");
    let mut rest = state
        .strip_prefix(STATE_MAGIC.as_slice())
        .ok_or_else(corrupt)?;
    if rest.len() < 4 {
        return Err(corrupt());
    }
    let (count, tail) = rest.split_at(4);
    let count = u32::from_be_bytes(count.try_into().map_err(|_| corrupt())?);
    rest = tail;
    let provider = OpenMlsRustCrypto::default();
    {
        let mut values = provider
            .storage()
            .values
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for _ in 0..count {
            let key = read_chunk(&mut rest).ok_or_else(corrupt)?.to_vec();
            let value = read_chunk(&mut rest).ok_or_else(corrupt)?.to_vec();
            values.insert(key, value);
        }
    }
    if !rest.is_empty() {
        return Err(corrupt());
    }
    Ok(provider)
}

fn load_group(state: &[u8], group_id: &str) -> Result<(OpenMlsRustCrypto, PublicGroup), MlsReject> {
    let provider = load_state(state)?;
    let group = PublicGroup::load(
        provider.storage(),
        &GroupId::from_slice(group_id.as_bytes()),
    )
    .map_err(|err| MlsReject::new("public_group_state", format!("load failed: {err:?}")))?
    .ok_or_else(|| {
        MlsReject::new(
            "public_group_state",
            "stored public group state does not hold this group",
        )
    })?;
    Ok((provider, group))
}

fn mls_message(field: &'static str, bytes: &[u8]) -> Result<MlsMessageIn, MlsReject> {
    MlsMessageIn::tls_deserialize_exact(bytes)
        .map_err(|err| MlsReject::new(field, format!("not an MLS message: {err}")))
}

/// Track a new group from the creator's signed GroupInfo and ratchet tree. The
/// GroupInfo signature, the tree hash and every leaf are verified by
/// `PublicGroup::from_external`; the group id must be `group_id`, the epoch 0
/// and the ciphersuite the service's.
pub(crate) fn create_public_group(
    group_id: &str,
    group_info: &[u8],
    ratchet_tree: &[u8],
) -> Result<GroupState, MlsReject> {
    let verifiable = match mls_message("group_info", group_info)?.extract() {
        MlsMessageBodyIn::GroupInfo(info) => info,
        _ => return Err(MlsReject::new("group_info", "message is not a GroupInfo")),
    };
    let tree = RatchetTreeIn::tls_deserialize_exact(ratchet_tree)
        .map_err(|err| MlsReject::new("ratchet_tree", format!("not a ratchet tree: {err}")))?;
    let provider = OpenMlsRustCrypto::default();
    let (group, _info) = PublicGroup::from_external(
        provider.crypto(),
        provider.storage(),
        tree,
        verifiable,
        ProposalStore::new(),
    )
    .map_err(|err| MlsReject::new("group_info", format!("GroupInfo did not verify: {err:?}")))?;
    if group.group_id().as_slice() != group_id.as_bytes() {
        return Err(MlsReject::new(
            "group_id",
            "does not match the GroupInfo's MLS group id",
        ));
    }
    if group.ciphersuite() != CIPHERSUITE {
        return Err(MlsReject::new(
            "group_info",
            format!(
                "group ciphersuite {:?} is not the service ciphersuite",
                group.ciphersuite()
            ),
        ));
    }
    let epoch = group.group_context().epoch().as_u64();
    let members = members_of(&group)?;
    Ok(GroupState {
        epoch,
        state: dump_state(&provider),
        members,
    })
}

/// An accepted commit: the group state after merging it plus the identity of
/// the member (or external joiner) that signed it.
#[derive(Debug, Clone)]
pub(crate) struct AcceptedCommit {
    pub(crate) committed_epoch: u64,
    pub(crate) sender_identity: Vec<u8>,
    /// An external commit (RFC 9420 `new_member_commit`): the signer joined the
    /// group with it instead of being a member already.
    pub(crate) external: bool,
    pub(crate) after: GroupState,
}

/// The epoch a TLS-serialized handshake message was built on, read from its
/// cleartext header. Lets the caller report a stale epoch precisely before full
/// validation.
pub(crate) fn commit_epoch(commit: &[u8]) -> Result<(GroupId, u64), MlsReject> {
    let protocol = mls_message("commit", commit)?
        .try_into_protocol_message()
        .map_err(|err| MlsReject::new("commit", format!("not a protocol message: {err:?}")))?;
    Ok((protocol.group_id().clone(), protocol.epoch().as_u64()))
}

/// Validate a PublicMessage commit against the stored public state and merge it.
/// Rejects private (encrypted) handshakes, standalone proposals, commits for a
/// different group, and anything `PublicGroup::process_message` refuses (wrong
/// epoch, bad signature, invalid proposals or path, confirmation-tag mismatch).
pub(crate) fn apply_commit(
    state: &[u8],
    group_id: &str,
    commit: &[u8],
) -> Result<AcceptedCommit, MlsReject> {
    let (provider, mut group) = load_group(state, group_id)?;
    let protocol: ProtocolMessage = mls_message("commit", commit)?
        .try_into_protocol_message()
        .map_err(|err| MlsReject::new("commit", format!("not a protocol message: {err:?}")))?;
    if !matches!(protocol, ProtocolMessage::PublicMessage(_)) {
        return Err(MlsReject::new(
            "commit",
            "handshake messages must be MLS PublicMessages so the delivery service can validate them",
        ));
    }
    if protocol.group_id().as_slice() != group_id.as_bytes() {
        return Err(MlsReject::new("commit", "commit is for a different group"));
    }
    if protocol.content_type() != ContentType::Commit {
        return Err(MlsReject::new(
            "commit",
            "only commits are accepted; fold proposals into a commit",
        ));
    }
    let committed_epoch = group.group_context().epoch().as_u64();
    let processed = group
        .process_message(provider.crypto(), protocol)
        .map_err(|err| MlsReject::new("commit", format!("commit did not validate: {err:?}")))?;
    let sender_identity = basic_identity("commit", processed.credential().clone())?;
    if !matches!(
        processed.sender(),
        Sender::Member(_) | Sender::NewMemberCommit
    ) {
        return Err(MlsReject::new(
            "commit",
            "commit sender must be a member or an external joiner",
        ));
    }
    let external = matches!(processed.sender(), Sender::NewMemberCommit);
    let staged = match processed.into_content() {
        ProcessedMessageContent::StagedCommitMessage(staged) => staged,
        _ => return Err(MlsReject::new("commit", "message is not a commit")),
    };
    group
        .merge_commit(provider.storage(), *staged)
        .map_err(|err| MlsReject::new("commit", format!("merge failed: {err:?}")))?;
    let epoch = group.group_context().epoch().as_u64();
    let members = members_of(&group)?;
    Ok(AcceptedCommit {
        committed_epoch,
        sender_identity,
        external,
        after: GroupState {
            epoch,
            state: dump_state(&provider),
            members,
        },
    })
}

/// The members recorded in a stored public state.
#[cfg(test)]
pub(crate) fn state_members(state: &[u8], group_id: &str) -> Result<Vec<MemberLeaf>, MlsReject> {
    let (_provider, group) = load_group(state, group_id)?;
    members_of(&group)
}

/// The TLS-serialized public ratchet tree of a stored state (for GetGroupInfo).
pub(crate) fn export_ratchet_tree(state: &[u8], group_id: &str) -> Result<Vec<u8>, MlsReject> {
    let (_provider, group) = load_group(state, group_id)?;
    group
        .export_ratchet_tree()
        .tls_serialize_detached()
        .map_err(|err| MlsReject::new("ratchet_tree", format!("export failed: {err}")))
}

/// Verify a GroupInfo a committer uploads for the NEW epoch: it must be signed by
/// a member of the stored tree and match the group's context exactly. Returns
/// the GroupInfo's epoch.
pub(crate) fn verify_group_info(
    state: &[u8],
    group_id: &str,
    group_info: &[u8],
) -> Result<u64, MlsReject> {
    let (_provider, group) = load_group(state, group_id)?;
    let tree: RatchetTreeIn = group.export_ratchet_tree().into();
    let verifiable = match mls_message("group_info", group_info)?.extract() {
        MlsMessageBodyIn::GroupInfo(info) => info,
        _ => return Err(MlsReject::new("group_info", "message is not a GroupInfo")),
    };
    let scratch = OpenMlsRustCrypto::default();
    let (checked, _info) = PublicGroup::from_external(
        scratch.crypto(),
        scratch.storage(),
        tree,
        verifiable,
        ProposalStore::new(),
    )
    .map_err(|err| MlsReject::new("group_info", format!("GroupInfo did not verify: {err:?}")))?;
    if checked.group_context() != group.group_context() {
        return Err(MlsReject::new(
            "group_info",
            "GroupInfo does not describe the group's current epoch",
        ));
    }
    Ok(checked.group_context().epoch().as_u64())
}

/// Check that a Welcome is a well-formed MLS Welcome. Its secrets are encrypted
/// to the added members; the broker only routes it.
pub(crate) fn check_welcome(welcome: &[u8]) -> Result<(), MlsReject> {
    match mls_message("welcome", welcome)?.extract() {
        MlsMessageBodyIn::Welcome(_) => Ok(()),
        _ => Err(MlsReject::new("welcome", "message is not a Welcome")),
    }
}

/// Read the cleartext header of an application PrivateMessage: it must be a
/// PrivateMessage for `group_id` carrying application data. Returns its epoch.
/// The ciphertext itself is never opened.
pub(crate) fn application_message_epoch(
    group_id: &str,
    ciphertext: &[u8],
) -> Result<u64, MlsReject> {
    let protocol = mls_message("ciphertext", ciphertext)?
        .try_into_protocol_message()
        .map_err(|err| MlsReject::new("ciphertext", format!("not a protocol message: {err:?}")))?;
    if !matches!(protocol, ProtocolMessage::PrivateMessage(_)) {
        return Err(MlsReject::new(
            "ciphertext",
            "application messages must be MLS PrivateMessages",
        ));
    }
    if protocol.group_id().as_slice() != group_id.as_bytes() {
        return Err(MlsReject::new(
            "ciphertext",
            "message is for a different group",
        ));
    }
    if protocol.content_type() != ContentType::Application {
        return Err(MlsReject::new(
            "ciphertext",
            "only application messages are sent this way; submit handshakes as commits",
        ));
    }
    Ok(protocol.epoch().as_u64())
}
