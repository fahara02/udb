# End-to-end encrypted messaging service (UDB 0.6.0 design)

Status: design, 2026-10-14. 0.6.0's headline is a new native service for end-to-end
encrypted user-to-user messaging that any project on UDB can use. 0.6.0 also carries
the deprecation removals listed in the changelog's "Breaking for callers" section.

## 1. Decisions

| # | Decision | Why |
|---|---|---|
| D1 | **MLS (RFC 9420) for 1:1 and groups, via OpenMLS (MIT).** One protocol for both; a 1:1 chat is a two-member group. Ciphersuite `MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519` (mandatory-to-implement). | Standard, audited, forward secrecy + post-compromise security, efficient group rekeying; no bespoke crypto. |
| D2 | **The broker is the MLS Delivery Service and the key directory. It never holds group secrets and never sees plaintext.** | E2EE means the operator (and a compromised broker) cannot read messages. |
| D3 | **Handshake messages (proposals/commits) are MLS `PublicMessage`; application messages are `PrivateMessage`.** The broker keeps an OpenMLS `PublicGroup` per group and validates every commit against it. | The broker can enforce a linear epoch, an authoritative roster, and fan-out without any secret. Handshake content (who joined/left) is visible to the broker; it already is through fan-out. |
| D4 | **History is device-only by default. Encrypted backup is opt-in:** the client encrypts its history export with a key derived from a user recovery secret (Argon2id → HKDF), and the broker stores the opaque blob through the storage service. | Device-only is the strongest default; the opt-in backup restores history on a new device without giving the operator a key. |
| D5 | **Clients do the cryptography; the SDKs carry it.** One Rust core crate (`udb-mls`, OpenMLS + a storage provider) is used by the Rust SDK and compiled to WASM for TypeScript. Go/Python/PHP bindings come in 0.6.x through uniffi over the same crate. | One implementation of the crypto; no re-implementation per language. |
| D6 | **Identity = an MLS `BasicCredential` naming the UDB principal and device** (`<tenant>/<subject>/<device_id>`), signed with a per-device Ed25519 key the broker publishes in its device directory. Clients pin keys and show safety numbers; a key change is an event. | The broker vouches for "this key belongs to this account's device"; users can verify it out of band. |
| D7 | **Metadata privacy is a non-goal for 0.6.0.** The broker sees group membership, sizes and timing. Sealed sender / private group membership are later work. | Keeps 0.6.0 shippable; documented, not hidden. |

## 2. Service surface

Proto package `udb.core.messaging.services.v1`
(`proto/udb/core/messaging/services/v1/messaging_service.proto`), entities in
`proto/udb/core/messaging/entity/v1/`. Native listener, endpoint security by scope
(`messaging:send`, `messaging:read`, `messaging:admin`).

| RPC | Kind | Purpose |
|---|---|---|
| `RegisterDevice` | mutation | Device id + Ed25519 signature public key + credential; one per device |
| `ListDevices` | read | A principal's devices with key fingerprints (for verification) |
| `RevokeDevice` | destructive | Removes the device; its groups get a removal hint |
| `PublishKeyPackages` | mutation | Upload one-time KeyPackages (+ one last-resort) for a device |
| `ClaimKeyPackages` | mutation | Atomically claim one KeyPackage per target device (consumed, never reused) |
| `GetKeyPackageStock` | read | How many one-time packages remain (clients top up below a threshold) |
| `CreateGroup` | mutation | Register a group: id, creator's initial public `GroupInfo`/ratchet tree |
| `SubmitCommit` | mutation | `PublicMessage` commit + `Welcome` per added device; accepted only if its epoch is current and `PublicGroup` validation passes (concurrent commits linearize: the loser re-bases) |
| `SendMessage` | mutation | `PrivateMessage` ciphertext for the group's current epoch; fanned out to every member device's inbox |
| `StreamInbox` | read, server-stream | A device's ordered inbox (commits, welcomes, messages) from its cursor |
| `AckInbox` | mutation | Advance the device cursor; acknowledged entries become collectable |
| `GetGroupInfo` | read | Public `GroupInfo` for an external join, when the group allows it |
| `PutBackup` / `GetBackup` / `DeleteBackup` | mutation / read / destructive | Opaque client-encrypted history backup (D4), stored via the storage service |

Every RPC carries `operation_kind` and, where replay matters, an
`idempotency_contract` (Send/Commit keyed by a client message id).

## 3. Data model (proto-driven DDL, tenant RLS)

`messaging_device`, `messaging_key_package` (one row per package, `claimed_at`),
`messaging_group` (id, epoch, `public_group` bytes — the serialized OpenMLS
`PublicGroup`, created_by), `messaging_group_member` (group, device, leaf index —
derived from the validated tree, never client-asserted), `messaging_inbox` (device,
seq, kind, payload bytes, group, epoch, created_at, acked_at), `messaging_backup`
(principal, object ref, created_at). All carry `tenant_id`; owner fields name the
subject so `PurgeSubject` covers messaging: devices, key packages, inbox rows and
backups of the subject are erased, and groups they were in get a removal hint.

## 4. Broker implementation

- Module `src/runtime/service/messaging_service/`, laid out like
  `notification_service/` (`mod.rs`, `handlers.rs`, `store.rs`, `model.rs`,
  `events.rs`, `errors.rs`).
- Registered on `NativeControlPlaneServices` (`src/runtime/service/mod.rs`) beside
  `notification`, wrapped by method security like every native service.
- Columns through `native_model(...)` (`src/runtime/native_catalog.rs`), never
  hand-written column names.
- Writes co-commit with their outbox event: a new inbox row emits
  `udb.messaging.inbox.appended` carrying only `{tenant, device, seq}`, so live
  subscribers wake without content.
- `SubmitCommit` runs in one transaction: lock the group row (`FOR UPDATE`), check
  the epoch, validate with `PublicGroup::process_message`, persist the new public
  group, rewrite the member table, append inbox rows for every member device (and
  Welcomes for added ones).
- Retention: inbox rows are deleted once every target device acknowledged them, or
  after `UDB_MESSAGING_INBOX_TTL` (default 30 days), by the scheduler.
- Broker dependency: `openmls` + `openmls_rust_crypto`, for DS validation only.

## 5. Threat model summary

- Broker compromise: cannot read past or future messages; can deny service, drop or
  delay messages (clients detect gaps through MLS generation counters), and could try
  to add a malicious device to an account — mitigated by key pinning, safety numbers
  and device-added events to the account's other devices (D6).
- Stolen device: removed via `RevokeDevice` plus a commit; post-compromise security
  heals on the next commit.
- Replay: MLS generation/epoch checks on clients; idempotency keys on the broker.

## 6. Phases and acceptance

| Phase | Scope | Acceptance (CI) |
|---|---|---|
| M1 | Protos, entities, DDL, device directory, key packages | served tests: a claim consumes exactly once under concurrency; RLS isolation between tenants |
| M2 | Groups: CreateGroup/SubmitCommit with `PublicGroup` validation, epoch linearization | two concurrent commits → exactly one accepted, the other gets `FAILED_PRECONDITION{current_epoch}` |
| M3 | SendMessage, StreamInbox/AckInbox, retention, outbox events | 3-device group round trip; the broker database holds no plaintext (scan test); TTL purge |
| M4 | `udb-mls` crate + Rust SDK client + TS (WASM) client | Rust↔TS interop group chat in the live suite |
| M5 | Backup (D4), PurgeSubject coverage, docs, benchmark rows | restore-on-new-device test; the PurgeSubject receipt lists messaging tables |

0.6.0 ships when M1–M5 are green alongside the deprecation removals.
