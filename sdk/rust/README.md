# UDB Rust SDK (`udb-client`)

Typed tonic clients for the [UDB](https://github.com/fahara02/udb) broker, plus
the tenant/project metadata and token lifecycle the broker expects.

> **`udb-client` is the client. `udb` is the broker.** The `udb` crate on
> crates.io is the server: its default features pull in every backend driver —
> sqlx, mongodb, cassandra, kafka, elasticsearch, S3. Depending on it to make a
> gRPC call compiles a database engine you will not use.

## Install

```toml
[dependencies]
udb-client = "0.5"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

No system `protoc` is required — the vendored compiler is used, matching the
broker's own build.

## Use

```rust,no_run
use udb_client::proto::udb::entity::v1::SelectRequest;
use udb_client::{Metadata, UdbClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let meta = Metadata::new("tenant-1")
        .with_project("default")
        .with_bearer_token(std::env::var("UDB_TOKEN")?);

    let mut udb = UdbClient::connect("http://127.0.0.1:50051", meta).await?;

    // `select_records` decodes the canonical representation. See "Reading a
    // result" below before reaching for the raw `select`.
    let records = udb
        .select_records(SelectRequest {
            message_type: "myapp.v1.Invoice".into(),
            limit: 10,
            ..Default::default()
        })
        .await?;

    for record in records.iter() {
        println!("{} {}", record.str("id")?, record.i64("amount_minor")?);
    }
    Ok(())
}
```

## Reading a result

A `RecordSet` carries the page in two parallel fields, and only one is the data:

| field | what it is |
| --- | --- |
| `records_json` | **The records.** One JSON object each. What every UDB SDK decodes. |
| `rows` | Compatibility only. Sent as one **empty** entry per record. |

Nothing in the generated struct marks the difference, so `set.rows[i].fields`
compiles, type-checks, and returns the correct record COUNT with every field map
empty — a populated table reads back as a page of empty entities and no error is
raised anywhere.

Use [`UdbClient::select_records`], or `Records::decode` on a `RecordSet` you
already have. Both read `records_json`, reject a structurally inconsistent page
instead of returning a plausible one, and keep integers exact — a `BIGINT` past
2^53 survives, which it cannot when a decoder routes numbers through a double.

```rust,no_run
# use udb_client::{Records, UdbClient};
# async fn f(udb: &mut UdbClient, req: udb_client::proto::udb::entity::v1::SelectRequest)
# -> Result<(), Box<dyn std::error::Error>> {
let set = udb.select(req).await?;          // raw RecordSet
let records = Records::decode(&set)?;      // canonical records
for record in records.iter() {
    let id = record.str("id")?;
    let created = record.timestamp("created_at")?;   // RFC 3339 text
    let blob = record.bytes("body")?;                // BYTEA, base64-decoded
    let closed = record.optional("closed_at", udb_client::Record::timestamp)?;
    println!("{id} {created} {} bytes, closed={closed:?}", blob.len());
}
# Ok(())
# }
```

A complete program, including login, is in
[`examples/quickstart.rs`](examples/quickstart.rs):

```sh
UDB_TENANT_ID=tenant-1 UDB_TOKEN=... cargo run --example quickstart -- myapp.v1.Invoice
```

## Versions: current tonic/prost, not the broker's

This crate tracks **tonic 0.14 / prost 0.14** — deliberately NOT the broker's older
pins.

0.5.21 pinned tonic 0.12 / prost 0.13 to match the broker, reasoning that a
consumer linking both would get one set of generated types. That optimises for a
consumer who links a gRPC *server* as a library beside its client, which is rare,
and penalises the common one: `UpsertRequest.payload` and `.expected` are
`Option<prost_types::Struct>`, so a workspace on current prost could not build a
payload or express a compare-and-swap at all — its `Struct` was a different
nominal type with the same name.

`sdk/rust-consumer-check/` is a crate that declares its own `prost-types` and
`tonic` and passes them across this SDK's public API. It runs in CI. If these pins
drift from the ecosystem again, it stops compiling — which is the check whose
absence let 0.5.21 ship unusable to exactly the consumers it was written for.

## Two listeners, two authorization models

UDB serves its data plane and its native services on **separate listeners with
different authorization models** — the data plane authorizes through Casbin, the
native services through scope-based endpoint security. A credential accepted by
one is not automatically accepted by the other, and the mismatch presents as a
permissions error rather than a wrong-address error. `UdbClient` speaks to the
data plane (`:50051` by default).

## Identity is per-connection, audit is per-request

`Metadata` splits deliberately:

- **Identity** — tenant, user, project, scopes, service identity — is fixed for
  the connection. It is not settable per call, because a tenant that a caller can
  vary per request is not an isolation boundary.
- **Audit** — purpose, correlation id, catalog version — varies per call via
  `with_audit`.

Every method routes through `UdbClient::request`, the single point that applies
metadata. If applying tenant scope were the caller's job, forgetting once would
be a cross-tenant read.

Reaching an RPC this wrapper does not expose yet:

```rust,no_run
# use udb_client::{Metadata, UdbClient};
# async fn f(udb: &mut UdbClient) -> Result<(), Box<dyn std::error::Error>> {
let request = udb.request(/* any request message */ ())?; // metadata applied
// udb.raw().some_rpc(request).await?;
# Ok(())
# }
```

## Tokens rotate — let `TokenManager` hold them

The broker's refresh token is **single-use**: each successful refresh mints a new
one and invalidates the presented one atomically. A client that keeps sending its
original refresh token authenticates, refreshes once, then fails with
`Unauthenticated: invalid credential` at the second refresh boundary — an hour or
a day later, far from the code that caused it.

```rust,no_run
# use udb_client::{Metadata, TokenManager, UdbClient};
# async fn f(channel: tonic::transport::Channel) -> Result<(), Box<dyn std::error::Error>> {
let tokens = TokenManager::new(channel, Metadata::new("tenant-1"));
tokens.login("alice", "hunter2").await?;

// Refreshes if stale, persists the rotated refresh token, single-flighted so
// concurrent callers share one round-trip instead of racing to spend the same
// single-use credential.
let meta = tokens.authenticated_metadata().await?;
let mut udb = UdbClient::connect("http://127.0.0.1:50051", meta).await?;
# Ok(())
# }
```

`TokenManager`'s `Debug` redacts the stored tokens.

## Typed errors, and retries that will not double-charge you

Failures decode the broker's `udb-error-detail-bin` trailer into `UdbError`, so
you read what the broker actually said instead of matching on message strings:

```rust,no_run
# use udb_client::{UdbClient, UdbError};
# async fn f(udb: &mut UdbClient, req: udb_client::proto::udb::entity::v1::SelectRequest) {
match udb.select(req).await {
    // `records_json` is the canonical field, so its length is the record count.
    Ok(set) => println!("{} record(s)", set.records_json.len()),
    Err(err) => {
        if let Some(cap) = err.capability_required() {
            eprintln!("this deployment is missing {cap}");
        }
        for v in err.field_violations() {
            eprintln!("rejected field: {}", v.field);
        }
        eprintln!("correlation id for a bug report: {:?}", err.correlation_id());
    }
}
# }
```

Retry policy comes from the **contract**, not from this client's opinion. The
generated registry (`generated_rpcs`) carries each RPC's declared
`operation_kind` and `idempotency_contract`, and the default policy reads them:

| RPC | retried | because the contract says |
|---|---|---|
| `select`, `vector_search` | yes | `read_only` — applies no mutation |
| `upsert`, `update`, `delete` | yes | `replay_safe` — the broker declares them replayable |
| `bulk_cas`, `vector_upsert` | no | neither read-only nor declared replayable |

That table is not hand-maintained; it is what the descriptor says. An earlier
draft of this client hard-coded "mutations are never retried", which the contract
contradicts for three of them — refusing a retry the broker was happy to serve
costs availability for no safety gain. Deciding from the descriptor rather than
the method name is the whole point of the `operation_kind` annotation.

One subtlety worth knowing if you read the registry directly: `replay_safe` comes
from an OPTIONAL `idempotency_contract` and is `false` when simply undeclared, so
most read-only RPCs report `false`. Use `RpcSpec::retry_safe()` (or
`is_retry_safe`), which is `read_only || replay_safe`.

`err.is_retryable()` trusts the broker's own `retryable` flag over any guess made
from the gRPC code, and backoff honours `retry_after_ms` when the broker sends
one. Override the contract's choice per client when you have reason to:

```rust,no_run
# use udb_client::{CallPolicy, UdbClient};
# fn f(udb: &UdbClient) {
let retrying = udb.with_policy(CallPolicy::idempotent());
# }
```

## Generated types

Stubs are generated **at build time** from the protos rather than committed, so
this crate cannot drift from the contract it ships with. They are laid out by
proto package — `udb.entity.v1` is `udb_client::proto::udb::entity::v1`. The
module tree is emitted by `build.rs` from the packages tonic actually produced,
so a new service in the contract needs no hand-edited list here.

Every service in the contract has a typed client: `build.rs` compiles each
`*/services/v1/*.proto` (the data plane and every native service, including
auth, storage, workflow and WebRTC), so `AuthnServiceClient`, `StorageServiceClient`
and the rest sit beside `DataBrokerClient` under `udb_client::proto`.

The whole contract is also available as an encoded descriptor set,
`udb_client::FILE_DESCRIPTOR_SET`, for tools that build requests at runtime
(with `prost-reflect`, for example) instead of through a stub.

`build.rs` resolves the protos from `../../proto` in a repo checkout, falling
back to a vendored `./proto`.

## End-to-end encrypted messaging (feature `mls`)

The `mls` feature adds `udb_client::messaging::MessagingClient`, a client for
the `MessagingService` built on [`udb-mls`](../../crates/udb-mls) (OpenMLS,
RFC 9420). The broker is the delivery service and key directory; every key and
every plaintext stays on the device. The feature is off by default so the plain
client carries no OpenMLS dependencies.

```toml
udb-client = { version = "0.6", features = ["mls"] }
```

```rust,no_run
use udb_client::messaging::{mls, MessagingClient};
use udb_client::Metadata;

# async fn example(meta: Metadata) -> Result<(), Box<dyn std::error::Error>> {
// The device's keys and group state live in this file (owner-only, replaced
// atomically). Implement `mls::StateStore` to keep them somewhere else.
let device = mls::MlsClient::open_or_create(
    mls::DeviceIdentity::new(&meta.tenant_id, "alice", "alice-phone")?,
    mls::FileStore::new("alice-phone.mls"),
)?;
// MessagingService is a native service: connect to the native listener.
let mut chat = MessagingClient::connect("http://127.0.0.1:50061", meta, device).await?;
chat.register_device().await?;      // signature key + credential into the directory
chat.ensure_key_packages().await?;  // top up one-time KeyPackages (+ a last-resort one)

chat.create_group("room-1").await?;
chat.add_members("room-1", &["bob"], &[]).await?; // every active device of bob
chat.send("room-1", b"hello bob").await?;

for event in chat.sync().await? {   // or `next_event()` on a follow stream
    if let mls::InboxEvent::Message { sender_device_id, plaintext, .. } = event {
        println!("{sender_device_id}: {}", String::from_utf8_lossy(&plaintext));
    }
}
chat.update_keys("room-1").await?;  // rotate this device's keys (post-compromise security)
# Ok(())
# }
```

- The device credential names `<tenant>/<subject>/<device_id>`; the tenant must
  be the canonical tenant id the token resolves to. Compare
  `DirectoryDevice.key_fingerprint` (from `list_devices`) out of band to verify a
  peer's device.
- Commits are `PublicMessage`s the broker validates; if another member's commit
  wins the epoch, the client processes the inbox up to the winning epoch,
  rebuilds the same change and resubmits it (`CommitOutcome::rebases`).
- Inbox entries are processed and the state saved **before** they are
  acknowledged, so a crash replays at most entries the client then skips.
- `InboxEvent::RemovalHint` reports a revoked member device; commit its removal
  with `remove_devices`.

`tests/live_messaging.rs` drives three devices through a real broker (gated on
`UDB_LIVE_SDK_TESTS=1` plus `UDB_AUTH_GRPC_TARGET` and the `UDB_LIVE_*` login
variables); `examples/messaging_peer.rs` is a scripted device the TypeScript
SDK's Rust<->TypeScript test drives.

```sh
cargo test --features mls --test live_messaging -- --nocapture
```

## Benchmark

`tests/live_perf.rs` measures every RPC in the contract against a live broker
and writes `perf_report_rust.md`, the same report the Go, Python, TypeScript
and PHP harnesses produce. It sends and times every request through tonic
itself; the request bodies come from a plan the shared Python seed writes, so
each RPC runs down its success path against real disposable entities. The seed
leaves those entities in place, so run it against a broker you can reset (CI
recreates the database before each SDK).

```sh
export UDB_LIVE_SDK_TESTS=1 UDB_LIVE_PERF=1
export UDB_GRPC_TARGET=127.0.0.1:50051 UDB_AUTH_GRPC_TARGET=127.0.0.1:50061
export UDB_LIVE_TENANT=<tenant> UDB_LIVE_PROJECT=<project>
export UDB_LIVE_USERNAME=<admin> UDB_LIVE_PASSWORD=<password>
# a second user in the same tenant holding the platform_admin role:
export UDB_LIVE_PLATFORM_USERNAME=<platform admin> UDB_LIVE_PLATFORM_PASSWORD=<password>
pip install -e "sdk/python[dev]"
python sdk/python/tests/export_bench_plan.py /tmp/udb-bench-plan.json
cd sdk/rust && UDB_BENCH_PLAN=/tmp/udb-bench-plan.json cargo test --release --test live_perf -- --nocapture
```

The plan holds no credentials: the harness logs in with the variables above and
substitutes its own session material where a request carries one. Without
`UDB_LIVE_SDK_TESTS=1` and `UDB_LIVE_PERF=1` the test does nothing, so a plain
`cargo test` stays offline.

## Publishing

`cargo publish` cannot reach outside the package directory, so the protos must be
vendored in first:

```sh
udb proto export --out sdk/rust/proto      # also writes third_party/googleapis
cargo publish -p udb-client
```

Verify the packaged crate builds standalone before pushing the tag:

```sh
cargo package -p udb-client --allow-dirty && cargo build --manifest-path \
  target/package/udb-client-<version>/Cargo.toml
```

## Development

This crate is deliberately **excluded from the broker's cargo workspace** — it is
a consumer of the wire contract, not part of the broker, and excluding it keeps a
root `cargo build` from compiling the client or unifying features with it.

```sh
cd sdk/rust
cargo test                 # unit tests
cargo build --examples     # compile-checks the documented usage
cargo clippy --all-targets -- -D warnings
cargo test --features mls  # plus the messaging client (crates/udb-mls)
cargo clippy --all-targets --features mls -- -D warnings
```

Lib doctests are off: google/api's proto comments embed indented HTTP and proto
examples, and rustdoc reads an indented block in a doc comment as a Rust doctest,
so `cargo test` tries to compile Google's prose. `examples/` covers usage instead,
which is a stronger check — a real program rather than a fragment.

## License

MIT OR Apache-2.0, matching the broker.
