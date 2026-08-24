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

    let set = udb
        .select(SelectRequest {
            message_type: "myapp.v1.Invoice".into(),
            limit: 10,
            ..Default::default()
        })
        .await?;

    println!("{} row(s)", set.rows.len());
    Ok(())
}
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
    Ok(set) => println!("{} row(s)", set.rows.len()),
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

`build.rs` resolves the protos from `../../proto` in a repo checkout, falling
back to a vendored `./proto`.

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
```

Lib doctests are off: google/api's proto comments embed indented HTTP and proto
examples, and rustdoc reads an indented block in a doc comment as a Rust doctest,
so `cargo test` tries to compile Google's prose. `examples/` covers usage instead,
which is a stronger check — a real program rather than a fragment.

## License

MIT OR Apache-2.0, matching the broker.
