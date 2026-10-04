//! Rust client for [UDB](https://github.com/fahara02/udb) — a proto-driven gRPC
//! broker over multiple databases.
//!
//! ```no_run
//! use udb_client::{Metadata, UdbClient};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let meta = Metadata::new("tenant-1")
//!     .with_project("default")
//!     .with_bearer_token(std::env::var("UDB_TOKEN")?);
//!
//! let mut udb = UdbClient::connect("http://127.0.0.1:50051", meta).await?;
//! let set = udb
//!     .select(udb_client::proto::udb::entity::v1::SelectRequest {
//!         message_type: "myapp.v1.Invoice".into(),
//!         ..Default::default()
//!     })
//!     .await?;
//! // `records_json` is the canonical representation; `set.rows` is a
//! // compatibility field the broker sends EMPTY. Decode, never index `rows`.
//! for record in udb_client::Records::decode(&set)?.iter() {
//!     println!("{}", record.str("id")?);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Two listeners, two authorization models
//!
//! UDB serves its data plane and its native services on SEPARATE listeners with
//! DIFFERENT authorization models — the data plane authorizes through Casbin,
//! the native services through scope-based endpoint security. A credential
//! accepted by one is not automatically accepted by the other, and the mismatch
//! surfaces as a permissions error rather than a wrong-address error. [`UdbClient`]
//! speaks to the data plane.
//!
//! # Generated types
//!
//! The stubs under [`proto`] are generated at build time from the protos rather
//! than committed, so this crate cannot drift from the contract it ships with.
//! They are laid out by proto package: `udb.entity.v1` is
//! [`proto::udb::entity::v1`].

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]
// `tonic::Status` is a large error type, and clippy flags every fallible gRPC
// call for it. Boxing it would make this client's errors differ from the type
// every tonic user already handles, which is a worse trade than the move cost.
#![allow(clippy::result_large_err)]

pub mod auth;
pub mod client;
pub mod error;
/// The RPC registry generated from the proto descriptor set.
///
/// Regenerate with `udb sdk generate --lang rust --out sdk`; CI fails if the
/// committed copy differs from what the descriptor produces.
pub mod generated_rpcs;
pub mod metadata;
pub mod record;

/// Generated protobuf and tonic client types, nested by proto package.
///
/// The module tree is emitted by `build.rs` from the packages tonic actually
/// generated, so adding a service to the contract does not require editing a
/// hand-maintained list here.
pub mod proto {
    // Generated code: hold it to its own standards, not ours.
    #![allow(clippy::all, clippy::pedantic, rustdoc::all)]
    #![allow(missing_debug_implementations)]
    include!(concat!(env!("OUT_DIR"), "/udb_modules.rs"));
}

pub use auth::{Token, TokenManager};
pub use client::{SelectError, UdbClient};
pub use error::{CallPolicy, UdbError};
pub use generated_rpcs::{is_retry_safe, spec_for_path, RpcSpec};
pub use metadata::Metadata;
pub use record::{DecodeError, Record, Records};

// ── Runtime re-exports ────────────────────────────────────────────────────────
//
// This crate's public API is made of these crates' types: `UpsertRequest.payload`
// is a `prost_types::Struct`, every call returns a `tonic::Status`, and every
// generated message is a `prost::Message`. Without these re-exports a consumer
// has to work out which VERSIONS this crate resolved and add matching
// dependencies by hand — and getting it wrong produces a second nominal
// `prost_types::Struct` that cannot be passed to `payload`, with an error naming
// two paths that look identical.
//
// Re-exporting means the consumer can always name the exact instances this crate
// uses (`udb_client::prost_types::Struct`), whatever it resolves to. Adding a
// direct dependency stays optional rather than being a hidden requirement.
pub use prost;
pub use prost_types;
pub use serde_json;
pub use tonic;
