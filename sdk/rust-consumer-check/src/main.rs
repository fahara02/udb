//! Compile-time proof that a consumer's own `prost-types` and `tonic` are the
//! same nominal types the SDK's public API uses.
//!
//! Every assertion here is the compiler's. If `udb-client` moves to a prost or
//! tonic major this crate does not share, these lines stop compiling — which is
//! the whole point, and is what nothing checked when 0.5.21 shipped.

use std::collections::BTreeMap;

// The consumer's own crates, resolved independently of the SDK.
use prost_types::{value::Kind, Struct, Value};
use tonic::Status;

use udb_client::proto::udb::entity::v1::UpsertRequest;
use udb_client::{CallPolicy, Metadata, UdbError};

fn main() {
    // 1. prost-types: a Struct built HERE must fit the SDK's request fields.
    //    `payload` and `expected` are on the main write path — you cannot upsert
    //    with a body, or express a compare-and-swap, without this working.
    let mut fields = BTreeMap::new();
    fields.insert(
        "amount".to_string(),
        Value {
            kind: Some(Kind::NumberValue(42.0)),
        },
    );
    let payload = Struct { fields };

    let req = UpsertRequest {
        message_type: "consumer.v1.Probe".into(),
        payload: Some(payload.clone()),
        expected: Some(payload),
        ..Default::default()
    };

    // 2. tonic: the SDK's error carries a `tonic::Status`, so a consumer cannot
    //    handle a failure without naming tonic. It must be OUR tonic.
    let status: Status = Status::unavailable("probe");
    let err: UdbError = UdbError::from(status);
    let _code = err.code();

    // 3. The SDK's own types still work alongside them.
    let _meta = Metadata::new("tenant-probe").with_project("default");
    let _policy = CallPolicy::from_contract("/udb.services.v1.DataBroker/Select");

    println!(
        "consumer-owned prost-types and tonic match the SDK: {}",
        req.message_type
    );
}
