//! Compile-time proof that `udb-client` alone is enough.
//!
//! This crate depends on `udb-client` and nothing else. Every type it needs from
//! prost, tonic, and serde_json is reached through the SDK's re-exports, so the
//! file is an executable statement of what a consumer must add to their
//! `Cargo.toml` to use the whole public API: nothing.
//!
//! Every assertion is the compiler's, except the record-decoding one at the end,
//! which asserts on the exact wire shape the broker sends.

use std::collections::BTreeMap;

// Reached through the SDK — this crate declares no prost, tonic, or serde_json.
use udb_client::prost_types::{value::Kind, Struct, Value};
use udb_client::tonic::Status;

use udb_client::proto::udb::entity::v1::{RecordSet, Row, UpsertRequest};
use udb_client::{CallPolicy, DecodeError, Metadata, Record, Records, UdbError};

fn main() {
    write_path();
    error_path();
    read_path();
    println!("udb-client's re-exports are sufficient: no direct prost/tonic/serde_json");
}

/// `payload` and `expected` are `prost_types::Struct`. A consumer must be able to
/// build one without adding prost-types themselves.
fn write_path() {
    let mut fields = BTreeMap::new();
    fields.insert(
        "amount_minor".to_string(),
        Value {
            kind: Some(Kind::NumberValue(42.0)),
        },
    );
    let payload = Struct { fields };

    let _req = UpsertRequest {
        message_type: "consumer.v1.Probe".into(),
        payload: Some(payload.clone()),
        expected: Some(payload),
        ..Default::default()
    };
}

/// Every failure carries a `tonic::Status`, so handling one requires naming
/// tonic. It must be reachable without depending on tonic.
fn error_path() {
    let status: Status = Status::unavailable("probe");
    let err: UdbError = UdbError::from(status);
    assert!(
        err.is_retryable(),
        "UNAVAILABLE is retryable without detail"
    );

    let _meta = Metadata::new("tenant-probe").with_project("default");
    let _policy = CallPolicy::from_contract("/udb.services.v1.DataBroker/Select");
}

/// The read path, against the EXACT shape the broker puts on the wire: records in
/// `records_json`, one empty compatibility `Row` each.
fn read_path() {
    let served = RecordSet {
        records_json: vec![
            br#"{"id":"inv-1","amount_minor":9007199254740993,"paid":false}"#.to_vec(),
        ],
        // What the broker actually sends: present, aligned, and EMPTY.
        rows: vec![Row::default()],
        total_count: 1,
        ..RecordSet::default()
    };

    // The trap this crate exists to document: the compatibility field is
    // populated enough to look like an answer.
    assert_eq!(served.rows.len(), 1, "the row COUNT is right...");
    assert!(served.rows[0].fields.is_empty(), "...and it holds nothing");

    let records: Records = Records::decode(&served).expect("served shape must decode");
    let record: &Record = records.get(0).expect("one record");
    assert_eq!(record.str("id").expect("id"), "inv-1");
    // Past 2^53. Survives because the decoder never routes it through a double.
    assert_eq!(
        record.i64("amount_minor").expect("amount"),
        9007199254740993
    );
    assert!(!record.bool("paid").expect("paid"));

    // Typed errors are reachable and matchable from outside the crate.
    match record.str("nope") {
        Err(DecodeError::MissingField { field }) => assert_eq!(field, "nope"),
        other => panic!("expected MissingField, got {other:?}"),
    }

    // And the defect's own signature stays an error rather than an empty page.
    let compat_only = RecordSet {
        rows: vec![Row::default()],
        total_count: 1,
        ..RecordSet::default()
    };
    assert!(
        matches!(
            Records::decode(&compat_only),
            Err(DecodeError::CanonicalRecordsMissing { rows: 1 })
        ),
        "rows without records_json must not decode as an empty result"
    );
}
