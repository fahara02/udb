//! Regression fixtures for relational read decoding, exercised from OUTSIDE the
//! crate — the way a consumer sees it.
//!
//! The unit tests in `src/record.rs` cover the decoder's branches. These pin the
//! two things that actually went wrong in 0.5.21:
//!
//! 1. the exact wire shape the broker sends must decode, and
//! 2. the shape a broken client would produce must NOT decode as an empty page.
//!
//! Both fixtures are written as literal bytes rather than built from a helper, so
//! they keep describing the wire even if the helpers change.

use udb_client::proto::udb::entity::v1::{RecordSet, Row};
use udb_client::{DecodeError, Records};

/// Byte-for-byte what `rows_to_record_set` puts on the wire: the records in
/// `records_json`, one EMPTY `Row` each, `total_count` equal to the record count.
fn served_page() -> RecordSet {
    RecordSet {
        records_json: vec![
            br#"{"id":"inv-1","amount_minor":100,"created_at":"2026-08-26T10:00:00Z"}"#.to_vec(),
            br#"{"id":"inv-2","amount_minor":9007199254740993,"created_at":"2026-08-26T11:00:00Z"}"#
                .to_vec(),
        ],
        rows: vec![Row::default(), Row::default()],
        total_count: 2,
        ..RecordSet::default()
    }
}

#[test]
fn the_shape_the_broker_sends_decodes() {
    let records = Records::decode(&served_page()).expect("served page must decode");
    assert_eq!(records.len(), 2);
    assert_eq!(records.get(0).unwrap().str("id").unwrap(), "inv-1");
    assert_eq!(records.get(0).unwrap().i64("amount_minor").unwrap(), 100);
    assert_eq!(
        records.get(1).unwrap().timestamp("created_at").unwrap(),
        "2026-08-26T11:00:00Z"
    );
}

/// The defect, stated as a test: the compatibility representation is present and
/// EMPTY on a page that really has records. Any reader of `rows` sees the right
/// count and no data.
#[test]
fn the_compatibility_rows_are_empty_on_a_populated_page() {
    let page = served_page();
    assert_eq!(page.rows.len(), 2, "the count is right");
    assert!(
        page.rows.iter().all(|row| row.fields.is_empty()),
        "and every field map is empty — this is why `rows` must not be read"
    );
}

/// A BIGINT past 2^53 has to survive. It does not if a decoder routes numbers
/// through a double, which is what `google.protobuf.Value` would force.
#[test]
fn integers_past_the_double_boundary_survive() {
    let records = Records::decode(&served_page()).unwrap();
    assert_eq!(
        records.get(1).unwrap().i64("amount_minor").unwrap(),
        9007199254740993,
        "2^53+1 must round-trip exactly"
    );
}

/// Acceptance: a fixture containing only empty rows must NOT become a successful
/// empty entity. This is the shape that turned real rows into empty records.
#[test]
fn rows_without_records_json_is_rejected_not_reported_as_empty() {
    let compat_only = RecordSet {
        records_json: Vec::new(),
        rows: vec![Row::default(), Row::default(), Row::default()],
        total_count: 3,
        ..RecordSet::default()
    };
    match Records::decode(&compat_only) {
        Err(DecodeError::CanonicalRecordsMissing { rows }) => assert_eq!(rows, 3),
        Ok(records) => panic!(
            "decoded {} record(s) — a page with missing canonical records must not \
             be reported as a successful result",
            records.len()
        ),
        Err(other) => panic!("expected CanonicalRecordsMissing, got {other}"),
    }
}

/// A page that is genuinely empty is still fine — the check above must not make
/// zero records unrepresentable.
#[test]
fn a_truly_empty_page_still_decodes() {
    let empty = RecordSet::default();
    assert!(Records::decode(&empty).unwrap().is_empty());
}

/// Revisions are index-aligned with `records_json`. A misalignment would hand a
/// record the NEXT record's CAS token, so it must fail rather than guess.
#[test]
fn misaligned_revisions_fail_closed() {
    let mut page = served_page();
    page.record_revisions = vec!["r1".into()];
    assert!(matches!(
        Records::decode(&page),
        Err(DecodeError::LengthMismatch {
            what: "record_revisions",
            ..
        })
    ));
}

#[test]
fn aligned_revisions_attach_to_the_right_record() {
    let mut page = served_page();
    page.record_revisions = vec![String::new(), "rev-2".into()];
    let records = Records::decode(&page).unwrap();
    assert_eq!(records.get(0).unwrap().revision(), None, "untracked row");
    assert_eq!(records.get(1).unwrap().revision(), Some("rev-2"));
}

/// The cache path and the relational path must be indistinguishable. Both emit
/// records in `records_json` with one empty `Row` each, so the same fixture
/// stands for both — this test fails if that ever stops being true for the
/// decoder.
#[test]
fn cached_and_uncached_shapes_decode_identically() {
    let uncached = served_page();
    let cached = RecordSet {
        records_json: uncached.records_json.clone(),
        rows: vec![Row::default(); uncached.records_json.len()],
        total_count: uncached.records_json.len() as i32,
        ..RecordSet::default()
    };
    assert_eq!(
        Records::decode(&uncached).unwrap(),
        Records::decode(&cached).unwrap(),
        "a cached read and an uncached read must decode to the same records"
    );
}
