//! Decoding relational reads.
//!
//! # Why this module exists
//!
//! [`RecordSet`] carries a returned page in TWO parallel representations, and
//! only one of them is canonical:
//!
//! - **`records_json`** — one JSON object per record. This is the data. Every
//!   other UDB SDK (Go, TypeScript, Python, Java, C#, PHP) reads it.
//! - **`rows`** — a `map<string, google.protobuf.Value>` per record, kept for
//!   wire compatibility. The broker emits it EMPTY on the relational read path.
//!
//! The proto declares both without a discriminator, so the generated struct
//! gives no hint which one to read. Reading `rows` compiles, type-checks, and
//! returns the correct RECORD COUNT with every field map empty — a populated
//! table reads back as a page of empty entities, with no error anywhere. That is
//! the failure this module exists to make impossible.
//!
//! [`Records::decode`] reads the canonical representation, fails closed on a
//! structurally inconsistent page, and preserves integer values exactly.
//!
//! ```no_run
//! # use udb_client::{Records, UdbClient};
//! # async fn example(udb: &mut UdbClient, req: udb_client::proto::udb::entity::v1::SelectRequest)
//! # -> Result<(), Box<dyn std::error::Error>> {
//! let records = Records::decode(&udb.select(req).await?)?;
//! for record in records.iter() {
//!     println!("{} — {}", record.str("id")?, record.i64("amount_minor")?);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Integer exactness
//!
//! Values are decoded with `serde_json`, whose `Number` holds `i64`/`u64`
//! natively, so `9007199254740993` survives. They are deliberately NOT routed
//! through `prost_types::Value`, whose only numeric kind is `NumberValue(f64)` —
//! that representation cannot hold a 64-bit integer, and rounds silently.
//! [`Record::i64`] and [`Record::u64`] reject a value that is not an exact
//! integer of that width rather than truncating it.

use std::collections::BTreeMap;
use std::fmt;

use serde_json::{Map, Value};

use crate::proto::udb::entity::v1::RecordSet;

/// One decoded record: the broker's canonical JSON object for a single row.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    fields: Map<String, Value>,
    revision: Option<String>,
}

impl Record {
    /// The record's fields, exactly as the broker serialized them.
    pub fn fields(&self) -> &Map<String, Value> {
        &self.fields
    }

    /// Consume the record and take its fields.
    pub fn into_fields(self) -> Map<String, Value> {
        self.fields
    }

    /// The opaque revision token for this record, when the read asked for one
    /// (`SelectRequest::include_revision`).
    ///
    /// Feed it back as `UpdateRequest::expected_revision` /
    /// `DeleteRequest::expected_revision` for optimistic concurrency.
    pub fn revision(&self) -> Option<&str> {
        self.revision.as_deref()
    }

    /// Raw access to one field. `None` when the column is absent from the
    /// projection — distinct from present-and-SQL-NULL, which is
    /// `Some(Value::Null)`.
    pub fn get(&self, field: &str) -> Option<&Value> {
        self.fields.get(field)
    }

    /// Whether the field is present and SQL NULL.
    pub fn is_null(&self, field: &str) -> bool {
        matches!(self.fields.get(field), Some(Value::Null))
    }

    /// A present, non-NULL field, or a typed error naming which of the two it
    /// was. Absent and NULL are separate errors on purpose: a caller reading a
    /// column that was never selected has a different bug from one reading a
    /// column that is legitimately empty.
    fn required(&self, field: &str) -> Result<&Value, DecodeError> {
        match self.fields.get(field) {
            None => Err(DecodeError::MissingField {
                field: field.to_string(),
            }),
            Some(Value::Null) => Err(DecodeError::NullField {
                field: field.to_string(),
            }),
            Some(value) => Ok(value),
        }
    }

    fn wrong_type(field: &str, expected: &'static str, found: &Value) -> DecodeError {
        DecodeError::UnexpectedType {
            field: field.to_string(),
            expected,
            found: json_kind(found),
        }
    }

    /// A required string field.
    pub fn str(&self, field: &str) -> Result<&str, DecodeError> {
        match self.required(field)? {
            Value::String(text) => Ok(text),
            other => Err(Self::wrong_type(field, "string", other)),
        }
    }

    /// A required boolean field.
    pub fn bool(&self, field: &str) -> Result<bool, DecodeError> {
        match self.required(field)? {
            Value::Bool(value) => Ok(*value),
            other => Err(Self::wrong_type(field, "bool", other)),
        }
    }

    /// A required signed 64-bit integer.
    ///
    /// Fails rather than truncating when the value is fractional, out of `i64`
    /// range, or lost precision before it reached us.
    pub fn i64(&self, field: &str) -> Result<i64, DecodeError> {
        let value = self.required(field)?;
        let Value::Number(number) = value else {
            return Err(Self::wrong_type(field, "integer", value));
        };
        number
            .as_i64()
            .ok_or_else(|| inexact_integer(field, number))
    }

    /// A required unsigned 64-bit integer.
    pub fn u64(&self, field: &str) -> Result<u64, DecodeError> {
        let value = self.required(field)?;
        let Value::Number(number) = value else {
            return Err(Self::wrong_type(field, "integer", value));
        };
        number
            .as_u64()
            .ok_or_else(|| inexact_integer(field, number))
    }

    /// A required signed 32-bit integer. Rejects anything outside `i32` rather
    /// than wrapping — a narrowing cast is how an out-of-range BIGINT becomes a
    /// plausible small number.
    pub fn i32(&self, field: &str) -> Result<i32, DecodeError> {
        let wide = self.i64(field)?;
        i32::try_from(wide).map_err(|_| DecodeError::IntegerOutOfRange {
            field: field.to_string(),
            width: "i32",
            value: wide.to_string(),
        })
    }

    /// A required floating-point field. Accepts an integer-valued number too,
    /// since JSON does not distinguish `1` from `1.0`.
    pub fn f64(&self, field: &str) -> Result<f64, DecodeError> {
        let value = self.required(field)?;
        match value {
            Value::Number(number) => number.as_f64().ok_or_else(|| DecodeError::UnexpectedType {
                field: field.to_string(),
                expected: "float",
                found: "number out of f64 range",
            }),
            other => Err(Self::wrong_type(field, "float", other)),
        }
    }

    /// A required `BYTEA` field.
    ///
    /// The broker base64-encodes binary columns so they survive JSON. Some
    /// executor paths additionally tag the cell with a `base64:` prefix; both
    /// spellings decode here so a caller does not have to know which path served
    /// the read.
    pub fn bytes(&self, field: &str) -> Result<Vec<u8>, DecodeError> {
        use base64::Engine as _;

        let value = self.required(field)?;
        let Value::String(text) = value else {
            return Err(Self::wrong_type(field, "base64 string", value));
        };
        let body = text.strip_prefix("base64:").unwrap_or(text);
        base64::engine::general_purpose::STANDARD
            .decode(body)
            .map_err(|err| DecodeError::InvalidBase64 {
                field: field.to_string(),
                reason: err.to_string(),
            })
    }

    /// A required timestamp, as the RFC 3339 text the broker emitted.
    ///
    /// Returned as `&str` rather than a parsed instant so this crate does not
    /// force a date-time library on its callers; hand it to whichever one the
    /// consumer already uses. `TIMESTAMPTZ` is emitted in the canonical `Z`
    /// form.
    pub fn timestamp(&self, field: &str) -> Result<&str, DecodeError> {
        self.str(field)
    }

    /// A required JSON/JSONB object field.
    pub fn object(&self, field: &str) -> Result<&Map<String, Value>, DecodeError> {
        match self.required(field)? {
            Value::Object(map) => Ok(map),
            other => Err(Self::wrong_type(field, "object", other)),
        }
    }

    /// A required array field.
    pub fn array(&self, field: &str) -> Result<&[Value], DecodeError> {
        match self.required(field)? {
            Value::Array(items) => Ok(items),
            other => Err(Self::wrong_type(field, "array", other)),
        }
    }

    /// `Ok(None)` when the field is absent or NULL, otherwise the decoded value.
    ///
    /// Wraps any accessor above:
    /// `record.optional("closed_at", Record::timestamp)?`.
    pub fn optional<'a, T, F>(&'a self, field: &str, read: F) -> Result<Option<T>, DecodeError>
    where
        F: FnOnce(&'a Self, &str) -> Result<T, DecodeError>,
    {
        match self.fields.get(field) {
            None | Some(Value::Null) => Ok(None),
            Some(_) => read(self, field).map(Some),
        }
    }
}

/// A decoded page of records, plus the page metadata that came with it.
#[derive(Debug, Clone, PartialEq)]
pub struct Records {
    records: Vec<Record>,
    total_count: i32,
    next_page_token: String,
}

impl Records {
    /// Decode a [`RecordSet`] into its canonical records.
    ///
    /// Fails closed rather than returning a plausible-but-wrong page. In
    /// particular a set whose `rows` are populated while `records_json` is empty
    /// is rejected instead of being reported as zero records — that shape is the
    /// signature of reading the compatibility representation, and silently
    /// turning it into an empty result is precisely the data-loss this decoder
    /// exists to prevent.
    pub fn decode(set: &RecordSet) -> Result<Self, DecodeError> {
        if set.records_json.is_empty() && !set.rows.is_empty() {
            return Err(DecodeError::CanonicalRecordsMissing {
                rows: set.rows.len(),
            });
        }
        // `rows` is index-aligned with `records_json` when present at all. The
        // broker emits one empty entry per record on the relational path, so a
        // DIFFERENT non-zero length means the two representations disagree about
        // how many records this page holds, and neither can be trusted.
        if !set.rows.is_empty() && set.rows.len() != set.records_json.len() {
            return Err(DecodeError::LengthMismatch {
                what: "rows",
                expected: set.records_json.len(),
                found: set.rows.len(),
            });
        }
        // Revisions are absent unless the read asked for them; when present they
        // are index-aligned, and a misalignment would attach a record's revision
        // token to a DIFFERENT record — an optimistic-concurrency write against
        // the wrong row.
        if !set.record_revisions.is_empty() && set.record_revisions.len() != set.records_json.len()
        {
            return Err(DecodeError::LengthMismatch {
                what: "record_revisions",
                expected: set.records_json.len(),
                found: set.record_revisions.len(),
            });
        }

        let mut records = Vec::with_capacity(set.records_json.len());
        for (index, blob) in set.records_json.iter().enumerate() {
            if blob.is_empty() {
                return Err(DecodeError::EmptyRecord { index });
            }
            let value: Value =
                serde_json::from_slice(blob).map_err(|err| DecodeError::Malformed {
                    index,
                    reason: err.to_string(),
                })?;
            let Value::Object(fields) = value else {
                return Err(DecodeError::NotAnObject {
                    index,
                    found: json_kind(&value),
                });
            };
            records.push(Record {
                fields,
                // An empty slot means "no tracked revision yet", which is not the
                // same as "revisions were not requested" — keep only a non-empty
                // token.
                revision: set
                    .record_revisions
                    .get(index)
                    .filter(|token| !token.is_empty())
                    .cloned(),
            });
        }

        Ok(Self {
            records,
            // Reported, not enforced. On the relational and cache paths this is
            // the page's own row count, but it is a plain `int32` on a message
            // several code paths build, and failing a correct read over a
            // metadata disagreement would be a worse bug than the one this
            // decoder fixes. `records.len()` is authoritative for iteration.
            total_count: set.total_count,
            next_page_token: set.next_page_token.clone(),
        })
    }

    /// Number of records on this page.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Record> {
        self.records.iter()
    }

    pub fn get(&self, index: usize) -> Option<&Record> {
        self.records.get(index)
    }

    /// The broker's reported total. See the note in [`Records::decode`]: prefer
    /// [`Records::len`] for how many records are actually in hand.
    pub fn total_count(&self) -> i32 {
        self.total_count
    }

    /// Token for the next page, empty when this is the last one.
    pub fn next_page_token(&self) -> &str {
        &self.next_page_token
    }

    pub fn into_records(self) -> Vec<Record> {
        self.records
    }
}

impl<'a> IntoIterator for &'a Records {
    type Item = &'a Record;
    type IntoIter = std::slice::Iter<'a, Record>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.iter()
    }
}

impl IntoIterator for Records {
    type Item = Record;
    type IntoIter = std::vec::IntoIter<Record>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.into_iter()
    }
}

/// Why a [`RecordSet`] or one of its fields could not be decoded.
///
/// Every variant names the record index or field, so a failure points at the
/// data rather than at this module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// The set carries compatibility `rows` but no canonical `records_json`.
    CanonicalRecordsMissing { rows: usize },
    /// A parallel array disagreed with `records_json` about the record count.
    LengthMismatch {
        what: &'static str,
        expected: usize,
        found: usize,
    },
    /// A record body was zero bytes.
    EmptyRecord { index: usize },
    /// A record body was not valid JSON.
    Malformed { index: usize, reason: String },
    /// A record body was valid JSON but not a JSON object.
    NotAnObject { index: usize, found: &'static str },
    /// The field was not in the returned projection.
    MissingField { field: String },
    /// The field was present and SQL NULL.
    NullField { field: String },
    /// The field held a different JSON kind than the accessor asked for.
    UnexpectedType {
        field: String,
        expected: &'static str,
        found: &'static str,
    },
    /// A numeric field was not an exact integer of the requested width —
    /// fractional, or beyond what the type can hold.
    NotAnExactInteger { field: String, value: String },
    /// An integer was exact but outside the narrower requested width.
    IntegerOutOfRange {
        field: String,
        width: &'static str,
        value: String,
    },
    /// A binary field was not decodable base64.
    InvalidBase64 { field: String, reason: String },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CanonicalRecordsMissing { rows } => write!(
                f,
                "record set carries {rows} compatibility row(s) but no records_json: the canonical \
                 records are missing, so this is not an empty result"
            ),
            Self::LengthMismatch {
                what,
                expected,
                found,
            } => write!(
                f,
                "record set is inconsistent: {found} {what} against {expected} record(s)"
            ),
            Self::EmptyRecord { index } => {
                write!(f, "record {index} has an empty body")
            }
            Self::Malformed { index, reason } => {
                write!(f, "record {index} is not valid JSON: {reason}")
            }
            Self::NotAnObject { index, found } => {
                write!(f, "record {index} is a JSON {found}, expected an object")
            }
            Self::MissingField { field } => {
                write!(f, "field `{field}` is not in the returned projection")
            }
            Self::NullField { field } => write!(f, "field `{field}` is NULL"),
            Self::UnexpectedType {
                field,
                expected,
                found,
            } => write!(f, "field `{field}` is a {found}, expected {expected}"),
            Self::NotAnExactInteger { field, value } => write!(
                f,
                "field `{field}` holds {value}, which is not an exact integer of the requested width"
            ),
            Self::IntegerOutOfRange {
                field,
                width,
                value,
            } => write!(f, "field `{field}` holds {value}, out of range for {width}"),
            Self::InvalidBase64 { field, reason } => {
                write!(f, "field `{field}` is not valid base64: {reason}")
            }
        }
    }
}

impl std::error::Error for DecodeError {}

fn inexact_integer(field: &str, number: &serde_json::Number) -> DecodeError {
    DecodeError::NotAnExactInteger {
        field: field.to_string(),
        value: number.to_string(),
    }
}

fn json_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Build the `rows`-shaped map a caller might expect, from the canonical
/// records, for the narrow case of code that already speaks
/// `google.protobuf.Value`.
///
/// Provided so such a caller does not hand-roll the conversion and reintroduce
/// the f64 rounding this module avoids: integers beyond 2^53 CANNOT be
/// represented in `google.protobuf.Value`, so they are rendered as their exact
/// decimal string rather than silently rounded.
pub fn record_to_prost_struct(record: &Record) -> prost_types::Struct {
    prost_types::Struct {
        fields: record
            .fields()
            .iter()
            .map(|(key, value)| (key.clone(), json_to_prost_value(value)))
            .collect(),
    }
}

fn json_to_prost_value(value: &Value) -> prost_types::Value {
    use prost_types::value::Kind;

    let kind = match value {
        Value::Null => Kind::NullValue(0),
        Value::Bool(value) => Kind::BoolValue(*value),
        Value::Number(number) => match exact_f64(number) {
            Some(value) => Kind::NumberValue(value),
            // Out of f64's exact-integer range. A rounded number would be a
            // silent corruption; the decimal spelling is lossless and the
            // caller can see what happened.
            None => Kind::StringValue(number.to_string()),
        },
        Value::String(text) => Kind::StringValue(text.clone()),
        Value::Array(items) => Kind::ListValue(prost_types::ListValue {
            values: items.iter().map(json_to_prost_value).collect(),
        }),
        Value::Object(map) => Kind::StructValue(prost_types::Struct {
            fields: map
                .iter()
                .map(|(key, value)| (key.clone(), json_to_prost_value(value)))
                .collect(),
        }),
    };
    prost_types::Value { kind: Some(kind) }
}

/// `Some(f64)` only when the conversion is exact.
fn exact_f64(number: &serde_json::Number) -> Option<f64> {
    const EXACT: i64 = 1 << 53;
    if let Some(value) = number.as_i64() {
        return (-EXACT..=EXACT).contains(&value).then_some(value as f64);
    }
    if let Some(value) = number.as_u64() {
        return (value <= EXACT as u64).then_some(value as f64);
    }
    number.as_f64()
}

/// Group records by a string field, preserving order within each group.
///
/// A small convenience for the common "read a page, bucket it by owner" shape,
/// kept here so it uses the checked accessors rather than raw map indexing.
pub fn group_by_str<'a>(
    records: &'a Records,
    field: &str,
) -> Result<BTreeMap<&'a str, Vec<&'a Record>>, DecodeError> {
    let mut grouped: BTreeMap<&'a str, Vec<&'a Record>> = BTreeMap::new();
    for record in records.iter() {
        grouped.entry(record.str(field)?).or_default().push(record);
    }
    Ok(grouped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::udb::entity::v1::Row;

    fn set(bodies: &[&str]) -> RecordSet {
        RecordSet {
            records_json: bodies.iter().map(|b| b.as_bytes().to_vec()).collect(),
            // Exactly what the broker sends on the relational path: one EMPTY
            // compatibility row per record.
            rows: bodies.iter().map(|_| Row::default()).collect(),
            total_count: bodies.len() as i32,
            ..RecordSet::default()
        }
    }

    /// The precise v0.5.21 wire shape: populated `records_json`, aligned empty
    /// `rows`. This is the fixture that must decode.
    #[test]
    fn decodes_the_shape_the_broker_actually_sends() {
        let decoded = Records::decode(&set(&[r#"{"id":"a","n":1}"#])).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded.get(0).unwrap().str("id").unwrap(), "a");
        assert_eq!(decoded.get(0).unwrap().i64("n").unwrap(), 1);
    }

    /// The defect's signature must NOT decode as an empty result.
    #[test]
    fn rows_without_canonical_records_is_an_error_not_an_empty_page() {
        let broken = RecordSet {
            records_json: Vec::new(),
            rows: vec![Row::default(), Row::default()],
            total_count: 2,
            ..RecordSet::default()
        };
        assert_eq!(
            Records::decode(&broken),
            Err(DecodeError::CanonicalRecordsMissing { rows: 2 })
        );
    }

    #[test]
    fn a_genuinely_empty_page_is_fine() {
        let decoded = Records::decode(&RecordSet::default()).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn integers_beyond_f64_survive() {
        let decoded = Records::decode(&set(&[r#"{"big":9007199254740993}"#])).unwrap();
        assert_eq!(
            decoded.get(0).unwrap().i64("big").unwrap(),
            9007199254740993
        );
    }

    #[test]
    fn u64_above_i64_max_survives() {
        let decoded = Records::decode(&set(&[r#"{"big":18446744073709551615}"#])).unwrap();
        assert_eq!(decoded.get(0).unwrap().u64("big").unwrap(), u64::MAX);
        // The same value is NOT a valid i64, and must say so rather than wrap.
        assert!(matches!(
            decoded.get(0).unwrap().i64("big"),
            Err(DecodeError::NotAnExactInteger { .. })
        ));
    }

    #[test]
    fn fractional_is_not_an_integer() {
        let decoded = Records::decode(&set(&[r#"{"n":1.5}"#])).unwrap();
        assert!(matches!(
            decoded.get(0).unwrap().i64("n"),
            Err(DecodeError::NotAnExactInteger { .. })
        ));
    }

    #[test]
    fn i32_rejects_overflow_instead_of_wrapping() {
        let decoded = Records::decode(&set(&[r#"{"n":2147483648}"#])).unwrap();
        assert!(matches!(
            decoded.get(0).unwrap().i32("n"),
            Err(DecodeError::IntegerOutOfRange { .. })
        ));
    }

    #[test]
    fn negative_is_not_unsigned() {
        let decoded = Records::decode(&set(&[r#"{"n":-1}"#])).unwrap();
        assert!(matches!(
            decoded.get(0).unwrap().u64("n"),
            Err(DecodeError::NotAnExactInteger { .. })
        ));
    }

    #[test]
    fn absent_and_null_are_different_errors() {
        let decoded = Records::decode(&set(&[r#"{"present":null}"#])).unwrap();
        let record = decoded.get(0).unwrap();
        assert!(matches!(
            record.str("present"),
            Err(DecodeError::NullField { .. })
        ));
        assert!(matches!(
            record.str("absent"),
            Err(DecodeError::MissingField { .. })
        ));
        assert!(record.is_null("present"));
        assert!(!record.is_null("absent"));
        assert_eq!(record.optional("present", Record::str).unwrap(), None);
        assert_eq!(record.optional("absent", Record::str).unwrap(), None);
    }

    #[test]
    fn bytes_decode_with_and_without_the_prefix() {
        let decoded = Records::decode(&set(&[r#"{"a":"aGk=","b":"base64:aGk="}"#])).unwrap();
        let record = decoded.get(0).unwrap();
        assert_eq!(record.bytes("a").unwrap(), b"hi");
        assert_eq!(record.bytes("b").unwrap(), b"hi");
    }

    #[test]
    fn bad_base64_is_a_typed_error() {
        let decoded = Records::decode(&set(&[r#"{"a":"!!!!"}"#])).unwrap();
        assert!(matches!(
            decoded.get(0).unwrap().bytes("a"),
            Err(DecodeError::InvalidBase64 { .. })
        ));
    }

    #[test]
    fn malformed_json_names_the_record() {
        let broken = RecordSet {
            records_json: vec![b"{".to_vec()],
            rows: vec![Row::default()],
            ..RecordSet::default()
        };
        assert!(matches!(
            Records::decode(&broken),
            Err(DecodeError::Malformed { index: 0, .. })
        ));
    }

    #[test]
    fn a_non_object_record_is_rejected() {
        let broken = RecordSet {
            records_json: vec![b"[1,2]".to_vec()],
            ..RecordSet::default()
        };
        assert!(matches!(
            Records::decode(&broken),
            Err(DecodeError::NotAnObject { index: 0, .. })
        ));
    }

    #[test]
    fn an_empty_record_body_is_rejected() {
        let broken = RecordSet {
            records_json: vec![Vec::new()],
            ..RecordSet::default()
        };
        assert_eq!(
            Records::decode(&broken),
            Err(DecodeError::EmptyRecord { index: 0 })
        );
    }

    #[test]
    fn misaligned_revisions_are_rejected() {
        let mut broken = set(&[r#"{"id":"a"}"#, r#"{"id":"b"}"#]);
        broken.record_revisions = vec!["r1".into()];
        assert!(matches!(
            Records::decode(&broken),
            Err(DecodeError::LengthMismatch {
                what: "record_revisions",
                ..
            })
        ));
    }

    #[test]
    fn misaligned_rows_are_rejected() {
        let mut broken = set(&[r#"{"id":"a"}"#, r#"{"id":"b"}"#]);
        broken.rows = vec![Row::default()];
        assert!(matches!(
            Records::decode(&broken),
            Err(DecodeError::LengthMismatch { what: "rows", .. })
        ));
    }

    #[test]
    fn revisions_attach_to_their_own_record() {
        let mut with_revisions = set(&[r#"{"id":"a"}"#, r#"{"id":"b"}"#]);
        with_revisions.record_revisions = vec![String::new(), "r2".into()];
        let decoded = Records::decode(&with_revisions).unwrap();
        // An empty slot means "not tracked yet", not "revision is the empty
        // string".
        assert_eq!(decoded.get(0).unwrap().revision(), None);
        assert_eq!(decoded.get(1).unwrap().revision(), Some("r2"));
    }

    #[test]
    fn page_metadata_is_carried_through() {
        let mut page = set(&[r#"{"id":"a"}"#]);
        page.next_page_token = "next".into();
        page.total_count = 97;
        let decoded = Records::decode(&page).unwrap();
        assert_eq!(decoded.next_page_token(), "next");
        // Reported as sent; `len` stays authoritative for what is in hand.
        assert_eq!(decoded.total_count(), 97);
        assert_eq!(decoded.len(), 1);
    }

    #[test]
    fn prost_conversion_refuses_to_round_a_big_integer() {
        use prost_types::value::Kind;

        let decoded = Records::decode(&set(&[r#"{"small":42,"big":9007199254740993}"#])).unwrap();
        let converted = record_to_prost_struct(decoded.get(0).unwrap());
        assert_eq!(
            converted.fields["small"].kind,
            Some(Kind::NumberValue(42.0))
        );
        // f64 cannot hold this. Exact decimal text beats a silently wrong number.
        assert_eq!(
            converted.fields["big"].kind,
            Some(Kind::StringValue("9007199254740993".into()))
        );
    }

    #[test]
    fn typed_accessors_reject_the_wrong_kind() {
        let decoded = Records::decode(&set(&[r#"{"s":"text","n":1}"#])).unwrap();
        let record = decoded.get(0).unwrap();
        assert!(matches!(
            record.i64("s"),
            Err(DecodeError::UnexpectedType {
                expected: "integer",
                found: "string",
                ..
            })
        ));
        assert!(matches!(
            record.str("n"),
            Err(DecodeError::UnexpectedType {
                expected: "string",
                found: "number",
                ..
            })
        ));
    }

    #[test]
    fn objects_and_arrays_round_trip() {
        let decoded = Records::decode(&set(&[r#"{"meta":{"k":"v"},"tags":["a","b"]}"#])).unwrap();
        let record = decoded.get(0).unwrap();
        assert_eq!(
            record.object("meta").unwrap()["k"],
            Value::String("v".into())
        );
        assert_eq!(record.array("tags").unwrap().len(), 2);
    }

    #[test]
    fn grouping_uses_checked_accessors() {
        let decoded = Records::decode(&set(&[
            r#"{"owner":"a","id":1}"#,
            r#"{"owner":"b","id":2}"#,
            r#"{"owner":"a","id":3}"#,
        ]))
        .unwrap();
        let grouped = group_by_str(&decoded, "owner").unwrap();
        assert_eq!(grouped["a"].len(), 2);
        assert_eq!(grouped["b"].len(), 1);
        assert!(matches!(
            group_by_str(&decoded, "missing"),
            Err(DecodeError::MissingField { .. })
        ));
    }
}
