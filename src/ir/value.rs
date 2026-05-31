//! Neutral value type used across backends (U2 step 1).
//!
//! Every backend the broker speaks to needs a slightly different in-wire
//! representation — sqlx bind parameters, Mongo BSON, Cypher parameters,
//! ClickHouse values, Qdrant payload JSON, Redis bytes, S3 keys. `LogicalValue`
//! is the **broker-side** representation: an enum closed enough that a
//! compiler can dispatch on it, open enough that any reasonable scalar/array
//! a proto/manifest can declare round-trips.
//!
//! Higher-precision types that don't fit a primitive (decimal, uuid, time
//! zones) flow through `Json`; the per-backend compiler decides how to bind.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A backend-neutral value the IR can carry as a filter operand, a record
/// field, or a parameter bind.
///
/// Wire format: this type derives the default serde external tagging
/// (`{"String":"foo"}`). The gRPC-facing surface that wraps it later will
/// translate to/from the explicit `oneof` declared in the proto contract.
/// Don't change the variant set without a contract review — see the
/// `LogicalValue::*` tokens carried in the SDKs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LogicalValue {
    /// Explicit SQL NULL / Mongo `null` / absent field. Distinct from a
    /// missing key in a record so the compiler can choose `IS NULL`
    /// vs. omitted-field semantics deliberately.
    Null,
    Bool(bool),
    /// Signed 64-bit integer. Backends that store narrower widths (int4,
    /// uint32) narrow at bind time; over-wide values surface a typed
    /// `CompileError::ValueOutOfRange`.
    Int(i64),
    Float(f64),
    String(String),
    /// Raw bytes. Compilers either bind as `bytea` (Postgres) / `BinData`
    /// (Mongo) / base64 (HTTP JSON) according to the wire's natural form.
    Bytes(Vec<u8>),
    /// Timestamps survive cross-backend round-trips via UTC. Local-zone
    /// conversion is the consumer's concern.
    Timestamp(DateTime<Utc>),
    /// Free-form JSON for backends with native document types (`jsonb`,
    /// BSON, Qdrant payload). Filter/comparison ops do **not** recurse into
    /// `Json`; use a sibling field path instead.
    Json(serde_json::Value),
    /// Homogeneous arrays. Used both for `IN` lists and for native array
    /// columns (`text[]`, `int4[]`, Mongo arrays, ClickHouse `Array(T)`).
    Array(Vec<LogicalValue>),
}

impl LogicalValue {
    /// Stable short label for compile errors and trace spans. Not part of
    /// the wire contract — callers should match on the variant.
    pub fn type_token(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "bool",
            Self::Int(_) => "int",
            Self::Float(_) => "float",
            Self::String(_) => "string",
            Self::Bytes(_) => "bytes",
            Self::Timestamp(_) => "timestamp",
            Self::Json(_) => "json",
            Self::Array(_) => "array",
        }
    }

    /// True when the value is `Null`. Used by SQL compilers to lower
    /// `field = NULL` to `field IS NULL` (the SQL surprise that bites
    /// every neutral-IR project once).
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_round_trips_through_serde() {
        let cases = [
            LogicalValue::Null,
            LogicalValue::Bool(true),
            LogicalValue::Int(42),
            LogicalValue::Float(3.14),
            LogicalValue::String("hello".to_string()),
            LogicalValue::Bytes(vec![1, 2, 3]),
            LogicalValue::Json(serde_json::json!({"k": "v"})),
            LogicalValue::Array(vec![
                LogicalValue::Int(1),
                LogicalValue::Int(2),
                LogicalValue::Null,
            ]),
        ];
        for case in &cases {
            let s = serde_json::to_string(case).expect("serialize");
            let back: LogicalValue = serde_json::from_str(&s).expect("deserialize");
            assert_eq!(case, &back, "round-trip mismatch for {}", case.type_token());
        }
    }

    #[test]
    fn type_tokens_are_pinned() {
        // SDKs surface these in errors; changing one is a contract change.
        assert_eq!(LogicalValue::Null.type_token(), "null");
        assert_eq!(LogicalValue::Bool(false).type_token(), "bool");
        assert_eq!(LogicalValue::Int(0).type_token(), "int");
        assert_eq!(LogicalValue::Float(0.0).type_token(), "float");
        assert_eq!(LogicalValue::String(String::new()).type_token(), "string");
    }
}
