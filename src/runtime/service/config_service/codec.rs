//! Value <-> storage/proto encodings for typed flag values. The storage pair is
//! `(value_type, value_json)`; the proto arm is a `FlagValue` oneof.

use tonic::Status;

use crate::proto::udb::core::config::services::v1 as config_pb;

use super::eval::FlagVal;

const VALUE_TYPE_BOOL: &str = "BOOL";
const VALUE_TYPE_STRING: &str = "STRING";
const VALUE_TYPE_NUMBER: &str = "NUMBER";
const VALUE_TYPE_JSON: &str = "JSON";

fn number_to_json(n: f64) -> String {
    serde_json::Number::from_f64(n)
        .map(serde_json::Value::Number)
        .unwrap_or(serde_json::Value::Null)
        .to_string()
}

/// Encode a typed value to its `(value_type, value_json)` storage pair.
pub(crate) fn flag_val_to_stored(value: &FlagVal) -> (String, String) {
    match value {
        FlagVal::Bool(b) => (
            VALUE_TYPE_BOOL.to_string(),
            serde_json::Value::Bool(*b).to_string(),
        ),
        FlagVal::Number(n) => (VALUE_TYPE_NUMBER.to_string(), number_to_json(*n)),
        FlagVal::Str(s) => (
            VALUE_TYPE_STRING.to_string(),
            serde_json::Value::String(s.clone()).to_string(),
        ),
        FlagVal::Json(j) => (VALUE_TYPE_JSON.to_string(), j.clone()),
    }
}

/// Decode a stored `(value_type, value_json)` pair back to a typed value.
pub(crate) fn stored_to_flag_val(value_type: &str, value_json: &str) -> FlagVal {
    let parsed: serde_json::Value =
        serde_json::from_str(value_json).unwrap_or(serde_json::Value::Null);
    match value_type {
        VALUE_TYPE_NUMBER => FlagVal::Number(parsed.as_f64().unwrap_or(0.0)),
        VALUE_TYPE_STRING => FlagVal::Str(parsed.as_str().unwrap_or("").to_string()),
        VALUE_TYPE_JSON => FlagVal::Json(parsed.to_string()),
        // BOOL is the default/fallback type.
        _ => FlagVal::Bool(parsed.as_bool().unwrap_or(false)),
    }
}

/// Convert the request oneof into a typed value, validating JSON arms.
pub(crate) fn proto_to_flag_val(value: &Option<config_pb::FlagValue>) -> Result<FlagVal, Status> {
    use config_pb::flag_value::Value;
    let inner = value
        .as_ref()
        .and_then(|fv| fv.value.as_ref())
        .ok_or_else(|| {
            crate::runtime::executor_utils::invalid_argument_fields(
                "value is required",
                [("value", "must set one FlagValue arm")],
            )
        })?;
    Ok(match inner {
        Value::BoolValue(b) => FlagVal::Bool(*b),
        Value::NumberValue(n) => FlagVal::Number(*n),
        Value::StringValue(s) => FlagVal::Str(s.clone()),
        Value::JsonValue(j) => {
            let text = if j.trim().is_empty() {
                "null"
            } else {
                j.as_str()
            };
            let parsed: serde_json::Value = serde_json::from_str(text).map_err(|e| {
                crate::runtime::executor_utils::invalid_argument_fields(
                    format!("json_value is not valid JSON: {e}"),
                    [("value.json_value", "must be valid JSON")],
                )
            })?;
            FlagVal::Json(parsed.to_string())
        }
    })
}

pub(crate) fn flag_val_to_proto(value: &FlagVal) -> config_pb::FlagValue {
    use config_pb::flag_value::Value;
    config_pb::FlagValue {
        value: Some(match value {
            FlagVal::Bool(b) => Value::BoolValue(*b),
            FlagVal::Number(n) => Value::NumberValue(*n),
            FlagVal::Str(s) => Value::StringValue(s.clone()),
            FlagVal::Json(j) => Value::JsonValue(j.clone()),
        }),
    }
}
