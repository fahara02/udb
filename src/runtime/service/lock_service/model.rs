//! Lock DTO / JSON decoders and the small pure helpers (lease name, unix time)
//! for the native `LockService`. Extracted verbatim from the former god file —
//! the mediated-read row → `Lock` mapping and the durable-row decode are
//! byte-for-byte identical.

use chrono::DateTime;

use crate::proto::udb::core::lock::services::v1 as lock_pb;

/// The mutual-exclusion lease name. Tenant-prefixed from the VERIFIED claim so two
/// tenants can hold the same logical name without colliding, and so a caller can
/// never serialize against another tenant's lock.
pub(crate) fn lease_name(tenant_id: &str, lock_name: &str) -> String {
    format!("app_lock:{tenant_id}:{lock_name}")
}

pub(crate) fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Parse a JSON timestamp value (RFC3339 string, as TIMESTAMPTZ comes back from
/// the mediated read) to unix seconds; 0 when absent/null/unparseable.
fn json_unix(map: &serde_json::Map<String, serde_json::Value>, key: &str) -> i64 {
    map.get(key)
        .and_then(|v| v.as_str())
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp())
        .unwrap_or(0)
}

/// Read a JSONB column back as a compact JSON string (it arrives as a nested
/// object); an already-string value passes through, absent → `{}`.
fn json_object_str(map: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    match map.get(key) {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(v) => serde_json::to_string(v).unwrap_or_else(|_| "{}".to_string()),
        None => "{}".to_string(),
    }
}

/// Map a mediated read row into the `Lock` inventory DTO.
pub(crate) fn lock_dto_from_json(row: &serde_json::Value) -> lock_pb::Lock {
    let map = lock_json_object(row);
    lock_pb::Lock {
        lock_id: json_str(map, "lock_id"),
        tenant_id: json_str(map, "tenant_id"),
        lock_name: json_str(map, "lock_name"),
        owner_id: json_str(map, "owner_id"),
        fencing_token: json_i64(map, "fencing_token"),
        lease_ttl_seconds: json_i64(map, "lease_ttl_seconds") as i32,
        status: json_str(map, "status"),
        acquired_at_unix: json_unix(map, "acquired_at"),
        expires_at_unix: json_unix(map, "expires_at"),
        metadata_json: json_object_str(map, "metadata_json"),
    }
}

fn lock_json_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
    row.get("n")
        .and_then(serde_json::Value::as_object)
        .or_else(|| row.as_object())
        .unwrap_or_else(|| {
            static EMPTY: std::sync::OnceLock<serde_json::Map<String, serde_json::Value>> =
                std::sync::OnceLock::new();
            EMPTY.get_or_init(serde_json::Map::new)
        })
}

fn json_str(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    match row.get(key) {
        Some(serde_json::Value::String(value)) => value.clone(),
        Some(serde_json::Value::Number(value)) => value.to_string(),
        Some(serde_json::Value::Bool(value)) => value.to_string(),
        _ => String::new(),
    }
}

fn json_i64(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> i64 {
    match row.get(key) {
        Some(serde_json::Value::Number(value)) => value.as_i64().unwrap_or(0),
        Some(serde_json::Value::String(value)) => value.trim().parse::<i64>().unwrap_or(0),
        _ => 0,
    }
}

/// A durable lock row decoded from the native read JSON.
pub(crate) struct StoredLock {
    pub(crate) lock_id: String,
    pub(crate) owner_id: String,
    pub(crate) fencing_token: i64,
    pub(crate) status: String,
}

pub(crate) fn stored_lock_from_json(row: &serde_json::Value) -> StoredLock {
    let map = lock_json_object(row);
    StoredLock {
        lock_id: json_str(map, "lock_id"),
        owner_id: json_str(map, "owner_id"),
        fencing_token: json_i64(map, "fencing_token"),
        status: json_str(map, "status"),
    }
}
