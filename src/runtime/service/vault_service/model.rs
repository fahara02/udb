//! Durable-row DTOs and JSON decoders for the native `VaultService`: the
//! redacting-`Debug` `StoredSecret` / `StoredTransitKey` rows, the mediated-read
//! JSON accessors (mirroring `lock_service`), and the transit-version selectors.
//! Extracted verbatim from the former god file — the row decode and the
//! active/allowed-version selection are byte-for-byte identical.

use std::fmt;

use super::config::KEY_STATE_ACTIVE;

/// A durable KV secret version decoded from the native read JSON. Even though
/// the two `*_*` columns are CIPHERTEXT, they are still sensitive, so this type
/// also redacts them in `Debug`.
pub(crate) struct StoredSecret {
    pub(crate) secret_id: String,
    pub(crate) version: i64,
    pub(crate) ciphertext: String,
    pub(crate) data_key_wrapped: String,
    pub(crate) state: String,
    pub(crate) metadata_json: String,
}

impl fmt::Debug for StoredSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoredSecret")
            .field("secret_id", &self.secret_id)
            .field("version", &self.version)
            .field("state", &self.state)
            .field("ciphertext", &"[redacted]")
            .field("data_key_wrapped", &"[redacted]")
            .finish()
    }
}

/// A durable transit-key version decoded from the native read JSON.
pub(crate) struct StoredTransitKey {
    pub(crate) key_id: String,
    pub(crate) version: i64,
    pub(crate) algorithm: String,
    pub(crate) wrapped_key_material: String,
    pub(crate) state: String,
}

impl fmt::Debug for StoredTransitKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoredTransitKey")
            .field("key_id", &self.key_id)
            .field("version", &self.version)
            .field("state", &self.state)
            .field("algorithm", &self.algorithm)
            .field("wrapped_key_material", &"[redacted]")
            .finish()
    }
}

// ── JSON decoders (mirror lock_service) ───────────────────────────────────────

pub(crate) fn json_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
    row.get("n")
        .and_then(serde_json::Value::as_object)
        .or_else(|| row.as_object())
        .unwrap_or_else(|| {
            static EMPTY: std::sync::OnceLock<serde_json::Map<String, serde_json::Value>> =
                std::sync::OnceLock::new();
            EMPTY.get_or_init(serde_json::Map::new)
        })
}

pub(crate) fn json_str(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    match row.get(key) {
        Some(serde_json::Value::String(value)) => value.clone(),
        Some(serde_json::Value::Number(value)) => value.to_string(),
        Some(serde_json::Value::Bool(value)) => value.to_string(),
        Some(other @ (serde_json::Value::Object(_) | serde_json::Value::Array(_))) => {
            other.to_string()
        }
        _ => String::new(),
    }
}

pub(crate) fn json_i64(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> i64 {
    match row.get(key) {
        Some(serde_json::Value::Number(value)) => value.as_i64().unwrap_or(0),
        Some(serde_json::Value::String(value)) => value.trim().parse::<i64>().unwrap_or(0),
        _ => 0,
    }
}

pub(crate) fn stored_secret_from_json(row: &serde_json::Value) -> StoredSecret {
    let map = json_object(row);
    StoredSecret {
        secret_id: json_str(map, "secret_id"),
        version: json_i64(map, "version"),
        ciphertext: json_str(map, "ciphertext"),
        data_key_wrapped: json_str(map, "data_key_wrapped"),
        state: json_str(map, "state"),
        metadata_json: json_str(map, "metadata_json"),
    }
}

pub(crate) fn stored_transit_key_from_json(row: &serde_json::Value) -> StoredTransitKey {
    let map = json_object(row);
    StoredTransitKey {
        key_id: json_str(map, "key_id"),
        version: json_i64(map, "version"),
        algorithm: json_str(map, "algorithm"),
        wrapped_key_material: json_str(map, "wrapped_key_material"),
        state: json_str(map, "state"),
    }
}

/// The ACTIVE transit version with the highest number (the one Encrypt/Sign use).
pub(crate) fn active_transit(versions: &[StoredTransitKey]) -> Option<&StoredTransitKey> {
    versions
        .iter()
        .filter(|k| k.state == KEY_STATE_ACTIVE)
        .max_by_key(|k| k.version)
}

/// The specific transit version, if it is in one of the `allowed` states.
pub(crate) fn transit_version<'a>(
    versions: &'a [StoredTransitKey],
    version: i64,
    allowed: &[&str],
) -> Option<&'a StoredTransitKey> {
    versions
        .iter()
        .find(|k| k.version == version && allowed.contains(&k.state.as_str()))
}
