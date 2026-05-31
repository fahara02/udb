//! Canonical cache key for executor results (U2 step 8).
//!
//! The legacy cache (`runtime::core::tx_object::cache_get`) keyed on
//! `(operation, message_type, filter_hash)` — RPC-shaped, not
//! manifest-shaped. Two different RPCs reading the same Postgres rows
//! couldn't share the cache, and a fan-out leg couldn't share with a
//! direct select on the same predicate.
//!
//! The canonical key here hashes the **rendered wire shape plus context**:
//!
//! - `backend` + `instance` (so different replicas don't collide)
//! - the [`CompiledRendering`] shape token (sql / json / kv / object)
//! - the rendered statement / body / key template / object descriptor
//! - bound parameters in declaration order (for `Sql` and `KeyValue`)
//! - tenant + project ids
//! - manifest checksum (so a schema-changing migration invalidates
//!   everything in one stroke)
//!
//! Cache key is `sha256-base64url(canonical_string)` — short, URL-safe,
//! collision-resistant.

use sha2::{Digest, Sha256};

use super::compile::{CompiledRendering, KeyValueOp, ObjectOp};
use crate::backend::BackendKind;

/// Inputs that affect cache correctness but aren't carried inside the
/// `CompiledRendering` itself.
#[derive(Debug, Clone, Copy)]
pub struct CacheKeyContext<'a> {
    pub backend: &'a BackendKind,
    pub instance: Option<&'a str>,
    pub tenant_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    /// Manifest checksum at compile time. Changing the schema (and thus
    /// the column-name resolution) invalidates every cached entry from
    /// before the migration without an explicit DELETE.
    pub manifest_checksum: &'a str,
}

/// Build the canonical cache key for a `CompiledRendering` + context.
///
/// The function is **deterministic** — same input, same output — and
/// **stable** across processes. The on-the-wire format is:
///
/// `b64(sha256(canonical_string))`
///
/// where `canonical_string` is a `\n`-separated, ordered concatenation
/// of the fields the cache must distinguish.
pub fn canonical_cache_key(rendering: &CompiledRendering, ctx: &CacheKeyContext<'_>) -> String {
    let mut hasher = Sha256::new();

    // Context block — always first so the same rendering for different
    // tenants/projects hashes differently.
    hasher.update(b"udb-cache-v1\n");
    hasher.update(ctx.backend.as_str().as_bytes());
    hasher.update(b"\n");
    hasher.update(ctx.instance.unwrap_or("default").as_bytes());
    hasher.update(b"\n");
    hasher.update(ctx.tenant_id.unwrap_or("").as_bytes());
    hasher.update(b"\n");
    hasher.update(ctx.project_id.unwrap_or("").as_bytes());
    hasher.update(b"\n");
    hasher.update(ctx.manifest_checksum.as_bytes());
    hasher.update(b"\n");

    // Rendering block — shape token + the rendering payload.
    hasher.update(rendering.shape_token().as_bytes());
    hasher.update(b"\n");
    match rendering {
        CompiledRendering::Sql {
            statement, params, ..
        } => {
            hasher.update(statement.as_bytes());
            hasher.update(b"\n");
            // Params are positional — encode them as a JSON array so the
            // order matters and ValueOutOfRange/type differences propagate.
            let params_json = serde_json::to_vec(params).unwrap_or_else(|_| b"[]".to_vec());
            hasher.update(&params_json);
            hasher.update(b"\n");
        }
        CompiledRendering::Json {
            method, path, body, ..
        } => {
            hasher.update(format!("{method:?}").as_bytes());
            hasher.update(b"\n");
            hasher.update(path.as_bytes());
            hasher.update(b"\n");
            // Body uses canonical_json (sorted keys, no whitespace) so
            // equivalent documents hash identically.
            let canonical = canonical_json(body);
            hasher.update(canonical.as_bytes());
            hasher.update(b"\n");
        }
        CompiledRendering::KeyValue {
            op,
            key_template,
            value,
            ttl_seconds,
            ..
        } => {
            hasher.update(kv_op_token(*op).as_bytes());
            hasher.update(b"\n");
            hasher.update(key_template.as_bytes());
            hasher.update(b"\n");
            if let Some(v) = value {
                hasher.update(v);
            }
            hasher.update(b"\n");
            if let Some(ttl) = ttl_seconds {
                hasher.update(ttl.to_be_bytes());
            }
            hasher.update(b"\n");
        }
        CompiledRendering::Object {
            op,
            bucket,
            key,
            content_type,
            ..
        } => {
            hasher.update(object_op_token(*op).as_bytes());
            hasher.update(b"\n");
            hasher.update(bucket.as_bytes());
            hasher.update(b"\n");
            hasher.update(key.as_bytes());
            hasher.update(b"\n");
            hasher.update(content_type.as_deref().unwrap_or("").as_bytes());
            hasher.update(b"\n");
        }
    }

    let digest = hasher.finalize();
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD as B64};
    B64.encode(digest)
}

fn kv_op_token(op: KeyValueOp) -> &'static str {
    match op {
        KeyValueOp::Get => "get",
        KeyValueOp::Set => "set",
        KeyValueOp::Delete => "del",
        KeyValueOp::Exists => "exists",
        KeyValueOp::Scan => "scan",
    }
}

fn object_op_token(op: ObjectOp) -> &'static str {
    match op {
        ObjectOp::GetObject => "get",
        ObjectOp::PutObject => "put",
        ObjectOp::HeadObject => "head",
        ObjectOp::DeleteObject => "del",
        ObjectOp::ListObjects => "list",
        ObjectOp::GeneratePresigned => "presign",
    }
}

/// Serialise a JSON value with **sorted keys** and no whitespace so two
/// equivalent documents emit identical bytes. `serde_json::to_string`
/// preserves insertion order, which would make two compilers' output
/// hash differently even when semantically equal.
fn canonical_json(value: &serde_json::Value) -> String {
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

fn write_canonical(value: &serde_json::Value, out: &mut String) {
    use serde_json::Value::*;
    match value {
        Null => out.push_str("null"),
        Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Number(n) => out.push_str(&n.to_string()),
        String(s) => {
            out.push('"');
            for ch in s.chars() {
                match ch {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\t' => out.push_str("\\t"),
                    c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
                    c => out.push(c),
                }
            }
            out.push('"');
        }
        Array(arr) => {
            out.push('[');
            for (idx, item) in arr.iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        Object(map) => {
            out.push('{');
            // Sort keys lexicographically — that's what gives equivalent
            // documents identical bytes.
            let mut keys: Vec<&std::string::String> = map.keys().collect();
            keys.sort();
            for (idx, k) in keys.iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                out.push('"');
                out.push_str(k);
                out.push_str("\":");
                write_canonical(&map[k.as_str()], out);
            }
            out.push('}');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::compile::{HttpMethod, KeyValueOp};
    use crate::ir::value::LogicalValue;

    fn pg_rendering() -> CompiledRendering {
        CompiledRendering::Sql {
            backend: BackendKind::Postgres,
            statement: "SELECT * FROM x WHERE id = $1".to_string(),
            params: vec![LogicalValue::String("abc".into())],
        }
    }

    fn ctx<'a>(checksum: &'a str) -> CacheKeyContext<'a> {
        CacheKeyContext {
            backend: &BackendKind::Postgres,
            instance: Some("primary"),
            tenant_id: Some("acme"),
            project_id: Some("billing"),
            manifest_checksum: checksum,
        }
    }

    #[test]
    fn identical_inputs_produce_identical_keys() {
        let k1 = canonical_cache_key(&pg_rendering(), &ctx("v1"));
        let k2 = canonical_cache_key(&pg_rendering(), &ctx("v1"));
        assert_eq!(k1, k2);
    }

    #[test]
    fn tenant_separates_keys() {
        let k1 = canonical_cache_key(&pg_rendering(), &ctx("v1"));
        let mut other = ctx("v1");
        other.tenant_id = Some("other-tenant");
        let k2 = canonical_cache_key(&pg_rendering(), &other);
        assert_ne!(k1, k2);
    }

    #[test]
    fn manifest_checksum_separates_keys() {
        let k1 = canonical_cache_key(&pg_rendering(), &ctx("v1"));
        let k2 = canonical_cache_key(&pg_rendering(), &ctx("v2"));
        assert_ne!(k1, k2, "schema change must invalidate cache");
    }

    #[test]
    fn instance_separates_keys() {
        let k1 = canonical_cache_key(&pg_rendering(), &ctx("v1"));
        let mut other = ctx("v1");
        other.instance = Some("replica-1");
        let k2 = canonical_cache_key(&pg_rendering(), &other);
        assert_ne!(k1, k2);
    }

    #[test]
    fn parameter_values_affect_key() {
        let r1 = pg_rendering();
        let r2 = CompiledRendering::Sql {
            backend: BackendKind::Postgres,
            statement: "SELECT * FROM x WHERE id = $1".into(),
            params: vec![LogicalValue::String("def".into())],
        };
        assert_ne!(
            canonical_cache_key(&r1, &ctx("v1")),
            canonical_cache_key(&r2, &ctx("v1"))
        );
    }

    #[test]
    fn equivalent_json_bodies_produce_identical_keys() {
        let body_a: serde_json::Value = serde_json::from_str(r#"{"a": 1, "b": [2, 3]}"#).unwrap();
        let body_b: serde_json::Value = serde_json::from_str(r#"{"b": [2, 3], "a": 1}"#).unwrap();
        let r_a = CompiledRendering::Json {
            backend: BackendKind::Mongodb,
            method: HttpMethod::Post,
            path: "/action/find".into(),
            body: body_a,
        };
        let r_b = CompiledRendering::Json {
            backend: BackendKind::Mongodb,
            method: HttpMethod::Post,
            path: "/action/find".into(),
            body: body_b,
        };
        let key_a = canonical_cache_key(
            &r_a,
            &CacheKeyContext {
                backend: &BackendKind::Mongodb,
                instance: None,
                tenant_id: Some("t"),
                project_id: None,
                manifest_checksum: "v1",
            },
        );
        let key_b = canonical_cache_key(
            &r_b,
            &CacheKeyContext {
                backend: &BackendKind::Mongodb,
                instance: None,
                tenant_id: Some("t"),
                project_id: None,
                manifest_checksum: "v1",
            },
        );
        assert_eq!(key_a, key_b, "JSON key order must not affect cache key");
    }

    #[test]
    fn key_value_renderings_distinguish_by_op() {
        let template = "udb:t1:M:abc".to_string();
        let get = CompiledRendering::KeyValue {
            backend: BackendKind::Redis,
            op: KeyValueOp::Get,
            key_template: template.clone(),
            value: None,
            ttl_seconds: None,
        };
        let set = CompiledRendering::KeyValue {
            backend: BackendKind::Redis,
            op: KeyValueOp::Set,
            key_template: template,
            value: Some(b"v".to_vec()),
            ttl_seconds: Some(60),
        };
        let c = CacheKeyContext {
            backend: &BackendKind::Redis,
            instance: None,
            tenant_id: None,
            project_id: None,
            manifest_checksum: "v1",
        };
        assert_ne!(canonical_cache_key(&get, &c), canonical_cache_key(&set, &c));
    }

    #[test]
    fn key_is_short_url_safe_base64() {
        // Sanity-check the shape — base64url, no padding, fixed length.
        let key = canonical_cache_key(&pg_rendering(), &ctx("v1"));
        // SHA-256 = 32 bytes, base64-url-no-pad → 43 chars.
        assert_eq!(key.len(), 43);
        assert!(
            key.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "key must be URL-safe base64: {key}"
        );
    }
}
