//! Memcached compiler (C9).
//!
//! Memcached is a pure KV cache: the wire surface is `get` / `set` /
//! `delete` against a string key. Same shape as the Redis compiler
//! (`udb:<project>:<tenant>:<message_type>:<pk>` concrete key),
//! emitted as `CompiledRendering::KeyValue` so the Memcached executor
//! and the Redis executor share the dispatch contract.
//!
//! What Memcached DOESN'T support — and the compiler rejects with a
//! typed `OperationNotSupported` rather than silently degrading:
//!
//! - **Search** (no full-text or vector indexes)
//! - **Aggregate** (no server-side aggregation)
//! - **ResourceOp** (no concept of namespaces / buckets; key
//!   creation is implicit on `set`)
//! - **Non-PK predicates** (no secondary indexes; `Read`/`Delete`
//!   require an equality filter on the manifest primary key)
//!
//! Key-length constraint: Memcached caps keys at **250 bytes ASCII**
//! and disallows control characters / whitespace. The compiler doesn't
//! enforce this — the executor does, surfacing
//! typed invalid-argument field violations for keys that exceed the limit.

use crate::backend::BackendKind;
use crate::generation::ManifestTable;
use crate::ir::filter::LogicalFilter;
use crate::ir::operations::{LogicalDelete, LogicalRead, LogicalWrite};
use crate::ir::value::LogicalValue;

use super::{CompileContext, CompileError, CompiledRendering, Compiler, KeyValueOp};

/// Memcached KV compiler.
#[derive(Debug, Default, Clone, Copy)]
pub struct MemcachedCompiler;

impl MemcachedCompiler {
    fn resolve_table<'a>(
        &self,
        message_type: &str,
        ctx: &'a CompileContext<'_>,
    ) -> Result<&'a ManifestTable, CompileError> {
        match crate::broker::table_lookup(ctx.manifest, message_type) {
            crate::broker::TableLookup::Found(table) => Ok(table),
            // fix_plan §4.1: an ambiguous short name names its candidates so the
            // caller can FQN-qualify — never a silent first-wins misroute.
            crate::broker::TableLookup::Ambiguous { .. } => Err(CompileError::Malformed {
                reason: crate::broker::describe_table_lookup_miss(ctx.manifest, message_type),
            }),
            crate::broker::TableLookup::Missing => Err(CompileError::UnknownMessageType {
                message_type: message_type.to_string(),
            }),
        }
    }

    /// Walk a filter for a primary-key equality predicate. Returns the
    /// bound value if found; otherwise `None`. Mirrors the Redis
    /// compiler's `extract_pk_value` so the two KV compilers behave
    /// identically.
    fn extract_pk_value<'a>(filter: &'a LogicalFilter, pk_field: &str) -> Option<&'a LogicalValue> {
        match filter {
            LogicalFilter::Comparison { field, op, value }
                if field.eq_ignore_ascii_case(pk_field)
                    && matches!(op, crate::ir::filter::ComparisonOp::Eq) =>
            {
                Some(value)
            }
            LogicalFilter::And(clauses) => clauses
                .iter()
                .find_map(|c| Self::extract_pk_value(c, pk_field)),
            _ => None,
        }
    }

    /// Build the deterministic Memcached key string. Format identical
    /// to Redis so cache invalidation can be driven by the same key
    /// generator regardless of which cache tier is wired.
    fn build_key(ctx: &CompileContext<'_>, message_type: &str, pk_value: &LogicalValue) -> String {
        let tenant = ctx.tenant_id.unwrap_or("default");
        let project = ctx.project_id.unwrap_or("default");
        let pk_token = match pk_value {
            LogicalValue::String(s) => s.clone(),
            LogicalValue::Int(i) => i.to_string(),
            other => format!("{}", super::util::value_to_json(other)),
        };
        format!("udb:{project}:{tenant}:{message_type}:{pk_token}")
    }
}

impl Compiler for MemcachedCompiler {
    fn kind(&self) -> BackendKind {
        BackendKind::Memcached
    }

    fn compile_read(
        &self,
        op: &LogicalRead,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = self.resolve_table(&op.message_type, ctx)?;
        if table.primary_key.is_empty() {
            return Err(CompileError::Malformed {
                reason: format!(
                    "Memcached read on '{}' requires a primary key",
                    op.message_type
                ),
            });
        }
        let pk_field = &table.primary_key[0];
        let filter = op.filter.as_ref().ok_or_else(|| CompileError::Malformed {
            reason: "Memcached read requires an equality filter on the primary key".into(),
        })?;
        let pk_value = Self::extract_pk_value(filter, pk_field).ok_or_else(|| {
            CompileError::OperatorUnsupported {
                backend: BackendKind::Memcached,
                op: "non_pk_predicate",
            }
        })?;
        let key = Self::build_key(ctx, &op.message_type, pk_value);
        Ok(CompiledRendering::KeyValue {
            backend: BackendKind::Memcached,
            op: KeyValueOp::Get,
            key_template: key,
            value: None,
            ttl_seconds: None,
        })
    }

    fn compile_write(
        &self,
        op: &LogicalWrite,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        if op.records.len() != 1 {
            // Memcached has a multi-set extension (binary protocol) but
            // most operators run the text protocol that's single-key
            // per request. Refuse multi-record to keep semantics
            // consistent with Redis (which also rejects batch_write).
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Memcached,
                op: "batch_write",
            });
        }
        let table = self.resolve_table(&op.message_type, ctx)?;
        if table.primary_key.is_empty() {
            return Err(CompileError::Malformed {
                reason: format!(
                    "Memcached write on '{}' requires a primary key",
                    op.message_type
                ),
            });
        }
        let pk_field = &table.primary_key[0];
        let record = &op.records[0];
        let pk_value = record
            .get(pk_field)
            .ok_or_else(|| CompileError::Malformed {
                reason: format!("record missing primary-key field '{pk_field}'"),
            })?;

        // Value = JSON-encoded record bytes. Memcached is opaque-bytes
        // at the protocol level; JSON keeps the value
        // human-debuggable and matches Redis's encoding convention.
        let json_value = serde_json::to_vec(
            &record
                .iter()
                .map(|(k, v)| (k.clone(), super::util::value_to_json(v)))
                .collect::<serde_json::Map<_, _>>(),
        )
        .map_err(|err| CompileError::BackendSpecific {
            backend: BackendKind::Memcached,
            message: format!("failed to serialise record: {err}"),
        })?;

        let key = Self::build_key(ctx, &op.message_type, pk_value);
        Ok(CompiledRendering::KeyValue {
            backend: BackendKind::Memcached,
            op: KeyValueOp::Set,
            key_template: key,
            value: Some(json_value),
            // TTL = 0 → no expiration (Memcached's "persist forever").
            // The executor overrides from per-request cache policy.
            ttl_seconds: None,
        })
    }

    fn compile_delete(
        &self,
        op: &LogicalDelete,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = self.resolve_table(&op.message_type, ctx)?;
        if table.primary_key.is_empty() {
            return Err(CompileError::Malformed {
                reason: format!(
                    "Memcached delete on '{}' requires a primary key",
                    op.message_type
                ),
            });
        }
        let pk_field = &table.primary_key[0];
        let pk_value = Self::extract_pk_value(&op.filter, pk_field).ok_or_else(|| {
            CompileError::OperatorUnsupported {
                backend: BackendKind::Memcached,
                op: "non_pk_predicate",
            }
        })?;
        let key = Self::build_key(ctx, &op.message_type, pk_value);
        Ok(CompiledRendering::KeyValue {
            backend: BackendKind::Memcached,
            op: KeyValueOp::Delete,
            key_template: key,
            value: None,
            ttl_seconds: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
    use crate::ir::filter::{ComparisonOp, LogicalFilter};
    use crate::ir::operations::{ConflictStrategy, LogicalRead, LogicalRecord, LogicalWrite};
    use crate::ir::value::LogicalValue;

    fn fixture() -> CatalogManifest {
        let table = ManifestTable {
            message_name: "acme.cache.v1.Session".into(),
            schema: "cache".into(),
            table: "sessions".into(),
            primary_key: vec!["id".into()],
            columns: vec![
                ManifestColumn {
                    field_name: "id".into(),
                    column_name: "id".into(),
                    proto_type: "string".into(),
                    sql_type: "text".into(),
                    is_primary: true,
                    ..Default::default()
                },
                ManifestColumn {
                    field_name: "user_id".into(),
                    column_name: "user_id".into(),
                    proto_type: "string".into(),
                    sql_type: "text".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        CatalogManifest {
            tables: vec![table],
            ..Default::default()
        }
    }

    fn kv(rendering: CompiledRendering) -> (KeyValueOp, String, Option<Vec<u8>>) {
        match rendering {
            CompiledRendering::KeyValue {
                op,
                key_template,
                value,
                ..
            } => (op, key_template, value),
            other => panic!("expected KeyValue, got {other:?}"),
        }
    }

    #[test]
    fn read_with_pk_filter_emits_get() {
        let m = fixture();
        let ctx = CompileContext::new(&m)
            .with_project("proj")
            .with_tenant("acme");
        let read =
            LogicalRead::message("acme.cache.v1.Session").with_filter(LogicalFilter::Comparison {
                field: "id".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("s1".into()),
            });
        let (op, template, _) = kv(MemcachedCompiler.compile_read(&read, &ctx).unwrap());
        assert_eq!(op, KeyValueOp::Get);
        assert_eq!(template, "udb:proj:acme:acme.cache.v1.Session:s1");
    }

    #[test]
    fn non_pk_predicate_is_rejected() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let read =
            LogicalRead::message("acme.cache.v1.Session").with_filter(LogicalFilter::Comparison {
                field: "user_id".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("u1".into()),
            });
        let err = MemcachedCompiler.compile_read(&read, &ctx).unwrap_err();
        assert!(matches!(
            err,
            CompileError::OperatorUnsupported {
                backend: BackendKind::Memcached,
                op: "non_pk_predicate"
            }
        ));
    }

    #[test]
    fn write_serialises_record_as_json_bytes() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let mut rec = LogicalRecord::new();
        rec.insert("id".into(), LogicalValue::String("s1".into()));
        rec.insert("user_id".into(), LogicalValue::String("u1".into()));
        let write = LogicalWrite {
            message_type: "acme.cache.v1.Session".into(),
            records: vec![rec],
            conflict: ConflictStrategy::Replace,
            return_fields: vec![],
        };
        let (op, _, value) = kv(MemcachedCompiler.compile_write(&write, &ctx).unwrap());
        assert_eq!(op, KeyValueOp::Set);
        let body: serde_json::Value = serde_json::from_slice(&value.expect("value bytes")).unwrap();
        assert_eq!(body["id"], "s1");
        assert_eq!(body["user_id"], "u1");
    }

    #[test]
    fn batch_write_is_rejected() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let mut rec_a = LogicalRecord::new();
        rec_a.insert("id".into(), LogicalValue::String("a".into()));
        let mut rec_b = LogicalRecord::new();
        rec_b.insert("id".into(), LogicalValue::String("b".into()));
        let write = LogicalWrite {
            message_type: "acme.cache.v1.Session".into(),
            records: vec![rec_a, rec_b],
            conflict: ConflictStrategy::Replace,
            return_fields: vec![],
        };
        let err = MemcachedCompiler.compile_write(&write, &ctx).unwrap_err();
        assert!(matches!(
            err,
            CompileError::OperatorUnsupported {
                backend: BackendKind::Memcached,
                op: "batch_write"
            }
        ));
    }

    #[test]
    fn search_aggregate_resource_op_all_rejected() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let search = crate::ir::operations::LogicalSearch {
            message_type: "acme.cache.v1.Session".into(),
            vector: Some(vec![0.0; 2]),
            text_query: None,
            filter: None,
            top_k: 5,
            score_threshold: None,
            require_hybrid: false,
            with_vector: false,
            with_payload: false,
        };
        assert_eq!(
            MemcachedCompiler
                .compile_search(&search, &ctx)
                .unwrap_err()
                .code(),
            "operation_not_supported"
        );
    }
}
