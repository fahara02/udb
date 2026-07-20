//! Redis compiler (U2 step 6b).
//!
//! Redis is a key-value store; the IR's `LogicalRead` / `Write` /
//! `Delete` map to GET / SET / DEL commands on a deterministic key.
//! The namespace is:
//!
//! `udb:{project}:{tenant}:{message_type}:{primary_key_value}`
//!
//! ...which is what the existing read-through cache already uses for typed
//! Select. The compiler resolves tenant/project/pk into the concrete key
//! because generic dispatch receives only executor JSON, not the original IR
//! filter.
//!
//! `LogicalSearch` is intentionally unsupported (Redis isn't a vector
//! store; even RediSearch is a separate module not assumed available).

use crate::backend::BackendKind;
use crate::generation::ManifestTable;
use crate::ir::filter::LogicalFilter;
use crate::ir::operations::{LogicalDelete, LogicalRead, LogicalWrite};
use crate::ir::value::LogicalValue;

use super::{CompileContext, CompileError, CompiledRendering, Compiler, KeyValueOp};

/// Redis key-value compiler.
#[derive(Debug, Default, Clone, Copy)]
pub struct RedisCompiler;

impl RedisCompiler {
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

    /// Walk a filter for a primary-key equality and return the bound value.
    /// Redis can only do point lookups via key, so anything richer is a
    /// `CompileError::OperatorUnsupported`.
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

    /// Build the deterministic Redis key namespace pattern + concrete key.
    fn build_key(
        ctx: &CompileContext<'_>,
        message_type: &str,
        pk_value: &LogicalValue,
    ) -> (String, String) {
        let tenant = ctx.tenant_id.unwrap_or("default");
        let project = ctx.project_id.unwrap_or("default");
        let pk_token = match pk_value {
            LogicalValue::String(s) => s.clone(),
            LogicalValue::Int(i) => i.to_string(),
            other => format!("{}", super::util::value_to_json(other)),
        };
        let template = format!("udb:{{project}}:{{tenant}}:{message_type}:{{pk}}");
        let key = format!("udb:{project}:{tenant}:{message_type}:{pk_token}");
        (template, key)
    }
}

impl Compiler for RedisCompiler {
    fn kind(&self) -> BackendKind {
        BackendKind::Redis
    }

    fn compile_read(
        &self,
        op: &LogicalRead,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        let table = self.resolve_table(&op.message_type, ctx)?;
        if table.primary_key.is_empty() {
            return Err(CompileError::Malformed {
                reason: format!("Redis read on '{}' requires a primary key", op.message_type),
            });
        }
        let pk_field = &table.primary_key[0];
        let filter = op.filter.as_ref().ok_or_else(|| CompileError::Malformed {
            reason: "Redis read requires an equality filter on the primary key".into(),
        })?;
        let pk_value = Self::extract_pk_value(filter, pk_field).ok_or_else(|| {
            CompileError::OperatorUnsupported {
                backend: BackendKind::Redis,
                op: "non_pk_predicate",
            }
        })?;
        let (_pattern, key) = Self::build_key(ctx, &op.message_type, pk_value);
        Ok(CompiledRendering::KeyValue {
            backend: BackendKind::Redis,
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
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::Redis,
                op: "batch_write",
            });
        }
        let table = self.resolve_table(&op.message_type, ctx)?;
        if table.primary_key.is_empty() {
            return Err(CompileError::Malformed {
                reason: format!(
                    "Redis write on '{}' requires a primary key",
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

        // Value = JSON-encoded record bytes. The executor decides whether
        // to MessagePack-encode instead based on instance label.
        let json_value = serde_json::to_vec(
            &record
                .iter()
                .map(|(k, v)| (k.clone(), super::util::value_to_json(v)))
                .collect::<serde_json::Map<_, _>>(),
        )
        .map_err(|err| CompileError::BackendSpecific {
            backend: BackendKind::Redis,
            message: format!("failed to serialise record: {err}"),
        })?;

        let (_pattern, key) = Self::build_key(ctx, &op.message_type, pk_value);
        Ok(CompiledRendering::KeyValue {
            backend: BackendKind::Redis,
            op: KeyValueOp::Set,
            key_template: key,
            value: Some(json_value),
            // TTL defaults; the runtime overrides from per-request cache
            // policy. Zero means "no TTL" / persist.
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
                    "Redis delete on '{}' requires a primary key",
                    op.message_type
                ),
            });
        }
        let pk_field = &table.primary_key[0];
        let pk_value = Self::extract_pk_value(&op.filter, pk_field).ok_or_else(|| {
            CompileError::OperatorUnsupported {
                backend: BackendKind::Redis,
                op: "non_pk_predicate",
            }
        })?;
        let (_pattern, key) = Self::build_key(ctx, &op.message_type, pk_value);
        Ok(CompiledRendering::KeyValue {
            backend: BackendKind::Redis,
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
    use crate::ir::operations::{LogicalRead, LogicalRecord, LogicalWrite};
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
        let ctx = CompileContext::new(&m).with_tenant("acme");
        let read =
            LogicalRead::message("acme.cache.v1.Session").with_filter(LogicalFilter::Comparison {
                field: "id".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("abc".into()),
            });
        let (op, template, _) = kv(RedisCompiler.compile_read(&read, &ctx).unwrap());
        assert_eq!(op, KeyValueOp::Get);
        assert_eq!(template, "udb:default:acme:acme.cache.v1.Session:abc");
    }

    #[test]
    fn non_pk_predicate_is_rejected() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let read =
            LogicalRead::message("acme.cache.v1.Session").with_filter(LogicalFilter::Comparison {
                field: "user_id".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("xyz".into()),
            });
        let err = RedisCompiler.compile_read(&read, &ctx).unwrap_err();
        assert!(matches!(
            err,
            CompileError::OperatorUnsupported {
                backend: BackendKind::Redis,
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
            conflict: crate::ir::operations::ConflictStrategy::Replace,
            return_fields: vec![],
        };
        let (op, _, value) = kv(RedisCompiler.compile_write(&write, &ctx).unwrap());
        assert_eq!(op, KeyValueOp::Set);
        let body: serde_json::Value = serde_json::from_slice(&value.expect("value bytes")).unwrap();
        assert_eq!(body["id"], "s1");
        assert_eq!(body["user_id"], "u1");
    }

    #[test]
    fn search_is_unsupported() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let search = crate::ir::operations::LogicalSearch {
            message_type: "acme.cache.v1.Session".into(),
            vector: Some(vec![0.0; 3]),
            text_query: None,
            filter: None,
            top_k: 5,
            score_threshold: None,
            require_hybrid: false,
            with_vector: false,
            with_payload: false,
        };
        let err = RedisCompiler.compile_search(&search, &ctx).unwrap_err();
        assert_eq!(err.code(), "operation_not_supported");
    }
}
