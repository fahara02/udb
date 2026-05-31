//! S3 / MinIO compiler (U2 step 6b).
//!
//! S3 is an object store, not a tabular one. The IR maps:
//!
//! - `LogicalRead` → `GetObject` (bucket+key from primary-key value).
//! - `LogicalWrite` → `PutObject` (bytes come from a magic `_bytes` field
//!   in the record, content-type from optional `_content_type`).
//! - `LogicalDelete` → `DeleteObject`.
//! - `LogicalResourceOp` on `Bucket` → ensure/drop/list bucket.
//!
//! `LogicalSearch` is not supported on S3. The shape stays consistent
//! with the existing S3 admin RPCs in the runtime, so the executor can
//! consume `CompiledRendering::Object` directly.

use crate::backend::BackendKind;
use crate::generation::ManifestTable;
use crate::ir::filter::LogicalFilter;
use crate::ir::operations::{
    LogicalDelete, LogicalRead, LogicalResourceOp, LogicalWrite, ResourceKind, ResourceOpKind,
};
use crate::ir::value::LogicalValue;

use super::{CompileContext, CompileError, CompiledRendering, Compiler, ObjectOp};

/// S3 / MinIO object-store compiler.
#[derive(Debug, Default, Clone, Copy)]
pub struct S3Compiler;

impl S3Compiler {
    fn resolve_table<'a>(
        &self,
        message_type: &str,
        ctx: &'a CompileContext<'_>,
    ) -> Result<&'a ManifestTable, CompileError> {
        crate::broker::table_for_message(ctx.manifest, message_type).ok_or_else(|| {
            CompileError::UnknownMessageType {
                message_type: message_type.to_string(),
            }
        })
    }

    /// Bucket name: use the manifest's `schema` as the bucket. Object
    /// stores don't have a "schema/table" model, so we reuse `schema` for
    /// bucket and resolve key from the primary-key field.
    fn bucket_for(table: &ManifestTable) -> &str {
        &table.schema
    }

    /// C7/C8: prepend the tenant + project namespace to the resolved
    /// key so every read/write/delete is scoped to the caller's
    /// tenant. Returns the key unchanged when no context is supplied.
    /// Convention: `t:<tenant>/p:<project>/<key>`. Operators who want
    /// a different layout configure it at the bucket policy level.
    fn scoped_key(ctx: &CompileContext<'_>, key: &str) -> String {
        let tid = ctx.tenant_id.unwrap_or("");
        let pid = ctx.project_id.unwrap_or("");
        if tid.is_empty() && pid.is_empty() {
            return key.to_string();
        }
        let tid = if tid.is_empty() { "default" } else { tid };
        let pid = if pid.is_empty() { "default" } else { pid };
        format!("t:{tid}/p:{pid}/{key}")
    }

    fn key_from_filter(filter: &LogicalFilter, pk_field: &str) -> Option<String> {
        match filter {
            LogicalFilter::Comparison { field, op, value }
                if field.eq_ignore_ascii_case(pk_field)
                    && matches!(op, crate::ir::filter::ComparisonOp::Eq) =>
            {
                match value {
                    LogicalValue::String(s) => Some(s.clone()),
                    LogicalValue::Int(i) => Some(i.to_string()),
                    _ => None,
                }
            }
            LogicalFilter::And(clauses) => clauses
                .iter()
                .find_map(|c| Self::key_from_filter(c, pk_field)),
            _ => None,
        }
    }
}

impl Compiler for S3Compiler {
    fn kind(&self) -> BackendKind {
        BackendKind::S3
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
                    "S3 read on '{}' requires a primary-key field (used as object key)",
                    op.message_type
                ),
            });
        }
        let pk = &table.primary_key[0];
        let filter = op.filter.as_ref().ok_or_else(|| CompileError::Malformed {
            reason: "S3 read requires an equality filter on the primary key".into(),
        })?;
        let key =
            Self::key_from_filter(filter, pk).ok_or_else(|| CompileError::OperatorUnsupported {
                backend: BackendKind::S3,
                op: "non_pk_predicate",
            })?;
        Ok(CompiledRendering::Object {
            backend: BackendKind::S3,
            op: ObjectOp::GetObject,
            bucket: Self::bucket_for(table).to_string(),
            key: Self::scoped_key(ctx, &key),
            content_type: None,
        })
    }

    fn compile_write(
        &self,
        op: &LogicalWrite,
        ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        if op.records.len() != 1 {
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::S3,
                op: "batch_write",
            });
        }
        let table = self.resolve_table(&op.message_type, ctx)?;
        if table.primary_key.is_empty() {
            return Err(CompileError::Malformed {
                reason: format!(
                    "S3 write on '{}' requires a primary-key field (used as object key)",
                    op.message_type
                ),
            });
        }
        let pk = &table.primary_key[0];
        let record = &op.records[0];
        let key = match record.get(pk).ok_or_else(|| CompileError::Malformed {
            reason: format!("record missing primary-key field '{pk}'"),
        })? {
            LogicalValue::String(s) => s.clone(),
            LogicalValue::Int(i) => i.to_string(),
            other => {
                return Err(CompileError::Malformed {
                    reason: format!(
                        "primary key '{pk}' must be String or Int for S3 keys; got {}",
                        other.type_token()
                    ),
                });
            }
        };
        let content_type = record.get("_content_type").and_then(|v| match v {
            LogicalValue::String(s) => Some(s.clone()),
            _ => None,
        });
        Ok(CompiledRendering::Object {
            backend: BackendKind::S3,
            op: ObjectOp::PutObject,
            bucket: Self::bucket_for(table).to_string(),
            key: Self::scoped_key(ctx, &key),
            content_type,
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
                    "S3 delete on '{}' requires a primary-key field",
                    op.message_type
                ),
            });
        }
        let pk = &table.primary_key[0];
        let key = Self::key_from_filter(&op.filter, pk).ok_or_else(|| {
            CompileError::OperatorUnsupported {
                backend: BackendKind::S3,
                op: "non_pk_predicate",
            }
        })?;
        Ok(CompiledRendering::Object {
            backend: BackendKind::S3,
            op: ObjectOp::DeleteObject,
            bucket: Self::bucket_for(table).to_string(),
            key: Self::scoped_key(ctx, &key),
            content_type: None,
        })
    }

    fn compile_resource_op(
        &self,
        op: &LogicalResourceOp,
        _ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        if !matches!(op.resource_kind, ResourceKind::Bucket) {
            return Err(CompileError::OperatorUnsupported {
                backend: BackendKind::S3,
                op: "non_bucket_resource",
            });
        }
        let object_op = match op.op {
            ResourceOpKind::Ensure => ObjectOp::PutObject, // bucket lifecycle reuses put semantics
            ResourceOpKind::Drop => ObjectOp::DeleteObject,
            ResourceOpKind::List => ObjectOp::ListObjects,
        };
        Ok(CompiledRendering::Object {
            backend: BackendKind::S3,
            op: object_op,
            bucket: op.resource_name.clone(),
            key: String::new(),
            content_type: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
    use crate::ir::filter::{ComparisonOp, LogicalFilter};
    use crate::ir::operations::{LogicalDelete, LogicalRead, LogicalRecord, LogicalWrite};
    use crate::ir::value::LogicalValue;

    fn fixture() -> CatalogManifest {
        let table = ManifestTable {
            message_name: "acme.docs.v1.Document".into(),
            schema: "doc-bucket".into(),
            table: "documents".into(),
            primary_key: vec!["object_key".into()],
            columns: vec![ManifestColumn {
                field_name: "object_key".into(),
                column_name: "object_key".into(),
                proto_type: "string".into(),
                sql_type: "text".into(),
                is_primary: true,
                ..Default::default()
            }],
            ..Default::default()
        };
        CatalogManifest {
            tables: vec![table],
            ..Default::default()
        }
    }

    fn obj(rendering: CompiledRendering) -> (ObjectOp, String, String) {
        match rendering {
            CompiledRendering::Object {
                op, bucket, key, ..
            } => (op, bucket, key),
            other => panic!("expected Object rendering, got {other:?}"),
        }
    }

    #[test]
    fn read_resolves_bucket_and_key() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let read =
            LogicalRead::message("acme.docs.v1.Document").with_filter(LogicalFilter::Comparison {
                field: "object_key".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("invoices/2026-05-28.pdf".into()),
            });
        let (op, bucket, key) = obj(S3Compiler.compile_read(&read, &ctx).unwrap());
        assert_eq!(op, ObjectOp::GetObject);
        assert_eq!(bucket, "doc-bucket");
        assert_eq!(key, "invoices/2026-05-28.pdf");
    }

    #[test]
    fn write_pulls_content_type_from_record() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let mut rec = LogicalRecord::new();
        rec.insert("object_key".into(), LogicalValue::String("a.txt".into()));
        rec.insert(
            "_content_type".into(),
            LogicalValue::String("text/plain".into()),
        );
        let write = LogicalWrite {
            message_type: "acme.docs.v1.Document".into(),
            records: vec![rec],
            conflict: crate::ir::operations::ConflictStrategy::Replace,
            return_fields: vec![],
        };
        match S3Compiler.compile_write(&write, &ctx).unwrap() {
            CompiledRendering::Object {
                op,
                bucket,
                key,
                content_type,
                ..
            } => {
                assert_eq!(op, ObjectOp::PutObject);
                assert_eq!(bucket, "doc-bucket");
                assert_eq!(key, "a.txt");
                assert_eq!(content_type.as_deref(), Some("text/plain"));
            }
            other => panic!("expected Object, got {other:?}"),
        }
    }

    #[test]
    fn delete_uses_key_from_filter() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let del = LogicalDelete {
            message_type: "acme.docs.v1.Document".into(),
            filter: LogicalFilter::Comparison {
                field: "object_key".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("a.txt".into()),
            },
            return_fields: vec![],
        };
        let (op, _, key) = obj(S3Compiler.compile_delete(&del, &ctx).unwrap());
        assert_eq!(op, ObjectOp::DeleteObject);
        assert_eq!(key, "a.txt");
    }

    // ── C7/C8 — tenant + project key prefix ────────────────────────

    #[test]
    fn read_with_tenant_context_prepends_key_prefix() {
        let m = fixture();
        let ctx = CompileContext::new(&m)
            .with_tenant("acme")
            .with_project("p1");
        let read =
            LogicalRead::message("acme.docs.v1.Document").with_filter(LogicalFilter::Comparison {
                field: "object_key".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("invoices/2026-05.pdf".into()),
            });
        let (_, _, key) = obj(S3Compiler.compile_read(&read, &ctx).unwrap());
        assert_eq!(key, "t:acme/p:p1/invoices/2026-05.pdf");
    }

    #[test]
    fn write_with_tenant_context_prepends_key_prefix() {
        let m = fixture();
        let ctx = CompileContext::new(&m).with_tenant("acme");
        let mut rec = LogicalRecord::new();
        rec.insert("object_key".into(), LogicalValue::String("a.txt".into()));
        let write = LogicalWrite {
            message_type: "acme.docs.v1.Document".into(),
            records: vec![rec],
            conflict: crate::ir::operations::ConflictStrategy::Replace,
            return_fields: vec![],
        };
        match S3Compiler.compile_write(&write, &ctx).unwrap() {
            CompiledRendering::Object { key, .. } => {
                assert_eq!(key, "t:acme/p:default/a.txt");
            }
            other => panic!("expected Object, got {other:?}"),
        }
    }
}
