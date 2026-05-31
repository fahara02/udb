//! Azure Blob Storage compiler (C9). Object-store semantics mirroring
//! the S3 compiler: bucket=container, key=blob name. The compiler is
//! deliberately small — Azure Blob's surface is `Get/Put/Delete blob`
//! plus container lifecycle. Aggregation / search / multi-record
//! writes are rejected with typed `OperationNotSupported`.
//!
//! C7/C8: tenant context is prepended to the blob key as
//! `t:<tenant>/p:<project>/<key>` (same convention as S3).

use crate::backend::BackendKind;
use crate::generation::ManifestTable;
use crate::ir::filter::LogicalFilter;
use crate::ir::operations::{
    LogicalDelete, LogicalRead, LogicalResourceOp, LogicalWrite, ResourceKind, ResourceOpKind,
};
use crate::ir::value::LogicalValue;

use super::{CompileContext, CompileError, CompiledRendering, Compiler, ObjectOp};

#[derive(Debug, Default, Clone, Copy)]
pub struct AzureBlobCompiler;

impl AzureBlobCompiler {
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

    /// Container name = manifest schema (mirrors S3's bucket = schema).
    fn container_for(table: &ManifestTable) -> &str {
        &table.schema
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

    /// C7/C8: tenant + project prefix on the blob name.
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
}

impl Compiler for AzureBlobCompiler {
    fn kind(&self) -> BackendKind {
        BackendKind::AzureBlob
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
                    "Azure Blob read on '{}' requires a primary-key field (blob name)",
                    op.message_type
                ),
            });
        }
        let pk = &table.primary_key[0];
        let filter = op.filter.as_ref().ok_or_else(|| CompileError::Malformed {
            reason: "Azure Blob read requires an equality filter on the primary key".into(),
        })?;
        let key =
            Self::key_from_filter(filter, pk).ok_or_else(|| CompileError::OperatorUnsupported {
                backend: BackendKind::AzureBlob,
                op: "non_pk_predicate",
            })?;
        Ok(CompiledRendering::Object {
            backend: BackendKind::AzureBlob,
            op: ObjectOp::GetObject,
            bucket: Self::container_for(table).to_string(),
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
                backend: BackendKind::AzureBlob,
                op: "batch_write",
            });
        }
        let table = self.resolve_table(&op.message_type, ctx)?;
        if table.primary_key.is_empty() {
            return Err(CompileError::Malformed {
                reason: format!(
                    "Azure Blob write on '{}' requires a primary-key field",
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
                        "primary key '{pk}' must be String or Int for blob names; got {}",
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
            backend: BackendKind::AzureBlob,
            op: ObjectOp::PutObject,
            bucket: Self::container_for(table).to_string(),
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
                    "Azure Blob delete on '{}' requires a primary-key field",
                    op.message_type
                ),
            });
        }
        let pk = &table.primary_key[0];
        let key = Self::key_from_filter(&op.filter, pk).ok_or_else(|| {
            CompileError::OperatorUnsupported {
                backend: BackendKind::AzureBlob,
                op: "non_pk_predicate",
            }
        })?;
        Ok(CompiledRendering::Object {
            backend: BackendKind::AzureBlob,
            op: ObjectOp::DeleteObject,
            bucket: Self::container_for(table).to_string(),
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
                backend: BackendKind::AzureBlob,
                op: "non_container_resource",
            });
        }
        let object_op = match op.op {
            ResourceOpKind::Ensure => ObjectOp::PutObject,
            ResourceOpKind::Drop => ObjectOp::DeleteObject,
            ResourceOpKind::List => ObjectOp::ListObjects,
        };
        Ok(CompiledRendering::Object {
            backend: BackendKind::AzureBlob,
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
    use crate::ir::operations::{
        ConflictStrategy, LogicalDelete, LogicalRead, LogicalRecord, LogicalWrite,
    };
    use crate::ir::value::LogicalValue;

    fn fixture() -> CatalogManifest {
        let table = ManifestTable {
            message_name: "acme.files.v1.Doc".into(),
            schema: "documents".into(),
            table: "doc".into(),
            primary_key: vec!["blob_name".into()],
            columns: vec![ManifestColumn {
                field_name: "blob_name".into(),
                column_name: "blob_name".into(),
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

    fn obj(r: CompiledRendering) -> (ObjectOp, String, String) {
        match r {
            CompiledRendering::Object {
                op, bucket, key, ..
            } => (op, bucket, key),
            other => panic!("expected Object, got {other:?}"),
        }
    }

    #[test]
    fn read_resolves_container_and_key() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let read =
            LogicalRead::message("acme.files.v1.Doc").with_filter(LogicalFilter::Comparison {
                field: "blob_name".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("invoices/q1.pdf".into()),
            });
        let (op, bucket, key) = obj(AzureBlobCompiler.compile_read(&read, &ctx).unwrap());
        assert_eq!(op, ObjectOp::GetObject);
        assert_eq!(bucket, "documents");
        assert_eq!(key, "invoices/q1.pdf");
    }

    #[test]
    fn write_with_tenant_prepends_key_prefix() {
        let m = fixture();
        let ctx = CompileContext::new(&m).with_tenant("acme");
        let mut rec = LogicalRecord::new();
        rec.insert("blob_name".into(), LogicalValue::String("file.dat".into()));
        let write = LogicalWrite {
            message_type: "acme.files.v1.Doc".into(),
            records: vec![rec],
            conflict: ConflictStrategy::Replace,
            return_fields: vec![],
        };
        match AzureBlobCompiler.compile_write(&write, &ctx).unwrap() {
            CompiledRendering::Object { key, .. } => {
                assert_eq!(key, "t:acme/p:default/file.dat");
            }
            other => panic!("expected Object, got {other:?}"),
        }
    }

    #[test]
    fn delete_resolves_key() {
        let m = fixture();
        let ctx = CompileContext::new(&m);
        let del = LogicalDelete {
            message_type: "acme.files.v1.Doc".into(),
            filter: LogicalFilter::Comparison {
                field: "blob_name".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("a.dat".into()),
            },
            return_fields: vec![],
        };
        let (op, _, key) = obj(AzureBlobCompiler.compile_delete(&del, &ctx).unwrap());
        assert_eq!(op, ObjectOp::DeleteObject);
        assert_eq!(key, "a.dat");
    }
}
