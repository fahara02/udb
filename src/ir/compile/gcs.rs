//! Google Cloud Storage compiler (C9). Object-store semantics
//! identical in shape to S3 / Azure Blob: bucket + object key.
//! C7/C8 tenant prefix applied to keys.

use crate::backend::BackendKind;
use crate::generation::ManifestTable;
use crate::ir::filter::LogicalFilter;
use crate::ir::operations::{
    LogicalDelete, LogicalRead, LogicalResourceOp, LogicalWrite, ResourceKind, ResourceOpKind,
};
use crate::ir::value::LogicalValue;

use super::{CompileContext, CompileError, CompiledRendering, Compiler, ObjectOp};

#[derive(Debug, Default, Clone, Copy)]
pub struct GcsCompiler;

impl GcsCompiler {
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

    fn bucket_for(table: &ManifestTable) -> &str {
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

impl Compiler for GcsCompiler {
    fn kind(&self) -> BackendKind {
        BackendKind::Gcs
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
                    "GCS read on '{}' requires a primary-key field (object name)",
                    op.message_type
                ),
            });
        }
        let pk = &table.primary_key[0];
        let filter = op.filter.as_ref().ok_or_else(|| CompileError::Malformed {
            reason: "GCS read requires an equality filter on the primary key".into(),
        })?;
        let key =
            Self::key_from_filter(filter, pk).ok_or_else(|| CompileError::OperatorUnsupported {
                backend: BackendKind::Gcs,
                op: "non_pk_predicate",
            })?;
        Ok(CompiledRendering::Object {
            backend: BackendKind::Gcs,
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
                backend: BackendKind::Gcs,
                op: "batch_write",
            });
        }
        let table = self.resolve_table(&op.message_type, ctx)?;
        if table.primary_key.is_empty() {
            return Err(CompileError::Malformed {
                reason: format!(
                    "GCS write on '{}' requires a primary-key field",
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
                        "primary key '{pk}' must be String or Int; got {}",
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
            backend: BackendKind::Gcs,
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
                    "GCS delete on '{}' requires a primary-key field",
                    op.message_type
                ),
            });
        }
        let pk = &table.primary_key[0];
        let key = Self::key_from_filter(&op.filter, pk).ok_or_else(|| {
            CompileError::OperatorUnsupported {
                backend: BackendKind::Gcs,
                op: "non_pk_predicate",
            }
        })?;
        Ok(CompiledRendering::Object {
            backend: BackendKind::Gcs,
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
                backend: BackendKind::Gcs,
                op: "non_bucket_resource",
            });
        }
        let object_op = match op.op {
            ResourceOpKind::Ensure => ObjectOp::PutObject,
            ResourceOpKind::Drop => ObjectOp::DeleteObject,
            ResourceOpKind::List => ObjectOp::ListObjects,
        };
        Ok(CompiledRendering::Object {
            backend: BackendKind::Gcs,
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
    use crate::ir::operations::LogicalRead;
    use crate::ir::value::LogicalValue;

    fn fixture() -> CatalogManifest {
        let table = ManifestTable {
            message_name: "acme.docs.v1.Doc".into(),
            schema: "my-bucket".into(),
            table: "doc".into(),
            primary_key: vec!["object_name".into()],
            columns: vec![ManifestColumn {
                field_name: "object_name".into(),
                column_name: "object_name".into(),
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

    #[test]
    fn read_resolves_bucket_and_key_with_tenant_prefix() {
        let m = fixture();
        let ctx = CompileContext::new(&m)
            .with_tenant("acme")
            .with_project("p1");
        let read =
            LogicalRead::message("acme.docs.v1.Doc").with_filter(LogicalFilter::Comparison {
                field: "object_name".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("file.pdf".into()),
            });
        match GcsCompiler.compile_read(&read, &ctx).unwrap() {
            CompiledRendering::Object {
                bucket, key, op, ..
            } => {
                assert_eq!(op, ObjectOp::GetObject);
                assert_eq!(bucket, "my-bucket");
                assert_eq!(key, "t:acme/p:p1/file.pdf");
            }
            other => panic!("expected Object, got {other:?}"),
        }
    }
}
