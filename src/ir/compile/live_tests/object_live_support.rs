use std::collections::BTreeMap;

use crate::backend::BackendKind;
use crate::generation::CatalogManifest;
use crate::ir::compile::{
    CompileContext, CompileOperation, CompiledRendering, ObjectOp, compile_for_backend,
};
use crate::ir::filter::{ComparisonOp, LogicalFilter};
use crate::ir::operations::{
    ConflictStrategy, LogicalDelete, LogicalRead, LogicalRecord, LogicalWrite,
};
use crate::ir::value::LogicalValue;
use crate::runtime::executors::ObjectExecutor;

use super::support::customer_manifest;

const MESSAGE: &str = "acme.billing.v1.Customer";

#[derive(Debug)]
struct ObjectDispatch {
    op: ObjectOp,
    bucket: String,
    key: String,
    content_type: Option<String>,
}

pub(super) async fn assert_compiled_object_tenant_isolation<E>(
    backend: BackendKind,
    backend_label: &str,
    project: &'static str,
    executor: &E,
    bucket: &str,
) where
    E: ObjectExecutor + Sync,
{
    let manifest = object_manifest(bucket);
    let key = format!("objects/{}.txt", uuid::Uuid::new_v4().simple());
    let tenant_a_bytes = format!("{backend_label} tenant-a compiled object bytes").into_bytes();
    let tenant_b_bytes = format!("{backend_label} tenant-b compiled object bytes").into_bytes();

    let write_a = compile_write(&backend, &manifest, project, "tenant-a", &key);
    let write_b = compile_write(&backend, &manifest, project, "tenant-b", &key);
    assert_eq!(write_a.op, ObjectOp::PutObject);
    assert_eq!(write_b.op, ObjectOp::PutObject);
    assert_ne!(
        write_a.key.as_str(),
        write_b.key.as_str(),
        "compiled {backend_label} keys must include tenant context"
    );
    assert_eq!(
        write_a.key,
        format!("t:tenant-a/p:{project}/{key}"),
        "compiled {backend_label} key must include tenant/project prefix"
    );

    put_object(executor, write_a, tenant_a_bytes.clone(), backend_label).await;
    put_object(executor, write_b, tenant_b_bytes.clone(), backend_label).await;

    assert_eq!(
        get_object(
            executor,
            compile_read(&backend, &manifest, project, "tenant-a", &key),
            backend_label,
        )
        .await,
        tenant_a_bytes,
        "compiled {backend_label} read must return tenant-a object"
    );
    assert_eq!(
        get_object(
            executor,
            compile_read(&backend, &manifest, project, "tenant-b", &key),
            backend_label,
        )
        .await,
        tenant_b_bytes,
        "compiled {backend_label} read must return tenant-b object"
    );

    delete_object(
        executor,
        compile_delete(&backend, &manifest, project, "tenant-a", &key),
        backend_label,
    )
    .await;
    assert!(
        ObjectExecutor::get_object(
            executor,
            &object_spec(compile_read(&backend, &manifest, project, "tenant-a", &key)).to_string()
        )
        .await
        .is_err(),
        "tenant-a object must be gone after compiled {backend_label} delete"
    );
    assert_eq!(
        get_object(
            executor,
            compile_read(&backend, &manifest, project, "tenant-b", &key),
            backend_label,
        )
        .await,
        tenant_b_bytes,
        "tenant-b object must remain after tenant-a compiled {backend_label} delete"
    );

    delete_object(
        executor,
        compile_delete(&backend, &manifest, project, "tenant-b", &key),
        backend_label,
    )
    .await;
}

fn object_manifest(bucket: &str) -> CatalogManifest {
    let mut manifest = customer_manifest(bucket);
    let table = &mut manifest.tables[0];
    table.primary_key = vec!["id".to_string()];
    manifest
}

fn compile_write(
    backend: &BackendKind,
    manifest: &CatalogManifest,
    project: &'static str,
    tenant: &'static str,
    key: &str,
) -> ObjectDispatch {
    let write = LogicalWrite {
        message_type: MESSAGE.to_string(),
        records: vec![record([
            ("id", key),
            ("name", "Object"),
            ("email", "object@example.com"),
            ("tenant_id", tenant),
            ("_content_type", "text/plain"),
        ])],
        conflict: ConflictStrategy::Replace,
        return_fields: Vec::new(),
    };
    compile_object(
        backend,
        manifest,
        project,
        tenant,
        CompileOperation::Write(&write),
    )
}

fn compile_read(
    backend: &BackendKind,
    manifest: &CatalogManifest,
    project: &'static str,
    tenant: &'static str,
    key: &str,
) -> ObjectDispatch {
    let read = LogicalRead::message(MESSAGE).with_filter(LogicalFilter::Comparison {
        field: "id".to_string(),
        op: ComparisonOp::Eq,
        value: LogicalValue::String(key.to_string()),
    });
    compile_object(
        backend,
        manifest,
        project,
        tenant,
        CompileOperation::Read(&read),
    )
}

fn compile_delete(
    backend: &BackendKind,
    manifest: &CatalogManifest,
    project: &'static str,
    tenant: &'static str,
    key: &str,
) -> ObjectDispatch {
    let delete = LogicalDelete {
        message_type: MESSAGE.to_string(),
        filter: LogicalFilter::Comparison {
            field: "id".to_string(),
            op: ComparisonOp::Eq,
            value: LogicalValue::String(key.to_string()),
        },
        return_fields: Vec::new(),
    };
    compile_object(
        backend,
        manifest,
        project,
        tenant,
        CompileOperation::Delete(&delete),
    )
}

fn compile_object(
    backend: &BackendKind,
    manifest: &CatalogManifest,
    project: &'static str,
    tenant: &'static str,
    operation: CompileOperation<'_>,
) -> ObjectDispatch {
    let ctx = CompileContext::new(manifest)
        .with_tenant(tenant)
        .with_project(project);
    match compile_for_backend(backend, operation, &ctx)
        .expect("object compiler is present")
        .expect("object operation compiles")
    {
        CompiledRendering::Object {
            backend: actual,
            op,
            bucket,
            key,
            content_type,
        } => {
            assert_eq!(&actual, backend);
            ObjectDispatch {
                op,
                bucket,
                key,
                content_type,
            }
        }
        other => panic!("expected Object rendering for {backend:?}, got {other:?}"),
    }
}

async fn put_object<E>(executor: &E, dispatch: ObjectDispatch, bytes: Vec<u8>, backend_label: &str)
where
    E: ObjectExecutor + Sync,
{
    assert_eq!(dispatch.op, ObjectOp::PutObject);
    ObjectExecutor::put_object(executor, &object_spec(dispatch).to_string(), bytes)
        .await
        .unwrap_or_else(|err| panic!("execute compiled {backend_label} put: {err}"));
}

async fn get_object<E>(executor: &E, dispatch: ObjectDispatch, backend_label: &str) -> Vec<u8>
where
    E: ObjectExecutor + Sync,
{
    assert_eq!(dispatch.op, ObjectOp::GetObject);
    ObjectExecutor::get_object(executor, &object_spec(dispatch).to_string())
        .await
        .unwrap_or_else(|err| panic!("execute compiled {backend_label} get: {err}"))
}

async fn delete_object<E>(executor: &E, dispatch: ObjectDispatch, backend_label: &str)
where
    E: ObjectExecutor + Sync,
{
    assert_eq!(dispatch.op, ObjectOp::DeleteObject);
    ObjectExecutor::delete_object(executor, &object_spec(dispatch).to_string())
        .await
        .unwrap_or_else(|err| panic!("execute compiled {backend_label} delete: {err}"));
}

fn object_spec(dispatch: ObjectDispatch) -> serde_json::Value {
    let ObjectDispatch {
        bucket,
        key,
        content_type,
        ..
    } = dispatch;
    let mut spec = serde_json::json!({
        "bucket": bucket,
        "key": key.clone(),
        "object_key": key,
        "compiler_mediated": true,
    });
    if let Some(content_type) = content_type {
        spec["content_type"] = serde_json::json!(content_type);
    }
    spec
}

fn record<const N: usize>(fields: [(&str, &str); N]) -> LogicalRecord {
    let mut record = BTreeMap::new();
    for (field, value) in fields {
        record.insert(field.to_string(), LogicalValue::String(value.to_string()));
    }
    record
}
