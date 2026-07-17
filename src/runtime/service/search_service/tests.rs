//! Unit guards for the native `SearchService`: request-body cross-tenant
//! rejection, field-violation shapes, the typed setup-capability details, the
//! fail-closed source-tenant-column gate, the CDC freshness scope filter, the
//! pure RRF scores, the hit-payload tenant-scope stripping, the never-strand
//! reindex writeback, the worker job-loading SQL shapes, the envelope-nested
//! freshness body reader, and the tenant-scoped source enumeration. Copied
//! verbatim from the former god file; imports are explicit (no `use super::*`).

use std::sync::Arc;

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::proto::udb::core::search::services::v1 as search_pb;
use crate::proto::udb::core::search::services::v1::search_service_server::SearchService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::catalog::CatalogManager;
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

use super::SearchServiceImpl;
use super::config::{
    BACKEND_QDRANT, SEARCH_REINDEX_PAGE_LIMIT, STATUS_ACTIVE, STATUS_REINDEXING,
    TENANT_SCOPE_PAYLOAD_KEY,
};
use super::errors::{
    full_text_only_requires_mediated_ir_status, require_source_tenant_column,
    search_capability_status, search_index_not_found_status, validate_search_query,
};
use super::fusion::reciprocal_rank_fusion;
use super::handlers::hit_payload_json;
use super::model::StoredIndex;
use super::store::search_index_model;
use super::workers::{
    event_in_index_scope, freshness_event_body, freshness_jobs_sql, payload_vector,
    reindex_jobs_sql, reindex_writeback, source_rows_select_request, teardown_jobs_sql,
};

fn decode_detail(status: &Status) -> ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(ERROR_DETAIL_METADATA_KEY)
        .expect("error-detail trailer present")
        .to_bytes()
        .expect("trailer decodes to bytes");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
}

fn assert_single_field_violation(status: &Status, field: &str, description: &str) {
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, field);
    assert_eq!(detail.field_violations[0].description, description);
}

fn assert_schema_not_found_detail(status: &Status, operation: &str) {
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert_eq!(status.message(), "search index not found");
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Schema as i32);
    assert_eq!(detail.backend, "search");
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.capability_required, "search_index_not_found");
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

/// A caller scoped to tenant-a must not register an index under tenant-b by
/// putting a foreign tenant_id in the request BODY; the scope guard rejects
/// this before any catalog/store access (no Postgres needed).
#[tokio::test]
async fn create_index_rejects_cross_tenant_body() {
    let svc = SearchServiceImpl::new(); // no runtime/catalog (guard runs first)
    let mut request = Request::new(search_pb::CreateIndexRequest {
        tenant_id: "tenant-b".to_string(),
        index_name: "contacts".to_string(),
        source_message_type: "acme.crm.entity.v1.Contact".to_string(),
        backend: "qdrant".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .create_index(request)
        .await
        .expect_err("cross-tenant body must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn create_index_missing_required_fields_carries_field_violations() {
    let svc = SearchServiceImpl::new(); // no runtime/catalog; validation must fire first
    let mut request = Request::new(search_pb::CreateIndexRequest {
        tenant_id: "tenant-a".to_string(),
        index_name: "  ".to_string(),
        source_message_type: String::new(),
        backend: "qdrant".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .create_index(request)
        .await
        .expect_err("missing index fields must be rejected before runtime/catalog access");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "index_name and source_message_type are required"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 2);
    assert_eq!(detail.field_violations[0].field, "index_name");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty search index name"
    );
    assert_eq!(detail.field_violations[1].field, "source_message_type");
    assert_eq!(
        detail.field_violations[1].description,
        "must be a non-empty source message type"
    );
}

#[tokio::test]
async fn create_index_unsupported_backend_carries_field_violation() {
    let svc = SearchServiceImpl::new(); // no runtime/catalog; backend validation fires first
    let mut request = Request::new(search_pb::CreateIndexRequest {
        tenant_id: "tenant-a".to_string(),
        index_name: "contacts".to_string(),
        source_message_type: "acme.crm.entity.v1.Contact".to_string(),
        backend: "memory".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .create_index(request)
        .await
        .expect_err("unsupported backend must be rejected before runtime/catalog access");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "unsupported search backend 'memory' (expected 'qdrant' or 'elasticsearch')"
    );
    assert_single_field_violation(&err, "backend", "must be 'qdrant' or 'elasticsearch'");
}

#[tokio::test]
async fn delete_index_missing_index_name_carries_field_violation() {
    let svc = SearchServiceImpl::new(); // no runtime, no channels (admit no-op)
    let mut request = Request::new(search_pb::DeleteIndexRequest {
        tenant_id: "tenant-a".to_string(),
        index_name: String::new(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .delete_index(request)
        .await
        .expect_err("missing index_name must be rejected before runtime access");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "index_name is required");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "index_name");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty search index name"
    );
}

#[test]
fn source_message_type_missing_from_catalog_carries_field_violation() {
    let catalog = Arc::new(CatalogManager::new(
        crate::generation::CatalogManifest::default(),
    ));
    let svc = SearchServiceImpl::new().with_catalog(Some(catalog));
    let err = svc
        .resolve_source_tenant_column("default", "acme.crm.entity.v1.Contact")
        .expect_err("unknown source message type must fail before store access");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "source_message_type 'acme.crm.entity.v1.Contact' is not present in the active catalog manifest"
    );
    assert_single_field_violation(
        &err,
        "source_message_type",
        "must be present in the active catalog manifest",
    );
}

#[test]
fn search_missing_setup_capabilities_carry_typed_detail() {
    for (operation, capability, message) in [
        (
            "native_entity_dispatch",
            "runtime_native_entity_dispatch",
            "search service requires runtime native-entity dispatch (no runtime configured)",
        ),
        (
            "catalog_lookup",
            "active_catalog",
            "search service requires the active catalog (no catalog configured)",
        ),
    ] {
        let err = search_capability_status(operation, capability, message);
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(err.message(), message);
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "search");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, capability);
        assert!(!detail.retryable);
    }
}

#[test]
fn full_text_only_search_requires_typed_capability_detail() {
    let err = full_text_only_requires_mediated_ir_status();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        err.message(),
        "full-text-only search requires the mediated IR full-text path (P2.2, pending); \
         supply query_vector for vector or hybrid search"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Capability as i32);
    assert_eq!(detail.backend, "search");
    assert_eq!(detail.operation, "full_text_only_search");
    assert_eq!(detail.capability_required, "mediated_ir_full_text_path");
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

#[test]
fn search_index_not_found_status_carries_schema_detail() {
    assert_schema_not_found_detail(&search_index_not_found_status("reindex"), "reindex");
}

#[test]
fn empty_search_query_carries_field_violations() {
    let err = validate_search_query(&search_pb::SearchRequest {
        query_text: " ".to_string(),
        query_vector: Vec::new(),
        ..Default::default()
    })
    .expect_err("empty search query must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "search requires query_text and/or query_vector"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 2);
    assert_eq!(detail.field_violations[0].field, "query_text");
    assert_eq!(
        detail.field_violations[0].description,
        "must be non-empty when query_vector is empty"
    );
    assert_eq!(detail.field_violations[1].field, "query_vector");
    assert_eq!(
        detail.field_violations[1].description,
        "must be non-empty when query_text is empty"
    );
}

/// CreateIndex fails closed when the source entity has no resolvable tenant
/// column (a Search would otherwise have no tenant predicate to inject).
#[test]
fn create_index_fails_closed_without_source_tenant_column() {
    let err = require_source_tenant_column(None, "acme.crm.entity.v1.Contact")
        .expect_err("missing tenant column must fail closed");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "source entity 'acme.crm.entity.v1.Contact' has no resolvable tenant column; refusing to register a tenant-scoped search index over it (fail closed)"
    );
    assert_single_field_violation(
        &err,
        "source_message_type",
        "must resolve to a tenant-scoped source entity",
    );
    // A blank/whitespace column is treated as unresolved (still fail closed).
    let blank = require_source_tenant_column(Some("  ".to_string()), "X")
        .expect_err("blank column must fail closed");
    assert_eq!(blank.code(), tonic::Code::InvalidArgument);
    assert_single_field_violation(
        &blank,
        "source_message_type",
        "must resolve to a tenant-scoped source entity",
    );
    // A real column resolves.
    assert_eq!(
        require_source_tenant_column(Some("tenant_id".to_string()), "X").unwrap(),
        "tenant_id"
    );
}

/// The CDC freshness filter drops a tenant-less event and a foreign-tenant
/// event, and keeps only a matching-tenant event (fail closed).
#[test]
fn freshness_filter_drops_tenantless_and_foreign_events() {
    let topic = "udb.search.search_indexes.cdc";
    // Tenant-less payload → dropped.
    let tenantless = serde_json::json!({ "id": "row-1" });
    assert!(!event_in_index_scope(topic, &tenantless, "acme"));
    // Foreign tenant → dropped.
    let foreign = serde_json::json!({ "id": "row-1", "tenant_id": "other" });
    assert!(!event_in_index_scope(topic, &foreign, "acme"));
    // Matching tenant → kept.
    let matching = serde_json::json!({ "id": "row-1", "tenant_id": "acme" });
    assert!(event_in_index_scope(topic, &matching, "acme"));
    // Empty index scope can match nothing.
    assert!(!event_in_index_scope(topic, &matching, ""));
}

/// Pure RRF: known ranks → known fused scores. With list_one = [a,b,c] and
/// list_two = [b,a]: doc "a" is rank 0 then rank 1 (1/60 + 1/61); doc "b" is
/// rank 1 then rank 0 (1/61 + 1/60) — so a and b are EXACTLY tied, broken by
/// id ascending → a before b; doc "c" appears once at rank 2 (1/62). Order:
/// a, b, c.
#[test]
fn reciprocal_rank_fusion_known_scores() {
    let list_one = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let list_two = vec!["b".to_string(), "a".to_string()];
    let fused = reciprocal_rank_fusion(&[list_one, list_two]);

    let score_of = |id: &str| {
        fused
            .iter()
            .find(|(d, _)| d == id)
            .map(|(_, s)| *s)
            .unwrap()
    };
    let expect_a = 1.0 / 60.0 + 1.0 / 61.0;
    let expect_b = 1.0 / 61.0 + 1.0 / 60.0;
    let expect_c = 1.0 / 62.0;
    assert!((score_of("a") - expect_a).abs() < 1e-12);
    assert!((score_of("b") - expect_b).abs() < 1e-12);
    assert!((score_of("c") - expect_c).abs() < 1e-12);
    // a and b are tied on score; the id-ascending tie-break makes order
    // deterministic. c is strictly lowest.
    let order: Vec<&str> = fused.iter().map(|(d, _)| d.as_str()).collect();
    assert_eq!(order, vec!["a", "b", "c"]);
}

/// Hit payload mapping (16.2.4): the broker-stamped `_tenant_id` scope key
/// is stripped from returned payloads; application fields survive; an
/// absent or tenant-only payload maps to an empty `payload_json`.
#[test]
fn hit_payload_json_strips_tenant_scope_key() {
    let payload = crate::runtime::executor_utils::json_to_struct(&serde_json::json!({
        TENANT_SCOPE_PAYLOAD_KEY: "acme",
        "title": "hello",
    }))
    .expect("payload struct");
    let json = hit_payload_json(Some(&payload));
    let value: serde_json::Value = serde_json::from_str(&json).expect("payload_json decodes");
    assert!(
        value.get(TENANT_SCOPE_PAYLOAD_KEY).is_none(),
        "the tenant scope key must never leak into hit payloads"
    );
    assert_eq!(value.get("title").and_then(|v| v.as_str()), Some("hello"));
    // No payload → empty string.
    assert_eq!(hit_payload_json(None), "");
    // A tenant-only payload has nothing left to return.
    let tenant_only = crate::runtime::executor_utils::json_to_struct(&serde_json::json!({
        TENANT_SCOPE_PAYLOAD_KEY: "acme",
    }))
    .expect("tenant-only struct");
    assert_eq!(hit_payload_json(Some(&tenant_only)), "");
}

/// The reindex failure path NEVER strands an index in REINDEXING: both
/// outcomes restore ACTIVE, and a failure records `last_error` in the
/// mutable metadata column.
#[test]
fn reindex_writeback_never_strands_reindexing() {
    let (ok_status, ok_metadata) = reindex_writeback(None);
    assert_eq!(ok_status, STATUS_ACTIVE);
    assert_ne!(ok_status, STATUS_REINDEXING);
    assert_eq!(ok_metadata, "{}");
    let (err_status, err_metadata) = reindex_writeback(Some("qdrant unreachable"));
    assert_eq!(err_status, STATUS_ACTIVE);
    assert_ne!(err_status, STATUS_REINDEXING);
    let metadata: serde_json::Value =
        serde_json::from_str(&err_metadata).expect("failure metadata decodes");
    assert_eq!(
        metadata.get("last_error").and_then(|v| v.as_str()),
        Some("qdrant unreachable")
    );
}

/// Job-loading SQL shape (mirrors the metering SQL-shape tests): journal
/// join, BOTH payload envelope levels (the CDC journal envelope-nesting
/// lesson), the outbox+journal completion dedup pair, deterministic order,
/// and a bound LIMIT.
#[test]
fn worker_job_loading_sql_shape() {
    let model = search_index_model();
    let journal = "udb_system.cdc_event_journal";
    let outbox = "udb_system.outbox_events";

    let reindex = reindex_jobs_sql(&model, journal, outbox);
    assert!(reindex.contains(&format!("FROM {journal} j")));
    assert!(reindex.contains("search_indexes"));
    assert!(reindex.contains("j.payload->'payload'->>'index_name'"));
    assert!(reindex.contains("j.payload->'payload'->>'tenant_id'"));
    assert!(reindex.contains("reindex_event_id"));
    assert_eq!(
        reindex.matches("NOT EXISTS").count(),
        2,
        "reindex dedup must check the completion marker in BOTH outbox and journal"
    );
    assert!(reindex.contains("ORDER BY j.published_at ASC, j.event_id ASC"));
    assert!(reindex.contains("LIMIT $4"));

    let teardown = teardown_jobs_sql(&model, journal, outbox);
    assert!(teardown.contains("teardown_event_id"));
    assert_eq!(teardown.matches("NOT EXISTS").count(), 2);
    assert!(teardown.contains("LIMIT $4"));

    let freshness = freshness_jobs_sql(&model, journal, outbox);
    // Only embedding-carrying events are candidates (no journal clog from
    // vector-less source changes), at EITHER envelope level.
    assert!(freshness.contains("jsonb_typeof(j.payload->'vector') = 'array'"));
    assert!(freshness.contains("jsonb_typeof(j.payload->'payload'->'vector') = 'array'"));
    assert!(freshness.contains("source_event_id"));
    assert_eq!(freshness.matches("NOT EXISTS").count(), 2);
    assert!(freshness.contains("LIMIT $3"));
}

/// The CDC journal may envelope-nest the emitted payload under
/// `payload.payload`; the freshness body reader must fall through to the
/// nested level (and the vector extractor accepts `vector`/`embedding`).
#[test]
fn freshness_event_body_reads_nested_journal_envelope() {
    let nested = serde_json::json!({
        "topic": "acme.crm.contacts.cdc",
        "payload": { "id": "row-1", "tenant_id": "acme", "vector": [0.25, 0.5] }
    });
    let body = freshness_event_body(&nested);
    assert_eq!(body.get("id").and_then(|v| v.as_str()), Some("row-1"));
    assert_eq!(payload_vector(body), Some(vec![0.25, 0.5]));
    let flat = serde_json::json!({ "id": "row-2", "vector": [1.0] });
    assert_eq!(
        freshness_event_body(&flat)
            .get("id")
            .and_then(|v| v.as_str()),
        Some("row-2")
    );
    // Source rows may carry the embedding under `embedding` instead.
    assert_eq!(
        payload_vector(&serde_json::json!({ "embedding": [0.5, 1.0] })),
        Some(vec![0.5, 1.0])
    );
    // An empty vector is no vector (never upsert a zero-length embedding).
    assert_eq!(payload_vector(&serde_json::json!({ "vector": [] })), None);
}

/// The reindex/teardown source enumeration is tenant-scoped via the STORED
/// source tenant column, project-scoped when the table requires it, and
/// cursor-paged on the primary key (mirrors the embedding backfill's
/// select-request test).
#[test]
fn source_rows_select_request_is_tenant_scoped_and_paged() {
    let index = StoredIndex {
        index_id: "idx-1".to_string(),
        index_name: "contacts".to_string(),
        source_message_type: "acme.crm.entity.v1.Contact".to_string(),
        backend: BACKEND_QDRANT.to_string(),
        resource_name: String::new(),
        vector_dims: 3,
        tenant_column: "tenant_id".to_string(),
        source_cdc_topic: "acme.crm.contacts.cdc".to_string(),
        status: STATUS_REINDEXING.to_string(),
    };
    let request = source_rows_select_request(
        &index,
        "acme",
        "contact_id",
        Vec::new(),
        Some("after-1"),
        Some("project_id"),
        "proj-1",
    );
    assert_eq!(request.message_type, "acme.crm.entity.v1.Contact");
    assert_eq!(request.limit, SEARCH_REINDEX_PAGE_LIMIT);
    assert_eq!(request.sort.len(), 1);
    assert_eq!(request.sort[0].field, "contact_id");
    assert!(!request.sort[0].descending);
    let filter = crate::runtime::executor_utils::struct_to_json(
        request.filter.as_ref().expect("filter present"),
    );
    let ands = filter
        .get("$and")
        .and_then(|v| v.as_array())
        .expect("$and filter");
    assert!(
        ands.iter()
            .any(|f| f.get("tenant_id").and_then(|v| v.as_str()) == Some("acme")),
        "the stored source tenant column must scope the read"
    );
    assert!(
        ands.iter()
            .any(|f| f.get("project_id").and_then(|v| v.as_str()) == Some("proj-1")),
        "a project-scoped source table must carry the project predicate"
    );
    assert!(
        ands.iter().any(|f| {
            f.get("contact_id")
                .and_then(|c| c.get("$gt"))
                .and_then(|v| v.as_str())
                == Some("after-1")
        }),
        "pagination must cursor on the primary key"
    );
}
