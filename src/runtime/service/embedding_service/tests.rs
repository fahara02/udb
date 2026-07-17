//! Unit guards for the native `EmbeddingService`: the no-credential work payload,
//! request-body cross-tenant rejection, field-violation shapes, the typed
//! setup-capability / schema details, the fail-closed source-tenant-column gate,
//! the tenant-tagged embedding point, the `Retrieve` deadline gate + gRPC-timeout
//! parser, the CDC-envelope change decoders, the reported-vector shape gate, the
//! worker job-loading SQL shapes, and the hit-payload tenant-scope stripping.
//! Copied verbatim from the former god file; imports are explicit (no
//! `use super::*`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::proto::udb::core::embedding::services::v1 as embedding_pb;
use crate::proto::udb::core::embedding::services::v1::embedding_service_server::EmbeddingService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::catalog::CatalogManager;
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

use super::EmbeddingServiceImpl;
use super::config::{EMBEDDING_BACKFILL_PAGE_LIMIT, STATUS_ACTIVE};
use super::errors::{
    embedding_capability_status, embedding_source_not_found_status, require_source_tenant_column,
    validate_reported_vector,
};
use super::events::{bound_embedding_text, build_work_event_payload};
use super::handlers::{parse_grpc_timeout, remaining_before_deadline, retrieve_hit_payload_json};
use super::model::{StoredSource, build_embedding_point, merge_retrieve_filter};
use super::workers::{
    backfill_read_context, backfill_select_request, embedding_teardown_jobs_sql,
    embedding_teardown_point_ids_sql, embedding_work_jobs_sql, extract_source_text,
    parse_source_text_fields, source_change_is_delete, source_change_row, source_change_row_pk,
};

fn decode_detail(status: &Status) -> ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(ERROR_DETAIL_METADATA_KEY)
        .expect("typed detail metadata");
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
    assert_eq!(status.message(), "embedding source not found");
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Schema as i32);
    assert_eq!(detail.backend, "embedding");
    assert_eq!(detail.operation, operation);
    assert_eq!(detail.capability_required, "embedding_source_not_found");
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
}

/// The change/backfill WORK payload carries the row pk + text + non-secret
/// routing, and provably NO credential field — model creds live in the sidecar.
#[test]
fn work_event_payload_has_no_credentials() {
    let payload = build_work_event_payload(
        "acme",
        "contacts",
        "row-1",
        "Ada Lovelace, London",
        "text-embedding-3-small",
        "acme_contacts_vectors",
        8000,
    );
    let object = payload.as_object().expect("object payload");
    // The intended non-secret routing keys are present.
    for key in [
        "tenant_id",
        "source",
        "row_pk",
        "text",
        "model_id",
        "target_collection",
    ] {
        assert!(object.contains_key(key), "missing routing key {key}");
    }
    assert_eq!(object.get("row_pk").and_then(|v| v.as_str()), Some("row-1"));
    assert_eq!(
        object.get("text").and_then(|v| v.as_str()),
        Some("Ada Lovelace, London")
    );
    // No credential-shaped key may EVER appear in a work payload.
    for forbidden in [
        "api_key",
        "apikey",
        "secret",
        "token",
        "password",
        "credential",
        "credentials",
        "authorization",
        "key",
    ] {
        assert!(
            !object.contains_key(forbidden),
            "work payload leaked credential-shaped key {forbidden}"
        );
    }
}

/// Over-long source text is bounded before it enters a work event: within the
/// limit it is unchanged; over the limit it is cut (preferring a whitespace
/// boundary so a word is not split) to at most `max_chars`, char-based so a
/// multi-byte boundary never panics.
#[test]
fn work_event_text_is_bounded() {
    // Within bound ⇒ byte-for-byte unchanged.
    assert_eq!(bound_embedding_text("hello world", 100), "hello world");
    assert_eq!(bound_embedding_text("abcde", 5), "abcde");
    // Over bound with whitespace ⇒ cut at the last word boundary, no mid-word split.
    assert_eq!(
        bound_embedding_text("alpha beta gamma delta", 12),
        "alpha beta"
    );
    // Over bound with no whitespace ⇒ hard cut at exactly max chars.
    let hard = bound_embedding_text("abcdefghijklmno", 5);
    assert_eq!(hard, "abcde");
    assert_eq!(hard.chars().count(), 5);
    // Multi-byte safety: char-based, never panics, stays within the char bound.
    assert!(
        bound_embedding_text("café ☕ ambulance 🚑 dispatch", 8)
            .chars()
            .count()
            <= 8
    );
    assert_eq!(bound_embedding_text("ééééééé", 3).chars().count(), 3);
    // A zero bound yields empty; a bounded work-event text carries the cut value.
    assert_eq!(bound_embedding_text("anything", 0), "");
    let payload = build_work_event_payload("t", "s", "r", "alpha beta gamma delta", "m", "c", 12);
    assert_eq!(
        payload.get("text").and_then(|v| v.as_str()),
        Some("alpha beta")
    );
}

/// A sidecar callback scoped to tenant-a must not report a vector for tenant-b
/// by putting a foreign tenant_id in the request BODY; the scope guard rejects
/// this before any store/vector access (no Postgres needed).
#[tokio::test]
async fn report_embedding_rejects_cross_tenant_body() {
    let svc = EmbeddingServiceImpl::new(); // no runtime/catalog (guard runs first)
    let mut request = Request::new(embedding_pb::ReportEmbeddingRequest {
        tenant_id: "tenant-b".to_string(),
        source_name: "contacts".to_string(),
        row_pk: "row-1".to_string(),
        vector: vec![0.1, 0.2, 0.3],
        model: "m".to_string(),
        dims: 3,
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .report_embedding(request)
        .await
        .expect_err("cross-tenant body must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn register_source_missing_required_fields_carries_field_violations() {
    let svc = EmbeddingServiceImpl::new(); // no runtime/catalog; validation runs first
    let mut request = Request::new(embedding_pb::RegisterSourceRequest {
        tenant_id: "tenant-a".to_string(),
        source_name: " ".to_string(),
        source_message_type: String::new(),
        text_fields: Vec::new(),
        target_collection: "contacts_vec".to_string(),
        model_id: String::new(),
        metadata_json: String::new(),
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

    let err = svc
        .register_source(request)
        .await
        .expect_err("missing source identity must fail");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "source_name and source_message_type are required"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 2);
    assert_eq!(detail.field_violations[0].field, "source_name");
    assert_eq!(
        detail.field_violations[0].description,
        "must be a non-empty embedding source name"
    );
    assert_eq!(detail.field_violations[1].field, "source_message_type");
    assert_eq!(
        detail.field_violations[1].description,
        "must be a non-empty source message type"
    );
}

#[tokio::test]
async fn report_embedding_missing_identity_carries_field_violations() {
    let svc = EmbeddingServiceImpl::new(); // no runtime; validation runs first
    let mut request = Request::new(embedding_pb::ReportEmbeddingRequest {
        tenant_id: "tenant-a".to_string(),
        source_name: String::new(),
        row_pk: " ".to_string(),
        vector: vec![0.1, 0.2],
        model: "m".to_string(),
        dims: 2,
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

    let err = svc
        .report_embedding(request)
        .await
        .expect_err("missing report identity must fail");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "source_name and row_pk are required");
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 2);
    assert_eq!(detail.field_violations[0].field, "source_name");
    assert_eq!(detail.field_violations[1].field, "row_pk");
}

#[tokio::test]
async fn retrieve_missing_query_vector_carries_field_violation() {
    let svc = EmbeddingServiceImpl::new(); // no runtime/catalog; validation runs first
    let mut request = Request::new(embedding_pb::RetrieveRequest {
        tenant_id: "tenant-a".to_string(),
        source_name: "contacts".to_string(),
        query_text: String::new(),
        query_vector: Vec::new(),
        top_k: 10,
        filter_json: String::new(),
        score_threshold: 0.0,
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

    let err = svc
        .retrieve(request)
        .await
        .expect_err("missing query vector must fail");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "query_vector is required (the broker does not embed queries; supply a vector)"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, "query_vector");
    assert_eq!(
        detail.field_violations[0].description,
        "must contain at least one embedding dimension"
    );
}

#[test]
fn source_message_type_missing_from_catalog_carries_field_violation() {
    let catalog = Arc::new(CatalogManager::new(
        crate::generation::CatalogManifest::default(),
    ));
    let svc = EmbeddingServiceImpl::new().with_catalog(Some(catalog));
    let err = svc
        .resolve_source_tenant_column("default", "acme.crm.entity.v1.Contact")
        .expect_err("unknown source message type must fail before store/vector access");
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
fn embedding_missing_setup_capabilities_carry_typed_detail() {
    for (operation, capability, message) in [
        (
            "native_entity_dispatch",
            "runtime_native_entity_dispatch",
            "embedding service requires runtime native-entity dispatch (no runtime configured)",
        ),
        (
            "catalog_lookup",
            "active_catalog",
            "embedding service requires the active catalog (no catalog configured)",
        ),
    ] {
        let err = embedding_capability_status(operation, capability, message);
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(err.message(), message);
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "embedding");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, capability);
        assert!(!detail.retryable);
    }
}

#[test]
fn embedding_source_not_found_statuses_carry_schema_detail() {
    for operation in ["backfill", "report_embedding", "retrieve"] {
        assert_schema_not_found_detail(&embedding_source_not_found_status(operation), operation);
    }
}

/// The reported embedding is tenant-tagged and fails CLOSED on an empty tenant
/// (no fail-open): the stored point carries the `_tenant_id` payload key.
#[test]
fn embedding_point_is_tenant_tagged_no_fail_open() {
    // Empty (unverified) tenant ⇒ fail closed, never stored.
    let err = build_embedding_point("row-1", vec![0.1, 0.2], "  ")
        .expect_err("empty tenant must fail closed");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        err.message(),
        "embedding upsert requires a verified tenant; refusing to store an unscoped vector \
         (no fail-open)"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Policy as i32);
    assert_eq!(detail.operation, "embedding_vector_upsert");
    assert_eq!(detail.policy_decision_id, "verified_tenant_required");
    assert!(!detail.retryable);
    assert_eq!(detail.retry_after_ms, 0);
    // A verified tenant ⇒ the point is tagged with `_tenant_id`.
    let point = build_embedding_point("row-1", vec![0.1, 0.2], "acme").expect("verified tenant ok");
    assert_eq!(point.id, "row-1");
    let payload = point.payload.expect("tenant-tag payload present");
    assert!(
        payload.fields.contains_key("_tenant_id"),
        "stored vector is not tenant-tagged"
    );
}

/// The caller `filter_json` is merged UNDER the mandatory tenant clause: the
/// verified `_tenant_id` `must` condition stays first and authoritative, a caller
/// filter can only narrow, and any attempt to reference an internal `_`-prefixed
/// payload key (a tenant-escape) is rejected fail-closed.
#[test]
fn retrieve_filter_merges_under_authoritative_tenant_clause() {
    // Empty / whitespace filter ⇒ the original tenant-only clause, byte-for-byte.
    let tenant_only =
        serde_json::json!({ "must": [{ "key": "_tenant_id", "match": { "value": "t1" } }] });
    assert_eq!(merge_retrieve_filter("t1", "").unwrap(), tenant_only);
    assert_eq!(merge_retrieve_filter("t1", "   ").unwrap(), tenant_only);

    // A caller `must` condition is appended AFTER the tenant clause (which stays
    // first and is ANDed, so the caller can only narrow scope).
    let merged = merge_retrieve_filter(
        "t1",
        r#"{"must":[{"key":"doc_type","match":{"value":"invoice"}}]}"#,
    )
    .unwrap();
    let must = merged.get("must").unwrap().as_array().unwrap();
    assert_eq!(must.len(), 2);
    assert_eq!(must[0].get("key").unwrap(), "_tenant_id");
    assert_eq!(must[1].get("key").unwrap(), "doc_type");

    // `should` / `must_not` groups are carried through, tenant still forced into `must`.
    let merged = merge_retrieve_filter(
        "t1",
        r#"{"should":[{"key":"tag","match":{"value":"x"}}],"must_not":[{"key":"archived","match":{"value":true}}]}"#,
    )
    .unwrap();
    assert!(merged.get("should").is_some());
    assert!(merged.get("must_not").is_some());
    assert_eq!(
        merged.get("must").unwrap().as_array().unwrap()[0]
            .get("key")
            .unwrap(),
        "_tenant_id"
    );

    // SECURITY: a caller may not reference any internal `_`-prefixed key, even
    // nested, and malformed input fails closed rather than being ignored.
    assert!(
        merge_retrieve_filter(
            "t1",
            r#"{"must":[{"key":"_tenant_id","match":{"value":"OTHER"}}]}"#
        )
        .is_err(),
        "must reject an attempt to override the tenant clause"
    );
    assert!(
        merge_retrieve_filter(
            "t1",
            r#"{"must":[{"key":"_project","match":{"value":"p"}}]}"#
        )
        .is_err()
    );
    assert!(
        merge_retrieve_filter(
            "t1",
            r#"{"must":[{"should":[{"key":"_tenant_id","match":{"value":"x"}}]}]}"#
        )
        .is_err(),
        "must reject a nested internal-key reference"
    );
    assert!(merge_retrieve_filter("t1", "not json").is_err());
    assert!(merge_retrieve_filter("t1", "[1,2,3]").is_err());
    assert!(merge_retrieve_filter("t1", r#"{"whatever":[]}"#).is_err());
    assert!(merge_retrieve_filter("t1", r#"{"must":{}}"#).is_err());
}

/// RegisterSource fails closed when the source entity has no resolvable tenant
/// column (a reported vector / Retrieve would otherwise have no tenant
/// predicate to inject).
#[test]
fn register_source_fails_closed_without_source_tenant_column() {
    let err = require_source_tenant_column(None, "acme.crm.entity.v1.Contact")
        .expect_err("missing tenant column must fail closed");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "source entity 'acme.crm.entity.v1.Contact' has no resolvable tenant column; refusing to register a tenant-scoped embedding source over it (fail closed)"
    );
    assert_single_field_violation(
        &err,
        "source_message_type",
        "must resolve to a tenant-scoped source entity",
    );
    let blank = require_source_tenant_column(Some("  ".to_string()), "X")
        .expect_err("blank column must fail closed");
    assert_eq!(blank.code(), tonic::Code::InvalidArgument);
    assert_single_field_violation(
        &blank,
        "source_message_type",
        "must resolve to a tenant-scoped source entity",
    );
    assert_eq!(
        require_source_tenant_column(Some("tenant_id".to_string()), "X").unwrap(),
        "tenant_id"
    );
}

/// A Retrieve whose gRPC deadline is already past returns `deadline_exceeded`
/// before any semantic-search dispatch; a future deadline yields the remaining
/// budget.
#[test]
fn retrieve_past_deadline_returns_deadline_exceeded() {
    let base = Instant::now();
    // Deadline in the past relative to `now` ⇒ exceeded.
    let now = base + Duration::from_secs(1);
    let err = remaining_before_deadline(Some(base), now)
        .expect_err("past deadline must be deadline_exceeded");
    assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
    // A future deadline ⇒ a positive remaining budget.
    let future = base + Duration::from_secs(30);
    let remaining = remaining_before_deadline(Some(future), base)
        .expect("future deadline ok")
        .expect("some remaining budget");
    assert!(remaining > Duration::from_secs(20));
    // No deadline ⇒ unbounded (None).
    assert!(
        remaining_before_deadline(None, base)
            .expect("no deadline ok")
            .is_none()
    );
}

/// The gRPC timeout header parser handles the standard unit suffixes.
#[test]
fn parses_grpc_timeout_units() {
    assert_eq!(parse_grpc_timeout("5S"), Some(Duration::from_secs(5)));
    assert_eq!(parse_grpc_timeout("250m"), Some(Duration::from_millis(250)));
    assert_eq!(parse_grpc_timeout("1M"), Some(Duration::from_secs(60)));
    assert_eq!(parse_grpc_timeout(""), None);
    assert_eq!(parse_grpc_timeout("abc"), None);
}

#[test]
fn source_change_helpers_accept_cdc_envelope() {
    let payload = serde_json::json!({
        "tenant_id": "tenant-a",
        "project_id": "project-a",
        "operation": "upsert",
        "document_id": "invoice-1",
        "payload": {
            "email": "ada@example.com",
            "status": "OPEN"
        }
    });
    assert_eq!(source_change_row_pk(&payload), "invoice-1");
    assert!(!source_change_is_delete(&payload));
    let row = source_change_row(&payload).expect("source row");
    assert_eq!(
        extract_source_text(row, &["email".to_string(), "status".to_string()]),
        "ada@example.com OPEN"
    );

    let deleted = serde_json::json!({
        "tenant_id": "tenant-a",
        "operation": "delete",
        "document_id": "invoice-1",
        "payload": { "email": "ada@example.com" }
    });
    assert!(source_change_is_delete(&deleted));
}

#[test]
fn source_text_fields_parse_jsonb_and_string_wrapped_json() {
    assert_eq!(
        parse_source_text_fields(r#"["email","status"]"#),
        vec!["email".to_string(), "status".to_string()]
    );
    assert_eq!(
        parse_source_text_fields(r#""[\"email\",\"status\"]""#),
        vec!["email".to_string(), "status".to_string()]
    );
    assert!(parse_source_text_fields("not-json").is_empty());
}

#[test]
fn backfill_select_request_is_tenant_scoped_and_cursor_bounded() {
    let source = StoredSource {
        source_id: "src-1".to_string(),
        tenant_id: "tenant-a".to_string(),
        source_name: "contacts".to_string(),
        source_message_type: "acme.crm.v1.Contact".to_string(),
        text_fields_json: r#"["email","status"]"#.to_string(),
        target_collection: "contacts_vec".to_string(),
        model_id: "model-a".to_string(),
        tenant_column: "tenant_id".to_string(),
        source_cdc_topic: "acme.contacts.cdc".to_string(),
        status: STATUS_ACTIVE.to_string(),
    };
    let request = backfill_select_request(
        &source,
        "contact_id",
        &["email".to_string(), "status".to_string()],
        Some("contact-7"),
        None,
        "",
    );
    assert_eq!(request.message_type, "acme.crm.v1.Contact");
    assert_eq!(request.fields, ["contact_id", "email", "status"]);
    assert_eq!(request.limit, EMBEDDING_BACKFILL_PAGE_LIMIT);
    assert_eq!(request.sort[0].field, "contact_id");
    let filter = request.filter.as_ref().expect("tenant filter");
    let json = crate::runtime::executor_utils::struct_to_json(filter);
    assert_eq!(json["$and"][0]["tenant_id"], "tenant-a");
    assert_eq!(json["$and"][1]["contact_id"]["$gt"], "contact-7");

    // With a project-scoped source, the SELECT MUST carry the project column
    // (else the planner rejects it: "project isolation requires filter on
    // project_id") — the bug that made backfill emit zero work per row.
    let scoped = backfill_select_request(
        &source,
        "contact_id",
        &["email".to_string()],
        None,
        Some("project_id"),
        "proj-9",
    );
    let scoped_json =
        crate::runtime::executor_utils::struct_to_json(scoped.filter.as_ref().unwrap());
    assert_eq!(scoped_json["$and"][0]["tenant_id"], "tenant-a");
    assert_eq!(scoped_json["$and"][1]["project_id"], "proj-9");
}

#[test]
fn backfill_read_context_uses_served_read_gate() {
    let context = backfill_read_context("tenant-a", "project-a");
    assert_eq!(context.tenant_id, "tenant-a");
    assert_eq!(context.project_id, "project-a");
    assert_eq!(context.purpose, "embedding_backfill");
    assert_eq!(context.scopes, vec!["udb:read".to_string()]);
    assert_eq!(context.service_identity, "udb.embedding.backfill");
}

/// Dimension truth (16.6.2 shape gate): an empty vector and a `dims` claim
/// contradicting the actual vector length are both rejected with typed
/// field violations; an unset (non-positive) or matching `dims` passes.
#[test]
fn reported_vector_shape_gate_rejects_empty_and_mismatched_dims() {
    let err = validate_reported_vector(0, &[]).expect_err("empty vector must fail");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(err.message(), "vector is required");
    assert_single_field_violation(
        &err,
        "vector",
        "must contain at least one embedding dimension",
    );

    let err = validate_reported_vector(5, &[0.1, 0.2, 0.3]).expect_err("dims mismatch must fail");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "dims (5) does not match the reported vector length (3)"
    );
    assert_single_field_violation(
        &err,
        "dims",
        "must equal the reported vector length when set",
    );

    validate_reported_vector(0, &[0.1]).expect("unset dims must pass");
    validate_reported_vector(-1, &[0.1]).expect("negative dims treated as unset");
    validate_reported_vector(3, &[0.1, 0.2, 0.3]).expect("matching dims must pass");
}

/// The RPC surface enforces the shape gate before admission/store/vector
/// access: a mismatched `dims` is refused on a bare service (no runtime).
#[tokio::test]
async fn report_embedding_dims_mismatch_carries_field_violation() {
    let svc = EmbeddingServiceImpl::new(); // no runtime/catalog; validation runs first
    let mut request = Request::new(embedding_pb::ReportEmbeddingRequest {
        tenant_id: "tenant-a".to_string(),
        source_name: "contacts".to_string(),
        row_pk: "row-1".to_string(),
        vector: vec![0.1, 0.2, 0.3],
        model: "m".to_string(),
        dims: 5,
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .report_embedding(request)
        .await
        .expect_err("dims mismatch must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert_single_field_violation(
        &err,
        "dims",
        "must equal the reported vector length when set",
    );
}

/// Loader parity (16.6.3): the change-driven work loader reads every journal
/// payload key with the SAME `COALESCE(top-level, payload->'payload'->>…)`
/// nesting fallback the backfill loader uses — the CDC journal ENVELOPE-nests
/// emitted payloads, so a top-level-only read matches zero real rows.
#[test]
fn work_loader_sql_reads_nested_journal_envelope() {
    let sql = embedding_work_jobs_sql("udb_cdc_event_journal", "udb_outbox");
    for fragment in [
        "COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '')",
        "COALESCE(j.payload->>'project_id', j.payload->'payload'->>'project_id', '')",
        "COALESCE(o.payload->>'source_event_id', o.payload->'payload'->>'source_event_id')",
        "COALESCE(done.payload->>'source_event_id', done.payload->'payload'->>'source_event_id')",
    ] {
        assert!(
            sql.contains(fragment),
            "work loader SQL lost the nested-envelope fallback: missing {fragment}"
        );
    }
    // No key may still be read top-level-only.
    assert!(
        !sql.contains("COALESCE(j.payload->>'tenant_id', '')"),
        "work loader SQL still reads tenant_id top-level only"
    );
}

/// Source teardown reads its journal keys with the same nested-envelope
/// fallback, and dedups by `teardown_event_id` in both outbox and journal.
#[test]
fn teardown_sqls_read_nested_journal_envelope() {
    let jobs_sql = embedding_teardown_jobs_sql("udb_cdc_event_journal", "udb_outbox");
    for fragment in [
        "COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '')",
        "COALESCE(j.payload->>'source', j.payload->'payload'->>'source', '')",
        "COALESCE(j.payload->>'target_collection', j.payload->'payload'->>'target_collection', '')",
        "COALESCE(o.payload->>'teardown_event_id', o.payload->'payload'->>'teardown_event_id')",
        "COALESCE(done.payload->>'teardown_event_id', done.payload->'payload'->>'teardown_event_id')",
    ] {
        assert!(
            jobs_sql.contains(fragment),
            "teardown loader SQL missing nested-envelope fallback: {fragment}"
        );
    }
    let ids_sql = embedding_teardown_point_ids_sql("udb_cdc_event_journal", "udb_outbox");
    for fragment in [
        "COALESCE(j.payload->>'row_pk', j.payload->'payload'->>'row_pk', '')",
        "COALESCE(o.payload->>'row_pk', o.payload->'payload'->>'row_pk', '')",
        "COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '')",
        "COALESCE(o.payload->>'source', o.payload->'payload'->>'source', '')",
    ] {
        assert!(
            ids_sql.contains(fragment),
            "teardown point-id SQL missing nested-envelope fallback: {fragment}"
        );
    }
    // Bounded, keyset-paginated enumeration — never an unbounded scan.
    assert!(ids_sql.contains("LIMIT $6"));
    assert!(ids_sql.contains("(ids.target_collection, ids.row_pk) > ($4, $5)"));
}

/// Retrieve hits carry the stored user payload with the internal
/// `_tenant_id` write-stamp stripped; tag-only or absent payloads stay "".
#[test]
fn retrieve_hit_payload_strips_internal_tenant_tag() {
    let payload = crate::runtime::executor_utils::json_to_struct(&serde_json::json!({
        "_tenant_id": "acme",
        "title": "Q3 report",
        "chunk": 4,
    }))
    .expect("struct payload");
    let json = retrieve_hit_payload_json(Some(&payload));
    let value: serde_json::Value = serde_json::from_str(&json).expect("payload_json is valid JSON");
    assert_eq!(value["title"], "Q3 report");
    assert_eq!(value["chunk"], 4.0);
    assert!(
        value.get("_tenant_id").is_none(),
        "internal tenant tag leaked into RetrieveHit payload_json"
    );

    // A payload that is ONLY the internal tag serializes to the empty
    // sentinel, and no payload stays empty too.
    let tag_only = crate::runtime::executor_utils::json_to_struct(&serde_json::json!({
        "_tenant_id": "acme",
    }))
    .expect("struct payload");
    assert_eq!(retrieve_hit_payload_json(Some(&tag_only)), "");
    assert_eq!(retrieve_hit_payload_json(None), "");
}
