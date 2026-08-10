#!/usr/bin/env python3
"""Guard the UDB-native typed error-detail contract.

Chapter 14.7 originally described a future google.rpc Status.details shape. The
current shipped contract is UDB-native: the broker attaches a prost-encoded
udb.entity.v1.ErrorDetail under the binary trailer key udb-error-detail-bin,
and SDKs decode that same proto. This guard pins that actual contract so docs,
runtime helpers, and SDK mappings cannot drift independently.
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
_READ_CACHE: dict[Path, str] | None = None


@dataclass(frozen=True)
class TokenCheck:
    label: str
    path: str
    tokens: tuple[str, ...]


LEGACY_INLINE_DETAIL_DECODE_TOKENS = frozenset(
    {
        'decode(raw.as_ref()).expect("typed detail decodes")',
        'ErrorDetail::decode(raw.as_ref()).expect("typed detail decodes")',
        "ErrorDetail::decode(raw.as_ref())",
    }
)


TOKEN_CHECKS: tuple[TokenCheck, ...] = (
    TokenCheck(
        "canonical ErrorDetail proto",
        "proto/udb/entity/v1/error.proto",
        (
            "package udb.entity.v1;",
            "under the binary trailer metadata key `udb-error-detail-bin`",
            "enum ErrorKind",
            "ERROR_KIND_CAPABILITY",
            "ERROR_KIND_POLICY",
            "ERROR_KIND_QUOTA",
            "ERROR_KIND_SCHEMA",
            "ERROR_KIND_RETRYABLE",
            "ERROR_KIND_VALIDATION",
            "message ErrorFieldViolation",
            "string field = 1;",
            "string description = 2;",
            "message ErrorDetail",
            "string backend = 1;",
            "string operation = 2;",
            "string capability_required = 3;",
            "bool retryable = 4;",
            "int64 retry_after_ms = 5;",
            "ErrorKind kind = 8;",
            "repeated ErrorFieldViolation field_violations = 9;",
        ),
    ),
    TokenCheck(
        "runtime ErrorDetail builder and unit decoders",
        "src/runtime/executor_utils.rs",
        (
            'pub(crate) const ERROR_DETAIL_METADATA_KEY: &str = "udb-error-detail-bin";',
            "const MAX_ERROR_DETAIL_STRING_BYTES: usize = 8 * 1024;",
            "fn status_with_error_detail(",
            "messages are preserved, while malformed public text is bounded",
            'let message = bounded_error_detail_string(message.into(), "error");',
            "let detail = sanitized_error_detail(detail);",
            "tonic::Status::with_metadata(code, message, metadata)",
            "fn non_negative_retry_after_ms(retry_after_ms: i64) -> i64",
            "fn bounded_error_detail_string(value: String, fallback: &str) -> String",
            "fn sanitized_error_detail(mut detail: crate::proto::ErrorDetail) -> crate::proto::ErrorDetail",
            "detail.retry_after_ms = non_negative_retry_after_ms(detail.retry_after_ms);",
            "if !detail.retryable",
            "detail.retry_after_ms = 0;",
            "if detail.kind == crate::proto::ErrorKind::Validation as i32",
            "detail.backend.clear();",
            "detail.operation.clear();",
            "detail.capability_required.clear();",
            "detail.retryable = false;",
            "detail.retry_after_ms = 0;",
            "if detail.field_violations.is_empty()",
            'field: "field".to_string()',
            'description: "invalid field".to_string()',
            "&& detail.kind != crate::proto::ErrorKind::Quota as i32",
            "&& detail.kind != crate::proto::ErrorKind::Retryable as i32",
            "if detail.kind != crate::proto::ErrorKind::Validation as i32",
            "detail.field_violations.clear();",
            "if detail.kind != crate::proto::ErrorKind::Policy as i32",
            "detail.policy_decision_id.clear();",
            "if detail.kind != crate::proto::ErrorKind::Capability as i32",
            "&& detail.kind != crate::proto::ErrorKind::Schema as i32",
            "detail.capability_required.clear();",
            "retry_after_ms: non_negative_retry_after_ms(retry_after_ms)",
            "fn bounded_error_detail_field_path(",
            "field.chars().any(char::is_whitespace)",
            "fn bounded_error_detail_token(",
            "split_whitespace().collect::<Vec<_>>().join(\"_\")",
            'bounded_error_detail_token(std::mem::take(&mut detail.backend), "backend")',
            'bounded_error_detail_token(std::mem::take(&mut detail.operation), "operation")',
            "pub(crate) fn capability_status(",
            "kind: crate::proto::ErrorKind::Capability as i32",
            "pub(crate) fn policy_status(",
            "pub(crate) fn policy_status_with_code(",
            "kind: crate::proto::ErrorKind::Policy as i32",
            "pub(crate) fn retryable_status(",
            "pub(crate) fn backend_transport_status(",
            "backend_transport_status_is_typed_retryable",
            "pub(crate) fn deadline_exceeded_status(",
            "status_with_error_detail(tonic::Code::DeadlineExceeded, message, detail)",
            "deadline_exceeded_status_preserves_deadline_code_with_retry_detail",
            "kind: crate::proto::ErrorKind::Retryable as i32",
            "pub(crate) fn retryable_aborted_status(",
            "status_with_error_detail(tonic::Code::Aborted, message, detail)",
            "pub(crate) fn quota_status(",
            "kind: crate::proto::ErrorKind::Quota as i32",
            "pub(crate) fn quota_refusal_status(",
            "retryable: false",
            "pub(crate) fn schema_status(",
            "status_with_error_detail(code, message, detail)",
            "pub(crate) fn internal_status(",
            "status_with_error_detail(tonic::Code::Internal, message, detail)",
            "internal_status_carries_internal_kind_with_identity",
            "sqlx_transient_transport_error_maps_to_retryable_unavailable",
            "sqlx_non_transient_non_database_error_preserves_internal_code_with_detail",
            "untagged_store_string_preserves_internal_code_with_detail",
            "fn referential_constraint_status() -> tonic::Status",
            '"database"',
            '"referential_constraint"',
            '"foreign_key_violation"',
            "referential_constraint_status_carries_schema_detail",
            '"unique_constraint"',
            '"unique_violation"',
            "unique_constraint_status_preserves_already_exists_with_schema_detail",
            "pub(crate) fn invalid_argument_fields",
            "pub(crate) fn failed_precondition_fields",
            "kind: crate::proto::ErrorKind::Validation as i32",
            "crate::proto::ErrorFieldViolation",
            "fn tagged_status_to_typed_status(",
            "tagged_store_invalid_argument_preserves_validation_detail",
            "tagged_store_referential_constraint_preserves_schema_detail",
            "tagged_store_already_exists_preserves_schema_detail",
            "tagged_store_unavailable_preserves_retryable_detail",
            "pub(crate) fn prefix_status(",
            "tonic::Status::with_details_and_metadata(",
            "bytes::Bytes::copy_from_slice(status.details())",
            "status.metadata().clone()",
            "prefix_status_preserves_typed_error_detail_metadata",
            "pub(crate) fn compile_error_status(",
            "kind: crate::proto::ErrorKind::Schema as i32",
            "fn decode_detail(status: &tonic::Status) -> ErrorDetail",
            "capability_status_carries_typed_detail_and_preserves_message",
            "policy_status_carries_typed_detail_and_preserves_message",
            "policy_status_with_code_preserves_permission_denied_code",
            "retryable_status_is_unavailable_with_backoff",
            "backend_transport_status_is_typed_retryable",
            "retryable_aborted_status_preserves_aborted_code",
            "error_detail_builder_clears_retryable_field_violations",
            "quota_status_is_resource_exhausted_with_backoff",
            "error_detail_builder_clears_quota_field_violations",
            "assert!(detail.field_violations.is_empty());",
            "error_detail_builder_clears_non_validation_field_violations",
            "assert_eq!(capability_detail.kind, ErrorKind::Capability as i32);",
            "assert_eq!(policy_detail.kind, ErrorKind::Policy as i32);",
            "assert_eq!(schema_detail.kind, ErrorKind::Schema as i32);",
            "ErrorKind::Internal as i32",
            "ErrorKind::Unspecified as i32",
            "assert_eq!(detail.kind, kind);",
            "error_detail_builder_canonicalizes_non_retryable_error_kinds",
            "assert!(!detail.retryable);",
            "assert_eq!(detail.retry_after_ms, 0);",
            "retryable_details_never_expose_negative_backoff",
            "error_detail_builder_never_exposes_negative_backoff",
            "error_detail_builder_canonicalizes_retryable_identity_tokens",
            'assert_eq!(detail.backend, "fair_admission");',
            'assert_eq!(detail.operation, "worker_queue");',
            "error_detail_builder_clears_non_retryable_backoff",
            "error_detail_builder_clears_non_policy_decision_ids",
            "error_detail_builder_clears_non_capability_required_fields",
            "error_detail_builder_canonicalizes_validation_retry_shape",
            "assert!(detail.backend.is_empty());",
            "assert!(detail.operation.is_empty());",
            "assert_eq!(fallback_detail.field_violations.len(), 1);",
            'assert_eq!(fallback_detail.field_violations[0].field, "field");',
            "error_detail_public_strings_are_bounded_and_control_free",
            "let long_message = \"m\".repeat(super::MAX_ERROR_DETAIL_STRING_BYTES + 8);",
            "let long_description = \"d\".repeat(super::MAX_ERROR_DETAIL_STRING_BYTES + 8);",
            "assert_eq!(status.message().len(), super::MAX_ERROR_DETAIL_STRING_BYTES);",
            "assert!(status.message().chars().all(|ch| !ch.is_control()));",
            'assert_eq!(fallback_status.message(), "error");',
            "field_detail.field_violations[4].description.len()",
            "quota_refusal_status_is_resource_exhausted_without_retry",
            "reject_oversized_object_carries_typed_quota_detail",
            "invalid_argument_fields_carries_structured_field_violations",
            "failed_precondition_fields_carries_validation_detail_without_changing_code",
            "compile_error_status_carries_code_as_schema_kind",
            "schema_status_carries_schema_kind_without_changing_code",
        ),
    ),
    TokenCheck(
        "Qdrant transient transport failures use typed retryable detail",
        "src/runtime/executors/qdrant.rs",
        (
            "backend_transport_status",
            'backend_transport_status("Qdrant", "collection check", err)',
            'backend_transport_status("Qdrant", "collection create", err)',
            "status.is_server_error()",
            'retryable_status(',
            'backend_transport_status("Qdrant", "search", err)',
            'backend_transport_status("Qdrant", "upsert", err)',
            'backend_transport_status("Qdrant", "delete", err)',
            'backend_transport_status("Qdrant", "filtered delete", err)',
            'backend_transport_status("Qdrant", "payload patch", err)',
            'backend_transport_status("Qdrant", "response decode", err)',
            'backend_transport_status("qdrant", "delete", e)',
            'backend_transport_status("qdrant", "list", e)',
            'backend_transport_status("qdrant", "list parse", e)',
        ),
    ),
    TokenCheck(
        "Qdrant provider request rejections use typed schema detail",
        "src/runtime/executor_utils.rs",
        (
            "qdrant_status",
            "schema_status(",
            "tonic::Code::FailedPrecondition",
            '"qdrant_http_rejected"',
            "qdrant_client_http_rejections_carry_schema_detail",
        ),
    ),
    TokenCheck(
        "Qdrant collection create provider rejections use typed schema detail",
        "src/runtime/executors/qdrant.rs",
        (
            "schema_status(",
            "tonic::Code::FailedPrecondition",
            '"Qdrant"',
            '"collection create"',
            '"qdrant_collection_create_rejected"',
        ),
    ),
    TokenCheck(
        "Qdrant collection miss uses typed schema detail",
        "src/runtime/executors/qdrant.rs",
        (
            "fn qdrant_collection_not_found_status(",
            "schema_status(",
            "tonic::Code::NotFound",
            '"qdrant"',
            '"collection_check"',
            '"qdrant_collection_not_found"',
            '"Qdrant collection {collection} is missing"',
            "return Err(qdrant_collection_not_found_status(collection));",
            "qdrant_collection_not_found_carries_schema_detail",
            "assert_schema_detail(",
            "ErrorKind::Schema",
        ),
    ),
    TokenCheck(
        "Qdrant executor internals use typed internal detail",
        "src/runtime/executors/qdrant.rs",
        (
            "fn qdrant_internal_status(",
            'crate::runtime::executor_utils::internal_status("qdrant", operation, message)',
            '"search_result_encode"',
            '"drop_resource"',
            '"qdrant drop_resource status: {}"',
            "qdrant_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "qdrant"',
        ),
    ),
    TokenCheck(
        "Pinecone transient transport failures use typed retryable detail",
        "src/runtime/executors/pinecone.rs",
        (
            "backend_transport_status",
            'backend_transport_status("pinecone", "request", e)',
            'backend_transport_status("pinecone", "response read", e)',
            'backend_transport_status("pinecone", "response parse", e)',
        ),
    ),
    TokenCheck(
        "Pinecone executor internals use typed internal detail",
        "src/runtime/executors/pinecone.rs",
        (
            "fn pinecone_internal_status(",
            'crate::runtime::executor_utils::internal_status("pinecone", operation, message)',
            "fn encode_pinecone_response(",
            '"query_response_encode"',
            '"mutate_response_encode"',
            '"search_response_encode"',
            "pinecone_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "pinecone"',
        ),
    ),
    TokenCheck(
        "Weaviate transient transport failures use typed retryable detail",
        "src/runtime/executors/weaviate.rs",
        (
            "backend_transport_status",
            'backend_transport_status("weaviate", "request", e)',
            'backend_transport_status("weaviate", "response read", e)',
            'backend_transport_status("weaviate", "response parse", e)',
        ),
    ),
    TokenCheck(
        "Weaviate executor internals use typed internal detail",
        "src/runtime/executors/weaviate.rs",
        (
            "fn weaviate_internal_status(",
            'crate::runtime::executor_utils::internal_status("weaviate", operation, message)',
            "fn encode_weaviate_response(",
            '"query_response_encode"',
            '"mutate_response_encode"',
            '"search_response_encode"',
            "weaviate_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "weaviate"',
        ),
    ),
    TokenCheck(
        "Qdrant unsupported generic/object/transaction operations use typed capability detail",
        "src/runtime/executors/qdrant.rs",
        (
            "capability_status",
            '"qdrant"',
            '"generic_query"',
            "qdrant does not support generic query; use search",
            '"object_store"',
            "qdrant is not an object store",
            '"transactions"',
            "qdrant does not support transactions",
        ),
    ),
    TokenCheck(
        "Pinecone unsupported object/transaction operations use typed capability detail",
        "src/runtime/executors/pinecone.rs",
        (
            "capability_status",
            '"pinecone"',
            '"object_store"',
            "Pinecone is not an object store",
            '"transactions"',
            "Pinecone has no transaction primitive",
        ),
    ),
    TokenCheck(
        "Weaviate unsupported object/transaction operations use typed capability detail",
        "src/runtime/executors/weaviate.rs",
        (
            "capability_status",
            '"weaviate"',
            '"object_store"',
            "Weaviate is not an object store",
            '"transactions"',
            "Weaviate has no transaction primitive",
        ),
    ),
    TokenCheck(
        "Elasticsearch transient transport failures use typed retryable detail",
        "src/runtime/executors/elasticsearch.rs",
        (
            "backend_transport_status",
            'backend_transport_status("Elasticsearch", "request", e)',
            'backend_transport_status("Elasticsearch", "response read", e)',
            '"Elasticsearch"',
            '"response parse"',
            'backend_transport_status("Elasticsearch", "_bulk", e)',
            'backend_transport_status("Elasticsearch", "_bulk response read", e)',
            '"_bulk response parse"',
        ),
    ),
    TokenCheck(
        "Elasticsearch executor internals use typed internal detail",
        "src/runtime/executors/elasticsearch.rs",
        (
            "fn elasticsearch_internal_status(",
            'crate::runtime::executor_utils::internal_status("elasticsearch", operation, message)',
            "fn encode_elasticsearch_response(",
            '"query_response_encode"',
            '"bulk_response_encode"',
            '"mutate_response_encode"',
            '"search_response_encode"',
            "elasticsearch_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "elasticsearch"',
        ),
    ),
    TokenCheck(
        "Elasticsearch unsupported object/transaction operations use typed capability detail",
        "src/runtime/executors/elasticsearch.rs",
        (
            "capability_status",
            '"elasticsearch"',
            '"object_store"',
            "Elasticsearch is not an object store; route to S3/MinIO",
            '"transactions"',
            "Elasticsearch does not provide multi-document",
        ),
    ),
    TokenCheck(
        "Redis transient command failures use typed retryable detail",
        "src/runtime/executors/redis.rs",
        (
            "backend_transport_status",
            'backend_transport_status("redis", "connection", err)',
            'backend_transport_status("redis", "GET", err)',
            'backend_transport_status("redis", "MGET", err)',
            'backend_transport_status("redis", "EXISTS", err)',
            'backend_transport_status("redis", "SCAN", err)',
            'backend_transport_status("redis", "MGET after SCAN", err)',
            'backend_transport_status("redis", "SETEX", err)',
            'backend_transport_status("redis", "SET", err)',
            'backend_transport_status("redis", "DEL", err)',
            'backend_transport_status("redis", "EXPIRE", err)',
        ),
    ),
    TokenCheck(
        "Redis unsupported search/object/resource/transaction operations use typed capability detail",
        "src/runtime/executors/redis.rs",
        (
            "capability_status",
            '"redis"',
            '"vector_search"',
            "redis does not support vector search",
            '"object_store"',
            "redis is not an object store",
            '"resource_lifecycle"',
            "redis does not expose resource lifecycle operations",
            '"transactions"',
            "redis generic dispatch does not expose MULTI/EXEC transactions",
        ),
    ),
    TokenCheck(
        "CacheService Redis transient failures use typed retryable detail",
        "src/runtime/service/cache_service",
        (
            'backend_transport_status("redis", context, err)',
            'backend_transport_status(',
            '"redis",',
            '"connection",',
            'map_err("GET meta", err)',
            'map_err("GET counter", err)',
            'map_err("GET", err)',
            'map_err("TTL", err)',
            'map_err("SET", err)',
            'map_err("SCAN", err)',
            'map_err("DEL", err)',
            'map_err("INCRBY", err)',
        ),
    ),
    TokenCheck(
        "CacheService missing Redis capability uses typed capability detail",
        "src/runtime/service/cache_service",
        (
            "redis_capability_status",
            "capability_status",
            '"cache"',
            '"service_startup"',
            '"redis_feature"',
            "cache service requires the `redis` feature/backend",
            '"request_dispatch"',
            '"redis_backend"',
            "cache service requires a configured Redis backend",
            "cache_missing_redis_capability_carries_typed_detail",
        ),
    ),
    TokenCheck(
        "AssetService missing runtime/store capability uses typed capability detail",
        "src/runtime/service/asset_service",
        (
            "asset_capability_status",
            "capability_status",
            '"asset"',
            '"native_entity_dispatch"',
            '"runtime_native_entity_dispatch"',
            "asset service requires runtime native entity dispatch",
            '"postgres_store"',
            "asset service requires a Postgres-backed store (no PG pool configured)",
            "asset_missing_runtime_capability_carries_typed_detail",
        ),
    ),
    TokenCheck(
        "AssetService native-state crypto failures use typed capability detail",
        "src/runtime/service/asset_service",
        (
            "fn native_state_encryption_failed_status(",
            "fn native_state_decryption_failed_status(",
            "asset_capability_status(",
            '"native_state_encrypt"',
            '"native_state_decrypt"',
            '"native_state_encryption"',
            '"native-state encryption failed: {err}"',
            '"native-state decryption failed: {err}"',
            ".map_err(native_state_encryption_failed_status)",
            ".map_err(native_state_decryption_failed_status)",
            "native_state_crypto_failures_carry_capability_detail",
            "ErrorKind::Capability",
        ),
    ),
    TokenCheck(
        "AssetService not-found denials use typed schema detail",
        "src/runtime/service/asset_service",
        (
            "fn asset_schema_not_found_status(",
            "crate::runtime::executor_utils::schema_status",
            "tonic::Code::NotFound",
            '"pipeline_definition_not_found"',
            '"pipeline_instance_not_found"',
            '"pipeline_step_not_found"',
            '"asset_not_found"',
            'return Err(asset_schema_not_found_status(',
            '"get_pipeline_definition"',
            '"start_pipeline"',
            '"get_pipeline"',
            '"complete_step"',
            '"get_asset"',
            "asset_not_found_statuses_carry_schema_detail",
            "assert_schema_not_found_detail(",
            "ErrorKind::Schema",
            'detail.backend, "asset"',
        ),
    ),
    TokenCheck(
        "AssetService internal failures use typed internal detail",
        "src/runtime/service/asset_service",
        (
            "fn asset_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status",
            'crate::runtime::executor_utils::internal_status("asset", operation, message)',
            '"handle_storage_finalized"',
            '"handle_trigger_event"',
            '"resolve_finalized_file"',
            '"start_pipeline_for_file"',
            '"advance_pipeline_instance"',
            '"decode_pipeline_instance"',
            '"decode_pipeline_step"',
            '"start_pipeline"',
            '"get_pipeline"',
            '"complete_step"',
            '"list_assets"',
            '"load_trigger_topics"',
            '"match pipeline definition failed: {e}"',
            '"match pipeline definition by trigger_topic failed: {e}"',
            '"aggregate step status failed: {err}"',
            '"pipeline definition steps not JSON: {e}"',
            "asset_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "asset"',
        ),
    ),
    TokenCheck(
        "native service missing runtime/store capability uses typed capability detail",
        "src/runtime/service/analytics_service",
        (
            "analytics_capability_status",
            "capability_status",
            '"analytics"',
            '"postgres_store"',
            "analytics service requires a Postgres-backed store (no PG pool configured)",
            "analytics_missing_postgres_capability_carries_typed_detail",
        ),
    ),
    TokenCheck(
        "AnalyticsService internal failures use typed internal detail",
        "src/runtime/service/analytics_service",
        (
            "fn analytics_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status",
            'crate::runtime::executor_utils::internal_status("analytics", operation, message)',
            '"get_pipeline_summary"',
            '"get_executor_performance"',
            '"get_reconciliation_analytics"',
            '"get_throughput"',
            '"get_sla_compliance"',
            '"trigger_snapshot"',
            "analytics_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "analytics"',
        ),
    ),
    TokenCheck(
        "LockService missing runtime capability uses typed capability detail",
        "src/runtime/service/lock_service",
        (
            "lock_capability_status",
            "capability_status",
            '"lock"',
            '"native_entity_dispatch"',
            '"runtime_native_entity_dispatch"',
            "lock service requires runtime native-entity dispatch (no runtime configured)",
            "lock_missing_runtime_capability_carries_typed_detail",
        ),
    ),
    TokenCheck(
        "LockService not-held miss uses typed schema detail",
        "src/runtime/service/lock_service",
        (
            "fn lock_not_held_status(operation: &'static str) -> Status",
            "crate::runtime::executor_utils::schema_status",
            "tonic::Code::NotFound",
            '"lock_not_held"',
            'lock_not_held_status("renew_lock")',
            "lock_not_held_carries_schema_detail",
            "ErrorKind::Schema",
            'detail.backend, "lock"',
        ),
    ),
    TokenCheck(
        "ConfigService missing runtime capability uses typed capability detail",
        "src/runtime/service/config_service",
        (
            "config_capability_status",
            "capability_status",
            '"config"',
            '"native_entity_dispatch"',
            '"runtime_native_entity_dispatch"',
            "config service requires runtime native-entity dispatch (no runtime configured)",
            "config_missing_runtime_capability_carries_typed_detail",
        ),
    ),
    TokenCheck(
        "SchedulerService missing store capability uses typed capability detail",
        "src/runtime/service/scheduler_service",
        (
            "scheduler_capability_status",
            "capability_status",
            '"scheduler"',
            '"postgres_store"',
            "scheduler service requires a Postgres-backed store (no PG pool configured)",
            "scheduler_missing_postgres_capability_carries_typed_detail",
        ),
    ),
    TokenCheck(
        "SchedulerService not-found denials use typed schema detail",
        "src/runtime/service/scheduler_service",
        (
            "fn scheduler_not_found_status(",
            "crate::runtime::executor_utils::schema_status",
            "tonic::Code::NotFound",
            '"scheduled_job_not_found"',
            '"active_scheduled_job_not_found"',
            '"paused_scheduled_job_not_found"',
            'scheduler_not_found_status(',
            '"get_job"',
            '"delete_job"',
            '"pause_job"',
            '"resume_job"',
            "scheduler_not_found_statuses_carry_schema_detail",
            "assert_schema_not_found_detail(",
            "ErrorKind::Schema",
            'detail.backend, "scheduler"',
        ),
    ),
    TokenCheck(
        "SchedulerService internal failures use typed internal detail",
        "src/runtime/service/scheduler_service",
        (
            "fn scheduler_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status",
            'crate::runtime::executor_utils::internal_status("scheduler", operation, message)',
            '"decode_scheduled_job"',
            '"create_scheduled_job"',
            '"get_scheduled_job"',
            '"list_scheduled_jobs_count"',
            '"list_scheduled_jobs"',
            '"delete_scheduled_job"',
            '"pause_scheduled_job"',
            '"resume_scheduled_job"',
            '"scheduler_tick_claim"',
            '"scheduler_tick_outbox_insert"',
            "scheduler_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "scheduler"',
        ),
    ),
    TokenCheck(
        "WorkflowService missing store capability uses typed capability detail",
        "src/runtime/service/workflow_service",
        (
            "workflow_capability_status",
            "capability_status",
            '"workflow"',
            '"postgres_store"',
            "workflow service requires a Postgres-backed store (no PG pool configured)",
            "workflow_missing_postgres_capability_carries_typed_detail",
        ),
    ),
    TokenCheck(
        "WebhookService missing store capability uses typed capability detail",
        "src/runtime/service/webhook_service",
        (
            "webhook_capability_status",
            "capability_status",
            '"webhook"',
            '"postgres_store"',
            "webhook service requires a Postgres-backed store (no PG pool configured)",
            "webhook_missing_postgres_capability_carries_typed_detail",
        ),
    ),
    TokenCheck(
        "WebhookService not-found denials use typed schema detail",
        "src/runtime/service/webhook_service",
        (
            "fn webhook_endpoint_not_found_status(operation: &'static str) -> Status",
            "crate::runtime::executor_utils::schema_status",
            "tonic::Code::NotFound",
            '"webhook_endpoint_not_found"',
            'webhook_endpoint_not_found_status("get_endpoint")',
            'webhook_endpoint_not_found_status("update_endpoint")',
            'webhook_endpoint_not_found_status("delete_endpoint")',
            "webhook_endpoint_not_found_statuses_carry_schema_detail",
            "assert_schema_not_found_detail(",
            "ErrorKind::Schema",
            'detail.backend, "webhook"',
        ),
    ),
    TokenCheck(
        "WebhookService internal failures use typed internal detail",
        "src/runtime/service/webhook_service",
        (
            "fn webhook_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status",
            'crate::runtime::executor_utils::internal_status("webhook", operation, message)',
            '"decode_webhook_endpoint"',
            '"decode_webhook_delivery"',
            '"create_webhook_endpoint"',
            '"get_webhook_endpoint"',
            '"list_webhook_endpoints_count"',
            '"list_webhook_endpoints"',
            '"update_webhook_endpoint"',
            '"delete_webhook_endpoint"',
            '"list_webhook_deliveries_count"',
            '"list_webhook_deliveries"',
            "webhook_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "webhook"',
        ),
    ),
    TokenCheck(
        "TenantService missing setup capability uses typed capability detail",
        "src/runtime/service/tenant_service",
        (
            "tenant_capability_status",
            "capability_status",
            '"tenant"',
            '"purge_tenant"',
            '"catalog_manifest"',
            "tenant service requires the catalog manifest for purge",
            '"native_entity_dispatch"',
            '"runtime_native_entity_dispatch"',
            "tenant service requires runtime native entity dispatch",
            '"postgres_store"',
            "tenant service requires a Postgres-backed store (no PG pool configured)",
            "tenant_missing_setup_capabilities_carry_typed_detail",
        ),
    ),
    TokenCheck(
        "TenantService not-found denials use typed schema detail",
        "src/runtime/service/tenant_service",
        (
            "fn tenant_not_found_status(operation: &'static str) -> Status",
            "crate::runtime::executor_utils::schema_status",
            "tonic::Code::NotFound",
            '"tenant_not_found"',
            'tenant_not_found_status("get_tenant")',
            'tenant_not_found_status("update_tenant")',
            "tenant_not_found_statuses_carry_schema_detail",
            "assert_schema_not_found_detail(",
            "ErrorKind::Schema",
            'detail.backend, "tenant"',
        ),
    ),
    TokenCheck(
        "TenantService internal failures use typed internal detail",
        "src/runtime/service/tenant_service",
        (
            "fn tenant_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status",
            'crate::runtime::executor_utils::internal_status("tenant", operation, message)',
            '"decode_tenant"',
            '"create_tenant"',
            '"resolve_tenant_after_create"',
            '"list_tenants_count"',
            '"list_tenants"',
            '"update_tenant"',
            "tenant_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "tenant"',
        ),
    ),
    TokenCheck(
        "WebrtcService missing setup capability uses typed capability detail",
        "src/runtime/service/webrtc_service/mod.rs",
        (
            "webrtc_capability_status",
            "capability_status",
            '"webrtc"',
            '"native_entity_dispatch"',
            '"runtime_native_entity_dispatch"',
            "webrtc service requires runtime native entity dispatch",
            '"postgres_store"',
            "webrtc service requires a Postgres-backed store (no PG pool configured)",
            "webrtc_missing_setup_capabilities_carry_typed_detail",
        ),
    ),
    TokenCheck(
        "WebrtcService not-found denials use typed schema detail",
        "src/runtime/service/webrtc_service/mod.rs",
        (
            "fn webrtc_schema_not_found_status(",
            "crate::runtime::executor_utils::schema_status",
            "tonic::Code::NotFound",
            '"webrtc_room_not_found"',
            '"webrtc_peer_not_found"',
            '"webrtc_track_not_found_or_ended"',
            '"get_room"',
            '"update_room"',
            '"close_room"',
            '"get_peer"',
            '"mute_track"',
            "webrtc_not_found_statuses_carry_schema_detail",
            "assert_schema_not_found_detail(",
            "ErrorKind::Schema",
            'detail.backend, "webrtc"',
        ),
    ),
    TokenCheck(
        "WebrtcService TURN-secret setup uses typed capability detail",
        "src/runtime/service/webrtc_service/mod.rs",
        (
            "turn_secret_not_configured_status",
            "webrtc_capability_status_with_reason",
            '"turn_secret"',
            "TURN secret not configured; set UDB_TURN_SECRET to issue credentials",
            "turn_secret_not_configured_status(\"join_session\")",
            "turn_secret_not_configured_status(\"issue_credentials\")",
            "ERROR_DETAIL_METADATA_KEY",
            "ErrorKind::Capability",
            "turn_secret_setup_capability_preserves_reason_and_typed_detail",
        ),
    ),
    TokenCheck(
        "WebrtcService reason denials use typed policy/capability detail",
        "src/runtime/service/webrtc_service/mod.rs",
        (
            "fn webrtc_policy_status_with_reason(",
            "fn webrtc_policy_status_with_code_and_reason(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "fn peer_not_active_status()",
            "fn peer_not_active_permission_status()",
            "fn room_not_joinable_status()",
            "fn egress_tenant_scope_mismatch_status()",
            "fn sfu_backend_unavailable_status(",
            '"require_active_peer_membership"',
            '"signal_peer_membership"',
            '"peer_not_active"',
            '"join_room"',
            '"room_not_joinable"',
            '"stop_egress"',
            '"egress_tenant_scope_mismatch"',
            '"sfu_join_token"',
            '"sfu_backend"',
            "return Err(peer_not_active_status());",
            "peer_not_active_permission_status()",
            "return Err(egress_tenant_scope_mismatch_status());",
            "return Err(room_not_joinable_status());",
            ".map_err(sfu_backend_unavailable_status)",
            "webrtc_state_denials_preserve_reason_and_policy_detail",
            "webrtc_permission_denials_preserve_reason_and_policy_detail",
            "sfu_backend_unavailable_preserves_reason_and_capability_detail",
            "ErrorKind::Policy",
            "ErrorKind::Capability",
            "ROOM_FULL",
            "PEER_NOT_ACTIVE",
            "EGRESS_TENANT_SCOPE_MISMATCH",
            "SFU_BACKEND_UNAVAILABLE",
        ),
    ),
    TokenCheck(
        "WebrtcService egress setup uses typed capability detail",
        "src/runtime/service/webrtc_service/mod.rs",
        (
            "egress_not_enabled_status",
            "egress_backend_unavailable_status",
            "webrtc_capability_status_with_reason",
            '"webrtc_egress_enabled"',
            '"webrtc_egress_backend"',
            "EGRESS_NOT_ENABLED_MESSAGE",
            "EGRESS_BACKEND_UNAVAILABLE_MESSAGE",
            "require_egress_enabled(\"start_room_composite\")",
            "require_egress_enabled(\"list_egress\")",
            "list_egress_backend(&req.tenant_id, &req.room_id, \"list_egress\")",
            "egress_disabled_returns_failed_precondition_not_unimplemented",
            "egress_enabled_without_backend_carries_capability_detail",
            "ERROR_DETAIL_METADATA_KEY",
            "ErrorKind::Capability",
        ),
    ),
    TokenCheck(
        "WebrtcService internal failures use typed internal detail",
        "src/runtime/service/webrtc_service/mod.rs",
        (
            "fn webrtc_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status",
            'crate::runtime::executor_utils::internal_status("webrtc", operation, message)',
            '"require_active_peer_membership"',
            '"join_room"',
            '"sfu_join_metadata"',
            '"disconnect_bound_peer"',
            '"touch_bound_peer_membership"',
            '"reap_stale_peers"',
            '"decode_room"',
            '"decode_peer"',
            '"update_room"',
            '"close_room"',
            '"list_rooms"',
            '"leave_room"',
            '"unpublish_track"',
            '"mute_track"',
            '"signal"',
            '"verify peer membership failed: {err}"',
            '"claim room slot failed: {err}"',
            '"disconnect peer transaction failed: {err}"',
            '"close room transaction failed: {err}"',
            '"signaling stream error: {e}"',
            "webrtc_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "webrtc"',
        ),
    ),
    TokenCheck(
        "Authz audit internal failures use typed internal detail",
        "src/runtime/service/auth_service/authz/audit.rs",
        (
            "fn authz_audit_internal_status(",
            'crate::runtime::executor_utils::internal_status("authz", operation, message)',
            '"decode_access_audit"',
            '"list_access_audits"',
            '"count_access_audits"',
            '"decode access audit failed: {e}"',
            '"list access audits failed: {err}"',
            '"count access audits failed: {err}"',
            "authz_audit_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "authz"',
        ),
    ),
    TokenCheck(
        "Authz tuple internal failures use typed internal detail",
        "src/runtime/service/auth_service/authz/tuples.rs",
        (
            "fn authz_tuple_internal_status(",
            'crate::runtime::executor_utils::internal_status("authz", operation, message)',
            '"store_role_binding"',
            '"store_relationship_tuple"',
            '"store role binding failed: {err}"',
            '"store relationship tuple failed: {err}"',
            "authz_tuple_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "authz"',
        ),
    ),
    TokenCheck(
        "Authz service internals use typed internal detail",
        "src/runtime/service/auth_service/authz/mod.rs",
        (
            "fn authz_internal_status(",
            'crate::runtime::executor_utils::internal_status("authz", operation, message)',
            '"decode_role"',
            '"decode_user_role"',
            '"decode_policy_rule"',
            '"read_authz_revision_fence"',
            '"load_authz_policies"',
            '"decode_authz_policy"',
            '"load_role_bindings"',
            '"decode_role_binding"',
            '"load_grouping_tuples"',
            '"decode_grouping_tuple"',
            '"load_relationship_tuples"',
            '"decode_relationship_tuple"',
            '"store_authz_policy"',
            '"assign_role_principal"',
            '"assign_role"',
            '"encode_policy_conditions"',
            '"create_policy_rule"',
            '"revoke_role"',
            '"list_user_roles"',
            '"get_role"',
            '"list_roles"',
            '"read_role_scope"',
            '"delete_role"',
            '"delete_role_assignments"',
            '"get_policy_rule"',
            '"list_policy_rules"',
            '"delete_policy_rule"',
            '"sign_policy_bundle"',
            "authz_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "authz"',
        ),
    ),
    TokenCheck(
        "Authz governance revision internals use typed internal detail",
        "src/runtime/service/auth_service/authz/governance.rs",
        (
            "fn governance_internal_status(",
            'crate::runtime::executor_utils::internal_status("authz", operation, message)',
            '"read_authz_revision"',
            '"bump_authz_revision"',
            '"read authz revision failed: {err}"',
            '"bump authz revision failed: {err}"',
            "governance_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "authz"',
        ),
    ),
    TokenCheck(
        "Authz governance simulation internals use typed internal detail",
        "src/runtime/service/auth_service/authz/governance_sim.rs",
        (
            "fn governance_sim_internal_status(",
            'crate::runtime::executor_utils::internal_status("authz", operation, message)',
            '"list_policy_versions"',
            '"seed_builtin_role"',
            '"list policy versions failed: {err}"',
            '"seed role failed: {err}"',
            "governance_sim_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "authz"',
        ),
    ),
    TokenCheck(
        "ControlPlaneService stream internals use typed internal detail",
        "src/runtime/service/auth_service/control_plane/mod.rs",
        (
            "fn internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status",
            'crate::runtime::executor_utils::internal_status("control_plane", operation, message)',
            '"stream_resources"',
            '"delta_resources"',
            '"control stream error: {e}"',
            '"control delta stream error: {e}"',
        ),
    ),
    TokenCheck(
        "ControlPlaneService internal detail coverage decodes typed detail",
        "src/runtime/service/auth_service/control_plane/tests.rs",
        (
            "fn assert_internal_detail(",
            "control_plane_internal_status_carries_typed_detail",
            "ErrorKind::Internal",
            'detail.backend, backend',
            '"control_plane"',
            '"delta_resources"',
        ),
    ),
    TokenCheck(
        "Control-plane store internals use typed internal detail",
        "src/runtime/service/auth_service/control_plane/store.rs",
        (
            "fn control_store_internal_status(",
            'crate::runtime::executor_utils::internal_status("control_plane", operation, message)',
            '"typed_native_write_json"',
            '"typed_native_write_compile"',
            '"typed_native_read_json"',
            '"typed_native_read_compile"',
            '"ensure_node_state"',
            '"typed native write JSON failed: {err}"',
            '"typed native write did not compile to SQL"',
            '"typed native read JSON failed: {err}"',
            '"typed native read did not compile to SQL"',
            '"node state vanished after ensure"',
            "control_store_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "control_plane"',
        ),
    ),
    TokenCheck(
        "Control-plane sourcing internals use typed internal detail",
        "src/runtime/service/auth_service/control_plane/sourcing.rs",
        (
            "fn control_sourcing_internal_status(",
            'crate::runtime::executor_utils::internal_status("control_plane", operation, message)',
            '"backend_target_payload"',
            '"service_enablement_payload"',
            '"method_security_payload"',
            '"routing_policy_payload"',
            '"project_routing_payload"',
            '"rls_policy_payload"',
            '"backend target payload encode failed: {e}"',
            '"service enablement payload encode failed: {e}"',
            '"method security payload encode failed: {e}"',
            '"routing policy payload encode failed: {e}"',
            '"project routing payload encode failed: {e}"',
            '"rls policy payload encode failed: {e}"',
            "control_sourcing_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "control_plane"',
        ),
    ),
    TokenCheck(
        "IdentityProviderService SAML internals use typed internal detail",
        "src/runtime/service/auth_service/idp/mod.rs",
        (
            "fn idp_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status",
            'crate::runtime::executor_utils::internal_status("identity_provider", operation, message)',
            '"start_saml_login"',
            '"saml_acs_dev_self_assert"',
            '"AuthnRequest build failed: {e}"',
            '"dev SAML self-assert failed: {e}"',
            "idp_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "identity_provider"',
        ),
    ),
    TokenCheck(
        "IdentityProvider store internals use typed internal detail",
        "src/runtime/service/auth_service/idp/store.rs",
        (
            "fn idp_store_internal_status(",
            'crate::runtime::executor_utils::internal_status("identity_provider", operation, message)',
            '"idp_secret_encrypt"',
            '"typed_native_mutation_json"',
            '"typed_native_mutation_compile"',
            '"external_identity_upsert_verify"',
            '"principal_hard_delete_tx_begin"',
            '"principal_hard_delete_tx_commit"',
            '"idp secret encryption-at-rest failed: {err}"',
            '"typed native mutation JSON failed: {err}"',
            '"typed native mutation did not compile to SQL"',
            '"external identity vanished after upsert"',
            '"principal hard delete tx begin failed: {err}"',
            '"principal hard delete tx commit failed: {err}"',
            "idp_store_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "identity_provider"',
        ),
    ),
    TokenCheck(
        "Authn signing-key internals use typed internal detail",
        "src/runtime/service/auth_service/authn/signing_keys.rs",
        (
            "fn signing_key_internal_status(",
            'crate::runtime::executor_utils::internal_status("authn", operation, message)',
            '"jwks_registry_read"',
            '"active_signing_key_read"',
            '"active_signing_key_decrypt"',
            '"compromise_signing_key"',
            '"signing-key registry read failed: {err}"',
            '"active signing-key read failed: {err}"',
            '"active signing-key decrypt failed: {err}"',
            '"compromise signing key failed: {err}"',
            "signing_key_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "authn"',
        ),
    ),
    TokenCheck(
        "Authn token-family internals use typed internal detail",
        "src/runtime/service/auth_service/authn/token_family.rs",
        (
            "fn token_family_internal_status(",
            'crate::runtime::executor_utils::internal_status("authn", operation, message)',
            '"mint_refresh_family"',
            '"revoke_families_for_session"',
            '"revoke_families_for_principal"',
            '"rotate_refresh_family"',
            '"inspect_refresh_family"',
            '"revoke_reused_family"',
            '"mint refresh family failed: {err}"',
            '"revoke families for session failed: {err}"',
            '"revoke families for principal failed: {err}"',
            '"rotate refresh family failed: {err}"',
            '"inspect refresh family failed: {err}"',
            '"revoke reused family failed: {err}"',
            "token_family_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "authn"',
        ),
    ),
    TokenCheck(
        "Authn core user lifecycle internals use typed internal detail",
        "src/runtime/service/auth_service/authn/core.rs",
        (
            "fn authn_core_internal_status(",
            'crate::runtime::executor_utils::internal_status("authn", operation, message)',
            '"create_user_tx_begin"',
            '"create_user_tx_commit"',
            '"get_user"',
            '"list_users"',
            '"update_user_load"',
            '"change_user_status_load"',
            '"change_user_status_tx_begin"',
            '"change_user_status_store"',
            '"change_user_status_tx_commit"',
            '"admin_reset_password_load"',
            '"create user tx begin failed: {err}"',
            '"create user commit failed: {err}"',
            '"status change tx begin failed: {err}"',
            '"status change commit failed: {err}"',
            "authn_core_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "authn"',
        ),
    ),
    TokenCheck(
        "ApiKey service internals use typed internal detail",
        "src/runtime/service/auth_service/apikey.rs",
        (
            "fn internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status",
            'crate::runtime::executor_utils::internal_status("api_key", operation, message)',
            '"create_api_key_store"',
            '"get_api_key_load"',
            '"list_api_keys_store"',
            '"update_api_key_load"',
            '"update_api_key_store"',
            '"revoke_api_key_load"',
            '"revoke_api_key_store"',
            '"rotate_api_key_load"',
            '"rotate_api_key_store"',
            '"emergency_revoke_select"',
            '"emergency_revoke_store"',
            '"validate_api_key_store"',
            '"usage_stats_query"',
            '"emergency revoke select failed: {err}"',
            '"usage stats query failed: {err}"',
            "api_key_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "api_key"',
        ),
    ),
    TokenCheck(
        "NotificationService missing setup capability uses typed capability detail",
        "src/runtime/service/notification_service",
        (
            "notification_capability_status",
            "capability_status",
            '"notification"',
            '"native_entity_dispatch"',
            '"runtime_native_entity_dispatch"',
            "notification service requires runtime native entity dispatch",
            '"postgres_store"',
            "notification service requires a Postgres-backed store (no PG pool configured)",
            "notification_missing_setup_capabilities_carry_typed_detail",
        ),
    ),
    TokenCheck(
        "NotificationService internal failures use typed internal detail",
        "src/runtime/service/notification_service",
        (
            "fn notification_internal_status(",
            'crate::runtime::executor_utils::internal_status("notification", operation, message)',
            '"decode_notification_log"',
            '"decode_template"',
            '"decode_preference"',
            '"decode_delivery_attempt"',
            '"retry_notification_update"',
            '"report_delivery_attempt"',
            '"upsert_template_query"',
            '"delivery_stats_query"',
            '"set_preference_query"',
            "notification_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "notification"',
        ),
    ),
    TokenCheck(
        "NotificationService retry lifecycle denial uses typed policy detail",
        "src/runtime/service/notification_service",
        (
            "fn notification_policy_status_with_reason(",
            "crate::runtime::executor_utils::policy_status",
            "fn notification_policy_status_with_code(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "fn notification_tenant_metadata_required_status(operation: &'static str) -> Status",
            "fn notification_not_retryable_status()",
            '"retry_notification"',
            '"notification_not_retryable"',
            '"tenant_metadata_required"',
            '"tenant-scoped metadata is required"',
            '"get_notification"',
            '"get_template"',
            '"list_templates"',
            "NOT_RETRYABLE_STATE",
            'notification_tenant_metadata_required_status("get_notification")',
            'notification_tenant_metadata_required_status("retry_notification")',
            'notification_tenant_metadata_required_status("get_template")',
            'notification_tenant_metadata_required_status("list_templates")',
            "return Err(notification_not_retryable_status());",
            "retry_not_retryable_state_carries_policy_detail_and_reason",
            "tenant_metadata_required_status_carries_permission_denied_policy_detail",
            "ErrorKind::Policy",
            '"notification not found or not in a retryable (FAILED) state"',
        ),
    ),
    TokenCheck(
        "NotificationService not-found denials use typed schema detail",
        "src/runtime/service/notification_service",
        (
            "fn notification_schema_not_found_status(",
            "crate::runtime::executor_utils::schema_status",
            "fn notification_template_not_found_status(",
            "tonic::Code::NotFound",
            '"notification_not_found"',
            '"notification_template_not_found"',
            '"notification_preference_not_found"',
            "notification_template_not_found_status(",
            "notification_schema_not_found_status(",
            '"send_notification"',
            '"get_notification"',
            '"get_template"',
            '"get_preference"',
            "notification_not_found_statuses_carry_schema_detail",
            "notification_template_not_found_status_keeps_reason_and_schema_detail",
            "assert_schema_not_found_detail(",
            "ErrorKind::Schema",
            'detail.backend, "notification"',
            "TEMPLATE_NOT_FOUND",
        ),
    ),
    TokenCheck(
        "BackupService missing setup capability uses typed capability detail",
        "src/runtime/service/backup_service",
        (
            "backup_capability_status",
            "capability_status",
            '"backup"',
            '"native_entity_dispatch"',
            '"runtime_native_entity_dispatch"',
            "backup service requires runtime native-entity dispatch (no runtime configured)",
            '"postgres_store"',
            "backup service requires a Postgres-backed store (no PG pool configured)",
            '"tenant_table_enumeration"',
            '"catalog_manifest"',
            "backup service requires the catalog manifest to enumerate tenant tables",
            "backup_missing_setup_capabilities_carry_typed_detail",
        ),
    ),
    TokenCheck(
        "EmbeddingService missing setup capability uses typed capability detail",
        "src/runtime/service/embedding_service",
        (
            "embedding_capability_status",
            "capability_status",
            "fn embedding_policy_status_with_code(",
            "policy_status_with_code",
            '"embedding"',
            '"native_entity_dispatch"',
            '"runtime_native_entity_dispatch"',
            "embedding service requires runtime native-entity dispatch (no runtime configured)",
            '"catalog_lookup"',
            '"active_catalog"',
            "embedding service requires the active catalog (no catalog configured)",
            '"embedding_vector_upsert"',
            '"verified_tenant_required"',
            "embedding_missing_setup_capabilities_carry_typed_detail",
            "embedding_point_is_tenant_tagged_no_fail_open",
            "ErrorKind::Policy",
            'detail.policy_decision_id, "verified_tenant_required"',
        ),
    ),
    TokenCheck(
        "EmbeddingService source miss uses typed schema detail",
        "src/runtime/service/embedding_service",
        (
            "fn embedding_source_not_found_status(operation: &'static str) -> Status",
            "crate::runtime::executor_utils::schema_status",
            "tonic::Code::NotFound",
            '"embedding_source_not_found"',
            'embedding_source_not_found_status("backfill")',
            'embedding_source_not_found_status("report_embedding")',
            'embedding_source_not_found_status("retrieve")',
            "embedding_source_not_found_statuses_carry_schema_detail",
            "assert_schema_not_found_detail(",
            "ErrorKind::Schema",
            'detail.backend, "embedding"',
        ),
    ),
    TokenCheck(
        "SearchService missing setup capability uses typed capability detail",
        "src/runtime/service/search_service",
        (
            "search_capability_status",
            "capability_status",
            '"search"',
            '"native_entity_dispatch"',
            '"runtime_native_entity_dispatch"',
            "search service requires runtime native-entity dispatch (no runtime configured)",
            '"catalog_lookup"',
            '"active_catalog"',
            "search service requires the active catalog (no catalog configured)",
            "search_missing_setup_capabilities_carry_typed_detail",
        ),
    ),
    TokenCheck(
        "SearchService full-text-only refusal uses typed capability detail",
        "src/runtime/service/search_service",
        (
            "fn full_text_only_requires_mediated_ir_status() -> Status",
            "search_capability_status(",
            '"full_text_only_search"',
            '"mediated_ir_full_text_path"',
            "full-text-only search requires the mediated IR full-text path",
            "return Err(full_text_only_requires_mediated_ir_status());",
            "full_text_only_search_requires_typed_capability_detail",
            "ErrorKind::Capability",
            'detail.capability_required, "mediated_ir_full_text_path"',
        ),
    ),
    TokenCheck(
        "SearchService index miss uses typed schema detail",
        "src/runtime/service/search_service",
        (
            "fn search_index_not_found_status(operation: &'static str) -> Status",
            "crate::runtime::executor_utils::schema_status",
            "tonic::Code::NotFound",
            '"search_index_not_found"',
            'search_index_not_found_status("reindex")',
            "search_index_not_found_status_carries_schema_detail",
            "assert_schema_not_found_detail(",
            "ErrorKind::Schema",
            'detail.backend, "search"',
        ),
    ),
    TokenCheck(
        "LiveQueryService missing runtime capability uses typed capability detail",
        "src/runtime/service/livequery_service",
        (
            "livequery_capability_status",
            "capability_status",
            '"livequery"',
            '"native_entity_dispatch"',
            '"runtime_native_entity_dispatch"',
            "live query service requires runtime native-entity dispatch (no runtime configured)",
            "livequery_missing_runtime_capability_carries_typed_detail",
        ),
    ),
    TokenCheck(
        "MeteringService missing runtime capability uses typed capability detail",
        "src/runtime/service/metering_service",
        (
            "metering_capability_status",
            "capability_status",
            '"metering"',
            '"native_entity_dispatch"',
            '"runtime_native_entity_dispatch"',
            "metering service requires runtime native-entity dispatch (no runtime configured)",
            "metering_missing_runtime_capability_carries_typed_detail",
        ),
    ),
    TokenCheck(
        "MeteringService internal failures use typed internal detail",
        "src/runtime/service/metering_service",
        (
            "fn metering_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status",
            'crate::runtime::executor_utils::internal_status("metering", operation, message)',
            '"windowed_usage_begin"',
            '"windowed_usage_tenant_scope"',
            '"windowed_usage_aggregate"',
            '"windowed_usage_commit"',
            "metering_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "metering"',
        ),
    ),
    TokenCheck(
        "StorageService missing runtime capability uses typed capability detail",
        "src/runtime/service/storage_service",
        (
            "storage_capability_status",
            "capability_status",
            '"storage"',
            '"native_entity_dispatch"',
            '"object_stream"',
            '"runtime_native_entity_dispatch"',
            "storage service requires runtime native entity dispatch",
            "storage service requires runtime",
            "storage_missing_runtime_capabilities_carry_typed_detail",
        ),
    ),
    TokenCheck(
        "StorageService internal failures use typed internal detail",
        "src/runtime/service/storage_service",
        (
            "fn storage_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status",
            'crate::runtime::executor_utils::internal_status("storage", operation, message)',
            '"tenant_size_sum_begin"',
            '"tenant_size_sum_tenant_scope"',
            '"tenant_size_sum_aggregate"',
            '"tenant_size_sum_commit"',
            "storage_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "storage"',
        ),
    ),
    TokenCheck(
        "VaultService internal failures use typed internal detail",
        "src/runtime/service/vault_service",
        (
            "fn vault_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status",
            'crate::runtime::executor_utils::internal_status("vault", operation, message)',
            '"data_key_decode"',
            '"seal_transit_payload"',
            '"open_transit_payload"',
            '"create_postgres_login_role"',
            '"get_secret_decode_plaintext"',
            '"decrypt_transit_plaintext"',
            '"get_secret_parse_envelope"',
            "vault_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "vault"',
        ),
    ),
    TokenCheck(
        "VaultService seal/setup capability uses typed capability detail",
        "src/runtime/service/vault_service",
        (
            "vault_capability_status",
            "capability_status",
            '"vault"',
            '"seal_gate"',
            '"vault_master_key"',
            "fn vault_master_key_operation_status(",
            '"wrap_data_key"',
            '"unwrap_data_key"',
            "fn vault_db_credentials_config_status(",
            '"generate_database_credentials"',
            '"database_credentials_config"',
            "fn vault_db_native_store_required_status()",
            '"postgres_native_store"',
            "fn vault_db_role_creation_status(",
            '"postgres_role_management"',
            "fn vault_confirmation_token_required_status()",
            "failed_precondition_fields",
            '"confirmation_token"',
            '"native_entity_dispatch"',
            '"runtime_native_entity_dispatch"',
            "vault is sealed: master key unavailable",
            "vault service requires runtime native-entity dispatch (no runtime configured)",
            "sealed_vault_fails_closed",
            "vault_missing_runtime_carries_capability_detail",
            "destroy_secret_missing_confirmation_carries_failed_precondition_field_violation",
            "vault_setup_failures_carry_capability_detail",
            "ErrorKind::Capability",
            "ErrorKind::Validation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
        ),
    ),
    TokenCheck(
        "VaultService not-found denials use typed schema detail",
        "src/runtime/service/vault_service",
        (
            "fn vault_schema_not_found_status(",
            "crate::runtime::executor_utils::schema_status",
            "tonic::Code::NotFound",
            '"vault_secret_not_found"',
            '"vault_transit_key_not_found"',
            '"vault_transit_active_key_not_found"',
            '"vault_transit_key_version_not_found"',
            '"get_secret"',
            '"rotate_transit_key"',
            '"encrypt"',
            '"decrypt"',
            '"sign"',
            '"verify"',
            '"hmac"',
            "vault_not_found_statuses_carry_schema_detail",
            "assert_schema_not_found_detail(",
            "ErrorKind::Schema",
            'detail.backend, "vault"',
        ),
    ),
    TokenCheck(
        "Memcached transient command failures use typed retryable detail",
        "src/runtime/executors/memcached.rs",
        (
            "backend_transport_status",
            "async fn blocking<F, R>(&self, operation: &'static str, f: F)",
            'backend_transport_status("memcached", operation, e)',
            'self.blocking("version", |c|',
            '.blocking("get", move |c|',
            "c.get::<Vec<u8>>(&key).map_err(|e| e.to_string())",
            'self.blocking("set", move |c|',
            '.blocking("delete", move |c|',
            "c.delete(&key)",
        ),
    ),
    TokenCheck(
        "Memcached unsupported search/object/resource/transaction operations use typed capability detail",
        "src/runtime/executors/memcached.rs",
        (
            "capability_status",
            '"memcached"',
            '"search"',
            "Memcached has no search surface; route to a vector / text backend",
            '"object_store"',
            "Memcached is not an object store; route to S3/MinIO",
            '"resource_lifecycle"',
            "Memcached has no per-resource drop",
            '"transactions"',
            "Memcached has no multi-key transaction primitive",
        ),
    ),
    TokenCheck(
        "Neo4j unsupported search/object/transaction operations use typed capability detail",
        "src/runtime/executors/neo4j.rs",
        (
            "capability_status",
            '"neo4j"',
            '"vector_search"',
            "neo4j does not support generic vector search dispatch",
            '"object_store"',
            "neo4j is not an object store",
            '"transactions"',
            "neo4j transactions are not exposed via generic dispatch",
        ),
    ),
    TokenCheck(
        "Postgres unsupported search/object/resource/transaction operations use typed capability detail",
        "src/runtime/executors/postgres.rs",
        (
            "capability_status",
            '"postgres"',
            '"vector_search"',
            "postgres does not support vector search; use query",
            '"object_store"',
            "postgres is not an object store",
            '"resource_lifecycle"',
            "postgres resource lifecycle is managed via catalog migrations",
            '"typed_transaction_rpc"',
            "use the typed transaction RPC for relational transactions",
        ),
    ),
    TokenCheck(
        "Postgres executor internals use typed internal detail",
        "src/runtime/executors/postgres.rs",
        (
            "fn postgres_executor_internal_status(",
            'crate::runtime::executor_utils::internal_status("postgres", operation, message)',
            "fn encode_postgres_response(",
            '"query_transaction_start"',
            '"query"',
            '"query_transaction_commit"',
            '"query_response_encode"',
            '"mutate_transaction_start"',
            '"mutate_transaction_commit"',
            "postgres_executor_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "postgres"',
        ),
    ),
    TokenCheck(
        "SQLite unsupported search/object operations use typed capability detail",
        "src/runtime/executors/sqlite.rs",
        (
            "capability_status",
            '"sqlite"',
            '"vector_search"',
            "SQLite backend does not provide native vector search",
            '"object_store"',
            "SQLite backend is not an object store; route to S3/MinIO",
        ),
    ),
    TokenCheck(
        "ClickHouse unsupported search/object/transaction operations use typed capability detail",
        "src/runtime/executors/clickhouse.rs",
        (
            "capability_status",
            '"clickhouse"',
            '"vector_search"',
            "clickhouse does not support vector/document search",
            '"object_store"',
            "clickhouse is not an object store",
            '"transactions"',
            "clickhouse does not support multi-statement transactions",
        ),
    ),
    TokenCheck(
        "ClickHouse executor internals use typed internal detail",
        "src/runtime/executors/clickhouse.rs",
        (
            "fn clickhouse_internal_status(",
            'crate::runtime::executor_utils::internal_status("clickhouse", operation, message)',
            "fn encode_clickhouse_response(",
            '"query"',
            '"query_response_encode"',
            '"mutate_ddl"',
            '"mutate_insert_rows"',
            '"ensure_resource"',
            '"drop_resource"',
            '"list_resources"',
            "clickhouse_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "clickhouse"',
        ),
    ),
    TokenCheck(
        "MySQL unsupported search/object operations use typed capability detail",
        "src/runtime/executors/mysql.rs",
        (
            "capability_status",
            '"mysql"',
            '"vector_search"',
            "MySQL backend does not provide native vector search",
            '"object_store"',
            "MySQL backend is not an object store; route to S3/MinIO",
        ),
    ),
    TokenCheck(
        "MySQL executor internals use typed internal detail",
        "src/runtime/executors/mysql.rs",
        (
            "fn mysql_internal_status(",
            'crate::runtime::executor_utils::internal_status("mysql", operation, message)',
            "fn encode_mysql_response(",
            '"query_transaction_start"',
            '"query"',
            '"query_transaction_commit"',
            '"query_response_encode"',
            '"mutate_transaction_start"',
            '"mutate"',
            '"mutate_transaction_commit"',
            '"ensure_resource"',
            '"drop_resource"',
            '"list_resources"',
            '"transaction_begin"',
            '"transaction_statement"',
            '"transaction_commit"',
            "mysql_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "mysql"',
        ),
    ),
    TokenCheck(
        "SQL Server unsupported object operations use typed capability detail",
        "src/runtime/executors/mssql.rs",
        (
            "capability_status",
            '"mssql"',
            '"object_store"',
            "SQL Server is not an object store; route to S3/MinIO",
        ),
    ),
    TokenCheck(
        "SQL Server executor internals use typed internal detail",
        "src/runtime/executors/mssql.rs",
        (
            "fn mssql_internal_status(",
            'crate::runtime::executor_utils::internal_status("mssql", operation, message)',
            "fn encode_mssql_response(",
            '"query"',
            '"query_response_encode"',
            '"mutate"',
            '"list_resources_parse"',
            '"transaction"',
            "mssql_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "mssql"',
        ),
    ),
    TokenCheck(
        "Cassandra unsupported search/object/transaction operations use typed capability detail",
        "src/runtime/executors/cassandra.rs",
        (
            "capability_status",
            '"cassandra"',
            '"search"',
            "Cassandra has no native search surface",
            '"object_store"',
            "Cassandra is not an object store",
            '"transactions"',
            "Cassandra has no multi-statement transactions",
        ),
    ),
    TokenCheck(
        "MongoDB unsupported search/object/native-only operations use typed capability detail",
        "src/runtime/executors/mongodb.rs",
        (
            "capability_status",
            '"mongodb"',
            '"vector_search"',
            "mongodb does not support generic vector search dispatch",
            '"object_store"',
            "mongodb is not an object store",
            '"mongodb_native_change_streams"',
            "MongoDB change streams require the mongodb-native feature",
            '"mongodb_native_transport"',
            "MongoDB generic transactions require native transport via the mongodb-native feature",
        ),
    ),
    TokenCheck(
        "S3 transient object failures use typed retryable detail",
        "src/runtime/executors/s3.rs",
        (
            "backend_transport_status",
            "retryable_status",
            "HTTP_RETRYABLE_BACKOFF_MS",
            'backend_transport_status("S3", "get_object", err)',
            'backend_transport_status("S3", "body read", err)',
            'backend_transport_status("S3", "put_object", err)',
            'backend_transport_status("S3", "create_multipart_upload", e)',
            '"S3 create_multipart_upload returned no upload_id"',
            'backend_transport_status("S3", "upload_part", e)',
            'backend_transport_status("S3", "upload_part (final)", e)',
            'backend_transport_status("S3", "complete_multipart_upload", e)',
            'backend_transport_status("S3", "delete_object", err)',
            'backend_transport_status("S3", "delete_bucket", e)',
            'backend_transport_status("S3", "list_buckets", e)',
        ),
    ),
    TokenCheck(
        "S3 unsupported generic operations use typed capability detail",
        "src/runtime/executors/s3.rs",
        (
            "capability_status",
            '"s3"',
            '"generic_query"',
            "s3 does not support generic query",
            '"object_dispatch"',
            "s3 mutation is via get_object/put_object",
            '"search"',
            "s3 does not support search",
            '"transactions"',
            "s3 does not support transactions",
        ),
    ),
    TokenCheck(
        "S3 core presign failures use typed retryable detail",
        "src/runtime/core/setup_data.rs",
        (
            'backend_transport_status("S3",',
            '"object head"',
            '"multipart init"',
            '"part presign"',
            'backend_transport_status("S3", "presign", err)',
        ),
    ),
    TokenCheck(
        "S3 transaction object mutation failures use typed retryable detail",
        "src/runtime/core/tx_object.rs",
        (
            "backend_transport_status(",
            '"S3",',
            '"put_object"',
        ),
    ),
    TokenCheck(
        "S3 catalog bucket setup failures use typed retryable detail",
        "src/runtime/core/catalog_sql.rs",
        (
            "backend_transport_status(",
            '"S3/MinIO"',
            '"create bucket"',
            '"verify bucket"',
        ),
    ),
    TokenCheck(
        "catalog feature-disabled setup gaps use typed capability detail",
        "src/runtime/core/catalog_sql.rs",
        (
            "fn qdrant_feature_disabled_status(",
            '"ensure_qdrant_store"',
            '"verify_qdrant_store"',
            '"qdrant_feature"',
            "qdrant/vector feature is not enabled",
            "fn s3_feature_disabled_status(",
            '"ensure_s3_bucket"',
            '"verify_s3_bucket"',
            '"s3_feature"',
            "s3/object-store feature is not enabled",
            "catalog_feature_disabled_statuses_carry_capability_detail",
            "ErrorKind::Capability",
            "detail.capability_required",
        ),
    ),
    TokenCheck(
        "core catalog SQL internals use typed internal detail",
        "src/runtime/core/catalog_sql.rs",
        (
            "fn catalog_sql_internal_status(",
            'crate::runtime::executor_utils::internal_status("catalog_sql", operation, message)',
            '"apply_sql_artifact"',
            '"apply_sql_artifact_row"',
            '"apply_sql_artifact_chunk"',
            '"execute_raw_sql"',
            '"load_last_manifest_acquire"',
            '"load_last_manifest_checksum_query"',
            '"load_last_manifest_json_query"',
            '"load_last_manifest_checksum_existence_query"',
            '"load_manifest_by_checksum_json_query"',
            '"load_manifest_by_checksum_json_transaction_begin"',
            '"load_manifest_by_checksum_statement_timeout_setup"',
            '"load_manifest_by_checksum_timeout_json_query"',
            '"load_manifest_by_checksum_json_transaction_commit"',
            '"save_manifest_transaction_begin"',
            '"save_manifest_lock"',
            '"save_manifest_latest_checksum_query"',
            '"save_manifest_touch"',
            '"save_manifest_touch_commit"',
            '"save_manifest_serialise"',
            '"save_manifest_upsert"',
            '"save_manifest_upsert_commit"',
            '"pg_catalog_introspection_transaction_begin"',
            '"pg_catalog_schema_introspection"',
            '"pg_catalog_introspection_transaction_commit"',
            '"cdc_outbox_metrics_query"',
            "catalog_sql_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "catalog_sql"',
        ),
    ),
    TokenCheck(
        "Azure Blob transient object failures use typed retryable detail",
        "src/runtime/executors/azureblob.rs",
        (
            "backend_transport_status",
            'backend_transport_status("azure blob", "get", e)',
            'backend_transport_status("azure blob", "put", e)',
            'backend_transport_status("azure blob", "read", e)',
            'backend_transport_status("azure", "put_block", e)',
            'backend_transport_status("azure", "put_block (final)", e)',
            'backend_transport_status("azure", "put_block_list", e)',
            'backend_transport_status("azure blob", "delete", e)',
            'backend_transport_status("azure blob", "create container", e)',
            'backend_transport_status("azure blob", "drop container", e)',
            'backend_transport_status("azure blob", "list containers", e)',
        ),
    ),
    TokenCheck(
        "Azure Blob unsupported generic operations use typed capability detail",
        "src/runtime/executors/azureblob.rs",
        (
            "capability_status",
            '"azureblob"',
            '"generic_query"',
            "Azure Blob has no query surface; use get_object",
            '"object_dispatch"',
            "Azure Blob has no mutation surface; use put_object",
            '"search"',
            "Azure Blob is not searchable",
            '"transactions"',
            "Azure Blob has no transaction primitive",
        ),
    ),
    TokenCheck(
        "GCS transient object failures use typed retryable detail",
        "src/runtime/executors/gcs.rs",
        (
            "backend_transport_status",
            'backend_transport_status("gcs", "download", e)',
            'backend_transport_status("gcs", "upload", e)',
            'backend_transport_status("gcs", "streamed download", e)',
            'backend_transport_status("gcs", "download chunk", e)',
            'backend_transport_status("gcs", "streamed upload", e)',
            'backend_transport_status("gcs", "delete", e)',
            'backend_transport_status("gcs", "create bucket", e)',
            'backend_transport_status("gcs", "drop bucket", e)',
            'backend_transport_status("gcs", "list buckets", e)',
        ),
    ),
    TokenCheck(
        "GCS unsupported generic operations use typed capability detail",
        "src/runtime/executors/gcs.rs",
        (
            "capability_status",
            '"gcs"',
            '"generic_query"',
            "GCS has no query surface; use get_object",
            '"object_dispatch"',
            "GCS has no mutation surface; use put_object",
            '"search"',
            "GCS is not searchable",
            '"transactions"',
            "GCS has no transaction primitive",
        ),
    ),
    TokenCheck(
        "generic inline object size cap uses typed quota detail",
        "src/runtime/executor_utils.rs",
        (
            "pub(crate) fn reject_oversized_object",
            "quota_refusal_status(",
            '"inline_object_size"',
            "reject_oversized_object_carries_typed_quota_detail",
        ),
    ),
    TokenCheck(
        "embedded runtime metadata validation uses typed field violations",
        "src/embedded.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn insert_ascii(",
            '"{key} is not valid ASCII metadata"',
            '"must be valid ASCII gRPC metadata"',
            "embedded_context_rejects_invalid_metadata",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "x-tenant-id"',
        ),
    ),
    TokenCheck(
        "method-security policy denials use typed policy detail",
        "src/runtime/service/method_security.rs",
        (
            "fn method_security_policy_denied(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "tonic::Code::PermissionDenied",
            '"method_security"',
            "deny_reason::SCOPE",
            "deny_reason::TENANT_MISMATCH",
            "method_security_policy_denial_carries_policy_detail",
            "body_tenant_b_with_tenant_a_token_is_denied",
            "missing_action_scope_is_denied",
            "decode_detail(&err)",
            "ErrorKind::Policy",
            "detail.policy_decision_id",
        ),
    ),
    TokenCheck(
        "executor utility validation helpers use typed field violations",
        "src/runtime/executor_utils.rs",
        (
            "fn executor_utils_invalid_field(",
            "pub(crate) fn json_i64(",
            "pub(crate) fn json_f64(",
            "pub(crate) fn json_required_str",
            "pub(crate) fn json_required_f32_vec",
            "pub(crate) fn object_bytes_from_json",
            "pub(crate) fn validate_identifier",
            "pub(crate) fn parse_sql_dispatch",
            '"expected integer, got {value}"',
            '"expected number, got {value}"',
            '"{key} is required"',
            '"{key} must be an array"',
            '"{key} must not be empty"',
            '"{key} must contain only numbers"',
            '"invalid object base64: {err}"',
            '"object bytes are required as data_base64, content_base64, data_text, or content_text"',
            '"{label} \'{value}\' is not a valid SQL identifier"',
            '"invalid dispatch JSON: {e}"',
            '"missing `sql` in dispatch request"',
            "shared_json_validation_helpers_carry_field_violations",
            "shared_dispatch_validation_helpers_carry_field_violations",
            "fn decode_detail(status: &tonic::Status) -> ErrorDetail",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"value"',
            '"collection"',
            '"vector"',
            '"data_base64"',
            '"object_bytes"',
            '"schema"',
            '"request_json"',
            '"sql"',
            '"plan"',
        ),
    ),
    TokenCheck(
        "authz optimistic-concurrency aborts use typed retry detail",
        "src/runtime/service/auth_service/authz/governance_activate.rs",
        (
            "crate::runtime::executor_utils::retryable_aborted_status",
            '"authz"',
            '"policy version expected revision"',
            '"live policy revision"',
            '"live relationship revision"',
            '"canary policy version expected revision"',
            '"canary expected revision"',
        ),
    ),
    TokenCheck(
        "authz activation lifecycle denials use typed policy detail",
        "src/runtime/service/auth_service/authz/governance_activate.rs",
        (
            "fn activation_policy_status(",
            "crate::runtime::executor_utils::policy_status",
            "fn policy_version_not_activatable_status(",
            "fn rollback_target_required_status(",
            "fn policy_version_not_canariable_status(",
            "fn canary_not_active_status(",
            "fn canary_not_promote_eligible_status(",
            '"policy_version_activate"',
            '"policy_version_rollback"',
            '"policy_canary_activate"',
            '"policy_canary_promote"',
            '"policy_version_not_activatable"',
            '"rollback_target_required"',
            '"policy_version_not_canariable"',
            '"canary_not_active"',
            '"canary_not_promote_eligible"',
            "return Err(policy_version_not_activatable_status(state));",
            "return Err(rollback_target_required_status());",
            "return Err(policy_version_not_canariable_status(state));",
            "return Err(canary_not_active_status(st));",
            "return Err(canary_not_promote_eligible_status());",
            "fn assert_policy_detail(",
            "activation_lifecycle_denials_carry_policy_detail",
            "ErrorKind::Policy",
            "assert_eq!(detail.policy_decision_id, policy_decision_id);",
        ),
    ),
    TokenCheck(
        "authz activation request validation uses typed field violations",
        "src/runtime/service/auth_service/authz/governance_activate.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn activation_required_field(",
            "fn activation_field_violation(",
            "fn validate_unique_document_policy_ids(",
            "fn validate_canary_scope(",
            '"policy_version_id is required"',
            '"policy_set_id is required"',
            '"canary_id is required"',
            '"duplicate policy id in version document: {}"',
            '"duplicate policy id {}"',
            '"canary scope_values must be non-empty for NODE/TENANT scope"',
            '"must be non-empty for NODE or TENANT canary scope"',
            '"canary PERCENT scope must be 1..=100 (0 includes nobody)"',
            '"must be in the range 1..=100"',
            '"must be a non-empty policy version id"',
            '"must be a non-empty policy set id"',
            '"must be a non-empty policy canary id"',
            "activate_policy_version_missing_policy_version_id_carries_field_violation",
            "rollback_policy_version_missing_policy_set_id_carries_field_violation",
            "activate_canary_missing_policy_version_id_carries_field_violation",
            "promote_canary_missing_canary_id_carries_field_violation",
            "get_canary_status_missing_canary_id_carries_field_violation",
            "duplicate_policy_id_validation_carries_field_violation",
            "canary_scope_validation_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"policies.id"',
            '"scope_values"',
            '"scope_percent"',
        ),
    ),
    TokenCheck(
        "authz simulation request validation uses typed field violations",
        "src/runtime/service/auth_service/authz/governance_sim.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn simulation_required_field(",
            '"test_case is required"',
            '"tenant_id is required"',
            '"must include a simulation case to explain"',
            '"must be a non-empty tenant id"',
            "explain_policy_missing_test_case_carries_field_violation",
            "seed_builtin_roles_missing_tenant_id_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
        ),
    ),
    TokenCheck(
        "authz tuple request validation uses typed field violations",
        "src/runtime/service/auth_service/authz/tuples.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn authz_tuple_invalid_fields",
            '"binding is required"',
            '"binding subject and role are required"',
            '"binding tenant or project is required"',
            '"tuple is required"',
            '"tuple subject, relation, and object are required"',
            '"tuple tenant or project is required"',
            '"must include a role binding"',
            '"must be a non-empty binding subject"',
            '"must be a non-empty role"',
            '"must include tenant or project scope for the binding"',
            '"must include a relationship tuple"',
            '"must be a non-empty tuple subject"',
            '"must be a non-empty tuple relation"',
            '"must be a non-empty tuple object"',
            '"must include tenant or project scope for the tuple"',
            "put_role_binding_missing_binding_carries_field_violation",
            "put_role_binding_missing_identity_carries_field_violations",
            "put_role_binding_missing_scope_carries_field_violations",
            "put_relationship_missing_tuple_carries_field_violation",
            "put_relationship_missing_identity_carries_field_violations",
            "put_relationship_missing_scope_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            "assert_validation_fields(",
        ),
    ),
    TokenCheck(
        "Authn native runtime capability uses typed capability detail",
        "src/runtime/service/auth_service/authn/mod.rs",
        (
            "fn authn_capability_status(",
            "crate::runtime::executor_utils::capability_status",
            '"authn"',
            '"native_entity_dispatch"',
            '"runtime_native_entity_dispatch"',
            "this operation requires the native typed authn runtime",
            '"webauthn_credentials"',
            '"native_postgres_auth_store"',
            "WebAuthn requires the native Postgres auth store to persist passkeys and challenges",
            "authn_missing_runtime_capability_carries_typed_detail",
            "ErrorKind::Capability",
        ),
    ),
    TokenCheck(
        "Authn native authz explicit denials use typed permission policy detail",
        "src/runtime/service/auth_service/authn/mod.rs",
        (
            "fn authn_permission_policy_status(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "tonic::Code::PermissionDenied",
            "fn native_authz_denied_status(",
            '"authn_native_rpc_authorize"',
            "decision.matched_policy_ids.is_empty()",
            "return Ok(decision.decision_id);",
            "Err(native_authz_denied_status(",
            "decision.decision_id.clone()",
            "decision.deny_reason",
            "native_authz_denial_carries_permission_policy_detail",
            "assert_permission_policy_detail(",
            "ErrorKind::Policy",
        ),
    ),
    TokenCheck(
        "Authn WebAuthn feature-disabled fallback uses typed capability detail",
        "src/runtime/service/auth_service/authn/mod.rs",
        (
            "fn webauthn_feature_required_status() -> Status",
            "authn_capability_status(",
            '"webauthn_rpc"',
            '"webauthn_feature"',
            "WebAuthn requires building UDB with the `webauthn` feature",
            "start_web_authn_registration",
            "finish_web_authn_registration",
            "start_web_authn_authentication",
            "finish_web_authn_authentication",
            "webauthn_feature_required_status_carries_capability_detail",
            "ErrorKind::Capability",
            'detail.backend, "authn"',
            "detail.capability_required",
        ),
    ),
    TokenCheck(
        "Authn WebAuthn RP/config setup uses typed capability detail",
        "src/runtime/service/auth_service/authn/mod.rs",
        (
            "fn webauthn_config_capability_status(",
            "authn_capability_status(operation, capability_required, message)",
            '"webauthn_relying_party_config"',
            '"webauthn_rp_config"',
            '"webauthn_https_origin"',
            '"webauthn_production_rp_host"',
            '"webauthn_softauth_disabled"',
            '"webauthn_policy_config"',
            '"webauthn_policy_builder_support"',
            '"webauthn_origin_url"',
            '"webauthn_config_builder"',
            "WebAuthn requires UDB_WEBAUTHN_RP_ID and UDB_WEBAUTHN_ORIGIN",
            "production WebAuthn requires an https UDB_WEBAUTHN_ORIGIN",
            "production WebAuthn RP id/origin must not be localhost/127.0.0.1",
            "production WebAuthn must not enable UDB_WEBAUTHN_TEST_MODE",
            "requested WebAuthn attestation/resident-key/user-verification policy",
            "invalid WebAuthn origin: {err}",
            "invalid WebAuthn RP config: {err:?}",
            "invalid WebAuthn config: {err:?}",
            "webauthn_config_statuses_carry_capability_detail",
            "ErrorKind::Capability",
            "detail.capability_required",
        ),
    ),
    TokenCheck(
        "Authn WebAuthn attestation trust setup uses typed capability detail",
        "src/runtime/service/auth_service/authn/mod.rs",
        (
            "fn webauthn_attestation_capability_status(",
            "authn_capability_status(operation, capability_required, message)",
            "fn webauthn_attestation_roots_status(",
            '"webauthn_attestation_trust"',
            '"webauthn_attestation_roots"',
            "WebAuthn policy: attestation format '{fmt}' requires configured trust roots",
            "WebAuthn policy: read attestation roots PEM failed from {path}: {err}",
            "WebAuthn policy: parse attestation roots PEM failed: {err}",
            "UDB_WEBAUTHN_ATTESTATION_ROOTS_PEM",
            "UDB_WEBAUTHN_ATTESTATION_ROOTS_PEM_PATH",
            "demanded_attestation_without_trust_roots_carries_capability_detail",
            "attestation_roots_setup_failures_carry_capability_detail",
            "assert_attestation_capability_detail(",
            "typed detail trailer is present",
            "ErrorKind::Capability",
            "detail.capability_required",
        ),
    ),
    TokenCheck(
        "Authn WebAuthn attestation crypto/trust refusals use typed detail",
        "src/runtime/service/auth_service/authn/mod.rs",
        (
            "fn webauthn_attestation_crypto_status(",
            '"webauthn_attestation_crypto"',
            "fn webauthn_attestation_chain_untrusted_status(",
            '"webauthn_attestation_chain_not_trusted"',
            "WebAuthn policy: create attestation chain stack failed: {err}",
            "WebAuthn policy: create packed attestation verifier failed: {err}",
            "WebAuthn policy: create tpm attestation verifier failed: {err}",
            "WebAuthn policy: create android-key attestation verifier failed: {err}",
            "WebAuthn policy: create fido-u2f attestation verifier failed: {err}",
            "WebAuthn policy: attestation certificate chain is not trusted",
            "attestation_crypto_failures_carry_capability_detail",
            "attestation_untrusted_chain_carries_policy_detail",
            "assert_attestation_capability_detail(",
            "assert_webauthn_policy_detail(",
            "ErrorKind::Capability",
            "ErrorKind::Policy",
        ),
    ),
    TokenCheck(
        "Authn native Postgres store capability uses typed capability detail",
        "src/runtime/service/auth_service/authn/lifecycle.rs",
        (
            "authn_capability_status",
            '"postgres_auth_store"',
            '"native_postgres_auth_store"',
            "this operation requires the native Postgres auth store",
            "authn_missing_postgres_store_capability_carries_typed_detail",
            "ErrorKind::Capability",
        ),
    ),
    TokenCheck(
        "Authn refresh-family store capability uses typed capability detail",
        "src/runtime/service/auth_service/authn/token_family.rs",
        (
            "authn_capability_status",
            '"refresh_token_rotation"',
            '"native_postgres_auth_store"',
            "refresh-token rotation requires the native Postgres auth store",
            "refresh_family_missing_postgres_store_capability_carries_typed_detail",
            "ErrorKind::Capability",
        ),
    ),
    TokenCheck(
        "Authn OIDC feature-disabled fallback uses typed capability detail",
        "src/runtime/service/auth_service/authn/login.rs",
        (
            "fn oidc_feature_required_status() -> Status",
            "authn_capability_status(",
            '"oidc_authentication"',
            '"oidc_feature"',
            "OIDC authentication requires building UDB with the `oidc` feature",
            "oidc_feature_required_status_carries_capability_detail",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Capability",
            'detail.backend, "authn"',
            "detail.capability_required",
        ),
    ),
    TokenCheck(
        "Authn OIDC/passkey failed preconditions use typed detail",
        "src/runtime/service/auth_service/authn/mod.rs",
        (
            "fn authn_policy_status(",
            "crate::runtime::executor_utils::policy_status",
            "fn oidc_provider_disabled_status(",
            '"oidc_authenticate"',
            '"identity_provider_disabled"',
            "identity provider is disabled for this tenant",
            "fn oidc_jwks_url_invalid_status(",
            '"oidc_provider_config"',
            '"oidc_jwks_url"',
            "configured OIDC jwks_url is invalid: {err}",
            "fn webauthn_passkeys_required_status(",
            '"webauthn_authentication"',
            '"webauthn_passkey_required"',
            "user has no registered WebAuthn passkeys",
            "authn_oidc_and_passkey_preconditions_carry_typed_detail",
            "assert_policy_detail(",
            "assert_capability_detail(",
            "ErrorKind::Policy",
            "ErrorKind::Capability",
        ),
    ),
    TokenCheck(
        "Authn core/lifecycle lookup misses use typed schema detail",
        "src/runtime/service/auth_service/authn/mod.rs",
        (
            "fn authn_schema_not_found_status(",
            "crate::runtime::executor_utils::schema_status",
            "tonic::Code::NotFound",
            '"authn"',
            "fn authn_user_not_found_status() -> Status",
            '"user_lookup"',
            '"authn_user_not_found"',
            "fn authn_otp_not_found_status() -> Status",
            '"otp_lookup"',
            '"authn_otp_not_found"',
            "fn authn_device_not_found_status() -> Status",
            '"device_revoke"',
            '"authn_device_not_found_or_already_revoked"',
            "fn assert_schema_not_found_detail(",
            "authn_lookup_misses_carry_typed_schema_detail",
            "authn_user_not_found_status()",
            "authn_otp_not_found_status()",
            "authn_device_not_found_status()",
            "ErrorKind::Schema",
            "assert_eq!(status.code(), tonic::Code::NotFound);",
            "assert_eq!(detail.backend, \"authn\");",
        ),
    ),
    TokenCheck(
        "Authn core user lookup miss call sites use typed schema helper",
        "src/runtime/service/auth_service/authn/core.rs",
        (
            ".ok_or_else(super::authn_user_not_found_status)?;",
            "get_user_impl",
            "update_user_impl",
            "change_user_status_impl",
            "admin_reset_password_impl",
        ),
    ),
    TokenCheck(
        "Authn lifecycle user/device lookup miss call sites use typed schema helpers",
        "src/runtime/service/auth_service/authn/lifecycle.rs",
        (
            ".ok_or_else(super::authn_user_not_found_status)?;",
            "return Err(super::authn_device_not_found_status());",
            "authorize_target_user",
            "revoke_device_impl",
            "issue_mfa_challenge_impl",
            "list_mfa_factors_impl",
            "disable_mfa_factor_impl",
            "admin_reset_mfa_impl",
        ),
    ),
    TokenCheck(
        "Authn WebAuthn lookup miss call sites use typed schema helper",
        "src/runtime/service/auth_service/authn/mod.rs",
        (
            ".ok_or_else(authn_user_not_found_status)?;",
            "start_webauthn_registration_impl",
            "finish_webauthn_registration_impl",
            "start_webauthn_authentication_impl",
            "finish_webauthn_authentication_impl",
        ),
    ),
    TokenCheck(
        "Authn login lookup miss call sites use typed schema helpers",
        "src/runtime/service/auth_service/authn/login.rs",
        (
            ".ok_or_else(super::authn_user_not_found_status)?;",
            ".ok_or_else(super::authn_otp_not_found_status)?;",
            "change_password_impl",
            "reset_password_impl",
        ),
    ),
    TokenCheck(
        "Authn MFA lookup miss call sites use typed schema helpers",
        "src/runtime/service/auth_service/authn/mfa.rs",
        (
            ".ok_or_else(super::authn_user_not_found_status)?;",
            ".ok_or_else(super::authn_otp_not_found_status)?;",
            "send_otp_impl",
            "resend_otp_impl",
            "enroll_mfa_impl",
            "confirm_mfa_enrollment_impl",
            "generate_recovery_codes_impl",
            "send_phone_verification_impl",
        ),
    ),
    TokenCheck(
        "Authn session lookup miss call site uses typed schema helper",
        "src/runtime/service/auth_service/authn/sessions.rs",
        (
            ".ok_or_else(super::authn_user_not_found_status)?;",
            "authorize_list_sessions_target",
        ),
    ),
    TokenCheck(
        "backend plugin dispatch instance misses use typed capability detail",
        "src/backend/plugins/mod.rs",
        (
            "fn dispatch_instance_not_configured_status(",
            "crate::runtime::executor_utils::capability_status",
            '"dispatch_executor"',
            '"configured_instance"',
            "dispatch_instance_not_configured_carries_capability_detail",
            "typed detail trailer is present",
            "ErrorDetail::decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Capability",
            'detail.backend, "mysql"',
            'detail.operation, "dispatch_executor"',
            'detail.capability_required, "configured_instance"',
        ),
    ),
    TokenCheck(
        "Authn server-side session capability uses typed capability detail",
        "src/runtime/service/auth_service/authn/sessions.rs",
        (
            "authn_capability_status",
            '"session_creation"',
            '"create_session"',
            '"server_side_sessions"',
            "sessions disabled (set UDB_SESSION_ENABLED and UDB_SESSION_HASH_SECRET)",
            "create_session_disabled_carries_capability_detail",
            "create_login_session_disabled_carries_capability_detail",
            "ErrorKind::Capability",
            'detail.backend, "authn"',
            "detail.capability_required",
        ),
    ),
    TokenCheck(
        "ControlPlaneService missing store capability uses typed capability detail",
        "src/runtime/service/auth_service/control_plane/mod.rs",
        (
            "crate::runtime::executor_utils::capability_status",
            '"control_plane"',
            '"postgres_store"',
            "control-plane service requires a Postgres-backed store (no PG pool configured)",
            "fn require_pool(&self) -> Result<&PgPool, Status>",
        ),
    ),
    TokenCheck(
        "ControlPlaneService missing store capability decoder test",
        "src/runtime/service/auth_service/control_plane/tests.rs",
        (
            "control_plane_missing_postgres_store_capability_carries_typed_detail",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Capability",
            '"control_plane"',
            '"postgres_store"',
        ),
    ),
    TokenCheck(
        "ControlPlaneService rollback missing target uses typed policy detail",
        "src/runtime/service/auth_service/control_plane/mod.rs",
        (
            "fn rollback_target_required_status() -> Status",
            "crate::runtime::executor_utils::policy_status",
            '"control_plane_rollback"',
            '"rollback_target_required"',
            "no retained snapshot to roll back to for this (node, resource_type, target_version)",
            "ok_or_else(Self::rollback_target_required_status)?",
        ),
    ),
    TokenCheck(
        "ControlPlaneService rollback missing target decoder test",
        "src/runtime/service/auth_service/control_plane/tests.rs",
        (
            "control_plane_rollback_missing_target_carries_policy_detail",
            "ControlPlaneServiceImpl::rollback_target_required_status()",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Policy",
            '"control_plane_rollback"',
            '"rollback_target_required"',
        ),
    ),
    TokenCheck(
        "ControlPlaneService AckStatus node-state miss uses typed schema detail",
        "src/runtime/service/auth_service/control_plane/mod.rs",
        (
            "fn node_state_not_found_status() -> Status",
            "crate::runtime::executor_utils::schema_status",
            "tonic::Code::NotFound",
            '"control_plane"',
            '"AckStatus"',
            '"node_state_not_found"',
            "no node state for this (node, resource_type)",
            "ok_or_else(Self::node_state_not_found_status)?",
        ),
    ),
    TokenCheck(
        "ControlPlaneService AckStatus node-state miss decoder test",
        "src/runtime/service/auth_service/control_plane/tests.rs",
        (
            "control_plane_node_state_not_found_carries_schema_detail",
            "ControlPlaneServiceImpl::node_state_not_found_status()",
            "assert_schema_detail(",
            "ErrorKind::Schema",
            '"control_plane"',
            '"AckStatus"',
            '"node_state_not_found"',
        ),
    ),
    TokenCheck(
        "IdentityProviderService missing store capability uses typed capability detail",
        "src/runtime/service/auth_service/idp/mod.rs",
        (
            "crate::runtime::executor_utils::capability_status",
            '"identity_provider"',
            '"postgres_store"',
            "identity-provider service requires a Postgres-backed store (no PG pool configured)",
            "fn require_pool(&self) -> Result<&PgPool, Status>",
        ),
    ),
    TokenCheck(
        "IdentityProviderService missing store capability decoder test",
        "src/runtime/service/auth_service/idp/tests.rs",
        (
            "idp_missing_postgres_store_capability_carries_typed_detail",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Capability",
            '"identity_provider"',
            '"postgres_store"',
        ),
    ),
    TokenCheck(
        "IdentityProviderService lookup misses use typed schema detail",
        "src/runtime/service/auth_service/idp/mod.rs",
        (
            "fn idp_schema_not_found_status(",
            "crate::runtime::executor_utils::schema_status",
            "tonic::Code::NotFound",
            '"identity_provider"',
            "fn idp_provider_not_found_status() -> Status",
            '"provider_lookup"',
            '"identity_provider_not_found"',
            "fn idp_scim_user_not_found_status() -> Status",
            '"scim_user_lookup"',
            '"scim_user_not_found"',
            "fn idp_scim_group_not_found_status() -> Status",
            '"scim_group_mapping_lookup"',
            '"scim_group_not_found"',
            ".ok_or_else(idp_provider_not_found_status)",
            ".ok_or_else(idp_scim_user_not_found_status)",
            "return Err(idp_scim_group_not_found_status());",
        ),
    ),
    TokenCheck(
        "IdentityProviderService lookup miss decoder test",
        "src/runtime/service/auth_service/idp/tests.rs",
        (
            "fn assert_schema_not_found_detail(",
            "idp_lookup_misses_carry_typed_schema_detail",
            "idp_provider_not_found_status()",
            "idp_scim_user_not_found_status()",
            "idp_scim_group_not_found_status()",
            "ErrorKind::Schema",
            '"identity_provider"',
            '"provider_lookup"',
            '"identity_provider_not_found"',
            '"scim_user_lookup"',
            '"scim_user_not_found"',
            '"scim_group_mapping_lookup"',
            '"scim_group_not_found"',
            "assert_eq!(status.code(), tonic::Code::NotFound);",
            "assert_eq!(detail.capability_required, schema_code);",
        ),
    ),
    TokenCheck(
        "IdentityProviderService provider/SAML setup capability uses typed capability detail",
        "src/runtime/service/auth_service/idp/mod.rs",
        (
            "fn idp_capability_status(",
            "crate::runtime::executor_utils::capability_status",
            '"identity_provider"',
            '"provider_login"',
            '"provider_enabled"',
            '"saml_login"',
            '"saml_sso_url"',
            '"metadata_fetch"',
            '"saml_metadata_url"',
            "identity provider '{display_name}' is disabled",
            "identity provider is disabled",
            "provider has no SAML SSO URL; import metadata first",
            "metadata fetch failed: {err}",
        ),
    ),
    TokenCheck(
        "IdentityProviderService provider/SAML setup capability decoder test",
        "src/runtime/service/auth_service/idp/tests.rs",
        (
            "idp_saml_provider_setup_capabilities_carry_typed_detail",
            "idp_provider_disabled_status",
            "idp_provider_disabled_static_status",
            "idp_saml_sso_url_missing_status",
            "idp_metadata_fetch_failed_status",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Capability",
            '"provider_enabled"',
            '"saml_sso_url"',
            '"saml_metadata_url"',
            "detail.capability_required",
        ),
    ),
    TokenCheck(
        "IdentityProviderService local policy denials use typed policy detail",
        "src/runtime/service/auth_service/idp/mod.rs",
        (
            "fn idp_policy_status(",
            "crate::runtime::executor_utils::policy_status",
            "fn idp_permission_policy_status(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "tonic::Code::PermissionDenied",
            "fn idp_scim_group_mapping_required_status(",
            "fn idp_account_linking_explicit_required_status(",
            "fn idp_saml_replay_rejected_status() -> Status",
            "fn idp_jit_provisioning_rejected_status(reason: impl std::fmt::Display) -> Status",
            '"scim_create_group"',
            '"scim_group_mapping_required"',
            '"idp_account_linking"',
            '"explicit_link_required"',
            '"saml_acs"',
            '"saml_assertion_replay"',
            '"idp_jit_provisioning"',
            '"jit_policy_rejected"',
            "groups are mapping-driven and not persisted",
            "an account with this email exists; explicit account linking is required",
            "SAML assertion has already been consumed (replay rejected)",
            "JIT provisioning rejected: {reason}",
            "return Err(idp_scim_group_mapping_required_status());",
            "return Err(idp_account_linking_explicit_required_status());",
            "return Err(idp_saml_replay_rejected_status());",
            "return Err(idp_jit_provisioning_rejected_status(reason));",
        ),
    ),
    TokenCheck(
        "IdentityProviderService local policy denials decoder test",
        "src/runtime/service/auth_service/idp/tests.rs",
        (
            "fn assert_policy_detail(",
            "fn assert_permission_policy_detail(",
            "idp_scim_group_mapping_policy_carries_typed_detail",
            "idp_saml_and_jit_permission_denials_carry_typed_detail",
            "idp_scim_group_mapping_required_status()",
            "idp_account_linking_explicit_required_status()",
            "idp_saml_replay_rejected_status()",
            "idp_jit_provisioning_rejected_status(",
            "ErrorKind::Policy",
            '"scim_create_group"',
            '"scim_group_mapping_required"',
            '"idp_account_linking"',
            '"explicit_link_required"',
            '"saml_acs"',
            '"saml_assertion_replay"',
            '"idp_jit_provisioning"',
            '"jit_policy_rejected"',
            "assert_eq!(detail.policy_decision_id, policy_decision_id);",
        ),
    ),
    TokenCheck(
        "SAML HTTP denial rendering uses typed IdP replay status",
        "src/runtime/service/auth_service/idp/saml_http.rs",
        (
            "status_response(super::idp_saml_replay_rejected_status())",
            "body.starts_with(\"HTTP/1.1 403\")",
            "body.contains(\"\\\"authenticated\\\": false\")",
        ),
    ),
    TokenCheck(
        "Authz core missing store capability uses typed capability detail",
        "src/runtime/service/auth_service/authz/mod.rs",
        (
            "fn authz_capability_status(",
            "crate::runtime::executor_utils::capability_status",
            '"authz"',
            '"postgres_auth_store"',
            '"snapshot_fallback"',
            "this operation requires a Postgres-backed auth store (no PG pool configured)",
            "native authz requires a Postgres-backed auth store",
            "authz_missing_store_capabilities_carry_typed_detail",
            "ErrorKind::Capability",
        ),
    ),
    TokenCheck(
        "Authz runtime-backed persistence capability uses typed capability detail",
        "src/runtime/service/auth_service/authz/mod.rs",
        (
            '"runtime_native_entity_dispatch"',
            '"policy_persistence"',
            '"role_persistence"',
            '"tuple_persistence"',
            '"user_role_persistence"',
            "native authz requires runtime-backed policy persistence",
            "native authz requires runtime-backed role persistence",
            "native authz requires runtime-backed tuple persistence",
            "native authz requires runtime-backed user-role persistence",
            "authz_missing_runtime_capabilities_carry_typed_detail",
            "ErrorKind::Capability",
        ),
    ),
    TokenCheck(
        "Authz policy-bundle signing capability uses typed capability detail",
        "src/runtime/service/auth_service/authz/mod.rs",
        (
            "fn policy_bundle_signing_not_configured_status(",
            "authz_capability_status(",
            '"policy_bundle_signing"',
            '"policy_bundle_signing_secret"',
            "policy bundle signing is not configured; set UDB_POLICY_BUNDLE_SECRET",
            "policy_bundle_signing_missing_secret_carries_capability_detail",
            "ErrorKind::Capability",
            "detail.capability_required",
        ),
    ),
    TokenCheck(
        "Authz role/policy/governance not-found denials use typed schema detail",
        "src/runtime/service/auth_service/authz/mod.rs",
        (
            "fn authz_not_found_status(",
            "crate::runtime::executor_utils::schema_status",
            "tonic::Code::NotFound",
            '"role_not_found"',
            '"policy_rule_not_found"',
            '"policy_draft_not_found"',
            '"policy_version_not_found"',
            '"policy_set_not_found"',
            '"policy_canary_not_found"',
            'Err(authz_not_found_status(',
            'return Err(authz_not_found_status(',
            "authz_not_found_denials_carry_schema_detail",
            "assert_schema_detail(",
            "ErrorKind::Schema",
            'detail.backend, "authz"',
        ),
    ),
    TokenCheck(
        "Authz governance store misses use shared typed schema detail",
        "src/runtime/service/auth_service/authz/governance_store.rs",
        (
            "authz_not_found_status(",
            '"load_policy_draft"',
            '"policy_draft_not_found"',
            '"policy draft not found"',
            '"load_policy_version"',
            '"policy_version_not_found"',
            '"policy version not found"',
        ),
    ),
    TokenCheck(
        "Authz governance activation misses use shared typed schema detail",
        "src/runtime/service/auth_service/authz/governance_activate.rs",
        (
            "authz_not_found_status(",
            '"load_policy_set"',
            '"policy_set_not_found"',
            '"policy set not found"',
            '"load_canary"',
            '"policy_canary_not_found"',
            '"canary not found"',
        ),
    ),
    TokenCheck(
        "Authz governed direct mutation denials use typed policy detail",
        "src/runtime/service/auth_service/authz/mod.rs",
        (
            "fn governed_direct_mutation_status(",
            "crate::runtime::executor_utils::policy_status",
            '"authz_governed_direct_mutation"',
            '"put_authz_policy_disabled"',
            '"create_policy_rule_disabled"',
            "governed mode: direct {rpc} is disabled",
            "return Err(governed_direct_mutation_status(",
            "governed_direct_mutation_denials_carry_policy_detail",
            "ErrorKind::Policy",
            "assert_eq!(detail.policy_decision_id, policy_decision_id);",
        ),
    ),
    TokenCheck(
        "Authz tuple governed direct mutation denials use typed policy detail",
        "src/runtime/service/auth_service/authz/tuples.rs",
        (
            "governed_direct_mutation_status(",
            '"PutRoleBinding"',
            '"put_role_binding_disabled"',
            '"PutRelationship"',
            '"put_relationship_disabled"',
        ),
    ),
    TokenCheck(
        "Authz governed role mutation denials use typed policy detail",
        "src/runtime/service/auth_service/authz/governance.rs",
        (
            "fn governed_role_mutation_status(",
            "crate::runtime::executor_utils::policy_status",
            '"authz_governed_role_mutation"',
            '"role_mutation_requires_governance"',
            "return Err(governed_role_mutation_status(rpc));",
            "governed_role_mutation_denial_carries_policy_detail",
            "ErrorKind::Policy",
            "detail.policy_decision_id",
        ),
    ),
    TokenCheck(
        "Authz governance authorization denials use typed policy detail",
        "src/runtime/service/auth_service/authz/governance.rs",
        (
            "fn governance_permission_status(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "tonic::Code::PermissionDenied",
            "fn governance_actor_required_status(",
            "fn governance_impersonation_denied_status(",
            "fn break_glass_reason_required_status(",
            "fn break_glass_ttl_invalid_status(",
            "fn governance_scope_required_status(",
            "fn governance_policy_denied_status(",
            '"governance_actor_required"',
            '"governance_impersonation_scope_required"',
            '"break_glass_reason_required"',
            '"break_glass_ttl_invalid"',
            '"governance_scope_required"',
            '"governance_policy_denied"',
            "governance_actor_required_status(rpc)",
            "governance_impersonation_denied_status(",
            "break_glass_reason_required_status(rpc)",
            "break_glass_ttl_invalid_status(rpc)",
            "governance_scope_required_status(rpc, required_scopes)",
            "governance_policy_denied_status(",
            "governance_authorization_denials_carry_policy_detail",
            "assert_permission_policy_detail(",
            "ErrorKind::Policy",
            "detail.policy_decision_id",
        ),
    ),
    TokenCheck(
        "Authz governance runtime-backed persistence capability uses typed capability detail",
        "src/runtime/service/auth_service/authz/governance_store.rs",
        (
            "authz_capability_status",
            '"draft_persistence"',
            '"runtime_native_entity_dispatch"',
            "native authz requires runtime-backed draft persistence",
        ),
    ),
    TokenCheck(
        "Authz governance store internals use typed internal detail",
        "src/runtime/service/auth_service/authz/governance_store.rs",
        (
            "fn governance_store_internal_status(",
            'crate::runtime::executor_utils::internal_status("authz", operation, message)',
            '"persist_draft_document"',
            '"load_draft"',
            '"load_policy_set"',
            '"load_version"',
            '"load_approval"',
            '"promote_draft_to_version"',
            '"decode_governance_row"',
            '"persist draft document failed: {err}"',
            '"load draft failed: {err}"',
            '"load policy set failed: {err}"',
            '"load version failed: {err}"',
            '"load approval failed: {err}"',
            '"promote draft to version failed: {err}"',
            '"decode governance row failed: {e}"',
            "governance_store_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "authz"',
        ),
    ),
    TokenCheck(
        "Authz policy-set runtime-backed persistence capability uses typed capability detail",
        "src/runtime/service/auth_service/authz/governance_drafts.rs",
        (
            "authz_capability_status",
            '"policy_set_persistence"',
            '"runtime_native_entity_dispatch"',
            "native authz requires runtime-backed policy-set persistence",
        ),
    ),
    TokenCheck(
        "Authz governance draft internals use typed internal detail",
        "src/runtime/service/auth_service/authz/governance_drafts.rs",
        (
            "fn governance_draft_internal_status(",
            'crate::runtime::executor_utils::internal_status("authz", operation, message)',
            '"ensure_policy_set"',
            '"create_policy_draft"',
            '"update_policy_draft"',
            '"submit_policy_draft"',
            '"record_policy_approval"',
            '"update_draft_status"',
            '"ensure policy set failed: {err}"',
            '"ensure policy set returned no id"',
            '"create policy draft failed: {err}"',
            '"update policy draft failed: {err}"',
            '"submit policy draft failed: {err}"',
            '"record approval failed: {err}"',
            '"update draft status failed: {err}"',
            "governance_draft_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "authz"',
        ),
    ),
    TokenCheck(
        "Authz policy-draft lifecycle denials use typed policy detail",
        "src/runtime/service/auth_service/authz/governance_drafts.rs",
        (
            "fn governance_policy_status(",
            "crate::runtime::executor_utils::policy_status",
            "fn governance_policy_status_with_code(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "fn draft_not_editable_status(",
            "fn draft_not_editable_static_status(",
            "fn draft_not_submittable_status(",
            "fn draft_not_reviewable_status(",
            "fn draft_separation_of_duties_status() -> Status",
            '"policy_draft_update"',
            '"policy_draft_submit"',
            '"policy_draft_review"',
            '"draft_not_editable"',
            '"draft_not_submittable"',
            '"draft_not_reviewable"',
            '"separation_of_duties"',
            "tonic::Code::PermissionDenied",
            "return Err(draft_not_editable_status(&draft.status));",
            "return Err(draft_not_editable_static_status());",
            "return Err(draft_not_submittable_status(&draft.status));",
            "return Err(draft_not_reviewable_status(&draft.status));",
            "return Err(draft_separation_of_duties_status());",
            "fn assert_policy_detail(",
            "fn assert_permission_policy_detail(",
            "policy_draft_lifecycle_denials_carry_policy_detail",
            "policy_draft_separation_of_duties_carries_permission_policy_detail",
            "ErrorKind::Policy",
            "assert_eq!(detail.policy_decision_id, policy_decision_id);",
        ),
    ),
    TokenCheck(
        "Authz revision runtime-backed persistence capability uses typed capability detail",
        "src/runtime/service/auth_service/authz/governance.rs",
        (
            "authz_capability_status",
            '"revision_persistence"',
            '"runtime_native_entity_dispatch"',
            "native authz requires runtime-backed revision persistence",
        ),
    ),
    TokenCheck(
        "Authz canary runtime-backed persistence capability uses typed capability detail",
        "src/runtime/service/auth_service/authz/governance_activate.rs",
        (
            "authz_capability_status",
            '"canary_persistence"',
            '"runtime_native_entity_dispatch"',
            "native authz requires runtime-backed canary persistence",
        ),
    ),
    TokenCheck(
        "Authz governance activation internals use typed internal detail",
        "src/runtime/service/auth_service/authz/governance_activate.rs",
        (
            "fn activation_internal_status(",
            'crate::runtime::executor_utils::internal_status("authz", operation, message)',
            '"activation_tx_begin"',
            '"clear_policies"',
            '"insert_policy"',
            '"clear_tuples"',
            '"insert_grouping_tuple"',
            '"insert_relationship_tuple"',
            '"supersede_prior_version"',
            '"activate_version"',
            '"update_policy_set_pointers"',
            '"activation_tx_commit"',
            '"read_active_version"',
            '"create_canary"',
            '"create_canary_id_decode"',
            '"load_canary"',
            '"list_active_canaries"',
            '"update_canary_state"',
            '"read_node_state_ledger"',
            '"activation tx begin failed: {err}"',
            '"clear policies failed: {err}"',
            '"insert policy failed: {err}"',
            '"clear tuples failed: {err}"',
            '"insert grouping tuple failed: {err}"',
            '"insert relationship tuple failed: {err}"',
            '"supersede prior version failed: {err}"',
            '"activate version failed: {err}"',
            '"update policy set pointers failed: {err}"',
            '"activation tx commit failed: {err}"',
            '"read active version failed: {err}"',
            '"create canary failed: {err}"',
            '"create canary returned no id: {err}"',
            '"load canary failed: {err}"',
            '"list active canaries failed: {err}"',
            '"update canary state failed: {err}"',
            '"read node-state ledger failed: {err}"',
            "activation_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "authz"',
        ),
    ),
    TokenCheck(
        "Authz tuple service runtime-backed persistence capability uses typed capability detail",
        "src/runtime/service/auth_service/authz/tuples.rs",
        (
            "authz_capability_status",
            '"tuple_persistence"',
            '"runtime_native_entity_dispatch"',
            "native authz requires runtime-backed tuple persistence",
        ),
    ),
    TokenCheck(
        "authz core/admin request validation uses typed field violations",
        "src/runtime/service/auth_service/authz/mod.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn authz_invalid_fields",
            "fn authz_required_field(",
            '"policy is required"',
            '"policy id is required"',
            '"policy effect must be \'allow\' or \'deny\', got \'{}\'"',
            '"user_id is required"',
            '"object is required"',
            '"action is required"',
            '"name is required"',
            '"created_by is required"',
            '"assigned_by is required"',
            '"updated_by is required"',
            '"deleted_by is required"',
            '"tenant_id or domain is required"',
            '"tenant_id is required"',
            '"tenant_id is required for a policy bundle"',
            '"subject is required"',
            '"domain is required"',
            '"role_id or role_code is required"',
            '"policy_id is required"',
            '"user_role_id is required"',
            '"user_id (or principal_id) and role_id are required"',
            '"group role bindings require an explicit principal_id (IdP/SCIM group mapping)"',
            '"must include an authz policy"',
            '"must be a non-empty policy id"',
            '"must be either \'allow\' or \'deny\'"',
            '"must be a non-empty user id"',
            '"must be a non-empty object"',
            '"must be a non-empty action"',
            '"must be a non-empty role name"',
            '"must be a non-empty creator id"',
            '"must be a non-empty assigner id"',
            '"must be a non-empty updater id"',
            '"must be a non-empty deleter id"',
            '"must include tenant_id or a tenant/project/resource domain"',
            '"must be a non-empty policy subject"',
            '"must be a non-empty policy domain"',
            '"must include role_id or role_code"',
            '"must be a non-empty user-role assignment id"',
            '"must be a non-empty tenant id"',
            '"must be a non-empty tenant id for a policy bundle"',
            '"must include user_id or principal_id for the role binding"',
            '"must be explicit for group role bindings"',
            "put_authz_policy_missing_policy_carries_field_violation",
            "put_authz_policy_invalid_effect_carries_field_violation",
            "check_access_missing_user_id_carries_field_violation",
            "check_access_missing_object_carries_field_violation",
            "check_access_missing_action_carries_field_violation",
            "create_role_missing_name_carries_field_violation",
            "create_role_missing_scope_carries_field_violations",
            "create_role_missing_created_by_carries_field_violation",
            "create_role_invalid_created_by_carries_field_violation",
            "assign_role_missing_identity_carries_field_violations",
            "assign_role_group_missing_principal_id_carries_field_violation",
            "assign_role_missing_assigned_by_carries_field_violation",
            "assign_role_missing_scope_carries_field_violations",
            "create_policy_rule_missing_subject_carries_field_violation",
            "create_policy_rule_missing_effect_carries_field_violation",
            "list_user_permissions_missing_user_id_carries_field_violation",
            "get_role_missing_lookup_carries_field_violations",
            "update_role_missing_updated_by_carries_field_violation",
            "delete_role_missing_deleted_by_carries_field_violation",
            "revoke_role_missing_user_role_id_carries_field_violation",
            "get_policy_rule_missing_policy_id_carries_field_violation",
            "delete_policy_rule_missing_policy_id_carries_field_violation",
            "get_native_access_missing_tenant_id_carries_field_violation",
            "get_policy_bundle_missing_tenant_id_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            "assert_validation_fields(",
        ),
    ),
    TokenCheck(
        "authz role/policy attribution denials use typed policy detail",
        "src/runtime/service/auth_service/authz/mod.rs",
        (
            "fn authz_attribution_policy_status(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "tonic::Code::PermissionDenied",
            "fn created_by_caller_mismatch_status(operation: &'static str) -> Status",
            "fn assigned_by_caller_mismatch_status() -> Status",
            '"create_role"',
            '"create_policy_rule"',
            '"assign_role"',
            '"created_by_caller_mismatch"',
            '"assigned_by_caller_mismatch"',
            "return Err(created_by_caller_mismatch_status(\"create_role\"));",
            "return Err(created_by_caller_mismatch_status(\"create_policy_rule\"));",
            "return Err(assigned_by_caller_mismatch_status());",
            "create_role_created_by_mismatch_carries_policy_detail",
            "assign_role_assigned_by_mismatch_carries_policy_detail",
            "create_policy_rule_created_by_mismatch_carries_policy_detail",
            "fn assert_permission_policy_detail(",
            "ErrorKind::Policy",
            "detail.policy_decision_id",
        ),
    ),
    TokenCheck(
        "authz mapping validation uses typed field violations",
        "src/runtime/service/auth_service/mappings.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            '"policy effect is required"',
            '"effect"',
            '"must be either ALLOW or DENY"',
            "entity_effect_to_runtime",
        ),
    ),
    TokenCheck(
        "authz draft optimistic-concurrency abort uses typed retry detail",
        "src/runtime/service/auth_service/authz/governance_drafts.rs",
        (
            "crate::runtime::executor_utils::retryable_aborted_status",
            '"authz"',
            '"draft expected updated_at"',
            "draft was modified concurrently",
        ),
    ),
    TokenCheck(
        "authz snapshot reload race uses typed retry detail",
        "src/runtime/service/auth_service/authz/mod.rs",
        (
            "crate::runtime::executor_utils::retryable_aborted_status",
            '"authz"',
            '"snapshot reload revision"',
            "authz revision changed while loading snapshot",
        ),
    ),
    TokenCheck(
        "vault secret CAS abort uses typed retry detail",
        "src/runtime/service/vault_service",
        (
            "crate::runtime::executor_utils::retryable_aborted_status",
            '"vault"',
            '"secret version CAS"',
            "CAS conflict: expected version",
        ),
    ),
    TokenCheck(
        "vault request validation uses typed field violations",
        "src/runtime/service/vault_service",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn vault_required_field(",
            "fn vault_field_violation",
            "fn vault_required_secret_path(",
            "fn vault_required_key_name(",
            '"secret_path is required"',
            '"key_name is required"',
            '"not a vault transit ciphertext envelope"',
            '"vault ciphertext envelope is too short"',
            '"role_name must be 1..128 ASCII chars using letters, digits, _, -, :, or ."',
            '"ttl_seconds must be 0/default or at least {MIN_DB_CREDENTIAL_TTL_SECONDS}"',
            '"must be a non-empty vault secret path"',
            '"must be a non-empty vault transit key name"',
            '"must match udb-vault:v<version>:<base64>"',
            '"must be base64-encoded vault transit ciphertext bytes"',
            '"must include a 12-byte nonce and encrypted payload"',
            '"must be 1..128 ASCII chars using letters, digits, _, -, :, or ."',
            '"must be 0/default or at least {MIN_DB_CREDENTIAL_TTL_SECONDS}"',
            "put_secret_missing_secret_path_carries_field_violation",
            "decrypt_missing_key_name_carries_field_violation",
            "decrypt_malformed_ciphertext_carries_field_violation",
            "transit_ciphertext_helpers_carry_field_violations",
            "dynamic_database_credential_validation_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "secret_path"',
            'detail.field_violations[0].field, "key_name"',
            'assert_single_field_violation(&ttl, "ttl_seconds"',
        ),
    ),
    TokenCheck(
        "API-key request validation uses typed field violations",
        "src/runtime/service/auth_service/apikey.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn required_field(",
            '"owner_id is required"',
            '"key_id is required"',
            '"at least one selector is required (prefix/owner/tenant/project/scope/created_before)"',
            '"emergency revoke requires tenant_id or tenant context"',
            '"must be a non-empty API key owner id"',
            '"must be a non-empty API key id"',
            '"must include at least one of key_prefix, owner_id, tenant_id, project_id, scope, or created_before"',
            '"must be supplied directly or by caller tenant context"',
            "create_api_key_missing_owner_id_carries_field_violation",
            "list_api_keys_missing_owner_id_carries_field_violation",
            "rotate_api_key_missing_key_id_carries_field_violation",
            "get_api_key_usage_stats_missing_key_id_carries_field_violation",
            "emergency_revoke_missing_selector_carries_field_violation",
            "emergency_revoke_missing_tenant_context_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
        ),
    ),
    TokenCheck(
        "API-key setup capability uses typed capability detail",
        "src/runtime/service/auth_service/apikey.rs",
        (
            "fn capability_status(",
            "crate::runtime::executor_utils::capability_status",
            '"api_key"',
            '"create_hashing"',
            '"rotate_hashing"',
            '"emergency_revoke"',
            '"usage_stats"',
            '"api_key_hash_secret"',
            '"postgres_backend"',
            '"API key hashing requires UDB_SESSION_HASH_SECRET"',
            '"emergency revoke requires a Postgres backend"',
            '"api-key usage stats require a Postgres backend"',
            "create_api_key_missing_hash_secret_carries_capability_detail",
            "rotate_api_key_missing_hash_secret_carries_capability_detail",
            "emergency_revoke_missing_postgres_carries_capability_detail",
            "get_api_key_usage_stats_missing_postgres_carries_capability_detail",
            "ErrorKind::Capability",
            'detail.backend, "api_key"',
            "detail.capability_required",
        ),
    ),
    TokenCheck(
        "API-key not-found denials use typed schema detail",
        "src/runtime/service/auth_service/apikey.rs",
        (
            "fn api_key_not_found_status(operation: &'static str) -> Status",
            "crate::runtime::executor_utils::schema_status",
            "Code::NotFound",
            '"api_key_not_found"',
            'Self::api_key_not_found_status("get_api_key")',
            'Self::api_key_not_found_status("update_api_key")',
            'Self::api_key_not_found_status("revoke_api_key")',
            'Self::api_key_not_found_status("rotate_api_key")',
            "api_key_not_found_statuses_carry_schema_detail",
            'assert_schema_detail(&err, "get_api_key", "api_key_not_found", "api key not found")',
            "ErrorKind::Schema",
            'detail.backend, "api_key"',
        ),
    ),
    TokenCheck(
        "API-key tenant-boundary denials use typed policy detail",
        "src/runtime/service/auth_service/apikey.rs",
        (
            "fn policy_status_with_code(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "fn tenant_context_required_status() -> Status",
            "fn tenant_mismatch_status() -> Status",
            "fn read_tenant_required_status() -> Status",
            '"api_key_tenant_scope"',
            '"caller_tenant_context_required"',
            '"caller_tenant_mismatch"',
            '"api_key_read_scope"',
            '"tenant_scoped_bearer_required"',
            "return Err(Self::tenant_context_required_status());",
            "return Err(Self::tenant_mismatch_status());",
            "return Err(Self::read_tenant_required_status());",
            "fn assert_policy_detail(",
            "get_api_key_enforces_claim_tenant_after_resolve",
            "enforce_caller_tenant_denies_empty_caller_for_tenant_scoped_key",
            "read_tenant_filter_requires_tenant_scope_with_policy_detail",
            "ErrorKind::Policy",
            "detail.policy_decision_id",
        ),
    ),
    TokenCheck(
        "control-plane request validation uses typed field violations",
        "src/runtime/service/auth_service/control_plane/mod.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn required_field(",
            "fn empty_discovery_stream_status(",
            "fn missing_discovery_node_id_status(",
            "fn empty_delta_stream_status(",
            "fn missing_delta_node_id_status(",
            '"node_id is required"',
            '"resource_type is required"',
            '"empty control discovery stream"',
            '"node_id is required on the first DiscoveryRequest"',
            '"empty control delta stream"',
            '"node_id is required on the first DeltaDiscoveryRequest"',
            '"must be a non-empty control-plane node id"',
            '"must specify a control-plane resource type"',
            '"must include an initial DiscoveryRequest"',
            '"must be a non-empty node id on the first DiscoveryRequest"',
            '"must include an initial DeltaDiscoveryRequest"',
            '"must be a non-empty node id on the first DeltaDiscoveryRequest"',
        ),
    ),
    TokenCheck(
        "control-plane validation decoder tests",
        "src/runtime/service/auth_service/control_plane/tests.rs",
        (
            "get_resources_missing_resource_type_carries_field_violation",
            "ack_status_missing_node_id_carries_field_violation",
            "ack_status_missing_resource_type_carries_field_violation",
            "rollback_resources_missing_node_id_carries_field_violation",
            "stream_resources_empty_stream_status_carries_field_violation",
            "stream_resources_missing_node_id_status_carries_field_violation",
            "delta_resources_empty_stream_status_carries_field_violation",
            "delta_resources_missing_node_id_status_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            'assert_validation_field(&err, "stream", "must include an initial DiscoveryRequest")',
            'assert_validation_field(',
            "missing resource_type must fail before Postgres availability",
            "missing node_id must fail before Postgres availability",
        ),
    ),
    TokenCheck(
        "control-plane store validation uses typed field violations",
        "src/runtime/service/auth_service/control_plane/store.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn control_store_required_field(",
            "fn validate_resource_upsert_input(",
            "fn validate_node_id(",
            '"resource name is required"',
            '"resource_type is required"',
            '"payload_json must be valid JSON"',
            '"node_id is required"',
            '"subscribed_names JSON failed: {err}"',
            '"must be a non-empty control-plane resource name"',
            '"must specify a control-plane resource type"',
            '"must be valid JSON"',
            '"must be a non-empty control-plane node id"',
            '"must be valid subscribed_names JSON"',
            "upsert_resource_missing_name_carries_field_violation",
            "upsert_resource_missing_type_carries_field_violation",
            "upsert_resource_invalid_payload_json_carries_field_violation",
            "ensure_node_state_missing_node_id_carries_field_violation",
            "subscribed_names_json_failure_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
        ),
    ),
    TokenCheck(
        "typed store RPC validation uses field violations",
        "src/runtime/service/handlers_stores.rs",
        (
            "crate::runtime::executor_utils::{invalid_argument_fields, json_into_struct, struct_to_json}",
            "fn store_rpc_invalid_fields",
            "fn require_resource_backend(",
            "fn require_collection(",
            '"resource.backend is required"',
            '"resource.resource_name (or resource.message_type) is required"',
            '"must be a non-empty backend name"',
            '"must be non-empty when resource.message_type is empty"',
            '"must be non-empty when resource.resource_name is empty"',
            "store_rpc_missing_backend_carries_field_violation",
            "store_rpc_missing_collection_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            "detail.field_violations.len(), expected.len()",
        ),
    ),
    TokenCheck(
        "admin handler validation uses typed field violations",
        "src/runtime/service/handlers_admin.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn admin_required_field(",
            "fn invalid_redaction_payload_json(",
            "fn validate_projection_drift_request(",
            '"payload_json must be JSON: {err}"',
            '"project_id is required in request or metadata"',
            '"message_type is required"',
            '"full projection drift scans require limit > 0 to bound the canonical source read"',
            '"unsupported projection drift scan_mode \'{other}\'; use sample or full"',
            '"must be valid JSON bytes"',
            '"must be supplied in request or metadata"',
            '"must be a non-empty projected message type"',
            '"must be greater than 0 when scan_mode is full"',
            '"must be sample or full"',
            '"must match a configured projection plan for the project"',
            "redaction_preview_invalid_payload_json_carries_field_violation",
            "projection_drift_missing_project_id_carries_field_violation",
            "projection_drift_missing_message_type_carries_field_violation",
            "projection_drift_full_scan_without_limit_carries_field_violation",
            "projection_drift_unknown_scan_mode_carries_field_violation",
            "projection_drift_missing_plan_status_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
        ),
    ),
    TokenCheck(
        "admin handler setup capability uses typed capability detail",
        "src/runtime/service/handlers_admin.rs",
        (
            "fn admin_capability_status(",
            "crate::runtime::executor_utils::capability_status",
            '"admin"',
            '"projection_drift"',
            '"projection_engine"',
            '"ensure_baseline"',
            '"admin_seed_enabled"',
            '"projection engine is not available; configure Postgres canonical source and system store"',
            '"admin baseline seeding is disabled; set UDB_ENABLE_ADMIN_SEED=1 to enable"',
            "admin_setup_capabilities_carry_typed_detail",
            "ErrorKind::Capability",
            'detail.backend, "admin"',
            "detail.capability_required",
        ),
    ),
    TokenCheck(
        "admin handler internal failures use typed internal detail",
        "src/runtime/service/handlers_admin.rs",
        (
            "fn admin_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status",
            'crate::runtime::executor_utils::internal_status("admin", operation, message)',
            '"PreviewCdcRedaction"',
            '"ScanProjectionDrift"',
            '"EnsureBaseline"',
            '"failed to serialize redaction preview: {err}"',
            '"projection source scan failed: {err}"',
            '"failed to encode drift row key as JSON: {err}"',
            '"failed to serialize drift summary: {err}"',
            '"EnsureBaseline saga insert failed: {err}"',
            '"EnsureBaseline dlq insert failed: {err}"',
            "admin_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "admin"',
        ),
    ),
    TokenCheck(
        "vector handler hybrid capability uses typed capability detail",
        "src/runtime/service/handlers_vector.rs",
        (
            "fn vector_hybrid_search_unsupported_status()",
            "crate::runtime::executor_utils::capability_status",
            '"qdrant"',
            '"VectorHybridSearch"',
            '"hybrid_search"',
            '"backend qdrant does not support hybrid_search"',
            "Err(vector_hybrid_search_unsupported_status())",
            "vector_hybrid_search_unsupported_carries_capability_detail",
            "ERROR_DETAIL_METADATA_KEY",
            "ErrorKind::Capability",
        ),
    ),
    TokenCheck(
        "meta handler catalog incompatibility/not-found uses typed schema detail",
        "src/runtime/service/handlers_meta.rs",
        (
            "fn catalog_version_incompatible_status(",
            "fn message_schema_not_found_status(",
            "fn project_scope_mismatch_status(",
            "crate::runtime::executor_utils::schema_status",
            "crate::runtime::executor_utils::policy_status_with_code",
            "tonic::Code::FailedPrecondition",
            "tonic::Code::NotFound",
            "tonic::Code::PermissionDenied",
            '"catalog"',
            '"catalog_version_incompatible"',
            '"message_schema_not_found"',
            '"project_scope_mismatch"',
            'Err(catalog_version_incompatible_status(',
            "Err(message_schema_not_found_status(",
            'Err(project_scope_mismatch_status(',
            '"LookupMessageSchema"',
            '"ListMessageSchemas"',
            "catalog_version_incompatible_carries_schema_detail",
            "message_schema_not_found_carries_schema_detail",
            "project_scope_mismatch_carries_policy_detail",
            "ERROR_DETAIL_METADATA_KEY",
            "ErrorKind::Schema",
            "ErrorKind::Policy",
        ),
    ),
    TokenCheck(
        "catalog handler catalog-version miss uses typed schema detail",
        "src/runtime/service/handlers_catalog.rs",
        (
            "fn catalog_version_not_found_status() -> Status",
            "crate::runtime::executor_utils::schema_status",
            "tonic::Code::NotFound",
            '"catalog"',
            '"GetCatalogVersion"',
            '"catalog_version_not_found"',
            '"catalog version not found"',
            "Err(catalog_version_not_found_status())",
            "catalog_version_not_found_carries_schema_detail",
            "ERROR_DETAIL_METADATA_KEY",
            "ErrorKind::Schema",
        ),
    ),
    TokenCheck(
        "catalog handler internal failures use typed internal detail",
        "src/runtime/service/handlers_catalog.rs",
        (
            "fn catalog_handler_internal_status(",
            'crate::runtime::executor_utils::internal_status("catalog", operation, message)',
            '"GetCatalogManifest"',
            '"ActivateCatalog"',
            '"RollbackCatalog"',
            '"ApproveMigrationPlan"',
            '"failed to serialize catalog manifest: {e}"',
            '"failed to stage active catalog in memory: {err}"',
            '"failed to activate catalog in memory: {err}"',
            '"failed to write ActivateCatalog audit log: {}"',
            '"failed to stage rollback catalog in memory: {err}"',
            '"failed to activate rollback catalog in memory: {err}"',
            '"failed to write RollbackCatalog audit log: {}"',
            '"approval token metadata failed: {err}"',
            "catalog_handler_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "catalog"',
        ),
    ),
    TokenCheck(
        "DataBroker service catalog/RLS refusals use typed schema/policy detail",
        "src/runtime/service/mod.rs",
        (
            "fn catalog_compatibility_status(",
            "crate::runtime::executor_utils::schema_status",
            "catalog_compatibility_status(operation, msg)",
            '"catalog_version_incompatible"',
            "crate::runtime::executor_utils::policy_status",
            '"generic_dispatch_rls_bypass"',
            '"rls_bypass_review_required"',
            "operation may bypass tenant isolation/RLS; set spec_json.udb_allow_rls_bypass=true after explicit tenant-scope review",
        ),
    ),
    TokenCheck(
        "DataBroker service catalog/RLS typed details are decoder-tested",
        "src/runtime/service/tests.rs",
        (
            "catalog_compatibility_status_carries_schema_detail",
            "rls_bypass_guard_blocks_resource_drop_without_ack",
            "decode_detail(&err)",
            "ErrorKind::Schema",
            "ErrorKind::Policy",
            'detail.capability_required, "catalog_version_incompatible"',
            'detail.policy_decision_id, "rls_bypass_review_required"',
        ),
    ),
    TokenCheck(
        "policy handler project validation uses typed field violations",
        "src/runtime/service/handlers_policy.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "crate::runtime::executor_utils::policy_status_with_code",
            "fn policy_required_field(",
            "fn admin_summary_project_scope_status(",
            "fn validate_ensure_project_id(",
            '"project_id is required"',
            '"must be a non-empty project id"',
            '"GetAdminSummary"',
            '"project_scope_mismatch"',
            "Err(admin_summary_project_scope_status())",
            "ensure_project_missing_project_id_carries_field_violation",
            "admin_summary_project_scope_mismatch_carries_policy_detail",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            "ErrorKind::Policy",
            'detail.field_violations[0].field, "project_id"',
        ),
    ),
    TokenCheck(
        "catalog migration lifecycle abort uses typed retry detail",
        "src/runtime/core/catalog_sql.rs",
        (
            "crate::runtime::executor_utils::retryable_aborted_status",
            '"catalog"',
            '"manifest ledger lifecycle"',
            "manifest ledger advanced during migration lifecycle",
        ),
    ),
    TokenCheck(
        "XA transaction aborts use typed retry detail",
        "src/runtime/core/tx_object.rs",
        (
            "crate::runtime::executor_utils::retryable_aborted_status",
            '"transaction"',
            '"xa prepare"',
            '"xa in-doubt recovery"',
            "2PC PREPARE failed",
            "is IN-DOUBT and will be resolved by XA recovery",
        ),
    ),
    TokenCheck(
        "transactional PostgreSQL begin failures use typed retryable detail",
        "src/runtime/core/tx_object.rs",
        (
            "crate::runtime::executor_utils::retryable_status",
            '"postgres"',
            '"transaction_begin"',
            "HTTP_RETRYABLE_BACKOFF_MS",
            "PostgreSQL begin failed",
        ),
    ),
    TokenCheck(
        "served quota/backpressure error uses typed detail",
        "src/runtime/channels.rs",
        (
            "crate::runtime::executor_utils::quota_status",
            '"channel"',
            'format!("{} immediate admission", op.as_str())',
            'format!("{} {label} admission", op.as_str())',
            'format!("{} fair admission", op.as_str())',
            'format!("{} scope control", op.as_str())',
            "crate::runtime::executor_utils::retryable_status",
            '"channel"',
            "ScopeControl::Paused",
            "30_000",
            'let message = format!("{} {label} closed", op.as_str());',
            'message.clone()',
            "decode_detail(&same_tenant)",
            "decode_detail(&draining)",
            "decode_detail(&shed)",
            "closed_channel_reports_unavailable_with_retryable_detail",
            "detail.operation, \"read_channel_closed\"",
            "ErrorKind::Quota",
            "ErrorKind::Retryable",
            "detail.retry_after_ms, 30_000",
            "detail.retry_after_ms, 5_000",
        ),
    ),
    TokenCheck(
        "startup-not-ready refusal uses typed retryable detail",
        "src/runtime/service/mod.rs",
        (
            "crate::runtime::executor_utils::retryable_status",
            '"data_broker"',
            '"startup_not_ready"',
            "HTTP_RETRYABLE_BACKOFF_MS",
            "DataBroker is not ready",
        ),
    ),
    TokenCheck(
        "served deadline/timeouts use typed retryable detail",
        "src/runtime/service/mod.rs",
        (
            "crate::runtime::executor_utils::deadline_exceeded_status",
            "backend_label",
            'format!("{} channel", op.as_str())',
            'format!("{} channel timeout", op.as_str())',
        ),
    ),
    TokenCheck(
        "stream batch deadline/timeouts use typed retryable detail",
        "src/runtime/service/native_helpers.rs",
        (
            "crate::runtime::executor_utils::deadline_exceeded_status",
            "backend",
            'format!("{} channel", op.as_str())',
            'format!("{} channel timeout", op.as_str())',
        ),
    ),
    TokenCheck(
        "native request-scope mismatches use typed policy detail",
        "src/runtime/service/native_helpers.rs",
        (
            "fn native_scope_policy_status(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "tonic::Code::PermissionDenied",
            '"native_request_scope"',
            '"tenant_metadata_mismatch"',
            '"project_metadata_mismatch"',
            '"tenant_claim_mismatch"',
            '"project_claim_mismatch"',
            "request_scope_rejects_header_tenant_mismatch",
            "request_scope_rejects_header_project_mismatch",
            "request_scope_rejects_claim_tenant_mismatch_with_policy_detail",
            "request_scope_rejects_claim_project_mismatch_with_policy_detail",
            "ErrorKind::Policy",
            "detail.policy_decision_id",
        ),
    ),
    TokenCheck(
        "shared service authz denials use typed policy detail",
        "src/runtime/service/mod.rs",
        (
            "fn service_policy_denied(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "tonic::Code::PermissionDenied",
            '"data_plane_authorize"',
            '"data_plane_authorize_item"',
            '"purpose_required"',
            '"portal_permission"',
            '"portal_operator_required"',
            '"portal_viewer_required"',
            '"admin_scope"',
            '"admin_scope_required"',
            '"webrtc_peer_token"',
            '"webrtc_peer_scope_required"',
            '"webrtc_peer_tenant_mismatch"',
            "decision.decision_id",
            "decision.deny_reason",
        ),
    ),
    TokenCheck(
        "shared service authz policy detail tests decode trailers",
        "src/runtime/service/tests.rs",
        (
            "fn assert_policy_detail(",
            "ErrorKind::Policy",
            "admin_scope_denial_carries_policy_detail",
            "webrtc_peer_policy_denials_carry_policy_detail",
            "portal_permissions_distinguish_viewer_and_operator",
            "broker_v2_select_denied_without_policy",
            "broker_v2_admin_rpc_denied_without_grant",
            "broker_v2_batch_item_denial_carries_policy_detail",
            '"data_plane_authorize"',
            '"data_plane_authorize_item"',
            '"portal_permission"',
            '"admin_scope"',
            '"webrtc_peer_token"',
            "detail.policy_decision_id.starts_with(\"authz_\")",
        ),
    ),
    TokenCheck(
        "security IP allowlist denials use typed policy detail",
        "src/runtime/security.rs",
        (
            "fn ip_allowlist_policy_status(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "tonic::Code::PermissionDenied",
            '"ip_allowlist"',
            '"peer_address_missing"',
            '"peer_address_invalid"',
            '"ip_not_in_allowlist"',
            "assert_ip_allowlist_policy_detail",
            "ip_allowlist_missing_or_invalid_peer_carries_policy_detail",
            "ErrorKind::Policy",
            "decode_detail(status)",
        ),
    ),
    TokenCheck(
        "security select export controls use typed policy detail",
        "src/runtime/security.rs",
        (
            "fn export_control_policy_status(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "tonic::Code::PermissionDenied",
            '"select_export_controls"',
            '"pii_export_scope_required"',
            "select_export_controls_pii_denial_carries_policy_detail",
            "select_export_controls_allow_export_purpose_or_pii_scope",
            "ErrorKind::Policy",
            "decode_detail(&err)",
        ),
    ),
    TokenCheck(
        "CDC stream subscribe denials use typed policy detail",
        "src/runtime/cdc/engine_tail.rs",
        (
            "fn cdc_stream_policy_status(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "tonic::Code::PermissionDenied",
            '"cdc_stream"',
            '"cdc_read_scope_required"',
            '"tenant_scope_required"',
            "cdc_stream_scope_denials_carry_policy_detail",
            "ErrorKind::Policy",
            "decode_detail(status)",
        ),
    ),
    TokenCheck(
        "executor timeout wrapper uses typed retryable deadline detail",
        "src/runtime/executor_utils.rs",
        (
            "deadline_exceeded_status(",
            'format!("generic_{operation}")',
            "UDB_EXECUTOR_TIMEOUT_MS",
        ),
    ),
    TokenCheck(
        "read fence hard deadline uses typed retryable detail",
        "src/runtime/core/accessors.rs",
        (
            "crate::runtime::executor_utils::deadline_exceeded_status",
            'format!("read_fence_{}", warning.kind_token())',
            "read fence did not clear",
        ),
    ),
    TokenCheck(
        "Embedding retrieve deadlines use typed retryable detail",
        "src/runtime/service/embedding_service",
        (
            "crate::runtime::executor_utils::deadline_exceeded_status",
            '"embedding"',
            '"retrieve"',
            # The enterprise retrieval rework unified the hybrid/vector search
            # paths behind one budgeted dispatch, so the two per-path operation
            # tokens collapsed into the single "retrieve" operation with a
            # pre-dispatch deadline guard + an in-flight timeout — both typed.
            '"retrieve exceeded its deadline"',
            '"retrieve deadline exceeded before semantic search dispatch"',
        ),
    ),
    TokenCheck(
        "Memcached blocking deadline uses typed retryable detail",
        "src/runtime/executors/memcached.rs",
        (
            "deadline_exceeded_status",
            '"memcached"',
            "operation",
            "memcached operation deadline exceeded",
        ),
    ),
    TokenCheck(
        "served connection-budget error uses typed quota detail",
        "src/runtime/connection_manager.rs",
        (
            "crate::runtime::executor_utils::quota_status",
            '"connection_manager"',
            '"tenant_connection_budget"',
            "decode_detail(&err)",
            "ErrorKind::Quota",
            "detail.retry_after_ms, 30",
        ),
    ),
    TokenCheck(
        "REST executor HTTP 429 uses typed quota detail",
        "src/runtime/executor_utils.rs",
        (
            "429 => quota_status(\n            backend,\n            \"request\",\n            HTTP_RETRYABLE_BACKOFF_MS,",
            'format!("{backend} 429: {detail}")',
            "http_status_to_tonic_429_carries_quota_retry_after_detail",
            "reqwest::StatusCode::TOO_MANY_REQUESTS",
            "detail.retry_after_ms, HTTP_RETRYABLE_BACKOFF_MS",
        ),
    ),
    TokenCheck(
        "REST executor HTTP 401/403 uses typed policy detail",
        "src/runtime/executor_utils.rs",
        (
            "401 | 403 => policy_status_with_code(",
            "tonic::Code::PermissionDenied",
            '"backend_http_authz"',
            'format!("{}_http_{code}", backend)',
            "http_status_to_tonic_authz_rejection_carries_policy_detail",
        ),
    ),
    TokenCheck(
        "REST executor HTTP 409 uses typed schema detail",
        "src/runtime/executor_utils.rs",
        (
            "409 => schema_status(",
            "tonic::Code::AlreadyExists",
            '"backend_http_conflict"',
            'format!("{backend} 409: {detail}")',
            "http_status_to_tonic_conflict_carries_schema_detail",
        ),
    ),
    TokenCheck(
        "REST executor HTTP 404 uses typed schema detail",
        "src/runtime/executor_utils.rs",
        (
            "404 => schema_status(",
            "tonic::Code::NotFound",
            '"backend_http_not_found"',
            'format!("{backend} 404: {detail}")',
            "http_status_to_tonic_not_found_carries_schema_detail",
        ),
    ),
    TokenCheck(
        "REST executor unexpected HTTP status uses typed internal detail",
        "src/runtime/executor_utils.rs",
        (
            "_ => internal_status(",
            'format!("http_{code}")',
            'format!("{backend} {code}: {detail}")',
            "http_status_to_tonic_unexpected_status_carries_internal_detail",
        ),
    ),
    TokenCheck(
        "shared sqlx fallback errors use typed internal detail",
        "src/runtime/executor_utils.rs",
        (
            'internal_status("database", context, format!("{context}{detail}"))',
            'internal_status("database", context, context.to_string())',
            'internal_status("database", error_prefix, format!("{error_prefix}: {err}"))',
            "sqlx_non_transient_non_database_error_preserves_internal_code_with_detail",
        ),
    ),
    TokenCheck(
        "untagged store String boundary uses typed internal detail",
        "src/runtime/executor_utils.rs",
        (
            'internal_status("store", "string_status", s)',
            "untagged_store_string_preserves_internal_code_with_detail",
            "tagged_store_invalid_argument_preserves_validation_detail",
            "tagged_store_unavailable_preserves_retryable_detail",
        ),
    ),
    TokenCheck(
        "Elasticsearch shared 429 mapping test decodes typed quota detail",
        "src/runtime/executors/elasticsearch.rs",
        (
            "es_status_maps_429_to_resource_exhausted",
            "decode_detail(&status)",
            "ErrorKind::Quota",
            "detail.retry_after_ms, 250",
            'detail.backend, "Elasticsearch"',
            'detail.operation, "request"',
        ),
    ),
    TokenCheck(
        "public bootstrap throttling uses typed quota detail",
        "src/runtime/service/method_security.rs",
        (
            "crate::runtime::executor_utils::quota_status",
            "crate::runtime::executor_utils::invalid_argument_fields",
            '"method_security"',
            '"public_bootstrap_rate_limit"',
            '"request context required: send x-correlation-id, x-request-id, or traceparent"',
            '"must include x-correlation-id, x-request-id, or traceparent"',
            "public_bootstrap_retry_after_ms()",
            "request_context_required_status()",
            "request_context_required_status_carries_field_violation",
            "decode_detail(&err)",
            "ErrorKind::Quota",
            "ErrorKind::Validation",
            "detail.retry_after_ms >= 1_000",
            'detail.field_violations[0].field, "request_context"',
        ),
    ),
    TokenCheck(
        "distributed data-plane rate limit uses typed quota detail",
        "src/runtime/service/mod.rs",
        (
            "crate::runtime::executor_utils::quota_status",
            '"data_broker"',
            '"distributed rate limit"',
            "retry_after_ms",
            "rate limit exceeded",
        ),
    ),
    TokenCheck(
        "distributed rate-limit Redis infra failures use typed retryable detail",
        "src/runtime/service/mod.rs",
        (
            "crate::runtime::executor_utils::retryable_status",
            "crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS",
            '"rate_limit_connection"',
            '"rate_limit_eval"',
            'format!("rate limit redis error: {e}")',
        ),
    ),
    TokenCheck(
        "generic backend ping failures use typed retryable detail",
        "src/runtime/core/probe_dispatch.rs",
        (
            'backend_transport_status(',
            '"postgres", "ping", err',
            '"redis", "ping", err',
            '"qdrant", "ping", err',
        ),
    ),
    TokenCheck(
        "generic backend ping missing-backend failures use typed capability detail",
        "src/runtime/core/probe_dispatch.rs",
        (
            "probe_backend_not_configured_status",
            "capability_status",
            '"ping"',
            '"mongodb_backend"',
            "mongodb not configured",
            '"neo4j_backend"',
            "neo4j not configured",
            '"clickhouse_backend"',
            "clickhouse not configured",
            '"qdrant_backend"',
            "qdrant not configured",
            '"s3_backend"',
            "s3/minio not configured",
        ),
    ),
    TokenCheck(
        "served GenericDispatch ping failures use typed retryable detail",
        "src/runtime/service/handlers_data.rs",
        (
            "let dispatch_backend = breaker_backend.clone();",
            "executor\n                            .ping()",
            "backend_transport_status(",
            "&dispatch_backend",
            '"ping"',
        ),
    ),
    TokenCheck(
        "outbox idempotency Redis fail-closed uses typed retryable detail",
        "src/runtime/core/probe_dispatch.rs",
        (
            "crate::runtime::executor_utils::retryable_status",
            "crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS",
            '"redis"',
            '"idempotency_dedup_check"',
            "idempotency dedup store unavailable; keyed enqueue refused (fail-closed)",
        ),
    ),
    TokenCheck(
        "backend instance circuit breaker failures use typed retryable detail",
        "src/runtime/core/accessors.rs",
        (
            "retryable_status(",
            '"circuit_breaker_open"',
            "HTTP_RETRYABLE_BACKOFF_MS",
            "backend instance '{}:{}' circuit breaker is open",
            "redis instance '{instance}' circuit breaker is open",
        ),
    ),
    TokenCheck(
        "core service helper validation uses typed field violations",
        "src/runtime/service/mod.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn unknown_backend_status(",
            "fn unknown_generic_operation_status(",
            "fn parse_catalog_manifest_payload(",
            '"unknown backend \'{backend}\'"',
            '"unknown operation \'{operation}\'; allowed: ping, probe, ensure_resource, drop_resource, list_resources, query, mutate, transaction, search, get_object, put_object, delete_object"',
            '"manifest_json is required"',
            '"manifest_json is not a CatalogManifest: {err}"',
            '"must name a supported backend"',
            '"must be a supported generic dispatch operation"',
            '"must contain a CatalogManifest JSON payload"',
            '"must decode as a CatalogManifest"',
        ),
    ),
    TokenCheck(
        "core service helper validation decoder tests",
        "src/runtime/service/tests.rs",
        (
            "parse_catalog_manifest_payload_empty_carries_field_violation",
            "parse_catalog_manifest_payload_invalid_json_carries_field_violation",
            "unknown_backend_status_carries_field_violation",
            "unknown_generic_operation_status_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"must contain a CatalogManifest JSON payload"',
            '"must be a supported generic dispatch operation"',
        ),
    ),
    TokenCheck(
        "core service runtime-support capability uses typed capability detail",
        "src/runtime/service/mod.rs",
        (
            "fn backend_runtime_unsupported_status(",
            "crate::runtime::executor_utils::capability_status",
            '"backend_runtime_support"',
            "state.diagnostic(kind.as_str())",
            "check_backend_capability(",
            "check_generic_dispatch_operation(",
        ),
    ),
    TokenCheck(
        "core service runtime-support capability decoder test",
        "src/runtime/service/tests.rs",
        (
            "backend_runtime_unsupported_status_carries_capability_detail",
            "backend_runtime_unsupported_status(",
            '"backend_runtime_support"',
            "ErrorKind::Capability",
            "detail.capability_required",
            'assert!(detail.field_violations.is_empty())',
        ),
    ),
    TokenCheck(
        "core accessor validation uses typed field violations",
        "src/runtime/core/accessors.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn invalid_backend_selector_status(",
            "fn invalid_read_fence_json_status(",
            '"unknown backend \'{selector}\'"',
            '"invalid read_fence_json: {err}"',
            '"must name a supported backend"',
            '"must decode as a ReadFence JSON payload"',
            "backend_selector_validation_carries_field_violations",
            "malformed_read_fence_json_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"backend"',
            '"read_fence_json"',
        ),
    ),
    TokenCheck(
        "core accessor bounded-read refusal uses typed policy detail",
        "src/runtime/core/accessors.rs",
        (
            "fn bounded_read_refused_status(",
            "crate::runtime::executor_utils::policy_status",
            '"read_consistency"',
            '"bounded_staleness_requires_real_position"',
            "bounded_read_refusal_carries_policy_detail_in_sync_selector",
            "bounded_read_refusal_carries_policy_detail_in_async_selector",
            "wall-clock backend must refuse bounded-staleness reads",
            "bounded-staleness read refused for backend 's3'",
            "ErrorKind::Policy",
            "detail.policy_decision_id",
        ),
    ),
    TokenCheck(
        "native entity transaction validation uses typed field violations",
        "src/runtime/core/native_store.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn empty_native_entity_transaction_status(",
            '"native entity transaction requires at least one operation"',
            '"must contain at least one operation"',
            "empty_native_entity_transaction_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"ops"',
        ),
    ),
    TokenCheck(
        "native entity update miss uses typed schema detail",
        "src/runtime/core/native_store.rs",
        (
            "fn native_entity_update_not_found_status() -> tonic::Status",
            "crate::runtime::executor_utils::schema_status",
            "tonic::Code::NotFound",
            '"native_entity"',
            '"native_entity_update"',
            '"native_entity_update_not_found"',
            '"native entity update affected no rows"',
            "return Err(native_entity_update_not_found_status());",
            "native_entity_update_miss_carries_schema_detail",
            "assert_schema_detail(",
            "ErrorKind::Schema",
        ),
    ),
    TokenCheck(
        "native store setup capability uses typed capability detail",
        "src/runtime/core/native_store.rs",
        (
            "fn native_store_capability_status(",
            "crate::runtime::executor_utils::capability_status",
            '"neutral_ir_compiler"',
            '"native_entity_dispatch_operation"',
            '"postgres_native_transaction"',
            '"native_entity_mutation_dispatch"',
            '"postgres_pool"',
            '"sqlite_native_store"',
            '"mysql_native_store"',
            '"native_entity_store"',
            '"postgres_sql_mutation"',
            '"native entity backend \'{}\' has no neutral-IR compiler"',
            '"native typed transactions are currently implemented only for postgres, got \'{}\'"',
            '"native entity transaction compiled unsupported operation \'{}\'"',
            '"native entity transaction compiled a non-SQL mutation"',
            '"native store backend \'{}\' exposes no Postgres pool',
            '"sqlite native store is not configured"',
            '"mysql native store is not configured"',
            '"native-service persistence backend \'{other}\' is not implemented; see extend_udb.md"',
            "native_store_missing_capabilities_carry_typed_detail",
            "ErrorKind::Capability",
            "detail.capability_required",
        ),
    ),
    TokenCheck(
        "core native store internals use typed internal detail",
        "src/runtime/core/native_store.rs",
        (
            "fn native_store_internal_status(",
            'crate::runtime::executor_utils::internal_status("native_store", operation, message)',
            '"native_entity_rows_decode"',
            '"native_entity_rows_shape"',
            '"native_entity_mutation_decode"',
            '"native_entity_mutation_shape"',
            '"native_entity_transaction_start"',
            '"compiled_native_mutation_json"',
            '"native_entity_transaction_mutation"',
            '"native_entity_transaction_commit"',
            '"native_advisory_lease"',
            "native_store_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "native_store"',
        ),
    ),
    TokenCheck(
        "native entity store internal failures use typed internal detail",
        "src/runtime/service/native_entity_store.rs",
        (
            "fn native_store_internal_status(",
            "crate::runtime::executor_utils::internal_status(backend, operation, message)",
            "fn store_err(",
            "fn store_err_str(",
            '"kv_roundtrip_probe"',
            '"native store {op} on \'{backend}\' failed: {err}"',
            "kv round-trip mismatch: wrote {value:?}, read {got:?}",
            "native_store_internal_status_carries_typed_detail",
            "ErrorKind::Internal",
            'detail.backend, "postgres"',
            'detail.operation, "kv_get"',
        ),
    ),
    TokenCheck(
        "core transaction and outbox validation uses typed field violations",
        "src/runtime/core/mod.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn core_invalid_field(",
            '"conflicting transaction strategies in request routing policy"',
            '"unsupported transaction strategy \'{value}\'"',
            '"outbox topic is required"',
            '"outbox partition_key is required"',
            '"event payload must be a JSON object conforming to the EventEnvelope schema"',
            '"event payload field \'{field}\' must be a non-empty string"',
            '"event payload field \'event_id\' must be a valid UUID: {err}"',
            '"outbox partition_key must equal payload.document_id"',
            "prepare_outbox_envelope_boundary_errors_carry_field_violations",
            "requested_tx_strategy_parses_and_rejects_conflicts",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"routing_policy"',
            '"topic"',
            '"partition_key"',
            '"payload"',
            '"payload.event_id"',
            '"payload.tenant_id"',
        ),
    ),
    TokenCheck(
        "core transaction policy and decrypt setup use typed detail",
        "src/runtime/core/mod.rs",
        (
            "fn two_phase_unsupported_operation_status(",
            "crate::runtime::executor_utils::policy_status",
            '"transaction_strategy"',
            '"two_phase_participant_required"',
            "fn two_phase_disabled_status(",
            '"two_phase_execution_disabled"',
            "two_phase requested but prepared transaction execution is disabled",
            "fn decrypt_encryption_key_missing_status(",
            "crate::runtime::executor_utils::capability_status",
            '"record_decryption"',
            '"udb_encryption_key"',
            "column email is encrypted but UDB encryption key is not configured",
            "two_phase_strategy_fails_before_side_effects",
            "decrypt_without_configured_key_carries_capability_detail",
            "ErrorKind::Policy",
            "ErrorKind::Capability",
        ),
    ),
    TokenCheck(
        "core runtime internals use typed internal detail",
        "src/runtime/core/mod.rs",
        (
            "fn core_internal_status(",
            'internal_status("core", operation, message)',
            '"set_request_context"',
            '"reset_request_context"',
            '"serialize_record_json"',
            '"decrypt_record_value"',
            "core_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "core"',
        ),
    ),
    TokenCheck(
        "system catalog internals use typed internal detail",
        "src/runtime/system.rs",
        (
            "fn system_catalog_internal_status(",
            'crate::runtime::executor_utils::internal_status("system_catalog", operation, message)',
            '"bootstrap_begin"',
            '"bootstrap_statement"',
            '"bootstrap_commit"',
            '"inspect_relation"',
            "system_catalog_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "system_catalog"',
        ),
    ),
    TokenCheck(
        "core generic dispatch helper validation uses typed field violations",
        "src/runtime/core/helpers.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "crate::runtime::executor_utils::failed_precondition_fields",
            "fn core_helper_invalid_field(",
            "fn core_helper_failed_precondition_field(",
            '"invalid spec_json: {err}"',
            '"params/parameters must be an array"',
            '"param_types entries must be strings"',
            '"param_types must be an array"',
            '"sql is required"',
            '"generic PostgreSQL dispatch accepts exactly one statement"',
            '"generic PostgreSQL query allows only SELECT, WITH, SHOW, or EXPLAIN"',
            '"must start with SELECT, WITH, SHOW, or EXPLAIN"',
            '"generic PostgreSQL mutate allows only INSERT, UPDATE, or DELETE"',
            '"must start with INSERT, UPDATE, or DELETE"',
            '"generic query allows only SELECT, WITH, SHOW, EXPLAIN, or PRAGMA"',
            '"must start with SELECT, WITH, SHOW, EXPLAIN, or PRAGMA"',
            '"generic mutate allows only INSERT, UPDATE, DELETE, or REPLACE"',
            '"must start with INSERT, UPDATE, DELETE, or REPLACE"',
            '"param_types length must match params length"',
            '"array_string params must contain only strings"',
            '"array_int params must contain only integers"',
            '"array_float params must contain only numbers"',
            '"array_bool params must contain only booleans"',
            '"timestamptz params must be RFC3339 strings: {err}"',
            '"uuid params must be UUID strings: {err}"',
            '"{param_type} params must be arrays"',
            "generic_dispatch_json_and_sql_validation_carry_field_violations",
            "typed_generic_pg_param_validation_carries_field_violations",
            "assert_failed_precondition_field_violation(",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"spec_json"',
            '"params"',
            '"param_types"',
            '"sql"',
        ),
    ),
    TokenCheck(
        "catalog admin validation uses typed field violations",
        "src/runtime/core/catalog_admin.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn catalog_admin_invalid_field(",
            "fn parse_catalog_manifest_json(",
            "fn validate_approval_token_for_plan(",
            "fn parse_migration_run_id(",
            "fn parse_dlq_status_filter(",
            "fn parse_dlq_id(",
            '"manifest_json is not valid UTF-8"',
            '"manifest_json parse error: {err}"',
            '"approval_token must not be empty"',
            '"run_id must be a UUID"',
            '"dlq_id must be a UUID"',
            '"invalid status_filter \'{status_filter}\'; must be one of {:?}"',
            "catalog_admin_manifest_validation_carries_field_violations",
            "catalog_admin_id_and_filter_validation_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"manifest_json"',
            '"approval_token"',
            '"run_id"',
            '"dlq_id"',
            '"status_filter"',
        ),
    ),
    TokenCheck(
        "catalog admin failed preconditions use typed detail",
        "src/runtime/core/catalog_admin.rs",
        (
            "fn catalog_admin_policy_status(",
            "crate::runtime::executor_utils::policy_status",
            "fn catalog_admin_policy_not_found_status(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "fn catalog_admin_schema_status(",
            "fn catalog_admin_not_found_status(",
            "crate::runtime::executor_utils::schema_status",
            "fn catalog_admin_capability_status(",
            "crate::runtime::executor_utils::capability_status",
            '"approval_token_required"',
            '"migration_run_not_approved"',
            '"approval_token_mismatch"',
            '"migration_run_not_preflight"',
            '"migration_apply_preflight_failed"',
            '"migration_approval_state_changed"',
            '"migration_phase_refused"',
            '"dlq_event_missing_topic"',
            '"staged_catalog_not_found"',
            '"staged_catalog_version_not_found"',
            '"migration_run_not_found"',
            '"dlq_event_not_found"',
            '"dlq_event_not_found_or_not_replayable"',
            '"policy_not_found"',
            '"canonical_system_store"',
            "catalog_admin_failed_preconditions_carry_typed_detail",
            "catalog_admin_activate_not_found_statuses_carry_schema_detail",
            "catalog_admin_migration_dlq_not_found_statuses_carry_schema_detail",
            "catalog_admin_policy_delete_not_found_carries_policy_detail",
            "apply_migration_validation_rejects_empty_token",
            "apply_migration_validation_rejects_wrong_token",
            "apply_migration_validation_rejects_non_approved_states",
            "catalog_admin_not_found_status(",
            "migration_run_not_found_status(",
            "dlq_event_not_found_status(",
            "dlq_event_not_found_or_not_replayable_status(",
            "DLQ event has no topic and cannot be replayed",
            "no canonical store is registered; saga/audit admin requires a provisioned",
            "ErrorKind::Policy",
            "ErrorKind::Schema",
            "ErrorKind::Capability",
        ),
    ),
    TokenCheck(
        "catalog admin internals use typed internal detail",
        "src/runtime/core/catalog_admin.rs",
        (
            "fn catalog_admin_internal_status(",
            'crate::runtime::executor_utils::internal_status("catalog_admin", operation, message)',
            '"migration_audit_payload_json_upgrade"',
            '"migration_audit_payload_json_backfill"',
            '"migration_audit_approved_state_upgrade"',
            '"stage_catalog"',
            '"activate_catalog_lookup"',
            '"activate_catalog_begin"',
            '"activate_catalog_deactivate"',
            '"activate_catalog_update"',
            '"activate_catalog_version_fetch"',
            '"activate_catalog_log"',
            '"activate_catalog_project_binding"',
            '"activate_catalog_reload_log"',
            '"activate_catalog_commit"',
            '"get_catalog_versions"',
            '"plan_migration_manifest_load"',
            '"plan_migration_manifest_parse"',
            '"plan_migration_schema_check"',
            '"plan_migration_run_insert"',
            '"plan_migration_op_insert"',
            '"approve_migration_plan"',
            '"apply_migration_run_fetch"',
            '"apply_migration_fetch_ops"',
            '"apply_migration_state_update"',
            '"apply_migration_phase_failure_finalize"',
            '"apply_migration_phased_runner"',
            '"apply_migration_finalize"',
            '"apply_migration_paused"',
            '"get_migration_status"',
            '"list_dlq_events"',
            '"get_dlq_event"',
            '"replay_dlq_event_lookup"',
            '"replay_dlq_event_id_decode"',
            '"replay_dlq_payload_decode"',
            '"replay_dlq_begin"',
            '"replay_dlq_enqueue"',
            '"replay_dlq_update"',
            '"replay_dlq_commit"',
            '"update_dlq_status"',
            '"get_cdc_status"',
            '"pause_cdc"',
            '"resume_cdc"',
            '"stepdown_cdc_leader"',
            '"list_policies"',
            '"list_policies_page"',
            '"put_policy_update"',
            '"put_policy_insert"',
            '"delete_policy"',
            '"write_audit_log"',
            '"ensure_project"',
            '"list_projects"',
            '"list_migration_runs"',
            '"list_admin_audit_logs"',
            '"verify_admin_audit_log_chain"',
            "catalog_admin_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "catalog_admin"',
        ),
    ),
    TokenCheck(
        "Postgres data-plane helper validation uses typed field violations",
        "src/runtime/postgres_helpers.rs",
        (
            "invalid_argument_fields",
            "fn postgres_invalid_field(",
            "fn postgres_json_i64(",
            "fn postgres_json_f64(",
            '"must match exactly one manifest table message type"',
            '"tenant_id is required for join fusion"',
            '"join fusion requires at least two message types"',
            '"must match exactly one manifest table message type"',
            '"no foreign key path found for join fusion target {}"',
            '"join fusion supports only simple equality filters"',
            '"unknown join filter field {field}"',
            '"unknown join selected field {field}"',
            '"unknown join field prefix {message_type}"',
            '"parameter mismatch: {} columns, {} values"',
            '"UUID $in value must be a string"',
            '"invalid UUID in $in: {err}"',
            '"invalid UUID: {err}"',
            '"timestamptz value must be an RFC3339 string: {err}"',
            '"timestamp value must be an ISO-8601 string or null"',
            '"invalid date: {err}"',
            '"record_json must be valid JSON: {err}"',
            '"payload or record_json is required"',
            '"record must be a JSON object"',
            "join_fusion_validation_carries_field_violations",
            "postgres_bind_validation_carries_field_violations",
            "record_json_validation_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"message_type"',
            '"tenant_id"',
            '"filter"',
            '"fields"',
            '"value"',
            '"record_json"',
            '"payload"',
            '"record"',
        ),
    ),
    TokenCheck(
        "Postgres join-fusion tenant isolation refusal uses typed schema detail",
        "src/runtime/postgres_helpers.rs",
        (
            "fn join_fusion_missing_tenant_column_status(",
            "crate::runtime::executor_utils::schema_status",
            '"postgres"',
            '"join_fusion"',
            '"tenant_column_required"',
            "join fusion cannot safely select scoped table public.lefts without a tenant column",
            "join_fusion_fails_closed_for_scoped_table_without_tenant_column",
            "ErrorKind::Schema",
            "detail.capability_required",
        ),
    ),
    TokenCheck(
        "Postgres data-plane helper internals use typed internal detail",
        "src/runtime/postgres_helpers.rs",
        (
            "fn postgres_internal_status(",
            'crate::runtime::executor_utils::internal_status("postgres", operation, message)',
            '"execute_tx_plan"',
            '"transaction mutation failed: {err}"',
            "postgres_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, backend',
        ),
    ),
    TokenCheck(
        "tenant purge internals use typed internal detail",
        "src/runtime/core/tenant_purge.rs",
        (
            "fn tenant_purge_internal_status(",
            'crate::runtime::executor_utils::internal_status("tenant_purge", operation, message)',
            '"begin_tenant_purge"',
            '"delete_tenant_table"',
            '"commit_tenant_purge"',
            '"failed to begin tenant-purge transaction: {err}"',
            '"tenant purge failed deleting from {}.{}: {err}"',
            '"failed to commit tenant purge: {err}"',
            "tenant_purge_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, backend',
        ),
    ),
    TokenCheck(
        "core transaction object validation uses typed field violations",
        "src/runtime/core/tx_object.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn tx_object_invalid_field(",
            "fn validate_tx_identifier(",
            '"transaction stream requires at least one mutation"',
            '"must match exactly one manifest table message type"',
            '"topic \'{topic}\' is not in the registered topic registry; \\',
            '"enqueue_outbox_event transaction mutation requires payload"',
            '"unsupported transaction operation {}"',
            '"materialized view {}.{} is not declared in the proto AST"',
            '"materialized view query does not match the proto AST declaration"',
            '"{label} \'{value}\' is not a valid SQL identifier"',
            "tx_object_local_validation_carries_field_violations",
            "create_materialized_view_validation_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"mutations"',
            '"message_type"',
            '"topic"',
            '"payload"',
            '"operation"',
            '"schema"',
            '"name"',
            '"view"',
            '"query"',
        ),
    ),
    TokenCheck(
        "core transaction object failed preconditions use typed detail",
        "src/runtime/core/tx_object.rs",
        (
            "fn mysql_xa_plan_replay_status(",
            "crate::runtime::executor_utils::schema_status",
            '"xa_plan_replay"',
            '"mysql_xa_plan_replay"',
            "fn xa_unsupported_participants_status(",
            "crate::runtime::executor_utils::capability_status",
            '"xa_commit"',
            '"xa_participants"',
            "fn record_encryption_key_missing_status(",
            '"record_encryption"',
            '"udb_encryption_key"',
            "fn s3_feature_disabled_status(",
            '"put_tx_object"',
            '"s3_feature"',
            "tx_object_failed_preconditions_carry_typed_detail",
            "2PC refused before side effects: plan SQL cannot be replayed on the MySQL XA participant",
            "2PC refused before side effects: unsupported participants mysql, qdrant",
            "table private.accounts contains encrypted columns but UDB encryption key is not configured",
            "s3/object-store feature is not enabled",
            "ErrorKind::Schema",
            "ErrorKind::Capability",
        ),
    ),
    TokenCheck(
        "core transaction object internals use typed internal detail",
        "src/runtime/core/tx_object.rs",
        (
            "fn tx_object_internal_status(",
            'crate::runtime::executor_utils::internal_status("tx_object", operation, message)',
            '"enqueue_projection_tasks"',
            '"enqueue_tx_event"',
            '"mutation_failure_compensation"',
            '"xa_ledger_unavailable"',
            '"xa_ledger_commit_record"',
            '"postgres_commit_compensation"',
            '"create_materialized_view"',
            '"refresh_materialized_view"',
            '"initial_refresh_materialized_view"',
            '"encrypt_record_column"',
            "tx_object_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "tx_object"',
        ),
    ),
    TokenCheck(
        "core materialized view admin scope uses typed policy detail",
        "src/runtime/core/tx_object.rs",
        (
            "fn materialized_view_admin_scope_required_status() -> tonic::Status",
            "crate::runtime::executor_utils::policy_status_with_code",
            "tonic::Code::PermissionDenied",
            '"create_materialized_view"',
            '"admin_scope_required"',
            '"scope udb:admin is required"',
            "return Err(materialized_view_admin_scope_required_status());",
            "create_materialized_view_admin_scope_denial_carries_policy_detail",
            "ErrorKind::Policy",
            "assert_policy_detail(",
            "detail.policy_decision_id",
        ),
    ),
    TokenCheck(
        "OTP cooldown uses typed quota detail",
        "src/runtime/service/auth_service/authn/mfa.rs",
        (
            "crate::runtime::executor_utils::quota_status",
            '"authn"',
            '"otp_cooldown"',
            "retry_after_secs as i64 * 1_000",
        ),
    ),
    TokenCheck(
        "authn MFA request validation uses typed field violations",
        "src/runtime/service/auth_service/authn/mfa.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn mfa_required_field(",
            '"tenant_id is required"',
            '"phone is required"',
            '"phone must be at most 32 characters (E.164 format)"',
            '"must be a non-empty tenant id"',
            '"must be a non-empty phone number"',
            '"must be at most 32 characters (E.164 format)"',
            "put_mfa_policy_missing_tenant_id_carries_field_violation",
            "get_mfa_policy_missing_tenant_id_carries_field_violation",
            "send_phone_verification_missing_phone_carries_field_violation",
            "send_phone_verification_long_phone_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
        ),
    ),
    TokenCheck(
        "authn MFA WebAuthn enrollment policy uses typed policy detail",
        "src/runtime/service/auth_service/authn/mfa.rs",
        (
            "fn mfa_webauthn_enrollment_rpc_required_status(",
            "crate::runtime::executor_utils::policy_status",
            '"mfa_enrollment"',
            '"webauthn_enrollment_rpc_required"',
            "WebAuthn enrollment uses StartWebAuthnRegistration and FinishWebAuthnRegistration",
            "return Err(mfa_webauthn_enrollment_rpc_required_status());",
            "webauthn_mfa_enrollment_policy_carries_typed_detail",
            "ErrorKind::Policy",
            "assert_eq!(detail.policy_decision_id, policy_decision_id);",
        ),
    ),
    TokenCheck(
        "authn MFA internals use typed ErrorDetail",
        "src/runtime/service/auth_service/authn/mfa.rs",
        (
            "fn mfa_internal_status(",
            'crate::runtime::executor_utils::internal_status("authn", operation, message)',
            '"issue_otp_store"',
            '"otp_cooldown_lookup"',
            '"verify_otp_load"',
            '"verify_otp_expire_update"',
            '"verify_otp_failed_attempt_update"',
            '"verify_otp_consume_pending"',
            '"send_otp_user_load"',
            '"verify_otp_email_user_load"',
            '"verify_otp_email_user_update"',
            '"verify_otp_phone_mark_verified"',
            '"verify_otp_failure_audit_lookup"',
            '"resend_otp_original_load"',
            '"resend_otp_user_load"',
            '"resend_otp_supersede_update"',
            '"enroll_mfa_user_load"',
            '"enroll_mfa_secret_encrypt"',
            '"enroll_mfa_store"',
            '"confirm_mfa_user_load"',
            '"confirm_mfa_store"',
            '"recovery_codes_user_load"',
            '"recovery_codes_replace"',
            '"put_mfa_policy"',
            '"get_mfa_policy"',
            '"send_phone_verification_user_load"',
            '"send_phone_verification_set_phone"',
            "mfa_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "authn"',
        ),
    ),
    TokenCheck(
        "authn core user request validation uses typed field violations",
        "src/runtime/service/auth_service/authn/core.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn authn_core_invalid_fields",
            "fn create_user_password_policy_status(",
            '"username and email are required"',
            '"one of user_id, username, or email is required"',
            '"new_status is required"',
            '"password must contain a symbol"',
            '"must be a non-empty username"',
            '"must be a non-empty email address"',
            '"must include at least one lookup identity"',
            '"must specify a target user status"',
            '"must satisfy the configured password policy for native users"',
            "create_user_missing_identity_carries_field_violations",
            "get_user_missing_lookup_identity_carries_field_violations",
            "change_user_status_missing_new_status_carries_field_violation",
            "create_user_password_policy_status_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            "assert_validation_fields(",
        ),
    ),
    TokenCheck(
        "Authn native password secret capability uses typed capability detail",
        "src/runtime/service/auth_service/authn/core.rs",
        (
            "fn create_user_password_secret_status() -> Status",
            "authn_capability_status(",
            '"native_user_passwords"',
            '"password_hash_secret"',
            "native user passwords require UDB_PASSWORD_HASH_SECRET or UDB_SESSION_HASH_SECRET",
            "create_user_password_secret_status_carries_capability_detail",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Capability",
            'detail.backend, "authn"',
            "detail.capability_required",
            "return Err(create_user_password_secret_status());",
        ),
    ),
    TokenCheck(
        "authn core tenant/principal policy denials use typed policy detail",
        "src/runtime/service/auth_service/authn/core.rs",
        (
            "fn authn_core_policy_status(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "tonic::Code::PermissionDenied",
            "fn authn_read_tenant_scope_required_status() -> Status",
            "fn authn_read_tenant_mismatch_status() -> Status",
            "fn created_by_principal_mismatch_status() -> Status",
            '"authn_user_read"',
            '"tenant_scoped_bearer_required"',
            '"tenant_mismatch"',
            '"create_user"',
            '"created_by_principal_mismatch"',
            "return Err(authn_read_tenant_scope_required_status());",
            "return Err(authn_read_tenant_mismatch_status());",
            "return Err(created_by_principal_mismatch_status());",
            "read_tenant_filter_denies_cross_tenant_request_for_non_admin",
            "read_tenant_filter_denies_tenantless_non_admin",
            "create_user_created_by_mismatch_carries_policy_detail",
            "ErrorKind::Policy",
            "assert_policy_detail(",
            "detail.policy_decision_id",
        ),
    ),
    TokenCheck(
        "authn lifecycle request validation uses typed field violations",
        "src/runtime/service/auth_service/authn/lifecycle.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn lifecycle_invalid_fields",
            '"user_id is required"',
            '"tenant_id is required"',
            '"at least one selector is required (signing_key_id/token_family_id/tenant_id/principal_id)"',
            '"user_id and credential_id are required"',
            '"must be a non-empty user id"',
            '"must be a non-empty tenant id"',
            '"must include at least one of signing_key_id, token_family_id, tenant_id, or principal_id"',
            '"must be a non-empty WebAuthn credential id"',
            "list_devices_missing_user_id_carries_field_violation",
            "admin_revoke_session_missing_user_id_carries_field_violation",
            "admin_revoke_all_user_sessions_missing_user_id_carries_field_violation",
            "admin_revoke_all_tenant_sessions_missing_tenant_id_carries_field_violation",
            "emergency_revoke_missing_selector_carries_field_violation",
            "list_webauthn_credentials_missing_user_id_carries_field_violation",
            "delete_webauthn_credential_missing_identity_carries_field_violations",
            "rename_passkey_missing_identity_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            "assert_validation_fields(",
        ),
    ),
    TokenCheck(
        "authn lifecycle device revoke policy uses typed policy detail",
        "src/runtime/service/auth_service/authn/lifecycle.rs",
        (
            "fn lifecycle_policy_status_with_code(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "tonic::Code::PermissionDenied",
            "fn revoke_device_tenant_scope_required_status() -> Status",
            '"revoke_device"',
            '"tenant_scoped_bearer_required"',
            '"device revoke requires a tenant-scoped bearer token or a cross-tenant admin role"',
            "return Err(revoke_device_tenant_scope_required_status());",
            "revoke_device_tenantless_non_admin_carries_policy_detail",
            "ErrorKind::Policy",
            "assert_policy_detail(",
            "detail.policy_decision_id",
        ),
    ),
    TokenCheck(
        "authn lifecycle internals use typed ErrorDetail",
        "src/runtime/service/auth_service/authn/lifecycle.rs",
        (
            "fn lifecycle_internal_status(",
            'crate::runtime::executor_utils::internal_status("authn", operation, message)',
            '"authorize_target_user_load"',
            '"token_revocation_tx_begin"',
            '"token_revocation_insert"',
            '"token_revocation_commit"',
            '"list_devices_query"',
            '"revoke_device_tx_begin"',
            '"revoke_device_update"',
            '"revoke_device_commit"',
            '"revoke_device_families"',
            '"admin_revoke_session_tx_begin"',
            '"admin_revoke_session_store"',
            '"admin_revoke_session_commit"',
            '"admin_revoke_all_sessions_tx_begin"',
            '"admin_revoke_all_sessions_store"',
            '"admin_revoke_all_sessions_commit"',
            '"revoke_tenant_tx_begin"',
            '"revoke_tenant_sessions"',
            '"revoke_tenant_families"',
            '"revoke_tenant_commit"',
            '"revoke_user_families"',
            '"emergency_revoke_tx_begin"',
            '"emergency_revoke_principal_sessions"',
            '"emergency_revoke_tenant_sessions"',
            '"emergency_revoke_tenant_families"',
            '"emergency_revoke_commit"',
            '"revoke_family"',
            '"issue_mfa_challenge_user_load"',
            '"issue_mfa_challenge_expiry"',
            '"issue_mfa_challenge_runtime_write"',
            '"issue_mfa_challenge_pg_insert"',
            '"verify_mfa_challenge_time"',
            '"verify_mfa_proof_user_load"',
            '"verify_mfa_recovery_code"',
            '"list_mfa_factors_user_load"',
            '"disable_mfa_factor_user_load"',
            '"disable_mfa_factor_store"',
            '"revoke_recovery_codes_user_load"',
            '"revoke_recovery_codes_replace"',
            '"admin_reset_mfa_user_load"',
            '"admin_reset_mfa_store"',
            '"delete_webauthn_credentials_runtime"',
            '"delete_webauthn_credentials_pg"',
            '"list_webauthn_credentials"',
            '"delete_webauthn_credential_runtime"',
            '"delete_webauthn_credential_pg"',
            '"rename_passkey_runtime"',
            '"rename_passkey_pg"',
            "lifecycle_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "authn"',
        ),
    ),
    TokenCheck(
        "authn session request validation uses typed field violations",
        "src/runtime/service/auth_service/authn/sessions.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn session_invalid_fields",
            "fn unsupported_validate_token_type_status(",
            '"principal is required"',
            '"refresh_token or session_id is required"',
            '"context.principal_id is required for all_sessions logout"',
            '"user_id is required"',
            '"supported token_type values are SESSION, API_KEY, JWT_ACCESS, and JWT_REFRESH"',
            '"must include an authenticated principal"',
            '"must include a refresh token or session id"',
            '"must be a non-empty principal id when all_sessions is true"',
            '"must be a non-empty user id"',
            '"must be SESSION, API_KEY, JWT_ACCESS, or JWT_REFRESH"',
            "create_session_missing_principal_carries_field_violation",
            "refresh_token_missing_credential_carries_field_violations",
            "logout_all_sessions_missing_principal_context_carries_field_violation",
            "list_sessions_missing_user_id_carries_field_violation",
            "unsupported_validate_token_type_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            "assert_validation_fields(",
        ),
    ),
    TokenCheck(
        "authn session policy denials use typed policy detail",
        "src/runtime/service/auth_service/authn/sessions.rs",
        (
            "fn session_policy_status_with_code(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "fn list_sessions_tenant_scope_required_status() -> Status",
            "fn list_sessions_target_tenant_required_status() -> Status",
            "fn refresh_user_active_status() -> Status",
            '"tenant_scoped_bearer_required"',
            '"target_user_tenant_required"',
            '"user_not_active"',
            "return Err(list_sessions_tenant_scope_required_status());",
            "return Err(list_sessions_target_tenant_required_status());",
            "return Err(refresh_user_active_status());",
            "list_sessions_denies_tenantless_non_admin_before_store_access",
            "session_policy_denials_carry_typed_detail",
            "ErrorKind::Policy",
            "assert_policy_detail(",
            "detail.policy_decision_id",
        ),
    ),
    TokenCheck(
        "authn session internals use typed internal detail",
        "src/runtime/service/auth_service/authn/sessions.rs",
        (
            "fn session_internal_status(",
            'crate::runtime::executor_utils::internal_status("authn", operation, message)',
            '"authorize_list_sessions_target_user"',
            '"create_login_session"',
            '"create_session"',
            '"refresh_session"',
            '"revoke_sessions_for_principal"',
            '"revoke_session"',
            '"refresh_token_legacy_session"',
            '"logout_all_sessions"',
            '"logout_session"',
            '"validate_token_session"',
            '"validate_token_api_key"',
            '"get_session"',
            '"list_sessions"',
            '"validate_csrf_session"',
            "session_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "authn"',
        ),
    ),
    TokenCheck(
        "authn login/password request validation uses typed field violations",
        "src/runtime/service/auth_service/authn/login.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn login_invalid_fields",
            '"no credential supplied (set api_key, session_id, bearer_token, or external_provider_id+external_token)"',
            '"must include one supported credential"',
            '"must satisfy the configured password policy"',
            "authenticate_missing_credential_carries_field_violations",
            "change_password_weak_new_password_carries_field_violation",
            "reset_password_weak_new_password_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            "assert_validation_fields(",
        ),
    ),
    TokenCheck(
        "authn login tenant MFA policy uses typed policy detail",
        "src/runtime/service/auth_service/authn/login.rs",
        (
            "fn tenant_mfa_enrollment_required_status(",
            "crate::runtime::executor_utils::policy_status",
            '"password_login"',
            '"tenant_mfa_enrollment_required"',
            "MFA enrollment required by tenant policy",
            "return Err(tenant_mfa_enrollment_required_status());",
            "tenant_mfa_enrollment_policy_carries_typed_detail",
            "ErrorKind::Policy",
            "assert_eq!(detail.policy_decision_id, policy_decision_id);",
        ),
    ),
    TokenCheck(
        "authn login/password permission denials use typed policy detail",
        "src/runtime/service/auth_service/authn/login.rs",
        (
            "fn login_policy_status_with_code(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "tonic::Code::PermissionDenied",
            "fn password_login_user_active_status() -> Status",
            "fn password_change_otp_verified_status() -> Status",
            "fn reset_password_request_valid_status() -> Status",
            '"password_login"',
            '"user_not_active"',
            '"change_password"',
            '"password_change_otp_verified"',
            '"reset_password"',
            '"reset_request_valid"',
            "return Err(password_login_user_active_status());",
            "return Err(password_change_otp_verified_status());",
            ".ok_or_else(reset_password_request_valid_status)?;",
            "login_password_policy_denials_carry_permission_detail",
            "assert_permission_policy_detail(",
            "ErrorKind::Policy",
            "detail.policy_decision_id",
        ),
    ),
    TokenCheck(
        "authn login/password internals use typed ErrorDetail",
        "src/runtime/service/auth_service/authn/login.rs",
        (
            "fn login_internal_status(",
            'crate::runtime::executor_utils::internal_status("authn", operation, message)',
            '"authenticate_api_key"',
            '"authenticate_session"',
            '"password_login_user_lookup"',
            '"password_login_email_lookup"',
            '"password_login_failed_attempt_update"',
            '"password_login_success_update"',
            '"password_login_tenant_mfa_policy"',
            '"password_login_consume_recovery_code"',
            '"change_password_user_load"',
            '"change_password_otp_load"',
            '"change_password_store"',
            '"forgot_password_username_lookup"',
            '"forgot_password_email_lookup"',
            '"reset_password_user_load"',
            '"reset_password_store"',
            "login_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "authn"',
        ),
    ),
    TokenCheck(
        "authn main request validation uses typed field violations",
        "src/runtime/service/auth_service/authn/mod.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn authn_field_violation(",
            "pub(super) fn require_uuid_arg(",
            "fn webauthn_user_id_required_status(",
            "fn webauthn_user_id_uuid_status(",
            "fn webauthn_challenge_id_invalid_status(",
            "fn validate_webauthn_finish_fields(",
            "fn invalid_webauthn_registration_credential_json_status(",
            "fn invalid_webauthn_authentication_credential_json_status(",
            "fn oidc_id_token_required_status(",
            "fn oidc_issuer_required_status(",
            "fn oidc_client_id_required_status(",
            "fn oidc_client_id_audience_mismatch_status(",
            "fn oidc_nonce_required_status(",
            "fn invalid_oidc_issuer_status(",
            '"{field} is required"',
            '"{field} must be a valid UUID"',
            '"user_id is required"',
            '"WebAuthn users must have UUID user_id"',
            '"challenge_id must be a valid UUID"',
            '"challenge_id and public_key_credential_json are required"',
            '"invalid WebAuthn registration credential JSON: {}"',
            '"invalid WebAuthn authentication credential JSON: {}"',
            '"OIDC authentication requires external_token or bearer_token containing an ID token"',
            '"OIDC issuer is required (provider registry, request issuer, or UDB_OIDC_ISSUER)"',
            '"OIDC client_id/audience is required"',
            '"OIDC client_id and audience must match"',
            '"OIDC nonce is required in attributes[\\"nonce\\"]"',
            '"invalid OIDC issuer: {err}"',
            '"must be a non-empty UUID"',
            '"must be a valid UUID"',
            '"must be a non-empty user id"',
            '"must be a valid UUID for WebAuthn ceremonies"',
            '"must be a non-empty WebAuthn challenge id"',
            '"must be a non-empty WebAuthn credential JSON payload"',
            '"must decode as a WebAuthn registration credential"',
            '"must decode as a WebAuthn authentication credential"',
            '"must contain an OIDC ID token when bearer_token is empty"',
            '"must contain an OIDC ID token when external_token is empty"',
            '"must be supplied by the provider registry, request issuer, or UDB_OIDC_ISSUER"',
            '"must be supplied by the request, provider registry, or UDB_OIDC_CLIENT_ID"',
            '"must be supplied when client_id is empty and no provider/default client is configured"',
            '"must match audience when both are supplied"',
            '"must match client_id when both are supplied"',
            '"must include a non-empty OIDC nonce"',
            '"must be a valid OIDC issuer URL"',
            "require_uuid_arg_carries_field_violations",
            "webauthn_boundary_validation_carries_field_violations",
            "oidc_boundary_validation_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'assert_single_field_violation(&missing, "otp_id"',
            'assert_single_field_violation(&malformed, "device_id"',
            'assert_single_field_violation(&missing_user, "user_id"',
            "let bad_user_uuid = AuthnServiceImpl::webauthn_user_id_uuid_status();",
            'assert_single_field_violation(&bad_challenge, "challenge_id"',
            "let missing_issuer = AuthnServiceImpl::oidc_issuer_required_status();",
            "let missing_nonce = AuthnServiceImpl::oidc_nonce_required_status();",
            'assert_single_field_violation(&bad_issuer,',
            'detail.field_violations[1].field',
        ),
    ),
    TokenCheck(
        "authn main internals use typed ErrorDetail",
        "src/runtime/service/auth_service/authn/mod.rs",
        (
            "fn authn_internal_status(",
            'crate::runtime::executor_utils::internal_status("authn", operation, message)',
            '"emit_event_in_tx"',
            '"user_is_active"',
            '"jwt_session_validate"',
            '"oidc_http_client_build"',
            '"oidc_claims_serialize"',
            '"oidc_verification_task"',
            '"store_webauthn_challenge_decode_json"',
            '"store_webauthn_challenge_expiry"',
            '"load_webauthn_challenge_query"',
            '"load_webauthn_challenge_user_id"',
            '"load_webauthn_challenge_state_json"',
            '"load_webauthn_challenge_tenant_id"',
            '"load_webauthn_challenge_project_id"',
            '"consume_webauthn_challenge_runtime"',
            '"consume_webauthn_challenge_pg"',
            '"load_webauthn_passkeys_query"',
            '"load_webauthn_passkeys_decode_json"',
            '"webauthn_credential_id_serialize"',
            '"insert_webauthn_passkey_decode_json"',
            '"update_webauthn_passkey_decode_json"',
            '"update_webauthn_passkey_runtime"',
            '"update_webauthn_passkey_pg"',
            '"start_webauthn_registration_user_load"',
            '"start_webauthn_registration_decode_passkey"',
            '"start_webauthn_registration_begin"',
            '"start_webauthn_registration_challenge_serialize"',
            '"start_webauthn_registration_state_serialize"',
            '"finish_webauthn_registration_user_load"',
            '"finish_webauthn_registration_state_decode"',
            '"finish_webauthn_registration_origin_parse"',
            '"finish_webauthn_registration_dev_make"',
            '"finish_webauthn_registration_dev_decode"',
            '"finish_webauthn_registration_passkey_serialize"',
            '"finish_webauthn_registration_user_store"',
            '"start_webauthn_authentication_user_load"',
            '"start_webauthn_authentication_decode_passkey"',
            '"start_webauthn_authentication_begin"',
            '"start_webauthn_authentication_challenge_serialize"',
            '"start_webauthn_authentication_state_serialize"',
            '"finish_webauthn_authentication_user_load"',
            '"finish_webauthn_authentication_state_decode"',
            '"finish_webauthn_authentication_origin_parse"',
            '"finish_webauthn_authentication_dev_make"',
            '"finish_webauthn_authentication_dev_decode"',
            '"finish_webauthn_authentication_passkey_decode"',
            '"finish_webauthn_authentication_passkey_serialize"',
            "authn_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "authn"',
        ),
    ),
    TokenCheck(
        "Authn WebAuthn attestation missing fields use typed field violations",
        "src/runtime/service/auth_service/authn/mod.rs",
        (
            "fn webauthn_attestation_invalid_field_status(",
            "crate::runtime::executor_utils::invalid_argument_fields",
            '"attStmt.sig"',
            '"attStmt.alg"',
            '"attStmt.x5c"',
            '"attStmt.certInfo"',
            '"attStmt.pubArea"',
            '"authData.rpIdHash"',
            '"authData"',
            '"authData.flags"',
            '"authData.aaguid"',
            '"authData.credentialIdLength"',
            '"authData.credentialId"',
            '"authData.credentialPublicKey"',
            '"attestationObject"',
            "WebAuthn policy: packed attestation missing attStmt.sig",
            "WebAuthn policy: unparseable attestationObject",
            "WebAuthn policy: malformed authenticator data (cannot evaluate UV)",
            "WebAuthn policy: packed attestation signature is invalid",
            "WebAuthn policy: tpm attestation signature is invalid",
            "WebAuthn policy: android-key attestation signature is invalid",
            "WebAuthn policy: fido-u2f attestation signature is invalid",
            "WebAuthn policy: verify packed attestation signature failed",
            "WebAuthn policy: verify tpm attestation signature failed",
            "WebAuthn policy: verify android-key attestation signature failed",
            "WebAuthn policy: verify fido-u2f attestation signature failed",
            "WebAuthn policy: tpm attestation missing attStmt.certInfo",
            "WebAuthn policy: android-key attestation missing attStmt.sig",
            "WebAuthn policy: fido-u2f attestation missing attStmt.sig",
            "WebAuthn policy: parse packed attestation leaf certificate failed",
            "WebAuthn policy: parse tpm attestation leaf certificate failed",
            "WebAuthn policy: parse android-key attestation leaf certificate failed",
            "WebAuthn policy: parse fido-u2f attestation leaf certificate failed",
            "WebAuthn policy: fido-u2f attestation has malformed authenticator data",
            "WebAuthn policy: fido-u2f attestation missing attested credential data",
            "WebAuthn policy: fido-u2f attestation missing credential AAGUID",
            "WebAuthn policy: fido-u2f attestation missing credential id length",
            "WebAuthn policy: fido-u2f attestation malformed credential id length",
            "WebAuthn policy: fido-u2f attestation truncated credential id",
            "WebAuthn policy: fido-u2f attestation requires an EC2/P-256/ES256 credential key",
            "WebAuthn policy: {fmt} attestation alg {alg} is not supported",
            "WebAuthn policy: attestation format '{fmt}' is not supported for statement",
            "WebAuthn policy: attestation format '{fmt}' is not supported for OpenSSL chain",
            "WebAuthn policy: attestation format '{fmt}' did not include attStmt.x5c",
            "WebAuthn policy: parse attestation leaf certificate failed",
            "WebAuthn policy: parse attestation intermediate certificate failed",
            "WebAuthn policy: tpm attestation requires attStmt.ver = \\\"2.0\\\"",
            "WebAuthn policy: tpm certInfo magic is invalid",
            "WebAuthn policy: tpm certInfo type is not TPM_ST_ATTEST_CERTIFY",
            "WebAuthn policy: tpm certInfo extraData does not match authenticator/client data",
            "WebAuthn policy: tpm certInfo truncated before certify info",
            "WebAuthn policy: tpm attestation certInfo name does not match pubArea",
            "WebAuthn policy: tpm pubArea is truncated before nameAlg",
            "WebAuthn policy: tpm pubArea nameAlg 0x{alg:04x} is not supported",
            '"must be a supported COSE algorithm for WebAuthn attestation verification"',
            '"must be packed, tpm, android-key, or fido-u2f for attestation signature verification"',
            '"must be packed, tpm, android-key, or fido-u2f for attestation chain validation"',
            '"must include at least one X.509 certificate for attestation chain validation"',
            '"must contain a valid DER-encoded attestation leaf certificate"',
            '"must contain valid DER-encoded attestation intermediate certificates"',
            '"must verify the packed attestation statement signature"',
            '"must verify the TPM attestation statement signature"',
            '"must verify the android-key attestation statement signature"',
            '"must verify the FIDO U2F attestation statement signature"',
            '"must be a well-formed packed attestation statement signature"',
            '"must be a well-formed TPM attestation statement signature"',
            '"must be a well-formed android-key attestation statement signature"',
            '"must be a well-formed FIDO U2F attestation statement signature"',
            '"must be \\\"2.0\\\" for TPM attestation signature verification"',
            '"must contain a valid TPM2B certInfo structure"',
            '"must contain a TPM_ST_ATTEST_CERTIFY attestation type"',
            '"must decode as a WebAuthn attestationObject CBOR map"',
            '"must be at least 37 bytes to evaluate WebAuthn user verification"',
            '"must contain a valid DER-encoded packed attestation leaf certificate"',
            '"must contain a valid DER-encoded TPM attestation leaf certificate"',
            '"must contain a valid DER-encoded android-key attestation leaf certificate"',
            '"must contain a valid DER-encoded FIDO U2F attestation leaf certificate"',
            '"must decode as an EC2/P-256/ES256 COSE credential public key"',
            '"must bind to the authenticator data and clientDataHash"',
            '"must include TPM clockInfo, firmwareVersion, and certify info"',
            '"must name the same TPM public area as attStmt.pubArea"',
            '"must include a TPM public area nameAlg"',
            '"must use SHA-256, SHA-384, or SHA-512 as the TPM nameAlg"',
            "assert_attestation_field_violation(",
            "registration_policy_unparseable_attestation_object_carries_field_violation",
            "registration_policy_malformed_auth_data_carries_field_violation",
            "packed_attestation_invalid_signature_carries_field_violation",
            "tpm_attestation_invalid_signature_carries_field_violation",
            "android_key_attestation_invalid_signature_carries_field_violation",
            "fido_u2f_attestation_invalid_signature_carries_field_violation",
            "packed_attestation_malformed_signature_carries_field_violation",
            "tpm_attestation_malformed_signature_carries_field_violation",
            "android_key_attestation_malformed_signature_carries_field_violation",
            "fido_u2f_attestation_malformed_signature_carries_field_violation",
            "packed_attestation_malformed_leaf_certificate_carries_field_violation",
            "tpm_attestation_malformed_leaf_certificate_carries_field_violation",
            "android_key_attestation_malformed_leaf_certificate_carries_field_violation",
            "fido_u2f_attestation_malformed_leaf_certificate_carries_field_violation",
            "unsupported_attestation_alg_carries_field_violation",
            "unsupported_attestation_format_carries_field_violation",
            "unsupported_attestation_chain_format_carries_field_violation",
            "malformed_attestation_chain_leaf_carries_field_violation",
            "malformed_attestation_chain_intermediate_carries_field_violation",
            "tpm_attestation_invalid_version_carries_field_violation",
            "tpm_attestation_cert_info_name_mismatch_carries_field_violation",
            "tpm_cert_info_rejects_malformed_structure_with_field_detail",
            "fido_u2f_attestation_missing_auth_data_field_carries_validation_detail",
            "fido_u2f_attestation_malformed_auth_data_carries_validation_detail",
            "tpm_cert_info_binds_extra_data_and_pub_area_name",
            "typed detail trailer is present",
            "ErrorKind::Validation",
            "detail.field_violations[0].field",
            "detail.field_violations[0].description",
        ),
    ),
    TokenCheck(
        "Authn WebAuthn policy denials use typed policy detail",
        "src/runtime/service/auth_service/authn/mod.rs",
        (
            "fn webauthn_policy_status(",
            "crate::runtime::executor_utils::policy_status",
            '"webauthn_registration_policy"',
            '"webauthn_assertion_policy"',
            '"attestation_conveyance_not_allowed"',
            '"resident_key_required"',
            '"registration_user_verification_required"',
            '"assertion_user_verification_required"',
            "WebAuthn policy: attestation conveyance '{fmt}' not permitted",
            "WebAuthn policy: tenant requires a resident (discoverable) key",
            "WebAuthn policy: tenant requires user verification but the registration",
            "WebAuthn policy: tenant requires user verification but the assertion reported",
            "assert_webauthn_policy_detail(",
            "assert_eq!(detail.kind, ErrorKind::Policy as i32);",
            "assert_eq!(detail.operation, operation);",
            "assert_eq!(detail.policy_decision_id, policy_decision_id);",
            "deny_registration_when_attestation_conveyance_required_but_none",
            "deny_registration_when_resident_key_required_but_not_reported",
            "registration_uv_required_denies_uv_false_with_policy_detail",
            "assertion_uv_required_denies_uv_false_and_allows_uv_true",
        ),
    ),
    TokenCheck(
        "Authn WebAuthn ceremony and user-scope denials use typed permission policy detail",
        "src/runtime/service/auth_service/authn/mod.rs",
        (
            "fn webauthn_permission_policy_status(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "tonic::Code::PermissionDenied",
            "fn webauthn_invalid_ceremony_status(operation: &'static str) -> Status",
            "fn webauthn_user_tenant_mismatch_status(operation: &'static str) -> Status",
            "fn webauthn_user_project_mismatch_status(operation: &'static str) -> Status",
            '"invalid_webauthn_ceremony"',
            '"tenant_id_user_mismatch"',
            '"project_id_user_mismatch"',
            '"finish_webauthn_registration"',
            '"finish_webauthn_authentication"',
            '"start_webauthn_registration"',
            '"start_webauthn_authentication"',
            "return Err(webauthn_invalid_ceremony_status(",
            "return Err(webauthn_user_tenant_mismatch_status(",
            "return Err(webauthn_user_project_mismatch_status(",
            "fn assert_permission_policy_detail(",
            "assert_eq!(status.code(), tonic::Code::PermissionDenied);",
            "webauthn_permission_denials_carry_typed_policy_detail",
        ),
    ),
    TokenCheck(
        "IdP provider and mapping validation uses typed field violations",
        "src/runtime/service/auth_service/idp/mod.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn idp_invalid_fields",
            "fn idp_tenant_id_required_status(",
            "fn idp_display_name_required_status(",
            "fn idp_claims_json_invalid_status(",
            "fn idp_subject_user_required_status(",
            "fn idp_claims_subject_required_status(",
            "fn idp_saml_metadata_required_status(",
            "fn idp_saml_metadata_invalid_status(",
            "fn idp_scim_user_json_invalid_status(",
            "fn idp_scim_patch_invalid_status(",
            "fn idp_scim_group_json_invalid_status(",
            '"{field} is required"',
            '"claims_json is not valid JSON: {err}"',
            '"subject and user_id are required"',
            '"claims have no resolvable subject"',
            '"metadata_xml is required (or set the provider\'s saml_metadata_url)"',
            '"invalid SAML metadata: {err}"',
            '"must be a non-empty tenant id"',
            '"must be a non-empty display name"',
            '"must decode as a JSON object of IdP claims"',
            '"must be a non-empty external subject"',
            '"must be a non-empty UDB user id"',
            '"must map to a non-empty external subject claim"',
            '"must contain SAML metadata XML when the provider has no metadata URL"',
            '"must decode as valid SAML metadata XML"',
            '"must decode as a valid SCIM User resource"',
            '"must contain supported SCIM PATCH operations for a User resource"',
            '"must decode as a valid SCIM Group resource"',
            "map_err(idp_claims_json_invalid_status)",
            "map_err(idp_saml_metadata_invalid_status)",
            "map_err(idp_scim_user_json_invalid_status)",
            "map_err(idp_scim_patch_invalid_status)",
            "map_err(idp_scim_group_json_invalid_status)",
            "return Err(idp_subject_user_required_status());",
            "return Err(idp_claims_subject_required_status());",
            "return Err(idp_saml_metadata_required_status());",
        ),
    ),
    TokenCheck(
        "IdP provider and mapping validation decoder tests",
        "src/runtime/service/auth_service/idp/tests.rs",
        (
            "idp_provider_boundary_validation_carries_field_violations",
            "idp_tenant_id_required_status()",
            "idp_display_name_required_status()",
            'idp_claims_json_invalid_status("expected value")',
            "idp_subject_user_required_status()",
            "idp_claims_subject_required_status()",
            "idp_saml_scim_boundary_validation_carries_field_violations",
            "idp_saml_metadata_required_status()",
            'idp_saml_metadata_invalid_status("missing entityID")',
            'idp_scim_user_json_invalid_status("SCIM user must be a JSON object")',
            'idp_scim_patch_invalid_status("unsupported SCIM patch")',
            'idp_scim_group_json_invalid_status("SCIM group is missing required displayName")',
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            "assert_validation_fields(",
            '"must decode as a JSON object of IdP claims"',
            '"must map to a non-empty external subject claim"',
            '"must decode as valid SAML metadata XML"',
            '"must decode as a valid SCIM User resource"',
            '"must decode as a valid SCIM Group resource"',
        ),
    ),
    TokenCheck(
        "IdP store validation uses typed field violations",
        "src/runtime/service/auth_service/idp/store.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn store_invalid_fields",
            "fn uuid_field_status(",
            "fn invalid_json_field_value_status(",
            "fn named_json_field_status(",
            "fn required_store_field_status(",
            "fn not_on_or_after_out_of_range_status(",
            '"{field} must be a UUID"',
            '"invalid JSON field value: {err}"',
            '"{field} must be valid JSON: {err}"',
            '"{field} is required"',
            '"not_on_or_after_unix is out of range"',
            '"must be a valid UUID"',
            '"must decode as valid JSON"',
            '"must be non-empty"',
            '"must be a valid Unix timestamp representable by chrono"',
            "uuid_value(\"not-a-uuid\", \"provider_id\")",
            "required_store_field_status(\"tenant_id\")",
            "not_on_or_after_out_of_range_status()",
            "json_or_null(\"{not-json\")",
            "json_value(\"{not-json\", \"saml_idp_certs_json\")",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            "assert_single_field(",
        ),
    ),
    TokenCheck(
        "OTP cooldown live test decodes typed quota detail",
        "src/runtime/service/auth_service/tests/authn_otp_password_live.rs",
        (
            "decode_detail(&throttled)",
            "ErrorKind::Quota",
            'detail.backend, "authn"',
            'detail.operation, "otp_cooldown"',
            "detail.retry_after_ms > 0",
        ),
    ),
    TokenCheck(
        "LiveQuery streaming backpressure uses typed quota detail",
        "src/runtime/service/livequery_service",
        (
            "livequery_backpressure_status",
            "crate::runtime::executor_utils::quota_status",
            '"livequery"',
            '"subscriber_channel"',
            '"delta feed lag"',
            "backpressure_status_carries_typed_quota_detail",
            "decode_detail(&status)",
            "ErrorKind::Quota",
        ),
    ),
    TokenCheck(
        "LiveQuery request validation uses typed field violations",
        "src/runtime/service/livequery_service",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn livequery_required_field(",
            '"message_type is required"',
            "must name exactly one known tenant-scoped UDB entity",
            '"live query predicate field must not be empty"',
            '"live query predicate comparison op is unspecified"',
            '"must be a non-empty source message type"',
            '"must be a non-empty live query predicate field"',
            '"must specify a live query predicate comparison operator"',
            "subscribe_missing_message_type_carries_field_violation",
            "unknown_source_fails_closed",
            "subscribe_empty_predicate_field_carries_field_violation",
            "subscribe_unspecified_predicate_op_carries_field_violation",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "message_type"',
            'detail.field_violations[0].field, "filters.field"',
            'detail.field_violations[0].field, "filters.op"',
        ),
    ),
    TokenCheck(
        "transaction inline object size cap uses typed quota detail",
        "src/runtime/core/tx_object.rs",
        (
            "crate::runtime::executor_utils::quota_refusal_status",
            '"object"',
            '"transaction inline object size"',
            "Use GeneratePresignedUrl for files > 1MB",
        ),
    ),
    TokenCheck(
        "gRPC object stream size cap uses typed quota detail",
        "src/runtime/core/setup_data.rs",
        (
            "crate::runtime::executor_utils::quota_refusal_status",
            '"object"',
            '"grpc object stream size"',
            "object exceeds UDB_MAX_OBJECT_BYTES",
        ),
    ),
    TokenCheck(
        "core setup-data validation uses typed field violations",
        "src/runtime/core/setup_data.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn setup_data_invalid_field(",
            "fn unknown_message_type_status(",
            "fn empty_object_stream_status(",
            "fn unsupported_presign_method_status(",
            "fn invalid_part_count_status(",
            "fn invalid_presign_ttl_status(",
            '"unknown message_type"',
            '"empty object stream"',
            '"presigned URLs support only PUT or GET"',
            '"part_count must be positive"',
            '"invalid presign ttl: {err}"',
            "setup_data_boundary_validation_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"message_type"',
            '"stream"',
            '"method"',
            '"part_count"',
            '"ttl_seconds"',
        ),
    ),
    TokenCheck(
        "core setup-data vector/object capability refusals use typed detail",
        "src/runtime/core/setup_data.rs",
        (
            "crate::runtime::executor_utils::capability_status",
            "fn setup_data_capability_status(",
            "fn qdrant_vector_feature_status(",
            "fn vector_hybrid_qdrant_only_status(",
            "fn no_object_store_feature_status(",
            "fn s3_object_feature_status(",
            "fn s3_minio_feature_status(",
            "fn gcs_feature_status(",
            "fn azureblob_feature_status(",
            "fn object_instance_missing_status(",
            "fn unsupported_object_backend_status(",
            "fn typed_object_backend_required_status(",
            '"qdrant_feature"',
            '"object_store_feature"',
            '"s3_feature"',
            '"gcs_feature"',
            '"azureblob_feature"',
            '"configured_instance"',
            '"supported_object_backend"',
            '"typed_vector_search_backend"',
            '"typed_vector_upsert_backend"',
            '"object_store_backend"',
            "setup_data_vector_object_capability_refusals_carry_detail",
            "setup_data_typed_dispatch_backend_refusals_carry_capability_detail",
            "setup_data_typed_object_backend_refusal_carries_capability_detail",
            "ErrorKind::Capability",
            "detail.capability_required",
        ),
    ),
    TokenCheck(
        "core setup-data internals use typed internal detail",
        "src/runtime/core/setup_data.rs",
        (
            "fn setup_data_internal_status(",
            'crate::runtime::executor_utils::internal_status("setup_data", operation, message)',
            '"select_connection_acquire"',
            '"select_query"',
            '"join_connection_acquire"',
            '"join_query"',
            '"upsert_transaction_begin"',
            '"upsert_projection_task_enqueue"',
            '"upsert_commit"',
            '"cdc_outbox_emit"',
            '"delete_transaction_begin"',
            '"delete_query"',
            '"delete_projection_task_enqueue"',
            '"delete_commit"',
            '"idempotency_dedup_claim_shape"',
            '"idempotency_response_summary_shape"',
            '"idempotency_response_write_receipt_missing"',
            '"idempotency_response_write_receipt_mismatch"',
            '"idempotency_response_persist_row_count"',
            '"write_receipt_json_encode"',
            '"mutation_resource_uri_token"',
            '"mutation_resource_uri_identity_source"',
            '"mutation_resource_uri_identity_required"',
            '"mutation_resource_uri_identity_ambiguous"',
            '"mutation_resource_uri_equality_scalar"',
            '"mutation_resource_uri_scalar_equality_required"',
            '"upsert_returning_record_json"',
            '"idempotency_replay_response"',
            '"vector_search_spec_encode"',
            '"vector_upsert_spec_encode"',
            '"vector_search_response_parse"',
            "setup_data_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "setup_data"',
        ),
    ),
    TokenCheck(
        "core probe/dispatch validation uses typed field violations",
        "src/runtime/core/probe_dispatch.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn probe_dispatch_invalid_field(",
            "fn outbox_topic_not_allowed_status(",
            "fn unknown_probe_backend_status(",
            '"topic \'{topic}\' is not in the registered topic registry; \\',
            '"unknown backend \'{backend}\'; valid: postgres, redis, mongodb, neo4j, clickhouse, qdrant, s3, minio"',
            "probe_dispatch_validation_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"topic"',
            '"backend"',
        ),
    ),
    TokenCheck(
        "core probe/dispatch internals use typed internal detail",
        "src/runtime/core/probe_dispatch.rs",
        (
            "fn probe_dispatch_internal_status(",
            'crate::runtime::executor_utils::internal_status("probe_dispatch", operation, message)',
            '"enqueue_outbox_event"',
            '"topic_policy_allows"',
            '"failed to enqueue event: {e}"',
            '"topic policy query failed: {err}"',
            "probe_dispatch_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "probe_dispatch"',
        ),
    ),
    TokenCheck(
        "data handler neutral-IR validation uses typed field violations",
        "src/runtime/service/handlers_data.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn handlers_data_invalid_field(",
            "fn neutral_ir_compile_failed_status(",
            '"put_object spec_json must be valid JSON: {err}"',
            '"unknown operation \'{other}\'; allowed: ping, probe, ensure_resource, drop_resource, list_resources, query, mutate, transaction, search, get_object, put_object, delete_object"',
            '"invalid spec_json: {err}"',
            '"backend \'{backend}\' has no neutral-IR compiler"',
            '"neutral IR dispatch requires `ir.op`"',
            '"neutral IR dispatch `ir` must be an object"',
            '"invalid LogicalRead: {err}"',
            '"invalid LogicalWrite: {err}"',
            '"invalid LogicalUpdate: {err}"',
            '"invalid LogicalDelete: {err}"',
            '"invalid LogicalSearch: {err}"',
            '"invalid LogicalResourceOp: {err}"',
            '"invalid LogicalAggregate: {err}"',
            '"unsupported neutral IR op \'{other}\'"',
            '"neutral IR compile failed [{}]: {err}"',
            '"compiled Neo4j rendering missing statement"',
            '"compiled Neo4j statement missing text"',
            '"compiled Qdrant resource op missing collection name in path"',
            '"compiled MongoDB createCollection missing collection"',
            '"compiled MongoDB dropIndex missing name"',
            '"MongoDB compiled rendering body must be an object"',
            '"Qdrant compiled rendering body must be an object"',
            '"compiled SQL has more placeholders than params"',
            '"compiled SQL has more params than placeholders"',
            "neutral_ir_validation_carries_field_violations",
            "compiled_rendering_validation_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"spec_json"',
            '"operation"',
            '"backend"',
            '"ir.op"',
            '"ir"',
            '"statements"',
            '"statements.statement"',
            '"collection"',
            '"name"',
            '"body"',
            '"params"',
        ),
    ),
    TokenCheck(
        "data handler generic dispatch denials use typed policy/capability details",
        "src/runtime/service/handlers_data.rs",
        (
            "fn generic_dispatch_scope_status(",
            "crate::runtime::executor_utils::policy_status_with_code",
            '"GenericDispatch"',
            '"dispatch_scope_required"',
            "Err(generic_dispatch_scope_status())",
            "fn raw_dispatch_disabled_status(",
            "crate::runtime::executor_utils::policy_status",
            '"generic_dispatch_raw_dispatch"',
            '"raw_dispatch_requires_ir_envelope"',
            "fn neutral_ir_compiler_unavailable_status(",
            "crate::runtime::executor_utils::capability_status",
            '"neutral_ir_compiler"',
            "fn generic_dispatch_compiled_capability_status(",
            '"generic_dispatch_object_resource_op"',
            '"qdrant_resource_http_method"',
            '"mongodb_resource_path"',
            "compiled_rendering_capability_denials_carry_error_detail",
            "raw_dispatch_gate_blocks_mediated_backend_in_production",
            "generic_dispatch_scope_denial_carries_policy_detail",
            "ErrorKind::Policy",
            "ErrorKind::Capability",
            "assert_policy_detail(",
            "assert_capability_detail(",
        ),
    ),
    TokenCheck(
        "data handler generic dispatch internal failures use typed internal detail",
        "src/runtime/service/handlers_data.rs",
        (
            "fn generic_dispatch_internal_status(",
            "crate::runtime::executor_utils::internal_status(backend, operation, message)",
            "panic_backend",
            "panic_operation",
            '"probe"',
            '"list_resources"',
            '"backend operation panicked; request failed (broker stayed up)"',
            "generic_dispatch_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            "detail.backend, backend",
            "detail.operation, operation",
        ),
    ),
    TokenCheck(
        "Azure Blob executor op validation uses typed field violations",
        "src/runtime/executors/azureblob.rs",
        (
            "crate::runtime::executor_utils::{",
            "invalid_argument_fields",
            "fn object_op_mismatch_status(",
            'format!("{method} expects op=\\"{expected}\\", got \'{actual}\'")',
            '"must be \\"{expected}\\" when calling {method}"',
            "object_operation_mismatch_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "op"',
        ),
    ),
    TokenCheck(
        "GCS executor op validation uses typed field violations",
        "src/runtime/executors/gcs.rs",
        (
            "crate::runtime::executor_utils::{",
            "invalid_argument_fields",
            "fn object_op_mismatch_status(",
            'format!("{method} expects op=\\"{expected}\\", got \'{actual}\'")',
            '"must be \\"{expected}\\" when calling {method}"',
            "object_operation_mismatch_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "op"',
        ),
    ),
    TokenCheck(
        "Weaviate executor resource-spec validation uses typed field violations",
        "src/runtime/executors/weaviate.rs",
        (
            "invalid_argument_fields",
            "fn invalid_ensure_resource_spec_status(",
            '"invalid spec: {err}"',
            '"must be valid JSON for Weaviate ensure_resource"',
            "ensure_resource_spec_validation_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "spec_json"',
        ),
    ),
    TokenCheck(
        "Pinecone executor resource-spec validation uses typed field violations",
        "src/runtime/executors/pinecone.rs",
        (
            "invalid_argument_fields",
            "fn invalid_ensure_resource_spec_status(",
            '"invalid spec: {err}"',
            '"must be valid JSON for Pinecone ensure_resource"',
            "ensure_resource_spec_validation_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "spec_json"',
        ),
    ),
    TokenCheck(
        "Elasticsearch executor resource-spec validation uses typed field violations",
        "src/runtime/executors/elasticsearch.rs",
        (
            "invalid_argument_fields",
            "fn invalid_ensure_resource_spec_status(",
            '"invalid ensure_resource spec: {err}"',
            '"must be valid JSON for Elasticsearch ensure_resource"',
            "ensure_resource_spec_validation_carries_field_violation",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "spec_json"',
        ),
    ),
    TokenCheck(
        "Cassandra executor CQL validation uses typed field violations",
        "src/runtime/executors/cassandra.rs",
        (
            "invalid_argument_fields",
            "fn cassandra_sql_validation_status(",
            '"cassandra query accepts SELECT only, got \'{other}\'"',
            '"must start with SELECT for Cassandra query dispatch"',
            '"cassandra compiler-mediated mutate does not accept \'{}\'"',
            '"compiler-mediated Cassandra mutation must be CREATE/DROP table, index, or keyspace DDL"',
            '"cassandra mutate accepts INSERT/UPDATE/DELETE/BATCH only, got \'{other}\'"',
            '"must start with INSERT, UPDATE, DELETE, or BEGIN for Cassandra mutation dispatch"',
            "query_keyword_validation_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "sql"',
        ),
    ),
    TokenCheck(
        "Cassandra executor internals use typed internal detail",
        "src/runtime/executors/cassandra.rs",
        (
            "fn cassandra_internal_status(",
            'crate::runtime::executor_utils::internal_status("cassandra", operation, message)',
            "fn encode_cassandra_response(",
            '"live_probe_query"',
            '"query"',
            '"query_response_encode"',
            '"mutate"',
            '"list_resources_parse"',
            "cassandra_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "cassandra"',
        ),
    ),
    TokenCheck(
        "ClickHouse executor validation uses typed field violations",
        "src/runtime/executors/clickhouse.rs",
        (
            "invalid_argument_fields",
            "fn clickhouse_invalid_field_status(",
            "fn invalid_clickhouse_request_json_status(",
            "fn clickhouse_required_field_status(",
            "fn clickhouse_identifier_status(",
            '"invalid request json: {err}"',
            '"must be valid JSON for ClickHouse generic dispatch"',
            '"missing required field \'table\'"',
            '"rows must be an array"',
            '"must be a valid ClickHouse identifier"',
            '"ClickHouse filter values must be scalar"',
            '"filter values must be scalar JSON values"',
            '"compiler-mediated ClickHouse mutation must be INSERT, CREATE TABLE IF NOT EXISTS, DROP TABLE IF EXISTS, or ALTER TABLE ... DELETE WHERE"',
            '"compiled ClickHouse mutate allows only INSERT, CREATE TABLE IF NOT EXISTS,',
            "clickhouse_generic_dispatch_validation_carries_field_violations",
            "clickhouse_template_validation_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"request_json"',
            '"table"',
            '"rows"',
            '"columns"',
            '"filter"',
            '"order_by"',
            '"sql"',
        ),
    ),
    TokenCheck(
        "SQLite executor validation uses typed field violations",
        "src/runtime/executors/sqlite.rs",
        (
            "invalid_argument_fields",
            "fn sqlite_invalid_field_status(",
            "fn invalid_sqlite_resource_spec_status(",
            "fn invalid_sqlite_tx_json_status(",
            "fn sqlite_required_field_status(",
            "fn sqlite_identifier_status(",
            '"invalid resource spec: {err}"',
            '"must be valid JSON for SQLite table resource creation"',
            '"invalid tx JSON: {err}"',
            '"must be valid JSON for SQLite transaction dispatch"',
            '"table resource spec requires columns"',
            '"table resource spec requires at least one column"',
            '"column missing name"',
            '"column missing type"',
            '"invalid SQL type for column \'{name}\'"',
            '"missing `statements` array in tx request"',
            '"tx statement missing `sql`"',
            "sqlite_resource_validation_carries_field_violations",
            "sqlite_transaction_validation_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"spec_json"',
            '"request_json"',
            '"columns"',
            '"columns.name"',
            '"columns.type"',
            '"resource_name"',
            '"statements"',
            '"statements.sql"',
        ),
    ),
    TokenCheck(
        "SQLite executor internals use typed internal detail",
        "src/runtime/executors/sqlite.rs",
        (
            "fn sqlite_internal_status(",
            'crate::runtime::executor_utils::internal_status("sqlite", operation, message)',
            "fn encode_sqlite_response(",
            '"context_table_create"',
            '"query_transaction_start"',
            '"query"',
            '"query_transaction_commit"',
            '"query_response_encode"',
            '"mutate_transaction_start"',
            '"mutate"',
            '"mutate_transaction_commit"',
            '"ensure_resource"',
            '"drop_resource"',
            '"list_resources"',
            '"transaction_begin"',
            '"transaction_statement"',
            '"transaction_commit"',
            "sqlite_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "sqlite"',
        ),
    ),
    TokenCheck(
        "MySQL executor validation uses typed field violations",
        "src/runtime/executors/mysql.rs",
        (
            "invalid_argument_fields",
            "fn mysql_invalid_field_status(",
            "fn invalid_mysql_resource_spec_status(",
            "fn invalid_mysql_tx_json_status(",
            "fn mysql_required_field_status(",
            "fn mysql_identifier_status(",
            '"invalid resource spec: {err}"',
            '"must be valid JSON for MySQL table resource creation"',
            '"invalid tx JSON: {err}"',
            '"must be valid JSON for MySQL transaction dispatch"',
            '"table resource spec requires columns"',
            '"table resource spec requires at least one column"',
            '"column missing name"',
            '"column missing type"',
            '"invalid SQL type for column \'{name}\'"',
            '"missing `statements` array in tx request"',
            '"tx statement missing `sql`"',
            "mysql_resource_validation_carries_field_violations",
            "mysql_transaction_validation_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"spec_json"',
            '"request_json"',
            '"columns"',
            '"columns.name"',
            '"columns.type"',
            '"engine"',
            '"resource_name"',
            '"statements"',
            '"statements.sql"',
        ),
    ),
    TokenCheck(
        "Neo4j executor validation uses typed field violations",
        "src/runtime/executors/neo4j.rs",
        (
            "invalid_argument_fields",
            "fn neo4j_invalid_field_status(",
            "fn invalid_neo4j_request_json_status(",
            "fn neo4j_required_field_status(",
            "fn unsupported_neo4j_operation_status(",
            "fn neo4j_identifier_status(",
            '"invalid request json: {err}"',
            '"must be valid JSON for Neo4j generic dispatch"',
            '"missing required field \'{field}\'"',
            '"unsupported Neo4j mutation operation \'{operation}\'"',
            "neo4j_query_validation_carries_field_violations",
            "neo4j_mutation_validation_carries_field_violations",
            "neo4j_resource_identifier_validation_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"request_json"',
            '"label"',
            '"operation"',
            '"cypher"',
            '"resource_name"',
            '"constraint_property"',
        ),
    ),
    TokenCheck(
        "Neo4j executor internals use typed internal detail",
        "src/runtime/executors/neo4j.rs",
        (
            "fn neo4j_internal_status(",
            'crate::runtime::executor_utils::internal_status("neo4j", operation, message)',
            "fn encode_neo4j_response(",
            '"query_cypher"',
            '"find_nodes"',
            '"query_response_encode"',
            '"mutate_cypher"',
            '"mutate_response_encode"',
            '"create_node"',
            '"update_node"',
            '"delete_node"',
            '"create_relationship"',
            '"ensure_resource"',
            '"drop_resource"',
            '"list_resources"',
            "neo4j_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "neo4j"',
        ),
    ),
    TokenCheck(
        "Qdrant executor validation uses typed field violations",
        "src/runtime/executors/qdrant.rs",
        (
            "invalid_argument_fields",
            "fn qdrant_invalid_field_status(",
            "fn invalid_qdrant_request_json_status(",
            "fn invalid_qdrant_ensure_resource_spec_status(",
            "fn qdrant_required_field_status(",
            "fn unsupported_qdrant_operation_status(",
            '"invalid request json: {err}"',
            '"must be valid JSON for Qdrant generic dispatch"',
            '"invalid qdrant ensure_resource spec: {err}"',
            '"must be valid JSON for Qdrant ensure_resource"',
            '"Qdrant collection name must be 1–255 characters"',
            '"Qdrant collection name may only contain ASCII letters, digits, hyphens, and underscores"',
            '"Qdrant collection name may not start with \'.\' or \'-\'"',
            '"points must be an array"',
            '"point_ids must be an array when filter is absent"',
            '"payload is required"',
            '"set_payload requires point_ids or filter"',
            '"unsupported Qdrant mutation operation \'{operation}\'"',
            "qdrant_collection_validation_carries_field_violations",
            "qdrant_search_validation_carries_field_violations",
            "qdrant_mutation_validation_carries_field_violations",
            "qdrant_resource_spec_validation_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"request_json"',
            '"spec_json"',
            '"collection"',
            '"points"',
            '"point_ids"',
            '"payload"',
            '"operation"',
        ),
    ),
    TokenCheck(
        "MongoDB executor validation uses typed field violations",
        "src/runtime/executors/mongodb.rs",
        (
            "invalid_argument_fields",
            "fn mongodb_invalid_field_status(",
            "fn invalid_mongodb_request_json_status(",
            "fn mongodb_required_field_status(",
            "fn unsupported_mongodb_mutation_operation_status(",
            '"invalid request json: {err}"',
            '"must be valid JSON for MongoDB generic dispatch"',
            '"missing required field \'collection\'"',
            '"missing required field \'operation\'"',
            '"document is required"',
            '"documents must be an array"',
            '"filter is required"',
            '"update is required"',
            '"indexes must be an array"',
            '"index name is required"',
            '"operations must be an array"',
            '"unsupported MongoDB mutation operation \'{operation}\'"',
            "mongodb_query_validation_carries_field_violations",
            "mongodb_mutation_validation_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"request_json"',
            '"collection"',
            '"operation"',
            '"document"',
            '"documents"',
            '"filter"',
            '"update"',
            '"indexes"',
            '"name"',
            '"operations"',
        ),
    ),
    TokenCheck(
        "MongoDB executor internals use typed internal detail",
        "src/runtime/executors/mongodb.rs",
        (
            "fn mongodb_internal_status(",
            'crate::runtime::executor_utils::internal_status("mongodb", operation, message)',
            "fn encode_mongodb_response",
            '"watch_changes"',
            '"aggregate_documents"',
            '"aggregate_response_encode"',
            '"list_indexes"',
            '"list_indexes_response_encode"',
            '"find_documents"',
            '"query_response_encode"',
            '"ensure_indexes"',
            '"run_command_create_collection"',
            '"ensure_resource_indexes"',
            '"drop_resource"',
            '"list_resources"',
            '"transaction"',
            "mongodb_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "mongodb"',
        ),
    ),
    TokenCheck(
        "PostgreSQL executor request JSON validation uses typed field violations",
        "src/runtime/executors/postgres.rs",
        (
            "invalid_argument_fields",
            "fn invalid_postgres_request_json_status(",
            '"invalid request json: {err}"',
            '"must be valid JSON for PostgreSQL generic dispatch"',
            "request_json_validation_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "request_json"',
        ),
    ),
    TokenCheck(
        "S3 executor request JSON validation uses typed field violations",
        "src/runtime/executors/s3.rs",
        (
            "invalid_argument_fields",
            "fn invalid_s3_request_json_status(",
            '"invalid request json: {err}"',
            '"must be valid JSON for S3 object dispatch"',
            "request_json_validation_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "request_json"',
        ),
    ),
    TokenCheck(
        "S3 executor internals use typed internal detail",
        "src/runtime/executors/s3.rs",
        (
            "fn s3_internal_status(",
            'internal_status("S3", operation, message)',
            '"create_bucket"',
            '"s3 create_bucket failed: {}: {}"',
            "internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "S3"',
        ),
    ),
    TokenCheck(
        "Redis executor validation uses typed field violations",
        "src/runtime/executors/redis.rs",
        (
            "invalid_argument_fields",
            "fn redis_invalid_field(",
            "fn invalid_redis_request_json_status(",
            "fn redis_required_field_status(",
            "fn unsupported_redis_operation_status(",
            '"invalid request json: {err}"',
            '"must be valid JSON for Redis generic dispatch"',
            '"key is required"',
            '"keys must be an array"',
            '"value is required"',
            '"ttl is required"',
            '"unsupported Redis {kind} operation \'{operation}\'"',
            "redis_request_json_validation_carries_field_violation",
            "redis_required_field_validation_carries_field_violations",
            "redis_unsupported_operation_validation_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"request_json"',
            '"key"',
            '"keys"',
            '"value"',
            '"ttl"',
            '"operation"',
        ),
    ),
    TokenCheck(
        "Memcached executor validation uses typed field violations",
        "src/runtime/executors/memcached.rs",
        (
            "invalid_argument_fields",
            "fn memcached_invalid_field(",
            "fn invalid_memcached_dispatch_json_status(",
            "fn memcached_missing_field_status(",
            "fn unsupported_memcached_operation_status(",
            '"memcached key must be 1–250 bytes (got {})"',
            '"memcached key may not contain whitespace or control characters"',
            '"invalid dispatch JSON: {err}"',
            '"must be valid JSON for Memcached key-value dispatch"',
            '"missing `op`/`operation` in dispatch request"',
            '"missing `key` in dispatch request"',
            '"bad base64 value: {e}"',
            '"set op requires `value`"',
            '"memcached query expects op=\\"get\\", got \'{operation}\'"',
            '"memcached mutate op \'{operation}\' is not supported"',
            "parse_kv_boundary_validation_carries_field_violations",
            "operation_validation_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"request_json"',
            '"op"',
            '"key"',
            '"value"',
        ),
    ),
    TokenCheck(
        "Memcached executor internals use typed internal detail",
        "src/runtime/executors/memcached.rs",
        (
            "fn memcached_internal_status(",
            'internal_status("memcached", operation, message)',
            '"memcached blocking task join: {e}"',
            "internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "memcached"',
        ),
    ),
    TokenCheck(
        "native hard quota errors use typed non-retryable detail",
        "src/runtime/service/lock_service",
        (
            "crate::runtime::executor_utils::quota_refusal_status",
            '"lock"',
            '"tenant active-lock quota"',
            "tenant active-lock quota exhausted",
        ),
    ),
    TokenCheck(
        "search hard quota error uses typed non-retryable detail",
        "src/runtime/service/search_service",
        (
            "crate::runtime::executor_utils::quota_refusal_status",
            '"search"',
            '"tenant search-index quota"',
            "tenant search-index quota exhausted",
        ),
    ),
    TokenCheck(
        "embedding hard quota error uses typed non-retryable detail",
        "src/runtime/service/embedding_service",
        (
            "crate::runtime::executor_utils::quota_refusal_status",
            '"embedding"',
            '"tenant embedding-source quota"',
            "tenant embedding-source quota exhausted",
        ),
    ),
    TokenCheck(
        "embedding request validation uses typed field violations",
        "src/runtime/service/embedding_service",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn embedding_field_violation",
            "fn embedding_required_field(",
            "fn validate_register_source_required_fields(",
            "fn validate_report_embedding_required_fields(",
            '"source_name and source_message_type are required"',
            '"target_collection is required"',
            '"source_name is required"',
            '"source_name and row_pk are required"',
            '"vector is required"',
            # Retrieval rework: query_text is now genuinely consumed (hybrid
            # text query + rerank), so the old "the broker does not embed
            # queries" parenthetical was dropped; the vector remains required.
            '"query_vector is required"',
            '"must contain an already-computed query embedding"',
            '"must identify exactly one entity in the active catalog manifest"',
            "source entity '{message_type}' has no resolvable tenant column",
            '"must be a non-empty embedding source name"',
            '"must be a non-empty source message type"',
            '"must be a non-empty target vector collection"',
            '"must be a non-empty source row primary key"',
            '"must contain at least one embedding dimension"',
            '"must identify exactly one entity in the active catalog manifest"',
            '"must resolve to a tenant-scoped source entity"',
            "register_source_missing_required_fields_carries_field_violations",
            "report_embedding_missing_identity_carries_field_violations",
            "retrieve_missing_query_vector_carries_field_violation",
            "source_message_type_missing_from_catalog_carries_field_violation",
            "register_source_fails_closed_without_source_tenant_column",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "source_name"',
            'detail.field_violations[1].field, "source_message_type"',
            'detail.field_violations[1].field, "row_pk"',
            'detail.field_violations[0].field, "query_vector"',
            "let blank = require_source_tenant_column",
            'assert_single_field_violation(',
        ),
    ),
    TokenCheck(
        "cache hard quota error uses typed non-retryable detail",
        "src/runtime/service/cache_service",
        (
            "crate::runtime::executor_utils::quota_refusal_status",
            '"cache"',
            '"namespace byte budget"',
            "byte budget exhausted",
        ),
    ),
    TokenCheck(
        "storage hard quota error uses typed non-retryable detail",
        "src/runtime/service/storage_service",
        (
            "crate::runtime::executor_utils::quota_refusal_status",
            '"storage"',
            '"tenant storage quota"',
            "STORAGE_QUOTA_EXCEEDED",
        ),
    ),
    TokenCheck(
        "storage quota lease contention uses typed retryable detail",
        "src/runtime/service/storage_service",
        (
            "crate::runtime::executor_utils::retryable_status",
            "crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS",
            '"storage"',
            '"quota_lock"',
            "storage quota lock contended; retry shortly",
        ),
    ),
    TokenCheck(
        "storage upload lifecycle denial uses typed policy detail with reason",
        "src/runtime/service/storage_service",
        (
            "fn storage_policy_status_with_reason(",
            "crate::runtime::executor_utils::policy_status",
            "fn upload_already_finalized_status() -> Status",
            "fn uploaded_object_missing_status() -> Status",
            '"finalize_upload"',
            '"upload_already_finalized"',
            '"uploaded_object_present"',
            "ALREADY_FINALIZED",
            "OBJECT_NOT_PRESENT",
            "return Err(upload_already_finalized_status());",
            "return Err(uploaded_object_missing_status());",
            "upload_already_finalized_carries_policy_detail_and_reason",
            "upload_presence_denial_carries_policy_detail_and_reason",
            "ErrorKind::Policy",
            "assert_policy_detail_with_reason(",
        ),
    ),
    TokenCheck(
        "storage upload head mismatches use typed validation detail with reason",
        "src/runtime/service/storage_service",
        (
            "crate::runtime::executor_utils::failed_precondition_fields",
            "fn upload_etag_mismatch_status() -> Status",
            "fn upload_size_mismatch_status(head_size: i64, declared_size: i64) -> Status",
            '"uploaded object etag does not match"',
            '"uploaded object size {head_size} does not match declared {declared_size}"',
            '"etag"',
            '"size_bytes"',
            "UPLOAD_SIZE_MISMATCH",
            "return Err(upload_etag_mismatch_status());",
            "return Err(upload_size_mismatch_status(*head_size, req.size_bytes));",
            "upload_head_mismatches_carry_validation_detail_and_reason",
            "assert_validation_detail_with_reason(",
            "ErrorKind::Validation",
        ),
    ),
    TokenCheck(
        "storage object stream missing store uses typed capability detail with reason",
        "src/runtime/service/storage_service",
        (
            "fn storage_capability_status_with_reason(",
            "fn object_stream_requires_store_status() -> Status",
            '"object_stream"',
            '"object_store"',
            "UNSUPPORTED_OBJECT_BACKEND",
            "object byte streaming requires a configured object store",
            "return Err(object_stream_requires_store_status());",
            "object_stream_requires_store_carries_capability_detail_and_reason",
            "ErrorKind::Capability",
            "assert_capability_detail_with_reason(",
        ),
    ),
    TokenCheck(
        "storage file-not-found denials use typed schema detail",
        "src/runtime/service/storage_service",
        (
            "fn storage_file_not_found_status(operation: &'static str) -> Status",
            "crate::runtime::executor_utils::schema_status",
            "tonic::Code::NotFound",
            '"file_not_found"',
            'return Err(storage_file_not_found_status("finalize_upload")),',
            'return Err(storage_file_not_found_status("get_download_url"));',
            'return Err(storage_file_not_found_status("download_file"));',
            'return Err(storage_file_not_found_status("get_file"));',
            'return Err(storage_file_not_found_status("update_file")),',
            'return Err(storage_file_not_found_status("delete_file")),',
            "storage_file_not_found_statuses_carry_schema_detail",
            "assert_schema_detail(",
            "ErrorKind::Schema",
            'detail.backend, "storage"',
        ),
    ),
    TokenCheck(
        "storage download missing object bytes uses typed policy detail with reason",
        "src/runtime/service/storage_service",
        (
            "fn file_object_bytes_missing_status() -> Status",
            "fn object_store_bytes_missing_status() -> Status",
            '"download_file"',
            '"file_object_bytes_present"',
            '"object_store_bytes_present"',
            "OBJECT_NOT_PRESENT",
            "return Err(file_object_bytes_missing_status());",
            "return Err(object_store_bytes_missing_status());",
            "download_object_absence_carries_policy_detail_and_reason",
            "ErrorKind::Policy",
            "assert_policy_detail_with_reason(",
        ),
    ),
    TokenCheck(
        "metering quota aggregate unavailable uses typed retryable detail",
        "src/runtime/service/metering_service",
        (
            "crate::runtime::executor_utils::retryable_status",
            "crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS",
            '"metering"',
            '"quota_aggregate"',
            "quota usage aggregate unavailable; failing closed",
        ),
    ),
    TokenCheck(
        "served validation error uses typed field violations",
        "src/runtime/service/auth_service/authz/governance_drafts.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn governance_required_field(",
            '"tenant_id is required"',
            '"draft_id is required"',
            '"a reason is required for an approval decision"',
            '"must be a non-empty tenant id"',
            '"must be a non-empty policy draft id"',
            '"must be a non-empty approval decision reason"',
            "update_policy_draft_missing_draft_id_carries_field_violation",
            "diff_policy_draft_missing_draft_id_carries_field_violation",
            "submit_policy_draft_missing_draft_id_carries_field_violation",
            "approve_policy_draft_missing_reason_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
        ),
    ),
    TokenCheck(
        "native helper validation uses typed field violations",
        "src/runtime/service/native_helpers.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            '"tenant_id is required"',
            '"must be a non-empty tenant id"',
            '"{field} must be a valid UUID"',
            '"must be a valid UUID"',
            "request_scope_missing_tenant_carries_field_violation",
            "parse_uuid_invalid_value_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "tenant_id"',
            'detail.field_violations[0].field, "user_id"',
        ),
    ),
    TokenCheck(
        "core tenant purge validation uses typed field violations",
        "src/runtime/core/tenant_purge.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn validate_purge_tenant_id(",
            '"tenant_id is required"',
            '"must be a non-empty tenant id"',
            "purge_tenant_missing_tenant_id_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "tenant_id"',
        ),
    ),
    TokenCheck(
        "notification template validation uses typed field violations",
        "src/runtime/service/notification_service",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            '"variables.{field}"',
            '"template variable is required but was not provided"',
            '"locale must be 10 characters or fewer"',
            '"must be 10 characters or fewer"',
            "VARIABLE_MISSING",
            "variable_missing_status_carries_reason_and_field_violation",
            "template_locale_too_long_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "variables.ResourceName"',
            'detail.field_violations[0].field, "locale"',
        ),
    ),
    TokenCheck(
        "notification request validation uses typed field violations",
        "src/runtime/service/notification_service",
        (
            "fn notification_required_field(",
            '"event_type is required"',
            '"log_id is required"',
            '"a terminal delivery status (SENT|DELIVERED|FAILED|PENDING) is required"',
            '"tenant_id is required"',
            '"must be a non-empty notification event type"',
            '"must be a non-empty notification log id"',
            '"must be one of SENT, DELIVERED, FAILED, or PENDING"',
            '"must be a non-empty tenant id"',
            "send_notification_missing_event_type_carries_field_violation",
            "report_delivery_missing_log_id_carries_field_violation",
            "report_delivery_unspecified_status_carries_field_violation",
            "upsert_template_missing_event_type_carries_field_violation",
            "set_preference_missing_tenant_status_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "event_type"',
            'detail.field_violations[0].field, "log_id"',
            'detail.field_violations[0].field, "status"',
            'detail.field_violations[0].field, "tenant_id"',
        ),
    ),
    TokenCheck(
        "cache request validation uses typed field violations",
        "src/runtime/service/cache_service",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            '"namespace is required"',
            '"must be a non-empty namespace"',
            '"namespace must not contain \':\' or whitespace"',
            '"must not contain \':\' or whitespace"',
            "DeleteNamespace flushes the whole namespace; confirmation_token is required",
            '"must be present to flush a cache namespace"',
            'format!("{name} is required")',
            '"must be a non-empty string"',
            "cache_validation_statuses_carry_field_violations",
            "delete_namespace_missing_confirmation_token_carries_field_violation",
            'detail.field_violations[0].field, "namespace"',
            'detail.field_violations[0].field, "confirmation_token"',
            'detail.field_violations[0].field, "key"',
            "ErrorKind::Validation",
        ),
    ),
    TokenCheck(
        "storage register upload validation uses typed field violations",
        "src/runtime/service/storage_service",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn storage_field_violation",
            "fn file_type_to_db(",
            "fn file_status_to_db(",
            "fn validate_register_upload_required_fields(",
            '"tenant_id and filename are required"',
            '"unknown file type: {other}"',
            '"unknown file status: {other}"',
            '"must be a non-empty filename"',
            '"must be a supported FileType enum value"',
            '"must be a supported FileStatus enum value"',
            "register_upload_missing_filename_carries_field_violation",
            "file_type_and_status_validation_carry_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "filename"',
            "assert_single_field_violation(",
            '"file_type"',
            '"status"',
        ),
    ),
    TokenCheck(
        "config request validation uses typed field violations",
        "src/runtime/service/config_service",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn require_flag_key(",
            '"flag_key is required"',
            '"must be a non-empty flag key"',
            "fn ensure_evaluate_key_limit(",
            '"too many keys (max {MAX_EVALUATE_KEYS})"',
            '"must contain at most 256 keys"',
            '"value is required"',
            '"must set one FlagValue arm"',
            '"json_value is not valid JSON: {e}"',
            '"value.json_value"',
            '"must be valid JSON"',
            "put_flag_missing_value_carries_field_violation",
            "get_flag_missing_key_carries_field_violation",
            "evaluate_flags_key_limit_carries_field_violation",
            "json_flag_value_validation_carries_field_violation",
            "ErrorKind::Validation",
        ),
    ),
    TokenCheck(
        "backup request validation uses typed field violations",
        "src/runtime/service/backup_service",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn required_backup_field(",
            '"tenant_id is required"',
            '"must be a non-empty tenant id"',
            '"source_tenant_id, target_tenant_id and backup_id are required"',
            '"must be a non-empty source tenant id"',
            '"must be a non-empty target tenant id"',
            "RestoreTenant overwrites a tenant's data; confirmation_token is required",
            '"must be present to restore over tenant data"',
            '"backup_id is required"',
            '"must be a non-empty backup id"',
            '"policy_name is required"',
            '"must be a non-empty policy name"',
            "start_backup_missing_tenant_id_carries_field_violation",
            "restore_tenant_missing_identity_carries_field_violations",
            "restore_requires_confirmation_token",
            "get_backup_missing_backup_id_carries_field_violation",
            "get_backup_policy_missing_policy_name_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "tenant_id"',
            'detail.field_violations[0].field, "source_tenant_id"',
            'detail.field_violations[1].field, "target_tenant_id"',
            'detail.field_violations[0].field, "confirmation_token"',
            'detail.field_violations[0].field, "backup_id"',
            'detail.field_violations[0].field, "policy_name"',
        ),
    ),
    TokenCheck(
        "backup restore state denials use typed policy detail",
        "src/runtime/service/backup_service",
        (
            "fn backup_policy_status(",
            "crate::runtime::executor_utils::policy_status",
            "fn restore_target_not_fresh_status(existing_rows: u64) -> Status",
            "fn backup_run_missing_object_prefix_status() -> Status",
            '"restore_tenant"',
            '"restore_target_not_fresh"',
            '"backup_run_missing_object_prefix"',
            "return Err(restore_target_not_fresh_status_in(existing_rows, occupied));",
            "return Err(backup_run_missing_object_prefix_status());",
            "restore_over_existing_tenant_is_rejected",
            "backup_run_missing_object_prefix_carries_policy_detail",
            "ErrorKind::Policy",
            "assert_policy_detail(",
        ),
    ),
    TokenCheck(
        "BackupService not-found denials use typed schema detail",
        "src/runtime/service/backup_service",
        (
            "fn backup_not_found_status(",
            "crate::runtime::executor_utils::schema_status",
            "tonic::Code::NotFound",
            '"backup_run_not_found"',
            '"backup_policy_not_found"',
            'backup_not_found_status(',
            '"restore_tenant"',
            '"get_backup"',
            '"get_backup_policy"',
            "backup_not_found_statuses_carry_schema_detail",
            "assert_schema_not_found_detail(",
            "ErrorKind::Schema",
            'detail.backend, "backup"',
        ),
    ),
    TokenCheck(
        "BackupService internal failures use typed internal detail",
        "src/runtime/service/backup_service",
        (
            "fn backup_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status",
            'crate::runtime::executor_utils::internal_status("backup", operation, message)',
            '"start_backup_read_table"',
            '"start_backup_encrypt_artifact"',
            '"start_backup_serialize_manifest"',
            '"restore_freshness_probe"',
            '"restore_manifest_parse"',
            '"restore_transaction_begin"',
            '"restore_artifact_utf8"',
            '"restore_decrypt_artifact"',
            '"restore_row_parse"',
            '"restore_row_reserialize"',
            '"restore_insert_row"',
            '"restore_transaction_commit"',
            "backup_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "backup"',
        ),
    ),
    TokenCheck(
        "tenant movement scope denials use typed policy detail",
        "src/runtime/tenant_movement.rs",
        (
            "fn policy_operation(self) -> &'static str",
            "pub fn tenant_movement_policy_status(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "tonic::Code::PermissionDenied",
            '"tenant_movement_backup_export"',
            '"tenant_movement_restore_import"',
            '"tenant_movement_replication_publication"',
            '"tenant_movement_purge"',
            '"tenant_movement_scope_required"',
            "tenant_movement_scope_status_carries_policy_detail",
            "ErrorKind::Policy",
        ),
    ),
    TokenCheck(
        "backup/tenant services route movement denials through typed policy detail",
        "src/runtime/service/backup_service",
        (
            "tenant_movement_policy_status",
            "validate_tenant_movement_scope(&movement)",
            "TenantMovementOperation::BackupExport",
            "TenantMovementOperation::RestoreImport",
            "restore_cross_tenant_movement_carries_policy_detail",
            '"tenant_movement_restore_import"',
            '"tenant_movement_scope_required"',
        ),
    ),
    TokenCheck(
        "tenant purge routes movement denials through typed policy detail",
        "src/runtime/service/tenant_service",
        (
            "tenant_movement_policy_status",
            "validate_tenant_movement_scope(&movement)",
            "TenantMovementOperation::TenantPurge",
            "tenant_movement_policy_status(movement.operation, err)",
        ),
    ),
    TokenCheck(
        "lock request validation uses typed field violations",
        "src/runtime/service/lock_service",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn validate_lock_identity(",
            '"lock_name and owner_id are required"',
            '"must be a non-empty lock name"',
            '"must be a non-empty owner id"',
            "acquire_lock_missing_identity_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "lock_name"',
            'detail.field_violations[1].field, "owner_id"',
        ),
    ),
    TokenCheck(
        "lock state denials use typed policy detail",
        "src/runtime/service/lock_service",
        (
            "fn lock_policy_status(",
            "crate::runtime::executor_utils::policy_status",
            "fn lock_policy_status_with_code(",
            "crate::runtime::executor_utils::policy_status_with_code",
            "fn stale_fencing_token_status(provided: i64, stored: i64) -> Status",
            "fn lock_lease_lost_status() -> Status",
            "fn lock_held_by_different_owner_status(operation: &'static str) -> Status",
            '"lock_fencing"',
            '"stale_fencing_token"',
            '"renew_lock"',
            '"lock_lease_lost"',
            '"release_lock"',
            '"lock_owner_mismatch"',
            "return Err(stale_fencing_token_status(provided, stored));",
            "return Err(lock_lease_lost_status());",
            'return Err(lock_held_by_different_owner_status("renew_lock"));',
            'return Err(lock_held_by_different_owner_status("release_lock"));',
            "stale_fencing_token_is_rejected",
            "lock_lease_lost_carries_policy_detail",
            "lock_owner_mismatch_carries_permission_denied_policy_detail",
            "ErrorKind::Policy",
            "assert_policy_detail(",
        ),
    ),
    TokenCheck(
        "scheduler create-job validation uses typed field violations",
        "src/runtime/service/scheduler_service",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn scheduler_required_field(",
            '"name is required"',
            '"must be a non-empty job name"',
            '"cron_expression is required for CRON jobs"',
            '"must be a non-empty cron expression for CRON jobs"',
            '"cron_expression is not a valid 5-field cron or @macro"',
            '"must be a valid 5-field cron expression or @macro"',
            '"next_fire_at (RFC3339) is required for ONE_SHOT jobs"',
            '"must be a non-empty RFC3339 timestamp for ONE_SHOT jobs"',
            '"unknown schedule_type: {other} (expected CRON or ONE_SHOT)"',
            '"unknown job status filter: {other}"',
            '"must be CRON or ONE_SHOT"',
            '"must be a known job status"',
            "create_job_missing_name_carries_field_violation",
            "create_one_shot_job_missing_next_fire_at_carries_field_violation",
            "schedule_type_unknown_value_carries_field_violation",
            "job_status_filter_unknown_value_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "name"',
            'detail.field_violations[0].field, "next_fire_at"',
            'detail.field_violations[0].field, "schedule_type"',
            'detail.field_violations[0].field, "status_filter"',
        ),
    ),
    TokenCheck(
        "analytics request validation uses typed field violations",
        "src/runtime/service/analytics_service",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn analytics_required_field(",
            '"stage_name is required"',
            '"must be a non-empty pipeline stage name"',
            "record_pipeline_metric_missing_stage_name_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "stage_name"',
        ),
    ),
    TokenCheck(
        "asset request validation uses typed field violations",
        "src/runtime/service/asset_service",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn asset_invalid_field(",
            "fn asset_required_field(",
            "fn active_storage_file_required_status(",
            '"name is required"',
            '"file_id is required"',
            '"steps must be valid JSON: {e}"',
            '"native JSON field is invalid: {err}"',
            '"unknown asset status: {other}"',
            '"unknown step status: {other}"',
            '"unknown step type: {other}"',
            '"file_id does not reference an active storage file owned by this tenant"',
            '"must be a non-empty pipeline definition name"',
            '"must be a non-empty storage file id"',
            '"must be valid JSON"',
            '"must be valid native JSON"',
            '"must be a supported AssetStatus enum value"',
            '"must be a supported StepStatus enum value"',
            '"must be a supported StepType enum value"',
            '"must reference an active storage file owned by this tenant"',
            "create_pipeline_definition_missing_name_carries_field_violation",
            "create_pipeline_definition_invalid_steps_carries_field_violation",
            "register_asset_missing_file_id_carries_field_violation",
            "asset_helper_validation_carries_field_violations",
            "register_asset_inactive_file_status_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "name"',
            'detail.field_violations[0].field, "steps"',
            'detail.field_violations[0].field, "file_id"',
            "assert_single_field_violation(",
        ),
    ),
    TokenCheck(
        "metering request validation uses typed field violations",
        "src/runtime/service/metering_service",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn metering_required_field(",
            "fn metering_nonnegative_field(",
            '"method is required"',
            '"must be a non-empty usage method"',
            '"metric is required"',
            '"must be a non-empty metric name"',
            '"limit_value must be >= 0"',
            '"window_seconds must be >= 0"',
            '"must be greater than or equal to 0"',
            "record_usage_missing_method_carries_field_violation",
            "query_usage_missing_metric_carries_field_violation",
            "put_quota_negative_limit_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "method"',
            'detail.field_violations[0].field, "metric"',
            'detail.field_violations[0].field, "limit_value"',
        ),
    ),
    TokenCheck(
        "tenant request validation uses typed field violations",
        "src/runtime/service/tenant_service",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn tenant_required_field(",
            "fn tenant_field_violation(",
            "fn validate_create_tenant_required_fields(",
            '"code and name are required"',
            '"must be a non-empty tenant code"',
            '"must be a non-empty tenant name"',
            '"tenant_id is required"',
            '"must be a non-empty tenant id"',
            "PurgeTenant is an irreversible hard delete; confirmation_token is required",
            '"must be present to purge tenant data"',
            '"config_key is required"',
            '"must be a non-empty config key"',
            '"unknown tenant type: {other}"',
            '"unsupported tenant type {other}"',
            '"unknown tenant status: {other}"',
            '"unsupported tenant status {other}"',
            '"unknown config type: {other}"',
            '"unsupported config type {other}"',
            "create_tenant_missing_code_and_name_carries_field_violations",
            "purge_tenant_missing_tenant_id_carries_field_violation",
            "purge_tenant_missing_confirmation_token_carries_field_violation",
            "update_tenant_config_missing_key_carries_field_violation",
            "tenant_enum_normalizers_carry_field_violations",
            "assert_single_field_violation(",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "code"',
            'detail.field_violations[1].field, "name"',
            'detail.field_violations[0].field, "tenant_id"',
            'detail.field_violations[0].field, "confirmation_token"',
            'detail.field_violations[0].field, "config_key"',
            'assert_single_field_violation(&tenant_type, "type"',
            'assert_single_field_violation(',
            'assert_single_field_violation(&config_type, "type"',
        ),
    ),
    TokenCheck(
        "webhook request validation uses typed field violations",
        "src/runtime/service/webhook_service",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn webhook_required_field(",
            "fn webhook_url_violation(",
            '"url is required"',
            '"webhook url must use https (cleartext http and non-http schemes are rejected)"',
            '"webhook url has a malformed IPv6 host"',
            '"webhook url must include a valid host"',
            '"webhook url host {host} resolves to a private/loopback/link-local address (SSRF blocked)"',
            '"webhook url host localhost is not an allowed external target (SSRF blocked)"',
            '"must be a non-empty HTTPS webhook URL"',
            '"must use https scheme"',
            '"must contain a well-formed bracketed IPv6 host"',
            '"must include a valid external host"',
            '"must not target private, loopback, link-local, CGNAT, unspecified, multicast, or reserved IP ranges"',
            '"must not target localhost hostnames"',
            "create_endpoint_missing_url_carries_field_violation",
            "webhook_url_validation_carries_field_violations",
            "assert_url_field_violation(",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "url"',
        ),
    ),
    TokenCheck(
        "webhook delivery-time SSRF denials use typed policy detail",
        "src/runtime/service/webhook_service",
        (
            "fn webhook_policy_status(",
            "crate::runtime::executor_utils::policy_status",
            "fn webhook_host_unresolved_status(",
            "fn webhook_host_blocked_address_status(",
            "fn webhook_host_no_addresses_status(",
            '"webhook_delivery_ssrf"',
            '"webhook_host_unresolved"',
            '"webhook_host_blocked_address"',
            '"webhook_host_no_addresses"',
            "webhook_host_unresolved_status(&host, err)",
            "return Err(webhook_host_blocked_address_status(&host, addr.ip()));",
            "return Err(webhook_host_no_addresses_status(&host));",
            "delivery_time_ssrf_denials_carry_policy_detail",
            "ErrorKind::Policy",
            "assert_policy_detail(",
        ),
    ),
    TokenCheck(
        "webrtc room request validation uses typed field violations",
        "src/runtime/service/webrtc_service/mod.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            '"tenant_id and name are required"',
            '"must be a non-empty room name"',
            '"room_id is required on the first signaling message"',
            '"peer_id is required on the first signaling message"',
            '"tenant_id is required on the first signaling message"',
            '"must be a non-empty signaling room id"',
            '"must be a non-empty signaling peer id"',
            '"must be a non-empty signaling tenant id"',
            '"unknown room state: {other}"',
            '"unknown peer state: {other}"',
            '"unknown track kind: {other}"',
            '"webrtc JSON field is invalid: {err}"',
            '"empty signaling stream"',
            '"must be ACTIVE, IDLE, or CLOSED"',
            '"must be a known peer state"',
            '"must be AUDIO, VIDEO, SCREEN, or DATA"',
            '"must be valid JSON"',
            '"must include an initial signaling message"',
            "create_room_missing_name_carries_field_violation",
            "signaling_first_message_missing_room_id_carries_field_violation",
            "signaling_first_message_missing_peer_id_carries_field_violation",
            "signaling_first_message_missing_tenant_id_carries_field_violation",
            "webrtc_enum_helpers_carry_field_violations",
            "webrtc_json_helper_carries_field_violation",
            "empty_signaling_stream_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "name"',
            'detail.field_violations[0].field, "room_id"',
            'detail.field_violations[0].field, "peer_id"',
            'detail.field_violations[0].field, "tenant_id"',
            'assert_validation_field(&track, "kind", "must be AUDIO, VIDEO, SCREEN, or DATA")',
            'assert_validation_field(&err, "json", "must be valid JSON")',
            'assert_validation_field(',
        ),
    ),
    TokenCheck(
        "search request validation uses typed field violations",
        "src/runtime/service/search_service",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn search_field_violation",
            "fn search_required_field(",
            "fn validate_create_index_required_fields(",
            "fn validate_search_query(",
            '"index_name and source_message_type are required"',
            '"index_name is required"',
            '"unsupported search backend \'{backend}\' (expected \'{BACKEND_QDRANT}\' or',
            '"unsupported search backend \'memory\' (expected \'qdrant\' or \'elasticsearch\')"',
            '"must identify exactly one entity in the active catalog manifest"',
            "source entity '{message_type}' has no resolvable tenant column",
            '"search requires query_text and/or query_vector"',
            '"must be a non-empty search index name"',
            '"must be a non-empty source message type"',
            '"must be \'{BACKEND_QDRANT}\' or \'{BACKEND_ELASTICSEARCH}\'"',
            '"must identify exactly one entity in the active catalog manifest"',
            '"must resolve to a tenant-scoped source entity"',
            '"must be non-empty when query_vector is empty"',
            '"must be non-empty when query_text is empty"',
            "create_index_missing_required_fields_carries_field_violations",
            "create_index_unsupported_backend_carries_field_violation",
            "delete_index_missing_index_name_carries_field_violation",
            "source_message_type_missing_from_catalog_carries_field_violation",
            "create_index_fails_closed_without_source_tenant_column",
            "empty_search_query_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "index_name"',
            'detail.field_violations[1].field, "source_message_type"',
            "let blank = require_source_tenant_column",
            'assert_single_field_violation(&err, "backend"',
            'assert_single_field_violation(',
        ),
    ),
    TokenCheck(
        "workflow request validation uses typed field violations",
        "src/runtime/service/workflow_service",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn workflow_required_field(",
            "fn workflow_size_field(",
            '"workflow_type is required"',
            '"payload exceeds {MAX_PAYLOAD_BYTES} bytes"',
            '"compensations exceed {MAX_COMPENSATIONS_BYTES} bytes"',
            '"signal_name is required"',
            '"unknown workflow status filter: {other}"',
            '"must be a non-empty workflow type"',
            '"must be no larger than {limit} bytes"',
            '"must be a non-empty workflow signal name"',
            '"must be a known workflow status"',
            "start_workflow_missing_type_carries_field_violation",
            "start_workflow_oversized_payload_carries_field_violation",
            "signal_workflow_missing_signal_name_carries_field_violation",
            "workflow_status_filter_unknown_value_carries_field_violation",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, "workflow_type"',
            'detail.field_violations[0].field, "payload"',
            'detail.field_violations[0].field, "signal_name"',
            'detail.field_violations[0].field, "status_filter"',
        ),
    ),
    TokenCheck(
        "workflow terminal state denials use typed policy detail",
        "src/runtime/service/workflow_service",
        (
            "fn workflow_policy_status(",
            "crate::runtime::executor_utils::policy_status",
            "fn workflow_cancel_terminal_status() -> Status",
            "fn workflow_signal_terminal_status() -> Status",
            '"cancel_workflow"',
            '"signal_workflow"',
            '"workflow_terminal_state"',
            "workflow is in a terminal state and cannot be cancelled",
            "workflow is in a terminal state and cannot be signalled",
            "_ => Err(workflow_cancel_terminal_status()),",
            "return Err(workflow_signal_terminal_status());",
            "workflow_terminal_cancel_and_signal_denials_carry_policy_detail",
            "ErrorKind::Policy",
            "assert_policy_detail(",
        ),
    ),
    TokenCheck(
        "workflow not-found denials use typed schema detail",
        "src/runtime/service/workflow_service",
        (
            "fn workflow_not_found_status(operation: &'static str) -> Status",
            "crate::runtime::executor_utils::schema_status",
            "tonic::Code::NotFound",
            '"workflow_not_found"',
            'workflow_not_found_status("get_workflow")',
            'workflow_not_found_status("cancel_workflow")',
            'workflow_not_found_status("signal_workflow")',
            "workflow_not_found_statuses_carry_schema_detail",
            "assert_schema_not_found_detail(",
            "ErrorKind::Schema",
            'detail.backend, "workflow"',
        ),
    ),
    TokenCheck(
        "WorkflowService internal failures use typed internal detail",
        "src/runtime/service/workflow_service",
        (
            "fn workflow_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status",
            'crate::runtime::executor_utils::internal_status("workflow", operation, message)',
            '"decode_workflow_instance"',
            '"start_workflow"',
            '"get_workflow"',
            '"list_workflows_count"',
            '"cancel_workflow_load"',
            '"signal_workflow_load"',
            '"workflow_tick_claim"',
            '"workflow_tick_outbox_insert"',
            "workflow_internal_status_carries_typed_detail",
            "fn assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "workflow"',
        ),
    ),
    TokenCheck(
        "saga admin validation uses typed field violations",
        "src/runtime/saga.rs",
        (
            "crate::runtime::executor_utils::invalid_argument_fields",
            "fn saga_field_violation(",
            "fn parse_saga_uuid_field(",
            "fn validate_tx_id_filter(",
            "fn parse_saga_status_filter(",
            '"invalid status_filter \'{status_filter}\'; must be one of {allowed:?}"',
            '"must be one of {}"',
            '"tx_id_filter must be a valid UUID"',
            '"saga_id must be a UUID"',
            '"must be a valid UUID"',
            "saga_admin_status_filter_carries_field_violation",
            "saga_admin_uuid_validation_carries_field_violations",
            "decode(raw.as_ref()).expect(\"typed detail decodes\")",
            "ErrorKind::Validation",
            'detail.field_violations[0].field, field',
            '"status_filter"',
            '"tx_id_filter"',
            '"saga_id"',
        ),
    ),
    TokenCheck(
        "saga recompensation lifecycle refusal uses typed policy detail",
        "src/runtime/saga.rs",
        (
            "fn saga_recompensation_not_retryable_status(",
            "crate::runtime::executor_utils::policy_status",
            '"retry_saga_compensation"',
            '"saga_not_retryable"',
            "is not in a retryable state (failed_compensation or manual_review)",
            "saga_recompensation_not_retryable_carries_policy_detail",
            "assert_policy_detail(",
            "ErrorKind::Policy",
            "detail.policy_decision_id",
        ),
    ),
    TokenCheck(
        "saga admin not-found paths use typed schema detail",
        "src/runtime/saga.rs",
        (
            "fn saga_not_found_status(",
            "crate::runtime::executor_utils::schema_status",
            "tonic::Code::NotFound",
            '"saga"',
            '"saga_not_found"',
            'saga_not_found_status("get_saga", saga_id)',
            'saga_not_found_status("mark_saga_reviewed", saga_id)',
            "saga_not_found_statuses_carry_schema_detail",
            "assert_schema_detail(",
            "ErrorKind::Schema",
            'detail.backend, "saga"',
        ),
    ),
    TokenCheck(
        "saga admin internals use typed internal detail",
        "src/runtime/saga.rs",
        (
            "fn saga_internal_status(",
            'crate::runtime::executor_utils::internal_status("saga", operation, message)',
            '"list_sagas"',
            '"get_saga"',
            '"mark_saga_reviewed"',
            '"retry_saga_compensation"',
            '"list_sagas query failed: {err}"',
            '"get_saga failed: {err}"',
            '"mark_saga_reviewed failed: {msg}"',
            '"retry_saga_compensation failed: {msg}"',
            "saga_internal_status_carries_typed_detail",
            "assert_internal_detail(",
            "ErrorKind::Internal",
            'detail.backend, "saga"',
        ),
    ),
    TokenCheck(
        "served capability error uses typed detail",
        "src/runtime/executors/mssql.rs",
        (
            "ensure_resource_returns_typed_capability_error_not_unimplemented",
            "crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY",
            "ErrorDetail::decode(raw.as_ref())",
            "ErrorKind::Capability",
            'detail.capability_required, "native_resource_lifecycle"',
        ),
    ),
    TokenCheck(
        "PublishCDC disabled tailer uses typed capability detail",
        "src/runtime/service/handlers_tx.rs",
        (
            "crate::runtime::executor_utils::capability_status",
            '"cdc"',
            '"PublishCDC"',
            '"cdc_tailer"',
            "CDC tailer is not configured; set UDB_KAFKA_BROKERS to enable PublishCDC",
        ),
    ),
    TokenCheck(
        "transaction/object missing backend uses typed capability detail",
        "src/runtime/core/tx_object.rs",
        (
            "crate::runtime::executor_utils::capability_status",
            '"postgres"',
            '"begin_tx"',
            '"postgres_backend"',
            "PostgreSQL backend is not configured",
            '"qdrant"',
            '"qdrant_backend"',
            "Qdrant backend is not configured",
            '"s3"',
            '"s3_backend"',
            "S3/MinIO backend is not configured",
        ),
    ),
    TokenCheck(
        "Postgres read-routing missing backend uses typed capability detail",
        "src/runtime/core/accessors.rs",
        (
            "fn postgres_backend_not_configured_status(operation: &'static str) -> tonic::Status",
            "crate::runtime::executor_utils::capability_status",
            '"postgres"',
            '"postgres_backend"',
            "PostgreSQL backend is not configured",
            '"read_fence_primary"',
            '"primary_read"',
            '"routed_read"',
            '"replica_or_primary_read"',
            '"routed_primary_pool"',
        ),
    ),
    TokenCheck(
        "core backend resolver missing setup uses typed capability detail",
        "src/runtime/core/accessors.rs",
        (
            "fn backend_not_configured_status(",
            "capability_required: &'static str",
            '"pool_lookup"',
            "PostgreSQL is not configured",
            '"redis"',
            '"instance_resolver"',
            '"redis_backend"',
            "redis not configured",
            '"qdrant"',
            '"qdrant_backend"',
            "Qdrant backend is not configured",
            '"s3"',
            '"s3_backend"',
            "s3/minio not configured",
            '"mongodb"',
            '"mongodb_backend"',
            "mongodb not configured",
            '"neo4j"',
            '"neo4j_backend"',
            "neo4j not configured",
            '"clickhouse"',
            '"clickhouse_backend"',
            "clickhouse not configured",
            "backend_resolver_missing_setup_carries_capability_detail",
            "s3_resolver_missing_backend_carries_capability_detail",
        ),
    ),
    TokenCheck(
        "core backend resolver connectivity uses typed capability detail",
        "src/runtime/core/accessors.rs",
        (
            "fn backend_instance_not_connected_status(",
            '"backend_instance_connected"',
            "fn backend_instance_disabled_status(",
            '"backend_instance_enabled"',
            "fn backend_executor_not_registered_status(",
            '"backend_executor_registered"',
            "fn backend_executor_not_connected_status(",
            '"backend_executor_connected"',
            "backend_resolver_connectivity_denials_carry_capability_detail",
            "postgres instance 'replica-a' is not connected",
            "backend instance 'postgres:replica-a' is disabled",
            "backend executor 'qdrant:default' is not registered",
            "backend executor 'qdrant:default' is registered but not connected",
            "ErrorKind::Capability",
        ),
    ),
    TokenCheck(
        "public API rules document the real UDB-native detail contract",
        "docs/api-rules.md",
        (
            "canonical server error model is gRPC status code/message plus UDB-native",
            "`udb.entity.v1.ErrorDetail`",
            "`udb-error-detail-bin`",
            "| `ErrorDetail` field | Carries | Public field |",
            "| `retryable` + `retry_after_ms` | server-advised retryability/backoff |",
            "| `field_violations[]` | invalid request field paths and descriptions |",
            "Hard quota/capacity refusals should still attach",
            "`retryable=false` and `retry_after_ms=0`",
            "failures should attach `kind=VALIDATION` and `field_violations`",
            "field-fix errors must not advertise backoff metadata",
            "conflicts should preserve `ABORTED`",
            "`kind=RETRYABLE`",
            "SDKs should synthesize the same `ErrorDetail` shape",
            "`backend=transport`",
            "caller cancellation",
            "### Stable String Reason Registry",
            "`ApiError.code`, the `error-reason` gRPC metadata trailer",
            "| `STORAGE_QUOTA_EXCEEDED` | StorageService |",
            "| `PIPELINE_DEFINITION_INVALID` | AssetService |",
            "| `TEMPLATE_NOT_FOUND` | NotificationService |",
            "| `ROOM_FULL` | RoomService |",
            "Success stays the",
            "decoded typed message",
        ),
    ),
    TokenCheck(
        "OpenAPI postprocess advertises REST ApiError boundary",
        "scripts/openapi-postprocess.mjs",
        (
            "REST_ERROR_STATUS_RESPONSES",
            "404: ['NOT_FOUND']",
            "429: ['RESOURCE_EXHAUSTED']",
            "504: ['DEADLINE_EXCEEDED']",
            "function applyRestErrorBoundary(operation)",
            "Body preserves the canonical gRPC code/message and UDB ErrorDetail-derived fields.",
            "'x-udb-grpc-codes': grpcCodes",
            "$ref: API_ERROR_REF",
        ),
    ),
    TokenCheck(
        "OpenAPI guard rejects stale REST error boundary",
        "scripts/check-openapi-api-rules.mjs",
        (
            "grpcHttpStatusMap",
            "404: ['NOT_FOUND']",
            "requiredApiErrorFields",
            "function isForbiddenSuccessWrapper(schema)",
            "default REST error response must use #/definitions/v1ApiError",
            "missing REST error response 404 for gRPC NOT_FOUND",
            "success response must be a bare typed body",
            "stale rpcStatus default response was not caught",
            "missing NOT_FOUND->404 response was not caught",
            "success envelope response was not caught",
        ),
    ),
    TokenCheck(
        "Go SDK decodes generated ErrorDetail",
        "sdk/go/udbclient/errordetail.go",
        (
            "udb.entity.v1.ErrorDetail",
            "func (e *Error) Detail() (*entityv1.ErrorDetail, bool)",
            "proto.Unmarshal(e.DetailBin, &d)",
            "func (e *Error) Retryable() bool",
            "func (e *Error) Kind() entityv1.ErrorKind",
            "type FieldViolation struct",
            "func (e *Error) FieldViolations() []FieldViolation",
            'Fields().ByName("field_violations")',
        ),
    ),
    TokenCheck(
        "Go SDK template synthesizes transport ErrorDetail",
        "sdk-templates/go/udbclient/generated_client.go.tmpl",
        (
            "synthesizeTransportErrorDetailBin",
            "codes.Unavailable, codes.DeadlineExceeded, codes.Canceled",
            'Backend:      "transport"',
            "transportErrorOperation(code)",
            'return "deadline_exceeded"',
            'return "cancelled"',
            "Retryable:    code != codes.Canceled",
            "entityv1.ErrorKind_ERROR_KIND_RETRYABLE",
            "proto.Marshal(detail)",
        ),
    ),
    TokenCheck(
        "Go checked-in SDK synthesizes transport ErrorDetail",
        "sdk/go/udbclient/generated_client.go",
        (
            'const errorDetailTrailer = "udb-error-detail-bin"',
            "synthesizeTransportErrorDetailBin",
            "codes.Unavailable, codes.DeadlineExceeded, codes.Canceled",
            'Backend:      "transport"',
            "transportErrorOperation(code)",
            'return "deadline_exceeded"',
            'return "cancelled"',
            "Retryable:    code != codes.Canceled",
            "entityv1.ErrorKind_ERROR_KIND_RETRYABLE",
            "proto.Marshal(detail)",
        ),
    ),
    TokenCheck(
        "TypeScript SDK decodes ErrorDetail trailer",
        "sdk-templates/typescript/generatedClient.ts.tmpl",
        (
            'const ERROR_DETAIL_TRAILER = "udb-error-detail-bin";',
            "export interface UdbErrorDetail",
            "export interface UdbFieldViolation",
            "function decodeErrorDetailBytes(bytes: Buffer): UdbErrorDetail",
            "function decodeFieldViolationBytes(bytes: Buffer): UdbFieldViolation",
            "detail.retryable = value !== 0;",
            "detail.retry_after_ms = value;",
            "detail.kindName = ERROR_KIND_NAMES[value];",
            "detail.field_violations",
            "get fieldViolations(): UdbFieldViolation[]",
            "return this.detail?.field_violations ?? [];",
        ),
    ),
    TokenCheck(
        "TypeScript SDK synthesizes transport ErrorDetail",
        "sdk-templates/typescript/generatedClient.ts.tmpl",
        (
            "function synthesizeTransportErrorDetail(err: grpc.ServiceError): UdbErrorDetail | undefined",
            "grpc.status.UNAVAILABLE",
            "grpc.status.DEADLINE_EXCEEDED",
            "grpc.status.CANCELLED",
            'backend: "transport"',
            "retryable = err.code !== grpc.status.CANCELLED",
            "kind: 5",
        ),
    ),
    TokenCheck(
        "TypeScript checked-in SDK decodes and synthesizes ErrorDetail",
        "sdk/typescript/generatedClient.ts",
        (
            'const ERROR_DETAIL_TRAILER = "udb-error-detail-bin";',
            "export interface UdbErrorDetail",
            "function decodeErrorDetailBytes(bytes: Buffer): UdbErrorDetail",
            "function decodeFieldViolationBytes(bytes: Buffer): UdbFieldViolation",
            "detail.retryable = value !== 0;",
            "detail.retry_after_ms = value;",
            "detail.field_violations",
            "function synthesizeTransportErrorDetail(err: grpc.ServiceError): UdbErrorDetail | undefined",
            "grpc.status.UNAVAILABLE",
            "grpc.status.DEADLINE_EXCEEDED",
            "grpc.status.CANCELLED",
            'backend: "transport"',
            "retryable = err.code !== grpc.status.CANCELLED",
            "kind: 5",
        ),
    ),
    TokenCheck(
        "Python SDK decodes ErrorDetail trailer",
        "sdk-templates/python/udb_client/generated_client.py.tmpl",
        (
            '_ERROR_DETAIL_TRAILER = "udb-error-detail-bin"',
            "from udb.entity.v1.error_pb2 import ErrorDetail",
            "decoded.ParseFromString(detail)",
            "self.error_detail = _decode_error_detail(detail)",
            "def _field_violations_from_detail(detail: Any | None) -> list[dict[str, str]]:",
            'self.field_violations: list[dict[str, str]] = _field_violations_from_detail(ed)',
            "def is_retryable(self) -> bool:",
            "def kind(self) -> str:",
        ),
    ),
    TokenCheck(
        "Python SDK synthesizes transport ErrorDetail",
        "sdk-templates/python/udb_client/generated_client.py.tmpl",
        (
            "def _synthesize_transport_error_detail(error: grpc.RpcError) -> bytes | None:",
            "grpc.StatusCode.UNAVAILABLE",
            "grpc.StatusCode.DEADLINE_EXCEEDED",
            "grpc.StatusCode.CANCELLED",
            'detail.backend = "transport"',
            "detail.retryable = code != grpc.StatusCode.CANCELLED",
            "detail.kind = ErrorKind.ERROR_KIND_RETRYABLE",
            "_extract_error_detail(error) or _synthesize_transport_error_detail(error)",
        ),
    ),
    TokenCheck(
        "Python checked-in SDK decodes and synthesizes ErrorDetail",
        "sdk/python/udb_client/generated_client.py",
        (
            '_ERROR_DETAIL_TRAILER = "udb-error-detail-bin"',
            "from udb.entity.v1.error_pb2 import ErrorDetail",
            "decoded.ParseFromString(detail)",
            "self.error_detail = _decode_error_detail(detail)",
            "def _field_violations_from_detail(detail: Any | None) -> list[dict[str, str]]:",
            'self.field_violations: list[dict[str, str]] = _field_violations_from_detail(ed)',
            "def _synthesize_transport_error_detail(error: grpc.RpcError) -> bytes | None:",
            "grpc.StatusCode.UNAVAILABLE",
            "grpc.StatusCode.DEADLINE_EXCEEDED",
            "grpc.StatusCode.CANCELLED",
            'detail.backend = "transport"',
            "detail.retryable = code != grpc.StatusCode.CANCELLED",
            "detail.kind = ErrorKind.ERROR_KIND_RETRYABLE",
            "_extract_error_detail(error) or _synthesize_transport_error_detail(error)",
        ),
    ),
    TokenCheck(
        "PHP SDK decodes ErrorDetail trailer",
        "sdk-templates/php/src/Generated/GeneratedClient.php.tmpl",
        (
            "udb-error-detail-bin",
            "private function decodeErrorDetail(mixed $status): ?object",
            "$fqn = '\\\\Udb\\\\Entity\\\\V1\\\\ErrorDetail';",
            "$detail->mergeFromString((string) $values[0]);",
            "$exception->errorDetail = $detail;",
        ),
    ),
    TokenCheck(
        "PHP SDK synthesizes transport ErrorDetail",
        "sdk-templates/php/src/Generated/GeneratedClient.php.tmpl",
        (
            "synthesizeTransportErrorDetail",
            "[1, 4, 14]",
            "$detail->setBackend('transport');",
            "$detail->setRetryable($code !== 1);",
            "$detail->setKind($kindFqn::ERROR_KIND_RETRYABLE);",
            "$this->decodeErrorDetail($status) ?? $this->synthesizeTransportErrorDetail($status)",
        ),
    ),
    TokenCheck(
        "PHP checked-in generated SDK decodes and synthesizes ErrorDetail",
        "sdk/php/src/Generated/GeneratedClient.php",
        (
            "udb-error-detail-bin",
            "private function decodeErrorDetail(mixed $status): ?object",
            "$fqn = '\\\\Udb\\\\Entity\\\\V1\\\\ErrorDetail';",
            "$detail->mergeFromString((string) $values[0]);",
            "$exception->errorDetail = $detail;",
            "synthesizeTransportErrorDetail",
            "[1, 4, 14]",
            "$detail->setBackend('transport');",
            "$detail->setRetryable($code !== 1);",
            "$detail->setKind($kindFqn::ERROR_KIND_RETRYABLE);",
            "$this->decodeErrorDetail($status) ?? $this->synthesizeTransportErrorDetail($status)",
        ),
    ),
    TokenCheck(
        "PHP SDK handwritten exception synthesizes transport ErrorDetail",
        "sdk/php/src/Exceptions/UdbRpcException.php",
        (
            "synthesizeTransportErrorDetail",
            "self::decodeErrorDetail($status) ?? self::synthesizeTransportErrorDetail($status)",
            "[1, 4, 14]",
            "$detail->setBackend('transport');",
            "$detail->setRetryable($code !== 1);",
            "$detail->setKind($kindFqn::ERROR_KIND_RETRYABLE);",
            "public function fieldViolations(): array",
            "$ed->getFieldViolations()",
        ),
    ),
    TokenCheck(
        "Java SDK decodes ErrorDetail trailer in support template",
        "sdk-templates/java/src/main/java/dev/udb/client/generated/GeneratedClientSupport.java",
        (
            'Metadata.Key.of("udb-error-detail-bin", Metadata.BINARY_BYTE_MARSHALLER)',
            "public byte[] errorDetail()",
            "public ErrorDetail decodedErrorDetail()",
            "public boolean retryable()",
            "public long retryAfterMs()",
            "public ErrorKind kind()",
            "public java.util.List<java.util.Map<String, String>> fieldViolations()",
            'findFieldByName("field_violations")',
            "ErrorDetail.parseFrom(raw)",
            "synthesizeTransportDetail",
            "Status.Code.DEADLINE_EXCEEDED",
            "Status.Code.CANCELLED",
            ".setBackend(\"transport\")",
            ".setRetryable(code != Status.Code.CANCELLED)",
        ),
    ),
    TokenCheck(
        "Java SDK decodes ErrorDetail trailer in checked-in runtime",
        "sdk/java/src/main/java/dev/udb/client/generated/GeneratedClientSupport.java",
        (
            'Metadata.Key.of("udb-error-detail-bin", Metadata.BINARY_BYTE_MARSHALLER)',
            "public byte[] errorDetail()",
            "public ErrorDetail decodedErrorDetail()",
            "public boolean retryable()",
            "public long retryAfterMs()",
            "public ErrorKind kind()",
            "public java.util.List<java.util.Map<String, String>> fieldViolations()",
            'findFieldByName("field_violations")',
            "ErrorDetail.parseFrom(raw)",
            "synthesizeTransportDetail",
            "Status.Code.DEADLINE_EXCEEDED",
            "Status.Code.CANCELLED",
            ".setBackend(\"transport\")",
            ".setRetryable(code != Status.Code.CANCELLED)",
        ),
    ),
    TokenCheck(
        "C# SDK decodes ErrorDetail trailer in runtime template",
        "sdk-templates/csharp/Udb.Client/GeneratedClientRuntime.cs",
        (
            "Raw protobuf-encoded udb.entity.v1.ErrorDetail bytes",
            "public global::Udb.Entity.V1.ErrorDetail? DecodedErrorDetail",
            "public bool Retryable => DecodedErrorDetail?.Retryable ?? false;",
            "public long RetryAfterMs => DecodedErrorDetail?.RetryAfterMs ?? 0L;",
            "public global::Udb.Entity.V1.ErrorKind Kind",
            "public System.Collections.Generic.IReadOnlyList<(string Field, string Description)> FieldViolations",
            'FindFieldByName("field_violations")',
            "global::Udb.Entity.V1.ErrorDetail.Parser.ParseFrom(raw)",
            "global::Udb.Entity.V1.ErrorKind.Unspecified",
            "SynthesizeTransportDetail",
            "StatusCode.DeadlineExceeded",
            "StatusCode.Cancelled",
            "TransportErrorOperation(code)",
            'StatusCode.DeadlineExceeded => "deadline_exceeded"',
            'Backend = "transport"',
            "Retryable = code != StatusCode.Cancelled",
        ),
    ),
    TokenCheck(
        "C# SDK decodes ErrorDetail trailer in checked-in runtime",
        "sdk/csharp/Udb.Client/GeneratedClientRuntime.cs",
        (
            "Raw protobuf-encoded udb.entity.v1.ErrorDetail bytes",
            "public global::Udb.Entity.V1.ErrorDetail? DecodedErrorDetail",
            "public bool Retryable => DecodedErrorDetail?.Retryable ?? false;",
            "public long RetryAfterMs => DecodedErrorDetail?.RetryAfterMs ?? 0L;",
            "public global::Udb.Entity.V1.ErrorKind Kind",
            "public System.Collections.Generic.IReadOnlyList<(string Field, string Description)> FieldViolations",
            'FindFieldByName("field_violations")',
            "global::Udb.Entity.V1.ErrorDetail.Parser.ParseFrom(raw)",
            "global::Udb.Entity.V1.ErrorKind.Unspecified",
            "SynthesizeTransportDetail",
            "StatusCode.DeadlineExceeded",
            "StatusCode.Cancelled",
            "TransportErrorOperation(code)",
            'StatusCode.DeadlineExceeded => "deadline_exceeded"',
            'Backend = "transport"',
            "Retryable = code != StatusCode.Cancelled",
            'string.Equals(entry.Key, "udb-error-detail-bin", StringComparison.OrdinalIgnoreCase)',
        ),
    ),
    TokenCheck(
        "SDK conformance runner hard-gates ErrorDetail parity",
        "sdk-conformance/run.mjs",
        (
            '"error-details"',
            "function checkErrorDetailConformance()",
            '"dist-test/sdkhelpers.test.js"',
            '"tests/test_simple_client.py"',
            '"-k", "error_detail"',
            '"TestErrorDetail"',
            '"FullyQualifiedName~UdbRpcExceptionTests"',
            '"-Dtest=UdbRpcExceptionTest"',
            '"tests/Unit/SimpleClientTest.php"',
            "Boolean(process.env.CI) || strictSelected",
            'return { name, status: "FAIL", note: "setup error" };',
            'results.push({ name, status: "FAIL", note: "setup error" });',
            '"typed validation/quota/transport ErrorDetail + field violations aligned across SDK slices"',
        ),
    ),
    TokenCheck(
        "served ErrorDetail smoke proves live trailer transport",
        "scripts/error_detail_served_smoke.py",
        (
            'ERROR_DETAIL_METADATA_KEY = "udb-error-detail-bin"',
            'VALIDATION_STATUS = "INVALID_ARGUMENT"',
            'QUOTA_STATUS = "RESOURCE_EXHAUSTED"',
            "matches: list[object]",
            "def _trailing_metadata_items(",
            "def decode_error_detail(",
            "def check_error_detail(",
            "def rpc_status_message(",
            "def load_request(",
            "def _service_descriptor_module_candidates(",
            "def assert_request_matches_method(",
            "request.DESCRIPTOR.full_name",
            "method_descriptor.input_type.full_name",
            "generated service descriptor was not found",
            "does not match RPC input",
            "MAX_PROOF_INPUT_BYTES = 1_048_576",
            "def _read_proof_text(",
            "proof file must exist and be a regular file",
            "proof file must be <=",
            "could not be imported",
            "does not expose",
            "object_pairs_hook=_reject_duplicate_json_keys",
            "parse_constant=_reject_non_finite_json_constant",
            "request JSON must be a valid JSON object",
            "request JSON must be a JSON object",
            "request JSON must not contain duplicate key",
            "request JSON must not contain non-standard constant",
            "GRPC_METADATA_NAME_CHARS",
            "gRPC metadata header name must contain only lowercase letters",
            "gRPC metadata header name must not start with grpc-",
            "def _contains_control_character(",
            "def validate_grpc_target(",
            "gRPC target must be a host:port authority, not a URL or path",
            "gRPC target must not include control characters",
            "gRPC target port must be an integer from 1 to 65535",
            "MAX_LIVE_TIMEOUT_SECONDS = 120.0",
            "def validate_timeout_seconds(",
            "timeout must be a finite number of seconds",
            "timeout must be greater than 0 seconds",
            "timeout must be <= 120 seconds",
            "MAX_STATUS_MESSAGE_BYTES = 8_192",
            "MAX_FIELD_VIOLATION_DESCRIPTION_BYTES = 8_192",
            "MAX_ERROR_DETAIL_TRAILER_BYTES = 1_048_576",
            "def invoke_expect_error(",
            "def validate_runtime_unary_call(",
            "runtime unary call must be callable",
            "runtime unary factory raised error",
            "runtime unary call raised non-gRPC error",
            'validate_method_path(f"{label} runtime proof", method)',
            "ERROR_KIND_VALIDATION",
            "ERROR_KIND_QUOTA",
            "got unknown ErrorDetail.kind",
            "def _assert_error_detail_token(",
            "ErrorDetail.backend must be non-empty",
            "ErrorDetail.{field} must not include control characters",
            "ErrorDetail.operation must not include surrounding whitespace",
            "got ErrorDetail.backend",
            "got ErrorDetail.operation",
            "gRPC status code could not be read",
            "gRPC status code must be a grpc.StatusCode",
            "gRPC status message could not be read",
            "gRPC status message must be a string",
            "gRPC status message must be non-empty",
            "gRPC status message must not include surrounding whitespace",
            "gRPC status message must not contain control characters",
            "gRPC status message must be <=",
            "field_violations",
            "field_violations[{index}].field must be non-empty",
            "field_violations[{index}].field must not include control characters",
            "field violation {violation.field!r} must include a non-empty description",
            "description must not contain control characters",
            "description must be <=",
            "control-character field description regression was not caught",
            "control-character field violation regression was not caught",
            "oversized field description regression was not caught",
            "quota/backpressure detail must not include field_violations",
            "validation detail must not include retry_after_ms",
            "validation detail must not include backend/operation",
            "validation proof must include exactly",
            "got retryable=True, want False",
            "got retryable=False, want True",
            "got retry_after_ms=100, want >= 200",
            "retry_after_ms",
            "def validate_live_proof_inputs(",
            "def validate_live_check_expectations(",
            "def validate_required_expected_token(",
            "def validate_expected_token(",
            "def validate_method_path(",
            "method must be a full gRPC method path like /package.Service/Method",
            "method must not include surrounding whitespace",
            "must not include whitespace",
            "validation proof must expect INVALID_ARGUMENT",
            "quota retry/backpressure proof must expect RESOURCE_EXHAUSTED",
            "quota retry/backpressure proof requires --quota-retry-after-min-ms > 0",
            "quota retry/backpressure proof requires --quota-backend",
            "quota retry/backpressure proof requires --quota-operation",
            "expected exactly one udb-error-detail-bin trailer",
            "trailer metadata could not be read",
            "trailer metadata iteration failed",
            "trailer metadata must be iterable",
            "trailer metadata item could not be read",
            "trailer metadata item must be a key/value pair",
            "trailer metadata key must be a string",
            "trailer metadata key must be lowercase",
            "trailer must be bytes",
            "trailer must be <=",
            "duplicate ErrorDetail trailer regression was not caught",
            "unreadable ErrorDetail trailer metadata regression was not caught",
            "failing ErrorDetail trailer metadata iterator regression was not caught",
            "non-iterable ErrorDetail trailer metadata regression was not caught",
            "failing ErrorDetail trailer metadata item regression was not caught",
            "malformed ErrorDetail trailer metadata item regression was not caught",
            "non-string ErrorDetail trailer metadata key regression was not caught",
            "uppercase ErrorDetail trailer metadata key regression was not caught",
            "string ErrorDetail trailer regression was not caught",
            "oversized ErrorDetail trailer regression was not caught",
            "initial-metadata ErrorDetail regression was not caught",
            "--validation-method",
            "--validation-request-module",
            "--validation-request-message",
            "--validation-request-json",
            "--validation-field",
            "--quota-method",
            "--quota-request-module",
            "--quota-request-message",
            "--quota-request-json",
            "--quota-retry-after-min-ms",
            "quota retry-after floor",
            "--quota-backend",
            "--quota-operation",
            "REQUIRED_LIVE_PROOF_INPUTS",
            "def missing_required_live_proofs(",
            "if isinstance(value, (int, float)):",
            "--require-all-proofs",
            "missing field regression was not caught",
            "non-grpc StatusCode regression was not caught",
            "unreadable status code regression was not caught",
            "empty status message regression was not caught",
            "unreadable status message regression was not caught",
            "non-string status message regression was not caught",
            "padded status message regression was not caught",
            "control-character status message regression was not caught",
            "oversized status message regression was not caught",
            "unknown ErrorDetail kind regression was not caught",
            "runtime method-path validation regression was not caught",
            "runtime expected-token validation regression was not caught",
            "runtime expected-kind validation regression was not caught",
            "def validate_runtime_request_message(",
            "runtime request-message validation regression was not caught",
            "method/request descriptor mismatch regression was not caught",
            "missing service descriptor regression was not caught",
            "def validate_runtime_metadata(",
            "def validate_runtime_timeout_seconds(",
            "def validate_runtime_channel_method(",
            "runtime channel must expose callable unary_unary",
            "runtime metadata validation regression was not caught",
            "runtime timeout validation regression was not caught",
            "runtime channel-method validation regression was not caught",
            "runtime unary-call validation regression was not caught",
            "runtime unary-factory validation regression was not caught",
            "runtime unary non-gRPC error validation regression was not caught",
            "validation runtime proof must expect",
            "validation runtime proof requires an expected field",
            "quota runtime proof requires expected backend and operation",
            "runtime validation semantics regression was not caught",
            "runtime validation field semantics regression was not caught",
            "runtime quota semantics regression was not caught",
            "extra validation field regression was not caught",
            "empty field description regression was not caught",
            "malformed extra field violation regression was not caught",
            "validation retry-after regression was not caught",
            "validation retryable regression was not caught",
            "validation backend/operation regression was not caught",
            "quota retryable regression was not caught",
            "quota retry-after floor regression was not caught",
            "quota field-violations regression was not caught",
            "quota backend/operation regression was not caught",
            "quota backend token regression was not caught",
            "quota operation token regression was not caught",
            "quota proof missing positive retry-after",
            "validation proof malformed method path",
            "validation proof field has surrounding whitespace",
            "validation proof field has control character",
            "quota proof method path has surrounding whitespace",
            "quota proof missing backend",
            "quota proof missing operation",
            "quota proof backend has embedded whitespace",
            "quota proof backend has control character",
            "quota proof operation has surrounding whitespace",
            "quota proof operation has control character",
            "whitespace-only required proof input regression was not caught",
            "whitespace-only focused proof readiness regression was not caught",
            "array request JSON",
            "missing request JSON file",
            "missing request module",
            "missing request message",
            "oversized request JSON file",
            "malformed request JSON",
            "duplicate-key request JSON",
            "non-finite request JSON",
            "malformed gRPC header name regression was not caught",
            "reserved gRPC header name regression was not caught",
            "URL-shaped gRPC target regression was not caught",
            "whitespace gRPC target regression was not caught",
            "control-character gRPC target regression was not caught",
            "missing-port gRPC target regression was not caught",
            "non-positive timeout regression was not caught",
            "infinite timeout regression was not caught",
            "excessive timeout regression was not caught",
            "validation proof status weakened",
            "quota proof status weakened",
            "error detail served smoke selftest passed",
        ),
    ),
    TokenCheck(
        "served ErrorDetail workflow is self-contained",
        ".github/workflows/error-detail-served-smoke.yml",
        (
            "error-detail-served:",
            "release_tag:",
            "release_asset:",
            "postgres:",
            "mongodb:",
            "broker_artifact_run_id:",
            "uses: ./.github/actions/resolve-served-binary",
            "broker-artifact-run-id: ${{ inputs.broker_artifact_run_id }}",
            "uses: ./.github/actions/start-backends",
            "uses: ./.github/actions/broker-env",
            "UDB_OTP_COOLDOWN_SECONDS=60",
            "uses: ./.github/actions/launch-broker",
            "Bootstrap served-smoke user",
            "scripts/write_error_detail_served_smoke_inputs.py",
            "python -m pip install -e sdk/python",
            'clickhouse: "false"',
            'neo4j: "false"',
            'enable_column_backend: "false"',
            'enable_graph_backend: "false"',
            "python scripts/error_detail_served_smoke.py --selftest",
            "--require-all-proofs",
            "done < smoke-input/header.txt",
            "--validation-method /udb.core.authn.services.v1.AuthnService/SendPhoneVerification",
            "--validation-request-message SendPhoneVerificationRequest",
            "--validation-request-json smoke-input/validation.json",
            "--validation-field phone",
            "--quota-method /udb.core.authn.services.v1.AuthnService/SendOTP",
            "--quota-request-message SendOTPRequest",
            "--quota-request-json smoke-input/quota.json",
            "--quota-retry-after-min-ms 1000",
            "--quota-backend authn",
            "--quota-operation otp_cooldown",
            "error-detail-served-smoke-diagnostics",
        ),
    ),
    TokenCheck(
        "served ErrorDetail input generator emits full proof metadata",
        "scripts/write_error_detail_served_smoke_inputs.py",
        (
            'PROOF_PURPOSE = "error-detail-served-smoke"',
            "class AuthProof:",
            "principal = auth.authenticate_bearer(login.access_token)",
            'getattr(principal.principal, "tenant_id", "") or tenant_hint',
            "return AuthProof(tenant_id=tenant_id, project_id=project, bearer=login.access_token)",
            '("x-tenant-id", auth.tenant_id)',
            'tenant_id=auth.tenant_id',
            'project_id=auth.project_id',
            '("x-purpose", PROOF_PURPOSE)',
            '("x-request-id", f"error-detail-served-smoke-{nonce}")',
            '("x-scopes", "udb:admin")',
            '"phone": ""',
            "x-purpose: {PROOF_PURPOSE}",
            "x-request-id: error-detail-served-smoke-{nonce}",
            "x-scopes: udb:admin",
        ),
    ),
    TokenCheck(
        "cross-language SDK ErrorDetail fixtures assert canonical validation and quota shapes",
        "sdk/go/udbclient/errordetail_test.go",
        (
            "entityv1.ErrorKind_ERROR_KIND_VALIDATION",
            'Field: "email", Description: "must be a valid email"',
            "len(got) != 1",
            "TestErrorDetailQuotaRetryAfterDecode",
            "entityv1.ErrorKind_ERROR_KIND_QUOTA",
            "RetryAfterMs: 250",
            "TestTransportErrorDetailSynthesized",
            "codes.DeadlineExceeded",
            'd.GetBackend() != "transport"',
            "TestCancelledTransportErrorDetailIsNotRetryable",
            "codes.Canceled",
            'd.GetOperation() != "cancelled"',
            "mapped.Retryable()",
            "entityv1.ErrorKind_ERROR_KIND_RETRYABLE",
        ),
    ),
    TokenCheck(
        "TypeScript SDK ErrorDetail fixture asserts canonical validation and quota shapes",
        "sdk/typescript/sdkhelpers.test.ts",
        (
            'assert.equal(e.kindName, "VALIDATION");',
            "assert.deepEqual(e.fieldViolations",
            'field: "email", description: "must be a valid email"',
            "the real decoder preserves retryable quota backoff detail",
            'assert.equal(e.kindName, "QUOTA");',
            "assert.equal(e.detail?.retry_after_ms, 250);",
            "trailerless transport errors synthesize the same retryable detail shape",
            "grpc.status.DEADLINE_EXCEEDED",
            'assert.equal(e.detail?.backend, "transport");',
            "trailerless cancellation synthesizes non-retryable transport detail",
            "grpc.status.CANCELLED",
            'assert.equal(e.detail?.operation, "cancelled");',
            "assert.equal(e.retryable, false);",
            'assert.equal(e.kindName, "RETRYABLE");',
        ),
    ),
    TokenCheck(
        "Python SDK ErrorDetail fixture asserts canonical validation and quota shapes",
        "sdk/python/tests/test_simple_client.py",
        (
            "ErrorFieldViolation(field=\"email\", description=\"must be a valid email\")",
            'assert wrapped.kind() == "ERROR_KIND_VALIDATION"',
            'assert wrapped.field_violations == [',
            "test_error_detail_quota_retry_after_decoded_typed",
            "ErrorKind.ERROR_KIND_QUOTA",
            "assert wrapped.retry_after_ms == 250",
            "test_transport_error_detail_synthesized_typed",
            "grpc.StatusCode.DEADLINE_EXCEEDED",
            'assert wrapped.error_detail.backend == "transport"',
            "test_cancelled_transport_error_detail_synthesized_not_retryable",
            "grpc.StatusCode.CANCELLED",
            'assert wrapped.error_detail.operation == "cancelled"',
            "assert wrapped.is_retryable() is False",
            'assert wrapped.kind() == "ERROR_KIND_RETRYABLE"',
        ),
    ),
    TokenCheck(
        "PHP SDK ErrorDetail fixture asserts canonical validation and quota shapes",
        "sdk/php/tests/Unit/SimpleClientTest.php",
        (
            "\\Udb\\Entity\\V1\\ErrorKind::ERROR_KIND_VALIDATION",
            "$violation->setField('email');",
            "$violation->setDescription('must be a valid email');",
            "->and($e->fieldViolations())->toBe([",
            "preserves retryable quota backoff detail",
            "\\Udb\\Entity\\V1\\ErrorKind::ERROR_KIND_QUOTA",
            "->and($e->errorDetail->getRetryAfterMs())->toBe(250)",
            "synthesizes retryable transport detail without a trailer",
            "'code' => 4",
            "->and($e->errorDetail->getBackend())->toBe('transport')",
            "synthesizes non-retryable cancelled transport errorDetail without a trailer",
            "'code' => 1",
            "->and($e->errorDetail->getOperation())->toBe('cancelled')",
            "->and($e->isRetryable())->toBeFalse()",
            "->and($e->kind())->toBe('ERROR_KIND_RETRYABLE')",
        ),
    ),
    TokenCheck(
        "Java SDK ErrorDetail fixture asserts canonical validation and quota shapes",
        "sdk/java/src/test/java/dev/udb/client/UdbRpcExceptionTest.java",
        (
            "ErrorKind.ERROR_KIND_VALIDATION",
            '.setField("email")',
            '.setDescription("must be a valid email")',
            "GeneratedClientSupport.mapError(",
            "assertEquals(",
            "ex.fieldViolations()",
            "decodesQuotaErrorDetailRetryBackoff",
            "ErrorKind.ERROR_KIND_QUOTA",
            "assertEquals(250, ex.retryAfterMs())",
            "Status.DEADLINE_EXCEEDED",
            'assertEquals("deadline_exceeded", ex.decodedErrorDetail().getOperation())',
            "synthesizesCancelledTransportErrorDetailAsNotRetryable",
            "Status.CANCELLED",
            'assertEquals("cancelled", ex.decodedErrorDetail().getOperation())',
            "assertFalse(ex.retryable())",
            "assertEquals(0, ex.retryAfterMs())",
        ),
    ),
    TokenCheck(
        "C# SDK ErrorDetail fixture asserts canonical validation and quota shapes",
        "sdk/csharp/Udb.Client.Tests/UdbRpcExceptionTests.cs",
        (
            "EntityV1.ErrorKind.Validation",
            'Field = "email"',
            'Description = "must be a valid email"',
            "new UdbRpcException(",
            "Assert.Single(ex.FieldViolations)",
            "Decodes_Quota_ErrorDetail_Retry_Backoff",
            "EntityV1.ErrorKind.Quota",
            "Assert.Equal(250, ex.RetryAfterMs)",
            "StatusCode.DeadlineExceeded",
            'Assert.Equal("deadline_exceeded", ex.DecodedErrorDetail!.Operation)',
            "Synthesizes_Cancelled_Transport_ErrorDetail_As_Not_Retryable",
            "StatusCode.Cancelled",
            'Assert.Equal("cancelled", ex.DecodedErrorDetail!.Operation)',
            "Assert.False(ex.Retryable)",
            "Assert.Equal(0, ex.RetryAfterMs)",
        ),
    ),
)

ERROR_REASON_REGISTRY: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "src/runtime/service/storage_service",
        (
            "STORAGE_QUOTA_EXCEEDED",
            "UPLOAD_URL_UNAVAILABLE",
            "OBJECT_NOT_PRESENT",
            "UPLOAD_SIZE_MISMATCH",
            "ALREADY_FINALIZED",
            "UNSUPPORTED_OBJECT_BACKEND",
        ),
    ),
    (
        "src/runtime/service/asset_service",
        (
            "PIPELINE_DEFINITION_INVALID",
            "STEP_TYPE_UNSUPPORTED",
            "PIPELINE_ALREADY_STARTED",
        ),
    ),
    (
        "src/runtime/service/notification_service",
        (
            "TEMPLATE_NOT_FOUND",
            "VARIABLE_MISSING",
            "NOT_RETRYABLE_STATE",
        ),
    ),
    (
        "src/runtime/service/webrtc_service/mod.rs",
        (
            "ROOM_FULL",
            "PEER_NOT_ACTIVE",
            "TURN_NOT_CONFIGURED",
            "SFU_BACKEND_UNAVAILABLE",
            "EGRESS_NOT_ENABLED",
            "EGRESS_BACKEND_UNAVAILABLE",
        ),
    ),
)

DOC_FORBIDDEN = (
    "canonical server error model is gRPC `google.rpc.Status`",
    "google.rpc.Status.details",
    "information rides in `google.rpc.Status.details`",
    "The error body is `ApiError` mapped from `google.rpc.Status`",
)

DIRECT_INVALID_ARGUMENT_PATTERNS = (
    "Status::invalid_argument",
    "tonic::Status::invalid_argument",
    "invalid_argument(",
)

DIRECT_INVALID_ARGUMENT_REGEXES = (
    re.compile(r"\b(?:tonic::)?Status::new\(\s*(?:tonic::)?Code::InvalidArgument\b"),
    re.compile(r"\b(?:tonic::)?Status::with_metadata\(\s*(?:tonic::)?Code::InvalidArgument\b"),
    re.compile(r"\b(?:tonic::)?Status::with_details\(\s*(?:tonic::)?Code::InvalidArgument\b"),
    re.compile(r"\b(?:tonic::)?Status::with_details_and_metadata\(\s*(?:tonic::)?Code::InvalidArgument\b"),
)

DIRECT_FAILED_PRECONDITION_PATTERNS = (
    "Status::failed_precondition",
    "tonic::Status::failed_precondition",
    "failed_precondition(",
)

DIRECT_FAILED_PRECONDITION_REGEXES = (
    re.compile(r"\b(?:tonic::)?Status::new\(\s*(?:tonic::)?Code::FailedPrecondition\b"),
    re.compile(r"\b(?:tonic::)?Status::with_metadata\(\s*(?:tonic::)?Code::FailedPrecondition\b"),
    re.compile(r"\b(?:tonic::)?Status::with_details\(\s*(?:tonic::)?Code::FailedPrecondition\b"),
    re.compile(r"\b(?:tonic::)?Status::with_details_and_metadata\(\s*(?:tonic::)?Code::FailedPrecondition\b"),
)

DIRECT_PERMISSION_DENIED_PATTERNS = (
    "Status::permission_denied",
    "tonic::Status::permission_denied",
)

DIRECT_PERMISSION_DENIED_REGEXES = (
    re.compile(r"\b(?:tonic::)?Status::new\(\s*(?:tonic::)?Code::PermissionDenied\b"),
    re.compile(r"\b(?:tonic::)?Status::with_metadata\(\s*(?:tonic::)?Code::PermissionDenied\b"),
    re.compile(r"\b(?:tonic::)?Status::with_details\(\s*(?:tonic::)?Code::PermissionDenied\b"),
    re.compile(r"\b(?:tonic::)?Status::with_details_and_metadata\(\s*(?:tonic::)?Code::PermissionDenied\b"),
)

DIRECT_NOT_FOUND_PATTERNS = (
    "Status::not_found",
    "tonic::Status::not_found",
)

DIRECT_NOT_FOUND_REGEXES = (
    re.compile(r"\b(?:tonic::)?Status::new\(\s*(?:tonic::)?Code::NotFound\b"),
    re.compile(r"\b(?:tonic::)?Status::with_metadata\(\s*(?:tonic::)?Code::NotFound\b"),
    re.compile(r"\b(?:tonic::)?Status::with_details\(\s*(?:tonic::)?Code::NotFound\b"),
    re.compile(r"\b(?:tonic::)?Status::with_details_and_metadata\(\s*(?:tonic::)?Code::NotFound\b"),
)

DIRECT_ALREADY_EXISTS_PATTERNS = (
    "Status::already_exists",
    "tonic::Status::already_exists",
)

DIRECT_ALREADY_EXISTS_REGEXES = (
    re.compile(r"\b(?:tonic::)?Status::new\(\s*(?:tonic::)?Code::AlreadyExists\b"),
    re.compile(r"\b(?:tonic::)?Status::with_metadata\(\s*(?:tonic::)?Code::AlreadyExists\b"),
    re.compile(r"\b(?:tonic::)?Status::with_details\(\s*(?:tonic::)?Code::AlreadyExists\b"),
    re.compile(r"\b(?:tonic::)?Status::with_details_and_metadata\(\s*(?:tonic::)?Code::AlreadyExists\b"),
)

DIRECT_RETRY_DETAIL_CONSTRUCTORS = (
    "Status::unavailable",
    "tonic::Status::unavailable",
    "Status::resource_exhausted",
    "tonic::Status::resource_exhausted",
    "Status::aborted",
    "tonic::Status::aborted",
    "Status::deadline_exceeded",
    "tonic::Status::deadline_exceeded",
)

DIRECT_RETRY_DETAIL_REGEXES = (
    (
        re.compile(r"\b(?:tonic::)?Status::new\(\s*(?:tonic::)?Code::Unavailable\b"),
        "Status::new(Code::Unavailable)",
    ),
    (
        re.compile(r"\b(?:tonic::)?Status::with_metadata\(\s*(?:tonic::)?Code::Unavailable\b"),
        "Status::with_metadata(Code::Unavailable)",
    ),
    (
        re.compile(r"\b(?:tonic::)?Status::with_details\(\s*(?:tonic::)?Code::Unavailable\b"),
        "Status::with_details(Code::Unavailable)",
    ),
    (
        re.compile(r"\b(?:tonic::)?Status::with_details_and_metadata\(\s*(?:tonic::)?Code::Unavailable\b"),
        "Status::with_details_and_metadata(Code::Unavailable)",
    ),
    (
        re.compile(r"\b(?:tonic::)?Status::new\(\s*(?:tonic::)?Code::ResourceExhausted\b"),
        "Status::new(Code::ResourceExhausted)",
    ),
    (
        re.compile(r"\b(?:tonic::)?Status::with_metadata\(\s*(?:tonic::)?Code::ResourceExhausted\b"),
        "Status::with_metadata(Code::ResourceExhausted)",
    ),
    (
        re.compile(r"\b(?:tonic::)?Status::with_details\(\s*(?:tonic::)?Code::ResourceExhausted\b"),
        "Status::with_details(Code::ResourceExhausted)",
    ),
    (
        re.compile(r"\b(?:tonic::)?Status::with_details_and_metadata\(\s*(?:tonic::)?Code::ResourceExhausted\b"),
        "Status::with_details_and_metadata(Code::ResourceExhausted)",
    ),
    (
        re.compile(r"\b(?:tonic::)?Status::new\(\s*(?:tonic::)?Code::Aborted\b"),
        "Status::new(Code::Aborted)",
    ),
    (
        re.compile(r"\b(?:tonic::)?Status::with_metadata\(\s*(?:tonic::)?Code::Aborted\b"),
        "Status::with_metadata(Code::Aborted)",
    ),
    (
        re.compile(r"\b(?:tonic::)?Status::with_details\(\s*(?:tonic::)?Code::Aborted\b"),
        "Status::with_details(Code::Aborted)",
    ),
    (
        re.compile(r"\b(?:tonic::)?Status::with_details_and_metadata\(\s*(?:tonic::)?Code::Aborted\b"),
        "Status::with_details_and_metadata(Code::Aborted)",
    ),
    (
        re.compile(r"\b(?:tonic::)?Status::new\(\s*(?:tonic::)?Code::DeadlineExceeded\b"),
        "Status::new(Code::DeadlineExceeded)",
    ),
    (
        re.compile(r"\b(?:tonic::)?Status::with_metadata\(\s*(?:tonic::)?Code::DeadlineExceeded\b"),
        "Status::with_metadata(Code::DeadlineExceeded)",
    ),
    (
        re.compile(r"\b(?:tonic::)?Status::with_details\(\s*(?:tonic::)?Code::DeadlineExceeded\b"),
        "Status::with_details(Code::DeadlineExceeded)",
    ),
    (
        re.compile(r"\b(?:tonic::)?Status::with_details_and_metadata\(\s*(?:tonic::)?Code::DeadlineExceeded\b"),
        "Status::with_details_and_metadata(Code::DeadlineExceeded)",
    ),
)

DIRECT_UNIMPLEMENTED_PATTERNS = (
    "Status::unimplemented",
    "tonic::Status::unimplemented",
    "unimplemented!",
)

DIRECT_UNIMPLEMENTED_REGEXES = (
    re.compile(r"\b(?:tonic::)?Status::new\(\s*(?:tonic::)?Code::Unimplemented\b"),
    re.compile(r"\b(?:tonic::)?Status::with_metadata\(\s*(?:tonic::)?Code::Unimplemented\b"),
    re.compile(r"\b(?:tonic::)?Status::with_details\(\s*(?:tonic::)?Code::Unimplemented\b"),
    re.compile(r"\b(?:tonic::)?Status::with_details_and_metadata\(\s*(?:tonic::)?Code::Unimplemented\b"),
)

AUTHN_LOOKUP_NOT_FOUND_PATHS = (
    "src/runtime/service/auth_service/authn/mod.rs",
    "src/runtime/service/auth_service/authn/core.rs",
    "src/runtime/service/auth_service/authn/lifecycle.rs",
    "src/runtime/service/auth_service/authn/login.rs",
    "src/runtime/service/auth_service/authn/mfa.rs",
    "src/runtime/service/auth_service/authn/sessions.rs",
)

AUTHN_DIRECT_NOT_FOUND_PATTERNS = (
    'Status::not_found("user not found")',
    'tonic::Status::not_found("user not found")',
    'Status::not_found("otp not found")',
    'tonic::Status::not_found("otp not found")',
    'Status::not_found("device not found or already revoked")',
    'tonic::Status::not_found("device not found or already revoked")',
)

ANALYTICS_INTERNAL_STATUS_PATH = "src/runtime/service/analytics_service"
ADMIN_HANDLERS_INTERNAL_STATUS_PATH = "src/runtime/service/handlers_admin.rs"
APIKEY_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/apikey.rs"
ASSET_INTERNAL_STATUS_PATH = "src/runtime/service/asset_service"
AUTHN_CORE_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/authn/core.rs"
AUTHN_LIFECYCLE_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/authn/lifecycle.rs"
AUTHN_LOGIN_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/authn/login.rs"
AUTHN_MOD_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/authn/mod.rs"
AUTHN_MFA_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/authn/mfa.rs"
AUTHN_SESSIONS_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/authn/sessions.rs"
AUTHN_SIGNING_KEYS_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/authn/signing_keys.rs"
AUTHN_TOKEN_FAMILY_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/authn/token_family.rs"
AUTHZ_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/authz/mod.rs"
AUTHZ_AUDIT_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/authz/audit.rs"
AUTHZ_GOVERNANCE_ACTIVATE_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/authz/governance_activate.rs"
AUTHZ_GOVERNANCE_DRAFTS_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/authz/governance_drafts.rs"
AUTHZ_GOVERNANCE_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/authz/governance.rs"
AUTHZ_GOVERNANCE_SIM_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/authz/governance_sim.rs"
AUTHZ_GOVERNANCE_STORE_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/authz/governance_store.rs"
AUTHZ_TUPLES_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/authz/tuples.rs"
BACKUP_INTERNAL_STATUS_PATH = "src/runtime/service/backup_service"
CASSANDRA_INTERNAL_STATUS_PATH = "src/runtime/executors/cassandra.rs"
CATALOG_ADMIN_INTERNAL_STATUS_PATH = "src/runtime/core/catalog_admin.rs"
CATALOG_HANDLERS_INTERNAL_STATUS_PATH = "src/runtime/service/handlers_catalog.rs"
CATALOG_SQL_INTERNAL_STATUS_PATH = "src/runtime/core/catalog_sql.rs"
CLICKHOUSE_INTERNAL_STATUS_PATH = "src/runtime/executors/clickhouse.rs"
CONTROL_PLANE_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/control_plane/mod.rs"
CONTROL_PLANE_SOURCING_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/control_plane/sourcing.rs"
CONTROL_PLANE_STORE_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/control_plane/store.rs"
CORE_MOD_INTERNAL_STATUS_PATH = "src/runtime/core/mod.rs"
CORE_NATIVE_STORE_INTERNAL_STATUS_PATH = "src/runtime/core/native_store.rs"
DATA_HANDLERS_INTERNAL_STATUS_PATH = "src/runtime/service/handlers_data.rs"
ELASTICSEARCH_INTERNAL_STATUS_PATH = "src/runtime/executors/elasticsearch.rs"
IDP_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/idp/mod.rs"
IDP_STORE_INTERNAL_STATUS_PATH = "src/runtime/service/auth_service/idp/store.rs"
MEMCACHED_INTERNAL_STATUS_PATH = "src/runtime/executors/memcached.rs"
METERING_INTERNAL_STATUS_PATH = "src/runtime/service/metering_service"
MONGODB_INTERNAL_STATUS_PATH = "src/runtime/executors/mongodb.rs"
MYSQL_INTERNAL_STATUS_PATH = "src/runtime/executors/mysql.rs"
MSSQL_INTERNAL_STATUS_PATH = "src/runtime/executors/mssql.rs"
NATIVE_ENTITY_STORE_INTERNAL_STATUS_PATH = "src/runtime/service/native_entity_store.rs"
NEO4J_INTERNAL_STATUS_PATH = "src/runtime/executors/neo4j.rs"
NOTIFICATION_INTERNAL_STATUS_PATH = "src/runtime/service/notification_service"
PINECONE_INTERNAL_STATUS_PATH = "src/runtime/executors/pinecone.rs"
POSTGRES_EXECUTOR_INTERNAL_STATUS_PATH = "src/runtime/executors/postgres.rs"
POSTGRES_HELPERS_INTERNAL_STATUS_PATH = "src/runtime/postgres_helpers.rs"
PROBE_DISPATCH_INTERNAL_STATUS_PATH = "src/runtime/core/probe_dispatch.rs"
QDRANT_INTERNAL_STATUS_PATH = "src/runtime/executors/qdrant.rs"
SAGA_INTERNAL_STATUS_PATH = "src/runtime/saga.rs"
S3_INTERNAL_STATUS_PATH = "src/runtime/executors/s3.rs"
SCHEDULER_INTERNAL_STATUS_PATH = "src/runtime/service/scheduler_service"
SETUP_DATA_INTERNAL_STATUS_PATH = "src/runtime/core/setup_data.rs"
SQLITE_INTERNAL_STATUS_PATH = "src/runtime/executors/sqlite.rs"
STORAGE_INTERNAL_STATUS_PATH = "src/runtime/service/storage_service"
SYSTEM_CATALOG_INTERNAL_STATUS_PATH = "src/runtime/system.rs"
TENANT_PURGE_INTERNAL_STATUS_PATH = "src/runtime/core/tenant_purge.rs"
TENANT_INTERNAL_STATUS_PATH = "src/runtime/service/tenant_service"
TX_OBJECT_INTERNAL_STATUS_PATH = "src/runtime/core/tx_object.rs"
VAULT_INTERNAL_STATUS_PATH = "src/runtime/service/vault_service"
WEBHOOK_INTERNAL_STATUS_PATH = "src/runtime/service/webhook_service"
WEAVIATE_INTERNAL_STATUS_PATH = "src/runtime/executors/weaviate.rs"
WEBRTC_INTERNAL_STATUS_PATH = "src/runtime/service/webrtc_service/mod.rs"
WORKFLOW_INTERNAL_STATUS_PATH = "src/runtime/service/workflow_service"

DIRECT_INTERNAL_PATTERNS = (
    "Status::internal",
    "tonic::Status::internal",
)

DIRECT_INTERNAL_REGEXES = (
    re.compile(r"\b(?:tonic::)?Status::new\(\s*(?:tonic::)?Code::Internal\b"),
    re.compile(r"\b(?:tonic::)?Status::with_metadata\(\s*(?:tonic::)?Code::Internal\b"),
    re.compile(r"\b(?:tonic::)?Status::with_details\(\s*(?:tonic::)?Code::Internal\b"),
    re.compile(r"\b(?:tonic::)?Status::with_details_and_metadata\(\s*(?:tonic::)?Code::Internal\b"),
)

def read(path: Path) -> str:
    if _READ_CACHE is not None and path in _READ_CACHE:
        return _READ_CACHE[path]
    try:
        if path.is_dir():
            # A TokenCheck path may name a modularized service DIRECTORY; read all
            # its `.rs` files concatenated so tokens split across submodules
            # (handlers/errors/predicate/tests/…) are still found after a
            # god-file → module-tree refactor.
            text = "\n".join(p.read_text(encoding="utf-8") for p in sorted(path.rglob("*.rs")))
        else:
            text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        text = ""
    if _READ_CACHE is not None:
        _READ_CACHE[path] = text
    return text


def live_rust_source_paths(root: Path) -> list[Path]:
    paths: list[Path] = []
    for source_root_name in ("src", "crates"):
        source_root = root / source_root_name
        if source_root.exists():
            paths.extend(sorted(source_root.rglob("*.rs")))
    return sorted(paths)


def live_invalid_argument_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    for path in live_rust_source_paths(root):
        rel = path.relative_to(root).as_posix()
        for lineno, line in enumerate(read(path).splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("//"):
                continue
            if any(pattern in line for pattern in DIRECT_INVALID_ARGUMENT_PATTERNS):
                hits.append(f"{rel}:{lineno}: direct invalid_argument constructor")
            for pattern in DIRECT_INVALID_ARGUMENT_REGEXES:
                if pattern.search(line):
                    hits.append(f"{rel}:{lineno}: direct concrete InvalidArgument status constructor")
    return hits


def live_failed_precondition_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    for path in live_rust_source_paths(root):
        rel = path.relative_to(root).as_posix()
        for lineno, line in enumerate(read(path).splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("//"):
                continue
            if any(pattern in line for pattern in DIRECT_FAILED_PRECONDITION_PATTERNS):
                hits.append(f"{rel}:{lineno}: direct failed_precondition constructor")
            for pattern in DIRECT_FAILED_PRECONDITION_REGEXES:
                if pattern.search(line):
                    hits.append(f"{rel}:{lineno}: direct concrete FailedPrecondition status constructor")
    return hits


def live_permission_denied_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    for path in live_rust_source_paths(root):
        rel = path.relative_to(root).as_posix()
        for lineno, line in enumerate(read(path).splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("//"):
                continue
            if any(pattern in line for pattern in DIRECT_PERMISSION_DENIED_PATTERNS):
                hits.append(f"{rel}:{lineno}: direct permission_denied constructor")
            for pattern in DIRECT_PERMISSION_DENIED_REGEXES:
                if pattern.search(line):
                    hits.append(f"{rel}:{lineno}: direct concrete PermissionDenied status constructor")
    return hits


def live_not_found_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    for path in live_rust_source_paths(root):
        rel = path.relative_to(root).as_posix()
        for lineno, line in enumerate(read(path).splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("//"):
                continue
            if any(pattern in line for pattern in DIRECT_NOT_FOUND_PATTERNS):
                hits.append(f"{rel}:{lineno}: direct not_found constructor")
            for pattern in DIRECT_NOT_FOUND_REGEXES:
                if pattern.search(line):
                    hits.append(f"{rel}:{lineno}: direct concrete NotFound status constructor")
    return hits


def live_already_exists_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    for path in live_rust_source_paths(root):
        rel = path.relative_to(root).as_posix()
        for lineno, line in enumerate(read(path).splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("//"):
                continue
            if any(pattern in line for pattern in DIRECT_ALREADY_EXISTS_PATTERNS):
                hits.append(f"{rel}:{lineno}: direct already_exists constructor")
            for pattern in DIRECT_ALREADY_EXISTS_REGEXES:
                if pattern.search(line):
                    hits.append(f"{rel}:{lineno}: direct concrete AlreadyExists status constructor")
    return hits


def live_retry_detail_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    for path in live_rust_source_paths(root):
        rel = path.relative_to(root).as_posix()
        for lineno, line in enumerate(read(path).splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("//"):
                continue
            for constructor in DIRECT_RETRY_DETAIL_CONSTRUCTORS:
                if constructor in line:
                    hits.append(f"{rel}:{lineno}: direct {constructor} constructor")
            for pattern, constructor in DIRECT_RETRY_DETAIL_REGEXES:
                if pattern.search(line):
                    hits.append(f"{rel}:{lineno}: direct {constructor} constructor")
    return hits


def live_unimplemented_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    for path in live_rust_source_paths(root):
        rel = path.relative_to(root).as_posix()
        for lineno, line in enumerate(read(path).splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("//") or stripped.startswith("//!"):
                continue
            if any(pattern in line for pattern in DIRECT_UNIMPLEMENTED_PATTERNS):
                hits.append(f"{rel}:{lineno}: direct unimplemented status/path")
            for pattern in DIRECT_UNIMPLEMENTED_REGEXES:
                if pattern.search(line):
                    hits.append(f"{rel}:{lineno}: direct concrete Unimplemented status constructor")
    return hits


def live_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    for path in live_rust_source_paths(root):
        rel = path.relative_to(root).as_posix()
        for lineno, line in enumerate(read(path).splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("//") or stripped.startswith("//!"):
                continue
            for pattern in DIRECT_INTERNAL_PATTERNS:
                if pattern in line:
                    hits.append(f"{rel}:{lineno}: direct internal constructor")
            for pattern in DIRECT_INTERNAL_REGEXES:
                if pattern.search(line):
                    hits.append(f"{rel}:{lineno}: direct concrete Internal status constructor")
    return hits


def authn_lookup_not_found_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    for rel in AUTHN_LOOKUP_NOT_FOUND_PATHS:
        path = root / rel
        for lineno, line in enumerate(read(path).splitlines(), 1):
            stripped = line.lstrip()
            if stripped.startswith("//") or stripped.startswith("//!"):
                continue
            for pattern in AUTHN_DIRECT_NOT_FOUND_PATTERNS:
                if pattern in line:
                    hits.append(f"{rel}:{lineno}: direct Authn lookup not_found constructor")
    return hits


def workflow_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / WORKFLOW_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{WORKFLOW_INTERNAL_STATUS_PATH}:{lineno}: direct WorkflowService internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{WORKFLOW_INTERNAL_STATUS_PATH}:{lineno}: direct WorkflowService concrete Internal status constructor")
    return hits


def analytics_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / ANALYTICS_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{ANALYTICS_INTERNAL_STATUS_PATH}:{lineno}: direct AnalyticsService internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{ANALYTICS_INTERNAL_STATUS_PATH}:{lineno}: direct AnalyticsService concrete Internal status constructor")
    return hits


def backup_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / BACKUP_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{BACKUP_INTERNAL_STATUS_PATH}:{lineno}: direct BackupService internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{BACKUP_INTERNAL_STATUS_PATH}:{lineno}: direct BackupService concrete Internal status constructor")
    return hits


def metering_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / METERING_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{METERING_INTERNAL_STATUS_PATH}:{lineno}: direct MeteringService internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{METERING_INTERNAL_STATUS_PATH}:{lineno}: direct MeteringService concrete Internal status constructor")
    return hits


def scheduler_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / SCHEDULER_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{SCHEDULER_INTERNAL_STATUS_PATH}:{lineno}: direct SchedulerService internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{SCHEDULER_INTERNAL_STATUS_PATH}:{lineno}: direct SchedulerService concrete Internal status constructor")
    return hits


def sqlite_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / SQLITE_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{SQLITE_INTERNAL_STATUS_PATH}:{lineno}: direct SQLite executor internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{SQLITE_INTERNAL_STATUS_PATH}:{lineno}: direct SQLite executor concrete Internal status constructor")
    return hits


def storage_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / STORAGE_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{STORAGE_INTERNAL_STATUS_PATH}:{lineno}: direct StorageService internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{STORAGE_INTERNAL_STATUS_PATH}:{lineno}: direct StorageService concrete Internal status constructor")
    return hits


def vault_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / VAULT_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{VAULT_INTERNAL_STATUS_PATH}:{lineno}: direct VaultService internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{VAULT_INTERNAL_STATUS_PATH}:{lineno}: direct VaultService concrete Internal status constructor")
    return hits


def notification_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / NOTIFICATION_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{NOTIFICATION_INTERNAL_STATUS_PATH}:{lineno}: direct NotificationService internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{NOTIFICATION_INTERNAL_STATUS_PATH}:{lineno}: direct NotificationService concrete Internal status constructor")
    return hits


def memcached_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / MEMCACHED_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{MEMCACHED_INTERNAL_STATUS_PATH}:{lineno}: direct Memcached executor internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{MEMCACHED_INTERNAL_STATUS_PATH}:{lineno}: direct Memcached executor concrete Internal status constructor")
    return hits


def mssql_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / MSSQL_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{MSSQL_INTERNAL_STATUS_PATH}:{lineno}: direct SQL Server executor internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{MSSQL_INTERNAL_STATUS_PATH}:{lineno}: direct SQL Server executor concrete Internal status constructor")
    return hits


def mongodb_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / MONGODB_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{MONGODB_INTERNAL_STATUS_PATH}:{lineno}: direct MongoDB executor internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{MONGODB_INTERNAL_STATUS_PATH}:{lineno}: direct MongoDB executor concrete Internal status constructor")
    return hits


def mysql_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / MYSQL_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{MYSQL_INTERNAL_STATUS_PATH}:{lineno}: direct MySQL executor internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{MYSQL_INTERNAL_STATUS_PATH}:{lineno}: direct MySQL executor concrete Internal status constructor")
    return hits


def neo4j_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / NEO4J_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{NEO4J_INTERNAL_STATUS_PATH}:{lineno}: direct Neo4j executor internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{NEO4J_INTERNAL_STATUS_PATH}:{lineno}: direct Neo4j executor concrete Internal status constructor")
    return hits


def elasticsearch_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / ELASTICSEARCH_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{ELASTICSEARCH_INTERNAL_STATUS_PATH}:{lineno}: direct Elasticsearch executor internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{ELASTICSEARCH_INTERNAL_STATUS_PATH}:{lineno}: direct Elasticsearch executor concrete Internal status constructor")
    return hits


def postgres_executor_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / POSTGRES_EXECUTOR_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{POSTGRES_EXECUTOR_INTERNAL_STATUS_PATH}:{lineno}: direct Postgres executor internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{POSTGRES_EXECUTOR_INTERNAL_STATUS_PATH}:{lineno}: direct Postgres executor concrete Internal status constructor")
    return hits


def postgres_helpers_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / POSTGRES_HELPERS_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{POSTGRES_HELPERS_INTERNAL_STATUS_PATH}:{lineno}: direct Postgres helper internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{POSTGRES_HELPERS_INTERNAL_STATUS_PATH}:{lineno}: direct Postgres helper concrete Internal status constructor")
    return hits


def probe_dispatch_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / PROBE_DISPATCH_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{PROBE_DISPATCH_INTERNAL_STATUS_PATH}:{lineno}: direct probe dispatch internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{PROBE_DISPATCH_INTERNAL_STATUS_PATH}:{lineno}: direct probe dispatch concrete Internal status constructor")
    return hits


def qdrant_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / QDRANT_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{QDRANT_INTERNAL_STATUS_PATH}:{lineno}: direct Qdrant executor internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{QDRANT_INTERNAL_STATUS_PATH}:{lineno}: direct Qdrant executor concrete Internal status constructor")
    return hits


def cassandra_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / CASSANDRA_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{CASSANDRA_INTERNAL_STATUS_PATH}:{lineno}: direct Cassandra executor internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{CASSANDRA_INTERNAL_STATUS_PATH}:{lineno}: direct Cassandra executor concrete Internal status constructor")
    return hits


def clickhouse_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / CLICKHOUSE_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{CLICKHOUSE_INTERNAL_STATUS_PATH}:{lineno}: direct ClickHouse executor internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{CLICKHOUSE_INTERNAL_STATUS_PATH}:{lineno}: direct ClickHouse executor concrete Internal status constructor")
    return hits


def pinecone_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / PINECONE_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{PINECONE_INTERNAL_STATUS_PATH}:{lineno}: direct Pinecone executor internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{PINECONE_INTERNAL_STATUS_PATH}:{lineno}: direct Pinecone executor concrete Internal status constructor")
    return hits


def saga_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / SAGA_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{SAGA_INTERNAL_STATUS_PATH}:{lineno}: direct saga internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{SAGA_INTERNAL_STATUS_PATH}:{lineno}: direct saga concrete Internal status constructor")
    return hits


def s3_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / S3_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{S3_INTERNAL_STATUS_PATH}:{lineno}: direct S3 executor internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{S3_INTERNAL_STATUS_PATH}:{lineno}: direct S3 executor concrete Internal status constructor")
    return hits


def tenant_purge_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / TENANT_PURGE_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{TENANT_PURGE_INTERNAL_STATUS_PATH}:{lineno}: direct tenant purge internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{TENANT_PURGE_INTERNAL_STATUS_PATH}:{lineno}: direct tenant purge concrete Internal status constructor")
    return hits


def tx_object_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / TX_OBJECT_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{TX_OBJECT_INTERNAL_STATUS_PATH}:{lineno}: direct transaction object internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{TX_OBJECT_INTERNAL_STATUS_PATH}:{lineno}: direct transaction object concrete Internal status constructor")
    return hits


def system_catalog_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / SYSTEM_CATALOG_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{SYSTEM_CATALOG_INTERNAL_STATUS_PATH}:{lineno}: direct system catalog internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{SYSTEM_CATALOG_INTERNAL_STATUS_PATH}:{lineno}: direct system catalog concrete Internal status constructor")
    return hits


def core_mod_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / CORE_MOD_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{CORE_MOD_INTERNAL_STATUS_PATH}:{lineno}: direct core runtime internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{CORE_MOD_INTERNAL_STATUS_PATH}:{lineno}: direct core runtime concrete Internal status constructor")
    return hits


def core_native_store_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / CORE_NATIVE_STORE_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{CORE_NATIVE_STORE_INTERNAL_STATUS_PATH}:{lineno}: direct core native store internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{CORE_NATIVE_STORE_INTERNAL_STATUS_PATH}:{lineno}: direct core native store concrete Internal status constructor")
    return hits


def weaviate_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / WEAVIATE_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{WEAVIATE_INTERNAL_STATUS_PATH}:{lineno}: direct Weaviate executor internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{WEAVIATE_INTERNAL_STATUS_PATH}:{lineno}: direct Weaviate executor concrete Internal status constructor")
    return hits


def catalog_handlers_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / CATALOG_HANDLERS_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{CATALOG_HANDLERS_INTERNAL_STATUS_PATH}:{lineno}: direct catalog handler internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{CATALOG_HANDLERS_INTERNAL_STATUS_PATH}:{lineno}: direct catalog handler concrete Internal status constructor")
    return hits


def catalog_admin_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / CATALOG_ADMIN_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{CATALOG_ADMIN_INTERNAL_STATUS_PATH}:{lineno}: direct catalog admin internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{CATALOG_ADMIN_INTERNAL_STATUS_PATH}:{lineno}: direct catalog admin concrete Internal status constructor")
    return hits


def catalog_sql_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / CATALOG_SQL_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{CATALOG_SQL_INTERNAL_STATUS_PATH}:{lineno}: direct catalog SQL internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{CATALOG_SQL_INTERNAL_STATUS_PATH}:{lineno}: direct catalog SQL concrete Internal status constructor")
    return hits


def setup_data_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / SETUP_DATA_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{SETUP_DATA_INTERNAL_STATUS_PATH}:{lineno}: direct setup-data internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{SETUP_DATA_INTERNAL_STATUS_PATH}:{lineno}: direct setup-data concrete Internal status constructor")
    return hits


def admin_handlers_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / ADMIN_HANDLERS_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{ADMIN_HANDLERS_INTERNAL_STATUS_PATH}:{lineno}: direct admin handler internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{ADMIN_HANDLERS_INTERNAL_STATUS_PATH}:{lineno}: direct admin handler concrete Internal status constructor")
    return hits


def authz_audit_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / AUTHZ_AUDIT_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{AUTHZ_AUDIT_INTERNAL_STATUS_PATH}:{lineno}: direct authz audit internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{AUTHZ_AUDIT_INTERNAL_STATUS_PATH}:{lineno}: direct authz audit concrete Internal status constructor")
    return hits


def authz_tuples_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / AUTHZ_TUPLES_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{AUTHZ_TUPLES_INTERNAL_STATUS_PATH}:{lineno}: direct authz tuple internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{AUTHZ_TUPLES_INTERNAL_STATUS_PATH}:{lineno}: direct authz tuple concrete Internal status constructor")
    return hits


def authz_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / AUTHZ_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{AUTHZ_INTERNAL_STATUS_PATH}:{lineno}: direct authz service internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{AUTHZ_INTERNAL_STATUS_PATH}:{lineno}: direct authz service concrete Internal status constructor")
    return hits


def authz_governance_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / AUTHZ_GOVERNANCE_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{AUTHZ_GOVERNANCE_INTERNAL_STATUS_PATH}:{lineno}: direct authz governance internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{AUTHZ_GOVERNANCE_INTERNAL_STATUS_PATH}:{lineno}: direct authz governance concrete Internal status constructor")
    return hits


def authz_governance_drafts_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / AUTHZ_GOVERNANCE_DRAFTS_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{AUTHZ_GOVERNANCE_DRAFTS_INTERNAL_STATUS_PATH}:{lineno}: direct authz governance draft internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{AUTHZ_GOVERNANCE_DRAFTS_INTERNAL_STATUS_PATH}:{lineno}: direct authz governance draft concrete Internal status constructor")
    return hits


def authz_governance_activate_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / AUTHZ_GOVERNANCE_ACTIVATE_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{AUTHZ_GOVERNANCE_ACTIVATE_INTERNAL_STATUS_PATH}:{lineno}: direct authz governance activation internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{AUTHZ_GOVERNANCE_ACTIVATE_INTERNAL_STATUS_PATH}:{lineno}: direct authz governance activation concrete Internal status constructor")
    return hits


def authz_governance_sim_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / AUTHZ_GOVERNANCE_SIM_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{AUTHZ_GOVERNANCE_SIM_INTERNAL_STATUS_PATH}:{lineno}: direct authz governance simulation internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{AUTHZ_GOVERNANCE_SIM_INTERNAL_STATUS_PATH}:{lineno}: direct authz governance simulation concrete Internal status constructor")
    return hits


def authz_governance_store_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / AUTHZ_GOVERNANCE_STORE_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{AUTHZ_GOVERNANCE_STORE_INTERNAL_STATUS_PATH}:{lineno}: direct authz governance store internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{AUTHZ_GOVERNANCE_STORE_INTERNAL_STATUS_PATH}:{lineno}: direct authz governance store concrete Internal status constructor")
    return hits


def control_plane_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / CONTROL_PLANE_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{CONTROL_PLANE_INTERNAL_STATUS_PATH}:{lineno}: direct control-plane internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{CONTROL_PLANE_INTERNAL_STATUS_PATH}:{lineno}: direct control-plane concrete Internal status constructor")
    return hits


def control_plane_store_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / CONTROL_PLANE_STORE_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{CONTROL_PLANE_STORE_INTERNAL_STATUS_PATH}:{lineno}: direct control-plane store internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{CONTROL_PLANE_STORE_INTERNAL_STATUS_PATH}:{lineno}: direct control-plane store concrete Internal status constructor")
    return hits


def control_plane_sourcing_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / CONTROL_PLANE_SOURCING_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{CONTROL_PLANE_SOURCING_INTERNAL_STATUS_PATH}:{lineno}: direct control-plane sourcing internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{CONTROL_PLANE_SOURCING_INTERNAL_STATUS_PATH}:{lineno}: direct control-plane sourcing concrete Internal status constructor")
    return hits


def idp_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / IDP_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{IDP_INTERNAL_STATUS_PATH}:{lineno}: direct IdentityProviderService internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{IDP_INTERNAL_STATUS_PATH}:{lineno}: direct IdentityProviderService concrete Internal status constructor")
    return hits


def idp_store_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / IDP_STORE_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{IDP_STORE_INTERNAL_STATUS_PATH}:{lineno}: direct IdentityProvider store internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{IDP_STORE_INTERNAL_STATUS_PATH}:{lineno}: direct IdentityProvider store concrete Internal status constructor")
    return hits


def authn_signing_keys_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / AUTHN_SIGNING_KEYS_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{AUTHN_SIGNING_KEYS_INTERNAL_STATUS_PATH}:{lineno}: direct authn signing-key internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{AUTHN_SIGNING_KEYS_INTERNAL_STATUS_PATH}:{lineno}: direct authn signing-key concrete Internal status constructor")
    return hits


def authn_token_family_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / AUTHN_TOKEN_FAMILY_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{AUTHN_TOKEN_FAMILY_INTERNAL_STATUS_PATH}:{lineno}: direct authn token-family internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{AUTHN_TOKEN_FAMILY_INTERNAL_STATUS_PATH}:{lineno}: direct authn token-family concrete Internal status constructor")
    return hits


def authn_core_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / AUTHN_CORE_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{AUTHN_CORE_INTERNAL_STATUS_PATH}:{lineno}: direct authn core internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{AUTHN_CORE_INTERNAL_STATUS_PATH}:{lineno}: direct authn core concrete Internal status constructor")
    return hits


def authn_lifecycle_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / AUTHN_LIFECYCLE_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{AUTHN_LIFECYCLE_INTERNAL_STATUS_PATH}:{lineno}: direct authn lifecycle internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{AUTHN_LIFECYCLE_INTERNAL_STATUS_PATH}:{lineno}: direct authn lifecycle concrete Internal status constructor")
    return hits


def authn_login_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / AUTHN_LOGIN_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{AUTHN_LOGIN_INTERNAL_STATUS_PATH}:{lineno}: direct authn login internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{AUTHN_LOGIN_INTERNAL_STATUS_PATH}:{lineno}: direct authn login concrete Internal status constructor")
    return hits


def authn_mfa_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / AUTHN_MFA_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{AUTHN_MFA_INTERNAL_STATUS_PATH}:{lineno}: direct authn MFA internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{AUTHN_MFA_INTERNAL_STATUS_PATH}:{lineno}: direct authn MFA concrete Internal status constructor")
    return hits


def authn_mod_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / AUTHN_MOD_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{AUTHN_MOD_INTERNAL_STATUS_PATH}:{lineno}: direct authn main internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{AUTHN_MOD_INTERNAL_STATUS_PATH}:{lineno}: direct authn main concrete Internal status constructor")
    return hits


def authn_sessions_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / AUTHN_SESSIONS_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{AUTHN_SESSIONS_INTERNAL_STATUS_PATH}:{lineno}: direct authn session internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{AUTHN_SESSIONS_INTERNAL_STATUS_PATH}:{lineno}: direct authn session concrete Internal status constructor")
    return hits


def apikey_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / APIKEY_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{APIKEY_INTERNAL_STATUS_PATH}:{lineno}: direct ApiKeyService internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{APIKEY_INTERNAL_STATUS_PATH}:{lineno}: direct ApiKeyService concrete Internal status constructor")
    return hits


def asset_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / ASSET_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{ASSET_INTERNAL_STATUS_PATH}:{lineno}: direct AssetService internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{ASSET_INTERNAL_STATUS_PATH}:{lineno}: direct AssetService concrete Internal status constructor")
    return hits


def data_handlers_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / DATA_HANDLERS_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{DATA_HANDLERS_INTERNAL_STATUS_PATH}:{lineno}: direct data handler internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{DATA_HANDLERS_INTERNAL_STATUS_PATH}:{lineno}: direct data handler concrete Internal status constructor")
    return hits


def native_entity_store_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / NATIVE_ENTITY_STORE_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{NATIVE_ENTITY_STORE_INTERNAL_STATUS_PATH}:{lineno}: direct native entity store internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{NATIVE_ENTITY_STORE_INTERNAL_STATUS_PATH}:{lineno}: direct native entity store concrete Internal status constructor")
    return hits


def tenant_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / TENANT_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{TENANT_INTERNAL_STATUS_PATH}:{lineno}: direct TenantService internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{TENANT_INTERNAL_STATUS_PATH}:{lineno}: direct TenantService concrete Internal status constructor")
    return hits


def webhook_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / WEBHOOK_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{WEBHOOK_INTERNAL_STATUS_PATH}:{lineno}: direct WebhookService internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{WEBHOOK_INTERNAL_STATUS_PATH}:{lineno}: direct WebhookService concrete Internal status constructor")
    return hits


def webrtc_raw_internal_constructor_hits(root: Path) -> list[str]:
    hits: list[str] = []
    path = root / WEBRTC_INTERNAL_STATUS_PATH
    for lineno, line in enumerate(read(path).splitlines(), 1):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("//!"):
            continue
        for pattern in DIRECT_INTERNAL_PATTERNS:
            if pattern in line:
                hits.append(f"{WEBRTC_INTERNAL_STATUS_PATH}:{lineno}: direct WebrtcService internal constructor")
        for pattern in DIRECT_INTERNAL_REGEXES:
            if pattern.search(line):
                hits.append(f"{WEBRTC_INTERNAL_STATUS_PATH}:{lineno}: direct WebrtcService concrete Internal status constructor")
    return hits


def check_root(root: Path) -> list[str]:
    global _READ_CACHE
    previous_read_cache = _READ_CACHE
    _READ_CACHE = {}
    try:
        return _check_root_cached(root)
    finally:
        _READ_CACHE = previous_read_cache


def whitespace_insensitive(text: str) -> str:
    return re.sub(r"\s+", "", text)


def loose_rust_call(text: str) -> str:
    return whitespace_insensitive(text).replace(",", "")


def token_present(token: str, text: str) -> bool:
    if token in text:
        return True
    if token in LEGACY_INLINE_DETAIL_DECODE_TOKENS:
        return "decode_error_detail_from_raw(" in text
    if "(" in token or "\n" in token:
        return whitespace_insensitive(token) in whitespace_insensitive(text) or loose_rust_call(token) in loose_rust_call(text)
    return False


def _check_root_cached(root: Path) -> list[str]:
    failures: list[str] = []
    for check in TOKEN_CHECKS:
        path = root / check.path
        text = read(path)
        if not text:
            failures.append(f"{check.label}: missing {check.path}")
            continue
        for token in check.tokens:
            if not token_present(token, text):
                failures.append(f"{check.label}: missing token {token!r} in {check.path}")

    api_rules = read(root / "docs/api-rules.md")
    if "### Stable String Reason Registry" not in api_rules:
        failures.append("docs/api-rules.md: missing Stable String Reason Registry")
    if "`error-reason` gRPC metadata trailer" not in api_rules:
        failures.append("docs/api-rules.md: missing error-reason public surface wording")
    for source, reasons in ERROR_REASON_REGISTRY:
        text = read(root / source)
        if not text:
            failures.append(f"stable reason registry: missing {source}")
            continue
        for reason in reasons:
            const_pattern = re.compile(
                rf"const\s+{re.escape(reason)}\s*:\s*&str\s*=\s*\"{re.escape(reason)}\""
            )
            if not const_pattern.search(text):
                failures.append(f"stable reason registry: missing source const {reason} in {source}")
            if f"| `{reason}` |" not in api_rules:
                failures.append(f"stable reason registry: docs/api-rules.md missing {reason}")

    for token in DOC_FORBIDDEN:
        if token in api_rules:
            failures.append(f"docs/api-rules.md: stale google.rpc detail wording remains: {token!r}")
    for hit in live_invalid_argument_constructor_hits(root):
        failures.append(f"source validation posture: {hit}")
    for hit in live_failed_precondition_constructor_hits(root):
        failures.append(f"source failed-precondition posture: {hit}")
    for hit in live_permission_denied_constructor_hits(root):
        failures.append(f"source policy-detail posture: {hit}")
    for hit in live_not_found_constructor_hits(root):
        failures.append(f"source schema-detail posture: {hit}")
    for hit in live_already_exists_constructor_hits(root):
        failures.append(f"source schema-detail posture: {hit}")
    for hit in live_retry_detail_constructor_hits(root):
        failures.append(f"retry/quota detail posture: {hit}")
    for hit in live_unimplemented_constructor_hits(root):
        failures.append(f"source unimplemented posture: {hit}")
    for hit in live_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in authn_lookup_not_found_constructor_hits(root):
        failures.append(f"source schema-detail posture: {hit}")
    for hit in authn_signing_keys_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in authn_token_family_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in authn_core_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in authn_lifecycle_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in authn_login_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in authn_mfa_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in authn_mod_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in authn_sessions_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in admin_handlers_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in analytics_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in apikey_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in asset_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in authz_audit_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in authz_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in authz_governance_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in authz_governance_activate_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in authz_governance_drafts_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in authz_governance_sim_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in authz_governance_store_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in authz_tuples_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in backup_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in cassandra_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in catalog_admin_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in catalog_handlers_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in catalog_sql_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in clickhouse_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in control_plane_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in control_plane_sourcing_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in control_plane_store_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in core_mod_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in core_native_store_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in data_handlers_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in elasticsearch_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in idp_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in idp_store_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in memcached_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in metering_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in mongodb_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in mysql_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in mssql_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in native_entity_store_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in neo4j_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in scheduler_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in setup_data_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in sqlite_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in storage_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in notification_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in pinecone_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in postgres_executor_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in postgres_helpers_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in probe_dispatch_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in qdrant_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in saga_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in s3_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in system_catalog_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in tenant_purge_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in tx_object_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in vault_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in tenant_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in webhook_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in weaviate_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in webrtc_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    for hit in workflow_raw_internal_constructor_hits(root):
        failures.append(f"source internal-detail posture: {hit}")
    executor_utils = read(root / "src/runtime/executor_utils.rs")
    if "pub(crate) fn status_with_error_detail(" in executor_utils:
        failures.append(
            "runtime ErrorDetail builder must stay private to executor_utils.rs helper wrappers"
        )
    return failures


def write_fixture(root: Path) -> None:
    fixture_text_by_path: dict[Path, list[str]] = {}
    for check in TOKEN_CHECKS:
        path = root / check.path
        fixture_text_by_path.setdefault(path, []).extend(check.tokens)
    api_rules = root / "docs/api-rules.md"
    reason_rows = ["### Stable String Reason Registry", "`error-reason` gRPC metadata trailer"]
    for _, reasons in ERROR_REASON_REGISTRY:
        for reason in reasons:
            reason_rows.append(f"| `{reason}` | TestService | `error-reason` trailer |")
    fixture_text_by_path.setdefault(api_rules, []).extend(reason_rows)
    for source, reasons in ERROR_REASON_REGISTRY:
        path = root / source
        consts = [f'const {reason}: &str = "{reason}";' for reason in reasons]
        fixture_text_by_path.setdefault(path, []).extend(consts)
    for path, lines in fixture_text_by_path.items():
        # A suffix-less TokenCheck path names a service DIRECTORY (read as the
        # concatenation of its `.rs` files); the fixture writes the tokens into a
        # `mod.rs` under it so the good-fixture selftest still passes.
        target = path if path.suffix else path / "mod.rs"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_selftest() -> None:
    global _READ_CACHE
    with tempfile.TemporaryDirectory(prefix="udb-error-detail-posture-") as tmp:
        root = Path(tmp)
        write_fixture(root)
        failures = check_root(root)
        assert not failures, failures

        original_check_root_cached = _check_root_cached
        sentinel_cache: dict[Path, str] = {root / "sentinel": "cached"}
        _READ_CACHE = sentinel_cache
        try:
            def raising_check_root_cached(_: Path) -> list[str]:
                raise RuntimeError("cache restore fixture")

            globals()["_check_root_cached"] = raising_check_root_cached
            try:
                check_root(root)
            except RuntimeError as error:
                assert "cache restore fixture" in str(error), error
            else:
                raise AssertionError("check_root cache-restore fixture did not raise")
            assert _READ_CACHE is sentinel_cache, "check_root did not restore the previous read cache after an exception"
        finally:
            globals()["_check_root_cached"] = original_check_root_cached
            _READ_CACHE = None

        stale = root / "docs/api-rules.md"
        stale.write_text(
            read(stale) + "\nThe error body is `ApiError` mapped from `google.rpc.Status`.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("google.rpc" in failure for failure in failures), failures

        write_fixture(root)
        target = root / "src/runtime/executor_utils.rs"
        target.write_text(read(target).replace("pub(crate) fn retryable_status(", ""), encoding="utf-8")
        failures = check_root(root)
        assert any("retryable_status" in failure for failure in failures), failures

        write_fixture(root)
        target = root / "src/runtime/executor_utils.rs"
        target.write_text(read(target).replace("fn status_with_error_detail(", "pub(crate) fn status_with_error_detail("), encoding="utf-8")
        failures = check_root(root)
        assert any("must stay private" in failure for failure in failures), failures

        write_fixture(root)
        authn_lookup = root / "src/runtime/service/auth_service/authn/login.rs"
        authn_lookup.write_text(
            read(authn_lookup)
            + '\nfn bad() { tonic::Status::not_found("user not found"); }\n'
            + '\nfn also_bad() { Status::not_found("otp not found"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Authn lookup not_found constructor" in failure for failure in failures), failures

        write_fixture(root)
        admin_handlers_internal = root / ADMIN_HANDLERS_INTERNAL_STATUS_PATH
        admin_handlers_internal_good = read(admin_handlers_internal)
        admin_handlers_internal.write_text(
            admin_handlers_internal_good + '\nfn bad() { tonic::Status::internal("admin handler failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct admin handler internal constructor" in failure for failure in failures), failures
        admin_handlers_internal.write_text(
            admin_handlers_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "admin handler failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct admin handler concrete Internal status constructor" in failure for failure in failures), failures
        admin_handlers_internal.write_text(
            admin_handlers_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("admin handler" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        authz_audit_internal = root / AUTHZ_AUDIT_INTERNAL_STATUS_PATH
        authz_audit_internal_good = read(authz_audit_internal)
        authz_audit_internal.write_text(
            authz_audit_internal_good + '\nfn bad() { tonic::Status::internal("authz audit failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authz audit internal constructor" in failure for failure in failures), failures
        authz_audit_internal.write_text(
            authz_audit_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "authz audit failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authz audit concrete Internal status constructor" in failure for failure in failures), failures
        authz_audit_internal.write_text(
            authz_audit_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("authz audit" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        authz_tuple_internal = root / AUTHZ_TUPLES_INTERNAL_STATUS_PATH
        authz_tuple_internal_good = read(authz_tuple_internal)
        authz_tuple_internal.write_text(
            authz_tuple_internal_good + '\nfn bad() { tonic::Status::internal("authz tuple failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authz tuple internal constructor" in failure for failure in failures), failures
        authz_tuple_internal.write_text(
            authz_tuple_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "authz tuple failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authz tuple concrete Internal status constructor" in failure for failure in failures), failures
        authz_tuple_internal.write_text(
            authz_tuple_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("authz tuple" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        authz_internal = root / AUTHZ_INTERNAL_STATUS_PATH
        authz_internal_good = read(authz_internal)
        authz_internal.write_text(
            authz_internal_good + '\nfn bad() { tonic::Status::internal("authz service failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authz service internal constructor" in failure for failure in failures), failures
        authz_internal.write_text(
            authz_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "authz service failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authz service concrete Internal status constructor" in failure for failure in failures), failures
        authz_internal.write_text(
            authz_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("authz service" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        authz_governance_internal = root / AUTHZ_GOVERNANCE_INTERNAL_STATUS_PATH
        authz_governance_internal_good = read(authz_governance_internal)
        authz_governance_internal.write_text(
            authz_governance_internal_good + '\nfn bad() { tonic::Status::internal("authz governance failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authz governance internal constructor" in failure for failure in failures), failures
        authz_governance_internal.write_text(
            authz_governance_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "authz governance failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authz governance concrete Internal status constructor" in failure for failure in failures), failures
        authz_governance_internal.write_text(
            authz_governance_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("authz governance" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        authz_governance_activate_internal = root / AUTHZ_GOVERNANCE_ACTIVATE_INTERNAL_STATUS_PATH
        authz_governance_activate_internal_good = read(authz_governance_activate_internal)
        authz_governance_activate_internal.write_text(
            authz_governance_activate_internal_good
            + '\nfn bad() { tonic::Status::internal("authz governance activation failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authz governance activation internal constructor" in failure for failure in failures), failures
        authz_governance_activate_internal.write_text(
            authz_governance_activate_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "authz governance activation failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authz governance activation concrete Internal status constructor" in failure for failure in failures), failures
        authz_governance_activate_internal.write_text(
            authz_governance_activate_internal_good
            + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("authz governance activation" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        authz_governance_drafts_internal = root / AUTHZ_GOVERNANCE_DRAFTS_INTERNAL_STATUS_PATH
        authz_governance_drafts_internal_good = read(authz_governance_drafts_internal)
        authz_governance_drafts_internal.write_text(
            authz_governance_drafts_internal_good
            + '\nfn bad() { tonic::Status::internal("authz governance draft failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authz governance draft internal constructor" in failure for failure in failures), failures
        authz_governance_drafts_internal.write_text(
            authz_governance_drafts_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "authz governance draft failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authz governance draft concrete Internal status constructor" in failure for failure in failures), failures
        authz_governance_drafts_internal.write_text(
            authz_governance_drafts_internal_good
            + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("authz governance draft" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        authz_governance_sim_internal = root / AUTHZ_GOVERNANCE_SIM_INTERNAL_STATUS_PATH
        authz_governance_sim_internal_good = read(authz_governance_sim_internal)
        authz_governance_sim_internal.write_text(
            authz_governance_sim_internal_good + '\nfn bad() { tonic::Status::internal("authz governance sim failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authz governance simulation internal constructor" in failure for failure in failures), failures
        authz_governance_sim_internal.write_text(
            authz_governance_sim_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "authz governance sim failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authz governance simulation concrete Internal status constructor" in failure for failure in failures), failures
        authz_governance_sim_internal.write_text(
            authz_governance_sim_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("authz governance simulation" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        authz_governance_store_internal = root / AUTHZ_GOVERNANCE_STORE_INTERNAL_STATUS_PATH
        authz_governance_store_internal_good = read(authz_governance_store_internal)
        authz_governance_store_internal.write_text(
            authz_governance_store_internal_good
            + '\nfn bad() { tonic::Status::internal("authz governance store failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authz governance store internal constructor" in failure for failure in failures), failures
        authz_governance_store_internal.write_text(
            authz_governance_store_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "authz governance store failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authz governance store concrete Internal status constructor" in failure for failure in failures), failures
        authz_governance_store_internal.write_text(
            authz_governance_store_internal_good
            + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("authz governance store" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        control_plane_internal = root / CONTROL_PLANE_INTERNAL_STATUS_PATH
        control_plane_internal_good = read(control_plane_internal)
        control_plane_internal.write_text(
            control_plane_internal_good + '\nfn bad() { tonic::Status::internal("control-plane stream failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct control-plane internal constructor" in failure for failure in failures), failures
        control_plane_internal.write_text(
            control_plane_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "control-plane stream failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct control-plane concrete Internal status constructor" in failure for failure in failures), failures
        control_plane_internal.write_text(
            control_plane_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("control-plane" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        control_plane_store_internal = root / CONTROL_PLANE_STORE_INTERNAL_STATUS_PATH
        control_plane_store_internal_good = read(control_plane_store_internal)
        control_plane_store_internal.write_text(
            control_plane_store_internal_good + '\nfn bad() { tonic::Status::internal("control-plane store failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct control-plane store internal constructor" in failure for failure in failures), failures
        control_plane_store_internal.write_text(
            control_plane_store_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "control-plane store failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct control-plane store concrete Internal status constructor" in failure for failure in failures), failures
        control_plane_store_internal.write_text(
            control_plane_store_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("control-plane store" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        control_plane_sourcing_internal = root / CONTROL_PLANE_SOURCING_INTERNAL_STATUS_PATH
        control_plane_sourcing_internal_good = read(control_plane_sourcing_internal)
        control_plane_sourcing_internal.write_text(
            control_plane_sourcing_internal_good + '\nfn bad() { tonic::Status::internal("control-plane sourcing failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct control-plane sourcing internal constructor" in failure for failure in failures), failures
        control_plane_sourcing_internal.write_text(
            control_plane_sourcing_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "control-plane sourcing failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct control-plane sourcing concrete Internal status constructor" in failure for failure in failures), failures
        control_plane_sourcing_internal.write_text(
            control_plane_sourcing_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("control-plane sourcing" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        idp_internal = root / IDP_INTERNAL_STATUS_PATH
        idp_internal_good = read(idp_internal)
        idp_internal.write_text(
            idp_internal_good + '\nfn bad() { tonic::Status::internal("idp SAML failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct IdentityProviderService internal constructor" in failure for failure in failures), failures
        idp_internal.write_text(
            idp_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "idp SAML failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct IdentityProviderService concrete Internal status constructor" in failure for failure in failures), failures
        idp_internal.write_text(
            idp_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("IdentityProviderService" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        idp_store_internal = root / IDP_STORE_INTERNAL_STATUS_PATH
        idp_store_internal_good = read(idp_store_internal)
        idp_store_internal.write_text(
            idp_store_internal_good + '\nfn bad() { tonic::Status::internal("idp store failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct IdentityProvider store internal constructor" in failure for failure in failures), failures
        idp_store_internal.write_text(
            idp_store_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "idp store failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct IdentityProvider store concrete Internal status constructor" in failure for failure in failures), failures
        idp_store_internal.write_text(
            idp_store_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("IdentityProvider store" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        authn_signing_keys_internal = root / AUTHN_SIGNING_KEYS_INTERNAL_STATUS_PATH
        authn_signing_keys_internal_good = read(authn_signing_keys_internal)
        authn_signing_keys_internal.write_text(
            authn_signing_keys_internal_good + '\nfn bad() { tonic::Status::internal("authn signing-key failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authn signing-key internal constructor" in failure for failure in failures), failures
        authn_signing_keys_internal.write_text(
            authn_signing_keys_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "authn signing-key failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authn signing-key concrete Internal status constructor" in failure for failure in failures), failures
        authn_signing_keys_internal.write_text(
            authn_signing_keys_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("authn signing-key" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        authn_token_family_internal = root / AUTHN_TOKEN_FAMILY_INTERNAL_STATUS_PATH
        authn_token_family_internal_good = read(authn_token_family_internal)
        authn_token_family_internal.write_text(
            authn_token_family_internal_good + '\nfn bad() { tonic::Status::internal("authn token-family failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authn token-family internal constructor" in failure for failure in failures), failures
        authn_token_family_internal.write_text(
            authn_token_family_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "authn token-family failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authn token-family concrete Internal status constructor" in failure for failure in failures), failures
        authn_token_family_internal.write_text(
            authn_token_family_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("authn token-family" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        authn_core_internal = root / AUTHN_CORE_INTERNAL_STATUS_PATH
        authn_core_internal_good = read(authn_core_internal)
        authn_core_internal.write_text(
            authn_core_internal_good + '\nfn bad() { tonic::Status::internal("authn core failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authn core internal constructor" in failure for failure in failures), failures
        authn_core_internal.write_text(
            authn_core_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "authn core failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authn core concrete Internal status constructor" in failure for failure in failures), failures
        authn_core_internal.write_text(
            authn_core_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("authn core" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        authn_lifecycle_internal = root / AUTHN_LIFECYCLE_INTERNAL_STATUS_PATH
        authn_lifecycle_internal_good = read(authn_lifecycle_internal)
        authn_lifecycle_internal.write_text(
            authn_lifecycle_internal_good + '\nfn bad() { tonic::Status::internal("authn lifecycle failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authn lifecycle internal constructor" in failure for failure in failures), failures
        authn_lifecycle_internal.write_text(
            authn_lifecycle_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "authn lifecycle failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authn lifecycle concrete Internal status constructor" in failure for failure in failures), failures
        authn_lifecycle_internal.write_text(
            authn_lifecycle_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("authn lifecycle" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        authn_login_internal = root / AUTHN_LOGIN_INTERNAL_STATUS_PATH
        authn_login_internal_good = read(authn_login_internal)
        authn_login_internal.write_text(
            authn_login_internal_good + '\nfn bad() { tonic::Status::internal("authn login failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authn login internal constructor" in failure for failure in failures), failures
        authn_login_internal.write_text(
            authn_login_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "authn login failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authn login concrete Internal status constructor" in failure for failure in failures), failures
        authn_login_internal.write_text(
            authn_login_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("authn login" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        authn_mfa_internal = root / AUTHN_MFA_INTERNAL_STATUS_PATH
        authn_mfa_internal_good = read(authn_mfa_internal)
        authn_mfa_internal.write_text(
            authn_mfa_internal_good + '\nfn bad() { tonic::Status::internal("authn MFA failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authn MFA internal constructor" in failure for failure in failures), failures
        authn_mfa_internal.write_text(
            authn_mfa_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "authn MFA failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authn MFA concrete Internal status constructor" in failure for failure in failures), failures
        authn_mfa_internal.write_text(
            authn_mfa_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("authn MFA" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        authn_mod_internal = root / AUTHN_MOD_INTERNAL_STATUS_PATH
        authn_mod_internal_good = read(authn_mod_internal)
        authn_mod_internal.write_text(
            authn_mod_internal_good + '\nfn bad() { tonic::Status::internal("authn main failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authn main internal constructor" in failure for failure in failures), failures
        authn_mod_internal.write_text(
            authn_mod_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "authn main failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authn main concrete Internal status constructor" in failure for failure in failures), failures
        authn_mod_internal.write_text(
            authn_mod_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("authn main" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        authn_sessions_internal = root / AUTHN_SESSIONS_INTERNAL_STATUS_PATH
        authn_sessions_internal_good = read(authn_sessions_internal)
        authn_sessions_internal.write_text(
            authn_sessions_internal_good + '\nfn bad() { tonic::Status::internal("authn session failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authn session internal constructor" in failure for failure in failures), failures
        authn_sessions_internal.write_text(
            authn_sessions_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "authn session failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct authn session concrete Internal status constructor" in failure for failure in failures), failures
        authn_sessions_internal.write_text(
            authn_sessions_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("authn session" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        apikey_internal = root / APIKEY_INTERNAL_STATUS_PATH
        apikey_internal_good = read(apikey_internal)
        apikey_internal.write_text(
            apikey_internal_good + '\nfn bad() { tonic::Status::internal("api key store failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct ApiKeyService internal constructor" in failure for failure in failures), failures
        apikey_internal.write_text(
            apikey_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "api key store failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct ApiKeyService concrete Internal status constructor" in failure for failure in failures), failures
        apikey_internal.write_text(
            apikey_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("ApiKeyService" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        asset_internal_dir = root / ASSET_INTERNAL_STATUS_PATH
        # See the metering note above: mutate a `.rs` file inside a service dir.
        asset_internal = (
            asset_internal_dir / "mod.rs"
            if asset_internal_dir.is_dir()
            else asset_internal_dir
        )
        asset_internal_good = read(asset_internal)
        asset_internal.write_text(
            asset_internal_good + '\nfn bad() { tonic::Status::internal("asset db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct AssetService internal constructor" in failure for failure in failures), failures
        asset_internal.write_text(
            asset_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "asset db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct AssetService concrete Internal status constructor" in failure for failure in failures), failures
        asset_internal.write_text(
            asset_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("AssetService" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        analytics_internal_dir = root / ANALYTICS_INTERNAL_STATUS_PATH
        # See the metering note above: mutate a `.rs` file inside a service dir.
        analytics_internal = (
            analytics_internal_dir / "mod.rs"
            if analytics_internal_dir.is_dir()
            else analytics_internal_dir
        )
        analytics_internal_good = read(analytics_internal)
        analytics_internal.write_text(
            analytics_internal_good + '\nfn bad() { tonic::Status::internal("analytics db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct AnalyticsService internal constructor" in failure for failure in failures), failures
        analytics_internal.write_text(
            analytics_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "analytics db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct AnalyticsService concrete Internal status constructor" in failure for failure in failures), failures
        analytics_internal.write_text(
            analytics_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("AnalyticsService" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        backup_internal_dir = root / BACKUP_INTERNAL_STATUS_PATH
        # See the metering note above: mutate a `.rs` file inside a service dir.
        backup_internal = (
            backup_internal_dir / "mod.rs"
            if backup_internal_dir.is_dir()
            else backup_internal_dir
        )
        backup_internal_good = read(backup_internal)
        backup_internal.write_text(
            backup_internal_good + '\nfn bad() { tonic::Status::internal("backup db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct BackupService internal constructor" in failure for failure in failures), failures
        backup_internal.write_text(
            backup_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "backup db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct BackupService concrete Internal status constructor" in failure for failure in failures), failures
        backup_internal.write_text(
            backup_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("BackupService" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        catalog_admin_internal = root / CATALOG_ADMIN_INTERNAL_STATUS_PATH
        catalog_admin_internal_good = read(catalog_admin_internal)
        catalog_admin_internal.write_text(
            catalog_admin_internal_good + '\nfn bad() { tonic::Status::internal("catalog admin failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct catalog admin internal constructor" in failure for failure in failures), failures
        catalog_admin_internal.write_text(
            catalog_admin_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "catalog admin failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct catalog admin concrete Internal status constructor" in failure for failure in failures), failures
        catalog_admin_internal.write_text(
            catalog_admin_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("catalog admin" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        catalog_handlers_internal = root / CATALOG_HANDLERS_INTERNAL_STATUS_PATH
        catalog_handlers_internal_good = read(catalog_handlers_internal)
        catalog_handlers_internal.write_text(
            catalog_handlers_internal_good + '\nfn bad() { tonic::Status::internal("catalog handler failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct catalog handler internal constructor" in failure for failure in failures), failures
        catalog_handlers_internal.write_text(
            catalog_handlers_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "catalog handler failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct catalog handler concrete Internal status constructor" in failure for failure in failures), failures
        catalog_handlers_internal.write_text(
            catalog_handlers_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("catalog handler" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        catalog_sql_internal = root / CATALOG_SQL_INTERNAL_STATUS_PATH
        catalog_sql_internal_good = read(catalog_sql_internal)
        catalog_sql_internal.write_text(
            catalog_sql_internal_good + '\nfn bad() { tonic::Status::internal("catalog SQL failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct catalog SQL internal constructor" in failure for failure in failures), failures
        catalog_sql_internal.write_text(
            catalog_sql_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "catalog SQL failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct catalog SQL concrete Internal status constructor" in failure for failure in failures), failures
        catalog_sql_internal.write_text(
            catalog_sql_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("catalog SQL" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        setup_data_internal = root / SETUP_DATA_INTERNAL_STATUS_PATH
        setup_data_internal_good = read(setup_data_internal)
        setup_data_internal.write_text(
            setup_data_internal_good + '\nfn bad() { tonic::Status::internal("setup-data failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct setup-data internal constructor" in failure for failure in failures), failures
        setup_data_internal.write_text(
            setup_data_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "setup-data failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct setup-data concrete Internal status constructor" in failure for failure in failures), failures
        setup_data_internal.write_text(
            setup_data_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("setup-data" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        data_handlers_internal = root / DATA_HANDLERS_INTERNAL_STATUS_PATH
        data_handlers_internal_good = read(data_handlers_internal)
        data_handlers_internal.write_text(
            data_handlers_internal_good + '\nfn bad() { tonic::Status::internal("data handler failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct data handler internal constructor" in failure for failure in failures), failures
        data_handlers_internal.write_text(
            data_handlers_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "data handler failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct data handler concrete Internal status constructor" in failure for failure in failures), failures
        data_handlers_internal.write_text(
            data_handlers_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("data handler" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        native_entity_store_internal = root / NATIVE_ENTITY_STORE_INTERNAL_STATUS_PATH
        native_entity_store_internal_good = read(native_entity_store_internal)
        native_entity_store_internal.write_text(
            native_entity_store_internal_good + '\nfn bad() { tonic::Status::internal("native entity store failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct native entity store internal constructor" in failure for failure in failures), failures
        native_entity_store_internal.write_text(
            native_entity_store_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "native entity store failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct native entity store concrete Internal status constructor" in failure for failure in failures), failures
        native_entity_store_internal.write_text(
            native_entity_store_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("native entity store" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        metering_internal_dir = root / METERING_INTERNAL_STATUS_PATH
        # The scan path may name a modularized service DIRECTORY; mutate a `.rs`
        # file inside it (write_text on a directory raises IsADirectoryError).
        metering_internal = (
            metering_internal_dir / "mod.rs"
            if metering_internal_dir.is_dir()
            else metering_internal_dir
        )
        metering_internal_good = read(metering_internal)
        metering_internal.write_text(
            metering_internal_good + '\nfn bad() { tonic::Status::internal("metering db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct MeteringService internal constructor" in failure for failure in failures), failures
        metering_internal.write_text(
            metering_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "metering db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct MeteringService concrete Internal status constructor" in failure for failure in failures), failures
        metering_internal.write_text(
            metering_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("MeteringService" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        scheduler_internal_dir = root / SCHEDULER_INTERNAL_STATUS_PATH
        # See the metering note above: mutate a `.rs` file inside a service dir.
        scheduler_internal = (
            scheduler_internal_dir / "mod.rs"
            if scheduler_internal_dir.is_dir()
            else scheduler_internal_dir
        )
        scheduler_internal_good = read(scheduler_internal)
        scheduler_internal.write_text(
            scheduler_internal_good + '\nfn bad() { tonic::Status::internal("scheduler db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct SchedulerService internal constructor" in failure for failure in failures), failures
        scheduler_internal.write_text(
            scheduler_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "scheduler db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct SchedulerService concrete Internal status constructor" in failure for failure in failures), failures
        scheduler_internal.write_text(
            scheduler_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("SchedulerService" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        sqlite_internal = root / SQLITE_INTERNAL_STATUS_PATH
        sqlite_internal_good = read(sqlite_internal)
        sqlite_internal.write_text(
            sqlite_internal_good + '\nfn bad() { tonic::Status::internal("sqlite failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct SQLite executor internal constructor" in failure for failure in failures), failures
        sqlite_internal.write_text(
            sqlite_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "sqlite failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct SQLite executor concrete Internal status constructor" in failure for failure in failures), failures
        sqlite_internal.write_text(
            sqlite_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("SQLite executor" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        storage_internal_dir = root / STORAGE_INTERNAL_STATUS_PATH
        # See the metering note above: mutate a `.rs` file inside a service dir.
        storage_internal = (
            storage_internal_dir / "mod.rs"
            if storage_internal_dir.is_dir()
            else storage_internal_dir
        )
        storage_internal_good = read(storage_internal)
        storage_internal.write_text(
            storage_internal_good + '\nfn bad() { tonic::Status::internal("storage db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct StorageService internal constructor" in failure for failure in failures), failures
        storage_internal.write_text(
            storage_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "storage db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct StorageService concrete Internal status constructor" in failure for failure in failures), failures
        storage_internal.write_text(
            storage_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("StorageService" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        notification_internal_dir = root / NOTIFICATION_INTERNAL_STATUS_PATH
        # See the metering note above: mutate a `.rs` file inside a service dir.
        notification_internal = (
            notification_internal_dir / "mod.rs"
            if notification_internal_dir.is_dir()
            else notification_internal_dir
        )
        notification_internal_good = read(notification_internal)
        notification_internal.write_text(
            notification_internal_good + '\nfn bad() { tonic::Status::internal("notification db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct NotificationService internal constructor" in failure for failure in failures), failures
        notification_internal.write_text(
            notification_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "notification db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct NotificationService concrete Internal status constructor" in failure for failure in failures), failures
        notification_internal.write_text(
            notification_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("NotificationService" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        memcached_internal = root / MEMCACHED_INTERNAL_STATUS_PATH
        memcached_internal_good = read(memcached_internal)
        memcached_internal.write_text(
            memcached_internal_good + '\nfn bad() { tonic::Status::internal("memcached failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Memcached executor internal constructor" in failure for failure in failures), failures
        memcached_internal.write_text(
            memcached_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "memcached failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Memcached executor concrete Internal status constructor" in failure for failure in failures), failures
        memcached_internal.write_text(
            memcached_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("Memcached executor" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        mongodb_internal = root / MONGODB_INTERNAL_STATUS_PATH
        mongodb_internal_good = read(mongodb_internal)
        mongodb_internal.write_text(
            mongodb_internal_good + '\nfn bad() { tonic::Status::internal("mongodb failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct MongoDB executor internal constructor" in failure for failure in failures), failures
        mongodb_internal.write_text(
            mongodb_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "mongodb failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct MongoDB executor concrete Internal status constructor" in failure for failure in failures), failures
        mongodb_internal.write_text(
            mongodb_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("MongoDB executor" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        mysql_internal = root / MYSQL_INTERNAL_STATUS_PATH
        mysql_internal_good = read(mysql_internal)
        mysql_internal.write_text(
            mysql_internal_good + '\nfn bad() { tonic::Status::internal("mysql failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct MySQL executor internal constructor" in failure for failure in failures), failures
        mysql_internal.write_text(
            mysql_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "mysql failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct MySQL executor concrete Internal status constructor" in failure for failure in failures), failures
        mysql_internal.write_text(
            mysql_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("MySQL executor" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        neo4j_internal = root / NEO4J_INTERNAL_STATUS_PATH
        neo4j_internal_good = read(neo4j_internal)
        neo4j_internal.write_text(
            neo4j_internal_good + '\nfn bad() { tonic::Status::internal("neo4j failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Neo4j executor internal constructor" in failure for failure in failures), failures
        neo4j_internal.write_text(
            neo4j_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "neo4j failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Neo4j executor concrete Internal status constructor" in failure for failure in failures), failures
        neo4j_internal.write_text(
            neo4j_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("Neo4j executor" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        mssql_internal = root / MSSQL_INTERNAL_STATUS_PATH
        mssql_internal_good = read(mssql_internal)
        mssql_internal.write_text(
            mssql_internal_good + '\nfn bad() { tonic::Status::internal("mssql failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct SQL Server executor internal constructor" in failure for failure in failures), failures
        mssql_internal.write_text(
            mssql_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "mssql failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct SQL Server executor concrete Internal status constructor" in failure for failure in failures), failures
        mssql_internal.write_text(
            mssql_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("SQL Server executor" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        elasticsearch_internal = root / ELASTICSEARCH_INTERNAL_STATUS_PATH
        elasticsearch_internal_good = read(elasticsearch_internal)
        elasticsearch_internal.write_text(
            elasticsearch_internal_good + '\nfn bad() { tonic::Status::internal("elasticsearch failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Elasticsearch executor internal constructor" in failure for failure in failures), failures
        elasticsearch_internal.write_text(
            elasticsearch_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "elasticsearch failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Elasticsearch executor concrete Internal status constructor" in failure for failure in failures), failures
        elasticsearch_internal.write_text(
            elasticsearch_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("Elasticsearch executor" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        postgres_executor_internal = root / POSTGRES_EXECUTOR_INTERNAL_STATUS_PATH
        postgres_executor_internal_good = read(postgres_executor_internal)
        postgres_executor_internal.write_text(
            postgres_executor_internal_good + '\nfn bad() { tonic::Status::internal("postgres executor failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Postgres executor internal constructor" in failure for failure in failures), failures
        postgres_executor_internal.write_text(
            postgres_executor_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "postgres executor failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Postgres executor concrete Internal status constructor" in failure for failure in failures), failures
        postgres_executor_internal.write_text(
            postgres_executor_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("Postgres executor" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        postgres_helpers_internal = root / POSTGRES_HELPERS_INTERNAL_STATUS_PATH
        postgres_helpers_internal_good = read(postgres_helpers_internal)
        postgres_helpers_internal.write_text(
            postgres_helpers_internal_good + '\nfn bad() { tonic::Status::internal("postgres helper failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Postgres helper internal constructor" in failure for failure in failures), failures
        postgres_helpers_internal.write_text(
            postgres_helpers_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "postgres helper failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Postgres helper concrete Internal status constructor" in failure for failure in failures), failures
        postgres_helpers_internal.write_text(
            postgres_helpers_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("Postgres helper" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        probe_dispatch_internal = root / PROBE_DISPATCH_INTERNAL_STATUS_PATH
        probe_dispatch_internal_good = read(probe_dispatch_internal)
        probe_dispatch_internal.write_text(
            probe_dispatch_internal_good + '\nfn bad() { tonic::Status::internal("probe dispatch failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct probe dispatch internal constructor" in failure for failure in failures), failures
        probe_dispatch_internal.write_text(
            probe_dispatch_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "probe dispatch failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct probe dispatch concrete Internal status constructor" in failure for failure in failures), failures
        probe_dispatch_internal.write_text(
            probe_dispatch_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("probe dispatch" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        qdrant_internal = root / QDRANT_INTERNAL_STATUS_PATH
        qdrant_internal_good = read(qdrant_internal)
        qdrant_internal.write_text(
            qdrant_internal_good + '\nfn bad() { tonic::Status::internal("qdrant failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Qdrant executor internal constructor" in failure for failure in failures), failures
        qdrant_internal.write_text(
            qdrant_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "qdrant failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Qdrant executor concrete Internal status constructor" in failure for failure in failures), failures
        qdrant_internal.write_text(
            qdrant_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("Qdrant executor" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        cassandra_internal = root / CASSANDRA_INTERNAL_STATUS_PATH
        cassandra_internal_good = read(cassandra_internal)
        cassandra_internal.write_text(
            cassandra_internal_good + '\nfn bad() { tonic::Status::internal("cassandra failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Cassandra executor internal constructor" in failure for failure in failures), failures
        cassandra_internal.write_text(
            cassandra_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "cassandra failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Cassandra executor concrete Internal status constructor" in failure for failure in failures), failures
        cassandra_internal.write_text(
            cassandra_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("Cassandra executor" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        clickhouse_internal = root / CLICKHOUSE_INTERNAL_STATUS_PATH
        clickhouse_internal_good = read(clickhouse_internal)
        clickhouse_internal.write_text(
            clickhouse_internal_good + '\nfn bad() { tonic::Status::internal("clickhouse failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct ClickHouse executor internal constructor" in failure for failure in failures), failures
        clickhouse_internal.write_text(
            clickhouse_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "clickhouse failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct ClickHouse executor concrete Internal status constructor" in failure for failure in failures), failures
        clickhouse_internal.write_text(
            clickhouse_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("ClickHouse executor" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        pinecone_internal = root / PINECONE_INTERNAL_STATUS_PATH
        pinecone_internal_good = read(pinecone_internal)
        pinecone_internal.write_text(
            pinecone_internal_good + '\nfn bad() { tonic::Status::internal("pinecone failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Pinecone executor internal constructor" in failure for failure in failures), failures
        pinecone_internal.write_text(
            pinecone_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "pinecone failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Pinecone executor concrete Internal status constructor" in failure for failure in failures), failures
        pinecone_internal.write_text(
            pinecone_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("Pinecone executor" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        saga_internal = root / SAGA_INTERNAL_STATUS_PATH
        saga_internal_good = read(saga_internal)
        saga_internal.write_text(
            saga_internal_good + '\nfn bad() { tonic::Status::internal("saga failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct saga internal constructor" in failure for failure in failures), failures
        saga_internal.write_text(
            saga_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "saga failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct saga concrete Internal status constructor" in failure for failure in failures), failures
        saga_internal.write_text(
            saga_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("saga" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        s3_internal = root / S3_INTERNAL_STATUS_PATH
        s3_internal_good = read(s3_internal)
        s3_internal.write_text(
            s3_internal_good + '\nfn bad() { tonic::Status::internal("s3 failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct S3 executor internal constructor" in failure for failure in failures), failures
        s3_internal.write_text(
            s3_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "s3 failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct S3 executor concrete Internal status constructor" in failure for failure in failures), failures
        s3_internal.write_text(
            s3_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("S3 executor" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        core_mod_internal = root / CORE_MOD_INTERNAL_STATUS_PATH
        core_mod_internal_good = read(core_mod_internal)
        core_mod_internal.write_text(
            core_mod_internal_good + '\nfn bad() { tonic::Status::internal("core failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct core runtime internal constructor" in failure for failure in failures), failures
        core_mod_internal.write_text(
            core_mod_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "core failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct core runtime concrete Internal status constructor" in failure for failure in failures), failures
        core_mod_internal.write_text(
            core_mod_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("core runtime" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        core_native_store_internal = root / CORE_NATIVE_STORE_INTERNAL_STATUS_PATH
        core_native_store_internal_good = read(core_native_store_internal)
        core_native_store_internal.write_text(
            core_native_store_internal_good + '\nfn bad() { tonic::Status::internal("core native store failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct core native store internal constructor" in failure for failure in failures), failures
        core_native_store_internal.write_text(
            core_native_store_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "core native store failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct core native store concrete Internal status constructor" in failure for failure in failures), failures
        core_native_store_internal.write_text(
            core_native_store_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("core native store" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        system_catalog_internal = root / SYSTEM_CATALOG_INTERNAL_STATUS_PATH
        system_catalog_internal_good = read(system_catalog_internal)
        system_catalog_internal.write_text(
            system_catalog_internal_good + '\nfn bad() { tonic::Status::internal("system catalog failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct system catalog internal constructor" in failure for failure in failures), failures
        system_catalog_internal.write_text(
            system_catalog_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "system catalog failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct system catalog concrete Internal status constructor" in failure for failure in failures), failures
        system_catalog_internal.write_text(
            system_catalog_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("system catalog" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        tenant_purge_internal = root / TENANT_PURGE_INTERNAL_STATUS_PATH
        tenant_purge_internal_good = read(tenant_purge_internal)
        tenant_purge_internal.write_text(
            tenant_purge_internal_good + '\nfn bad() { tonic::Status::internal("tenant purge failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct tenant purge internal constructor" in failure for failure in failures), failures
        tenant_purge_internal.write_text(
            tenant_purge_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "tenant purge failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct tenant purge concrete Internal status constructor" in failure for failure in failures), failures
        tenant_purge_internal.write_text(
            tenant_purge_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("tenant purge" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        tx_object_internal = root / TX_OBJECT_INTERNAL_STATUS_PATH
        tx_object_internal_good = read(tx_object_internal)
        tx_object_internal.write_text(
            tx_object_internal_good + '\nfn bad() { tonic::Status::internal("tx object failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct transaction object internal constructor" in failure for failure in failures), failures
        tx_object_internal.write_text(
            tx_object_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "tx object failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct transaction object concrete Internal status constructor" in failure for failure in failures), failures
        tx_object_internal.write_text(
            tx_object_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("transaction object" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        weaviate_internal = root / WEAVIATE_INTERNAL_STATUS_PATH
        weaviate_internal_good = read(weaviate_internal)
        weaviate_internal.write_text(
            weaviate_internal_good + '\nfn bad() { tonic::Status::internal("weaviate failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Weaviate executor internal constructor" in failure for failure in failures), failures
        weaviate_internal.write_text(
            weaviate_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "weaviate failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Weaviate executor concrete Internal status constructor" in failure for failure in failures), failures
        weaviate_internal.write_text(
            weaviate_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("Weaviate executor" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        vault_internal_dir = root / VAULT_INTERNAL_STATUS_PATH
        # See the metering note above: mutate a `.rs` file inside a service dir.
        vault_internal = (
            vault_internal_dir / "mod.rs"
            if vault_internal_dir.is_dir()
            else vault_internal_dir
        )
        vault_internal_good = read(vault_internal)
        vault_internal.write_text(
            vault_internal_good + '\nfn bad() { tonic::Status::internal("vault db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct VaultService internal constructor" in failure for failure in failures), failures
        vault_internal.write_text(
            vault_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "vault db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct VaultService concrete Internal status constructor" in failure for failure in failures), failures
        vault_internal.write_text(
            vault_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("VaultService" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        tenant_internal_dir = root / TENANT_INTERNAL_STATUS_PATH
        # See the metering note above: mutate a `.rs` file inside a service dir.
        tenant_internal = (
            tenant_internal_dir / "mod.rs"
            if tenant_internal_dir.is_dir()
            else tenant_internal_dir
        )
        tenant_internal_good = read(tenant_internal)
        tenant_internal.write_text(
            tenant_internal_good + '\nfn bad() { tonic::Status::internal("tenant db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct TenantService internal constructor" in failure for failure in failures), failures
        tenant_internal.write_text(
            tenant_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "tenant db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct TenantService concrete Internal status constructor" in failure for failure in failures), failures
        tenant_internal.write_text(
            tenant_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("TenantService" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        webhook_internal_dir = root / WEBHOOK_INTERNAL_STATUS_PATH
        # See the metering note above: mutate a `.rs` file inside a service dir.
        webhook_internal = (
            webhook_internal_dir / "mod.rs"
            if webhook_internal_dir.is_dir()
            else webhook_internal_dir
        )
        webhook_internal_good = read(webhook_internal)
        webhook_internal.write_text(
            webhook_internal_good + '\nfn bad() { tonic::Status::internal("webhook db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct WebhookService internal constructor" in failure for failure in failures), failures
        webhook_internal.write_text(
            webhook_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "webhook db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct WebhookService concrete Internal status constructor" in failure for failure in failures), failures
        webhook_internal.write_text(
            webhook_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("WebhookService" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        webrtc_internal = root / WEBRTC_INTERNAL_STATUS_PATH
        webrtc_internal_good = read(webrtc_internal)
        webrtc_internal.write_text(
            webrtc_internal_good + '\nfn bad() { tonic::Status::internal("webrtc db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct WebrtcService internal constructor" in failure for failure in failures), failures
        webrtc_internal.write_text(
            webrtc_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "webrtc db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct WebrtcService concrete Internal status constructor" in failure for failure in failures), failures
        webrtc_internal.write_text(
            webrtc_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("WebrtcService" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        workflow_internal_dir = root / WORKFLOW_INTERNAL_STATUS_PATH
        # See the metering note above: mutate a `.rs` file inside a service dir.
        workflow_internal = (
            workflow_internal_dir / "mod.rs"
            if workflow_internal_dir.is_dir()
            else workflow_internal_dir
        )
        workflow_internal_good = read(workflow_internal)
        workflow_internal.write_text(
            workflow_internal_good + '\nfn bad() { tonic::Status::internal("workflow db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct WorkflowService internal constructor" in failure for failure in failures), failures
        workflow_internal.write_text(
            workflow_internal_good
            + '\nfn also_bad() { tonic::Status::new(tonic::Code::Internal, "workflow db failed"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct WorkflowService concrete Internal status constructor" in failure for failure in failures), failures
        workflow_internal.write_text(
            workflow_internal_good + "\n//! Historical note: Status::internal used to be direct here.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("WorkflowService" in failure and "Internal" in failure for failure in failures), failures

        write_fixture(root)
        api_rules = root / "docs/api-rules.md"
        api_rules.write_text(
            read(api_rules)
            .replace("\nThe error body is `ApiError` mapped from `google.rpc.Status`.\n", "\n")
            .replace("| `ROOM_FULL` | RoomService |", "")
            .replace("| `ROOM_FULL` | TestService | `error-reason` trailer |\n", ""),
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("ROOM_FULL" in failure for failure in failures), failures

        write_fixture(root)
        direct = root / "src/live_invalid.rs"
        direct.parent.mkdir(parents=True, exist_ok=True)
        direct.write_text(
            "fn bad() { tonic::Status::invalid_argument(\"field is required\"); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct invalid_argument constructor" in failure for failure in failures), failures
        direct.write_text(
            "fn bad() { tonic::Status::new(tonic::Code::InvalidArgument, \"field is required\"); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete InvalidArgument status constructor" in failure for failure in failures), failures
        direct.write_text(
            "fn bad() { Status::with_metadata(Code::InvalidArgument, \"field is required\", metadata); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete InvalidArgument status constructor" in failure for failure in failures), failures
        direct.write_text(
            "fn bad() { Status::with_details(Code::InvalidArgument, \"field is required\", details); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete InvalidArgument status constructor" in failure for failure in failures), failures
        direct.write_text(
            "fn bad() { tonic::Status::with_details_and_metadata(tonic::Code::InvalidArgument, \"field is required\", details, metadata); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete InvalidArgument status constructor" in failure for failure in failures), failures
        direct.write_text(
            "//! Historical note: Status::invalid_argument used to be direct.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("direct" in failure and "InvalidArgument" in failure for failure in failures), failures
        crate_direct = root / "crates/udb-portable/src/lib.rs"
        crate_direct.parent.mkdir(parents=True, exist_ok=True)
        crate_direct.write_text(
            "fn bad() { tonic::Status::invalid_argument(\"field is required\"); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("crates/udb-portable/src/lib.rs" in failure for failure in failures), failures

        write_fixture(root)
        direct_precondition = root / "src/live_failed_precondition.rs"
        direct_precondition.parent.mkdir(parents=True, exist_ok=True)
        direct_precondition.write_text(
            "fn bad() { tonic::Status::failed_precondition(\"not ready\"); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct failed_precondition constructor" in failure for failure in failures), failures
        direct_precondition.write_text(
            "fn bad() { tonic::Status::new(tonic::Code::FailedPrecondition, \"not ready\"); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete FailedPrecondition status constructor" in failure for failure in failures), failures
        direct_precondition.write_text(
            "fn bad() { Status::with_metadata(Code::FailedPrecondition, \"not ready\", metadata); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete FailedPrecondition status constructor" in failure for failure in failures), failures
        direct_precondition.write_text(
            "fn bad() { Status::with_details(Code::FailedPrecondition, \"not ready\", details); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete FailedPrecondition status constructor" in failure for failure in failures), failures
        direct_precondition.write_text(
            "fn bad() { tonic::Status::with_details_and_metadata(tonic::Code::FailedPrecondition, \"not ready\", details, metadata); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete FailedPrecondition status constructor" in failure for failure in failures), failures
        direct_precondition.write_text(
            "//! Historical note: Status::failed_precondition used to be direct.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("direct" in failure and "FailedPrecondition" in failure for failure in failures), failures
        crate_precondition = root / "crates/udb-wasm/src/lib.rs"
        crate_precondition.parent.mkdir(parents=True, exist_ok=True)
        crate_precondition.write_text(
            "fn bad() { tonic::Status::failed_precondition(\"not ready\"); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("crates/udb-wasm/src/lib.rs" in failure for failure in failures), failures

        write_fixture(root)
        direct_permission = root / "src/raw_permission_denied.rs"
        direct_permission.parent.mkdir(parents=True, exist_ok=True)
        direct_permission.write_text(
            "fn bad() { tonic::Status::permission_denied(\"denied\"); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct permission_denied constructor" in failure for failure in failures), failures
        direct_permission.write_text(
            "fn bad() { tonic::Status::new(tonic::Code::PermissionDenied, \"denied\"); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete PermissionDenied status constructor" in failure for failure in failures), failures
        direct_permission.write_text(
            "fn bad() { Status::with_metadata(Code::PermissionDenied, \"denied\", metadata); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete PermissionDenied status constructor" in failure for failure in failures), failures
        direct_permission.write_text(
            "fn bad() { Status::with_details(Code::PermissionDenied, \"denied\", details); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete PermissionDenied status constructor" in failure for failure in failures), failures
        direct_permission.write_text(
            "fn bad() { tonic::Status::with_details_and_metadata(tonic::Code::PermissionDenied, \"denied\", details, metadata); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete PermissionDenied status constructor" in failure for failure in failures), failures
        direct_permission.write_text(
            "//! Historical note: Status::permission_denied used to be direct.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("direct" in failure and "PermissionDenied" in failure for failure in failures), failures

        write_fixture(root)
        direct_not_found = root / "src/raw_not_found.rs"
        direct_not_found.parent.mkdir(parents=True, exist_ok=True)
        direct_not_found.write_text(
            "fn bad() { tonic::Status::not_found(\"missing\"); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct not_found constructor" in failure for failure in failures), failures
        direct_not_found.write_text(
            "fn bad() { tonic::Status::new(tonic::Code::NotFound, \"missing\"); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete NotFound status constructor" in failure for failure in failures), failures
        direct_not_found.write_text(
            "fn bad() { Status::with_metadata(Code::NotFound, \"missing\", metadata); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete NotFound status constructor" in failure for failure in failures), failures
        direct_not_found.write_text(
            "fn bad() { Status::with_details(Code::NotFound, \"missing\", details); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete NotFound status constructor" in failure for failure in failures), failures
        direct_not_found.write_text(
            "fn bad() { tonic::Status::with_details_and_metadata(tonic::Code::NotFound, \"missing\", details, metadata); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete NotFound status constructor" in failure for failure in failures), failures
        direct_not_found.write_text(
            "//! Historical note: Status::not_found used to be direct.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("direct" in failure and "NotFound" in failure for failure in failures), failures

        write_fixture(root)
        direct_already_exists = root / "src/raw_already_exists.rs"
        direct_already_exists.parent.mkdir(parents=True, exist_ok=True)
        direct_already_exists.write_text(
            "fn bad() { tonic::Status::already_exists(\"resource exists\"); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct already_exists constructor" in failure for failure in failures), failures
        direct_already_exists.write_text(
            "fn bad() { tonic::Status::new(tonic::Code::AlreadyExists, \"resource exists\"); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete AlreadyExists status constructor" in failure for failure in failures), failures
        direct_already_exists.write_text(
            "fn bad() { Status::with_metadata(Code::AlreadyExists, \"resource exists\", metadata); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete AlreadyExists status constructor" in failure for failure in failures), failures
        direct_already_exists.write_text(
            "fn bad() { Status::with_details(Code::AlreadyExists, \"resource exists\", details); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete AlreadyExists status constructor" in failure for failure in failures), failures
        direct_already_exists.write_text(
            "fn bad() { tonic::Status::with_details_and_metadata(tonic::Code::AlreadyExists, \"resource exists\", details, metadata); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete AlreadyExists status constructor" in failure for failure in failures), failures
        direct_already_exists.write_text(
            "//! Historical note: Status::already_exists used to be direct.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("direct" in failure and "AlreadyExists" in failure for failure in failures), failures
        crate_already_exists = root / "crates/udb-portable/src/conflict.rs"
        crate_already_exists.parent.mkdir(parents=True, exist_ok=True)
        crate_already_exists.write_text(
            "fn bad() { tonic::Status::already_exists(\"resource exists\"); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("crates/udb-portable/src/conflict.rs" in failure for failure in failures), failures

        write_fixture(root)
        direct_retry = root / "src/raw_retry.rs"
        direct_retry.parent.mkdir(parents=True, exist_ok=True)
        direct_retry.write_text(
            "fn unavailable() { tonic::Status::unavailable(\"backend unavailable\"); }\n"
            "fn bad() { tonic::Status::resource_exhausted(\"overloaded\"); }\n"
            "fn also_bad() { tonic::Status::aborted(\"conflict\"); }\n"
            "fn timed_out() { tonic::Status::deadline_exceeded(\"timeout\"); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct tonic::Status::unavailable constructor" in failure for failure in failures), failures
        assert any("direct tonic::Status::resource_exhausted constructor" in failure for failure in failures), failures
        assert any("direct tonic::Status::aborted constructor" in failure for failure in failures), failures
        assert any("direct tonic::Status::deadline_exceeded constructor" in failure for failure in failures), failures
        direct_retry.write_text(
            "fn unavailable() { Status::new(Code::Unavailable, \"backend unavailable\"); }\n"
            "fn bad() { Status::new(Code::ResourceExhausted, \"overloaded\"); }\n"
            "fn also_bad() { tonic::Status::new(tonic::Code::Aborted, \"conflict\"); }\n"
            "fn timed_out() { Status::new(Code::DeadlineExceeded, \"timeout\"); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Status::new(Code::Unavailable) constructor" in failure for failure in failures), failures
        assert any("direct Status::new(Code::ResourceExhausted) constructor" in failure for failure in failures), failures
        assert any("direct Status::new(Code::Aborted) constructor" in failure for failure in failures), failures
        assert any("direct Status::new(Code::DeadlineExceeded) constructor" in failure for failure in failures), failures
        direct_retry.write_text(
            "fn unavailable() { Status::with_metadata(Code::Unavailable, \"backend unavailable\", metadata); }\n"
            "fn bad() { Status::with_metadata(Code::ResourceExhausted, \"overloaded\", metadata); }\n"
            "fn also_bad() { tonic::Status::with_metadata(tonic::Code::Aborted, \"conflict\", metadata); }\n"
            "fn timed_out() { Status::with_metadata(Code::DeadlineExceeded, \"timeout\", metadata); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Status::with_metadata(Code::Unavailable) constructor" in failure for failure in failures), failures
        assert any("direct Status::with_metadata(Code::ResourceExhausted) constructor" in failure for failure in failures), failures
        assert any("direct Status::with_metadata(Code::Aborted) constructor" in failure for failure in failures), failures
        assert any("direct Status::with_metadata(Code::DeadlineExceeded) constructor" in failure for failure in failures), failures
        direct_retry.write_text(
            "fn unavailable() { Status::with_details(Code::Unavailable, \"backend unavailable\", details); }\n"
            "fn bad() { Status::with_details(Code::ResourceExhausted, \"overloaded\", details); }\n"
            "fn also_bad() { tonic::Status::with_details(tonic::Code::Aborted, \"conflict\", details); }\n"
            "fn timed_out() { Status::with_details(Code::DeadlineExceeded, \"timeout\", details); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Status::with_details(Code::Unavailable) constructor" in failure for failure in failures), failures
        assert any("direct Status::with_details(Code::ResourceExhausted) constructor" in failure for failure in failures), failures
        assert any("direct Status::with_details(Code::Aborted) constructor" in failure for failure in failures), failures
        assert any("direct Status::with_details(Code::DeadlineExceeded) constructor" in failure for failure in failures), failures
        direct_retry.write_text(
            "fn unavailable() { Status::with_details_and_metadata(Code::Unavailable, \"backend unavailable\", details, metadata); }\n"
            "fn bad() { Status::with_details_and_metadata(Code::ResourceExhausted, \"overloaded\", details, metadata); }\n"
            "fn also_bad() { tonic::Status::with_details_and_metadata(tonic::Code::Aborted, \"conflict\", details, metadata); }\n"
            "fn timed_out() { Status::with_details_and_metadata(Code::DeadlineExceeded, \"timeout\", details, metadata); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct Status::with_details_and_metadata(Code::Unavailable) constructor" in failure for failure in failures), failures
        assert any("direct Status::with_details_and_metadata(Code::ResourceExhausted) constructor" in failure for failure in failures), failures
        assert any("direct Status::with_details_and_metadata(Code::Aborted) constructor" in failure for failure in failures), failures
        assert any("direct Status::with_details_and_metadata(Code::DeadlineExceeded) constructor" in failure for failure in failures), failures
        direct_retry.unlink()
        crate_retry = root / "crates/udb-portable/src/retry.rs"
        crate_retry.parent.mkdir(parents=True, exist_ok=True)
        crate_retry.write_text(
            "fn unavailable() { tonic::Status::unavailable(\"backend unavailable\"); }\n"
            "fn bad() { tonic::Status::resource_exhausted(\"overloaded\"); }\n"
            "fn also_bad() { tonic::Status::aborted(\"conflict\"); }\n"
            "fn timed_out() { tonic::Status::deadline_exceeded(\"timeout\"); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("crates/udb-portable/src/retry.rs" in failure for failure in failures), failures
        crate_retry.write_text(
            "fn unavailable() { Status::with_details(Code::Unavailable, \"backend unavailable\", details); }\n"
            "fn bad() { Status::with_details(Code::ResourceExhausted, \"overloaded\", details); }\n"
            "fn also_bad() { tonic::Status::with_details_and_metadata(tonic::Code::Aborted, \"conflict\", details, metadata); }\n"
            "fn timed_out() { tonic::Status::with_details(tonic::Code::DeadlineExceeded, \"timeout\", details); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("crates/udb-portable/src/retry.rs" in failure for failure in failures), failures

        write_fixture(root)
        direct_unimplemented = root / "src/raw_unimplemented.rs"
        direct_unimplemented.parent.mkdir(parents=True, exist_ok=True)
        direct_unimplemented.write_text(
            "fn bad() { tonic::Status::unimplemented(\"feature not ready\"); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct unimplemented status/path" in failure for failure in failures), failures
        direct_unimplemented.write_text(
            "fn bad() { Status::new(Code::Unimplemented, \"feature not ready\"); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete Unimplemented status constructor" in failure for failure in failures), failures
        direct_unimplemented.write_text(
            "fn bad() { unimplemented!(\"feature not ready\"); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct unimplemented status/path" in failure for failure in failures), failures
        direct_unimplemented.write_text(
            "//! Historical note: Status::unimplemented used to be direct.\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert not any("source unimplemented posture" in failure for failure in failures), failures
        direct_unimplemented.unlink()
        crate_unimplemented = root / "crates/udb-wasm/src/unimplemented.rs"
        crate_unimplemented.parent.mkdir(parents=True, exist_ok=True)
        crate_unimplemented.write_text(
            "fn bad() { tonic::Status::with_details(tonic::Code::Unimplemented, \"feature not ready\", details); }\n",
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("crates/udb-wasm/src/unimplemented.rs" in failure for failure in failures), failures

        write_fixture(root)
        direct_internal = root / "src/raw_internal.rs"
        direct_internal.parent.mkdir(parents=True, exist_ok=True)
        direct_internal.write_text('fn bad() { tonic::Status::internal("new raw internal"); }\n', encoding="utf-8")
        failures = check_root(root)
        assert any("direct internal constructor" in failure for failure in failures), failures
        direct_internal.write_text(
            'fn bad() { tonic::Status::new(tonic::Code::Internal, "new raw internal"); }\n',
            encoding="utf-8",
        )
        failures = check_root(root)
        assert any("direct concrete Internal status constructor" in failure for failure in failures), failures
        direct_internal.write_text("// Historical note: Status::internal used to be direct.\n", encoding="utf-8")
        failures = check_root(root)
        assert not any("source internal-detail posture" in failure for failure in failures), failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--selftest", action="store_true", help="run fixture tests for this guard")
    args = parser.parse_args()

    if args.selftest:
        run_selftest()
        print("error-detail posture selftest passed")
        return 0

    if shutil.which("python") is None and shutil.which("python3") is None:
        print("warning: python executable not found in PATH", file=sys.stderr)

    failures = check_root(ROOT)
    if failures:
        print("error-detail posture FAILED:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1
    print("error-detail posture OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
