//! Unit fixtures for `ConfigService`. The `resolve_flag`/`evaluate_flag` pairs are
//! the SDK<->server evaluation contract (fixed inputs -> fixed outputs); the
//! request-guard tests prove tenant isolation and typed error details fire before
//! any store access.

use std::collections::HashMap;

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::ir::{ComparisonOp, LogicalFilter, LogicalPagination, LogicalValue};
use crate::proto::udb::core::config::services::v1 as config_pb;
use crate::proto::udb::core::config::services::v1::config_service_server::ConfigService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

use super::ConfigServiceImpl;
use super::codec::{flag_val_to_proto, flag_val_to_stored, proto_to_flag_val, stored_to_flag_val};
use super::config::{CONFIG_MSG, MAX_EVALUATE_KEYS, MAX_FLAGS_PER_KEY_SCAN};
use super::errors::config_capability_status;
use super::eval::{
    EvalContext, EvalFlag, FlagVal, bump_revision, evaluate_flag, resolve_flag, rollout_bucket,
};
use super::store::flag_candidates_batch_read;

fn decode_detail(status: &Status) -> ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(ERROR_DETAIL_METADATA_KEY)
        .expect("error-detail trailer present")
        .to_bytes()
        .expect("trailer decodes to bytes");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
}

fn assert_one_validation_field(status: &Status, field: &str, description: &str) {
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    let detail = decode_detail(status);
    assert_eq!(detail.kind, ErrorKind::Validation as i32);
    assert_eq!(detail.field_violations.len(), 1);
    assert_eq!(detail.field_violations[0].field, field);
    assert_eq!(detail.field_violations[0].description, description);
}

fn flag(
    flag_id: &str,
    project: &str,
    env: &str,
    value: FlagVal,
    enabled: bool,
    pct: i32,
    ctx_key: &str,
    revision: i64,
) -> EvalFlag {
    EvalFlag {
        flag_id: flag_id.to_string(),
        flag_key: "feature.x".to_string(),
        project_id: project.to_string(),
        environment: env.to_string(),
        value,
        enabled,
        rollout_percentage: pct,
        rollout_context_key: ctx_key.to_string(),
        revision,
    }
}

/// Scope precedence is environment > project > tenant-default, deterministically.
#[test]
fn scope_precedence_env_project_tenant_default() {
    let default = flag(
        "d",
        "",
        "",
        FlagVal::Str("default".into()),
        true,
        100,
        "",
        1,
    );
    let tenant_proj = flag(
        "p",
        "proj-1",
        "",
        FlagVal::Str("project".into()),
        true,
        100,
        "",
        2,
    );
    let env = flag(
        "e",
        "",
        "prod",
        FlagVal::Str("env".into()),
        true,
        100,
        "",
        3,
    );
    let env_proj = flag(
        "ep",
        "proj-1",
        "prod",
        FlagVal::Str("env+proj".into()),
        true,
        100,
        "",
        4,
    );

    let ctx = EvalContext {
        project_id: "proj-1".to_string(),
        environment: "prod".to_string(),
        attributes: HashMap::new(),
    };
    // All four are candidates; the most specific (env+project) wins.
    let candidates = vec![
        default.clone(),
        tenant_proj.clone(),
        env.clone(),
        env_proj.clone(),
    ];
    let resolved = resolve_flag(&candidates, &ctx).expect("a candidate must resolve");
    assert_eq!(resolved.flag_id, "ep");

    // Without the env+project row, the env-only row beats the project-only row.
    let candidates = vec![default.clone(), tenant_proj.clone(), env.clone()];
    assert_eq!(resolve_flag(&candidates, &ctx).unwrap().flag_id, "e");

    // Project-only beats tenant-default.
    let candidates = vec![default.clone(), tenant_proj.clone()];
    assert_eq!(resolve_flag(&candidates, &ctx).unwrap().flag_id, "p");

    // Only the tenant-default remains.
    let candidates = vec![default.clone()];
    assert_eq!(resolve_flag(&candidates, &ctx).unwrap().flag_id, "d");

    // A non-matching environment row is NOT a candidate.
    let other_env = flag(
        "o",
        "",
        "staging",
        FlagVal::Str("staging".into()),
        true,
        100,
        "",
        5,
    );
    assert!(resolve_flag(std::slice::from_ref(&other_env), &ctx).is_none());
}

/// `evaluate_flag` is deterministic: the same (flag, context) yields the same
/// value on every call, and percentage rollout is stable per subject. These
/// fixed input -> output pairs are the SDK<->server contract.
#[test]
fn evaluate_flag_is_deterministic_and_stable() {
    let ctx = |subject: &str| EvalContext {
        project_id: String::new(),
        environment: String::new(),
        attributes: HashMap::from([("user_id".to_string(), subject.to_string())]),
    };

    // Fully on / fully off / disabled are unconditional.
    let on = flag("a", "", "", FlagVal::Bool(true), true, 100, "user_id", 1);
    assert_eq!(evaluate_flag(&on, &ctx("u-1")), FlagVal::Bool(true));
    let zero = flag("b", "", "", FlagVal::Bool(true), true, 0, "user_id", 1);
    assert_eq!(evaluate_flag(&zero, &ctx("u-1")), FlagVal::Bool(false));
    let disabled = flag(
        "c",
        "",
        "",
        FlagVal::Str("v".into()),
        false,
        100,
        "user_id",
        1,
    );
    assert_eq!(
        evaluate_flag(&disabled, &ctx("u-1")),
        FlagVal::Str(String::new())
    );

    // Stable bucket: the same subject is sticky across many calls.
    let half = flag("h", "", "", FlagVal::Bool(true), true, 50, "user_id", 1);
    let first = evaluate_flag(&half, &ctx("subject-42"));
    for _ in 0..16 {
        assert_eq!(evaluate_flag(&half, &ctx("subject-42")), first);
    }

    // The bucket is a pure function of (flag_key, subject) — assert the exact
    // boundary so any drift in the hash/threshold is a test failure.
    let bucket = rollout_bucket("feature.x", "subject-42");
    let expected = if bucket < 50 {
        FlagVal::Bool(true)
    } else {
        FlagVal::Bool(false)
    };
    assert_eq!(first, expected);
    // Monotonicity: a subject in the 30% bucket is also in the 80% bucket.
    if bucket < 30 {
        let p30 = flag("p30", "", "", FlagVal::Bool(true), true, 30, "user_id", 1);
        let p80 = flag("p80", "", "", FlagVal::Bool(true), true, 80, "user_id", 1);
        assert_eq!(evaluate_flag(&p30, &ctx("subject-42")), FlagVal::Bool(true));
        assert_eq!(evaluate_flag(&p80, &ctx("subject-42")), FlagVal::Bool(true));
    }
}

/// The config-domain revision is monotone and bumps on every change.
#[test]
fn revision_bumps_monotonically() {
    assert_eq!(bump_revision(0), 1);
    assert_eq!(bump_revision(1), 2);
    let mut rev = 0;
    for expected in 1..=10 {
        rev = bump_revision(rev);
        assert_eq!(rev, expected);
    }
    // Saturating at the ceiling (never panics / wraps).
    assert_eq!(bump_revision(i64::MAX), i64::MAX);
}

/// A caller scoped to tenant-a must not write tenant-b's flag by putting a
/// foreign tenant_id in the request BODY; the guard rejects before any store
/// access (no Postgres needed) — mirrors `lock_service`/`tenant_service`.
#[tokio::test]
async fn put_flag_rejects_cross_tenant_body() {
    let svc = ConfigServiceImpl::new(); // no runtime, no channels (admit no-op)
    let mut request = Request::new(config_pb::PutFlagRequest {
        tenant_id: "tenant-b".to_string(),
        flag_key: "feature.x".to_string(),
        value: Some(flag_val_to_proto(&FlagVal::Bool(true))),
        enabled: true,
        rollout_percentage: 100,
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .put_flag(request)
        .await
        .expect_err("cross-tenant body must be rejected");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn put_flag_rejects_cross_project_body() {
    let svc = ConfigServiceImpl::new();
    let mut request = Request::new(config_pb::PutFlagRequest {
        tenant_id: "tenant-a".to_string(),
        project_id: "project-b".to_string(),
        flag_key: "feature.x".to_string(),
        value: Some(flag_val_to_proto(&FlagVal::Bool(true))),
        enabled: true,
        rollout_percentage: 100,
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    request
        .metadata_mut()
        .insert("x-udb-project-id", MetadataValue::from_static("project-a"));
    let err = svc
        .put_flag(request)
        .await
        .expect_err("cross-project body must be rejected before runtime access");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        decode_detail(&err).policy_decision_id,
        "project_metadata_mismatch"
    );
}

#[tokio::test]
async fn put_flag_missing_value_carries_field_violation() {
    let svc = ConfigServiceImpl::new(); // no runtime, no channels (admit no-op)
    let mut request = Request::new(config_pb::PutFlagRequest {
        tenant_id: "tenant-a".to_string(),
        flag_key: "feature.x".to_string(),
        value: None,
        enabled: true,
        rollout_percentage: 100,
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .put_flag(request)
        .await
        .expect_err("missing value must be rejected before runtime access");
    assert_eq!(err.message(), "value is required");
    assert_one_validation_field(&err, "value", "must set one FlagValue arm");
}

#[tokio::test]
async fn get_flag_missing_key_carries_field_violation() {
    let svc = ConfigServiceImpl::new(); // no runtime, no channels (admit no-op)
    let mut request = Request::new(config_pb::GetFlagRequest {
        tenant_id: "tenant-a".to_string(),
        flag_key: "  ".to_string(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .get_flag(request)
        .await
        .expect_err("missing key must be rejected before runtime access");
    assert_eq!(err.message(), "flag_key is required");
    assert_one_validation_field(&err, "flag_key", "must be a non-empty flag key");
}

#[tokio::test]
async fn evaluate_flags_key_limit_carries_field_violation() {
    let svc = ConfigServiceImpl::new(); // no runtime, no channels (admit no-op)
    let mut request = Request::new(config_pb::EvaluateFlagsRequest {
        tenant_id: "tenant-a".to_string(),
        keys: (0..=MAX_EVALUATE_KEYS)
            .map(|i| format!("feature.{i}"))
            .collect(),
        ..Default::default()
    });
    request
        .metadata_mut()
        .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
    let err = svc
        .evaluate_flags(request)
        .await
        .expect_err("oversized key set must be rejected before runtime access");
    assert_eq!(err.message(), "too many keys (max 256)");
    assert_one_validation_field(&err, "keys", "must contain at most 256 keys");
}

#[test]
fn json_flag_value_validation_carries_field_violation() {
    let value = Some(config_pb::FlagValue {
        value: Some(config_pb::flag_value::Value::JsonValue("{bad".to_string())),
    });
    let err = proto_to_flag_val(&value).expect_err("invalid JSON flag value");
    assert!(err.message().starts_with("json_value is not valid JSON:"));
    assert_one_validation_field(&err, "value.json_value", "must be valid JSON");
}

#[test]
fn config_missing_runtime_capability_carries_typed_detail() {
    let err = config_capability_status(
        "native_entity_dispatch",
        "runtime_native_entity_dispatch",
        "config service requires runtime native-entity dispatch (no runtime configured)",
    );
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        err.message(),
        "config service requires runtime native-entity dispatch (no runtime configured)"
    );
    let detail = decode_detail(&err);
    assert_eq!(detail.kind, ErrorKind::Capability as i32);
    assert_eq!(detail.backend, "config");
    assert_eq!(detail.operation, "native_entity_dispatch");
    assert_eq!(detail.capability_required, "runtime_native_entity_dispatch");
    assert!(!detail.retryable);
}

/// The batched EvaluateFlags path issues ONE read whose filter ANDs the
/// tenant scope with a single `flag_key IN (keys)` list, capped at
/// `keys × MAX_FLAGS_PER_KEY_SCAN` rows — never one read per key.
#[test]
fn evaluate_batch_read_builds_single_in_list_filter() {
    let keys = vec![
        "feature.a".to_string(),
        "feature.b".to_string(),
        "feature.c".to_string(),
    ];
    let read = flag_candidates_batch_read("tenant-a", &keys);
    assert_eq!(read.message_type, CONFIG_MSG);

    let Some(LogicalFilter::And(branches)) = read.filter else {
        panic!("batched read must AND the tenant scope with the key In-list");
    };
    assert_eq!(branches.len(), 2, "exactly tenant scope + one In-list");
    assert_eq!(
        branches[0],
        LogicalFilter::Comparison {
            field: "tenant_id".to_string(),
            op: ComparisonOp::Eq,
            value: LogicalValue::String("tenant-a".to_string()),
        },
        "the tenant predicate must always be present",
    );
    assert_eq!(
        branches[1],
        LogicalFilter::InList {
            field: "flag_key".to_string(),
            values: keys
                .iter()
                .map(|key| LogicalValue::String(key.clone()))
                .collect(),
        },
        "all keys must ride ONE In-list filter",
    );

    // Overall row cap = keys × per-key scope cap (documented bound).
    assert_eq!(
        read.pagination,
        Some(LogicalPagination::limit(3 * MAX_FLAGS_PER_KEY_SCAN)),
    );
}

/// Round-trip the typed value oneof through storage encoding and back.
#[test]
fn value_storage_roundtrip() {
    for value in [
        FlagVal::Bool(true),
        FlagVal::Number(42.5),
        FlagVal::Str("hello".into()),
        FlagVal::Json("{\"a\":1}".into()),
    ] {
        let (ty, json) = flag_val_to_stored(&value);
        assert_eq!(stored_to_flag_val(&ty, &json), value);
    }
}
