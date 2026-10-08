//! Redaction metadata must survive an actual served Redis cache hit.

use serde_json::json;
use uuid::Uuid;

use super::data_plane_live::{
    col, create_schema, dp_live_pg_dsn, dp_pool, dp_service_with, install_dp_security,
    served_upsert, teardown, with_ctx,
};
use crate::generation::{CatalogManifest, ManifestTable, ManifestTableSecurity};
use crate::proto::data_broker_server::DataBroker;
use crate::proto::{CacheOptions, SelectRequest};
use crate::runtime::config::RedisConfig;
use crate::runtime::error_reasons::reason_of;
use crate::runtime::executor_utils::{REDACTION_PLACEHOLDER, json_to_struct};
use crate::runtime::system::ensure_system_catalog;

#[tokio::test]
#[ignore = "requires live Postgres + Redis; runs in the CI unfiltered native live lane"]
async fn served_cached_select_preserves_redacted_fields_live() {
    let (Some(pg), Some(redis)) = (
        dp_live_pg_dsn(),
        super::support::require_live_dsn_any(&[
            "UDB_REDIS_DSN",
            "UDB_INTEGRATION_REDIS_URL",
            "UDB_LIVE_REDIS_DSN",
        ]),
    ) else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&pg).await;
    ensure_system_catalog(&pool).await.expect("system catalog");
    let schema = format!("udb_cache_redaction_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".notes \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, note_data TEXT, private_data TEXT, literal TEXT)"
    ))
    .execute(&pool).await.expect("fixture table");
    const MSG: &str = "acme.cache.v1.Note";
    let mut note = col("note_data", "TEXT", false);
    note.field_name = "note_text".into();
    note.security.mask_in_logs = true;
    let mut private = col("private_data", "TEXT", false);
    private.field_name = "private_text".into();
    private.security.is_pii = true;
    private.security.mask_in_logs = true;
    let table = ManifestTable {
        proto_package: "acme.cache.v1".into(),
        message_name: "Note".into(),
        schema: schema.clone(),
        table: "notes".into(),
        primary_key: vec!["id".into()],
        table_security: ManifestTableSecurity {
            tenant_column: "tenant_id".into(),
            ..Default::default()
        },
        columns: vec![
            col("id", "TEXT", true),
            col("tenant_id", "TEXT", false),
            note,
            private,
            col("literal", "TEXT", false),
        ],
        ..Default::default()
    };
    let svc = dp_service_with(
        &pg,
        CatalogManifest {
            tables: vec![table],
            ..Default::default()
        },
        |cfg| {
            cfg.redis = Some(RedisConfig {
                dsn: Some(redis),
                ..Default::default()
            });
        },
    )
    .await;
    served_upsert(&svc, &tenant, MSG, json!({
        "id": "one", "note_text": "stored private value", "private_text": "stored PII", "literal": REDACTION_PLACEHOLDER,
    }), "").await.expect("served fixture write");
    let request = SelectRequest {
        message_type: MSG.into(),
        filter: json_to_struct(&json!({"id": "one"})),
        fields: vec!["id".into(), "note_text".into(), "literal".into()],
        // Explicit positive limits use keyset pagination and bypass this cache.
        limit: 0,
        cache: Some(CacheOptions {
            bypass_read: true,
            ttl_seconds: 60,
            ..Default::default()
        }),
        ..Default::default()
    };
    let cold = svc
        .select(with_ctx(request.clone(), &tenant))
        .await
        .expect("cold served Select")
        .into_inner();
    assert_eq!(cold.redacted_fields, vec!["note_data"]);
    let row: serde_json::Value =
        serde_json::from_slice(&cold.records_json[0]).expect("record JSON");
    assert_eq!(row["note_data"], REDACTION_PLACEHOLDER);
    assert_eq!(row["literal"], REDACTION_PLACEHOLDER);
    let runtime = svc.runtime_snapshot();
    let hits_before = runtime.cache_metrics_snapshot().udb_cache_hit_total;
    let mut warm_request = request.clone();
    warm_request.cache.as_mut().unwrap().bypass_read = false;
    let warm = svc
        .select(with_ctx(warm_request, &tenant))
        .await
        .expect("warm served Select")
        .into_inner();
    assert_eq!(
        runtime.cache_metrics_snapshot().udb_cache_hit_total,
        hits_before + 1,
        "the regression must exercise a real Redis cache hit"
    );
    assert_eq!(
        warm, cold,
        "cache warmth must not change any response field"
    );

    let implicit = svc
        .select(with_ctx(
            SelectRequest {
                message_type: MSG.into(),
                filter: json_to_struct(&json!({"id": "one"})),
                ..Default::default()
            },
            &tenant,
        ))
        .await
        .expect("bare Select omits PII without requiring its scope")
        .into_inner();
    let row: serde_json::Value = serde_json::from_slice(&implicit.records_json[0]).unwrap();
    assert!(row.get("private_data").is_none());
    let denied = svc
        .select(with_ctx(
            SelectRequest {
                message_type: MSG.into(),
                fields: vec!["private_text".into()],
                filter: json_to_struct(&json!({"id": "one"})),
                ..Default::default()
            },
            &tenant,
        ))
        .await
        .expect_err("a PII field alias still requires the PII scope");
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    assert_eq!(reason_of(&denied).as_deref(), Some("UDB_SCOPE_MISSING"));
    let refused = served_upsert(
        &svc,
        &tenant,
        MSG,
        json!({
            "id": "one", "note_text": REDACTION_PLACEHOLDER,
        }),
        "",
    )
    .await
    .expect_err("writing a masked alias must not wipe the stored value");
    assert_eq!(
        reason_of(&refused).as_deref(),
        Some("UDB_REDACTED_VALUE_WRITE")
    );

    let mut privileged = with_ctx(request, &tenant);
    privileged.metadata_mut().insert(
        "x-scopes",
        "udb:admin,udb:read,udb:write,udb:pii:read".parse().unwrap(),
    );
    let unmasked = svc
        .select(privileged)
        .await
        .expect("PII-scoped served read")
        .into_inner();
    assert!(unmasked.redacted_fields.is_empty());
    let row: serde_json::Value = serde_json::from_slice(&unmasked.records_json[0]).unwrap();
    assert_eq!(row["note_data"], "stored private value");
    assert_eq!(row["literal"], REDACTION_PLACEHOLDER);
    teardown(&pool, &schema, &tenant).await;
}
