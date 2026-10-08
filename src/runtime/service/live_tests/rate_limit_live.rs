//! Actual gRPC refusals retain the effective operation limit and caller while
//! both the distributed Redis limiter and its local fallback enforce it.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::json;
use uuid::Uuid;

use super::data_plane_live::{
    create_schema, dp_live_pg_dsn, dp_pool, dp_service_with, install_dp_security,
    serve_data_broker, teardown, widget_manifest, with_ctx,
};
use crate::proto::{ErrorKind, SelectRequest, UpsertRequest};
use crate::runtime::config::RedisConfig;
use crate::runtime::executor_utils::{ERROR_DETAIL_METADATA_KEY, decode_error_detail_from_raw};
use crate::runtime::otel::UDB_VERSION_HEADER;
use crate::runtime::system::ensure_system_catalog;

#[tokio::test]
#[ignore = "requires live Postgres + Redis; exercised by the native CI lane"]
async fn served_rate_limit_reports_operation_ceiling_and_principal_live() {
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
    const MSG: &str = "acme.dp.v1.Widget";
    const WINDOW_SECS: u64 = 3600;
    for distributed in [false, true] {
        let schema = format!("rate_limit_{}", Uuid::new_v4().simple());
        create_schema(&pool, &schema).await;
        sqlx::query(&format!(
            "CREATE TABLE \"{schema}\".widgets (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT)"
        ))
        .execute(&pool)
        .await
        .expect("rate-limit fixture");
        let tenant = Uuid::new_v4().to_string();
        let principal = Uuid::new_v4().to_string();
        let svc = dp_service_with(
            &pg,
            widget_manifest(&schema, "widgets", "Widget", false),
            |cfg| {
                // from_env also populates the named instance catalog. Remove
                // those Redis instances so the no-backend iteration actually
                // exercises the local fallback even in the full CI stack.
                cfg.backend_instances.instances.retain(|instance| {
                    instance.canonical_backend() != Some(crate::backend::BackendKind::Redis)
                });
                cfg.redis = distributed.then(|| RedisConfig {
                    dsn: Some(redis.clone()),
                    ..Default::default()
                });
                cfg.service.rate_limit_enabled = true;
                cfg.service.rate_limit_window_secs = WINDOW_SECS;
                cfg.service.rate_limit_max_per_window = 20;
                cfg.service
                    .rate_limit_max_per_operation
                    .insert("SELECT".into(), 2);
                cfg.service.rate_limit_failure_mode = "closed".into();
            },
        )
        .await;
        assert_eq!(svc.runtime_snapshot().redis_clone().is_some(), distributed);
        let (mut client, shutdown, task) = serve_data_broker(svc).await;
        let id = Uuid::new_v4().to_string();
        client
            .upsert(with_ctx(
                UpsertRequest {
                    message_type: MSG.into(),
                    record_json: serde_json::to_vec(&json!({"id": id, "status": "initial"}))
                        .unwrap(),
                    ..Default::default()
                },
                &tenant,
            ))
            .await
            .expect("served seed write");
        // Keep the assertion inside one fixed window even when CI happens to
        // start this proof immediately before its boundary.
        let seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let remaining = WINDOW_SECS - seconds % WINDOW_SECS;
        if remaining < 5 {
            tokio::time::sleep(Duration::from_secs(remaining + 1)).await;
        }
        let select = || {
            let mut req = with_ctx(
                SelectRequest {
                    message_type: MSG.into(),
                    fields: vec!["id".into(), "status".into()],
                    ..Default::default()
                },
                &tenant,
            );
            req.metadata_mut()
                .insert("x-user-id", principal.parse().unwrap());
            req
        };
        for _ in 0..2 {
            let response = client
                .select(select())
                .await
                .expect("operation budget allows two reads");
            assert_eq!(
                response
                    .metadata()
                    .get(UDB_VERSION_HEADER)
                    .unwrap()
                    .to_str()
                    .unwrap(),
                env!("CARGO_PKG_VERSION")
            );
        }
        let refused = client
            .select(select())
            .await
            .expect_err("third read exceeds the explicit Select ceiling");
        assert_eq!(refused.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            refused
                .metadata()
                .get(UDB_VERSION_HEADER)
                .unwrap()
                .to_str()
                .unwrap(),
            env!("CARGO_PKG_VERSION")
        );
        let detail = decode_error_detail_from_raw(
            refused
                .metadata()
                .get_bin(ERROR_DETAIL_METADATA_KEY)
                .expect("actual typed rate-limit response trailer"),
        );
        assert_eq!(detail.reason, "UDB_RATE_LIMITED");
        assert_eq!(detail.kind, ErrorKind::RateLimited as i32);
        assert_eq!(detail.missing.get("limit").map(String::as_str), Some("2"));
        assert_eq!(
            detail.missing.get("window").map(String::as_str),
            Some("3600s")
        );
        assert_eq!(detail.missing.get("principal"), Some(&principal));
        assert!(detail.missing["bucket"].starts_with(&format!("udb:ratelimit:{tenant}:Select:")));
        assert!(detail.retryable && detail.retry_after_ms > 0);
        assert!(detail.retry_after_ms <= (WINDOW_SECS * 1000) as i64);
        client
            .upsert(with_ctx(
                UpsertRequest {
                    message_type: MSG.into(),
                    record_json: serde_json::to_vec(&json!({"id": id, "status": "after-limit"}))
                        .unwrap(),
                    ..Default::default()
                },
                &tenant,
            ))
            .await
            .expect("another operation retains the tenant default budget");
        shutdown.send(()).expect("stop proof listener");
        task.await.expect("proof listener stopped");
        teardown(&pool, &schema, &tenant).await;
    }
}
