//! Live Postgres tests of the monthly-partitioned auth audit log: a new log is
//! created partitioned and a write lands in its month; an unpartitioned log is
//! converted online while it is written to, keeping every row readable
//! through the parent along with its grants, row-level security and policy;
//! the cleanup pass drops an expired UDB-named month, but not one holding
//! rows the evidence export has not exported yet.
//!
//! Each test works in its own throwaway schema (the sink's `with_relation`),
//! never on the shared `udb_system.auth_audit_log`.
//!
//! Run: UDB_LIVE_AUTH_PG_DSN=... cargo test --lib live_postgres_auth_audit_log -- --ignored --nocapture

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::*;
use crate::runtime::cleanup::{PassMode, Retention, RowGuard, Target, TargetKind, maintain_target};
use crate::runtime::service::auth_service::audit_export::{AuditExportSink, PostgresAuditLogSink};

async fn live_pool() -> PgPool {
    let dsn = std::env::var("UDB_LIVE_AUTH_PG_DSN")
        .or_else(|_| std::env::var("UDB_INTEGRATION_PG_DSN"))
        .unwrap_or_else(|_| "postgres://udb:udb@127.0.0.1:55432/udb".to_string());
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(6)
        .connect(&dsn)
        .await
        .unwrap_or_else(|err| panic!("connect live postgres at {dsn}: {err}"))
}

async fn run(pool: &PgPool, sql: &str) {
    sqlx::Executor::execute(pool, sqlx::raw_sql(sql))
        .await
        .unwrap_or_else(|err| panic!("{sql}: {err}"));
}

async fn fresh_schema(pool: &PgPool) -> String {
    let schema = format!("udb_auditpart_{}", uuid::Uuid::new_v4().simple());
    run(pool, &format!("CREATE SCHEMA {schema}")).await;
    schema
}

async fn exists(pool: &PgPool, relation: &str) -> bool {
    sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(relation)
        .fetch_one(pool)
        .await
        .expect("to_regclass")
}

async fn count(pool: &PgPool, sql: &str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
}

async fn text(pool: &PgPool, sql: &str, relation: &str) -> String {
    sqlx::query_scalar(sql)
        .bind(relation)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
}

/// The partitions the rows with `operation` sit in.
async fn homes(pool: &PgPool, relation: &str, operation: &str) -> Vec<String> {
    sqlx::query_scalar(&format!(
        "SELECT DISTINCT c.relname::text FROM {relation} a JOIN pg_class c ON c.oid = a.tableoid \
         WHERE a.operation = $1 ORDER BY 1"
    ))
    .bind(operation)
    .fetch_all(pool)
    .await
    .expect("read where the rows are")
}

async fn primary_key(pool: &PgPool, relation: &str) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT ARRAY(SELECT a.attname::text \
                      FROM unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) \
                      JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum \
                      ORDER BY k.ord) \
         FROM pg_constraint con WHERE con.conrelid = $1::regclass AND con.contype = 'p'",
    )
    .bind(relation)
    .fetch_one(pool)
    .await
    .expect("read the primary key")
}

async fn default_partition(pool: &PgPool, relation: &str) -> String {
    text(
        pool,
        "SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = $1::regclass AND pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT'",
        relation,
    )
    .await
}

fn envelope(operation: &str) -> serde_json::Value {
    serde_json::json!({
        "event_id": uuid::Uuid::new_v4().to_string(),
        "actor_tenant": "tenant-a",
        "actor": "tester",
        "operation": operation,
    })
}

fn audit_target(schema: &str, evidence_state_key: Option<String>) -> Target {
    Target {
        schema: schema.to_string(),
        table: "auth_audit_log".to_string(),
        service: "compliance".to_string(),
        declared: None,
        kept_class: None,
        default_retention: None,
        capped: true,
        periodic: false,
        kind: TargetKind::Rows {
            column: "occurred_at".to_string(),
            evidence_state_key,
            only_when: None,
            guard: RowGuard::Unguarded,
            monthly_partitions: true,
        },
    }
}

/// A new log is partitioned: key `(audit_id, occurred_at)`, a default
/// partition, the current and next month; a write lands in its month, and a
/// second ensure on the partitioned table changes nothing.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_PG_DSN=... cargo test --lib live_postgres_auth_audit_log_is_created_partitioned -- --ignored --nocapture"]
async fn live_postgres_auth_audit_log_is_created_partitioned() {
    let pool = live_pool().await;
    let schema = fresh_schema(&pool).await;
    let relation = format!("{schema}.auth_audit_log");
    let sink = PostgresAuditLogSink::new(pool.clone()).with_relation(&relation);
    for i in 0..3 {
        sink.export("udb.test.partitioned", &envelope("fresh"))
            .await
            .unwrap_or_else(|err| panic!("export {i}: {err}"));
    }
    assert_eq!(
        text(
            &pool,
            "SELECT relkind::text FROM pg_class WHERE oid = $1::regclass",
            &relation
        )
        .await,
        "p",
        "a new log is partitioned"
    );
    assert_eq!(
        default_partition(&pool, &relation).await,
        "auth_audit_log_default"
    );
    assert_eq!(
        primary_key(&pool, &relation).await,
        ["audit_id", "occurred_at"]
    );
    let (current, next): (String, String) = sqlx::query_as(
        "SELECT 'auth_audit_log_p_' || to_char(now(), 'YYYYMM'), \
                'auth_audit_log_p_' || to_char(date_trunc('month', now()) + interval '1 month', 'YYYYMM')",
    )
    .fetch_one(&pool)
    .await
    .expect("month names");
    assert_eq!(
        homes(&pool, &relation, "fresh").await,
        [current.clone()],
        "writes land in the current month's partition"
    );
    assert!(exists(&pool, &format!("{schema}.{next}")).await);
    assert_eq!(
        count(
            &pool,
            &format!("SELECT count(*) FROM {relation} WHERE operation = 'fresh'")
        )
        .await,
        3
    );
    PostgresAuditLogSink::new(pool.clone())
        .with_relation(&relation)
        .ensure_table()
        .await
        .expect("ensuring a partitioned log again is a no-op");
    run(&pool, &format!("DROP SCHEMA {schema} CASCADE")).await;
}

/// An unpartitioned log of an older release, with old rows, a grant,
/// row-level security, a policy and a comment, written to before and during
/// the conversion: afterwards every row is readable through the partitioned
/// parent, the old table is its default partition, the grant, flags, policy
/// and comment are on the parent, the month from the floor on exists, new
/// writes land, a second conversion changes nothing, and the cleanup pass
/// deletes the expired rows from the old table without dropping it.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_PG_DSN=... cargo test --lib live_postgres_auth_audit_log_converts_online -- --ignored --nocapture"]
async fn live_postgres_auth_audit_log_converts_online_keeping_every_row() {
    let pool = live_pool().await;
    let schema = fresh_schema(&pool).await;
    let relation = format!("{schema}.auth_audit_log");
    // The table as releases before 0.6.22 created it.
    run(
        &pool,
        &format!(
            "CREATE TABLE {relation} ( \
                 audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), \
                 event_id UUID NOT NULL, \
                 event_type VARCHAR(200) NOT NULL, \
                 tenant_id VARCHAR(64) NOT NULL, \
                 actor VARCHAR(200) NOT NULL DEFAULT '', \
                 target_resource VARCHAR(400) NOT NULL DEFAULT '', \
                 operation VARCHAR(80) NOT NULL DEFAULT '', \
                 outcome VARCHAR(40) NOT NULL DEFAULT '', \
                 reason_code VARCHAR(200) NOT NULL DEFAULT '', \
                 correlation_id VARCHAR(120) NOT NULL DEFAULT '', \
                 trace_id VARCHAR(64) NOT NULL DEFAULT '', \
                 decision_id VARCHAR(120) NOT NULL DEFAULT '', \
                 source_ip VARCHAR(64) NOT NULL DEFAULT '', \
                 redaction_version VARCHAR(16) NOT NULL DEFAULT '', \
                 envelope JSONB NOT NULL, \
                 occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW() \
             )"
        ),
    )
    .await;
    run(
        &pool,
        &format!("CREATE INDEX auth_audit_log_occurred_at_idx ON {relation} (occurred_at)"),
    )
    .await;
    run(
        &pool,
        &format!(
            "INSERT INTO {relation} (event_id, event_type, tenant_id, operation, envelope, occurred_at) \
             SELECT gen_random_uuid(), 'udb.test.old', 'tenant-a', 'old', '{{}}'::jsonb, \
                    now() - interval '400 days' \
             FROM generate_series(1, 50)"
        ),
    )
    .await;
    // Written by the sink while the table is still plain.
    let sink = PostgresAuditLogSink::new(pool.clone()).with_relation(&relation);
    for _ in 0..5 {
        sink.export("udb.test.before", &envelope("before"))
            .await
            .expect("a write before the conversion");
    }
    assert_eq!(
        text(
            &pool,
            "SELECT relkind::text FROM pg_class WHERE oid = $1::regclass",
            &relation
        )
        .await,
        "r",
        "the sink never converts an existing plain log"
    );
    run(&pool, &format!("GRANT SELECT ON {relation} TO PUBLIC")).await;
    run(
        &pool,
        &format!("ALTER TABLE {relation} ENABLE ROW LEVEL SECURITY"),
    )
    .await;
    run(
        &pool,
        &format!("ALTER TABLE {relation} FORCE ROW LEVEL SECURITY"),
    )
    .await;
    run(
        &pool,
        &format!(
            "CREATE POLICY tenant_scope ON {relation} USING (tenant_id <> '' \
             OR current_setting('app.platform_admin', true) = 'true')"
        ),
    )
    .await;
    run(
        &pool,
        &format!("COMMENT ON TABLE {relation} IS 'durable auth audit log'"),
    )
    .await;

    // A writer keeps writing through the whole conversion.
    let stop = Arc::new(AtomicBool::new(false));
    let writer = tokio::spawn({
        let pool = pool.clone();
        let relation = relation.clone();
        let stop = stop.clone();
        async move {
            let sink = PostgresAuditLogSink::new(pool).with_relation(relation);
            let mut written = 0i64;
            while !stop.load(Ordering::SeqCst) {
                sink.export("udb.test.during", &envelope("during"))
                    .await
                    .expect("a write during the conversion lands");
                written += 1;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            written
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let done = convert_online(&pool, &schema, "auth_audit_log")
        .await
        .expect("the conversion");
    tokio::time::sleep(Duration::from_millis(100)).await;
    stop.store(true, Ordering::SeqCst);
    let during = writer.await.expect("writer task");
    assert!(during > 0, "the writer wrote during the conversion");
    eprintln!("conversion: {}", done.to_json());
    assert!(!done.already_partitioned);
    assert_eq!(
        done.legacy_partition.as_deref(),
        Some(format!("{schema}.auth_audit_log_legacy").as_str())
    );

    // Every row, through the parent.
    assert_eq!(
        text(
            &pool,
            "SELECT relkind::text FROM pg_class WHERE oid = $1::regclass",
            &relation
        )
        .await,
        "p"
    );
    assert_eq!(
        default_partition(&pool, &relation).await,
        "auth_audit_log_legacy"
    );
    let by_operation = |operation: &'static str| {
        let pool = pool.clone();
        let relation = relation.clone();
        async move {
            count(
                &pool,
                &format!("SELECT count(*) FROM {relation} WHERE operation = '{operation}'"),
            )
            .await
        }
    };
    assert_eq!(by_operation("old").await, 50);
    assert_eq!(by_operation("before").await, 5);
    assert_eq!(by_operation("during").await, during);
    assert_eq!(
        primary_key(&pool, &relation).await,
        ["audit_id", "occurred_at"]
    );
    assert_eq!(
        primary_key(&pool, &format!("{schema}.auth_audit_log_legacy")).await,
        ["audit_id", "occurred_at"]
    );

    // The grant, row-level security, policy and comment are the parent's.
    assert_eq!(
        count(
            &pool,
            &format!(
                "SELECT count(*) FROM pg_class c, aclexplode(c.relacl) a \
                 WHERE c.oid = '{relation}'::regclass AND a.grantee = 0 AND a.privilege_type = 'SELECT'"
            )
        )
        .await,
        1,
        "PUBLIC keeps SELECT"
    );
    let (enabled, forced): (bool, bool) = sqlx::query_as(
        "SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE oid = $1::regclass",
    )
    .bind(&relation)
    .fetch_one(&pool)
    .await
    .expect("read the parent's row-level security");
    assert!(enabled && forced);
    let quals: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT tablename::text, qual FROM pg_policies \
         WHERE schemaname = $1 AND policyname = 'tenant_scope' ORDER BY 1",
    )
    .bind(&schema)
    .fetch_all(&pool)
    .await
    .expect("read the policies");
    let tables: Vec<&str> = quals.iter().map(|(table, _)| table.as_str()).collect();
    assert!(tables.contains(&"auth_audit_log"), "{quals:?}");
    assert!(tables.contains(&"auth_audit_log_legacy"), "{quals:?}");
    assert!(
        quals.iter().all(|(_, qual)| *qual == quals[0].1),
        "the same policy everywhere: {quals:?}"
    );
    assert_eq!(
        text(
            &pool,
            "SELECT obj_description($1::regclass, 'pg_class')",
            &relation
        )
        .await,
        "durable auth audit log"
    );

    // The first month starts at the floor, the month after next.
    let first_month: String = sqlx::query_scalar(
        "SELECT to_char(date_trunc('month', now()) + interval '2 months', 'YYYYMM')",
    )
    .fetch_one(&pool)
    .await
    .expect("floor month");
    assert!(exists(&pool, &format!("{schema}.auth_audit_log_p_{first_month}")).await);

    // New writes land, in the old table until the floor.
    let after = PostgresAuditLogSink::new(pool.clone()).with_relation(&relation);
    for _ in 0..3 {
        after
            .export("udb.test.after", &envelope("after"))
            .await
            .expect("a write after the conversion");
    }
    assert_eq!(
        homes(&pool, &relation, "after").await,
        ["auth_audit_log_legacy".to_string()]
    );

    // Converting again changes nothing.
    let again = convert_online(&pool, &schema, "auth_audit_log")
        .await
        .expect("a second conversion");
    assert!(again.already_partitioned);

    // The cleanup pass trims the old rows out of the old table, which stays.
    let month = Duration::from_secs(30 * 86_400);
    let swept = maintain_target(
        &pool,
        &audit_target(&schema, None),
        Ok(Retention {
            rows: Some(month),
            kept: Some(month),
        }),
        PassMode {
            batch: 5_000,
            drain: true,
            sweep_partitions: false,
        },
    )
    .await
    .expect("cleanup on the converted log");
    assert_eq!(swept.dropped, 0, "the default partition is never dropped");
    assert_eq!(swept.deleted, 50, "only the 400-day-old rows go");
    assert!(exists(&pool, &format!("{schema}.auth_audit_log_legacy")).await);
    assert_eq!(by_operation("old").await, 0);
    assert_eq!(by_operation("before").await, 5);
    run(&pool, &format!("DROP SCHEMA {schema} CASCADE")).await;
}

/// The cleanup pass drops a UDB-named month whose rows are all past
/// retention, but with the evidence export on only once they were all
/// exported: a month holding a row past the export's high-water mark stays,
/// with that row. The default partition is never dropped.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_PG_DSN=... cargo test --lib live_postgres_auth_audit_log_cleanup -- --ignored --nocapture"]
async fn live_postgres_auth_audit_log_cleanup_drops_only_exported_expired_months() {
    let pool = live_pool().await;
    let schema = fresh_schema(&pool).await;
    let relation = format!("{schema}.auth_audit_log");
    PostgresAuditLogSink::new(pool.clone())
        .with_relation(&relation)
        .ensure_table()
        .await
        .expect("create the partitioned log");
    for (suffix, from, to) in [
        ("202001", "2020-01-01", "2020-02-01"),
        ("202002", "2020-02-01", "2020-03-01"),
    ] {
        run(
            &pool,
            &format!(
                "CREATE TABLE {schema}.auth_audit_log_p_{suffix} PARTITION OF {relation} \
                 FOR VALUES FROM ('{from}') TO ('{to}')"
            ),
        )
        .await;
    }
    run(
        &pool,
        &format!(
            "INSERT INTO {relation} (event_id, event_type, tenant_id, operation, envelope, occurred_at) VALUES \
             (gen_random_uuid(), 'udb.test', 'tenant-a', 'january', '{{}}'::jsonb, '2020-01-15'), \
             (gen_random_uuid(), 'udb.test', 'tenant-a', 'february', '{{}}'::jsonb, '2020-02-15')"
        ),
    )
    .await;
    crate::runtime::evidence_export::ensure_state_table(&pool)
        .await
        .expect("evidence export state table");
    // Exported up to 2020-02-10: all of January, part of February.
    let key = format!("udb:test:audit-partition:{schema}");
    sqlx::query(
        "INSERT INTO udb_system.evidence_export_state (worker, last_occurred_at) \
         VALUES ($1, '2020-02-10'::timestamptz) \
         ON CONFLICT (worker) DO UPDATE SET last_occurred_at = EXCLUDED.last_occurred_at",
    )
    .bind(&key)
    .execute(&pool)
    .await
    .expect("set the export's high-water mark");
    let month = Duration::from_secs(30 * 86_400);
    let retention = Retention {
        rows: Some(month),
        kept: Some(month),
    };
    let scheduled = PassMode {
        batch: 5_000,
        drain: false,
        sweep_partitions: false,
    };

    let guarded = maintain_target(
        &pool,
        &audit_target(&schema, Some(key.clone())),
        Ok(retention),
        scheduled,
    )
    .await
    .expect("cleanup with the evidence export on");
    assert_eq!(guarded.dropped, 1, "only the fully exported month goes");
    assert!(!exists(&pool, &format!("{schema}.auth_audit_log_p_202001")).await);
    assert!(exists(&pool, &format!("{schema}.auth_audit_log_p_202002")).await);
    assert_eq!(
        count(
            &pool,
            &format!("SELECT count(*) FROM {relation} WHERE operation = 'february'")
        )
        .await,
        1,
        "the unexported row is kept"
    );
    assert!(exists(&pool, &format!("{schema}.auth_audit_log_default")).await);

    // With the export off, the expired month goes too.
    let open = maintain_target(
        &pool,
        &audit_target(&schema, None),
        Ok(retention),
        scheduled,
    )
    .await
    .expect("cleanup with the evidence export off");
    assert_eq!(open.dropped, 1);
    assert!(!exists(&pool, &format!("{schema}.auth_audit_log_p_202002")).await);
    assert!(exists(&pool, &format!("{schema}.auth_audit_log_default")).await);

    sqlx::query("DELETE FROM udb_system.evidence_export_state WHERE worker = $1")
        .bind(&key)
        .execute(&pool)
        .await
        .expect("remove the test's export state");
    run(&pool, &format!("DROP SCHEMA {schema} CASCADE")).await;
}
