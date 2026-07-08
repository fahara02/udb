//! Live two-participant 2PC proof for master-plan 3.6.
//!
//! These tests intentionally sit behind `#[ignore]`: they require the
//! integration Postgres plus canonical MySQL stack, and Postgres must run with
//! `max_prepared_transactions > 0`.

use std::sync::Arc;
use std::time::Duration;

use sqlx::Row;
use udb::backend::BackendKind;
use udb::runtime::system::SystemCatalogConfig;
use udb::runtime::xa::{
    PrepareVote, XA_COMMIT_INTENT_REASON, XaCoordinator, XaDecision, XaLedgerEntry,
    XaMysqlParticipant, XaParticipant, XaParticipantHandle, XaRequest,
};
use udb::runtime::xa_postgres::{PostgresXaParticipant, list_udb_prepared_xacts};
use udb::runtime::xa_recovery::{
    InDoubtRegistry, MysqlInDoubtParticipant, RecoveryConfig, default_indoubt_registry,
    record_xa_ledger_entry, recover_xa_ledger_indoubt_with,
};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires integration Postgres + canonical MySQL with XA enabled"]
async fn clean_commit_commits_postgres_and_mysql_live() {
    let env = XaLiveEnv::new("clean").await;
    let request = request("clean");
    let pg = env.pg_participant("pg-clean");
    let mysql = env.mysql_participant("mysql-clean");

    let outcome = XaCoordinator::execute_with_write_ahead(
        request,
        vec![Box::new(pg), Box::new(mysql)],
        &capability_matrix(),
        |entry| {
            let pool = env.pg.clone();
            let config = env.system_config.clone();
            async move { record_xa_ledger_entry(&pool, &config, &entry).await }
        },
    )
    .await
    .expect("clean two-participant commit");

    assert_eq!(outcome.ledger.decision, XaDecision::Committed);
    assert!(outcome.participants.iter().all(|p| p.committed));
    record_xa_ledger_entry(&env.pg, &env.system_config, &outcome.ledger)
        .await
        .expect("record terminal XA ledger row");
    assert_eq!(env.pg_value().await, Some("pg-clean".to_string()));
    assert_eq!(env.mysql_value().await, Some("mysql-clean".to_string()));
    assert_eq!(env.ledger_decision(&outcome.ledger.xid).await, "committed");
    assert!(
        !list_udb_prepared_xacts(&env.pg)
            .await
            .unwrap()
            .contains(&outcome.ledger.xid),
        "clean commit must leave no PG prepared xact for its xid"
    );
    env.cleanup().await;
}

#[tokio::test]
#[ignore = "requires integration Postgres + canonical MySQL with XA enabled"]
async fn recovery_commits_after_write_ahead_before_any_phase2_commit_live() {
    let env = XaLiveEnv::new("after_write_ahead").await;
    let xid = XaCoordinator::new_xid();
    let pg = env.pg_participant("pg-recovery");
    let mysql = env.mysql_participant("mysql-recovery");

    assert_eq!(pg.prepare(&xid).await, PrepareVote::Prepared);
    assert_eq!(mysql.prepare(&xid).await, PrepareVote::Prepared);
    record_commit_intent(&env, &xid).await;

    assert_eq!(run_recovery(&env).await, 1);
    assert_eq!(env.ledger_decision(&xid).await, "committed");
    assert_eq!(env.pg_value().await, Some("pg-recovery".to_string()));
    assert_eq!(env.mysql_value().await, Some("mysql-recovery".to_string()));
    env.cleanup().await;
}

#[tokio::test]
#[ignore = "requires integration Postgres + canonical MySQL with XA enabled"]
async fn participant_prepare_failure_rolls_back_prepared_postgres_live() {
    let env = XaLiveEnv::new("prepare_fail").await;
    let request = request("prepare_fail");
    let pg = env.pg_participant("pg-should-rollback");
    let mysql = XaMysqlParticipant::new(
        "primary",
        env.mysql.clone(),
        vec![(
            format!(
                "INSERT INTO `{}`.`missing_table` (id) VALUES (?)",
                env.mysql_db
            ),
            vec![serde_json::json!(env.row_id)],
        )],
    );

    let err = XaCoordinator::execute_with_write_ahead(
        request,
        vec![Box::new(pg), Box::new(mysql)],
        &capability_matrix(),
        |entry| {
            let pool = env.pg.clone();
            let config = env.system_config.clone();
            async move { record_xa_ledger_entry(&pool, &config, &entry).await }
        },
    )
    .await
    .expect_err("MySQL prepare failure must abort the 2PC");

    let failed_xid = match err {
        udb::runtime::xa::XaError::PrepareFailed { ledger, .. } => {
            assert_eq!(ledger.decision, XaDecision::RolledBack);
            assert!(
                ledger.reason.contains("XA statement failed"),
                "unexpected abort reason: {}",
                ledger.reason
            );
            ledger.xid
        }
        other => panic!("expected prepare failure, got {other:?}"),
    };
    assert_eq!(env.pg_value().await, None);
    assert_eq!(env.mysql_value().await, None);
    assert!(
        !list_udb_prepared_xacts(&env.pg)
            .await
            .unwrap()
            .contains(&failed_xid),
        "prepare failure must roll back the already-prepared PG participant"
    );
    env.cleanup().await;
}

#[tokio::test]
#[ignore = "requires integration Postgres + canonical MySQL with XA enabled"]
async fn recovery_commits_mysql_after_mid_phase2_pg_commit_live() {
    let env = XaLiveEnv::new("mid_phase2").await;
    let xid = XaCoordinator::new_xid();
    let pg = env.pg_participant("pg-phase2");
    let mysql = env.mysql_participant("mysql-phase2");

    assert_eq!(pg.prepare(&xid).await, PrepareVote::Prepared);
    assert_eq!(mysql.prepare(&xid).await, PrepareVote::Prepared);
    record_commit_intent(&env, &xid).await;
    pg.commit_prepared(&xid)
        .await
        .expect("phase-2 committed Postgres before crash");

    assert_eq!(env.pg_value().await, Some("pg-phase2".to_string()));
    assert_eq!(env.mysql_value().await, None);
    assert_eq!(run_recovery(&env).await, 1);
    assert_eq!(env.ledger_decision(&xid).await, "committed");
    assert_eq!(env.mysql_value().await, Some("mysql-phase2".to_string()));
    assert_eq!(
        run_recovery(&env).await,
        0,
        "XA phase-2 crash recovery must be idempotent after the first pass"
    );
    assert!(
        !list_udb_prepared_xacts(&env.pg)
            .await
            .unwrap()
            .contains(&xid),
        "XA phase-2 crash recovery must not leave the recovered xid prepared"
    );
    env.cleanup().await;
}

struct XaLiveEnv {
    pg: sqlx::PgPool,
    mysql: sqlx::MySqlPool,
    mysql_admin: sqlx::MySqlPool,
    pg_schema: String,
    mysql_db: String,
    row_id: String,
    system_config: SystemCatalogConfig,
}

impl XaLiveEnv {
    async fn new(prefix: &str) -> Self {
        let suffix = Uuid::new_v4().simple().to_string();
        let pg_schema = format!("udb_xa_{prefix}_{suffix}");
        let mysql_db = format!("udb_xa_{prefix}_{suffix}");
        let system_schema = format!("udb_xa_sys_{prefix}_{suffix}");
        let row_id = format!("row_{suffix}");

        let pg = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&pg_dsn())
            .await
            .expect("connect integration Postgres");
        assert_pg_prepared_transactions_enabled(&pg).await;
        sqlx::query(&format!("CREATE SCHEMA \"{pg_schema}\""))
            .execute(&pg)
            .await
            .expect("create PG data schema");
        sqlx::query(&format!("CREATE SCHEMA \"{system_schema}\""))
            .execute(&pg)
            .await
            .expect("create PG system schema");
        sqlx::query(&format!(
            "CREATE TABLE \"{pg_schema}\".xa_items (id text PRIMARY KEY, value text NOT NULL)"
        ))
        .execute(&pg)
        .await
        .expect("create PG XA table");

        let mysql_admin = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&mysql_dsn())
            .await
            .expect("connect MySQL admin database");
        sqlx::query(&format!("CREATE DATABASE `{mysql_db}`"))
            .execute(&mysql_admin)
            .await
            .expect("create MySQL XA database");
        let mysql = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&swap_database(&mysql_dsn(), &mysql_db))
            .await
            .expect("connect MySQL XA database");
        sqlx::query(&format!(
            "CREATE TABLE `{mysql_db}`.`xa_items` (id varchar(191) PRIMARY KEY, value text NOT NULL)"
        ))
        .execute(&mysql)
        .await
        .expect("create MySQL XA table");

        Self {
            pg,
            mysql,
            mysql_admin,
            pg_schema,
            mysql_db,
            row_id,
            system_config: SystemCatalogConfig::with_schema(&system_schema),
        }
    }

    fn pg_participant(&self, value: &str) -> PostgresXaParticipant {
        PostgresXaParticipant::new(
            XaParticipantHandle::new("postgres", "primary"),
            self.pg.clone(),
            vec![format!(
                "INSERT INTO \"{}\".xa_items (id, value) VALUES ('{}', '{}')",
                self.pg_schema, self.row_id, value
            )],
        )
    }

    fn mysql_participant(&self, value: &str) -> XaMysqlParticipant {
        XaMysqlParticipant::new(
            "primary",
            self.mysql.clone(),
            vec![(
                format!(
                    "INSERT INTO `{}`.`xa_items` (id, value) VALUES (?, ?)",
                    self.mysql_db
                ),
                vec![serde_json::json!(self.row_id), serde_json::json!(value)],
            )],
        )
    }

    async fn pg_value(&self) -> Option<String> {
        sqlx::query(&format!(
            "SELECT value FROM \"{}\".xa_items WHERE id = $1",
            self.pg_schema
        ))
        .bind(&self.row_id)
        .fetch_optional(&self.pg)
        .await
        .expect("select PG XA row")
        .map(|row| row.try_get("value").expect("PG value"))
    }

    async fn mysql_value(&self) -> Option<String> {
        sqlx::query(&format!(
            "SELECT value FROM `{}`.`xa_items` WHERE id = ?",
            self.mysql_db
        ))
        .bind(&self.row_id)
        .fetch_optional(&self.mysql)
        .await
        .expect("select MySQL XA row")
        .map(|row| row.try_get("value").expect("MySQL value"))
    }

    async fn ledger_decision(&self, xid: &str) -> String {
        let relation = format!(
            "\"{}\".\"{}\"",
            self.system_config.cdc.system_schema, self.system_config.xa_ledger_table
        );
        sqlx::query(&format!("SELECT decision FROM {relation} WHERE xid = $1"))
            .bind(xid)
            .fetch_one(&self.pg)
            .await
            .expect("select XA ledger decision")
            .try_get("decision")
            .expect("decision")
    }

    async fn cleanup(&self) {
        let _ = sqlx::query(&format!(
            "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
            self.pg_schema
        ))
        .execute(&self.pg)
        .await;
        let _ = sqlx::query(&format!(
            "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
            self.system_config.cdc.system_schema
        ))
        .execute(&self.pg)
        .await;
        self.mysql.close().await;
        let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS `{}`", self.mysql_db))
            .execute(&self.mysql_admin)
            .await;
    }
}

impl Drop for XaLiveEnv {
    fn drop(&mut self) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let pg = self.pg.clone();
        let mysql_admin = self.mysql_admin.clone();
        let pg_schema = self.pg_schema.clone();
        let system_schema = self.system_config.cdc.system_schema.clone();
        let mysql_db = self.mysql_db.clone();
        handle.spawn(async move {
            let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{pg_schema}\" CASCADE"))
                .execute(&pg)
                .await;
            let _ = sqlx::query(&format!(
                "DROP SCHEMA IF EXISTS \"{system_schema}\" CASCADE"
            ))
            .execute(&pg)
            .await;
            let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS `{mysql_db}`"))
                .execute(&mysql_admin)
                .await;
        });
    }
}

fn request(correlation: &str) -> XaRequest {
    XaRequest {
        tenant_id: "tenant-a".to_string(),
        project_id: "billing".to_string(),
        origin_rpc: "tests/ha/xa_two_participant".to_string(),
        correlation_id: correlation.to_string(),
    }
}

fn pg_dsn() -> String {
    std::env::var("UDB_INTEGRATION_PG_DSN")
        .or_else(|_| std::env::var("UDB_PG_DSN"))
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgres://udb:udb@localhost:55432/udb".to_string())
}

fn mysql_dsn() -> String {
    std::env::var("UDB_MYSQL_DSN")
        .unwrap_or_else(|_| "mysql://udb:udb@127.0.0.1:53306/udb".to_string())
}

fn swap_database(dsn: &str, new_db: &str) -> String {
    match dsn.rsplit_once('/') {
        Some((prefix, _db)) => format!("{prefix}/{new_db}"),
        None => format!("{dsn}/{new_db}"),
    }
}

fn capability_matrix() -> Vec<udb::backend::BackendCapabilityMatrixEntry> {
    vec![
        BackendKind::Postgres.capability_matrix_entry(),
        BackendKind::Mysql.capability_matrix_entry(),
    ]
}

async fn assert_pg_prepared_transactions_enabled(pool: &sqlx::PgPool) {
    let value: String = sqlx::query("SHOW max_prepared_transactions")
        .fetch_one(pool)
        .await
        .expect("SHOW max_prepared_transactions")
        .try_get(0)
        .expect("max_prepared_transactions value");
    let configured = value
        .parse::<u32>()
        .expect("max_prepared_transactions is numeric");
    assert!(
        configured > 0,
        "Postgres 2PC requires max_prepared_transactions > 0"
    );
}

async fn record_commit_intent(env: &XaLiveEnv, xid: &str) {
    let entry = XaLedgerEntry::new(
        xid,
        "tenant-a",
        "billing",
        "tests/ha/xa_two_participant",
        "manual-phase",
        vec!["postgres:primary".to_string(), "mysql:primary".to_string()],
        XaDecision::InDoubt,
    )
    .with_reason(XA_COMMIT_INTENT_REASON);
    record_xa_ledger_entry(&env.pg, &env.system_config, &entry)
        .await
        .expect("record XA commit-intent ledger row");
}

async fn run_recovery(env: &XaLiveEnv) -> u64 {
    let mut registry: InDoubtRegistry = default_indoubt_registry(&env.pg);
    registry.register(Arc::new(MysqlInDoubtParticipant {
        label: "mysql:primary".to_string(),
        pool: env.mysql.clone(),
    }));
    recover_xa_ledger_indoubt_with(
        &env.pg,
        &env.system_config,
        &registry,
        &RecoveryConfig {
            interval: Duration::from_secs(1),
            max_attempts: 1,
        },
    )
    .await
    .expect("run XA in-doubt recovery")
}
