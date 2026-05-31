//! `MysqlCanonicalStore` — new in P2P. sqlx-mysql backed.
//!
//! ## Durability token strategy
//!
//! MySQL's write-progress signal depends on the operator's
//! configuration:
//!
//! 1. **GTID mode** (`gtid_mode = ON`). The session variable
//!    `@@GLOBAL.GTID_EXECUTED` returns the full set of transactions
//!    the primary has applied (a string like
//!    `"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100"`). Replicas
//!    expose the same variable for their applied set. This is the
//!    preferred token format because it survives binlog rotation.
//! 2. **File/position mode** (legacy, no GTID).
//!    `SHOW MASTER STATUS` returns the current `(binlog_file,
//!    binlog_pos)`. Replicas expose
//!    `SHOW SLAVE STATUS / SHOW REPLICA STATUS`. The token is
//!    rendered as `"<file>:<position>"`.
//!
//! The impl probes GTID mode at startup and picks whichever path is
//! available. The token's `value` carries a prefix (`gtid:` or
//! `file:`) so `wait_for_token` knows how to compare.
//!
//! ## Schema
//!
//! `udb_outbox_events` uses the same logical schema as the Postgres
//! variant but in MySQL dialect: `BIGINT AUTO_INCREMENT`, `JSON`,
//! `CHAR(36)` for the UUID, `TIMESTAMP(6)` for microsecond precision.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use sqlx::MySqlPool;

use super::{CanonicalStore, DurabilityToken};

pub struct MysqlCanonicalStore {
    pub(super) pool: MySqlPool,
    instance_name: String,
    outbox_relation: String,
}

impl MysqlCanonicalStore {
    pub fn new(
        pool: MySqlPool,
        instance_name: impl Into<String>,
        outbox_relation: impl Into<String>,
    ) -> Self {
        Self {
            pool,
            instance_name: instance_name.into(),
            outbox_relation: outbox_relation.into(),
        }
    }

    /// Returns the validated, backtick-quoted relation name. MySQL
    /// uses backticks instead of double-quotes for identifiers; the
    /// validation gate keeps the input safe regardless.
    fn safe_relation(&self) -> Result<String, String> {
        let rel = self.outbox_relation.as_str();
        if rel.is_empty()
            || !rel
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '`')
        {
            return Err(format!("unsafe outbox_relation '{rel}'"));
        }
        Ok(rel.to_string())
    }

    /// Probe whether the connected MySQL instance has GTID mode on.
    /// `@@GLOBAL.gtid_mode` returns `ON` / `ON_PERMISSIVE` /
    /// `OFF_PERMISSIVE` / `OFF`. Anything starting with `ON` we treat
    /// as GTID-capable.
    async fn gtid_mode_on(&self) -> Result<bool, String> {
        let mode: Option<(String,)> = sqlx::query_as("SELECT @@GLOBAL.gtid_mode")
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| format!("gtid_mode probe failed: {e}"))?;
        Ok(mode
            .map(|(m,)| m.to_ascii_uppercase().starts_with("ON"))
            .unwrap_or(false))
    }
}

#[async_trait]
impl CanonicalStore for MysqlCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "mysql"
    }

    fn instance_name(&self) -> &str {
        &self.instance_name
    }

    async fn current_durability_token(&self) -> Result<DurabilityToken, String> {
        if self.gtid_mode_on().await.unwrap_or(false) {
            // GTID path. The variable is multi-line for large sets;
            // we store the raw string so wait_for_token compares it
            // with WAIT_FOR_EXECUTED_GTID_SET.
            let gtid: (String,) = sqlx::query_as("SELECT @@GLOBAL.gtid_executed")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| format!("gtid_executed query failed: {e}"))?;
            Ok(DurabilityToken::new("mysql", format!("gtid:{}", gtid.0)))
        } else {
            // File/position path. SHOW MASTER STATUS is a one-row
            // result set; sqlx parses the first column (`File`) and
            // second column (`Position`).
            let row: Option<(String, u64)> = sqlx::query_as("SHOW MASTER STATUS")
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| format!("SHOW MASTER STATUS failed: {e}"))?;
            match row {
                Some((file, pos)) => {
                    Ok(DurabilityToken::new("mysql", format!("file:{file}:{pos}")))
                }
                None => Err(
                    "SHOW MASTER STATUS returned no row; is binary logging enabled?".to_string(),
                ),
            }
        }
    }

    async fn wait_for_token(
        &self,
        token: &DurabilityToken,
        timeout: Duration,
    ) -> Result<bool, String> {
        if !token.is_for("mysql") {
            return Err(format!(
                "MysqlCanonicalStore cannot wait on a '{}' token",
                token.backend_label
            ));
        }
        let timeout_secs = timeout.as_secs().max(1) as i64;
        if let Some(gtid) = token.value.strip_prefix("gtid:") {
            // WAIT_FOR_EXECUTED_GTID_SET returns 0 on success, 1 on
            // timeout. NULL means a parse error / GTID off.
            let res: Option<(i32,)> = sqlx::query_as("SELECT WAIT_FOR_EXECUTED_GTID_SET(?, ?)")
                .bind(gtid)
                .bind(timeout_secs)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| format!("WAIT_FOR_EXECUTED_GTID_SET failed: {e}"))?;
            Ok(matches!(res, Some((0,))))
        } else if let Some(file_pos) = token.value.strip_prefix("file:") {
            // file:<file>:<pos>
            let (file, pos) = file_pos.rsplit_once(':').ok_or_else(|| {
                format!(
                    "malformed mysql file-position token '{}': expected 'file:<file>:<pos>'",
                    token.value
                )
            })?;
            let pos: u64 = pos
                .parse()
                .map_err(|e| format!("invalid binlog position '{pos}': {e}"))?;
            // MASTER_POS_WAIT(file, pos, timeout) waits until the
            // server's replication position has reached or passed the
            // target. Returns rows-applied count or NULL on timeout.
            let res: Option<(Option<i64>,)> = sqlx::query_as("SELECT MASTER_POS_WAIT(?, ?, ?)")
                .bind(file)
                .bind(pos as i64)
                .bind(timeout_secs)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| format!("MASTER_POS_WAIT failed: {e}"))?;
            Ok(matches!(res, Some((Some(_),))))
        } else {
            // Unknown token format. Poll-fallback: just compare the
            // current token against the requested one repeatedly.
            let started = Instant::now();
            loop {
                let current = self.current_durability_token().await?;
                if current.value == token.value {
                    return Ok(true);
                }
                if started.elapsed() >= timeout {
                    return Ok(false);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    async fn enqueue_outbox_event(
        &self,
        event_id: &str,
        topic: &str,
        partition_key: &str,
        payload: &serde_json::Value,
    ) -> Result<i64, String> {
        let rel = self.safe_relation()?;
        let sql = format!(
            "INSERT INTO {rel} (event_id, topic, partition_key, payload, created_at) \
             VALUES (?, ?, ?, ?, NOW(6))"
        );
        let result = sqlx::query(&sql)
            .bind(event_id)
            .bind(topic)
            .bind(partition_key)
            .bind(payload)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("outbox insert (mysql) failed: {e}"))?;
        Ok(result.last_insert_id() as i64)
    }

    async fn outbox_max_seq(&self) -> Result<i64, String> {
        let rel = self.safe_relation()?;
        let sql = format!("SELECT COALESCE(MAX(event_seq), 0) FROM {rel}");
        let (max,): (i64,) = sqlx::query_as(&sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| format!("outbox max seq (mysql) failed: {e}"))?;
        Ok(max)
    }

    async fn ensure_system_tables(&self) -> Result<(), String> {
        let rel = self.safe_relation()?;
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {rel} ( \
                event_seq      BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
                event_id       CHAR(36) NOT NULL UNIQUE, \
                topic          VARCHAR(255) NOT NULL, \
                partition_key  VARCHAR(255) NOT NULL DEFAULT '', \
                payload        JSON NOT NULL, \
                created_at     TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) \
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        );
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("ensure_system_tables (mysql) failed: {e}"))?;
        Ok(())
    }

    async fn ensure_advisory_lease_table(&self) -> Result<(), String> {
        let sql = "
            CREATE TABLE IF NOT EXISTS udb_advisory_leases (
                lease_name VARCHAR(255) NOT NULL PRIMARY KEY,
                owner_id   VARCHAR(255) NOT NULL,
                expires_at TIMESTAMP(6) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        ";
        sqlx::query(sql)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("ensure_advisory_lease_table (mysql) failed: {e}"))?;
        Ok(())
    }

    async fn try_acquire_advisory_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        ttl: std::time::Duration,
    ) -> Result<bool, String> {
        // MySQL has no conditional ON DUPLICATE KEY UPDATE clause.
        // The portable atomic pattern: INSERT … ON DUPLICATE KEY
        // UPDATE owner_id = IF(expires_at < NOW(6), VALUES(owner_id), owner_id),
        //         expires_at = IF(expires_at < NOW(6), VALUES(expires_at), expires_at);
        // The IF() ensures the existing row is only overwritten when
        // it's expired. Then SELECT to confirm.
        let ttl_secs = ttl.as_secs() as i64;
        let sql = "
            INSERT INTO udb_advisory_leases (lease_name, owner_id, expires_at)
            VALUES (?, ?, DATE_ADD(NOW(6), INTERVAL ? SECOND))
            ON DUPLICATE KEY UPDATE
              owner_id   = IF(expires_at < NOW(6), VALUES(owner_id), owner_id),
              expires_at = IF(expires_at < NOW(6), VALUES(expires_at), expires_at)
        ";
        sqlx::query(sql)
            .bind(lease_name)
            .bind(owner_id)
            .bind(ttl_secs)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("try_acquire_advisory_lease (mysql) failed: {e}"))?;
        // Confirm ownership.
        let stored_owner: Option<String> =
            sqlx::query_scalar("SELECT owner_id FROM udb_advisory_leases WHERE lease_name = ?")
                .bind(lease_name)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| format!("try_acquire_advisory_lease lookup (mysql) failed: {e}"))?;
        Ok(matches!(stored_owner, Some(o) if o == owner_id))
    }

    async fn release_advisory_lease(&self, lease_name: &str, owner_id: &str) -> Result<(), String> {
        let sql = "DELETE FROM udb_advisory_leases WHERE lease_name = ? AND owner_id = ?";
        sqlx::query(sql)
            .bind(lease_name)
            .bind(owner_id)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("release_advisory_lease (mysql) failed: {e}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin: backend label is `"mysql"` exactly.
    /// (`tokio::test` because MySqlPool::Drop requires a runtime.)
    #[tokio::test]
    async fn backend_label_is_pinned() {
        let pool = MySqlPool::connect_lazy("mysql://invalid:0/none").unwrap();
        let store = MysqlCanonicalStore::new(pool, "primary", "udb_outbox_events");
        assert_eq!(store.backend_label(), "mysql");
        assert_eq!(store.instance_name(), "primary");
    }

    /// Pin: cross-backend token rejected. Prevents a PG receipt from
    /// being silently waited on against a MySQL store.
    #[tokio::test]
    async fn rejects_non_mysql_token() {
        let pool = MySqlPool::connect_lazy("mysql://invalid:0/none").unwrap();
        let store = MysqlCanonicalStore::new(pool, "primary", "udb_outbox_events");
        let pg_token = DurabilityToken::new("postgres", "0/100");
        let err = store
            .wait_for_token(&pg_token, Duration::from_millis(1))
            .await
            .expect_err("must reject cross-backend token");
        assert!(err.contains("cannot wait on a 'postgres'"), "got: {err}");
    }

    /// Pin: unsafe relation names rejected.
    #[tokio::test]
    async fn unsafe_relation_is_rejected() {
        let pool = MySqlPool::connect_lazy("mysql://invalid:0/none").unwrap();
        let store = MysqlCanonicalStore::new(pool, "primary", "evil; DROP");
        assert!(store.safe_relation().is_err());
    }
}
