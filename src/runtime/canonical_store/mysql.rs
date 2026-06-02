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

    /// Non-GTID durability token for a server acting as a replica: SHOW MASTER
    /// STATUS is empty there, so read the replica's applied coordinates
    /// (`Relay_Master_Log_File` + `Exec_Master_Log_Pos`). Tries the modern
    /// `SHOW REPLICA STATUS` first, falling back to `SHOW SLAVE STATUS` on
    /// older servers. Column names are looked up by name (wide result set).
    async fn replica_durability_token(&self) -> Result<DurabilityToken, String> {
        use sqlx::Row;
        let row = match sqlx::query("SHOW REPLICA STATUS")
            .fetch_optional(&self.pool)
            .await
        {
            Ok(Some(r)) => Some(r),
            // Older servers don't recognise REPLICA; try the legacy spelling.
            _ => sqlx::query("SHOW SLAVE STATUS")
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| format!("SHOW REPLICA/SLAVE STATUS failed: {e}"))?,
        };
        let row = row.ok_or_else(|| {
            "neither SHOW MASTER STATUS nor SHOW REPLICA STATUS returned a row; \
             binary logging / replication is not configured on this server"
                .to_string()
        })?;
        let file: String = row
            .try_get::<String, _>("Relay_Master_Log_File")
            .or_else(|_| row.try_get::<String, _>("Source_Log_File"))
            .map_err(|e| format!("replica status missing log-file column: {e}"))?;
        let pos: u64 = row
            .try_get::<u64, _>("Exec_Master_Log_Pos")
            .or_else(|_| {
                row.try_get::<i64, _>("Exec_Master_Log_Pos")
                    .map(|p| p as u64)
            })
            .or_else(|_| row.try_get::<u64, _>("Exec_Source_Log_Pos"))
            .map_err(|e| format!("replica status missing log-pos column: {e}"))?;
        Ok(DurabilityToken::new("mysql", format!("file:{file}:{pos}")))
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
            // File/position path. MySQL 8.4 removed `SHOW MASTER STATUS` and
            // renamed it to `SHOW BINARY LOG STATUS` (identical columns: `File`,
            // `Position`). Try the 8.4 form first and fall back to the legacy
            // form on MySQL < 8.4, so the store works across server versions.
            // Both are one-row result sets; sqlx parses the first column
            // (`File`) and second column (`Position`).
            let row: Option<(String, u64)> = match sqlx::query_as("SHOW BINARY LOG STATUS")
                .fetch_optional(&self.pool)
                .await
            {
                Ok(row) => row,
                Err(_) => sqlx::query_as("SHOW MASTER STATUS")
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| {
                        format!("SHOW BINARY LOG STATUS / SHOW MASTER STATUS failed: {e}")
                    })?,
            };
            match row {
                Some((file, pos)) => {
                    Ok(DurabilityToken::new("mysql", format!("file:{file}:{pos}")))
                }
                // SHOW MASTER STATUS is empty on a replica (non-GTID). Fall back
                // to the replica's applied coordinates so a replica-targeted
                // deployment can still produce a durability token.
                None => self.replica_durability_token().await,
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
            // Primary self-check: if THIS server's own binlog has already
            // reached or passed the target coordinates, the durability point is
            // satisfied without waiting. *_POS_WAIT only makes sense for a
            // replica catching up to its source; on a standalone primary (no
            // replica SQL thread) it returns NULL, so a primary waiting on its
            // own just-minted token would otherwise never clear.
            if let Ok(current) = self.current_durability_token().await
                && let Some(cur) = current.value.strip_prefix("file:")
                && let Some((cur_file, cur_pos)) = cur.rsplit_once(':')
                && let Ok(cur_pos) = cur_pos.parse::<u64>()
                && (cur_file, cur_pos) >= (file, pos)
            {
                // Zero-padded sequential binlog file names order lexically, so
                // the (file, pos) tuple compare is correct.
                return Ok(true);
            }
            // Replica catch-up path. MySQL 8.4 removed MASTER_POS_WAIT and
            // renamed it to SOURCE_POS_WAIT (identical args/semantics): wait
            // until the replica applies up to the target, returning the
            // rows-applied count or NULL on timeout. Try the 8.4 form first and
            // fall back to the legacy form on MySQL < 8.4.
            let res: Option<(Option<i64>,)> =
                match sqlx::query_as("SELECT SOURCE_POS_WAIT(?, ?, ?)")
                    .bind(file)
                    .bind(pos as i64)
                    .bind(timeout_secs)
                    .fetch_optional(&self.pool)
                    .await
                {
                    Ok(res) => res,
                    Err(_) => sqlx::query_as("SELECT MASTER_POS_WAIT(?, ?, ?)")
                        .bind(file)
                        .bind(pos as i64)
                        .bind(timeout_secs)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(|e| format!("SOURCE_POS_WAIT / MASTER_POS_WAIT failed: {e}"))?,
                };
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
                tokio::time::sleep(crate::runtime::canonical_store::durability_poll_interval(
                    timeout,
                    crate::runtime::canonical_store::MYSQL_DURABILITY_POLL_MS,
                ))
                .await;
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
        // B.7: outbox DDL comes from the shared `sql_schema` renderer (single
        // source of truth across SQL backends); execute/error-handling below
        // is unchanged.
        let sql = super::sql_schema::mysql_outbox_ddl(&rel);
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
        // UPDATE owner_id/expires_at when the row is expired or the
        // caller is refreshing its own live lease. Then SELECT to confirm.
        let ttl_secs = ttl.as_secs() as i64;
        // Run the upsert + ownership confirmation in ONE transaction (MySQL has
        // no RETURNING). InnoDB's INSERT … ON DUPLICATE KEY UPDATE takes an
        // X-lock on the row; the subsequent `SELECT … FOR UPDATE` in the same tx
        // reads that locked row and a competing acquire blocks until we commit —
        // closing the TOCTOU window the prior separate-pool SELECT had.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| format!("try_acquire_advisory_lease begin (mysql) failed: {e}"))?;
        let upsert = "
            INSERT INTO udb_advisory_leases (lease_name, owner_id, expires_at)
            VALUES (?, ?, DATE_ADD(NOW(6), INTERVAL ? SECOND))
            ON DUPLICATE KEY UPDATE
              owner_id   = IF(expires_at < NOW(6) OR owner_id = VALUES(owner_id), VALUES(owner_id), owner_id),
              expires_at = IF(expires_at < NOW(6) OR owner_id = VALUES(owner_id), VALUES(expires_at), expires_at)
        ";
        sqlx::query(upsert)
            .bind(lease_name)
            .bind(owner_id)
            .bind(ttl_secs)
            .execute(&mut *tx)
            .await
            .map_err(|e| format!("try_acquire_advisory_lease (mysql) failed: {e}"))?;
        let stored_owner: Option<String> = sqlx::query_scalar(
            "SELECT owner_id FROM udb_advisory_leases WHERE lease_name = ? FOR UPDATE",
        )
        .bind(lease_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| format!("try_acquire_advisory_lease lookup (mysql) failed: {e}"))?;
        tx.commit()
            .await
            .map_err(|e| format!("try_acquire_advisory_lease commit (mysql) failed: {e}"))?;
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
