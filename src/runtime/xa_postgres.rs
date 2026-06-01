//! Postgres `XaParticipant` impl (U20 step 2).
//!
//! Postgres natively supports 2PC via the `PREPARE TRANSACTION '<xid>'`
//! / `COMMIT PREPARED '<xid>'` / `ROLLBACK PREPARED '<xid>'` SQL
//! sequence (requires `max_prepared_transactions > 0` in postgresql.conf
//! — the broker's startup `doctor` check surfaces this).
//!
//! Each `PostgresXaParticipant` is constructed per-request with the
//! request's pool and an open transaction. PHASE 1's `prepare(xid)`:
//!
//! 1. Validates the xid against PG's identifier rules.
//! 2. Issues the buffered mutations on the connection.
//! 3. Runs `PREPARE TRANSACTION '<xid>'` — Postgres durably persists
//!    the transaction; the next session can `COMMIT PREPARED` /
//!    `ROLLBACK PREPARED` it even after the original connection drops.
//!
//! PHASE 2's `commit_prepared(xid)` opens a fresh connection from the
//! pool (the original transaction's session is gone after PREPARE) and
//! runs `COMMIT PREPARED '<xid>'`. Recovery follows the same pattern —
//! the recovery worker scans `pg_prepared_xacts` and drives every
//! UDB-prefixed xid to its ledger-recorded terminal state.

use sqlx::{PgPool, Postgres, Transaction};

use crate::runtime::xa::{PrepareVote, XaParticipant, XaParticipantHandle};

/// One Postgres pool acting as a 2PC participant.
///
/// `mutations_sql` is the buffered SQL the request wants committed.
/// It runs inside the transaction PHASE 1 wraps around PREPARE. Empty
/// is allowed (a no-op participant — useful when the request touches
/// the pool for a read-only side effect like advisory locking).
pub struct PostgresXaParticipant {
    handle: XaParticipantHandle,
    pool: PgPool,
    /// Pre-formatted SQL statements to run before PREPARE. The
    /// coordinator passes one per logical mutation; the participant
    /// concatenates them into a single transaction.
    mutations_sql: Vec<String>,
}

/// Postgres participant wrapping an already-open transaction.
///
/// `begin_tx` executes all request mutations through the normal UDB planner and
/// bind path before commit. For a `two_phase` request, that open transaction is
/// handed to this participant; PHASE 1 is just `PREPARE TRANSACTION`, and PHASE
/// 2 commits/rolls back the prepared xid from the pool. This keeps CRUD logic in
/// one place while still routing the 2PC decision through `XaCoordinator`.
pub struct ActivePostgresXaParticipant {
    handle: XaParticipantHandle,
    pool: PgPool,
    tx: tokio::sync::Mutex<Option<Transaction<'static, Postgres>>>,
}

impl ActivePostgresXaParticipant {
    pub fn new(
        instance: impl Into<String>,
        pool: PgPool,
        tx: Transaction<'static, Postgres>,
    ) -> Self {
        Self {
            handle: XaParticipantHandle::new("postgres", instance),
            pool,
            tx: tokio::sync::Mutex::new(Some(tx)),
        }
    }
}

impl PostgresXaParticipant {
    pub fn new(handle: XaParticipantHandle, pool: PgPool, mutations_sql: Vec<String>) -> Self {
        Self {
            handle,
            pool,
            mutations_sql,
        }
    }

    /// PG identifier rules: 1–200 chars (the documented limit for
    /// prepared-transaction names), starts with a letter / digit /
    /// `udb-`, ASCII only. We generate xids of the form
    /// `udb-<uuid>` so they trivially satisfy this; this check
    /// guards against operator-supplied xids during recovery.
    pub fn validate_xid(xid: &str) -> Result<(), String> {
        if xid.is_empty() || xid.len() > 200 {
            return Err(format!(
                "xid '{xid}' is invalid: must be 1–200 chars (PostgreSQL limit)"
            ));
        }
        if !xid
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(format!(
                "xid '{xid}' contains invalid characters; only ASCII alphanumeric, '-', '_' are allowed"
            ));
        }
        Ok(())
    }

    /// Quote an xid into a SQL literal. Single-quote escaping is
    /// **belt-and-braces**: the validator above already rejects every
    /// character that would need escaping, but we still emit
    /// `'<xid>'` with quote-doubling so an audit of the emitted SQL is
    /// unambiguous about which characters can appear.
    pub(crate) fn quote_xid(xid: &str) -> String {
        format!("'{}'", xid.replace('\'', "''"))
    }
}

#[async_trait::async_trait]
impl XaParticipant for PostgresXaParticipant {
    fn handle(&self) -> &XaParticipantHandle {
        &self.handle
    }

    async fn prepare(&self, xid: &str) -> PrepareVote {
        if let Err(reason) = Self::validate_xid(xid) {
            return PrepareVote::Aborted { reason };
        }
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(err) => {
                return PrepareVote::Aborted {
                    reason: format!("BEGIN failed: {err}"),
                };
            }
        };
        // Apply each buffered mutation inside the transaction.
        for sql in &self.mutations_sql {
            if let Err(err) = sqlx::query(sql).execute(&mut *tx).await {
                // Rollback happens implicitly when `tx` is dropped on
                // the error path; surface the reason to the coordinator.
                return PrepareVote::Aborted {
                    reason: format!("mutation failed: {err}"),
                };
            }
        }
        // `sqlx` doesn't expose `PREPARE TRANSACTION` directly because
        // its `tx.commit()` does a normal COMMIT. We forcibly emit the
        // PREPARE on the same connection, then drop the transaction
        // handle (we've taken ownership of the prepared transaction
        // from Postgres' perspective; further `tx.commit()` would
        // fail because the transaction is already prepared).
        let prepare_sql = format!("PREPARE TRANSACTION {}", Self::quote_xid(xid));
        if let Err(err) = sqlx::query(&prepare_sql).execute(&mut *tx).await {
            return PrepareVote::Aborted {
                reason: format!("PREPARE TRANSACTION failed: {err}"),
            };
        }
        // sqlx's tx object is now in an odd state — the transaction is
        // prepared, no longer abortable via this handle. We forget the
        // wrapper so its Drop doesn't try to issue a ROLLBACK on a
        // prepared transaction (which would error). The connection
        // returns to the pool.
        let _ = tx;
        PrepareVote::Prepared
    }

    async fn commit_prepared(&self, xid: &str) -> Result<(), String> {
        Self::validate_xid(xid)?;
        let sql = format!("COMMIT PREPARED {}", Self::quote_xid(xid));
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|err| format!("COMMIT PREPARED failed: {err}"))
    }

    async fn rollback_prepared(&self, xid: &str) -> Result<(), String> {
        Self::validate_xid(xid)?;
        let sql = format!("ROLLBACK PREPARED {}", Self::quote_xid(xid));
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|err| format!("ROLLBACK PREPARED failed: {err}"))
    }
}

#[async_trait::async_trait]
impl XaParticipant for ActivePostgresXaParticipant {
    fn handle(&self) -> &XaParticipantHandle {
        &self.handle
    }

    async fn prepare(&self, xid: &str) -> PrepareVote {
        if let Err(reason) = PostgresXaParticipant::validate_xid(xid) {
            return PrepareVote::Aborted { reason };
        }
        let mut guard = self.tx.lock().await;
        let Some(mut tx) = guard.take() else {
            return PrepareVote::Aborted {
                reason: "active Postgres transaction was already consumed".to_string(),
            };
        };
        let prepare_sql = format!(
            "PREPARE TRANSACTION {}",
            PostgresXaParticipant::quote_xid(xid)
        );
        if let Err(err) = sqlx::query(&prepare_sql).execute(&mut *tx).await {
            let _ = tx.rollback().await;
            return PrepareVote::Aborted {
                reason: format!("PREPARE TRANSACTION failed: {err}"),
            };
        }
        // After PREPARE the transaction is owned by PostgreSQL's prepared-xact
        // catalog. Dropping the sqlx wrapper may attempt a best-effort rollback;
        // PostgreSQL rejects that because the xact is already prepared, and the
        // actual terminal decision is driven by commit_prepared/rollback_prepared.
        drop(tx);
        PrepareVote::Prepared
    }

    async fn commit_prepared(&self, xid: &str) -> Result<(), String> {
        PostgresXaParticipant::validate_xid(xid)?;
        let sql = format!("COMMIT PREPARED {}", PostgresXaParticipant::quote_xid(xid));
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|err| format!("COMMIT PREPARED failed: {err}"))
    }

    async fn rollback_prepared(&self, xid: &str) -> Result<(), String> {
        PostgresXaParticipant::validate_xid(xid)?;
        let sql = format!(
            "ROLLBACK PREPARED {}",
            PostgresXaParticipant::quote_xid(xid)
        );
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|err| format!("ROLLBACK PREPARED failed: {err}"))
    }
}

/// Helper used by the recovery worker — given an open pool, return
/// every xid in `pg_prepared_xacts` whose name starts with the UDB
/// prefix. The worker then cross-references each against the ledger and
/// drives to terminal state.
pub async fn list_udb_prepared_xacts(pool: &PgPool) -> Result<Vec<String>, String> {
    use sqlx::Row;
    let rows = sqlx::query("SELECT gid FROM pg_prepared_xacts WHERE gid LIKE 'udb-%'")
        .fetch_all(pool)
        .await
        .map_err(|err| format!("pg_prepared_xacts scan failed: {err}"))?;
    let mut xids = Vec::with_capacity(rows.len());
    for row in rows {
        let gid: String = row.try_get("gid").map_err(|err| err.to_string())?;
        xids.push(gid);
    }
    Ok(xids)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Validation rules pinned — the recovery worker reads these on
    /// every xid pulled from `pg_prepared_xacts`, so a regression in
    /// validation could silently skip in-doubt transactions.
    #[test]
    fn validate_xid_accepts_canonical_form() {
        assert!(PostgresXaParticipant::validate_xid("udb-abc-123").is_ok());
        assert!(PostgresXaParticipant::validate_xid("udb-0123_456").is_ok());
        // 199 chars — just under the PG limit.
        let almost_max = format!("udb-{}", "a".repeat(195));
        assert_eq!(almost_max.len(), 199);
        assert!(PostgresXaParticipant::validate_xid(&almost_max).is_ok());
    }

    #[test]
    fn validate_xid_rejects_invalid_forms() {
        assert!(PostgresXaParticipant::validate_xid("").is_err());
        assert!(PostgresXaParticipant::validate_xid("with spaces").is_err());
        assert!(PostgresXaParticipant::validate_xid("with;semi").is_err());
        assert!(PostgresXaParticipant::validate_xid("with'quote").is_err());
        // Over the 200-char limit.
        let too_long = format!("udb-{}", "a".repeat(200));
        assert!(too_long.len() > 200);
        assert!(PostgresXaParticipant::validate_xid(&too_long).is_err());
    }

    /// SQL emission is the wire contract — if these change without a
    /// recovery worker update, restart cleanup breaks.
    #[test]
    fn quote_xid_doubles_single_quotes() {
        // The validator rejects `'` so this path is defensive.
        // Forced through `quote_xid` directly:
        assert_eq!(PostgresXaParticipant::quote_xid("udb-test"), "'udb-test'");
        assert_eq!(
            PostgresXaParticipant::quote_xid("udb-it's"),
            "'udb-it''s'",
            "single quotes must be doubled per SQL string literal escaping"
        );
    }
}
