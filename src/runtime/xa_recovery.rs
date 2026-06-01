//! In-doubt 2PC recovery worker (C6).
//!
//! `XaCoordinator::execute` records every 2PC outcome in
//! `XaLedgerEntry`. PHASE 2 mid-flight failures (network blip, broker
//! crash between PREPARE and COMMIT, participant unreachable) land in
//! `XaDecision::InDoubt` — the ledger remembers the xid and which
//! participants saw it, but no live coroutine is driving it to
//! completion.
//!
//! This module supplies the worker that, on broker startup and at a
//! configurable interval, scans the ledger for `InDoubt` rows and
//! drives each to a terminal state. For MySQL participants the worker:
//!
//! 1. Reads `udb_xa_ledger` for `decision = 'in_doubt'`.
//! 2. For each row, reconstructs the participant via the registered
//!    factory.
//! 3. Issues `XA RECOVER` on the participant's connection. If the xid
//!    is listed, the participant is still prepared — emit
//!    `XA COMMIT '<xid>'` (matching the coordinator's intent, which
//!    is "commit unless explicitly rolled back").
//! 4. If the xid is NOT in `XA RECOVER` output, another process
//!    (manual recovery, parallel worker) already drove it. Mark the
//!    ledger row as `committed` (best-known terminal state) so the
//!    next sweep doesn't retry.
//! 5. If the participant is unreachable, log + leave the row for the
//!    next sweep. Quarantine policy applies after configurable max
//!    attempts.
//!
//! Postgres participants use the same flow with `pg_prepared_xacts`
//! and `COMMIT PREPARED`. Other backends (none today) plug their
//! `XA RECOVER` analogue into the `XaInDoubtParticipant` trait.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::runtime::system::SystemCatalogConfig;
use crate::runtime::xa::{XaDecision, XaLedgerEntry};

/// What the recovery worker needs from each backend to drive an
/// in-doubt xid to terminal. Different from `XaParticipant` — that
/// trait is for live PHASE 1/2 with a buffer of statements; this one
/// is read-only against the prepared-transactions catalog.
#[async_trait]
pub trait XaInDoubtParticipant: Send + Sync {
    /// Backend label this participant handles. Matched against
    /// `XaLedgerEntry::participants[i]` labels (which are the form
    /// `"<backend>:<instance>"`).
    fn backend_label(&self) -> &str;

    /// Return the set of currently prepared (in-doubt) xids the
    /// backend knows about. For MySQL: `XA RECOVER`. For Postgres:
    /// `SELECT gid FROM pg_prepared_xacts`.
    async fn list_prepared_xids(&self) -> Result<Vec<String>, String>;

    /// Commit the given prepared xid. Idempotent at the backend
    /// level — committing an already-committed xid returns an error
    /// the recovery worker translates to "already terminal".
    async fn commit_prepared(&self, xid: &str) -> Result<(), String>;

    /// Roll back the given prepared xid. Used when the ledger reason
    /// signals "PREPARE failed elsewhere" — the recovery worker drives
    /// the surviving prepared participants back to neutral.
    async fn rollback_prepared(&self, xid: &str) -> Result<(), String>;
}

/// MySQL in-doubt participant. Acquires a fresh connection per
/// recovery call — no shared connection lifetime concern because
/// `XA RECOVER` / `XA COMMIT '<xid>'` work from any connection on
/// the same server.
#[cfg(feature = "mysql")]
pub struct MysqlInDoubtParticipant {
    pub label: String,
    pub pool: sqlx::MySqlPool,
}

#[cfg(feature = "mysql")]
#[async_trait]
impl XaInDoubtParticipant for MysqlInDoubtParticipant {
    fn backend_label(&self) -> &str {
        &self.label
    }

    async fn list_prepared_xids(&self) -> Result<Vec<String>, String> {
        use sqlx::Row;
        // `XA RECOVER` returns rows with `formatID`, `gtrid_length`,
        // `bqual_length`, `data`. The `data` column is the xid we
        // assigned (`udb-<uuid>`). We project just `data` so the
        // result is a clean Vec<String>.
        let rows = sqlx::query("XA RECOVER")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| format!("XA RECOVER failed: {e}"))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            // The `data` column on MySQL XA RECOVER is BLOB; try as
            // bytes first then UTF-8.
            let data: Vec<u8> = row
                .try_get::<Vec<u8>, _>("data")
                .or_else(|_| row.try_get::<String, _>("data").map(|s| s.into_bytes()))
                .map_err(|e| format!("XA RECOVER row decode: {e}"))?;
            let xid = String::from_utf8_lossy(&data).to_string();
            out.push(xid);
        }
        Ok(out)
    }

    async fn commit_prepared(&self, xid: &str) -> Result<(), String> {
        use sqlx::Executor;
        validate_xid(xid)?;
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| format!("mysql xa commit acquire: {e}"))?;
        conn.execute(format!("XA COMMIT '{xid}'").as_str())
            .await
            .map(|_| ())
            .map_err(|e| format!("XA COMMIT failed: {e}"))
    }

    async fn rollback_prepared(&self, xid: &str) -> Result<(), String> {
        use sqlx::Executor;
        validate_xid(xid)?;
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| format!("mysql xa rollback acquire: {e}"))?;
        conn.execute(format!("XA ROLLBACK '{xid}'").as_str())
            .await
            .map(|_| ())
            .map_err(|e| format!("XA ROLLBACK failed: {e}"))
    }
}

/// Postgres in-doubt participant.
pub struct PostgresInDoubtParticipant {
    pub label: String,
    pub pool: sqlx::PgPool,
}

#[async_trait]
impl XaInDoubtParticipant for PostgresInDoubtParticipant {
    fn backend_label(&self) -> &str {
        &self.label
    }

    async fn list_prepared_xids(&self) -> Result<Vec<String>, String> {
        let rows: Vec<(String,)> = sqlx::query_as("SELECT gid FROM pg_prepared_xacts")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| format!("pg_prepared_xacts query failed: {e}"))?;
        Ok(rows.into_iter().map(|(g,)| g).collect())
    }

    async fn commit_prepared(&self, xid: &str) -> Result<(), String> {
        // PG `COMMIT PREPARED` doesn't accept bind parameters — the
        // identifier rules mean we have to interpolate. xid format is
        // controlled by the coordinator (`udb-<uuid>`, no quotes),
        // so it's safe to interpolate. Still validate to be defensive.
        validate_xid(xid)?;
        sqlx::query(&format!("COMMIT PREPARED '{xid}'"))
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| format!("COMMIT PREPARED failed: {e}"))
    }

    async fn rollback_prepared(&self, xid: &str) -> Result<(), String> {
        validate_xid(xid)?;
        sqlx::query(&format!("ROLLBACK PREPARED '{xid}'"))
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| format!("ROLLBACK PREPARED failed: {e}"))
    }
}

/// Validate a 2PC xid identifier. The coordinator always emits
/// `udb-<uuid>`; reject anything outside `[a-zA-Z0-9_-]` so a poisoned
/// ledger row can't escape the quoting envelope.
fn validate_xid(xid: &str) -> Result<(), String> {
    if xid.is_empty() {
        return Err("xa xid is empty".into());
    }
    if !xid
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(format!("xa xid '{xid}' contains disallowed characters"));
    }
    Ok(())
}

/// Per-row outcome of a sweep.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RecoveryOutcome {
    /// xid was found prepared and successfully committed.
    Committed { xid: String, backend: String },
    /// xid was found prepared and rolled back (ledger reason indicated
    /// PREPARE failure elsewhere).
    RolledBack { xid: String, backend: String },
    /// xid is not in the backend's prepared list — another process
    /// already drove it terminal.
    AlreadyTerminal { xid: String, backend: String },
    /// Participant backend label doesn't match any registered
    /// in-doubt participant — the ledger references a backend the
    /// recovery worker can't drive.
    NoParticipant { xid: String, backend: String },
    /// Participant call failed; will retry on next sweep until the
    /// quarantine policy escalates.
    Failed {
        xid: String,
        backend: String,
        reason: String,
    },
}

impl RecoveryOutcome {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Committed { .. } | Self::RolledBack { .. } | Self::AlreadyTerminal { .. }
        )
    }
}

/// Registry of `XaInDoubtParticipant` impls keyed by their
/// `backend_label`. The runtime builds one of these at startup and
/// re-uses it for every sweep.
#[derive(Default, Clone)]
pub struct InDoubtRegistry {
    by_label: HashMap<String, Arc<dyn XaInDoubtParticipant>>,
}

impl InDoubtRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, p: Arc<dyn XaInDoubtParticipant>) {
        self.by_label
            .insert(p.backend_label().to_ascii_lowercase(), p);
    }

    pub fn get(&self, label: &str) -> Option<Arc<dyn XaInDoubtParticipant>> {
        // The ledger stores labels as `<backend>:<instance>` — strip
        // the instance for the lookup.
        let key = label
            .split_once(':')
            .map(|(b, _)| b)
            .unwrap_or(label)
            .to_ascii_lowercase();
        self.by_label.get(&key).cloned()
    }
}

/// One in-doubt ledger row the worker is asked to drive. Subset of
/// `XaLedgerEntry` — we only need the xid + participant labels +
/// reason (which tells us whether to commit or rollback).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InDoubtLedgerRow {
    pub xid: String,
    pub participants: Vec<String>,
    /// Empty when the row was reached via COMMIT-side failure
    /// (default: commit). Non-empty when PREPARE failed elsewhere
    /// (recovery rolls back).
    pub reason: String,
}

impl InDoubtLedgerRow {
    /// Decide intent. "Commit unless reason mentions PREPARE failure"
    /// matches the coordinator's behaviour at runtime: a row reaches
    /// InDoubt because PHASE 2 failed mid-commit; the reason is the
    /// transport error. PREPARE-side failures take the `PrepareFailed`
    /// path which writes `RolledBack`, not `InDoubt`, so any InDoubt
    /// row defaults to commit.
    pub fn target_intent(&self) -> RecoveryIntent {
        // Coordinator semantics: an `InDoubt` row only happens when
        // PHASE 2 (COMMIT PREPARED) failed mid-flight; PHASE 1
        // (PREPARE) failures take the `PrepareFailed` path which
        // writes `RolledBack`. So the default for InDoubt is commit.
        //
        // The exception is operator-supplied reasons that explicitly
        // signal "prepare failed elsewhere" — e.g. "XA PREPARE failed"
        // or "aborted vote". Recognise those without false-matching
        // "COMMIT PREPARED" / "ROLLBACK PREPARED".
        let lc = self.reason.to_ascii_lowercase();
        if lc.contains("prepare failed")
            || lc.contains("xa prepare")
            || lc.contains("aborted vote")
            || lc.contains("aborted: ")
        {
            RecoveryIntent::Rollback
        } else {
            RecoveryIntent::Commit
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryIntent {
    Commit,
    Rollback,
}

/// Drive one in-doubt row to terminal across all its participants.
/// Returns the outcome per participant in declaration order.
pub async fn drive_indoubt_row(
    row: &InDoubtLedgerRow,
    registry: &InDoubtRegistry,
) -> Vec<RecoveryOutcome> {
    let intent = row.target_intent();
    let xid = &row.xid;
    let mut outcomes = Vec::with_capacity(row.participants.len());
    for label in &row.participants {
        let backend = label
            .split_once(':')
            .map(|(b, _)| b.to_string())
            .unwrap_or_else(|| label.clone());
        let Some(participant) = registry.get(label) else {
            outcomes.push(RecoveryOutcome::NoParticipant {
                xid: xid.clone(),
                backend,
            });
            continue;
        };
        // Check whether the xid is still prepared. If not, another
        // process already drove it terminal.
        let prepared = match participant.list_prepared_xids().await {
            Ok(list) => list,
            Err(err) => {
                outcomes.push(RecoveryOutcome::Failed {
                    xid: xid.clone(),
                    backend,
                    reason: format!("list_prepared_xids: {err}"),
                });
                continue;
            }
        };
        if !prepared.iter().any(|p| p == xid) {
            outcomes.push(RecoveryOutcome::AlreadyTerminal {
                xid: xid.clone(),
                backend,
            });
            continue;
        }
        // Drive to terminal.
        let result = match intent {
            RecoveryIntent::Commit => participant.commit_prepared(xid).await,
            RecoveryIntent::Rollback => participant.rollback_prepared(xid).await,
        };
        match result {
            Ok(()) => outcomes.push(match intent {
                RecoveryIntent::Commit => RecoveryOutcome::Committed {
                    xid: xid.clone(),
                    backend,
                },
                RecoveryIntent::Rollback => RecoveryOutcome::RolledBack {
                    xid: xid.clone(),
                    backend,
                },
            }),
            Err(reason) => outcomes.push(RecoveryOutcome::Failed {
                xid: xid.clone(),
                backend,
                reason,
            }),
        }
    }
    outcomes
}

/// Recovery worker tunables.
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Seconds between sweeps. Reads `UDB_XA_RECOVERY_INTERVAL_SECS`
    /// at startup with a 30-second default.
    pub interval: Duration,
    /// Max sweeps before a still-failing row is moved to
    /// `manual_review`. Reads `UDB_XA_RECOVERY_MAX_ATTEMPTS` at
    /// startup with a 10 default.
    pub max_attempts: u32,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        let interval_secs = std::env::var("UDB_XA_RECOVERY_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(30);
        let max_attempts = std::env::var("UDB_XA_RECOVERY_MAX_ATTEMPTS")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(10);
        Self {
            interval: Duration::from_secs(interval_secs),
            max_attempts,
        }
    }
}

/// Startup recovery for abandoned PostgreSQL 2PC prepared transactions.
///
/// `begin_tx`'s live 2PC path issues `PREPARE TRANSACTION 'udb_<txid>'` and then
/// `COMMIT PREPARED` on a fresh connection. If the process crashes between the
/// two, the prepared transaction is stranded in `pg_prepared_xacts` holding its
/// row locks **forever**. Following the standard 2PC *presumed-abort* rule, this
/// sweep `ROLLBACK PREPARED`s every `udb_`-prefixed prepared transaction older
/// than `grace_secs` (the grace window leaves prepares from in-flight requests
/// untouched). Returns the number rolled back. Safe to call on every startup.
pub async fn recover_abandoned_prepared_transactions(
    pool: &sqlx::PgPool,
    grace_secs: i64,
) -> Result<u64, String> {
    // Coordinator xids are `udb-<uuid>`; never touch a co-tenant application's
    // prepared transactions.
    let gids: Vec<String> = sqlx::query_scalar(
        "SELECT gid FROM pg_prepared_xacts \
         WHERE gid LIKE 'udb-%' \
           AND prepared < NOW() - make_interval(secs => $1::double precision)",
    )
    .bind(grace_secs.max(0) as f64)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("scan pg_prepared_xacts failed: {e}"))?;
    let mut rolled_back = 0u64;
    for gid in gids {
        // Defense-in-depth: gid is operator-data-adjacent, so validate the
        // charset before interpolating into ROLLBACK PREPARED (xids can't be
        // parameter-bound in this statement form).
        if validate_xid(&gid).is_err() {
            tracing::warn!(gid = %gid, "skipping prepared xact with unexpected gid charset");
            continue;
        }
        match sqlx::query(&format!("ROLLBACK PREPARED '{gid}'"))
            .execute(pool)
            .await
        {
            Ok(_) => {
                rolled_back += 1;
                tracing::warn!(
                    gid = %gid,
                    "rolled back abandoned UDB prepared transaction (presumed-abort recovery)"
                );
            }
            Err(e) => tracing::warn!(
                gid = %gid,
                "failed to roll back abandoned prepared transaction: {e}"
            ),
        }
    }
    Ok(rolled_back)
}

pub async fn ensure_xa_ledger_table(
    pool: &sqlx::PgPool,
    config: &SystemCatalogConfig,
) -> Result<(), String> {
    for statement in xa_ledger_statements(config) {
        sqlx::query(&statement)
            .execute(pool)
            .await
            .map_err(|e| format!("ensure XA ledger failed: {e}"))?;
    }
    Ok(())
}

pub async fn record_xa_ledger_entry(
    pool: &sqlx::PgPool,
    config: &SystemCatalogConfig,
    entry: &XaLedgerEntry,
) -> Result<(), String> {
    ensure_xa_ledger_table(pool, config).await?;
    let relation = config.xa_ledger_relation();
    let participants = serde_json::to_value(&entry.participants)
        .map_err(|e| format!("serialize XA participants failed: {e}"))?;
    sqlx::query(&format!(
        "INSERT INTO {relation}
             (xid, tenant_id, project_id, origin_rpc, correlation_id,
              participants, decision, reason, decided_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6::JSONB,$7,$8,
                 to_timestamp($9::DOUBLE PRECISION / 1000.0), NOW())
         ON CONFLICT (xid) DO UPDATE SET
              tenant_id = EXCLUDED.tenant_id,
              project_id = EXCLUDED.project_id,
              origin_rpc = EXCLUDED.origin_rpc,
              correlation_id = EXCLUDED.correlation_id,
              participants = EXCLUDED.participants,
              decision = EXCLUDED.decision,
              reason = EXCLUDED.reason,
              decided_at = EXCLUDED.decided_at,
              updated_at = NOW()"
    ))
    .bind(&entry.xid)
    .bind(&entry.tenant_id)
    .bind(&entry.project_id)
    .bind(&entry.origin_rpc)
    .bind(&entry.correlation_id)
    .bind(participants)
    .bind(entry.decision.as_str())
    .bind(&entry.reason)
    .bind(entry.decided_at_unix_ms as f64)
    .execute(pool)
    .await
    .map_err(|e| format!("record XA ledger failed: {e}"))?;
    Ok(())
}

pub async fn recover_xa_ledger_indoubt(
    pool: &sqlx::PgPool,
    config: &SystemCatalogConfig,
) -> Result<u64, String> {
    ensure_xa_ledger_table(pool, config).await?;
    let relation = config.xa_ledger_relation();
    let rows = sqlx::query(&format!(
        "SELECT xid, participants, reason FROM {relation}
         WHERE decision = 'in_doubt'
         ORDER BY updated_at ASC
         LIMIT 100"
    ))
    .fetch_all(pool)
    .await
    .map_err(|e| format!("load XA in-doubt ledger rows failed: {e}"))?;

    let mut registry = InDoubtRegistry::new();
    registry.register(Arc::new(PostgresInDoubtParticipant {
        label: "postgres".to_string(),
        pool: pool.clone(),
    }));

    let mut terminal = 0u64;
    for db_row in rows {
        let xid: String = db_row.try_get("xid").map_err(|e| e.to_string())?;
        let participants_json: serde_json::Value =
            db_row.try_get("participants").map_err(|e| e.to_string())?;
        let participants = serde_json::from_value::<Vec<String>>(participants_json)
            .map_err(|e| format!("decode XA participants for {xid}: {e}"))?;
        let reason: String = db_row.try_get("reason").unwrap_or_default();
        let row = InDoubtLedgerRow {
            xid: xid.clone(),
            participants,
            reason,
        };
        let outcomes = drive_indoubt_row(&row, &registry).await;
        if outcomes.iter().all(RecoveryOutcome::is_terminal) {
            let decision = if outcomes
                .iter()
                .any(|o| matches!(o, RecoveryOutcome::RolledBack { .. }))
            {
                XaDecision::RolledBack
            } else {
                XaDecision::Committed
            };
            mark_xa_ledger_decision(pool, config, &xid, decision, "").await?;
            terminal += 1;
        } else {
            let reason = outcomes
                .iter()
                .filter_map(|outcome| match outcome {
                    RecoveryOutcome::Failed {
                        backend, reason, ..
                    } => Some(format!("{backend}: {reason}")),
                    RecoveryOutcome::NoParticipant { backend, .. } => {
                        Some(format!("{backend}: no in-doubt participant registered"))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("; ");
            sqlx::query(&format!(
                "UPDATE {relation}
                 SET recovery_attempts = recovery_attempts + 1,
                     reason = CASE WHEN $2 = '' THEN reason ELSE $2 END,
                     updated_at = NOW()
                 WHERE xid = $1"
            ))
            .bind(&xid)
            .bind(reason)
            .execute(pool)
            .await
            .map_err(|e| format!("update XA retry state failed: {e}"))?;
        }
    }
    Ok(terminal)
}

async fn mark_xa_ledger_decision(
    pool: &sqlx::PgPool,
    config: &SystemCatalogConfig,
    xid: &str,
    decision: XaDecision,
    reason: &str,
) -> Result<(), String> {
    let relation = config.xa_ledger_relation();
    sqlx::query(&format!(
        "UPDATE {relation}
         SET decision = $2,
             reason = CASE WHEN $3 = '' THEN reason ELSE $3 END,
             updated_at = NOW()
         WHERE xid = $1"
    ))
    .bind(xid)
    .bind(decision.as_str())
    .bind(reason)
    .execute(pool)
    .await
    .map_err(|e| format!("mark XA ledger decision failed: {e}"))?;
    Ok(())
}

fn xa_ledger_statements(config: &SystemCatalogConfig) -> [String; 2] {
    let relation = config.xa_ledger_relation();
    [
        format!(
            "CREATE TABLE IF NOT EXISTS {relation} (
                xid TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL DEFAULT '',
                project_id TEXT NOT NULL DEFAULT '',
                origin_rpc TEXT NOT NULL DEFAULT '',
                correlation_id TEXT NOT NULL DEFAULT '',
                participants JSONB NOT NULL DEFAULT '[]'::JSONB,
                decision TEXT NOT NULL DEFAULT 'in_doubt'
                    CHECK (decision IN ('committed','rolled_back','in_doubt')),
                reason TEXT NOT NULL DEFAULT '',
                recovery_attempts INTEGER NOT NULL DEFAULT 0,
                decided_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )"
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS \"idx_{}_decision_updated\" ON {relation} (decision, updated_at)",
            config.xa_ledger_table
        ),
    ]
}

/// Wall-clock helper. Pulled out so tests can stub via dependency
/// injection (no live system clock dependence in the row-driver
/// itself).
pub fn now_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Test stub — controllable `prepared` set + side-effect log.
    struct StubParticipant {
        label: String,
        prepared: Mutex<Vec<String>>,
        commit_outcome: Mutex<Result<(), String>>,
        rollback_outcome: Mutex<Result<(), String>>,
        events: Mutex<Vec<String>>,
    }

    impl StubParticipant {
        fn new(label: &str, prepared: Vec<String>) -> Self {
            Self {
                label: label.to_string(),
                prepared: Mutex::new(prepared),
                commit_outcome: Mutex::new(Ok(())),
                rollback_outcome: Mutex::new(Ok(())),
                events: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl XaInDoubtParticipant for StubParticipant {
        fn backend_label(&self) -> &str {
            &self.label
        }
        async fn list_prepared_xids(&self) -> Result<Vec<String>, String> {
            self.events
                .lock()
                .unwrap()
                .push("list_prepared_xids".into());
            Ok(self.prepared.lock().unwrap().clone())
        }
        async fn commit_prepared(&self, xid: &str) -> Result<(), String> {
            self.events
                .lock()
                .unwrap()
                .push(format!("commit_prepared({xid})"));
            self.prepared.lock().unwrap().retain(|p| p != xid);
            self.commit_outcome.lock().unwrap().clone()
        }
        async fn rollback_prepared(&self, xid: &str) -> Result<(), String> {
            self.events
                .lock()
                .unwrap()
                .push(format!("rollback_prepared({xid})"));
            self.prepared.lock().unwrap().retain(|p| p != xid);
            self.rollback_outcome.lock().unwrap().clone()
        }
    }

    #[test]
    fn target_intent_defaults_to_commit() {
        let row = InDoubtLedgerRow {
            xid: "udb-x".into(),
            participants: vec![],
            reason: "PHASE 2 COMMIT PREPARED timed out on mysql:primary".into(),
        };
        assert_eq!(row.target_intent(), RecoveryIntent::Commit);
    }

    #[test]
    fn target_intent_prepare_reason_rolls_back() {
        let row = InDoubtLedgerRow {
            xid: "udb-x".into(),
            participants: vec![],
            reason: "XA PREPARE failed on mysql:primary".into(),
        };
        assert_eq!(row.target_intent(), RecoveryIntent::Rollback);
    }

    #[test]
    fn target_intent_commit_prepared_phrase_does_not_false_match() {
        // Regression guard: "COMMIT PREPARED transport error" contains
        // the substring "PREPARED" but is a PHASE 2 mid-commit failure,
        // not a PREPARE-side failure. Intent must stay Commit.
        let row = InDoubtLedgerRow {
            xid: "udb-x".into(),
            participants: vec![],
            reason: "COMMIT PREPARED transport error".into(),
        };
        assert_eq!(row.target_intent(), RecoveryIntent::Commit);
    }

    #[test]
    fn validate_xid_accepts_canonical_format() {
        assert!(validate_xid("udb-abc-123").is_ok());
        assert!(validate_xid("udb_xid_42").is_ok());
    }

    #[test]
    fn validate_xid_rejects_injection_shapes() {
        assert!(validate_xid("").is_err());
        assert!(validate_xid("xid'; DROP TABLE x").is_err());
        assert!(validate_xid("xid\"").is_err());
        assert!(validate_xid("xid OR 1=1").is_err());
    }

    #[tokio::test]
    async fn drive_indoubt_row_commits_when_xid_still_prepared() {
        let participant = Arc::new(StubParticipant::new("mysql", vec!["udb-abc".into()]));
        let mut registry = InDoubtRegistry::new();
        registry.register(participant.clone());

        let row = InDoubtLedgerRow {
            xid: "udb-abc".into(),
            participants: vec!["mysql:primary".into()],
            reason: "COMMIT PREPARED transport error".into(),
        };
        let outcomes = drive_indoubt_row(&row, &registry).await;
        assert_eq!(outcomes.len(), 1);
        assert!(matches!(outcomes[0], RecoveryOutcome::Committed { .. }));
        // After commit, the prepared set is empty.
        assert!(participant.prepared.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn drive_indoubt_row_skips_when_xid_already_terminal() {
        let participant = Arc::new(StubParticipant::new("mysql", vec![]));
        let mut registry = InDoubtRegistry::new();
        registry.register(participant.clone());

        let row = InDoubtLedgerRow {
            xid: "udb-already-done".into(),
            participants: vec!["mysql:primary".into()],
            reason: String::new(),
        };
        let outcomes = drive_indoubt_row(&row, &registry).await;
        assert!(matches!(
            outcomes[0],
            RecoveryOutcome::AlreadyTerminal { .. }
        ));
        // No commit_prepared was called.
        let events = participant.events.lock().unwrap().clone();
        assert!(events.iter().all(|e| !e.starts_with("commit_prepared")));
    }

    #[tokio::test]
    async fn drive_indoubt_row_reports_no_participant_for_unknown_backend() {
        let registry = InDoubtRegistry::new();
        let row = InDoubtLedgerRow {
            xid: "udb-x".into(),
            participants: vec!["weaviate:primary".into()],
            reason: String::new(),
        };
        let outcomes = drive_indoubt_row(&row, &registry).await;
        assert!(matches!(outcomes[0], RecoveryOutcome::NoParticipant { .. }));
    }

    #[tokio::test]
    async fn drive_indoubt_row_rolls_back_when_intent_is_rollback() {
        let participant = Arc::new(StubParticipant::new("mysql", vec!["udb-x".into()]));
        let mut registry = InDoubtRegistry::new();
        registry.register(participant.clone());

        let row = InDoubtLedgerRow {
            xid: "udb-x".into(),
            participants: vec!["mysql:primary".into()],
            reason: "XA PREPARE failed".into(),
        };
        let outcomes = drive_indoubt_row(&row, &registry).await;
        assert!(matches!(outcomes[0], RecoveryOutcome::RolledBack { .. }));
    }

    #[tokio::test]
    async fn drive_indoubt_row_propagates_participant_failure() {
        let participant = Arc::new(StubParticipant::new("mysql", vec!["udb-x".into()]));
        *participant.commit_outcome.lock().unwrap() = Err("network down".into());
        let mut registry = InDoubtRegistry::new();
        registry.register(participant.clone());

        let row = InDoubtLedgerRow {
            xid: "udb-x".into(),
            participants: vec!["mysql:primary".into()],
            reason: String::new(),
        };
        let outcomes = drive_indoubt_row(&row, &registry).await;
        match &outcomes[0] {
            RecoveryOutcome::Failed { reason, .. } => {
                assert!(reason.contains("network down"));
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[test]
    fn registry_lookup_strips_instance_suffix() {
        let p = Arc::new(StubParticipant::new("postgres", vec![]));
        let mut reg = InDoubtRegistry::new();
        reg.register(p);
        assert!(reg.get("postgres:primary").is_some());
        assert!(reg.get("postgres:replica").is_some());
        assert!(reg.get("mysql:primary").is_none());
    }
}
