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
        use sqlx::{Executor, Row};
        // `XA RECOVER` returns rows with `formatID`, `gtrid_length`,
        // `bqual_length`, `data`. The `data` column is the xid we
        // assigned (`udb-<uuid>`). We project just `data` so the
        // result is a clean Vec<String>.
        //
        // MySQL rejects `XA RECOVER` over the prepared-statement protocol
        // with ER_UNSUPPORTED_PS (1295, "not supported in the prepared
        // statement protocol yet"), which is what `sqlx::query(..)` uses.
        // Pass the raw SQL straight to the executor so it runs via the
        // text protocol (COM_QUERY) instead — otherwise every MySQL
        // in-doubt recovery attempt fails and the ledger row is never
        // driven terminal.
        let rows = self
            .pool
            .fetch_all("XA RECOVER")
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
            .or_else(|e| {
                if is_already_terminal_xid_error(&e) {
                    Ok(()) // already committed/gone elsewhere — recovery is idempotent.
                } else {
                    Err(format!("XA COMMIT failed: {e}"))
                }
            })
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
            .or_else(|e| {
                if is_already_terminal_xid_error(&e) {
                    Ok(()) // already rolled back/gone elsewhere — recovery is idempotent.
                } else {
                    Err(format!("XA ROLLBACK failed: {e}"))
                }
            })
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
            .or_else(|e| {
                if is_already_terminal_xid_error(&e) {
                    Ok(()) // already committed/gone elsewhere — recovery is idempotent.
                } else {
                    Err(format!("COMMIT PREPARED failed: {e}"))
                }
            })
    }

    async fn rollback_prepared(&self, xid: &str) -> Result<(), String> {
        validate_xid(xid)?;
        sqlx::query(&format!("ROLLBACK PREPARED '{xid}'"))
            .execute(&self.pool)
            .await
            .map(|_| ())
            .or_else(|e| {
                if is_already_terminal_xid_error(&e) {
                    Ok(()) // already rolled back/gone elsewhere — recovery is idempotent.
                } else {
                    Err(format!("ROLLBACK PREPARED failed: {e}"))
                }
            })
    }
}

/// Idempotent recovery (2PC): an "unknown / undefined xid" error from a
/// `COMMIT`/`ROLLBACK` of a prepared transaction means that xid was ALREADY
/// driven terminal — by another node, a prior recovery pass, or the coordinator
/// itself — which is precisely the state recovery is trying to reach. It MUST be
/// treated as success, not a failure to retry: otherwise `drive_indoubt_row`
/// marks the (already-resolved) row `Failed`, the in-doubt worker churns retries,
/// and after `max_attempts` false-escalates a settled transaction to
/// `manual_review`. Recognised signals: MySQL `ER_XAER_NOTA` (1397, SQLSTATE
/// `XAE04`, "Unknown XID"); Postgres `COMMIT/ROLLBACK PREPARED` on a missing gid
/// (SQLSTATE `42704`, "prepared transaction … does not exist"). A message
/// fallback covers drivers that surface the state without the exact SQLSTATE.
/// (Industry-standard idempotent-recovery handling — see the Percona XA-recovery
/// guide referenced on `recover_abandoned_mysql_prepared`.)
fn is_already_terminal_xid_error(err: &sqlx::Error) -> bool {
    let Some(db) = err.as_database_error() else {
        return false;
    };
    if let Some(code) = db.code() {
        // XAE04 = MySQL XAER_NOTA (unknown xid); 42704 = Postgres undefined_object
        // (prepared transaction does not exist).
        if code == "XAE04" || code == "42704" {
            return true;
        }
    }
    let message = db.message().to_ascii_lowercase();
    message.contains("unknown xid")
        || message.contains("xaer_nota")
        || message.contains("does not exist")
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

    /// Iterate every registered participant as `(label, participant)`. Used by
    /// the MySQL orphan-prepare sweep (XA1) to poll each MySQL participant's
    /// `XA RECOVER` list — MySQL has no `pg_prepared_xacts`-style catalog view
    /// the coordinator's PG pool can scan, so abandoned MySQL prepares are only
    /// discoverable through the participant connections themselves.
    pub fn entries(&self) -> impl Iterator<Item = (&str, &Arc<dyn XaInDoubtParticipant>)> {
        self.by_label
            .iter()
            .map(|(label, participant)| (label.as_str(), participant))
    }

    pub fn get(&self, label: &str) -> Option<Arc<dyn XaInDoubtParticipant>> {
        // Exact `<backend>:<instance>` match first, so multi-instance
        // backends (e.g. two MySQL servers) resolve to the RIGHT pool…
        if let Some(p) = self.by_label.get(&label.to_ascii_lowercase()) {
            return Some(p.clone());
        }
        // …then fall back to the bare backend label for participants
        // registered without an instance suffix.
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
            Err(reason) if is_already_terminal_prepared_xid_error(&reason) => {
                outcomes.push(RecoveryOutcome::AlreadyTerminal {
                    xid: xid.clone(),
                    backend,
                })
            }
            Err(reason) => outcomes.push(RecoveryOutcome::Failed {
                xid: xid.clone(),
                backend,
                reason,
            }),
        }
    }
    outcomes
}

fn is_already_terminal_prepared_xid_error(reason: &str) -> bool {
    let lower = reason.to_ascii_lowercase();
    lower.contains("does not exist")
        || lower.contains("not found")
        || lower.contains("unknown xid")
        || lower.contains("xaer_nota")
        || lower.contains("no such transaction")
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

/// What the presumed-abort sweep should do with one aged `udb-%`
/// prepared transaction, given its XA-ledger row (if any).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedSweepAction {
    /// No ledger row (or the ledger says rolled back / prepare failed):
    /// presumed-abort applies — `ROLLBACK PREPARED`.
    PresumeAbort,
    /// The coordinator durably decided COMMIT (write-ahead row or
    /// terminal `committed` row): drive the straggler forward with
    /// `COMMIT PREPARED`. NEVER roll back.
    DriveCommit,
    /// Owned by the in-doubt recovery worker or parked for an operator:
    /// leave the prepared transaction untouched.
    Skip,
}

/// Item 5: the sweep consults the ledger before presuming abort. Only an
/// aged prepared xact with NO ledger row may be presumed aborted; any row
/// carrying a commit decision (terminal `committed`, or in-doubt with
/// commit intent) must never be rolled back.
pub fn prepared_sweep_action(ledger: Option<(&str, &str)>) -> PreparedSweepAction {
    match ledger {
        None => PreparedSweepAction::PresumeAbort,
        Some((decision, reason)) => match decision {
            "committed" => PreparedSweepAction::DriveCommit,
            "rolled_back" => PreparedSweepAction::PresumeAbort,
            "in_doubt" => {
                let row = InDoubtLedgerRow {
                    xid: String::new(),
                    participants: Vec::new(),
                    reason: reason.to_string(),
                };
                match row.target_intent() {
                    // Commit-intent rows belong to the in-doubt worker,
                    // which drives them to COMMIT — never abort them here.
                    RecoveryIntent::Commit => PreparedSweepAction::Skip,
                    RecoveryIntent::Rollback => PreparedSweepAction::PresumeAbort,
                }
            }
            // `manual_review` and anything unknown: fail safe — an
            // operator owns the decision, do not destroy state.
            _ => PreparedSweepAction::Skip,
        },
    }
}

/// Startup/periodic recovery for abandoned PostgreSQL 2PC prepared transactions.
///
/// `begin_tx`'s live 2PC path issues `PREPARE TRANSACTION 'udb_<txid>'` and then
/// `COMMIT PREPARED` on a fresh connection. If the process crashes between the
/// two, the prepared transaction is stranded in `pg_prepared_xacts` holding its
/// row locks **forever**. This sweep applies the 2PC *presumed-abort* rule
/// **ledger-aware** (item 5): each aged `udb-`-prefixed prepared transaction is
/// joined against the XA ledger first — commit-decided xids are driven forward
/// (or left to the in-doubt worker), and only xids with NO ledger record are
/// `ROLLBACK PREPARED`-ed. The grace window leaves prepares from in-flight
/// requests untouched. Returns the number driven terminal (rolled back or
/// committed). Safe to call on every startup and on a recovery interval.
pub async fn recover_abandoned_prepared_transactions(
    pool: &sqlx::PgPool,
    config: &SystemCatalogConfig,
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
    if gids.is_empty() {
        return Ok(0);
    }
    ensure_xa_ledger_table(pool, config).await?;
    let relation = config.xa_ledger_relation();
    let ledger_rows: Vec<(String, String, String)> = sqlx::query_as(&format!(
        "SELECT xid, decision, reason FROM {relation} WHERE xid = ANY($1)"
    ))
    .bind(&gids)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("join prepared xacts against XA ledger failed: {e}"))?;
    let ledger_by_xid: HashMap<String, (String, String)> = ledger_rows
        .into_iter()
        .map(|(xid, decision, reason)| (xid, (decision, reason)))
        .collect();
    let mut driven_terminal = 0u64;
    for gid in gids {
        // Defense-in-depth: gid is operator-data-adjacent, so validate the
        // charset before interpolating into COMMIT/ROLLBACK PREPARED (xids
        // can't be parameter-bound in this statement form).
        if validate_xid(&gid).is_err() {
            tracing::warn!(gid = %gid, "skipping prepared xact with unexpected gid charset");
            continue;
        }
        let ledger = ledger_by_xid
            .get(&gid)
            .map(|(decision, reason)| (decision.as_str(), reason.as_str()));
        match prepared_sweep_action(ledger) {
            PreparedSweepAction::Skip => {
                tracing::info!(
                    gid = %gid,
                    "leaving prepared transaction to the in-doubt recovery worker / operator"
                );
            }
            PreparedSweepAction::DriveCommit => {
                match sqlx::query(&format!("COMMIT PREPARED '{gid}'"))
                    .execute(pool)
                    .await
                {
                    Ok(_) => {
                        driven_terminal += 1;
                        tracing::warn!(
                            gid = %gid,
                            "committed straggling prepared transaction per ledger commit decision"
                        );
                    }
                    Err(e) => tracing::warn!(
                        gid = %gid,
                        "failed to commit ledger-decided prepared transaction: {e}"
                    ),
                }
            }
            PreparedSweepAction::PresumeAbort => {
                match sqlx::query(&format!("ROLLBACK PREPARED '{gid}'"))
                    .execute(pool)
                    .await
                {
                    Ok(_) => {
                        driven_terminal += 1;
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
        }
    }
    Ok(driven_terminal)
}

/// XA1: reason stamped on a bootstrapped MySQL orphan-prepare ledger row. It MUST
/// contain a phrase that routes [`InDoubtLedgerRow::target_intent`] to
/// `Rollback` (here: "xa prepare") — otherwise the in-doubt worker would COMMIT an
/// undecided orphaned prepare, violating the 2PC presumed-abort rule. Pinned by
/// `mysql_orphan_reason_routes_to_rollback_intent`.
pub(crate) const MYSQL_ORPHAN_PRESUMED_ABORT_REASON: &str =
    "xa prepare orphan: participant prepared with no coordinator decision (presumed abort)";

/// XA1: startup/periodic recovery for abandoned **MySQL** 2PC prepared
/// transactions.
///
/// MySQL participants have no coordinator-visible `pg_prepared_xacts` analogue —
/// a prepared `XA` transaction is only discoverable via `XA RECOVER` on the
/// participant connection itself, which (unlike Postgres) reports NO prepare
/// timestamp. So a MySQL participant that crashed AFTER `XA PREPARE` but BEFORE
/// the coordinator durably recorded its decision leaks a prepared transaction
/// holding row locks **forever**: the ledger-driven in-doubt worker never sees it
/// (there is no ledger row), and the Postgres abandoned-prepared sweep only scans
/// `pg_prepared_xacts`.
///
/// This sweep polls every registered MySQL participant's `XA RECOVER`, keeps only
/// coordinator (`udb-`-prefixed) xids, joins them against the XA ledger, and:
///   * **no ledger row** → records a bootstrap `in_doubt` row carrying a
///     rollback-intent reason and the participant label (`ON CONFLICT DO NOTHING`,
///     so a real coordinator decision that raced in first is never clobbered). The
///     existing in-doubt worker then drives it to `ROLLBACK PREPARED` on its next
///     pass — the 2PC *presumed-abort* rule (no durable commit decision ⇒ abort).
///     Because the coordinator writes its real decision within milliseconds of
///     PREPARE and that write wins the ledger upsert, a genuinely in-flight
///     transaction is reconciled to its true outcome before the next pass; only a
///     truly abandoned prepare survives to be rolled back (grace = one interval).
///   * **`committed`** → `XA COMMIT` the straggler now (the in-doubt worker only
///     scans `in_doubt`, so a commit-decided-but-still-prepared MySQL participant
///     would otherwise leak).
///   * **`rolled_back`** → `XA ROLLBACK` the straggler now (same rationale).
///   * **`in_doubt` / `manual_review`** → leave it: the in-doubt worker or an
///     operator owns that xid, so acting here would race them.
///
/// Returns the number of MySQL prepared xids driven to (or newly scheduled for) a
/// terminal state.
///
/// Recovery-procedure reference (xid round-trip, single- vs three-part `XA COMMIT`,
/// MySQL 5.7+ cross-connection recovery): Percona, "How to Deal with XA
/// Transactions Recovery" — https://www.percona.com/blog/how-to-deal-with-xa-transactions-recovery/.
/// UDB's coordinator xid is a printable single-part gtrid (`udb-<uuid>`, empty
/// bqual, default formatID via `xa::new_xid`), so the participant's single-part
/// `XA COMMIT '<xid>'` round-trips — the article's three-part hex form is only
/// needed for the non-printable/bqual'd xids app servers emit.
pub async fn recover_abandoned_mysql_prepared(
    pool: &sqlx::PgPool,
    config: &SystemCatalogConfig,
    registry: &InDoubtRegistry,
) -> Result<u64, String> {
    // (label, xid) for every coordinator-owned prepared xid across all registered
    // MySQL participants.
    let mut prepared: Vec<(String, String)> = Vec::new();
    for (label, participant) in registry.entries() {
        if !participant
            .backend_label()
            .to_ascii_lowercase()
            .starts_with("mysql")
        {
            continue;
        }
        match participant.list_prepared_xids().await {
            Ok(xids) => {
                for xid in xids {
                    // Only coordinator xids (`udb-<uuid>`); never touch a
                    // co-tenant application's own XA transactions.
                    if xid.starts_with("udb-") && validate_xid(&xid).is_ok() {
                        prepared.push((label.to_string(), xid));
                    }
                }
            }
            Err(err) => tracing::warn!(
                participant = %label,
                "XA RECOVER for the MySQL orphan sweep failed; skipping this participant: {err}"
            ),
        }
    }
    if prepared.is_empty() {
        return Ok(0);
    }
    ensure_xa_ledger_table(pool, config).await?;
    let relation = config.xa_ledger_relation();
    let xids: Vec<String> = prepared.iter().map(|(_, xid)| xid.clone()).collect();
    let ledger_rows: Vec<(String, String)> =
        sqlx::query_as(&format!("SELECT xid, decision FROM {relation} WHERE xid = ANY($1)"))
            .bind(&xids)
            .fetch_all(pool)
            .await
            .map_err(|e| format!("join MySQL prepared xids against XA ledger failed: {e}"))?;
    let decision_by_xid: HashMap<String, String> = ledger_rows.into_iter().collect();
    let mut driven_terminal = 0u64;
    for (label, xid) in prepared {
        let participant = match registry.get(&label) {
            Some(p) => p,
            None => continue,
        };
        match decision_by_xid.get(&xid).map(String::as_str) {
            None => {
                // Orphan: `xa prepare` in the reason routes
                // `InDoubtLedgerRow::target_intent()` to Rollback (presumed-abort);
                // ON CONFLICT DO NOTHING so a real decision that raced in first is
                // preserved. The in-doubt worker performs the rollback next pass.
                let participants = serde_json::to_value(vec![label.clone()])
                    .map_err(|e| format!("serialize MySQL orphan participant failed: {e}"))?;
                let inserted = sqlx::query(&format!(
                    "INSERT INTO {relation}
                         (xid, tenant_id, project_id, origin_rpc, correlation_id,
                          participants, decision, reason, decided_at, updated_at)
                     VALUES ($1,'','','xa-recovery-orphan-sweep','',$2::JSONB,'in_doubt',
                             $3, NOW(), NOW())
                     ON CONFLICT (xid) DO NOTHING"
                ))
                .bind(&xid)
                .bind(participants)
                .bind(MYSQL_ORPHAN_PRESUMED_ABORT_REASON)
                .execute(pool)
                .await
                .map_err(|e| format!("bootstrap MySQL orphan ledger row failed: {e}"))?;
                if inserted.rows_affected() > 0 {
                    driven_terminal += 1;
                    tracing::warn!(
                        xid = %xid,
                        participant = %label,
                        "bootstrapped abandoned MySQL prepared transaction for presumed-abort recovery"
                    );
                }
            }
            Some("committed") => match participant.commit_prepared(&xid).await {
                Ok(_) => {
                    driven_terminal += 1;
                    tracing::warn!(
                        xid = %xid,
                        participant = %label,
                        "committed straggling MySQL prepared transaction per ledger commit decision"
                    );
                }
                Err(e) => tracing::warn!(
                    xid = %xid,
                    "failed to commit ledger-decided MySQL prepared transaction: {e}"
                ),
            },
            Some("rolled_back") => match participant.rollback_prepared(&xid).await {
                Ok(_) => {
                    driven_terminal += 1;
                    tracing::warn!(
                        xid = %xid,
                        participant = %label,
                        "rolled back straggling MySQL prepared transaction per ledger rollback decision"
                    );
                }
                Err(e) => tracing::warn!(
                    xid = %xid,
                    "failed to roll back ledger-decided MySQL prepared transaction: {e}"
                ),
            },
            // `in_doubt` (owned by the in-doubt worker) / `manual_review` (owned by
            // an operator) / anything unknown: leave it, do not race the owner.
            Some(_) => {}
        }
    }
    Ok(driven_terminal)
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
    sqlx::query(&record_xa_ledger_upsert_sql(&relation))
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

fn record_xa_ledger_upsert_sql(relation: &str) -> String {
    format!(
        "INSERT INTO {relation} AS xa_current
             (xid, tenant_id, project_id, origin_rpc, correlation_id,
              participants, decision, reason, decided_at, updated_at)
         VALUES ($1,$2,$3,$4,$5,$6::JSONB,$7,$8,
                 to_timestamp($9::DOUBLE PRECISION / 1000.0), NOW())
         ON CONFLICT (xid) DO UPDATE SET
              tenant_id = CASE WHEN xa_current.decision = 'committed' THEN xa_current.tenant_id ELSE EXCLUDED.tenant_id END,
              project_id = CASE WHEN xa_current.decision = 'committed' THEN xa_current.project_id ELSE EXCLUDED.project_id END,
              origin_rpc = CASE WHEN xa_current.decision = 'committed' THEN xa_current.origin_rpc ELSE EXCLUDED.origin_rpc END,
              correlation_id = CASE WHEN xa_current.decision = 'committed' THEN xa_current.correlation_id ELSE EXCLUDED.correlation_id END,
              participants = CASE WHEN xa_current.decision = 'committed' THEN xa_current.participants ELSE EXCLUDED.participants END,
              decision = CASE WHEN xa_current.decision = 'committed' THEN xa_current.decision ELSE EXCLUDED.decision END,
              reason = CASE WHEN xa_current.decision = 'committed' THEN xa_current.reason ELSE EXCLUDED.reason END,
              decided_at = CASE WHEN xa_current.decision = 'committed' THEN xa_current.decided_at ELSE EXCLUDED.decided_at END,
              updated_at = CASE WHEN xa_current.decision = 'committed' THEN xa_current.updated_at ELSE NOW() END"
    )
}

/// One scan page for the in-doubt sweep. Pagination (item 5c) replaces the
/// old hard `LIMIT 100`: pages are fetched until the backlog is drained.
const INDOUBT_SCAN_PAGE_SIZE: usize = 200;

/// Registry with the always-available participant: the broker's own
/// Postgres pool. Callers (the startup/periodic recovery worker) extend
/// it with one `MysqlInDoubtParticipant` per configured MySQL instance.
pub fn default_indoubt_registry(pool: &sqlx::PgPool) -> InDoubtRegistry {
    let mut registry = InDoubtRegistry::new();
    registry.register(Arc::new(PostgresInDoubtParticipant {
        label: "postgres".to_string(),
        pool: pool.clone(),
    }));
    registry
}

/// Item 24: decide what an in-doubt row's decision becomes after one more
/// failed recovery attempt. At/after `max_attempts` the row is parked as
/// `manual_review` and the scan (which only selects `in_doubt`) stops
/// retrying it. `max_attempts == 0` disables escalation (retry forever).
pub fn next_decision_after_failed_attempt(
    attempts_after_increment: u32,
    max_attempts: u32,
) -> XaDecision {
    if max_attempts > 0 && attempts_after_increment >= max_attempts {
        XaDecision::ManualReview
    } else {
        XaDecision::InDoubt
    }
}

pub async fn recover_xa_ledger_indoubt(
    pool: &sqlx::PgPool,
    config: &SystemCatalogConfig,
) -> Result<u64, String> {
    let registry = default_indoubt_registry(pool);
    recover_xa_ledger_indoubt_with(pool, config, &registry, &RecoveryConfig::default()).await
}

/// In-doubt sweep against an explicit participant registry (item 23 wires
/// MySQL participants in via the recovery worker) with escalation to
/// `manual_review` after `recovery.max_attempts` failed sweeps (item 24).
pub async fn recover_xa_ledger_indoubt_with(
    pool: &sqlx::PgPool,
    config: &SystemCatalogConfig,
    registry: &InDoubtRegistry,
    recovery: &RecoveryConfig,
) -> Result<u64, String> {
    ensure_xa_ledger_table(pool, config).await?;
    let relation = config.xa_ledger_relation();
    let mut terminal = 0u64;
    // Failed rows get `updated_at = NOW()` and re-match the predicate, so
    // track processed xids to guarantee the page loop terminates after one
    // full pass over the backlog.
    let mut processed: std::collections::HashSet<String> = std::collections::HashSet::new();
    loop {
        let rows = sqlx::query(&format!(
            "SELECT xid, participants, reason, recovery_attempts FROM {relation}
             WHERE decision = 'in_doubt'
             ORDER BY updated_at ASC
             LIMIT {INDOUBT_SCAN_PAGE_SIZE}"
        ))
        .fetch_all(pool)
        .await
        .map_err(|e| format!("load XA in-doubt ledger rows failed: {e}"))?;
        let page_len = rows.len();
        let mut progressed = false;
        for db_row in rows {
            let xid: String = db_row.try_get("xid").map_err(|e| e.to_string())?;
            if !processed.insert(xid.clone()) {
                continue;
            }
            progressed = true;
            let participants_json: serde_json::Value =
                db_row.try_get("participants").map_err(|e| e.to_string())?;
            let participants = serde_json::from_value::<Vec<String>>(participants_json)
                .map_err(|e| format!("decode XA participants for {xid}: {e}"))?;
            let reason: String = db_row.try_get("reason").unwrap_or_default();
            let attempts: i32 = db_row.try_get("recovery_attempts").unwrap_or(0);
            let row = InDoubtLedgerRow {
                xid: xid.clone(),
                participants,
                reason,
            };
            let outcomes = drive_indoubt_row(&row, registry).await;
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
                let next_decision = next_decision_after_failed_attempt(
                    attempts.max(0) as u32 + 1,
                    recovery.max_attempts,
                );
                sqlx::query(&format!(
                    "UPDATE {relation}
                     SET recovery_attempts = recovery_attempts + 1,
                         decision = $3,
                         reason = CASE WHEN $2 = '' THEN reason ELSE $2 END,
                         updated_at = NOW()
                     WHERE xid = $1"
                ))
                .bind(&xid)
                .bind(reason)
                .bind(next_decision.as_str())
                .execute(pool)
                .await
                .map_err(|e| format!("update XA retry state failed: {e}"))?;
                if next_decision == XaDecision::ManualReview {
                    tracing::error!(
                        xid = %xid,
                        attempts = attempts + 1,
                        max_attempts = recovery.max_attempts,
                        "XA in-doubt transaction exceeded max recovery attempts; parked for manual review"
                    );
                }
            }
        }
        if page_len < INDOUBT_SCAN_PAGE_SIZE || !progressed {
            break;
        }
    }
    Ok(terminal)
}

/// One full XA recovery pass: drive in-doubt ledger rows terminal, then
/// run the ledger-aware presumed-abort sweep over aged prepared xacts.
/// Shared by the startup run and the periodic lease-gated worker (item 24).
pub async fn run_xa_recovery_pass(
    pool: &sqlx::PgPool,
    config: &SystemCatalogConfig,
    registry: &InDoubtRegistry,
    recovery: &RecoveryConfig,
    grace_secs: i64,
) -> Result<(u64, u64), String> {
    let ledger = recover_xa_ledger_indoubt_with(pool, config, registry, recovery).await?;
    let abandoned = recover_abandoned_prepared_transactions(pool, config, grace_secs).await?;
    // XA1: MySQL has no coordinator-scannable prepared-xact catalog, so abandoned
    // MySQL prepares are swept by polling each participant's `XA RECOVER`. Run it
    // AFTER the in-doubt worker so a bootstrap row inserted here is picked up on
    // the NEXT pass (the one-interval presumed-abort grace), never the same one.
    let mysql_orphans = recover_abandoned_mysql_prepared(pool, config, registry).await?;
    Ok((ledger, abandoned.saturating_add(mysql_orphans)))
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
         SET decision = CASE
                 WHEN decision = 'committed' AND $2 <> 'committed' THEN decision
                 ELSE $2
             END,
             reason = CASE
                 WHEN decision = 'committed' AND $2 <> 'committed' THEN reason
                 WHEN $3 = '' THEN reason
                 ELSE $3
             END,
             updated_at = CASE
                 WHEN decision = 'committed' AND $2 <> 'committed' THEN updated_at
                 ELSE NOW()
             END
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

fn xa_ledger_statements(config: &SystemCatalogConfig) -> Vec<String> {
    let relation = config.xa_ledger_relation();
    let table = &config.xa_ledger_table;
    vec![
        format!(
            "CREATE TABLE IF NOT EXISTS {relation} (
                xid TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL DEFAULT '',
                project_id TEXT NOT NULL DEFAULT '',
                origin_rpc TEXT NOT NULL DEFAULT '',
                correlation_id TEXT NOT NULL DEFAULT '',
                participants JSONB NOT NULL DEFAULT '[]'::JSONB,
                decision TEXT NOT NULL DEFAULT 'in_doubt'
                    CHECK (decision IN ('committed','rolled_back','in_doubt','manual_review')),
                reason TEXT NOT NULL DEFAULT '',
                recovery_attempts INTEGER NOT NULL DEFAULT 0,
                decided_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )"
        ),
        // Item 24 migration: ledgers created before the `manual_review`
        // decision existed carry the narrower CHECK. The DO block only
        // takes the ACCESS EXCLUSIVE lock when the constraint actually
        // needs widening, so steady-state calls stay catalog-read-only.
        format!(
            "DO $$
             BEGIN
                 IF EXISTS (
                     SELECT 1 FROM pg_constraint
                     WHERE conrelid = to_regclass('{relation}')
                       AND conname = '{table}_decision_check'
                       AND pg_get_constraintdef(oid) NOT LIKE '%manual_review%'
                 ) THEN
                     ALTER TABLE {relation} DROP CONSTRAINT \"{table}_decision_check\";
                     ALTER TABLE {relation} ADD CONSTRAINT \"{table}_decision_check\"
                         CHECK (decision IN ('committed','rolled_back','in_doubt','manual_review'));
                 END IF;
             END $$"
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
    fn mysql_orphan_reason_routes_to_rollback_intent() {
        // XA1 SAFETY INVARIANT: a bootstrapped MySQL orphan-prepare row is written
        // as `in_doubt`, so the in-doubt worker will drive it. Its reason MUST make
        // `target_intent()` == Rollback — if it ever routed to Commit, the worker
        // would COMMIT a prepare the coordinator never durably decided, violating
        // the 2PC presumed-abort rule. This pins the exact stamped reason.
        let row = InDoubtLedgerRow {
            xid: "udb-orphan".into(),
            participants: vec!["mysql:primary".into()],
            reason: MYSQL_ORPHAN_PRESUMED_ABORT_REASON.into(),
        };
        assert_eq!(row.target_intent(), RecoveryIntent::Rollback);
        // And prepared_sweep_action for that same in_doubt+rollback-intent row
        // presumes abort (the eventual ROLLBACK PREPARED), never a commit.
        assert_eq!(
            prepared_sweep_action(Some(("in_doubt", MYSQL_ORPHAN_PRESUMED_ABORT_REASON))),
            PreparedSweepAction::PresumeAbort
        );
    }

    #[test]
    fn prepared_sweep_never_rolls_back_commit_decided_rows() {
        assert_eq!(
            prepared_sweep_action(Some(("committed", ""))),
            PreparedSweepAction::DriveCommit
        );
        assert_eq!(
            prepared_sweep_action(Some((
                "in_doubt",
                crate::runtime::xa::XA_COMMIT_INTENT_REASON
            ))),
            PreparedSweepAction::Skip
        );
        assert_eq!(
            prepared_sweep_action(Some(("manual_review", ""))),
            PreparedSweepAction::Skip
        );
    }

    #[test]
    fn failed_attempt_policy_escalates_to_manual_review_at_max_attempts() {
        assert_eq!(
            next_decision_after_failed_attempt(1, 3),
            XaDecision::InDoubt
        );
        assert_eq!(
            next_decision_after_failed_attempt(3, 3),
            XaDecision::ManualReview
        );
        assert_eq!(
            next_decision_after_failed_attempt(99, 0),
            XaDecision::InDoubt,
            "max_attempts=0 means retry forever"
        );
    }

    #[test]
    fn indoubt_scan_page_size_processes_more_than_legacy_hundred_rows() {
        assert!(
            INDOUBT_SCAN_PAGE_SIZE > 100,
            "XA in-doubt recovery must page beyond the old hard LIMIT 100"
        );
    }

    #[test]
    fn record_xa_ledger_upsert_preserves_existing_committed_decision() {
        let sql = record_xa_ledger_upsert_sql("\"system\".\"udb_xa_ledger\"");
        assert!(sql.contains("AS xa_current"));
        assert!(sql.contains(
            "decision = CASE WHEN xa_current.decision = 'committed' THEN xa_current.decision ELSE EXCLUDED.decision END"
        ));
        assert!(sql.contains(
            "updated_at = CASE WHEN xa_current.decision = 'committed' THEN xa_current.updated_at ELSE NOW() END"
        ));
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

    #[tokio::test]
    async fn split_brain_duplicate_commit_is_treated_as_already_terminal() {
        let participant = Arc::new(StubParticipant::new("postgres", vec!["udb-x".into()]));
        *participant.commit_outcome.lock().unwrap() =
            Err("prepared transaction with identifier \"udb-x\" does not exist".into());
        let mut registry = InDoubtRegistry::new();
        registry.register(participant.clone());

        let row = InDoubtLedgerRow {
            xid: "udb-x".into(),
            participants: vec!["postgres:primary".into()],
            reason: "COMMIT PREPARED transport error".into(),
        };
        let outcomes = drive_indoubt_row(&row, &registry).await;
        assert!(matches!(
            outcomes[0],
            RecoveryOutcome::AlreadyTerminal { .. }
        ));
        assert!(
            outcomes.iter().all(RecoveryOutcome::is_terminal),
            "a duplicate recovery worker that loses the final COMMIT race must not poison the ledger"
        );
    }

    #[tokio::test]
    async fn repeated_indoubt_sweeps_do_not_recommit_terminal_xid() {
        let participant = Arc::new(StubParticipant::new("mysql", vec!["udb-repeat".into()]));
        let mut registry = InDoubtRegistry::new();
        registry.register(participant.clone());

        let row = InDoubtLedgerRow {
            xid: "udb-repeat".into(),
            participants: vec!["mysql:primary".into()],
            reason: String::new(),
        };
        let first = drive_indoubt_row(&row, &registry).await;
        assert!(matches!(first[0], RecoveryOutcome::Committed { .. }));

        let second = drive_indoubt_row(&row, &registry).await;
        assert!(matches!(second[0], RecoveryOutcome::AlreadyTerminal { .. }));
        let events = participant.events.lock().unwrap().clone();
        let commits = events
            .iter()
            .filter(|event| event.starts_with("commit_prepared"))
            .count();
        assert_eq!(commits, 1, "terminal recovery must be idempotent");
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
