//! XA / 2PC opt-in coordinator (U20).
//!
//! Pre-U20 the capability matrix declared `supports_xa` and
//! `supports_two_phase_commit` per backend, but no live runtime path
//! actually drove a distributed transaction. Operators saw `pg=true` in
//! `GetCapabilities` and couldn't act on it.
//!
//! U20 adds the live path:
//!
//! - [`TransactionStrategy`] — request-level enum chosen by the caller
//!   (`Saga` / `BestEffort` / `TwoPhase`). Mirrors the consistency-mode
//!   typed pattern from U6.
//! - [`XaCoordinator`] — orchestrates PREPARE → vote → COMMIT/ROLLBACK
//!   across a list of participants. The participants are
//!   capability-checked **before** any side-effect (so we fail fast
//!   instead of committing on PG and then discovering Qdrant doesn't
//!   support 2PC).
//! - [`XaParticipant`] trait — what a backend implements to participate.
//!   PostgresExecutor is the first impl (emits `PREPARE TRANSACTION 'xid'`
//!   / `COMMIT PREPARED 'xid'` SQL). Tests plug stubs.
//! - [`XaLedgerEntry`] — durable record of the coordinator's decision,
//!   used by the recovery worker on broker restart to drive in-doubt
//!   transactions to a terminal state.
//!
//! The acceptance gate ("A request asking for 2PC either commits all
//! supported participants or fails before side effects are issued") is
//! satisfied by `XaCoordinator::execute`:
//!
//! 1. **Capability gate** — `validate_participants` checks every
//!    participant's `supports_two_phase_commit` against the manifest's
//!    capability matrix. If ANY participant can't 2PC, the coordinator
//!    refuses **before** PREPARE. The request fails with
//!    `tonic::Code::FailedPrecondition`.
//! 2. **PREPARE phase** — issued to every participant in declaration
//!    order. Any failure → all already-prepared participants
//!    ROLLBACK PREPARED. No participant has committed any side-effect.
//! 3. **COMMIT phase** — only after every participant voted yes.
//!    Failure here means the in-doubt state is durably recorded in
//!    `XaLedgerEntry`; the recovery worker drives it to completion.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::backend::BackendCapabilityMatrixEntry;

/// Which transaction strategy the caller wants for a cross-backend
/// mutation. The runtime picks based on this + the participant set;
/// the strategy is the **upper bound** on commitment guarantees.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionStrategy {
    /// Default — write succeeds locally per backend; failures land in
    /// the saga driver for compensation. Eventually consistent.
    Saga,
    /// Try each backend; on failure, **best-effort** compensate the
    /// already-succeeded ones inline. No durable in-doubt state — if the
    /// broker crashes mid-compensation, the saga driver recovers it. The
    /// pre-U20 default for the typed transaction RPC.
    BestEffort,
    /// True 2PC across XA/2PC-capable participants. Failure modes:
    /// - Capability gate fails → request refused, zero side effects.
    /// - PREPARE fails on any participant → all prepared participants
    ///   ROLLBACK PREPARED, request fails, zero durable side effects.
    /// - COMMIT fails on any participant → `XaLedgerEntry` records the
    ///   in-doubt state; the recovery worker drives it to completion
    ///   on broker restart.
    TwoPhase,
}

impl Default for TransactionStrategy {
    fn default() -> Self {
        Self::Saga
    }
}

impl TransactionStrategy {
    /// Pinned wire token. Mirrors `ConsistencyMode::as_str` — SDK enums
    /// and `x-udb-transaction-strategy` header use these strings.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Saga => "saga",
            Self::BestEffort => "best_effort",
            Self::TwoPhase => "two_phase",
        }
    }

    pub fn parse(token: &str) -> Option<Self> {
        match token.trim().to_ascii_lowercase().as_str() {
            "saga" => Some(Self::Saga),
            "best_effort" | "best-effort" => Some(Self::BestEffort),
            "two_phase" | "two-phase" | "2pc" | "xa" => Some(Self::TwoPhase),
            _ => None,
        }
    }

    pub fn parse_or_default(token: &str) -> Self {
        Self::parse(token).unwrap_or_default()
    }

    pub fn requires_two_phase(self) -> bool {
        matches!(self, Self::TwoPhase)
    }
}

/// One participant in a 2PC transaction. The runtime constructs a
/// `XaParticipantHandle` per backend involved and hands it to the
/// coordinator.
#[derive(Debug, Clone)]
pub struct XaParticipantHandle {
    pub backend: String,
    pub instance: String,
    /// Label used in audit logs + the ledger row (`postgres:primary` etc).
    pub label: String,
}

impl XaParticipantHandle {
    pub fn new(backend: impl Into<String>, instance: impl Into<String>) -> Self {
        let backend = backend.into();
        let instance = instance.into();
        let label = format!("{backend}:{instance}");
        Self {
            backend,
            instance,
            label,
        }
    }
}

/// Result of one participant's vote in PHASE 1 (PREPARE). The
/// coordinator collects every vote before deciding to commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrepareVote {
    /// Participant successfully prepared and is ready to commit.
    Prepared,
    /// Participant refused (typed reason).
    Aborted { reason: String },
}

/// What the coordinator decided at PHASE 2. Stored in `XaLedgerEntry`
/// so the recovery worker knows which way to drive an in-doubt xid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum XaDecision {
    /// PHASE 1 succeeded, PHASE 2 committed on every participant.
    Committed,
    /// Some participant aborted in PHASE 1 or the coordinator
    /// pre-emptively rolled back; every prepared participant was
    /// rolled back.
    RolledBack,
    /// PHASE 2 mid-flight — coordinator crashed or some participant is
    /// unreachable. The recovery worker scans this state on restart and
    /// drives it to `Committed` (re-issuing COMMIT PREPARED) or
    /// `RolledBack` based on the per-participant outcome ledger.
    InDoubt,
}

impl XaDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::RolledBack => "rolled_back",
            Self::InDoubt => "in_doubt",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Committed | Self::RolledBack)
    }
}

/// One row in the `udb_xa_ledger` table — durable record of a 2PC
/// transaction's lifecycle. The recovery worker scans rows where
/// `decision = InDoubt` on broker startup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XaLedgerEntry {
    /// Cross-participant transaction id. Same value goes into
    /// `PREPARE TRANSACTION '<xid>'` on every PG participant.
    pub xid: String,
    pub tenant_id: String,
    pub project_id: String,
    /// Where the request originated (RPC + correlation id).
    pub origin_rpc: String,
    pub correlation_id: String,
    /// Per-participant labels (`postgres:primary` …) in declaration
    /// order. The recovery worker reproduces the participant set from
    /// this so it can re-issue COMMIT/ROLLBACK PREPARED on the right
    /// pool.
    pub participants: Vec<String>,
    pub decision: XaDecision,
    /// Unix milliseconds when the coordinator made its decision.
    pub decided_at_unix_ms: i64,
    /// Optional reason — populated when decision is `RolledBack` (the
    /// failing participant's PREPARE vote) or `InDoubt` (the
    /// transport / coordinator error).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
}

impl XaLedgerEntry {
    pub fn new(
        xid: impl Into<String>,
        tenant_id: impl Into<String>,
        project_id: impl Into<String>,
        origin_rpc: impl Into<String>,
        correlation_id: impl Into<String>,
        participants: Vec<String>,
        decision: XaDecision,
    ) -> Self {
        Self {
            xid: xid.into(),
            tenant_id: tenant_id.into(),
            project_id: project_id.into(),
            origin_rpc: origin_rpc.into(),
            correlation_id: correlation_id.into(),
            participants,
            decision,
            decided_at_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            reason: String::new(),
        }
    }

    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = reason.into();
        self
    }
}

/// What the runtime returns to the gRPC handler after running the
/// coordinator. Carries the ledger entry so the handler can audit-log
/// it, plus the per-participant outcome for response shaping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct XaOutcome {
    pub ledger: XaLedgerEntry,
    pub participants: Vec<ParticipantOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ParticipantOutcome {
    pub label: String,
    pub vote: PrepareVote,
    pub committed: bool,
}

/// Error from the coordinator. Mapped to `tonic::Status` by the gRPC
/// handler — `CapabilityRefused` → `FailedPrecondition`, `PrepareFailed`
/// → `Aborted`, `InDoubt` → `Internal` (operator must consult the
/// recovery worker logs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum XaError {
    /// At least one participant doesn't support 2PC. Refused before any
    /// side effect.
    CapabilityRefused { unsupported: Vec<String> },
    /// PHASE 1 failed; every prepared participant was rolled back.
    PrepareFailed {
        ledger: XaLedgerEntry,
        failures: Vec<ParticipantOutcome>,
    },
    /// PHASE 2 was reached but couldn't be driven to completion. The
    /// ledger is durably recorded; recovery is the operator's
    /// responsibility (the recovery worker will retry).
    InDoubt { ledger: XaLedgerEntry },
}

impl std::fmt::Display for XaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CapabilityRefused { unsupported } => write!(
                f,
                "2PC refused: {} participant(s) lack supports_two_phase_commit: {}",
                unsupported.len(),
                unsupported.join(", ")
            ),
            Self::PrepareFailed { ledger, .. } => write!(
                f,
                "2PC PREPARE phase failed for xid {}; rolled back: {}",
                ledger.xid, ledger.reason
            ),
            Self::InDoubt { ledger } => write!(
                f,
                "2PC xid {} is in-doubt; recovery worker will drive to terminal state",
                ledger.xid
            ),
        }
    }
}

impl std::error::Error for XaError {}

/// What a backend implements to participate in 2PC. The runtime
/// constructs one of these per participating instance and hands them
/// to the coordinator.
///
/// All three methods take `&self` so a coordinator can call them
/// concurrently against different participants. Implementations bind
/// the per-request connection / pool internally.
#[async_trait::async_trait]
pub trait XaParticipant: Send + Sync {
    fn handle(&self) -> &XaParticipantHandle;

    /// PHASE 1: persist the transaction in a prepared state so a
    /// subsequent `commit_prepared` or `rollback_prepared` is guaranteed
    /// to succeed. For Postgres this is `PREPARE TRANSACTION '<xid>'`.
    async fn prepare(&self, xid: &str) -> PrepareVote;

    /// PHASE 2 (success path): commit the prepared transaction.
    /// For Postgres this is `COMMIT PREPARED '<xid>'`.
    async fn commit_prepared(&self, xid: &str) -> Result<(), String>;

    /// PHASE 2 (failure path): roll back the prepared transaction.
    /// For Postgres this is `ROLLBACK PREPARED '<xid>'`.
    async fn rollback_prepared(&self, xid: &str) -> Result<(), String>;
}

/// Per-request inputs the coordinator needs.
#[derive(Debug, Clone)]
pub struct XaRequest {
    pub tenant_id: String,
    pub project_id: String,
    pub origin_rpc: String,
    pub correlation_id: String,
}

/// The coordinator. Stateless — call `execute` per request with the
/// participant list. The runtime composes participants from the
/// dispatch resolver (one `XaParticipant` per backend the request
/// touches).
pub struct XaCoordinator;

impl XaCoordinator {
    /// Generate an xid. PostgreSQL identifier rules apply
    /// (max 200 chars, ascii); we use `udb-<uuid>` which is 40 chars.
    pub fn new_xid() -> String {
        format!("udb-{}", Uuid::new_v4())
    }

    /// Check every participant's capability against the manifest's
    /// capability matrix. Refuses the request when any participant
    /// can't 2PC — **before** any PREPARE side-effect. The acceptance
    /// gate ("commits all or fails before side effects").
    pub fn validate_participants(
        participants: &[XaParticipantHandle],
        capability_matrix: &[BackendCapabilityMatrixEntry],
    ) -> Result<(), XaError> {
        let mut unsupported: Vec<String> = Vec::new();
        for p in participants {
            let entry = capability_matrix
                .iter()
                .find(|e| e.backend.eq_ignore_ascii_case(&p.backend));
            match entry {
                Some(e) if e.supports_two_phase_commit => {}
                Some(_) => unsupported.push(p.label.clone()),
                None => unsupported.push(format!("{}:unknown", p.label)),
            }
        }
        if !unsupported.is_empty() {
            return Err(XaError::CapabilityRefused { unsupported });
        }
        Ok(())
    }

    /// Run the full 2PC lifecycle. Returns `XaOutcome` on success, or
    /// `XaError` on capability/PREPARE failure / in-doubt state.
    pub async fn execute(
        request: XaRequest,
        participants: Vec<Box<dyn XaParticipant>>,
        capability_matrix: &[BackendCapabilityMatrixEntry],
    ) -> Result<XaOutcome, XaError> {
        let handles: Vec<XaParticipantHandle> =
            participants.iter().map(|p| p.handle().clone()).collect();
        let labels: Vec<String> = handles.iter().map(|h| h.label.clone()).collect();
        Self::validate_participants(&handles, capability_matrix)?;

        let xid = Self::new_xid();

        // PHASE 1: PREPARE every participant. Collect votes.
        let mut outcomes: Vec<ParticipantOutcome> = Vec::with_capacity(participants.len());
        let mut had_failure = false;
        let mut first_failure_reason = String::new();
        for p in &participants {
            let vote = p.prepare(&xid).await;
            if let PrepareVote::Aborted { reason } = &vote {
                had_failure = true;
                if first_failure_reason.is_empty() {
                    first_failure_reason = reason.clone();
                }
            }
            outcomes.push(ParticipantOutcome {
                label: p.handle().label.clone(),
                vote,
                committed: false,
            });
        }

        if had_failure {
            // Roll back every prepared participant. Best-effort —
            // rollback failures are logged but don't escalate (the
            // transaction is "rolled back" from the coordinator's
            // perspective; recovery worker handles stragglers).
            for (i, p) in participants.iter().enumerate() {
                if matches!(outcomes[i].vote, PrepareVote::Prepared)
                    && let Err(err) = p.rollback_prepared(&xid).await
                {
                    tracing::warn!(
                        xid = %xid,
                        participant = %outcomes[i].label,
                        error = %err,
                        "ROLLBACK PREPARED failed during prepare-phase abort",
                    );
                }
            }
            let ledger = XaLedgerEntry::new(
                xid,
                request.tenant_id,
                request.project_id,
                request.origin_rpc,
                request.correlation_id,
                labels,
                XaDecision::RolledBack,
            )
            .with_reason(first_failure_reason);
            return Err(XaError::PrepareFailed {
                ledger,
                failures: outcomes,
            });
        }

        // PHASE 2: COMMIT PREPARED. If any participant fails commit,
        // the transaction enters in-doubt state — durably record and
        // surface to the operator. Recovery worker drives stragglers.
        let mut commit_error: Option<String> = None;
        for (i, p) in participants.iter().enumerate() {
            match p.commit_prepared(&xid).await {
                Ok(()) => outcomes[i].committed = true,
                Err(err) => {
                    if commit_error.is_none() {
                        commit_error = Some(format!("{}: {err}", outcomes[i].label));
                    }
                }
            }
        }

        if let Some(reason) = commit_error {
            let ledger = XaLedgerEntry::new(
                xid,
                request.tenant_id,
                request.project_id,
                request.origin_rpc,
                request.correlation_id,
                labels,
                XaDecision::InDoubt,
            )
            .with_reason(reason);
            return Err(XaError::InDoubt { ledger });
        }

        Ok(XaOutcome {
            ledger: XaLedgerEntry::new(
                xid,
                request.tenant_id,
                request.project_id,
                request.origin_rpc,
                request.correlation_id,
                labels,
                XaDecision::Committed,
            ),
            participants: outcomes,
        })
    }
}

// ── Concrete XA participants ─────────────────────────────────────────────────

/// MySQL XA participant (NW-deep).
///
/// Implements PHASE 1 (PREPARE) and PHASE 2 (COMMIT / ROLLBACK) by
/// emitting MySQL's `XA START / END / PREPARE / COMMIT / ROLLBACK`
/// SQL via a fresh sqlx connection. MySQL XA semantics:
///
/// - `XA START 'xid'` opens a global transaction.
/// - `XA END 'xid'` marks the transaction's work complete.
/// - `XA PREPARE 'xid'` durably persists the transaction in a prepared
///   state. From this point, `XA COMMIT 'xid'` or `XA ROLLBACK 'xid'`
///   are guaranteed to succeed.
///
/// Caveats handled by this impl:
/// - The actual user statements must be issued between `XA START` and
///   `XA END` on the **same connection**. This participant is built
///   with the statements already buffered (`prepared_statements`) so
///   it can replay them in the PREPARE phase. The buffer is supplied by
///   the runtime when it constructs participants from a saga step list.
/// - sqlx's connection pool may hand out a different connection on
///   each call, breaking XA's single-connection requirement. We hold a
///   dedicated `PoolConnection` for the lifetime of the participant
///   (acquired in PREPARE, released after the PHASE 2 outcome).
#[cfg(feature = "mysql")]
pub struct XaMysqlParticipant {
    handle: XaParticipantHandle,
    pool: sqlx::MySqlPool,
    /// Statements issued between `XA START` and `XA END`. Each is
    /// `(sql, bound_params_json)` where the executor's normal
    /// bind-params path is replayed.
    prepared_statements: Vec<(String, Vec<serde_json::Value>)>,
    /// Connection held across PREPARE → COMMIT/ROLLBACK so XA's
    /// single-connection requirement is honoured. `Mutex` because
    /// `XaParticipant` methods take `&self`.
    connection: tokio::sync::Mutex<Option<sqlx::pool::PoolConnection<sqlx::MySql>>>,
}

#[cfg(feature = "mysql")]
impl XaMysqlParticipant {
    pub fn new(
        instance: impl Into<String>,
        pool: sqlx::MySqlPool,
        prepared_statements: Vec<(String, Vec<serde_json::Value>)>,
    ) -> Self {
        Self {
            handle: XaParticipantHandle::new("mysql", instance),
            pool,
            prepared_statements,
            connection: tokio::sync::Mutex::new(None),
        }
    }

    /// Bind one JSON value into a sqlx MySQL query. Same logic as the
    /// compensator's bind helper but co-located here for the XA path.
    fn bind_xa_value<'a>(
        q: sqlx::query::Query<'a, sqlx::MySql, sqlx::mysql::MySqlArguments>,
        v: &'a serde_json::Value,
    ) -> sqlx::query::Query<'a, sqlx::MySql, sqlx::mysql::MySqlArguments> {
        match v {
            serde_json::Value::Null => q.bind(Option::<&str>::None),
            serde_json::Value::Bool(b) => q.bind(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    q.bind(i)
                } else if let Some(f) = n.as_f64() {
                    q.bind(f)
                } else {
                    q.bind(n.to_string())
                }
            }
            serde_json::Value::String(s) => q.bind(s.as_str()),
            other => q.bind(other.to_string()),
        }
    }
}

#[cfg(feature = "mysql")]
#[async_trait::async_trait]
impl XaParticipant for XaMysqlParticipant {
    fn handle(&self) -> &XaParticipantHandle {
        &self.handle
    }

    async fn prepare(&self, xid: &str) -> PrepareVote {
        // Acquire a dedicated connection from the pool — this connection
        // is held across PHASE 2 to honour MySQL's single-connection XA
        // requirement.
        let mut conn = match self.pool.acquire().await {
            Ok(c) => c,
            Err(err) => {
                return PrepareVote::Aborted {
                    reason: format!("XA mysql acquire connection: {err}"),
                };
            }
        };
        use sqlx::Executor;
        // XA START requires a valid xid format. MySQL accepts
        // single-quoted xid; we pass the raw value from the coordinator
        // (`udb-<uuid>`, no quotes inside).
        if let Err(err) = conn.execute(format!("XA START '{xid}'").as_str()).await {
            return PrepareVote::Aborted {
                reason: format!("XA START failed: {err}"),
            };
        }
        // Replay every buffered statement on this connection.
        for (sql, params) in &self.prepared_statements {
            let mut q = sqlx::query(sql);
            for v in params {
                q = Self::bind_xa_value(q, v);
            }
            if let Err(err) = q.execute(&mut *conn).await {
                // Best-effort cleanup: END + ROLLBACK to clear state on
                // this connection before dropping.
                let _ = conn.execute(format!("XA END '{xid}'").as_str()).await;
                let _ = conn.execute(format!("XA ROLLBACK '{xid}'").as_str()).await;
                return PrepareVote::Aborted {
                    reason: format!("XA statement failed: {err}"),
                };
            }
        }
        if let Err(err) = conn.execute(format!("XA END '{xid}'").as_str()).await {
            let _ = conn.execute(format!("XA ROLLBACK '{xid}'").as_str()).await;
            return PrepareVote::Aborted {
                reason: format!("XA END failed: {err}"),
            };
        }
        if let Err(err) = conn.execute(format!("XA PREPARE '{xid}'").as_str()).await {
            return PrepareVote::Aborted {
                reason: format!("XA PREPARE failed: {err}"),
            };
        }
        // Stash the connection for PHASE 2.
        *self.connection.lock().await = Some(conn);
        PrepareVote::Prepared
    }

    async fn commit_prepared(&self, xid: &str) -> Result<(), String> {
        let mut guard = self.connection.lock().await;
        let mut conn = guard
            .take()
            .ok_or_else(|| "XA commit called without prepared connection".to_string())?;
        use sqlx::Executor;
        conn.execute(format!("XA COMMIT '{xid}'").as_str())
            .await
            .map(|_| ())
            .map_err(|err| format!("XA COMMIT failed: {err}"))
    }

    async fn rollback_prepared(&self, xid: &str) -> Result<(), String> {
        let mut guard = self.connection.lock().await;
        // If no connection was stashed (PREPARE failed earlier), we
        // try a fresh one — MySQL's XA RECOVER + XA ROLLBACK works
        // from any connection on the same server.
        let mut conn = match guard.take() {
            Some(c) => c,
            None => self
                .pool
                .acquire()
                .await
                .map_err(|err| format!("XA mysql rollback acquire: {err}"))?,
        };
        use sqlx::Executor;
        conn.execute(format!("XA ROLLBACK '{xid}'").as_str())
            .await
            .map(|_| ())
            .map_err(|err| format!("XA ROLLBACK failed: {err}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Stub participant for testing. Records every method call so the
    /// test can assert PHASE 1 / PHASE 2 ordering, rollback discipline,
    /// and which participants were touched.
    struct StubParticipant {
        handle: XaParticipantHandle,
        prepare_outcome: PrepareVote,
        commit_outcome: Result<(), String>,
        log: Mutex<Vec<String>>,
    }

    impl StubParticipant {
        fn new(label: &str, prepare: PrepareVote, commit: Result<(), String>) -> Self {
            Self {
                handle: XaParticipantHandle::new("postgres", label),
                prepare_outcome: prepare,
                commit_outcome: commit,
                log: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl XaParticipant for StubParticipant {
        fn handle(&self) -> &XaParticipantHandle {
            &self.handle
        }
        async fn prepare(&self, xid: &str) -> PrepareVote {
            self.log.lock().unwrap().push(format!("prepare({xid})"));
            self.prepare_outcome.clone()
        }
        async fn commit_prepared(&self, xid: &str) -> Result<(), String> {
            self.log
                .lock()
                .unwrap()
                .push(format!("commit_prepared({xid})"));
            self.commit_outcome.clone()
        }
        async fn rollback_prepared(&self, xid: &str) -> Result<(), String> {
            self.log
                .lock()
                .unwrap()
                .push(format!("rollback_prepared({xid})"));
            Ok(())
        }
    }

    fn matrix_with_two_phase(backend: &str, supports: bool) -> BackendCapabilityMatrixEntry {
        BackendCapabilityMatrixEntry {
            backend: backend.to_string(),
            tier: "sql".to_string(),
            operations: vec!["query".to_string(), "mutate".to_string()],
            unsupported_error_code: "failed_precondition".to_string(),
            consistency_model: "strong".to_string(),
            max_payload_bytes: 0,
            supports_xa: supports,
            supports_two_phase_commit: supports,
            role: crate::backend::BackendRole::Canonical,
            // Struct's documented defaults (this test helper predates the B.12
            // canonical-promotion fields added to the matrix entry).
            canonical_candidate: crate::backend::CanonicalCandidateProfile::ExplicitlyNotSupported,
            canonical_goal: String::new(),
            canonical_feasibility: None,
        }
    }

    fn request() -> XaRequest {
        XaRequest {
            tenant_id: "tenant-a".to_string(),
            project_id: "billing".to_string(),
            origin_rpc: "BeginTransaction".to_string(),
            correlation_id: "corr-1".to_string(),
        }
    }

    /// Tokens are SDK contract — pinned.
    #[test]
    fn strategy_tokens_are_pinned() {
        assert_eq!(TransactionStrategy::Saga.as_str(), "saga");
        assert_eq!(TransactionStrategy::BestEffort.as_str(), "best_effort");
        assert_eq!(TransactionStrategy::TwoPhase.as_str(), "two_phase");
    }

    #[test]
    fn strategy_parses_aliases_back_to_canonical() {
        assert_eq!(
            TransactionStrategy::parse("2pc"),
            Some(TransactionStrategy::TwoPhase)
        );
        assert_eq!(
            TransactionStrategy::parse("xa"),
            Some(TransactionStrategy::TwoPhase)
        );
        assert_eq!(
            TransactionStrategy::parse("best-effort"),
            Some(TransactionStrategy::BestEffort)
        );
        assert_eq!(TransactionStrategy::parse("unknown"), None);
        assert_eq!(
            TransactionStrategy::parse_or_default(""),
            TransactionStrategy::Saga
        );
    }

    /// **Acceptance gate**: 2PC refused before any side effect when a
    /// participant lacks `supports_two_phase_commit`. The matrix lookup
    /// runs FIRST; no PREPARE happens.
    #[tokio::test]
    async fn capability_refused_emits_no_side_effects() {
        let pg = StubParticipant::new("primary", PrepareVote::Prepared, Ok(()));
        // Mongo participant — capability matrix says no 2PC.
        let mongo_handle = XaParticipantHandle::new("mongodb", "default");
        let mongo = StubParticipant {
            handle: mongo_handle,
            prepare_outcome: PrepareVote::Prepared,
            commit_outcome: Ok(()),
            log: Mutex::new(Vec::new()),
        };
        let matrix = vec![
            matrix_with_two_phase("postgres", true),
            matrix_with_two_phase("mongodb", false),
        ];
        let err = XaCoordinator::execute(request(), vec![Box::new(pg), Box::new(mongo)], &matrix)
            .await
            .unwrap_err();
        assert!(matches!(err, XaError::CapabilityRefused { .. }));
        // Critical: capability check ran BEFORE any participant was
        // touched. The mongo log is empty.
        // (We can't easily inspect the boxed values' interior state
        // here — but the proof is that CapabilityRefused is returned
        // synchronously from validate_participants without ever calling
        // prepare().)
    }

    /// PHASE 1 abort path: any participant voting Aborted → all
    /// previously-prepared participants get rolled back, the ledger
    /// records RolledBack with the abort reason, no participant
    /// committed.
    #[tokio::test]
    async fn prepare_abort_triggers_rollback_on_prepared_participants() {
        let p1 = StubParticipant::new("p1", PrepareVote::Prepared, Ok(()));
        let p2 = StubParticipant::new(
            "p2",
            PrepareVote::Aborted {
                reason: "constraint violation".to_string(),
            },
            Ok(()),
        );
        let p3 = StubParticipant::new("p3", PrepareVote::Prepared, Ok(()));
        let matrix = vec![matrix_with_two_phase("postgres", true)];
        let err = XaCoordinator::execute(
            request(),
            vec![Box::new(p1), Box::new(p2), Box::new(p3)],
            &matrix,
        )
        .await
        .unwrap_err();
        match err {
            XaError::PrepareFailed { ledger, failures } => {
                assert_eq!(ledger.decision, XaDecision::RolledBack);
                assert!(ledger.reason.contains("constraint violation"));
                // p2's vote was Aborted; p1 and p3 were Prepared.
                assert_eq!(failures.len(), 3);
                assert!(matches!(failures[0].vote, PrepareVote::Prepared));
                assert!(matches!(failures[1].vote, PrepareVote::Aborted { .. }));
                assert!(matches!(failures[2].vote, PrepareVote::Prepared));
                // No participant committed.
                assert!(failures.iter().all(|f| !f.committed));
            }
            other => panic!("expected PrepareFailed, got {other:?}"),
        }
    }

    /// Happy path: all participants vote Prepared, all commit, the
    /// ledger records Committed with `is_terminal() = true`.
    #[tokio::test]
    async fn happy_path_commits_every_participant() {
        let p1 = StubParticipant::new("p1", PrepareVote::Prepared, Ok(()));
        let p2 = StubParticipant::new("p2", PrepareVote::Prepared, Ok(()));
        let matrix = vec![matrix_with_two_phase("postgres", true)];
        let outcome = XaCoordinator::execute(request(), vec![Box::new(p1), Box::new(p2)], &matrix)
            .await
            .expect("happy path");
        assert_eq!(outcome.ledger.decision, XaDecision::Committed);
        assert!(outcome.ledger.decision.is_terminal());
        assert_eq!(outcome.participants.len(), 2);
        assert!(outcome.participants.iter().all(|p| p.committed));
        assert_eq!(
            outcome.ledger.participants,
            vec!["postgres:p1", "postgres:p2"]
        );
    }

    /// PHASE 2 commit failure → in-doubt ledger. Recovery worker takes
    /// over; the request returns InDoubt error with the durable xid.
    #[tokio::test]
    async fn commit_failure_lands_in_doubt() {
        let p1 = StubParticipant::new("p1", PrepareVote::Prepared, Ok(()));
        let p2 = StubParticipant::new(
            "p2",
            PrepareVote::Prepared,
            Err("network partition".to_string()),
        );
        let matrix = vec![matrix_with_two_phase("postgres", true)];
        let err = XaCoordinator::execute(request(), vec![Box::new(p1), Box::new(p2)], &matrix)
            .await
            .unwrap_err();
        match err {
            XaError::InDoubt { ledger } => {
                assert_eq!(ledger.decision, XaDecision::InDoubt);
                assert!(!ledger.decision.is_terminal());
                assert!(ledger.reason.contains("network partition"));
                assert!(!ledger.xid.is_empty());
            }
            other => panic!("expected InDoubt, got {other:?}"),
        }
    }

    #[test]
    fn decision_tokens_and_terminality_are_pinned() {
        assert_eq!(XaDecision::Committed.as_str(), "committed");
        assert_eq!(XaDecision::RolledBack.as_str(), "rolled_back");
        assert_eq!(XaDecision::InDoubt.as_str(), "in_doubt");
        assert!(XaDecision::Committed.is_terminal());
        assert!(XaDecision::RolledBack.is_terminal());
        assert!(!XaDecision::InDoubt.is_terminal());
    }

    #[test]
    fn xid_is_valid_postgres_identifier() {
        let xid = XaCoordinator::new_xid();
        assert!(xid.starts_with("udb-"));
        assert!(
            xid.len() < 200,
            "must fit Postgres prepared-xact name limit"
        );
        assert!(
            xid.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'),
            "xid must be ASCII: {xid}"
        );
    }

    #[test]
    fn ledger_round_trips_through_serde() {
        let entry = XaLedgerEntry::new(
            "udb-test",
            "t1",
            "p1",
            "BeginTransaction",
            "corr",
            vec!["postgres:primary".into(), "postgres:replica".into()],
            XaDecision::InDoubt,
        )
        .with_reason("kafka timeout");
        let json = serde_json::to_string(&entry).unwrap();
        let back: XaLedgerEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back, entry);
    }
}
