//! In-doubt recovery for KafkaTransactional CDC publishes (U21 step 2).
//!
//! A crash mid-publish leaves the outbox state machine in this shape:
//!
//! - `delivery_state = 'publishing'` (the row started publishing in the
//!   prior epoch — `mark_cdc_delivery_state(event_id, "publishing", …)`
//!   wrote the row before the Kafka send was acknowledged), and
//! - the Kafka transaction the publish was inside is **in-doubt** at the
//!   broker (the producer never reached `commit_transaction`).
//!
//! When the engine restarts and calls `init_transactions()` on the new
//! `FutureProducer`, Kafka uses the stable `transactional.id` to find the
//! previous epoch's in-flight transactions and **aborts them**. From the
//! broker's perspective the prior publishes are gone — `read_committed`
//! consumers never saw them.
//!
//! But the outbox row is still in `publishing` state. The normal publish
//! loop only picks up `pending` rows, so without an explicit reset those
//! rows would be stuck forever. [`reset_indoubt_publishing_rows`] is the
//! sweep: scan for `publishing` rows whose `producer_epoch` is less than
//! the current `producer_epoch`, OR whose `publishing_started_at` is
//! older than the configured grace window, and reset them to `pending`
//! with an explicit `last_error` annotation so the operator sees the
//! recovery happened.
//!
//! ## When to call this
//!
//! From the CDC engine's startup path, **after** the transactional
//! producer is built (so `init_transactions` has already run and the
//! broker-side abort is complete). Calling it before `init_transactions`
//! would race the broker — we might reset a row whose Kafka transaction
//! the broker is about to commit.
//!
//! ## What the reset writes
//!
//! ```sql
//! UPDATE <outbox> SET
//!   delivery_state = 'pending',
//!   last_error = 'udb-cdc-indoubt-recovery: previous epoch was fenced; re-publishing',
//!   publishing_started_at = NULL
//! WHERE delivery_state = 'publishing'
//!   AND (producer_epoch < $1 OR publishing_started_at < NOW() - $2::interval)
//! ```
//!
//! Returns the number of rows reset so the caller can metric it.

use sqlx::PgPool;

/// Reset every `publishing` outbox row from a prior producer epoch
/// (or older than the grace window) to `pending` so the normal publish
/// loop re-runs it inside a fresh Kafka transaction.
///
/// Returns the number of rows reset, or an error string from the
/// database driver. Empty result on a clean restart is the success
/// case.
///
/// `current_epoch` is the running engine's `CdcConfig::producer_epoch`.
/// `grace_secs` is the soft fallback when the operator hasn't bumped
/// `producer_epoch`: any row stuck in `publishing` for longer than this
/// is treated as in-doubt. Default 300s (5 min) is a sensible value —
/// well past Kafka's default 60s transaction timeout, so we don't
/// race a publish that's still legitimately in flight.
pub async fn reset_indoubt_publishing_rows(
    pool: &PgPool,
    outbox_relation: &str,
    current_epoch: i64,
    grace_secs: i64,
) -> Result<u64, String> {
    // Defensive: reject identifiers we'd refuse to qi-quote. The
    // outbox_relation comes from `CdcConfig::outbox_relation` which is
    // already env-identifier-validated; this is belt-and-braces against
    // a future caller that passes a raw string.
    if outbox_relation.is_empty()
        || outbox_relation
            .chars()
            .any(|c| !(c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '"'))
    {
        return Err(format!(
            "reset_indoubt_publishing_rows: refusing unsafe outbox_relation '{outbox_relation}'"
        ));
    }
    if grace_secs < 0 {
        return Err(format!(
            "reset_indoubt_publishing_rows: grace_secs must be >= 0 (got {grace_secs})"
        ));
    }

    // Step 1: a 'publishing' row that already carries a `kafka_offset` had its
    // Kafka transaction COMMIT acknowledged (the offset is only recorded after
    // a successful commit) — the crash happened before the ack/delete. These
    // are NOT in-doubt: re-publishing them would duplicate. Finalize them as
    // 'acked' instead of resetting to 'pending'.
    let ack_sql = format!(
        "UPDATE {outbox_relation} SET \
            delivery_state = 'acked', \
            acked_at = NOW(), \
            last_error = COALESCE(last_error, '') || \
                ' | udb-cdc-indoubt-recovery: commit confirmed by recorded offset; acking' \
         WHERE delivery_state = 'publishing' \
           AND kafka_offset IS NOT NULL"
    );
    let acked = sqlx::query(&ack_sql)
        .execute(pool)
        .await
        .map(|res| res.rows_affected())
        .map_err(|e| format!("reset_indoubt_publishing_rows (ack) failed: {e}"))?;

    // Step 2: a 'publishing' row WITHOUT an offset never had a confirmed commit
    // (the prior epoch's transaction was fenced/aborted by init_transactions).
    // Reset these to 'pending' so the normal loop re-publishes in a fresh tx.
    let reset_sql = format!(
        "UPDATE {outbox_relation} SET \
            delivery_state = 'pending', \
            last_error = COALESCE(last_error, '') || \
                ' | udb-cdc-indoubt-recovery: previous epoch fenced; re-publishing', \
            publishing_started_at = NULL \
         WHERE delivery_state = 'publishing' \
           AND kafka_offset IS NULL \
           AND ( \
               producer_epoch < $1 \
               OR publishing_started_at < NOW() - make_interval(secs => $2::double precision) \
           )"
    );
    let reset = sqlx::query(&reset_sql)
        .bind(current_epoch)
        .bind(grace_secs as f64)
        .execute(pool)
        .await
        .map(|res| res.rows_affected())
        .map_err(|e| format!("reset_indoubt_publishing_rows failed: {e}"))?;

    Ok(acked + reset)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin the safety guard — a relation name with a `;` or `--`
    /// must be rejected before reaching sqlx. The check is structural,
    /// not parametric, so a regression would silently re-enable a SQL
    /// injection vector via the env-driven outbox table name.
    #[tokio::test]
    async fn rejects_unsafe_relation_name() {
        // We can't bring up a real pool here; instead test the
        // synchronous validation by inspecting the returned error
        // without needing a connection. The function short-circuits on
        // the relation check before touching sqlx, so the error is
        // the validation error regardless of pool state.
        //
        // Build a fake pool by lazy-connecting to an invalid DSN — the
        // pool is never touched because the validation short-circuits.
        let pool = PgPool::connect_lazy("postgres://invalid:0/none").unwrap();
        let err = reset_indoubt_publishing_rows(&pool, "evil; DROP", 0, 300)
            .await
            .expect_err("should reject unsafe relation");
        assert!(
            err.contains("refusing unsafe outbox_relation"),
            "got: {err}"
        );
        let err2 = reset_indoubt_publishing_rows(&pool, "", 0, 300)
            .await
            .expect_err("should reject empty relation");
        assert!(err2.contains("refusing unsafe"));
    }

    /// Pin: negative grace is rejected. Otherwise the interval would
    /// be `now() - negative` which equals `now() + |grace|` and would
    /// sweep rows from the *future* — i.e. every currently-publishing
    /// row. Defense against an operator typo in
    /// `UDB_CDC_INDOUBT_GRACE_SECS`.
    #[tokio::test]
    async fn rejects_negative_grace() {
        let pool = PgPool::connect_lazy("postgres://invalid:0/none").unwrap();
        let err = reset_indoubt_publishing_rows(&pool, "udb_system.outbox_events", 0, -1)
            .await
            .expect_err("negative grace must be rejected");
        assert!(err.contains("grace_secs must be >= 0"), "got: {err}");
    }

    /// Pin: schema-qualified names with dots are accepted. The
    /// `outbox_relation()` helper emits `udb_system.outbox_events`
    /// (or quoted variants); the validation must not reject these.
    #[tokio::test]
    async fn accepts_schema_qualified_relation() {
        let pool = PgPool::connect_lazy("postgres://invalid:0/none").unwrap();
        let res = reset_indoubt_publishing_rows(&pool, "udb_system.outbox_events", 0, 300).await;
        // The pool is invalid so we'll get a connection error, not a
        // validation error. The point is to confirm the validation
        // accepts the relation.
        match res {
            Ok(_) => {} // No rows reset, fine.
            Err(e) => assert!(
                !e.contains("refusing unsafe"),
                "schema-qualified should not be rejected: {e}"
            ),
        }
    }
}
