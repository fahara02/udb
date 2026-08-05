//! Live 2PC recovery idempotency (XA2) for `crate::runtime::xa_recovery`.
//!
//! Customer symptom: after a prepared xid is committed / rolled back (by another
//! node, a prior recovery pass, or the coordinator), a subsequent recovery pass
//! that COMMITs / ROLLBACKs that now-terminal xid used to return an error. The
//! in-doubt worker then treated the (already-settled) row as a failed attempt,
//! churned retries, and after `max_attempts` false-escalated a perfectly settled
//! transaction to `manual_review`.
//!
//! The fix (`is_already_terminal_xid_error`) recognises the participant's
//! "unknown / undefined xid" error — Postgres SQLSTATE `42704` ("prepared
//! transaction … does not exist"), MySQL `XAE04` / `XAER_NOTA` ("Unknown XID") —
//! and treats a COMMIT/ROLLBACK of an already-terminal xid as success, so
//! recovery is idempotent.
//!
//! These tests drive the SERVED recovery participants (`PostgresInDoubtParticipant`
//! / `MysqlInDoubtParticipant`) against the LIVE database and assert that a
//! COMMIT/ROLLBACK of an unknown or already-settled prepared xid returns `Ok(())`
//! — the error is a REAL database error from the real engine, not a mock. This is
//! exactly the participant-level unit the fix governs.
//!
//! Revert-proof: revert `is_already_terminal_xid_error` and
//! `PostgresInDoubtParticipant::commit_prepared` / `rollback_prepared` (and the
//! MySQL pair) propagate the raw engine error as `Err(...)`, so every
//! `.expect("… idempotent Ok")` below fails.
//!
//! Run: UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_xa_recovery_idempotent -- --ignored --nocapture

use super::support::{live_auth_db_lock, live_pg_pool};
use crate::runtime::xa::XaCoordinator;
use crate::runtime::xa_recovery::{PostgresInDoubtParticipant, XaInDoubtParticipant};
use sqlx::Executor;

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_xa_recovery_idempotent_postgres -- --ignored --nocapture"]
async fn live_xa_recovery_idempotent_postgres() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    let participant = PostgresInDoubtParticipant {
        label: "postgres:primary".to_string(),
        pool: pool.clone(),
    };

    // ── Leg A (always): recovery of an xid that was NEVER prepared here — the
    // canonical "already driven terminal elsewhere" state. The real engine returns
    // SQLSTATE 42704 ("prepared transaction … does not exist"), which the fix maps
    // to idempotent success. Revert ⇒ these are Err.
    let ghost_commit = XaCoordinator::new_xid();
    participant
        .commit_prepared(&ghost_commit)
        .await
        .expect("COMMIT PREPARED of an unknown xid must be treated as idempotent success (XA2)");
    let ghost_rollback = XaCoordinator::new_xid();
    participant
        .rollback_prepared(&ghost_rollback)
        .await
        .expect("ROLLBACK PREPARED of an unknown xid must be treated as idempotent success (XA2)");

    // ── Leg B (gated on server 2PC support): the true re-run scenario. Prepare a
    // REAL 2PC transaction, drive it terminal via the recovery participant, then
    // re-run recovery on the SAME (now-terminal) xid — it must still succeed and
    // must have vanished from the prepared list. Needs `max_prepared_transactions
    // > 0` (canonical dev PG may leave it at 0); if PREPARE is unsupported, Leg A
    // already carries the revert-proof assertion and this leg is skipped.
    pool.execute("CREATE SCHEMA IF NOT EXISTS udb_xa_probe")
        .await
        .expect("create XA probe schema");
    pool.execute("CREATE TABLE IF NOT EXISTS udb_xa_probe.scratch (id TEXT PRIMARY KEY)")
        .await
        .expect("create XA probe table");

    let gid = XaCoordinator::new_xid();
    let mut conn = pool.acquire().await.expect("acquire prepare connection");
    (&mut *conn)
        .execute("BEGIN")
        .await
        .expect("begin 2PC probe txn");
    (&mut *conn)
        .execute(format!("INSERT INTO udb_xa_probe.scratch (id) VALUES ('{gid}')").as_str())
        .await
        .expect("write inside the 2PC probe txn");
    let prepared = (&mut *conn)
        .execute(format!("PREPARE TRANSACTION '{gid}'").as_str())
        .await;

    match prepared {
        Ok(_) => {
            drop(conn); // the prepared xact outlives its originating session
            assert!(
                participant
                    .list_prepared_xids()
                    .await
                    .expect("list prepared xids")
                    .contains(&gid),
                "the freshly prepared xid must be listed as in-doubt"
            );
            // First recovery pass: drive it terminal (real COMMIT PREPARED).
            participant
                .commit_prepared(&gid)
                .await
                .expect("first recovery COMMIT of the prepared xid");
            // Re-run recovery on the now-terminal xid: MUST be idempotent Ok, both
            // for a repeated COMMIT and for a ROLLBACK (a peer that decided abort).
            participant
                .commit_prepared(&gid)
                .await
                .expect("re-COMMIT of a settled xid must be idempotent Ok (XA2)");
            participant
                .rollback_prepared(&gid)
                .await
                .expect("ROLLBACK of a settled xid must be idempotent Ok (XA2)");
            assert!(
                !participant
                    .list_prepared_xids()
                    .await
                    .expect("re-list prepared xids")
                    .contains(&gid),
                "the settled xid must no longer appear in the prepared list"
            );
        }
        Err(err) => {
            // 2PC disabled on this server — undo the open txn and skip Leg B.
            let _ = (&mut *conn).execute("ROLLBACK").await;
            drop(conn);
            eprintln!(
                "skipping Leg B (real PREPARE→COMMIT→re-COMMIT): server rejected PREPARE \
                 TRANSACTION (likely max_prepared_transactions=0): {err}"
            );
        }
    }

    pool.execute("DROP SCHEMA IF EXISTS udb_xa_probe CASCADE")
        .await
        .expect("drop XA probe schema");
}

/// MySQL variant — covers the `XAE04` / `XAER_NOTA` branch of
/// `is_already_terminal_xid_error`. Gated on the `mysql` feature and a reachable
/// MySQL; skips cleanly otherwise.
#[cfg(feature = "mysql")]
#[tokio::test]
#[ignore = "requires live MySQL; run with UDB_MYSQL_DSN=... cargo test --lib live_xa_recovery_idempotent_mysql -- --ignored --nocapture"]
async fn live_xa_recovery_idempotent_mysql() {
    use crate::runtime::xa_recovery::MysqlInDoubtParticipant;

    let dsn = std::env::var("UDB_MYSQL_DSN")
        .unwrap_or_else(|_| "mysql://udb:udb@127.0.0.1:53306/udb".to_string());
    let pool = match sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(&dsn)
        .await
    {
        Ok(pool) => pool,
        Err(err) => {
            eprintln!("skipping MySQL XA idempotency leg: no reachable MySQL at {dsn}: {err}");
            return;
        }
    };
    let participant = MysqlInDoubtParticipant {
        label: "mysql:primary".to_string(),
        pool,
    };

    // XA COMMIT / XA ROLLBACK of an xid that was never prepared → MySQL XAER_NOTA
    // (Unknown XID) → idempotent success under the fix. Revert ⇒ Err.
    let ghost_commit = XaCoordinator::new_xid();
    participant
        .commit_prepared(&ghost_commit)
        .await
        .expect("XA COMMIT of an unknown xid must be idempotent Ok (XAER_NOTA → success)");
    let ghost_rollback = XaCoordinator::new_xid();
    participant
        .rollback_prepared(&ghost_rollback)
        .await
        .expect("XA ROLLBACK of an unknown xid must be idempotent Ok (XAER_NOTA → success)");
}
