//! `SqliteCanonicalStore` — new in P2P. sqlx-sqlite backed.
//!
//! ## Why SQLite belongs as a peer
//!
//! SQLite is one of the most-deployed databases on earth (every
//! browser, every Android app, every iOS app, every desktop app
//! ships it). For UDB's "any project, any DB" promise to be honest,
//! SQLite has to be a first-class canonical store, not a curiosity.
//!
//! It also doubles as the substrate for the long-pending U27
//! follow-up: an in-memory test profile. A `SqliteCanonicalStore`
//! built against `sqlite::memory:` gives every test a real canonical
//! store with no Docker, no `pg_isready`, no `TEST_POSTGRES_DSN`.
//!
//! ## Durability token strategy
//!
//! SQLite has no LSN, no GTID. The closest single-writer monotone
//! signal is `PRAGMA data_version`, which the SQLite docs describe as
//! *"a number that increases every time the database file is
//! modified by another connection or by a checkpoint."* It's stable
//! within one connection's lifetime, which matches how the runtime
//! uses the canonical store (one pool per store).
//!
//! For the `wait_for_token` semantics, SQLite is **trivially
//! up-to-date with itself** — there's no replica lag. So
//! `wait_for_token` returns `Ok(true)` immediately whenever the
//! requested data_version is `<=` the current one, and polls otherwise
//! (covers the case where a writer commits between the read of
//! `current_durability_token` on side A and `wait_for_token` on
//! side B).

use std::time::{Duration, Instant};

use async_trait::async_trait;
use sqlx::SqlitePool;

use super::{CanonicalStore, DurabilityToken};

pub struct SqliteCanonicalStore {
    pub(super) pool: SqlitePool,
    instance_name: String,
    /// SQLite has no schemas — `outbox_relation` is just the table
    /// name (no qualifier). Validated case-by-case.
    outbox_table: String,
}

impl SqliteCanonicalStore {
    pub fn new(
        pool: SqlitePool,
        instance_name: impl Into<String>,
        outbox_table: impl Into<String>,
    ) -> Self {
        Self {
            pool,
            instance_name: instance_name.into(),
            outbox_table: outbox_table.into(),
        }
    }

    fn safe_table(&self) -> Result<&str, String> {
        let t = self.outbox_table.as_str();
        if t.is_empty() || !t.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(format!(
                "unsafe outbox_table '{t}' (SQLite identifiers must be [A-Za-z0-9_]+)"
            ));
        }
        Ok(t)
    }
}

#[async_trait]
impl CanonicalStore for SqliteCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "sqlite"
    }

    fn instance_name(&self) -> &str {
        &self.instance_name
    }

    async fn current_durability_token(&self) -> Result<DurabilityToken, String> {
        let v: (i64,) = sqlx::query_as("PRAGMA data_version")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| format!("PRAGMA data_version failed: {e}"))?;
        Ok(DurabilityToken::new("sqlite", v.0.to_string()))
    }

    async fn wait_for_token(
        &self,
        token: &DurabilityToken,
        timeout: Duration,
    ) -> Result<bool, String> {
        if !token.is_for("sqlite") {
            return Err(format!(
                "SqliteCanonicalStore cannot wait on a '{}' token",
                token.backend_label
            ));
        }
        let target: i64 = token
            .value
            .parse()
            .map_err(|e| format!("invalid sqlite data_version '{}': {e}", token.value))?;
        let started = Instant::now();
        let poll = crate::runtime::canonical_store::durability_poll_interval(
            timeout,
            crate::runtime::canonical_store::SQLITE_DURABILITY_POLL_MS,
        );
        loop {
            let (current,): (i64,) = sqlx::query_as("PRAGMA data_version")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| format!("PRAGMA data_version poll failed: {e}"))?;
            if current >= target {
                return Ok(true);
            }
            if started.elapsed() >= timeout {
                return Ok(false);
            }
            tokio::time::sleep(poll).await;
        }
    }

    async fn enqueue_outbox_event(
        &self,
        event_id: &str,
        topic: &str,
        partition_key: &str,
        payload: &serde_json::Value,
    ) -> Result<i64, String> {
        let table = self.safe_table()?;
        // SQLite uses ROWID as the auto-increment for `INTEGER
        // PRIMARY KEY` columns. `last_insert_rowid()` returns it.
        let sql = format!(
            "INSERT INTO {table} (event_id, topic, partition_key, payload, created_at) \
             VALUES (?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))"
        );
        // sqlx-sqlite serialises serde_json::Value to TEXT.
        let payload_text = serde_json::to_string(payload)
            .map_err(|e| format!("payload JSON serialise failed: {e}"))?;
        let result = sqlx::query(&sql)
            .bind(event_id)
            .bind(topic)
            .bind(partition_key)
            .bind(payload_text)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("outbox insert (sqlite) failed: {e}"))?;
        Ok(result.last_insert_rowid())
    }

    async fn outbox_max_seq(&self) -> Result<i64, String> {
        let table = self.safe_table()?;
        let sql = format!("SELECT COALESCE(MAX(event_seq), 0) FROM {table}");
        let (max,): (i64,) = sqlx::query_as(&sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| format!("outbox max seq (sqlite) failed: {e}"))?;
        Ok(max)
    }

    async fn ensure_system_tables(&self) -> Result<(), String> {
        let table = self.safe_table()?;
        // SQLite: `INTEGER PRIMARY KEY` is the auto-increment alias
        // for ROWID. `TEXT` for JSON since sqlite is typeless; the
        // app side parses with serde_json.
        //
        // B.7: outbox DDL comes from the shared `sql_schema` renderer (single
        // source of truth across SQL backends); execute/error-handling below
        // is unchanged.
        let sql = super::sql_schema::sqlite_outbox_ddl(table);
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("ensure_system_tables (sqlite) failed: {e}"))?;
        Ok(())
    }

    async fn ensure_advisory_lease_table(&self) -> Result<(), String> {
        let sql = "
            CREATE TABLE IF NOT EXISTS udb_advisory_leases (
                lease_name VARCHAR(255) PRIMARY KEY,
                owner_id   VARCHAR(255) NOT NULL,
                expires_at TEXT NOT NULL
            )
        ";
        sqlx::query(sql)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("ensure_advisory_lease_table (sqlite) failed: {e}"))?;
        Ok(())
    }

    async fn try_acquire_advisory_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        ttl: std::time::Duration,
    ) -> Result<bool, String> {
        // Atomic acquire in one statement:
        //   INSERT a new row, OR re-claim an expired row via ON
        //   CONFLICT DO UPDATE WHERE expires_at < NOW().
        // SQLite supports `ON CONFLICT(col) DO UPDATE SET … WHERE …`
        // since 3.24 — sqlx-sqlite ships a newer build.
        // The expires_at value is computed Rust-side so the comparison
        // uses a stable cutoff.
        let now = chrono::Utc::now();
        let now_iso = now.to_rfc3339();
        let expires = now + chrono::Duration::seconds(ttl.as_secs() as i64);
        let expires_iso = expires.to_rfc3339();
        // Confirm ownership in the SAME statement via RETURNING (sqlx-sqlite
        // supports it) — a separate follow-up SELECT was a TOCTOU window where a
        // concurrent caller could claim the lease between the upsert and the
        // confirmation, yielding a false-positive. RETURNING owner_id only emits
        // a row when our INSERT fired or the ON CONFLICT UPDATE matched (expired
        // takeover or our own refresh); a live row owned by another caller skips
        // the UPDATE and returns no row → Ok(false).
        let sql = "
            INSERT INTO udb_advisory_leases (lease_name, owner_id, expires_at)
            VALUES (?, ?, ?)
            ON CONFLICT(lease_name) DO UPDATE
              SET owner_id   = excluded.owner_id,
                  expires_at = excluded.expires_at
              WHERE udb_advisory_leases.expires_at < ?
                 OR udb_advisory_leases.owner_id = excluded.owner_id
            RETURNING owner_id
        ";
        let resulting_owner: Option<String> = sqlx::query_scalar(sql)
            .bind(lease_name)
            .bind(owner_id)
            .bind(&expires_iso)
            .bind(&now_iso)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| format!("try_acquire_advisory_lease (sqlite) failed: {e}"))?;
        Ok(matches!(resulting_owner, Some(o) if o == owner_id))
    }

    async fn release_advisory_lease(&self, lease_name: &str, owner_id: &str) -> Result<(), String> {
        let sql = "DELETE FROM udb_advisory_leases WHERE lease_name = ? AND owner_id = ?";
        sqlx::query(sql)
            .bind(lease_name)
            .bind(owner_id)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("release_advisory_lease (sqlite) failed: {e}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn in_memory_pool() -> SqlitePool {
        SqlitePoolOptions::new()
            .max_connections(1) // :memory: is per-connection in SQLite
            .connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite connect")
    }

    /// Pin: backend label is `"sqlite"` exactly.
    #[tokio::test]
    async fn backend_label_is_pinned() {
        let pool = in_memory_pool().await;
        let store = SqliteCanonicalStore::new(pool, "test", "udb_outbox_events");
        assert_eq!(store.backend_label(), "sqlite");
        assert_eq!(store.instance_name(), "test");
    }

    /// Pin: end-to-end smoke against in-memory SQLite. Proves the
    /// canonical store contract works without any external database
    /// — closes the U27 in-memory test profile follow-up.
    #[tokio::test]
    async fn in_memory_round_trip_proves_p2p_works_without_postgres() {
        let pool = in_memory_pool().await;
        let store = SqliteCanonicalStore::new(pool, "test", "udb_outbox_events");
        store.ensure_system_tables().await.expect("DDL");

        // Initial outbox is empty.
        assert_eq!(store.outbox_max_seq().await.expect("max seq"), 0);

        // Capture a token before any writes.
        let token_a = store.current_durability_token().await.expect("token a");
        assert_eq!(token_a.backend_label, "sqlite");

        // Enqueue one event.
        let seq = store
            .enqueue_outbox_event(
                "11111111-1111-1111-1111-111111111111",
                "test.topic",
                "pk-1",
                &serde_json::json!({"hello": "world"}),
            )
            .await
            .expect("enqueue");
        assert_eq!(seq, 1, "first auto-increment");
        assert_eq!(store.outbox_max_seq().await.expect("max seq"), 1);

        // Token after the write should be >= the earlier token.
        let token_b = store.current_durability_token().await.expect("token b");
        let a: i64 = token_a.value.parse().unwrap();
        let b: i64 = token_b.value.parse().unwrap();
        assert!(b >= a, "data_version is monotone");

        // wait_for_token at the current value clears instantly.
        let cleared = store
            .wait_for_token(&token_b, Duration::from_millis(10))
            .await
            .expect("wait");
        assert!(cleared);
    }

    /// Pin: cross-backend token rejected.
    #[tokio::test]
    async fn rejects_non_sqlite_token() {
        let pool = in_memory_pool().await;
        let store = SqliteCanonicalStore::new(pool, "test", "udb_outbox_events");
        let mysql_token = DurabilityToken::new("mysql", "gtid:xyz");
        let err = store
            .wait_for_token(&mysql_token, Duration::from_millis(1))
            .await
            .expect_err("must reject cross-backend token");
        assert!(err.contains("cannot wait on a 'mysql'"), "got: {err}");
    }

    /// Pin: NW1-3c — advisory lease acquire / release / contention.
    /// Two callers race; only one wins. Releasing lets the next win.
    /// Expired leases can be re-acquired.
    #[tokio::test]
    async fn advisory_lease_acquire_release_contention() {
        let pool = in_memory_pool().await;
        let store = SqliteCanonicalStore::new(pool, "test", "udb_outbox_events");
        store.ensure_advisory_lease_table().await.unwrap();

        let lease = "saga-worker";

        // Alice acquires.
        let alice = store
            .try_acquire_advisory_lease(lease, "alice", Duration::from_secs(60))
            .await
            .unwrap();
        assert!(alice, "first acquire wins");

        // Bob tries — fails.
        let bob = store
            .try_acquire_advisory_lease(lease, "bob", Duration::from_secs(60))
            .await
            .unwrap();
        assert!(
            !bob,
            "second acquirer must lose while Alice holds the lease"
        );

        // Alice re-acquires (refreshes own lease) — succeeds.
        let alice_again = store
            .try_acquire_advisory_lease(lease, "alice", Duration::from_secs(60))
            .await
            .unwrap();
        assert!(alice_again, "owner can refresh own lease");

        // Alice releases.
        store.release_advisory_lease(lease, "alice").await.unwrap();

        // Bob now wins.
        let bob_now = store
            .try_acquire_advisory_lease(lease, "bob", Duration::from_secs(60))
            .await
            .unwrap();
        assert!(bob_now, "after release, next acquirer wins");

        // Release by wrong owner is a no-op (idempotent).
        store.release_advisory_lease(lease, "alice").await.unwrap();
        // Bob still owns.
        let bob_still = store
            .try_acquire_advisory_lease(lease, "bob", Duration::from_secs(60))
            .await
            .unwrap();
        assert!(
            bob_still,
            "Bob still holds after wrong-owner release attempt"
        );
    }

    /// Pin: expired leases let a new owner in. Uses ttl=0 + sleep to
    /// force expiration.
    #[tokio::test]
    async fn expired_lease_is_re_acquirable() {
        let pool = in_memory_pool().await;
        let store = SqliteCanonicalStore::new(pool, "test", "udb_outbox_events");
        store.ensure_advisory_lease_table().await.unwrap();

        let lease = "ephemeral";
        store
            .try_acquire_advisory_lease(lease, "first", Duration::from_secs(0))
            .await
            .unwrap();
        // Sleep past the 0-second TTL.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // New owner acquires the now-expired lease.
        let second = store
            .try_acquire_advisory_lease(lease, "second", Duration::from_secs(60))
            .await
            .unwrap();
        assert!(second, "expired lease must be re-acquirable");
    }

    /// Pin: unsafe table name (SQLite identifiers don't allow `.` or
    /// `"` like PG/MySQL) is rejected.
    #[tokio::test]
    async fn unsafe_table_name_is_rejected() {
        let pool = in_memory_pool().await;
        let store = SqliteCanonicalStore::new(pool, "test", "evil; DROP");
        assert!(store.safe_table().is_err());
        // Even a schema-qualified name is rejected — SQLite doesn't
        // support them.
        let store2 = SqliteCanonicalStore::new(
            SqlitePool::connect_lazy("sqlite::memory:").unwrap(),
            "test",
            "udb_system.events",
        );
        assert!(store2.safe_table().is_err());
    }
}
