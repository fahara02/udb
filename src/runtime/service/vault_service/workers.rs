//! The leader-elected dynamic DB-credential lease reaper for the native
//! `VaultService`: revokes expired generated Postgres login roles and marks their
//! durable lease rows REVOKED. Extracted verbatim from the former god file — the
//! manifest-derived select/update SQL and the drop-then-mark ordering are
//! byte-for-byte identical. Spawned by the leader via
//! `NativeWorkerHost::spawn_while_leader` (`run_vault_db_lease_reaper_once` is
//! re-exported from `mod.rs` for `serve()`).

use sqlx::{PgPool, Row};

use super::config::{VAULT_DB_CREDENTIAL_LEASE_MSG, VAULT_DB_LEASE_REAPER_BATCH};
use super::dynamic::drop_postgres_login_role;

pub async fn run_vault_db_lease_reaper_once(pool: &PgPool, batch: i64) -> Result<i64, String> {
    let model = crate::runtime::native_catalog::native_model(
        VAULT_DB_CREDENTIAL_LEASE_MSG,
        &["lease_id", "username", "state", "expires_at", "revoked_at"],
    );
    let limit = batch.clamp(1, VAULT_DB_LEASE_REAPER_BATCH);
    let select_sql = format!(
        "SELECT {}, {} FROM {} WHERE {} = 'ACTIVE' AND {} IS NULL AND {} <= NOW() \
         ORDER BY {} ASC LIMIT $1",
        model.text_as("lease_id", "lease_id"),
        model.text_as("username", "username"),
        model.relation,
        model.q("state"),
        model.q("revoked_at"),
        model.q("expires_at"),
        model.q("expires_at")
    );
    let rows = sqlx::query(&select_sql)
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(|err| format!("read expired vault DB credential leases failed: {err}"))?;

    let update_sql = format!(
        "UPDATE {} SET {} = 'REVOKED', {} = NOW() \
         WHERE {} = $1::uuid AND {} = 'ACTIVE'",
        model.relation,
        model.q("state"),
        model.q("revoked_at"),
        model.q("lease_id"),
        model.q("state")
    );
    let mut revoked = 0i64;
    for row in rows {
        let lease_id: String = row
            .try_get("lease_id")
            .map_err(|err| format!("expired lease row missing lease_id: {err}"))?;
        let username: String = row
            .try_get("username")
            .map_err(|err| format!("expired lease row missing username: {err}"))?;
        drop_postgres_login_role(pool, &username).await?;
        let updated = sqlx::query(&update_sql)
            .bind(&lease_id)
            .execute(pool)
            .await
            .map_err(|err| format!("mark vault DB credential lease revoked failed: {err}"))?;
        revoked += updated.rows_affected() as i64;
    }
    Ok(revoked)
}
