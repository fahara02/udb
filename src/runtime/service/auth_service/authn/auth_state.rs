//! A cluster-wide version of the auth state a bearer verdict depends on.
//!
//! Database triggers advance one sequence whenever that state changes: a user's
//! status or tenant, a token revocation, a session revoked or rescoped (not a
//! `last_active_at` stamp), a refresh family revoked, a service grant or tenant
//! membership written. A remembered verdict is served only while the version
//! is still the one it was computed under, so a change made on any broker
//! node, or straight in SQL, drops it everywhere.
//!
//! Calls do not read the sequence. The same triggers publish each new value on
//! a notification channel at commit; one listener per service instance keeps
//! the latest value in memory, and re-reads the sequence after every
//! (re)connect and every 30 s as a backstop. A change is seen within the
//! notification delivery time (milliseconds), and a call costs no database
//! round trip. While the listener is not connected, every call reads the
//! sequence itself (one small statement), so a lost connection never serves a
//! stale verdict. `UDB_AUTH_STATE_READ_PER_CALL=true` always reads per call,
//! for deployments that want no delivery window at the price of that statement
//! on every bearer call.
//!
//! The triggers are installed once per service instance, on first use, and
//! only where missing: an existing trigger is never dropped, so installing never
//! waits on a busy table for more than the lock timeout. When they cannot be
//! installed (no Postgres store, or a role without DDL rights) the version is
//! unknown and the verdict cache falls back to its time bound alone.
//!
//! The sequence advances when a writing transaction commits (deferred
//! triggers), so the window in which a verdict can be computed against the old
//! state under the new version is the commit itself; the cache lifetime bounds
//! even that.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::OnceCell;

use crate::runtime::native_catalog::{NativeModel, native_model};

/// Name of the version sequence, in the users table's schema.
const SEQUENCE: &str = "udb_auth_state_version";
/// Name of the trigger function that advances it.
const FUNCTION: &str = "udb_auth_state_bump";
/// Trigger names carry a version: a changed definition gets a new name and is
/// added beside the old one, never by dropping a live trigger.
const TRIGGER_PREFIX: &str = "udb_auth_state_v1";
/// A failed install is retried no sooner than this.
const RETRY_AFTER_SECS: u64 = 60;
/// Installing a missing trigger waits at most this long for its table lock.
const INSTALL_LOCK_TIMEOUT: &str = "2s";
/// Notification channel the triggers publish each new version on.
const CHANNEL: &str = "udb_auth_state";
/// With no notification for this long, the listener re-reads the sequence.
const BACKSTOP: Duration = Duration::from_secs(30);
/// Wait before reconnecting a lost listener.
const RECONNECT_AFTER: Duration = Duration::from_secs(1);

/// `UDB_AUTH_STATE_READ_PER_CALL=true`: read the sequence on every call.
fn read_per_call() -> bool {
    static PER_CALL: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *PER_CALL.get_or_init(|| {
        std::env::var("UDB_AUTH_STATE_READ_PER_CALL")
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}

/// Per-service-instance install state and the latest known version.
#[derive(Default)]
pub(crate) struct AuthStateVersion {
    /// The qualified sequence name, once the sequence and function exist.
    installed: OnceCell<String>,
    /// Unix seconds of the last failed install attempt (0 = none).
    failed_at: AtomicU64,
    /// Some watched table did not exist at the last install; it is looked for
    /// again no sooner than [`RETRY_AFTER_SECS`] later.
    partial: AtomicBool,
    /// Unix seconds of the last install pass.
    installed_at: AtomicU64,
    /// The newest version the listener has seen.
    known: AtomicI64,
    /// The listener is connected and caught up: `known` is current.
    live: AtomicBool,
    /// The listener task has been started.
    listening: AtomicBool,
}

impl std::fmt::Debug for AuthStateVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthStateVersion")
            .field("installed", &self.installed.get().is_some())
            .field("live", &self.live.load(Ordering::Relaxed))
            .finish()
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// One table whose changes move the version, and which updates count.
struct Watched {
    model: NativeModel,
    /// Fields whose change on UPDATE moves the version; empty = any update.
    update_fields: &'static [&'static str],
    /// Whether an INSERT moves the version (a new revocation does; a new
    /// session or family does not, no verdict for it was remembered).
    on_insert: bool,
}

fn watched() -> Vec<Watched> {
    vec![
        Watched {
            model: native_model("udb.core.authn.entity.v1.User", &["status", "tenant_id"]),
            update_fields: &["status", "tenant_id"],
            on_insert: false,
        },
        Watched {
            model: native_model(
                "udb.core.authn.entity.v1.Session",
                &[
                    "is_active",
                    "expires_at",
                    "revoke_reason",
                    "scopes_json",
                    "metadata_json",
                    "tenant_id",
                    "project_id",
                    "principal_id",
                    "user_id",
                ],
            ),
            update_fields: &[
                "is_active",
                "expires_at",
                "revoke_reason",
                "scopes_json",
                "metadata_json",
                "tenant_id",
                "project_id",
                "principal_id",
                "user_id",
            ],
            on_insert: false,
        },
        Watched {
            model: native_model("udb.core.authn.entity.v1.TokenRevocation", &["jti_hash"]),
            update_fields: &[],
            on_insert: true,
        },
        Watched {
            model: native_model(
                "udb.core.authn.entity.v1.TokenFamily",
                &["revoked_at", "reuse_detected_at", "revocation_reason"],
            ),
            update_fields: &["revoked_at", "reuse_detected_at", "revocation_reason"],
            on_insert: false,
        },
        Watched {
            model: native_model(
                crate::runtime::service::auth_service::grants::GRANT_MSG,
                &[],
            ),
            update_fields: &[],
            on_insert: true,
        },
        Watched {
            model: native_model(
                crate::runtime::service::auth_service::memberships::MEMBERSHIP_MSG,
                &[],
            ),
            update_fields: &[],
            on_insert: true,
        },
    ]
}

/// The schema part of a qualified relation (`"schema"."table"`).
fn schema_of(relation: &str) -> Option<&str> {
    let (schema, _) = relation.split_once("\".\"")?;
    Some(schema.trim_start_matches('"'))
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The bare table name of a qualified relation.
fn table_of(relation: &str) -> Option<&str> {
    let (_, table) = relation.split_once("\".\"")?;
    Some(table.trim_end_matches('"'))
}

/// Install what is missing. Returns the sequence name and whether every
/// watched table was present (a table created later is picked up next pass).
async fn install(pool: &PgPool) -> Result<(String, bool), String> {
    let tables = watched();
    let schema = schema_of(&tables[0].model.relation)
        .ok_or_else(|| format!("unqualified users relation {}", tables[0].model.relation))?
        .to_string();
    let qschema = quote_ident(&schema);
    let sequence = format!("{qschema}.{}", quote_ident(SEQUENCE));
    let function = format!("{qschema}.{}", quote_ident(FUNCTION));

    let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
    // One installer at a time across every broker node.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('udb_auth_state_version_install'))")
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
    sqlx::query(&format!(
        "SET LOCAL lock_timeout = '{INSTALL_LOCK_TIMEOUT}'"
    ))
    .execute(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;
    sqlx::query(&format!("CREATE SEQUENCE IF NOT EXISTS {sequence}"))
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
    // SECURITY DEFINER: a writer whose role cannot use the sequence (an
    // operator fixing a row by hand) must not have its write refused. The new
    // value is published on CHANNEL; the notification is delivered at commit.
    let sequence_literal = sequence.replace('\'', "''");
    sqlx::query(&format!(
        "CREATE OR REPLACE FUNCTION {function}() RETURNS trigger \
         LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $udb$ \
         BEGIN PERFORM pg_catalog.pg_notify('{CHANNEL}', \
         pg_catalog.nextval('{sequence_literal}')::TEXT); RETURN NULL; END $udb$"
    ))
    .execute(&mut *tx)
    .await
    .map_err(|e| e.to_string())?;
    // A new function is executable by PUBLIC. A SECURITY DEFINER function any
    // role can call is a privilege-escalation path (vault refuses to issue
    // bounded database credentials on such a database), and nothing needs to
    // call this one directly: a trigger checks EXECUTE when it is created, not
    // when it fires, so every writer of a watched table still moves the version.
    sqlx::query(&format!("REVOKE ALL ON FUNCTION {function}() FROM PUBLIC"))
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;

    let mut complete = true;
    for table in &tables {
        let relation = &table.model.relation;
        let exists: bool = sqlx::query_scalar("SELECT pg_catalog.to_regclass($1) IS NOT NULL")
            .bind(relation)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        if !exists {
            complete = false;
            continue;
        }
        let Some(table_schema) = schema_of(relation) else {
            return Err(format!("unqualified relation {relation}"));
        };
        let Some(table_name) = table_of(relation) else {
            return Err(format!("unqualified relation {relation}"));
        };
        let existing: Vec<String> = sqlx::query_scalar(
            "SELECT t.tgname::TEXT FROM pg_catalog.pg_trigger t \
             JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2 AND t.tgname LIKE $3",
        )
        .bind(table_schema)
        .bind(table_name)
        .bind(format!("{TRIGGER_PREFIX}%"))
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;

        let update_name = format!("{TRIGGER_PREFIX}_upd");
        if !existing.contains(&update_name) {
            let when = if table.update_fields.is_empty() {
                String::new()
            } else {
                let mut columns: Vec<String> = table
                    .update_fields
                    .iter()
                    .map(|field| table.model.q(field))
                    .collect();
                // A soft-deleted row is gone for the verdict too.
                if let Some(column) = &table.model.soft_delete_column {
                    columns.push(quote_ident(column));
                }
                let changed: Vec<String> = columns
                    .iter()
                    .map(|col| format!("OLD.{col} IS DISTINCT FROM NEW.{col}"))
                    .collect();
                format!("WHEN ({})", changed.join(" OR "))
            };
            sqlx::query(&format!(
                "CREATE CONSTRAINT TRIGGER {} AFTER UPDATE ON {relation} \
                 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW {when} EXECUTE FUNCTION {function}()",
                quote_ident(&update_name)
            ))
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        }
        let write_name = format!("{TRIGGER_PREFIX}_write");
        if !existing.contains(&write_name) {
            let events = if table.on_insert {
                "INSERT OR DELETE"
            } else {
                "DELETE"
            };
            sqlx::query(&format!(
                "CREATE CONSTRAINT TRIGGER {} AFTER {events} ON {relation} \
                 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {function}()",
                quote_ident(&write_name)
            ))
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        }
    }
    tx.commit().await.map_err(|e| e.to_string())?;
    Ok((sequence, complete))
}

impl AuthStateVersion {
    /// The current auth-state version, or `None` when it cannot be known (no
    /// Postgres store, triggers not installable, or the read failed): the
    /// caller then relies on the cache lifetime alone. Served from memory while
    /// the listener is connected; read from the sequence otherwise.
    pub(crate) async fn current(self: &Arc<Self>, pool: Option<&PgPool>) -> Option<i64> {
        let pool = pool?;
        let sequence = self.installed_sequence(pool).await?;
        if !read_per_call() {
            self.start_listener(pool, &sequence);
            if self.live.load(Ordering::Acquire) {
                return Some(self.known.load(Ordering::Acquire));
            }
        }
        read_sequence(pool, &sequence).await
    }

    /// Start the one listener of this instance (no-op once started).
    fn start_listener(self: &Arc<Self>, pool: &PgPool, sequence: &str) {
        if self.listening.swap(true, Ordering::AcqRel) {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            self.listening.store(false, Ordering::Release);
            return;
        };
        runtime.spawn(listen(
            Arc::downgrade(self),
            pool.clone(),
            sequence.to_string(),
        ));
    }

    /// The qualified sequence name, installing the triggers when missing.
    async fn installed_sequence(&self, pool: &PgPool) -> Option<String> {
        match self.installed.get() {
            Some(sequence) => {
                if self.partial.load(Ordering::Relaxed)
                    && now_secs() >= self.installed_at.load(Ordering::Relaxed) + RETRY_AFTER_SECS
                {
                    self.installed_at.store(now_secs(), Ordering::Relaxed);
                    if let Ok((_, complete)) = install(pool).await {
                        self.partial.store(!complete, Ordering::Relaxed);
                    }
                }
                Some(sequence.clone())
            }
            None => {
                let failed_at = self.failed_at.load(Ordering::Relaxed);
                if failed_at != 0 && now_secs() < failed_at + RETRY_AFTER_SECS {
                    return None;
                }
                match self
                    .installed
                    .get_or_try_init(|| async {
                        let (sequence, complete) = install(pool).await?;
                        self.partial.store(!complete, Ordering::Relaxed);
                        self.installed_at.store(now_secs(), Ordering::Relaxed);
                        Ok::<_, String>(sequence)
                    })
                    .await
                {
                    Ok(sequence) => Some(sequence.clone()),
                    Err(error) => {
                        self.failed_at.store(now_secs(), Ordering::Relaxed);
                        tracing::warn!(
                            target: "udb::authn",
                            %error,
                            "auth-state version triggers could not be installed; a bearer \
                             verdict is remembered for UDB_AUTH_VERDICT_CACHE_MS without a \
                             cross-node change check (retrying in 60 s)",
                        );
                        None
                    }
                }
            }
        }
    }
}

/// Read the sequence: the version as of now.
async fn read_sequence(pool: &PgPool, sequence: &str) -> Option<i64> {
    match sqlx::query_scalar::<_, i64>(&format!(
        // `last_value` reads 1 both before and after the first nextval;
        // only `is_called` tells them apart.
        "/* udb_auth_state_version */ SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM {sequence}"
    ))
    .fetch_one(pool)
    .await
    {
        Ok(version) => Some(version),
        Err(error) => {
            tracing::debug!(target: "udb::authn", %error, "auth-state version read failed");
            None
        }
    }
}

/// Keep `known` current from the trigger notifications until the instance is
/// dropped. `live` is true only while connected and caught up.
async fn listen(state: Weak<AuthStateVersion>, pool: PgPool, sequence: String) {
    loop {
        if state.strong_count() == 0 {
            return;
        }
        let connected = async {
            let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
            listener.listen(CHANNEL).await?;
            Ok::<_, sqlx::Error>(listener)
        }
        .await;
        let mut listener = match connected {
            Ok(listener) => listener,
            Err(error) => {
                tracing::debug!(
                    target: "udb::authn",
                    %error,
                    "auth-state listener could not connect; calls read the version",
                );
                tokio::time::sleep(RECONNECT_AFTER).await;
                continue;
            }
        };
        // Anything committed before LISTEN took effect was not notified.
        let Some(version) = read_sequence(&pool, &sequence).await else {
            tokio::time::sleep(RECONNECT_AFTER).await;
            continue;
        };
        {
            let Some(state) = state.upgrade() else {
                return;
            };
            state.known.fetch_max(version, Ordering::AcqRel);
            state.live.store(true, Ordering::Release);
        }
        loop {
            let received = tokio::time::timeout(BACKSTOP, listener.try_recv()).await;
            let Some(state) = state.upgrade() else {
                return;
            };
            let version = match received {
                Ok(Ok(Some(notification))) => match notification.payload().parse::<i64>() {
                    Ok(version) => Some(version),
                    Err(_) => read_sequence(&pool, &sequence).await,
                },
                // No notification for a while: re-read as a backstop.
                Err(_) => read_sequence(&pool, &sequence).await,
                // Connection lost: changes may go unnotified until reconnected.
                Ok(Ok(None)) | Ok(Err(_)) => None,
            };
            match version {
                Some(version) => {
                    state.known.fetch_max(version, Ordering::AcqRel);
                }
                None => {
                    state.live.store(false, Ordering::Release);
                    break;
                }
            }
        }
        tokio::time::sleep(RECONNECT_AFTER).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualified_relations_split_into_schema_and_table() {
        assert_eq!(schema_of("\"udb_authn\".\"users\""), Some("udb_authn"));
        assert_eq!(table_of("\"udb_authn\".\"users\""), Some("users"));
        assert_eq!(schema_of("users"), None);
    }

    #[test]
    fn every_watched_table_is_in_the_users_schema_or_qualified() {
        for table in watched() {
            assert!(
                schema_of(&table.model.relation).is_some(),
                "{} must be schema-qualified",
                table.model.relation
            );
        }
    }
}
