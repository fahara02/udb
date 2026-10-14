//! The leader-elected inbox retention worker: deletes inbox rows that their
//! target device acknowledged, or that outlived `UDB_MESSAGING_INBOX_TTL`, and
//! claimed (spent) KeyPackages older than the same TTL. Spawned by `serve()`
//! under `WORKER_MESSAGING_INBOX_RETENTION`, the same way the lock expiry reaper
//! is, and run over the messaging store of every active project.

use std::time::Duration;

use sqlx::PgPool;
use tonic::Status;

use super::config::INBOX_RETENTION_BATCH;
use super::errors::{messaging_internal_status, messaging_store_status};
use super::store::{inbox_retention_sql, models, spent_key_package_retention_sql};

/// Batches one store pass deletes at most, so one pass never runs unbounded.
const MAX_BATCHES_PER_PASS: usize = 20;

/// Delete in bounded batches until a batch comes back short. Each batch is its
/// own transaction with the `app.platform_admin` escape installed
/// transaction-locally: retention is cross-tenant system work and the tables
/// FORCE row-level security even for their owner.
async fn delete_batches(
    pool: &PgPool,
    sql: &str,
    ttl: Duration,
    batch: i64,
) -> Result<i64, Status> {
    let mut deleted = 0i64;
    for _ in 0..MAX_BATCHES_PER_PASS {
        let mut tx = pool
            .begin()
            .await
            .map_err(|err| messaging_store_status("retention_begin", &err))?;
        sqlx::query("SELECT set_config('app.platform_admin', 'true', true)")
            .execute(&mut *tx)
            .await
            .map_err(|err| messaging_store_status("retention_scope", &err))?;
        let done = sqlx::query(sql)
            .bind(ttl.as_secs_f64())
            .bind(batch)
            .execute(&mut *tx)
            .await
            .map_err(|err| messaging_store_status("retention_delete", &err))?;
        tx.commit()
            .await
            .map_err(|err| messaging_store_status("retention_commit", &err))?;
        let rows = done.rows_affected() as i64;
        deleted += rows;
        if rows < batch {
            break;
        }
    }
    Ok(deleted)
}

/// One retention pass over one store. Returns the rows deleted.
pub(crate) async fn run_inbox_retention_once(
    pool: &PgPool,
    ttl: Duration,
    batch: i64,
) -> Result<i64, Status> {
    let batch = batch.clamp(1, INBOX_RETENTION_BATCH);
    let m = models();
    let inbox = delete_batches(pool, &inbox_retention_sql(&m.inbox), ttl, batch).await?;
    let spent = delete_batches(
        pool,
        &spent_key_package_retention_sql(&m.key_package),
        ttl,
        batch,
    )
    .await?;
    Ok(inbox + spent)
}

/// One retention pass over the messaging store of the default project and every
/// active project, each distinct physical instance once. Keeps going past a
/// failing project and reports the failures afterwards.
pub(crate) async fn run_inbox_retention_all_projects(
    runtime: &crate::runtime::DataBrokerRuntime,
    project_ids: Vec<String>,
    ttl: Duration,
    batch: i64,
) -> Result<i64, Status> {
    let mut seen = std::collections::BTreeSet::new();
    let mut deleted = 0i64;
    let mut failures = Vec::new();
    let mut projects = vec![String::new()];
    projects.extend(project_ids.into_iter().filter(|p| !p.trim().is_empty()));
    for project_id in projects {
        let context = crate::RequestContext {
            project_id: project_id.clone(),
            ..crate::RequestContext::default()
        };
        match runtime.native_store_postgres_binding_for_service("messaging", true, &context) {
            Ok((pool, instance)) => {
                let key = instance.as_deref().map(str::trim).unwrap_or("").to_string();
                if !seen.insert(key) {
                    continue;
                }
                match run_inbox_retention_once(&pool, ttl, batch).await {
                    Ok(count) => deleted += count,
                    Err(err) => failures.push(format!("{project_id}: {}", err.message())),
                }
            }
            Err(err) => failures.push(format!("{project_id}: {}", err.message())),
        }
    }
    if failures.is_empty() {
        Ok(deleted)
    } else {
        Err(messaging_internal_status(
            "inbox_retention_projects",
            format!(
                "messaging retention project failures: {}",
                failures.join("; ")
            ),
        ))
    }
}
