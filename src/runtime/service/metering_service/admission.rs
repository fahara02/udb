//! The admission-hook seam: the process-local metering sink, the canonical
//! metric label, and the durable ingest [`record_usage`] the leader calls from
//! `native_helpers::admit_on`.

use std::sync::{OnceLock, RwLock};

use sqlx::PgPool;
use tonic::Status;

use super::calc::wall_now_unix;
use super::config::DEFAULT_UNIT;
use super::errors::metering_internal_status;

static ADMISSION_METERING_POOL: OnceLock<RwLock<Option<PgPool>>> = OnceLock::new();

fn admission_metering_cell() -> &'static RwLock<Option<PgPool>> {
    ADMISSION_METERING_POOL.get_or_init(|| RwLock::new(None))
}

/// Install or clear the process-local metering sink used by the native admission
/// hook. The pool itself is still resolved by `native_store_pool_for_service`
/// during service construction; hot-path callers only clone this already-routed
/// `PgPool` handle and never read env/config.
pub(crate) fn install_admission_metering_pool(pool: Option<PgPool>) {
    if let Ok(mut guard) = admission_metering_cell().write() {
        *guard = pool;
    }
}

/// Clone the installed admission-metering pool, if any. Poison/unconfigured both
/// fail open because usage metering must never fail an admitted native request.
pub(crate) fn admission_metering_pool() -> Option<PgPool> {
    admission_metering_cell().read().ok()?.clone()
}

/// Canonical metric/method label for the automatic admission hook. Kept pure so
/// quotas can target the same string deterministically.
pub(crate) fn admission_metering_method(service_label: &str, op_label: &str) -> String {
    let service = service_label.trim();
    let op = op_label.trim();
    match (service.is_empty(), op.is_empty()) {
        (true, true) => "native.unknown".to_string(),
        (true, false) => format!("native.{op}"),
        (false, true) => service.to_string(),
        (false, false) => format!("{service}.{op}"),
    }
}

/// Append ONE durable usage event. This is the seam the leader calls from
/// `native_helpers::admit_on`: a single cheap INSERT, no read.
///
/// Metering must NEVER fail the request that triggered it, so any store error is
/// logged and swallowed and the function returns `Ok(())`. The INSERT sets the
/// per-statement tenant GUC via `set_config(..., is_local=true)` so the row's RLS
/// `WITH CHECK` passes even on a raw pooled connection (correct whether or not the
/// table is `FORCE ROW LEVEL SECURITY`); `usage_id`/audit columns fall to their
/// DB defaults.
async fn write_usage(
    pool: &PgPool,
    tenant_id: &str,
    principal_id: &str,
    method: &str,
    unit: &str,
    quantity: i64,
    now_unix: i64,
) -> Result<(), sqlx::Error> {
    let tenant_id = tenant_id.trim();
    let method = method.trim();
    if tenant_id.is_empty() || method.is_empty() {
        // Nothing to attribute — never an error surfaced to the metered caller.
        return Ok(());
    }
    let unit = {
        let u = unit.trim();
        if u.is_empty() { DEFAULT_UNIT } else { u }
    };
    let quantity = quantity.max(0);
    let occurred = if now_unix > 0 {
        now_unix
    } else {
        wall_now_unix()
    };

    // Single statement: the WHERE both sets the tenant GUC (so RLS WITH CHECK
    // passes) and is always true for a non-empty tenant, then the SELECT row is
    // inserted. No separate read; no timestamptz/uuid text bind.
    sqlx::query(
        "INSERT INTO udb_metering.usage_events \
         (tenant_id, principal_id, method, unit, quantity, occurred_at, occurred_at_unix) \
         SELECT $1, $2, $3, $4, $5, to_timestamp($6), $7 \
         WHERE set_config('app.current_tenant_id', $1, true) IS NOT NULL",
    )
    .bind(tenant_id)
    .bind(principal_id.trim())
    .bind(method)
    .bind(unit)
    .bind(quantity)
    .bind(occurred as f64)
    .bind(occurred)
    .execute(pool)
    .await?;
    Ok(())
}

/// Fail-open usage append for automatic admission metering. The operation being
/// measured must never fail because its telemetry sink is unavailable.
pub(crate) async fn record_usage(
    pool: &PgPool,
    tenant_id: &str,
    principal_id: &str,
    method: &str,
    unit: &str,
    quantity: i64,
    now_unix: i64,
) -> Result<(), Status> {
    let res = write_usage(
        pool,
        tenant_id,
        principal_id,
        method,
        unit,
        quantity,
        now_unix,
    )
    .await;

    if let Err(err) = res {
        tracing::warn!(
            target: "udb::metering",
            error = %err,
            tenant_id = %tenant_id,
            method = %method,
            "usage metering insert failed; swallowing so metering never fails the metered request",
        );
    }
    Ok(())
}

/// Strict usage append for the explicit MeteringService.RecordUsage contract.
/// Unlike the automatic admission hook, this RPC is itself the requested write:
/// acknowledging it without a durable row would undercount billing/quota usage.
pub(crate) async fn record_usage_strict(
    pool: &PgPool,
    tenant_id: &str,
    principal_id: &str,
    method: &str,
    unit: &str,
    quantity: i64,
    now_unix: i64,
) -> Result<(), Status> {
    write_usage(
        pool,
        tenant_id,
        principal_id,
        method,
        unit,
        quantity,
        now_unix,
    )
    .await
    .map_err(|err| {
        metering_internal_status(
            "record_usage_insert",
            format!("record usage durable append failed: {err}"),
        )
    })
}
