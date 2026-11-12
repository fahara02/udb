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
/// measured must never fail, or wait, because of its telemetry sink.
///
/// The event is handed to the background usage writer and this returns at once:
/// the metered request pays no database round trip. The writer stores queued
/// events in batches (one statement per tenant per flush), so a busy broker
/// makes a few metering writes per second instead of one per call. Events show
/// up in usage reads within one flush interval. When the queue is full the
/// event is not stored; the writer reports how many were lost, loudly.
pub(crate) async fn record_usage(
    pool: &PgPool,
    tenant_id: &str,
    principal_id: &str,
    method: &str,
    unit: &str,
    quantity: i64,
    now_unix: i64,
) -> Result<(), Status> {
    let tenant_id = tenant_id.trim();
    let method = method.trim();
    if tenant_id.is_empty() || method.is_empty() {
        // Nothing to attribute: never an error surfaced to the metered caller.
        return Ok(());
    }
    let unit = match unit.trim() {
        "" => DEFAULT_UNIT,
        u => u,
    };
    let occurred = if now_unix > 0 {
        now_unix
    } else {
        wall_now_unix()
    };
    usage_writer::enqueue(usage_writer::QueuedUsage {
        pool: pool.clone(),
        tenant_id: tenant_id.to_string(),
        principal_id: principal_id.trim().to_string(),
        method: method.to_string(),
        unit: unit.to_string(),
        quantity: quantity.max(0),
        occurred_unix: occurred,
    });
    Ok(())
}

/// The background writer behind [`record_usage`].
pub(crate) mod usage_writer {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use sqlx::PgPool;
    use tokio::sync::mpsc;

    /// Events waiting to be stored, at most. Past this an event is dropped and
    /// counted rather than making the request wait.
    const QUEUE_CAPACITY: usize = 65_536;
    /// Events stored per flush, at most.
    const MAX_BATCH: usize = 1_000;
    /// How long an event waits for others to share its flush.
    const FLUSH_INTERVAL: Duration = Duration::from_millis(250);
    /// One flush statement may take this long before it is abandoned.
    const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
    /// Lost-event reports are logged at most this often.
    const REPORT_INTERVAL: Duration = Duration::from_secs(10);

    pub(crate) struct QueuedUsage {
        pub(crate) pool: PgPool,
        pub(crate) tenant_id: String,
        pub(crate) principal_id: String,
        pub(crate) method: String,
        pub(crate) unit: String,
        pub(crate) quantity: i64,
        pub(crate) occurred_unix: i64,
    }

    static SENDER: Mutex<Option<mpsc::Sender<QueuedUsage>>> = Mutex::new(None);
    static DROPPED_QUEUE_FULL: AtomicU64 = AtomicU64::new(0);
    static DROPPED_WRITE_FAILED: AtomicU64 = AtomicU64::new(0);
    static STORED: AtomicU64 = AtomicU64::new(0);

    /// Events lost because the queue was full (or no runtime was running), and
    /// because their flush failed.
    pub(crate) fn dropped() -> (u64, u64) {
        (
            DROPPED_QUEUE_FULL.load(Ordering::Relaxed),
            DROPPED_WRITE_FAILED.load(Ordering::Relaxed),
        )
    }

    /// Events stored so far.
    pub(crate) fn stored() -> u64 {
        STORED.load(Ordering::Relaxed)
    }

    /// A sender bound to a running writer, starting one on the current runtime
    /// when there is none or the previous one's runtime has ended.
    fn sender() -> Option<mpsc::Sender<QueuedUsage>> {
        let mut guard = SENDER.lock().ok()?;
        if let Some(tx) = guard.as_ref().filter(|tx| !tx.is_closed()) {
            return Some(tx.clone());
        }
        let handle = tokio::runtime::Handle::try_current().ok()?;
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        handle.spawn(run(rx));
        *guard = Some(tx.clone());
        Some(tx)
    }

    fn lost(counter: &AtomicU64, reason: &str, count: u64) {
        counter.fetch_add(count, Ordering::Relaxed);
        crate::runtime::metrics::metering_events_lost()
            .with_label_values(&[reason])
            .inc_by(count);
    }

    pub(crate) fn enqueue(event: QueuedUsage) {
        let Some(tx) = sender() else {
            lost(&DROPPED_QUEUE_FULL, "queue_full", 1);
            return;
        };
        match tx.try_send(event) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                lost(&DROPPED_QUEUE_FULL, "queue_full", 1);
            }
            Err(mpsc::error::TrySendError::Closed(event)) => {
                // The writer's runtime ended between the check and the send:
                // start a new writer once and hand the event to it.
                if let Ok(mut guard) = SENDER.lock() {
                    *guard = None;
                }
                if !matches!(sender().map(|tx| tx.try_send(event)), Some(Ok(()))) {
                    lost(&DROPPED_QUEUE_FULL, "queue_full", 1);
                }
            }
        }
    }

    async fn run(mut rx: mpsc::Receiver<QueuedUsage>) {
        let mut reported = (0u64, 0u64);
        let mut last_report = Instant::now();
        let mut batch: Vec<QueuedUsage> = Vec::with_capacity(MAX_BATCH);
        loop {
            let Some(first) = rx.recv().await else {
                return;
            };
            batch.push(first);
            let deadline = tokio::time::Instant::now() + FLUSH_INTERVAL;
            while batch.len() < MAX_BATCH {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(event)) => batch.push(event),
                    Ok(None) | Err(_) => break,
                }
            }
            flush(&mut batch).await;
            let now = dropped();
            if now != reported && last_report.elapsed() >= REPORT_INTERVAL {
                tracing::warn!(
                    target: "udb::metering",
                    lost_queue_full = now.0,
                    lost_write_failed = now.1,
                    stored = stored(),
                    "usage metering lost events (totals since start); usage reads under-count by this many",
                );
                reported = now;
                last_report = Instant::now();
            }
        }
    }

    /// Store a batch: one statement per (pool, tenant), the tenant GUC set in
    /// the same statement so the row-level-security check passes.
    async fn flush(batch: &mut Vec<QueuedUsage>) {
        let mut groups: Vec<(PgPool, BTreeMap<String, Vec<QueuedUsage>>)> = Vec::new();
        for event in batch.drain(..) {
            let options = event.pool.connect_options();
            let slot = match groups
                .iter()
                .position(|(pool, _)| Arc::ptr_eq(&pool.connect_options(), &options))
            {
                Some(i) => i,
                None => {
                    groups.push((event.pool.clone(), BTreeMap::new()));
                    groups.len() - 1
                }
            };
            groups[slot]
                .1
                .entry(event.tenant_id.clone())
                .or_default()
                .push(event);
        }
        for (pool, tenants) in groups {
            for (tenant, events) in tenants {
                let count = events.len() as u64;
                match tokio::time::timeout(WRITE_TIMEOUT, write_tenant(&pool, &tenant, &events))
                    .await
                {
                    Ok(Ok(())) => {
                        STORED.fetch_add(count, Ordering::Relaxed);
                    }
                    Ok(Err(err)) => {
                        lost(&DROPPED_WRITE_FAILED, "write_failed", count);
                        tracing::warn!(
                            target: "udb::metering", error = %err, tenant_id = %tenant, events = count,
                            "usage metering flush failed; these events are not stored",
                        );
                    }
                    Err(_) => {
                        lost(&DROPPED_WRITE_FAILED, "write_failed", count);
                        tracing::warn!(
                            target: "udb::metering", tenant_id = %tenant, events = count,
                            "usage metering flush exceeded its time budget; these events are not stored",
                        );
                    }
                }
            }
        }
    }

    async fn write_tenant(
        pool: &PgPool,
        tenant: &str,
        events: &[QueuedUsage],
    ) -> Result<(), sqlx::Error> {
        let principals: Vec<&str> = events.iter().map(|e| e.principal_id.as_str()).collect();
        let methods: Vec<&str> = events.iter().map(|e| e.method.as_str()).collect();
        let units: Vec<&str> = events.iter().map(|e| e.unit.as_str()).collect();
        let quantities: Vec<i64> = events.iter().map(|e| e.quantity).collect();
        let occurred: Vec<i64> = events.iter().map(|e| e.occurred_unix).collect();
        sqlx::query(concat!(
            "/* udb_metering_usage_flush */ INSERT INTO udb_metering.usage_events ",
            "(tenant_id, principal_id, method, unit, quantity, occurred_at, occurred_at_unix) ",
            "SELECT $1, u.principal_id, u.method, u.unit, u.quantity, ",
            "to_timestamp(u.occurred::DOUBLE PRECISION), u.occurred ",
            "FROM UNNEST($2::TEXT[], $3::TEXT[], $4::TEXT[], $5::BIGINT[], $6::BIGINT[]) ",
            "AS u(principal_id, method, unit, quantity, occurred) ",
            "WHERE set_config('app.current_tenant_id', $1, true) IS NOT NULL",
        ))
        .bind(tenant)
        .bind(&principals)
        .bind(&methods)
        .bind(&units)
        .bind(&quantities)
        .bind(&occurred)
        .execute(pool)
        .await?;
        Ok(())
    }
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
