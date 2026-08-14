//! The seven `AnalyticsService` RPC handlers, extracted from the trait impl as
//! free `pub(crate) async fn`s taking `svc` where the trait method took `&self`.
//! `mod.rs` delegates one line to each. Bodies are verbatim — the same online
//! aggregation, canonical-tenant scoping, native-dispatch/raw-SQL split,
//! platform-admin gating, percentile rollup, and contract-declared event emission
//! as the former god file.

use sqlx::Row;
use tonic::{Request, Response, Status};

use crate::proto::udb::core::analytics::services::v1 as ana_pb;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{
    admit_on as native_admit_on, metadata_project_id, metadata_tenant_id, native_page_response,
    native_page_window, tenant_only_native_service_context,
};
use super::AnalyticsServiceImpl;
use super::config::{
    EVENT_TYPE_PIPELINE_METRIC_RECORDED, EVENT_TYPE_SNAPSHOT_TRIGGERED, MAX_ANALYTICS_READ_ROWS,
    PIPELINE_SUMMARY_PAGE_SIZE, PMS_MSG, SECONDS_PER_HOUR,
};
use super::errors::{analytics_internal_status, analytics_required_field, require_platform_admin};
use super::events::{analytics_event_payload, emit_analytics_event};
use super::model::{
    eps_from_json, eps_from_row, eps_model, pms_from_json, pms_from_row, pms_model, ras_from_json,
    ras_from_row, ras_model, timestamp_hour_period,
};
use super::rollup::run_analytics_rollup_scoped;
use super::store::{
    executor_performance_read, install_analytics_tenant_scope_sql, pipeline_summary_filter,
    pipeline_summary_read, pms_projection, reconciliation_analytics_read, sla_compliance_read,
    sla_compliance_sql,
};

pub(crate) async fn record_pipeline_metric(
    svc: &AnalyticsServiceImpl,
    request: Request<ana_pb::RecordPipelineMetricRequest>,
) -> Result<Response<ana_pb::RecordPipelineMetricResponse>, Status> {
    let metadata = request.metadata().clone();
    let mut req = request.into_inner();
    // B12: persist under the canonical tenant — the VALIDATED bearer/header
    // tenant (a UUID), not the raw request-body string (e.g. a human "code").
    // GetThroughput/GetPipelineSummary filter by this same canonical tenant, so
    // a divergent body value here would store rows the reads can never sum.
    if let Some(canonical) = metadata_tenant_id(&metadata) {
        req.tenant_id = canonical;
    }
    if req.stage_name.trim().is_empty() {
        return Err(analytics_required_field(
            "stage_name",
            "must be a non-empty pipeline stage name",
            "stage_name is required",
        ));
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "analytics",
        OperationChannel::Write,
        &req.tenant_id,
        None,
    )
    .await?;
    let pool = svc.require_pool()?;
    let m = pms_model();
    let rel = m.relation.clone();
    let existing_total = format!("existing.{}", m.q("total_requests"));
    let existing_successful = format!("existing.{}", m.q("successful"));
    let existing_failed = format!("existing.{}", m.q("failed"));
    let existing_avg = format!("existing.{}", m.q("avg_latency_ms"));
    let (succ, fail) = if req.is_success {
        (1i64, 0i64)
    } else {
        (0i64, 1i64)
    };
    // Online aggregation into the hourly bucket. ON CONFLICT targets the real
    // unique key (snapshot_hour, stage_name, tenant_id); the running mean
    // latency and derived error_rate/throughput are recomputed from the
    // post-increment total. Percentiles are out of scope for online
    // aggregation — the rollup pass fills them (see module docs).
    // In ON CONFLICT DO UPDATE, bare column refs on the RHS are the EXISTING
    // row's pre-update values (all SET RHS see the old row), and EXCLUDED.* is
    // the would-be-inserted row — so the running mean / rate / rps below are
    // computed consistently against the old totals.
    sqlx::query(&format!(
        "INSERT INTO {rel} AS existing \
           ({hour}, {stage}, {tenant}, {total}, {succ_c}, {fail_c}, {avg}, {err}, {rps}) \
         VALUES (date_trunc('hour', now()), $1, $2, 1, $3, $4, $5, \
                 $4::float8 / 1, 1::float8 / {secs_per_hour}) \
         ON CONFLICT ({hour}, {stage}, {tenant}) DO UPDATE SET \
           {total} = {existing_total} + 1, \
           {succ_c} = {existing_successful} + EXCLUDED.{succ_c}, \
           {fail_c} = {existing_failed} + EXCLUDED.{fail_c}, \
           {avg} = (COALESCE({existing_avg},0) * {existing_total} + $5) / ({existing_total} + 1), \
           {err} = ({existing_failed} + EXCLUDED.{fail_c})::float8 / ({existing_total} + 1), \
           {rps} = ({existing_total} + 1)::float8 / {secs_per_hour}",
        rel = rel,
        secs_per_hour = SECONDS_PER_HOUR,
        hour = m.q("snapshot_hour"),
        stage = m.q("stage_name"),
        tenant = m.q("tenant_id"),
        total = m.q("total_requests"),
        succ_c = m.q("successful"),
        fail_c = m.q("failed"),
        avg = m.q("avg_latency_ms"),
        err = m.q("error_rate"),
        rps = m.q("throughput_rps"),
        existing_total = existing_total,
        existing_successful = existing_successful,
        existing_failed = existing_failed,
        existing_avg = existing_avg,
    ))
    .bind(&req.stage_name)
    .bind(&req.tenant_id)
    .bind(succ)
    .bind(fail)
    .bind(req.latency_ms)
    .execute(pool)
    .await
    .map_err(|err| {
        crate::runtime::executor_utils::sqlx_error_to_status("record pipeline metric failed", &err)
    })?;
    // Fulfil the proto-declared `method_event_contract`: one outbox event per
    // durable mutation (topic `analytics.events`, at-least-once via CDC).
    emit_analytics_event(
        svc,
        EVENT_TYPE_PIPELINE_METRIC_RECORDED,
        &req.tenant_id,
        &metadata_project_id(&metadata).unwrap_or_default(),
        &req.stage_name,
        analytics_event_payload(
            EVENT_TYPE_PIPELINE_METRIC_RECORDED,
            &req.stage_name,
            &req.tenant_id,
            None,
        ),
    )
    .await;
    Ok(Response::new(ana_pb::RecordPipelineMetricResponse {
        accepted: true,
    }))
}

pub(crate) async fn get_pipeline_summary(
    svc: &AnalyticsServiceImpl,
    request: Request<ana_pb::GetPipelineSummaryRequest>,
) -> Result<Response<ana_pb::GetPipelineSummaryResponse>, Status> {
    let metadata = request.metadata().clone();
    let mut req = request.into_inner();
    // B12 sibling: read under the SAME canonical (validated bearer/header)
    // tenant RecordPipelineMetric persists under, so a tenant caller can
    // neither read foreign rows nor the NULL-tenant system-wide aggregates
    // by leaving the body tenant empty.
    if let Some(canonical) = metadata_tenant_id(&metadata) {
        req.tenant_id = canonical;
    }
    let page = native_page_window(req.page.as_ref(), PIPELINE_SUMMARY_PAGE_SIZE);
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "analytics",
        OperationChannel::Read,
        &req.tenant_id,
        None,
    )
    .await?;
    if req.hour_from.trim().is_empty()
        && req.hour_to.trim().is_empty()
        && let Some(runtime) = svc.runtime()
    {
        let context = tenant_only_native_service_context(&metadata, &req.tenant_id);
        let filter = pipeline_summary_filter(&req);
        let total = runtime
            .native_entity_count_for_service("analytics", &context, PMS_MSG, filter.clone())
            .await?;
        let rows = runtime
            .native_entity_read_for_service(
                "analytics",
                &context,
                pipeline_summary_read(filter, page.offset as u64, page.limit as u32),
            )
            .await?;
        let snapshots = rows.iter().map(pms_from_json).collect();
        return Ok(Response::new(ana_pb::GetPipelineSummaryResponse {
            snapshots,
            page: Some(native_page_response(
                req.page.as_ref(),
                total,
                PIPELINE_SUMMARY_PAGE_SIZE,
            )),
        }));
    }
    let pool = svc.require_pool()?;
    let m = pms_model();
    let rel = m.relation.clone();
    let projection = pms_projection(&m);
    // Transitional: this response includes COUNT(*) OVER() pagination metadata
    // plus timestamp casts. Native read/count handles the no-window path
    // above; timestamp windows remain on the capability-gated Postgres path
    // until dispatch preserves typed timestamp params across every backend.
    let rows = sqlx::query(&format!(
        "SELECT {projection}, COUNT(*) OVER() AS total_count FROM {rel} \
         WHERE ($1 = '' OR {stage} = $1) \
           AND ($2 = '' OR {tenant} = $2) \
           AND ($3 = '' OR {hour} >= $3::timestamptz) \
           AND ($4 = '' OR {hour} <= $4::timestamptz) \
         ORDER BY {hour} DESC LIMIT $5 OFFSET $6",
        stage = m.q("stage_name"),
        tenant = m.q("tenant_id"),
        hour = m.q("snapshot_hour"),
    ))
    .bind(&req.stage_name)
    .bind(&req.tenant_id)
    .bind(&req.hour_from)
    .bind(&req.hour_to)
    .bind(page.limit_i64())
    .bind(page.offset_i64())
    .fetch_all(pool)
    .await
    .map_err(|err| {
        analytics_internal_status(
            "get_pipeline_summary",
            format!("get pipeline summary failed: {err}"),
        )
    })?;
    let total: i64 = rows
        .first()
        .and_then(|r| r.try_get("total_count").ok())
        .unwrap_or(0);
    let snapshots = rows.iter().map(pms_from_row).collect();
    Ok(Response::new(ana_pb::GetPipelineSummaryResponse {
        snapshots,
        page: Some(native_page_response(
            req.page.as_ref(),
            total,
            PIPELINE_SUMMARY_PAGE_SIZE,
        )),
    }))
}

pub(crate) async fn get_executor_performance(
    svc: &AnalyticsServiceImpl,
    request: Request<ana_pb::GetExecutorPerformanceRequest>,
) -> Result<Response<ana_pb::GetExecutorPerformanceResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    // `executor_performance_summaries` is a system-global operational table
    // (no tenant column) — gate it behind the platform-admin identity.
    require_platform_admin("get_executor_performance")?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "analytics",
        OperationChannel::Read,
        &metadata_tenant_id(&metadata).unwrap_or_default(),
        None,
    )
    .await?;
    if req.date_from.trim().is_empty()
        && req.date_to.trim().is_empty()
        && let Some(runtime) = svc.runtime()
    {
        // Scope to the VALIDATED caller tenant, exactly as the admission check
        // above already resolves it. Passing "" here built a tenant-less context:
        // harmless only while these summary entities carry no tenant column, and
        // a `tenant_scope_required` failure the moment one is added.
        let context = tenant_only_native_service_context(
            &metadata,
            &metadata_tenant_id(&metadata).unwrap_or_default(),
        );
        let rows = runtime
            .native_entity_read_for_service("analytics", &context, executor_performance_read(&req))
            .await?;
        let summaries = rows.iter().map(eps_from_json).collect();
        return Ok(Response::new(ana_pb::GetExecutorPerformanceResponse {
            summaries,
        }));
    }
    let pool = svc.require_pool()?;
    let m = eps_model();
    let rel = m.relation.clone();
    // Transitional: date-range filters currently rely on Postgres date casts;
    // typed reads handle the simple entity list above.
    let projection = format!(
        "{id}, {date}, {exec}, {workload}, {dispatches}, {succ}, {timeouts}, {errors}, \
         COALESCE({avg_exec},0) AS avg_execution_ms, COALESCE({p99},0) AS p99_execution_ms, \
         COALESCE({avg_conf},0) AS avg_confidence, COALESCE({rate},0) AS success_rate, \
         COALESCE({cap},0) AS avg_capacity_utilisation, {recorded}",
        id = m.text_as("summary_id", "summary_id"),
        date = m.timestamp_unix_as("summary_date", "summary_date"),
        exec = m.text_or_empty_as("executor_identity", "executor_identity"),
        workload = m.text_or_empty_as("workload_kind", "workload_kind"),
        dispatches = m.select_as("total_dispatches", "total_dispatches"),
        succ = m.select_as("successful_results", "successful_results"),
        timeouts = m.select_as("timeout_count", "timeout_count"),
        errors = m.select_as("error_count", "error_count"),
        avg_exec = m.q("avg_execution_ms"),
        p99 = m.q("p99_execution_ms"),
        avg_conf = m.q("avg_confidence"),
        rate = m.q("success_rate"),
        cap = m.q("avg_capacity_utilisation"),
        recorded = m.timestamp_unix_as("recorded_at", "recorded_at"),
    );
    let rows = sqlx::query(&format!(
        "SELECT {projection} FROM {rel} \
         WHERE ($1 = '' OR {exec} = $1) \
           AND ($2 = '' OR {workload} = $2) \
           AND ($3 = '' OR {date} >= $3::date) \
           AND ($4 = '' OR {date} <= $4::date) \
         ORDER BY {date} DESC, {exec} LIMIT $5",
        exec = m.q("executor_identity"),
        workload = m.q("workload_kind"),
        date = m.q("summary_date"),
    ))
    .bind(&req.executor_identity)
    .bind(&req.workload_kind)
    .bind(&req.date_from)
    .bind(&req.date_to)
    .bind(i64::from(MAX_ANALYTICS_READ_ROWS))
    .fetch_all(pool)
    .await
    .map_err(|err| {
        analytics_internal_status(
            "get_executor_performance",
            format!("get executor performance failed: {err}"),
        )
    })?;
    let summaries = rows.iter().map(eps_from_row).collect();
    Ok(Response::new(ana_pb::GetExecutorPerformanceResponse {
        summaries,
    }))
}

pub(crate) async fn get_reconciliation_analytics(
    svc: &AnalyticsServiceImpl,
    request: Request<ana_pb::GetReconciliationAnalyticsRequest>,
) -> Result<Response<ana_pb::GetReconciliationAnalyticsResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    // `reconciliation_analytics_summaries` is a system-global operational
    // table (no tenant column) — gate it behind the platform-admin identity.
    require_platform_admin("get_reconciliation_analytics")?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "analytics",
        OperationChannel::Read,
        &metadata_tenant_id(&metadata).unwrap_or_default(),
        None,
    )
    .await?;
    if req.date_from.trim().is_empty()
        && req.date_to.trim().is_empty()
        && let Some(runtime) = svc.runtime()
    {
        // Scope to the VALIDATED caller tenant, exactly as the admission check
        // above already resolves it. Passing "" here built a tenant-less context:
        // harmless only while these summary entities carry no tenant column, and
        // a `tenant_scope_required` failure the moment one is added.
        let context = tenant_only_native_service_context(
            &metadata,
            &metadata_tenant_id(&metadata).unwrap_or_default(),
        );
        let rows = runtime
            .native_entity_read_for_service("analytics", &context, reconciliation_analytics_read())
            .await?;
        let summaries: Vec<_> = rows.iter().map(ras_from_json).collect();
        let total_recon: i64 = summaries.iter().map(|s| s.total_reconciliations).sum();
        let total_exact: i64 = summaries.iter().map(|s| s.exact_matches).sum();
        let overall_resolution_rate = if total_recon > 0 {
            total_exact as f64 / total_recon as f64
        } else {
            0.0
        };
        let avg_reconciliation_ms = if summaries.is_empty() {
            0.0
        } else {
            summaries
                .iter()
                .map(|s| s.avg_reconciliation_ms)
                .sum::<f64>()
                / summaries.len() as f64
        };
        return Ok(Response::new(ana_pb::GetReconciliationAnalyticsResponse {
            summaries,
            overall_resolution_rate,
            avg_reconciliation_ms,
        }));
    }
    let pool = svc.require_pool()?;
    let m = ras_model();
    let rel = m.relation.clone();
    // Transitional: date-range filters currently rely on Postgres date casts;
    // typed reads handle the simple entity list above.
    let projection = format!(
        "{id}, {date}, {total}, {exact}, {partial}, {hard}, {low}, \
         COALESCE({avg_ms},0) AS avg_reconciliation_ms, \
         COALESCE({rate},0) AS resolution_rate, \
         COALESCE({conf},0) AS avg_record_confidence, {recorded}",
        id = m.text_as("summary_id", "summary_id"),
        date = m.timestamp_unix_as("summary_date", "summary_date"),
        total = m.select_as("total_reconciliations", "total_reconciliations"),
        exact = m.select_as("exact_matches", "exact_matches"),
        partial = m.select_as("partial_conflicts", "partial_conflicts"),
        hard = m.select_as("hard_conflicts", "hard_conflicts"),
        low = m.select_as("low_confidence_flagged", "low_confidence_flagged"),
        avg_ms = m.q("avg_reconciliation_ms"),
        rate = m.q("resolution_rate"),
        conf = m.q("avg_record_confidence"),
        recorded = m.timestamp_unix_as("recorded_at", "recorded_at"),
    );
    let rows = sqlx::query(&format!(
        "SELECT {projection} FROM {rel} \
         WHERE ($1 = '' OR {date} >= $1::date) AND ($2 = '' OR {date} <= $2::date) \
         ORDER BY {date} DESC LIMIT $3",
        date = m.q("summary_date"),
    ))
    .bind(&req.date_from)
    .bind(&req.date_to)
    .bind(i64::from(MAX_ANALYTICS_READ_ROWS))
    .fetch_all(pool)
    .await
    .map_err(|err| {
        analytics_internal_status(
            "get_reconciliation_analytics",
            format!("get reconciliation analytics failed: {err}"),
        )
    })?;
    let summaries: Vec<_> = rows.iter().map(ras_from_row).collect();
    // Overall resolution rate = Σ exact_matches / Σ total_reconciliations;
    // avg_reconciliation_ms = mean of the per-day averages.
    let total_recon: i64 = summaries.iter().map(|s| s.total_reconciliations).sum();
    let total_exact: i64 = summaries.iter().map(|s| s.exact_matches).sum();
    let overall_resolution_rate = if total_recon > 0 {
        total_exact as f64 / total_recon as f64
    } else {
        0.0
    };
    let avg_reconciliation_ms = if summaries.is_empty() {
        0.0
    } else {
        summaries
            .iter()
            .map(|s| s.avg_reconciliation_ms)
            .sum::<f64>()
            / summaries.len() as f64
    };
    Ok(Response::new(ana_pb::GetReconciliationAnalyticsResponse {
        summaries,
        overall_resolution_rate,
        avg_reconciliation_ms,
    }))
}

pub(crate) async fn get_throughput(
    svc: &AnalyticsServiceImpl,
    request: Request<ana_pb::GetThroughputRequest>,
) -> Result<Response<ana_pb::GetThroughputResponse>, Status> {
    let metadata = request.metadata().clone();
    let mut req = request.into_inner();
    // B12: filter by the canonical tenant — the VALIDATED bearer/header tenant
    // (a UUID), the SAME value RecordPipelineMetric persists under. The raw
    // request-body `tenant_id` may be a human code (e.g. "sdk-live") that matches
    // zero stored rows, so SUM(total_requests) came back 0. Binding to the claim
    // tenant here makes the read scope match the write.
    if let Some(canonical) = metadata_tenant_id(&metadata) {
        req.tenant_id = canonical;
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "analytics",
        OperationChannel::Read,
        &req.tenant_id,
        None,
    )
    .await?;
    // B12: the typed `LogicalAggregate` dispatch returns 0 here at runtime even
    // though the identical raw SQL (and the typed READ over the same table+tenant
    // via GetPipelineSummary) returns the real sum — a defect in the typed
    // aggregate execution path that is NOT a tenant mismatch (RecordPipelineMetric,
    // GetPipelineSummary and this handler all key on the same canonical tenant).
    // Until that typed-aggregate bug is root-caused, serve the throughput SUM via
    // the SAME proven raw-SQL aggregate the windowed branch already uses below
    // (`COALESCE(SUM(..),0)::bigint`), so throughput reports real numbers. See
    // bug_report.md B12.
    let pool = svc.require_pool()?;
    let m = pms_model();
    let rel = m.relation.clone();
    // Transitional: timestamp-window casts stay on the configured Postgres
    // native store; the no-window aggregate path uses LogicalAggregate above.
    let row = sqlx::query(&format!(
        "SELECT COALESCE(AVG({rps}),0) AS avg_rps, \
                COALESCE(MAX({rps}),0) AS peak_rps, \
                COALESCE(SUM({total}),0)::bigint AS total_requests, \
                COALESCE(SUM({succ}),0)::bigint AS total_successful \
         FROM {rel} \
         WHERE ($1 = '' OR {tenant} = $1) \
           AND ($2 = '' OR {hour} >= $2::timestamptz) \
           AND ($3 = '' OR {hour} <= $3::timestamptz)",
        rps = m.q("throughput_rps"),
        total = m.q("total_requests"),
        succ = m.q("successful"),
        tenant = m.q("tenant_id"),
        hour = m.q("snapshot_hour"),
    ))
    .bind(&req.tenant_id)
    .bind(&req.hour_from)
    .bind(&req.hour_to)
    .fetch_one(pool)
    .await
    .map_err(|err| {
        analytics_internal_status("get_throughput", format!("get throughput failed: {err}"))
    })?;
    // Every selected column is COALESCE-ed, so it is always present and
    // non-NULL: a decode failure here means a genuine type/shape defect, never
    // an absent metric. Reporting 0 for it made a broken read indistinguishable
    // from a tenant with no traffic — the same silent-zero that left the typed
    // aggregate this raw SQL stands in for un-diagnosable from its symptom.
    // Surface the failure instead of swallowing it.
    let decode = |column: &str, err: sqlx::Error| {
        analytics_internal_status(
            "get_throughput",
            format!("get throughput decode of column '{column}' failed: {err}"),
        )
    };
    let total_requests: i64 = row
        .try_get("total_requests")
        .map_err(|err| decode("total_requests", err))?;
    let total_successful: i64 = row
        .try_get("total_successful")
        .map_err(|err| decode("total_successful", err))?;
    let avg_rps: f64 = row
        .try_get("avg_rps")
        .map_err(|err| decode("avg_rps", err))?;
    let peak_rps: f64 = row
        .try_get("peak_rps")
        .map_err(|err| decode("peak_rps", err))?;
    let overall_success_rate = if total_requests > 0 {
        total_successful as f64 / total_requests as f64
    } else {
        0.0
    };
    Ok(Response::new(ana_pb::GetThroughputResponse {
        avg_rps,
        peak_rps,
        total_requests,
        overall_success_rate,
    }))
}

pub(crate) async fn get_sla_compliance(
    svc: &AnalyticsServiceImpl,
    request: Request<ana_pb::GetSlaComplianceRequest>,
) -> Result<Response<ana_pb::GetSlaComplianceResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let p99_threshold = req.p99_threshold_ms;
    let err_threshold = req.error_rate_threshold;
    // The request body carries no tenant field; scope to the VERIFIED
    // claim/header tenant so a tenant caller never sees foreign-tenant or
    // NULL-tenant (system-wide aggregate) PMS rows.
    let tenant_id = metadata_tenant_id(&metadata).unwrap_or_default();
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "analytics",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    if req.date_from.trim().is_empty()
        && req.date_to.trim().is_empty()
        && let Some(runtime) = svc.runtime()
    {
        let context = tenant_only_native_service_context(&metadata, &tenant_id);
        let rows = runtime
            .native_entity_read_for_service(
                "analytics",
                &context,
                sla_compliance_read(&req, &tenant_id),
            )
            .await?;
        let mut entries = Vec::with_capacity(rows.len());
        let (mut p99_met, mut err_met) = (0i64, 0i64);
        for row in &rows {
            let snapshot = pms_from_json(row);
            let p99_sla_met = p99_threshold <= 0.0 || snapshot.p99_latency_ms <= p99_threshold;
            let error_rate_sla_met = err_threshold <= 0.0 || snapshot.error_rate <= err_threshold;
            if p99_sla_met {
                p99_met += 1;
            }
            if error_rate_sla_met {
                err_met += 1;
            }
            entries.push(ana_pb::SlaComplianceEntry {
                stage_name: snapshot.stage_name,
                period: timestamp_hour_period(snapshot.snapshot_hour.as_ref()),
                p99_latency_ms: snapshot.p99_latency_ms,
                error_rate: snapshot.error_rate,
                p99_sla_met,
                error_rate_sla_met,
            });
        }
        let n = entries.len() as f64;
        return Ok(Response::new(ana_pb::GetSlaComplianceResponse {
            overall_p99_compliance_rate: if n > 0.0 { p99_met as f64 / n } else { 0.0 },
            overall_error_rate_compliance_rate: if n > 0.0 { err_met as f64 / n } else { 0.0 },
            entries,
        }));
    }
    let pool = svc.require_pool()?;
    let m = pms_model();
    // Transitional: SLA periods need backend date formatting plus service-side
    // compliance rollup for date windows. The no-window row read uses native
    // entity dispatch above. The read runs in a transaction that first installs
    // the tenant RLS GUC (metering's read-path pattern) so the raw pooled
    // connection is row-scoped in addition to the explicit bound predicate.
    let mut tx = pool.begin().await.map_err(|err| {
        analytics_internal_status(
            "get_sla_compliance",
            format!("get sla compliance transaction failed: {err}"),
        )
    })?;
    if !tenant_id.trim().is_empty() {
        sqlx::query(install_analytics_tenant_scope_sql())
            .bind(&tenant_id)
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                analytics_internal_status(
                    "get_sla_compliance",
                    format!("get sla compliance tenant scope failed: {err}"),
                )
            })?;
    }
    let rows = sqlx::query(&sla_compliance_sql(&m))
        .bind(&req.stage_name)
        .bind(&req.date_from)
        .bind(&req.date_to)
        .bind(&tenant_id)
        .bind(i64::from(MAX_ANALYTICS_READ_ROWS))
        .fetch_all(&mut *tx)
        .await
        .map_err(|err| {
            analytics_internal_status(
                "get_sla_compliance",
                format!("get sla compliance failed: {err}"),
            )
        })?;
    tx.commit().await.map_err(|err| {
        analytics_internal_status(
            "get_sla_compliance",
            format!("get sla compliance transaction commit failed: {err}"),
        )
    })?;

    // A zero threshold means "no threshold configured" → treat as always met
    // so an unconfigured SLA doesn't report spurious violations.
    let mut entries = Vec::with_capacity(rows.len());
    let (mut p99_met, mut err_met) = (0i64, 0i64);
    for row in &rows {
        let p99: f64 = row.try_get("p99_latency_ms").unwrap_or(0.0);
        let error_rate: f64 = row.try_get("error_rate").unwrap_or(0.0);
        let p99_sla_met = p99_threshold <= 0.0 || p99 <= p99_threshold;
        let error_rate_sla_met = err_threshold <= 0.0 || error_rate <= err_threshold;
        if p99_sla_met {
            p99_met += 1;
        }
        if error_rate_sla_met {
            err_met += 1;
        }
        entries.push(ana_pb::SlaComplianceEntry {
            stage_name: row.try_get("stage_name").unwrap_or_default(),
            period: row.try_get("period").unwrap_or_default(),
            p99_latency_ms: p99,
            error_rate,
            p99_sla_met,
            error_rate_sla_met,
        });
    }
    let n = entries.len() as f64;
    Ok(Response::new(ana_pb::GetSlaComplianceResponse {
        overall_p99_compliance_rate: if n > 0.0 { p99_met as f64 / n } else { 0.0 },
        overall_error_rate_compliance_rate: if n > 0.0 { err_met as f64 / n } else { 0.0 },
        entries,
    }))
}

pub(crate) async fn trigger_snapshot(
    svc: &AnalyticsServiceImpl,
    request: Request<ana_pb::TriggerSnapshotRequest>,
) -> Result<Response<ana_pb::TriggerSnapshotResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    // Scope the pass to the VERIFIED claim/header tenant: a tenant caller
    // rolls up only its own PMS rows (never the NULL-tenant system-wide
    // aggregates or foreign tenants); the leader-elected worker runs the
    // same pass unscoped.
    let tenant_id = metadata_tenant_id(&metadata).unwrap_or_default();
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "analytics",
        OperationChannel::Write,
        &tenant_id,
        None,
    )
    .await?;
    let pool = svc.require_pool()?;
    // Counts/means are aggregated online (see module docs); the percentile
    // columns are what a snapshot pass genuinely has to write. Run the SAME
    // rollup the worker seam runs and report rows actually updated — not a
    // COUNT(*) of pre-existing rows.
    let written = run_analytics_rollup_scoped(pool, &tenant_id, &req.stage_name, &req.hour)
        .await
        .map_err(|status| {
            // Re-tag the rollup failure under this RPC's operation so the
            // served TriggerSnapshot path carries a `trigger_snapshot` typed
            // internal detail (the worker seam keeps `analytics_rollup`).
            analytics_internal_status(
                "trigger_snapshot",
                format!("trigger snapshot rollup failed: {}", status.message()),
            )
        })?;
    // Fulfil the proto-declared `method_event_contract` for this mutation.
    emit_analytics_event(
        svc,
        EVENT_TYPE_SNAPSHOT_TRIGGERED,
        &tenant_id,
        &metadata_project_id(&metadata).unwrap_or_default(),
        &req.stage_name,
        analytics_event_payload(
            EVENT_TYPE_SNAPSHOT_TRIGGERED,
            &req.stage_name,
            &tenant_id,
            Some(written as i64),
        ),
    )
    .await;
    Ok(Response::new(ana_pb::TriggerSnapshotResponse {
        snapshots_written: written as i32,
    }))
}
