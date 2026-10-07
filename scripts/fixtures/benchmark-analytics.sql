-- Disposable input records for the two read-only, ETL-fed analytics RPCs.
-- Run only after reset.sh recreates the isolated udb_ci database. These are
-- stored fixture observations, not claimed measurements of a live deployment.
INSERT INTO udb_analytics.executor_performance_summaries
    (summary_date, executor_identity, workload_kind, total_dispatches,
     successful_results, timeout_count, error_count, avg_execution_ms,
     p99_execution_ms, avg_confidence, success_rate, avg_capacity_utilisation)
VALUES ('2026-06-10', 'sdk-benchmark-executor', 'sdk-benchmark', 10,
        8, 1, 1, 100, 180, 0.9, 0.8, 0.5);

INSERT INTO udb_analytics.reconciliation_analytics_summaries
    (summary_date, total_reconciliations, exact_matches, partial_conflicts,
     hard_conflicts, low_confidence_flagged, avg_reconciliation_ms,
     resolution_rate, avg_record_confidence)
VALUES ('2026-06-10', 10, 8, 1, 1, 1, 25, 0.8, 0.9);
