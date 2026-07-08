# Service-Level Objectives (per-RPC latency budgets)

This is the published, **enforced** form of the SLO catalog. It is the prose
mirror of one single source of truth in code: `slo_catalog()` in
[`src/runtime/slo.rs`](../src/runtime/slo.rs). Every objective below names the
real Prometheus metric (emitted by `crate::runtime::metrics`) that measures it;
the `slo_metrics_exist` unit test guards each metric against the live registry,
and a second staleness test (`slo_doc_table_matches_catalog`) pins the table
below byte-for-byte to `slo_catalog()`.

**Do not hand-edit the table between the markers.** The numbers are not
authored here — they are rendered from the catalog. If this block is stale the
staleness test fails; regenerate it by copying the rendered table emitted by
that test (run `cargo test -p udb slo_doc_table_matches_catalog`).

## How the budgets are enforced

- **Absolute gate.** `scripts/bench_gate.py --absolute docs/slo.md` parses the
  `Latency target` column out of the table below and fails (non-zero exit) when
  a measured latency in the latest `bench-history/` snapshot exceeds its
  per-objective budget. No threshold is hardcoded in the script: every budget is
  read from this generated block, which the staleness test pins to the catalog.
- **Relative gate.** `scripts/bench_gate.py --relative` complements it by
  failing on a regression versus the prior snapshot (or a named release tag).

## SLO catalog

<!-- BEGIN GENERATED:slo -->
| Objective | Operation | gRPC method | Latency target | Availability | Latency metric | Availability metric |
|---|---|---|---|---|---|---|
| `authn.login` | Interactive credential login (password/MFA exchange → tokens) | `Login` | p99 ≤ 400 ms | 99.9% | `udb_grpc_duration_seconds` | `udb_grpc_requests_total` |
| `authn.refresh` | Refresh-token rotation → new access token | `RefreshToken` | p99 ≤ 150 ms | 99.95% | `udb_grpc_duration_seconds` | `udb_grpc_requests_total` |
| `authn.validate` | Access-token validation (bearer introspection) | `ValidateToken` | p99 ≤ 50 ms | 99.95% | `udb_grpc_duration_seconds` | `udb_grpc_requests_total` |
| `authz.check` | Single authorization decision (Casbin enforce) | `Check` | p99 ≤ 25 ms | 99.99% | `udb_grpc_duration_seconds` | `udb_grpc_requests_total` |
| `authz.batch_check` | Batched authorization decisions | `BatchCheck` | p99 ≤ 75 ms | 99.95% | `udb_grpc_duration_seconds` | `udb_grpc_requests_total` |
| `storage.presign` | Presigned-URL mint for upload/download | `PresignObject` | p99 ≤ 100 ms | 99.9% | `udb_grpc_duration_seconds` | `udb_object_ops_total` |
| `storage.finalize` | Finalize a completed upload (commit object metadata) | `FinalizeUpload` | p99 ≤ 250 ms | 99.9% | `udb_grpc_duration_seconds` | `udb_object_ops_total` |
| `storage.list` | List objects under a prefix | `ListObjects` | p99 ≤ 300 ms | 99.9% | `udb_grpc_duration_seconds` | `udb_object_ops_total` |
| `asset.pipeline_start` | Start an asset-processing pipeline run | `StartPipeline` | p99 ≤ 200 ms | 99.9% | `udb_grpc_duration_seconds` | `udb_grpc_requests_total` |
| `asset.pipeline_step` | Execute one asset pipeline step (EMBED → vector upsert) | `ExecuteStep` | p95 ≤ 2000 ms | 99.5% | `udb_grpc_duration_seconds` | `udb_vector_ops_total` |
| `webrtc.signaling` | WebRTC signaling relay (offer/answer/ICE) | `Signal` | p99 ≤ 100 ms | 99.9% | `udb_grpc_duration_seconds` | `udb_grpc_requests_total` |
| `cdc.publish` | End-to-end CDC publish (outbox row created → Kafka ack) | — | p99 ≤ 1000 ms | 99.9% | `udb_cdc_publish_latency_seconds` | `udb_cdc_errors_total` |
| `cdc.dlq` | CDC dead-letter backlog stays drained | — | — | 99.9% | `udb_cdc_dlq_depth` | `udb_cdc_dlq_depth` |
| `policy.distribution_ack` | Control-plane policy ACK latency (invalidation emit → node apply) | — | p99 ≤ 5000 ms | 99.9% | `udb_authz_policy_invalidation_lag_seconds` | `udb_authz_policy_reload_seconds` |
<!-- END GENERATED:slo -->
