# UDB SDK Live Perf — TypeScript (localhost)

RPCs measured: 262

Unary = full request/response round-trip. Streaming rows (kind=stream_open) report stream-open latency (establish the stream, no response drain), NOT first-message latency.

## Per-service mean latency

| Service | RPCs | mean ms |
|---|--:|--:|
| DataBroker | 76 | 22.61 |
| SignalingService | 1 | 11.39 |
| AuthnService | 50 | 5.96 |
| PeerService | 4 | 4.86 |
| AnalyticsService | 7 | 4.70 |
| TenantService | 6 | 4.61 |
| AuthzService | 41 | 4.53 |
| TurnService | 1 | 4.53 |
| TrackService | 4 | 4.32 |
| ApiKeyService | 9 | 4.32 |
| RoomService | 5 | 4.32 |
| AssetService | 8 | 4.16 |
| NotificationService | 11 | 4.08 |
| StorageService | 7 | 3.87 |
| IdentityProviderService | 27 | 3.39 |
| ControlPlaneService | 5 | 0.94 |

## Slowest 20 by p99

| RPC | kind | p50 ms | p99 ms | mean ms |
|---|---|--:|--:|--:|
| DataBroker/get_catalog_manifest | read_only | 172.04 | 837.09 | 262.57 |
| DataBroker/resume_cdc | mutation | 290.10 | 349.30 | 283.69 |
| DataBroker/preview_cdc_redaction | read_only | 60.47 | 142.84 | 69.87 |
| DataBroker/reload_policies | destructive | 133.65 | 133.65 | 133.65 |
| DataBroker/get_health_report | read_only | 57.82 | 111.81 | 65.61 |
| DataBroker/plan_migration | mutation | 54.35 | 57.49 | 64.16 |
| DataBroker/step_down_cdc_leader | mutation | 51.71 | 54.36 | 57.25 |
| DataBroker/rollback_catalog | destructive | 50.61 | 50.61 | 50.61 |
| DataBroker/select | read_only | 12.45 | 47.33 | 18.13 |
| DataBroker/pause_cdc | mutation | 39.41 | 39.88 | 39.91 |
| DataBroker/get_admin_summary | read_only | 27.74 | 36.94 | 28.46 |
| DataBroker/list_admin_audit_logs | read_only | 11.18 | 34.36 | 13.83 |
| DataBroker/get_cdc_status | read_only | 12.72 | 32.36 | 14.21 |
| DataBroker/replay_dlq_event | mutation | 24.23 | 32.00 | 22.59 |
| DataBroker/graph_mutate | mutation | 21.79 | 30.62 | 25.61 |
| DataBroker/list_migration_runs | read_only | 19.64 | 29.54 | 20.29 |
| AuthnService/list_mfa_factors | read_only | 12.78 | 28.60 | 13.39 |
| DataBroker/get_catalog_version | read_only | 19.05 | 28.30 | 19.99 |
| DataBroker/vector_search | read_only | 11.93 | 27.47 | 14.38 |
| AuthnService/introspect_token | read_only | 9.87 | 25.75 | 11.90 |
