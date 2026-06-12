# UDB SDK Live Perf — TypeScript (localhost)

RPCs measured: 251

## Per-service mean latency

| Service | RPCs | mean ms |
|---|--:|--:|
| DataBroker | 68 | 12.93 |
| StorageService | 7 | 5.07 |
| TurnService | 1 | 4.43 |
| TrackService | 4 | 4.39 |
| RoomService | 5 | 4.29 |
| TenantService | 6 | 4.14 |
| AuthnService | 50 | 4.07 |
| AssetService | 8 | 3.99 |
| PeerService | 4 | 3.87 |
| AuthzService | 41 | 3.70 |
| AnalyticsService | 7 | 3.63 |
| IdentityProviderService | 27 | 3.62 |
| NotificationService | 11 | 3.59 |
| ApiKeyService | 9 | 3.41 |
| ControlPlaneService | 3 | 1.64 |

## Slowest 20 by p99

| RPC | kind | p50 ms | p99 ms | mean ms |
|---|---|--:|--:|--:|
| DataBroker/get_catalog_manifest | read_only | 169.28 | 203.93 | 172.66 |
| DataBroker/preview_cdc_redaction | read_only | 16.21 | 100.74 | 28.17 |
| DataBroker/reload_policies | destructive | 70.59 | 70.59 | 70.59 |
| DataBroker/resume_cdc | mutation | 32.64 | 41.20 | 34.12 |
| DataBroker/get_health_report | read_only | 19.41 | 33.18 | 21.03 |
| DataBroker/put_policy | destructive | 32.06 | 32.06 | 32.06 |
| DataBroker/step_down_cdc_leader | mutation | 27.39 | 30.64 | 29.23 |
| DataBroker/get_admin_summary | read_only | 20.86 | 25.26 | 21.26 |
| DataBroker/replay_dlq_event | mutation | 22.14 | 24.65 | 22.19 |
| DataBroker/quarantine_dlq_event | mutation | 16.24 | 22.62 | 19.98 |
| DataBroker/approve_migration_plan | mutation | 18.42 | 22.12 | 14.65 |
| DataBroker/plan_migration | mutation | 20.95 | 21.57 | 20.87 |
| DataBroker/pause_cdc | mutation | 16.73 | 18.44 | 17.41 |
| DataBroker/verify_admin_audit_log | read_only | 10.28 | 17.27 | 11.01 |
| DataBroker/ensure_project | mutation | 14.97 | 17.24 | 16.03 |
| DataBroker/get_capabilities | read_only | 9.35 | 15.79 | 10.39 |
| NotificationService/get_template | read_only | 4.93 | 13.24 | 6.26 |
| DataBroker/list_admin_audit_logs | read_only | 8.69 | 12.93 | 9.25 |
| DataBroker/rollback_catalog | destructive | 12.92 | 12.92 | 12.92 |
| DataBroker/list_projects | read_only | 9.75 | 12.61 | 9.89 |
