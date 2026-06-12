# UDB SDK Live Perf — PHP (Docker → host)

RPCs measured: 262

Unary = full request/response round-trip. Streaming rows (kind=stream_open) report stream-open latency (initiate + cancel, no response drain), NOT first-message latency.

## Per-service mean latency

| Service | RPCs | mean ms |
|---|--:|--:|
| AnalyticsService | 7 | 21.78 |
| ControlPlaneService | 5 | 11.94 |
| ApiKeyService | 9 | 9.75 |
| NotificationService | 11 | 9.46 |
| DataBroker | 76 | 9.39 |
| AuthnService | 50 | 7.34 |
| AuthzService | 41 | 5.94 |
| TenantService | 6 | 4.26 |
| IdentityProviderService | 27 | 4.07 |
| TurnService | 1 | 3.72 |
| AssetService | 8 | 3.43 |
| PeerService | 4 | 3.43 |
| RoomService | 5 | 3.24 |
| TrackService | 4 | 2.94 |
| StorageService | 7 | 2.76 |
| SignalingService | 1 | 0.16 |

## Slowest 20 by p99

| RPC | kind | p50 ms | p99 ms | mean ms |
|---|---|--:|--:|--:|
| DataBroker/GetCatalogManifest | mutation | 202.68 | 211.26 | 204.91 |
| AnalyticsService/GetPipelineSummary | mutation | 56.97 | 67.43 | 59.26 |
| AuthzService/ListAccessDecisionAudits | mutation | 53.68 | 56.57 | 51.59 |
| NotificationService/ListTemplates | mutation | 40.40 | 43.61 | 40.27 |
| AuthzService/Authorize | mutation | 42.79 | 43.08 | 43.49 |
| ControlPlaneService/ListNodeStates | mutation | 39.97 | 40.61 | 49.80 |
| ApiKeyService/ValidateApiKey | mutation | 18.82 | 35.90 | 24.46 |
| DataBroker/ListAdminAuditLogs | mutation | 26.37 | 28.39 | 29.04 |
| DataBroker/GetHealthReport | mutation | 22.88 | 27.96 | 37.99 |
| AnalyticsService/GetSlaCompliance | mutation | 23.31 | 27.20 | 24.74 |
| DataBroker/GetAdminSummary | mutation | 23.60 | 26.61 | 24.04 |
| AuthnService/ListUsers | mutation | 24.04 | 25.43 | 23.34 |
| AnalyticsService/GetExecutorPerformance | mutation | 22.70 | 23.98 | 22.25 |
| DataBroker/PlanMigration | mutation | 22.29 | 23.44 | 31.65 |
| DataBroker/VerifyAdminAuditLog | mutation | 20.42 | 21.89 | 20.29 |
| NotificationService/GetTemplate | mutation | 20.21 | 21.79 | 20.48 |
| AnalyticsService/GetReconciliationAnalytics | mutation | 20.44 | 21.68 | 20.45 |
| DataBroker/PauseCdc | mutation | 19.31 | 20.16 | 20.20 |
| DataBroker/StepDownCdcLeader | mutation | 16.60 | 19.22 | 18.05 |
| DataBroker/ListMigrationRuns | mutation | 15.35 | 16.75 | 15.42 |
