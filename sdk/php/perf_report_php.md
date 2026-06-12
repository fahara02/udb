# UDB SDK Live Perf — PHP (Docker → host)

RPCs measured: 262

## Per-service mean latency

| Service | RPCs | mean ms |
|---|--:|--:|
| DataBroker | 76 | 274.01 |
| AnalyticsService | 7 | 15.92 |
| ControlPlaneService | 5 | 12.45 |
| RoomService | 5 | 11.54 |
| NotificationService | 11 | 8.23 |
| SignalingService | 1 | 7.83 |
| TurnService | 1 | 6.77 |
| TrackService | 4 | 5.87 |
| AuthzService | 41 | 4.68 |
| TenantService | 6 | 4.35 |
| ApiKeyService | 9 | 4.00 |
| AuthnService | 50 | 3.86 |
| IdentityProviderService | 27 | 3.65 |
| PeerService | 4 | 3.25 |
| StorageService | 7 | 3.01 |
| AssetService | 8 | 2.45 |

## Slowest 20 by p99

| RPC | kind | p50 ms | p99 ms | mean ms |
|---|---|--:|--:|--:|
| DataBroker/PublishCDC | mutation | 20002.33 | 20003.02 | 20001.90 |
| DataBroker/GetCatalogManifest | mutation | 115.91 | 125.51 | 121.75 |
| DataBroker/ResumeCdc | mutation | 25.54 | 54.97 | 38.17 |
| ControlPlaneService/ListNodeStates | mutation | 42.51 | 43.02 | 43.44 |
| AnalyticsService/GetPipelineSummary | mutation | 41.94 | 42.43 | 42.98 |
| NotificationService/ListTemplates | mutation | 36.51 | 42.27 | 39.09 |
| DataBroker/DeletePolicy | mutation | 27.64 | 41.16 | 30.51 |
| AuthzService/ListAccessDecisionAudits | mutation | 38.62 | 38.79 | 36.99 |
| DataBroker/StepDownCdcLeader | mutation | 34.72 | 34.87 | 32.68 |
| NotificationService/GetTemplate | mutation | 17.59 | 31.11 | 21.98 |
| DataBroker/PlanMigration | mutation | 21.83 | 30.87 | 26.44 |
| DataBroker/GetAdminSummary | mutation | 22.96 | 27.68 | 24.93 |
| AuthzService/Authorize | mutation | 24.73 | 25.03 | 24.48 |
| DataBroker/ReloadPolicies | destructive | 22.29 | 22.29 | 22.29 |
| DataBroker/BatchUpsert | read_only | 7.70 | 21.96 | 11.34 |
| DataBroker/GetHealthReport | mutation | 19.95 | 20.86 | 19.31 |
| DataBroker/ListMigrationRuns | mutation | 19.87 | 20.84 | 19.89 |
| AnalyticsService/GetReconciliationAnalytics | mutation | 19.58 | 20.17 | 19.25 |
| AnalyticsService/GetExecutorPerformance | mutation | 18.14 | 19.11 | 17.59 |
| DataBroker/GenericDispatch | mutation | 8.20 | 18.02 | 14.24 |
