# UDB SDK Live Perf — Python (localhost)

RPCs measured: 262

Unary = full request/response round-trip. Streaming rows (kind=stream_open) report stream-open latency (initiate + push request, no response drain), NOT first-message latency — a subscription stream emits only on events.

## Per-service mean latency

| Service | RPCs | mean ms |
|---|--:|--:|
| NotificationService | 11 | 109.11 |
| TenantService | 6 | 50.89 |
| AuthnService | 50 | 42.58 |
| DataBroker | 76 | 14.15 |
| ApiKeyService | 9 | 12.51 |
| AuthzService | 41 | 12.06 |
| StorageService | 7 | 11.50 |
| AnalyticsService | 7 | 10.22 |
| IdentityProviderService | 27 | 9.85 |
| AssetService | 8 | 7.85 |
| TrackService | 4 | 7.53 |
| ControlPlaneService | 5 | 6.22 |
| TurnService | 1 | 5.71 |
| RoomService | 5 | 5.18 |
| SignalingService | 1 | 4.45 |
| PeerService | 4 | 3.90 |

## Slowest 20 by p99

| RPC | kind | p50 ms | p99 ms | mean ms |
|---|---|--:|--:|--:|
| AuthnService/Login | mutation | 868.74 | 876.39 | 896.99 |
| AuthnService/CreateUser | mutation | 740.05 | 826.36 | 788.18 |
| NotificationService/GetTemplate | read_only | 382.11 | 754.64 | 398.44 |
| NotificationService/ListTemplates | read_only | 385.30 | 521.17 | 385.18 |
| TenantService/GetTenant | read_only | 187.35 | 294.21 | 204.93 |
| NotificationService/ListNotifications | read_only | 226.00 | 293.61 | 230.32 |
| DataBroker/GetCatalogManifest | read_only | 179.91 | 211.40 | 182.55 |
| DataBroker/GetAdminSummary | read_only | 92.92 | 201.47 | 107.14 |
| TenantService/GetTenantConfig | read_only | 55.93 | 166.79 | 68.05 |
| AuthzService/Authorize | read_only | 46.78 | 162.81 | 59.91 |
| DataBroker/GetHealthReport | read_only | 67.80 | 127.42 | 91.30 |
| AuthzService/PutRoleBinding | mutation | 68.09 | 74.32 | 65.20 |
| NotificationService/GetDeliveryStats | read_only | 33.48 | 68.24 | 39.25 |
| NotificationService/UpsertTemplate | mutation | 64.08 | 67.69 | 65.26 |
| AuthzService/PutRelationship | mutation | 50.36 | 62.04 | 54.42 |
| ApiKeyService/GetApiKeyUsageStats | read_only | 11.99 | 58.55 | 21.94 |
| DataBroker/LookupMessageSchema | read_only | 8.47 | 55.45 | 17.40 |
| DataBroker/EnsureProject | mutation | 47.69 | 54.94 | 50.20 |
| ApiKeyService/CreateApiKey | mutation | 43.62 | 53.40 | 47.40 |
| DataBroker/ListMessageSchemas | read_only | 16.85 | 51.19 | 22.06 |
