# UDB SDK Live Perf — Python (localhost)

RPCs measured: 262

## Per-service mean latency

| Service | RPCs | mean ms |
|---|--:|--:|
| DataBroker | 76 | 272.41 |
| AuthnService | 50 | 41.77 |
| ControlPlaneService | 5 | 22.94 |
| AuthzService | 41 | 11.31 |
| NotificationService | 11 | 9.39 |
| ApiKeyService | 9 | 8.44 |
| TenantService | 6 | 7.66 |
| AnalyticsService | 7 | 6.46 |
| TrackService | 4 | 4.02 |
| TurnService | 1 | 3.88 |
| StorageService | 7 | 3.78 |
| RoomService | 5 | 3.59 |
| SignalingService | 1 | 3.41 |
| IdentityProviderService | 27 | 3.31 |
| PeerService | 4 | 3.23 |
| AssetService | 8 | 3.21 |

## Slowest 20 by p99

| RPC | kind | p50 ms | p99 ms | mean ms |
|---|---|--:|--:|--:|
| DataBroker/PublishCDC | mutation | 20008.12 | 20009.42 | 20009.65 |
| AuthnService/Login | mutation | 886.18 | 891.99 | 892.76 |
| AuthnService/CreateUser | mutation | 771.63 | 831.12 | 801.29 |
| DataBroker/GetCatalogManifest | read_only | 179.73 | 193.62 | 175.31 |
| AuthzService/CheckAccess | read_only | 52.63 | 85.80 | 56.08 |
| AuthzService/Authorize | read_only | 64.39 | 82.58 | 65.19 |
| AuthnService/RenamePasskey | mutation | 65.51 | 67.01 | 68.05 |
| AuthzService/GetNativeAccess | read_only | 32.68 | 62.94 | 36.32 |
| AuthzService/PutRelationship | mutation | 53.45 | 56.27 | 58.25 |
| NotificationService/ListTemplates | read_only | 34.31 | 53.49 | 37.31 |
| ControlPlaneService/StreamResources | mutation | 48.17 | 52.22 | 50.15 |
| ControlPlaneService/DeltaResources | mutation | 44.48 | 47.07 | 47.02 |
| AuthzService/PutRoleBinding | mutation | 42.50 | 44.53 | 42.48 |
| AuthnService/DeleteWebAuthnCredential | mutation | 39.03 | 39.88 | 38.10 |
| DataBroker/GetHealthReport | read_only | 16.23 | 30.34 | 18.62 |
| ApiKeyService/CreateApiKey | mutation | 27.94 | 29.48 | 27.60 |
| TenantService/GetTenant | read_only | 14.91 | 28.47 | 16.39 |
| DataBroker/ApplyMigration | mutation | 27.48 | 28.31 | 22.46 |
| DataBroker/PauseCdc | mutation | 20.79 | 27.99 | 24.05 |
| AuthnService/VerifyMfaChallenge | read_only | 15.25 | 27.65 | 18.13 |
