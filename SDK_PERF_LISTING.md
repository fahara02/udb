# Cross-SDK per-service latency listing (all 4 SDKs, 262 RPCs each, localhost)

Mean ms per service (mean of per-RPC means). Source: `perf_report_{go,python,ts,php}.md`.
Numbers are localhost dev-box runs (loaded machine → ±, but the shape is consistent).

| Service | RPCs | Go | Python | TS | PHP |
|---|--:|--:|--:|--:|--:|
| DataBroker | 76 | 18.3 | 14.2 | 22.6 | 9.4 |
| AuthnService | 50 | 64.0 | 42.6 | 6.0 | 7.3 |
| AuthzService | 41 | 68.7 | 12.1 | 4.5 | 5.9 |
| IdentityProviderService | 27 | 7.2 | 9.8 | 3.4 | 4.1 |
| NotificationService | 11 | 9.9 | 109.1 | 4.1 | 9.5 |
| ApiKeyService | 9 | 9.1 | 12.5 | 4.3 | 9.8 |
| AssetService | 8 | 2.8 | 7.8 | 4.2 | 3.4 |
| AnalyticsService | 7 | 5.4 | 10.2 | 4.7 | 21.8 |
| StorageService | 7 | 2.8 | 11.5 | 3.9 | 2.8 |
| TenantService | 6 | 7.9 | 50.9 | 4.6 | 4.3 |
| ControlPlaneService | 5 | 38.5 | 6.2 | 0.9 | 11.9 |
| RoomService | 5 | 3.4 | 5.2 | 4.3 | 3.2 |
| PeerService | 4 | 3.0 | 3.9 | 4.9 | 3.4 |
| TrackService | 4 | 2.8 | 7.5 | 4.3 | 2.9 |
| TurnService | 1 | 2.8 | 5.7 | 4.5 | 3.7 |
| SignalingService | 1 | 0.0 | 4.5 | 11.4 | 0.2 |
