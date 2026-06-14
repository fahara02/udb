# UDB SDK Live Perf — Go (localhost)

RPCs measured: 262   tenant=b811961a-11e9-48d9-b3a9-f0d018a6d2ed

Unary RPCs = full request→response round-trip. Streaming RPCs (server/client/bidi) report STREAM-OPEN latency (initiate + send request + CloseSend), NOT first-message latency: a subscription stream's first message arrives only on an event, so draining it in a passive run would just hit the deadline. Streaming rows are marked in the note column.

## Per-service mean latency (mean of per-RPC means)

| Service | RPCs | mean |
|---|---:|---:|
| AuthnService | 50 | 26.081ms |
| ApiKeyService | 9 | 49.558ms |
| DataBroker | 76 | 5.779ms |
| AuthzService | 41 | 7.315ms |
| IdentityProviderService | 27 | 3.4ms |
| AnalyticsService | 7 | 11.146ms |
| NotificationService | 11 | 7.017ms |
| ControlPlaneService | 5 | 9.441ms |
| TenantService | 6 | 4.508ms |
| StorageService | 7 | 2.494ms |
| AssetService | 8 | 2.171ms |
| RoomService | 5 | 2.479ms |
| TrackService | 4 | 2.556ms |
| PeerService | 4 | 2.166ms |
| TurnService | 1 | 2.371ms |
| SignalingService | 1 | 0s |

## Slowest 25 RPCs by p99

| RPC | kind | p50 | p99 | mean | iters | note |
|---|---|---:|---:|---:|---:|---|
| AuthnService/Login | mutation | 550.213ms | 561.252ms | 552.541ms | 5 | mutation |
| AuthnService/CreateUser | mutation | 515.108ms | 534.099ms | 509.289ms | 5 | mutation (last code=Internal) |
| ApiKeyService/CreateApiKey | mutation | 343.884ms | 374.72ms | 320.079ms | 5 | mutation |
| ApiKeyService/ValidateApiKey | read_only | 36.499ms | 241.797ms | 82.116ms | 25 | read_only |
| DataBroker/GetHealthReport | read_only | 32.177ms | 113.375ms | 41.142ms | 25 | read_only |
| ControlPlaneService/ListNodeStates | read_only | 46.166ms | 70.587ms | 44.171ms | 25 | read_only |
| NotificationService/ListTemplates | read_only | 26.667ms | 41.241ms | 27.57ms | 25 | read_only |
| AnalyticsService/RecordPipelineMetric | mutation | 39.191ms | 40.101ms | 40.251ms | 5 | mutation |
| DataBroker/GetAdminSummary | read_only | 26.582ms | 34.522ms | 26.225ms | 25 | read_only |
| AuthzService/Authorize | read_only | 25.901ms | 34.158ms | 26.951ms | 25 | read_only |
| AuthzService/GetNativeAccess | read_only | 20.491ms | 32.835ms | 21.953ms | 25 | read_only |
| DataBroker/ApplyMigration | mutation | 26.901ms | 27.446ms | 27.325ms | 5 | mutation (last code=Internal) |
| AuthzService/PutRelationship | mutation | 26.142ms | 26.66ms | 25.835ms | 5 | mutation |
| ApiKeyService/GetApiKeyUsageStats | read_only | 14.467ms | 25.073ms | 14.682ms | 25 | read_only |
| AuthzService/CheckAccess | read_only | 14.622ms | 22.447ms | 15.875ms | 25 | read_only |
| IdentityProviderService/ListProviders | read_only | 10.499ms | 22.432ms | 12.247ms | 25 | read_only |
| DataBroker/EnsureProject | mutation | 21.243ms | 22.169ms | 20.496ms | 5 | mutation |
| AuthzService/ListAccessDecisionAudits | read_only | 10.915ms | 21.806ms | 11.958ms | 25 | read_only |
| DataBroker/GetCdcStatus | read_only | 7.591ms | 21.609ms | 8.732ms | 25 | read_only |
| NotificationService/GetDeliveryStats | read_only | 6.954ms | 20.146ms | 11.223ms | 25 | read_only (last code=Internal) |
| AuthzService/PutRoleBinding | mutation | 18.849ms | 19.971ms | 18.789ms | 5 | mutation |
| ApiKeyService/ListApiKeys | read_only | 5.718ms | 19.734ms | 6.881ms | 25 | read_only |
| DataBroker/PauseCdc | mutation | 18.472ms | 19.209ms | 17.21ms | 5 | mutation |
| DataBroker/GetCatalogVersions | read_only | 6.584ms | 18.867ms | 9.313ms | 25 | read_only |
| IdentityProviderService/ListExternalIdentities | read_only | 11.49ms | 17.941ms | 11.934ms | 25 | read_only |

## Full per-RPC table (sorted by service, then name)

| Service | RPC | kind | p50 | p99 | mean | min | max | iters |
|---|---|---|---:|---:|---:|---:|---:|---:|
| AnalyticsService | GetExecutorPerformance | read_only | 5.849ms | 12.967ms | 7.167ms | 2.691ms | 13.36ms | 25 |
| AnalyticsService | GetPipelineSummary | read_only | 4.833ms | 13.387ms | 7.086ms | 3.531ms | 13.966ms | 25 |
| AnalyticsService | GetReconciliationAnalytics | read_only | 4.674ms | 10.63ms | 6.19ms | 3.206ms | 13.047ms | 25 |
| AnalyticsService | GetSlaCompliance | read_only | 4.222ms | 10.563ms | 5.82ms | 2.039ms | 12.339ms | 25 |
| AnalyticsService | GetThroughput | read_only | 5.388ms | 7.396ms | 5.532ms | 850µs | 15.259ms | 25 |
| AnalyticsService | RecordPipelineMetric | mutation | 39.191ms | 40.101ms | 40.251ms | 37.832ms | 45.223ms | 5 |
| AnalyticsService | TriggerSnapshot | mutation | 6.141ms | 6.538ms | 5.973ms | 4.78ms | 6.765ms | 5 |
| ApiKeyService | CreateApiKey | mutation | 343.884ms | 374.72ms | 320.079ms | 86.244ms | 667.131ms | 5 |
| ApiKeyService | EmergencyRevokeApiKeys | destructive | 513µs | 513µs | 513µs | 513µs | 513µs | 1 |
| ApiKeyService | GetApiKey | read_only | 6.085ms | 13.108ms | 7.345ms | 4.204ms | 18.793ms | 25 |
| ApiKeyService | GetApiKeyUsageStats | read_only | 14.467ms | 25.073ms | 14.682ms | 5.533ms | 28.642ms | 25 |
| ApiKeyService | ListApiKeys | read_only | 5.718ms | 19.734ms | 6.881ms | 4.22ms | 19.77ms | 25 |
| ApiKeyService | RevokeApiKey | mutation | 4.668ms | 5.254ms | 5.49ms | 3.417ms | 10.363ms | 5 |
| ApiKeyService | RotateApiKey | mutation | 4.795ms | 4.825ms | 4.612ms | 3.763ms | 5.239ms | 5 |
| ApiKeyService | UpdateApiKey | mutation | 4.609ms | 4.752ms | 4.308ms | 3.541ms | 4.891ms | 5 |
| ApiKeyService | ValidateApiKey | read_only | 36.499ms | 241.797ms | 82.116ms | 13.478ms | 262.309ms | 25 |
| AssetService | CompleteStep | mutation | 2.689ms | 2.785ms | 2.603ms | 2.126ms | 2.994ms | 5 |
| AssetService | CreatePipelineDefinition | mutation | 2.223ms | 2.328ms | 2.017ms | 1.162ms | 2.672ms | 5 |
| AssetService | GetAsset | read_only | 2.114ms | 2.846ms | 2.007ms | 1.042ms | 2.88ms | 25 |
| AssetService | GetPipeline | read_only | 1.861ms | 2.661ms | 1.816ms | 572µs | 2.709ms | 25 |
| AssetService | GetPipelineDefinition | read_only | 2.158ms | 2.863ms | 2.126ms | 1.062ms | 3.23ms | 25 |
| AssetService | ListAssets | read_only | 2.112ms | 2.823ms | 1.991ms | 908µs | 2.974ms | 25 |
| AssetService | RegisterAsset | mutation | 2.116ms | 2.412ms | 2.213ms | 1.615ms | 3.3ms | 5 |
| AssetService | StartPipeline | mutation | 2.868ms | 3.008ms | 2.594ms | 1.513ms | 3.066ms | 5 |
| AuthnService | AdminResetMfa | destructive | 11.021ms | 11.021ms | 11.021ms | 11.021ms | 11.021ms | 1 |
| AuthnService | AdminResetPassword | destructive | 9.909ms | 9.909ms | 9.909ms | 9.909ms | 9.909ms | 1 |
| AuthnService | AdminRevokeAllTenantSessions | destructive | 2.113ms | 2.113ms | 2.113ms | 2.113ms | 2.113ms | 1 |
| AuthnService | AdminRevokeAllUserSessions | destructive | 1.852ms | 1.852ms | 1.852ms | 1.852ms | 1.852ms | 1 |
| AuthnService | AdminRevokeSession | destructive | 2.092ms | 2.092ms | 2.092ms | 2.092ms | 2.092ms | 1 |
| AuthnService | Authenticate | read_only | 4.264ms | 10.211ms | 5.125ms | 2.722ms | 16.509ms | 25 |
| AuthnService | ChangePassword | mutation | 1.639ms | 1.671ms | 1.582ms | 1.166ms | 2.211ms | 5 |
| AuthnService | ChangeUserStatus | destructive | 2.146ms | 2.146ms | 2.146ms | 2.146ms | 2.146ms | 1 |
| AuthnService | ConfirmMFAEnrollment | mutation | 5.382ms | 6.848ms | 5.482ms | 3.884ms | 6.862ms | 5 |
| AuthnService | CreateSession | mutation | 6.597ms | 6.869ms | 6.205ms | 4.894ms | 7.365ms | 5 |
| AuthnService | CreateUser | mutation | 515.108ms | 534.099ms | 509.289ms | 456.2ms | 540.653ms | 5 |
| AuthnService | DeleteWebAuthnCredential | mutation | 6.997ms | 7.5ms | 7.056ms | 6.102ms | 8.412ms | 5 |
| AuthnService | DisableMfaFactor | mutation | 4.021ms | 4.713ms | 4.169ms | 3.28ms | 4.844ms | 5 |
| AuthnService | EmergencyRevoke | destructive | 1.054ms | 1.054ms | 1.054ms | 1.054ms | 1.054ms | 1 |
| AuthnService | EnrollMFA | mutation | 3.359ms | 3.715ms | 3.595ms | 2.644ms | 5.089ms | 5 |
| AuthnService | FinishWebAuthnAuthentication | mutation | 1.048ms | 1.076ms | 1.162ms | 1.042ms | 1.602ms | 5 |
| AuthnService | FinishWebAuthnRegistration | mutation | 1.045ms | 1.065ms | 1.269ms | 1.033ms | 2.159ms | 5 |
| AuthnService | ForgotPassword | mutation | 16.629ms | 17.436ms | 17.203ms | 15.583ms | 19.857ms | 5 |
| AuthnService | GenerateRecoveryCodes | mutation | 3.712ms | 3.888ms | 4.054ms | 3.572ms | 5.402ms | 5 |
| AuthnService | GetJwks | read_only | 4.866ms | 6.639ms | 4.77ms | 3.175ms | 6.861ms | 25 |
| AuthnService | GetMfaPolicy | read_only | 4.556ms | 5.79ms | 4.485ms | 2.65ms | 6.621ms | 25 |
| AuthnService | GetSession | read_only | 5.056ms | 17.518ms | 7.685ms | 2.579ms | 19.289ms | 25 |
| AuthnService | GetUser | read_only | 3.754ms | 4.239ms | 3.678ms | 2.65ms | 4.652ms | 25 |
| AuthnService | IntrospectToken | read_only | 1.569ms | 1.701ms | 1.404ms | 1.05ms | 2.167ms | 25 |
| AuthnService | IssueMfaChallenge | mutation | 3.172ms | 3.201ms | 3.22ms | 2.719ms | 4.094ms | 5 |
| AuthnService | ListDevices | read_only | 4.241ms | 6.877ms | 4.474ms | 506µs | 7.761ms | 25 |
| AuthnService | ListMfaFactors | read_only | 3.83ms | 5.007ms | 4.039ms | 3.171ms | 5.431ms | 25 |
| AuthnService | ListSessions | read_only | 8.164ms | 17.106ms | 8.824ms | 2.458ms | 22.852ms | 25 |
| AuthnService | ListUsers | read_only | 7.28ms | 9.003ms | 7.13ms | 5.061ms | 10.868ms | 25 |
| AuthnService | ListWebAuthnCredentials | read_only | 3.756ms | 6.018ms | 3.807ms | 2.3ms | 6.122ms | 25 |
| AuthnService | Login | mutation | 550.213ms | 561.252ms | 552.541ms | 495.718ms | 612.095ms | 5 |
| AuthnService | Logout | mutation | 6.932ms | 8.029ms | 6.785ms | 4.876ms | 8.123ms | 5 |
| AuthnService | PutMfaPolicy | mutation | 6.596ms | 6.944ms | 6.617ms | 6.066ms | 7.068ms | 5 |
| AuthnService | RefreshSession | mutation | 4.858ms | 4.918ms | 5.017ms | 4.422ms | 6.216ms | 5 |
| AuthnService | RefreshToken | mutation | 4.419ms | 5.511ms | 5.654ms | 3.678ms | 10.453ms | 5 |
| AuthnService | RenamePasskey | mutation | 5.874ms | 5.936ms | 5.742ms | 4.909ms | 6.528ms | 5 |
| AuthnService | ResendOTP | mutation | 6.239ms | 6.658ms | 6.118ms | 4.247ms | 7.464ms | 5 |
| AuthnService | ResetPassword | mutation | 1.638ms | 1.764ms | 1.52ms | 1.048ms | 2.087ms | 5 |
| AuthnService | RevokeDevice | mutation | 12.278ms | 15.34ms | 12.984ms | 6.453ms | 23.171ms | 5 |
| AuthnService | RevokeRecoveryCodes | mutation | 5.314ms | 5.627ms | 5.767ms | 4.267ms | 8.332ms | 5 |
| AuthnService | RevokeSession | mutation | 5.926ms | 6.34ms | 6.256ms | 5.299ms | 8.102ms | 5 |
| AuthnService | SendOTP | mutation | 4.824ms | 4.957ms | 4.565ms | 3.817ms | 5.096ms | 5 |
| AuthnService | SendPhoneVerification | mutation | 4.913ms | 4.931ms | 4.617ms | 3.777ms | 5.302ms | 5 |
| AuthnService | StartWebAuthnAuthentication | mutation | 1.147ms | 2.233ms | 1.734ms | 1.038ms | 3.213ms | 5 |
| AuthnService | StartWebAuthnRegistration | mutation | 1.761ms | 1.989ms | 1.836ms | 1.568ms | 2.179ms | 5 |
| AuthnService | UpdateUser | mutation | 5.38ms | 5.478ms | 5.729ms | 5.227ms | 7.264ms | 5 |
| AuthnService | ValidateCSRF | read_only | 5.669ms | 6.969ms | 5.612ms | 2.845ms | 6.999ms | 25 |
| AuthnService | ValidateToken | read_only | 2.156ms | 2.688ms | 2.089ms | 1.347ms | 2.733ms | 25 |
| AuthnService | VerifyMfaChallenge | read_only | 6.598ms | 9.058ms | 6.888ms | 5.477ms | 9.27ms | 25 |
| AuthnService | VerifyOTP | read_only | 5.986ms | 7.833ms | 6.07ms | 4.412ms | 8.529ms | 25 |
| AuthzService | ActivateCanary | destructive | 2.711ms | 2.711ms | 2.711ms | 2.711ms | 2.711ms | 1 |
| AuthzService | ActivatePolicyVersion | destructive | 2.696ms | 2.696ms | 2.696ms | 2.696ms | 2.696ms | 1 |
| AuthzService | ApprovePolicyDraft | mutation | 12.702ms | 14.039ms | 13.682ms | 9.797ms | 19.68ms | 5 |
| AuthzService | AssignRole | mutation | 2.441ms | 2.518ms | 2.445ms | 2.037ms | 3.107ms | 5 |
| AuthzService | Authorize | read_only | 25.901ms | 34.158ms | 26.951ms | 20.302ms | 36.568ms | 25 |
| AuthzService | BatchCheckPermissions | read_only | 2.717ms | 4.242ms | 2.742ms | 1.037ms | 4.503ms | 25 |
| AuthzService | CheckAccess | read_only | 14.622ms | 22.447ms | 15.875ms | 11.846ms | 26.636ms | 25 |
| AuthzService | CreatePolicyDraft | mutation | 12.68ms | 17.619ms | 17.265ms | 10.458ms | 34.571ms | 5 |
| AuthzService | CreatePolicyRule | mutation | 2.201ms | 2.237ms | 2.227ms | 2.036ms | 2.479ms | 5 |
| AuthzService | CreateRole | mutation | 2.348ms | 2.871ms | 2.547ms | 2.073ms | 3.193ms | 5 |
| AuthzService | DeletePolicyRule | mutation | 2.996ms | 3.555ms | 2.972ms | 2.307ms | 3.611ms | 5 |
| AuthzService | DeleteRole | mutation | 2.252ms | 2.554ms | 2.368ms | 2.12ms | 2.717ms | 5 |
| AuthzService | DiffPolicyDraft | read_only | 10.307ms | 13.997ms | 11.269ms | 8.744ms | 14.353ms | 25 |
| AuthzService | ExplainPolicy | read_only | 10.286ms | 15.135ms | 11.041ms | 7.759ms | 18.728ms | 25 |
| AuthzService | GetAuthzRevision | read_only | 5.437ms | 8.636ms | 6.104ms | 4.079ms | 10.283ms | 25 |
| AuthzService | GetCanaryStatus | read_only | 9.31ms | 13.81ms | 9.728ms | 7.436ms | 13.886ms | 25 |
| AuthzService | GetNativeAccess | read_only | 20.491ms | 32.835ms | 21.953ms | 17.184ms | 44.219ms | 25 |
| AuthzService | GetPolicyBundle | read_only | 9.65ms | 12.419ms | 10.047ms | 7.516ms | 14.299ms | 25 |
| AuthzService | GetPolicyRule | read_only | 1.709ms | 3.247ms | 1.851ms | 508µs | 3.951ms | 25 |
| AuthzService | GetRole | read_only | 1.929ms | 2.543ms | 1.926ms | 1.518ms | 2.67ms | 25 |
| AuthzService | InvalidatePolicyBundles | destructive | 1.52ms | 1.52ms | 1.52ms | 1.52ms | 1.52ms | 1 |
| AuthzService | LintAuthzPolicies | read_only | 1.837ms | 2.745ms | 1.944ms | 509µs | 3.525ms | 25 |
| AuthzService | ListAccessDecisionAudits | read_only | 10.915ms | 21.806ms | 11.958ms | 6.783ms | 22.701ms | 25 |
| AuthzService | ListPolicyRules | read_only | 5.048ms | 7.734ms | 5.405ms | 3.246ms | 9.154ms | 25 |
| AuthzService | ListPolicyVersions | read_only | 7.118ms | 8.497ms | 7.015ms | 5.31ms | 8.755ms | 25 |
| AuthzService | ListRoles | read_only | 4.758ms | 6.636ms | 4.966ms | 1.822ms | 9.04ms | 25 |
| AuthzService | ListUserPermissions | read_only | 1.63ms | 2.752ms | 1.813ms | 1.055ms | 3.243ms | 25 |
| AuthzService | ListUserRoles | read_only | 1.607ms | 2.669ms | 1.808ms | 1.055ms | 3.282ms | 25 |
| AuthzService | MigrateLegacyPolicies | destructive | 2.112ms | 2.112ms | 2.112ms | 2.112ms | 2.112ms | 1 |
| AuthzService | PromoteCanary | destructive | 2.201ms | 2.201ms | 2.201ms | 2.201ms | 2.201ms | 1 |
| AuthzService | PutAuthzPolicy | mutation | 2.165ms | 2.179ms | 1.98ms | 1.627ms | 2.275ms | 5 |
| AuthzService | PutRelationship | mutation | 26.142ms | 26.66ms | 25.835ms | 23.942ms | 27.519ms | 5 |
| AuthzService | PutRoleBinding | mutation | 18.849ms | 19.971ms | 18.789ms | 16.145ms | 21.835ms | 5 |
| AuthzService | RejectPolicyDraft | mutation | 7.29ms | 7.996ms | 7.188ms | 5.907ms | 8.08ms | 5 |
| AuthzService | RevokeRole | mutation | 1.416ms | 1.573ms | 1.469ms | 1.05ms | 2.117ms | 5 |
| AuthzService | RollbackPolicyVersion | destructive | 2.732ms | 2.732ms | 2.732ms | 2.732ms | 2.732ms | 1 |
| AuthzService | SeedBuiltinRoles | mutation | 7.498ms | 8.096ms | 7.572ms | 6.649ms | 8.463ms | 5 |
| AuthzService | SimulatePolicy | mutation | 7.805ms | 7.841ms | 7.789ms | 7.259ms | 8.711ms | 5 |
| AuthzService | SubmitPolicyDraft | mutation | 8.847ms | 8.872ms | 8.877ms | 8.09ms | 10.249ms | 5 |
| AuthzService | UpdatePolicyDraft | mutation | 6.968ms | 7.998ms | 7.158ms | 6.106ms | 8.495ms | 5 |
| AuthzService | UpdateRole | mutation | 1.481ms | 1.562ms | 1.393ms | 1.027ms | 1.805ms | 5 |
| ControlPlaneService | AckStatus | mutation | 1.57ms | 1.67ms | 1.503ms | 1.099ms | 2.042ms | 5 |
| ControlPlaneService | DeltaResources | mutation | 0s | 0s | 0s | 0s | 0s | 5 |
| ControlPlaneService | GetResources | read_only | 1.495ms | 2.363ms | 1.528ms | 509µs | 2.71ms | 25 |
| ControlPlaneService | ListNodeStates | read_only | 46.166ms | 70.587ms | 44.171ms | 27.977ms | 84.525ms | 25 |
| ControlPlaneService | StreamResources | mutation | 0s | 0s | 0s | 0s | 0s | 5 |
| DataBroker | ActivateCatalog | destructive | 8.221ms | 8.221ms | 8.221ms | 8.221ms | 8.221ms | 1 |
| DataBroker | AnalyticalQuery | read_only | 2.9ms | 3.806ms | 3.113ms | 2.124ms | 5.983ms | 25 |
| DataBroker | ApplyMigration | mutation | 26.901ms | 27.446ms | 27.325ms | 22.268ms | 33.149ms | 5 |
| DataBroker | ApproveMigrationPlan | mutation | 15.665ms | 16.096ms | 13.15ms | 2.64ms | 17.158ms | 5 |
| DataBroker | BatchSelect | mutation | 0s | 0s | 0s | 0s | 0s | 5 |
| DataBroker | BatchUpsert | mutation | 0s | 0s | 111µs | 0s | 555µs | 5 |
| DataBroker | BeginTx | mutation | 0s | 0s | 0s | 0s | 0s | 5 |
| DataBroker | CacheDelete | mutation | 2.675ms | 2.92ms | 2.898ms | 2.103ms | 4.136ms | 5 |
| DataBroker | CacheGet | read_only | 2.842ms | 4.248ms | 2.902ms | 509µs | 4.347ms | 25 |
| DataBroker | CacheScan | read_only | 2.67ms | 3.702ms | 2.865ms | 1.975ms | 3.716ms | 25 |
| DataBroker | CacheSet | mutation | 2.721ms | 2.966ms | 2.849ms | 2.683ms | 3.172ms | 5 |
| DataBroker | CreateMaterializedView | mutation | 2.189ms | 2.632ms | 2.349ms | 2.117ms | 2.645ms | 5 |
| DataBroker | Delete | mutation | 3.211ms | 3.296ms | 3.393ms | 3.119ms | 4.144ms | 5 |
| DataBroker | DeletePolicy | mutation | 7.808ms | 7.864ms | 7.532ms | 5.86ms | 9.124ms | 5 |
| DataBroker | DismissDlqEvent | mutation | 2.683ms | 3.198ms | 2.752ms | 2.117ms | 3.199ms | 5 |
| DataBroker | DocumentDelete | mutation | 2.699ms | 2.708ms | 2.733ms | 2.117ms | 3.527ms | 5 |
| DataBroker | DocumentFind | read_only | 2.686ms | 3.335ms | 2.787ms | 2.047ms | 4.794ms | 25 |
| DataBroker | DocumentGet | read_only | 3.174ms | 3.672ms | 3.034ms | 2.215ms | 4.585ms | 25 |
| DataBroker | DocumentUpsert | mutation | 2.89ms | 3.077ms | 2.929ms | 2.1ms | 3.735ms | 5 |
| DataBroker | DropResource | destructive | 2.632ms | 2.632ms | 2.632ms | 2.632ms | 2.632ms | 1 |
| DataBroker | EnqueueOutboxEvent | mutation | 7.823ms | 8.472ms | 7.846ms | 6.91ms | 8.781ms | 5 |
| DataBroker | EnsureProject | mutation | 21.243ms | 22.169ms | 20.496ms | 15.031ms | 24.541ms | 5 |
| DataBroker | EnsureResource | mutation | 2.529ms | 2.569ms | 2.501ms | 2.107ms | 3.16ms | 5 |
| DataBroker | GeneratePresignedUrl | mutation | 2.524ms | 2.625ms | 2.479ms | 2.127ms | 2.659ms | 5 |
| DataBroker | GenericDispatch | mutation | 3.201ms | 3.393ms | 2.995ms | 2.133ms | 3.911ms | 5 |
| DataBroker | GetAdminSummary | read_only | 26.582ms | 34.522ms | 26.225ms | 19.502ms | 39.017ms | 25 |
| DataBroker | GetCapabilities | read_only | 7.836ms | 10.425ms | 8.006ms | 5.317ms | 13.122ms | 25 |
| DataBroker | GetCatalogManifest | read_only | 10.038ms | 15.985ms | 10.682ms | 8.739ms | 16.665ms | 25 |
| DataBroker | GetCatalogVersion | read_only | 5.196ms | 6.655ms | 5.373ms | 4.492ms | 6.656ms | 25 |
| DataBroker | GetCatalogVersions | read_only | 6.584ms | 18.867ms | 9.313ms | 4.477ms | 37.954ms | 25 |
| DataBroker | GetCdcStatus | read_only | 7.591ms | 21.609ms | 8.732ms | 4.662ms | 23ms | 25 |
| DataBroker | GetDlqEvent | read_only | 4.129ms | 5.988ms | 4.209ms | 2.279ms | 6.659ms | 25 |
| DataBroker | GetHealthReport | read_only | 32.177ms | 113.375ms | 41.142ms | 22.843ms | 140.502ms | 25 |
| DataBroker | GetMigrationStatus | read_only | 3.491ms | 6.156ms | 3.78ms | 2.843ms | 7.066ms | 25 |
| DataBroker | GetObject | read_only | 0s | 0s | 27µs | 0s | 684µs | 25 |
| DataBroker | GetSaga | read_only | 2.726ms | 4.554ms | 3.046ms | 2.023ms | 4.781ms | 25 |
| DataBroker | GraphMutate | mutation | 3.337ms | 4.469ms | 3.768ms | 2.682ms | 5.356ms | 5 |
| DataBroker | GraphQuery | read_only | 2.95ms | 4.534ms | 3.074ms | 2.196ms | 4.539ms | 25 |
| DataBroker | InitiateMultipartUpload | mutation | 5.26ms | 5.619ms | 5.327ms | 3.86ms | 7.663ms | 5 |
| DataBroker | LintPolicies | read_only | 7.086ms | 10.931ms | 7.504ms | 5.021ms | 13.056ms | 25 |
| DataBroker | ListAdminAuditLogs | read_only | 7.601ms | 11.58ms | 8.151ms | 5.49ms | 12.356ms | 25 |
| DataBroker | ListDlqEvents | read_only | 3.322ms | 5.472ms | 3.52ms | 1.967ms | 6.542ms | 25 |
| DataBroker | ListMessageSchemas | read_only | 3.805ms | 4.79ms | 3.729ms | 2.707ms | 4.88ms | 25 |
| DataBroker | ListMigrationRuns | read_only | 6.901ms | 10.61ms | 6.938ms | 5.133ms | 11.197ms | 25 |
| DataBroker | ListPolicies | read_only | 7.35ms | 12.842ms | 8.342ms | 4.884ms | 13.008ms | 25 |
| DataBroker | ListProjects | read_only | 5.947ms | 9.393ms | 6.467ms | 3.868ms | 11.799ms | 25 |
| DataBroker | ListResources | read_only | 2.161ms | 3.085ms | 2.295ms | 1.6ms | 4.125ms | 25 |
| DataBroker | ListSagas | read_only | 2.167ms | 2.817ms | 2.251ms | 1.582ms | 3.035ms | 25 |
| DataBroker | LookupMessageSchema | read_only | 2.912ms | 4.508ms | 3.18ms | 1.639ms | 6.119ms | 25 |
| DataBroker | MarkSagaReviewed | mutation | 3.764ms | 4.356ms | 4.264ms | 3.661ms | 5.799ms | 5 |
| DataBroker | PauseCdc | mutation | 18.472ms | 19.209ms | 17.21ms | 12.062ms | 20.974ms | 5 |
| DataBroker | PlanMigration | mutation | 8.647ms | 9.044ms | 8.933ms | 8.125ms | 10.272ms | 5 |
| DataBroker | PreviewCdcRedaction | read_only | 9.571ms | 12.525ms | 9.974ms | 7.673ms | 16.589ms | 25 |
| DataBroker | PublishCDC | mutation | 0s | 0s | 0s | 0s | 0s | 5 |
| DataBroker | PutObject | mutation | 0s | 0s | 0s | 0s | 0s | 5 |
| DataBroker | PutPolicy | destructive | 4.425ms | 4.425ms | 4.425ms | 4.425ms | 4.425ms | 1 |
| DataBroker | QuarantineDlqEvent | mutation | 1.593ms | 2.115ms | 1.867ms | 1.542ms | 2.545ms | 5 |
| DataBroker | ReloadPolicies | destructive | 12.001ms | 12.001ms | 12.001ms | 12.001ms | 12.001ms | 1 |
| DataBroker | ReplayDlqEvent | mutation | 2.525ms | 2.62ms | 2.395ms | 1.589ms | 3.231ms | 5 |
| DataBroker | ResumeCdc | mutation | 11.638ms | 12.44ms | 11.825ms | 10.545ms | 13.071ms | 5 |
| DataBroker | RetrySagaCompensation | mutation | 1.582ms | 1.841ms | 1.836ms | 1.568ms | 2.622ms | 5 |
| DataBroker | RollbackCatalog | destructive | 3.974ms | 3.974ms | 3.974ms | 3.974ms | 3.974ms | 1 |
| DataBroker | ScanProjectionDrift | read_only | 2.278ms | 3.293ms | 2.396ms | 504µs | 3.734ms | 25 |
| DataBroker | Select | read_only | 2.146ms | 3.797ms | 2.444ms | 1.549ms | 6.405ms | 25 |
| DataBroker | SelectV2 | read_only | 0s | 0s | 21µs | 0s | 522µs | 25 |
| DataBroker | StageCatalog | destructive | 3.094ms | 3.094ms | 3.094ms | 3.094ms | 3.094ms | 1 |
| DataBroker | StepDownCdcLeader | mutation | 11.876ms | 13.489ms | 12.617ms | 10.993ms | 15.644ms | 5 |
| DataBroker | TimeSeriesQuery | read_only | 2.639ms | 7.575ms | 2.98ms | 1.602ms | 8.565ms | 25 |
| DataBroker | TimeSeriesWrite | mutation | 2.129ms | 2.233ms | 2.214ms | 1.591ms | 3.499ms | 5 |
| DataBroker | Upsert | mutation | 2.171ms | 2.269ms | 2.161ms | 1.608ms | 2.633ms | 5 |
| DataBroker | ValidateCatalog | destructive | 1.591ms | 1.591ms | 1.591ms | 1.591ms | 1.591ms | 1 |
| DataBroker | VectorBatchUpsert | mutation | 0s | 0s | 0s | 0s | 0s | 5 |
| DataBroker | VectorHybridSearch | read_only | 2.522ms | 3.376ms | 2.465ms | 1.566ms | 3.407ms | 25 |
| DataBroker | VectorSearch | read_only | 2.148ms | 3.893ms | 2.349ms | 1.51ms | 4.892ms | 25 |
| DataBroker | VectorUpsert | mutation | 2.165ms | 2.664ms | 2.33ms | 1.714ms | 3.004ms | 5 |
| DataBroker | VerifyAdminAuditLog | read_only | 7.997ms | 12.692ms | 8.894ms | 5.65ms | 12.699ms | 25 |
| IdentityProviderService | CreateProvider | mutation | 3.019ms | 3.176ms | 2.817ms | 2.155ms | 3.352ms | 5 |
| IdentityProviderService | DisableProvider | mutation | 3.299ms | 3.392ms | 3.318ms | 2.658ms | 4.529ms | 5 |
| IdentityProviderService | ForceJwksRefresh | mutation | 5.294ms | 5.307ms | 5.457ms | 3.625ms | 8.709ms | 5 |
| IdentityProviderService | GetProvider | read_only | 2.904ms | 5.067ms | 3.272ms | 2.125ms | 7.5ms | 25 |
| IdentityProviderService | ImportSamlMetadata | mutation | 2.433ms | 2.651ms | 2.219ms | 354µs | 4.542ms | 5 |
| IdentityProviderService | LinkIdentity | mutation | 3.252ms | 3.349ms | 2.923ms | 2.121ms | 3.545ms | 5 |
| IdentityProviderService | ListExternalIdentities | read_only | 11.49ms | 17.941ms | 11.934ms | 6.593ms | 18.988ms | 25 |
| IdentityProviderService | ListProviders | read_only | 10.499ms | 22.432ms | 12.247ms | 6.242ms | 31.765ms | 25 |
| IdentityProviderService | PreviewClaimMapping | read_only | 2.44ms | 4.459ms | 2.642ms | 1.607ms | 4.93ms | 25 |
| IdentityProviderService | PreviewGroupMapping | read_only | 2.627ms | 3.644ms | 2.71ms | 1.583ms | 5.211ms | 25 |
| IdentityProviderService | ResolveExternalIdentity | mutation | 2.727ms | 2.882ms | 2.619ms | 2.191ms | 3.104ms | 5 |
| IdentityProviderService | SamlAcs | mutation | 2.825ms | 3.271ms | 2.697ms | 1.688ms | 3.763ms | 5 |
| IdentityProviderService | ScimCreateGroup | mutation | 2.719ms | 2.806ms | 2.718ms | 2.422ms | 3.121ms | 5 |
| IdentityProviderService | ScimCreateUser | mutation | 2.215ms | 2.352ms | 2.326ms | 2.166ms | 2.721ms | 5 |
| IdentityProviderService | ScimDeleteGroup | mutation | 2.128ms | 2.764ms | 2.524ms | 1.585ms | 4.484ms | 5 |
| IdentityProviderService | ScimDeleteUser | mutation | 2.753ms | 3.249ms | 2.775ms | 2.146ms | 3.534ms | 5 |
| IdentityProviderService | ScimGetGroup | mutation | 2.177ms | 2.639ms | 2.424ms | 2.165ms | 2.968ms | 5 |
| IdentityProviderService | ScimGetUser | mutation | 2.356ms | 3.377ms | 2.759ms | 2.291ms | 3.422ms | 5 |
| IdentityProviderService | ScimListGroups | mutation | 2.783ms | 3.356ms | 3.005ms | 2.12ms | 4.042ms | 5 |
| IdentityProviderService | ScimListUsers | mutation | 2.622ms | 2.625ms | 2.498ms | 2.121ms | 2.919ms | 5 |
| IdentityProviderService | ScimPatchGroup | mutation | 2.469ms | 2.749ms | 2.588ms | 2.132ms | 3.445ms | 5 |
| IdentityProviderService | ScimPatchUser | mutation | 2.373ms | 2.43ms | 2.183ms | 747µs | 3.517ms | 5 |
| IdentityProviderService | ScimReplaceUser | mutation | 2.345ms | 2.58ms | 2.25ms | 1.625ms | 2.692ms | 5 |
| IdentityProviderService | StartSamlLogin | mutation | 2.175ms | 2.207ms | 2.15ms | 1.627ms | 3.102ms | 5 |
| IdentityProviderService | TestProviderDiscovery | read_only | 2.617ms | 3.093ms | 2.448ms | 1.513ms | 3.421ms | 25 |
| IdentityProviderService | UnlinkIdentity | mutation | 2.124ms | 2.604ms | 2.128ms | 1.581ms | 2.716ms | 5 |
| IdentityProviderService | UpdateProvider | mutation | 2.159ms | 2.257ms | 2.165ms | 1.919ms | 2.354ms | 5 |
| NotificationService | GetDeliveryStats | read_only | 6.954ms | 20.146ms | 11.223ms | 4.894ms | 20.766ms | 25 |
| NotificationService | GetNotification | read_only | 2.142ms | 3.405ms | 2.059ms | 513µs | 3.895ms | 25 |
| NotificationService | GetPreference | read_only | 2.663ms | 3.969ms | 2.646ms | 1.607ms | 4.045ms | 25 |
| NotificationService | GetTemplate | read_only | 10.43ms | 12.939ms | 10.343ms | 7.784ms | 12.977ms | 25 |
| NotificationService | ListNotifications | read_only | 6.565ms | 7.854ms | 6.63ms | 2.122ms | 11.382ms | 25 |
| NotificationService | ListPreferences | read_only | 2.676ms | 3.831ms | 2.691ms | 1.598ms | 4.089ms | 25 |
| NotificationService | ListTemplates | read_only | 26.667ms | 41.241ms | 27.57ms | 16.185ms | 41.971ms | 25 |
| NotificationService | RetryNotification | mutation | 2.647ms | 3.181ms | 2.719ms | 2.141ms | 3.44ms | 5 |
| NotificationService | SendNotification | mutation | 2.641ms | 2.718ms | 2.498ms | 1.878ms | 2.876ms | 5 |
| NotificationService | SetPreference | mutation | 2.714ms | 2.921ms | 2.854ms | 2.143ms | 4.02ms | 5 |
| NotificationService | UpsertTemplate | mutation | 5.898ms | 5.942ms | 5.958ms | 5.056ms | 7.212ms | 5 |
| PeerService | GetPeer | read_only | 2.153ms | 3.691ms | 2.494ms | 1.471ms | 4.446ms | 25 |
| PeerService | JoinRoom | mutation | 1.596ms | 2.103ms | 1.798ms | 1.582ms | 2.12ms | 5 |
| PeerService | LeaveRoom | mutation | 2.129ms | 2.332ms | 2.104ms | 1.606ms | 2.383ms | 5 |
| PeerService | ListPeers | read_only | 2.104ms | 3.383ms | 2.268ms | 1.576ms | 3.695ms | 25 |
| RoomService | CloseRoom | mutation | 2.198ms | 2.689ms | 2.491ms | 2.15ms | 3.255ms | 5 |
| RoomService | CreateRoom | mutation | 2.691ms | 3.192ms | 2.915ms | 2.124ms | 3.94ms | 5 |
| RoomService | GetRoom | read_only | 2.588ms | 3.23ms | 2.528ms | 1.57ms | 3.686ms | 25 |
| RoomService | ListRooms | read_only | 2.42ms | 3.7ms | 2.47ms | 1.393ms | 3.98ms | 25 |
| RoomService | UpdateRoom | mutation | 1.991ms | 2.138ms | 1.992ms | 1.594ms | 2.641ms | 5 |
| SignalingService | Signal | mutation | 0s | 0s | 0s | 0s | 0s | 5 |
| StorageService | DeleteFile | mutation | 2.462ms | 2.65ms | 2.48ms | 2.186ms | 2.74ms | 5 |
| StorageService | FinalizeUpload | mutation | 2.572ms | 2.669ms | 2.492ms | 2.145ms | 2.867ms | 5 |
| StorageService | GetDownloadUrl | read_only | 2.645ms | 3.698ms | 2.615ms | 1.594ms | 4.611ms | 25 |
| StorageService | GetFile | read_only | 2.696ms | 3.201ms | 2.567ms | 1.52ms | 3.265ms | 25 |
| StorageService | ListFiles | read_only | 2.245ms | 3.12ms | 2.282ms | 1.57ms | 3.66ms | 25 |
| StorageService | RegisterUpload | mutation | 2.696ms | 3.173ms | 2.615ms | 1.588ms | 3.31ms | 5 |
| StorageService | UpdateFile | mutation | 2.427ms | 2.517ms | 2.41ms | 1.641ms | 3.126ms | 5 |
| TenantService | CreateTenant | mutation | 2.178ms | 2.215ms | 2.271ms | 2.14ms | 2.675ms | 5 |
| TenantService | GetTenant | read_only | 9.475ms | 12.128ms | 9.641ms | 6.301ms | 12.646ms | 25 |
| TenantService | GetTenantConfig | read_only | 7.852ms | 10.356ms | 8.209ms | 6.699ms | 10.606ms | 25 |
| TenantService | ListTenants | read_only | 1.625ms | 3.184ms | 2.02ms | 1.057ms | 5.405ms | 25 |
| TenantService | UpdateTenant | mutation | 2.13ms | 2.482ms | 2.254ms | 1.606ms | 3.235ms | 5 |
| TenantService | UpdateTenantConfig | mutation | 2.649ms | 3.053ms | 2.652ms | 2.135ms | 3.272ms | 5 |
| TrackService | ListTracks | read_only | 2.426ms | 3.595ms | 2.596ms | 1.449ms | 8.246ms | 25 |
| TrackService | MuteTrack | mutation | 2.23ms | 2.382ms | 2.476ms | 1.973ms | 3.759ms | 5 |
| TrackService | PublishTrack | mutation | 2.586ms | 2.955ms | 3.005ms | 1.795ms | 5.892ms | 5 |
| TrackService | UnpublishTrack | mutation | 2.203ms | 2.639ms | 2.148ms | 1.46ms | 2.833ms | 5 |
| TurnService | IssueCredentials | mutation | 2.56ms | 2.644ms | 2.371ms | 1.414ms | 2.836ms | 5 |
