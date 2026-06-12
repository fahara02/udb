# UDB SDK Live Perf — Go (localhost)

RPCs measured: 262   tenant=e6eaed2c-67e0-40b7-9842-53598e248dcb

Unary RPCs = full request→response round-trip. Streaming RPCs (server/client/bidi) report STREAM-OPEN latency (initiate + send request + CloseSend), NOT first-message latency: a subscription stream's first message arrives only on an event, so draining it in a passive run would just hit the deadline. Streaming rows are marked in the note column.

## Per-service mean latency (mean of per-RPC means)

| Service | RPCs | mean |
|---|---:|---:|
| AuthnService | 50 | 63.956ms |
| AuthzService | 41 | 68.699ms |
| DataBroker | 76 | 18.257ms |
| IdentityProviderService | 27 | 7.232ms |
| ControlPlaneService | 5 | 38.507ms |
| NotificationService | 11 | 9.864ms |
| ApiKeyService | 9 | 9.101ms |
| TenantService | 6 | 7.888ms |
| AnalyticsService | 7 | 5.417ms |
| AssetService | 8 | 2.797ms |
| StorageService | 7 | 2.817ms |
| RoomService | 5 | 3.405ms |
| PeerService | 4 | 3.044ms |
| TrackService | 4 | 2.792ms |
| TurnService | 1 | 2.753ms |
| SignalingService | 1 | 0s |

## Slowest 25 RPCs by p99

| RPC | kind | p50 | p99 | mean | iters | note |
|---|---|---:|---:|---:|---:|---|
| AuthnService/Login | mutation | 2.201375s | 3.080443s | 2.072356s | 5 | mutation |
| DataBroker/GetCatalogManifest | read_only | 204.557ms | 730.204ms | 364.936ms | 25 | read_only |
| AuthzService/PutRelationship | mutation | 642.283ms | 716.879ms | 646.829ms | 5 | mutation |
| AuthnService/CreateUser | mutation | 675.745ms | 689.631ms | 674.937ms | 5 | mutation (last code=Internal) |
| AuthzService/PutRoleBinding | mutation | 557.707ms | 583.211ms | 526.098ms | 5 | mutation |
| DataBroker/GetHealthReport | read_only | 47.52ms | 468.004ms | 143.082ms | 25 | read_only |
| ControlPlaneService/ListNodeStates | read_only | 100.359ms | 424.953ms | 171.84ms | 25 | read_only |
| AuthzService/Authorize | read_only | 112.81ms | 203.707ms | 117.782ms | 25 | read_only |
| AuthzService/GetNativeAccess | read_only | 70.629ms | 197.393ms | 87.728ms | 25 | read_only |
| AuthzService/GetCanaryStatus | read_only | 100.991ms | 168.108ms | 122.921ms | 25 | read_only (last code=PermissionDenied) |
| AuthzService/ExplainPolicy | read_only | 109.275ms | 167.231ms | 96.474ms | 25 | read_only (last code=PermissionDenied) |
| DataBroker/GetCatalogVersions | read_only | 83.322ms | 156.057ms | 94.287ms | 25 | read_only |
| DataBroker/GetCdcStatus | read_only | 56.832ms | 144.538ms | 87.809ms | 25 | read_only |
| AuthzService/DiffPolicyDraft | read_only | 19.83ms | 142.993ms | 38.113ms | 25 | read_only (last code=PermissionDenied) |
| AuthzService/RejectPolicyDraft | mutation | 102.516ms | 142.184ms | 115.533ms | 5 | mutation (last code=PermissionDenied) |
| AuthzService/ListPolicyVersions | read_only | 86.619ms | 131.916ms | 98.198ms | 25 | read_only (last code=PermissionDenied) |
| AuthzService/CheckAccess | read_only | 77.106ms | 129.899ms | 83.912ms | 25 | read_only |
| AuthzService/ListPolicyRules | read_only | 59.023ms | 124.658ms | 65.204ms | 25 | read_only |
| DataBroker/GetAdminSummary | read_only | 73.485ms | 123.036ms | 74.455ms | 25 | read_only |
| AuthzService/SeedBuiltinRoles | mutation | 116.932ms | 118.849ms | 115.115ms | 5 | mutation (last code=PermissionDenied) |
| AuthzService/SubmitPolicyDraft | mutation | 99.725ms | 110.362ms | 99.005ms | 5 | mutation (last code=PermissionDenied) |
| AuthzService/UpdatePolicyDraft | mutation | 79.947ms | 91.102ms | 83.542ms | 5 | mutation (last code=PermissionDenied) |
| AuthzService/GetAuthzRevision | read_only | 49.125ms | 90.941ms | 50.987ms | 25 | read_only |
| AuthzService/SimulatePolicy | mutation | 87.728ms | 90.649ms | 86.063ms | 5 | mutation (last code=PermissionDenied) |
| DataBroker/GetCatalogVersion | read_only | 66.695ms | 79.918ms | 67.706ms | 25 | read_only (last code=NotFound) |

## Full per-RPC table (sorted by service, then name)

| Service | RPC | kind | p50 | p99 | mean | min | max | iters |
|---|---|---|---:|---:|---:|---:|---:|---:|
| AnalyticsService | GetExecutorPerformance | read_only | 4.863ms | 9.84ms | 5.741ms | 3.648ms | 10.398ms | 25 |
| AnalyticsService | GetPipelineSummary | read_only | 4.241ms | 10.162ms | 5.329ms | 2.809ms | 10.942ms | 25 |
| AnalyticsService | GetReconciliationAnalytics | read_only | 4.898ms | 8.604ms | 5.66ms | 3.474ms | 9.186ms | 25 |
| AnalyticsService | GetSlaCompliance | read_only | 5.066ms | 6.981ms | 5.667ms | 4.106ms | 14.34ms | 25 |
| AnalyticsService | GetThroughput | read_only | 4.257ms | 5.842ms | 4.364ms | 3.152ms | 6.554ms | 25 |
| AnalyticsService | RecordPipelineMetric | mutation | 7.028ms | 7.517ms | 6.952ms | 5.383ms | 7.809ms | 5 |
| AnalyticsService | TriggerSnapshot | mutation | 4.241ms | 4.8ms | 4.206ms | 3.138ms | 5.06ms | 5 |
| ApiKeyService | CreateApiKey | mutation | 26.961ms | 27.971ms | 27.607ms | 23.078ms | 34.207ms | 5 |
| ApiKeyService | EmergencyRevokeApiKeys | destructive | 2.074ms | 2.074ms | 2.074ms | 2.074ms | 2.074ms | 1 |
| ApiKeyService | GetApiKey | read_only | 5.317ms | 8.041ms | 5.814ms | 3.702ms | 10.014ms | 25 |
| ApiKeyService | GetApiKeyUsageStats | read_only | 8.473ms | 20.473ms | 10.52ms | 5.361ms | 21.935ms | 25 |
| ApiKeyService | ListApiKeys | read_only | 8.096ms | 13.612ms | 8.96ms | 5.956ms | 18.504ms | 25 |
| ApiKeyService | RevokeApiKey | mutation | 4.963ms | 5.819ms | 5.252ms | 4.658ms | 6.08ms | 5 |
| ApiKeyService | RotateApiKey | mutation | 4.486ms | 4.656ms | 4.191ms | 3.169ms | 4.859ms | 5 |
| ApiKeyService | UpdateApiKey | mutation | 4.57ms | 4.604ms | 4.634ms | 3.203ms | 6.236ms | 5 |
| ApiKeyService | ValidateApiKey | read_only | 12.149ms | 19.45ms | 12.854ms | 9.194ms | 19.65ms | 25 |
| AssetService | CompleteStep | mutation | 2.852ms | 3.174ms | 3.036ms | 2.102ms | 4.34ms | 5 |
| AssetService | CreatePipelineDefinition | mutation | 2.774ms | 2.812ms | 2.752ms | 2.64ms | 2.837ms | 5 |
| AssetService | GetAsset | read_only | 3.673ms | 5.776ms | 3.685ms | 2.041ms | 7.173ms | 25 |
| AssetService | GetPipeline | read_only | 2.691ms | 3.521ms | 2.71ms | 1.769ms | 3.603ms | 25 |
| AssetService | GetPipelineDefinition | read_only | 2.454ms | 3.86ms | 2.582ms | 1.578ms | 4.973ms | 25 |
| AssetService | ListAssets | read_only | 2.596ms | 3.167ms | 2.512ms | 1.951ms | 3.264ms | 25 |
| AssetService | RegisterAsset | mutation | 2.106ms | 2.72ms | 2.348ms | 1.631ms | 3.181ms | 5 |
| AssetService | StartPipeline | mutation | 2.679ms | 3.163ms | 2.755ms | 2.092ms | 3.253ms | 5 |
| AuthnService | AdminResetMfa | destructive | 3.676ms | 3.676ms | 3.676ms | 3.676ms | 3.676ms | 1 |
| AuthnService | AdminResetPassword | destructive | 4.517ms | 4.517ms | 4.517ms | 4.517ms | 4.517ms | 1 |
| AuthnService | AdminRevokeAllTenantSessions | destructive | 1.591ms | 1.591ms | 1.591ms | 1.591ms | 1.591ms | 1 |
| AuthnService | AdminRevokeAllUserSessions | destructive | 2.476ms | 2.476ms | 2.476ms | 2.476ms | 2.476ms | 1 |
| AuthnService | AdminRevokeSession | destructive | 1.646ms | 1.646ms | 1.646ms | 1.646ms | 1.646ms | 1 |
| AuthnService | Authenticate | read_only | 5.346ms | 10.089ms | 5.825ms | 2.641ms | 10.206ms | 25 |
| AuthnService | ChangePassword | mutation | 2.357ms | 2.752ms | 2.506ms | 1.601ms | 3.618ms | 5 |
| AuthnService | ChangeUserStatus | destructive | 1.987ms | 1.987ms | 1.987ms | 1.987ms | 1.987ms | 1 |
| AuthnService | ConfirmMFAEnrollment | mutation | 6.24ms | 6.302ms | 6.027ms | 4.558ms | 7.192ms | 5 |
| AuthnService | CreateSession | mutation | 4.923ms | 4.966ms | 5.003ms | 3.468ms | 6.775ms | 5 |
| AuthnService | CreateUser | mutation | 675.745ms | 689.631ms | 674.937ms | 616.749ms | 721.627ms | 5 |
| AuthnService | DeleteWebAuthnCredential | mutation | 10.88ms | 12.171ms | 11.295ms | 9.214ms | 14.264ms | 5 |
| AuthnService | DisableMfaFactor | mutation | 3.699ms | 4.256ms | 3.893ms | 3.551ms | 4.26ms | 5 |
| AuthnService | EmergencyRevoke | destructive | 2.124ms | 2.124ms | 2.124ms | 2.124ms | 2.124ms | 1 |
| AuthnService | EnrollMFA | mutation | 3.229ms | 3.457ms | 3.712ms | 3.15ms | 5.56ms | 5 |
| AuthnService | FinishWebAuthnAuthentication | mutation | 1.06ms | 1.064ms | 969µs | 603µs | 1.065ms | 5 |
| AuthnService | FinishWebAuthnRegistration | mutation | 1.624ms | 1.674ms | 1.576ms | 1.081ms | 1.93ms | 5 |
| AuthnService | ForgotPassword | mutation | 18.072ms | 19.28ms | 18.094ms | 16.742ms | 19.476ms | 5 |
| AuthnService | GenerateRecoveryCodes | mutation | 4.344ms | 4.973ms | 4.423ms | 3.269ms | 6.134ms | 5 |
| AuthnService | GetJwks | read_only | 3.663ms | 5.877ms | 4.155ms | 2.647ms | 6.35ms | 25 |
| AuthnService | GetMfaPolicy | read_only | 4.5ms | 6.111ms | 4.612ms | 2.523ms | 8.122ms | 25 |
| AuthnService | GetSession | read_only | 4.852ms | 23.725ms | 8.018ms | 3.706ms | 32.274ms | 25 |
| AuthnService | GetUser | read_only | 3.793ms | 5.864ms | 3.989ms | 2.619ms | 6.524ms | 25 |
| AuthnService | IntrospectToken | read_only | 1.631ms | 3.099ms | 1.911ms | 507µs | 3.357ms | 25 |
| AuthnService | IssueMfaChallenge | mutation | 4.466ms | 4.671ms | 4.15ms | 3.146ms | 5.219ms | 5 |
| AuthnService | ListDevices | read_only | 3.573ms | 8.515ms | 4.327ms | 2.63ms | 8.591ms | 25 |
| AuthnService | ListMfaFactors | read_only | 3.378ms | 4.619ms | 3.504ms | 2.613ms | 4.74ms | 25 |
| AuthnService | ListSessions | read_only | 7.71ms | 16.614ms | 8.543ms | 5.819ms | 17.035ms | 25 |
| AuthnService | ListUsers | read_only | 6.587ms | 9.095ms | 6.879ms | 4.768ms | 9.443ms | 25 |
| AuthnService | ListWebAuthnCredentials | read_only | 3.285ms | 4.046ms | 3.454ms | 3.143ms | 4.498ms | 25 |
| AuthnService | Login | mutation | 2.201375s | 3.080443s | 2.072356s | 608.106ms | 3.800787s | 5 |
| AuthnService | Logout | mutation | 17.355ms | 20.528ms | 16.62ms | 7.301ms | 29.685ms | 5 |
| AuthnService | PutMfaPolicy | mutation | 23.938ms | 24.673ms | 20.69ms | 8.843ms | 26.053ms | 5 |
| AuthnService | RefreshSession | mutation | 19.328ms | 20.204ms | 19.469ms | 13.3ms | 29.846ms | 5 |
| AuthnService | RefreshToken | mutation | 14.058ms | 16.398ms | 14.404ms | 10.947ms | 17.201ms | 5 |
| AuthnService | RenamePasskey | mutation | 29.351ms | 30.609ms | 27.038ms | 16.275ms | 35.51ms | 5 |
| AuthnService | ResendOTP | mutation | 15.371ms | 20.12ms | 16.279ms | 12.108ms | 20.453ms | 5 |
| AuthnService | ResetPassword | mutation | 11.485ms | 11.746ms | 12.12ms | 8.569ms | 18.156ms | 5 |
| AuthnService | RevokeDevice | mutation | 19.486ms | 22.493ms | 19.573ms | 14.488ms | 26.153ms | 5 |
| AuthnService | RevokeRecoveryCodes | mutation | 11.37ms | 19.323ms | 14.457ms | 9.947ms | 21.076ms | 5 |
| AuthnService | RevokeSession | mutation | 7.699ms | 10.827ms | 9.347ms | 6.93ms | 13.741ms | 5 |
| AuthnService | SendOTP | mutation | 14.95ms | 20.504ms | 26.482ms | 13.007ms | 70.64ms | 5 |
| AuthnService | SendPhoneVerification | mutation | 13.723ms | 16.659ms | 16.109ms | 12.436ms | 24.136ms | 5 |
| AuthnService | StartWebAuthnAuthentication | mutation | 8.581ms | 9.782ms | 7.799ms | 1.905ms | 10.423ms | 5 |
| AuthnService | StartWebAuthnRegistration | mutation | 11.549ms | 12.96ms | 12.62ms | 8.795ms | 18.798ms | 5 |
| AuthnService | UpdateUser | mutation | 15.049ms | 19.437ms | 18.005ms | 13.082ms | 27.739ms | 5 |
| AuthnService | ValidateCSRF | read_only | 11.587ms | 29.123ms | 13.861ms | 6.222ms | 30.522ms | 25 |
| AuthnService | ValidateToken | read_only | 9.325ms | 20.515ms | 10.577ms | 4.865ms | 28.777ms | 25 |
| AuthnService | VerifyMfaChallenge | read_only | 26.036ms | 34.085ms | 25.847ms | 16.114ms | 36.883ms | 25 |
| AuthnService | VerifyOTP | read_only | 12.898ms | 18.652ms | 14.325ms | 9.134ms | 45.498ms | 25 |
| AuthzService | ActivateCanary | destructive | 14.241ms | 14.241ms | 14.241ms | 14.241ms | 14.241ms | 1 |
| AuthzService | ActivatePolicyVersion | destructive | 13.803ms | 13.803ms | 13.803ms | 13.803ms | 13.803ms | 1 |
| AuthzService | ApprovePolicyDraft | mutation | 21.966ms | 22.755ms | 21.294ms | 17.197ms | 26.229ms | 5 |
| AuthzService | AssignRole | mutation | 16.439ms | 23.27ms | 18ms | 11.039ms | 27.246ms | 5 |
| AuthzService | Authorize | read_only | 112.81ms | 203.707ms | 117.782ms | 59.868ms | 219.512ms | 25 |
| AuthzService | BatchCheckPermissions | read_only | 21.375ms | 54.906ms | 26.896ms | 10.131ms | 92.78ms | 25 |
| AuthzService | CheckAccess | read_only | 77.106ms | 129.899ms | 83.912ms | 47.742ms | 192.625ms | 25 |
| AuthzService | CreatePolicyDraft | mutation | 21.952ms | 21.963ms | 20.486ms | 14.474ms | 28.788ms | 5 |
| AuthzService | CreatePolicyRule | mutation | 6.124ms | 8.105ms | 6.774ms | 5.522ms | 8.361ms | 5 |
| AuthzService | CreateRole | mutation | 9.689ms | 10.74ms | 12.234ms | 5.914ms | 25.496ms | 5 |
| AuthzService | DeletePolicyRule | mutation | 7.369ms | 9.794ms | 7.872ms | 5.302ms | 10.697ms | 5 |
| AuthzService | DeleteRole | mutation | 11.289ms | 11.73ms | 11.373ms | 8.012ms | 17.59ms | 5 |
| AuthzService | DiffPolicyDraft | read_only | 19.83ms | 142.993ms | 38.113ms | 9.402ms | 148.563ms | 25 |
| AuthzService | ExplainPolicy | read_only | 109.275ms | 167.231ms | 96.474ms | 16.829ms | 172.526ms | 25 |
| AuthzService | GetAuthzRevision | read_only | 49.125ms | 90.941ms | 50.987ms | 22.71ms | 97.551ms | 25 |
| AuthzService | GetCanaryStatus | read_only | 100.991ms | 168.108ms | 122.921ms | 59.714ms | 517.377ms | 25 |
| AuthzService | GetNativeAccess | read_only | 70.629ms | 197.393ms | 87.728ms | 40.323ms | 298.283ms | 25 |
| AuthzService | GetPolicyBundle | read_only | 36.278ms | 64.944ms | 38.438ms | 16.696ms | 82.838ms | 25 |
| AuthzService | GetPolicyRule | read_only | 6.971ms | 11.888ms | 7.434ms | 4.635ms | 12.52ms | 25 |
| AuthzService | GetRole | read_only | 9.784ms | 13.846ms | 9.724ms | 5.324ms | 15.085ms | 25 |
| AuthzService | InvalidatePolicyBundles | destructive | 6.6ms | 6.6ms | 6.6ms | 6.6ms | 6.6ms | 1 |
| AuthzService | LintAuthzPolicies | read_only | 10.656ms | 27.347ms | 13.264ms | 4.944ms | 28.403ms | 25 |
| AuthzService | ListAccessDecisionAudits | read_only | 16.702ms | 28.958ms | 18.549ms | 8.925ms | 42.437ms | 25 |
| AuthzService | ListPolicyRules | read_only | 59.023ms | 124.658ms | 65.204ms | 6.604ms | 315.127ms | 25 |
| AuthzService | ListPolicyVersions | read_only | 86.619ms | 131.916ms | 98.198ms | 55.273ms | 146.112ms | 25 |
| AuthzService | ListRoles | read_only | 55.876ms | 79.072ms | 58.626ms | 33.605ms | 108.271ms | 25 |
| AuthzService | ListUserPermissions | read_only | 9.209ms | 34.533ms | 13.492ms | 4.953ms | 45.988ms | 25 |
| AuthzService | ListUserRoles | read_only | 8.598ms | 16.099ms | 9.635ms | 4.25ms | 19.387ms | 25 |
| AuthzService | MigrateLegacyPolicies | destructive | 6.427ms | 6.427ms | 6.427ms | 6.427ms | 6.427ms | 1 |
| AuthzService | PromoteCanary | destructive | 10.181ms | 10.181ms | 10.181ms | 10.181ms | 10.181ms | 1 |
| AuthzService | PutAuthzPolicy | mutation | 10.139ms | 11.222ms | 10.754ms | 7.632ms | 16.036ms | 5 |
| AuthzService | PutRelationship | mutation | 642.283ms | 716.879ms | 646.829ms | 504.548ms | 779.417ms | 5 |
| AuthzService | PutRoleBinding | mutation | 557.707ms | 583.211ms | 526.098ms | 411.05ms | 601.144ms | 5 |
| AuthzService | RejectPolicyDraft | mutation | 102.516ms | 142.184ms | 115.533ms | 63.894ms | 183.488ms | 5 |
| AuthzService | RevokeRole | mutation | 9.092ms | 9.952ms | 9.499ms | 8.412ms | 11.564ms | 5 |
| AuthzService | RollbackPolicyVersion | destructive | 7.702ms | 7.702ms | 7.702ms | 7.702ms | 7.702ms | 1 |
| AuthzService | SeedBuiltinRoles | mutation | 116.932ms | 118.849ms | 115.115ms | 92.184ms | 135.721ms | 5 |
| AuthzService | SimulatePolicy | mutation | 87.728ms | 90.649ms | 86.063ms | 66.641ms | 109.361ms | 5 |
| AuthzService | SubmitPolicyDraft | mutation | 99.725ms | 110.362ms | 99.005ms | 78.917ms | 116.454ms | 5 |
| AuthzService | UpdatePolicyDraft | mutation | 79.947ms | 91.102ms | 83.542ms | 72.434ms | 98.106ms | 5 |
| AuthzService | UpdateRole | mutation | 8.898ms | 9.83ms | 9.876ms | 8.364ms | 13.815ms | 5 |
| ControlPlaneService | AckStatus | mutation | 10.105ms | 10.558ms | 10.089ms | 8.54ms | 11.399ms | 5 |
| ControlPlaneService | DeltaResources | mutation | 0s | 0s | 0s | 0s | 0s | 5 |
| ControlPlaneService | GetResources | read_only | 9.542ms | 17.539ms | 10.604ms | 5.535ms | 27.766ms | 25 |
| ControlPlaneService | ListNodeStates | read_only | 100.359ms | 424.953ms | 171.84ms | 58.271ms | 766.338ms | 25 |
| ControlPlaneService | StreamResources | mutation | 0s | 0s | 0s | 0s | 0s | 5 |
| DataBroker | ActivateCatalog | destructive | 7.005ms | 7.005ms | 7.005ms | 7.005ms | 7.005ms | 1 |
| DataBroker | AnalyticalQuery | read_only | 3.175ms | 4.06ms | 3.164ms | 2.167ms | 4.251ms | 25 |
| DataBroker | ApplyMigration | mutation | 26.867ms | 30.849ms | 27.459ms | 24.098ms | 31.032ms | 5 |
| DataBroker | ApproveMigrationPlan | mutation | 16.093ms | 17.047ms | 14.031ms | 2.647ms | 18.699ms | 5 |
| DataBroker | BatchSelect | mutation | 0s | 0s | 0s | 0s | 0s | 5 |
| DataBroker | BatchUpsert | mutation | 0s | 0s | 0s | 0s | 0s | 5 |
| DataBroker | BeginTx | mutation | 0s | 0s | 105µs | 0s | 525µs | 5 |
| DataBroker | CacheDelete | mutation | 3.233ms | 4.05ms | 3.363ms | 2.034ms | 4.661ms | 5 |
| DataBroker | CacheGet | read_only | 3.177ms | 3.926ms | 3.162ms | 2.106ms | 4.789ms | 25 |
| DataBroker | CacheScan | read_only | 3.249ms | 4.473ms | 3.284ms | 2.063ms | 4.539ms | 25 |
| DataBroker | CacheSet | mutation | 2.989ms | 3.299ms | 3.041ms | 2.683ms | 3.386ms | 5 |
| DataBroker | CreateMaterializedView | mutation | 4.084ms | 4.292ms | 3.892ms | 2.621ms | 4.496ms | 5 |
| DataBroker | Delete | mutation | 3.827ms | 4.202ms | 4.146ms | 3.722ms | 5.15ms | 5 |
| DataBroker | DeletePolicy | mutation | 7.169ms | 7.653ms | 6.704ms | 4.502ms | 7.854ms | 5 |
| DataBroker | DismissDlqEvent | mutation | 4.107ms | 4.255ms | 4.162ms | 3.793ms | 4.858ms | 5 |
| DataBroker | DocumentDelete | mutation | 3.924ms | 4.298ms | 4.067ms | 3.147ms | 5.721ms | 5 |
| DataBroker | DocumentFind | read_only | 4.23ms | 6.091ms | 4.451ms | 3.046ms | 8.458ms | 25 |
| DataBroker | DocumentGet | read_only | 4.223ms | 11.579ms | 5.096ms | 2.604ms | 13.612ms | 25 |
| DataBroker | DocumentUpsert | mutation | 3.256ms | 4.875ms | 4.586ms | 1.822ms | 10.276ms | 5 |
| DataBroker | DropResource | destructive | 2.893ms | 2.893ms | 2.893ms | 2.893ms | 2.893ms | 1 |
| DataBroker | EnqueueOutboxEvent | mutation | 9.047ms | 9.418ms | 8.938ms | 7.202ms | 10.165ms | 5 |
| DataBroker | EnsureProject | mutation | 64.096ms | 71.824ms | 63.412ms | 46.612ms | 75.547ms | 5 |
| DataBroker | EnsureResource | mutation | 5.254ms | 5.453ms | 5.024ms | 3.666ms | 6.307ms | 5 |
| DataBroker | GeneratePresignedUrl | mutation | 6.201ms | 8.692ms | 7.671ms | 4.179ms | 14.358ms | 5 |
| DataBroker | GenericDispatch | mutation | 6.773ms | 8.599ms | 6.584ms | 4.041ms | 9.016ms | 5 |
| DataBroker | GetAdminSummary | read_only | 73.485ms | 123.036ms | 74.455ms | 39.977ms | 135.776ms | 25 |
| DataBroker | GetCapabilities | read_only | 17.295ms | 21.397ms | 17.127ms | 11.653ms | 22.907ms | 25 |
| DataBroker | GetCatalogManifest | read_only | 204.557ms | 730.204ms | 364.936ms | 149.72ms | 826.215ms | 25 |
| DataBroker | GetCatalogVersion | read_only | 66.695ms | 79.918ms | 67.706ms | 44.01ms | 95.712ms | 25 |
| DataBroker | GetCatalogVersions | read_only | 83.322ms | 156.057ms | 94.287ms | 41.684ms | 182.749ms | 25 |
| DataBroker | GetCdcStatus | read_only | 56.832ms | 144.538ms | 87.809ms | 33.433ms | 436.967ms | 25 |
| DataBroker | GetDlqEvent | read_only | 10.08ms | 29.346ms | 13.062ms | 4.513ms | 34.188ms | 25 |
| DataBroker | GetHealthReport | read_only | 47.52ms | 468.004ms | 143.082ms | 18.541ms | 480.182ms | 25 |
| DataBroker | GetMigrationStatus | read_only | 7.952ms | 14.249ms | 9.053ms | 4.685ms | 15.703ms | 25 |
| DataBroker | GetObject | read_only | 0s | 580µs | 206µs | 0s | 4.032ms | 25 |
| DataBroker | GetSaga | read_only | 10.244ms | 24.918ms | 11.429ms | 7.147ms | 26.17ms | 25 |
| DataBroker | GraphMutate | mutation | 6.174ms | 6.835ms | 6.861ms | 4.886ms | 10.332ms | 5 |
| DataBroker | GraphQuery | read_only | 8.036ms | 12.72ms | 8.637ms | 4.251ms | 15.731ms | 25 |
| DataBroker | InitiateMultipartUpload | mutation | 8.862ms | 21.107ms | 13.045ms | 5.041ms | 21.655ms | 5 |
| DataBroker | LintPolicies | read_only | 9.844ms | 15.356ms | 9.897ms | 5.017ms | 15.416ms | 25 |
| DataBroker | ListAdminAuditLogs | read_only | 7.997ms | 12.659ms | 8.185ms | 5.507ms | 13.026ms | 25 |
| DataBroker | ListDlqEvents | read_only | 3.298ms | 5.799ms | 3.558ms | 2.135ms | 5.884ms | 25 |
| DataBroker | ListMessageSchemas | read_only | 2.895ms | 3.873ms | 2.937ms | 1.64ms | 3.919ms | 25 |
| DataBroker | ListMigrationRuns | read_only | 7.128ms | 10.944ms | 7.326ms | 4.414ms | 11.301ms | 25 |
| DataBroker | ListPolicies | read_only | 7.671ms | 12.003ms | 7.86ms | 5.75ms | 12.143ms | 25 |
| DataBroker | ListProjects | read_only | 9.246ms | 14.399ms | 9.287ms | 6.467ms | 14.645ms | 25 |
| DataBroker | ListResources | read_only | 4.361ms | 6.739ms | 4.645ms | 3.186ms | 7.079ms | 25 |
| DataBroker | ListSagas | read_only | 5.008ms | 9.167ms | 5.583ms | 3.195ms | 9.227ms | 25 |
| DataBroker | LookupMessageSchema | read_only | 3.839ms | 6.092ms | 4.135ms | 2.729ms | 6.14ms | 25 |
| DataBroker | MarkSagaReviewed | mutation | 4.233ms | 4.474ms | 4.397ms | 3.298ms | 6.071ms | 5 |
| DataBroker | PauseCdc | mutation | 29.297ms | 30.754ms | 27.583ms | 18.424ms | 31.033ms | 5 |
| DataBroker | PlanMigration | mutation | 18.428ms | 23.989ms | 20.561ms | 16.5ms | 25.712ms | 5 |
| DataBroker | PreviewCdcRedaction | read_only | 14.142ms | 18.983ms | 14.612ms | 11.824ms | 21.213ms | 25 |
| DataBroker | PublishCDC | mutation | 0s | 0s | 111µs | 0s | 554µs | 5 |
| DataBroker | PutObject | mutation | 0s | 0s | 0s | 0s | 0s | 5 |
| DataBroker | PutPolicy | destructive | 37.023ms | 37.023ms | 37.023ms | 37.023ms | 37.023ms | 1 |
| DataBroker | QuarantineDlqEvent | mutation | 3.768ms | 3.942ms | 3.633ms | 2.865ms | 4.258ms | 5 |
| DataBroker | ReloadPolicies | destructive | 16.285ms | 16.285ms | 16.285ms | 16.285ms | 16.285ms | 1 |
| DataBroker | ReplayDlqEvent | mutation | 2.75ms | 2.791ms | 2.698ms | 2.231ms | 3.474ms | 5 |
| DataBroker | ResumeCdc | mutation | 16.191ms | 16.558ms | 16.224ms | 14.467ms | 18.212ms | 5 |
| DataBroker | RetrySagaCompensation | mutation | 2.837ms | 3.052ms | 2.921ms | 2.664ms | 3.354ms | 5 |
| DataBroker | RollbackCatalog | destructive | 7.081ms | 7.081ms | 7.081ms | 7.081ms | 7.081ms | 1 |
| DataBroker | ScanProjectionDrift | read_only | 3.504ms | 4.438ms | 3.42ms | 2.274ms | 4.567ms | 25 |
| DataBroker | Select | read_only | 4.751ms | 6.013ms | 4.759ms | 3.609ms | 6.675ms | 25 |
| DataBroker | SelectV2 | read_only | 0s | 0s | 28µs | 0s | 690µs | 25 |
| DataBroker | StageCatalog | destructive | 5.656ms | 5.656ms | 5.656ms | 5.656ms | 5.656ms | 1 |
| DataBroker | StepDownCdcLeader | mutation | 18.51ms | 19.419ms | 18.914ms | 17.91ms | 20.24ms | 5 |
| DataBroker | TimeSeriesQuery | read_only | 3.235ms | 4.287ms | 3.26ms | 2.231ms | 4.388ms | 25 |
| DataBroker | TimeSeriesWrite | mutation | 3.024ms | 3.116ms | 2.962ms | 2.701ms | 3.185ms | 5 |
| DataBroker | Upsert | mutation | 2.716ms | 3.523ms | 2.937ms | 2.155ms | 3.675ms | 5 |
| DataBroker | ValidateCatalog | destructive | 2.247ms | 2.247ms | 2.247ms | 2.247ms | 2.247ms | 1 |
| DataBroker | VectorBatchUpsert | mutation | 0s | 0s | 0s | 0s | 0s | 5 |
| DataBroker | VectorHybridSearch | read_only | 2.464ms | 2.984ms | 2.513ms | 1.699ms | 3.79ms | 25 |
| DataBroker | VectorSearch | read_only | 3.238ms | 5.377ms | 3.25ms | 2.231ms | 5.617ms | 25 |
| DataBroker | VectorUpsert | mutation | 2.975ms | 3.325ms | 3.251ms | 2.721ms | 4.288ms | 5 |
| DataBroker | VerifyAdminAuditLog | read_only | 8.414ms | 15.825ms | 9.884ms | 6.563ms | 15.905ms | 25 |
| IdentityProviderService | CreateProvider | mutation | 6.2ms | 7.555ms | 6.811ms | 5.206ms | 8.946ms | 5 |
| IdentityProviderService | DisableProvider | mutation | 8.766ms | 11.967ms | 9.909ms | 4.757ms | 17.587ms | 5 |
| IdentityProviderService | ForceJwksRefresh | mutation | 16.678ms | 16.899ms | 17.834ms | 9.602ms | 33.738ms | 5 |
| IdentityProviderService | GetProvider | read_only | 8.764ms | 27.124ms | 11.873ms | 4.818ms | 32.632ms | 25 |
| IdentityProviderService | ImportSamlMetadata | mutation | 11.088ms | 13.466ms | 12.043ms | 5.282ms | 21.992ms | 5 |
| IdentityProviderService | LinkIdentity | mutation | 11.237ms | 12.23ms | 10.939ms | 7.572ms | 14.227ms | 5 |
| IdentityProviderService | ListExternalIdentities | read_only | 21.933ms | 49.309ms | 26.02ms | 11.145ms | 61.264ms | 25 |
| IdentityProviderService | ListProviders | read_only | 20.892ms | 43.055ms | 21.556ms | 8.163ms | 43.51ms | 25 |
| IdentityProviderService | PreviewClaimMapping | read_only | 9.46ms | 18.555ms | 11.152ms | 4.347ms | 20.34ms | 25 |
| IdentityProviderService | PreviewGroupMapping | read_only | 5.268ms | 14.802ms | 5.971ms | 2.291ms | 16.249ms | 25 |
| IdentityProviderService | ResolveExternalIdentity | mutation | 3.23ms | 3.444ms | 3.362ms | 2.653ms | 4.26ms | 5 |
| IdentityProviderService | SamlAcs | mutation | 2.776ms | 3.067ms | 2.857ms | 2.138ms | 3.594ms | 5 |
| IdentityProviderService | ScimCreateGroup | mutation | 3.719ms | 4.289ms | 3.538ms | 2.275ms | 4.552ms | 5 |
| IdentityProviderService | ScimCreateUser | mutation | 2.793ms | 2.868ms | 3.057ms | 2.132ms | 5.02ms | 5 |
| IdentityProviderService | ScimDeleteGroup | mutation | 3.408ms | 3.953ms | 3.606ms | 3.058ms | 4.38ms | 5 |
| IdentityProviderService | ScimDeleteUser | mutation | 3.05ms | 3.3ms | 2.791ms | 2.114ms | 3.375ms | 5 |
| IdentityProviderService | ScimGetGroup | mutation | 2.66ms | 3.304ms | 3.465ms | 1.949ms | 6.766ms | 5 |
| IdentityProviderService | ScimGetUser | mutation | 2.68ms | 6.998ms | 4.027ms | 1.62ms | 7.051ms | 5 |
| IdentityProviderService | ScimListGroups | mutation | 2.509ms | 3.078ms | 2.679ms | 2.227ms | 3.237ms | 5 |
| IdentityProviderService | ScimListUsers | mutation | 2.658ms | 2.974ms | 2.676ms | 2.141ms | 3.056ms | 5 |
| IdentityProviderService | ScimPatchGroup | mutation | 2.744ms | 3.23ms | 2.856ms | 2.15ms | 3.467ms | 5 |
| IdentityProviderService | ScimPatchUser | mutation | 3.61ms | 3.868ms | 3.297ms | 2.055ms | 3.872ms | 5 |
| IdentityProviderService | ScimReplaceUser | mutation | 3.793ms | 3.992ms | 3.76ms | 3.092ms | 4.542ms | 5 |
| IdentityProviderService | StartSamlLogin | mutation | 3.268ms | 3.542ms | 3.57ms | 2.649ms | 5.191ms | 5 |
| IdentityProviderService | TestProviderDiscovery | read_only | 3.209ms | 5.965ms | 3.523ms | 1.615ms | 7.419ms | 25 |
| IdentityProviderService | UnlinkIdentity | mutation | 5.097ms | 5.266ms | 4.793ms | 3.788ms | 5.8ms | 5 |
| IdentityProviderService | UpdateProvider | mutation | 4.708ms | 7.842ms | 7.302ms | 2.75ms | 17.409ms | 5 |
| NotificationService | GetDeliveryStats | read_only | 7.378ms | 19.817ms | 10.229ms | 4.746ms | 19.871ms | 25 |
| NotificationService | GetNotification | read_only | 4.392ms | 6.492ms | 4.302ms | 2.153ms | 7.158ms | 25 |
| NotificationService | GetPreference | read_only | 4.036ms | 7.797ms | 4.438ms | 2.129ms | 8.179ms | 25 |
| NotificationService | GetTemplate | read_only | 24.235ms | 47.387ms | 26.125ms | 16.233ms | 51.019ms | 25 |
| NotificationService | ListNotifications | read_only | 13.901ms | 16.903ms | 14.253ms | 10.154ms | 17.985ms | 25 |
| NotificationService | ListPreferences | read_only | 2.749ms | 4.516ms | 2.905ms | 1.061ms | 4.953ms | 25 |
| NotificationService | ListTemplates | read_only | 30.779ms | 46.449ms | 32.091ms | 26.506ms | 47.376ms | 25 |
| NotificationService | RetryNotification | mutation | 2.485ms | 2.866ms | 2.487ms | 1.519ms | 4ms | 5 |
| NotificationService | SendNotification | mutation | 2.856ms | 3.273ms | 2.95ms | 2.519ms | 3.569ms | 5 |
| NotificationService | SetPreference | mutation | 3.472ms | 3.602ms | 3.325ms | 2.697ms | 4.058ms | 5 |
| NotificationService | UpsertTemplate | mutation | 5.493ms | 5.7ms | 5.401ms | 4.353ms | 6.167ms | 5 |
| PeerService | GetPeer | read_only | 2.731ms | 4.221ms | 2.936ms | 2.101ms | 4.856ms | 25 |
| PeerService | JoinRoom | mutation | 2.792ms | 2.806ms | 2.742ms | 2.179ms | 3.672ms | 5 |
| PeerService | LeaveRoom | mutation | 2.663ms | 3.001ms | 2.849ms | 2.131ms | 3.798ms | 5 |
| PeerService | ListPeers | read_only | 3.373ms | 5.575ms | 3.651ms | 2.119ms | 6.191ms | 25 |
| RoomService | CloseRoom | mutation | 3.72ms | 4.119ms | 3.568ms | 2.698ms | 4.562ms | 5 |
| RoomService | CreateRoom | mutation | 3.767ms | 3.939ms | 3.793ms | 3.233ms | 4.289ms | 5 |
| RoomService | GetRoom | read_only | 3.627ms | 4.843ms | 3.686ms | 2.147ms | 5.535ms | 25 |
| RoomService | ListRooms | read_only | 2.859ms | 4.476ms | 3.131ms | 1.757ms | 4.866ms | 25 |
| RoomService | UpdateRoom | mutation | 2.686ms | 3.299ms | 2.846ms | 2.389ms | 3.387ms | 5 |
| SignalingService | Signal | mutation | 0s | 0s | 0s | 0s | 0s | 5 |
| StorageService | DeleteFile | mutation | 2.688ms | 3.349ms | 2.902ms | 2.126ms | 3.676ms | 5 |
| StorageService | FinalizeUpload | mutation | 2.378ms | 2.559ms | 2.408ms | 2.112ms | 2.798ms | 5 |
| StorageService | GetDownloadUrl | read_only | 2.552ms | 3.771ms | 2.618ms | 1.087ms | 3.931ms | 25 |
| StorageService | GetFile | read_only | 2.604ms | 3.89ms | 2.787ms | 1.862ms | 4.601ms | 25 |
| StorageService | ListFiles | read_only | 2.893ms | 3.625ms | 2.915ms | 1.611ms | 3.857ms | 25 |
| StorageService | RegisterUpload | mutation | 2.917ms | 3.261ms | 3.037ms | 2.662ms | 3.63ms | 5 |
| StorageService | UpdateFile | mutation | 2.675ms | 3.375ms | 3.055ms | 2.646ms | 3.91ms | 5 |
| TenantService | CreateTenant | mutation | 2.655ms | 2.659ms | 2.555ms | 2.222ms | 2.721ms | 5 |
| TenantService | GetTenant | read_only | 17.364ms | 24.14ms | 17.868ms | 12.581ms | 25.077ms | 25 |
| TenantService | GetTenantConfig | read_only | 17.106ms | 27.515ms | 18.137ms | 12.955ms | 28.49ms | 25 |
| TenantService | ListTenants | read_only | 2.49ms | 3.243ms | 2.434ms | 966µs | 3.658ms | 25 |
| TenantService | UpdateTenant | mutation | 3.235ms | 3.877ms | 3.25ms | 2.557ms | 3.945ms | 5 |
| TenantService | UpdateTenantConfig | mutation | 2.775ms | 3.744ms | 3.08ms | 2.241ms | 4.265ms | 5 |
| TrackService | ListTracks | read_only | 2.689ms | 4.245ms | 2.932ms | 2.144ms | 4.568ms | 25 |
| TrackService | MuteTrack | mutation | 3.157ms | 3.281ms | 2.898ms | 1.594ms | 3.423ms | 5 |
| TrackService | PublishTrack | mutation | 2.74ms | 3.092ms | 2.789ms | 1.615ms | 3.844ms | 5 |
| TrackService | UnpublishTrack | mutation | 2.676ms | 2.966ms | 2.55ms | 1.607ms | 3.293ms | 5 |
| TurnService | IssueCredentials | mutation | 2.73ms | 2.867ms | 2.753ms | 1.628ms | 3.919ms | 5 |
