# UDB SDK Live Perf — Go (localhost)

RPCs measured: 262   tenant=e6eaed2c-67e0-40b7-9842-53598e248dcb

## Per-service mean latency (mean of per-RPC means)

| Service | RPCs | mean |
|---|---:|---:|
| DataBroker | 76 | 272.341ms |
| AuthnService | 50 | 23.803ms |
| AuthzService | 41 | 6.855ms |
| NotificationService | 11 | 6.259ms |
| ApiKeyService | 9 | 7.189ms |
| TenantService | 6 | 9.794ms |
| IdentityProviderService | 27 | 1.956ms |
| AnalyticsService | 7 | 5.647ms |
| StorageService | 7 | 4.606ms |
| ControlPlaneService | 5 | 6.313ms |
| AssetService | 8 | 2.592ms |
| RoomService | 5 | 2.589ms |
| TrackService | 4 | 3.157ms |
| PeerService | 4 | 2.837ms |
| SignalingService | 1 | 3.399ms |
| TurnService | 1 | 2.692ms |

## Slowest 25 RPCs by p99

| RPC | kind | p50 | p99 | mean | iters | note |
|---|---|---:|---:|---:|---:|---|
| DataBroker/PublishCDC | mutation | 20.003809s | 20.017063s | 20.008291s | 5 | mutation (last code=DeadlineExceeded) |
| AuthnService/CreateUser | mutation | 560.13ms | 561.864ms | 570.425ms | 5 | mutation (last code=Internal) |
| AuthnService/Login | mutation | 409.822ms | 410.136ms | 407.034ms | 5 | mutation |
| DataBroker/GetCatalogManifest | read_only | 166.004ms | 204.824ms | 167.356ms | 25 | read_only |
| TenantService/GetTenant | read_only | 22.833ms | 64.85ms | 29.582ms | 25 | read_only |
| DataBroker/GetAdminSummary | read_only | 34.319ms | 55.555ms | 37.046ms | 25 | read_only |
| DataBroker/GetHealthReport | read_only | 28.645ms | 43.007ms | 30.656ms | 25 | read_only |
| AuthzService/PutRelationship | mutation | 35.815ms | 38.424ms | 35.892ms | 5 | mutation |
| AuthzService/Authorize | read_only | 27.666ms | 36.898ms | 28.158ms | 25 | read_only |
| DataBroker/ResumeCdc | mutation | 19.097ms | 33.197ms | 22.988ms | 5 | mutation |
| NotificationService/ListTemplates | read_only | 23.542ms | 31.484ms | 23.872ms | 25 | read_only |
| ControlPlaneService/ListNodeStates | read_only | 25.071ms | 30.802ms | 25.919ms | 25 | read_only |
| StorageService/GetFile | read_only | 7.45ms | 27.67ms | 10.044ms | 25 | read_only (last code=InvalidArgument) |
| AuthzService/PutRoleBinding | mutation | 27.552ms | 27.653ms | 26.573ms | 5 | mutation |
| DataBroker/BeginTx | mutation | 18.144ms | 27.237ms | 23.172ms | 5 | mutation (last code=Internal) |
| DataBroker/GetCapabilities | read_only | 8.627ms | 25.99ms | 10.442ms | 25 | read_only |
| AuthzService/GetNativeAccess | read_only | 22.975ms | 25.935ms | 22.725ms | 25 | read_only |
| AuthzService/CheckAccess | read_only | 20.882ms | 25.421ms | 20.829ms | 25 | read_only |
| DataBroker/ApplyMigration | mutation | 24.691ms | 25.25ms | 25.549ms | 5 | mutation (last code=InvalidArgument) |
| ApiKeyService/CreateApiKey | mutation | 23.14ms | 24.094ms | 23.193ms | 5 | mutation |
| DataBroker/PreviewCdcRedaction | read_only | 15.019ms | 23.277ms | 15.532ms | 25 | read_only |
| TenantService/GetTenantConfig | read_only | 19.045ms | 22.149ms | 18.399ms | 25 | read_only |
| DataBroker/ListPolicies | read_only | 8.469ms | 19.627ms | 9.809ms | 25 | read_only |
| DataBroker/EnsureProject | mutation | 17.289ms | 19.573ms | 16.698ms | 5 | mutation |
| AuthnService/ForgotPassword | mutation | 16.057ms | 18.493ms | 16.363ms | 5 | mutation |

## Full per-RPC table (sorted by service, then name)

| Service | RPC | kind | p50 | p99 | mean | min | max | iters |
|---|---|---|---:|---:|---:|---:|---:|---:|
| AnalyticsService | GetExecutorPerformance | read_only | 5.213ms | 13.33ms | 6.942ms | 4.264ms | 14.394ms | 25 |
| AnalyticsService | GetPipelineSummary | read_only | 5.691ms | 8.127ms | 6.06ms | 4.177ms | 10.896ms | 25 |
| AnalyticsService | GetReconciliationAnalytics | read_only | 4.443ms | 9.248ms | 5.536ms | 2.941ms | 14.099ms | 25 |
| AnalyticsService | GetSlaCompliance | read_only | 4.225ms | 8.151ms | 4.848ms | 3.18ms | 8.619ms | 25 |
| AnalyticsService | GetThroughput | read_only | 4.034ms | 6.052ms | 4.1ms | 2.615ms | 7.438ms | 25 |
| AnalyticsService | RecordPipelineMetric | mutation | 7.244ms | 8.827ms | 7.684ms | 5.26ms | 11.416ms | 5 |
| AnalyticsService | TriggerSnapshot | mutation | 4.522ms | 5.241ms | 4.362ms | 3.101ms | 5.777ms | 5 |
| ApiKeyService | CreateApiKey | mutation | 23.14ms | 24.094ms | 23.193ms | 21.633ms | 24.676ms | 5 |
| ApiKeyService | EmergencyRevokeApiKeys | destructive | 2.334ms | 2.334ms | 2.334ms | 2.334ms | 2.334ms | 1 |
| ApiKeyService | GetApiKey | read_only | 4.631ms | 7.493ms | 4.956ms | 3.188ms | 11.79ms | 25 |
| ApiKeyService | GetApiKeyUsageStats | read_only | 4.935ms | 11.015ms | 5.789ms | 3.697ms | 11.023ms | 25 |
| ApiKeyService | ListApiKeys | read_only | 5.492ms | 7.209ms | 5.782ms | 4.413ms | 8.127ms | 25 |
| ApiKeyService | RevokeApiKey | mutation | 3.683ms | 3.87ms | 3.679ms | 3.197ms | 4.244ms | 5 |
| ApiKeyService | RotateApiKey | mutation | 3.683ms | 3.851ms | 3.634ms | 3.103ms | 4.374ms | 5 |
| ApiKeyService | UpdateApiKey | mutation | 4.429ms | 4.827ms | 4.701ms | 3.673ms | 6.423ms | 5 |
| ApiKeyService | ValidateApiKey | read_only | 10.17ms | 13.119ms | 10.632ms | 8.328ms | 18.56ms | 25 |
| AssetService | CompleteStep | mutation | 3.078ms | 3.081ms | 2.914ms | 2.219ms | 3.741ms | 5 |
| AssetService | CreatePipelineDefinition | mutation | 2.111ms | 2.4ms | 2.414ms | 2.091ms | 3.368ms | 5 |
| AssetService | GetAsset | read_only | 2.663ms | 3.537ms | 2.515ms | 1.575ms | 3.936ms | 25 |
| AssetService | GetPipeline | read_only | 2.676ms | 3.288ms | 2.586ms | 1.61ms | 3.323ms | 25 |
| AssetService | GetPipelineDefinition | read_only | 2.616ms | 3.764ms | 2.513ms | 514µs | 3.933ms | 25 |
| AssetService | ListAssets | read_only | 2.693ms | 3.706ms | 2.812ms | 2.118ms | 3.736ms | 25 |
| AssetService | RegisterAsset | mutation | 2.452ms | 2.587ms | 2.37ms | 1.584ms | 3.11ms | 5 |
| AssetService | StartPipeline | mutation | 2.641ms | 3.085ms | 2.61ms | 1.919ms | 3.361ms | 5 |
| AuthnService | AdminResetMfa | destructive | 7.233ms | 7.233ms | 7.233ms | 7.233ms | 7.233ms | 1 |
| AuthnService | AdminResetPassword | destructive | 3.163ms | 3.163ms | 3.163ms | 3.163ms | 3.163ms | 1 |
| AuthnService | AdminRevokeAllTenantSessions | destructive | 2.32ms | 2.32ms | 2.32ms | 2.32ms | 2.32ms | 1 |
| AuthnService | AdminRevokeAllUserSessions | destructive | 2.14ms | 2.14ms | 2.14ms | 2.14ms | 2.14ms | 1 |
| AuthnService | AdminRevokeSession | destructive | 2.117ms | 2.117ms | 2.117ms | 2.117ms | 2.117ms | 1 |
| AuthnService | Authenticate | read_only | 3.152ms | 4.531ms | 3.251ms | 2.124ms | 4.835ms | 25 |
| AuthnService | ChangePassword | mutation | 1.623ms | 1.623ms | 1.634ms | 1.228ms | 2.132ms | 5 |
| AuthnService | ChangeUserStatus | destructive | 2.115ms | 2.115ms | 2.115ms | 2.115ms | 2.115ms | 1 |
| AuthnService | ConfirmMFAEnrollment | mutation | 3.658ms | 3.719ms | 3.615ms | 3.147ms | 4.193ms | 5 |
| AuthnService | CreateSession | mutation | 3.776ms | 5.28ms | 4.371ms | 3.178ms | 5.893ms | 5 |
| AuthnService | CreateUser | mutation | 560.13ms | 561.864ms | 570.425ms | 555.793ms | 618.082ms | 5 |
| AuthnService | DeleteWebAuthnCredential | mutation | 12.273ms | 13.975ms | 12.523ms | 9.36ms | 15.24ms | 5 |
| AuthnService | DisableMfaFactor | mutation | 5.418ms | 5.559ms | 4.845ms | 3.601ms | 5.586ms | 5 |
| AuthnService | EmergencyRevoke | destructive | 1.574ms | 1.574ms | 1.574ms | 1.574ms | 1.574ms | 1 |
| AuthnService | EnrollMFA | mutation | 3.506ms | 3.687ms | 3.846ms | 3.196ms | 5.63ms | 5 |
| AuthnService | FinishWebAuthnAuthentication | mutation | 1.027ms | 1.305ms | 1.098ms | 508µs | 1.671ms | 5 |
| AuthnService | FinishWebAuthnRegistration | mutation | 1.664ms | 1.759ms | 1.602ms | 950µs | 2.33ms | 5 |
| AuthnService | ForgotPassword | mutation | 16.057ms | 18.493ms | 16.363ms | 12.936ms | 20.435ms | 5 |
| AuthnService | GenerateRecoveryCodes | mutation | 4.456ms | 5.123ms | 4.51ms | 3.47ms | 5.243ms | 5 |
| AuthnService | GetJwks | read_only | 5.617ms | 8.13ms | 5.559ms | 804µs | 10.164ms | 25 |
| AuthnService | GetMfaPolicy | read_only | 3.746ms | 6.743ms | 4.343ms | 3.135ms | 9.614ms | 25 |
| AuthnService | GetSession | read_only | 4.151ms | 6.408ms | 4.3ms | 3.159ms | 6.587ms | 25 |
| AuthnService | GetUser | read_only | 3.991ms | 5.427ms | 3.992ms | 2.325ms | 6.434ms | 25 |
| AuthnService | IntrospectToken | read_only | 1.956ms | 2.883ms | 1.997ms | 1.066ms | 3.397ms | 25 |
| AuthnService | IssueMfaChallenge | mutation | 4.446ms | 4.463ms | 4.373ms | 3.837ms | 4.916ms | 5 |
| AuthnService | ListDevices | read_only | 4.218ms | 7.075ms | 4.74ms | 3.018ms | 7.648ms | 25 |
| AuthnService | ListMfaFactors | read_only | 3.828ms | 5.204ms | 4.062ms | 3.178ms | 6.393ms | 25 |
| AuthnService | ListSessions | read_only | 8.331ms | 15.891ms | 9.252ms | 6.038ms | 18.172ms | 25 |
| AuthnService | ListUsers | read_only | 6.73ms | 11.702ms | 7.418ms | 4.297ms | 11.844ms | 25 |
| AuthnService | ListWebAuthnCredentials | read_only | 4.61ms | 6.738ms | 4.844ms | 3.246ms | 7.868ms | 25 |
| AuthnService | Login | mutation | 409.822ms | 410.136ms | 407.034ms | 377.826ms | 429.548ms | 5 |
| AuthnService | Logout | mutation | 3.841ms | 5.427ms | 4.365ms | 3.119ms | 5.72ms | 5 |
| AuthnService | PutMfaPolicy | mutation | 6.287ms | 6.595ms | 6.305ms | 5.489ms | 7.289ms | 5 |
| AuthnService | RefreshSession | mutation | 3.756ms | 4.287ms | 3.737ms | 2.707ms | 4.46ms | 5 |
| AuthnService | RefreshToken | mutation | 2.765ms | 3.032ms | 3.169ms | 2.113ms | 5.27ms | 5 |
| AuthnService | RenamePasskey | mutation | 9.338ms | 9.407ms | 9.577ms | 8.75ms | 11.07ms | 5 |
| AuthnService | ResendOTP | mutation | 3.157ms | 4.418ms | 3.66ms | 2.707ms | 5.028ms | 5 |
| AuthnService | ResetPassword | mutation | 826µs | 1.086ms | 818µs | 543µs | 1.089ms | 5 |
| AuthnService | RevokeDevice | mutation | 4.525ms | 5.662ms | 5.367ms | 3.541ms | 8.782ms | 5 |
| AuthnService | RevokeRecoveryCodes | mutation | 3.543ms | 3.749ms | 3.455ms | 2.094ms | 4.72ms | 5 |
| AuthnService | RevokeSession | mutation | 3.754ms | 4.818ms | 4.392ms | 3.352ms | 6.405ms | 5 |
| AuthnService | SendOTP | mutation | 2.788ms | 3.346ms | 2.995ms | 2.679ms | 3.486ms | 5 |
| AuthnService | SendPhoneVerification | mutation | 3.088ms | 3.443ms | 3.175ms | 2.688ms | 3.725ms | 5 |
| AuthnService | StartWebAuthnAuthentication | mutation | 746µs | 881µs | 748µs | 508µs | 1.095ms | 5 |
| AuthnService | StartWebAuthnRegistration | mutation | 539µs | 1.923ms | 1.178ms | 0s | 2.888ms | 5 |
| AuthnService | UpdateUser | mutation | 3.606ms | 3.652ms | 3.467ms | 2.632ms | 4.804ms | 5 |
| AuthnService | ValidateCSRF | read_only | 3.199ms | 4.283ms | 3.404ms | 2.59ms | 6.945ms | 25 |
| AuthnService | ValidateToken | read_only | 1.595ms | 4.171ms | 1.834ms | 1.035ms | 4.897ms | 25 |
| AuthnService | VerifyMfaChallenge | read_only | 13.419ms | 16.213ms | 13.562ms | 11.754ms | 16.965ms | 25 |
| AuthnService | VerifyOTP | read_only | 3.897ms | 6.663ms | 4.297ms | 3.122ms | 6.689ms | 25 |
| AuthzService | ActivateCanary | destructive | 2.005ms | 2.005ms | 2.005ms | 2.005ms | 2.005ms | 1 |
| AuthzService | ActivatePolicyVersion | destructive | 1.588ms | 1.588ms | 1.588ms | 1.588ms | 1.588ms | 1 |
| AuthzService | ApprovePolicyDraft | mutation | 9.529ms | 10.655ms | 10.041ms | 7.93ms | 13.169ms | 5 |
| AuthzService | AssignRole | mutation | 1.6ms | 1.607ms | 1.574ms | 1.463ms | 1.61ms | 5 |
| AuthzService | Authorize | read_only | 27.666ms | 36.898ms | 28.158ms | 23.734ms | 38.802ms | 25 |
| AuthzService | BatchCheckPermissions | read_only | 1.66ms | 2.904ms | 1.76ms | 1.018ms | 2.919ms | 25 |
| AuthzService | CheckAccess | read_only | 20.882ms | 25.421ms | 20.829ms | 15.73ms | 26.541ms | 25 |
| AuthzService | CreatePolicyDraft | mutation | 9.596ms | 10.368ms | 10.05ms | 9.177ms | 11.685ms | 5 |
| AuthzService | CreatePolicyRule | mutation | 1.756ms | 2.15ms | 1.933ms | 1.577ms | 2.585ms | 5 |
| AuthzService | CreateRole | mutation | 1.7ms | 1.796ms | 1.619ms | 1.015ms | 2.038ms | 5 |
| AuthzService | DeletePolicyRule | mutation | 1.532ms | 2.036ms | 1.49ms | 506µs | 2.852ms | 5 |
| AuthzService | DeleteRole | mutation | 1.531ms | 1.726ms | 1.593ms | 1.459ms | 1.753ms | 5 |
| AuthzService | DiffPolicyDraft | read_only | 6.924ms | 8.749ms | 7.078ms | 5.319ms | 8.814ms | 25 |
| AuthzService | ExplainPolicy | read_only | 6.808ms | 8.109ms | 6.857ms | 5.641ms | 9.8ms | 25 |
| AuthzService | GetAuthzRevision | read_only | 3.285ms | 5.237ms | 3.558ms | 1.446ms | 5.353ms | 25 |
| AuthzService | GetCanaryStatus | read_only | 7.149ms | 10.827ms | 7.359ms | 4.331ms | 11.421ms | 25 |
| AuthzService | GetNativeAccess | read_only | 22.975ms | 25.935ms | 22.725ms | 19.186ms | 25.94ms | 25 |
| AuthzService | GetPolicyBundle | read_only | 9.282ms | 11.012ms | 9.162ms | 7.515ms | 11.347ms | 25 |
| AuthzService | GetPolicyRule | read_only | 1.068ms | 1.622ms | 1.156ms | 530µs | 1.675ms | 25 |
| AuthzService | GetRole | read_only | 1.062ms | 1.612ms | 1.156ms | 566µs | 1.684ms | 25 |
| AuthzService | InvalidatePolicyBundles | destructive | 1.058ms | 1.058ms | 1.058ms | 1.058ms | 1.058ms | 1 |
| AuthzService | LintAuthzPolicies | read_only | 1.585ms | 2.801ms | 2.052ms | 858µs | 16.107ms | 25 |
| AuthzService | ListAccessDecisionAudits | read_only | 7.587ms | 13.088ms | 8.23ms | 4.502ms | 20.77ms | 25 |
| AuthzService | ListPolicyRules | read_only | 3.723ms | 6.129ms | 3.982ms | 2.66ms | 6.327ms | 25 |
| AuthzService | ListPolicyVersions | read_only | 6.933ms | 8.263ms | 7.051ms | 5.192ms | 9.241ms | 25 |
| AuthzService | ListRoles | read_only | 3.9ms | 5.382ms | 4.092ms | 2.894ms | 6.264ms | 25 |
| AuthzService | ListUserPermissions | read_only | 1.328ms | 1.997ms | 1.328ms | 512µs | 2.073ms | 25 |
| AuthzService | ListUserRoles | read_only | 1.132ms | 1.964ms | 1.319ms | 505µs | 2.607ms | 25 |
| AuthzService | MigrateLegacyPolicies | destructive | 2.207ms | 2.207ms | 2.207ms | 2.207ms | 2.207ms | 1 |
| AuthzService | PromoteCanary | destructive | 1.064ms | 1.064ms | 1.064ms | 1.064ms | 1.064ms | 1 |
| AuthzService | PutAuthzPolicy | mutation | 1.053ms | 1.576ms | 1.234ms | 897µs | 1.592ms | 5 |
| AuthzService | PutRelationship | mutation | 35.815ms | 38.424ms | 35.892ms | 31.099ms | 38.753ms | 5 |
| AuthzService | PutRoleBinding | mutation | 27.552ms | 27.653ms | 26.573ms | 20.781ms | 30.163ms | 5 |
| AuthzService | RejectPolicyDraft | mutation | 6.767ms | 7.453ms | 6.981ms | 5.597ms | 8.346ms | 5 |
| AuthzService | RevokeRole | mutation | 1.09ms | 1.529ms | 1.274ms | 1.061ms | 1.623ms | 5 |
| AuthzService | RollbackPolicyVersion | destructive | 1.061ms | 1.061ms | 1.061ms | 1.061ms | 1.061ms | 1 |
| AuthzService | SeedBuiltinRoles | mutation | 6.771ms | 7.24ms | 7.029ms | 6.186ms | 8.394ms | 5 |
| AuthzService | SimulatePolicy | mutation | 8.936ms | 10.225ms | 8.991ms | 6.569ms | 11.33ms | 5 |
| AuthzService | SubmitPolicyDraft | mutation | 8.436ms | 9.541ms | 8.721ms | 7.456ms | 10.01ms | 5 |
| AuthzService | UpdatePolicyDraft | mutation | 8.471ms | 8.887ms | 8.032ms | 5.557ms | 11.382ms | 5 |
| AuthzService | UpdateRole | mutation | 1.107ms | 1.332ms | 1.195ms | 1.039ms | 1.45ms | 5 |
| ControlPlaneService | AckStatus | mutation | 1.586ms | 1.734ms | 1.558ms | 1.053ms | 2.356ms | 5 |
| ControlPlaneService | DeltaResources | mutation | 1.145ms | 1.645ms | 1.435ms | 1.078ms | 2.216ms | 5 |
| ControlPlaneService | GetResources | read_only | 1.069ms | 1.66ms | 1.195ms | 577µs | 1.803ms | 25 |
| ControlPlaneService | ListNodeStates | read_only | 25.071ms | 30.802ms | 25.919ms | 21.461ms | 32.476ms | 25 |
| ControlPlaneService | StreamResources | mutation | 1.617ms | 1.693ms | 1.457ms | 1.056ms | 1.852ms | 5 |
| DataBroker | ActivateCatalog | destructive | 6.158ms | 6.158ms | 6.158ms | 6.158ms | 6.158ms | 1 |
| DataBroker | AnalyticalQuery | read_only | 3.439ms | 4.801ms | 3.633ms | 2.68ms | 4.822ms | 25 |
| DataBroker | ApplyMigration | mutation | 24.691ms | 25.25ms | 25.549ms | 17.895ms | 37.053ms | 5 |
| DataBroker | ApproveMigrationPlan | mutation | 13.877ms | 15.422ms | 12.525ms | 2.96ms | 17.795ms | 5 |
| DataBroker | BatchSelect | mutation | 2.871ms | 3.468ms | 2.992ms | 2.216ms | 3.674ms | 5 |
| DataBroker | BatchUpsert | mutation | 3.173ms | 4.395ms | 3.619ms | 2.638ms | 4.961ms | 5 |
| DataBroker | BeginTx | mutation | 18.144ms | 27.237ms | 23.172ms | 15.046ms | 37.759ms | 5 |
| DataBroker | CacheDelete | mutation | 2.655ms | 2.694ms | 2.548ms | 2.129ms | 2.749ms | 5 |
| DataBroker | CacheGet | read_only | 2.952ms | 4.864ms | 3.301ms | 2.187ms | 6.424ms | 25 |
| DataBroker | CacheScan | read_only | 3.618ms | 5.827ms | 3.869ms | 1.581ms | 7.134ms | 25 |
| DataBroker | CacheSet | mutation | 3.423ms | 3.652ms | 3.371ms | 2.775ms | 4.182ms | 5 |
| DataBroker | CreateMaterializedView | mutation | 3.44ms | 3.58ms | 3.221ms | 2.666ms | 3.739ms | 5 |
| DataBroker | Delete | mutation | 3.581ms | 3.817ms | 3.653ms | 3.233ms | 4.232ms | 5 |
| DataBroker | DeletePolicy | mutation | 6.64ms | 7.319ms | 6.477ms | 4.357ms | 7.901ms | 5 |
| DataBroker | DismissDlqEvent | mutation | 3.897ms | 4.83ms | 3.812ms | 2.161ms | 4.965ms | 5 |
| DataBroker | DocumentDelete | mutation | 2.906ms | 3.131ms | 2.953ms | 2.648ms | 3.262ms | 5 |
| DataBroker | DocumentFind | read_only | 3.249ms | 4.473ms | 3.176ms | 1.629ms | 4.625ms | 25 |
| DataBroker | DocumentGet | read_only | 3.189ms | 3.838ms | 3.102ms | 2.197ms | 3.864ms | 25 |
| DataBroker | DocumentUpsert | mutation | 4.173ms | 4.265ms | 3.931ms | 3.068ms | 4.74ms | 5 |
| DataBroker | DropResource | destructive | 2.117ms | 2.117ms | 2.117ms | 2.117ms | 2.117ms | 1 |
| DataBroker | EnqueueOutboxEvent | mutation | 8.013ms | 8.323ms | 7.741ms | 6.488ms | 8.858ms | 5 |
| DataBroker | EnsureProject | mutation | 17.289ms | 19.573ms | 16.698ms | 13.369ms | 19.831ms | 5 |
| DataBroker | EnsureResource | mutation | 2.951ms | 3.039ms | 2.871ms | 2.201ms | 3.227ms | 5 |
| DataBroker | GeneratePresignedUrl | mutation | 3.937ms | 3.981ms | 3.718ms | 2.692ms | 4.072ms | 5 |
| DataBroker | GenericDispatch | mutation | 3.2ms | 4.813ms | 3.739ms | 2.615ms | 5.41ms | 5 |
| DataBroker | GetAdminSummary | read_only | 34.319ms | 55.555ms | 37.046ms | 19.282ms | 87.423ms | 25 |
| DataBroker | GetCapabilities | read_only | 8.627ms | 25.99ms | 10.442ms | 5.819ms | 29.53ms | 25 |
| DataBroker | GetCatalogManifest | read_only | 166.004ms | 204.824ms | 167.356ms | 138.442ms | 217.593ms | 25 |
| DataBroker | GetCatalogVersion | read_only | 5.421ms | 7.198ms | 5.589ms | 4.265ms | 10.426ms | 25 |
| DataBroker | GetCatalogVersions | read_only | 4.757ms | 12.741ms | 5.759ms | 3.594ms | 13.169ms | 25 |
| DataBroker | GetCdcStatus | read_only | 4.574ms | 6.053ms | 4.764ms | 3.832ms | 6.667ms | 25 |
| DataBroker | GetDlqEvent | read_only | 3.456ms | 4.712ms | 3.53ms | 2.301ms | 4.953ms | 25 |
| DataBroker | GetHealthReport | read_only | 28.645ms | 43.007ms | 30.656ms | 17.883ms | 51.392ms | 25 |
| DataBroker | GetMigrationStatus | read_only | 5.152ms | 6.677ms | 5.044ms | 2.909ms | 8.919ms | 25 |
| DataBroker | GetObject | read_only | 4.933ms | 5.954ms | 4.824ms | 3.265ms | 6.115ms | 25 |
| DataBroker | GetSaga | read_only | 4.034ms | 6.467ms | 4.379ms | 3.166ms | 6.821ms | 25 |
| DataBroker | GraphMutate | mutation | 4.601ms | 4.877ms | 4.628ms | 4.208ms | 4.967ms | 5 |
| DataBroker | GraphQuery | read_only | 4.467ms | 6.407ms | 4.767ms | 3.261ms | 7.076ms | 25 |
| DataBroker | InitiateMultipartUpload | mutation | 4.784ms | 5.312ms | 4.81ms | 4.007ms | 5.468ms | 5 |
| DataBroker | LintPolicies | read_only | 7.91ms | 11.489ms | 8.477ms | 6.015ms | 12.165ms | 25 |
| DataBroker | ListAdminAuditLogs | read_only | 8.679ms | 17.614ms | 10.107ms | 6.484ms | 21.616ms | 25 |
| DataBroker | ListDlqEvents | read_only | 4.513ms | 5.878ms | 4.476ms | 2.658ms | 6.087ms | 25 |
| DataBroker | ListMessageSchemas | read_only | 3.954ms | 5.674ms | 4.307ms | 2.687ms | 10.538ms | 25 |
| DataBroker | ListMigrationRuns | read_only | 7.438ms | 10.982ms | 7.652ms | 5.336ms | 11.836ms | 25 |
| DataBroker | ListPolicies | read_only | 8.469ms | 19.627ms | 9.809ms | 5.374ms | 29.076ms | 25 |
| DataBroker | ListProjects | read_only | 7.105ms | 12.739ms | 8.222ms | 6.132ms | 13.249ms | 25 |
| DataBroker | ListResources | read_only | 3.287ms | 4.572ms | 3.375ms | 2.422ms | 4.596ms | 25 |
| DataBroker | ListSagas | read_only | 3.272ms | 4.872ms | 3.432ms | 2.573ms | 5.335ms | 25 |
| DataBroker | LookupMessageSchema | read_only | 3.266ms | 4.104ms | 3.388ms | 2.346ms | 4.246ms | 25 |
| DataBroker | MarkSagaReviewed | mutation | 3.482ms | 3.491ms | 3.272ms | 2.638ms | 3.545ms | 5 |
| DataBroker | PauseCdc | mutation | 16.815ms | 17.183ms | 17.343ms | 15.571ms | 21.065ms | 5 |
| DataBroker | PlanMigration | mutation | 14.604ms | 16.609ms | 16.054ms | 13.326ms | 21.331ms | 5 |
| DataBroker | PreviewCdcRedaction | read_only | 15.019ms | 23.277ms | 15.532ms | 12.14ms | 23.706ms | 25 |
| DataBroker | PublishCDC | mutation | 20.003809s | 20.017063s | 20.008291s | 20.000733s | 20.019082s | 5 |
| DataBroker | PutObject | mutation | 3.163ms | 3.438ms | 3.128ms | 2.644ms | 3.69ms | 5 |
| DataBroker | PutPolicy | destructive | 4.846ms | 4.846ms | 4.846ms | 4.846ms | 4.846ms | 1 |
| DataBroker | QuarantineDlqEvent | mutation | 3.222ms | 3.379ms | 3.311ms | 2.993ms | 3.783ms | 5 |
| DataBroker | ReloadPolicies | destructive | 14.457ms | 14.457ms | 14.457ms | 14.457ms | 14.457ms | 1 |
| DataBroker | ReplayDlqEvent | mutation | 2.678ms | 2.703ms | 2.612ms | 2.097ms | 3.423ms | 5 |
| DataBroker | ResumeCdc | mutation | 19.097ms | 33.197ms | 22.988ms | 13.667ms | 34.312ms | 5 |
| DataBroker | RetrySagaCompensation | mutation | 2.711ms | 2.835ms | 2.641ms | 2.147ms | 2.871ms | 5 |
| DataBroker | RollbackCatalog | destructive | 6.523ms | 6.523ms | 6.523ms | 6.523ms | 6.523ms | 1 |
| DataBroker | ScanProjectionDrift | read_only | 2.912ms | 3.84ms | 3.007ms | 2.098ms | 4ms | 25 |
| DataBroker | Select | read_only | 3.563ms | 4.309ms | 3.485ms | 2.585ms | 4.543ms | 25 |
| DataBroker | SelectV2 | read_only | 3.834ms | 5.264ms | 4.021ms | 2.857ms | 5.677ms | 25 |
| DataBroker | StageCatalog | destructive | 3.203ms | 3.203ms | 3.203ms | 3.203ms | 3.203ms | 1 |
| DataBroker | StepDownCdcLeader | mutation | 13.023ms | 13.932ms | 12.903ms | 10.77ms | 15.459ms | 5 |
| DataBroker | TimeSeriesQuery | read_only | 3.075ms | 3.829ms | 2.946ms | 1.564ms | 4.381ms | 25 |
| DataBroker | TimeSeriesWrite | mutation | 3.118ms | 3.201ms | 3.13ms | 2.866ms | 3.46ms | 5 |
| DataBroker | Upsert | mutation | 3.166ms | 3.323ms | 3.309ms | 2.621ms | 4.29ms | 5 |
| DataBroker | ValidateCatalog | destructive | 2.639ms | 2.639ms | 2.639ms | 2.639ms | 2.639ms | 1 |
| DataBroker | VectorBatchUpsert | mutation | 3.196ms | 3.221ms | 3.142ms | 2.679ms | 3.527ms | 5 |
| DataBroker | VectorHybridSearch | read_only | 3.308ms | 4.087ms | 3.361ms | 2.646ms | 4.915ms | 25 |
| DataBroker | VectorSearch | read_only | 3.437ms | 5.768ms | 3.573ms | 2.639ms | 5.835ms | 25 |
| DataBroker | VectorUpsert | mutation | 3.13ms | 3.246ms | 3.173ms | 2.887ms | 3.682ms | 5 |
| DataBroker | VerifyAdminAuditLog | read_only | 9.979ms | 15.13ms | 10.227ms | 6.537ms | 15.805ms | 25 |
| IdentityProviderService | CreateProvider | mutation | 1.594ms | 1.635ms | 1.661ms | 1.056ms | 2.44ms | 5 |
| IdentityProviderService | DisableProvider | mutation | 1.618ms | 1.817ms | 1.732ms | 1.559ms | 2.106ms | 5 |
| IdentityProviderService | ForceJwksRefresh | mutation | 1.607ms | 1.629ms | 1.438ms | 1.072ms | 1.809ms | 5 |
| IdentityProviderService | GetProvider | read_only | 1.564ms | 1.85ms | 1.487ms | 1.05ms | 2.106ms | 25 |
| IdentityProviderService | ImportSamlMetadata | mutation | 1.606ms | 1.655ms | 1.611ms | 1.067ms | 2.214ms | 5 |
| IdentityProviderService | LinkIdentity | mutation | 1.608ms | 2.021ms | 1.587ms | 1.037ms | 2.205ms | 5 |
| IdentityProviderService | ListExternalIdentities | read_only | 7.16ms | 12.335ms | 7.97ms | 5.239ms | 14.11ms | 25 |
| IdentityProviderService | ListProviders | read_only | 7.97ms | 13.367ms | 8.521ms | 4.164ms | 16.198ms | 25 |
| IdentityProviderService | PreviewClaimMapping | read_only | 2.164ms | 2.802ms | 2.129ms | 1.602ms | 2.856ms | 25 |
| IdentityProviderService | PreviewGroupMapping | read_only | 2.104ms | 2.737ms | 1.999ms | 1.068ms | 3.114ms | 25 |
| IdentityProviderService | ResolveExternalIdentity | mutation | 1.065ms | 1.619ms | 1.387ms | 1.051ms | 2.143ms | 5 |
| IdentityProviderService | SamlAcs | mutation | 1.604ms | 1.635ms | 1.619ms | 1.068ms | 2.186ms | 5 |
| IdentityProviderService | ScimCreateGroup | mutation | 1.595ms | 1.599ms | 1.432ms | 1.047ms | 1.814ms | 5 |
| IdentityProviderService | ScimCreateUser | mutation | 1.161ms | 1.995ms | 1.482ms | 1.009ms | 2.188ms | 5 |
| IdentityProviderService | ScimDeleteGroup | mutation | 1.345ms | 1.599ms | 1.48ms | 1.073ms | 2.269ms | 5 |
| IdentityProviderService | ScimDeleteUser | mutation | 1.058ms | 1.084ms | 1.177ms | 1.049ms | 1.641ms | 5 |
| IdentityProviderService | ScimGetGroup | mutation | 1.079ms | 1.1ms | 1.177ms | 1.058ms | 1.59ms | 5 |
| IdentityProviderService | ScimGetUser | mutation | 1.07ms | 1.599ms | 1.308ms | 1.046ms | 1.771ms | 5 |
| IdentityProviderService | ScimListGroups | mutation | 1.158ms | 1.616ms | 1.321ms | 1.101ms | 1.621ms | 5 |
| IdentityProviderService | ScimListUsers | mutation | 1.098ms | 1.585ms | 1.284ms | 1.057ms | 1.614ms | 5 |
| IdentityProviderService | ScimPatchGroup | mutation | 1.065ms | 1.574ms | 1.261ms | 1.036ms | 1.59ms | 5 |
| IdentityProviderService | ScimPatchUser | mutation | 1.107ms | 1.483ms | 1.263ms | 1.059ms | 1.596ms | 5 |
| IdentityProviderService | ScimReplaceUser | mutation | 1.292ms | 1.611ms | 1.295ms | 886µs | 1.642ms | 5 |
| IdentityProviderService | StartSamlLogin | mutation | 1.568ms | 1.597ms | 1.388ms | 1.039ms | 1.617ms | 5 |
| IdentityProviderService | TestProviderDiscovery | read_only | 1.082ms | 1.642ms | 1.225ms | 525µs | 1.727ms | 25 |
| IdentityProviderService | UnlinkIdentity | mutation | 1.076ms | 1.513ms | 1.266ms | 1.057ms | 1.623ms | 5 |
| IdentityProviderService | UpdateProvider | mutation | 1.102ms | 1.583ms | 1.313ms | 958µs | 1.821ms | 5 |
| NotificationService | GetDeliveryStats | read_only | 3.924ms | 11.563ms | 4.785ms | 2.033ms | 12.833ms | 25 |
| NotificationService | GetNotification | read_only | 1.461ms | 2.217ms | 1.427ms | 525µs | 2.236ms | 25 |
| NotificationService | GetPreference | read_only | 1.788ms | 2.997ms | 1.98ms | 1.07ms | 3.34ms | 25 |
| NotificationService | GetTemplate | read_only | 12.35ms | 16.413ms | 12.797ms | 10.248ms | 17.755ms | 25 |
| NotificationService | ListNotifications | read_only | 10.295ms | 15.286ms | 10.993ms | 8.654ms | 15.476ms | 25 |
| NotificationService | ListPreferences | read_only | 1.602ms | 2.191ms | 1.633ms | 1.043ms | 2.264ms | 25 |
| NotificationService | ListTemplates | read_only | 23.542ms | 31.484ms | 23.872ms | 18.821ms | 33ms | 25 |
| NotificationService | RetryNotification | mutation | 1.094ms | 1.192ms | 1.32ms | 1.05ms | 2.206ms | 5 |
| NotificationService | SendNotification | mutation | 1.378ms | 1.715ms | 1.456ms | 1.087ms | 1.757ms | 5 |
| NotificationService | SetPreference | mutation | 2.266ms | 2.286ms | 1.986ms | 1.054ms | 2.695ms | 5 |
| NotificationService | UpsertTemplate | mutation | 6.029ms | 7.018ms | 6.593ms | 5.898ms | 8.042ms | 5 |
| PeerService | GetPeer | read_only | 2.798ms | 3.614ms | 2.74ms | 1.699ms | 4.223ms | 25 |
| PeerService | JoinRoom | mutation | 2.687ms | 2.928ms | 2.711ms | 2.118ms | 3.157ms | 5 |
| PeerService | LeaveRoom | mutation | 2.789ms | 2.875ms | 2.922ms | 2.414ms | 3.843ms | 5 |
| PeerService | ListPeers | read_only | 2.888ms | 4.347ms | 2.976ms | 1.595ms | 4.466ms | 25 |
| RoomService | CloseRoom | mutation | 2.541ms | 3.023ms | 2.669ms | 1.576ms | 3.781ms | 5 |
| RoomService | CreateRoom | mutation | 2.686ms | 2.698ms | 2.533ms | 1.675ms | 3.416ms | 5 |
| RoomService | GetRoom | read_only | 2.53ms | 4.096ms | 2.588ms | 1.577ms | 4.573ms | 25 |
| RoomService | ListRooms | read_only | 2.916ms | 3.985ms | 3.019ms | 1.978ms | 6.818ms | 25 |
| RoomService | UpdateRoom | mutation | 2.272ms | 2.288ms | 2.136ms | 1.709ms | 2.691ms | 5 |
| SignalingService | Signal | mutation | 3.311ms | 3.783ms | 3.399ms | 2.107ms | 5.05ms | 5 |
| StorageService | DeleteFile | mutation | 2.352ms | 2.407ms | 2.139ms | 511µs | 3.417ms | 5 |
| StorageService | FinalizeUpload | mutation | 1.56ms | 1.84ms | 1.544ms | 1.103ms | 2.086ms | 5 |
| StorageService | GetDownloadUrl | read_only | 4.158ms | 6.214ms | 4.202ms | 1.049ms | 7.328ms | 25 |
| StorageService | GetFile | read_only | 7.45ms | 27.67ms | 10.044ms | 3.693ms | 37.048ms | 25 |
| StorageService | ListFiles | read_only | 5.192ms | 16.311ms | 5.875ms | 2.752ms | 16.791ms | 25 |
| StorageService | RegisterUpload | mutation | 4.526ms | 4.982ms | 4.457ms | 3.034ms | 6.083ms | 5 |
| StorageService | UpdateFile | mutation | 3.871ms | 4.218ms | 3.982ms | 3.348ms | 4.914ms | 5 |
| TenantService | CreateTenant | mutation | 3.232ms | 3.55ms | 3.22ms | 2.269ms | 3.858ms | 5 |
| TenantService | GetTenant | read_only | 22.833ms | 64.85ms | 29.582ms | 16.859ms | 122.048ms | 25 |
| TenantService | GetTenantConfig | read_only | 19.045ms | 22.149ms | 18.399ms | 12.629ms | 29.586ms | 25 |
| TenantService | ListTenants | read_only | 2.236ms | 3.675ms | 2.382ms | 1.24ms | 4.362ms | 25 |
| TenantService | UpdateTenant | mutation | 2.386ms | 2.413ms | 2.313ms | 1.72ms | 2.749ms | 5 |
| TenantService | UpdateTenantConfig | mutation | 3.006ms | 3.01ms | 2.871ms | 2.138ms | 3.455ms | 5 |
| TrackService | ListTracks | read_only | 2.944ms | 4.178ms | 3.046ms | 1.597ms | 4.239ms | 25 |
| TrackService | MuteTrack | mutation | 3.19ms | 3.296ms | 3.08ms | 2.175ms | 3.964ms | 5 |
| TrackService | PublishTrack | mutation | 3.872ms | 3.877ms | 3.477ms | 2.825ms | 3.937ms | 5 |
| TrackService | UnpublishTrack | mutation | 2.87ms | 3.311ms | 3.023ms | 2.323ms | 4.204ms | 5 |
| TurnService | IssueCredentials | mutation | 2.546ms | 2.789ms | 2.692ms | 2.201ms | 3.533ms | 5 |
