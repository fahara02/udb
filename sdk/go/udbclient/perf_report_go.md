# UDB SDK Live Perf — Go (localhost)

RPCs measured: 262   tenant=4c8ced75-271d-4e0b-ada5-640e3c0b1e27

Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities (a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request's reference/ID fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection latency. The TARGET is zero failures; any residual non-OK RPC is listed under Failures for the maintainer to finish.

Unary RPCs = full request→response round-trip. Non-CDC streaming RPCs report time-to-FIRST-RESPONSE with seeded inputs. CDC subscription (PublishCDC) reports time-to-FIRST-EVENT: the harness subscribes, fires a real Upsert that flows outbox→CDC→Kafka, and times the first delivered event. Streaming rows are marked in the note column.

## Seeded fixtures

Captured semantic field → seeded value keys used to resolve request fields: action, apply_run_id, approval_token, approve_draft_id, approve_run_id, approved_by, asset_id, assigned_by, auth_challenge_id, auth_token, bucket, canary_id, canary_version_id, catalog_manifest, challenge_id, close_room_id, code, collection, content_type, created_by, csrf_token, definition_id, delete_file_id, delete_policy_id, delete_role_id, delete_scim_user_id, deleted_by, device_id, disable_provider_id, dismiss_dlq_id, dlq_id, document_id, domain, ds_policy_id, event_type, external_identity_id, file_id, file_type, filename, instance_id, key_id, kind, leave_peer_id, locale, log_id, mark_saga_id, message_type, migration_id, mongo_collection, name, node_id, notification_id, object, object_key, otp_code, otp_id, owner_id, peer_id, plain_key, policy_draft_id, policy_id, policy_version_id, project, project_id, provider_id, quarantine_dlq_id, recipient_id, record_id, recovery_code, refresh_session_id, refresh_token, reg_challenge_id, reject_draft_id, rejected_by, relation, replay_dlq_id, reset_otp_code, reset_otp_id, resource, retry_saga_id, revoke_key_id, revoked_by, role, role_code, role_id, rollback_policy_set_id, rollback_target_version_id, room_id, saga_id, saml_provider_id, scim_group_id, scim_user_id, session_id, signal_peer_id, stage_name, step_id, subject, tenant, tenant_id, token, topic_pattern, track_id, ts_table, unpublish_track_id, update_draft_id, update_key_id, updated_by, user_id, user_role_id, username

## Per-service mean latency (mean of per-RPC means)

| Service | RPCs | mean |
|---|---:|---:|
| AuthnService | 50 | 69.949ms |
| DataBroker | 76 | 22.025ms |
| AuthzService | 41 | 21.831ms |
| IdentityProviderService | 27 | 20.577ms |
| StorageService | 7 | 19.38ms |
| ApiKeyService | 9 | 14.693ms |
| ControlPlaneService | 5 | 26.439ms |
| NotificationService | 11 | 11.299ms |
| AssetService | 8 | 12.921ms |
| TenantService | 6 | 10.056ms |
| RoomService | 5 | 10.707ms |
| PeerService | 4 | 11.497ms |
| AnalyticsService | 7 | 5.831ms |
| TrackService | 4 | 10.127ms |
| SignalingService | 1 | 27.124ms |
| TurnService | 1 | 4.243ms |

## Failures — still to fix (0)

No RPC returned a non-OK gRPC status — every RPC ran its success path.

## Slowest 25 RPCs by p99

| RPC | kind | err | p50 | p99 | mean | iters | note |
|---|---|---|---:|---:|---:|---:|---|
| AuthnService/ChangePassword | mutation | OK | 986.704ms | 986.704ms | 986.704ms | 5 | mutation (seeded success path) |
| AuthnService/ResetPassword | mutation | OK | 740.588ms | 740.588ms | 740.588ms | 5 | mutation (seeded success path) |
| AuthnService/CreateUser | mutation | OK | 494.589ms | 494.589ms | 494.589ms | 5 | mutation (seeded success path) |
| AuthnService/Login | mutation | OK | 378.113ms | 404.092ms | 394.182ms | 5 | mutation (seeded success path) |
| DataBroker/StageCatalog | destructive | OK | 345.665ms | 345.665ms | 345.665ms | 1 | destructive: 1 real call against a seeded disposable target |
| DataBroker/PublishCDC | mutation | OK | 245.62ms | 245.62ms | 189.322ms | 3 | cdc subscription: time-to-first-event (real mutation produced) |
| DataBroker/ApplyMigration | mutation | OK | 238.082ms | 238.082ms | 238.082ms | 5 | mutation (seeded success path) |
| AuthnService/DisableMfaFactor | mutation | OK | 169.308ms | 181.511ms | 134.97ms | 5 | mutation (seeded success path) |
| AuthzService/RollbackPolicyVersion | destructive | OK | 90.981ms | 90.981ms | 90.981ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthnService/EmergencyRevoke | destructive | OK | 90.161ms | 90.161ms | 90.161ms | 1 | destructive: 1 real call against a seeded disposable target |
| IdentityProviderService/SamlAcs | mutation | OK | 73.345ms | 75.984ms | 136.649ms | 5 | mutation (seeded success path) |
| AuthzService/SeedBuiltinRoles | mutation | OK | 68.315ms | 73.918ms | 67.92ms | 5 | mutation (seeded success path) |
| AuthzService/ActivatePolicyVersion | destructive | OK | 73.609ms | 73.609ms | 73.609ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthzService/PromoteCanary | destructive | OK | 63.221ms | 63.221ms | 63.221ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthnService/FinishWebAuthnAuthentication | mutation | OK | 57.93ms | 57.93ms | 57.93ms | 5 | mutation (seeded success path) |
| ApiKeyService/EmergencyRevokeApiKeys | destructive | OK | 57.86ms | 57.86ms | 57.86ms | 1 | destructive: 1 real call against a seeded disposable target |
| IdentityProviderService/ForceJwksRefresh | mutation | OK | 52.818ms | 55.48ms | 46.557ms | 5 | mutation (seeded success path) |
| ControlPlaneService/StreamResources | mutation | OK | 38.848ms | 53.129ms | 47.653ms | 5 | streaming: time-to-first-response (seeded; bidi) |
| AuthnService/AdminResetMfa | destructive | OK | 49.925ms | 49.925ms | 49.925ms | 1 | destructive: 1 real call against a seeded disposable target |
| IdentityProviderService/ScimPatchUser | mutation | OK | 41.077ms | 48.639ms | 45.374ms | 5 | mutation (seeded success path) |
| AuthzService/ApprovePolicyDraft | mutation | OK | 45.863ms | 45.863ms | 45.863ms | 5 | mutation (seeded success path) |
| DataBroker/Upsert | mutation | OK | 41.731ms | 45.51ms | 41.597ms | 5 | mutation (seeded success path) |
| DataBroker/BeginTx | mutation | OK | 28.015ms | 44.812ms | 32.775ms | 5 | streaming: time-to-first-response (seeded; bidi) |
| ControlPlaneService/DeltaResources | mutation | OK | 42.579ms | 44.548ms | 45.186ms | 5 | streaming: time-to-first-response (seeded; bidi) |
| AuthzService/ActivateCanary | destructive | OK | 43.781ms | 43.781ms | 43.781ms | 1 | destructive: 1 real call against a seeded disposable target |

## Full per-RPC table (sorted by service, then name)

| Service | RPC | kind | err | p50 | p99 | mean | min | max | iters |
|---|---|---|---|---:|---:|---:|---:|---:|---:|
| AnalyticsService | GetExecutorPerformance | read_only | OK | 4.302ms | 10.16ms | 5.36ms | 3.041ms | 10.946ms | 25 |
| AnalyticsService | GetPipelineSummary | read_only | OK | 4.39ms | 9.543ms | 5.377ms | 3.132ms | 9.606ms | 25 |
| AnalyticsService | GetReconciliationAnalytics | read_only | OK | 4.179ms | 9.017ms | 4.94ms | 2.547ms | 9.586ms | 25 |
| AnalyticsService | GetSlaCompliance | read_only | OK | 4.147ms | 6.712ms | 4.36ms | 1.035ms | 9.175ms | 25 |
| AnalyticsService | GetThroughput | read_only | OK | 4.17ms | 5.439ms | 4.213ms | 2.013ms | 8.485ms | 25 |
| AnalyticsService | RecordPipelineMetric | mutation | OK | 6.852ms | 7.34ms | 8.804ms | 6.41ms | 16.916ms | 5 |
| AnalyticsService | TriggerSnapshot | mutation | OK | 4.962ms | 7.888ms | 7.762ms | 4.679ms | 16.46ms | 5 |
| ApiKeyService | CreateApiKey | mutation | OK | 9.45ms | 10.134ms | 10.187ms | 8.398ms | 14.444ms | 5 |
| ApiKeyService | EmergencyRevokeApiKeys | destructive | OK | 57.86ms | 57.86ms | 57.86ms | 57.86ms | 57.86ms | 1 |
| ApiKeyService | GetApiKey | read_only | OK | 3.678ms | 6.227ms | 3.997ms | 1.553ms | 6.765ms | 25 |
| ApiKeyService | GetApiKeyUsageStats | read_only | OK | 4.713ms | 8.952ms | 5.427ms | 3.661ms | 9.567ms | 25 |
| ApiKeyService | ListApiKeys | read_only | OK | 4.177ms | 4.756ms | 3.986ms | 3.1ms | 5.617ms | 25 |
| ApiKeyService | RevokeApiKey | mutation | OK | 10.504ms | 10.504ms | 10.504ms | 10.504ms | 10.504ms | 5 |
| ApiKeyService | RotateApiKey | mutation | OK | 17.617ms | 17.617ms | 17.617ms | 17.617ms | 17.617ms | 5 |
| ApiKeyService | UpdateApiKey | mutation | OK | 10.513ms | 12.509ms | 13.65ms | 10.043ms | 24.71ms | 5 |
| ApiKeyService | ValidateApiKey | read_only | OK | 6.813ms | 12.434ms | 9.01ms | 5.547ms | 53.951ms | 25 |
| AssetService | CompleteStep | mutation | OK | 18.927ms | 19.062ms | 21.566ms | 18.058ms | 33.383ms | 5 |
| AssetService | CreatePipelineDefinition | mutation | OK | 23.788ms | 23.788ms | 23.788ms | 23.788ms | 23.788ms | 5 |
| AssetService | GetAsset | read_only | OK | 8.15ms | 10.857ms | 8.265ms | 6.835ms | 11.099ms | 25 |
| AssetService | GetPipeline | read_only | OK | 7.398ms | 9.295ms | 7.538ms | 6.299ms | 9.355ms | 25 |
| AssetService | GetPipelineDefinition | read_only | OK | 7.4ms | 11.62ms | 7.927ms | 5.89ms | 11.868ms | 25 |
| AssetService | ListAssets | read_only | OK | 9.017ms | 12.632ms | 9.276ms | 5.956ms | 14.187ms | 25 |
| AssetService | RegisterAsset | mutation | OK | 14.066ms | 15.487ms | 15.491ms | 12.118ms | 23.174ms | 5 |
| AssetService | StartPipeline | mutation | OK | 4.467ms | 4.839ms | 9.517ms | 3.692ms | 30.377ms | 5 |
| AuthnService | AdminResetMfa | destructive | OK | 49.925ms | 49.925ms | 49.925ms | 49.925ms | 49.925ms | 1 |
| AuthnService | AdminResetPassword | destructive | OK | 9.118ms | 9.118ms | 9.118ms | 9.118ms | 9.118ms | 1 |
| AuthnService | AdminRevokeAllTenantSessions | destructive | OK | 22.536ms | 22.536ms | 22.536ms | 22.536ms | 22.536ms | 1 |
| AuthnService | AdminRevokeAllUserSessions | destructive | OK | 28.512ms | 28.512ms | 28.512ms | 28.512ms | 28.512ms | 1 |
| AuthnService | AdminRevokeSession | destructive | OK | 31.914ms | 31.914ms | 31.914ms | 31.914ms | 31.914ms | 1 |
| AuthnService | Authenticate | read_only | OK | 15.099ms | 18.489ms | 15.284ms | 12.592ms | 19.861ms | 25 |
| AuthnService | ChangePassword | mutation | OK | 986.704ms | 986.704ms | 986.704ms | 986.704ms | 986.704ms | 5 |
| AuthnService | ChangeUserStatus | destructive | OK | 28.932ms | 28.932ms | 28.932ms | 28.932ms | 28.932ms | 1 |
| AuthnService | ConfirmMFAEnrollment | mutation | OK | 3.389ms | 3.672ms | 3.64ms | 2.647ms | 5.302ms | 5 |
| AuthnService | CreateSession | mutation | OK | 8.261ms | 8.426ms | 7.324ms | 5.346ms | 8.45ms | 5 |
| AuthnService | CreateUser | mutation | OK | 494.589ms | 494.589ms | 494.589ms | 494.589ms | 494.589ms | 5 |
| AuthnService | DeleteWebAuthnCredential | mutation | OK | 7.622ms | 7.844ms | 7.62ms | 7.209ms | 7.863ms | 5 |
| AuthnService | DisableMfaFactor | mutation | OK | 169.308ms | 181.511ms | 134.97ms | 18.862ms | 211.62ms | 5 |
| AuthnService | EmergencyRevoke | destructive | OK | 90.161ms | 90.161ms | 90.161ms | 90.161ms | 90.161ms | 1 |
| AuthnService | EnrollMFA | mutation | OK | 10.484ms | 11.795ms | 14.149ms | 9.552ms | 28.651ms | 5 |
| AuthnService | FinishWebAuthnAuthentication | mutation | OK | 57.93ms | 57.93ms | 57.93ms | 57.93ms | 57.93ms | 5 |
| AuthnService | FinishWebAuthnRegistration | mutation | OK | 29.96ms | 29.96ms | 29.96ms | 29.96ms | 29.96ms | 5 |
| AuthnService | ForgotPassword | mutation | OK | 15.812ms | 16.489ms | 16.016ms | 14.99ms | 17.032ms | 5 |
| AuthnService | GenerateRecoveryCodes | mutation | OK | 31.516ms | 31.81ms | 29.497ms | 22.114ms | 34.479ms | 5 |
| AuthnService | GetJwks | read_only | OK | 3.655ms | 6.359ms | 3.896ms | 2.623ms | 7.293ms | 25 |
| AuthnService | GetMfaPolicy | read_only | OK | 3.641ms | 4.713ms | 3.575ms | 2.593ms | 4.758ms | 25 |
| AuthnService | GetSession | read_only | OK | 3.648ms | 13.073ms | 4.783ms | 3.118ms | 14.059ms | 25 |
| AuthnService | GetUser | read_only | OK | 3.633ms | 6.369ms | 3.766ms | 2.63ms | 6.413ms | 25 |
| AuthnService | IntrospectToken | read_only | OK | 19.088ms | 23.515ms | 19.598ms | 16.829ms | 23.774ms | 25 |
| AuthnService | IssueMfaChallenge | mutation | OK | 9.499ms | 10.096ms | 11.91ms | 8.415ms | 22.233ms | 5 |
| AuthnService | ListDevices | read_only | OK | 3.683ms | 6.234ms | 4.225ms | 3.106ms | 6.45ms | 25 |
| AuthnService | ListMfaFactors | read_only | OK | 6.122ms | 9.722ms | 6.712ms | 2.011ms | 13.263ms | 25 |
| AuthnService | ListSessions | read_only | OK | 8.458ms | 10.792ms | 8.921ms | 6.97ms | 19.345ms | 25 |
| AuthnService | ListUsers | read_only | OK | 8.635ms | 14.958ms | 9.422ms | 6.695ms | 18.566ms | 25 |
| AuthnService | ListWebAuthnCredentials | read_only | OK | 4.757ms | 6.096ms | 4.982ms | 4.193ms | 6.421ms | 25 |
| AuthnService | Login | mutation | OK | 378.113ms | 404.092ms | 394.182ms | 361.603ms | 456.315ms | 5 |
| AuthnService | Logout | mutation | OK | 5.204ms | 5.921ms | 5.183ms | 3.802ms | 6.133ms | 5 |
| AuthnService | PutMfaPolicy | mutation | OK | 5.253ms | 5.371ms | 5.153ms | 4.345ms | 5.816ms | 5 |
| AuthnService | RefreshSession | mutation | OK | 13.272ms | 14.76ms | 16.959ms | 10.576ms | 34.699ms | 5 |
| AuthnService | RefreshToken | mutation | OK | 8.478ms | 8.478ms | 8.478ms | 8.478ms | 8.478ms | 5 |
| AuthnService | RenamePasskey | mutation | OK | 6.482ms | 6.566ms | 6.482ms | 5.774ms | 7.235ms | 5 |
| AuthnService | ResendOTP | mutation | OK | 24.734ms | 26.602ms | 21.057ms | 11.682ms | 28.649ms | 5 |
| AuthnService | ResetPassword | mutation | OK | 740.588ms | 740.588ms | 740.588ms | 740.588ms | 740.588ms | 5 |
| AuthnService | RevokeDevice | mutation | OK | 20.338ms | 20.338ms | 20.338ms | 20.338ms | 20.338ms | 5 |
| AuthnService | RevokeRecoveryCodes | mutation | OK | 9.497ms | 9.512ms | 9.618ms | 7.965ms | 12.441ms | 5 |
| AuthnService | RevokeSession | mutation | OK | 5.297ms | 7.494ms | 6.896ms | 4.248ms | 12.647ms | 5 |
| AuthnService | SendOTP | mutation | OK | 15.542ms | 16.9ms | 17.402ms | 13.421ms | 26.902ms | 5 |
| AuthnService | SendPhoneVerification | mutation | OK | 12.605ms | 23.534ms | 16.63ms | 11.688ms | 23.538ms | 5 |
| AuthnService | StartWebAuthnAuthentication | mutation | OK | 14.488ms | 16.308ms | 17.684ms | 9.851ms | 33.925ms | 5 |
| AuthnService | StartWebAuthnRegistration | mutation | OK | 13.223ms | 14.319ms | 13.679ms | 12.217ms | 15.438ms | 5 |
| AuthnService | UpdateUser | mutation | OK | 7.911ms | 9.382ms | 10.75ms | 7.493ms | 21.459ms | 5 |
| AuthnService | ValidateCSRF | read_only | OK | 4.427ms | 6.009ms | 4.622ms | 3.687ms | 6.051ms | 25 |
| AuthnService | ValidateToken | read_only | OK | 14.843ms | 17.064ms | 15.322ms | 11.878ms | 30.572ms | 25 |
| AuthnService | VerifyMfaChallenge | read_only | OK | 9.003ms | 10.368ms | 8.759ms | 5.754ms | 10.608ms | 25 |
| AuthnService | VerifyOTP | read_only | OK | 15.072ms | 28.829ms | 17.089ms | 11.079ms | 32.864ms | 25 |
| AuthzService | ActivateCanary | destructive | OK | 43.781ms | 43.781ms | 43.781ms | 43.781ms | 43.781ms | 1 |
| AuthzService | ActivatePolicyVersion | destructive | OK | 73.609ms | 73.609ms | 73.609ms | 73.609ms | 73.609ms | 1 |
| AuthzService | ApprovePolicyDraft | mutation | OK | 45.863ms | 45.863ms | 45.863ms | 45.863ms | 45.863ms | 5 |
| AuthzService | AssignRole | mutation | OK | 23.308ms | 31.452ms | 25.987ms | 20.587ms | 31.816ms | 5 |
| AuthzService | Authorize | read_only | OK | 18.397ms | 32.943ms | 19.953ms | 14.887ms | 36.228ms | 25 |
| AuthzService | BatchCheckPermissions | read_only | OK | 8.27ms | 24.774ms | 9.815ms | 6.755ms | 24.828ms | 25 |
| AuthzService | CheckAccess | read_only | OK | 7.899ms | 9.608ms | 8.019ms | 6.751ms | 10.565ms | 25 |
| AuthzService | CreatePolicyDraft | mutation | OK | 35.416ms | 37.636ms | 36.764ms | 28.572ms | 53.333ms | 5 |
| AuthzService | CreatePolicyRule | mutation | OK | 16.388ms | 17.245ms | 17.304ms | 14.493ms | 22.789ms | 5 |
| AuthzService | CreateRole | mutation | OK | 18.7ms | 18.7ms | 18.7ms | 18.7ms | 18.7ms | 5 |
| AuthzService | DeletePolicyRule | mutation | OK | 7.372ms | 7.402ms | 7.455ms | 6.958ms | 8.182ms | 5 |
| AuthzService | DeleteRole | mutation | OK | 8.421ms | 8.449ms | 12.59ms | 7.921ms | 29.986ms | 5 |
| AuthzService | DiffPolicyDraft | read_only | OK | 5.815ms | 14.193ms | 7.268ms | 5.207ms | 16.232ms | 25 |
| AuthzService | ExplainPolicy | read_only | OK | 2.077ms | 2.587ms | 1.917ms | 1.25ms | 2.778ms | 25 |
| AuthzService | GetAuthzRevision | read_only | OK | 3.265ms | 4.256ms | 3.419ms | 2.605ms | 4.914ms | 25 |
| AuthzService | GetCanaryStatus | read_only | OK | 4.178ms | 5.396ms | 4.395ms | 3.101ms | 10.703ms | 25 |
| AuthzService | GetNativeAccess | read_only | OK | 17.837ms | 33.029ms | 20.06ms | 14.414ms | 36.958ms | 25 |
| AuthzService | GetPolicyBundle | read_only | OK | 6.514ms | 9.409ms | 7.346ms | 5.687ms | 21.41ms | 25 |
| AuthzService | GetPolicyRule | read_only | OK | 4.214ms | 6.406ms | 4.415ms | 1.52ms | 8.384ms | 25 |
| AuthzService | GetRole | read_only | OK | 3.801ms | 6.897ms | 4.126ms | 2.592ms | 6.923ms | 25 |
| AuthzService | InvalidatePolicyBundles | destructive | OK | 41.117ms | 41.117ms | 41.117ms | 41.117ms | 41.117ms | 1 |
| AuthzService | LintAuthzPolicies | read_only | OK | 1.585ms | 2.128ms | 1.541ms | 1.04ms | 2.618ms | 25 |
| AuthzService | ListAccessDecisionAudits | read_only | OK | 11.125ms | 18.557ms | 12.134ms | 8.398ms | 21.293ms | 25 |
| AuthzService | ListPolicyRules | read_only | OK | 4.155ms | 5.737ms | 4.4ms | 3.182ms | 6.518ms | 25 |
| AuthzService | ListPolicyVersions | read_only | OK | 4.154ms | 6.045ms | 4.686ms | 2.684ms | 20.769ms | 25 |
| AuthzService | ListRoles | read_only | OK | 3.695ms | 5.785ms | 3.823ms | 505µs | 6.659ms | 25 |
| AuthzService | ListUserPermissions | read_only | OK | 1.07ms | 2.101ms | 1.232ms | 0s | 3.109ms | 25 |
| AuthzService | ListUserRoles | read_only | OK | 3.834ms | 5.736ms | 4.156ms | 2.478ms | 5.953ms | 25 |
| AuthzService | MigrateLegacyPolicies | destructive | OK | 24.58ms | 24.58ms | 24.58ms | 24.58ms | 24.58ms | 1 |
| AuthzService | PromoteCanary | destructive | OK | 63.221ms | 63.221ms | 63.221ms | 63.221ms | 63.221ms | 1 |
| AuthzService | PutAuthzPolicy | mutation | OK | 18.137ms | 25.559ms | 20.149ms | 14.899ms | 26.902ms | 5 |
| AuthzService | PutRelationship | mutation | OK | 20.321ms | 22.863ms | 22.679ms | 18.874ms | 32.181ms | 5 |
| AuthzService | PutRoleBinding | mutation | OK | 17.46ms | 19.955ms | 19.924ms | 15.246ms | 30.584ms | 5 |
| AuthzService | RejectPolicyDraft | mutation | OK | 42.401ms | 42.401ms | 42.401ms | 42.401ms | 42.401ms | 5 |
| AuthzService | RevokeRole | mutation | OK | 7.266ms | 7.436ms | 9.26ms | 5.537ms | 19.728ms | 5 |
| AuthzService | RollbackPolicyVersion | destructive | OK | 90.981ms | 90.981ms | 90.981ms | 90.981ms | 90.981ms | 1 |
| AuthzService | SeedBuiltinRoles | mutation | OK | 68.315ms | 73.918ms | 67.92ms | 59.196ms | 77.135ms | 5 |
| AuthzService | SimulatePolicy | mutation | OK | 9.436ms | 10.012ms | 14.834ms | 8.938ms | 36.46ms | 5 |
| AuthzService | SubmitPolicyDraft | mutation | OK | 17.563ms | 17.563ms | 17.563ms | 17.563ms | 17.563ms | 5 |
| AuthzService | UpdatePolicyDraft | mutation | OK | 28.431ms | 31.973ms | 28.646ms | 20.936ms | 35.054ms | 5 |
| AuthzService | UpdateRole | mutation | OK | 26.552ms | 30.542ms | 27.023ms | 16.157ms | 37.816ms | 5 |
| ControlPlaneService | AckStatus | mutation | OK | 7.436ms | 7.588ms | 7.331ms | 6.269ms | 8.56ms | 5 |
| ControlPlaneService | DeltaResources | mutation | OK | 42.579ms | 44.548ms | 45.186ms | 38.547ms | 60.323ms | 5 |
| ControlPlaneService | GetResources | read_only | OK | 4.243ms | 5.396ms | 4.277ms | 3.64ms | 5.45ms | 25 |
| ControlPlaneService | ListNodeStates | read_only | OK | 28.306ms | 33.483ms | 27.749ms | 21.015ms | 33.836ms | 25 |
| ControlPlaneService | StreamResources | mutation | OK | 38.848ms | 53.129ms | 47.653ms | 37.068ms | 70.569ms | 5 |
| DataBroker | ActivateCatalog | destructive | OK | 9.908ms | 9.908ms | 9.908ms | 9.908ms | 9.908ms | 1 |
| DataBroker | AnalyticalQuery | read_only | OK | 6.646ms | 7.777ms | 6.678ms | 5.727ms | 8.071ms | 25 |
| DataBroker | ApplyMigration | mutation | OK | 238.082ms | 238.082ms | 238.082ms | 238.082ms | 238.082ms | 5 |
| DataBroker | ApproveMigrationPlan | mutation | OK | 22.887ms | 22.887ms | 22.887ms | 22.887ms | 22.887ms | 5 |
| DataBroker | BatchSelect | mutation | OK | 5.3ms | 6.776ms | 6.012ms | 5.233ms | 7.457ms | 5 |
| DataBroker | BatchUpsert | mutation | OK | 38.304ms | 38.383ms | 38.813ms | 36.855ms | 42.279ms | 5 |
| DataBroker | BeginTx | mutation | OK | 28.015ms | 44.812ms | 32.775ms | 17.12ms | 48.173ms | 5 |
| DataBroker | CacheDelete | mutation | OK | 5.908ms | 6.918ms | 6.035ms | 5.158ms | 6.925ms | 5 |
| DataBroker | CacheGet | read_only | OK | 4.792ms | 5.933ms | 5.079ms | 4.19ms | 6.595ms | 25 |
| DataBroker | CacheScan | read_only | OK | 6.786ms | 8.169ms | 6.82ms | 3.064ms | 11.659ms | 25 |
| DataBroker | CacheSet | mutation | OK | 5.814ms | 6.643ms | 5.831ms | 506µs | 11.23ms | 5 |
| DataBroker | CreateMaterializedView | mutation | OK | 5.279ms | 5.282ms | 5.692ms | 4.71ms | 8.018ms | 5 |
| DataBroker | Delete | mutation | OK | 28.056ms | 29.269ms | 27.724ms | 23.252ms | 31.067ms | 5 |
| DataBroker | DeletePolicy | mutation | OK | 19.101ms | 19.101ms | 19.101ms | 19.101ms | 19.101ms | 5 |
| DataBroker | DismissDlqEvent | mutation | OK | 14.158ms | 32.894ms | 21.391ms | 10.871ms | 36.79ms | 5 |
| DataBroker | DocumentDelete | mutation | OK | 6.172ms | 6.468ms | 6.579ms | 4.22ms | 11.33ms | 5 |
| DataBroker | DocumentFind | read_only | OK | 4.241ms | 6.227ms | 4.503ms | 3.637ms | 6.311ms | 25 |
| DataBroker | DocumentGet | read_only | OK | 4.259ms | 4.884ms | 4.38ms | 3.678ms | 5.857ms | 25 |
| DataBroker | DocumentUpsert | mutation | OK | 4.694ms | 5.354ms | 5.661ms | 4.243ms | 9.744ms | 5 |
| DataBroker | DropResource | destructive | OK | 26.483ms | 26.483ms | 26.483ms | 26.483ms | 26.483ms | 1 |
| DataBroker | EnqueueOutboxEvent | mutation | OK | 10.171ms | 10.171ms | 10.171ms | 10.171ms | 10.171ms | 5 |
| DataBroker | EnsureProject | mutation | OK | 11.106ms | 26.193ms | 16.988ms | 9.45ms | 28.167ms | 5 |
| DataBroker | EnsureResource | mutation | OK | 14.868ms | 19.421ms | 16.41ms | 11.127ms | 21.845ms | 5 |
| DataBroker | GeneratePresignedUrl | mutation | OK | 4.179ms | 4.487ms | 4.728ms | 1.018ms | 10.749ms | 5 |
| DataBroker | GenericDispatch | mutation | OK | 7.83ms | 9.5ms | 8.52ms | 6.333ms | 11.127ms | 5 |
| DataBroker | GetAdminSummary | read_only | OK | 21.324ms | 33.49ms | 22.679ms | 18.259ms | 33.597ms | 25 |
| DataBroker | GetCapabilities | read_only | OK | 5.833ms | 6.451ms | 5.812ms | 5.238ms | 6.712ms | 25 |
| DataBroker | GetCatalogManifest | read_only | OK | 10.794ms | 12.938ms | 11.026ms | 8.494ms | 19.412ms | 25 |
| DataBroker | GetCatalogVersion | read_only | OK | 4.705ms | 6.381ms | 4.631ms | 513µs | 8.451ms | 25 |
| DataBroker | GetCatalogVersions | read_only | OK | 4.764ms | 11.102ms | 5.475ms | 3.671ms | 12.83ms | 25 |
| DataBroker | GetCdcStatus | read_only | OK | 4.737ms | 6.047ms | 4.783ms | 4.15ms | 6.761ms | 25 |
| DataBroker | GetDlqEvent | read_only | OK | 5.84ms | 8.358ms | 6.066ms | 3.652ms | 8.604ms | 25 |
| DataBroker | GetHealthReport | read_only | OK | 3.142ms | 3.689ms | 3.045ms | 2.303ms | 4.284ms | 25 |
| DataBroker | GetMigrationStatus | read_only | OK | 6.435ms | 6.952ms | 6.093ms | 3.663ms | 7.999ms | 25 |
| DataBroker | GetObject | read_only | OK | 8.429ms | 10.117ms | 8.625ms | 6.881ms | 12.298ms | 25 |
| DataBroker | GetSaga | read_only | OK | 4.849ms | 6.82ms | 5.242ms | 4.202ms | 6.958ms | 25 |
| DataBroker | GraphMutate | mutation | OK | 19.504ms | 33.414ms | 55.146ms | 15.485ms | 189.173ms | 5 |
| DataBroker | GraphQuery | read_only | OK | 13.19ms | 17.628ms | 13.796ms | 10.133ms | 21.494ms | 25 |
| DataBroker | InitiateMultipartUpload | mutation | OK | 15.431ms | 21.466ms | 18.396ms | 10.736ms | 33.131ms | 5 |
| DataBroker | LintPolicies | read_only | OK | 4.553ms | 6.104ms | 4.789ms | 3.574ms | 7.448ms | 25 |
| DataBroker | ListAdminAuditLogs | read_only | OK | 5.241ms | 7.392ms | 5.385ms | 3.994ms | 7.482ms | 25 |
| DataBroker | ListDlqEvents | read_only | OK | 5.099ms | 6.861ms | 5.134ms | 2.612ms | 7.435ms | 25 |
| DataBroker | ListMessageSchemas | read_only | OK | 2.111ms | 3.116ms | 2.179ms | 1.552ms | 3.184ms | 25 |
| DataBroker | ListMigrationRuns | read_only | OK | 4.571ms | 6.808ms | 4.602ms | 1.515ms | 7.129ms | 25 |
| DataBroker | ListPolicies | read_only | OK | 4.222ms | 6.28ms | 4.367ms | 3.105ms | 7.449ms | 25 |
| DataBroker | ListProjects | read_only | OK | 4.177ms | 5.728ms | 4.296ms | 3.114ms | 5.822ms | 25 |
| DataBroker | ListResources | read_only | OK | 3.799ms | 4.803ms | 3.875ms | 504µs | 5.845ms | 25 |
| DataBroker | ListSagas | read_only | OK | 4.688ms | 7.665ms | 5.02ms | 3.639ms | 12.081ms | 25 |
| DataBroker | LookupMessageSchema | read_only | OK | 2.09ms | 3.238ms | 2.232ms | 1.03ms | 3.521ms | 25 |
| DataBroker | MarkSagaReviewed | mutation | OK | 14.739ms | 17.563ms | 15.202ms | 10.519ms | 19.276ms | 5 |
| DataBroker | PauseCdc | mutation | OK | 13.728ms | 19.106ms | 16.542ms | 12.863ms | 24.115ms | 5 |
| DataBroker | PlanMigration | mutation | OK | 18.681ms | 19.154ms | 20.247ms | 16.625ms | 28.68ms | 5 |
| DataBroker | PreviewCdcRedaction | read_only | OK | 9.125ms | 20.564ms | 10.516ms | 6.666ms | 28.844ms | 25 |
| DataBroker | PublishCDC | mutation | OK | 245.62ms | 245.62ms | 189.322ms | 58.766ms | 263.58ms | 3 |
| DataBroker | PutObject | mutation | OK | 27.161ms | 35.304ms | 27.028ms | 17.023ms | 35.603ms | 5 |
| DataBroker | PutPolicy | destructive | OK | 26.701ms | 26.701ms | 26.701ms | 26.701ms | 26.701ms | 1 |
| DataBroker | QuarantineDlqEvent | mutation | OK | 25.238ms | 28.403ms | 24.986ms | 15.03ms | 32.354ms | 5 |
| DataBroker | ReloadPolicies | destructive | OK | 12.277ms | 12.277ms | 12.277ms | 12.277ms | 12.277ms | 1 |
| DataBroker | ReplayDlqEvent | mutation | OK | 34.512ms | 34.512ms | 34.512ms | 34.512ms | 34.512ms | 5 |
| DataBroker | ResumeCdc | mutation | OK | 13.497ms | 14.637ms | 17.102ms | 13.155ms | 30.932ms | 5 |
| DataBroker | RetrySagaCompensation | mutation | OK | 15.096ms | 15.096ms | 15.096ms | 15.096ms | 15.096ms | 5 |
| DataBroker | RollbackCatalog | destructive | OK | 5.246ms | 5.246ms | 5.246ms | 5.246ms | 5.246ms | 1 |
| DataBroker | ScanProjectionDrift | read_only | OK | 11.187ms | 23.607ms | 12.052ms | 8.946ms | 24.588ms | 25 |
| DataBroker | Select | read_only | OK | 5.231ms | 5.752ms | 5.224ms | 4.502ms | 7.042ms | 25 |
| DataBroker | SelectV2 | read_only | OK | 5.238ms | 7.377ms | 5.86ms | 4.156ms | 18.092ms | 25 |
| DataBroker | StageCatalog | destructive | OK | 345.665ms | 345.665ms | 345.665ms | 345.665ms | 345.665ms | 1 |
| DataBroker | StepDownCdcLeader | mutation | OK | 16.237ms | 28.739ms | 19.681ms | 11.796ms | 29.548ms | 5 |
| DataBroker | TimeSeriesQuery | read_only | OK | 7.263ms | 8.746ms | 7.332ms | 5.793ms | 9.007ms | 25 |
| DataBroker | TimeSeriesWrite | mutation | OK | 4.015ms | 4.04ms | 4.105ms | 3.822ms | 4.746ms | 5 |
| DataBroker | Upsert | mutation | OK | 41.731ms | 45.51ms | 41.597ms | 33.302ms | 49.175ms | 5 |
| DataBroker | ValidateCatalog | destructive | OK | 2.632ms | 2.632ms | 2.632ms | 2.632ms | 2.632ms | 1 |
| DataBroker | VectorBatchUpsert | mutation | OK | 6.34ms | 6.865ms | 17.806ms | 5.313ms | 64.664ms | 5 |
| DataBroker | VectorHybridSearch | read_only | OK | 4.78ms | 6.498ms | 5.03ms | 3.707ms | 7.8ms | 25 |
| DataBroker | VectorSearch | read_only | OK | 4.76ms | 6.354ms | 4.935ms | 3.659ms | 7.265ms | 25 |
| DataBroker | VectorUpsert | mutation | OK | 9.651ms | 10.934ms | 10.154ms | 9.472ms | 11.092ms | 5 |
| DataBroker | VerifyAdminAuditLog | read_only | OK | 6.739ms | 9.387ms | 7.074ms | 4.841ms | 14.668ms | 25 |
| IdentityProviderService | CreateProvider | mutation | OK | 19.138ms | 19.138ms | 19.138ms | 19.138ms | 19.138ms | 5 |
| IdentityProviderService | DisableProvider | mutation | OK | 15.822ms | 17.712ms | 19.278ms | 15.403ms | 31.695ms | 5 |
| IdentityProviderService | ForceJwksRefresh | mutation | OK | 52.818ms | 55.48ms | 46.557ms | 17.97ms | 78.673ms | 5 |
| IdentityProviderService | GetProvider | read_only | OK | 3.773ms | 5.927ms | 4.199ms | 1.023ms | 6.86ms | 25 |
| IdentityProviderService | ImportSamlMetadata | mutation | OK | 15.804ms | 24.722ms | 20.417ms | 15.266ms | 30.829ms | 5 |
| IdentityProviderService | LinkIdentity | mutation | OK | 32.532ms | 32.759ms | 27.866ms | 18.821ms | 34.651ms | 5 |
| IdentityProviderService | ListExternalIdentities | read_only | OK | 7.359ms | 9.812ms | 7.337ms | 5.217ms | 10.629ms | 25 |
| IdentityProviderService | ListProviders | read_only | OK | 7.905ms | 9.679ms | 7.893ms | 5.922ms | 12.142ms | 25 |
| IdentityProviderService | PreviewClaimMapping | read_only | OK | 4.399ms | 5.586ms | 4.631ms | 3.03ms | 5.882ms | 25 |
| IdentityProviderService | PreviewGroupMapping | read_only | OK | 4.205ms | 5.772ms | 4.44ms | 3.638ms | 7.4ms | 25 |
| IdentityProviderService | ResolveExternalIdentity | mutation | OK | 7.728ms | 10.454ms | 13.127ms | 4.793ms | 36.247ms | 5 |
| IdentityProviderService | SamlAcs | mutation | OK | 73.345ms | 75.984ms | 136.649ms | 57.94ms | 411.849ms | 5 |
| IdentityProviderService | ScimCreateGroup | mutation | OK | 4.675ms | 5.032ms | 4.627ms | 3.311ms | 5.75ms | 5 |
| IdentityProviderService | ScimCreateUser | mutation | OK | 36.542ms | 39.655ms | 33.198ms | 23.449ms | 41.025ms | 5 |
| IdentityProviderService | ScimDeleteGroup | mutation | OK | 4.998ms | 5.005ms | 4.57ms | 3.302ms | 5.287ms | 5 |
| IdentityProviderService | ScimDeleteUser | mutation | OK | 43.692ms | 43.692ms | 43.692ms | 43.692ms | 43.692ms | 5 |
| IdentityProviderService | ScimGetGroup | mutation | OK | 8.162ms | 8.328ms | 8.038ms | 6.781ms | 9.208ms | 5 |
| IdentityProviderService | ScimGetUser | mutation | OK | 6.982ms | 7.013ms | 6.828ms | 5.972ms | 7.302ms | 5 |
| IdentityProviderService | ScimListGroups | mutation | OK | 6.066ms | 6.421ms | 5.63ms | 3.708ms | 7.149ms | 5 |
| IdentityProviderService | ScimListUsers | mutation | OK | 12.354ms | 13.207ms | 12.702ms | 10.146ms | 16.397ms | 5 |
| IdentityProviderService | ScimPatchGroup | mutation | OK | 12.039ms | 12.352ms | 13.395ms | 10.099ms | 21.121ms | 5 |
| IdentityProviderService | ScimPatchUser | mutation | OK | 41.077ms | 48.639ms | 45.374ms | 27.928ms | 79.081ms | 5 |
| IdentityProviderService | ScimReplaceUser | mutation | OK | 20.287ms | 20.745ms | 21.822ms | 17.952ms | 31.386ms | 5 |
| IdentityProviderService | StartSamlLogin | mutation | OK | 4.264ms | 5.291ms | 5.044ms | 3.86ms | 7.7ms | 5 |
| IdentityProviderService | TestProviderDiscovery | read_only | OK | 4.706ms | 5.332ms | 4.55ms | 3.493ms | 5.721ms | 25 |
| IdentityProviderService | UnlinkIdentity | mutation | OK | 5.11ms | 6.735ms | 9.282ms | 3.834ms | 25.904ms | 5 |
| IdentityProviderService | UpdateProvider | mutation | OK | 27.883ms | 32.586ms | 25.304ms | 13.846ms | 35.905ms | 5 |
| NotificationService | GetDeliveryStats | read_only | OK | 5.171ms | 13.573ms | 7.084ms | 3.643ms | 15.137ms | 25 |
| NotificationService | GetNotification | read_only | OK | 6.776ms | 11.759ms | 7.498ms | 5.264ms | 14.671ms | 25 |
| NotificationService | GetPreference | read_only | OK | 5.892ms | 7.421ms | 6.274ms | 5.19ms | 8.858ms | 25 |
| NotificationService | GetTemplate | read_only | OK | 5.796ms | 8.393ms | 6.203ms | 5.068ms | 8.454ms | 25 |
| NotificationService | ListNotifications | read_only | OK | 12.217ms | 16.057ms | 12.493ms | 10.292ms | 18.91ms | 25 |
| NotificationService | ListPreferences | read_only | OK | 10.624ms | 13.197ms | 10.975ms | 9.322ms | 17.894ms | 25 |
| NotificationService | ListTemplates | read_only | OK | 10.517ms | 12.822ms | 10.592ms | 7.231ms | 13.077ms | 25 |
| NotificationService | RetryNotification | mutation | OK | 13.375ms | 13.375ms | 13.375ms | 13.375ms | 13.375ms | 5 |
| NotificationService | SendNotification | mutation | OK | 32.958ms | 38.507ms | 33.678ms | 27.63ms | 40.295ms | 5 |
| NotificationService | SetPreference | mutation | OK | 6.915ms | 7.55ms | 9.306ms | 4.263ms | 21.003ms | 5 |
| NotificationService | UpsertTemplate | mutation | OK | 6.95ms | 7.065ms | 6.812ms | 5.33ms | 7.988ms | 5 |
| PeerService | GetPeer | read_only | OK | 5.871ms | 9.272ms | 6.402ms | 2.223ms | 14.658ms | 25 |
| PeerService | JoinRoom | mutation | OK | 18.434ms | 30.521ms | 23.616ms | 12.771ms | 38.703ms | 5 |
| PeerService | LeaveRoom | mutation | OK | 5.832ms | 6.026ms | 9.604ms | 3.021ms | 29.243ms | 5 |
| PeerService | ListPeers | read_only | OK | 6.272ms | 7.818ms | 6.366ms | 5.2ms | 8.01ms | 25 |
| RoomService | CloseRoom | mutation | OK | 17.847ms | 30.377ms | 23.061ms | 11.988ms | 37.534ms | 5 |
| RoomService | CreateRoom | mutation | OK | 10.693ms | 11.363ms | 13.517ms | 9.054ms | 26.291ms | 5 |
| RoomService | GetRoom | read_only | OK | 6.067ms | 8.91ms | 6.46ms | 4.036ms | 9.496ms | 25 |
| RoomService | ListRooms | read_only | OK | 5.735ms | 6.797ms | 5.695ms | 4.148ms | 7.256ms | 25 |
| RoomService | UpdateRoom | mutation | OK | 4.783ms | 4.793ms | 4.801ms | 4.705ms | 5.017ms | 5 |
| SignalingService | Signal | mutation | OK | 27.124ms | 27.124ms | 27.124ms | 27.124ms | 27.124ms | 5 |
| StorageService | DeleteFile | mutation | OK | 41.66ms | 41.66ms | 41.66ms | 41.66ms | 41.66ms | 5 |
| StorageService | FinalizeUpload | mutation | OK | 25.811ms | 26.357ms | 28.132ms | 21.795ms | 41.212ms | 5 |
| StorageService | GetDownloadUrl | read_only | OK | 7.689ms | 9.07ms | 7.616ms | 6.235ms | 9.97ms | 25 |
| StorageService | GetFile | read_only | OK | 6.089ms | 7.511ms | 6.213ms | 5.22ms | 8.437ms | 25 |
| StorageService | ListFiles | read_only | OK | 11.372ms | 19.403ms | 12.438ms | 6.174ms | 38.089ms | 25 |
| StorageService | RegisterUpload | mutation | OK | 17.279ms | 27.85ms | 19.364ms | 10.374ms | 29.048ms | 5 |
| StorageService | UpdateFile | mutation | OK | 17.21ms | 19.468ms | 20.234ms | 15.01ms | 32.742ms | 5 |
| TenantService | CreateTenant | mutation | OK | 7.448ms | 7.597ms | 7.76ms | 7.394ms | 8.943ms | 5 |
| TenantService | GetTenant | read_only | OK | 6.386ms | 9.087ms | 6.822ms | 4.787ms | 10.126ms | 25 |
| TenantService | GetTenantConfig | read_only | OK | 6.423ms | 9.101ms | 6.687ms | 3.169ms | 10.152ms | 25 |
| TenantService | ListTenants | read_only | OK | 5.277ms | 7.486ms | 5.575ms | 4.655ms | 7.786ms | 25 |
| TenantService | UpdateTenant | mutation | OK | 6.42ms | 7.857ms | 8.812ms | 6.377ms | 16.998ms | 5 |
| TenantService | UpdateTenantConfig | mutation | OK | 26.448ms | 30.496ms | 24.677ms | 17.23ms | 31.406ms | 5 |
| TrackService | ListTracks | read_only | OK | 6.33ms | 7.833ms | 6.409ms | 4.407ms | 9.277ms | 25 |
| TrackService | MuteTrack | mutation | OK | 6.33ms | 17.398ms | 11.805ms | 5.59ms | 23.437ms | 5 |
| TrackService | PublishTrack | mutation | OK | 11.24ms | 13.332ms | 14.494ms | 8.838ms | 28.224ms | 5 |
| TrackService | UnpublishTrack | mutation | OK | 4.73ms | 5.278ms | 7.801ms | 4.227ms | 20.517ms | 5 |
| TurnService | IssueCredentials | mutation | OK | 4.364ms | 4.754ms | 4.243ms | 2.068ms | 5.81ms | 5 |
