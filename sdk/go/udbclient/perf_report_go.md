# UDB SDK Live Perf — Go (localhost)

RPCs measured: 265   tenant=ee5c154d-f806-47af-b96c-54f0edde0306

Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities (a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request's reference/ID fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection latency. The TARGET is zero failures; any residual non-OK RPC is listed under Failures for the maintainer to finish.

Unary RPCs = full request→response round-trip. Non-CDC streaming RPCs report time-to-FIRST-RESPONSE with seeded inputs. CDC subscription (PublishCDC) reports time-to-FIRST-EVENT: the harness subscribes, fires a real Upsert that flows outbox→CDC→Kafka, and times the first delivered event. Streaming rows are marked in the note column.

## Seeded fixtures

Captured semantic field → seeded value keys used to resolve request fields: action, apply_run_id, approval_token, approve_draft_id, approve_run_id, approved_by, asset_id, assigned_by, auth_challenge_id, auth_token, bucket, canary_id, canary_version_id, catalog_manifest, challenge_id, close_room_id, code, collection, content_type, created_by, csrf_token, definition_id, delete_file_id, delete_policy_id, delete_role_id, delete_scim_user_id, deleted_by, device_id, disable_provider_id, dismiss_dlq_id, dlq_id, document_id, domain, ds_policy_id, event_type, external_identity_id, file_id, file_type, filename, finalize_file_id, gov_exp, instance_id, join_session_room_id, key_id, kind, leave_peer_id, locale, log_id, mark_saga_id, message_type, migration_id, mongo_collection, name, node_id, notification_id, object, object_key, otp_code, otp_id, owner_id, peer_id, plain_key, policy_draft_id, policy_id, policy_version_id, project, project_id, provider_id, quarantine_dlq_id, recipient_id, record_id, recovery_code, refresh_session_id, refresh_token, reg_challenge_id, reject_draft_id, rejected_by, relation, replay_dlq_id, reset_otp_code, reset_otp_id, resource, retry_saga_id, revoke_key_id, revoked_by, role, role_code, role_id, rollback_policy_set_id, rollback_target_version_id, room_id, saga_id, saml_provider_id, scim_group_id, scim_user_id, session_id, signal_peer_id, stage_name, step_id, subject, tenant, tenant_id, token, topic_pattern, track_id, ts_table, unpublish_track_id, update_draft_id, update_key_id, updated_by, user_id, user_role_id, username

## Per-service mean latency (mean of per-RPC means)

| Service | RPCs | mean |
|---|---:|---:|
| AuthnService | 50 | 51.378ms |
| DataBroker | 77 | 19.134ms |
| AuthzService | 41 | 21.484ms |
| IdentityProviderService | 27 | 16.273ms |
| ControlPlaneService | 5 | 29.816ms |
| NotificationService | 11 | 12.587ms |
| ApiKeyService | 9 | 14.241ms |
| StorageService | 8 | 15.974ms |
| AssetService | 8 | 10.869ms |
| PeerService | 5 | 11.85ms |
| TenantService | 6 | 8.508ms |
| RoomService | 5 | 10.178ms |
| AnalyticsService | 7 | 5.603ms |
| TrackService | 4 | 8.905ms |
| SignalingService | 1 | 8.449ms |
| TurnService | 1 | 4.093ms |

## Failures — still to fix (0)

No RPC returned a non-OK gRPC status — every RPC ran its success path.

## Slowest 25 RPCs by p99

| RPC | kind | err | p50 | p99 | mean | iters | note |
|---|---|---|---:|---:|---:|---:|---|
| AuthnService/ChangePassword | mutation | OK | 838.961ms | 838.961ms | 838.961ms | 5 | mutation (seeded success path) |
| AuthnService/Login | mutation | OK | 403.259ms | 410.987ms | 396.039ms | 5 | mutation (seeded success path) |
| AuthnService/ResetPassword | mutation | OK | 409.412ms | 409.412ms | 409.412ms | 5 | mutation (seeded success path) |
| AuthnService/CreateUser | mutation | OK | 370.277ms | 370.277ms | 370.277ms | 5 | mutation (seeded success path) |
| DataBroker/PublishCDC | mutation | OK | 246.985ms | 246.985ms | 201.475ms | 3 | cdc subscription: time-to-first-event (real mutation produced) |
| DataBroker/StageCatalog | destructive | OK | 242.552ms | 242.552ms | 242.552ms | 1 | destructive: 1 real call against a seeded disposable target |
| DataBroker/ApplyMigration | mutation | OK | 164.365ms | 164.365ms | 164.365ms | 5 | mutation (seeded success path) |
| AuthzService/Authorize | read_only | OK | 21.751ms | 151.066ms | 35.054ms | 25 | read_only (seeded success path) |
| AuthzService/PromoteCanary | destructive | OK | 104.021ms | 104.021ms | 104.021ms | 1 | destructive: 1 real call against a seeded disposable target |
| IdentityProviderService/SamlAcs | mutation | OK | 56.074ms | 80.34ms | 64.425ms | 5 | mutation (seeded success path) |
| AuthzService/ActivatePolicyVersion | destructive | OK | 70.602ms | 70.602ms | 70.602ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthzService/RollbackPolicyVersion | destructive | OK | 69.744ms | 69.744ms | 69.744ms | 1 | destructive: 1 real call against a seeded disposable target |
| ControlPlaneService/StreamResources | mutation | OK | 56.552ms | 67.29ms | 61.226ms | 5 | streaming: time-to-first-response (seeded; bidi) |
| IdentityProviderService/CreateProvider | mutation | OK | 58.946ms | 58.946ms | 58.946ms | 5 | mutation (seeded success path) |
| DataBroker/GetAdminSummary | read_only | OK | 24.57ms | 56.835ms | 28.432ms | 25 | read_only (seeded success path) |
| DataBroker/GetObject | read_only | OK | 19.547ms | 50.916ms | 24.331ms | 25 | streaming: time-to-first-response (seeded; server_streaming) |
| ControlPlaneService/DeltaResources | mutation | OK | 49.146ms | 50.062ms | 49.468ms | 5 | streaming: time-to-first-response (seeded; bidi) |
| ApiKeyService/EmergencyRevokeApiKeys | destructive | OK | 49.402ms | 49.402ms | 49.402ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthzService/ApprovePolicyDraft | mutation | OK | 48.909ms | 48.909ms | 48.909ms | 5 | mutation (seeded success path) |
| AuthzService/SeedBuiltinRoles | mutation | OK | 44.653ms | 48.135ms | 47.634ms | 5 | mutation (seeded success path) |
| DataBroker/GraphQuery | read_only | OK | 21.227ms | 39.543ms | 31.82ms | 25 | read_only (seeded success path) |
| ControlPlaneService/ListNodeStates | read_only | OK | 24.864ms | 39.436ms | 26.104ms | 25 | read_only (seeded success path) |
| AuthzService/ActivateCanary | destructive | OK | 37.663ms | 37.663ms | 37.663ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthzService/CreatePolicyDraft | mutation | OK | 33.143ms | 36.835ms | 35.135ms | 5 | mutation (seeded success path) |
| DataBroker/DropResource | destructive | OK | 36.163ms | 36.163ms | 36.163ms | 1 | destructive: 1 real call against a seeded disposable target |

## Full per-RPC table (sorted by service, then name)

| Service | RPC | kind | err | p50 | p99 | mean | min | max | iters |
|---|---|---|---|---:|---:|---:|---:|---:|---:|
| AnalyticsService | GetExecutorPerformance | read_only | OK | 4.244ms | 8.153ms | 5.205ms | 2.845ms | 8.205ms | 25 |
| AnalyticsService | GetPipelineSummary | read_only | OK | 5.084ms | 8.633ms | 5.713ms | 3.483ms | 10.289ms | 25 |
| AnalyticsService | GetReconciliationAnalytics | read_only | OK | 3.902ms | 8.362ms | 4.939ms | 2.776ms | 8.42ms | 25 |
| AnalyticsService | GetSlaCompliance | read_only | OK | 4.167ms | 6.861ms | 4.594ms | 3.321ms | 7.286ms | 25 |
| AnalyticsService | GetThroughput | read_only | OK | 4.166ms | 5.499ms | 4.316ms | 506µs | 9.534ms | 25 |
| AnalyticsService | RecordPipelineMetric | mutation | OK | 7.313ms | 8.602ms | 7.729ms | 4.795ms | 11.368ms | 5 |
| AnalyticsService | TriggerSnapshot | mutation | OK | 5.527ms | 5.563ms | 6.727ms | 4.51ms | 12.538ms | 5 |
| ApiKeyService | CreateApiKey | mutation | OK | 11.465ms | 12.184ms | 11.326ms | 8.409ms | 13.985ms | 5 |
| ApiKeyService | EmergencyRevokeApiKeys | destructive | OK | 49.402ms | 49.402ms | 49.402ms | 49.402ms | 49.402ms | 1 |
| ApiKeyService | GetApiKey | read_only | OK | 3.962ms | 5.669ms | 4.332ms | 3.03ms | 6.046ms | 25 |
| ApiKeyService | GetApiKeyUsageStats | read_only | OK | 4.162ms | 8.704ms | 5.393ms | 3.206ms | 8.707ms | 25 |
| ApiKeyService | ListApiKeys | read_only | OK | 3.954ms | 11.078ms | 4.836ms | 2.666ms | 12.876ms | 25 |
| ApiKeyService | RevokeApiKey | mutation | OK | 17.65ms | 17.65ms | 17.65ms | 17.65ms | 17.65ms | 5 |
| ApiKeyService | RotateApiKey | mutation | OK | 14.551ms | 14.551ms | 14.551ms | 14.551ms | 14.551ms | 5 |
| ApiKeyService | UpdateApiKey | mutation | OK | 11.85ms | 15.933ms | 13.24ms | 9.658ms | 17.907ms | 5 |
| ApiKeyService | ValidateApiKey | read_only | OK | 7.377ms | 10.568ms | 7.437ms | 4.763ms | 10.658ms | 25 |
| AssetService | CompleteStep | mutation | OK | 20.49ms | 22.003ms | 20.62ms | 17.386ms | 25.794ms | 5 |
| AssetService | CreatePipelineDefinition | mutation | OK | 11.569ms | 11.569ms | 11.569ms | 11.569ms | 11.569ms | 5 |
| AssetService | GetAsset | read_only | OK | 7.146ms | 15.952ms | 8.466ms | 5.489ms | 17.622ms | 25 |
| AssetService | GetPipeline | read_only | OK | 5.957ms | 11.893ms | 7.016ms | 4.279ms | 12.908ms | 25 |
| AssetService | GetPipelineDefinition | read_only | OK | 6.547ms | 11.951ms | 7.012ms | 5.177ms | 11.979ms | 25 |
| AssetService | ListAssets | read_only | OK | 8.302ms | 11.077ms | 8.692ms | 6.682ms | 13.313ms | 25 |
| AssetService | RegisterAsset | mutation | OK | 13.028ms | 13.259ms | 13.087ms | 11.727ms | 14.399ms | 5 |
| AssetService | StartPipeline | mutation | OK | 5.201ms | 5.768ms | 10.493ms | 4.289ms | 32.35ms | 5 |
| AuthnService | AdminResetMfa | destructive | OK | 27.59ms | 27.59ms | 27.59ms | 27.59ms | 27.59ms | 1 |
| AuthnService | AdminResetPassword | destructive | OK | 10.08ms | 10.08ms | 10.08ms | 10.08ms | 10.08ms | 1 |
| AuthnService | AdminRevokeAllTenantSessions | destructive | OK | 19.985ms | 19.985ms | 19.985ms | 19.985ms | 19.985ms | 1 |
| AuthnService | AdminRevokeAllUserSessions | destructive | OK | 11.264ms | 11.264ms | 11.264ms | 11.264ms | 11.264ms | 1 |
| AuthnService | AdminRevokeSession | destructive | OK | 18.53ms | 18.53ms | 18.53ms | 18.53ms | 18.53ms | 1 |
| AuthnService | Authenticate | read_only | OK | 15.484ms | 28.963ms | 16.602ms | 11.474ms | 30.349ms | 25 |
| AuthnService | ChangePassword | mutation | OK | 838.961ms | 838.961ms | 838.961ms | 838.961ms | 838.961ms | 5 |
| AuthnService | ChangeUserStatus | destructive | OK | 15.11ms | 15.11ms | 15.11ms | 15.11ms | 15.11ms | 1 |
| AuthnService | ConfirmMFAEnrollment | mutation | OK | 3.953ms | 3.976ms | 3.869ms | 3.543ms | 4.029ms | 5 |
| AuthnService | CreateSession | mutation | OK | 6.032ms | 6.05ms | 5.808ms | 4.972ms | 6.304ms | 5 |
| AuthnService | CreateUser | mutation | OK | 370.277ms | 370.277ms | 370.277ms | 370.277ms | 370.277ms | 5 |
| AuthnService | DeleteWebAuthnCredential | mutation | OK | 7.978ms | 9.009ms | 8.019ms | 6.128ms | 9.198ms | 5 |
| AuthnService | DisableMfaFactor | mutation | OK | 12.515ms | 13.431ms | 12.929ms | 11.263ms | 15.52ms | 5 |
| AuthnService | EmergencyRevoke | destructive | OK | 14.193ms | 14.193ms | 14.193ms | 14.193ms | 14.193ms | 1 |
| AuthnService | EnrollMFA | mutation | OK | 15.56ms | 15.708ms | 14.956ms | 12.297ms | 16.129ms | 5 |
| AuthnService | FinishWebAuthnAuthentication | mutation | OK | 36.072ms | 36.072ms | 36.072ms | 36.072ms | 36.072ms | 5 |
| AuthnService | FinishWebAuthnRegistration | mutation | OK | 31.698ms | 31.698ms | 31.698ms | 31.698ms | 31.698ms | 5 |
| AuthnService | ForgotPassword | mutation | OK | 16.332ms | 16.99ms | 17.022ms | 15.277ms | 20.395ms | 5 |
| AuthnService | GenerateRecoveryCodes | mutation | OK | 32.903ms | 33.141ms | 32.453ms | 27.527ms | 36.878ms | 5 |
| AuthnService | GetJwks | read_only | OK | 4.002ms | 6.166ms | 4.196ms | 2.737ms | 6.633ms | 25 |
| AuthnService | GetMfaPolicy | read_only | OK | 3.323ms | 4.835ms | 3.46ms | 2.683ms | 6.24ms | 25 |
| AuthnService | GetSession | read_only | OK | 3.418ms | 7.008ms | 4.083ms | 2.74ms | 10.426ms | 25 |
| AuthnService | GetUser | read_only | OK | 3.27ms | 4.125ms | 3.289ms | 2.206ms | 5.797ms | 25 |
| AuthnService | IntrospectToken | read_only | OK | 17.009ms | 21.563ms | 17.748ms | 14.992ms | 23.444ms | 25 |
| AuthnService | IssueMfaChallenge | mutation | OK | 9.706ms | 9.713ms | 9.798ms | 9.146ms | 11.199ms | 5 |
| AuthnService | ListDevices | read_only | OK | 3.414ms | 6.481ms | 3.79ms | 2.681ms | 7.252ms | 25 |
| AuthnService | ListMfaFactors | read_only | OK | 5.041ms | 7.302ms | 5.476ms | 3.861ms | 8.66ms | 25 |
| AuthnService | ListSessions | read_only | OK | 7.033ms | 9.486ms | 7.903ms | 5.626ms | 21.203ms | 25 |
| AuthnService | ListUsers | read_only | OK | 5.734ms | 7.888ms | 6.003ms | 4.489ms | 8.074ms | 25 |
| AuthnService | ListWebAuthnCredentials | read_only | OK | 3.395ms | 5.008ms | 3.547ms | 2.546ms | 5.69ms | 25 |
| AuthnService | Login | mutation | OK | 403.259ms | 410.987ms | 396.039ms | 367.734ms | 429.122ms | 5 |
| AuthnService | Logout | mutation | OK | 5.202ms | 5.287ms | 5.012ms | 4.382ms | 5.647ms | 5 |
| AuthnService | PutMfaPolicy | mutation | OK | 5.587ms | 6.041ms | 5.594ms | 5.067ms | 6.207ms | 5 |
| AuthnService | RefreshSession | mutation | OK | 24.239ms | 29.857ms | 23.963ms | 12.317ms | 33.968ms | 5 |
| AuthnService | RefreshToken | mutation | OK | 8.212ms | 8.212ms | 8.212ms | 8.212ms | 8.212ms | 5 |
| AuthnService | RenamePasskey | mutation | OK | 5.971ms | 6.982ms | 6.078ms | 3.175ms | 8.295ms | 5 |
| AuthnService | ResendOTP | mutation | OK | 11.95ms | 14.185ms | 12.72ms | 11.205ms | 14.814ms | 5 |
| AuthnService | ResetPassword | mutation | OK | 409.412ms | 409.412ms | 409.412ms | 409.412ms | 409.412ms | 5 |
| AuthnService | RevokeDevice | mutation | OK | 12.689ms | 12.689ms | 12.689ms | 12.689ms | 12.689ms | 5 |
| AuthnService | RevokeRecoveryCodes | mutation | OK | 8.356ms | 9.067ms | 8.46ms | 7.581ms | 9.125ms | 5 |
| AuthnService | RevokeSession | mutation | OK | 3.961ms | 4.416ms | 4.1ms | 3.211ms | 5.036ms | 5 |
| AuthnService | SendOTP | mutation | OK | 11.785ms | 12.814ms | 11.643ms | 9.391ms | 12.995ms | 5 |
| AuthnService | SendPhoneVerification | mutation | OK | 11.436ms | 12.376ms | 12.551ms | 10.771ms | 17.393ms | 5 |
| AuthnService | StartWebAuthnAuthentication | mutation | OK | 14.76ms | 14.914ms | 14.557ms | 12.486ms | 16.897ms | 5 |
| AuthnService | StartWebAuthnRegistration | mutation | OK | 14.113ms | 14.204ms | 14.38ms | 12.807ms | 17.588ms | 5 |
| AuthnService | UpdateUser | mutation | OK | 8.928ms | 9.097ms | 8.623ms | 7.669ms | 9.129ms | 5 |
| AuthnService | ValidateCSRF | read_only | OK | 3.291ms | 3.928ms | 3.236ms | 2.142ms | 5.588ms | 25 |
| AuthnService | ValidateToken | read_only | OK | 13.397ms | 16.62ms | 13.82ms | 11.315ms | 17.667ms | 25 |
| AuthnService | VerifyMfaChallenge | read_only | OK | 6.899ms | 9.913ms | 7.005ms | 5.004ms | 10.733ms | 25 |
| AuthnService | VerifyOTP | read_only | OK | 15.467ms | 20.518ms | 16.115ms | 12.647ms | 22.543ms | 25 |
| AuthzService | ActivateCanary | destructive | OK | 37.663ms | 37.663ms | 37.663ms | 37.663ms | 37.663ms | 1 |
| AuthzService | ActivatePolicyVersion | destructive | OK | 70.602ms | 70.602ms | 70.602ms | 70.602ms | 70.602ms | 1 |
| AuthzService | ApprovePolicyDraft | mutation | OK | 48.909ms | 48.909ms | 48.909ms | 48.909ms | 48.909ms | 5 |
| AuthzService | AssignRole | mutation | OK | 23.073ms | 23.315ms | 22.945ms | 21.798ms | 23.742ms | 5 |
| AuthzService | Authorize | read_only | OK | 21.751ms | 151.066ms | 35.054ms | 14.085ms | 205.241ms | 25 |
| AuthzService | BatchCheckPermissions | read_only | OK | 7.831ms | 10.442ms | 8.168ms | 5.864ms | 11.927ms | 25 |
| AuthzService | CheckAccess | read_only | OK | 7.332ms | 12.139ms | 8.751ms | 6.438ms | 31.501ms | 25 |
| AuthzService | CreatePolicyDraft | mutation | OK | 33.143ms | 36.835ms | 35.135ms | 29.002ms | 45.941ms | 5 |
| AuthzService | CreatePolicyRule | mutation | OK | 15.485ms | 17.813ms | 16.327ms | 15.059ms | 17.97ms | 5 |
| AuthzService | CreateRole | mutation | OK | 20.322ms | 20.322ms | 20.322ms | 20.322ms | 20.322ms | 5 |
| AuthzService | DeletePolicyRule | mutation | OK | 7.469ms | 8.693ms | 8.322ms | 6.81ms | 11.631ms | 5 |
| AuthzService | DeleteRole | mutation | OK | 10.623ms | 14.379ms | 12.849ms | 6.14ms | 23.95ms | 5 |
| AuthzService | DiffPolicyDraft | read_only | OK | 10.445ms | 15.658ms | 10.965ms | 8.955ms | 16.223ms | 25 |
| AuthzService | ExplainPolicy | read_only | OK | 7.073ms | 9.445ms | 7.371ms | 5.583ms | 10.176ms | 25 |
| AuthzService | GetAuthzRevision | read_only | OK | 3.453ms | 4.327ms | 3.545ms | 2.671ms | 5.278ms | 25 |
| AuthzService | GetCanaryStatus | read_only | OK | 9.182ms | 11.319ms | 9.412ms | 6.991ms | 12.677ms | 25 |
| AuthzService | GetNativeAccess | read_only | OK | 17.241ms | 29.121ms | 19.617ms | 13.697ms | 35.374ms | 25 |
| AuthzService | GetPolicyBundle | read_only | OK | 8.289ms | 11.846ms | 9.195ms | 5.219ms | 28.796ms | 25 |
| AuthzService | GetPolicyRule | read_only | OK | 4.462ms | 7.656ms | 4.747ms | 3.459ms | 7.675ms | 25 |
| AuthzService | GetRole | read_only | OK | 4.431ms | 6.799ms | 4.782ms | 3.225ms | 7.149ms | 25 |
| AuthzService | InvalidatePolicyBundles | destructive | OK | 24.771ms | 24.771ms | 24.771ms | 24.771ms | 24.771ms | 1 |
| AuthzService | LintAuthzPolicies | read_only | OK | 1.928ms | 2.578ms | 1.964ms | 1.394ms | 2.907ms | 25 |
| AuthzService | ListAccessDecisionAudits | read_only | OK | 10.508ms | 23.283ms | 12.504ms | 8.063ms | 23.532ms | 25 |
| AuthzService | ListPolicyRules | read_only | OK | 4.811ms | 7.215ms | 4.992ms | 3.433ms | 7.239ms | 25 |
| AuthzService | ListPolicyVersions | read_only | OK | 9.98ms | 13.84ms | 10.438ms | 7.862ms | 16.511ms | 25 |
| AuthzService | ListRoles | read_only | OK | 3.797ms | 5.384ms | 3.901ms | 2.677ms | 5.822ms | 25 |
| AuthzService | ListUserPermissions | read_only | OK | 1.097ms | 1.683ms | 1.228ms | 510µs | 1.728ms | 25 |
| AuthzService | ListUserRoles | read_only | OK | 3.819ms | 6.088ms | 4.008ms | 2.676ms | 6.921ms | 25 |
| AuthzService | MigrateLegacyPolicies | destructive | OK | 32.027ms | 32.027ms | 32.027ms | 32.027ms | 32.027ms | 1 |
| AuthzService | PromoteCanary | destructive | OK | 104.021ms | 104.021ms | 104.021ms | 104.021ms | 104.021ms | 1 |
| AuthzService | PutAuthzPolicy | mutation | OK | 14.999ms | 15.589ms | 15.3ms | 13.613ms | 17.681ms | 5 |
| AuthzService | PutRelationship | mutation | OK | 19.323ms | 19.659ms | 20.022ms | 19.305ms | 22.504ms | 5 |
| AuthzService | PutRoleBinding | mutation | OK | 16.127ms | 17.11ms | 16.455ms | 13.507ms | 21.453ms | 5 |
| AuthzService | RejectPolicyDraft | mutation | OK | 24.828ms | 24.828ms | 24.828ms | 24.828ms | 24.828ms | 5 |
| AuthzService | RevokeRole | mutation | OK | 7.306ms | 8.78ms | 10.13ms | 5.99ms | 21.981ms | 5 |
| AuthzService | RollbackPolicyVersion | destructive | OK | 69.744ms | 69.744ms | 69.744ms | 69.744ms | 69.744ms | 1 |
| AuthzService | SeedBuiltinRoles | mutation | OK | 44.653ms | 48.135ms | 47.634ms | 38.835ms | 64.876ms | 5 |
| AuthzService | SimulatePolicy | mutation | OK | 14.671ms | 15.073ms | 17.39ms | 13.595ms | 29.698ms | 5 |
| AuthzService | SubmitPolicyDraft | mutation | OK | 22.197ms | 22.197ms | 22.197ms | 22.197ms | 22.197ms | 5 |
| AuthzService | UpdatePolicyDraft | mutation | OK | 25.372ms | 30.668ms | 26.408ms | 20.778ms | 31.394ms | 5 |
| AuthzService | UpdateRole | mutation | OK | 17.586ms | 17.628ms | 16.179ms | 11.531ms | 18.072ms | 5 |
| ControlPlaneService | AckStatus | mutation | OK | 7.665ms | 9.251ms | 8.085ms | 5.378ms | 11.429ms | 5 |
| ControlPlaneService | DeltaResources | mutation | OK | 49.146ms | 50.062ms | 49.468ms | 44.945ms | 57.249ms | 5 |
| ControlPlaneService | GetResources | read_only | OK | 3.955ms | 5.855ms | 4.195ms | 3.08ms | 5.98ms | 25 |
| ControlPlaneService | ListNodeStates | read_only | OK | 24.864ms | 39.436ms | 26.104ms | 20.375ms | 45.464ms | 25 |
| ControlPlaneService | StreamResources | mutation | OK | 56.552ms | 67.29ms | 61.226ms | 48.845ms | 82.471ms | 5 |
| DataBroker | ActivateCatalog | destructive | OK | 6.517ms | 6.517ms | 6.517ms | 6.517ms | 6.517ms | 1 |
| DataBroker | AnalyticalQuery | read_only | OK | 5.719ms | 8.456ms | 6.057ms | 4.242ms | 8.771ms | 25 |
| DataBroker | ApplyMigration | mutation | OK | 164.365ms | 164.365ms | 164.365ms | 164.365ms | 164.365ms | 5 |
| DataBroker | ApproveMigrationPlan | mutation | OK | 26.778ms | 26.778ms | 26.778ms | 26.778ms | 26.778ms | 5 |
| DataBroker | BatchSelect | mutation | OK | 5.02ms | 5.196ms | 4.99ms | 4.217ms | 5.623ms | 5 |
| DataBroker | BatchUpsert | mutation | OK | 23.05ms | 27.36ms | 25.233ms | 21.596ms | 31.501ms | 5 |
| DataBroker | BeginTx | mutation | OK | 19.324ms | 21.21ms | 19.649ms | 14.639ms | 24.123ms | 5 |
| DataBroker | CacheDelete | mutation | OK | 5.231ms | 6.087ms | 5.418ms | 4.448ms | 6.26ms | 5 |
| DataBroker | CacheGet | read_only | OK | 5.457ms | 7.732ms | 5.642ms | 3.769ms | 8.046ms | 25 |
| DataBroker | CacheScan | read_only | OK | 8.301ms | 14.309ms | 8.704ms | 5.934ms | 15.911ms | 25 |
| DataBroker | CacheSet | mutation | OK | 4.773ms | 5.031ms | 4.838ms | 4.282ms | 5.72ms | 5 |
| DataBroker | CreateMaterializedView | mutation | OK | 5.882ms | 6.695ms | 6.381ms | 4.961ms | 8.693ms | 5 |
| DataBroker | Delete | mutation | OK | 24.746ms | 27.884ms | 27.054ms | 21.882ms | 36.654ms | 5 |
| DataBroker | DeletePolicy | mutation | OK | 15.274ms | 15.274ms | 15.274ms | 15.274ms | 15.274ms | 5 |
| DataBroker | DismissDlqEvent | mutation | OK | 15.336ms | 16.37ms | 15.546ms | 12.047ms | 19.637ms | 5 |
| DataBroker | DocumentDelete | mutation | OK | 4.332ms | 4.617ms | 5.358ms | 4.047ms | 9.716ms | 5 |
| DataBroker | DocumentFind | read_only | OK | 4.735ms | 7.2ms | 5.049ms | 3.212ms | 9.014ms | 25 |
| DataBroker | DocumentGet | read_only | OK | 5.168ms | 9.319ms | 5.686ms | 3.234ms | 10.461ms | 25 |
| DataBroker | DocumentUpsert | mutation | OK | 4.537ms | 4.667ms | 4.832ms | 4.029ms | 6.529ms | 5 |
| DataBroker | DropResource | destructive | OK | 36.163ms | 36.163ms | 36.163ms | 36.163ms | 36.163ms | 1 |
| DataBroker | EnqueueOutboxEvent | mutation | OK | 10.129ms | 10.129ms | 10.129ms | 10.129ms | 10.129ms | 5 |
| DataBroker | EnsureBaseline | mutation | OK | 14.512ms | 15.093ms | 15.005ms | 12.997ms | 17.421ms | 5 |
| DataBroker | EnsureProject | mutation | OK | 11.611ms | 12.636ms | 11.984ms | 8.696ms | 16.994ms | 5 |
| DataBroker | EnsureResource | mutation | OK | 13.118ms | 13.189ms | 13.412ms | 12.042ms | 16.124ms | 5 |
| DataBroker | GeneratePresignedUrl | mutation | OK | 3.223ms | 3.371ms | 3.154ms | 2.707ms | 3.488ms | 5 |
| DataBroker | GenericDispatch | mutation | OK | 7.42ms | 7.694ms | 7.896ms | 6.256ms | 11.477ms | 5 |
| DataBroker | GetAdminSummary | read_only | OK | 24.57ms | 56.835ms | 28.432ms | 16.262ms | 63.541ms | 25 |
| DataBroker | GetCapabilities | read_only | OK | 7.277ms | 11.531ms | 7.341ms | 4.118ms | 15.116ms | 25 |
| DataBroker | GetCatalogManifest | read_only | OK | 8.708ms | 11.297ms | 8.722ms | 6.448ms | 11.908ms | 25 |
| DataBroker | GetCatalogVersion | read_only | OK | 4.996ms | 12.905ms | 6.112ms | 506µs | 15.345ms | 25 |
| DataBroker | GetCatalogVersions | read_only | OK | 4.142ms | 5.64ms | 4.303ms | 2.662ms | 5.958ms | 25 |
| DataBroker | GetCdcStatus | read_only | OK | 4.059ms | 7.533ms | 4.671ms | 2.625ms | 10.691ms | 25 |
| DataBroker | GetDlqEvent | read_only | OK | 5.01ms | 9.151ms | 5.87ms | 2.785ms | 21.05ms | 25 |
| DataBroker | GetHealthReport | read_only | OK | 2.188ms | 3.609ms | 2.794ms | 1.565ms | 14.562ms | 25 |
| DataBroker | GetMigrationStatus | read_only | OK | 5.717ms | 6.208ms | 5.495ms | 3.898ms | 6.369ms | 25 |
| DataBroker | GetObject | read_only | OK | 19.547ms | 50.916ms | 24.331ms | 5.78ms | 113.639ms | 25 |
| DataBroker | GetSaga | read_only | OK | 4.539ms | 6.071ms | 4.648ms | 3.2ms | 10.75ms | 25 |
| DataBroker | GraphMutate | mutation | OK | 20.256ms | 26.239ms | 104.839ms | 18.57ms | 439.798ms | 5 |
| DataBroker | GraphQuery | read_only | OK | 21.227ms | 39.543ms | 31.82ms | 14.883ms | 257.231ms | 25 |
| DataBroker | InitiateMultipartUpload | mutation | OK | 9.733ms | 10.548ms | 9.988ms | 7.978ms | 12.476ms | 5 |
| DataBroker | LintPolicies | read_only | OK | 5.476ms | 8.375ms | 5.561ms | 3.78ms | 9.337ms | 25 |
| DataBroker | ListAdminAuditLogs | read_only | OK | 5.484ms | 7.919ms | 5.833ms | 3.621ms | 12.021ms | 25 |
| DataBroker | ListDlqEvents | read_only | OK | 5.302ms | 10.017ms | 6.314ms | 3.358ms | 23.099ms | 25 |
| DataBroker | ListMessageSchemas | read_only | OK | 1.882ms | 2.374ms | 1.911ms | 1.119ms | 2.476ms | 25 |
| DataBroker | ListMigrationRuns | read_only | OK | 4.415ms | 9.193ms | 5.123ms | 1.019ms | 9.712ms | 25 |
| DataBroker | ListPolicies | read_only | OK | 5.081ms | 7.67ms | 5.154ms | 2.632ms | 8.418ms | 25 |
| DataBroker | ListProjects | read_only | OK | 4.024ms | 6.218ms | 4.371ms | 2.787ms | 8.549ms | 25 |
| DataBroker | ListResources | read_only | OK | 3.963ms | 5.458ms | 4.154ms | 2.676ms | 7.63ms | 25 |
| DataBroker | ListSagas | read_only | OK | 4.038ms | 6.73ms | 4.313ms | 2.036ms | 7.336ms | 25 |
| DataBroker | LookupMessageSchema | read_only | OK | 2.147ms | 2.391ms | 2.001ms | 1.319ms | 2.4ms | 25 |
| DataBroker | MarkSagaReviewed | mutation | OK | 11.915ms | 12.564ms | 12.613ms | 11.338ms | 15.391ms | 5 |
| DataBroker | PauseCdc | mutation | OK | 10.898ms | 11.347ms | 11.023ms | 9.324ms | 12.808ms | 5 |
| DataBroker | PlanMigration | mutation | OK | 15.375ms | 15.991ms | 16.374ms | 13.938ms | 21.217ms | 5 |
| DataBroker | PreviewCdcRedaction | read_only | OK | 9.234ms | 13.553ms | 9.389ms | 5.962ms | 14.909ms | 25 |
| DataBroker | PublishCDC | mutation | OK | 246.985ms | 246.985ms | 201.475ms | 109.688ms | 247.752ms | 3 |
| DataBroker | PutObject | mutation | OK | 18.58ms | 20.05ms | 17.947ms | 13.605ms | 22.207ms | 5 |
| DataBroker | PutPolicy | destructive | OK | 17.972ms | 17.972ms | 17.972ms | 17.972ms | 17.972ms | 1 |
| DataBroker | QuarantineDlqEvent | mutation | OK | 14.13ms | 14.639ms | 13.641ms | 11.382ms | 15.716ms | 5 |
| DataBroker | ReloadPolicies | destructive | OK | 13.79ms | 13.79ms | 13.79ms | 13.79ms | 13.79ms | 1 |
| DataBroker | ReplayDlqEvent | mutation | OK | 17.755ms | 17.755ms | 17.755ms | 17.755ms | 17.755ms | 5 |
| DataBroker | ResumeCdc | mutation | OK | 13.576ms | 13.989ms | 14.547ms | 12.094ms | 20.398ms | 5 |
| DataBroker | RetrySagaCompensation | mutation | OK | 16.18ms | 16.18ms | 16.18ms | 16.18ms | 16.18ms | 5 |
| DataBroker | RollbackCatalog | destructive | OK | 5.639ms | 5.639ms | 5.639ms | 5.639ms | 5.639ms | 1 |
| DataBroker | ScanProjectionDrift | read_only | OK | 11.888ms | 17.068ms | 11.995ms | 9.326ms | 18.819ms | 25 |
| DataBroker | Select | read_only | OK | 5.317ms | 6.975ms | 5.406ms | 4.325ms | 7.016ms | 25 |
| DataBroker | SelectV2 | read_only | OK | 5.087ms | 10.908ms | 5.926ms | 4.288ms | 16.371ms | 25 |
| DataBroker | StageCatalog | destructive | OK | 242.552ms | 242.552ms | 242.552ms | 242.552ms | 242.552ms | 1 |
| DataBroker | StepDownCdcLeader | mutation | OK | 10.423ms | 10.798ms | 10.602ms | 9.64ms | 11.86ms | 5 |
| DataBroker | TimeSeriesQuery | read_only | OK | 5.984ms | 10.758ms | 6.8ms | 5.097ms | 13.085ms | 25 |
| DataBroker | TimeSeriesWrite | mutation | OK | 2.224ms | 2.708ms | 2.765ms | 2.153ms | 4.539ms | 5 |
| DataBroker | Upsert | mutation | OK | 22.743ms | 24.872ms | 22.967ms | 20.684ms | 25.121ms | 5 |
| DataBroker | ValidateCatalog | destructive | OK | 1.614ms | 1.614ms | 1.614ms | 1.614ms | 1.614ms | 1 |
| DataBroker | VectorBatchUpsert | mutation | OK | 5.434ms | 5.554ms | 12.958ms | 4.371ms | 44.424ms | 5 |
| DataBroker | VectorHybridSearch | read_only | OK | 4.88ms | 5.898ms | 4.914ms | 3.781ms | 7.609ms | 25 |
| DataBroker | VectorSearch | read_only | OK | 4.429ms | 6.019ms | 4.637ms | 3.272ms | 8.977ms | 25 |
| DataBroker | VectorUpsert | mutation | OK | 8.261ms | 9.3ms | 8.613ms | 7.418ms | 9.962ms | 5 |
| DataBroker | VerifyAdminAuditLog | read_only | OK | 6.861ms | 14.33ms | 7.994ms | 5.001ms | 15.236ms | 25 |
| IdentityProviderService | CreateProvider | mutation | OK | 58.946ms | 58.946ms | 58.946ms | 58.946ms | 58.946ms | 5 |
| IdentityProviderService | DisableProvider | mutation | OK | 24.626ms | 28.543ms | 26.278ms | 18.582ms | 37.807ms | 5 |
| IdentityProviderService | ForceJwksRefresh | mutation | OK | 22.446ms | 26.86ms | 32.6ms | 19.124ms | 72.893ms | 5 |
| IdentityProviderService | GetProvider | read_only | OK | 3.467ms | 5.651ms | 3.913ms | 504µs | 7.657ms | 25 |
| IdentityProviderService | ImportSamlMetadata | mutation | OK | 19.194ms | 21.005ms | 28.488ms | 18.704ms | 64.49ms | 5 |
| IdentityProviderService | LinkIdentity | mutation | OK | 19.968ms | 21.462ms | 20.351ms | 15.488ms | 25.015ms | 5 |
| IdentityProviderService | ListExternalIdentities | read_only | OK | 7.461ms | 11.319ms | 7.988ms | 5.073ms | 21.81ms | 25 |
| IdentityProviderService | ListProviders | read_only | OK | 7.882ms | 11.766ms | 8.446ms | 5.229ms | 12.013ms | 25 |
| IdentityProviderService | PreviewClaimMapping | read_only | OK | 4.454ms | 6.219ms | 4.501ms | 3.118ms | 6.46ms | 25 |
| IdentityProviderService | PreviewGroupMapping | read_only | OK | 3.943ms | 8.081ms | 4.441ms | 769µs | 8.763ms | 25 |
| IdentityProviderService | ResolveExternalIdentity | mutation | OK | 6.604ms | 7.875ms | 11.82ms | 5.442ms | 32.661ms | 5 |
| IdentityProviderService | SamlAcs | mutation | OK | 56.074ms | 80.34ms | 64.425ms | 49.094ms | 85.787ms | 5 |
| IdentityProviderService | ScimCreateGroup | mutation | OK | 3.726ms | 4.016ms | 4.394ms | 3.374ms | 7.256ms | 5 |
| IdentityProviderService | ScimCreateUser | mutation | OK | 23.511ms | 26.13ms | 24.434ms | 22.908ms | 26.552ms | 5 |
| IdentityProviderService | ScimDeleteGroup | mutation | OK | 3.471ms | 3.478ms | 3.389ms | 2.871ms | 3.793ms | 5 |
| IdentityProviderService | ScimDeleteUser | mutation | OK | 28.007ms | 28.007ms | 28.007ms | 28.007ms | 28.007ms | 5 |
| IdentityProviderService | ScimGetGroup | mutation | OK | 5.447ms | 5.684ms | 5.574ms | 4.42ms | 7.231ms | 5 |
| IdentityProviderService | ScimGetUser | mutation | OK | 5.292ms | 5.397ms | 5.281ms | 5.026ms | 5.603ms | 5 |
| IdentityProviderService | ScimListGroups | mutation | OK | 3.301ms | 3.329ms | 3.159ms | 2.771ms | 3.512ms | 5 |
| IdentityProviderService | ScimListUsers | mutation | OK | 8.479ms | 8.594ms | 8.229ms | 6.574ms | 10.162ms | 5 |
| IdentityProviderService | ScimPatchGroup | mutation | OK | 8.293ms | 8.613ms | 8.979ms | 6.915ms | 12.948ms | 5 |
| IdentityProviderService | ScimPatchUser | mutation | OK | 29.546ms | 30.231ms | 29.195ms | 18ms | 40.674ms | 5 |
| IdentityProviderService | ScimReplaceUser | mutation | OK | 15.262ms | 15.78ms | 15.879ms | 14.816ms | 18.639ms | 5 |
| IdentityProviderService | StartSamlLogin | mutation | OK | 3.714ms | 3.777ms | 3.679ms | 3.262ms | 4.254ms | 5 |
| IdentityProviderService | TestProviderDiscovery | read_only | OK | 4.339ms | 9.836ms | 4.995ms | 3.184ms | 10.947ms | 25 |
| IdentityProviderService | UnlinkIdentity | mutation | OK | 4.355ms | 5.65ms | 5.158ms | 2.86ms | 8.98ms | 5 |
| IdentityProviderService | UpdateProvider | mutation | OK | 17.302ms | 18.098ms | 16.823ms | 14.928ms | 18.114ms | 5 |
| NotificationService | GetDeliveryStats | read_only | OK | 10.493ms | 30.961ms | 13.085ms | 3.779ms | 32.693ms | 25 |
| NotificationService | GetNotification | read_only | OK | 7.859ms | 14.256ms | 8.473ms | 5.98ms | 15.129ms | 25 |
| NotificationService | GetPreference | read_only | OK | 7.114ms | 11.594ms | 7.488ms | 4.911ms | 11.707ms | 25 |
| NotificationService | GetTemplate | read_only | OK | 6.62ms | 9.99ms | 7.115ms | 5.281ms | 10.717ms | 25 |
| NotificationService | ListNotifications | read_only | OK | 12.953ms | 16.835ms | 13.28ms | 10.411ms | 17.475ms | 25 |
| NotificationService | ListPreferences | read_only | OK | 11.686ms | 15.737ms | 12.431ms | 9.691ms | 17.601ms | 25 |
| NotificationService | ListTemplates | read_only | OK | 13.534ms | 19.321ms | 14.132ms | 9.802ms | 29.702ms | 25 |
| NotificationService | RetryNotification | mutation | OK | 17.729ms | 17.729ms | 17.729ms | 17.729ms | 17.729ms | 5 |
| NotificationService | SendNotification | mutation | OK | 29.989ms | 34.866ms | 32.013ms | 27.986ms | 37.515ms | 5 |
| NotificationService | SetPreference | mutation | OK | 6.717ms | 7.054ms | 6.807ms | 6.126ms | 7.502ms | 5 |
| NotificationService | UpsertTemplate | mutation | OK | 6.268ms | 6.671ms | 5.907ms | 3.828ms | 7.436ms | 5 |
| PeerService | GetPeer | read_only | OK | 7.213ms | 8.917ms | 7.248ms | 4.917ms | 9.241ms | 25 |
| PeerService | JoinRoom | mutation | OK | 23.524ms | 23.524ms | 22.23ms | 18.394ms | 24.181ms | 5 |
| PeerService | JoinSession | mutation | OK | 15.787ms | 16.015ms | 16.243ms | 14.369ms | 19.576ms | 5 |
| PeerService | LeaveRoom | mutation | OK | 5.602ms | 5.646ms | 6.641ms | 3.478ms | 13.498ms | 5 |
| PeerService | ListPeers | read_only | OK | 6.714ms | 8.427ms | 6.885ms | 4.477ms | 10.493ms | 25 |
| RoomService | CloseRoom | mutation | OK | 18.117ms | 23.026ms | 18.866ms | 14.008ms | 24.226ms | 5 |
| RoomService | CreateRoom | mutation | OK | 12.813ms | 12.853ms | 12.79ms | 11.908ms | 14.094ms | 5 |
| RoomService | GetRoom | read_only | OK | 6.179ms | 7.711ms | 6.289ms | 4.855ms | 8.574ms | 25 |
| RoomService | ListRooms | read_only | OK | 6.891ms | 14.439ms | 8.271ms | 5.035ms | 16.868ms | 25 |
| RoomService | UpdateRoom | mutation | OK | 4.532ms | 4.909ms | 4.674ms | 4.406ms | 5.112ms | 5 |
| SignalingService | Signal | mutation | OK | 8.449ms | 8.449ms | 8.449ms | 8.449ms | 8.449ms | 5 |
| StorageService | DeleteFile | mutation | OK | 25.605ms | 25.605ms | 25.605ms | 25.605ms | 25.605ms | 5 |
| StorageService | DownloadFile | read_only | OK | 15.505ms | 25.062ms | 16.485ms | 11.979ms | 26.098ms | 25 |
| StorageService | FinalizeUpload | mutation | OK | 28ms | 28ms | 28ms | 28ms | 28ms | 5 |
| StorageService | GetDownloadUrl | read_only | OK | 7.351ms | 10.621ms | 7.908ms | 6.542ms | 18.846ms | 25 |
| StorageService | GetFile | read_only | OK | 7.026ms | 11.748ms | 8.044ms | 4.926ms | 22.495ms | 25 |
| StorageService | ListFiles | read_only | OK | 12.818ms | 17.549ms | 13.485ms | 10.268ms | 19.87ms | 25 |
| StorageService | RegisterUpload | mutation | OK | 12.147ms | 12.572ms | 12.073ms | 10.905ms | 13.567ms | 5 |
| StorageService | UpdateFile | mutation | OK | 15.921ms | 17.539ms | 16.194ms | 14.696ms | 17.917ms | 5 |
| TenantService | CreateTenant | mutation | OK | 9.043ms | 9.158ms | 9.016ms | 8.526ms | 9.722ms | 5 |
| TenantService | GetTenant | read_only | OK | 6.622ms | 9.086ms | 6.92ms | 4.904ms | 10.682ms | 25 |
| TenantService | GetTenantConfig | read_only | OK | 6.257ms | 7.762ms | 6.358ms | 4.795ms | 8.432ms | 25 |
| TenantService | ListTenants | read_only | OK | 6.507ms | 9.554ms | 6.686ms | 3.787ms | 10.268ms | 25 |
| TenantService | UpdateTenant | mutation | OK | 5.439ms | 6.096ms | 5.465ms | 4.489ms | 6.122ms | 5 |
| TenantService | UpdateTenantConfig | mutation | OK | 17.496ms | 18.183ms | 16.605ms | 12.93ms | 20.125ms | 5 |
| TrackService | ListTracks | read_only | OK | 8.062ms | 13.759ms | 9.127ms | 5.948ms | 20.889ms | 25 |
| TrackService | MuteTrack | mutation | OK | 5.178ms | 5.662ms | 5.401ms | 4.648ms | 6.637ms | 5 |
| TrackService | PublishTrack | mutation | OK | 13.666ms | 20.03ms | 15.027ms | 9.413ms | 21.589ms | 5 |
| TrackService | UnpublishTrack | mutation | OK | 6.16ms | 6.428ms | 6.067ms | 5.241ms | 6.95ms | 5 |
| TurnService | IssueCredentials | mutation | OK | 3.98ms | 4.171ms | 4.093ms | 3.79ms | 4.673ms | 5 |
