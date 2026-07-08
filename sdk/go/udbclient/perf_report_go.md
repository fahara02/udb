# UDB SDK Live Perf — Go (localhost)

RPCs measured: 344   tenant=b97e16a7-3533-494c-92a1-84cc2cda045a

Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities (a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request's reference/ID fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection latency. The TARGET is zero failures; any residual non-OK RPC is listed under Failures for the maintainer to finish.

Unary RPCs = full request→response round-trip. Non-CDC streaming RPCs report time-to-FIRST-RESPONSE with seeded inputs. CDC subscription (PublishCDC) reports time-to-FIRST-EVENT: the harness subscribes, fires a real Upsert that flows outbox→CDC→Kafka, and times the first delivered event. Streaming rows are marked in the note column.

## Seeded fixtures

Captured semantic field → seeded value keys used to resolve request fields: action, admin_reset_mfa_user_id, admin_reset_password_user_id, apply_run_id, approval_token, approve_draft_id, approve_run_id, approved_by, asset_id, assigned_by, auth_challenge_id, auth_token, backup_id, bucket, canary_id, canary_version_id, cancel_workflow_id, catalog_manifest, catalog_manifest_b64, challenge_id, change_password_user_id, change_status_user_id, close_room_id, code, collection, content_type, created_by, csrf_token, definition_id, delete_endpoint_id, delete_file_id, delete_policy_id, delete_role_id, delete_scim_user_id, deleted_by, device_id, disable_mfa_user_id, disable_provider_id, dismiss_dlq_id, dlq_id, document_id, domain, ds_policy_id, egress_id, endpoint_id, event_type, external_identity_id, file_id, file_type, filename, finalize_file_id, gov_exp, instance_id, job_id, join_session_room_id, key_id, kind, leave_peer_id, locale, log_id, mark_saga_id, message_type, migration_id, mongo_collection, name, node_id, notification_id, object, object_key, otp_code, otp_id, owner_id, peer_id, plain_key, policy_draft_id, policy_id, policy_version_id, project, project_id, provider_id, purge_tenant_id, quarantine_dlq_id, recipient_id, record_id, recovery_code, refresh_session_id, refresh_token, reg_challenge_id, reject_draft_id, rejected_by, relation, release_fencing_token, renew_fencing_token, replay_dlq_id, reset_otp_code, reset_otp_id, resource, resource_name, restore_tenant_id, retry_saga_id, revoke_device_id, revoke_device_user_id, revoke_key_id, revoke_recovery_user_id, revoked_by, role, role_code, role_id, rollback_policy_set_id, rollback_target_version_id, room_id, saga_id, saml_provider_id, scim_group_id, scim_user_id, session_id, signal_peer_id, stage_name, step_id, subject, tenant, tenant_code, tenant_id, token, topic_pattern, track_id, ts_table, unpublish_track_id, update_draft_id, update_key_id, updated_by, user_id, user_role_id, username, vault_ciphertext, vault_create_key_name, vault_db_role, vault_delete_secret_path, vault_destroy_secret_path, vault_key_name, vault_put_secret_path, vault_secret_path, vault_signature, workflow_id

## Per-service mean latency (mean of per-RPC means)

| Service | RPCs | mean |
|---|---:|---:|
| AuthnService | 50 | 115.698ms |
| BackupService | 8 | 407.203ms |
| DataBroker | 77 | 38.451ms |
| AuthzService | 41 | 34.978ms |
| IdentityProviderService | 27 | 24.436ms |
| TenantService | 7 | 70.911ms |
| ControlPlaneService | 6 | 71.518ms |
| CacheService | 7 | 50.657ms |
| VaultService | 14 | 24.821ms |
| ConfigService | 5 | 68.548ms |
| StorageService | 8 | 32.589ms |
| NotificationService | 12 | 20.865ms |
| AssetService | 8 | 26.669ms |
| EmbeddingService | 6 | 29.883ms |
| ApiKeyService | 9 | 19.891ms |
| SearchService | 5 | 29.249ms |
| RoomService | 9 | 15.255ms |
| LockService | 3 | 42.829ms |
| PeerService | 5 | 23.79ms |
| WorkflowService | 5 | 23.396ms |
| MeteringService | 6 | 18.545ms |
| SchedulerService | 6 | 18.128ms |
| WebhookService | 6 | 17.222ms |
| TrackService | 4 | 17.108ms |
| AnalyticsService | 7 | 7.982ms |
| SignalingService | 1 | 23.78ms |
| LiveQueryService | 1 | 20.317ms |
| TurnService | 1 | 12.506ms |

## Failures — still to fix (0)

No RPC returned a non-OK gRPC status — every RPC ran its success path.

## Slowest 25 RPCs by p99

| RPC | api_alias | operation_id | kind | err | p50 | p99 | mean | iters | note |
|---|---|---|---|---|---:|---:|---:|---:|---|
| BackupService/StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 1.799201s | 1.814677s | 1.804755s | 5 | mutation (seeded success path) |
| AuthnService/ChangePassword | change_password | changePassword | mutation | OK | 1.644847s | 1.644847s | 1.644847s | 5 | mutation (seeded success path) |
| BackupService/RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 1.310661s | 1.310661s | 1.310661s | 1 | destructive: 1 real call against a seeded disposable target |
| AuthnService/ResetPassword | reset_password | resetPassword | mutation | OK | 988.986ms | 988.986ms | 988.986ms | 5 | mutation (seeded success path) |
| AuthnService/Login | login | login | mutation | OK | 940.984ms | 957.287ms | 932.025ms | 5 | mutation (seeded success path) |
| DataBroker/StageCatalog | stage_catalog | stageCatalog | destructive | OK | 935.046ms | 935.046ms | 935.046ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthnService/CreateUser | create_user | createUser | mutation | OK | 881.058ms | 881.058ms | 881.058ms | 5 | mutation (seeded success path) |
| TenantService/PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 386.206ms | 386.206ms | 386.206ms | 1 | destructive: 1 real call against a seeded disposable target |
| DataBroker/ApplyMigration | apply_migration | applyMigration | mutation | OK | 286.853ms | 286.853ms | 286.853ms | 5 | mutation (seeded success path) |
| DataBroker/PublishCDC | publish_cdc | publishCdc | mutation | OK | 247.583ms | 247.583ms | 267.341ms | 3 | cdc subscription: time-to-first-event (real mutation produced) |
| ConfigService/EvaluateFlags | evaluate_flags | evaluateFlags | read_only | OK | 96.24ms | 244.244ms | 115.004ms | 25 | read_only (seeded success path) |
| AuthnService/FinishWebAuthnAuthentication | finish_web_authn_authentication | finishWebAuthnAuthentication | mutation | OK | 216.794ms | 216.794ms | 216.794ms | 5 | mutation (seeded success path) |
| IdentityProviderService/SamlAcs | saml_acs | samlAcs | mutation | OK | 143.886ms | 171.126ms | 156.725ms | 5 | mutation (seeded success path) |
| ControlPlaneService/ListNodeStates | list_node_states | listNodeStates | read_only | OK | 57.796ms | 152.986ms | 71.529ms | 25 | read_only (seeded success path) |
| ConfigService/ListFlags | list_flags | listFlags | read_only | OK | 68.599ms | 150.095ms | 82.191ms | 25 | read_only (seeded success path) |
| ControlPlaneService/DeltaResources | delta_resources | deltaResources | mutation | OK | 140.734ms | 146.121ms | 147.053ms | 5 | streaming: time-to-first-response (seeded; bidi) |
| CacheService/Scan | cache_scan | cacheNamespaceScan | read_only | OK | 51.505ms | 141.134ms | 63.924ms | 25 | read_only (seeded success path) |
| AuthzService/PromoteCanary | promote_canary | promoteCanary | destructive | OK | 139.924ms | 139.924ms | 139.924ms | 1 | destructive: 1 real call against a seeded disposable target |
| EmbeddingService/ListSources | list_sources | listEmbeddingSources | read_only | OK | 28.136ms | 137.497ms | 37.917ms | 25 | read_only (seeded success path) |
| ConfigService/GetFlag | get_flag | getFlag | read_only | OK | 65.306ms | 131.394ms | 82.769ms | 25 | read_only (seeded success path) |
| CacheService/GetNamespaceStats | get_cache_namespace_stats | getCacheNamespaceStats | read_only | OK | 91.398ms | 126.167ms | 97.117ms | 25 | read_only (seeded success path) |
| ControlPlaneService/StreamResources | stream_resources | streamResources | mutation | OK | 111.537ms | 112.764ms | 122.696ms | 5 | streaming: time-to-first-response (seeded; bidi) |
| DataBroker/ValidateCatalog | validate_catalog | validateCatalog | destructive | OK | 111.402ms | 111.402ms | 111.402ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthzService/RollbackPolicyVersion | rollback_policy_version | rollbackPolicyVersion | destructive | OK | 103.993ms | 103.993ms | 103.993ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthnService/VerifyOTP | verify_otp | verifyOtp | read_only | OK | 45.086ms | 103.796ms | 52.359ms | 25 | read_only (seeded success path) |

## Full per-RPC table (sorted by service, then name)

| Service | RPC | api_alias | operation_id | kind | err | p50 | p99 | mean | min | max | iters |
|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|
| AnalyticsService | GetExecutorPerformance | get_executor_performance | getExecutorPerformance | read_only | OK | 9.093ms | 12.505ms | 8.381ms | 3.951ms | 13.197ms | 25 |
| AnalyticsService | GetPipelineSummary | get_pipeline_summary | getPipelineSummary | read_only | OK | 9.475ms | 12.751ms | 8.718ms | 5.189ms | 13.427ms | 25 |
| AnalyticsService | GetReconciliationAnalytics | get_reconciliation_analytics | getReconciliationAnalytics | read_only | OK | 8.298ms | 20.616ms | 9.065ms | 4.329ms | 24.239ms | 25 |
| AnalyticsService | GetSlaCompliance | get_sla_compliance | getSlaCompliance | read_only | OK | 5.833ms | 9.831ms | 6.357ms | 4.429ms | 10.689ms | 25 |
| AnalyticsService | GetThroughput | get_throughput | getThroughput | read_only | OK | 6.755ms | 9.872ms | 7.053ms | 4.67ms | 11.326ms | 25 |
| AnalyticsService | RecordPipelineMetric | record_pipeline_metric | recordPipelineMetric | mutation | OK | 9.173ms | 9.569ms | 9.294ms | 7.535ms | 11.89ms | 5 |
| AnalyticsService | TriggerSnapshot | trigger_snapshot | triggerSnapshot | mutation | OK | 6.59ms | 7.862ms | 7.006ms | 5.948ms | 8.339ms | 5 |
| ApiKeyService | CreateApiKey | create_api_key | createApiKey | mutation | OK | 20.507ms | 21.329ms | 21.953ms | 15.875ms | 31.763ms | 5 |
| ApiKeyService | EmergencyRevokeApiKeys | emergency_revoke_api_keys | emergencyRevokeApiKeys | destructive | OK | 53.76ms | 53.76ms | 53.76ms | 53.76ms | 53.76ms | 1 |
| ApiKeyService | GetApiKey | get_api_key | getApiKey | read_only | OK | 7.081ms | 10.497ms | 7.309ms | 4.464ms | 12.279ms | 25 |
| ApiKeyService | GetApiKeyUsageStats | get_api_key_usage_stats | getApiKeyUsageStats | read_only | OK | 10.835ms | 13.95ms | 9.574ms | 4.233ms | 14.367ms | 25 |
| ApiKeyService | ListApiKeys | list_api_keys | listApiKeys | read_only | OK | 6.934ms | 9.173ms | 7.106ms | 4.063ms | 9.65ms | 25 |
| ApiKeyService | RevokeApiKey | revoke_api_key | revokeApiKey | mutation | OK | 16.709ms | 16.709ms | 16.709ms | 16.709ms | 16.709ms | 5 |
| ApiKeyService | RotateApiKey | rotate_api_key | rotateApiKey | mutation | OK | 30.559ms | 30.559ms | 30.559ms | 30.559ms | 30.559ms | 5 |
| ApiKeyService | UpdateApiKey | update_api_key | updateApiKey | mutation | OK | 19.098ms | 19.855ms | 20.566ms | 16.622ms | 28.163ms | 5 |
| ApiKeyService | ValidateApiKey | validate_api_key | validateApiKey | read_only | OK | 10.412ms | 18.578ms | 11.481ms | 7.818ms | 24.535ms | 25 |
| AssetService | CompleteStep | complete_step | completeStep | mutation | OK | 34.506ms | 46.114ms | 38.338ms | 30.173ms | 47.748ms | 5 |
| AssetService | CreatePipelineDefinition | create_pipeline_definition | createPipelineDefinition | mutation | OK | 20.751ms | 20.751ms | 20.751ms | 20.751ms | 20.751ms | 5 |
| AssetService | GetAsset | get_asset | getAsset | read_only | OK | 17.575ms | 24.559ms | 18.068ms | 12.919ms | 24.634ms | 25 |
| AssetService | GetPipeline | get_pipeline | getPipeline | read_only | OK | 15.83ms | 27.046ms | 18.2ms | 13.023ms | 46.959ms | 25 |
| AssetService | GetPipelineDefinition | get_pipeline_definition | getPipelineDefinition | read_only | OK | 18.258ms | 55.463ms | 22.309ms | 14.083ms | 60.798ms | 25 |
| AssetService | ListAssets | list_assets | listAssets | read_only | OK | 20.415ms | 27.58ms | 21.25ms | 13.716ms | 34.358ms | 25 |
| AssetService | RegisterAsset | register_asset | registerAsset | mutation | OK | 29.692ms | 35.735ms | 31.566ms | 23.169ms | 44.604ms | 5 |
| AssetService | StartPipeline | start_pipeline | startPipeline | mutation | OK | 25.723ms | 30.923ms | 42.868ms | 9.141ms | 129.533ms | 5 |
| AuthnService | AdminResetMfa | admin_reset_mfa | adminResetMfa | destructive | OK | 61.087ms | 61.087ms | 61.087ms | 61.087ms | 61.087ms | 1 |
| AuthnService | AdminResetPassword | admin_reset_password | adminResetPassword | destructive | OK | 24.041ms | 24.041ms | 24.041ms | 24.041ms | 24.041ms | 1 |
| AuthnService | AdminRevokeAllTenantSessions | admin_revoke_all_tenant_sessions | adminRevokeAllTenantSessions | destructive | OK | 34.947ms | 34.947ms | 34.947ms | 34.947ms | 34.947ms | 1 |
| AuthnService | AdminRevokeAllUserSessions | admin_revoke_all_user_sessions | adminRevokeAllUserSessions | destructive | OK | 27.642ms | 27.642ms | 27.642ms | 27.642ms | 27.642ms | 1 |
| AuthnService | AdminRevokeSession | admin_revoke_session | adminRevokeSession | destructive | OK | 24.899ms | 24.899ms | 24.899ms | 24.899ms | 24.899ms | 1 |
| AuthnService | Authenticate | authenticate | authenticate | read_only | OK | 44.097ms | 96.879ms | 50.091ms | 31.712ms | 108.042ms | 25 |
| AuthnService | ChangePassword | change_password | changePassword | mutation | OK | 1.644847s | 1.644847s | 1.644847s | 1.644847s | 1.644847s | 5 |
| AuthnService | ChangeUserStatus | change_user_status | changeUserStatus | destructive | OK | 41.206ms | 41.206ms | 41.206ms | 41.206ms | 41.206ms | 1 |
| AuthnService | ConfirmMFAEnrollment | confirm_mfaenrollment | confirmMfaenrollment | mutation | OK | 5.394ms | 5.499ms | 5.628ms | 4.719ms | 7.461ms | 5 |
| AuthnService | CreateSession | create_session | createSession | mutation | OK | 9.159ms | 10.038ms | 9.711ms | 7.707ms | 12.733ms | 5 |
| AuthnService | CreateUser | create_user | createUser | mutation | OK | 881.058ms | 881.058ms | 881.058ms | 881.058ms | 881.058ms | 5 |
| AuthnService | DeleteWebAuthnCredential | delete_web_authn_credential | deleteWebAuthnCredential | mutation | OK | 30.287ms | 35.411ms | 30.735ms | 21.764ms | 40.013ms | 5 |
| AuthnService | DisableMfaFactor | disable_mfa_factor | disableMfaFactor | mutation | OK | 35.145ms | 40.418ms | 36.621ms | 28.35ms | 47.6ms | 5 |
| AuthnService | EmergencyRevoke | emergency_revoke | emergencyRevoke | destructive | OK | 19.421ms | 19.421ms | 19.421ms | 19.421ms | 19.421ms | 1 |
| AuthnService | EnrollMFA | enroll_mfa | enrollMfa | mutation | OK | 20.815ms | 24.151ms | 23.291ms | 19.525ms | 32.341ms | 5 |
| AuthnService | FinishWebAuthnAuthentication | finish_web_authn_authentication | finishWebAuthnAuthentication | mutation | OK | 216.794ms | 216.794ms | 216.794ms | 216.794ms | 216.794ms | 5 |
| AuthnService | FinishWebAuthnRegistration | finish_web_authn_registration | finishWebAuthnRegistration | mutation | OK | 52.409ms | 52.409ms | 52.409ms | 52.409ms | 52.409ms | 5 |
| AuthnService | ForgotPassword | forgot_password | forgotPassword | mutation | OK | 30.464ms | 37.687ms | 33.936ms | 23.816ms | 47.533ms | 5 |
| AuthnService | GenerateRecoveryCodes | generate_recovery_codes | generateRecoveryCodes | mutation | OK | 42.771ms | 48.82ms | 43.374ms | 36.188ms | 51.627ms | 5 |
| AuthnService | GetJwks | get_jwks | getJwks | read_only | OK | 6.886ms | 13.553ms | 7.298ms | 3.489ms | 15.456ms | 25 |
| AuthnService | GetMfaPolicy | get_mfa_policy | getMfaPolicy | read_only | OK | 5.434ms | 12.634ms | 6.504ms | 3.87ms | 16.72ms | 25 |
| AuthnService | GetSession | get_session | getSession | read_only | OK | 7.517ms | 10.037ms | 7.528ms | 5.039ms | 14.149ms | 25 |
| AuthnService | GetUser | get_user | getUser | read_only | OK | 5.377ms | 6.976ms | 5.53ms | 4.082ms | 8.298ms | 25 |
| AuthnService | IntrospectToken | introspect_token | introspectToken | read_only | OK | 50.827ms | 73.392ms | 54.695ms | 35.142ms | 99.585ms | 25 |
| AuthnService | IssueMfaChallenge | issue_mfa_challenge | issueMfaChallenge | mutation | OK | 16.294ms | 30.545ms | 21.748ms | 14.37ms | 32.199ms | 5 |
| AuthnService | ListDevices | list_devices | listDevices | read_only | OK | 6.52ms | 10.316ms | 6.83ms | 4.134ms | 12.438ms | 25 |
| AuthnService | ListMfaFactors | list_mfa_factors | listMfaFactors | read_only | OK | 10.811ms | 20.09ms | 12.721ms | 7.658ms | 31.16ms | 25 |
| AuthnService | ListSessions | list_sessions | listSessions | read_only | OK | 16.594ms | 42.229ms | 18.914ms | 9.12ms | 48.035ms | 25 |
| AuthnService | ListUsers | list_users | listUsers | read_only | OK | 13.24ms | 22.523ms | 15.803ms | 7.598ms | 51.654ms | 25 |
| AuthnService | ListWebAuthnCredentials | list_web_authn_credentials | listWebAuthnCredentials | read_only | OK | 8.634ms | 15.283ms | 8.892ms | 4.464ms | 17.885ms | 25 |
| AuthnService | Login | login | login | mutation | OK | 940.984ms | 957.287ms | 932.025ms | 771.707ms | 1.205122s | 5 |
| AuthnService | Logout | logout | logout | mutation | OK | 14.994ms | 19.286ms | 16.051ms | 10.391ms | 20.653ms | 5 |
| AuthnService | PutMfaPolicy | put_mfa_policy | putMfaPolicy | mutation | OK | 8.388ms | 11.918ms | 10.03ms | 7.766ms | 14.236ms | 5 |
| AuthnService | RefreshSession | refresh_session | refreshSession | mutation | OK | 33.502ms | 35.847ms | 33.567ms | 25.059ms | 42.469ms | 5 |
| AuthnService | RefreshToken | refresh_token | refreshToken | mutation | OK | 24.07ms | 24.07ms | 24.07ms | 24.07ms | 24.07ms | 5 |
| AuthnService | RenamePasskey | rename_passkey | renamePasskey | mutation | OK | 11.468ms | 13.211ms | 11.87ms | 9.594ms | 14.632ms | 5 |
| AuthnService | ResendOTP | resend_otp | resendOtp | mutation | OK | 24.037ms | 24.962ms | 23.961ms | 20.603ms | 27.424ms | 5 |
| AuthnService | ResetPassword | reset_password | resetPassword | mutation | OK | 988.986ms | 988.986ms | 988.986ms | 988.986ms | 988.986ms | 5 |
| AuthnService | RevokeDevice | revoke_device | revokeDevice | mutation | OK | 26.031ms | 26.031ms | 26.031ms | 26.031ms | 26.031ms | 5 |
| AuthnService | RevokeRecoveryCodes | revoke_recovery_codes | revokeRecoveryCodes | mutation | OK | 24.652ms | 26.122ms | 25.251ms | 21.848ms | 29.432ms | 5 |
| AuthnService | RevokeSession | revoke_session | revokeSession | mutation | OK | 10.871ms | 12.522ms | 11.321ms | 9.923ms | 12.974ms | 5 |
| AuthnService | SendOTP | send_otp | sendOtp | mutation | OK | 21.579ms | 22.318ms | 21.564ms | 20.374ms | 22.362ms | 5 |
| AuthnService | SendPhoneVerification | send_phone_verification | sendPhoneVerification | mutation | OK | 17.569ms | 19.219ms | 19.017ms | 16.243ms | 24.773ms | 5 |
| AuthnService | StartWebAuthnAuthentication | start_web_authn_authentication | startWebAuthnAuthentication | mutation | OK | 18.286ms | 22.501ms | 20.58ms | 17.156ms | 27.776ms | 5 |
| AuthnService | StartWebAuthnRegistration | start_web_authn_registration | startWebAuthnRegistration | mutation | OK | 24.465ms | 25.49ms | 22.645ms | 17.117ms | 27.095ms | 5 |
| AuthnService | UpdateUser | update_user | updateUser | mutation | OK | 12.688ms | 13.325ms | 12.932ms | 11.47ms | 14.658ms | 5 |
| AuthnService | ValidateCSRF | validate_csrf | validateCsrf | read_only | OK | 6.881ms | 9.229ms | 7.139ms | 4.973ms | 12.82ms | 25 |
| AuthnService | ValidateToken | validate_token | validateToken | read_only | OK | 45.952ms | 81.432ms | 47.864ms | 28.275ms | 82.871ms | 25 |
| AuthnService | VerifyMfaChallenge | verify_mfa_challenge | verifyMfaChallenge | read_only | OK | 41.347ms | 85.276ms | 49.431ms | 10.443ms | 121.202ms | 25 |
| AuthnService | VerifyOTP | verify_otp | verifyOtp | read_only | OK | 45.086ms | 103.796ms | 52.359ms | 19.921ms | 131.489ms | 25 |
| AuthzService | ActivateCanary | activate_canary | activateCanary | destructive | OK | 47.309ms | 47.309ms | 47.309ms | 47.309ms | 47.309ms | 1 |
| AuthzService | ActivatePolicyVersion | activate_policy_version | activatePolicyVersion | destructive | OK | 69.943ms | 69.943ms | 69.943ms | 69.943ms | 69.943ms | 1 |
| AuthzService | ApprovePolicyDraft | approve_policy_draft | approvePolicyDraft | mutation | OK | 52.732ms | 52.732ms | 52.732ms | 52.732ms | 52.732ms | 5 |
| AuthzService | AssignRole | assign_role | assignRole | mutation | OK | 35.929ms | 37.193ms | 35.925ms | 32.164ms | 40.208ms | 5 |
| AuthzService | Authorize | authorize | authorize | read_only | OK | 39.237ms | 53.751ms | 39.372ms | 29.033ms | 61.604ms | 25 |
| AuthzService | BatchCheckPermissions | batch_check_permissions | batchCheckPermissions | read_only | OK | 20.879ms | 75.835ms | 29.207ms | 10.873ms | 78.78ms | 25 |
| AuthzService | CheckAccess | check_access | checkAccess | read_only | OK | 15.569ms | 18.601ms | 15.014ms | 10.687ms | 22.264ms | 25 |
| AuthzService | CreatePolicyDraft | create_policy_draft | createPolicyDraft | mutation | OK | 72.109ms | 95.62ms | 80.765ms | 48.178ms | 124.343ms | 5 |
| AuthzService | CreatePolicyRule | create_policy_rule | createPolicyRule | mutation | OK | 27.438ms | 28.201ms | 29.767ms | 22.869ms | 44.807ms | 5 |
| AuthzService | CreateRole | create_role | createRole | mutation | OK | 43.18ms | 43.18ms | 43.18ms | 43.18ms | 43.18ms | 5 |
| AuthzService | DeletePolicyRule | delete_policy_rule | deletePolicyRule | mutation | OK | 11.765ms | 15.491ms | 14.832ms | 11.169ms | 24.171ms | 5 |
| AuthzService | DeleteRole | delete_role | deleteRole | mutation | OK | 35.98ms | 36.965ms | 40.798ms | 22.761ms | 79.717ms | 5 |
| AuthzService | DiffPolicyDraft | diff_policy_draft | diffPolicyDraft | read_only | OK | 18.597ms | 26.508ms | 19.705ms | 12.878ms | 36.859ms | 25 |
| AuthzService | ExplainPolicy | explain_policy | explainPolicy | read_only | OK | 12.94ms | 19.715ms | 14.306ms | 9.184ms | 20.377ms | 25 |
| AuthzService | GetAuthzRevision | get_authz_revision | getAuthzRevision | read_only | OK | 5.346ms | 8.054ms | 5.514ms | 3.399ms | 8.327ms | 25 |
| AuthzService | GetCanaryStatus | get_canary_status | getCanaryStatus | read_only | OK | 15.901ms | 23.07ms | 16.431ms | 11.494ms | 24.592ms | 25 |
| AuthzService | GetNativeAccess | get_native_access | getNativeAccess | read_only | OK | 32.919ms | 46.548ms | 34.187ms | 27.207ms | 48.849ms | 25 |
| AuthzService | GetPolicyBundle | get_policy_bundle | getPolicyBundle | read_only | OK | 11.889ms | 16.351ms | 12.421ms | 8.917ms | 17.611ms | 25 |
| AuthzService | GetPolicyRule | get_policy_rule | getPolicyRule | read_only | OK | 8.189ms | 12.247ms | 8.454ms | 4.925ms | 12.789ms | 25 |
| AuthzService | GetRole | get_role | getRole | read_only | OK | 7.286ms | 11.45ms | 7.483ms | 4.262ms | 12.851ms | 25 |
| AuthzService | InvalidatePolicyBundles | invalidate_policy_bundles | invalidatePolicyBundles | destructive | OK | 44.992ms | 44.992ms | 44.992ms | 44.992ms | 44.992ms | 1 |
| AuthzService | LintAuthzPolicies | lint_authz_policies | lintAuthzPolicies | read_only | OK | 2.753ms | 3.873ms | 2.847ms | 1.458ms | 4.64ms | 25 |
| AuthzService | ListAccessDecisionAudits | list_access_decision_audits | listAccessDecisionAudits | read_only | OK | 20.664ms | 34.643ms | 21.98ms | 11.974ms | 36.031ms | 25 |
| AuthzService | ListPolicyRules | list_policy_rules | listPolicyRules | read_only | OK | 6.401ms | 9.569ms | 6.692ms | 4.225ms | 12.21ms | 25 |
| AuthzService | ListPolicyVersions | list_policy_versions | listPolicyVersions | read_only | OK | 15.554ms | 20.666ms | 15.881ms | 12.01ms | 24.176ms | 25 |
| AuthzService | ListRoles | list_roles | listRoles | read_only | OK | 6.435ms | 8.903ms | 6.796ms | 4.376ms | 12.871ms | 25 |
| AuthzService | ListUserPermissions | list_user_permissions | listUserPermissions | read_only | OK | 3.319ms | 5.786ms | 3.497ms | 1.85ms | 6.838ms | 25 |
| AuthzService | ListUserRoles | list_user_roles | listUserRoles | read_only | OK | 7.286ms | 10.758ms | 7.955ms | 4.63ms | 21.348ms | 25 |
| AuthzService | MigrateLegacyPolicies | migrate_legacy_policies | migrateLegacyPolicies | destructive | OK | 50.49ms | 50.49ms | 50.49ms | 50.49ms | 50.49ms | 1 |
| AuthzService | PromoteCanary | promote_canary | promoteCanary | destructive | OK | 139.924ms | 139.924ms | 139.924ms | 139.924ms | 139.924ms | 1 |
| AuthzService | PutAuthzPolicy | put_authz_policy | putAuthzPolicy | mutation | OK | 31.34ms | 39.744ms | 38.448ms | 26.518ms | 65.935ms | 5 |
| AuthzService | PutRelationship | put_relationship | putRelationship | mutation | OK | 31.787ms | 32.235ms | 31.72ms | 29.413ms | 33.664ms | 5 |
| AuthzService | PutRoleBinding | put_role_binding | putRoleBinding | mutation | OK | 22.89ms | 23.838ms | 24.115ms | 20.827ms | 30.802ms | 5 |
| AuthzService | RejectPolicyDraft | reject_policy_draft | rejectPolicyDraft | mutation | OK | 42.41ms | 42.41ms | 42.41ms | 42.41ms | 42.41ms | 5 |
| AuthzService | RevokeRole | revoke_role | revokeRole | mutation | OK | 15.958ms | 17.823ms | 22.142ms | 8.749ms | 56.595ms | 5 |
| AuthzService | RollbackPolicyVersion | rollback_policy_version | rollbackPolicyVersion | destructive | OK | 103.993ms | 103.993ms | 103.993ms | 103.993ms | 103.993ms | 1 |
| AuthzService | SeedBuiltinRoles | seed_builtin_roles | seedBuiltinRoles | mutation | OK | 81.146ms | 91.302ms | 81.445ms | 62.538ms | 92.353ms | 5 |
| AuthzService | SimulatePolicy | simulate_policy | simulatePolicy | mutation | OK | 31.383ms | 34.913ms | 30.567ms | 18.369ms | 45.033ms | 5 |
| AuthzService | SubmitPolicyDraft | submit_policy_draft | submitPolicyDraft | mutation | OK | 41.895ms | 41.895ms | 41.895ms | 41.895ms | 41.895ms | 5 |
| AuthzService | UpdatePolicyDraft | update_policy_draft | updatePolicyDraft | mutation | OK | 39.052ms | 41.978ms | 42.105ms | 37.193ms | 53.453ms | 5 |
| AuthzService | UpdateRole | update_role | updateRole | mutation | OK | 59.447ms | 66.642ms | 56.86ms | 38.757ms | 69.088ms | 5 |
| BackupService | DeleteBackupPolicy | delete_backup_policy | deleteBackupPolicy | mutation | OK | 26.981ms | 30.92ms | 26.668ms | 18.159ms | 37.684ms | 5 |
| BackupService | GetBackup | get_backup | getBackup | read_only | OK | 28.451ms | 42.949ms | 29.875ms | 20.647ms | 44.489ms | 25 |
| BackupService | GetBackupPolicy | get_backup_policy | getBackupPolicy | read_only | OK | 16.714ms | 23.47ms | 17.085ms | 11.718ms | 26.486ms | 25 |
| BackupService | ListBackupPolicies | list_backup_policies | listBackupPolicies | read_only | OK | 14.333ms | 18.193ms | 14.943ms | 11.642ms | 18.633ms | 25 |
| BackupService | ListBackups | list_backups | listBackups | read_only | OK | 16.174ms | 20.528ms | 16.082ms | 11.77ms | 21.173ms | 25 |
| BackupService | PutBackupPolicy | put_backup_policy | putBackupPolicy | mutation | OK | 39.421ms | 39.624ms | 37.552ms | 31.559ms | 40.24ms | 5 |
| BackupService | RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 1.310661s | 1.310661s | 1.310661s | 1.310661s | 1.310661s | 1 |
| BackupService | StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 1.799201s | 1.814677s | 1.804755s | 1.674689s | 2.049774s | 5 |
| CacheService | CreateNamespace | create_cache_namespace | createCacheNamespace | mutation | OK | 17.807ms | 22.569ms | 19.216ms | 13.173ms | 24.761ms | 5 |
| CacheService | Delete | cache_delete | cacheNamespaceDelete | mutation | OK | 15.143ms | 16.29ms | 14.847ms | 11.623ms | 17.274ms | 5 |
| CacheService | DeleteNamespace | delete_cache_namespace | deleteCacheNamespace | destructive | OK | 75.41ms | 75.41ms | 75.41ms | 75.41ms | 75.41ms | 1 |
| CacheService | Get | cache_get | cacheNamespaceGet | read_only | OK | 61.021ms | 94.613ms | 63.587ms | 38.685ms | 147.308ms | 25 |
| CacheService | GetNamespaceStats | get_cache_namespace_stats | getCacheNamespaceStats | read_only | OK | 91.398ms | 126.167ms | 97.117ms | 74.534ms | 127.137ms | 25 |
| CacheService | Scan | cache_scan | cacheNamespaceScan | read_only | OK | 51.505ms | 141.134ms | 63.924ms | 20.221ms | 274.513ms | 25 |
| CacheService | Set | cache_set | cacheNamespaceSet | mutation | OK | 20.548ms | 22.746ms | 20.497ms | 17.959ms | 23.211ms | 5 |
| ConfigService | DeleteFlag | delete_flag | deleteFlag | destructive | OK | 30.413ms | 30.413ms | 30.413ms | 30.413ms | 30.413ms | 1 |
| ConfigService | EvaluateFlags | evaluate_flags | evaluateFlags | read_only | OK | 96.24ms | 244.244ms | 115.004ms | 44.551ms | 349.357ms | 25 |
| ConfigService | GetFlag | get_flag | getFlag | read_only | OK | 65.306ms | 131.394ms | 82.769ms | 43.013ms | 209.274ms | 25 |
| ConfigService | ListFlags | list_flags | listFlags | read_only | OK | 68.599ms | 150.095ms | 82.191ms | 40.947ms | 150.671ms | 25 |
| ConfigService | PutFlag | put_flag | putFlag | mutation | OK | 30.162ms | 34.766ms | 32.364ms | 26.614ms | 40.288ms | 5 |
| ControlPlaneService | AckStatus | ack_status | ackStatus | mutation | OK | 10.348ms | 12.476ms | 11.225ms | 9.13ms | 14.016ms | 5 |
| ControlPlaneService | DeltaResources | delta_resources | deltaResources | mutation | OK | 140.734ms | 146.121ms | 147.053ms | 129.044ms | 185.078ms | 5 |
| ControlPlaneService | GetResources | get_resources | getResources | read_only | OK | 6.178ms | 12.358ms | 7.177ms | 4.947ms | 22.747ms | 25 |
| ControlPlaneService | ListNodeStates | list_node_states | listNodeStates | read_only | OK | 57.796ms | 152.986ms | 71.529ms | 45.682ms | 154.589ms | 25 |
| ControlPlaneService | RollbackResources | rollback_resources | rollbackResources | mutation | OK | 65.662ms | 70.049ms | 69.426ms | 64.349ms | 81.744ms | 5 |
| ControlPlaneService | StreamResources | stream_resources | streamResources | mutation | OK | 111.537ms | 112.764ms | 122.696ms | 104.042ms | 175.562ms | 5 |
| DataBroker | ActivateCatalog | activate_catalog | activateCatalog | destructive | OK | 8.471ms | 8.471ms | 8.471ms | 8.471ms | 8.471ms | 1 |
| DataBroker | AnalyticalQuery | analytical_query | analyticalQuery | read_only | OK | 10.684ms | 13.409ms | 10.686ms | 6.908ms | 15.385ms | 25 |
| DataBroker | ApplyMigration | apply_migration | applyMigration | mutation | OK | 286.853ms | 286.853ms | 286.853ms | 286.853ms | 286.853ms | 5 |
| DataBroker | ApproveMigrationPlan | approve_migration_plan | approveMigrationPlan | mutation | OK | 43.178ms | 43.178ms | 43.178ms | 43.178ms | 43.178ms | 1 |
| DataBroker | BatchSelect | batch_select | batchSelect | mutation | OK | 24.018ms | 25.68ms | 27.494ms | 14.757ms | 52.972ms | 5 |
| DataBroker | BatchUpsert | batch_upsert | batchUpsert | mutation | OK | 83.427ms | 88.243ms | 94.398ms | 70.65ms | 156.477ms | 5 |
| DataBroker | BeginTx | begin_tx | beginTx | mutation | OK | 30.996ms | 32.229ms | 32.885ms | 23.781ms | 48.451ms | 5 |
| DataBroker | CacheDelete | cache_delete | cacheDelete | mutation | OK | 7.064ms | 7.119ms | 7.091ms | 6.414ms | 7.956ms | 5 |
| DataBroker | CacheGet | cache_get | cacheGet | read_only | OK | 7.621ms | 11.259ms | 8.191ms | 6.388ms | 11.369ms | 25 |
| DataBroker | CacheScan | cache_scan | cacheScan | read_only | OK | 13.079ms | 16.636ms | 12.871ms | 9.699ms | 20.055ms | 25 |
| DataBroker | CacheSet | cache_set | cacheSet | mutation | OK | 10.242ms | 10.534ms | 9.732ms | 7.146ms | 10.722ms | 5 |
| DataBroker | CreateMaterializedView | create_materialized_view | createMaterializedView | mutation | OK | 7.908ms | 7.924ms | 8.033ms | 6.832ms | 10.15ms | 5 |
| DataBroker | Delete | delete | delete | mutation | OK | 51.68ms | 54.859ms | 58.762ms | 48.059ms | 90.062ms | 5 |
| DataBroker | DeletePolicy | delete_policy | deletePolicy | mutation | OK | 25.708ms | 25.708ms | 25.708ms | 25.708ms | 25.708ms | 5 |
| DataBroker | DismissDlqEvent | dismiss_dlq_event | dismissDlqEvent | mutation | OK | 19.625ms | 20.636ms | 19.491ms | 17.119ms | 22.822ms | 5 |
| DataBroker | DocumentDelete | document_delete | documentDelete | mutation | OK | 9.156ms | 11.274ms | 13.958ms | 7.242ms | 34.596ms | 5 |
| DataBroker | DocumentFind | document_find | documentFind | read_only | OK | 9.363ms | 12.293ms | 9.235ms | 5.982ms | 13.314ms | 25 |
| DataBroker | DocumentGet | document_get | documentGet | read_only | OK | 9.416ms | 11.534ms | 9.255ms | 6.526ms | 11.534ms | 25 |
| DataBroker | DocumentUpsert | document_upsert | documentUpsert | mutation | OK | 14.618ms | 15.165ms | 13.187ms | 8.349ms | 16.753ms | 5 |
| DataBroker | DropResource | drop_resource | dropResource | destructive | OK | 60.58ms | 60.58ms | 60.58ms | 60.58ms | 60.58ms | 1 |
| DataBroker | EnqueueOutboxEvent | enqueue_outbox_event | enqueueOutboxEvent | mutation | OK | 19.9ms | 19.9ms | 19.9ms | 19.9ms | 19.9ms | 5 |
| DataBroker | EnsureBaseline | ensure_baseline | ensureBaseline | mutation | OK | 24.405ms | 25.512ms | 24.149ms | 21.656ms | 25.689ms | 5 |
| DataBroker | EnsureProject | ensure_project | ensureProject | mutation | OK | 17.665ms | 20.983ms | 18.623ms | 14.523ms | 22.656ms | 5 |
| DataBroker | EnsureResource | ensure_resource | ensureResource | mutation | OK | 21.934ms | 22.355ms | 21.595ms | 19.602ms | 23.517ms | 5 |
| DataBroker | GeneratePresignedUrl | generate_presigned_url | generatePresignedUrl | mutation | OK | 5.721ms | 6.29ms | 6.669ms | 4.662ms | 11.572ms | 5 |
| DataBroker | GenericDispatch | generic_dispatch | genericDispatch | mutation | OK | 10.854ms | 24.879ms | 16.347ms | 6.711ms | 29.374ms | 5 |
| DataBroker | GetAdminSummary | get_admin_summary | getAdminSummary | read_only | OK | 43.768ms | 66.583ms | 47.222ms | 27.661ms | 127.294ms | 25 |
| DataBroker | GetCapabilities | get_capabilities | getCapabilities | read_only | OK | 8.931ms | 17.482ms | 9.737ms | 6.442ms | 17.794ms | 25 |
| DataBroker | GetCatalogManifest | get_catalog_manifest | getCatalogManifest | read_only | OK | 16.101ms | 20.33ms | 16.72ms | 13.464ms | 21.234ms | 25 |
| DataBroker | GetCatalogVersion | get_catalog_version | getCatalogVersion | read_only | OK | 5.56ms | 7.129ms | 5.693ms | 4.387ms | 8.209ms | 25 |
| DataBroker | GetCatalogVersions | get_catalog_versions | getCatalogVersions | read_only | OK | 5.934ms | 7.604ms | 5.877ms | 4.007ms | 8.518ms | 25 |
| DataBroker | GetCdcStatus | get_cdc_status | getCdcStatus | read_only | OK | 4.996ms | 6.309ms | 5.077ms | 3.525ms | 7.8ms | 25 |
| DataBroker | GetDlqEvent | get_dlq_event | getDlqEvent | read_only | OK | 7.13ms | 11.369ms | 7.588ms | 4.508ms | 13.845ms | 25 |
| DataBroker | GetHealthReport | get_health_report | getHealthReport | read_only | OK | 3.956ms | 7.623ms | 4.801ms | 2.818ms | 8.733ms | 25 |
| DataBroker | GetMigrationStatus | get_migration_status | getMigrationStatus | read_only | OK | 6.876ms | 13.082ms | 7.56ms | 4.846ms | 14.163ms | 25 |
| DataBroker | GetObject | get_object | getObject | read_only | OK | 9.937ms | 16.642ms | 10.821ms | 8.442ms | 16.807ms | 25 |
| DataBroker | GetSaga | get_saga | getSaga | read_only | OK | 6.493ms | 8.657ms | 6.834ms | 4.759ms | 13.012ms | 25 |
| DataBroker | GraphMutate | graph_mutate | graphMutate | mutation | OK | 39.576ms | 41.234ms | 71.205ms | 22.371ms | 225.374ms | 5 |
| DataBroker | GraphQuery | graph_query | graphQuery | read_only | OK | 15.686ms | 19.374ms | 16.489ms | 11.986ms | 29.125ms | 25 |
| DataBroker | InitiateMultipartUpload | initiate_multipart_upload | initiateMultipartUpload | mutation | OK | 23.311ms | 24.134ms | 26.085ms | 16.039ms | 49.194ms | 5 |
| DataBroker | LintPolicies | lint_policies | lintPolicies | read_only | OK | 7.036ms | 9.843ms | 7.267ms | 4.894ms | 11.837ms | 25 |
| DataBroker | ListAdminAuditLogs | list_admin_audit_logs | listAdminAuditLogs | read_only | OK | 8.065ms | 11.93ms | 8.422ms | 5.808ms | 13.74ms | 25 |
| DataBroker | ListDlqEvents | list_dlq_events | listDlqEvents | read_only | OK | 8.049ms | 11.134ms | 7.784ms | 4.8ms | 14.303ms | 25 |
| DataBroker | ListMessageSchemas | list_message_schemas | listMessageSchemas | read_only | OK | 3.068ms | 6.452ms | 3.315ms | 2.076ms | 7.046ms | 25 |
| DataBroker | ListMigrationRuns | list_migration_runs | listMigrationRuns | read_only | OK | 6.471ms | 11.81ms | 6.927ms | 4.373ms | 13.314ms | 25 |
| DataBroker | ListPolicies | list_policies | listPolicies | read_only | OK | 6.394ms | 11.194ms | 6.822ms | 4.508ms | 11.964ms | 25 |
| DataBroker | ListProjects | list_projects | listProjects | read_only | OK | 7.134ms | 10.108ms | 7.185ms | 5.027ms | 11.019ms | 25 |
| DataBroker | ListResources | list_resources | listResources | read_only | OK | 5.445ms | 8.473ms | 5.943ms | 4.03ms | 8.781ms | 25 |
| DataBroker | ListSagas | list_sagas | listSagas | read_only | OK | 6.156ms | 9.385ms | 6.569ms | 4.79ms | 11.763ms | 25 |
| DataBroker | LookupMessageSchema | lookup_message_schema | lookupMessageSchema | read_only | OK | 2.762ms | 5.059ms | 3.138ms | 1.965ms | 5.811ms | 25 |
| DataBroker | MarkSagaReviewed | mark_saga_reviewed | markSagaReviewed | mutation | OK | 25.008ms | 26.011ms | 24.707ms | 19.242ms | 32.185ms | 5 |
| DataBroker | PauseCdc | pause_cdc | pauseCdc | mutation | OK | 19.835ms | 21.373ms | 20.203ms | 16.358ms | 23.88ms | 5 |
| DataBroker | PlanMigration | plan_migration | planMigration | mutation | OK | 23.404ms | 23.525ms | 21.852ms | 18.771ms | 24.449ms | 5 |
| DataBroker | PreviewCdcRedaction | preview_cdc_redaction | previewCdcRedaction | read_only | OK | 15.874ms | 32.782ms | 17.795ms | 9.526ms | 35.626ms | 25 |
| DataBroker | PublishCDC | publish_cdc | publishCdc | mutation | OK | 247.583ms | 247.583ms | 267.341ms | 243.963ms | 310.479ms | 3 |
| DataBroker | PutObject | put_object | putObject | mutation | OK | 27.389ms | 28.153ms | 26.944ms | 22.888ms | 29.018ms | 5 |
| DataBroker | PutPolicy | put_policy | putPolicy | destructive | OK | 30.161ms | 30.161ms | 30.161ms | 30.161ms | 30.161ms | 1 |
| DataBroker | QuarantineDlqEvent | quarantine_dlq_event | quarantineDlqEvent | mutation | OK | 19.054ms | 19.367ms | 17.549ms | 14.046ms | 19.516ms | 5 |
| DataBroker | ReloadPolicies | reload_policies | reloadPolicies | destructive | OK | 21.773ms | 21.773ms | 21.773ms | 21.773ms | 21.773ms | 1 |
| DataBroker | ReplayDlqEvent | replay_dlq_event | replayDlqEvent | mutation | OK | 32.883ms | 32.883ms | 32.883ms | 32.883ms | 32.883ms | 5 |
| DataBroker | ResumeCdc | resume_cdc | resumeCdc | mutation | OK | 16.266ms | 17.536ms | 16.172ms | 14.098ms | 17.908ms | 5 |
| DataBroker | RetrySagaCompensation | retry_saga_compensation | retrySagaCompensation | mutation | OK | 18.565ms | 18.565ms | 18.565ms | 18.565ms | 18.565ms | 5 |
| DataBroker | RollbackCatalog | rollback_catalog | rollbackCatalog | destructive | OK | 9.608ms | 9.608ms | 9.608ms | 9.608ms | 9.608ms | 1 |
| DataBroker | ScanProjectionDrift | scan_projection_drift | scanProjectionDrift | read_only | OK | 16.236ms | 20.004ms | 16.281ms | 13.02ms | 20.838ms | 25 |
| DataBroker | Select | select | select | read_only | OK | 7.364ms | 11.435ms | 7.911ms | 5.606ms | 12.182ms | 25 |
| DataBroker | SelectV2 | select_v_2 | selectV2 | read_only | OK | 8.772ms | 14.258ms | 9.317ms | 6.364ms | 17.368ms | 25 |
| DataBroker | StageCatalog | stage_catalog | stageCatalog | destructive | OK | 935.046ms | 935.046ms | 935.046ms | 935.046ms | 935.046ms | 1 |
| DataBroker | StepDownCdcLeader | step_down_cdc_leader | stepDownCdcLeader | mutation | OK | 18.813ms | 19.511ms | 18.565ms | 15.672ms | 21.168ms | 5 |
| DataBroker | TimeSeriesQuery | time_series_query | timeSeriesQuery | read_only | OK | 12.169ms | 16.13ms | 12.802ms | 9.612ms | 18.051ms | 25 |
| DataBroker | TimeSeriesWrite | time_series_write | timeSeriesWrite | mutation | OK | 14.719ms | 16.258ms | 15.906ms | 12.291ms | 23.548ms | 5 |
| DataBroker | Upsert | upsert | upsert | mutation | OK | 68.06ms | 68.072ms | 67.768ms | 66.081ms | 69.217ms | 5 |
| DataBroker | ValidateCatalog | validate_catalog | validateCatalog | destructive | OK | 111.402ms | 111.402ms | 111.402ms | 111.402ms | 111.402ms | 1 |
| DataBroker | VectorBatchUpsert | vector_batch_upsert | vectorBatchUpsert | mutation | OK | 8.343ms | 9.378ms | 23.859ms | 7.896ms | 85.65ms | 5 |
| DataBroker | VectorHybridSearch | vector_hybrid_search | vectorHybridSearch | read_only | OK | 7.681ms | 9.883ms | 8.093ms | 5.528ms | 11.288ms | 25 |
| DataBroker | VectorSearch | vector_search | vectorSearch | read_only | OK | 7.435ms | 13.063ms | 8.248ms | 5.455ms | 13.305ms | 25 |
| DataBroker | VectorUpsert | vector_upsert | vectorUpsert | mutation | OK | 12.911ms | 14.023ms | 13.815ms | 12.575ms | 16.789ms | 5 |
| DataBroker | VerifyAdminAuditLog | verify_admin_audit_log | verifyAdminAuditLog | read_only | OK | 13.68ms | 19.02ms | 13.751ms | 9.074ms | 20.195ms | 25 |
| EmbeddingService | Backfill | backfill | backfillEmbeddingSource | mutation | OK | 21.311ms | 24.1ms | 21.865ms | 19.287ms | 24.318ms | 5 |
| EmbeddingService | DeleteSource | delete_source | deleteEmbeddingSource | destructive | OK | 29.419ms | 29.419ms | 29.419ms | 29.419ms | 29.419ms | 1 |
| EmbeddingService | ListSources | list_sources | listEmbeddingSources | read_only | OK | 28.136ms | 137.497ms | 37.917ms | 16.101ms | 169.532ms | 25 |
| EmbeddingService | RegisterSource | register_source | registerEmbeddingSource | mutation | OK | 31.421ms | 31.773ms | 30.744ms | 27.249ms | 34.587ms | 5 |
| EmbeddingService | ReportEmbedding | report_embedding | reportEmbedding | mutation | OK | 27.498ms | 29.286ms | 39.362ms | 22.661ms | 90.979ms | 5 |
| EmbeddingService | Retrieve | retrieve | retrieveEmbedding | read_only | OK | 19.322ms | 26.018ms | 19.989ms | 13.099ms | 30.552ms | 25 |
| IdentityProviderService | CreateProvider | create_provider | createProvider | mutation | OK | 25.109ms | 25.109ms | 25.109ms | 25.109ms | 25.109ms | 5 |
| IdentityProviderService | DisableProvider | disable_provider | disableProvider | mutation | OK | 24.961ms | 36.834ms | 28.524ms | 20.675ms | 36.981ms | 5 |
| IdentityProviderService | ForceJwksRefresh | force_jwks_refresh | forceJwksRefresh | mutation | OK | 35.44ms | 39.556ms | 37.793ms | 27.602ms | 51.046ms | 5 |
| IdentityProviderService | GetProvider | get_provider | getProvider | read_only | OK | 6.263ms | 11.365ms | 6.886ms | 4.026ms | 17.822ms | 25 |
| IdentityProviderService | ImportSamlMetadata | import_saml_metadata | importSamlMetadata | mutation | OK | 31.312ms | 38.444ms | 32.659ms | 23.181ms | 40.09ms | 5 |
| IdentityProviderService | LinkIdentity | link_identity | linkIdentity | mutation | OK | 38.901ms | 39.918ms | 39.522ms | 21.618ms | 68.368ms | 5 |
| IdentityProviderService | ListExternalIdentities | list_external_identities | listExternalIdentities | read_only | OK | 10.254ms | 14.37ms | 11.161ms | 6.787ms | 22.369ms | 25 |
| IdentityProviderService | ListProviders | list_providers | listProviders | read_only | OK | 11.235ms | 15.987ms | 11.686ms | 7.425ms | 20.931ms | 25 |
| IdentityProviderService | PreviewClaimMapping | preview_claim_mapping | previewClaimMapping | read_only | OK | 7.672ms | 12.141ms | 8.198ms | 4.204ms | 20.37ms | 25 |
| IdentityProviderService | PreviewGroupMapping | preview_group_mapping | previewGroupMapping | read_only | OK | 7.216ms | 15.027ms | 8.089ms | 5.252ms | 17.519ms | 25 |
| IdentityProviderService | ResolveExternalIdentity | resolve_external_identity | resolveExternalIdentity | mutation | OK | 12.343ms | 13.151ms | 17.342ms | 8.261ms | 41.023ms | 5 |
| IdentityProviderService | SamlAcs | saml_acs | samlAcs | mutation | OK | 143.886ms | 171.126ms | 156.725ms | 118.821ms | 225.155ms | 5 |
| IdentityProviderService | ScimCreateGroup | scim_create_group | scimCreateGroup | mutation | OK | 6.131ms | 7.374ms | 6.557ms | 5.138ms | 8.584ms | 5 |
| IdentityProviderService | ScimCreateUser | scim_create_user | scimCreateUser | mutation | OK | 38.843ms | 39.308ms | 39.914ms | 34.936ms | 48.472ms | 5 |
| IdentityProviderService | ScimDeleteGroup | scim_delete_group | scimDeleteGroup | mutation | OK | 4.629ms | 6.21ms | 5.642ms | 4.517ms | 8.301ms | 5 |
| IdentityProviderService | ScimDeleteUser | scim_delete_user | scimDeleteUser | mutation | OK | 55.247ms | 55.247ms | 55.247ms | 55.247ms | 55.247ms | 5 |
| IdentityProviderService | ScimGetGroup | scim_get_group | scimGetGroup | mutation | OK | 7.922ms | 10.385ms | 8.62ms | 6.941ms | 10.887ms | 5 |
| IdentityProviderService | ScimGetUser | scim_get_user | scimGetUser | mutation | OK | 7.132ms | 8.122ms | 8.354ms | 6.977ms | 12.518ms | 5 |
| IdentityProviderService | ScimListGroups | scim_list_groups | scimListGroups | mutation | OK | 7.073ms | 8.79ms | 7.365ms | 4.84ms | 10.25ms | 5 |
| IdentityProviderService | ScimListUsers | scim_list_users | scimListUsers | mutation | OK | 16.546ms | 17.356ms | 15.299ms | 8.345ms | 19.47ms | 5 |
| IdentityProviderService | ScimPatchGroup | scim_patch_group | scimPatchGroup | mutation | OK | 21.656ms | 25.75ms | 25.408ms | 14.889ms | 43.354ms | 5 |
| IdentityProviderService | ScimPatchUser | scim_patch_user | scimPatchUser | mutation | OK | 30.289ms | 32.05ms | 29.149ms | 24.297ms | 32.667ms | 5 |
| IdentityProviderService | ScimReplaceUser | scim_replace_user | scimReplaceUser | mutation | OK | 22.901ms | 23.61ms | 22.75ms | 20.408ms | 24.757ms | 5 |
| IdentityProviderService | StartSamlLogin | start_saml_login | startSamlLogin | mutation | OK | 6.469ms | 8.615ms | 9.877ms | 5.9ms | 22.038ms | 5 |
| IdentityProviderService | TestProviderDiscovery | test_provider_discovery | testProviderDiscovery | read_only | OK | 9.515ms | 14.014ms | 9.971ms | 5.759ms | 14.339ms | 25 |
| IdentityProviderService | UnlinkIdentity | unlink_identity | unlinkIdentity | mutation | OK | 6.531ms | 7.993ms | 8.109ms | 5.439ms | 15.1ms | 5 |
| IdentityProviderService | UpdateProvider | update_provider | updateProvider | mutation | OK | 25.047ms | 25.111ms | 23.807ms | 20.149ms | 25.605ms | 5 |
| LiveQueryService | Subscribe | subscribe | liveQuerySubscribe | read_only | OK | 20.713ms | 28.698ms | 20.317ms | 13.097ms | 28.699ms | 25 |
| LockService | AcquireLock | acquire_lock | acquireLock | mutation | OK | 49.187ms | 53.641ms | 51.324ms | 45.352ms | 59.619ms | 5 |
| LockService | ReleaseLock | release_lock | releaseLock | mutation | OK | 25.404ms | 25.823ms | 23.694ms | 13.998ms | 36.341ms | 5 |
| LockService | RenewLock | renew_lock | renewLock | mutation | OK | 50.067ms | 59.01ms | 53.468ms | 40.284ms | 71.078ms | 5 |
| MeteringService | CheckQuota | check_quota | checkQuota | read_only | OK | 15.891ms | 24.592ms | 17.055ms | 10.569ms | 26.974ms | 25 |
| MeteringService | GetQuota | get_quota | getQuota | read_only | OK | 13.993ms | 19.424ms | 14.918ms | 10.821ms | 25.562ms | 25 |
| MeteringService | ListQuotas | list_quotas | listQuotas | read_only | OK | 14.999ms | 22.959ms | 15.404ms | 10.697ms | 25.306ms | 25 |
| MeteringService | PutQuota | put_quota | putQuota | mutation | OK | 29.721ms | 35.656ms | 31.951ms | 28.967ms | 35.981ms | 5 |
| MeteringService | QueryUsage | query_usage | queryUsage | read_only | OK | 14.413ms | 32.405ms | 16.728ms | 11.927ms | 33.846ms | 25 |
| MeteringService | RecordUsage | record_usage | recordUsage | mutation | OK | 14.596ms | 14.959ms | 15.215ms | 12.983ms | 19.968ms | 5 |
| NotificationService | GetDeliveryStats | get_delivery_stats | getDeliveryStats | read_only | OK | 18.929ms | 25.598ms | 17.024ms | 9.752ms | 27.233ms | 25 |
| NotificationService | GetNotification | get_notification | getNotification | read_only | OK | 15.08ms | 26.963ms | 16.375ms | 12.304ms | 29.355ms | 25 |
| NotificationService | GetPreference | get_preference | getPreference | read_only | OK | 14.046ms | 19.118ms | 14.656ms | 11.851ms | 21.712ms | 25 |
| NotificationService | GetTemplate | get_template | getTemplate | read_only | OK | 16.568ms | 20.349ms | 16.268ms | 11.364ms | 21.448ms | 25 |
| NotificationService | ListNotifications | list_notifications | listNotifications | read_only | OK | 22.505ms | 37.732ms | 24.86ms | 17.98ms | 45.741ms | 25 |
| NotificationService | ListPreferences | list_preferences | listPreferences | read_only | OK | 26.053ms | 32.174ms | 26.88ms | 20.463ms | 48.318ms | 25 |
| NotificationService | ListTemplates | list_templates | listTemplates | read_only | OK | 24.47ms | 31.25ms | 24.659ms | 18.879ms | 31.281ms | 25 |
| NotificationService | ReportDelivery | report_delivery | reportDelivery | mutation | OK | 20.717ms | 26.19ms | 21.91ms | 16.532ms | 29.059ms | 5 |
| NotificationService | RetryNotification | retry_notification | retryNotification | mutation | OK | 20.683ms | 20.683ms | 20.683ms | 20.683ms | 20.683ms | 5 |
| NotificationService | SendNotification | send_notification | sendNotification | mutation | OK | 43.283ms | 45.567ms | 43.656ms | 40.435ms | 46.944ms | 5 |
| NotificationService | SetPreference | set_preference | setPreference | mutation | OK | 14.924ms | 15.212ms | 14.767ms | 13.038ms | 16.167ms | 5 |
| NotificationService | UpsertTemplate | upsert_template | upsertTemplate | mutation | OK | 8.648ms | 8.967ms | 8.646ms | 7.678ms | 9.424ms | 5 |
| PeerService | GetPeer | get_peer | getPeer | read_only | OK | 14.159ms | 27.117ms | 16.088ms | 10.894ms | 27.738ms | 25 |
| PeerService | JoinRoom | join_room | joinRoom | mutation | OK | 32.663ms | 39.28ms | 37.076ms | 28.482ms | 54.089ms | 5 |
| PeerService | JoinSession | join_session | joinSession | mutation | OK | 32.678ms | 33.235ms | 32.386ms | 28.848ms | 34.987ms | 5 |
| PeerService | LeaveRoom | leave_room | leaveRoom | mutation | OK | 11.976ms | 14.17ms | 16.027ms | 9.958ms | 33.659ms | 5 |
| PeerService | ListPeers | list_peers | listPeers | read_only | OK | 15.274ms | 25.444ms | 17.375ms | 12.564ms | 40.801ms | 25 |
| RoomService | CloseRoom | close_room | closeRoom | mutation | OK | 36.271ms | 36.622ms | 33.474ms | 27.096ms | 37.683ms | 5 |
| RoomService | CreateRoom | create_room | createRoom | mutation | OK | 23.996ms | 25.17ms | 24.271ms | 19.239ms | 29.893ms | 5 |
| RoomService | GetRoom | get_room | getRoom | read_only | OK | 14.727ms | 17.915ms | 14.983ms | 11.537ms | 20.779ms | 25 |
| RoomService | ListEgress | list_egress | listEgress | read_only | CAPABILITY_SKIPPED | 6.591ms | 9.15ms | 6.951ms | 4.788ms | 10.456ms | 25 |
| RoomService | ListRooms | list_rooms | listRooms | read_only | OK | 16.394ms | 23.078ms | 16.976ms | 13.234ms | 23.62ms | 25 |
| RoomService | StartRoomComposite | start_room_composite | startRoomComposite | mutation | CAPABILITY_SKIPPED | 9.543ms | 12.266ms | 9.836ms | 6.826ms | 13.222ms | 5 |
| RoomService | StartTrackEgress | start_track_egress | startTrackEgress | mutation | CAPABILITY_SKIPPED | 8.568ms | 8.968ms | 9.486ms | 6.802ms | 15.652ms | 5 |
| RoomService | StopEgress | stop_egress | stopEgress | mutation | CAPABILITY_SKIPPED | 9.496ms | 9.847ms | 8.882ms | 6.076ms | 10.021ms | 5 |
| RoomService | UpdateRoom | update_room | updateRoom | mutation | OK | 11.838ms | 14.126ms | 12.438ms | 10.362ms | 15.157ms | 5 |
| SchedulerService | CreateJob | create_job | createJob | mutation | OK | 19.963ms | 21.625ms | 20.092ms | 16.758ms | 23.643ms | 5 |
| SchedulerService | DeleteJob | delete_job | deleteJob | destructive | OK | 19.591ms | 19.591ms | 19.591ms | 19.591ms | 19.591ms | 1 |
| SchedulerService | GetJob | get_job | getJob | read_only | OK | 14.804ms | 20.303ms | 15.37ms | 9.731ms | 26.074ms | 25 |
| SchedulerService | ListJobs | list_jobs | listJobs | read_only | OK | 15.875ms | 23.461ms | 16.781ms | 11.39ms | 27.671ms | 25 |
| SchedulerService | PauseJob | pause_job | pauseJob | mutation | OK | 21.361ms | 21.361ms | 21.361ms | 21.361ms | 21.361ms | 5 |
| SchedulerService | ResumeJob | resume_job | resumeJob | mutation | OK | 15.573ms | 15.573ms | 15.573ms | 15.573ms | 15.573ms | 5 |
| SearchService | CreateIndex | create_index | createSearchIndex | mutation | OK | 30.544ms | 43.164ms | 35.076ms | 26.093ms | 46.27ms | 5 |
| SearchService | DeleteIndex | delete_index | deleteSearchIndex | destructive | OK | 37.911ms | 37.911ms | 37.911ms | 37.911ms | 37.911ms | 1 |
| SearchService | ListIndexes | list_indexes | listSearchIndexes | read_only | OK | 20.393ms | 30.181ms | 20.875ms | 13.39ms | 32.913ms | 25 |
| SearchService | Reindex | reindex | reindexSearchIndex | mutation | OK | 33.036ms | 34.964ms | 32.433ms | 28.249ms | 35.673ms | 5 |
| SearchService | Search | search | search | read_only | OK | 20.199ms | 24.717ms | 19.947ms | 13.722ms | 27.314ms | 25 |
| SignalingService | Signal | signal | signal | mutation | OK | 23.78ms | 23.78ms | 23.78ms | 23.78ms | 23.78ms | 5 |
| StorageService | DeleteFile | delete_file | deleteFile | mutation | OK | 49.216ms | 49.216ms | 49.216ms | 49.216ms | 49.216ms | 5 |
| StorageService | DownloadFile | download_file | downloadFile | read_only | OK | 28.145ms | 35.128ms | 28.306ms | 23.314ms | 35.221ms | 25 |
| StorageService | FinalizeUpload | finalize_upload | finalizeUpload | mutation | OK | 55.288ms | 55.288ms | 55.288ms | 55.288ms | 55.288ms | 5 |
| StorageService | GetDownloadUrl | get_download_url | getDownloadUrl | read_only | OK | 17.411ms | 21.377ms | 17.589ms | 14.283ms | 21.931ms | 25 |
| StorageService | GetFile | get_file | getFile | read_only | OK | 15.051ms | 20.128ms | 15.351ms | 11.582ms | 21.251ms | 25 |
| StorageService | ListFiles | list_files | listFiles | read_only | OK | 28.331ms | 62.172ms | 32.503ms | 20.37ms | 67.948ms | 25 |
| StorageService | RegisterUpload | register_upload | registerUpload | mutation | OK | 27.718ms | 28.304ms | 27.946ms | 25.47ms | 31.271ms | 5 |
| StorageService | UpdateFile | update_file | updateFile | mutation | OK | 34.443ms | 35.89ms | 34.51ms | 32.604ms | 36.331ms | 5 |
| TenantService | CreateTenant | create_tenant | createTenant | mutation | OK | 11.826ms | 15.3ms | 13.752ms | 9.723ms | 21.845ms | 5 |
| TenantService | GetTenant | get_tenant | getTenant | read_only | OK | 18.354ms | 33.223ms | 19.273ms | 11.519ms | 35.851ms | 25 |
| TenantService | GetTenantConfig | get_tenant_config | getTenantConfig | read_only | OK | 14.462ms | 29.235ms | 15.79ms | 12.056ms | 29.503ms | 25 |
| TenantService | ListTenants | list_tenants | listTenants | read_only | OK | 8.723ms | 12.406ms | 9.196ms | 6.517ms | 13.448ms | 25 |
| TenantService | PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 386.206ms | 386.206ms | 386.206ms | 386.206ms | 386.206ms | 1 |
| TenantService | UpdateTenant | update_tenant | updateTenant | mutation | OK | 18.105ms | 18.181ms | 18.437ms | 14.389ms | 23.771ms | 5 |
| TenantService | UpdateTenantConfig | update_tenant_config | updateTenantConfig | mutation | OK | 33.252ms | 34.968ms | 33.721ms | 28.351ms | 39.965ms | 5 |
| TrackService | ListTracks | list_tracks | listTracks | read_only | OK | 15.503ms | 22.896ms | 16.344ms | 11.875ms | 25.194ms | 25 |
| TrackService | MuteTrack | mute_track | muteTrack | mutation | OK | 13.969ms | 14.94ms | 13.861ms | 10.935ms | 15.97ms | 5 |
| TrackService | PublishTrack | publish_track | publishTrack | mutation | OK | 25.484ms | 26.089ms | 24.141ms | 18.514ms | 32.016ms | 5 |
| TrackService | UnpublishTrack | unpublish_track | unpublishTrack | mutation | OK | 13.169ms | 16.667ms | 14.087ms | 11.361ms | 17.753ms | 5 |
| TurnService | IssueCredentials | issue_credentials | issueCredentials | mutation | OK | 12.186ms | 13.181ms | 12.506ms | 9.828ms | 15.651ms | 5 |
| VaultService | CreateTransitKey | create_transit_key | createTransitKey | mutation | OK | 33.117ms | 33.117ms | 33.117ms | 33.117ms | 33.117ms | 5 |
| VaultService | Decrypt | decrypt | vaultDecrypt | read_only | OK | 18.325ms | 22.559ms | 18.66ms | 14.224ms | 24.893ms | 25 |
| VaultService | DeleteSecret | delete_secret | deleteSecret | mutation | OK | 15.843ms | 18.63ms | 20.032ms | 12.618ms | 38.262ms | 5 |
| VaultService | DestroySecret | destroy_secret | destroySecret | destructive | OK | 31.894ms | 31.894ms | 31.894ms | 31.894ms | 31.894ms | 1 |
| VaultService | Encrypt | encrypt | vaultEncrypt | mutation | OK | 14.383ms | 17.08ms | 15.6ms | 13.423ms | 19.239ms | 5 |
| VaultService | GenerateDatabaseCredentials | generate_database_credentials | generateDatabaseCredentials | mutation | OK | 43.964ms | 51.684ms | 51.625ms | 38.023ms | 80.824ms | 5 |
| VaultService | GetSecret | get_secret | getSecret | read_only | OK | 20.475ms | 41.548ms | 22.243ms | 15.661ms | 41.972ms | 25 |
| VaultService | Hmac | hmac | vaultHmac | mutation | OK | 21.425ms | 26.453ms | 22.172ms | 14.247ms | 31.219ms | 5 |
| VaultService | ListSecrets | list_secrets | listSecrets | read_only | OK | 15.688ms | 22.422ms | 16.116ms | 10.756ms | 23.695ms | 25 |
| VaultService | PutSecret | put_secret | putSecret | mutation | OK | 36.707ms | 36.707ms | 36.707ms | 36.707ms | 36.707ms | 5 |
| VaultService | RotateTransitKey | rotate_transit_key | rotateTransitKey | mutation | OK | 46.674ms | 47.284ms | 46.873ms | 35.944ms | 59.106ms | 5 |
| VaultService | SealStatus | seal_status | vaultSealStatus | read_only | OK | 2.532ms | 3.89ms | 2.615ms | 1.607ms | 5.163ms | 25 |
| VaultService | Sign | sign | vaultSign | mutation | OK | 16.222ms | 17.037ms | 15.7ms | 11.733ms | 18.253ms | 5 |
| VaultService | Verify | verify | vaultVerify | read_only | OK | 13.467ms | 19.021ms | 14.144ms | 11.169ms | 22.067ms | 25 |
| WebhookService | CreateEndpoint | create_endpoint | createWebhookEndpoint | mutation | OK | 17.082ms | 18.78ms | 18.153ms | 14.813ms | 23.899ms | 5 |
| WebhookService | DeleteEndpoint | delete_endpoint | deleteWebhookEndpoint | destructive | OK | 25.334ms | 25.334ms | 25.334ms | 25.334ms | 25.334ms | 1 |
| WebhookService | GetEndpoint | get_endpoint | getWebhookEndpoint | read_only | OK | 10.368ms | 15.191ms | 10.413ms | 6.203ms | 15.313ms | 25 |
| WebhookService | ListDeliveries | list_deliveries | listWebhookDeliveries | read_only | OK | 13.891ms | 20.727ms | 14.349ms | 10.471ms | 24.428ms | 25 |
| WebhookService | ListEndpoints | list_endpoints | listWebhookEndpoints | read_only | OK | 14.091ms | 19.759ms | 14.954ms | 10.48ms | 26.092ms | 25 |
| WebhookService | UpdateEndpoint | update_endpoint | updateWebhookEndpoint | mutation | OK | 19.118ms | 19.215ms | 20.128ms | 16.041ms | 28.543ms | 5 |
| WorkflowService | CancelWorkflow | cancel_workflow | cancelWorkflow | destructive | OK | 30.458ms | 30.458ms | 30.458ms | 30.458ms | 30.458ms | 1 |
| WorkflowService | GetWorkflow | get_workflow | getWorkflow | read_only | OK | 11.819ms | 28.059ms | 14.479ms | 7.712ms | 49.642ms | 25 |
| WorkflowService | ListWorkflows | list_workflows | listWorkflows | read_only | OK | 15.399ms | 20.567ms | 15.586ms | 10.38ms | 21.199ms | 25 |
| WorkflowService | SignalWorkflow | signal_workflow | signalWorkflow | mutation | OK | 31.215ms | 35.535ms | 30.309ms | 21.462ms | 36.443ms | 5 |
| WorkflowService | StartWorkflow | start_workflow | startWorkflow | mutation | OK | 26.1ms | 26.734ms | 26.15ms | 25.009ms | 26.988ms | 5 |
