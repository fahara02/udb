# UDB SDK Live Perf — Go (localhost)

RPCs measured: 344   tenant=2517f3f2-01e2-47c5-82c1-12f2775f4196

Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities (a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request's reference/ID fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection latency. The TARGET is zero failures; any residual non-OK RPC is listed under Failures for the maintainer to finish.

Unary RPCs = full request→response round-trip. Non-CDC streaming RPCs report time-to-FIRST-RESPONSE with seeded inputs. CDC subscription (PublishCDC) reports time-to-FIRST-EVENT: the harness subscribes, fires a real Upsert that flows outbox→CDC→Kafka, and times the first delivered event. Streaming rows are marked in the note column.

## Seeded fixtures

Captured semantic field → seeded value keys used to resolve request fields: action, admin_reset_mfa_user_id, admin_reset_password_user_id, apply_run_id, approval_token, approve_draft_id, approve_run_id, approved_by, asset_id, assigned_by, auth_challenge_id, auth_token, backup_id, bucket, canary_id, canary_version_id, cancel_workflow_id, catalog_manifest, catalog_manifest_b64, challenge_id, change_password_user_id, change_status_user_id, close_room_id, code, collection, content_type, created_by, csrf_token, definition_id, delete_endpoint_id, delete_file_id, delete_policy_id, delete_role_id, delete_scim_user_id, deleted_by, device_id, disable_mfa_user_id, disable_provider_id, dismiss_dlq_id, dlq_id, document_id, domain, ds_policy_id, egress_id, endpoint_id, event_type, external_identity_id, file_id, file_type, filename, finalize_file_id, gov_exp, instance_id, job_id, join_session_room_id, key_id, kind, leave_peer_id, locale, log_id, mark_saga_id, message_type, migration_id, mongo_collection, name, node_id, notification_id, object, object_key, otp_code, otp_id, owner_id, peer_id, plain_key, policy_draft_id, policy_id, policy_version_id, project, project_id, provider_id, purge_tenant_id, quarantine_dlq_id, recipient_id, record_id, recovery_code, refresh_session_id, refresh_token, reg_challenge_id, reject_draft_id, rejected_by, relation, release_fencing_token, renew_fencing_token, replay_dlq_id, reset_otp_code, reset_otp_id, resource, resource_name, restore_tenant_id, retry_saga_id, revoke_device_id, revoke_device_user_id, revoke_key_id, revoke_recovery_user_id, revoked_by, role, role_code, role_id, rollback_policy_set_id, rollback_resource_version, rollback_target_version_id, room_id, saga_id, saml_provider_id, scim_group_id, scim_user_id, session_id, signal_peer_id, stage_name, step_id, subject, tenant, tenant_code, tenant_id, token, topic_pattern, track_id, ts_table, unpublish_track_id, update_draft_id, update_key_id, updated_by, user_id, user_role_id, username, vault_ciphertext, vault_create_key_name, vault_db_role, vault_delete_secret_path, vault_destroy_secret_path, vault_key_name, vault_put_secret_path, vault_secret_path, vault_signature, workflow_id

## Per-service mean latency (mean of per-RPC means)

| Service | RPCs | mean |
|---|---:|---:|
| AuthnService | 50 | 73.537ms |
| BackupService | 8 | 406.576ms |
| DataBroker | 77 | 31.412ms |
| AuthzService | 41 | 28.861ms |
| IdentityProviderService | 27 | 39.134ms |
| TenantService | 7 | 64.277ms |
| ControlPlaneService | 6 | 59.13ms |
| NotificationService | 12 | 28.235ms |
| VaultService | 14 | 21.962ms |
| CacheService | 7 | 40.57ms |
| StorageService | 8 | 32.939ms |
| AssetService | 8 | 25.954ms |
| ApiKeyService | 9 | 22.226ms |
| SearchService | 5 | 37.817ms |
| SchedulerService | 6 | 22.91ms |
| LockService | 3 | 44.403ms |
| EmbeddingService | 6 | 22.189ms |
| MeteringService | 6 | 19.924ms |
| RoomService | 9 | 12.689ms |
| PeerService | 5 | 22.503ms |
| WebhookService | 6 | 17.633ms |
| ConfigService | 5 | 19.607ms |
| WorkflowService | 5 | 19.576ms |
| AnalyticsService | 7 | 10.837ms |
| TrackService | 4 | 15.485ms |
| LiveQueryService | 1 | 17.996ms |
| SignalingService | 1 | 13.99ms |
| TurnService | 1 | 10.512ms |

## Failures — still to fix (0)

No RPC returned a non-OK gRPC status — every RPC ran its success path.

## Slowest 25 RPCs by p99

| RPC | api_alias | operation_id | kind | err | p50 | p99 | mean | iters | note |
|---|---|---|---|---|---:|---:|---:|---:|---|
| BackupService/StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 1.514431s | 1.629935s | 1.552014s | 5 | mutation (seeded success path) |
| BackupService/RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 1.571977s | 1.571977s | 1.571977s | 1 | destructive: 1 real call against a seeded disposable target |
| AuthnService/ChangePassword | change_password | changePassword | mutation | OK | 1.082326s | 1.082326s | 1.082326s | 5 | mutation (seeded success path) |
| AuthnService/ResetPassword | reset_password | resetPassword | mutation | OK | 633.944ms | 633.944ms | 633.944ms | 5 | mutation (seeded success path) |
| DataBroker/StageCatalog | stage_catalog | stageCatalog | destructive | OK | 540.334ms | 540.334ms | 540.334ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthnService/Login | login | login | mutation | OK | 480.283ms | 535.531ms | 493.756ms | 5 | mutation (seeded success path) |
| AuthnService/CreateUser | create_user | createUser | mutation | OK | 516.539ms | 516.539ms | 516.539ms | 5 | mutation (seeded success path) |
| TenantService/PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 347.081ms | 347.081ms | 347.081ms | 1 | destructive: 1 real call against a seeded disposable target |
| DataBroker/PublishCDC | publish_cdc | publishCdc | mutation | OK | 247.16ms | 247.16ms | 202.939ms | 3 | cdc subscription: time-to-first-event (real mutation produced) |
| DataBroker/ApplyMigration | apply_migration | applyMigration | mutation | OK | 218.01ms | 218.01ms | 218.01ms | 5 | mutation (seeded success path) |
| CacheService/GetNamespaceStats | get_cache_namespace_stats | getCacheNamespaceStats | read_only | OK | 99.984ms | 149.496ms | 109.078ms | 25 | read_only (seeded success path) |
| DataBroker/Delete | delete | delete | mutation | OK | 135.606ms | 146.542ms | 139.02ms | 5 | mutation (seeded success path) |
| DataBroker/BatchUpsert | batch_upsert | batchUpsert | mutation | OK | 130.862ms | 136.727ms | 131.55ms | 5 | streaming: time-to-first-response (seeded; bidi) |
| DataBroker/Upsert | upsert | upsert | mutation | OK | 108.754ms | 125.969ms | 113.002ms | 5 | mutation (seeded success path) |
| ControlPlaneService/DeltaResources | delta_resources | deltaResources | mutation | OK | 116.402ms | 121.267ms | 113.979ms | 5 | streaming: time-to-first-response (seeded; bidi) |
| ControlPlaneService/StreamResources | stream_resources | streamResources | mutation | OK | 101.15ms | 102.61ms | 98.308ms | 5 | streaming: time-to-first-response (seeded; bidi) |
| NotificationService/GetPreference | get_preference | getPreference | read_only | OK | 35.324ms | 101.212ms | 45.576ms | 25 | read_only (seeded success path) |
| CacheService/DeleteNamespace | delete_cache_namespace | deleteCacheNamespace | destructive | OK | 99.074ms | 99.074ms | 99.074ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthzService/PromoteCanary | promote_canary | promoteCanary | destructive | OK | 97.247ms | 97.247ms | 97.247ms | 1 | destructive: 1 real call against a seeded disposable target |
| IdentityProviderService/ScimDeleteUser | scim_delete_user | scimDeleteUser | mutation | OK | 94.588ms | 94.588ms | 94.588ms | 5 | mutation (seeded success path) |
| SearchService/ListIndexes | list_indexes | listSearchIndexes | read_only | OK | 56.604ms | 92.375ms | 57.155ms | 25 | read_only (seeded success path) |
| ControlPlaneService/RollbackResources | rollback_resources | rollbackResources | mutation | OK | 81.157ms | 82.557ms | 78.88ms | 5 | mutation (seeded success path) |
| SchedulerService/ListJobs | list_jobs | listJobs | read_only | OK | 26.359ms | 82.118ms | 36.154ms | 25 | read_only (seeded success path) |
| AuthzService/RollbackPolicyVersion | rollback_policy_version | rollbackPolicyVersion | destructive | OK | 82.07ms | 82.07ms | 82.07ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthnService/FinishWebAuthnAuthentication | finish_web_authn_authentication | finishWebAuthnAuthentication | mutation | OK | 81.673ms | 81.673ms | 81.673ms | 5 | mutation (seeded success path) |

## Full per-RPC table (sorted by service, then name)

| Service | RPC | api_alias | operation_id | kind | err | p50 | p99 | mean | min | max | iters |
|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|
| AnalyticsService | GetExecutorPerformance | get_executor_performance | getExecutorPerformance | read_only | OK | 12.351ms | 21.217ms | 13.389ms | 4.366ms | 45.516ms | 25 |
| AnalyticsService | GetPipelineSummary | get_pipeline_summary | getPipelineSummary | read_only | OK | 13.561ms | 18.466ms | 12.775ms | 5.059ms | 24.422ms | 25 |
| AnalyticsService | GetReconciliationAnalytics | get_reconciliation_analytics | getReconciliationAnalytics | read_only | OK | 12.076ms | 15.437ms | 11.297ms | 5.641ms | 16.235ms | 25 |
| AnalyticsService | GetSlaCompliance | get_sla_compliance | getSlaCompliance | read_only | OK | 9.308ms | 14.413ms | 9.933ms | 4.452ms | 18.003ms | 25 |
| AnalyticsService | GetThroughput | get_throughput | getThroughput | read_only | OK | 8.579ms | 11.902ms | 9.074ms | 6.25ms | 17.628ms | 25 |
| AnalyticsService | RecordPipelineMetric | record_pipeline_metric | recordPipelineMetric | mutation | OK | 10.283ms | 12.175ms | 11.21ms | 9.007ms | 14.67ms | 5 |
| AnalyticsService | TriggerSnapshot | trigger_snapshot | triggerSnapshot | mutation | OK | 8.122ms | 8.505ms | 8.179ms | 6.821ms | 9.655ms | 5 |
| ApiKeyService | CreateApiKey | create_api_key | createApiKey | mutation | OK | 15.901ms | 17.225ms | 16.338ms | 14.905ms | 18.215ms | 5 |
| ApiKeyService | EmergencyRevokeApiKeys | emergency_revoke_api_keys | emergencyRevokeApiKeys | destructive | OK | 68.068ms | 68.068ms | 68.068ms | 68.068ms | 68.068ms | 1 |
| ApiKeyService | GetApiKey | get_api_key | getApiKey | read_only | OK | 10.703ms | 13.529ms | 10.291ms | 5.037ms | 16.105ms | 25 |
| ApiKeyService | GetApiKeyUsageStats | get_api_key_usage_stats | getApiKeyUsageStats | read_only | OK | 16.207ms | 22.433ms | 14.699ms | 6.626ms | 22.477ms | 25 |
| ApiKeyService | ListApiKeys | list_api_keys | listApiKeys | read_only | OK | 9.036ms | 11.745ms | 9.012ms | 5.622ms | 11.941ms | 25 |
| ApiKeyService | RevokeApiKey | revoke_api_key | revokeApiKey | mutation | OK | 18.581ms | 18.581ms | 18.581ms | 18.581ms | 18.581ms | 5 |
| ApiKeyService | RotateApiKey | rotate_api_key | rotateApiKey | mutation | OK | 23.397ms | 23.397ms | 23.397ms | 23.397ms | 23.397ms | 5 |
| ApiKeyService | UpdateApiKey | update_api_key | updateApiKey | mutation | OK | 21.87ms | 22.279ms | 23.07ms | 18.533ms | 32.182ms | 5 |
| ApiKeyService | ValidateApiKey | validate_api_key | validateApiKey | read_only | OK | 15.712ms | 25.114ms | 16.58ms | 12.204ms | 29.634ms | 25 |
| AssetService | CompleteStep | complete_step | completeStep | mutation | OK | 49.189ms | 63.167ms | 53.751ms | 44.861ms | 65.633ms | 5 |
| AssetService | CreatePipelineDefinition | create_pipeline_definition | createPipelineDefinition | mutation | OK | 26.23ms | 26.23ms | 26.23ms | 26.23ms | 26.23ms | 5 |
| AssetService | GetAsset | get_asset | getAsset | read_only | OK | 22.784ms | 47.896ms | 24.56ms | 15.945ms | 51.121ms | 25 |
| AssetService | GetPipeline | get_pipeline | getPipeline | read_only | OK | 21.127ms | 25.557ms | 21.371ms | 15.992ms | 28.328ms | 25 |
| AssetService | GetPipelineDefinition | get_pipeline_definition | getPipelineDefinition | read_only | OK | 18.344ms | 24.339ms | 18.57ms | 12.992ms | 25.143ms | 25 |
| AssetService | ListAssets | list_assets | listAssets | read_only | OK | 18.265ms | 22.369ms | 18.134ms | 13.887ms | 24.33ms | 25 |
| AssetService | RegisterAsset | register_asset | registerAsset | mutation | OK | 26.484ms | 28.63ms | 26.239ms | 21.932ms | 30.028ms | 5 |
| AssetService | StartPipeline | start_pipeline | startPipeline | mutation | OK | 10.903ms | 15.35ms | 18.776ms | 7.07ms | 53.445ms | 5 |
| AuthnService | AdminResetMfa | admin_reset_mfa | adminResetMfa | destructive | OK | 45.546ms | 45.546ms | 45.546ms | 45.546ms | 45.546ms | 1 |
| AuthnService | AdminResetPassword | admin_reset_password | adminResetPassword | destructive | OK | 17.048ms | 17.048ms | 17.048ms | 17.048ms | 17.048ms | 1 |
| AuthnService | AdminRevokeAllTenantSessions | admin_revoke_all_tenant_sessions | adminRevokeAllTenantSessions | destructive | OK | 25.942ms | 25.942ms | 25.942ms | 25.942ms | 25.942ms | 1 |
| AuthnService | AdminRevokeAllUserSessions | admin_revoke_all_user_sessions | adminRevokeAllUserSessions | destructive | OK | 19.602ms | 19.602ms | 19.602ms | 19.602ms | 19.602ms | 1 |
| AuthnService | AdminRevokeSession | admin_revoke_session | adminRevokeSession | destructive | OK | 19.959ms | 19.959ms | 19.959ms | 19.959ms | 19.959ms | 1 |
| AuthnService | Authenticate | authenticate | authenticate | read_only | OK | 32.933ms | 62.708ms | 37.432ms | 25.781ms | 68.851ms | 25 |
| AuthnService | ChangePassword | change_password | changePassword | mutation | OK | 1.082326s | 1.082326s | 1.082326s | 1.082326s | 1.082326s | 5 |
| AuthnService | ChangeUserStatus | change_user_status | changeUserStatus | destructive | OK | 19.564ms | 19.564ms | 19.564ms | 19.564ms | 19.564ms | 1 |
| AuthnService | ConfirmMFAEnrollment | confirm_mfaenrollment | confirmMfaenrollment | mutation | OK | 5.044ms | 5.049ms | 5.26ms | 4.781ms | 6.547ms | 5 |
| AuthnService | CreateSession | create_session | createSession | mutation | OK | 9.794ms | 10.446ms | 9.177ms | 6.177ms | 10.519ms | 5 |
| AuthnService | CreateUser | create_user | createUser | mutation | OK | 516.539ms | 516.539ms | 516.539ms | 516.539ms | 516.539ms | 5 |
| AuthnService | DeleteWebAuthnCredential | delete_web_authn_credential | deleteWebAuthnCredential | mutation | OK | 10.904ms | 11.084ms | 10.893ms | 10.358ms | 11.263ms | 5 |
| AuthnService | DisableMfaFactor | disable_mfa_factor | disableMfaFactor | mutation | OK | 18.948ms | 24.155ms | 20.872ms | 17.676ms | 24.87ms | 5 |
| AuthnService | EmergencyRevoke | emergency_revoke | emergencyRevoke | destructive | OK | 22.279ms | 22.279ms | 22.279ms | 22.279ms | 22.279ms | 1 |
| AuthnService | EnrollMFA | enroll_mfa | enrollMfa | mutation | OK | 16.807ms | 19.824ms | 18.036ms | 13.563ms | 23.578ms | 5 |
| AuthnService | FinishWebAuthnAuthentication | finish_web_authn_authentication | finishWebAuthnAuthentication | mutation | OK | 81.673ms | 81.673ms | 81.673ms | 81.673ms | 81.673ms | 5 |
| AuthnService | FinishWebAuthnRegistration | finish_web_authn_registration | finishWebAuthnRegistration | mutation | OK | 58.94ms | 58.94ms | 58.94ms | 58.94ms | 58.94ms | 5 |
| AuthnService | ForgotPassword | forgot_password | forgotPassword | mutation | OK | 21.572ms | 22.829ms | 22.045ms | 20.109ms | 24.279ms | 5 |
| AuthnService | GenerateRecoveryCodes | generate_recovery_codes | generateRecoveryCodes | mutation | OK | 41.307ms | 43.815ms | 45.225ms | 40.084ms | 59.802ms | 5 |
| AuthnService | GetJwks | get_jwks | getJwks | read_only | OK | 6.172ms | 8.1ms | 5.996ms | 3.288ms | 8.223ms | 25 |
| AuthnService | GetMfaPolicy | get_mfa_policy | getMfaPolicy | read_only | OK | 5.294ms | 8.1ms | 5.55ms | 3.269ms | 8.253ms | 25 |
| AuthnService | GetSession | get_session | getSession | read_only | OK | 5.713ms | 6.955ms | 5.752ms | 4.361ms | 7.512ms | 25 |
| AuthnService | GetUser | get_user | getUser | read_only | OK | 6.226ms | 7.936ms | 6.168ms | 4.034ms | 13.131ms | 25 |
| AuthnService | IntrospectToken | introspect_token | introspectToken | read_only | OK | 44.966ms | 60.064ms | 46.835ms | 37.687ms | 60.124ms | 25 |
| AuthnService | IssueMfaChallenge | issue_mfa_challenge | issueMfaChallenge | mutation | OK | 15.718ms | 16.921ms | 15.432ms | 12.983ms | 17.623ms | 5 |
| AuthnService | ListDevices | list_devices | listDevices | read_only | OK | 8.357ms | 12.605ms | 8.346ms | 4.389ms | 15.901ms | 25 |
| AuthnService | ListMfaFactors | list_mfa_factors | listMfaFactors | read_only | OK | 9.668ms | 12.004ms | 9.231ms | 6.19ms | 12.95ms | 25 |
| AuthnService | ListSessions | list_sessions | listSessions | read_only | OK | 11.114ms | 14.5ms | 10.874ms | 6.578ms | 14.816ms | 25 |
| AuthnService | ListUsers | list_users | listUsers | read_only | OK | 12.986ms | 18.084ms | 13.132ms | 8.254ms | 18.5ms | 25 |
| AuthnService | ListWebAuthnCredentials | list_web_authn_credentials | listWebAuthnCredentials | read_only | OK | 7.419ms | 9.969ms | 7.483ms | 5.137ms | 11.24ms | 25 |
| AuthnService | Login | login | login | mutation | OK | 480.283ms | 535.531ms | 493.756ms | 453.484ms | 535.918ms | 5 |
| AuthnService | Logout | logout | logout | mutation | OK | 8.877ms | 10.027ms | 9.381ms | 8.192ms | 11.497ms | 5 |
| AuthnService | PutMfaPolicy | put_mfa_policy | putMfaPolicy | mutation | OK | 9.123ms | 10.08ms | 11.992ms | 8.848ms | 22.811ms | 5 |
| AuthnService | RefreshSession | refresh_session | refreshSession | mutation | OK | 33ms | 33.41ms | 35.625ms | 22.687ms | 56.308ms | 5 |
| AuthnService | RefreshToken | refresh_token | refreshToken | mutation | OK | 14.309ms | 14.309ms | 14.309ms | 14.309ms | 14.309ms | 5 |
| AuthnService | RenamePasskey | rename_passkey | renamePasskey | mutation | OK | 10.834ms | 11.895ms | 11.157ms | 8.546ms | 13.702ms | 5 |
| AuthnService | ResendOTP | resend_otp | resendOtp | mutation | OK | 19.607ms | 20.997ms | 19.329ms | 16.777ms | 22.39ms | 5 |
| AuthnService | ResetPassword | reset_password | resetPassword | mutation | OK | 633.944ms | 633.944ms | 633.944ms | 633.944ms | 633.944ms | 5 |
| AuthnService | RevokeDevice | revoke_device | revokeDevice | mutation | OK | 28.044ms | 28.044ms | 28.044ms | 28.044ms | 28.044ms | 5 |
| AuthnService | RevokeRecoveryCodes | revoke_recovery_codes | revokeRecoveryCodes | mutation | OK | 22.007ms | 23.012ms | 20.625ms | 16.589ms | 23.405ms | 5 |
| AuthnService | RevokeSession | revoke_session | revokeSession | mutation | OK | 13.981ms | 14.801ms | 14.149ms | 13.232ms | 14.896ms | 5 |
| AuthnService | SendOTP | send_otp | sendOtp | mutation | OK | 17.577ms | 18ms | 19.939ms | 15.576ms | 31.002ms | 5 |
| AuthnService | SendPhoneVerification | send_phone_verification | sendPhoneVerification | mutation | OK | 17.522ms | 18.74ms | 17.824ms | 16.382ms | 19.739ms | 5 |
| AuthnService | StartWebAuthnAuthentication | start_web_authn_authentication | startWebAuthnAuthentication | mutation | OK | 27.247ms | 28.998ms | 25.381ms | 15.614ms | 30.362ms | 5 |
| AuthnService | StartWebAuthnRegistration | start_web_authn_registration | startWebAuthnRegistration | mutation | OK | 15.999ms | 16.622ms | 16.694ms | 14.192ms | 21.235ms | 5 |
| AuthnService | UpdateUser | update_user | updateUser | mutation | OK | 12.675ms | 12.751ms | 12.021ms | 10.405ms | 13.111ms | 5 |
| AuthnService | ValidateCSRF | validate_csrf | validateCsrf | read_only | OK | 5.913ms | 7.175ms | 5.892ms | 4.293ms | 8.156ms | 25 |
| AuthnService | ValidateToken | validate_token | validateToken | read_only | OK | 31.927ms | 38.327ms | 31.186ms | 24.488ms | 43.05ms | 25 |
| AuthnService | VerifyMfaChallenge | verify_mfa_challenge | verifyMfaChallenge | read_only | OK | 11.12ms | 13.592ms | 10.954ms | 6.602ms | 13.756ms | 25 |
| AuthnService | VerifyOTP | verify_otp | verifyOtp | read_only | OK | 29.899ms | 40.923ms | 31.474ms | 24.291ms | 42.516ms | 25 |
| AuthzService | ActivateCanary | activate_canary | activateCanary | destructive | OK | 54.421ms | 54.421ms | 54.421ms | 54.421ms | 54.421ms | 1 |
| AuthzService | ActivatePolicyVersion | activate_policy_version | activatePolicyVersion | destructive | OK | 75.088ms | 75.088ms | 75.088ms | 75.088ms | 75.088ms | 1 |
| AuthzService | ApprovePolicyDraft | approve_policy_draft | approvePolicyDraft | mutation | OK | 64.087ms | 64.087ms | 64.087ms | 64.087ms | 64.087ms | 5 |
| AuthzService | AssignRole | assign_role | assignRole | mutation | OK | 32.117ms | 33.204ms | 33.694ms | 28.515ms | 43.88ms | 5 |
| AuthzService | Authorize | authorize | authorize | read_only | OK | 26.783ms | 39.354ms | 29.105ms | 21.186ms | 39.996ms | 25 |
| AuthzService | BatchCheckPermissions | batch_check_permissions | batchCheckPermissions | read_only | OK | 12.611ms | 17.404ms | 13.088ms | 9.989ms | 17.983ms | 25 |
| AuthzService | CheckAccess | check_access | checkAccess | read_only | OK | 11.68ms | 17.598ms | 13.216ms | 9.88ms | 25.922ms | 25 |
| AuthzService | CreatePolicyDraft | create_policy_draft | createPolicyDraft | mutation | OK | 54.832ms | 63.618ms | 56.343ms | 44.566ms | 68.28ms | 5 |
| AuthzService | CreatePolicyRule | create_policy_rule | createPolicyRule | mutation | OK | 31.409ms | 31.73ms | 31.032ms | 28.218ms | 34.508ms | 5 |
| AuthzService | CreateRole | create_role | createRole | mutation | OK | 30.665ms | 30.665ms | 30.665ms | 30.665ms | 30.665ms | 5 |
| AuthzService | DeletePolicyRule | delete_policy_rule | deletePolicyRule | mutation | OK | 12.075ms | 13.008ms | 12.177ms | 10.474ms | 13.838ms | 5 |
| AuthzService | DeleteRole | delete_role | deleteRole | mutation | OK | 14.24ms | 14.739ms | 17.35ms | 11.307ms | 33.31ms | 5 |
| AuthzService | DiffPolicyDraft | diff_policy_draft | diffPolicyDraft | read_only | OK | 19.201ms | 23.328ms | 19.631ms | 15.886ms | 24.483ms | 25 |
| AuthzService | ExplainPolicy | explain_policy | explainPolicy | read_only | OK | 10.865ms | 23.263ms | 11.85ms | 8.191ms | 24.277ms | 25 |
| AuthzService | GetAuthzRevision | get_authz_revision | getAuthzRevision | read_only | OK | 6.833ms | 9.679ms | 7.103ms | 3.778ms | 10.093ms | 25 |
| AuthzService | GetCanaryStatus | get_canary_status | getCanaryStatus | read_only | OK | 16.119ms | 27.258ms | 17.45ms | 12.028ms | 31.166ms | 25 |
| AuthzService | GetNativeAccess | get_native_access | getNativeAccess | read_only | OK | 23.983ms | 39.25ms | 26.037ms | 18.926ms | 48.405ms | 25 |
| AuthzService | GetPolicyBundle | get_policy_bundle | getPolicyBundle | read_only | OK | 9.727ms | 11.607ms | 10.097ms | 8.048ms | 20.317ms | 25 |
| AuthzService | GetPolicyRule | get_policy_rule | getPolicyRule | read_only | OK | 6.164ms | 9.613ms | 6.658ms | 4.634ms | 10.227ms | 25 |
| AuthzService | GetRole | get_role | getRole | read_only | OK | 6.31ms | 8.048ms | 6.268ms | 3.734ms | 8.724ms | 25 |
| AuthzService | InvalidatePolicyBundles | invalidate_policy_bundles | invalidatePolicyBundles | destructive | OK | 33.889ms | 33.889ms | 33.889ms | 33.889ms | 33.889ms | 1 |
| AuthzService | LintAuthzPolicies | lint_authz_policies | lintAuthzPolicies | read_only | OK | 2.711ms | 3.838ms | 2.65ms | 769µs | 4.087ms | 25 |
| AuthzService | ListAccessDecisionAudits | list_access_decision_audits | listAccessDecisionAudits | read_only | OK | 17.039ms | 34.638ms | 20.413ms | 11.741ms | 36.173ms | 25 |
| AuthzService | ListPolicyRules | list_policy_rules | listPolicyRules | read_only | OK | 7.077ms | 9.259ms | 7.392ms | 5.395ms | 12.878ms | 25 |
| AuthzService | ListPolicyVersions | list_policy_versions | listPolicyVersions | read_only | OK | 15.878ms | 24.428ms | 16.617ms | 12.596ms | 27.076ms | 25 |
| AuthzService | ListRoles | list_roles | listRoles | read_only | OK | 8.795ms | 12.391ms | 9.08ms | 5.415ms | 12.488ms | 25 |
| AuthzService | ListUserPermissions | list_user_permissions | listUserPermissions | read_only | OK | 2.81ms | 5.22ms | 3.369ms | 1.894ms | 5.423ms | 25 |
| AuthzService | ListUserRoles | list_user_roles | listUserRoles | read_only | OK | 8.256ms | 11.263ms | 8.226ms | 5.083ms | 12.578ms | 25 |
| AuthzService | MigrateLegacyPolicies | migrate_legacy_policies | migrateLegacyPolicies | destructive | OK | 48.111ms | 48.111ms | 48.111ms | 48.111ms | 48.111ms | 1 |
| AuthzService | PromoteCanary | promote_canary | promoteCanary | destructive | OK | 97.247ms | 97.247ms | 97.247ms | 97.247ms | 97.247ms | 1 |
| AuthzService | PutAuthzPolicy | put_authz_policy | putAuthzPolicy | mutation | OK | 22.094ms | 22.508ms | 22ms | 20.197ms | 23.49ms | 5 |
| AuthzService | PutRelationship | put_relationship | putRelationship | mutation | OK | 25.814ms | 27.324ms | 28.696ms | 24.675ms | 40.143ms | 5 |
| AuthzService | PutRoleBinding | put_role_binding | putRoleBinding | mutation | OK | 22.648ms | 22.807ms | 22.429ms | 20.312ms | 24.15ms | 5 |
| AuthzService | RejectPolicyDraft | reject_policy_draft | rejectPolicyDraft | mutation | OK | 36.098ms | 36.098ms | 36.098ms | 36.098ms | 36.098ms | 5 |
| AuthzService | RevokeRole | revoke_role | revokeRole | mutation | OK | 10.713ms | 11.161ms | 15.626ms | 9.917ms | 35.947ms | 5 |
| AuthzService | RollbackPolicyVersion | rollback_policy_version | rollbackPolicyVersion | destructive | OK | 82.07ms | 82.07ms | 82.07ms | 82.07ms | 82.07ms | 1 |
| AuthzService | SeedBuiltinRoles | seed_builtin_roles | seedBuiltinRoles | mutation | OK | 72.786ms | 78.509ms | 69.592ms | 53.559ms | 79.332ms | 5 |
| AuthzService | SimulatePolicy | simulate_policy | simulatePolicy | mutation | OK | 19.681ms | 33.57ms | 26.987ms | 17.459ms | 45.298ms | 5 |
| AuthzService | SubmitPolicyDraft | submit_policy_draft | submitPolicyDraft | mutation | OK | 22.89ms | 22.89ms | 22.89ms | 22.89ms | 22.89ms | 5 |
| AuthzService | UpdatePolicyDraft | update_policy_draft | updatePolicyDraft | mutation | OK | 42.846ms | 47.711ms | 42.735ms | 34.729ms | 53.078ms | 5 |
| AuthzService | UpdateRole | update_role | updateRole | mutation | OK | 31.133ms | 34.54ms | 31.813ms | 22.22ms | 43.592ms | 5 |
| BackupService | DeleteBackupPolicy | delete_backup_policy | deleteBackupPolicy | mutation | OK | 19.493ms | 24.5ms | 22.488ms | 18.048ms | 31.632ms | 5 |
| BackupService | GetBackup | get_backup | getBackup | read_only | OK | 24.446ms | 31.565ms | 24.97ms | 19.268ms | 31.914ms | 25 |
| BackupService | GetBackupPolicy | get_backup_policy | getBackupPolicy | read_only | OK | 14.599ms | 20.482ms | 15.38ms | 12.141ms | 26.749ms | 25 |
| BackupService | ListBackupPolicies | list_backup_policies | listBackupPolicies | read_only | OK | 14.878ms | 18.697ms | 15.11ms | 12.364ms | 19.677ms | 25 |
| BackupService | ListBackups | list_backups | listBackups | read_only | OK | 19.237ms | 25.064ms | 19.578ms | 15.391ms | 30.629ms | 25 |
| BackupService | PutBackupPolicy | put_backup_policy | putBackupPolicy | mutation | OK | 28.66ms | 29.193ms | 31.093ms | 27.171ms | 43.252ms | 5 |
| BackupService | RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 1.571977s | 1.571977s | 1.571977s | 1.571977s | 1.571977s | 1 |
| BackupService | StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 1.514431s | 1.629935s | 1.552014s | 1.293493s | 1.81062s | 5 |
| CacheService | CreateNamespace | create_cache_namespace | createCacheNamespace | mutation | OK | 13.462ms | 14.624ms | 14.298ms | 13.129ms | 16.987ms | 5 |
| CacheService | Delete | cache_delete | cacheNamespaceDelete | mutation | OK | 12.72ms | 14.197ms | 13.341ms | 12.324ms | 14.972ms | 5 |
| CacheService | DeleteNamespace | delete_cache_namespace | deleteCacheNamespace | destructive | OK | 99.074ms | 99.074ms | 99.074ms | 99.074ms | 99.074ms | 1 |
| CacheService | Get | cache_get | cacheNamespaceGet | read_only | OK | 13.351ms | 25.068ms | 16.652ms | 10.065ms | 73.636ms | 25 |
| CacheService | GetNamespaceStats | get_cache_namespace_stats | getCacheNamespaceStats | read_only | OK | 99.984ms | 149.496ms | 109.078ms | 78.034ms | 179.554ms | 25 |
| CacheService | Scan | cache_scan | cacheNamespaceScan | read_only | OK | 13.265ms | 15.939ms | 13.392ms | 10.56ms | 23.415ms | 25 |
| CacheService | Set | cache_set | cacheNamespaceSet | mutation | OK | 17.578ms | 18.832ms | 18.157ms | 17.082ms | 19.997ms | 5 |
| ConfigService | DeleteFlag | delete_flag | deleteFlag | destructive | OK | 24.347ms | 24.347ms | 24.347ms | 24.347ms | 24.347ms | 1 |
| ConfigService | EvaluateFlags | evaluate_flags | evaluateFlags | read_only | OK | 14.97ms | 28.111ms | 16.388ms | 10.194ms | 29.379ms | 25 |
| ConfigService | GetFlag | get_flag | getFlag | read_only | OK | 14.096ms | 19.647ms | 14.646ms | 10.236ms | 25.759ms | 25 |
| ConfigService | ListFlags | list_flags | listFlags | read_only | OK | 14.379ms | 17.277ms | 15.063ms | 11.753ms | 30.059ms | 25 |
| ConfigService | PutFlag | put_flag | putFlag | mutation | OK | 27.418ms | 29.3ms | 27.592ms | 25.756ms | 29.315ms | 5 |
| ControlPlaneService | AckStatus | ack_status | ackStatus | mutation | OK | 12.699ms | 13.897ms | 11.754ms | 8.657ms | 14.297ms | 5 |
| ControlPlaneService | DeltaResources | delta_resources | deltaResources | mutation | OK | 116.402ms | 121.267ms | 113.979ms | 98.839ms | 129.758ms | 5 |
| ControlPlaneService | GetResources | get_resources | getResources | read_only | OK | 6.543ms | 8.31ms | 6.485ms | 4.843ms | 8.407ms | 25 |
| ControlPlaneService | ListNodeStates | list_node_states | listNodeStates | read_only | OK | 43.857ms | 55.002ms | 45.374ms | 37.582ms | 62.07ms | 25 |
| ControlPlaneService | RollbackResources | rollback_resources | rollbackResources | mutation | OK | 81.157ms | 82.557ms | 78.88ms | 71.793ms | 83.32ms | 5 |
| ControlPlaneService | StreamResources | stream_resources | streamResources | mutation | OK | 101.15ms | 102.61ms | 98.308ms | 84.635ms | 103.989ms | 5 |
| DataBroker | ActivateCatalog | activate_catalog | activateCatalog | destructive | OK | 9.592ms | 9.592ms | 9.592ms | 9.592ms | 9.592ms | 1 |
| DataBroker | AnalyticalQuery | analytical_query | analyticalQuery | read_only | OK | 9.354ms | 10.96ms | 9.305ms | 7.329ms | 10.987ms | 25 |
| DataBroker | ApplyMigration | apply_migration | applyMigration | mutation | OK | 218.01ms | 218.01ms | 218.01ms | 218.01ms | 218.01ms | 5 |
| DataBroker | ApproveMigrationPlan | approve_migration_plan | approveMigrationPlan | mutation | OK | 32.257ms | 32.257ms | 32.257ms | 32.257ms | 32.257ms | 1 |
| DataBroker | BatchSelect | batch_select | batchSelect | mutation | OK | 9.348ms | 11.617ms | 12.08ms | 8.118ms | 21.995ms | 5 |
| DataBroker | BatchUpsert | batch_upsert | batchUpsert | mutation | OK | 130.862ms | 136.727ms | 131.55ms | 117.869ms | 143.478ms | 5 |
| DataBroker | BeginTx | begin_tx | beginTx | mutation | OK | 32.784ms | 32.893ms | 31.056ms | 26.489ms | 34.431ms | 5 |
| DataBroker | CacheDelete | cache_delete | cacheDelete | mutation | OK | 9.074ms | 9.806ms | 9.2ms | 8.032ms | 10.669ms | 5 |
| DataBroker | CacheGet | cache_get | cacheGet | read_only | OK | 7.611ms | 10.193ms | 8.015ms | 5.676ms | 18.748ms | 25 |
| DataBroker | CacheScan | cache_scan | cacheScan | read_only | OK | 15.433ms | 21.031ms | 16.13ms | 10.707ms | 34.551ms | 25 |
| DataBroker | CacheSet | cache_set | cacheSet | mutation | OK | 9.033ms | 9.478ms | 9.653ms | 7.322ms | 15.013ms | 5 |
| DataBroker | CreateMaterializedView | create_materialized_view | createMaterializedView | mutation | OK | 9.933ms | 11.174ms | 10.07ms | 8.436ms | 11.993ms | 5 |
| DataBroker | Delete | delete | delete | mutation | OK | 135.606ms | 146.542ms | 139.02ms | 123.511ms | 155.524ms | 5 |
| DataBroker | DeletePolicy | delete_policy | deletePolicy | mutation | OK | 19.163ms | 19.163ms | 19.163ms | 19.163ms | 19.163ms | 5 |
| DataBroker | DismissDlqEvent | dismiss_dlq_event | dismissDlqEvent | mutation | OK | 14.968ms | 16.695ms | 16.488ms | 13.034ms | 24.444ms | 5 |
| DataBroker | DocumentDelete | document_delete | documentDelete | mutation | OK | 7.805ms | 7.844ms | 8.83ms | 7.034ms | 13.85ms | 5 |
| DataBroker | DocumentFind | document_find | documentFind | read_only | OK | 9.101ms | 11.157ms | 9.005ms | 5.995ms | 11.724ms | 25 |
| DataBroker | DocumentGet | document_get | documentGet | read_only | OK | 8.394ms | 9.321ms | 8.208ms | 6.286ms | 10.292ms | 25 |
| DataBroker | DocumentUpsert | document_upsert | documentUpsert | mutation | OK | 8.129ms | 8.554ms | 8.519ms | 6.554ms | 11.43ms | 5 |
| DataBroker | DropResource | drop_resource | dropResource | destructive | OK | 29.583ms | 29.583ms | 29.583ms | 29.583ms | 29.583ms | 1 |
| DataBroker | EnqueueOutboxEvent | enqueue_outbox_event | enqueueOutboxEvent | mutation | OK | 15.392ms | 15.392ms | 15.392ms | 15.392ms | 15.392ms | 5 |
| DataBroker | EnsureBaseline | ensure_baseline | ensureBaseline | mutation | OK | 23.043ms | 23.1ms | 22.982ms | 19.173ms | 30.192ms | 5 |
| DataBroker | EnsureProject | ensure_project | ensureProject | mutation | OK | 21.648ms | 22.912ms | 21.673ms | 18.593ms | 25.963ms | 5 |
| DataBroker | EnsureResource | ensure_resource | ensureResource | mutation | OK | 31.587ms | 37.354ms | 34.146ms | 28.678ms | 43.035ms | 5 |
| DataBroker | GeneratePresignedUrl | generate_presigned_url | generatePresignedUrl | mutation | OK | 8.395ms | 9.667ms | 8.386ms | 5.915ms | 10.404ms | 5 |
| DataBroker | GenericDispatch | generic_dispatch | genericDispatch | mutation | OK | 9.646ms | 9.944ms | 9.76ms | 6.633ms | 14.146ms | 5 |
| DataBroker | GetAdminSummary | get_admin_summary | getAdminSummary | read_only | OK | 42.691ms | 53.117ms | 42.01ms | 26.748ms | 56.295ms | 25 |
| DataBroker | GetCapabilities | get_capabilities | getCapabilities | read_only | OK | 9.515ms | 15.494ms | 10.412ms | 7.272ms | 18.414ms | 25 |
| DataBroker | GetCatalogManifest | get_catalog_manifest | getCatalogManifest | read_only | OK | 16.342ms | 20.156ms | 16.646ms | 13.161ms | 31.424ms | 25 |
| DataBroker | GetCatalogVersion | get_catalog_version | getCatalogVersion | read_only | OK | 7.275ms | 10.649ms | 7.405ms | 4.674ms | 11.693ms | 25 |
| DataBroker | GetCatalogVersions | get_catalog_versions | getCatalogVersions | read_only | OK | 8.56ms | 12.354ms | 8.83ms | 5.148ms | 12.583ms | 25 |
| DataBroker | GetCdcStatus | get_cdc_status | getCdcStatus | read_only | OK | 7.481ms | 9.431ms | 7.373ms | 5.22ms | 9.542ms | 25 |
| DataBroker | GetDlqEvent | get_dlq_event | getDlqEvent | read_only | OK | 8.088ms | 12.761ms | 9.902ms | 6.069ms | 48.331ms | 25 |
| DataBroker | GetHealthReport | get_health_report | getHealthReport | read_only | OK | 4.482ms | 6.001ms | 4.576ms | 2.736ms | 6.782ms | 25 |
| DataBroker | GetMigrationStatus | get_migration_status | getMigrationStatus | read_only | OK | 7.11ms | 8.665ms | 7.198ms | 5.002ms | 8.92ms | 25 |
| DataBroker | GetObject | get_object | getObject | read_only | OK | 10.907ms | 14.664ms | 11.135ms | 8.259ms | 16.733ms | 25 |
| DataBroker | GetSaga | get_saga | getSaga | read_only | OK | 7.641ms | 9.963ms | 7.99ms | 5.647ms | 10.214ms | 25 |
| DataBroker | GraphMutate | graph_mutate | graphMutate | mutation | OK | 31.148ms | 31.981ms | 30.082ms | 25.809ms | 33.891ms | 5 |
| DataBroker | GraphQuery | graph_query | graphQuery | read_only | OK | 20.805ms | 36.518ms | 23.225ms | 16.36ms | 43.265ms | 25 |
| DataBroker | InitiateMultipartUpload | initiate_multipart_upload | initiateMultipartUpload | mutation | OK | 16.77ms | 20.228ms | 17.806ms | 12.977ms | 22.437ms | 5 |
| DataBroker | LintPolicies | lint_policies | lintPolicies | read_only | OK | 8.614ms | 13.137ms | 9.11ms | 5.913ms | 15.478ms | 25 |
| DataBroker | ListAdminAuditLogs | list_admin_audit_logs | listAdminAuditLogs | read_only | OK | 9.412ms | 12.961ms | 9.949ms | 6.352ms | 14.433ms | 25 |
| DataBroker | ListDlqEvents | list_dlq_events | listDlqEvents | read_only | OK | 8.035ms | 10.438ms | 7.868ms | 4.397ms | 10.441ms | 25 |
| DataBroker | ListMessageSchemas | list_message_schemas | listMessageSchemas | read_only | OK | 3.224ms | 5.665ms | 3.504ms | 1.882ms | 6.259ms | 25 |
| DataBroker | ListMigrationRuns | list_migration_runs | listMigrationRuns | read_only | OK | 7.519ms | 10.086ms | 7.626ms | 4.857ms | 10.591ms | 25 |
| DataBroker | ListPolicies | list_policies | listPolicies | read_only | OK | 7.071ms | 9.831ms | 7.237ms | 4.808ms | 10.363ms | 25 |
| DataBroker | ListProjects | list_projects | listProjects | read_only | OK | 6.972ms | 8.407ms | 6.925ms | 4.346ms | 9.475ms | 25 |
| DataBroker | ListResources | list_resources | listResources | read_only | OK | 6.98ms | 7.948ms | 6.65ms | 4.684ms | 8.178ms | 25 |
| DataBroker | ListSagas | list_sagas | listSagas | read_only | OK | 8.414ms | 17.085ms | 10.94ms | 4.799ms | 55.089ms | 25 |
| DataBroker | LookupMessageSchema | lookup_message_schema | lookupMessageSchema | read_only | OK | 3.237ms | 5.165ms | 3.454ms | 1.654ms | 5.304ms | 25 |
| DataBroker | MarkSagaReviewed | mark_saga_reviewed | markSagaReviewed | mutation | OK | 19.853ms | 21.124ms | 20.375ms | 18.642ms | 23.201ms | 5 |
| DataBroker | PauseCdc | pause_cdc | pauseCdc | mutation | OK | 21.69ms | 23.751ms | 21.956ms | 18.121ms | 25.935ms | 5 |
| DataBroker | PlanMigration | plan_migration | planMigration | mutation | OK | 23.442ms | 25.155ms | 22.577ms | 16.12ms | 25.331ms | 5 |
| DataBroker | PreviewCdcRedaction | preview_cdc_redaction | previewCdcRedaction | read_only | OK | 13.355ms | 19.723ms | 14.387ms | 9.392ms | 22.364ms | 25 |
| DataBroker | PublishCDC | publish_cdc | publishCdc | mutation | OK | 247.16ms | 247.16ms | 202.939ms | 101.414ms | 260.244ms | 3 |
| DataBroker | PutObject | put_object | putObject | mutation | OK | 28.292ms | 30.521ms | 25.841ms | 19.821ms | 30.565ms | 5 |
| DataBroker | PutPolicy | put_policy | putPolicy | destructive | OK | 18.477ms | 18.477ms | 18.477ms | 18.477ms | 18.477ms | 1 |
| DataBroker | QuarantineDlqEvent | quarantine_dlq_event | quarantineDlqEvent | mutation | OK | 15.034ms | 17.037ms | 16.074ms | 12.978ms | 21.475ms | 5 |
| DataBroker | ReloadPolicies | reload_policies | reloadPolicies | destructive | OK | 21ms | 21ms | 21ms | 21ms | 21ms | 1 |
| DataBroker | ReplayDlqEvent | replay_dlq_event | replayDlqEvent | mutation | OK | 23.184ms | 23.184ms | 23.184ms | 23.184ms | 23.184ms | 5 |
| DataBroker | ResumeCdc | resume_cdc | resumeCdc | mutation | OK | 15.505ms | 16.906ms | 15.664ms | 14.15ms | 17.177ms | 5 |
| DataBroker | RetrySagaCompensation | retry_saga_compensation | retrySagaCompensation | mutation | OK | 17ms | 17ms | 17ms | 17ms | 17ms | 5 |
| DataBroker | RollbackCatalog | rollback_catalog | rollbackCatalog | destructive | OK | 8.955ms | 8.955ms | 8.955ms | 8.955ms | 8.955ms | 1 |
| DataBroker | ScanProjectionDrift | scan_projection_drift | scanProjectionDrift | read_only | OK | 14.747ms | 21.33ms | 15.53ms | 10.637ms | 22.78ms | 25 |
| DataBroker | Select | select | select | read_only | OK | 8.009ms | 9.756ms | 8.175ms | 6.606ms | 10.21ms | 25 |
| DataBroker | SelectV2 | select_v_2 | selectV2 | read_only | OK | 10.063ms | 14.143ms | 10.186ms | 7.195ms | 14.555ms | 25 |
| DataBroker | StageCatalog | stage_catalog | stageCatalog | destructive | OK | 540.334ms | 540.334ms | 540.334ms | 540.334ms | 540.334ms | 1 |
| DataBroker | StepDownCdcLeader | step_down_cdc_leader | stepDownCdcLeader | mutation | OK | 14.802ms | 15.161ms | 14.396ms | 13.034ms | 15.213ms | 5 |
| DataBroker | TimeSeriesQuery | time_series_query | timeSeriesQuery | read_only | OK | 14.356ms | 16.875ms | 14.331ms | 11.944ms | 18.259ms | 25 |
| DataBroker | TimeSeriesWrite | time_series_write | timeSeriesWrite | mutation | OK | 12.077ms | 13.235ms | 13.337ms | 10.267ms | 19.738ms | 5 |
| DataBroker | Upsert | upsert | upsert | mutation | OK | 108.754ms | 125.969ms | 113.002ms | 99.405ms | 126.521ms | 5 |
| DataBroker | ValidateCatalog | validate_catalog | validateCatalog | destructive | OK | 71.162ms | 71.162ms | 71.162ms | 71.162ms | 71.162ms | 1 |
| DataBroker | VectorBatchUpsert | vector_batch_upsert | vectorBatchUpsert | mutation | OK | 9.069ms | 10.465ms | 9.826ms | 8.163ms | 12.644ms | 5 |
| DataBroker | VectorHybridSearch | vector_hybrid_search | vectorHybridSearch | read_only | OK | 10.46ms | 14.607ms | 11.027ms | 8.439ms | 15.043ms | 25 |
| DataBroker | VectorSearch | vector_search | vectorSearch | read_only | OK | 10.216ms | 12.599ms | 10.308ms | 7.512ms | 12.647ms | 25 |
| DataBroker | VectorUpsert | vector_upsert | vectorUpsert | mutation | OK | 15.167ms | 15.451ms | 15.068ms | 13.658ms | 16.681ms | 5 |
| DataBroker | VerifyAdminAuditLog | verify_admin_audit_log | verifyAdminAuditLog | read_only | OK | 16.482ms | 22.986ms | 17.653ms | 12.026ms | 25.891ms | 25 |
| EmbeddingService | Backfill | backfill | backfillEmbeddingSource | mutation | OK | 18.868ms | 19.978ms | 18.904ms | 16.22ms | 21.346ms | 5 |
| EmbeddingService | DeleteSource | delete_source | deleteEmbeddingSource | destructive | OK | 27.084ms | 27.084ms | 27.084ms | 27.084ms | 27.084ms | 1 |
| EmbeddingService | ListSources | list_sources | listEmbeddingSources | read_only | OK | 14.764ms | 20.234ms | 15.341ms | 11.739ms | 21.095ms | 25 |
| EmbeddingService | RegisterSource | register_source | registerEmbeddingSource | mutation | OK | 28.271ms | 29.062ms | 28.401ms | 26.736ms | 29.67ms | 5 |
| EmbeddingService | ReportEmbedding | report_embedding | reportEmbedding | mutation | OK | 21.565ms | 22.394ms | 24.287ms | 19.991ms | 36.3ms | 5 |
| EmbeddingService | Retrieve | retrieve | retrieveEmbedding | read_only | OK | 16.567ms | 28.95ms | 19.115ms | 12.914ms | 51.225ms | 25 |
| IdentityProviderService | CreateProvider | create_provider | createProvider | mutation | OK | 20.135ms | 20.135ms | 20.135ms | 20.135ms | 20.135ms | 5 |
| IdentityProviderService | DisableProvider | disable_provider | disableProvider | mutation | OK | 19.208ms | 23.251ms | 21.049ms | 18.342ms | 25.501ms | 5 |
| IdentityProviderService | ForceJwksRefresh | force_jwks_refresh | forceJwksRefresh | mutation | OK | 32.759ms | 36.818ms | 494.1ms | 24.834ms | 2.344074s | 5 |
| IdentityProviderService | GetProvider | get_provider | getProvider | read_only | OK | 6.433ms | 9.826ms | 6.439ms | 3.819ms | 10.164ms | 25 |
| IdentityProviderService | ImportSamlMetadata | import_saml_metadata | importSamlMetadata | mutation | OK | 28.414ms | 29.02ms | 28.058ms | 23.331ms | 31.265ms | 5 |
| IdentityProviderService | LinkIdentity | link_identity | linkIdentity | mutation | OK | 35.225ms | 36.191ms | 35.346ms | 31.344ms | 39.695ms | 5 |
| IdentityProviderService | ListExternalIdentities | list_external_identities | listExternalIdentities | read_only | OK | 11.613ms | 15.457ms | 11.814ms | 8.114ms | 15.829ms | 25 |
| IdentityProviderService | ListProviders | list_providers | listProviders | read_only | OK | 12.258ms | 18.05ms | 12.606ms | 7.75ms | 22.337ms | 25 |
| IdentityProviderService | PreviewClaimMapping | preview_claim_mapping | previewClaimMapping | read_only | OK | 5.619ms | 7.819ms | 5.963ms | 3.92ms | 10.296ms | 25 |
| IdentityProviderService | PreviewGroupMapping | preview_group_mapping | previewGroupMapping | read_only | OK | 5.15ms | 6.432ms | 5.176ms | 3.753ms | 6.563ms | 25 |
| IdentityProviderService | ResolveExternalIdentity | resolve_external_identity | resolveExternalIdentity | mutation | OK | 10.355ms | 11.655ms | 18.973ms | 8.729ms | 54.332ms | 5 |
| IdentityProviderService | SamlAcs | saml_acs | samlAcs | mutation | OK | 66.229ms | 68.32ms | 77.893ms | 65.919ms | 122.98ms | 5 |
| IdentityProviderService | ScimCreateGroup | scim_create_group | scimCreateGroup | mutation | OK | 6.42ms | 8.938ms | 6.895ms | 3.971ms | 9.292ms | 5 |
| IdentityProviderService | ScimCreateUser | scim_create_user | scimCreateUser | mutation | OK | 28.713ms | 31.355ms | 29.557ms | 26.894ms | 32.349ms | 5 |
| IdentityProviderService | ScimDeleteGroup | scim_delete_group | scimDeleteGroup | mutation | OK | 5.366ms | 6.379ms | 5.303ms | 2.987ms | 7.225ms | 5 |
| IdentityProviderService | ScimDeleteUser | scim_delete_user | scimDeleteUser | mutation | OK | 94.588ms | 94.588ms | 94.588ms | 94.588ms | 94.588ms | 5 |
| IdentityProviderService | ScimGetGroup | scim_get_group | scimGetGroup | mutation | OK | 9.145ms | 10.609ms | 9.874ms | 7.662ms | 12.88ms | 5 |
| IdentityProviderService | ScimGetUser | scim_get_user | scimGetUser | mutation | OK | 11.55ms | 11.559ms | 11.175ms | 9.074ms | 13.73ms | 5 |
| IdentityProviderService | ScimListGroups | scim_list_groups | scimListGroups | mutation | OK | 8.263ms | 9.509ms | 8.488ms | 5.791ms | 10.764ms | 5 |
| IdentityProviderService | ScimListUsers | scim_list_users | scimListUsers | mutation | OK | 17.367ms | 19.266ms | 17.311ms | 14.132ms | 21.303ms | 5 |
| IdentityProviderService | ScimPatchGroup | scim_patch_group | scimPatchGroup | mutation | OK | 15.559ms | 15.688ms | 14.89ms | 12.335ms | 15.863ms | 5 |
| IdentityProviderService | ScimPatchUser | scim_patch_user | scimPatchUser | mutation | OK | 39.325ms | 41.305ms | 37.047ms | 28.8ms | 41.476ms | 5 |
| IdentityProviderService | ScimReplaceUser | scim_replace_user | scimReplaceUser | mutation | OK | 25.315ms | 28.004ms | 25.276ms | 20.756ms | 28.183ms | 5 |
| IdentityProviderService | StartSamlLogin | start_saml_login | startSamlLogin | mutation | OK | 5.568ms | 5.993ms | 5.681ms | 4.498ms | 7.523ms | 5 |
| IdentityProviderService | TestProviderDiscovery | test_provider_discovery | testProviderDiscovery | read_only | OK | 8.339ms | 9.689ms | 8.344ms | 6.693ms | 10.383ms | 25 |
| IdentityProviderService | UnlinkIdentity | unlink_identity | unlinkIdentity | mutation | OK | 15.839ms | 26.417ms | 19.973ms | 7.022ms | 38.705ms | 5 |
| IdentityProviderService | UpdateProvider | update_provider | updateProvider | mutation | OK | 22.645ms | 26.332ms | 24.664ms | 19.952ms | 32.487ms | 5 |
| LiveQueryService | Subscribe | subscribe | liveQuerySubscribe | read_only | OK | 16.441ms | 26.06ms | 17.996ms | 14.612ms | 26.338ms | 25 |
| LockService | AcquireLock | acquire_lock | acquireLock | mutation | OK | 60.491ms | 70.52ms | 61.875ms | 48.577ms | 72.069ms | 5 |
| LockService | ReleaseLock | release_lock | releaseLock | mutation | OK | 23.99ms | 24.11ms | 26.878ms | 19.572ms | 46.575ms | 5 |
| LockService | RenewLock | renew_lock | renewLock | mutation | OK | 39.726ms | 50.903ms | 44.456ms | 34.772ms | 59.497ms | 5 |
| MeteringService | CheckQuota | check_quota | checkQuota | read_only | OK | 18.535ms | 31.503ms | 19.202ms | 12.797ms | 33.309ms | 25 |
| MeteringService | GetQuota | get_quota | getQuota | read_only | OK | 12.376ms | 17.884ms | 13.374ms | 11.012ms | 18.77ms | 25 |
| MeteringService | ListQuotas | list_quotas | listQuotas | read_only | OK | 13.477ms | 26.625ms | 15.399ms | 11.315ms | 26.774ms | 25 |
| MeteringService | PutQuota | put_quota | putQuota | mutation | OK | 38.982ms | 40.543ms | 38.993ms | 29.588ms | 49.567ms | 5 |
| MeteringService | QueryUsage | query_usage | queryUsage | read_only | OK | 15.806ms | 23.756ms | 17.152ms | 11.184ms | 34.891ms | 25 |
| MeteringService | RecordUsage | record_usage | recordUsage | mutation | OK | 14.539ms | 15.243ms | 15.423ms | 11.381ms | 21.953ms | 5 |
| NotificationService | GetDeliveryStats | get_delivery_stats | getDeliveryStats | read_only | OK | 18.704ms | 23.67ms | 16.499ms | 7.207ms | 29.168ms | 25 |
| NotificationService | GetNotification | get_notification | getNotification | read_only | OK | 21.56ms | 37.113ms | 24.332ms | 16.145ms | 41.252ms | 25 |
| NotificationService | GetPreference | get_preference | getPreference | read_only | OK | 35.324ms | 101.212ms | 45.576ms | 15.66ms | 142.071ms | 25 |
| NotificationService | GetTemplate | get_template | getTemplate | read_only | OK | 34.658ms | 76.163ms | 39.35ms | 18.018ms | 77.801ms | 25 |
| NotificationService | ListNotifications | list_notifications | listNotifications | read_only | OK | 33.655ms | 46.17ms | 35.854ms | 27.472ms | 70.755ms | 25 |
| NotificationService | ListPreferences | list_preferences | listPreferences | read_only | OK | 24.822ms | 31.19ms | 25.289ms | 18.606ms | 36.438ms | 25 |
| NotificationService | ListTemplates | list_templates | listTemplates | read_only | OK | 19.711ms | 25.96ms | 20.555ms | 16.681ms | 28.198ms | 25 |
| NotificationService | ReportDelivery | report_delivery | reportDelivery | mutation | OK | 20.162ms | 22.068ms | 21.127ms | 17.816ms | 25.513ms | 5 |
| NotificationService | RetryNotification | retry_notification | retryNotification | mutation | OK | 22.273ms | 22.273ms | 22.273ms | 22.273ms | 22.273ms | 5 |
| NotificationService | SendNotification | send_notification | sendNotification | mutation | OK | 59.398ms | 62.269ms | 55.115ms | 45.523ms | 62.698ms | 5 |
| NotificationService | SetPreference | set_preference | setPreference | mutation | OK | 20.429ms | 20.651ms | 19.316ms | 16.2ms | 22.753ms | 5 |
| NotificationService | UpsertTemplate | upsert_template | upsertTemplate | mutation | OK | 13.024ms | 14.292ms | 13.529ms | 10.268ms | 18.39ms | 5 |
| PeerService | GetPeer | get_peer | getPeer | read_only | OK | 21.251ms | 31.471ms | 21.618ms | 14.026ms | 36.984ms | 25 |
| PeerService | JoinRoom | join_room | joinRoom | mutation | OK | 29.723ms | 30.268ms | 29.43ms | 23.456ms | 36.253ms | 5 |
| PeerService | JoinSession | join_session | joinSession | mutation | OK | 31.182ms | 32.616ms | 31.54ms | 28.594ms | 35.928ms | 5 |
| PeerService | LeaveRoom | leave_room | leaveRoom | mutation | OK | 9.196ms | 11.885ms | 11.882ms | 8.929ms | 20.419ms | 5 |
| PeerService | ListPeers | list_peers | listPeers | read_only | OK | 18.038ms | 20.954ms | 18.043ms | 14.878ms | 21.77ms | 25 |
| RoomService | CloseRoom | close_room | closeRoom | mutation | OK | 27.547ms | 28.071ms | 27.188ms | 23.848ms | 29.032ms | 5 |
| RoomService | CreateRoom | create_room | createRoom | mutation | OK | 20.184ms | 21.065ms | 20.071ms | 18.273ms | 21.894ms | 5 |
| RoomService | GetRoom | get_room | getRoom | read_only | OK | 15.18ms | 17.781ms | 15.121ms | 10.759ms | 19.122ms | 25 |
| RoomService | ListEgress | list_egress | listEgress | read_only | CAPABILITY_SKIPPED | 6.643ms | 10.775ms | 7.256ms | 4.273ms | 19.17ms | 25 |
| RoomService | ListRooms | list_rooms | listRooms | read_only | OK | 14.038ms | 16.583ms | 14.307ms | 11.788ms | 18.413ms | 25 |
| RoomService | StartRoomComposite | start_room_composite | startRoomComposite | mutation | CAPABILITY_SKIPPED | 6.574ms | 8.324ms | 6.641ms | 4.779ms | 8.734ms | 5 |
| RoomService | StartTrackEgress | start_track_egress | startTrackEgress | mutation | CAPABILITY_SKIPPED | 6.294ms | 6.552ms | 5.97ms | 4.752ms | 7.25ms | 5 |
| RoomService | StopEgress | stop_egress | stopEgress | mutation | CAPABILITY_SKIPPED | 6.875ms | 7.393ms | 6.921ms | 6.039ms | 7.81ms | 5 |
| RoomService | UpdateRoom | update_room | updateRoom | mutation | OK | 10.219ms | 11.935ms | 10.725ms | 8.84ms | 12.725ms | 5 |
| SchedulerService | CreateJob | create_job | createJob | mutation | OK | 27.568ms | 28.858ms | 27.505ms | 16.331ms | 44.291ms | 5 |
| SchedulerService | DeleteJob | delete_job | deleteJob | destructive | OK | 16.927ms | 16.927ms | 16.927ms | 16.927ms | 16.927ms | 1 |
| SchedulerService | GetJob | get_job | getJob | read_only | OK | 11.211ms | 23.106ms | 12.317ms | 8.239ms | 27.697ms | 25 |
| SchedulerService | ListJobs | list_jobs | listJobs | read_only | OK | 26.359ms | 82.118ms | 36.154ms | 11.737ms | 93.251ms | 25 |
| SchedulerService | PauseJob | pause_job | pauseJob | mutation | OK | 22.119ms | 22.119ms | 22.119ms | 22.119ms | 22.119ms | 5 |
| SchedulerService | ResumeJob | resume_job | resumeJob | mutation | OK | 22.436ms | 22.436ms | 22.436ms | 22.436ms | 22.436ms | 5 |
| SearchService | CreateIndex | create_index | createSearchIndex | mutation | OK | 40.246ms | 44.645ms | 42.55ms | 32.368ms | 58.432ms | 5 |
| SearchService | DeleteIndex | delete_index | deleteSearchIndex | destructive | OK | 24.061ms | 24.061ms | 24.061ms | 24.061ms | 24.061ms | 1 |
| SearchService | ListIndexes | list_indexes | listSearchIndexes | read_only | OK | 56.604ms | 92.375ms | 57.155ms | 10.673ms | 243.164ms | 25 |
| SearchService | Reindex | reindex | reindexSearchIndex | mutation | OK | 38.419ms | 43.5ms | 40.173ms | 36.948ms | 44.968ms | 5 |
| SearchService | Search | search | search | read_only | OK | 25.898ms | 38.444ms | 25.146ms | 9.163ms | 41.141ms | 25 |
| SignalingService | Signal | signal | signal | mutation | OK | 13.99ms | 13.99ms | 13.99ms | 13.99ms | 13.99ms | 5 |
| StorageService | DeleteFile | delete_file | deleteFile | mutation | OK | 51.68ms | 51.68ms | 51.68ms | 51.68ms | 51.68ms | 5 |
| StorageService | DownloadFile | download_file | downloadFile | read_only | OK | 29.377ms | 42.626ms | 30.636ms | 21.288ms | 45.2ms | 25 |
| StorageService | FinalizeUpload | finalize_upload | finalizeUpload | mutation | OK | 57.879ms | 57.879ms | 57.879ms | 57.879ms | 57.879ms | 5 |
| StorageService | GetDownloadUrl | get_download_url | getDownloadUrl | read_only | OK | 19.161ms | 26.947ms | 19.255ms | 11.795ms | 31.472ms | 25 |
| StorageService | GetFile | get_file | getFile | read_only | OK | 14.032ms | 19.206ms | 14.594ms | 11.776ms | 21.846ms | 25 |
| StorageService | ListFiles | list_files | listFiles | read_only | OK | 28.27ms | 46.447ms | 29.775ms | 18.627ms | 47.285ms | 25 |
| StorageService | RegisterUpload | register_upload | registerUpload | mutation | OK | 27.776ms | 28.261ms | 28.857ms | 26.06ms | 34.887ms | 5 |
| StorageService | UpdateFile | update_file | updateFile | mutation | OK | 30.575ms | 31.231ms | 30.837ms | 28.042ms | 33.944ms | 5 |
| TenantService | CreateTenant | create_tenant | createTenant | mutation | OK | 13.73ms | 14.433ms | 13.715ms | 12.013ms | 15.98ms | 5 |
| TenantService | GetTenant | get_tenant | getTenant | read_only | OK | 15.136ms | 26.703ms | 16.749ms | 11.532ms | 37.752ms | 25 |
| TenantService | GetTenantConfig | get_tenant_config | getTenantConfig | read_only | OK | 14.097ms | 23.789ms | 15.735ms | 10.477ms | 28.978ms | 25 |
| TenantService | ListTenants | list_tenants | listTenants | read_only | OK | 12.348ms | 16.001ms | 11.811ms | 7.237ms | 16.058ms | 25 |
| TenantService | PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 347.081ms | 347.081ms | 347.081ms | 347.081ms | 347.081ms | 1 |
| TenantService | UpdateTenant | update_tenant | updateTenant | mutation | OK | 13.604ms | 15.422ms | 13.54ms | 11.062ms | 15.666ms | 5 |
| TenantService | UpdateTenantConfig | update_tenant_config | updateTenantConfig | mutation | OK | 30.927ms | 32.174ms | 31.308ms | 25.777ms | 37.718ms | 5 |
| TrackService | ListTracks | list_tracks | listTracks | read_only | OK | 15.442ms | 21.234ms | 15.914ms | 13.051ms | 22.445ms | 25 |
| TrackService | MuteTrack | mute_track | muteTrack | mutation | OK | 11.962ms | 12.053ms | 11.745ms | 10.843ms | 12.313ms | 5 |
| TrackService | PublishTrack | publish_track | publishTrack | mutation | OK | 20.659ms | 21.463ms | 20.79ms | 19.145ms | 22.635ms | 5 |
| TrackService | UnpublishTrack | unpublish_track | unpublishTrack | mutation | OK | 13.018ms | 13.226ms | 13.491ms | 12.492ms | 16.02ms | 5 |
| TurnService | IssueCredentials | issue_credentials | issueCredentials | mutation | OK | 10.663ms | 12.203ms | 10.512ms | 6.617ms | 12.751ms | 5 |
| VaultService | CreateTransitKey | create_transit_key | createTransitKey | mutation | OK | 30.77ms | 30.77ms | 30.77ms | 30.77ms | 30.77ms | 5 |
| VaultService | Decrypt | decrypt | vaultDecrypt | read_only | OK | 21.324ms | 33.981ms | 23.088ms | 17.442ms | 45.291ms | 25 |
| VaultService | DeleteSecret | delete_secret | deleteSecret | mutation | OK | 15.335ms | 17.326ms | 19.514ms | 13.875ms | 35.864ms | 5 |
| VaultService | DestroySecret | destroy_secret | destroySecret | destructive | OK | 24.372ms | 24.372ms | 24.372ms | 24.372ms | 24.372ms | 1 |
| VaultService | Encrypt | encrypt | vaultEncrypt | mutation | OK | 15.337ms | 16.503ms | 15.624ms | 14.479ms | 16.811ms | 5 |
| VaultService | GenerateDatabaseCredentials | generate_database_credentials | generateDatabaseCredentials | mutation | OK | 35.337ms | 36.199ms | 34.656ms | 29.787ms | 39.246ms | 5 |
| VaultService | GetSecret | get_secret | getSecret | read_only | OK | 21.013ms | 34.134ms | 22.563ms | 15.845ms | 37.315ms | 25 |
| VaultService | Hmac | hmac | vaultHmac | mutation | OK | 13.428ms | 14.432ms | 13.585ms | 11.335ms | 17.24ms | 5 |
| VaultService | ListSecrets | list_secrets | listSecrets | read_only | OK | 16.665ms | 24.351ms | 17.457ms | 11.663ms | 31.455ms | 25 |
| VaultService | PutSecret | put_secret | putSecret | mutation | OK | 41.93ms | 41.93ms | 41.93ms | 41.93ms | 41.93ms | 5 |
| VaultService | RotateTransitKey | rotate_transit_key | rotateTransitKey | mutation | OK | 35.746ms | 36.495ms | 35.979ms | 30.655ms | 45.667ms | 5 |
| VaultService | SealStatus | seal_status | vaultSealStatus | read_only | OK | 2.125ms | 3.574ms | 2.351ms | 781µs | 3.692ms | 25 |
| VaultService | Sign | sign | vaultSign | mutation | OK | 12.644ms | 13.109ms | 12.065ms | 9.258ms | 13.174ms | 5 |
| VaultService | Verify | verify | vaultVerify | read_only | OK | 12.986ms | 16.683ms | 13.511ms | 10.696ms | 17.756ms | 25 |
| WebhookService | CreateEndpoint | create_endpoint | createWebhookEndpoint | mutation | OK | 16.731ms | 18.25ms | 16.686ms | 12.489ms | 19.242ms | 5 |
| WebhookService | DeleteEndpoint | delete_endpoint | deleteWebhookEndpoint | destructive | OK | 20.854ms | 20.854ms | 20.854ms | 20.854ms | 20.854ms | 1 |
| WebhookService | GetEndpoint | get_endpoint | getWebhookEndpoint | read_only | OK | 12.704ms | 19.675ms | 13.319ms | 9.708ms | 25.286ms | 25 |
| WebhookService | ListDeliveries | list_deliveries | listWebhookDeliveries | read_only | OK | 16.172ms | 21.528ms | 16.677ms | 11.104ms | 28.597ms | 25 |
| WebhookService | ListEndpoints | list_endpoints | listWebhookEndpoints | read_only | OK | 20.315ms | 24.836ms | 20.051ms | 15.549ms | 34.651ms | 25 |
| WebhookService | UpdateEndpoint | update_endpoint | updateWebhookEndpoint | mutation | OK | 18.473ms | 19.009ms | 18.215ms | 14.962ms | 23.039ms | 5 |
| WorkflowService | CancelWorkflow | cancel_workflow | cancelWorkflow | destructive | OK | 29.806ms | 29.806ms | 29.806ms | 29.806ms | 29.806ms | 1 |
| WorkflowService | GetWorkflow | get_workflow | getWorkflow | read_only | OK | 11.33ms | 13.749ms | 11.668ms | 7.549ms | 19.991ms | 25 |
| WorkflowService | ListWorkflows | list_workflows | listWorkflows | read_only | OK | 13.628ms | 24.34ms | 14.629ms | 8.776ms | 29.635ms | 25 |
| WorkflowService | SignalWorkflow | signal_workflow | signalWorkflow | mutation | OK | 20.946ms | 22.166ms | 20.374ms | 16.454ms | 22.592ms | 5 |
| WorkflowService | StartWorkflow | start_workflow | startWorkflow | mutation | OK | 21.012ms | 21.074ms | 21.402ms | 18.292ms | 26.639ms | 5 |
