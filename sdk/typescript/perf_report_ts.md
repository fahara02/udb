# UDB SDK Live Perf — TypeScript (localhost)

RPCs measured: 344   tenant=c9ef3f54-452e-435a-b75f-45ad4a18c078

Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities (a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request's reference/ID fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection latency. Any residual non-OK RPC is listed under Failures for the maintainer to finish.

Unary = full request/response round-trip. Non-CDC server-streaming RPCs (kind=stream) report time-to-FIRST-RESPONSE with seeded inputs; client-streaming/bidi RPCs (kind=stream_open) report stream-open latency. CDC subscription (publish_cdc, kind=stream) reports time-to-FIRST-EVENT: the harness subscribes, fires a real seeded Upsert that flows outbox→CDC→Kafka, and times the first delivered event.

RPCs run on the AUTH ROUTE in three phases (BENCH_RPC_BODIES.md "Execution order"): Phase 1 establishes the session (AuthnService login → refresh_token → refresh_session → authenticate → validate_token → introspect_token → get_jwks), then the seed phase; Phase 2 measures everything else; Phase 3 LAST runs the session/credential-teardown AuthnService RPCs (logout, revoke_*, change/reset password, admin_reset_mfa, disable_mfa_factor, …) against the seeded DISPOSABLE user/session so the admin's own session is never killed mid-run.

## Seeded fixtures

Captured semantic field → seeded value keys used to resolve request fields: action, apply_run_id, approval_token, approve_draft_id, approve_run_id, approved_by, asset_id, assigned_by, auth_challenge_id, backup_id, bucket, canary_id, canary_version_id, cancel_workflow_id, catalog_manifest_b64, challenge_id, close_room_id, code, collection, content_type, created_by, csrf_token, definition_id, delete_endpoint_id, delete_file_id, delete_job_id, delete_policy_id, delete_role_id, delete_scim_user_id, deleted_by, device_id, disable_provider_id, dismiss_dlq_id, dlq_id, document_id, domain, ds_policy_id, egress_id, endpoint_id, event_type, external_identity_id, fencing_token, file_id, file_type, filename, finalize_file_id, gov_exp, instance_id, job_id, join_session_room_id, key_id, key_prefix, kind, leave_peer_id, locale, log_id, mark_saga_id, message_type, migration_id, mongo_collection, name, node_id, notification_id, object, object_key, otp_code, otp_id, owner_id, peer_id, plain_key, policy_draft_id, policy_id, policy_version_id, project, project_id, provider_id, purge_tenant_id, quarantine_dlq_id, recipient_id, record_id, recovery_code, refresh_session_id, refresh_token, reg_challenge_id, reject_draft_id, rejected_by, relation, release_fencing_token, renew_fencing_token, replay_dlq_id, reset_otp_code, reset_otp_id, resource, resource_name, restore_tenant_id, retry_saga_id, revoke_key_id, revoke_key_prefix, revoked_by, role, role_code, role_id, rollback_policy_set_id, rollback_resource_version, rollback_target_version_id, room_id, saga_id, saml_provider_id, scim_group_id, scim_user_id, session_id, signal_peer_id, stage_name, step_id, subject, tenant, tenant_code, tenant_id, token, topic_pattern, track_id, ts_table, unpublish_track_id, update_draft_id, update_key_id, update_key_prefix, updated_by, user_id, user_role_id, username, vault_ciphertext, vault_create_key_name, vault_db_role, vault_delete_secret_path, vault_destroy_secret_path, vault_key_name, vault_put_secret_path, vault_secret_path, vault_signature, workflow_id

## Per-service mean latency

| Service | RPCs | mean ms |
|---|--:|--:|
| BackupService | 8 | 604.58 |
| StorageService | 8 | 335.75 |
| SearchService | 5 | 125.43 |
| TenantService | 7 | 117.46 |
| AuthnService | 50 | 109.97 |
| LockService | 3 | 82.49 |
| AuthzService | 41 | 74.00 |
| CacheService | 7 | 64.88 |
| ControlPlaneService | 6 | 55.32 |
| PeerService | 5 | 52.49 |
| ConfigService | 5 | 51.29 |
| DataBroker | 77 | 44.82 |
| VaultService | 14 | 44.72 |
| WebhookService | 6 | 39.46 |
| TrackService | 4 | 38.86 |
| NotificationService | 12 | 38.14 |
| WorkflowService | 5 | 37.13 |
| IdentityProviderService | 27 | 36.48 |
| ApiKeyService | 9 | 35.40 |
| RoomService | 9 | 33.42 |
| EmbeddingService | 6 | 32.73 |
| MeteringService | 6 | 29.22 |
| AssetService | 8 | 27.77 |
| SchedulerService | 6 | 26.42 |
| TurnService | 1 | 20.99 |
| SignalingService | 1 | 14.35 |
| LiveQueryService | 1 | 12.18 |
| AnalyticsService | 7 | 10.50 |

## Failures (0)

No RPC returned a non-OK gRPC status.

## Capability Skips (4)

| RPC | api_alias | operation_id | kind | reason |
|---|---|---|---|---|
| RoomService/ListEgress | list_egress | listEgress | read_only | capability skipped: udb udb.core.webrtc.services.v1.RoomService/ListEgress: webrtc_egress_enabled (code=FAILED_PRECONDITION) |
| RoomService/StartRoomComposite | start_room_composite | startRoomComposite | mutation | capability skipped: udb udb.core.webrtc.services.v1.RoomService/StartRoomComposite: webrtc_egress_enabled (code=FAILED_PRECONDITION) |
| RoomService/StartTrackEgress | start_track_egress | startTrackEgress | mutation | capability skipped: udb udb.core.webrtc.services.v1.RoomService/StartTrackEgress: webrtc_egress_enabled (code=FAILED_PRECONDITION) |
| RoomService/StopEgress | stop_egress | stopEgress | mutation | capability skipped: udb udb.core.webrtc.services.v1.RoomService/StopEgress: webrtc_egress_enabled (code=FAILED_PRECONDITION) |

## Slowest 20 by p99

| RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms | note |
|---|---|---|---|---|--:|--:|--:|---|
| BackupService/StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 2702.43 | 2712.87 | 2718.47 | mutation (seeded success path) |
| BackupService/RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 1868.24 | 1868.24 | 1868.24 | destructive: 1 real call against a seeded disposable target |
| AuthnService/ChangePassword | change_password | changePassword | mutation | OK | 1464.42 | 1464.42 | 1464.42 | mutation (seeded success path) |
| StorageService/RegisterUpload | register_upload | registerUpload | mutation | OK | 863.46 | 1168.71 | 872.70 | mutation (seeded success path) |
| DataBroker/StageCatalog | stage_catalog | stageCatalog | destructive | OK | 966.44 | 966.44 | 966.44 | destructive: 1 real call against a seeded disposable target |
| AuthnService/CreateUser | create_user | createUser | mutation | OK | 876.91 | 883.73 | 861.37 | mutation (seeded success path) |
| AuthnService/ResetPassword | reset_password | resetPassword | mutation | OK | 875.86 | 875.86 | 875.86 | mutation (seeded success path) |
| StorageService/UpdateFile | update_file | updateFile | mutation | OK | 711.30 | 754.64 | 709.72 | mutation (seeded success path) |
| StorageService/FinalizeUpload | finalize_upload | finalizeUpload | mutation | OK | 620.28 | 620.28 | 620.28 | mutation (seeded success path) |
| AuthnService/Login | login | login | mutation | OK | 561.91 | 568.55 | 567.43 | mutation (seeded success path) |
| SearchService/Reindex | reindex | reindexSearchIndex | mutation | OK | 425.46 | 547.32 | 421.81 | mutation (seeded success path) |
| DataBroker/ApplyMigration | apply_migration | applyMigration | mutation | OK | 357.35 | 357.35 | 357.35 | mutation (seeded success path) |
| TenantService/UpdateTenant | update_tenant | updateTenant | mutation | OK | 118.95 | 357.32 | 251.50 | mutation (seeded success path) |
| AuthzService/SeedBuiltinRoles | seed_builtin_roles | seedBuiltinRoles | mutation | OK | 262.20 | 338.26 | 299.59 | mutation (seeded success path) |
| StorageService/DeleteFile | delete_file | deleteFile | mutation | OK | 321.38 | 321.38 | 321.38 | mutation (seeded success path) |
| TenantService/PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 301.51 | 301.51 | 301.51 | destructive: 1 real call against a seeded disposable target |
| CacheService/GetCacheNamespaceStats | get_cache_namespace_stats | getCacheNamespaceStats | read_only | OK | 110.02 | 281.93 | 140.55 | read_only (seeded success path) |
| ControlPlaneService/RollbackResources | rollback_resources | rollbackResources | mutation | OK | 237.28 | 245.93 | 209.04 | mutation (seeded success path) |
| DataBroker/PublishCDC | publish_cdc | publishCdc | stream | OK | 245.47 | 245.47 | 246.07 | cdc: time-to-first-event (real seeded Upsert produced) |
| AuthzService/ApprovePolicyDraft | approve_policy_draft | approvePolicyDraft | mutation | OK | 239.14 | 239.14 | 239.14 | mutation (seeded success path) |

## Full per-RPC table (sorted by service, then RPC)

| Service | RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms | note |
|---|---|---|---|---|---|--:|--:|--:|---|
| AnalyticsService | GetExecutorPerformance | get_executor_performance | getExecutorPerformance | read_only | OK | 11.24 | 14.74 | 10.13 | read_only (seeded success path) |
| AnalyticsService | GetPipelineSummary | get_pipeline_summary | getPipelineSummary | read_only | OK | 8.29 | 12.33 | 8.99 | read_only (seeded success path) |
| AnalyticsService | GetReconciliationAnalytics | get_reconciliation_analytics | getReconciliationAnalytics | read_only | OK | 9.61 | 13.14 | 9.77 | read_only (seeded success path) |
| AnalyticsService | GetSlaCompliance | get_sla_compliance | getSlaCompliance | read_only | OK | 8.82 | 15.32 | 9.31 | read_only (seeded success path) |
| AnalyticsService | GetThroughput | get_throughput | getThroughput | read_only | OK | 8.75 | 13.51 | 9.06 | read_only (seeded success path) |
| AnalyticsService | RecordPipelineMetric | record_pipeline_metric | recordPipelineMetric | mutation | OK | 12.30 | 13.09 | 11.91 | mutation (seeded success path) |
| AnalyticsService | TriggerSnapshot | trigger_snapshot | triggerSnapshot | mutation | OK | 14.95 | 15.08 | 14.33 | mutation (seeded success path) |
| ApiKeyService | CreateApiKey | create_api_key | createApiKey | mutation | OK | 18.70 | 22.83 | 24.18 | mutation (seeded success path) |
| ApiKeyService | EmergencyRevokeApiKeys | emergency_revoke_api_keys | emergencyRevokeApiKeys | destructive | OK | 58.18 | 58.18 | 58.18 | destructive: 1 real call against a seeded disposable target |
| ApiKeyService | GetApiKey | get_api_key | getApiKey | read_only | OK | 8.28 | 19.98 | 9.72 | read_only (seeded success path) |
| ApiKeyService | GetApiKeyUsageStats | get_api_key_usage_stats | getApiKeyUsageStats | read_only | OK | 14.33 | 17.29 | 12.58 | read_only (seeded success path) |
| ApiKeyService | ListApiKeys | list_api_keys | listApiKeys | read_only | OK | 9.08 | 11.34 | 9.15 | read_only (seeded success path) |
| ApiKeyService | RevokeApiKey | revoke_api_key | revokeApiKey | mutation | OK | 35.16 | 35.16 | 35.16 | mutation (seeded success path) |
| ApiKeyService | RotateApiKey | rotate_api_key | rotateApiKey | mutation | OK | 99.63 | 99.63 | 99.63 | mutation (seeded success path) |
| ApiKeyService | UpdateApiKey | update_api_key | updateApiKey | mutation | OK | 54.79 | 67.56 | 55.54 | mutation (seeded success path) |
| ApiKeyService | ValidateApiKey | validate_api_key | validateApiKey | read_only | OK | 13.53 | 20.29 | 14.44 | read_only (seeded success path) |
| AssetService | CompleteStep | complete_step | completeStep | mutation | OK | 58.03 | 60.05 | 52.84 | mutation (seeded success path) |
| AssetService | CreatePipelineDefinition | create_pipeline_definition | createPipelineDefinition | mutation | OK | 22.69 | 34.05 | 26.79 | mutation (seeded success path) |
| AssetService | GetAsset | get_asset | getAsset | read_only | OK | 14.94 | 26.84 | 15.95 | read_only (seeded success path) |
| AssetService | GetPipeline | get_pipeline | getPipeline | read_only | OK | 16.15 | 21.47 | 17.22 | read_only (seeded success path) |
| AssetService | GetPipelineDefinition | get_pipeline_definition | getPipelineDefinition | read_only | OK | 20.54 | 31.07 | 21.81 | read_only (seeded success path) |
| AssetService | ListAssets | list_assets | listAssets | read_only | OK | 19.41 | 29.35 | 20.47 | read_only (seeded success path) |
| AssetService | RegisterAsset | register_asset | registerAsset | mutation | OK | 39.17 | 43.69 | 37.09 | mutation (seeded success path) |
| AssetService | StartPipeline | start_pipeline | startPipeline | mutation | OK | 24.00 | 36.66 | 29.97 | mutation (seeded success path) |
| AuthnService | AdminResetMfa | admin_reset_mfa | adminResetMfa | destructive | OK | 34.26 | 34.26 | 34.26 | destructive: 1 real call against a seeded disposable target |
| AuthnService | AdminResetPassword | admin_reset_password | adminResetPassword | destructive | OK | 19.09 | 19.09 | 19.09 | destructive: 1 real call against a seeded disposable target |
| AuthnService | AdminRevokeAllTenantSessions | admin_revoke_all_tenant_sessions | adminRevokeAllTenantSessions | destructive | OK | 23.10 | 23.10 | 23.10 | destructive: 1 real call against a seeded disposable target |
| AuthnService | AdminRevokeAllUserSessions | admin_revoke_all_user_sessions | adminRevokeAllUserSessions | destructive | OK | 22.73 | 22.73 | 22.73 | destructive: 1 real call against a seeded disposable target |
| AuthnService | AdminRevokeSession | admin_revoke_session | adminRevokeSession | destructive | OK | 23.55 | 23.55 | 23.55 | destructive: 1 real call against a seeded disposable target |
| AuthnService | Authenticate | authenticate | authenticate | read_only | OK | 38.28 | 69.77 | 41.05 | read_only (seeded success path) |
| AuthnService | ChangePassword | change_password | changePassword | mutation | OK | 1464.42 | 1464.42 | 1464.42 | mutation (seeded success path) |
| AuthnService | ChangeUserStatus | change_user_status | changeUserStatus | destructive | OK | 87.15 | 87.15 | 87.15 | destructive: 1 real call against a seeded disposable target |
| AuthnService | ConfirmMFAEnrollment | confirm_mfaenrollment | confirmMfaenrollment | mutation | OK | 9.40 | 12.56 | 10.68 | mutation (seeded success path) |
| AuthnService | CreateSession | create_session | createSession | mutation | OK | 12.62 | 18.21 | 16.30 | mutation (seeded success path) |
| AuthnService | CreateUser | create_user | createUser | mutation | OK | 876.91 | 883.73 | 861.37 | mutation (seeded success path) |
| AuthnService | DeleteWebAuthnCredential | delete_web_authn_credential | deleteWebAuthnCredential | mutation | OK | 21.16 | 26.66 | 23.41 | mutation (seeded success path) |
| AuthnService | DisableMfaFactor | disable_mfa_factor | disableMfaFactor | mutation | OK | 24.07 | 27.53 | 27.62 | mutation (seeded success path) |
| AuthnService | EmergencyRevoke | emergency_revoke | emergencyRevoke | destructive | OK | 22.63 | 22.63 | 22.63 | destructive: 1 real call against a seeded disposable target |
| AuthnService | EnrollMFA | enroll_mfa | enrollMfa | mutation | OK | 29.39 | 42.70 | 33.83 | mutation (seeded success path) |
| AuthnService | FinishWebAuthnAuthentication | finish_web_authn_authentication | finishWebAuthnAuthentication | mutation | OK | 162.93 | 162.93 | 162.93 | mutation (seeded success path) |
| AuthnService | FinishWebAuthnRegistration | finish_web_authn_registration | finishWebAuthnRegistration | mutation | OK | 85.81 | 85.81 | 85.81 | mutation (seeded success path) |
| AuthnService | ForgotPassword | forgot_password | forgotPassword | mutation | OK | 16.45 | 30.35 | 20.89 | mutation (seeded success path) |
| AuthnService | GenerateRecoveryCodes | generate_recovery_codes | generateRecoveryCodes | mutation | OK | 128.23 | 143.42 | 147.36 | mutation (seeded success path) |
| AuthnService | GetJwks | get_jwks | getJwks | read_only | OK | 7.28 | 9.54 | 7.50 | read_only (seeded success path) |
| AuthnService | GetMfaPolicy | get_mfa_policy | getMfaPolicy | read_only | OK | 6.28 | 8.20 | 6.28 | read_only (seeded success path) |
| AuthnService | GetSession | get_session | getSession | read_only | OK | 6.85 | 9.41 | 7.00 | read_only (seeded success path) |
| AuthnService | GetUser | get_user | getUser | read_only | OK | 6.77 | 8.09 | 6.79 | read_only (seeded success path) |
| AuthnService | IntrospectToken | introspect_token | introspectToken | read_only | OK | 44.39 | 54.14 | 44.35 | read_only (seeded success path) |
| AuthnService | IssueMfaChallenge | issue_mfa_challenge | issueMfaChallenge | mutation | OK | 68.38 | 74.76 | 65.16 | mutation (seeded success path) |
| AuthnService | ListDevices | list_devices | listDevices | read_only | OK | 7.33 | 9.34 | 7.44 | read_only (seeded success path) |
| AuthnService | ListMfaFactors | list_mfa_factors | listMfaFactors | read_only | OK | 9.93 | 15.22 | 10.35 | read_only (seeded success path) |
| AuthnService | ListSessions | list_sessions | listSessions | read_only | OK | 12.55 | 15.12 | 12.50 | read_only (seeded success path) |
| AuthnService | ListUsers | list_users | listUsers | read_only | OK | 10.99 | 13.03 | 10.97 | read_only (seeded success path) |
| AuthnService | ListWebAuthnCredentials | list_web_authn_credentials | listWebAuthnCredentials | read_only | OK | 6.60 | 9.14 | 6.82 | read_only (seeded success path) |
| AuthnService | Login | login | login | mutation | OK | 561.91 | 568.55 | 567.43 | mutation (seeded success path) |
| AuthnService | Logout | logout | logout | mutation | OK | 12.20 | 13.32 | 17.24 | mutation (seeded success path) |
| AuthnService | PutMfaPolicy | put_mfa_policy | putMfaPolicy | mutation | OK | 34.01 | 42.79 | 37.12 | mutation (seeded success path) |
| AuthnService | RefreshSession | refresh_session | refreshSession | mutation | OK | 39.17 | 39.94 | 52.98 | mutation (seeded success path) |
| AuthnService | RefreshToken | refresh_token | refreshToken | mutation | OK | 11.23 | 11.23 | 11.23 | mutation (seeded success path) |
| AuthnService | RenamePasskey | rename_passkey | renamePasskey | mutation | OK | 26.18 | 28.59 | 26.59 | mutation (seeded success path) |
| AuthnService | ResendOTP | resend_otp | resendOtp | mutation | OK | 66.80 | 68.04 | 59.79 | mutation (seeded success path) |
| AuthnService | ResetPassword | reset_password | resetPassword | mutation | OK | 875.86 | 875.86 | 875.86 | mutation (seeded success path) |
| AuthnService | RevokeDevice | revoke_device | revokeDevice | mutation | OK | 36.59 | 36.59 | 36.59 | mutation (seeded success path) |
| AuthnService | RevokeRecoveryCodes | revoke_recovery_codes | revokeRecoveryCodes | mutation | OK | 30.63 | 34.02 | 30.31 | mutation (seeded success path) |
| AuthnService | RevokeSession | revoke_session | revokeSession | mutation | OK | 16.10 | 16.65 | 15.28 | mutation (seeded success path) |
| AuthnService | SendOTP | send_otp | sendOtp | mutation | OK | 65.75 | 86.28 | 84.71 | mutation (seeded success path) |
| AuthnService | SendPhoneVerification | send_phone_verification | sendPhoneVerification | mutation | OK | 51.13 | 56.07 | 51.70 | mutation (seeded success path) |
| AuthnService | StartWebAuthnAuthentication | start_web_authn_authentication | startWebAuthnAuthentication | mutation | OK | 110.78 | 131.28 | 106.70 | mutation (seeded success path) |
| AuthnService | StartWebAuthnRegistration | start_web_authn_registration | startWebAuthnRegistration | mutation | OK | 81.86 | 84.32 | 73.41 | mutation (seeded success path) |
| AuthnService | UpdateUser | update_user | updateUser | mutation | OK | 48.90 | 49.95 | 43.70 | mutation (seeded success path) |
| AuthnService | ValidateCSRF | validate_csrf | validateCsrf | read_only | OK | 6.90 | 8.50 | 6.90 | read_only (seeded success path) |
| AuthnService | ValidateToken | validate_token | validateToken | read_only | OK | 33.38 | 46.09 | 34.09 | read_only (seeded success path) |
| AuthnService | VerifyMfaChallenge | verify_mfa_challenge | verifyMfaChallenge | read_only | OK | 10.95 | 13.12 | 10.90 | read_only (seeded success path) |
| AuthnService | VerifyOTP | verify_otp | verifyOtp | read_only | OK | 40.10 | 65.53 | 42.64 | read_only (seeded success path) |
| AuthzService | ActivateCanary | activate_canary | activateCanary | destructive | OK | 41.98 | 41.98 | 41.98 | destructive: 1 real call against a seeded disposable target |
| AuthzService | ActivatePolicyVersion | activate_policy_version | activatePolicyVersion | destructive | OK | 94.49 | 94.49 | 94.49 | destructive: 1 real call against a seeded disposable target |
| AuthzService | ApprovePolicyDraft | approve_policy_draft | approvePolicyDraft | mutation | OK | 239.14 | 239.14 | 239.14 | mutation (seeded success path) |
| AuthzService | AssignRole | assign_role | assignRole | mutation | OK | 229.33 | 229.84 | 313.20 | mutation (seeded success path) |
| AuthzService | Authorize | authorize | authorize | read_only | OK | 33.17 | 40.36 | 33.06 | read_only (seeded success path) |
| AuthzService | BatchCheckPermissions | batch_check_permissions | batchCheckPermissions | read_only | OK | 12.93 | 15.93 | 13.39 | read_only (seeded success path) |
| AuthzService | CheckAccess | check_access | checkAccess | read_only | OK | 13.85 | 19.49 | 14.49 | read_only (seeded success path) |
| AuthzService | CreatePolicyDraft | create_policy_draft | createPolicyDraft | mutation | OK | 224.78 | 229.41 | 228.58 | mutation (seeded success path) |
| AuthzService | CreatePolicyRule | create_policy_rule | createPolicyRule | mutation | OK | 79.29 | 98.24 | 82.47 | mutation (seeded success path) |
| AuthzService | CreateRole | create_role | createRole | mutation | OK | 88.66 | 88.66 | 88.66 | mutation (seeded success path) |
| AuthzService | DeletePolicyRule | delete_policy_rule | deletePolicyRule | mutation | OK | 43.89 | 47.64 | 44.05 | mutation (seeded success path) |
| AuthzService | DeleteRole | delete_role | deleteRole | mutation | OK | 37.24 | 38.90 | 52.27 | mutation (seeded success path) |
| AuthzService | DiffPolicyDraft | diff_policy_draft | diffPolicyDraft | read_only | OK | 26.76 | 49.68 | 31.50 | read_only (seeded success path) |
| AuthzService | ExplainPolicy | explain_policy | explainPolicy | read_only | OK | 15.33 | 23.12 | 15.65 | read_only (seeded success path) |
| AuthzService | GetAuthzRevision | get_authz_revision | getAuthzRevision | read_only | OK | 10.37 | 13.10 | 10.54 | read_only (seeded success path) |
| AuthzService | GetCanaryStatus | get_canary_status | getCanaryStatus | read_only | OK | 17.53 | 25.10 | 18.01 | read_only (seeded success path) |
| AuthzService | GetNativeAccess | get_native_access | getNativeAccess | read_only | OK | 35.33 | 58.64 | 38.97 | read_only (seeded success path) |
| AuthzService | GetPolicyBundle | get_policy_bundle | getPolicyBundle | read_only | OK | 15.34 | 25.46 | 16.30 | read_only (seeded success path) |
| AuthzService | GetPolicyRule | get_policy_rule | getPolicyRule | read_only | OK | 10.18 | 16.43 | 10.23 | read_only (seeded success path) |
| AuthzService | GetRole | get_role | getRole | read_only | OK | 11.01 | 20.85 | 11.51 | read_only (seeded success path) |
| AuthzService | InvalidatePolicyBundles | invalidate_policy_bundles | invalidatePolicyBundles | destructive | OK | 44.56 | 44.56 | 44.56 | destructive: 1 real call against a seeded disposable target |
| AuthzService | LintAuthzPolicies | lint_authz_policies | lintAuthzPolicies | read_only | OK | 5.51 | 7.44 | 5.89 | read_only (seeded success path) |
| AuthzService | ListAccessDecisionAudits | list_access_decision_audits | listAccessDecisionAudits | read_only | OK | 26.88 | 48.17 | 29.85 | read_only (seeded success path) |
| AuthzService | ListPolicyRules | list_policy_rules | listPolicyRules | read_only | OK | 9.72 | 13.00 | 9.85 | read_only (seeded success path) |
| AuthzService | ListPolicyVersions | list_policy_versions | listPolicyVersions | read_only | OK | 19.43 | 27.40 | 20.10 | read_only (seeded success path) |
| AuthzService | ListRoles | list_roles | listRoles | read_only | OK | 17.65 | 26.10 | 17.42 | read_only (seeded success path) |
| AuthzService | ListUserPermissions | list_user_permissions | listUserPermissions | read_only | OK | 4.66 | 7.16 | 5.16 | read_only (seeded success path) |
| AuthzService | ListUserRoles | list_user_roles | listUserRoles | read_only | OK | 15.93 | 36.85 | 19.04 | read_only (seeded success path) |
| AuthzService | MigrateLegacyPolicies | migrate_legacy_policies | migrateLegacyPolicies | destructive | OK | 49.65 | 49.65 | 49.65 | destructive: 1 real call against a seeded disposable target |
| AuthzService | PromoteCanary | promote_canary | promoteCanary | destructive | OK | 132.73 | 132.73 | 132.73 | destructive: 1 real call against a seeded disposable target |
| AuthzService | PutAuthzPolicy | put_authz_policy | putAuthzPolicy | mutation | OK | 88.60 | 95.55 | 85.43 | mutation (seeded success path) |
| AuthzService | PutRelationship | put_relationship | putRelationship | mutation | OK | 54.24 | 62.66 | 71.44 | mutation (seeded success path) |
| AuthzService | PutRoleBinding | put_role_binding | putRoleBinding | mutation | OK | 41.66 | 55.48 | 47.47 | mutation (seeded success path) |
| AuthzService | RejectPolicyDraft | reject_policy_draft | rejectPolicyDraft | mutation | OK | 105.82 | 105.82 | 105.82 | mutation (seeded success path) |
| AuthzService | RevokeRole | revoke_role | revokeRole | mutation | OK | 35.89 | 41.72 | 40.38 | mutation (seeded success path) |
| AuthzService | RollbackPolicyVersion | rollback_policy_version | rollbackPolicyVersion | destructive | OK | 109.42 | 109.42 | 109.42 | destructive: 1 real call against a seeded disposable target |
| AuthzService | SeedBuiltinRoles | seed_builtin_roles | seedBuiltinRoles | mutation | OK | 262.20 | 338.26 | 299.59 | mutation (seeded success path) |
| AuthzService | SimulatePolicy | simulate_policy | simulatePolicy | mutation | OK | 82.72 | 85.75 | 96.90 | mutation (seeded success path) |
| AuthzService | SubmitPolicyDraft | submit_policy_draft | submitPolicyDraft | mutation | OK | 163.78 | 163.78 | 163.78 | mutation (seeded success path) |
| AuthzService | UpdatePolicyDraft | update_policy_draft | updatePolicyDraft | mutation | OK | 165.72 | 213.00 | 181.05 | mutation (seeded success path) |
| AuthzService | UpdateRole | update_role | updateRole | mutation | OK | 106.35 | 115.26 | 99.95 | mutation (seeded success path) |
| BackupService | DeleteBackupPolicy | delete_backup_policy | deleteBackupPolicy | mutation | OK | 52.39 | 57.40 | 55.15 | mutation (seeded success path) |
| BackupService | GetBackup | get_backup | getBackup | read_only | OK | 42.33 | 75.33 | 46.28 | read_only (seeded success path) |
| BackupService | GetBackupPolicy | get_backup_policy | getBackupPolicy | read_only | OK | 23.54 | 33.92 | 24.56 | read_only (seeded success path) |
| BackupService | ListBackupPolicies | list_backup_policies | listBackupPolicies | read_only | OK | 23.04 | 38.07 | 24.26 | read_only (seeded success path) |
| BackupService | ListBackups | list_backups | listBackups | read_only | OK | 23.74 | 34.11 | 24.46 | read_only (seeded success path) |
| BackupService | PutBackupPolicy | put_backup_policy | putBackupPolicy | mutation | OK | 79.16 | 79.79 | 75.25 | mutation (seeded success path) |
| BackupService | RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 1868.24 | 1868.24 | 1868.24 | destructive: 1 real call against a seeded disposable target |
| BackupService | StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 2702.43 | 2712.87 | 2718.47 | mutation (seeded success path) |
| CacheService | CacheDelete | cache_delete | cacheNamespaceDelete | mutation | OK | 51.21 | 51.69 | 48.27 | mutation (seeded success path) |
| CacheService | CacheGet | cache_get | cacheNamespaceGet | read_only | OK | 24.29 | 52.55 | 28.88 | read_only (seeded success path) |
| CacheService | CacheScan | cache_scan | cacheNamespaceScan | read_only | OK | 16.43 | 28.61 | 18.47 | read_only (seeded success path) |
| CacheService | CacheSet | cache_set | cacheNamespaceSet | mutation | OK | 56.08 | 56.22 | 48.83 | mutation (seeded success path) |
| CacheService | CreateCacheNamespace | create_cache_namespace | createCacheNamespace | mutation | OK | 49.06 | 50.38 | 43.43 | mutation (seeded success path) |
| CacheService | DeleteCacheNamespace | delete_cache_namespace | deleteCacheNamespace | destructive | OK | 125.73 | 125.73 | 125.73 | destructive: 1 real call against a seeded disposable target |
| CacheService | GetCacheNamespaceStats | get_cache_namespace_stats | getCacheNamespaceStats | read_only | OK | 110.02 | 281.93 | 140.55 | read_only (seeded success path) |
| ConfigService | DeleteFlag | delete_flag | deleteFlag | destructive | OK | 33.14 | 33.14 | 33.14 | destructive: 1 real call against a seeded disposable target |
| ConfigService | EvaluateFlags | evaluate_flags | evaluateFlags | read_only | OK | 20.49 | 28.09 | 22.75 | read_only (seeded success path) |
| ConfigService | GetFlag | get_flag | getFlag | read_only | OK | 29.20 | 56.47 | 33.11 | read_only (seeded success path) |
| ConfigService | ListFlags | list_flags | listFlags | read_only | OK | 26.25 | 52.65 | 29.67 | read_only (seeded success path) |
| ConfigService | PutFlag | put_flag | putFlag | mutation | OK | 124.45 | 159.82 | 137.79 | mutation (seeded success path) |
| ControlPlaneService | AckStatus | ack_status | ackStatus | mutation | OK | 40.76 | 50.23 | 44.24 | mutation (seeded success path) |
| ControlPlaneService | DeltaResources | delta_resources | deltaResources | stream_open | OK | 0.62 | 0.62 | 0.62 | streaming: stream-open latency |
| ControlPlaneService | GetResources | get_resources | getResources | read_only | OK | 11.70 | 29.60 | 14.76 | read_only (seeded success path) |
| ControlPlaneService | ListNodeStates | list_node_states | listNodeStates | read_only | OK | 58.18 | 87.72 | 62.82 | read_only (seeded success path) |
| ControlPlaneService | RollbackResources | rollback_resources | rollbackResources | mutation | OK | 237.28 | 245.93 | 209.04 | mutation (seeded success path) |
| ControlPlaneService | StreamResources | stream_resources | streamResources | stream_open | OK | 0.47 | 0.47 | 0.47 | streaming: stream-open latency |
| DataBroker | ActivateCatalog | activate_catalog | activateCatalog | destructive | OK | 8.44 | 8.44 | 8.44 | destructive: 1 real call against a seeded disposable target |
| DataBroker | AnalyticalQuery | analytical_query | analyticalQuery | read_only | OK | 17.98 | 23.26 | 19.55 | read_only (seeded success path) |
| DataBroker | ApplyMigration | apply_migration | applyMigration | mutation | OK | 357.35 | 357.35 | 357.35 | mutation (seeded success path) |
| DataBroker | ApproveMigrationPlan | approve_migration_plan | approveMigrationPlan | mutation | OK | 109.18 | 109.18 | 109.18 | mutation (seeded success path) |
| DataBroker | BatchSelect | batch_select | batchSelect | stream_open | OK | 0.68 | 0.68 | 0.68 | streaming: stream-open latency |
| DataBroker | BatchUpsert | batch_upsert | batchUpsert | stream_open | OK | 0.38 | 0.38 | 0.38 | streaming: stream-open latency |
| DataBroker | BeginTx | begin_tx | beginTx | stream_open | OK | 0.35 | 0.35 | 0.35 | streaming: stream-open latency |
| DataBroker | CacheDelete | cache_delete | cacheDelete | mutation | OK | 11.19 | 11.57 | 11.53 | mutation (seeded success path) |
| DataBroker | CacheGet | cache_get | cacheGet | read_only | OK | 20.51 | 28.66 | 20.71 | read_only (seeded success path) |
| DataBroker | CacheScan | cache_scan | cacheScan | read_only | OK | 23.05 | 31.77 | 22.93 | read_only (seeded success path) |
| DataBroker | CacheSet | cache_set | cacheSet | mutation | OK | 16.45 | 18.01 | 16.33 | mutation (seeded success path) |
| DataBroker | CreateMaterializedView | create_materialized_view | createMaterializedView | mutation | OK | 12.35 | 12.63 | 11.73 | mutation (seeded success path) |
| DataBroker | Delete | delete | delete | mutation | OK | 119.33 | 127.10 | 116.20 | mutation (seeded success path) |
| DataBroker | DeletePolicy | delete_policy | deletePolicy | mutation | OK | 27.01 | 27.01 | 27.01 | mutation (seeded success path) |
| DataBroker | DismissDlqEvent | dismiss_dlq_event | dismissDlqEvent | mutation | OK | 23.08 | 23.59 | 22.54 | mutation (seeded success path) |
| DataBroker | DocumentDelete | document_delete | documentDelete | mutation | OK | 11.00 | 12.48 | 13.78 | mutation (seeded success path) |
| DataBroker | DocumentFind | document_find | documentFind | read_only | OK | 10.74 | 14.91 | 10.88 | read_only (seeded success path) |
| DataBroker | DocumentGet | document_get | documentGet | read_only | OK | 11.38 | 15.68 | 11.82 | read_only (seeded success path) |
| DataBroker | DocumentUpsert | document_upsert | documentUpsert | mutation | OK | 12.14 | 13.44 | 17.09 | mutation (seeded success path) |
| DataBroker | DropResource | drop_resource | dropResource | destructive | OK | 51.60 | 51.60 | 51.60 | destructive: 1 real call against a seeded disposable target |
| DataBroker | EnqueueOutboxEvent | enqueue_outbox_event | enqueueOutboxEvent | mutation | OK | 28.39 | 28.39 | 28.39 | mutation (seeded success path) |
| DataBroker | EnsureBaseline | ensure_baseline | ensureBaseline | mutation | OK | 41.34 | 41.47 | 36.45 | mutation (seeded success path) |
| DataBroker | EnsureProject | ensure_project | ensureProject | mutation | OK | 63.28 | 89.52 | 78.97 | mutation (seeded success path) |
| DataBroker | EnsureResource | ensure_resource | ensureResource | mutation | OK | 34.71 | 35.76 | 31.85 | mutation (seeded success path) |
| DataBroker | GeneratePresignedUrl | generate_presigned_url | generatePresignedUrl | mutation | OK | 9.48 | 10.80 | 10.21 | mutation (seeded success path) |
| DataBroker | GenericDispatch | generic_dispatch | genericDispatch | mutation | OK | 12.29 | 13.62 | 12.28 | mutation (seeded success path) |
| DataBroker | GetAdminSummary | get_admin_summary | getAdminSummary | read_only | OK | 40.23 | 62.40 | 41.90 | read_only (seeded success path) |
| DataBroker | GetCapabilities | get_capabilities | getCapabilities | read_only | OK | 12.31 | 21.79 | 12.74 | read_only (seeded success path) |
| DataBroker | GetCatalogManifest | get_catalog_manifest | getCatalogManifest | read_only | OK | 24.78 | 55.76 | 28.94 | read_only (seeded success path) |
| DataBroker | GetCatalogVersion | get_catalog_version | getCatalogVersion | read_only | OK | 9.12 | 13.88 | 9.96 | read_only (seeded success path) |
| DataBroker | GetCatalogVersions | get_catalog_versions | getCatalogVersions | read_only | OK | 9.92 | 15.58 | 10.65 | read_only (seeded success path) |
| DataBroker | GetCdcStatus | get_cdc_status | getCdcStatus | read_only | OK | 8.51 | 20.20 | 10.05 | read_only (seeded success path) |
| DataBroker | GetDlqEvent | get_dlq_event | getDlqEvent | read_only | OK | 10.68 | 17.87 | 11.44 | read_only (seeded success path) |
| DataBroker | GetHealthReport | get_health_report | getHealthReport | read_only | OK | 6.62 | 15.51 | 8.18 | read_only (seeded success path) |
| DataBroker | GetMigrationStatus | get_migration_status | getMigrationStatus | read_only | OK | 10.05 | 15.71 | 10.28 | read_only (seeded success path) |
| DataBroker | GetObject | get_object | getObject | stream | OK | 13.16 | 14.98 | 14.00 | streaming: time-to-first-response (seeded) |
| DataBroker | GetSaga | get_saga | getSaga | read_only | OK | 11.59 | 30.64 | 14.59 | read_only (seeded success path) |
| DataBroker | GraphMutate | graph_mutate | graphMutate | mutation | OK | 36.61 | 38.74 | 157.31 | mutation (seeded success path) |
| DataBroker | GraphQuery | graph_query | graphQuery | read_only | OK | 24.14 | 62.79 | 31.13 | read_only (seeded success path) |
| DataBroker | InitiateMultipartUpload | initiate_multipart_upload | initiateMultipartUpload | mutation | OK | 19.14 | 22.86 | 22.64 | mutation (seeded success path) |
| DataBroker | LintPolicies | lint_policies | lintPolicies | read_only | OK | 28.37 | 40.90 | 28.68 | read_only (seeded success path) |
| DataBroker | ListAdminAuditLogs | list_admin_audit_logs | listAdminAuditLogs | read_only | OK | 24.01 | 30.39 | 22.78 | read_only (seeded success path) |
| DataBroker | ListDlqEvents | list_dlq_events | listDlqEvents | read_only | OK | 12.49 | 25.08 | 15.27 | read_only (seeded success path) |
| DataBroker | ListMessageSchemas | list_message_schemas | listMessageSchemas | read_only | OK | 9.96 | 14.53 | 10.72 | read_only (seeded success path) |
| DataBroker | ListMigrationRuns | list_migration_runs | listMigrationRuns | read_only | OK | 11.49 | 23.38 | 13.13 | read_only (seeded success path) |
| DataBroker | ListPolicies | list_policies | listPolicies | read_only | OK | 9.98 | 13.84 | 10.45 | read_only (seeded success path) |
| DataBroker | ListProjects | list_projects | listProjects | read_only | OK | 10.87 | 43.61 | 17.49 | read_only (seeded success path) |
| DataBroker | ListResources | list_resources | listResources | read_only | OK | 12.03 | 29.94 | 13.49 | read_only (seeded success path) |
| DataBroker | ListSagas | list_sagas | listSagas | read_only | OK | 9.93 | 30.18 | 12.75 | read_only (seeded success path) |
| DataBroker | LookupMessageSchema | lookup_message_schema | lookupMessageSchema | read_only | OK | 8.38 | 13.00 | 8.49 | read_only (seeded success path) |
| DataBroker | MarkSagaReviewed | mark_saga_reviewed | markSagaReviewed | mutation | OK | 28.20 | 30.53 | 30.65 | mutation (seeded success path) |
| DataBroker | PauseCdc | pause_cdc | pauseCdc | mutation | OK | 21.06 | 23.16 | 22.34 | mutation (seeded success path) |
| DataBroker | PlanMigration | plan_migration | planMigration | mutation | OK | 26.29 | 28.77 | 27.73 | mutation (seeded success path) |
| DataBroker | PreviewCdcRedaction | preview_cdc_redaction | previewCdcRedaction | read_only | OK | 17.49 | 31.27 | 20.60 | read_only (seeded success path) |
| DataBroker | PublishCDC | publish_cdc | publishCdc | stream | OK | 245.47 | 245.47 | 246.07 | cdc: time-to-first-event (real seeded Upsert produced) |
| DataBroker | PutObject | put_object | putObject | stream_open | OK | 1.52 | 1.52 | 1.52 | streaming: stream-open latency |
| DataBroker | PutPolicy | put_policy | putPolicy | destructive | OK | 20.13 | 20.13 | 20.13 | destructive: 1 real call against a seeded disposable target |
| DataBroker | QuarantineDlqEvent | quarantine_dlq_event | quarantineDlqEvent | mutation | OK | 19.34 | 20.65 | 21.03 | mutation (seeded success path) |
| DataBroker | ReloadPolicies | reload_policies | reloadPolicies | destructive | OK | 14.13 | 14.13 | 14.13 | destructive: 1 real call against a seeded disposable target |
| DataBroker | ReplayDlqEvent | replay_dlq_event | replayDlqEvent | mutation | OK | 29.45 | 29.45 | 29.45 | mutation (seeded success path) |
| DataBroker | ResumeCdc | resume_cdc | resumeCdc | mutation | OK | 17.96 | 18.91 | 18.18 | mutation (seeded success path) |
| DataBroker | RetrySagaCompensation | retry_saga_compensation | retrySagaCompensation | mutation | OK | 16.36 | 16.36 | 16.36 | mutation (seeded success path) |
| DataBroker | RollbackCatalog | rollback_catalog | rollbackCatalog | destructive | OK | 8.79 | 8.79 | 8.79 | destructive: 1 real call against a seeded disposable target |
| DataBroker | ScanProjectionDrift | scan_projection_drift | scanProjectionDrift | read_only | OK | 22.25 | 37.27 | 24.15 | read_only (seeded success path) |
| DataBroker | Select | select | select | read_only | OK | 12.25 | 36.09 | 14.26 | read_only (seeded success path) |
| DataBroker | SelectV2 | select_v_2 | selectV2 | stream | OK | 11.79 | 12.03 | 11.69 | streaming: time-to-first-response (seeded) |
| DataBroker | StageCatalog | stage_catalog | stageCatalog | destructive | OK | 966.44 | 966.44 | 966.44 | destructive: 1 real call against a seeded disposable target |
| DataBroker | StepDownCdcLeader | step_down_cdc_leader | stepDownCdcLeader | mutation | OK | 17.88 | 18.67 | 18.41 | mutation (seeded success path) |
| DataBroker | TimeSeriesQuery | time_series_query | timeSeriesQuery | read_only | OK | 17.17 | 28.18 | 20.83 | read_only (seeded success path) |
| DataBroker | TimeSeriesWrite | time_series_write | timeSeriesWrite | mutation | OK | 13.76 | 20.76 | 20.01 | mutation (seeded success path) |
| DataBroker | Upsert | upsert | upsert | mutation | OK | 105.22 | 131.21 | 126.57 | mutation (seeded success path) |
| DataBroker | ValidateCatalog | validate_catalog | validateCatalog | destructive | OK | 111.69 | 111.69 | 111.69 | destructive: 1 real call against a seeded disposable target |
| DataBroker | VectorBatchUpsert | vector_batch_upsert | vectorBatchUpsert | stream_open | OK | 0.48 | 0.48 | 0.48 | streaming: stream-open latency |
| DataBroker | VectorHybridSearch | vector_hybrid_search | vectorHybridSearch | read_only | OK | 13.30 | 27.22 | 15.44 | read_only (seeded success path) |
| DataBroker | VectorSearch | vector_search | vectorSearch | read_only | OK | 10.89 | 22.98 | 13.82 | read_only (seeded success path) |
| DataBroker | VectorUpsert | vector_upsert | vectorUpsert | mutation | OK | 26.42 | 26.80 | 30.62 | mutation (seeded success path) |
| DataBroker | VerifyAdminAuditLog | verify_admin_audit_log | verifyAdminAuditLog | read_only | OK | 13.99 | 20.48 | 14.54 | read_only (seeded success path) |
| EmbeddingService | Backfill | backfill | backfillEmbeddingSource | mutation | OK | 33.04 | 37.10 | 33.38 | mutation (seeded success path) |
| EmbeddingService | DeleteSource | delete_source | deleteEmbeddingSource | destructive | OK | 35.93 | 35.93 | 35.93 | destructive: 1 real call against a seeded disposable target |
| EmbeddingService | ListSources | list_sources | listEmbeddingSources | read_only | OK | 17.98 | 21.06 | 18.16 | read_only (seeded success path) |
| EmbeddingService | RegisterSource | register_source | registerEmbeddingSource | mutation | OK | 49.98 | 54.42 | 46.64 | mutation (seeded success path) |
| EmbeddingService | ReportEmbedding | report_embedding | reportEmbedding | mutation | OK | 25.79 | 43.74 | 37.87 | mutation (seeded success path) |
| EmbeddingService | Retrieve | retrieve | retrieveEmbedding | read_only | OK | 23.12 | 36.15 | 24.43 | read_only (seeded success path) |
| IdentityProviderService | CreateProvider | create_provider | createProvider | mutation | OK | 24.22 | 24.22 | 24.22 | mutation (seeded success path) |
| IdentityProviderService | DisableProvider | disable_provider | disableProvider | mutation | OK | 41.59 | 74.73 | 54.26 | mutation (seeded success path) |
| IdentityProviderService | ForceJwksRefresh | force_jwks_refresh | forceJwksRefresh | mutation | OK | 64.34 | 78.77 | 63.61 | mutation (seeded success path) |
| IdentityProviderService | GetProvider | get_provider | getProvider | read_only | OK | 9.87 | 15.02 | 10.63 | read_only (seeded success path) |
| IdentityProviderService | ImportSamlMetadata | import_saml_metadata | importSamlMetadata | mutation | OK | 40.49 | 41.75 | 38.44 | mutation (seeded success path) |
| IdentityProviderService | LinkIdentity | link_identity | linkIdentity | mutation | OK | 35.84 | 38.96 | 35.47 | mutation (seeded success path) |
| IdentityProviderService | ListExternalIdentities | list_external_identities | listExternalIdentities | read_only | OK | 17.14 | 27.08 | 17.59 | read_only (seeded success path) |
| IdentityProviderService | ListProviders | list_providers | listProviders | read_only | OK | 14.75 | 25.61 | 17.02 | read_only (seeded success path) |
| IdentityProviderService | PreviewClaimMapping | preview_claim_mapping | previewClaimMapping | read_only | OK | 10.43 | 28.85 | 14.06 | read_only (seeded success path) |
| IdentityProviderService | PreviewGroupMapping | preview_group_mapping | previewGroupMapping | read_only | OK | 8.09 | 16.87 | 8.92 | read_only (seeded success path) |
| IdentityProviderService | ResolveExternalIdentity | resolve_external_identity | resolveExternalIdentity | mutation | OK | 15.26 | 17.32 | 22.03 | mutation (seeded success path) |
| IdentityProviderService | SamlAcs | saml_acs | samlAcs | mutation | OK | 119.65 | 131.61 | 125.12 | mutation (seeded success path) |
| IdentityProviderService | ScimCreateGroup | scim_create_group | scimCreateGroup | mutation | OK | 8.66 | 10.15 | 9.87 | mutation (seeded success path) |
| IdentityProviderService | ScimCreateUser | scim_create_user | scimCreateUser | mutation | OK | 46.70 | 48.33 | 45.84 | mutation (seeded success path) |
| IdentityProviderService | ScimDeleteGroup | scim_delete_group | scimDeleteGroup | mutation | OK | 14.90 | 27.81 | 19.93 | mutation (seeded success path) |
| IdentityProviderService | ScimDeleteUser | scim_delete_user | scimDeleteUser | mutation | OK | 88.20 | 88.20 | 88.20 | mutation (seeded success path) |
| IdentityProviderService | ScimGetGroup | scim_get_group | scimGetGroup | mutation | OK | 17.00 | 20.24 | 20.42 | mutation (seeded success path) |
| IdentityProviderService | ScimGetUser | scim_get_user | scimGetUser | mutation | OK | 12.03 | 13.31 | 12.32 | mutation (seeded success path) |
| IdentityProviderService | ScimListGroups | scim_list_groups | scimListGroups | mutation | OK | 10.40 | 13.48 | 11.63 | mutation (seeded success path) |
| IdentityProviderService | ScimListUsers | scim_list_users | scimListUsers | mutation | OK | 23.01 | 26.28 | 22.89 | mutation (seeded success path) |
| IdentityProviderService | ScimPatchGroup | scim_patch_group | scimPatchGroup | mutation | OK | 19.47 | 21.90 | 20.49 | mutation (seeded success path) |
| IdentityProviderService | ScimPatchUser | scim_patch_user | scimPatchUser | mutation | OK | 51.48 | 225.64 | 128.17 | mutation (seeded success path) |
| IdentityProviderService | ScimReplaceUser | scim_replace_user | scimReplaceUser | mutation | OK | 60.83 | 75.97 | 61.85 | mutation (seeded success path) |
| IdentityProviderService | StartSamlLogin | start_saml_login | startSamlLogin | mutation | OK | 22.51 | 34.65 | 27.46 | mutation (seeded success path) |
| IdentityProviderService | TestProviderDiscovery | test_provider_discovery | testProviderDiscovery | read_only | OK | 9.59 | 14.96 | 10.74 | read_only (seeded success path) |
| IdentityProviderService | UnlinkIdentity | unlink_identity | unlinkIdentity | mutation | OK | 17.35 | 17.51 | 32.96 | mutation (seeded success path) |
| IdentityProviderService | UpdateProvider | update_provider | updateProvider | mutation | OK | 39.23 | 43.55 | 40.86 | mutation (seeded success path) |
| LiveQueryService | Subscribe | subscribe | liveQuerySubscribe | stream_open | OK | 12.18 | 12.18 | 12.18 | streaming: stream-open latency |
| LockService | AcquireLock | acquire_lock | acquireLock | mutation | OK | 67.13 | 76.72 | 70.89 | mutation (seeded success path) |
| LockService | ReleaseLock | release_lock | releaseLock | mutation | OK | 34.28 | 44.28 | 35.62 | mutation (seeded success path) |
| LockService | RenewLock | renew_lock | renewLock | mutation | OK | 140.90 | 160.22 | 140.96 | mutation (seeded success path) |
| MeteringService | CheckQuota | check_quota | checkQuota | read_only | OK | 16.76 | 21.09 | 16.78 | read_only (seeded success path) |
| MeteringService | GetQuota | get_quota | getQuota | read_only | OK | 18.65 | 27.57 | 19.67 | read_only (seeded success path) |
| MeteringService | ListQuotas | list_quotas | listQuotas | read_only | OK | 20.19 | 32.77 | 22.37 | read_only (seeded success path) |
| MeteringService | PutQuota | put_quota | putQuota | mutation | OK | 63.51 | 78.01 | 71.29 | mutation (seeded success path) |
| MeteringService | QueryUsage | query_usage | queryUsage | read_only | OK | 19.57 | 35.87 | 20.90 | read_only (seeded success path) |
| MeteringService | RecordUsage | record_usage | recordUsage | mutation | OK | 19.33 | 33.55 | 24.32 | mutation (seeded success path) |
| NotificationService | GetDeliveryStats | get_delivery_stats | getDeliveryStats | read_only | OK | 21.60 | 31.91 | 20.66 | read_only (seeded success path) |
| NotificationService | GetNotification | get_notification | getNotification | read_only | OK | 18.11 | 26.13 | 19.02 | read_only (seeded success path) |
| NotificationService | GetPreference | get_preference | getPreference | read_only | OK | 18.38 | 43.22 | 22.43 | read_only (seeded success path) |
| NotificationService | GetTemplate | get_template | getTemplate | read_only | OK | 58.54 | 85.30 | 59.58 | read_only (seeded success path) |
| NotificationService | ListNotifications | list_notifications | listNotifications | read_only | OK | 33.06 | 69.80 | 36.90 | read_only (seeded success path) |
| NotificationService | ListPreferences | list_preferences | listPreferences | read_only | OK | 34.12 | 71.51 | 38.80 | read_only (seeded success path) |
| NotificationService | ListTemplates | list_templates | listTemplates | read_only | OK | 26.55 | 36.88 | 27.88 | read_only (seeded success path) |
| NotificationService | ReportDelivery | report_delivery | reportDelivery | mutation | OK | 47.11 | 54.54 | 50.96 | mutation (seeded success path) |
| NotificationService | RetryNotification | retry_notification | retryNotification | mutation | OK | 48.69 | 48.69 | 48.69 | mutation (seeded success path) |
| NotificationService | SendNotification | send_notification | sendNotification | mutation | OK | 87.01 | 87.32 | 85.59 | mutation (seeded success path) |
| NotificationService | SetPreference | set_preference | setPreference | mutation | OK | 24.93 | 25.09 | 26.02 | mutation (seeded success path) |
| NotificationService | UpsertTemplate | upsert_template | upsertTemplate | mutation | OK | 19.54 | 29.01 | 21.10 | mutation (seeded success path) |
| PeerService | GetPeer | get_peer | getPeer | read_only | OK | 18.69 | 32.94 | 20.20 | read_only (seeded success path) |
| PeerService | JoinRoom | join_room | joinRoom | mutation | OK | 65.62 | 129.05 | 88.08 | mutation (seeded success path) |
| PeerService | JoinSession | join_session | joinSession | mutation | OK | 66.30 | 69.12 | 64.18 | mutation (seeded success path) |
| PeerService | LeaveRoom | leave_room | leaveRoom | mutation | OK | 32.73 | 65.30 | 66.83 | mutation (seeded success path) |
| PeerService | ListPeers | list_peers | listPeers | read_only | OK | 21.26 | 36.41 | 23.15 | read_only (seeded success path) |
| RoomService | CloseRoom | close_room | closeRoom | mutation | OK | 89.31 | 129.66 | 134.19 | mutation (seeded success path) |
| RoomService | CreateRoom | create_room | createRoom | mutation | OK | 71.67 | 74.06 | 58.97 | mutation (seeded success path) |
| RoomService | GetRoom | get_room | getRoom | read_only | OK | 18.03 | 21.73 | 18.20 | read_only (seeded success path) |
| RoomService | ListEgress | list_egress | listEgress | read_only | CAPABILITY_SKIPPED | 9.69 | 13.13 | 9.82 | capability skipped: udb udb.core.webrtc.services.v1.RoomService/ListEgress: webrtc_egress_enabled (code=FAILED_PRECONDITION) |
| RoomService | ListRooms | list_rooms | listRooms | read_only | OK | 17.58 | 27.02 | 19.13 | read_only (seeded success path) |
| RoomService | StartRoomComposite | start_room_composite | startRoomComposite | mutation | CAPABILITY_SKIPPED | 10.03 | 13.67 | 13.93 | capability skipped: udb udb.core.webrtc.services.v1.RoomService/StartRoomComposite: webrtc_egress_enabled (code=FAILED_PRECONDITION) |
| RoomService | StartTrackEgress | start_track_egress | startTrackEgress | mutation | CAPABILITY_SKIPPED | 18.47 | 20.34 | 16.40 | capability skipped: udb udb.core.webrtc.services.v1.RoomService/StartTrackEgress: webrtc_egress_enabled (code=FAILED_PRECONDITION) |
| RoomService | StopEgress | stop_egress | stopEgress | mutation | CAPABILITY_SKIPPED | 13.34 | 15.40 | 13.53 | capability skipped: udb udb.core.webrtc.services.v1.RoomService/StopEgress: webrtc_egress_enabled (code=FAILED_PRECONDITION) |
| RoomService | UpdateRoom | update_room | updateRoom | mutation | OK | 13.83 | 16.14 | 16.62 | mutation (seeded success path) |
| SchedulerService | CreateJob | create_job | createJob | mutation | OK | 24.50 | 26.88 | 24.88 | mutation (seeded success path) |
| SchedulerService | DeleteJob | delete_job | deleteJob | destructive | OK | 20.75 | 20.75 | 20.75 | destructive: 1 real call against a seeded disposable target |
| SchedulerService | GetJob | get_job | getJob | read_only | OK | 14.38 | 17.05 | 14.52 | read_only (seeded success path) |
| SchedulerService | ListJobs | list_jobs | listJobs | read_only | OK | 18.70 | 23.73 | 19.56 | read_only (seeded success path) |
| SchedulerService | PauseJob | pause_job | pauseJob | mutation | OK | 23.21 | 23.21 | 23.21 | mutation (seeded success path) |
| SchedulerService | ResumeJob | resume_job | resumeJob | mutation | OK | 55.59 | 55.59 | 55.59 | mutation (seeded success path) |
| SearchService | CreateIndex | create_index | createSearchIndex | mutation | OK | 122.63 | 150.54 | 134.77 | mutation (seeded success path) |
| SearchService | DeleteIndex | delete_index | deleteSearchIndex | destructive | OK | 30.65 | 30.65 | 30.65 | destructive: 1 real call against a seeded disposable target |
| SearchService | ListIndexes | list_indexes | listSearchIndexes | read_only | OK | 19.54 | 30.27 | 20.81 | read_only (seeded success path) |
| SearchService | Reindex | reindex | reindexSearchIndex | mutation | OK | 425.46 | 547.32 | 421.81 | mutation (seeded success path) |
| SearchService | Search | search | search | read_only | OK | 17.69 | 24.11 | 19.13 | read_only (seeded success path) |
| SignalingService | Signal | signal | signal | stream_open | OK | 14.35 | 14.35 | 14.35 | streaming: stream-open latency |
| StorageService | DeleteFile | delete_file | deleteFile | mutation | OK | 321.38 | 321.38 | 321.38 | mutation (seeded success path) |
| StorageService | DownloadFile | download_file | downloadFile | stream | OK | 41.63 | 62.61 | 48.36 | streaming: time-to-first-response (seeded) |
| StorageService | FinalizeUpload | finalize_upload | finalizeUpload | mutation | OK | 620.28 | 620.28 | 620.28 | mutation (seeded success path) |
| StorageService | GetDownloadUrl | get_download_url | getDownloadUrl | read_only | OK | 21.84 | 73.56 | 29.57 | read_only (seeded success path) |
| StorageService | GetFile | get_file | getFile | read_only | OK | 29.29 | 45.40 | 28.95 | read_only (seeded success path) |
| StorageService | ListFiles | list_files | listFiles | read_only | OK | 52.82 | 74.64 | 55.01 | read_only (seeded success path) |
| StorageService | RegisterUpload | register_upload | registerUpload | mutation | OK | 863.46 | 1168.71 | 872.70 | mutation (seeded success path) |
| StorageService | UpdateFile | update_file | updateFile | mutation | OK | 711.30 | 754.64 | 709.72 | mutation (seeded success path) |
| TenantService | CreateTenant | create_tenant | createTenant | mutation | OK | 18.96 | 21.71 | 46.14 | mutation (seeded success path) |
| TenantService | GetTenant | get_tenant | getTenant | read_only | OK | 29.85 | 38.22 | 30.83 | read_only (seeded success path) |
| TenantService | GetTenantConfig | get_tenant_config | getTenantConfig | read_only | OK | 27.44 | 61.19 | 32.00 | read_only (seeded success path) |
| TenantService | ListTenants | list_tenants | listTenants | read_only | OK | 22.35 | 30.42 | 22.09 | read_only (seeded success path) |
| TenantService | PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 301.51 | 301.51 | 301.51 | destructive: 1 real call against a seeded disposable target |
| TenantService | UpdateTenant | update_tenant | updateTenant | mutation | OK | 118.95 | 357.32 | 251.50 | mutation (seeded success path) |
| TenantService | UpdateTenantConfig | update_tenant_config | updateTenantConfig | mutation | OK | 135.30 | 180.74 | 138.11 | mutation (seeded success path) |
| TrackService | ListTracks | list_tracks | listTracks | read_only | OK | 29.91 | 43.07 | 29.79 | read_only (seeded success path) |
| TrackService | MuteTrack | mute_track | muteTrack | mutation | OK | 38.57 | 43.87 | 39.75 | mutation (seeded success path) |
| TrackService | PublishTrack | publish_track | publishTrack | mutation | OK | 48.71 | 98.57 | 64.60 | mutation (seeded success path) |
| TrackService | UnpublishTrack | unpublish_track | unpublishTrack | mutation | OK | 19.79 | 22.70 | 21.30 | mutation (seeded success path) |
| TurnService | IssueCredentials | issue_credentials | issueCredentials | mutation | OK | 20.42 | 27.84 | 20.99 | mutation (seeded success path) |
| VaultService | CreateTransitKey | create_transit_key | createTransitKey | mutation | OK | 52.28 | 52.28 | 52.28 | mutation (seeded success path) |
| VaultService | Decrypt | decrypt | vaultDecrypt | read_only | OK | 50.96 | 74.24 | 51.00 | read_only (seeded success path) |
| VaultService | DeleteSecret | delete_secret | deleteSecret | mutation | OK | 21.77 | 33.39 | 26.40 | mutation (seeded success path) |
| VaultService | DestroySecret | destroy_secret | destroySecret | destructive | OK | 27.66 | 27.66 | 27.66 | destructive: 1 real call against a seeded disposable target |
| VaultService | Encrypt | encrypt | vaultEncrypt | mutation | OK | 15.51 | 16.58 | 18.45 | mutation (seeded success path) |
| VaultService | GenerateDatabaseCredentials | generate_database_credentials | generateDatabaseCredentials | mutation | OK | 40.75 | 44.81 | 39.20 | mutation (seeded success path) |
| VaultService | GetSecret | get_secret | getSecret | read_only | OK | 42.36 | 92.13 | 46.92 | read_only (seeded success path) |
| VaultService | Hmac | hmac | vaultHmac | mutation | OK | 17.25 | 17.41 | 18.38 | mutation (seeded success path) |
| VaultService | ListSecrets | list_secrets | listSecrets | read_only | OK | 30.74 | 58.63 | 36.86 | read_only (seeded success path) |
| VaultService | PutSecret | put_secret | putSecret | mutation | OK | 96.32 | 96.32 | 96.32 | mutation (seeded success path) |
| VaultService | RotateTransitKey | rotate_transit_key | rotateTransitKey | mutation | OK | 107.89 | 117.91 | 110.37 | mutation (seeded success path) |
| VaultService | SealStatus | seal_status | vaultSealStatus | read_only | OK | 5.42 | 7.10 | 5.55 | read_only (seeded success path) |
| VaultService | Sign | sign | vaultSign | mutation | OK | 57.94 | 64.23 | 58.63 | mutation (seeded success path) |
| VaultService | Verify | verify | vaultVerify | read_only | OK | 35.75 | 75.54 | 38.07 | read_only (seeded success path) |
| WebhookService | CreateEndpoint | create_endpoint | createWebhookEndpoint | mutation | OK | 71.61 | 81.63 | 70.73 | mutation (seeded success path) |
| WebhookService | DeleteEndpoint | delete_endpoint | deleteWebhookEndpoint | destructive | OK | 19.97 | 19.97 | 19.97 | destructive: 1 real call against a seeded disposable target |
| WebhookService | GetEndpoint | get_endpoint | getWebhookEndpoint | read_only | OK | 24.78 | 43.73 | 27.32 | read_only (seeded success path) |
| WebhookService | ListDeliveries | list_deliveries | listWebhookDeliveries | read_only | OK | 32.31 | 46.65 | 33.63 | read_only (seeded success path) |
| WebhookService | ListEndpoints | list_endpoints | listWebhookEndpoints | read_only | OK | 38.13 | 54.67 | 37.71 | read_only (seeded success path) |
| WebhookService | UpdateEndpoint | update_endpoint | updateWebhookEndpoint | mutation | OK | 42.22 | 42.92 | 47.43 | mutation (seeded success path) |
| WorkflowService | CancelWorkflow | cancel_workflow | cancelWorkflow | destructive | OK | 25.97 | 25.97 | 25.97 | destructive: 1 real call against a seeded disposable target |
| WorkflowService | GetWorkflow | get_workflow | getWorkflow | read_only | OK | 27.26 | 124.09 | 44.58 | read_only (seeded success path) |
| WorkflowService | ListWorkflows | list_workflows | listWorkflows | read_only | OK | 28.70 | 53.71 | 30.60 | read_only (seeded success path) |
| WorkflowService | SignalWorkflow | signal_workflow | signalWorkflow | mutation | OK | 47.63 | 47.71 | 48.22 | mutation (seeded success path) |
| WorkflowService | StartWorkflow | start_workflow | startWorkflow | mutation | OK | 37.90 | 41.23 | 36.26 | mutation (seeded success path) |
