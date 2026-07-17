# UDB SDK Live Perf — TypeScript (localhost)

RPCs measured: 353   tenant=ce82f123-9f2c-4a3c-b9fa-8fdb46842154

Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities (a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request's reference/ID fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection latency. Any residual non-OK RPC is listed under Failures for the maintainer to finish.

Unary = full request/response round-trip. Non-CDC server-streaming RPCs (kind=stream) report time-to-FIRST-RESPONSE with seeded inputs; client-streaming/bidi RPCs (kind=stream_open) report stream-open latency. CDC subscription (publish_cdc, kind=stream) reports time-to-FIRST-EVENT: the harness subscribes, fires a real seeded Upsert that flows outbox→CDC→Kafka, and times the first delivered event.

RPCs run on the AUTH ROUTE in three phases (BENCH_RPC_BODIES.md "Execution order"): Phase 1 establishes the session (AuthnService login -> refresh_session -> authenticate -> validate_token → introspect_token → get_jwks), then the seed phase; Phase 2 measures everything else; Phase 3 LAST runs the session/credential-teardown AuthnService RPCs (logout, revoke_*, change/reset password, admin_reset_mfa, disable_mfa_factor, …) against the seeded DISPOSABLE user/session so the admin's own session is never killed mid-run. The final terminal destructive tenant purge uses the verified tenant-scoped benchmark credential, matching the other SDK harnesses.

## Seeded fixtures

Captured semantic field → seeded value keys used to resolve request fields: action, admin_reset_mfa_user_id, admin_reset_password_user_id, apply_run_id, approval_token, approve_draft_id, approve_run_id, approved_by, asset_id, assigned_by, auth_challenge_id, backup_id, bucket, canary_id, canary_version_id, cancel_workflow_id, catalog_manifest_b64, challenge_id, change_password_user_id, change_status_user_id, close_room_id, code, collection, content_type, created_by, csrf_token, definition_id, delete_endpoint_id, delete_file_id, delete_job_id, delete_policy_id, delete_role_id, delete_scim_user_id, deleted_by, device_id, disable_mfa_user_id, disable_provider_id, dismiss_dlq_id, dlq_id, document_id, domain, ds_policy_id, egress_id, endpoint_id, event_type, external_identity_id, fencing_token, file_id, file_type, filename, finalize_file_id, gov_exp, instance_id, job_id, join_session_room_id, key_id, key_prefix, kind, leave_peer_id, locale, log_id, mark_saga_id, message_type, migration_id, mongo_collection, name, node_id, notification_id, object, object_key, otp_code, otp_id, owner_id, peer_id, plain_key, policy_draft_id, policy_id, policy_version_id, project, project_id, provider_id, purge_tenant_id, quarantine_dlq_id, recipient_id, record_id, recovery_code, refresh_session_id, refresh_token, reg_challenge_id, reissue_file_id, reject_draft_id, rejected_by, relation, release_fencing_token, renew_fencing_token, replay_dlq_id, reset_otp_code, reset_otp_id, resource, resource_name, restore_tenant_id, retry_saga_id, revoke_device_id, revoke_device_user_id, revoke_key_id, revoke_key_prefix, revoke_recovery_user_id, revoked_by, role, role_code, role_id, rollback_policy_set_id, rollback_resource_version, rollback_target_version_id, room_id, saga_id, saml_provider_id, scim_group_id, scim_user_id, session_id, signal_peer_id, stage_name, step_id, subject, tenant, tenant_code, tenant_id, token, topic_pattern, track_id, ts_table, unpublish_track_id, update_draft_id, update_key_id, update_key_prefix, updated_by, user_id, user_role_id, username, vault_ciphertext, vault_create_key_name, vault_db_role, vault_delete_secret_path, vault_destroy_secret_path, vault_key_name, vault_put_secret_path, vault_secret_path, vault_signature, vault_signing_key_name, workflow_id

## Per-service mean latency

| Service | RPCs | mean ms |
|---|--:|--:|
| BackupService | 8 | 425.58 |
| AuthnService | 50 | 94.36 |
| TenantService | 7 | 43.33 |
| DataBroker | 77 | 36.38 |
| LockService | 5 | 33.90 |
| AuthzService | 41 | 29.99 |
| ApiKeyService | 9 | 28.18 |
| ConfigService | 5 | 27.39 |
| StorageService | 9 | 26.28 |
| ControlPlaneService | 6 | 25.85 |
| VaultService | 20 | 25.60 |
| EmbeddingService | 6 | 25.38 |
| SearchService | 5 | 25.22 |
| IdentityProviderService | 27 | 23.57 |
| AssetService | 8 | 23.50 |
| PeerService | 5 | 23.25 |
| TurnService | 1 | 21.84 |
| WorkflowService | 5 | 21.07 |
| NotificationService | 12 | 20.12 |
| WebhookService | 6 | 19.42 |
| CacheService | 7 | 19.12 |
| SchedulerService | 6 | 19.04 |
| TrackService | 4 | 18.50 |
| MeteringService | 6 | 17.58 |
| RoomService | 9 | 17.45 |
| AnalyticsService | 7 | 16.52 |
| LiveQueryService | 1 | 11.02 |
| SignalingService | 1 | 9.39 |

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
| BackupService/RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 1697.32 | 1697.32 | 1697.32 | destructive: 1 real call against a seeded disposable target |
| BackupService/StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 1583.45 | 1644.02 | 1588.34 | mutation (seeded success path) |
| AuthnService/ChangePassword | change_password | changePassword | mutation | OK | 1633.76 | 1633.76 | 1633.76 | mutation (seeded success path) |
| DataBroker/StageCatalog | stage_catalog | stageCatalog | destructive | OK | 923.51 | 923.51 | 923.51 | destructive: 1 real call against a seeded disposable target |
| AuthnService/Login | login | login | mutation | OK | 820.85 | 821.87 | 785.00 | mutation (seeded success path) |
| AuthnService/CreateUser | create_user | createUser | mutation | OK | 719.63 | 761.56 | 711.91 | mutation (seeded success path) |
| AuthnService/ResetPassword | reset_password | resetPassword | mutation | OK | 668.87 | 668.87 | 668.87 | mutation (seeded success path) |
| DataBroker/PublishCDC | publish_cdc | publishCdc | stream | OK | 245.29 | 245.29 | 249.86 | cdc: time-to-first-event (real seeded Upsert produced) |
| DataBroker/ApplyMigration | apply_migration | applyMigration | mutation | OK | 215.28 | 215.28 | 215.28 | mutation (seeded success path) |
| TenantService/PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 189.80 | 189.80 | 189.80 | destructive: 1 real call against a seeded disposable target |
| IdentityProviderService/SamlAcs | saml_acs | samlAcs | mutation | OK | 113.54 | 126.41 | 115.68 | mutation (seeded success path) |
| DataBroker/ValidateCatalog | validate_catalog | validateCatalog | destructive | OK | 121.13 | 121.13 | 121.13 | destructive: 1 real call against a seeded disposable target |
| AuthzService/RollbackPolicyVersion | rollback_policy_version | rollbackPolicyVersion | destructive | OK | 115.68 | 115.68 | 115.68 | destructive: 1 real call against a seeded disposable target |
| ApiKeyService/EmergencyRevokeApiKeys | emergency_revoke_api_keys | emergencyRevokeApiKeys | destructive | OK | 112.14 | 112.14 | 112.14 | destructive: 1 real call against a seeded disposable target |
| AuthzService/PromoteCanary | promote_canary | promoteCanary | destructive | OK | 98.96 | 98.96 | 98.96 | destructive: 1 real call against a seeded disposable target |
| ControlPlaneService/ListNodeStates | list_node_states | listNodeStates | read_only | OK | 55.79 | 84.71 | 59.08 | read_only (seeded success path) |
| AuthzService/ActivatePolicyVersion | activate_policy_version | activatePolicyVersion | destructive | OK | 79.54 | 79.54 | 79.54 | destructive: 1 real call against a seeded disposable target |
| ControlPlaneService/RollbackResources | rollback_resources | rollbackResources | mutation | OK | 75.02 | 78.25 | 71.72 | mutation (seeded success path) |
| DataBroker/TimeSeriesWrite | time_series_write | timeSeriesWrite | mutation | OK | 70.71 | 77.25 | 71.22 | mutation (seeded success path) |
| AuthnService/FinishWebAuthnAuthentication | finish_web_authn_authentication | finishWebAuthnAuthentication | mutation | OK | 75.10 | 75.10 | 75.10 | mutation (seeded success path) |

## Full per-RPC table (sorted by service, then RPC)

| Service | RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms | note |
|---|---|---|---|---|---|--:|--:|--:|---|
| AnalyticsService | GetExecutorPerformance | get_executor_performance | getExecutorPerformance | read_only | OK | 11.59 | 15.86 | 11.86 | read_only (seeded success path) |
| AnalyticsService | GetPipelineSummary | get_pipeline_summary | getPipelineSummary | read_only | OK | 11.72 | 15.36 | 11.95 | read_only (seeded success path) |
| AnalyticsService | GetReconciliationAnalytics | get_reconciliation_analytics | getReconciliationAnalytics | read_only | OK | 12.37 | 17.32 | 12.73 | read_only (seeded success path) |
| AnalyticsService | GetSlaCompliance | get_sla_compliance | getSlaCompliance | read_only | OK | 14.65 | 27.90 | 17.34 | read_only (seeded success path) |
| AnalyticsService | GetThroughput | get_throughput | getThroughput | read_only | OK | 12.95 | 28.67 | 14.16 | read_only (seeded success path) |
| AnalyticsService | RecordPipelineMetric | record_pipeline_metric | recordPipelineMetric | mutation | OK | 19.19 | 21.61 | 20.32 | mutation (seeded success path) |
| AnalyticsService | TriggerSnapshot | trigger_snapshot | triggerSnapshot | mutation | OK | 23.61 | 33.82 | 27.26 | mutation (seeded success path) |
| ApiKeyService | CreateApiKey | create_api_key | createApiKey | mutation | OK | 19.28 | 19.49 | 20.59 | mutation (seeded success path) |
| ApiKeyService | EmergencyRevokeApiKeys | emergency_revoke_api_keys | emergencyRevokeApiKeys | destructive | OK | 112.14 | 112.14 | 112.14 | destructive: 1 real call against a seeded disposable target |
| ApiKeyService | GetApiKey | get_api_key | getApiKey | read_only | OK | 8.58 | 11.35 | 8.55 | read_only (seeded success path) |
| ApiKeyService | GetApiKeyUsageStats | get_api_key_usage_stats | getApiKeyUsageStats | read_only | OK | 9.44 | 13.80 | 10.07 | read_only (seeded success path) |
| ApiKeyService | ListApiKeys | list_api_keys | listApiKeys | read_only | OK | 10.28 | 13.84 | 10.24 | read_only (seeded success path) |
| ApiKeyService | RevokeApiKey | revoke_api_key | revokeApiKey | mutation | OK | 21.40 | 21.40 | 21.40 | mutation (seeded success path) |
| ApiKeyService | RotateApiKey | rotate_api_key | rotateApiKey | mutation | OK | 30.95 | 30.95 | 30.95 | mutation (seeded success path) |
| ApiKeyService | UpdateApiKey | update_api_key | updateApiKey | mutation | OK | 26.18 | 29.96 | 26.88 | mutation (seeded success path) |
| ApiKeyService | ValidateApiKey | validate_api_key | validateApiKey | read_only | OK | 11.85 | 16.52 | 12.78 | read_only (seeded success path) |
| AssetService | CompleteStep | complete_step | completeStep | mutation | OK | 40.23 | 40.86 | 40.01 | mutation (seeded success path) |
| AssetService | CreatePipelineDefinition | create_pipeline_definition | createPipelineDefinition | mutation | OK | 16.76 | 18.12 | 17.55 | mutation (seeded success path) |
| AssetService | GetAsset | get_asset | getAsset | read_only | OK | 17.19 | 29.08 | 19.53 | read_only (seeded success path) |
| AssetService | GetPipeline | get_pipeline | getPipeline | read_only | OK | 16.77 | 31.16 | 18.16 | read_only (seeded success path) |
| AssetService | GetPipelineDefinition | get_pipeline_definition | getPipelineDefinition | read_only | OK | 18.66 | 30.11 | 20.06 | read_only (seeded success path) |
| AssetService | ListAssets | list_assets | listAssets | read_only | OK | 17.49 | 23.58 | 17.73 | read_only (seeded success path) |
| AssetService | RegisterAsset | register_asset | registerAsset | mutation | OK | 27.80 | 32.57 | 28.90 | mutation (seeded success path) |
| AssetService | StartPipeline | start_pipeline | startPipeline | mutation | OK | 16.35 | 19.53 | 26.06 | mutation (seeded success path) |
| AuthnService | AdminResetMfa | admin_reset_mfa | adminResetMfa | destructive | OK | 30.11 | 30.11 | 30.11 | destructive: 1 real call against a seeded disposable target |
| AuthnService | AdminResetPassword | admin_reset_password | adminResetPassword | destructive | OK | 13.74 | 13.74 | 13.74 | destructive: 1 real call against a seeded disposable target |
| AuthnService | AdminRevokeAllTenantSessions | admin_revoke_all_tenant_sessions | adminRevokeAllTenantSessions | destructive | OK | 16.77 | 16.77 | 16.77 | destructive: 1 real call against a seeded disposable target |
| AuthnService | AdminRevokeAllUserSessions | admin_revoke_all_user_sessions | adminRevokeAllUserSessions | destructive | OK | 18.02 | 18.02 | 18.02 | destructive: 1 real call against a seeded disposable target |
| AuthnService | AdminRevokeSession | admin_revoke_session | adminRevokeSession | destructive | OK | 15.20 | 15.20 | 15.20 | destructive: 1 real call against a seeded disposable target |
| AuthnService | Authenticate | authenticate | authenticate | read_only | OK | 31.79 | 58.53 | 36.17 | read_only (seeded success path) |
| AuthnService | ChangePassword | change_password | changePassword | mutation | OK | 1633.76 | 1633.76 | 1633.76 | mutation (seeded success path) |
| AuthnService | ChangeUserStatus | change_user_status | changeUserStatus | destructive | OK | 22.73 | 22.73 | 22.73 | destructive: 1 real call against a seeded disposable target |
| AuthnService | ConfirmMFAEnrollment | confirm_mfaenrollment | confirmMfaenrollment | mutation | OK | 10.03 | 10.32 | 10.24 | mutation (seeded success path) |
| AuthnService | CreateSession | create_session | createSession | mutation | OK | 14.99 | 15.26 | 13.96 | mutation (seeded success path) |
| AuthnService | CreateUser | create_user | createUser | mutation | OK | 719.63 | 761.56 | 711.91 | mutation (seeded success path) |
| AuthnService | DeleteWebAuthnCredential | delete_web_authn_credential | deleteWebAuthnCredential | mutation | OK | 14.79 | 15.74 | 14.73 | mutation (seeded success path) |
| AuthnService | DisableMfaFactor | disable_mfa_factor | disableMfaFactor | mutation | OK | 19.47 | 20.43 | 18.43 | mutation (seeded success path) |
| AuthnService | EmergencyRevoke | emergency_revoke | emergencyRevoke | destructive | OK | 21.27 | 21.27 | 21.27 | destructive: 1 real call against a seeded disposable target |
| AuthnService | EnrollMFA | enroll_mfa | enrollMfa | mutation | OK | 19.33 | 21.61 | 19.80 | mutation (seeded success path) |
| AuthnService | FinishWebAuthnAuthentication | finish_web_authn_authentication | finishWebAuthnAuthentication | mutation | OK | 75.10 | 75.10 | 75.10 | mutation (seeded success path) |
| AuthnService | FinishWebAuthnRegistration | finish_web_authn_registration | finishWebAuthnRegistration | mutation | OK | 49.16 | 49.16 | 49.16 | mutation (seeded success path) |
| AuthnService | ForgotPassword | forgot_password | forgotPassword | mutation | OK | 11.07 | 11.80 | 11.79 | mutation (seeded success path) |
| AuthnService | GenerateRecoveryCodes | generate_recovery_codes | generateRecoveryCodes | mutation | OK | 43.91 | 44.53 | 44.58 | mutation (seeded success path) |
| AuthnService | GetJwks | get_jwks | getJwks | read_only | OK | 8.24 | 12.73 | 8.74 | read_only (seeded success path) |
| AuthnService | GetMfaPolicy | get_mfa_policy | getMfaPolicy | read_only | OK | 8.36 | 12.70 | 8.65 | read_only (seeded success path) |
| AuthnService | GetSession | get_session | getSession | read_only | OK | 7.55 | 10.08 | 7.74 | read_only (seeded success path) |
| AuthnService | GetUser | get_user | getUser | read_only | OK | 7.73 | 9.55 | 7.93 | read_only (seeded success path) |
| AuthnService | IntrospectToken | introspect_token | introspectToken | read_only | OK | 38.25 | 57.41 | 40.81 | read_only (seeded success path) |
| AuthnService | IssueMfaChallenge | issue_mfa_challenge | issueMfaChallenge | mutation | OK | 17.02 | 17.68 | 16.97 | mutation (seeded success path) |
| AuthnService | ListDevices | list_devices | listDevices | read_only | OK | 9.27 | 15.41 | 10.15 | read_only (seeded success path) |
| AuthnService | ListMfaFactors | list_mfa_factors | listMfaFactors | read_only | OK | 13.30 | 18.14 | 13.83 | read_only (seeded success path) |
| AuthnService | ListSessions | list_sessions | listSessions | read_only | OK | 15.20 | 32.40 | 17.27 | read_only (seeded success path) |
| AuthnService | ListUsers | list_users | listUsers | read_only | OK | 16.04 | 18.97 | 15.71 | read_only (seeded success path) |
| AuthnService | ListWebAuthnCredentials | list_web_authn_credentials | listWebAuthnCredentials | read_only | OK | 8.45 | 13.63 | 8.94 | read_only (seeded success path) |
| AuthnService | Login | login | login | mutation | OK | 820.85 | 821.87 | 785.00 | mutation (seeded success path) |
| AuthnService | Logout | logout | logout | mutation | OK | 13.38 | 14.63 | 35.10 | mutation (seeded success path) |
| AuthnService | PutMfaPolicy | put_mfa_policy | putMfaPolicy | mutation | OK | 8.04 | 12.44 | 10.45 | mutation (seeded success path) |
| AuthnService | RefreshSession | refresh_session | refreshSession | mutation | OK | 19.44 | 21.50 | 19.60 | mutation (seeded success path) |
| AuthnService | RefreshToken | refresh_token | refreshToken | mutation | OK | 18.76 | 18.76 | 18.76 | mutation (seeded success path) |
| AuthnService | RenamePasskey | rename_passkey | renamePasskey | mutation | OK | 13.85 | 14.40 | 15.34 | mutation (seeded success path) |
| AuthnService | ResendOTP | resend_otp | resendOtp | mutation | OK | 23.44 | 23.61 | 22.62 | mutation (seeded success path) |
| AuthnService | ResetPassword | reset_password | resetPassword | mutation | OK | 668.87 | 668.87 | 668.87 | mutation (seeded success path) |
| AuthnService | RevokeDevice | revoke_device | revokeDevice | mutation | OK | 17.69 | 17.69 | 17.69 | mutation (seeded success path) |
| AuthnService | RevokeRecoveryCodes | revoke_recovery_codes | revokeRecoveryCodes | mutation | OK | 17.39 | 18.29 | 17.20 | mutation (seeded success path) |
| AuthnService | RevokeSession | revoke_session | revokeSession | mutation | OK | 8.67 | 9.49 | 8.74 | mutation (seeded success path) |
| AuthnService | SendOTP | send_otp | sendOtp | mutation | OK | 17.95 | 19.21 | 18.83 | mutation (seeded success path) |
| AuthnService | SendPhoneVerification | send_phone_verification | sendPhoneVerification | mutation | OK | 16.36 | 25.04 | 20.96 | mutation (seeded success path) |
| AuthnService | StartWebAuthnAuthentication | start_web_authn_authentication | startWebAuthnAuthentication | mutation | OK | 19.81 | 22.31 | 20.70 | mutation (seeded success path) |
| AuthnService | StartWebAuthnRegistration | start_web_authn_registration | startWebAuthnRegistration | mutation | OK | 19.53 | 20.57 | 19.65 | mutation (seeded success path) |
| AuthnService | UpdateUser | update_user | updateUser | mutation | OK | 12.72 | 13.05 | 12.82 | mutation (seeded success path) |
| AuthnService | ValidateCSRF | validate_csrf | validateCsrf | read_only | OK | 8.01 | 10.48 | 8.05 | read_only (seeded success path) |
| AuthnService | ValidateToken | validate_token | validateToken | read_only | OK | 27.49 | 37.50 | 28.68 | read_only (seeded success path) |
| AuthnService | VerifyMfaChallenge | verify_mfa_challenge | verifyMfaChallenge | read_only | OK | 12.43 | 16.27 | 12.73 | read_only (seeded success path) |
| AuthnService | VerifyOTP | verify_otp | verifyOtp | read_only | OK | 21.23 | 32.39 | 21.85 | read_only (seeded success path) |
| AuthzService | ActivateCanary | activate_canary | activateCanary | destructive | OK | 46.52 | 46.52 | 46.52 | destructive: 1 real call against a seeded disposable target |
| AuthzService | ActivatePolicyVersion | activate_policy_version | activatePolicyVersion | destructive | OK | 79.54 | 79.54 | 79.54 | destructive: 1 real call against a seeded disposable target |
| AuthzService | ApprovePolicyDraft | approve_policy_draft | approvePolicyDraft | mutation | OK | 56.85 | 56.85 | 56.85 | mutation (seeded success path) |
| AuthzService | AssignRole | assign_role | assignRole | mutation | OK | 28.60 | 41.63 | 40.69 | mutation (seeded success path) |
| AuthzService | Authorize | authorize | authorize | read_only | OK | 37.63 | 55.23 | 39.71 | read_only (seeded success path) |
| AuthzService | BatchCheckPermissions | batch_check_permissions | batchCheckPermissions | read_only | OK | 14.68 | 38.42 | 16.68 | read_only (seeded success path) |
| AuthzService | CheckAccess | check_access | checkAccess | read_only | OK | 13.15 | 15.21 | 13.44 | read_only (seeded success path) |
| AuthzService | CreatePolicyDraft | create_policy_draft | createPolicyDraft | mutation | OK | 45.49 | 45.99 | 43.86 | mutation (seeded success path) |
| AuthzService | CreatePolicyRule | create_policy_rule | createPolicyRule | mutation | OK | 24.10 | 25.52 | 25.66 | mutation (seeded success path) |
| AuthzService | CreateRole | create_role | createRole | mutation | OK | 33.17 | 33.17 | 33.17 | mutation (seeded success path) |
| AuthzService | DeletePolicyRule | delete_policy_rule | deletePolicyRule | mutation | OK | 13.28 | 19.49 | 16.80 | mutation (seeded success path) |
| AuthzService | DeleteRole | delete_role | deleteRole | mutation | OK | 15.88 | 18.95 | 21.16 | mutation (seeded success path) |
| AuthzService | DiffPolicyDraft | diff_policy_draft | diffPolicyDraft | read_only | OK | 17.52 | 22.29 | 17.35 | read_only (seeded success path) |
| AuthzService | ExplainPolicy | explain_policy | explainPolicy | read_only | OK | 11.02 | 12.93 | 11.17 | read_only (seeded success path) |
| AuthzService | GetAuthzRevision | get_authz_revision | getAuthzRevision | read_only | OK | 7.61 | 11.42 | 7.96 | read_only (seeded success path) |
| AuthzService | GetCanaryStatus | get_canary_status | getCanaryStatus | read_only | OK | 14.47 | 24.18 | 15.38 | read_only (seeded success path) |
| AuthzService | GetNativeAccess | get_native_access | getNativeAccess | read_only | OK | 29.33 | 35.57 | 29.79 | read_only (seeded success path) |
| AuthzService | GetPolicyBundle | get_policy_bundle | getPolicyBundle | read_only | OK | 11.80 | 18.37 | 12.73 | read_only (seeded success path) |
| AuthzService | GetPolicyRule | get_policy_rule | getPolicyRule | read_only | OK | 10.00 | 12.95 | 10.00 | read_only (seeded success path) |
| AuthzService | GetRole | get_role | getRole | read_only | OK | 8.35 | 12.49 | 8.78 | read_only (seeded success path) |
| AuthzService | InvalidatePolicyBundles | invalidate_policy_bundles | invalidatePolicyBundles | destructive | OK | 35.08 | 35.08 | 35.08 | destructive: 1 real call against a seeded disposable target |
| AuthzService | LintAuthzPolicies | lint_authz_policies | lintAuthzPolicies | read_only | OK | 4.63 | 6.35 | 4.84 | read_only (seeded success path) |
| AuthzService | ListAccessDecisionAudits | list_access_decision_audits | listAccessDecisionAudits | read_only | OK | 18.66 | 25.46 | 18.79 | read_only (seeded success path) |
| AuthzService | ListPolicyRules | list_policy_rules | listPolicyRules | read_only | OK | 8.52 | 11.10 | 8.57 | read_only (seeded success path) |
| AuthzService | ListPolicyVersions | list_policy_versions | listPolicyVersions | read_only | OK | 16.51 | 30.84 | 17.41 | read_only (seeded success path) |
| AuthzService | ListRoles | list_roles | listRoles | read_only | OK | 9.47 | 13.18 | 9.78 | read_only (seeded success path) |
| AuthzService | ListUserPermissions | list_user_permissions | listUserPermissions | read_only | OK | 4.80 | 5.53 | 4.68 | read_only (seeded success path) |
| AuthzService | ListUserRoles | list_user_roles | listUserRoles | read_only | OK | 8.87 | 11.08 | 8.71 | read_only (seeded success path) |
| AuthzService | MigrateLegacyPolicies | migrate_legacy_policies | migrateLegacyPolicies | destructive | OK | 53.18 | 53.18 | 53.18 | destructive: 1 real call against a seeded disposable target |
| AuthzService | PromoteCanary | promote_canary | promoteCanary | destructive | OK | 98.96 | 98.96 | 98.96 | destructive: 1 real call against a seeded disposable target |
| AuthzService | PutAuthzPolicy | put_authz_policy | putAuthzPolicy | mutation | OK | 25.72 | 32.52 | 28.67 | mutation (seeded success path) |
| AuthzService | PutRelationship | put_relationship | putRelationship | mutation | OK | 30.45 | 31.22 | 30.68 | mutation (seeded success path) |
| AuthzService | PutRoleBinding | put_role_binding | putRoleBinding | mutation | OK | 20.76 | 20.86 | 21.03 | mutation (seeded success path) |
| AuthzService | RejectPolicyDraft | reject_policy_draft | rejectPolicyDraft | mutation | OK | 34.07 | 34.07 | 34.07 | mutation (seeded success path) |
| AuthzService | RevokeRole | revoke_role | revokeRole | mutation | OK | 10.39 | 11.54 | 13.21 | mutation (seeded success path) |
| AuthzService | RollbackPolicyVersion | rollback_policy_version | rollbackPolicyVersion | destructive | OK | 115.68 | 115.68 | 115.68 | destructive: 1 real call against a seeded disposable target |
| AuthzService | SeedBuiltinRoles | seed_builtin_roles | seedBuiltinRoles | mutation | OK | 56.05 | 59.19 | 55.40 | mutation (seeded success path) |
| AuthzService | SimulatePolicy | simulate_policy | simulatePolicy | mutation | OK | 23.63 | 26.49 | 25.51 | mutation (seeded success path) |
| AuthzService | SubmitPolicyDraft | submit_policy_draft | submitPolicyDraft | mutation | OK | 29.71 | 29.71 | 29.71 | mutation (seeded success path) |
| AuthzService | UpdatePolicyDraft | update_policy_draft | updatePolicyDraft | mutation | OK | 31.84 | 33.88 | 44.53 | mutation (seeded success path) |
| AuthzService | UpdateRole | update_role | updateRole | mutation | OK | 23.51 | 24.13 | 23.97 | mutation (seeded success path) |
| BackupService | DeleteBackupPolicy | delete_backup_policy | deleteBackupPolicy | mutation | OK | 20.36 | 21.26 | 19.18 | mutation (seeded success path) |
| BackupService | GetBackup | get_backup | getBackup | read_only | OK | 26.11 | 35.95 | 26.58 | read_only (seeded success path) |
| BackupService | GetBackupPolicy | get_backup_policy | getBackupPolicy | read_only | OK | 14.71 | 21.51 | 15.77 | read_only (seeded success path) |
| BackupService | ListBackupPolicies | list_backup_policies | listBackupPolicies | read_only | OK | 14.85 | 29.81 | 16.73 | read_only (seeded success path) |
| BackupService | ListBackups | list_backups | listBackups | read_only | OK | 15.07 | 21.49 | 15.80 | read_only (seeded success path) |
| BackupService | PutBackupPolicy | put_backup_policy | putBackupPolicy | mutation | OK | 24.22 | 25.60 | 24.91 | mutation (seeded success path) |
| BackupService | RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 1697.32 | 1697.32 | 1697.32 | destructive: 1 real call against a seeded disposable target |
| BackupService | StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 1583.45 | 1644.02 | 1588.34 | mutation (seeded success path) |
| CacheService | CacheDelete | cache_delete | cacheNamespaceDelete | mutation | OK | 13.38 | 14.66 | 15.07 | mutation (seeded success path) |
| CacheService | CacheGet | cache_get | cacheNamespaceGet | read_only | OK | 12.10 | 15.73 | 12.54 | read_only (seeded success path) |
| CacheService | CacheScan | cache_scan | cacheNamespaceScan | read_only | OK | 11.85 | 14.75 | 12.04 | read_only (seeded success path) |
| CacheService | CacheSet | cache_set | cacheNamespaceSet | mutation | OK | 19.90 | 21.04 | 19.93 | mutation (seeded success path) |
| CacheService | CreateCacheNamespace | create_cache_namespace | createCacheNamespace | mutation | OK | 15.67 | 16.94 | 16.11 | mutation (seeded success path) |
| CacheService | DeleteCacheNamespace | delete_cache_namespace | deleteCacheNamespace | destructive | OK | 33.91 | 33.91 | 33.91 | destructive: 1 real call against a seeded disposable target |
| CacheService | GetCacheNamespaceStats | get_cache_namespace_stats | getCacheNamespaceStats | read_only | OK | 24.14 | 30.25 | 24.24 | read_only (seeded success path) |
| ConfigService | DeleteFlag | delete_flag | deleteFlag | destructive | OK | 35.58 | 35.58 | 35.58 | destructive: 1 real call against a seeded disposable target |
| ConfigService | EvaluateFlags | evaluate_flags | evaluateFlags | read_only | OK | 16.95 | 34.06 | 19.45 | read_only (seeded success path) |
| ConfigService | GetFlag | get_flag | getFlag | read_only | OK | 15.73 | 19.63 | 15.70 | read_only (seeded success path) |
| ConfigService | ListFlags | list_flags | listFlags | read_only | OK | 15.03 | 18.16 | 15.09 | read_only (seeded success path) |
| ConfigService | PutFlag | put_flag | putFlag | mutation | OK | 41.48 | 43.67 | 51.12 | mutation (seeded success path) |
| ControlPlaneService | AckStatus | ack_status | ackStatus | mutation | OK | 13.48 | 14.10 | 12.99 | mutation (seeded success path) |
| ControlPlaneService | DeltaResources | delta_resources | deltaResources | stream_open | OK | 1.88 | 1.88 | 1.88 | streaming: stream-open latency |
| ControlPlaneService | GetResources | get_resources | getResources | read_only | OK | 8.80 | 10.94 | 8.86 | read_only (seeded success path) |
| ControlPlaneService | ListNodeStates | list_node_states | listNodeStates | read_only | OK | 55.79 | 84.71 | 59.08 | read_only (seeded success path) |
| ControlPlaneService | RollbackResources | rollback_resources | rollbackResources | mutation | OK | 75.02 | 78.25 | 71.72 | mutation (seeded success path) |
| ControlPlaneService | StreamResources | stream_resources | streamResources | stream_open | OK | 0.58 | 0.58 | 0.58 | streaming: stream-open latency |
| DataBroker | ActivateCatalog | activate_catalog | activateCatalog | destructive | OK | 17.83 | 17.83 | 17.83 | destructive: 1 real call against a seeded disposable target |
| DataBroker | AnalyticalQuery | analytical_query | analyticalQuery | read_only | OK | 13.73 | 17.36 | 14.19 | read_only (seeded success path) |
| DataBroker | ApplyMigration | apply_migration | applyMigration | mutation | OK | 215.28 | 215.28 | 215.28 | mutation (seeded success path) |
| DataBroker | ApproveMigrationPlan | approve_migration_plan | approveMigrationPlan | mutation | OK | 27.04 | 27.04 | 27.04 | mutation (seeded success path) |
| DataBroker | BatchSelect | batch_select | batchSelect | stream_open | OK | 0.49 | 0.49 | 0.49 | streaming: stream-open latency |
| DataBroker | BatchUpsert | batch_upsert | batchUpsert | stream_open | OK | 0.42 | 0.42 | 0.42 | streaming: stream-open latency |
| DataBroker | BeginTx | begin_tx | beginTx | stream_open | OK | 0.37 | 0.37 | 0.37 | streaming: stream-open latency |
| DataBroker | CacheDelete | cache_delete | cacheDelete | mutation | OK | 10.39 | 11.48 | 10.36 | mutation (seeded success path) |
| DataBroker | CacheGet | cache_get | cacheGet | read_only | OK | 13.15 | 23.59 | 14.14 | read_only (seeded success path) |
| DataBroker | CacheScan | cache_scan | cacheScan | read_only | OK | 17.33 | 22.97 | 17.65 | read_only (seeded success path) |
| DataBroker | CacheSet | cache_set | cacheSet | mutation | OK | 17.00 | 21.58 | 19.59 | mutation (seeded success path) |
| DataBroker | CreateMaterializedView | create_materialized_view | createMaterializedView | mutation | OK | 10.91 | 11.23 | 10.29 | mutation (seeded success path) |
| DataBroker | Delete | delete | delete | mutation | OK | 34.98 | 35.64 | 35.14 | mutation (seeded success path) |
| DataBroker | DeletePolicy | delete_policy | deletePolicy | mutation | OK | 24.74 | 24.74 | 24.74 | mutation (seeded success path) |
| DataBroker | DismissDlqEvent | dismiss_dlq_event | dismissDlqEvent | mutation | OK | 26.74 | 28.17 | 26.11 | mutation (seeded success path) |
| DataBroker | DocumentDelete | document_delete | documentDelete | mutation | OK | 8.33 | 10.38 | 8.93 | mutation (seeded success path) |
| DataBroker | DocumentFind | document_find | documentFind | read_only | OK | 11.07 | 17.61 | 12.03 | read_only (seeded success path) |
| DataBroker | DocumentGet | document_get | documentGet | read_only | OK | 9.62 | 12.18 | 9.96 | read_only (seeded success path) |
| DataBroker | DocumentUpsert | document_upsert | documentUpsert | mutation | OK | 18.18 | 24.27 | 19.13 | mutation (seeded success path) |
| DataBroker | DropResource | drop_resource | dropResource | destructive | OK | 36.89 | 36.89 | 36.89 | destructive: 1 real call against a seeded disposable target |
| DataBroker | EnqueueOutboxEvent | enqueue_outbox_event | enqueueOutboxEvent | mutation | OK | 23.66 | 23.66 | 23.66 | mutation (seeded success path) |
| DataBroker | EnsureBaseline | ensure_baseline | ensureBaseline | mutation | OK | 20.93 | 21.17 | 21.53 | mutation (seeded success path) |
| DataBroker | EnsureProject | ensure_project | ensureProject | mutation | OK | 16.78 | 17.28 | 16.43 | mutation (seeded success path) |
| DataBroker | EnsureResource | ensure_resource | ensureResource | mutation | OK | 20.27 | 26.70 | 33.03 | mutation (seeded success path) |
| DataBroker | GeneratePresignedUrl | generate_presigned_url | generatePresignedUrl | mutation | OK | 8.31 | 8.81 | 8.26 | mutation (seeded success path) |
| DataBroker | GenericDispatch | generic_dispatch | genericDispatch | mutation | OK | 7.87 | 9.22 | 8.19 | mutation (seeded success path) |
| DataBroker | GetAdminSummary | get_admin_summary | getAdminSummary | read_only | OK | 42.89 | 67.41 | 44.17 | read_only (seeded success path) |
| DataBroker | GetCapabilities | get_capabilities | getCapabilities | read_only | OK | 13.02 | 20.07 | 13.96 | read_only (seeded success path) |
| DataBroker | GetCatalogManifest | get_catalog_manifest | getCatalogManifest | read_only | OK | 29.93 | 54.60 | 33.17 | read_only (seeded success path) |
| DataBroker | GetCatalogVersion | get_catalog_version | getCatalogVersion | read_only | OK | 9.54 | 13.48 | 10.00 | read_only (seeded success path) |
| DataBroker | GetCatalogVersions | get_catalog_versions | getCatalogVersions | read_only | OK | 9.29 | 20.06 | 10.58 | read_only (seeded success path) |
| DataBroker | GetCdcStatus | get_cdc_status | getCdcStatus | read_only | OK | 11.46 | 17.35 | 12.06 | read_only (seeded success path) |
| DataBroker | GetDlqEvent | get_dlq_event | getDlqEvent | read_only | OK | 13.06 | 16.05 | 12.97 | read_only (seeded success path) |
| DataBroker | GetHealthReport | get_health_report | getHealthReport | read_only | OK | 6.67 | 10.46 | 7.54 | read_only (seeded success path) |
| DataBroker | GetMigrationStatus | get_migration_status | getMigrationStatus | read_only | OK | 10.32 | 13.63 | 10.92 | read_only (seeded success path) |
| DataBroker | GetObject | get_object | getObject | stream | OK | 12.82 | 13.04 | 13.37 | streaming: time-to-first-response (seeded) |
| DataBroker | GetSaga | get_saga | getSaga | read_only | OK | 11.04 | 14.80 | 11.36 | read_only (seeded success path) |
| DataBroker | GraphMutate | graph_mutate | graphMutate | mutation | OK | 19.99 | 28.00 | 35.42 | mutation (seeded success path) |
| DataBroker | GraphQuery | graph_query | graphQuery | read_only | OK | 22.76 | 28.01 | 23.86 | read_only (seeded success path) |
| DataBroker | InitiateMultipartUpload | initiate_multipart_upload | initiateMultipartUpload | mutation | OK | 18.19 | 19.12 | 17.49 | mutation (seeded success path) |
| DataBroker | LintPolicies | lint_policies | lintPolicies | read_only | OK | 11.26 | 14.25 | 11.31 | read_only (seeded success path) |
| DataBroker | ListAdminAuditLogs | list_admin_audit_logs | listAdminAuditLogs | read_only | OK | 15.02 | 21.16 | 15.19 | read_only (seeded success path) |
| DataBroker | ListDlqEvents | list_dlq_events | listDlqEvents | read_only | OK | 11.32 | 15.52 | 11.43 | read_only (seeded success path) |
| DataBroker | ListMessageSchemas | list_message_schemas | listMessageSchemas | read_only | OK | 6.00 | 8.91 | 6.56 | read_only (seeded success path) |
| DataBroker | ListMigrationRuns | list_migration_runs | listMigrationRuns | read_only | OK | 10.11 | 14.53 | 10.36 | read_only (seeded success path) |
| DataBroker | ListPolicies | list_policies | listPolicies | read_only | OK | 8.95 | 11.50 | 9.23 | read_only (seeded success path) |
| DataBroker | ListProjects | list_projects | listProjects | read_only | OK | 10.48 | 13.66 | 10.78 | read_only (seeded success path) |
| DataBroker | ListResources | list_resources | listResources | read_only | OK | 8.13 | 10.30 | 8.43 | read_only (seeded success path) |
| DataBroker | ListSagas | list_sagas | listSagas | read_only | OK | 10.37 | 13.04 | 10.29 | read_only (seeded success path) |
| DataBroker | LookupMessageSchema | lookup_message_schema | lookupMessageSchema | read_only | OK | 6.54 | 7.94 | 6.49 | read_only (seeded success path) |
| DataBroker | MarkSagaReviewed | mark_saga_reviewed | markSagaReviewed | mutation | OK | 22.22 | 22.32 | 21.70 | mutation (seeded success path) |
| DataBroker | PauseCdc | pause_cdc | pauseCdc | mutation | OK | 17.39 | 18.22 | 17.59 | mutation (seeded success path) |
| DataBroker | PlanMigration | plan_migration | planMigration | mutation | OK | 26.85 | 29.53 | 25.18 | mutation (seeded success path) |
| DataBroker | PreviewCdcRedaction | preview_cdc_redaction | previewCdcRedaction | read_only | OK | 20.15 | 35.38 | 22.50 | read_only (seeded success path) |
| DataBroker | PublishCDC | publish_cdc | publishCdc | stream | OK | 245.29 | 245.29 | 249.86 | cdc: time-to-first-event (real seeded Upsert produced) |
| DataBroker | PutObject | put_object | putObject | stream_open | OK | 1.02 | 1.02 | 1.02 | streaming: stream-open latency |
| DataBroker | PutPolicy | put_policy | putPolicy | destructive | OK | 22.80 | 22.80 | 22.80 | destructive: 1 real call against a seeded disposable target |
| DataBroker | QuarantineDlqEvent | quarantine_dlq_event | quarantineDlqEvent | mutation | OK | 20.57 | 22.48 | 31.80 | mutation (seeded success path) |
| DataBroker | ReloadPolicies | reload_policies | reloadPolicies | destructive | OK | 13.67 | 13.67 | 13.67 | destructive: 1 real call against a seeded disposable target |
| DataBroker | ReplayDlqEvent | replay_dlq_event | replayDlqEvent | mutation | OK | 29.39 | 29.39 | 29.39 | mutation (seeded success path) |
| DataBroker | ResumeCdc | resume_cdc | resumeCdc | mutation | OK | 20.12 | 20.71 | 21.62 | mutation (seeded success path) |
| DataBroker | RetrySagaCompensation | retry_saga_compensation | retrySagaCompensation | mutation | OK | 26.78 | 26.78 | 26.78 | mutation (seeded success path) |
| DataBroker | RollbackCatalog | rollback_catalog | rollbackCatalog | destructive | OK | 12.51 | 12.51 | 12.51 | destructive: 1 real call against a seeded disposable target |
| DataBroker | ScanProjectionDrift | scan_projection_drift | scanProjectionDrift | read_only | OK | 19.05 | 28.52 | 20.12 | read_only (seeded success path) |
| DataBroker | Select | select | select | read_only | OK | 11.92 | 16.96 | 12.45 | read_only (seeded success path) |
| DataBroker | SelectV2 | select_v_2 | selectV2 | stream | OK | 12.31 | 13.99 | 12.59 | streaming: time-to-first-response (seeded) |
| DataBroker | StageCatalog | stage_catalog | stageCatalog | destructive | OK | 923.51 | 923.51 | 923.51 | destructive: 1 real call against a seeded disposable target |
| DataBroker | StepDownCdcLeader | step_down_cdc_leader | stepDownCdcLeader | mutation | OK | 20.57 | 20.76 | 21.68 | mutation (seeded success path) |
| DataBroker | TimeSeriesQuery | time_series_query | timeSeriesQuery | read_only | OK | 12.41 | 16.26 | 12.49 | read_only (seeded success path) |
| DataBroker | TimeSeriesWrite | time_series_write | timeSeriesWrite | mutation | OK | 70.71 | 77.25 | 71.22 | mutation (seeded success path) |
| DataBroker | Upsert | upsert | upsert | mutation | OK | 56.98 | 61.89 | 61.46 | mutation (seeded success path) |
| DataBroker | ValidateCatalog | validate_catalog | validateCatalog | destructive | OK | 121.13 | 121.13 | 121.13 | destructive: 1 real call against a seeded disposable target |
| DataBroker | VectorBatchUpsert | vector_batch_upsert | vectorBatchUpsert | stream_open | OK | 0.38 | 0.38 | 0.38 | streaming: stream-open latency |
| DataBroker | VectorHybridSearch | vector_hybrid_search | vectorHybridSearch | read_only | OK | 10.43 | 11.80 | 10.36 | read_only (seeded success path) |
| DataBroker | VectorSearch | vector_search | vectorSearch | read_only | OK | 11.71 | 17.78 | 12.00 | read_only (seeded success path) |
| DataBroker | VectorUpsert | vector_upsert | vectorUpsert | mutation | OK | 16.13 | 21.22 | 16.87 | mutation (seeded success path) |
| DataBroker | VerifyAdminAuditLog | verify_admin_audit_log | verifyAdminAuditLog | read_only | OK | 18.25 | 29.76 | 19.91 | read_only (seeded success path) |
| EmbeddingService | Backfill | backfill | backfillEmbeddingSource | mutation | OK | 17.82 | 26.06 | 22.42 | mutation (seeded success path) |
| EmbeddingService | DeleteSource | delete_source | deleteEmbeddingSource | destructive | OK | 34.91 | 34.91 | 34.91 | destructive: 1 real call against a seeded disposable target |
| EmbeddingService | ListSources | list_sources | listEmbeddingSources | read_only | OK | 17.91 | 22.22 | 17.57 | read_only (seeded success path) |
| EmbeddingService | RegisterSource | register_source | registerEmbeddingSource | mutation | OK | 31.88 | 34.39 | 32.49 | mutation (seeded success path) |
| EmbeddingService | ReportEmbedding | report_embedding | reportEmbedding | mutation | OK | 25.68 | 26.67 | 25.11 | mutation (seeded success path) |
| EmbeddingService | Retrieve | retrieve | retrieveEmbedding | read_only | OK | 19.72 | 24.89 | 19.80 | read_only (seeded success path) |
| IdentityProviderService | CreateProvider | create_provider | createProvider | mutation | OK | 26.07 | 26.07 | 26.07 | mutation (seeded success path) |
| IdentityProviderService | DisableProvider | disable_provider | disableProvider | mutation | OK | 25.04 | 25.51 | 37.09 | mutation (seeded success path) |
| IdentityProviderService | ForceJwksRefresh | force_jwks_refresh | forceJwksRefresh | mutation | OK | 29.00 | 32.88 | 30.88 | mutation (seeded success path) |
| IdentityProviderService | GetProvider | get_provider | getProvider | read_only | OK | 10.16 | 21.43 | 11.43 | read_only (seeded success path) |
| IdentityProviderService | ImportSamlMetadata | import_saml_metadata | importSamlMetadata | mutation | OK | 21.58 | 21.94 | 21.31 | mutation (seeded success path) |
| IdentityProviderService | LinkIdentity | link_identity | linkIdentity | mutation | OK | 24.27 | 39.60 | 30.92 | mutation (seeded success path) |
| IdentityProviderService | ListExternalIdentities | list_external_identities | listExternalIdentities | read_only | OK | 11.51 | 15.63 | 11.32 | read_only (seeded success path) |
| IdentityProviderService | ListProviders | list_providers | listProviders | read_only | OK | 13.03 | 19.71 | 13.72 | read_only (seeded success path) |
| IdentityProviderService | PreviewClaimMapping | preview_claim_mapping | previewClaimMapping | read_only | OK | 9.29 | 13.11 | 9.26 | read_only (seeded success path) |
| IdentityProviderService | PreviewGroupMapping | preview_group_mapping | previewGroupMapping | read_only | OK | 8.94 | 30.72 | 11.04 | read_only (seeded success path) |
| IdentityProviderService | ResolveExternalIdentity | resolve_external_identity | resolveExternalIdentity | mutation | OK | 16.83 | 18.05 | 20.63 | mutation (seeded success path) |
| IdentityProviderService | SamlAcs | saml_acs | samlAcs | mutation | OK | 113.54 | 126.41 | 115.68 | mutation (seeded success path) |
| IdentityProviderService | ScimCreateGroup | scim_create_group | scimCreateGroup | mutation | OK | 12.01 | 12.17 | 11.27 | mutation (seeded success path) |
| IdentityProviderService | ScimCreateUser | scim_create_user | scimCreateUser | mutation | OK | 33.75 | 35.63 | 43.08 | mutation (seeded success path) |
| IdentityProviderService | ScimDeleteGroup | scim_delete_group | scimDeleteGroup | mutation | OK | 8.70 | 9.20 | 8.53 | mutation (seeded success path) |
| IdentityProviderService | ScimDeleteUser | scim_delete_user | scimDeleteUser | mutation | OK | 53.39 | 53.39 | 53.39 | mutation (seeded success path) |
| IdentityProviderService | ScimGetGroup | scim_get_group | scimGetGroup | mutation | OK | 12.36 | 12.96 | 12.35 | mutation (seeded success path) |
| IdentityProviderService | ScimGetUser | scim_get_user | scimGetUser | mutation | OK | 12.25 | 14.24 | 15.01 | mutation (seeded success path) |
| IdentityProviderService | ScimListGroups | scim_list_groups | scimListGroups | mutation | OK | 6.55 | 7.25 | 7.43 | mutation (seeded success path) |
| IdentityProviderService | ScimListUsers | scim_list_users | scimListUsers | mutation | OK | 15.86 | 16.06 | 14.78 | mutation (seeded success path) |
| IdentityProviderService | ScimPatchGroup | scim_patch_group | scimPatchGroup | mutation | OK | 13.23 | 14.88 | 13.93 | mutation (seeded success path) |
| IdentityProviderService | ScimPatchUser | scim_patch_user | scimPatchUser | mutation | OK | 29.71 | 49.97 | 44.72 | mutation (seeded success path) |
| IdentityProviderService | ScimReplaceUser | scim_replace_user | scimReplaceUser | mutation | OK | 23.20 | 24.52 | 23.43 | mutation (seeded success path) |
| IdentityProviderService | StartSamlLogin | start_saml_login | startSamlLogin | mutation | OK | 8.62 | 8.86 | 8.79 | mutation (seeded success path) |
| IdentityProviderService | TestProviderDiscovery | test_provider_discovery | testProviderDiscovery | read_only | OK | 9.05 | 11.40 | 9.15 | read_only (seeded success path) |
| IdentityProviderService | UnlinkIdentity | unlink_identity | unlinkIdentity | mutation | OK | 8.10 | 10.64 | 10.30 | mutation (seeded success path) |
| IdentityProviderService | UpdateProvider | update_provider | updateProvider | mutation | OK | 21.68 | 22.30 | 20.99 | mutation (seeded success path) |
| LiveQueryService | Subscribe | subscribe | liveQuerySubscribe | stream_open | OK | 11.02 | 11.02 | 11.02 | streaming: stream-open latency |
| LockService | AcquireLock | acquire_lock | acquireLock | mutation | OK | 46.78 | 62.21 | 58.91 | mutation (seeded success path) |
| LockService | GetLock | get_lock | getLock | read_only | OK | 16.14 | 24.21 | 16.64 | read_only (seeded success path) |
| LockService | ListLocks | list_locks | listLocks | read_only | OK | 16.54 | 19.64 | 16.47 | read_only (seeded success path) |
| LockService | ReleaseLock | release_lock | releaseLock | mutation | OK | 15.56 | 20.29 | 20.19 | mutation (seeded success path) |
| LockService | RenewLock | renew_lock | renewLock | mutation | OK | 45.40 | 48.87 | 57.29 | mutation (seeded success path) |
| MeteringService | CheckQuota | check_quota | checkQuota | read_only | OK | 15.25 | 21.18 | 15.63 | read_only (seeded success path) |
| MeteringService | GetQuota | get_quota | getQuota | read_only | OK | 14.59 | 26.63 | 15.76 | read_only (seeded success path) |
| MeteringService | ListQuotas | list_quotas | listQuotas | read_only | OK | 15.12 | 20.67 | 15.79 | read_only (seeded success path) |
| MeteringService | PutQuota | put_quota | putQuota | mutation | OK | 31.19 | 34.61 | 32.21 | mutation (seeded success path) |
| MeteringService | QueryUsage | query_usage | queryUsage | read_only | OK | 13.37 | 17.84 | 14.05 | read_only (seeded success path) |
| MeteringService | RecordUsage | record_usage | recordUsage | mutation | OK | 12.77 | 13.12 | 12.03 | mutation (seeded success path) |
| NotificationService | GetDeliveryStats | get_delivery_stats | getDeliveryStats | read_only | OK | 11.46 | 16.50 | 12.19 | read_only (seeded success path) |
| NotificationService | GetNotification | get_notification | getNotification | read_only | OK | 15.52 | 19.02 | 15.42 | read_only (seeded success path) |
| NotificationService | GetPreference | get_preference | getPreference | read_only | OK | 14.87 | 29.01 | 17.39 | read_only (seeded success path) |
| NotificationService | GetTemplate | get_template | getTemplate | read_only | OK | 15.77 | 21.01 | 16.28 | read_only (seeded success path) |
| NotificationService | ListNotifications | list_notifications | listNotifications | read_only | OK | 21.84 | 29.21 | 23.51 | read_only (seeded success path) |
| NotificationService | ListPreferences | list_preferences | listPreferences | read_only | OK | 21.89 | 42.35 | 23.70 | read_only (seeded success path) |
| NotificationService | ListTemplates | list_templates | listTemplates | read_only | OK | 22.42 | 36.11 | 25.12 | read_only (seeded success path) |
| NotificationService | ReportDelivery | report_delivery | reportDelivery | mutation | OK | 19.39 | 22.59 | 19.89 | mutation (seeded success path) |
| NotificationService | RetryNotification | retry_notification | retryNotification | mutation | OK | 23.97 | 23.97 | 23.97 | mutation (seeded success path) |
| NotificationService | SendNotification | send_notification | sendNotification | mutation | OK | 43.04 | 43.22 | 41.35 | mutation (seeded success path) |
| NotificationService | SetPreference | set_preference | setPreference | mutation | OK | 13.69 | 14.26 | 12.78 | mutation (seeded success path) |
| NotificationService | UpsertTemplate | upsert_template | upsertTemplate | mutation | OK | 9.09 | 10.46 | 9.90 | mutation (seeded success path) |
| PeerService | GetPeer | get_peer | getPeer | read_only | OK | 17.21 | 21.15 | 16.83 | read_only (seeded success path) |
| PeerService | JoinRoom | join_room | joinRoom | mutation | OK | 26.95 | 27.11 | 37.08 | mutation (seeded success path) |
| PeerService | JoinSession | join_session | joinSession | mutation | OK | 26.27 | 28.71 | 28.62 | mutation (seeded success path) |
| PeerService | LeaveRoom | leave_room | leaveRoom | mutation | OK | 13.05 | 21.68 | 16.79 | mutation (seeded success path) |
| PeerService | ListPeers | list_peers | listPeers | read_only | OK | 17.05 | 20.31 | 16.96 | read_only (seeded success path) |
| RoomService | CloseRoom | close_room | closeRoom | mutation | OK | 39.36 | 40.70 | 37.52 | mutation (seeded success path) |
| RoomService | CreateRoom | create_room | createRoom | mutation | OK | 18.05 | 19.43 | 19.15 | mutation (seeded success path) |
| RoomService | GetRoom | get_room | getRoom | read_only | OK | 17.69 | 29.63 | 19.27 | read_only (seeded success path) |
| RoomService | ListEgress | list_egress | listEgress | read_only | CAPABILITY_SKIPPED | 9.05 | 14.13 | 9.70 | capability skipped: udb udb.core.webrtc.services.v1.RoomService/ListEgress: webrtc_egress_enabled (code=FAILED_PRECONDITION) |
| RoomService | ListRooms | list_rooms | listRooms | read_only | OK | 20.19 | 28.12 | 21.39 | read_only (seeded success path) |
| RoomService | StartRoomComposite | start_room_composite | startRoomComposite | mutation | CAPABILITY_SKIPPED | 9.04 | 9.95 | 9.20 | capability skipped: udb udb.core.webrtc.services.v1.RoomService/StartRoomComposite: webrtc_egress_enabled (code=FAILED_PRECONDITION) |
| RoomService | StartTrackEgress | start_track_egress | startTrackEgress | mutation | CAPABILITY_SKIPPED | 8.76 | 8.94 | 8.73 | capability skipped: udb udb.core.webrtc.services.v1.RoomService/StartTrackEgress: webrtc_egress_enabled (code=FAILED_PRECONDITION) |
| RoomService | StopEgress | stop_egress | stopEgress | mutation | CAPABILITY_SKIPPED | 7.86 | 8.18 | 8.18 | capability skipped: udb udb.core.webrtc.services.v1.RoomService/StopEgress: webrtc_egress_enabled (code=FAILED_PRECONDITION) |
| RoomService | UpdateRoom | update_room | updateRoom | mutation | OK | 15.19 | 28.74 | 23.90 | mutation (seeded success path) |
| SchedulerService | CreateJob | create_job | createJob | mutation | OK | 19.75 | 19.78 | 19.73 | mutation (seeded success path) |
| SchedulerService | DeleteJob | delete_job | deleteJob | destructive | OK | 23.81 | 23.81 | 23.81 | destructive: 1 real call against a seeded disposable target |
| SchedulerService | GetJob | get_job | getJob | read_only | OK | 13.85 | 21.91 | 14.51 | read_only (seeded success path) |
| SchedulerService | ListJobs | list_jobs | listJobs | read_only | OK | 17.14 | 26.20 | 17.60 | read_only (seeded success path) |
| SchedulerService | PauseJob | pause_job | pauseJob | mutation | OK | 21.46 | 21.46 | 21.46 | mutation (seeded success path) |
| SchedulerService | ResumeJob | resume_job | resumeJob | mutation | OK | 17.14 | 17.14 | 17.14 | mutation (seeded success path) |
| SearchService | CreateIndex | create_index | createSearchIndex | mutation | OK | 29.17 | 31.82 | 31.29 | mutation (seeded success path) |
| SearchService | DeleteIndex | delete_index | deleteSearchIndex | destructive | OK | 32.29 | 32.29 | 32.29 | destructive: 1 real call against a seeded disposable target |
| SearchService | ListIndexes | list_indexes | listSearchIndexes | read_only | OK | 15.54 | 19.17 | 15.83 | read_only (seeded success path) |
| SearchService | Reindex | reindex | reindexSearchIndex | mutation | OK | 31.60 | 32.06 | 30.47 | mutation (seeded success path) |
| SearchService | Search | search | search | read_only | OK | 15.67 | 20.13 | 16.21 | read_only (seeded success path) |
| SignalingService | Signal | signal | signal | stream_open | OK | 9.39 | 9.39 | 9.39 | streaming: stream-open latency |
| StorageService | DeleteFile | delete_file | deleteFile | mutation | OK | 35.44 | 35.44 | 35.44 | mutation (seeded success path) |
| StorageService | DownloadFile | download_file | downloadFile | stream | OK | 28.61 | 30.70 | 28.85 | streaming: time-to-first-response (seeded) |
| StorageService | FinalizeUpload | finalize_upload | finalizeUpload | mutation | OK | 42.22 | 42.22 | 42.22 | mutation (seeded success path) |
| StorageService | GetDownloadUrl | get_download_url | getDownloadUrl | read_only | OK | 16.98 | 24.85 | 18.02 | read_only (seeded success path) |
| StorageService | GetFile | get_file | getFile | read_only | OK | 15.68 | 32.25 | 17.34 | read_only (seeded success path) |
| StorageService | ListFiles | list_files | listFiles | read_only | OK | 22.69 | 30.45 | 23.25 | read_only (seeded success path) |
| StorageService | RegisterUpload | register_upload | registerUpload | mutation | OK | 21.78 | 22.67 | 22.57 | mutation (seeded success path) |
| StorageService | ReissueUploadUrl | reissue_upload_url | reissueUploadUrl | read_only | OK | 17.52 | 26.90 | 18.86 | read_only (seeded success path) |
| StorageService | UpdateFile | update_file | updateFile | mutation | OK | 31.48 | 31.94 | 29.96 | mutation (seeded success path) |
| TenantService | CreateTenant | create_tenant | createTenant | mutation | OK | 14.66 | 16.99 | 16.26 | mutation (seeded success path) |
| TenantService | GetTenant | get_tenant | getTenant | read_only | OK | 15.20 | 22.40 | 15.80 | read_only (seeded success path) |
| TenantService | GetTenantConfig | get_tenant_config | getTenantConfig | read_only | OK | 17.64 | 30.42 | 18.47 | read_only (seeded success path) |
| TenantService | ListTenants | list_tenants | listTenants | read_only | OK | 15.25 | 19.31 | 15.37 | read_only (seeded success path) |
| TenantService | PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 189.80 | 189.80 | 189.80 | destructive: 1 real call against a seeded disposable target |
| TenantService | UpdateTenant | update_tenant | updateTenant | mutation | OK | 17.19 | 17.99 | 16.90 | mutation (seeded success path) |
| TenantService | UpdateTenantConfig | update_tenant_config | updateTenantConfig | mutation | OK | 32.28 | 34.45 | 30.73 | mutation (seeded success path) |
| TrackService | ListTracks | list_tracks | listTracks | read_only | OK | 14.51 | 22.15 | 15.75 | read_only (seeded success path) |
| TrackService | MuteTrack | mute_track | muteTrack | mutation | OK | 13.57 | 16.37 | 14.00 | mutation (seeded success path) |
| TrackService | PublishTrack | publish_track | publishTrack | mutation | OK | 20.30 | 21.81 | 31.66 | mutation (seeded success path) |
| TrackService | UnpublishTrack | unpublish_track | unpublishTrack | mutation | OK | 12.96 | 13.06 | 12.59 | mutation (seeded success path) |
| TurnService | IssueCredentials | issue_credentials | issueCredentials | mutation | OK | 24.74 | 26.58 | 21.84 | mutation (seeded success path) |
| VaultService | BatchDecrypt | batch_decrypt | vaultBatchDecrypt | mutation | OK | 22.92 | 23.20 | 24.59 | mutation (seeded success path) |
| VaultService | BatchEncrypt | batch_encrypt | vaultBatchEncrypt | mutation | OK | 20.57 | 22.16 | 20.52 | mutation (seeded success path) |
| VaultService | CreateTransitKey | create_transit_key | createTransitKey | mutation | OK | 24.38 | 24.38 | 24.38 | mutation (seeded success path) |
| VaultService | Decrypt | decrypt | vaultDecrypt | read_only | OK | 19.64 | 27.91 | 20.26 | read_only (seeded success path) |
| VaultService | DeleteSecret | delete_secret | deleteSecret | mutation | OK | 18.23 | 21.38 | 20.50 | mutation (seeded success path) |
| VaultService | DestroySecret | destroy_secret | destroySecret | destructive | OK | 30.90 | 30.90 | 30.90 | destructive: 1 real call against a seeded disposable target |
| VaultService | Encrypt | encrypt | vaultEncrypt | mutation | OK | 29.49 | 29.69 | 27.29 | mutation (seeded success path) |
| VaultService | GenerateDatabaseCredentials | generate_database_credentials | generateDatabaseCredentials | mutation | OK | 32.96 | 33.78 | 33.38 | mutation (seeded success path) |
| VaultService | GenerateDataKey | generate_data_key | vaultGenerateDataKey | mutation | OK | 23.45 | 24.33 | 32.51 | mutation (seeded success path) |
| VaultService | GetSecret | get_secret | getSecret | read_only | OK | 18.90 | 33.84 | 20.51 | read_only (seeded success path) |
| VaultService | GetTransitPublicKey | get_transit_public_key | vaultGetTransitPublicKey | read_only | OK | 16.31 | 21.85 | 17.02 | read_only (seeded success path) |
| VaultService | Hmac | hmac | vaultHmac | mutation | OK | 23.54 | 25.58 | 23.15 | mutation (seeded success path) |
| VaultService | ListSecrets | list_secrets | listSecrets | read_only | OK | 18.35 | 25.70 | 18.88 | read_only (seeded success path) |
| VaultService | PutSecret | put_secret | putSecret | mutation | OK | 28.68 | 28.68 | 28.68 | mutation (seeded success path) |
| VaultService | Rewrap | rewrap | vaultRewrap | mutation | OK | 23.86 | 31.90 | 36.22 | mutation (seeded success path) |
| VaultService | RotateTransitKey | rotate_transit_key | rotateTransitKey | mutation | OK | 47.65 | 49.81 | 46.03 | mutation (seeded success path) |
| VaultService | SealStatus | seal_status | vaultSealStatus | read_only | OK | 4.44 | 6.02 | 4.72 | read_only (seeded success path) |
| VaultService | Sign | sign | vaultSign | mutation | OK | 20.35 | 23.08 | 21.13 | mutation (seeded success path) |
| VaultService | UndeleteSecret | undelete_secret | undeleteSecret | mutation | OK | 36.33 | 36.33 | 36.33 | mutation (seeded success path) |
| VaultService | Verify | verify | vaultVerify | read_only | OK | 23.59 | 42.94 | 24.99 | read_only (seeded success path) |
| WebhookService | CreateEndpoint | create_endpoint | createWebhookEndpoint | mutation | OK | 16.65 | 17.66 | 16.64 | mutation (seeded success path) |
| WebhookService | DeleteEndpoint | delete_endpoint | deleteWebhookEndpoint | destructive | OK | 17.00 | 17.00 | 17.00 | destructive: 1 real call against a seeded disposable target |
| WebhookService | GetEndpoint | get_endpoint | getWebhookEndpoint | read_only | OK | 18.90 | 34.80 | 20.84 | read_only (seeded success path) |
| WebhookService | ListDeliveries | list_deliveries | listWebhookDeliveries | read_only | OK | 21.59 | 37.25 | 22.53 | read_only (seeded success path) |
| WebhookService | ListEndpoints | list_endpoints | listWebhookEndpoints | read_only | OK | 23.69 | 32.77 | 23.58 | read_only (seeded success path) |
| WebhookService | UpdateEndpoint | update_endpoint | updateWebhookEndpoint | mutation | OK | 15.01 | 16.80 | 15.91 | mutation (seeded success path) |
| WorkflowService | CancelWorkflow | cancel_workflow | cancelWorkflow | destructive | OK | 32.45 | 32.45 | 32.45 | destructive: 1 real call against a seeded disposable target |
| WorkflowService | GetWorkflow | get_workflow | getWorkflow | read_only | OK | 14.37 | 18.73 | 14.51 | read_only (seeded success path) |
| WorkflowService | ListWorkflows | list_workflows | listWorkflows | read_only | OK | 20.11 | 28.65 | 20.70 | read_only (seeded success path) |
| WorkflowService | SignalWorkflow | signal_workflow | signalWorkflow | mutation | OK | 17.65 | 19.16 | 18.04 | mutation (seeded success path) |
| WorkflowService | StartWorkflow | start_workflow | startWorkflow | mutation | OK | 20.73 | 20.78 | 19.68 | mutation (seeded success path) |
