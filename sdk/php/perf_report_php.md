# UDB SDK Live Perf — PHP (Docker → host)

RPCs measured: 353   tenant=87f023f6-6698-4c49-8084-603dd390b3e7

Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities (a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request's reference/ID fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection latency. The TARGET is zero failures; any residual non-OK RPC is listed under Failures for the maintainer to finish.

Unary = full request/response round-trip. Streaming rows (kind=stream_open) report stream-open latency (initiate + cancel, no response drain), NOT first-message latency.

## Seeded fixtures

Captured semantic field -> seeded value keys used to resolve request fields: action, apply_run_id, approval_token, approve_draft_id, approve_run_id, approved_by, asset_id, assigned_by, auth_challenge_id, backup_id, bucket, canary_id, canary_version_id, cancel_workflow_id, catalog_manifest, catalog_manifest_b64, challenge_id, close_room_id, code, content_type, created_by, csrf_token, definition_id, delete_endpoint_id, delete_file_id, delete_policy_id, delete_role_id, delete_scim_user_id, deleted_by, device_id, disable_provider_id, dismiss_dlq_id, dlq_id, document_id, domain, ds_policy_id, egress_id, endpoint_id, event_type, external_identity_id, file_id, file_type, filename, finalize_file_id, gov_exp, instance_id, job_id, join_session_room_id, key_id, key_prefix, kind, leave_peer_id, locale, log_id, mark_saga_id, message_type, migration_id, mongo_collection, name, node_id, notification_id, object, object_key, otp_code, otp_id, owner_id, peer_id, plain_key, policy_draft_id, policy_id, policy_version_id, project, project_id, provider_id, quarantine_dlq_id, recipient_id, record_id, recovery_code, refresh_session_id, refresh_token, refresh_token_session_id, reg_challenge_id, reissue_file_id, reject_draft_id, rejected_by, relation, release_fencing_token, renew_fencing_token, replay_dlq_id, reset_otp_code, reset_otp_id, resource, resource_name, restore_tenant_id, retry_saga_id, revoke_key_id, revoke_key_prefix, revoked_by, role, role_code, role_id, rollback_policy_set_id, rollback_resource_version, rollback_target_version_id, room_id, saga_id, saml_provider_id, scim_group_id, scim_user_id, session_id, stage_name, step_id, subject, tenant, tenant_code, tenant_id, token, topic_pattern, track_id, ts_table, unpublish_track_id, update_draft_id, update_key_id, update_key_prefix, updated_by, user_id, username, vault_ciphertext, vault_create_key_name, vault_db_role, vault_delete_secret_path, vault_destroy_secret_path, vault_hmac_key_name, vault_key_name, vault_put_secret_path, vault_secret_path, vault_signature, vault_signing_key_name, workflow_id

## Per-service mean latency

| Service | RPCs | mean ms |
|---|--:|--:|
| BackupService | 8 | 549.04 |
| AuthnService | 50 | 143.80 |
| SchedulerService | 6 | 141.53 |
| TenantService | 7 | 115.12 |
| VaultService | 20 | 84.90 |
| TurnService | 1 | 81.23 |
| LockService | 5 | 80.62 |
| ApiKeyService | 9 | 67.65 |
| CacheService | 7 | 67.53 |
| ConfigService | 5 | 65.41 |
| SearchService | 5 | 65.15 |
| IdentityProviderService | 27 | 62.65 |
| EmbeddingService | 6 | 58.61 |
| WorkflowService | 5 | 53.07 |
| TrackService | 4 | 52.84 |
| AuthzService | 41 | 52.29 |
| DataBroker | 77 | 47.24 |
| WebhookService | 6 | 46.84 |
| NotificationService | 12 | 43.60 |
| StorageService | 9 | 42.97 |
| ControlPlaneService | 6 | 34.53 |
| PeerService | 5 | 34.50 |
| MeteringService | 6 | 34.10 |
| AssetService | 8 | 32.56 |
| AnalyticsService | 7 | 24.70 |
| RoomService | 9 | 21.36 |
| LiveQueryService | 1 | 2.64 |
| SignalingService | 1 | 0.36 |

## Capability skips (4)

| RPC | api_alias | operation_id | kind | p99 ms |
|---|---|---|---|--:|
| RoomService/ListEgress | list_egress | listEgress | read_only | 10.73 |
| RoomService/StartRoomComposite | start_room_composite | startRoomComposite | mutation | 9.31 |
| RoomService/StartTrackEgress | start_track_egress | startTrackEgress | mutation | 8.77 |
| RoomService/StopEgress | stop_egress | stopEgress | mutation | 7.96 |

## Failures (0)

No RPC returned a non-OK gRPC status.

## Slowest 20 by p99

| RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms |
|---|---|---|---|---|--:|--:|--:|
| BackupService/StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 2050.55 | 2195.67 | 2009.45 |
| BackupService/RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 2173.71 | 2173.71 | 2173.71 |
| AuthnService/ChangePassword | change_password | changePassword | mutation | OK | 1564.82 | 1564.82 | 1564.82 |
| DataBroker/StageCatalog | stage_catalog | stageCatalog | destructive | OK | 857.40 | 857.40 | 857.40 |
| AuthnService/CreateUser | create_user | createUser | mutation | OK | 759.82 | 822.41 | 760.11 |
| AuthnService/ResetPassword | reset_password | resetPassword | mutation | OK | 802.24 | 802.24 | 802.24 |
| AuthnService/EnrollMFA | enroll_mfa | enrollMfa | mutation | OK | 603.36 | 674.21 | 593.20 |
| AuthnService/Login | login | login | mutation | OK | 574.38 | 590.29 | 601.93 |
| DataBroker/ApplyMigration | apply_migration | applyMigration | mutation | OK | 578.33 | 578.33 | 578.33 |
| TenantService/PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 524.99 | 524.99 | 524.99 |
| AuthnService/GenerateRecoveryCodes | generate_recovery_codes | generateRecoveryCodes | mutation | OK | 103.06 | 453.75 | 260.87 |
| VaultService/UndeleteSecret | undelete_secret | undeleteSecret | mutation | OK | 430.34 | 430.34 | 430.34 |
| SchedulerService/ResumeJob | resume_job | resumeJob | mutation | OK | 423.74 | 423.74 | 423.74 |
| IdentityProviderService/ScimDeleteUser | scim_delete_user | scimDeleteUser | mutation | OK | 344.79 | 344.79 | 344.79 |
| DataBroker/ValidateCatalog | validate_catalog | validateCatalog | destructive | OK | 276.96 | 276.96 | 276.96 |
| AuthzService/ActivatePolicyVersion | activate_policy_version | activatePolicyVersion | destructive | OK | 272.18 | 272.18 | 272.18 |
| LockService/RenewLock | renew_lock | renewLock | mutation | OK | 112.50 | 253.52 | 160.20 |
| CacheService/DeleteNamespace | delete_cache_namespace | deleteCacheNamespace | destructive | OK | 247.09 | 247.09 | 247.09 |
| AuthzService/MigrateLegacyPolicies | migrate_legacy_policies | migrateLegacyPolicies | destructive | OK | 237.86 | 237.86 | 237.86 |
| AuthnService/FinishWebAuthnAuthentication | finish_web_authn_authentication | finishWebAuthnAuthentication | mutation | OK | 233.12 | 233.12 | 233.12 |

## Full per-RPC table (sorted by service, then RPC)

| Service | RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms | iters |
|---|---|---|---|---|---|--:|--:|--:|--:|
| AnalyticsService | GetExecutorPerformance | get_executor_performance | getExecutorPerformance | read_only | OK | 21.29 | 28.29 | 20.67 | 25 |
| AnalyticsService | GetPipelineSummary | get_pipeline_summary | getPipelineSummary | read_only | OK | 15.08 | 28.41 | 17.27 | 25 |
| AnalyticsService | GetReconciliationAnalytics | get_reconciliation_analytics | getReconciliationAnalytics | read_only | OK | 22.10 | 29.00 | 22.11 | 25 |
| AnalyticsService | GetSlaCompliance | get_sla_compliance | getSlaCompliance | read_only | OK | 20.35 | 40.36 | 22.28 | 25 |
| AnalyticsService | GetThroughput | get_throughput | getThroughput | read_only | OK | 16.37 | 26.26 | 18.07 | 25 |
| AnalyticsService | RecordPipelineMetric | record_pipeline_metric | recordPipelineMetric | mutation | OK | 16.28 | 30.00 | 21.22 | 5 |
| AnalyticsService | TriggerSnapshot | trigger_snapshot | triggerSnapshot | mutation | OK | 42.46 | 47.77 | 51.31 | 5 |
| ApiKeyService | CreateApiKey | create_api_key | createApiKey | mutation | OK | 16.30 | 30.79 | 21.40 | 5 |
| ApiKeyService | EmergencyRevokeApiKeys | emergency_revoke_api_keys | emergencyRevokeApiKeys | destructive | OK | 206.99 | 206.99 | 206.99 | 1 |
| ApiKeyService | GetApiKey | get_api_key | getApiKey | read_only | OK | 9.29 | 20.18 | 10.59 | 25 |
| ApiKeyService | GetApiKeyUsageStats | get_api_key_usage_stats | getApiKeyUsageStats | read_only | OK | 9.60 | 13.90 | 10.30 | 25 |
| ApiKeyService | ListApiKeys | list_api_keys | listApiKeys | read_only | OK | 14.14 | 18.33 | 13.68 | 25 |
| ApiKeyService | RevokeApiKey | revoke_api_key | revokeApiKey | mutation | OK | 139.85 | 139.85 | 139.85 | 1 |
| ApiKeyService | RotateApiKey | rotate_api_key | rotateApiKey | mutation | OK | 133.37 | 133.37 | 133.37 | 1 |
| ApiKeyService | UpdateApiKey | update_api_key | updateApiKey | mutation | OK | 42.94 | 44.37 | 58.84 | 5 |
| ApiKeyService | ValidateApiKey | validate_api_key | validateApiKey | read_only | OK | 12.68 | 20.35 | 13.83 | 25 |
| AssetService | CompleteStep | complete_step | completeStep | mutation | OK | 41.17 | 45.25 | 65.54 | 5 |
| AssetService | CreatePipelineDefinition | create_pipeline_definition | createPipelineDefinition | mutation | OK | 23.04 | 23.80 | 21.97 | 5 |
| AssetService | GetAsset | get_asset | getAsset | read_only | OK | 17.39 | 32.66 | 19.51 | 25 |
| AssetService | GetPipeline | get_pipeline | getPipeline | read_only | OK | 24.17 | 40.67 | 26.15 | 25 |
| AssetService | GetPipelineDefinition | get_pipeline_definition | getPipelineDefinition | read_only | OK | 21.67 | 33.28 | 22.29 | 25 |
| AssetService | ListAssets | list_assets | listAssets | read_only | OK | 20.05 | 34.20 | 21.06 | 25 |
| AssetService | RegisterAsset | register_asset | registerAsset | mutation | OK | 32.10 | 45.81 | 42.95 | 5 |
| AssetService | StartPipeline | start_pipeline | startPipeline | mutation | OK | 17.30 | 25.25 | 41.03 | 5 |
| AuthnService | AdminResetMfa | admin_reset_mfa | adminResetMfa | destructive | OK | 125.62 | 125.62 | 125.62 | 1 |
| AuthnService | AdminResetPassword | admin_reset_password | adminResetPassword | destructive | OK | 99.23 | 99.23 | 99.23 | 1 |
| AuthnService | AdminRevokeAllTenantSessions | admin_revoke_all_tenant_sessions | adminRevokeAllTenantSessions | destructive | OK | 116.19 | 116.19 | 116.19 | 1 |
| AuthnService | AdminRevokeAllUserSessions | admin_revoke_all_user_sessions | adminRevokeAllUserSessions | destructive | OK | 116.83 | 116.83 | 116.83 | 1 |
| AuthnService | AdminRevokeSession | admin_revoke_session | adminRevokeSession | destructive | OK | 109.82 | 109.82 | 109.82 | 1 |
| AuthnService | Authenticate | authenticate | authenticate | read_only | OK | 30.04 | 47.88 | 32.24 | 25 |
| AuthnService | ChangePassword | change_password | changePassword | mutation | OK | 1564.82 | 1564.82 | 1564.82 | 1 |
| AuthnService | ChangeUserStatus | change_user_status | changeUserStatus | destructive | OK | 139.16 | 139.16 | 139.16 | 1 |
| AuthnService | ConfirmMFAEnrollment | confirm_mfaenrollment | confirmMfaenrollment | mutation | OK | 10.26 | 11.92 | 37.76 | 5 |
| AuthnService | CreateSession | create_session | createSession | mutation | OK | 166.40 | 180.18 | 175.30 | 5 |
| AuthnService | CreateUser | create_user | createUser | mutation | OK | 759.82 | 822.41 | 760.11 | 5 |
| AuthnService | DeleteWebAuthnCredential | delete_web_authn_credential | deleteWebAuthnCredential | mutation | OK | 12.21 | 12.37 | 23.38 | 5 |
| AuthnService | DisableMfaFactor | disable_mfa_factor | disableMfaFactor | mutation | OK | 17.80 | 31.23 | 38.18 | 5 |
| AuthnService | EmergencyRevoke | emergency_revoke | emergencyRevoke | destructive | OK | 103.13 | 103.13 | 103.13 | 1 |
| AuthnService | EnrollMFA | enroll_mfa | enrollMfa | mutation | OK | 603.36 | 674.21 | 593.20 | 5 |
| AuthnService | FinishWebAuthnAuthentication | finish_web_authn_authentication | finishWebAuthnAuthentication | mutation | OK | 233.12 | 233.12 | 233.12 | 1 |
| AuthnService | FinishWebAuthnRegistration | finish_web_authn_registration | finishWebAuthnRegistration | mutation | OK | 82.33 | 82.33 | 82.33 | 1 |
| AuthnService | ForgotPassword | forgot_password | forgotPassword | mutation | OK | 11.36 | 12.36 | 27.91 | 5 |
| AuthnService | GenerateRecoveryCodes | generate_recovery_codes | generateRecoveryCodes | mutation | OK | 103.06 | 453.75 | 260.87 | 5 |
| AuthnService | GetJwks | get_jwks | getJwks | read_only | OK | 9.86 | 22.06 | 12.27 | 25 |
| AuthnService | GetMfaPolicy | get_mfa_policy | getMfaPolicy | read_only | OK | 14.27 | 23.01 | 14.51 | 25 |
| AuthnService | GetSession | get_session | getSession | read_only | OK | 8.11 | 16.62 | 9.77 | 25 |
| AuthnService | GetUser | get_user | getUser | read_only | OK | 9.27 | 14.95 | 9.94 | 25 |
| AuthnService | IntrospectToken | introspect_token | introspectToken | read_only | OK | 65.79 | 82.01 | 66.66 | 25 |
| AuthnService | IssueMfaChallenge | issue_mfa_challenge | issueMfaChallenge | mutation | OK | 27.98 | 28.50 | 27.48 | 5 |
| AuthnService | ListDevices | list_devices | listDevices | read_only | OK | 14.33 | 19.34 | 14.21 | 25 |
| AuthnService | ListMfaFactors | list_mfa_factors | listMfaFactors | read_only | OK | 13.01 | 22.94 | 14.67 | 25 |
| AuthnService | ListSessions | list_sessions | listSessions | read_only | OK | 16.89 | 27.36 | 17.51 | 25 |
| AuthnService | ListUsers | list_users | listUsers | read_only | OK | 14.53 | 19.36 | 15.13 | 25 |
| AuthnService | ListWebAuthnCredentials | list_web_authn_credentials | listWebAuthnCredentials | read_only | OK | 6.87 | 11.57 | 7.25 | 25 |
| AuthnService | Login | login | login | mutation | OK | 574.38 | 590.29 | 601.93 | 5 |
| AuthnService | Logout | logout | logout | mutation | OK | 10.86 | 15.77 | 31.59 | 5 |
| AuthnService | PutMfaPolicy | put_mfa_policy | putMfaPolicy | mutation | OK | 31.79 | 57.51 | 50.79 | 5 |
| AuthnService | RefreshSession | refresh_session | refreshSession | mutation | OK | 24.33 | 36.27 | 47.80 | 5 |
| AuthnService | RefreshToken | refresh_token | refreshToken | mutation | OK | 142.45 | 142.45 | 142.45 | 1 |
| AuthnService | RenamePasskey | rename_passkey | renamePasskey | mutation | OK | 8.42 | 10.15 | 24.22 | 5 |
| AuthnService | ResendOTP | resend_otp | resendOtp | mutation | OK | 62.09 | 76.70 | 77.62 | 5 |
| AuthnService | ResetPassword | reset_password | resetPassword | mutation | OK | 802.24 | 802.24 | 802.24 | 1 |
| AuthnService | RevokeDevice | revoke_device | revokeDevice | mutation | OK | 151.56 | 151.56 | 151.56 | 1 |
| AuthnService | RevokeRecoveryCodes | revoke_recovery_codes | revokeRecoveryCodes | mutation | OK | 19.03 | 19.97 | 32.57 | 5 |
| AuthnService | RevokeSession | revoke_session | revokeSession | mutation | OK | 24.76 | 26.51 | 46.56 | 5 |
| AuthnService | SendOTP | send_otp | sendOtp | mutation | OK | 18.25 | 19.02 | 20.93 | 5 |
| AuthnService | SendPhoneVerification | send_phone_verification | sendPhoneVerification | mutation | OK | 26.22 | 30.03 | 37.94 | 5 |
| AuthnService | StartWebAuthnAuthentication | start_web_authn_authentication | startWebAuthnAuthentication | mutation | OK | 39.80 | 40.51 | 37.29 | 5 |
| AuthnService | StartWebAuthnRegistration | start_web_authn_registration | startWebAuthnRegistration | mutation | OK | 30.35 | 36.14 | 33.51 | 5 |
| AuthnService | UpdateUser | update_user | updateUser | mutation | OK | 17.14 | 33.34 | 34.40 | 5 |
| AuthnService | ValidateCSRF | validate_csrf | validateCsrf | read_only | OK | 12.86 | 17.12 | 13.10 | 25 |
| AuthnService | ValidateToken | validate_token | validateToken | read_only | OK | 27.02 | 36.97 | 27.32 | 25 |
| AuthnService | VerifyMfaChallenge | verify_mfa_challenge | verifyMfaChallenge | read_only | OK | 13.65 | 20.83 | 14.36 | 25 |
| AuthnService | VerifyOTP | verify_otp | verifyOtp | read_only | OK | 121.59 | 214.03 | 113.02 | 25 |
| AuthzService | ActivateCanary | activate_canary | activateCanary | destructive | OK | 53.51 | 53.51 | 53.51 | 1 |
| AuthzService | ActivatePolicyVersion | activate_policy_version | activatePolicyVersion | destructive | OK | 272.18 | 272.18 | 272.18 | 1 |
| AuthzService | ApprovePolicyDraft | approve_policy_draft | approvePolicyDraft | mutation | OK | 105.48 | 105.48 | 105.48 | 1 |
| AuthzService | AssignRole | assign_role | assignRole | mutation | OK | 31.70 | 32.70 | 29.90 | 5 |
| AuthzService | Authorize | authorize | authorize | read_only | OK | 35.38 | 55.75 | 38.60 | 25 |
| AuthzService | BatchCheckPermissions | batch_check_permissions | batchCheckPermissions | read_only | OK | 25.33 | 66.94 | 29.73 | 25 |
| AuthzService | CheckAccess | check_access | checkAccess | read_only | OK | 18.21 | 36.93 | 20.83 | 25 |
| AuthzService | CreatePolicyDraft | create_policy_draft | createPolicyDraft | mutation | OK | 53.14 | 79.46 | 62.52 | 5 |
| AuthzService | CreatePolicyRule | create_policy_rule | createPolicyRule | mutation | OK | 40.27 | 40.52 | 40.14 | 5 |
| AuthzService | CreateRole | create_role | createRole | mutation | OK | 23.85 | 25.57 | 24.66 | 5 |
| AuthzService | DeletePolicyRule | delete_policy_rule | deletePolicyRule | mutation | OK | 14.41 | 25.63 | 31.54 | 5 |
| AuthzService | DeleteRole | delete_role | deleteRole | mutation | OK | 16.00 | 17.73 | 37.32 | 5 |
| AuthzService | DiffPolicyDraft | diff_policy_draft | diffPolicyDraft | read_only | OK | 21.08 | 33.10 | 23.67 | 25 |
| AuthzService | ExplainPolicy | explain_policy | explainPolicy | read_only | OK | 12.99 | 26.00 | 15.71 | 25 |
| AuthzService | GetAuthzRevision | get_authz_revision | getAuthzRevision | read_only | OK | 9.93 | 13.30 | 9.57 | 25 |
| AuthzService | GetCanaryStatus | get_canary_status | getCanaryStatus | read_only | OK | 17.53 | 32.95 | 19.20 | 25 |
| AuthzService | GetNativeAccess | get_native_access | getNativeAccess | read_only | OK | 27.59 | 42.79 | 30.15 | 25 |
| AuthzService | GetPolicyBundle | get_policy_bundle | getPolicyBundle | read_only | OK | 12.76 | 18.97 | 14.40 | 25 |
| AuthzService | GetPolicyRule | get_policy_rule | getPolicyRule | read_only | OK | 11.18 | 17.72 | 11.25 | 25 |
| AuthzService | GetRole | get_role | getRole | read_only | OK | 10.39 | 20.75 | 11.79 | 25 |
| AuthzService | InvalidatePolicyBundles | invalidate_policy_bundles | invalidatePolicyBundles | destructive | OK | 194.05 | 194.05 | 194.05 | 1 |
| AuthzService | LintAuthzPolicies | lint_authz_policies | lintAuthzPolicies | read_only | OK | 3.31 | 5.91 | 3.67 | 25 |
| AuthzService | ListAccessDecisionAudits | list_access_decision_audits | listAccessDecisionAudits | read_only | OK | 27.38 | 37.95 | 27.19 | 25 |
| AuthzService | ListPolicyRules | list_policy_rules | listPolicyRules | read_only | OK | 9.05 | 13.87 | 9.19 | 25 |
| AuthzService | ListPolicyVersions | list_policy_versions | listPolicyVersions | read_only | OK | 16.95 | 25.82 | 17.98 | 25 |
| AuthzService | ListRoles | list_roles | listRoles | read_only | OK | 13.15 | 21.44 | 13.44 | 25 |
| AuthzService | ListUserPermissions | list_user_permissions | listUserPermissions | read_only | OK | 5.46 | 11.23 | 5.85 | 25 |
| AuthzService | ListUserRoles | list_user_roles | listUserRoles | read_only | OK | 9.22 | 14.06 | 9.83 | 25 |
| AuthzService | MigrateLegacyPolicies | migrate_legacy_policies | migrateLegacyPolicies | destructive | OK | 237.86 | 237.86 | 237.86 | 1 |
| AuthzService | PromoteCanary | promote_canary | promoteCanary | destructive | OK | 173.67 | 173.67 | 173.67 | 1 |
| AuthzService | PutAuthzPolicy | put_authz_policy | putAuthzPolicy | mutation | OK | 39.36 | 43.69 | 39.23 | 5 |
| AuthzService | PutRelationship | put_relationship | putRelationship | mutation | OK | 35.10 | 39.34 | 35.83 | 5 |
| AuthzService | PutRoleBinding | put_role_binding | putRoleBinding | mutation | OK | 31.18 | 48.70 | 58.07 | 5 |
| AuthzService | RejectPolicyDraft | reject_policy_draft | rejectPolicyDraft | mutation | OK | 46.82 | 46.82 | 46.82 | 1 |
| AuthzService | RevokeRole | revoke_role | revokeRole | mutation | OK | 12.26 | 13.20 | 24.60 | 5 |
| AuthzService | RollbackPolicyVersion | rollback_policy_version | rollbackPolicyVersion | destructive | OK | 96.74 | 96.74 | 96.74 | 1 |
| AuthzService | SeedBuiltinRoles | seed_builtin_roles | seedBuiltinRoles | mutation | OK | 59.46 | 84.11 | 79.13 | 5 |
| AuthzService | SimulatePolicy | simulate_policy | simulatePolicy | mutation | OK | 24.14 | 25.23 | 53.79 | 5 |
| AuthzService | SubmitPolicyDraft | submit_policy_draft | submitPolicyDraft | mutation | OK | 29.60 | 29.60 | 29.60 | 1 |
| AuthzService | UpdatePolicyDraft | update_policy_draft | updatePolicyDraft | mutation | OK | 53.84 | 71.73 | 56.03 | 5 |
| AuthzService | UpdateRole | update_role | updateRole | mutation | OK | 48.16 | 50.82 | 48.99 | 5 |
| BackupService | DeleteBackupPolicy | delete_backup_policy | deleteBackupPolicy | mutation | OK | 21.88 | 38.33 | 45.52 | 5 |
| BackupService | GetBackup | get_backup | getBackup | read_only | OK | 43.35 | 58.82 | 43.02 | 25 |
| BackupService | GetBackupPolicy | get_backup_policy | getBackupPolicy | read_only | OK | 29.59 | 40.62 | 30.26 | 25 |
| BackupService | ListBackupPolicies | list_backup_policies | listBackupPolicies | read_only | OK | 21.09 | 40.87 | 24.05 | 25 |
| BackupService | ListBackups | list_backups | listBackups | read_only | OK | 20.41 | 41.17 | 23.30 | 25 |
| BackupService | PutBackupPolicy | put_backup_policy | putBackupPolicy | mutation | OK | 39.31 | 53.97 | 43.03 | 5 |
| BackupService | RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 2173.71 | 2173.71 | 2173.71 | 1 |
| BackupService | StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 2050.55 | 2195.67 | 2009.45 | 5 |
| CacheService | CreateNamespace | create_cache_namespace | createCacheNamespace | mutation | OK | 34.80 | 46.87 | 50.42 | 5 |
| CacheService | Delete | cache_delete | cacheNamespaceDelete | mutation | OK | 14.50 | 31.10 | 37.70 | 5 |
| CacheService | DeleteNamespace | delete_cache_namespace | deleteCacheNamespace | destructive | OK | 247.09 | 247.09 | 247.09 | 1 |
| CacheService | Get | cache_get | cacheNamespaceGet | read_only | OK | 17.07 | 26.77 | 18.32 | 25 |
| CacheService | GetNamespaceStats | get_cache_namespace_stats | getCacheNamespaceStats | read_only | OK | 46.97 | 69.02 | 49.60 | 25 |
| CacheService | Scan | cache_scan | cacheNamespaceScan | read_only | OK | 16.90 | 29.38 | 18.51 | 25 |
| CacheService | Set | cache_set | cacheNamespaceSet | mutation | OK | 31.43 | 35.47 | 51.11 | 5 |
| ConfigService | DeleteFlag | delete_flag | deleteFlag | destructive | OK | 198.78 | 198.78 | 198.78 | 1 |
| ConfigService | EvaluateFlags | evaluate_flags | evaluateFlags | read_only | OK | 19.49 | 33.00 | 20.86 | 25 |
| ConfigService | GetFlag | get_flag | getFlag | read_only | OK | 20.43 | 39.91 | 22.79 | 25 |
| ConfigService | ListFlags | list_flags | listFlags | read_only | OK | 19.39 | 35.50 | 21.98 | 25 |
| ConfigService | PutFlag | put_flag | putFlag | mutation | OK | 58.96 | 72.34 | 62.62 | 5 |
| ControlPlaneService | AckStatus | ack_status | ackStatus | mutation | OK | 20.77 | 20.97 | 34.87 | 5 |
| ControlPlaneService | DeltaResources | delta_resources | deltaResources | stream_open | OK | 0.23 | 0.23 | 0.23 | 1 |
| ControlPlaneService | GetResources | get_resources | getResources | read_only | OK | 8.02 | 9.72 | 7.93 | 25 |
| ControlPlaneService | ListNodeStates | list_node_states | listNodeStates | read_only | OK | 58.98 | 92.91 | 61.43 | 25 |
| ControlPlaneService | RollbackResources | rollback_resources | rollbackResources | mutation | OK | 77.56 | 77.91 | 102.33 | 5 |
| ControlPlaneService | StreamResources | stream_resources | streamResources | stream_open | OK | 0.38 | 0.38 | 0.38 | 1 |
| DataBroker | ActivateCatalog | activate_catalog | activateCatalog | destructive | OK | 168.78 | 168.78 | 168.78 | 1 |
| DataBroker | AnalyticalQuery | analytical_query | analyticalQuery | read_only | OK | 17.52 | 22.97 | 17.20 | 25 |
| DataBroker | ApplyMigration | apply_migration | applyMigration | mutation | OK | 578.33 | 578.33 | 578.33 | 1 |
| DataBroker | ApproveMigrationPlan | approve_migration_plan | approveMigrationPlan | mutation | OK | 55.88 | 55.88 | 55.88 | 1 |
| DataBroker | BatchSelect | batch_select | batchSelect | stream_open | OK | 0.36 | 0.36 | 0.36 | 1 |
| DataBroker | BatchUpsert | batch_upsert | batchUpsert | stream_open | OK | 0.24 | 0.24 | 0.24 | 1 |
| DataBroker | BeginTx | begin_tx | beginTx | stream_open | OK | 1.53 | 1.53 | 1.53 | 1 |
| DataBroker | CacheDelete | cache_delete | cacheDelete | mutation | OK | 11.59 | 12.65 | 11.33 | 5 |
| DataBroker | CacheGet | cache_get | cacheGet | read_only | OK | 10.64 | 14.15 | 10.33 | 25 |
| DataBroker | CacheScan | cache_scan | cacheScan | read_only | OK | 13.96 | 19.10 | 14.61 | 25 |
| DataBroker | CacheSet | cache_set | cacheSet | mutation | OK | 8.96 | 10.99 | 10.27 | 5 |
| DataBroker | CreateMaterializedView | create_materialized_view | createMaterializedView | mutation | OK | 12.76 | 17.07 | 14.55 | 5 |
| DataBroker | Delete | delete | delete | mutation | OK | 46.13 | 47.31 | 43.89 | 5 |
| DataBroker | DeletePolicy | delete_policy | deletePolicy | mutation | OK | 39.37 | 39.37 | 39.37 | 1 |
| DataBroker | DismissDlqEvent | dismiss_dlq_event | dismissDlqEvent | mutation | OK | 34.02 | 34.83 | 35.44 | 5 |
| DataBroker | DocumentDelete | document_delete | documentDelete | mutation | OK | 8.66 | 9.02 | 11.25 | 5 |
| DataBroker | DocumentFind | document_find | documentFind | read_only | OK | 7.93 | 12.70 | 8.41 | 25 |
| DataBroker | DocumentGet | document_get | documentGet | read_only | OK | 9.53 | 15.35 | 10.22 | 25 |
| DataBroker | DocumentUpsert | document_upsert | documentUpsert | mutation | OK | 9.68 | 9.93 | 11.29 | 5 |
| DataBroker | DropResource | drop_resource | dropResource | destructive | OK | 37.65 | 37.65 | 37.65 | 1 |
| DataBroker | EnqueueOutboxEvent | enqueue_outbox_event | enqueueOutboxEvent | mutation | OK | 94.28 | 94.28 | 94.28 | 1 |
| DataBroker | EnsureBaseline | ensure_baseline | ensureBaseline | mutation | OK | 33.98 | 36.90 | 38.07 | 5 |
| DataBroker | EnsureProject | ensure_project | ensureProject | mutation | OK | 24.98 | 25.42 | 22.85 | 5 |
| DataBroker | EnsureResource | ensure_resource | ensureResource | mutation | OK | 40.23 | 43.47 | 43.27 | 5 |
| DataBroker | GeneratePresignedUrl | generate_presigned_url | generatePresignedUrl | mutation | OK | 8.38 | 8.51 | 25.81 | 5 |
| DataBroker | GenericDispatch | generic_dispatch | genericDispatch | mutation | OK | 9.23 | 10.82 | 26.95 | 5 |
| DataBroker | GetAdminSummary | get_admin_summary | getAdminSummary | read_only | OK | 31.07 | 37.74 | 30.96 | 25 |
| DataBroker | GetCapabilities | get_capabilities | getCapabilities | read_only | OK | 16.07 | 21.67 | 16.34 | 25 |
| DataBroker | GetCatalogManifest | get_catalog_manifest | getCatalogManifest | read_only | OK | 92.93 | 115.23 | 93.34 | 25 |
| DataBroker | GetCatalogVersion | get_catalog_version | getCatalogVersion | read_only | OK | 6.70 | 16.19 | 8.55 | 25 |
| DataBroker | GetCatalogVersions | get_catalog_versions | getCatalogVersions | read_only | OK | 12.73 | 20.49 | 12.93 | 25 |
| DataBroker | GetCdcStatus | get_cdc_status | getCdcStatus | read_only | OK | 8.85 | 14.01 | 8.82 | 25 |
| DataBroker | GetDlqEvent | get_dlq_event | getDlqEvent | read_only | OK | 14.57 | 19.22 | 14.53 | 25 |
| DataBroker | GetHealthReport | get_health_report | getHealthReport | read_only | OK | 8.46 | 10.73 | 8.89 | 25 |
| DataBroker | GetMigrationStatus | get_migration_status | getMigrationStatus | read_only | OK | 7.50 | 10.66 | 7.71 | 25 |
| DataBroker | GetObject | get_object | getObject | stream_open | OK | 0.39 | 0.39 | 0.39 | 1 |
| DataBroker | GetSaga | get_saga | getSaga | read_only | OK | 8.20 | 10.16 | 8.12 | 25 |
| DataBroker | GraphMutate | graph_mutate | graphMutate | mutation | OK | 54.67 | 73.58 | 101.15 | 5 |
| DataBroker | GraphQuery | graph_query | graphQuery | read_only | OK | 31.97 | 45.83 | 33.15 | 25 |
| DataBroker | InitiateMultipartUpload | initiate_multipart_upload | initiateMultipartUpload | mutation | OK | 13.71 | 13.86 | 31.30 | 5 |
| DataBroker | LintPolicies | lint_policies | lintPolicies | read_only | OK | 8.92 | 11.34 | 9.07 | 25 |
| DataBroker | ListAdminAuditLogs | list_admin_audit_logs | listAdminAuditLogs | read_only | OK | 22.74 | 30.74 | 22.66 | 25 |
| DataBroker | ListDlqEvents | list_dlq_events | listDlqEvents | read_only | OK | 13.48 | 19.85 | 13.94 | 25 |
| DataBroker | ListMessageSchemas | list_message_schemas | listMessageSchemas | read_only | OK | 4.90 | 6.35 | 5.01 | 25 |
| DataBroker | ListMigrationRuns | list_migration_runs | listMigrationRuns | read_only | OK | 11.09 | 19.44 | 11.53 | 25 |
| DataBroker | ListPolicies | list_policies | listPolicies | read_only | OK | 9.38 | 12.41 | 9.32 | 25 |
| DataBroker | ListProjects | list_projects | listProjects | read_only | OK | 8.34 | 12.10 | 8.63 | 25 |
| DataBroker | ListResources | list_resources | listResources | read_only | OK | 8.34 | 16.10 | 9.65 | 25 |
| DataBroker | ListSagas | list_sagas | listSagas | read_only | OK | 9.95 | 14.72 | 10.69 | 25 |
| DataBroker | LookupMessageSchema | lookup_message_schema | lookupMessageSchema | read_only | OK | 5.47 | 7.94 | 5.81 | 25 |
| DataBroker | MarkSagaReviewed | mark_saga_reviewed | markSagaReviewed | mutation | OK | 27.54 | 37.73 | 30.73 | 5 |
| DataBroker | PauseCdc | pause_cdc | pauseCdc | mutation | OK | 34.58 | 38.39 | 34.49 | 5 |
| DataBroker | PlanMigration | plan_migration | planMigration | mutation | OK | 37.45 | 38.52 | 37.70 | 5 |
| DataBroker | PreviewCdcRedaction | preview_cdc_redaction | previewCdcRedaction | read_only | OK | 26.26 | 41.62 | 26.87 | 25 |
| DataBroker | PublishCDC | publish_cdc | publishCdc | stream_open | OK | 0.83 | 0.83 | 0.83 | 1 |
| DataBroker | PutObject | put_object | putObject | mutation | OK | 30.72 | 30.72 | 30.91 | 3 |
| DataBroker | PutPolicy | put_policy | putPolicy | destructive | OK | 34.46 | 34.46 | 34.46 | 1 |
| DataBroker | QuarantineDlqEvent | quarantine_dlq_event | quarantineDlqEvent | mutation | OK | 43.50 | 45.96 | 43.45 | 5 |
| DataBroker | ReloadPolicies | reload_policies | reloadPolicies | destructive | OK | 37.80 | 37.80 | 37.80 | 1 |
| DataBroker | ReplayDlqEvent | replay_dlq_event | replayDlqEvent | mutation | OK | 51.84 | 51.84 | 51.84 | 1 |
| DataBroker | ResumeCdc | resume_cdc | resumeCdc | mutation | OK | 27.56 | 30.94 | 27.81 | 5 |
| DataBroker | RetrySagaCompensation | retry_saga_compensation | retrySagaCompensation | mutation | OK | 21.16 | 21.16 | 21.16 | 1 |
| DataBroker | RollbackCatalog | rollback_catalog | rollbackCatalog | destructive | OK | 18.08 | 18.08 | 18.08 | 1 |
| DataBroker | ScanProjectionDrift | scan_projection_drift | scanProjectionDrift | read_only | OK | 20.23 | 40.57 | 22.63 | 25 |
| DataBroker | Select | select | select | read_only | OK | 16.90 | 21.08 | 16.59 | 25 |
| DataBroker | SelectV2 | select_v_2 | selectV2 | stream_open | OK | 1.37 | 1.37 | 1.37 | 1 |
| DataBroker | StageCatalog | stage_catalog | stageCatalog | destructive | OK | 857.40 | 857.40 | 857.40 | 1 |
| DataBroker | StepDownCdcLeader | step_down_cdc_leader | stepDownCdcLeader | mutation | OK | 22.65 | 38.92 | 29.69 | 5 |
| DataBroker | TimeSeriesQuery | time_series_query | timeSeriesQuery | read_only | OK | 24.01 | 30.40 | 23.81 | 25 |
| DataBroker | TimeSeriesWrite | time_series_write | timeSeriesWrite | mutation | OK | 75.58 | 76.50 | 84.88 | 5 |
| DataBroker | Upsert | upsert | upsert | mutation | OK | 63.54 | 68.22 | 63.27 | 5 |
| DataBroker | ValidateCatalog | validate_catalog | validateCatalog | destructive | OK | 276.96 | 276.96 | 276.96 | 1 |
| DataBroker | VectorBatchUpsert | vector_batch_upsert | vectorBatchUpsert | stream_open | OK | 0.35 | 0.35 | 0.35 | 1 |
| DataBroker | VectorHybridSearch | vector_hybrid_search | vectorHybridSearch | read_only | OK | 7.53 | 11.18 | 7.91 | 25 |
| DataBroker | VectorSearch | vector_search | vectorSearch | read_only | OK | 6.93 | 10.84 | 7.52 | 25 |
| DataBroker | VectorUpsert | vector_upsert | vectorUpsert | mutation | OK | 19.00 | 19.84 | 22.98 | 5 |
| DataBroker | VerifyAdminAuditLog | verify_admin_audit_log | verifyAdminAuditLog | read_only | OK | 10.67 | 17.34 | 11.43 | 25 |
| EmbeddingService | Backfill | backfill | backfillEmbeddingSource | mutation | OK | 45.58 | 50.43 | 59.79 | 5 |
| EmbeddingService | DeleteSource | delete_source | deleteEmbeddingSource | destructive | OK | 169.36 | 169.36 | 169.36 | 1 |
| EmbeddingService | ListSources | list_sources | listEmbeddingSources | read_only | OK | 28.22 | 40.80 | 28.57 | 25 |
| EmbeddingService | RegisterSource | register_source | registerEmbeddingSource | mutation | OK | 25.69 | 30.01 | 27.82 | 5 |
| EmbeddingService | ReportEmbedding | report_embedding | reportEmbedding | mutation | OK | 39.81 | 40.21 | 32.95 | 5 |
| EmbeddingService | Retrieve | retrieve | retrieveEmbedding | read_only | OK | 32.66 | 44.83 | 33.21 | 25 |
| IdentityProviderService | CreateProvider | create_provider | createProvider | mutation | OK | 21.14 | 21.14 | 21.14 | 1 |
| IdentityProviderService | DisableProvider | disable_provider | disableProvider | mutation | OK | 46.10 | 61.22 | 56.02 | 5 |
| IdentityProviderService | ForceJwksRefresh | force_jwks_refresh | forceJwksRefresh | mutation | OK | 49.37 | 54.32 | 49.53 | 5 |
| IdentityProviderService | GetProvider | get_provider | getProvider | read_only | OK | 12.76 | 21.43 | 13.49 | 25 |
| IdentityProviderService | ImportSamlMetadata | import_saml_metadata | importSamlMetadata | mutation | OK | 46.86 | 48.97 | 40.09 | 5 |
| IdentityProviderService | LinkIdentity | link_identity | linkIdentity | mutation | OK | 48.51 | 50.51 | 67.30 | 5 |
| IdentityProviderService | ListExternalIdentities | list_external_identities | listExternalIdentities | read_only | OK | 12.30 | 22.41 | 13.86 | 25 |
| IdentityProviderService | ListProviders | list_providers | listProviders | read_only | OK | 16.17 | 19.35 | 16.47 | 25 |
| IdentityProviderService | PreviewClaimMapping | preview_claim_mapping | previewClaimMapping | read_only | OK | 9.85 | 12.49 | 9.41 | 25 |
| IdentityProviderService | PreviewGroupMapping | preview_group_mapping | previewGroupMapping | read_only | OK | 9.79 | 13.46 | 9.55 | 25 |
| IdentityProviderService | ResolveExternalIdentity | resolve_external_identity | resolveExternalIdentity | mutation | OK | 12.15 | 24.28 | 46.98 | 5 |
| IdentityProviderService | SamlAcs | saml_acs | samlAcs | mutation | OK | 97.48 | 104.04 | 126.39 | 5 |
| IdentityProviderService | ScimCreateGroup | scim_create_group | scimCreateGroup | mutation | OK | 7.63 | 8.45 | 7.85 | 5 |
| IdentityProviderService | ScimCreateUser | scim_create_user | scimCreateUser | mutation | OK | 53.41 | 54.93 | 49.50 | 5 |
| IdentityProviderService | ScimDeleteGroup | scim_delete_group | scimDeleteGroup | mutation | OK | 9.55 | 10.25 | 32.93 | 5 |
| IdentityProviderService | ScimDeleteUser | scim_delete_user | scimDeleteUser | mutation | OK | 344.79 | 344.79 | 344.79 | 1 |
| IdentityProviderService | ScimGetGroup | scim_get_group | scimGetGroup | mutation | OK | 15.36 | 18.00 | 44.23 | 5 |
| IdentityProviderService | ScimGetUser | scim_get_user | scimGetUser | mutation | OK | 13.23 | 22.02 | 114.98 | 5 |
| IdentityProviderService | ScimListGroups | scim_list_groups | scimListGroups | mutation | OK | 7.53 | 7.62 | 29.68 | 5 |
| IdentityProviderService | ScimListUsers | scim_list_users | scimListUsers | mutation | OK | 17.47 | 29.16 | 131.33 | 5 |
| IdentityProviderService | ScimPatchGroup | scim_patch_group | scimPatchGroup | mutation | OK | 18.55 | 18.65 | 44.65 | 5 |
| IdentityProviderService | ScimPatchUser | scim_patch_user | scimPatchUser | mutation | OK | 38.63 | 50.01 | 127.54 | 5 |
| IdentityProviderService | ScimReplaceUser | scim_replace_user | scimReplaceUser | mutation | OK | 33.14 | 47.15 | 163.70 | 5 |
| IdentityProviderService | StartSamlLogin | start_saml_login | startSamlLogin | mutation | OK | 10.36 | 13.60 | 41.00 | 5 |
| IdentityProviderService | TestProviderDiscovery | test_provider_discovery | testProviderDiscovery | read_only | OK | 9.50 | 12.67 | 9.89 | 25 |
| IdentityProviderService | UnlinkIdentity | unlink_identity | unlinkIdentity | mutation | OK | 6.21 | 8.84 | 27.68 | 5 |
| IdentityProviderService | UpdateProvider | update_provider | updateProvider | mutation | OK | 52.69 | 58.08 | 51.48 | 5 |
| LiveQueryService | Subscribe | subscribe | liveQuerySubscribe | stream_open | OK | 2.64 | 2.64 | 2.64 | 1 |
| LockService | AcquireLock | acquire_lock | acquireLock | mutation | OK | 86.92 | 88.97 | 88.62 | 5 |
| LockService | GetLock | get_lock | getLock | read_only | OK | 20.01 | 34.23 | 20.92 | 25 |
| LockService | ListLocks | list_locks | listLocks | read_only | OK | 17.16 | 29.41 | 19.15 | 25 |
| LockService | ReleaseLock | release_lock | releaseLock | mutation | OK | 22.93 | 34.31 | 114.23 | 5 |
| LockService | RenewLock | renew_lock | renewLock | mutation | OK | 112.50 | 253.52 | 160.20 | 5 |
| MeteringService | CheckQuota | check_quota | checkQuota | read_only | OK | 26.60 | 45.76 | 28.81 | 25 |
| MeteringService | GetQuota | get_quota | getQuota | read_only | OK | 23.44 | 38.25 | 24.64 | 25 |
| MeteringService | ListQuotas | list_quotas | listQuotas | read_only | OK | 22.24 | 39.01 | 22.41 | 25 |
| MeteringService | PutQuota | put_quota | putQuota | mutation | OK | 52.32 | 64.95 | 59.68 | 5 |
| MeteringService | QueryUsage | query_usage | queryUsage | read_only | OK | 22.29 | 41.56 | 23.59 | 25 |
| MeteringService | RecordUsage | record_usage | recordUsage | mutation | OK | 14.91 | 30.40 | 45.45 | 5 |
| NotificationService | GetDeliveryStats | get_delivery_stats | getDeliveryStats | read_only | OK | 13.97 | 26.45 | 15.80 | 25 |
| NotificationService | GetNotification | get_notification | getNotification | read_only | OK | 21.91 | 29.00 | 22.09 | 25 |
| NotificationService | GetPreference | get_preference | getPreference | read_only | OK | 16.73 | 28.10 | 17.81 | 25 |
| NotificationService | GetTemplate | get_template | getTemplate | read_only | OK | 22.83 | 42.11 | 25.17 | 25 |
| NotificationService | ListNotifications | list_notifications | listNotifications | read_only | OK | 32.32 | 55.28 | 34.99 | 25 |
| NotificationService | ListPreferences | list_preferences | listPreferences | read_only | OK | 24.08 | 37.44 | 25.01 | 25 |
| NotificationService | ListTemplates | list_templates | listTemplates | read_only | OK | 26.20 | 43.49 | 27.45 | 25 |
| NotificationService | ReportDelivery | report_delivery | reportDelivery | mutation | OK | 42.33 | 60.18 | 74.30 | 5 |
| NotificationService | RetryNotification | retry_notification | retryNotification | mutation | OK | 127.82 | 127.82 | 127.82 | 1 |
| NotificationService | SendNotification | send_notification | sendNotification | mutation | OK | 84.96 | 111.33 | 92.72 | 5 |
| NotificationService | SetPreference | set_preference | setPreference | mutation | OK | 30.84 | 52.13 | 48.54 | 5 |
| NotificationService | UpsertTemplate | upsert_template | upsertTemplate | mutation | OK | 10.19 | 11.24 | 11.51 | 5 |
| PeerService | GetPeer | get_peer | getPeer | read_only | OK | 15.47 | 24.54 | 15.99 | 25 |
| PeerService | JoinRoom | join_room | joinRoom | mutation | OK | 27.44 | 27.83 | 26.83 | 5 |
| PeerService | JoinSession | join_session | joinSession | mutation | OK | 32.20 | 40.26 | 83.73 | 5 |
| PeerService | LeaveRoom | leave_room | leaveRoom | mutation | OK | 11.43 | 11.59 | 30.59 | 5 |
| PeerService | ListPeers | list_peers | listPeers | read_only | OK | 14.26 | 22.70 | 15.34 | 25 |
| RoomService | CloseRoom | close_room | closeRoom | mutation | OK | 37.84 | 44.15 | 68.67 | 5 |
| RoomService | CreateRoom | create_room | createRoom | mutation | OK | 24.60 | 24.76 | 25.36 | 5 |
| RoomService | GetRoom | get_room | getRoom | read_only | OK | 21.45 | 36.19 | 22.10 | 25 |
| RoomService | ListEgress | list_egress | listEgress | read_only | CAPABILITY_SKIPPED | 5.87 | 10.73 | 7.08 | 25 |
| RoomService | ListRooms | list_rooms | listRooms | read_only | OK | 12.92 | 17.83 | 13.39 | 25 |
| RoomService | StartRoomComposite | start_room_composite | startRoomComposite | mutation | CAPABILITY_SKIPPED | 9.04 | 9.31 | 8.27 | 5 |
| RoomService | StartTrackEgress | start_track_egress | startTrackEgress | mutation | CAPABILITY_SKIPPED | 8.34 | 8.77 | 8.04 | 5 |
| RoomService | StopEgress | stop_egress | stopEgress | mutation | CAPABILITY_SKIPPED | 7.44 | 7.96 | 7.50 | 5 |
| RoomService | UpdateRoom | update_room | updateRoom | mutation | OK | 13.28 | 24.87 | 31.84 | 5 |
| SchedulerService | CreateJob | create_job | createJob | mutation | OK | 27.59 | 42.01 | 42.14 | 5 |
| SchedulerService | DeleteJob | delete_job | deleteJob | destructive | OK | 151.66 | 151.66 | 151.66 | 1 |
| SchedulerService | GetJob | get_job | getJob | read_only | OK | 13.99 | 21.42 | 15.33 | 25 |
| SchedulerService | ListJobs | list_jobs | listJobs | read_only | OK | 19.14 | 32.13 | 20.28 | 25 |
| SchedulerService | PauseJob | pause_job | pauseJob | mutation | OK | 196.01 | 196.01 | 196.01 | 1 |
| SchedulerService | ResumeJob | resume_job | resumeJob | mutation | OK | 423.74 | 423.74 | 423.74 | 1 |
| SearchService | CreateIndex | create_index | createSearchIndex | mutation | OK | 59.96 | 80.24 | 64.63 | 5 |
| SearchService | DeleteIndex | delete_index | deleteSearchIndex | destructive | OK | 141.48 | 141.48 | 141.48 | 1 |
| SearchService | ListIndexes | list_indexes | listSearchIndexes | read_only | OK | 23.74 | 31.92 | 23.87 | 25 |
| SearchService | Reindex | reindex | reindexSearchIndex | mutation | OK | 55.42 | 66.45 | 65.52 | 5 |
| SearchService | Search | search | search | read_only | OK | 26.66 | 53.55 | 30.25 | 25 |
| SignalingService | Signal | signal | signal | stream_open | OK | 0.36 | 0.36 | 0.36 | 1 |
| StorageService | DeleteFile | delete_file | deleteFile | mutation | OK | 118.27 | 118.27 | 118.27 | 1 |
| StorageService | DownloadFile | download_file | downloadFile | stream_open | OK | 0.32 | 0.32 | 0.32 | 1 |
| StorageService | FinalizeUpload | finalize_upload | finalizeUpload | mutation | OK | 76.04 | 76.04 | 76.04 | 1 |
| StorageService | GetDownloadUrl | get_download_url | getDownloadUrl | read_only | OK | 25.51 | 34.02 | 27.57 | 25 |
| StorageService | GetFile | get_file | getFile | read_only | OK | 15.58 | 34.43 | 18.14 | 25 |
| StorageService | ListFiles | list_files | listFiles | read_only | OK | 31.76 | 43.25 | 31.39 | 25 |
| StorageService | RegisterUpload | register_upload | registerUpload | mutation | OK | 31.78 | 40.93 | 35.05 | 5 |
| StorageService | ReissueUploadUrl | reissue_upload_url | reissueUploadUrl | read_only | OK | 22.28 | 35.70 | 24.03 | 25 |
| StorageService | UpdateFile | update_file | updateFile | mutation | OK | 46.24 | 47.73 | 55.94 | 5 |
| TenantService | CreateTenant | create_tenant | createTenant | mutation | OK | 21.45 | 36.48 | 60.69 | 5 |
| TenantService | GetTenant | get_tenant | getTenant | read_only | OK | 20.50 | 31.90 | 22.46 | 25 |
| TenantService | GetTenantConfig | get_tenant_config | getTenantConfig | read_only | OK | 21.65 | 34.49 | 23.30 | 25 |
| TenantService | ListTenants | list_tenants | listTenants | read_only | OK | 20.10 | 33.77 | 22.20 | 25 |
| TenantService | PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 524.99 | 524.99 | 524.99 | 1 |
| TenantService | UpdateTenant | update_tenant | updateTenant | mutation | OK | 47.26 | 56.11 | 71.32 | 5 |
| TenantService | UpdateTenantConfig | update_tenant_config | updateTenantConfig | mutation | OK | 69.69 | 73.41 | 80.89 | 5 |
| TrackService | ListTracks | list_tracks | listTracks | read_only | OK | 21.04 | 38.41 | 24.19 | 25 |
| TrackService | MuteTrack | mute_track | muteTrack | mutation | OK | 14.92 | 16.68 | 70.97 | 5 |
| TrackService | PublishTrack | publish_track | publishTrack | mutation | OK | 28.86 | 37.22 | 31.50 | 5 |
| TrackService | UnpublishTrack | unpublish_track | unpublishTrack | mutation | OK | 17.00 | 26.57 | 84.71 | 5 |
| TurnService | IssueCredentials | issue_credentials | issueCredentials | mutation | OK | 16.71 | 19.72 | 81.23 | 5 |
| VaultService | BatchDecrypt | batch_decrypt | vaultBatchDecrypt | mutation | OK | 17.92 | 18.17 | 50.99 | 5 |
| VaultService | BatchEncrypt | batch_encrypt | vaultBatchEncrypt | mutation | OK | 26.07 | 29.11 | 65.44 | 5 |
| VaultService | CreateTransitKey | create_transit_key | createTransitKey | mutation | OK | 56.31 | 56.31 | 56.31 | 1 |
| VaultService | Decrypt | decrypt | vaultDecrypt | read_only | OK | 31.67 | 52.38 | 33.10 | 25 |
| VaultService | DeleteSecret | delete_secret | deleteSecret | mutation | OK | 45.68 | 60.53 | 79.68 | 5 |
| VaultService | DestroySecret | destroy_secret | destroySecret | destructive | OK | 223.04 | 223.04 | 223.04 | 1 |
| VaultService | Encrypt | encrypt | vaultEncrypt | mutation | OK | 25.20 | 28.27 | 24.97 | 5 |
| VaultService | GenerateDataKey | generate_data_key | vaultGenerateDataKey | mutation | OK | 36.88 | 44.24 | 63.68 | 5 |
| VaultService | GenerateDatabaseCredentials | generate_database_credentials | generateDatabaseCredentials | mutation | OK | 70.72 | 76.96 | 108.92 | 5 |
| VaultService | GetSecret | get_secret | getSecret | read_only | OK | 26.98 | 38.69 | 27.20 | 25 |
| VaultService | GetTransitPublicKey | get_transit_public_key | vaultGetTransitPublicKey | read_only | OK | 16.78 | 34.42 | 19.06 | 25 |
| VaultService | Hmac | hmac | vaultHmac | mutation | OK | 30.77 | 36.76 | 88.18 | 5 |
| VaultService | ListSecrets | list_secrets | listSecrets | read_only | OK | 29.96 | 44.15 | 30.20 | 25 |
| VaultService | PutSecret | put_secret | putSecret | mutation | OK | 46.06 | 46.06 | 46.06 | 1 |
| VaultService | Rewrap | rewrap | vaultRewrap | mutation | OK | 118.82 | 139.06 | 116.47 | 5 |
| VaultService | RotateTransitKey | rotate_transit_key | rotateTransitKey | mutation | OK | 137.91 | 150.29 | 163.87 | 5 |
| VaultService | SealStatus | seal_status | vaultSealStatus | read_only | OK | 3.99 | 5.80 | 4.18 | 25 |
| VaultService | Sign | sign | vaultSign | mutation | OK | 44.86 | 55.57 | 42.03 | 5 |
| VaultService | UndeleteSecret | undelete_secret | undeleteSecret | mutation | OK | 430.34 | 430.34 | 430.34 | 1 |
| VaultService | Verify | verify | vaultVerify | read_only | OK | 21.57 | 40.16 | 24.26 | 25 |
| WebhookService | CreateEndpoint | create_endpoint | createWebhookEndpoint | mutation | OK | 17.57 | 20.20 | 20.90 | 5 |
| WebhookService | DeleteEndpoint | delete_endpoint | deleteWebhookEndpoint | destructive | OK | 146.16 | 146.16 | 146.16 | 1 |
| WebhookService | GetEndpoint | get_endpoint | getWebhookEndpoint | read_only | OK | 16.54 | 25.77 | 18.37 | 25 |
| WebhookService | ListDeliveries | list_deliveries | listWebhookDeliveries | read_only | OK | 15.83 | 21.36 | 16.06 | 25 |
| WebhookService | ListEndpoints | list_endpoints | listWebhookEndpoints | read_only | OK | 22.32 | 34.98 | 23.36 | 25 |
| WebhookService | UpdateEndpoint | update_endpoint | updateWebhookEndpoint | mutation | OK | 16.63 | 29.44 | 56.18 | 5 |
| WorkflowService | CancelWorkflow | cancel_workflow | cancelWorkflow | destructive | OK | 154.86 | 154.86 | 154.86 | 1 |
| WorkflowService | GetWorkflow | get_workflow | getWorkflow | read_only | OK | 12.13 | 24.39 | 14.98 | 25 |
| WorkflowService | ListWorkflows | list_workflows | listWorkflows | read_only | OK | 18.58 | 44.54 | 21.99 | 25 |
| WorkflowService | SignalWorkflow | signal_workflow | signalWorkflow | mutation | OK | 20.62 | 21.21 | 50.58 | 5 |
| WorkflowService | StartWorkflow | start_workflow | startWorkflow | mutation | OK | 24.17 | 26.13 | 22.92 | 5 |
