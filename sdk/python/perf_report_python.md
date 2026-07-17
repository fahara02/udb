# UDB SDK Live Perf — Python (localhost)

RPCs measured: 353   tenant=0bc1cc07-cc1b-4973-9bb5-2552b532d57c

Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities (a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request's reference/ID fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection latency. The TARGET is zero failures; any residual non-OK RPC is listed under Failures for the maintainer to finish.

Unary = full request/response round-trip. Non-CDC streaming RPCs (kind=stream_first_recv) report time-to-FIRST-RESPONSE with seeded inputs. CDC subscription (kind=cdc_first_event, PublishCDC) reports time-to-FIRST-EVENT: the harness subscribes, fires a real Upsert that flows outbox->CDC->Kafka, and times the first delivered event.

## Seeded fixtures

Captured semantic field -> seeded value keys used to resolve request fields: access_token, action, admin_reset_mfa_user_id, admin_reset_password_user_id, apply_run_id, approval_token, approve_draft_id, approve_run_id, approved_by, asset_id, assigned_by, auth_challenge_id, backup_id, bucket, canary_id, canary_version_id, cancel_workflow_id, catalog_manifest, catalog_manifest_b64, challenge_id, change_status_user_id, close_room_id, code, collection, content_type, created_by, csrf_token, current_password, definition_id, delete_endpoint_id, delete_file_id, delete_policy_id, delete_scim_user_id, deleted_by, device_id, disable_mfa_user_id, disable_provider_id, dismiss_dlq_id, dlq_id, document_id, domain, ds_policy_id, egress_id, email, endpoint_id, event_type, external_identity_id, file_id, file_size_bytes, file_type, filename, finalize_file_id, gov_exp, identifier, instance_id, job_id, join_session_room_id, key_id, kind, leave_peer_id, locale, log_id, mark_saga_id, message_type, migration_id, mongo_collection, name, new_password, node_id, notification_id, object, object_key, otp_code, otp_id, owner_id, password, peer_id, plain_key, policy_draft_id, policy_id, policy_version_id, project, project_id, provider_id, purge_tenant_id, quarantine_dlq_id, recipient_id, record_id, recovery_code, refresh_token, reg_challenge_id, reissue_file_id, reject_draft_id, rejected_by, relation, release_fencing_token, renew_fencing_token, replay_dlq_id, reset_otp_code, reset_otp_id, resource, resource_name, restore_tenant_id, retry_saga_id, revoke_device_id, revoke_key_id, revoke_recovery_user_id, revoked_by, role, role_code, role_id, rollback_policy_set_id, rollback_resource_version, rollback_target_version_id, room_id, saga_id, saml_provider_id, scim_group_id, scim_user_id, send_otp_user_id, session_id, signal_peer_id, stage_name, step_id, subject, tenant, tenant_id, token, topic_pattern, track_id, ts_table, unpublish_track_id, update_draft_id, update_draft_updated_at_unix, update_key_id, updated_by, user_id, user_role_id, username, vault_ciphertext, vault_create_key_name, vault_db_role, vault_delete_secret_path, vault_destroy_secret_path, vault_key_name, vault_put_secret_path, vault_secret_path, vault_signature, vault_signing_key_name, vector_collection, workflow_id

## Per-service mean latency

| Service | RPCs | mean ms |
|---|--:|--:|
| BackupService | 8 | 350.95 |
| AuthnService | 50 | 92.99 |
| TenantService | 7 | 54.71 |
| ControlPlaneService | 6 | 53.54 |
| LockService | 5 | 42.97 |
| DataBroker | 77 | 31.76 |
| SearchService | 5 | 28.45 |
| AuthzService | 41 | 28.08 |
| LiveQueryService | 1 | 27.98 |
| WorkflowService | 5 | 26.30 |
| EmbeddingService | 6 | 26.15 |
| StorageService | 9 | 25.16 |
| PeerService | 5 | 24.70 |
| VaultService | 20 | 24.57 |
| SignalingService | 1 | 24.39 |
| WebhookService | 6 | 24.31 |
| ConfigService | 5 | 23.40 |
| AssetService | 8 | 23.14 |
| IdentityProviderService | 27 | 21.83 |
| CacheService | 7 | 20.88 |
| NotificationService | 12 | 20.32 |
| TurnService | 1 | 19.86 |
| ApiKeyService | 9 | 18.72 |
| TrackService | 4 | 18.47 |
| AnalyticsService | 7 | 17.36 |
| MeteringService | 6 | 17.27 |
| SchedulerService | 6 | 14.87 |
| RoomService | 9 | 14.62 |

## Failures (0)

No RPC returned a non-OK gRPC status.

## Capability Skips (4)

These RPCs reached the served path but require an optional backend capability disabled in this local profile.

| RPC | api_alias | operation_id | kind | detail |
|---|---|---|---|---|
| RoomService/ListEgress | list_egress | listEgress | read_only | webrtc egress is not enabled; set UDB_WEBRTC_EGRESS_ENABLED=1 and wire an egress backend |
| RoomService/StartRoomComposite | start_room_composite | startRoomComposite | mutation | webrtc egress is not enabled; set UDB_WEBRTC_EGRESS_ENABLED=1 and wire an egress backend |
| RoomService/StartTrackEgress | start_track_egress | startTrackEgress | mutation | webrtc egress is not enabled; set UDB_WEBRTC_EGRESS_ENABLED=1 and wire an egress backend |
| RoomService/StopEgress | stop_egress | stopEgress | mutation | webrtc egress is not enabled; set UDB_WEBRTC_EGRESS_ENABLED=1 and wire an egress backend |

## Slowest 20 by p99

| RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms |
|---|---|---|---|---|--:|--:|--:|
| AuthnService/ChangePassword | change_password | changePassword | mutation | OK | 1501.92 | 1501.92 | 1501.92 |
| BackupService/RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 1391.34 | 1391.34 | 1391.34 |
| BackupService/StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 1308.94 | 1308.94 | 1276.82 |
| AuthnService/ResetPassword | reset_password | resetPassword | mutation | OK | 807.04 | 807.04 | 807.04 |
| AuthnService/Login | login | login | mutation | OK | 773.73 | 773.73 | 754.04 |
| AuthnService/CreateUser | create_user | createUser | mutation | OK | 661.14 | 661.14 | 691.58 |
| DataBroker/StageCatalog | stage_catalog | stageCatalog | destructive | OK | 650.51 | 650.51 | 650.51 |
| TenantService/PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 277.35 | 277.35 | 277.35 |
| DataBroker/ApplyMigration | apply_migration | applyMigration | mutation | OK | 208.22 | 208.22 | 208.22 |
| DataBroker/ActivateCatalog | activate_catalog | activateCatalog | destructive | OK | 126.01 | 126.01 | 126.01 |
| IdentityProviderService/SamlAcs | saml_acs | samlAcs | mutation | OK | 123.94 | 123.94 | 121.19 |
| DataBroker/ValidateCatalog | validate_catalog | validateCatalog | destructive | OK | 115.51 | 115.51 | 115.51 |
| DataBroker/PublishCDC | publish_cdc | publishCdc | cdc_first_event | OK | 110.42 | 110.42 | 110.42 |
| ControlPlaneService/StreamResources | stream_resources | streamResources | stream_first_recv | OK | 95.04 | 95.04 | 89.96 |
| AuthzService/ActivatePolicyVersion | activate_policy_version | activatePolicyVersion | destructive | OK | 92.03 | 92.03 | 92.03 |
| ControlPlaneService/RollbackResources | rollback_resources | rollbackResources | mutation | OK | 90.04 | 90.04 | 84.32 |
| AuthzService/PromoteCanary | promote_canary | promoteCanary | destructive | OK | 83.57 | 83.57 | 83.57 |
| DataBroker/TimeSeriesWrite | time_series_write | timeSeriesWrite | mutation | OK | 71.90 | 71.90 | 71.00 |
| LockService/AcquireLock | acquire_lock | acquireLock | mutation | OK | 70.02 | 70.02 | 80.15 |
| AuthzService/SeedBuiltinRoles | seed_builtin_roles | seedBuiltinRoles | mutation | OK | 69.69 | 69.69 | 75.67 |

## Full per-RPC table (sorted by service, then RPC)

| Service | RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms | iters |
|---|---|---|---|---|---|--:|--:|--:|--:|
| AnalyticsService | GetExecutorPerformance | get_executor_performance | getExecutorPerformance | read_only | OK | 19.57 | 32.49 | 22.21 | 10 |
| AnalyticsService | GetPipelineSummary | get_pipeline_summary | getPipelineSummary | read_only | OK | 19.05 | 30.11 | 21.59 | 10 |
| AnalyticsService | GetReconciliationAnalytics | get_reconciliation_analytics | getReconciliationAnalytics | read_only | OK | 13.23 | 15.47 | 14.29 | 10 |
| AnalyticsService | GetSlaCompliance | get_sla_compliance | getSlaCompliance | read_only | OK | 13.55 | 15.82 | 13.76 | 10 |
| AnalyticsService | GetThroughput | get_throughput | getThroughput | read_only | OK | 10.96 | 12.35 | 11.30 | 10 |
| AnalyticsService | RecordPipelineMetric | record_pipeline_metric | recordPipelineMetric | mutation | OK | 17.11 | 17.11 | 16.59 | 3 |
| AnalyticsService | TriggerSnapshot | trigger_snapshot | triggerSnapshot | mutation | OK | 21.00 | 21.00 | 21.82 | 3 |
| ApiKeyService | CreateApiKey | create_api_key | createApiKey | mutation | OK | 23.13 | 23.13 | 22.00 | 3 |
| ApiKeyService | EmergencyRevokeApiKeys | emergency_revoke_api_keys | emergencyRevokeApiKeys | destructive | OK | 31.36 | 31.36 | 31.36 | 1 |
| ApiKeyService | GetApiKey | get_api_key | getApiKey | read_only | OK | 6.56 | 8.89 | 7.16 | 10 |
| ApiKeyService | GetApiKeyUsageStats | get_api_key_usage_stats | getApiKeyUsageStats | read_only | OK | 7.56 | 9.44 | 7.76 | 10 |
| ApiKeyService | ListApiKeys | list_api_keys | listApiKeys | read_only | OK | 7.66 | 9.01 | 7.96 | 10 |
| ApiKeyService | RevokeApiKey | revoke_api_key | revokeApiKey | mutation | OK | 24.97 | 24.97 | 24.97 | 3 |
| ApiKeyService | RotateApiKey | rotate_api_key | rotateApiKey | mutation | OK | 35.73 | 35.73 | 35.73 | 3 |
| ApiKeyService | UpdateApiKey | update_api_key | updateApiKey | mutation | OK | 20.13 | 20.13 | 19.49 | 3 |
| ApiKeyService | ValidateApiKey | validate_api_key | validateApiKey | read_only | OK | 11.63 | 13.87 | 12.06 | 10 |
| AssetService | CompleteStep | complete_step | completeStep | mutation | OK | 47.66 | 47.66 | 48.64 | 3 |
| AssetService | CreatePipelineDefinition | create_pipeline_definition | createPipelineDefinition | mutation | OK | 15.54 | 15.54 | 15.54 | 3 |
| AssetService | GetAsset | get_asset | getAsset | read_only | OK | 14.86 | 17.48 | 16.64 | 10 |
| AssetService | GetPipeline | get_pipeline | getPipeline | read_only | OK | 14.12 | 29.87 | 18.50 | 10 |
| AssetService | GetPipelineDefinition | get_pipeline_definition | getPipelineDefinition | read_only | OK | 15.19 | 17.26 | 16.38 | 10 |
| AssetService | ListAssets | list_assets | listAssets | read_only | OK | 17.98 | 19.96 | 18.40 | 10 |
| AssetService | RegisterAsset | register_asset | registerAsset | mutation | OK | 25.14 | 25.14 | 25.55 | 3 |
| AssetService | StartPipeline | start_pipeline | startPipeline | mutation | OK | 13.17 | 13.17 | 25.47 | 3 |
| AuthnService | AdminResetMfa | admin_reset_mfa | adminResetMfa | destructive | OK | 46.65 | 46.65 | 46.65 | 1 |
| AuthnService | AdminResetPassword | admin_reset_password | adminResetPassword | destructive | OK | 9.09 | 9.09 | 9.09 | 1 |
| AuthnService | AdminRevokeAllTenantSessions | admin_revoke_all_tenant_sessions | adminRevokeAllTenantSessions | destructive | OK | 22.19 | 22.19 | 22.19 | 1 |
| AuthnService | AdminRevokeAllUserSessions | admin_revoke_all_user_sessions | adminRevokeAllUserSessions | destructive | OK | 14.00 | 14.00 | 14.00 | 1 |
| AuthnService | AdminRevokeSession | admin_revoke_session | adminRevokeSession | destructive | OK | 18.05 | 18.05 | 18.05 | 1 |
| AuthnService | Authenticate | authenticate | authenticate | read_only | OK | 58.83 | 58.83 | 58.83 | 1 |
| AuthnService | ChangePassword | change_password | changePassword | mutation | OK | 1501.92 | 1501.92 | 1501.92 | 1 |
| AuthnService | ChangeUserStatus | change_user_status | changeUserStatus | destructive | OK | 16.19 | 16.19 | 16.19 | 1 |
| AuthnService | ConfirmMFAEnrollment | confirm_mfaenrollment | confirmMfaenrollment | mutation | OK | 4.94 | 4.94 | 5.19 | 3 |
| AuthnService | CreateSession | create_session | createSession | mutation | OK | 9.51 | 9.51 | 8.95 | 3 |
| AuthnService | CreateUser | create_user | createUser | mutation | OK | 661.14 | 661.14 | 691.58 | 3 |
| AuthnService | DeleteWebAuthnCredential | delete_web_authn_credential | deleteWebAuthnCredential | mutation | OK | 14.68 | 14.68 | 15.12 | 3 |
| AuthnService | DisableMfaFactor | disable_mfa_factor | disableMfaFactor | mutation | OK | 16.87 | 16.87 | 16.69 | 3 |
| AuthnService | EmergencyRevoke | emergency_revoke | emergencyRevoke | destructive | OK | 16.37 | 16.37 | 16.37 | 1 |
| AuthnService | EnrollMFA | enroll_mfa | enrollMfa | mutation | OK | 17.79 | 17.79 | 19.79 | 3 |
| AuthnService | FinishWebAuthnAuthentication | finish_web_authn_authentication | finishWebAuthnAuthentication | mutation | OK | 60.98 | 60.98 | 60.98 | 3 |
| AuthnService | FinishWebAuthnRegistration | finish_web_authn_registration | finishWebAuthnRegistration | mutation | OK | 49.85 | 49.85 | 49.85 | 3 |
| AuthnService | ForgotPassword | forgot_password | forgotPassword | mutation | OK | 11.63 | 11.63 | 11.43 | 3 |
| AuthnService | GenerateRecoveryCodes | generate_recovery_codes | generateRecoveryCodes | mutation | OK | 35.30 | 35.30 | 35.76 | 3 |
| AuthnService | GetJwks | get_jwks | getJwks | read_only | OK | 7.23 | 10.67 | 7.96 | 10 |
| AuthnService | GetMfaPolicy | get_mfa_policy | getMfaPolicy | read_only | OK | 6.21 | 8.19 | 6.63 | 10 |
| AuthnService | GetSession | get_session | getSession | read_only | OK | 7.01 | 10.64 | 7.79 | 10 |
| AuthnService | GetUser | get_user | getUser | read_only | OK | 6.40 | 7.77 | 6.33 | 10 |
| AuthnService | IntrospectToken | introspect_token | introspectToken | read_only | OK | 39.86 | 43.83 | 40.02 | 10 |
| AuthnService | IssueMfaChallenge | issue_mfa_challenge | issueMfaChallenge | mutation | OK | 13.50 | 13.50 | 13.73 | 3 |
| AuthnService | ListDevices | list_devices | listDevices | read_only | OK | 5.98 | 7.75 | 6.45 | 10 |
| AuthnService | ListMfaFactors | list_mfa_factors | listMfaFactors | read_only | OK | 11.31 | 16.08 | 12.22 | 10 |
| AuthnService | ListSessions | list_sessions | listSessions | read_only | OK | 14.49 | 19.11 | 16.47 | 10 |
| AuthnService | ListUsers | list_users | listUsers | read_only | OK | 10.46 | 12.16 | 10.78 | 10 |
| AuthnService | ListWebAuthnCredentials | list_web_authn_credentials | listWebAuthnCredentials | read_only | OK | 5.81 | 6.85 | 6.37 | 10 |
| AuthnService | Login | login | login | mutation | OK | 773.73 | 773.73 | 754.04 | 3 |
| AuthnService | Logout | logout | logout | mutation | OK | 7.52 | 7.52 | 13.15 | 3 |
| AuthnService | PutMfaPolicy | put_mfa_policy | putMfaPolicy | mutation | OK | 15.09 | 15.09 | 17.19 | 3 |
| AuthnService | RefreshSession | refresh_session | refreshSession | mutation | OK | 44.28 | 44.28 | 44.28 | 1 |
| AuthnService | RefreshToken | refresh_token | refreshToken | mutation | OK | 17.17 | 17.17 | 17.17 | 1 |
| AuthnService | RenamePasskey | rename_passkey | renamePasskey | mutation | OK | 10.41 | 10.41 | 10.20 | 3 |
| AuthnService | ResendOTP | resend_otp | resendOtp | mutation | OK | 17.12 | 17.12 | 17.12 | 1 |
| AuthnService | ResetPassword | reset_password | resetPassword | mutation | OK | 807.04 | 807.04 | 807.04 | 1 |
| AuthnService | RevokeDevice | revoke_device | revokeDevice | mutation | OK | 19.24 | 19.24 | 19.24 | 3 |
| AuthnService | RevokeRecoveryCodes | revoke_recovery_codes | revokeRecoveryCodes | mutation | OK | 15.53 | 15.53 | 19.82 | 3 |
| AuthnService | RevokeSession | revoke_session | revokeSession | mutation | OK | 9.40 | 9.40 | 9.12 | 3 |
| AuthnService | SendOTP | send_otp | sendOtp | mutation | OK | 21.67 | 21.67 | 21.67 | 1 |
| AuthnService | SendPhoneVerification | send_phone_verification | sendPhoneVerification | mutation | OK | 19.25 | 19.25 | 22.45 | 3 |
| AuthnService | StartWebAuthnAuthentication | start_web_authn_authentication | startWebAuthnAuthentication | mutation | OK | 19.06 | 19.06 | 18.80 | 3 |
| AuthnService | StartWebAuthnRegistration | start_web_authn_registration | startWebAuthnRegistration | mutation | OK | 19.51 | 19.51 | 18.06 | 3 |
| AuthnService | UpdateUser | update_user | updateUser | mutation | OK | 12.52 | 12.52 | 14.41 | 3 |
| AuthnService | ValidateCSRF | validate_csrf | validateCsrf | read_only | OK | 9.55 | 10.59 | 9.46 | 10 |
| AuthnService | ValidateToken | validate_token | validateToken | read_only | OK | 26.60 | 38.74 | 31.07 | 10 |
| AuthnService | VerifyMfaChallenge | verify_mfa_challenge | verifyMfaChallenge | read_only | OK | 12.38 | 12.38 | 12.38 | 1 |
| AuthnService | VerifyOTP | verify_otp | verifyOtp | read_only | OK | 19.45 | 19.45 | 19.45 | 1 |
| AuthzService | ActivateCanary | activate_canary | activateCanary | destructive | OK | 38.85 | 38.85 | 38.85 | 1 |
| AuthzService | ActivatePolicyVersion | activate_policy_version | activatePolicyVersion | destructive | OK | 92.03 | 92.03 | 92.03 | 1 |
| AuthzService | ApprovePolicyDraft | approve_policy_draft | approvePolicyDraft | mutation | OK | 50.16 | 50.16 | 50.16 | 3 |
| AuthzService | AssignRole | assign_role | assignRole | mutation | OK | 29.56 | 29.56 | 29.80 | 3 |
| AuthzService | Authorize | authorize | authorize | read_only | OK | 33.27 | 38.25 | 34.44 | 10 |
| AuthzService | BatchCheckPermissions | batch_check_permissions | batchCheckPermissions | read_only | OK | 12.24 | 13.61 | 12.33 | 10 |
| AuthzService | CheckAccess | check_access | checkAccess | read_only | OK | 12.19 | 13.56 | 12.27 | 10 |
| AuthzService | CreatePolicyDraft | create_policy_draft | createPolicyDraft | mutation | OK | 44.60 | 44.60 | 48.78 | 3 |
| AuthzService | CreatePolicyRule | create_policy_rule | createPolicyRule | mutation | OK | 25.85 | 25.85 | 24.55 | 3 |
| AuthzService | CreateRole | create_role | createRole | mutation | OK | 27.05 | 27.05 | 34.06 | 3 |
| AuthzService | DeletePolicyRule | delete_policy_rule | deletePolicyRule | mutation | OK | 10.61 | 10.61 | 10.66 | 3 |
| AuthzService | DeleteRole | delete_role | deleteRole | mutation | OK | 12.93 | 12.93 | 19.71 | 3 |
| AuthzService | DiffPolicyDraft | diff_policy_draft | diffPolicyDraft | read_only | OK | 17.02 | 31.57 | 20.92 | 10 |
| AuthzService | ExplainPolicy | explain_policy | explainPolicy | read_only | OK | 9.89 | 12.15 | 10.60 | 10 |
| AuthzService | GetAuthzRevision | get_authz_revision | getAuthzRevision | read_only | OK | 6.16 | 7.41 | 6.28 | 10 |
| AuthzService | GetCanaryStatus | get_canary_status | getCanaryStatus | read_only | OK | 15.38 | 19.51 | 17.09 | 10 |
| AuthzService | GetNativeAccess | get_native_access | getNativeAccess | read_only | OK | 33.66 | 43.43 | 34.36 | 10 |
| AuthzService | GetPolicyBundle | get_policy_bundle | getPolicyBundle | read_only | OK | 11.37 | 13.55 | 11.57 | 10 |
| AuthzService | GetPolicyRule | get_policy_rule | getPolicyRule | read_only | OK | 9.18 | 21.27 | 11.40 | 10 |
| AuthzService | GetRole | get_role | getRole | read_only | OK | 5.76 | 7.91 | 6.24 | 10 |
| AuthzService | InvalidatePolicyBundles | invalidate_policy_bundles | invalidatePolicyBundles | destructive | OK | 28.90 | 28.90 | 28.90 | 1 |
| AuthzService | LintAuthzPolicies | lint_authz_policies | lintAuthzPolicies | read_only | OK | 2.95 | 3.93 | 3.10 | 10 |
| AuthzService | ListAccessDecisionAudits | list_access_decision_audits | listAccessDecisionAudits | read_only | OK | 12.52 | 15.28 | 12.86 | 10 |
| AuthzService | ListPolicyRules | list_policy_rules | listPolicyRules | read_only | OK | 6.64 | 7.34 | 6.90 | 10 |
| AuthzService | ListPolicyVersions | list_policy_versions | listPolicyVersions | read_only | OK | 17.27 | 22.34 | 18.29 | 10 |
| AuthzService | ListRoles | list_roles | listRoles | read_only | OK | 7.32 | 9.91 | 7.58 | 10 |
| AuthzService | ListUserPermissions | list_user_permissions | listUserPermissions | read_only | OK | 2.99 | 3.59 | 3.17 | 10 |
| AuthzService | ListUserRoles | list_user_roles | listUserRoles | read_only | OK | 6.63 | 7.39 | 6.74 | 10 |
| AuthzService | MigrateLegacyPolicies | migrate_legacy_policies | migrateLegacyPolicies | destructive | OK | 41.41 | 41.41 | 41.41 | 1 |
| AuthzService | PromoteCanary | promote_canary | promoteCanary | destructive | OK | 83.57 | 83.57 | 83.57 | 1 |
| AuthzService | PutAuthzPolicy | put_authz_policy | putAuthzPolicy | mutation | OK | 24.71 | 24.71 | 23.34 | 3 |
| AuthzService | PutRelationship | put_relationship | putRelationship | mutation | OK | 28.56 | 28.56 | 27.79 | 3 |
| AuthzService | PutRoleBinding | put_role_binding | putRoleBinding | mutation | OK | 41.32 | 41.32 | 36.34 | 3 |
| AuthzService | RejectPolicyDraft | reject_policy_draft | rejectPolicyDraft | mutation | OK | 28.49 | 28.49 | 28.49 | 3 |
| AuthzService | RevokeRole | revoke_role | revokeRole | mutation | OK | 12.01 | 12.01 | 18.16 | 3 |
| AuthzService | RollbackPolicyVersion | rollback_policy_version | rollbackPolicyVersion | destructive | OK | 54.62 | 54.62 | 54.62 | 1 |
| AuthzService | SeedBuiltinRoles | seed_builtin_roles | seedBuiltinRoles | mutation | OK | 69.69 | 69.69 | 75.67 | 3 |
| AuthzService | SimulatePolicy | simulate_policy | simulatePolicy | mutation | OK | 23.94 | 23.94 | 24.25 | 3 |
| AuthzService | SubmitPolicyDraft | submit_policy_draft | submitPolicyDraft | mutation | OK | 44.74 | 44.74 | 44.74 | 3 |
| AuthzService | UpdatePolicyDraft | update_policy_draft | updatePolicyDraft | mutation | OK | 37.52 | 37.52 | 36.31 | 3 |
| AuthzService | UpdateRole | update_role | updateRole | mutation | OK | 33.16 | 33.16 | 43.12 | 3 |
| BackupService | DeleteBackupPolicy | delete_backup_policy | deleteBackupPolicy | mutation | OK | 21.34 | 21.34 | 23.81 | 3 |
| BackupService | GetBackup | get_backup | getBackup | read_only | OK | 21.47 | 29.17 | 24.83 | 10 |
| BackupService | GetBackupPolicy | get_backup_policy | getBackupPolicy | read_only | OK | 15.59 | 16.16 | 15.17 | 10 |
| BackupService | ListBackupPolicies | list_backup_policies | listBackupPolicies | read_only | OK | 15.24 | 19.66 | 16.43 | 10 |
| BackupService | ListBackups | list_backups | listBackups | read_only | OK | 14.49 | 16.16 | 19.78 | 10 |
| BackupService | PutBackupPolicy | put_backup_policy | putBackupPolicy | mutation | OK | 43.79 | 43.79 | 39.38 | 3 |
| BackupService | RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 1391.34 | 1391.34 | 1391.34 | 1 |
| BackupService | StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 1308.94 | 1308.94 | 1276.82 | 3 |
| CacheService | CreateNamespace | create_cache_namespace | createCacheNamespace | mutation | OK | 20.10 | 20.10 | 22.12 | 3 |
| CacheService | Delete | cache_delete | cacheNamespaceDelete | mutation | OK | 28.82 | 28.82 | 29.64 | 3 |
| CacheService | DeleteNamespace | delete_cache_namespace | deleteCacheNamespace | destructive | OK | 32.88 | 32.88 | 32.88 | 1 |
| CacheService | Get | cache_get | cacheNamespaceGet | read_only | OK | 9.82 | 12.52 | 11.52 | 10 |
| CacheService | GetNamespaceStats | get_cache_namespace_stats | getCacheNamespaceStats | read_only | OK | 17.25 | 19.51 | 20.26 | 10 |
| CacheService | Scan | cache_scan | cacheNamespaceScan | read_only | OK | 9.10 | 11.00 | 9.49 | 10 |
| CacheService | Set | cache_set | cacheNamespaceSet | mutation | OK | 19.28 | 19.28 | 20.23 | 3 |
| ConfigService | DeleteFlag | delete_flag | deleteFlag | destructive | OK | 34.29 | 34.29 | 34.29 | 1 |
| ConfigService | EvaluateFlags | evaluate_flags | evaluateFlags | read_only | OK | 15.93 | 21.37 | 17.44 | 10 |
| ConfigService | GetFlag | get_flag | getFlag | read_only | OK | 14.88 | 17.28 | 15.57 | 10 |
| ConfigService | ListFlags | list_flags | listFlags | read_only | OK | 14.51 | 17.50 | 14.77 | 10 |
| ConfigService | PutFlag | put_flag | putFlag | mutation | OK | 29.95 | 29.95 | 34.92 | 3 |
| ControlPlaneService | AckStatus | ack_status | ackStatus | mutation | OK | 20.34 | 20.34 | 18.32 | 3 |
| ControlPlaneService | DeltaResources | delta_resources | deltaResources | stream_first_recv | OK | 67.28 | 67.28 | 66.46 | 3 |
| ControlPlaneService | GetResources | get_resources | getResources | read_only | OK | 6.70 | 7.93 | 6.90 | 10 |
| ControlPlaneService | ListNodeStates | list_node_states | listNodeStates | read_only | OK | 53.07 | 64.47 | 55.26 | 10 |
| ControlPlaneService | RollbackResources | rollback_resources | rollbackResources | mutation | OK | 90.04 | 90.04 | 84.32 | 3 |
| ControlPlaneService | StreamResources | stream_resources | streamResources | stream_first_recv | OK | 95.04 | 95.04 | 89.96 | 3 |
| DataBroker | ActivateCatalog | activate_catalog | activateCatalog | destructive | OK | 126.01 | 126.01 | 126.01 | 1 |
| DataBroker | AnalyticalQuery | analytical_query | analyticalQuery | read_only | OK | 7.89 | 10.05 | 8.63 | 10 |
| DataBroker | ApplyMigration | apply_migration | applyMigration | mutation | OK | 208.22 | 208.22 | 208.22 | 3 |
| DataBroker | ApproveMigrationPlan | approve_migration_plan | approveMigrationPlan | mutation | OK | 52.12 | 52.12 | 52.12 | 3 |
| DataBroker | BatchSelect | batch_select | batchSelect | stream_first_recv | OK | 8.49 | 8.49 | 8.95 | 3 |
| DataBroker | BatchUpsert | batch_upsert | batchUpsert | stream_first_recv | OK | 45.33 | 45.33 | 48.79 | 3 |
| DataBroker | BeginTx | begin_tx | beginTx | stream_first_recv | OK | 24.02 | 24.02 | 27.26 | 3 |
| DataBroker | CacheDelete | cache_delete | cacheDelete | mutation | OK | 9.35 | 9.35 | 9.78 | 3 |
| DataBroker | CacheGet | cache_get | cacheGet | read_only | OK | 7.84 | 11.43 | 8.91 | 10 |
| DataBroker | CacheScan | cache_scan | cacheScan | read_only | OK | 12.56 | 13.53 | 12.29 | 10 |
| DataBroker | CacheSet | cache_set | cacheSet | mutation | OK | 12.39 | 12.39 | 11.82 | 3 |
| DataBroker | CreateMaterializedView | create_materialized_view | createMaterializedView | mutation | OK | 7.00 | 7.00 | 7.01 | 3 |
| DataBroker | Delete | delete | delete | mutation | OK | 38.82 | 38.82 | 38.69 | 3 |
| DataBroker | DeletePolicy | delete_policy | deletePolicy | mutation | OK | 19.19 | 19.19 | 19.19 | 3 |
| DataBroker | DismissDlqEvent | dismiss_dlq_event | dismissDlqEvent | mutation | OK | 24.38 | 24.38 | 22.35 | 3 |
| DataBroker | DocumentDelete | document_delete | documentDelete | mutation | OK | 8.53 | 8.53 | 8.51 | 3 |
| DataBroker | DocumentFind | document_find | documentFind | read_only | OK | 6.64 | 7.33 | 6.79 | 10 |
| DataBroker | DocumentGet | document_get | documentGet | read_only | OK | 7.35 | 8.70 | 7.46 | 10 |
| DataBroker | DocumentUpsert | document_upsert | documentUpsert | mutation | OK | 8.82 | 8.82 | 8.38 | 3 |
| DataBroker | DropResource | drop_resource | dropResource | destructive | OK | 18.19 | 18.19 | 18.19 | 1 |
| DataBroker | EnqueueOutboxEvent | enqueue_outbox_event | enqueueOutboxEvent | mutation | OK | 11.52 | 11.52 | 11.02 | 3 |
| DataBroker | EnsureBaseline | ensure_baseline | ensureBaseline | mutation | OK | 19.01 | 19.01 | 19.16 | 3 |
| DataBroker | EnsureProject | ensure_project | ensureProject | mutation | OK | 19.05 | 19.05 | 18.01 | 3 |
| DataBroker | EnsureResource | ensure_resource | ensureResource | mutation | OK | 20.29 | 20.29 | 19.28 | 3 |
| DataBroker | GeneratePresignedUrl | generate_presigned_url | generatePresignedUrl | mutation | OK | 7.19 | 7.19 | 7.41 | 3 |
| DataBroker | GenericDispatch | generic_dispatch | genericDispatch | mutation | OK | 5.23 | 5.23 | 5.58 | 3 |
| DataBroker | GetAdminSummary | get_admin_summary | getAdminSummary | read_only | OK | 32.54 | 41.85 | 38.93 | 10 |
| DataBroker | GetCapabilities | get_capabilities | getCapabilities | read_only | OK | 7.88 | 10.05 | 8.58 | 10 |
| DataBroker | GetCatalogManifest | get_catalog_manifest | getCatalogManifest | read_only | OK | 12.63 | 17.19 | 14.08 | 10 |
| DataBroker | GetCatalogVersion | get_catalog_version | getCatalogVersion | read_only | OK | 9.08 | 10.88 | 9.32 | 10 |
| DataBroker | GetCatalogVersions | get_catalog_versions | getCatalogVersions | read_only | OK | 6.65 | 8.34 | 7.06 | 10 |
| DataBroker | GetCdcStatus | get_cdc_status | getCdcStatus | read_only | OK | 5.91 | 7.14 | 6.35 | 10 |
| DataBroker | GetDlqEvent | get_dlq_event | getDlqEvent | read_only | OK | 7.84 | 9.15 | 8.25 | 10 |
| DataBroker | GetHealthReport | get_health_report | getHealthReport | read_only | OK | 3.35 | 4.71 | 3.68 | 10 |
| DataBroker | GetMigrationStatus | get_migration_status | getMigrationStatus | read_only | OK | 9.20 | 14.68 | 10.77 | 10 |
| DataBroker | GetObject | get_object | getObject | stream_first_recv | OK | 8.39 | 8.39 | 8.70 | 3 |
| DataBroker | GetSaga | get_saga | getSaga | read_only | OK | 7.47 | 10.15 | 7.93 | 10 |
| DataBroker | GraphMutate | graph_mutate | graphMutate | mutation | OK | 24.21 | 24.21 | 28.07 | 3 |
| DataBroker | GraphQuery | graph_query | graphQuery | read_only | OK | 19.06 | 31.01 | 22.94 | 10 |
| DataBroker | InitiateMultipartUpload | initiate_multipart_upload | initiateMultipartUpload | mutation | OK | 18.74 | 18.74 | 24.39 | 3 |
| DataBroker | LintPolicies | lint_policies | lintPolicies | read_only | OK | 6.95 | 8.19 | 7.12 | 10 |
| DataBroker | ListAdminAuditLogs | list_admin_audit_logs | listAdminAuditLogs | read_only | OK | 11.73 | 14.09 | 12.64 | 10 |
| DataBroker | ListDlqEvents | list_dlq_events | listDlqEvents | read_only | OK | 7.08 | 8.43 | 7.22 | 10 |
| DataBroker | ListMessageSchemas | list_message_schemas | listMessageSchemas | read_only | OK | 3.92 | 4.74 | 3.75 | 10 |
| DataBroker | ListMigrationRuns | list_migration_runs | listMigrationRuns | read_only | OK | 7.20 | 9.90 | 7.77 | 10 |
| DataBroker | ListPolicies | list_policies | listPolicies | read_only | OK | 6.29 | 8.14 | 7.15 | 10 |
| DataBroker | ListProjects | list_projects | listProjects | read_only | OK | 6.72 | 8.78 | 7.17 | 10 |
| DataBroker | ListResources | list_resources | listResources | read_only | OK | 4.97 | 6.75 | 5.77 | 10 |
| DataBroker | ListSagas | list_sagas | listSagas | read_only | OK | 6.89 | 10.93 | 8.16 | 10 |
| DataBroker | LookupMessageSchema | lookup_message_schema | lookupMessageSchema | read_only | OK | 3.32 | 4.18 | 3.59 | 10 |
| DataBroker | MarkSagaReviewed | mark_saga_reviewed | markSagaReviewed | mutation | OK | 16.86 | 16.86 | 16.14 | 3 |
| DataBroker | PauseCdc | pause_cdc | pauseCdc | mutation | OK | 20.22 | 20.22 | 23.99 | 3 |
| DataBroker | PlanMigration | plan_migration | planMigration | mutation | OK | 19.14 | 19.14 | 18.76 | 3 |
| DataBroker | PreviewCdcRedaction | preview_cdc_redaction | previewCdcRedaction | read_only | OK | 13.08 | 17.74 | 13.39 | 10 |
| DataBroker | PublishCDC | publish_cdc | publishCdc | cdc_first_event | OK | 110.42 | 110.42 | 110.42 | 1 |
| DataBroker | PutObject | put_object | putObject | stream_first_recv | OK | 23.39 | 23.39 | 25.88 | 3 |
| DataBroker | PutPolicy | put_policy | putPolicy | destructive | OK | 47.95 | 47.95 | 47.95 | 1 |
| DataBroker | QuarantineDlqEvent | quarantine_dlq_event | quarantineDlqEvent | mutation | OK | 23.68 | 23.68 | 23.65 | 3 |
| DataBroker | ReloadPolicies | reload_policies | reloadPolicies | destructive | OK | 22.19 | 22.19 | 22.19 | 1 |
| DataBroker | ReplayDlqEvent | replay_dlq_event | replayDlqEvent | mutation | OK | 27.40 | 27.40 | 27.40 | 3 |
| DataBroker | ResumeCdc | resume_cdc | resumeCdc | mutation | OK | 31.16 | 31.16 | 31.11 | 3 |
| DataBroker | RetrySagaCompensation | retry_saga_compensation | retrySagaCompensation | mutation | OK | 16.73 | 16.73 | 16.73 | 3 |
| DataBroker | RollbackCatalog | rollback_catalog | rollbackCatalog | destructive | OK | 8.52 | 8.52 | 8.52 | 1 |
| DataBroker | ScanProjectionDrift | scan_projection_drift | scanProjectionDrift | read_only | OK | 16.47 | 27.49 | 18.81 | 10 |
| DataBroker | Select | select | select | read_only | OK | 7.44 | 9.41 | 8.09 | 10 |
| DataBroker | SelectV2 | select_v_2 | selectV2 | stream_first_recv | OK | 8.48 | 8.48 | 8.40 | 3 |
| DataBroker | StageCatalog | stage_catalog | stageCatalog | destructive | OK | 650.51 | 650.51 | 650.51 | 1 |
| DataBroker | StepDownCdcLeader | step_down_cdc_leader | stepDownCdcLeader | mutation | OK | 28.55 | 28.55 | 26.78 | 3 |
| DataBroker | TimeSeriesQuery | time_series_query | timeSeriesQuery | read_only | OK | 10.62 | 13.61 | 11.00 | 10 |
| DataBroker | TimeSeriesWrite | time_series_write | timeSeriesWrite | mutation | OK | 71.90 | 71.90 | 71.00 | 3 |
| DataBroker | Upsert | upsert | upsert | mutation | OK | 35.44 | 35.44 | 79.41 | 3 |
| DataBroker | ValidateCatalog | validate_catalog | validateCatalog | destructive | OK | 115.51 | 115.51 | 115.51 | 1 |
| DataBroker | VectorBatchUpsert | vector_batch_upsert | vectorBatchUpsert | stream_first_recv | OK | 13.83 | 13.83 | 13.69 | 3 |
| DataBroker | VectorHybridSearch | vector_hybrid_search | vectorHybridSearch | read_only | OK | 6.45 | 7.75 | 6.80 | 10 |
| DataBroker | VectorSearch | vector_search | vectorSearch | read_only | OK | 7.04 | 7.65 | 7.24 | 10 |
| DataBroker | VectorUpsert | vector_upsert | vectorUpsert | mutation | OK | 21.30 | 21.30 | 20.38 | 3 |
| DataBroker | VerifyAdminAuditLog | verify_admin_audit_log | verifyAdminAuditLog | read_only | OK | 11.54 | 16.56 | 12.47 | 10 |
| EmbeddingService | Backfill | backfill | backfillEmbeddingSource | mutation | OK | 15.77 | 15.77 | 15.96 | 3 |
| EmbeddingService | DeleteSource | delete_source | deleteEmbeddingSource | destructive | OK | 55.57 | 55.57 | 55.57 | 1 |
| EmbeddingService | ListSources | list_sources | listEmbeddingSources | read_only | OK | 14.12 | 16.71 | 14.92 | 10 |
| EmbeddingService | RegisterSource | register_source | registerEmbeddingSource | mutation | OK | 20.96 | 20.96 | 31.37 | 3 |
| EmbeddingService | ReportEmbedding | report_embedding | reportEmbedding | mutation | OK | 17.43 | 17.43 | 18.93 | 3 |
| EmbeddingService | Retrieve | retrieve | retrieveEmbedding | read_only | OK | 20.02 | 21.56 | 20.14 | 10 |
| IdentityProviderService | CreateProvider | create_provider | createProvider | mutation | OK | 16.90 | 16.90 | 16.97 | 3 |
| IdentityProviderService | DisableProvider | disable_provider | disableProvider | mutation | OK | 17.90 | 17.90 | 18.05 | 3 |
| IdentityProviderService | ForceJwksRefresh | force_jwks_refresh | forceJwksRefresh | mutation | OK | 21.83 | 21.83 | 23.35 | 3 |
| IdentityProviderService | GetProvider | get_provider | getProvider | read_only | OK | 7.76 | 9.24 | 7.85 | 10 |
| IdentityProviderService | ImportSamlMetadata | import_saml_metadata | importSamlMetadata | mutation | OK | 24.54 | 24.54 | 24.88 | 3 |
| IdentityProviderService | LinkIdentity | link_identity | linkIdentity | mutation | OK | 23.25 | 23.25 | 23.41 | 3 |
| IdentityProviderService | ListExternalIdentities | list_external_identities | listExternalIdentities | read_only | OK | 12.49 | 15.77 | 12.62 | 10 |
| IdentityProviderService | ListProviders | list_providers | listProviders | read_only | OK | 10.61 | 13.13 | 11.32 | 10 |
| IdentityProviderService | PreviewClaimMapping | preview_claim_mapping | previewClaimMapping | read_only | OK | 6.25 | 6.79 | 6.33 | 10 |
| IdentityProviderService | PreviewGroupMapping | preview_group_mapping | previewGroupMapping | read_only | OK | 5.32 | 8.83 | 6.15 | 10 |
| IdentityProviderService | ResolveExternalIdentity | resolve_external_identity | resolveExternalIdentity | mutation | OK | 20.14 | 20.14 | 34.16 | 3 |
| IdentityProviderService | SamlAcs | saml_acs | samlAcs | mutation | OK | 123.94 | 123.94 | 121.19 | 3 |
| IdentityProviderService | ScimCreateGroup | scim_create_group | scimCreateGroup | mutation | OK | 5.84 | 5.84 | 6.06 | 3 |
| IdentityProviderService | ScimCreateUser | scim_create_user | scimCreateUser | mutation | OK | 38.20 | 38.20 | 38.67 | 3 |
| IdentityProviderService | ScimDeleteGroup | scim_delete_group | scimDeleteGroup | mutation | OK | 6.20 | 6.20 | 6.11 | 3 |
| IdentityProviderService | ScimDeleteUser | scim_delete_user | scimDeleteUser | mutation | OK | 55.79 | 55.79 | 55.79 | 3 |
| IdentityProviderService | ScimGetGroup | scim_get_group | scimGetGroup | mutation | OK | 9.88 | 9.88 | 9.29 | 3 |
| IdentityProviderService | ScimGetUser | scim_get_user | scimGetUser | mutation | OK | 9.64 | 9.64 | 9.69 | 3 |
| IdentityProviderService | ScimListGroups | scim_list_groups | scimListGroups | mutation | OK | 5.35 | 5.35 | 5.47 | 3 |
| IdentityProviderService | ScimListUsers | scim_list_users | scimListUsers | mutation | OK | 14.11 | 14.11 | 15.51 | 3 |
| IdentityProviderService | ScimPatchGroup | scim_patch_group | scimPatchGroup | mutation | OK | 12.35 | 12.35 | 12.34 | 3 |
| IdentityProviderService | ScimPatchUser | scim_patch_user | scimPatchUser | mutation | OK | 26.73 | 26.73 | 26.41 | 3 |
| IdentityProviderService | ScimReplaceUser | scim_replace_user | scimReplaceUser | mutation | OK | 28.20 | 28.20 | 35.12 | 3 |
| IdentityProviderService | StartSamlLogin | start_saml_login | startSamlLogin | mutation | OK | 6.91 | 6.91 | 6.26 | 3 |
| IdentityProviderService | TestProviderDiscovery | test_provider_discovery | testProviderDiscovery | read_only | OK | 8.82 | 11.29 | 12.36 | 10 |
| IdentityProviderService | UnlinkIdentity | unlink_identity | unlinkIdentity | mutation | OK | 6.88 | 6.88 | 8.90 | 3 |
| IdentityProviderService | UpdateProvider | update_provider | updateProvider | mutation | OK | 36.17 | 36.17 | 35.14 | 3 |
| LiveQueryService | Subscribe | subscribe | liveQuerySubscribe | stream_first_recv | OK | 24.58 | 24.58 | 27.98 | 3 |
| LockService | AcquireLock | acquire_lock | acquireLock | mutation | OK | 70.02 | 70.02 | 80.15 | 3 |
| LockService | GetLock | get_lock | getLock | read_only | OK | 18.00 | 30.19 | 19.94 | 10 |
| LockService | ListLocks | list_locks | listLocks | read_only | OK | 15.26 | 16.03 | 14.88 | 10 |
| LockService | ReleaseLock | release_lock | releaseLock | mutation | OK | 24.27 | 24.27 | 34.82 | 3 |
| LockService | RenewLock | renew_lock | renewLock | mutation | OK | 67.61 | 67.61 | 65.03 | 3 |
| MeteringService | CheckQuota | check_quota | checkQuota | read_only | OK | 21.68 | 25.70 | 22.38 | 10 |
| MeteringService | GetQuota | get_quota | getQuota | read_only | OK | 15.94 | 17.98 | 15.71 | 10 |
| MeteringService | ListQuotas | list_quotas | listQuotas | read_only | OK | 14.47 | 15.36 | 15.30 | 10 |
| MeteringService | PutQuota | put_quota | putQuota | mutation | OK | 24.62 | 24.62 | 24.31 | 3 |
| MeteringService | QueryUsage | query_usage | queryUsage | read_only | OK | 13.72 | 17.21 | 14.56 | 10 |
| MeteringService | RecordUsage | record_usage | recordUsage | mutation | OK | 11.75 | 11.75 | 11.38 | 3 |
| NotificationService | GetDeliveryStats | get_delivery_stats | getDeliveryStats | read_only | OK | 10.04 | 11.35 | 10.65 | 10 |
| NotificationService | GetNotification | get_notification | getNotification | read_only | OK | 16.23 | 21.69 | 17.16 | 10 |
| NotificationService | GetPreference | get_preference | getPreference | read_only | OK | 14.47 | 18.02 | 15.36 | 10 |
| NotificationService | GetTemplate | get_template | getTemplate | read_only | OK | 14.52 | 16.73 | 14.83 | 10 |
| NotificationService | ListNotifications | list_notifications | listNotifications | read_only | OK | 22.22 | 26.28 | 23.74 | 10 |
| NotificationService | ListPreferences | list_preferences | listPreferences | read_only | OK | 21.40 | 33.61 | 23.44 | 10 |
| NotificationService | ListTemplates | list_templates | listTemplates | read_only | OK | 20.96 | 21.88 | 20.93 | 10 |
| NotificationService | ReportDelivery | report_delivery | reportDelivery | mutation | OK | 16.66 | 16.66 | 17.35 | 3 |
| NotificationService | RetryNotification | retry_notification | retryNotification | mutation | OK | 20.76 | 20.76 | 20.76 | 3 |
| NotificationService | SendNotification | send_notification | sendNotification | mutation | OK | 55.85 | 55.85 | 52.45 | 3 |
| NotificationService | SetPreference | set_preference | setPreference | mutation | OK | 13.80 | 13.80 | 16.71 | 3 |
| NotificationService | UpsertTemplate | upsert_template | upsertTemplate | mutation | OK | 11.18 | 11.18 | 10.41 | 3 |
| PeerService | GetPeer | get_peer | getPeer | read_only | OK | 14.65 | 15.81 | 15.47 | 10 |
| PeerService | JoinRoom | join_room | joinRoom | mutation | OK | 42.90 | 42.90 | 44.34 | 3 |
| PeerService | JoinSession | join_session | joinSession | mutation | OK | 35.15 | 35.15 | 34.59 | 3 |
| PeerService | LeaveRoom | leave_room | leaveRoom | mutation | OK | 11.52 | 11.52 | 13.50 | 3 |
| PeerService | ListPeers | list_peers | listPeers | read_only | OK | 14.32 | 21.51 | 15.61 | 10 |
| RoomService | CloseRoom | close_room | closeRoom | mutation | OK | 23.71 | 23.71 | 23.72 | 3 |
| RoomService | CreateRoom | create_room | createRoom | mutation | OK | 28.34 | 28.34 | 28.79 | 3 |
| RoomService | GetRoom | get_room | getRoom | read_only | OK | 12.71 | 16.14 | 13.40 | 10 |
| RoomService | ListEgress | list_egress | listEgress | read_only | CAPABILITY_SKIPPED | 6.72 | 6.72 | 7.03 | 10 |
| RoomService | ListRooms | list_rooms | listRooms | read_only | OK | 14.04 | 16.87 | 14.61 | 10 |
| RoomService | StartRoomComposite | start_room_composite | startRoomComposite | mutation | CAPABILITY_SKIPPED | 9.59 | 9.59 | 9.59 | 3 |
| RoomService | StartTrackEgress | start_track_egress | startTrackEgress | mutation | CAPABILITY_SKIPPED | 13.39 | 13.39 | 13.39 | 3 |
| RoomService | StopEgress | stop_egress | stopEgress | mutation | CAPABILITY_SKIPPED | 7.55 | 7.55 | 7.55 | 3 |
| RoomService | UpdateRoom | update_room | updateRoom | mutation | OK | 13.60 | 13.60 | 13.51 | 3 |
| SchedulerService | CreateJob | create_job | createJob | mutation | OK | 17.03 | 17.03 | 16.44 | 3 |
| SchedulerService | DeleteJob | delete_job | deleteJob | destructive | OK | 16.61 | 16.61 | 16.61 | 1 |
| SchedulerService | GetJob | get_job | getJob | read_only | OK | 9.88 | 12.89 | 11.87 | 10 |
| SchedulerService | ListJobs | list_jobs | listJobs | read_only | OK | 13.78 | 17.51 | 14.02 | 10 |
| SchedulerService | PauseJob | pause_job | pauseJob | mutation | OK | 14.96 | 14.96 | 14.96 | 3 |
| SchedulerService | ResumeJob | resume_job | resumeJob | mutation | OK | 15.30 | 15.30 | 15.30 | 3 |
| SearchService | CreateIndex | create_index | createSearchIndex | mutation | OK | 26.44 | 26.44 | 28.50 | 3 |
| SearchService | DeleteIndex | delete_index | deleteSearchIndex | destructive | OK | 51.77 | 51.77 | 51.77 | 1 |
| SearchService | ListIndexes | list_indexes | listSearchIndexes | read_only | OK | 12.33 | 15.48 | 13.93 | 10 |
| SearchService | Reindex | reindex | reindexSearchIndex | mutation | OK | 27.94 | 27.94 | 31.06 | 3 |
| SearchService | Search | search | search | read_only | OK | 16.41 | 19.48 | 17.01 | 10 |
| SignalingService | Signal | signal | signal | stream_first_recv | OK | 24.39 | 24.39 | 24.39 | 3 |
| StorageService | DeleteFile | delete_file | deleteFile | mutation | OK | 23.84 | 23.84 | 23.84 | 3 |
| StorageService | DownloadFile | download_file | downloadFile | stream_first_recv | OK | 27.98 | 27.98 | 27.90 | 3 |
| StorageService | FinalizeUpload | finalize_upload | finalizeUpload | mutation | OK | 38.41 | 38.41 | 38.41 | 3 |
| StorageService | GetDownloadUrl | get_download_url | getDownloadUrl | read_only | OK | 27.94 | 32.23 | 24.74 | 10 |
| StorageService | GetFile | get_file | getFile | read_only | OK | 15.21 | 15.95 | 15.23 | 10 |
| StorageService | ListFiles | list_files | listFiles | read_only | OK | 20.96 | 24.62 | 20.98 | 10 |
| StorageService | RegisterUpload | register_upload | registerUpload | mutation | OK | 27.24 | 27.24 | 27.01 | 3 |
| StorageService | ReissueUploadUrl | reissue_upload_url | reissueUploadUrl | read_only | OK | 16.59 | 19.61 | 17.24 | 10 |
| StorageService | UpdateFile | update_file | updateFile | mutation | OK | 25.19 | 25.19 | 31.09 | 3 |
| TenantService | CreateTenant | create_tenant | createTenant | mutation | OK | 16.84 | 16.84 | 16.84 | 3 |
| TenantService | GetTenant | get_tenant | getTenant | read_only | OK | 16.23 | 18.64 | 16.18 | 10 |
| TenantService | GetTenantConfig | get_tenant_config | getTenantConfig | read_only | OK | 13.15 | 14.46 | 13.30 | 10 |
| TenantService | ListTenants | list_tenants | listTenants | read_only | OK | 12.57 | 16.50 | 14.35 | 10 |
| TenantService | PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 277.35 | 277.35 | 277.35 | 1 |
| TenantService | UpdateTenant | update_tenant | updateTenant | mutation | OK | 12.14 | 12.14 | 12.77 | 3 |
| TenantService | UpdateTenantConfig | update_tenant_config | updateTenantConfig | mutation | OK | 27.06 | 27.06 | 32.14 | 3 |
| TrackService | ListTracks | list_tracks | listTracks | read_only | OK | 12.77 | 16.40 | 13.64 | 10 |
| TrackService | MuteTrack | mute_track | muteTrack | mutation | OK | 10.61 | 10.61 | 10.82 | 3 |
| TrackService | PublishTrack | publish_track | publishTrack | mutation | OK | 35.59 | 35.59 | 33.75 | 3 |
| TrackService | UnpublishTrack | unpublish_track | unpublishTrack | mutation | OK | 15.95 | 15.95 | 15.67 | 3 |
| TurnService | IssueCredentials | issue_credentials | issueCredentials | mutation | OK | 16.61 | 16.61 | 19.86 | 3 |
| VaultService | BatchDecrypt | batch_decrypt | vaultBatchDecrypt | mutation | OK | 23.29 | 23.29 | 25.04 | 3 |
| VaultService | BatchEncrypt | batch_encrypt | vaultBatchEncrypt | mutation | OK | 20.73 | 20.73 | 21.63 | 3 |
| VaultService | CreateTransitKey | create_transit_key | createTransitKey | mutation | OK | 24.13 | 24.13 | 24.13 | 3 |
| VaultService | Decrypt | decrypt | vaultDecrypt | read_only | OK | 27.99 | 37.66 | 30.55 | 10 |
| VaultService | DeleteSecret | delete_secret | deleteSecret | mutation | OK | 11.96 | 11.96 | 17.49 | 3 |
| VaultService | DestroySecret | destroy_secret | destroySecret | destructive | OK | 34.70 | 34.70 | 34.70 | 1 |
| VaultService | Encrypt | encrypt | vaultEncrypt | mutation | OK | 14.78 | 14.78 | 15.47 | 3 |
| VaultService | GenerateDataKey | generate_data_key | vaultGenerateDataKey | mutation | OK | 41.37 | 41.37 | 36.93 | 3 |
| VaultService | GenerateDatabaseCredentials | generate_database_credentials | generateDatabaseCredentials | mutation | OK | 28.62 | 28.62 | 27.53 | 3 |
| VaultService | GetSecret | get_secret | getSecret | read_only | OK | 22.89 | 35.16 | 24.96 | 10 |
| VaultService | GetTransitPublicKey | get_transit_public_key | vaultGetTransitPublicKey | read_only | OK | 18.47 | 22.16 | 19.30 | 10 |
| VaultService | Hmac | hmac | vaultHmac | mutation | OK | 21.60 | 21.60 | 23.44 | 3 |
| VaultService | ListSecrets | list_secrets | listSecrets | read_only | OK | 21.75 | 26.05 | 22.27 | 10 |
| VaultService | PutSecret | put_secret | putSecret | mutation | OK | 34.51 | 34.51 | 34.51 | 3 |
| VaultService | Rewrap | rewrap | vaultRewrap | mutation | OK | 29.07 | 29.07 | 28.24 | 3 |
| VaultService | RotateTransitKey | rotate_transit_key | rotateTransitKey | mutation | OK | 36.87 | 36.87 | 38.49 | 3 |
| VaultService | SealStatus | seal_status | vaultSealStatus | read_only | OK | 3.36 | 4.21 | 3.60 | 10 |
| VaultService | Sign | sign | vaultSign | mutation | OK | 14.73 | 14.73 | 15.29 | 3 |
| VaultService | UndeleteSecret | undelete_secret | undeleteSecret | mutation | OK | 22.73 | 22.73 | 22.73 | 3 |
| VaultService | Verify | verify | vaultVerify | read_only | OK | 23.59 | 30.02 | 25.03 | 10 |
| WebhookService | CreateEndpoint | create_endpoint | createWebhookEndpoint | mutation | OK | 20.35 | 20.35 | 20.84 | 3 |
| WebhookService | DeleteEndpoint | delete_endpoint | deleteWebhookEndpoint | destructive | OK | 40.23 | 40.23 | 40.23 | 1 |
| WebhookService | GetEndpoint | get_endpoint | getWebhookEndpoint | read_only | OK | 14.63 | 17.20 | 15.45 | 10 |
| WebhookService | ListDeliveries | list_deliveries | listWebhookDeliveries | read_only | OK | 18.04 | 24.65 | 20.03 | 10 |
| WebhookService | ListEndpoints | list_endpoints | listWebhookEndpoints | read_only | OK | 21.08 | 28.11 | 20.99 | 10 |
| WebhookService | UpdateEndpoint | update_endpoint | updateWebhookEndpoint | mutation | OK | 26.48 | 26.48 | 28.29 | 3 |
| WorkflowService | CancelWorkflow | cancel_workflow | cancelWorkflow | destructive | OK | 35.40 | 35.40 | 35.40 | 1 |
| WorkflowService | GetWorkflow | get_workflow | getWorkflow | read_only | OK | 11.91 | 26.12 | 15.83 | 10 |
| WorkflowService | ListWorkflows | list_workflows | listWorkflows | read_only | OK | 14.04 | 21.13 | 15.84 | 10 |
| WorkflowService | SignalWorkflow | signal_workflow | signalWorkflow | mutation | OK | 20.17 | 20.17 | 21.20 | 3 |
| WorkflowService | StartWorkflow | start_workflow | startWorkflow | mutation | OK | 35.98 | 35.98 | 43.24 | 3 |
