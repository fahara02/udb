# UDB SDK Live Perf — Python (localhost)

RPCs measured: 344   tenant=a2684444-ddd4-4dd0-8d6f-dff5990284e8

Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities (a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request's reference/ID fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection latency. The TARGET is zero failures; any residual non-OK RPC is listed under Failures for the maintainer to finish.

Unary = full request/response round-trip. Non-CDC streaming RPCs (kind=stream_first_recv) report time-to-FIRST-RESPONSE with seeded inputs. CDC subscription (kind=cdc_first_event, PublishCDC) reports time-to-FIRST-EVENT: the harness subscribes, fires a real Upsert that flows outbox->CDC->Kafka, and times the first delivered event.

## Seeded fixtures

Captured semantic field -> seeded value keys used to resolve request fields: access_token, action, admin_reset_mfa_user_id, admin_reset_password_user_id, apply_run_id, approval_token, approve_draft_id, approve_run_id, approved_by, asset_id, assigned_by, auth_challenge_id, backup_id, bucket, canary_id, canary_version_id, cancel_workflow_id, catalog_manifest, catalog_manifest_b64, challenge_id, change_status_user_id, close_room_id, code, collection, content_type, created_by, csrf_token, current_password, definition_id, delete_endpoint_id, delete_file_id, delete_policy_id, delete_scim_user_id, deleted_by, device_id, disable_mfa_user_id, disable_provider_id, dismiss_dlq_id, dlq_id, document_id, domain, ds_policy_id, egress_id, email, endpoint_id, event_type, external_identity_id, file_id, file_size_bytes, file_type, filename, finalize_file_id, gov_exp, identifier, instance_id, job_id, join_session_room_id, key_id, kind, leave_peer_id, locale, log_id, mark_saga_id, message_type, migration_id, mongo_collection, name, new_password, node_id, notification_id, object, object_key, otp_code, otp_id, owner_id, password, peer_id, plain_key, policy_draft_id, policy_id, policy_version_id, project, project_id, provider_id, purge_tenant_id, quarantine_dlq_id, recipient_id, record_id, recovery_code, refresh_token, reg_challenge_id, reject_draft_id, rejected_by, relation, release_fencing_token, renew_fencing_token, replay_dlq_id, reset_otp_code, reset_otp_id, resource, resource_name, restore_tenant_id, retry_saga_id, revoke_device_id, revoke_key_id, revoke_recovery_user_id, revoked_by, role, role_code, role_id, rollback_policy_set_id, rollback_resource_version, rollback_target_version_id, room_id, saga_id, saml_provider_id, scim_group_id, scim_user_id, send_otp_user_id, session_id, signal_peer_id, stage_name, step_id, subject, tenant, tenant_id, token, topic_pattern, track_id, ts_table, unpublish_track_id, update_draft_id, update_draft_updated_at_unix, update_key_id, updated_by, user_id, user_role_id, username, vault_ciphertext, vault_create_key_name, vault_db_role, vault_delete_secret_path, vault_destroy_secret_path, vault_key_name, vault_put_secret_path, vault_secret_path, vault_signature, vector_collection, workflow_id

## Per-service mean latency

| Service | RPCs | mean ms |
|---|--:|--:|
| BackupService | 8 | 568.84 |
| AuthnService | 50 | 90.75 |
| LockService | 3 | 68.32 |
| ControlPlaneService | 6 | 65.23 |
| TenantService | 7 | 54.48 |
| DataBroker | 77 | 50.78 |
| CacheService | 7 | 41.32 |
| AuthzService | 41 | 39.60 |
| StorageService | 8 | 39.49 |
| NotificationService | 12 | 36.49 |
| SearchService | 5 | 34.90 |
| EmbeddingService | 6 | 33.50 |
| PeerService | 5 | 32.90 |
| VaultService | 14 | 31.31 |
| ApiKeyService | 9 | 30.65 |
| IdentityProviderService | 27 | 30.24 |
| AssetService | 8 | 29.55 |
| SchedulerService | 6 | 27.74 |
| ConfigService | 5 | 27.70 |
| MeteringService | 6 | 26.75 |
| WorkflowService | 5 | 25.97 |
| TurnService | 1 | 25.94 |
| WebhookService | 6 | 25.45 |
| LiveQueryService | 1 | 22.78 |
| SignalingService | 1 | 22.68 |
| TrackService | 4 | 21.39 |
| RoomService | 9 | 20.68 |
| AnalyticsService | 7 | 17.87 |

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
| BackupService/RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 2404.76 | 2404.76 | 2404.76 |
| BackupService/StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 1677.29 | 1677.29 | 1992.54 |
| AuthnService/ChangePassword | change_password | changePassword | mutation | OK | 1075.72 | 1075.72 | 1075.72 |
| DataBroker/StageCatalog | stage_catalog | stageCatalog | destructive | OK | 639.17 | 639.17 | 639.17 |
| AuthnService/Login | login | login | mutation | OK | 624.62 | 624.62 | 618.03 |
| AuthnService/ResetPassword | reset_password | resetPassword | mutation | OK | 604.61 | 604.61 | 604.61 |
| AuthnService/CreateUser | create_user | createUser | mutation | OK | 512.66 | 512.66 | 521.26 |
| DataBroker/ApplyMigration | apply_migration | applyMigration | mutation | OK | 466.23 | 466.23 | 466.23 |
| DataBroker/PreviewCdcRedaction | preview_cdc_redaction | previewCdcRedaction | read_only | OK | 196.43 | 325.60 | 234.26 |
| DataBroker/ScanProjectionDrift | scan_projection_drift | scanProjectionDrift | read_only | OK | 131.45 | 303.98 | 168.63 |
| AuthnService/RefreshSession | refresh_session | refreshSession | mutation | OK | 294.87 | 294.87 | 294.87 |
| DataBroker/PublishCDC | publish_cdc | publishCdc | cdc_first_event | OK | 285.97 | 285.97 | 285.97 |
| TenantService/PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 255.53 | 255.53 | 255.53 |
| AuthnService/Authenticate | authenticate | authenticate | read_only | OK | 200.33 | 200.33 | 200.33 |
| AuthnService/ValidateToken | validate_token | validateToken | read_only | OK | 56.19 | 186.34 | 94.44 |
| AuthzService/ActivatePolicyVersion | activate_policy_version | activatePolicyVersion | destructive | OK | 137.20 | 137.20 | 137.20 |
| DataBroker/ActivateCatalog | activate_catalog | activateCatalog | destructive | OK | 132.38 | 132.38 | 132.38 |
| AuthnService/RefreshToken | refresh_token | refreshToken | mutation | OK | 130.41 | 130.41 | 130.41 |
| DataBroker/Delete | delete | delete | mutation | OK | 129.39 | 129.39 | 123.57 |
| IdentityProviderService/ScimDeleteUser | scim_delete_user | scimDeleteUser | mutation | OK | 122.19 | 122.19 | 122.19 |

## Full per-RPC table (sorted by service, then RPC)

| Service | RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms | iters |
|---|---|---|---|---|---|--:|--:|--:|--:|
| AnalyticsService | GetExecutorPerformance | get_executor_performance | getExecutorPerformance | read_only | OK | 29.24 | 43.53 | 30.67 | 10 |
| AnalyticsService | GetPipelineSummary | get_pipeline_summary | getPipelineSummary | read_only | OK | 18.45 | 33.03 | 21.98 | 10 |
| AnalyticsService | GetReconciliationAnalytics | get_reconciliation_analytics | getReconciliationAnalytics | read_only | OK | 18.74 | 28.63 | 20.98 | 10 |
| AnalyticsService | GetSlaCompliance | get_sla_compliance | getSlaCompliance | read_only | OK | 13.02 | 23.50 | 15.89 | 10 |
| AnalyticsService | GetThroughput | get_throughput | getThroughput | read_only | OK | 14.46 | 20.66 | 16.45 | 10 |
| AnalyticsService | RecordPipelineMetric | record_pipeline_metric | recordPipelineMetric | mutation | OK | 10.61 | 10.61 | 11.04 | 3 |
| AnalyticsService | TriggerSnapshot | trigger_snapshot | triggerSnapshot | mutation | OK | 7.73 | 7.73 | 8.09 | 3 |
| ApiKeyService | CreateApiKey | create_api_key | createApiKey | mutation | OK | 19.64 | 19.64 | 20.63 | 3 |
| ApiKeyService | EmergencyRevokeApiKeys | emergency_revoke_api_keys | emergencyRevokeApiKeys | destructive | OK | 104.14 | 104.14 | 104.14 | 1 |
| ApiKeyService | GetApiKey | get_api_key | getApiKey | read_only | OK | 12.65 | 16.16 | 13.49 | 10 |
| ApiKeyService | GetApiKeyUsageStats | get_api_key_usage_stats | getApiKeyUsageStats | read_only | OK | 11.73 | 18.75 | 13.89 | 10 |
| ApiKeyService | ListApiKeys | list_api_keys | listApiKeys | read_only | OK | 12.64 | 14.63 | 12.97 | 10 |
| ApiKeyService | RevokeApiKey | revoke_api_key | revokeApiKey | mutation | OK | 37.63 | 37.63 | 37.63 | 3 |
| ApiKeyService | RotateApiKey | rotate_api_key | rotateApiKey | mutation | OK | 28.21 | 28.21 | 28.21 | 3 |
| ApiKeyService | UpdateApiKey | update_api_key | updateApiKey | mutation | OK | 19.23 | 19.23 | 20.06 | 3 |
| ApiKeyService | ValidateApiKey | validate_api_key | validateApiKey | read_only | OK | 24.88 | 31.97 | 24.87 | 10 |
| AssetService | CompleteStep | complete_step | completeStep | mutation | OK | 50.93 | 50.93 | 49.07 | 3 |
| AssetService | CreatePipelineDefinition | create_pipeline_definition | createPipelineDefinition | mutation | OK | 17.42 | 17.42 | 17.42 | 3 |
| AssetService | GetAsset | get_asset | getAsset | read_only | OK | 22.31 | 29.96 | 25.05 | 10 |
| AssetService | GetPipeline | get_pipeline | getPipeline | read_only | OK | 28.00 | 33.73 | 29.67 | 10 |
| AssetService | GetPipelineDefinition | get_pipeline_definition | getPipelineDefinition | read_only | OK | 24.44 | 28.38 | 25.82 | 10 |
| AssetService | ListAssets | list_assets | listAssets | read_only | OK | 27.91 | 28.53 | 27.62 | 10 |
| AssetService | RegisterAsset | register_asset | registerAsset | mutation | OK | 29.76 | 29.76 | 27.78 | 3 |
| AssetService | StartPipeline | start_pipeline | startPipeline | mutation | OK | 16.60 | 16.60 | 33.96 | 3 |
| AuthnService | AdminResetMfa | admin_reset_mfa | adminResetMfa | destructive | OK | 43.61 | 43.61 | 43.61 | 1 |
| AuthnService | AdminResetPassword | admin_reset_password | adminResetPassword | destructive | OK | 18.92 | 18.92 | 18.92 | 1 |
| AuthnService | AdminRevokeAllTenantSessions | admin_revoke_all_tenant_sessions | adminRevokeAllTenantSessions | destructive | OK | 20.13 | 20.13 | 20.13 | 1 |
| AuthnService | AdminRevokeAllUserSessions | admin_revoke_all_user_sessions | adminRevokeAllUserSessions | destructive | OK | 18.82 | 18.82 | 18.82 | 1 |
| AuthnService | AdminRevokeSession | admin_revoke_session | adminRevokeSession | destructive | OK | 26.38 | 26.38 | 26.38 | 1 |
| AuthnService | Authenticate | authenticate | authenticate | read_only | OK | 200.33 | 200.33 | 200.33 | 1 |
| AuthnService | ChangePassword | change_password | changePassword | mutation | OK | 1075.72 | 1075.72 | 1075.72 | 1 |
| AuthnService | ChangeUserStatus | change_user_status | changeUserStatus | destructive | OK | 22.15 | 22.15 | 22.15 | 1 |
| AuthnService | ConfirmMFAEnrollment | confirm_mfaenrollment | confirmMfaenrollment | mutation | OK | 9.79 | 9.79 | 9.64 | 3 |
| AuthnService | CreateSession | create_session | createSession | mutation | OK | 9.93 | 9.93 | 11.69 | 3 |
| AuthnService | CreateUser | create_user | createUser | mutation | OK | 512.66 | 512.66 | 521.26 | 3 |
| AuthnService | DeleteWebAuthnCredential | delete_web_authn_credential | deleteWebAuthnCredential | mutation | OK | 11.97 | 11.97 | 12.32 | 3 |
| AuthnService | DisableMfaFactor | disable_mfa_factor | disableMfaFactor | mutation | OK | 22.48 | 22.48 | 23.43 | 3 |
| AuthnService | EmergencyRevoke | emergency_revoke | emergencyRevoke | destructive | OK | 16.74 | 16.74 | 16.74 | 1 |
| AuthnService | EnrollMFA | enroll_mfa | enrollMfa | mutation | OK | 20.90 | 20.90 | 23.54 | 3 |
| AuthnService | FinishWebAuthnAuthentication | finish_web_authn_authentication | finishWebAuthnAuthentication | mutation | OK | 107.10 | 107.10 | 107.10 | 3 |
| AuthnService | FinishWebAuthnRegistration | finish_web_authn_registration | finishWebAuthnRegistration | mutation | OK | 69.24 | 69.24 | 69.24 | 3 |
| AuthnService | ForgotPassword | forgot_password | forgotPassword | mutation | OK | 13.26 | 13.26 | 12.93 | 3 |
| AuthnService | GenerateRecoveryCodes | generate_recovery_codes | generateRecoveryCodes | mutation | OK | 56.32 | 56.32 | 57.41 | 3 |
| AuthnService | GetJwks | get_jwks | getJwks | read_only | OK | 11.08 | 16.08 | 12.10 | 10 |
| AuthnService | GetMfaPolicy | get_mfa_policy | getMfaPolicy | read_only | OK | 9.49 | 10.82 | 9.21 | 10 |
| AuthnService | GetSession | get_session | getSession | read_only | OK | 9.72 | 12.87 | 10.66 | 10 |
| AuthnService | GetUser | get_user | getUser | read_only | OK | 9.40 | 11.92 | 9.84 | 10 |
| AuthnService | IntrospectToken | introspect_token | introspectToken | read_only | OK | 53.13 | 62.57 | 55.02 | 10 |
| AuthnService | IssueMfaChallenge | issue_mfa_challenge | issueMfaChallenge | mutation | OK | 19.28 | 19.28 | 20.50 | 3 |
| AuthnService | ListDevices | list_devices | listDevices | read_only | OK | 10.13 | 12.30 | 10.47 | 10 |
| AuthnService | ListMfaFactors | list_mfa_factors | listMfaFactors | read_only | OK | 13.50 | 14.20 | 13.20 | 10 |
| AuthnService | ListSessions | list_sessions | listSessions | read_only | OK | 14.86 | 22.26 | 17.14 | 10 |
| AuthnService | ListUsers | list_users | listUsers | read_only | OK | 17.59 | 18.73 | 17.25 | 10 |
| AuthnService | ListWebAuthnCredentials | list_web_authn_credentials | listWebAuthnCredentials | read_only | OK | 10.94 | 15.34 | 11.90 | 10 |
| AuthnService | Login | login | login | mutation | OK | 624.62 | 624.62 | 618.03 | 3 |
| AuthnService | Logout | logout | logout | mutation | OK | 8.54 | 8.54 | 19.58 | 3 |
| AuthnService | PutMfaPolicy | put_mfa_policy | putMfaPolicy | mutation | OK | 22.63 | 22.63 | 22.19 | 3 |
| AuthnService | RefreshSession | refresh_session | refreshSession | mutation | OK | 294.87 | 294.87 | 294.87 | 1 |
| AuthnService | RefreshToken | refresh_token | refreshToken | mutation | OK | 130.41 | 130.41 | 130.41 | 1 |
| AuthnService | RenamePasskey | rename_passkey | renamePasskey | mutation | OK | 13.24 | 13.24 | 13.29 | 3 |
| AuthnService | ResendOTP | resend_otp | resendOtp | mutation | OK | 29.81 | 29.81 | 29.81 | 1 |
| AuthnService | ResetPassword | reset_password | resetPassword | mutation | OK | 604.61 | 604.61 | 604.61 | 1 |
| AuthnService | RevokeDevice | revoke_device | revokeDevice | mutation | OK | 20.71 | 20.71 | 20.71 | 3 |
| AuthnService | RevokeRecoveryCodes | revoke_recovery_codes | revokeRecoveryCodes | mutation | OK | 17.86 | 17.86 | 18.09 | 3 |
| AuthnService | RevokeSession | revoke_session | revokeSession | mutation | OK | 10.54 | 10.54 | 9.58 | 3 |
| AuthnService | SendOTP | send_otp | sendOtp | mutation | OK | 23.17 | 23.17 | 23.17 | 1 |
| AuthnService | SendPhoneVerification | send_phone_verification | sendPhoneVerification | mutation | OK | 24.11 | 24.11 | 23.71 | 3 |
| AuthnService | StartWebAuthnAuthentication | start_web_authn_authentication | startWebAuthnAuthentication | mutation | OK | 30.11 | 30.11 | 29.62 | 3 |
| AuthnService | StartWebAuthnRegistration | start_web_authn_registration | startWebAuthnRegistration | mutation | OK | 26.50 | 26.50 | 26.26 | 3 |
| AuthnService | UpdateUser | update_user | updateUser | mutation | OK | 12.11 | 12.11 | 13.25 | 3 |
| AuthnService | ValidateCSRF | validate_csrf | validateCsrf | read_only | OK | 17.91 | 22.36 | 19.41 | 10 |
| AuthnService | ValidateToken | validate_token | validateToken | read_only | OK | 56.19 | 186.34 | 94.44 | 10 |
| AuthnService | VerifyMfaChallenge | verify_mfa_challenge | verifyMfaChallenge | read_only | OK | 18.86 | 18.86 | 18.86 | 1 |
| AuthnService | VerifyOTP | verify_otp | verifyOtp | read_only | OK | 28.99 | 28.99 | 28.99 | 1 |
| AuthzService | ActivateCanary | activate_canary | activateCanary | destructive | OK | 121.47 | 121.47 | 121.47 | 1 |
| AuthzService | ActivatePolicyVersion | activate_policy_version | activatePolicyVersion | destructive | OK | 137.20 | 137.20 | 137.20 | 1 |
| AuthzService | ApprovePolicyDraft | approve_policy_draft | approvePolicyDraft | mutation | OK | 61.92 | 61.92 | 61.92 | 3 |
| AuthzService | AssignRole | assign_role | assignRole | mutation | OK | 41.45 | 41.45 | 41.59 | 3 |
| AuthzService | Authorize | authorize | authorize | read_only | OK | 32.09 | 34.15 | 31.70 | 10 |
| AuthzService | BatchCheckPermissions | batch_check_permissions | batchCheckPermissions | read_only | OK | 16.23 | 20.53 | 17.26 | 10 |
| AuthzService | CheckAccess | check_access | checkAccess | read_only | OK | 18.79 | 25.06 | 19.82 | 10 |
| AuthzService | CreatePolicyDraft | create_policy_draft | createPolicyDraft | mutation | OK | 71.74 | 71.74 | 70.78 | 3 |
| AuthzService | CreatePolicyRule | create_policy_rule | createPolicyRule | mutation | OK | 30.28 | 30.28 | 30.19 | 3 |
| AuthzService | CreateRole | create_role | createRole | mutation | OK | 34.35 | 34.35 | 33.80 | 3 |
| AuthzService | DeletePolicyRule | delete_policy_rule | deletePolicyRule | mutation | OK | 12.59 | 12.59 | 12.87 | 3 |
| AuthzService | DeleteRole | delete_role | deleteRole | mutation | OK | 22.34 | 22.34 | 34.12 | 3 |
| AuthzService | DiffPolicyDraft | diff_policy_draft | diffPolicyDraft | read_only | OK | 22.03 | 31.56 | 25.82 | 10 |
| AuthzService | ExplainPolicy | explain_policy | explainPolicy | read_only | OK | 13.61 | 17.08 | 14.63 | 10 |
| AuthzService | GetAuthzRevision | get_authz_revision | getAuthzRevision | read_only | OK | 8.32 | 10.21 | 8.64 | 10 |
| AuthzService | GetCanaryStatus | get_canary_status | getCanaryStatus | read_only | OK | 21.20 | 22.88 | 21.23 | 10 |
| AuthzService | GetNativeAccess | get_native_access | getNativeAccess | read_only | OK | 30.11 | 40.70 | 32.43 | 10 |
| AuthzService | GetPolicyBundle | get_policy_bundle | getPolicyBundle | read_only | OK | 11.42 | 13.13 | 11.77 | 10 |
| AuthzService | GetPolicyRule | get_policy_rule | getPolicyRule | read_only | OK | 11.08 | 12.41 | 11.45 | 10 |
| AuthzService | GetRole | get_role | getRole | read_only | OK | 8.27 | 9.76 | 8.52 | 10 |
| AuthzService | InvalidatePolicyBundles | invalidate_policy_bundles | invalidatePolicyBundles | destructive | OK | 81.31 | 81.31 | 81.31 | 1 |
| AuthzService | LintAuthzPolicies | lint_authz_policies | lintAuthzPolicies | read_only | OK | 3.58 | 4.59 | 3.73 | 10 |
| AuthzService | ListAccessDecisionAudits | list_access_decision_audits | listAccessDecisionAudits | read_only | OK | 38.90 | 43.81 | 34.83 | 10 |
| AuthzService | ListPolicyRules | list_policy_rules | listPolicyRules | read_only | OK | 9.92 | 11.51 | 10.35 | 10 |
| AuthzService | ListPolicyVersions | list_policy_versions | listPolicyVersions | read_only | OK | 17.86 | 21.07 | 18.14 | 10 |
| AuthzService | ListRoles | list_roles | listRoles | read_only | OK | 12.11 | 18.70 | 13.66 | 10 |
| AuthzService | ListUserPermissions | list_user_permissions | listUserPermissions | read_only | OK | 3.52 | 5.16 | 3.95 | 10 |
| AuthzService | ListUserRoles | list_user_roles | listUserRoles | read_only | OK | 8.43 | 14.74 | 10.31 | 10 |
| AuthzService | MigrateLegacyPolicies | migrate_legacy_policies | migrateLegacyPolicies | destructive | OK | 71.28 | 71.28 | 71.28 | 1 |
| AuthzService | PromoteCanary | promote_canary | promoteCanary | destructive | OK | 114.64 | 114.64 | 114.64 | 1 |
| AuthzService | PutAuthzPolicy | put_authz_policy | putAuthzPolicy | mutation | OK | 27.06 | 27.06 | 27.59 | 3 |
| AuthzService | PutRelationship | put_relationship | putRelationship | mutation | OK | 35.49 | 35.49 | 35.86 | 3 |
| AuthzService | PutRoleBinding | put_role_binding | putRoleBinding | mutation | OK | 26.00 | 26.00 | 27.41 | 3 |
| AuthzService | RejectPolicyDraft | reject_policy_draft | rejectPolicyDraft | mutation | OK | 68.47 | 68.47 | 68.47 | 3 |
| AuthzService | RevokeRole | revoke_role | revokeRole | mutation | OK | 13.31 | 13.31 | 24.14 | 3 |
| AuthzService | RollbackPolicyVersion | rollback_policy_version | rollbackPolicyVersion | destructive | OK | 106.15 | 106.15 | 106.15 | 1 |
| AuthzService | SeedBuiltinRoles | seed_builtin_roles | seedBuiltinRoles | mutation | OK | 81.87 | 81.87 | 90.30 | 3 |
| AuthzService | SimulatePolicy | simulate_policy | simulatePolicy | mutation | OK | 25.13 | 25.13 | 31.81 | 3 |
| AuthzService | SubmitPolicyDraft | submit_policy_draft | submitPolicyDraft | mutation | OK | 27.44 | 27.44 | 27.44 | 3 |
| AuthzService | UpdatePolicyDraft | update_policy_draft | updatePolicyDraft | mutation | OK | 39.50 | 39.50 | 40.27 | 3 |
| AuthzService | UpdateRole | update_role | updateRole | mutation | OK | 33.31 | 33.31 | 34.86 | 3 |
| BackupService | DeleteBackupPolicy | delete_backup_policy | deleteBackupPolicy | mutation | OK | 21.75 | 21.75 | 20.77 | 3 |
| BackupService | GetBackup | get_backup | getBackup | read_only | OK | 32.67 | 40.82 | 33.24 | 10 |
| BackupService | GetBackupPolicy | get_backup_policy | getBackupPolicy | read_only | OK | 20.47 | 25.20 | 21.36 | 10 |
| BackupService | ListBackupPolicies | list_backup_policies | listBackupPolicies | read_only | OK | 18.12 | 23.21 | 19.55 | 10 |
| BackupService | ListBackups | list_backups | listBackups | read_only | OK | 21.81 | 32.72 | 23.27 | 10 |
| BackupService | PutBackupPolicy | put_backup_policy | putBackupPolicy | mutation | OK | 29.78 | 29.78 | 35.24 | 3 |
| BackupService | RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 2404.76 | 2404.76 | 2404.76 | 1 |
| BackupService | StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 1677.29 | 1677.29 | 1992.54 | 3 |
| CacheService | CreateNamespace | create_cache_namespace | createCacheNamespace | mutation | OK | 21.89 | 21.89 | 26.57 | 3 |
| CacheService | Delete | cache_delete | cacheNamespaceDelete | mutation | OK | 18.44 | 18.44 | 19.77 | 3 |
| CacheService | DeleteNamespace | delete_cache_namespace | deleteCacheNamespace | destructive | OK | 91.90 | 91.90 | 91.90 | 1 |
| CacheService | Get | cache_get | cacheNamespaceGet | read_only | OK | 14.09 | 16.40 | 14.29 | 10 |
| CacheService | GetNamespaceStats | get_cache_namespace_stats | getCacheNamespaceStats | read_only | OK | 91.45 | 111.87 | 94.51 | 10 |
| CacheService | Scan | cache_scan | cacheNamespaceScan | read_only | OK | 14.12 | 18.09 | 15.33 | 10 |
| CacheService | Set | cache_set | cacheNamespaceSet | mutation | OK | 21.58 | 21.58 | 26.90 | 3 |
| ConfigService | DeleteFlag | delete_flag | deleteFlag | destructive | OK | 29.65 | 29.65 | 29.65 | 1 |
| ConfigService | EvaluateFlags | evaluate_flags | evaluateFlags | read_only | OK | 18.76 | 21.70 | 19.12 | 10 |
| ConfigService | GetFlag | get_flag | getFlag | read_only | OK | 16.66 | 21.80 | 18.95 | 10 |
| ConfigService | ListFlags | list_flags | listFlags | read_only | OK | 20.95 | 22.24 | 20.10 | 10 |
| ConfigService | PutFlag | put_flag | putFlag | mutation | OK | 55.92 | 55.92 | 50.66 | 3 |
| ControlPlaneService | AckStatus | ack_status | ackStatus | mutation | OK | 12.37 | 12.37 | 12.42 | 3 |
| ControlPlaneService | DeltaResources | delta_resources | deltaResources | stream_first_recv | OK | 114.40 | 114.40 | 112.26 | 3 |
| ControlPlaneService | GetResources | get_resources | getResources | read_only | OK | 7.26 | 8.41 | 7.55 | 10 |
| ControlPlaneService | ListNodeStates | list_node_states | listNodeStates | read_only | OK | 45.75 | 55.09 | 48.31 | 10 |
| ControlPlaneService | RollbackResources | rollback_resources | rollbackResources | mutation | OK | 105.84 | 105.84 | 111.62 | 3 |
| ControlPlaneService | StreamResources | stream_resources | streamResources | stream_first_recv | OK | 101.72 | 101.72 | 99.19 | 3 |
| DataBroker | ActivateCatalog | activate_catalog | activateCatalog | destructive | OK | 132.38 | 132.38 | 132.38 | 1 |
| DataBroker | AnalyticalQuery | analytical_query | analyticalQuery | read_only | OK | 11.89 | 14.08 | 12.42 | 10 |
| DataBroker | ApplyMigration | apply_migration | applyMigration | mutation | OK | 466.23 | 466.23 | 466.23 | 3 |
| DataBroker | ApproveMigrationPlan | approve_migration_plan | approveMigrationPlan | mutation | OK | 109.58 | 109.58 | 109.58 | 3 |
| DataBroker | BatchSelect | batch_select | batchSelect | stream_first_recv | OK | 10.88 | 10.88 | 10.08 | 3 |
| DataBroker | BatchUpsert | batch_upsert | batchUpsert | stream_first_recv | OK | 113.98 | 113.98 | 121.18 | 3 |
| DataBroker | BeginTx | begin_tx | beginTx | stream_first_recv | OK | 42.56 | 42.56 | 41.80 | 3 |
| DataBroker | CacheDelete | cache_delete | cacheDelete | mutation | OK | 13.63 | 13.63 | 13.18 | 3 |
| DataBroker | CacheGet | cache_get | cacheGet | read_only | OK | 7.41 | 8.53 | 7.60 | 10 |
| DataBroker | CacheScan | cache_scan | cacheScan | read_only | OK | 14.35 | 16.85 | 15.24 | 10 |
| DataBroker | CacheSet | cache_set | cacheSet | mutation | OK | 13.59 | 13.59 | 13.55 | 3 |
| DataBroker | CreateMaterializedView | create_materialized_view | createMaterializedView | mutation | OK | 11.93 | 11.93 | 11.90 | 3 |
| DataBroker | Delete | delete | delete | mutation | OK | 129.39 | 129.39 | 123.57 | 3 |
| DataBroker | DeletePolicy | delete_policy | deletePolicy | mutation | OK | 48.92 | 48.92 | 48.92 | 3 |
| DataBroker | DismissDlqEvent | dismiss_dlq_event | dismissDlqEvent | mutation | OK | 20.87 | 20.87 | 23.01 | 3 |
| DataBroker | DocumentDelete | document_delete | documentDelete | mutation | OK | 10.85 | 10.85 | 12.02 | 3 |
| DataBroker | DocumentFind | document_find | documentFind | read_only | OK | 9.94 | 11.86 | 10.13 | 10 |
| DataBroker | DocumentGet | document_get | documentGet | read_only | OK | 10.60 | 11.72 | 10.19 | 10 |
| DataBroker | DocumentUpsert | document_upsert | documentUpsert | mutation | OK | 11.39 | 11.39 | 10.84 | 3 |
| DataBroker | DropResource | drop_resource | dropResource | destructive | OK | 48.78 | 48.78 | 48.78 | 1 |
| DataBroker | EnqueueOutboxEvent | enqueue_outbox_event | enqueueOutboxEvent | mutation | OK | 19.80 | 19.80 | 19.56 | 3 |
| DataBroker | EnsureBaseline | ensure_baseline | ensureBaseline | mutation | OK | 52.17 | 52.17 | 56.87 | 3 |
| DataBroker | EnsureProject | ensure_project | ensureProject | mutation | OK | 42.14 | 42.14 | 49.97 | 3 |
| DataBroker | EnsureResource | ensure_resource | ensureResource | mutation | OK | 29.54 | 29.54 | 30.95 | 3 |
| DataBroker | GeneratePresignedUrl | generate_presigned_url | generatePresignedUrl | mutation | OK | 8.01 | 8.01 | 7.40 | 3 |
| DataBroker | GenericDispatch | generic_dispatch | genericDispatch | mutation | OK | 9.03 | 9.03 | 9.12 | 3 |
| DataBroker | GetAdminSummary | get_admin_summary | getAdminSummary | read_only | OK | 39.41 | 44.28 | 39.92 | 10 |
| DataBroker | GetCapabilities | get_capabilities | getCapabilities | read_only | OK | 11.50 | 15.56 | 11.62 | 10 |
| DataBroker | GetCatalogManifest | get_catalog_manifest | getCatalogManifest | read_only | OK | 15.13 | 20.70 | 16.44 | 10 |
| DataBroker | GetCatalogVersion | get_catalog_version | getCatalogVersion | read_only | OK | 11.49 | 13.30 | 11.27 | 10 |
| DataBroker | GetCatalogVersions | get_catalog_versions | getCatalogVersions | read_only | OK | 9.48 | 10.41 | 9.43 | 10 |
| DataBroker | GetCdcStatus | get_cdc_status | getCdcStatus | read_only | OK | 9.75 | 11.55 | 13.95 | 10 |
| DataBroker | GetDlqEvent | get_dlq_event | getDlqEvent | read_only | OK | 9.47 | 10.77 | 9.16 | 10 |
| DataBroker | GetHealthReport | get_health_report | getHealthReport | read_only | OK | 4.48 | 5.67 | 4.63 | 10 |
| DataBroker | GetMigrationStatus | get_migration_status | getMigrationStatus | read_only | OK | 12.15 | 14.47 | 24.87 | 10 |
| DataBroker | GetObject | get_object | getObject | stream_first_recv | OK | 12.43 | 12.43 | 11.77 | 3 |
| DataBroker | GetSaga | get_saga | getSaga | read_only | OK | 9.47 | 12.27 | 12.28 | 10 |
| DataBroker | GraphMutate | graph_mutate | graphMutate | mutation | OK | 48.83 | 48.83 | 87.51 | 3 |
| DataBroker | GraphQuery | graph_query | graphQuery | read_only | OK | 24.15 | 34.76 | 28.60 | 10 |
| DataBroker | InitiateMultipartUpload | initiate_multipart_upload | initiateMultipartUpload | mutation | OK | 24.39 | 24.39 | 23.31 | 3 |
| DataBroker | LintPolicies | lint_policies | lintPolicies | read_only | OK | 8.63 | 18.16 | 11.36 | 10 |
| DataBroker | ListAdminAuditLogs | list_admin_audit_logs | listAdminAuditLogs | read_only | OK | 11.82 | 13.86 | 11.74 | 10 |
| DataBroker | ListDlqEvents | list_dlq_events | listDlqEvents | read_only | OK | 11.11 | 12.78 | 11.06 | 10 |
| DataBroker | ListMessageSchemas | list_message_schemas | listMessageSchemas | read_only | OK | 3.88 | 4.94 | 4.07 | 10 |
| DataBroker | ListMigrationRuns | list_migration_runs | listMigrationRuns | read_only | OK | 8.77 | 10.75 | 9.07 | 10 |
| DataBroker | ListPolicies | list_policies | listPolicies | read_only | OK | 9.34 | 13.47 | 10.64 | 10 |
| DataBroker | ListProjects | list_projects | listProjects | read_only | OK | 8.14 | 11.22 | 9.32 | 10 |
| DataBroker | ListResources | list_resources | listResources | read_only | OK | 7.94 | 8.84 | 7.91 | 10 |
| DataBroker | ListSagas | list_sagas | listSagas | read_only | OK | 11.50 | 14.68 | 17.39 | 10 |
| DataBroker | LookupMessageSchema | lookup_message_schema | lookupMessageSchema | read_only | OK | 3.60 | 4.85 | 3.93 | 10 |
| DataBroker | MarkSagaReviewed | mark_saga_reviewed | markSagaReviewed | mutation | OK | 46.16 | 46.16 | 53.91 | 3 |
| DataBroker | PauseCdc | pause_cdc | pauseCdc | mutation | OK | 31.70 | 31.70 | 31.78 | 3 |
| DataBroker | PlanMigration | plan_migration | planMigration | mutation | OK | 33.89 | 33.89 | 56.58 | 3 |
| DataBroker | PreviewCdcRedaction | preview_cdc_redaction | previewCdcRedaction | read_only | OK | 196.43 | 325.60 | 234.26 | 10 |
| DataBroker | PublishCDC | publish_cdc | publishCdc | cdc_first_event | OK | 285.97 | 285.97 | 285.97 | 1 |
| DataBroker | PutObject | put_object | putObject | stream_first_recv | OK | 38.82 | 38.82 | 49.27 | 3 |
| DataBroker | PutPolicy | put_policy | putPolicy | destructive | OK | 26.82 | 26.82 | 26.82 | 1 |
| DataBroker | QuarantineDlqEvent | quarantine_dlq_event | quarantineDlqEvent | mutation | OK | 21.83 | 21.83 | 28.13 | 3 |
| DataBroker | ReloadPolicies | reload_policies | reloadPolicies | destructive | OK | 25.32 | 25.32 | 25.32 | 1 |
| DataBroker | ReplayDlqEvent | replay_dlq_event | replayDlqEvent | mutation | OK | 46.52 | 46.52 | 46.52 | 3 |
| DataBroker | ResumeCdc | resume_cdc | resumeCdc | mutation | OK | 41.88 | 41.88 | 43.14 | 3 |
| DataBroker | RetrySagaCompensation | retry_saga_compensation | retrySagaCompensation | mutation | OK | 40.14 | 40.14 | 40.14 | 3 |
| DataBroker | RollbackCatalog | rollback_catalog | rollbackCatalog | destructive | OK | 11.31 | 11.31 | 11.31 | 1 |
| DataBroker | ScanProjectionDrift | scan_projection_drift | scanProjectionDrift | read_only | OK | 131.45 | 303.98 | 168.63 | 10 |
| DataBroker | Select | select | select | read_only | OK | 8.28 | 9.54 | 8.35 | 10 |
| DataBroker | SelectV2 | select_v_2 | selectV2 | stream_first_recv | OK | 8.51 | 8.51 | 8.14 | 3 |
| DataBroker | StageCatalog | stage_catalog | stageCatalog | destructive | OK | 639.17 | 639.17 | 639.17 | 1 |
| DataBroker | StepDownCdcLeader | step_down_cdc_leader | stepDownCdcLeader | mutation | OK | 35.77 | 35.77 | 35.62 | 3 |
| DataBroker | TimeSeriesQuery | time_series_query | timeSeriesQuery | read_only | OK | 17.69 | 25.88 | 19.80 | 10 |
| DataBroker | TimeSeriesWrite | time_series_write | timeSeriesWrite | mutation | OK | 19.71 | 19.71 | 20.98 | 3 |
| DataBroker | Upsert | upsert | upsert | mutation | OK | 115.13 | 115.13 | 117.75 | 3 |
| DataBroker | ValidateCatalog | validate_catalog | validateCatalog | destructive | OK | 76.33 | 76.33 | 76.33 | 1 |
| DataBroker | VectorBatchUpsert | vector_batch_upsert | vectorBatchUpsert | stream_first_recv | OK | 10.02 | 10.02 | 14.37 | 3 |
| DataBroker | VectorHybridSearch | vector_hybrid_search | vectorHybridSearch | read_only | OK | 7.90 | 8.65 | 7.78 | 10 |
| DataBroker | VectorSearch | vector_search | vectorSearch | read_only | OK | 8.23 | 8.61 | 8.03 | 10 |
| DataBroker | VectorUpsert | vector_upsert | vectorUpsert | mutation | OK | 17.65 | 17.65 | 19.56 | 3 |
| DataBroker | VerifyAdminAuditLog | verify_admin_audit_log | verifyAdminAuditLog | read_only | OK | 13.62 | 17.14 | 14.52 | 10 |
| EmbeddingService | Backfill | backfill | backfillEmbeddingSource | mutation | OK | 24.45 | 24.45 | 29.55 | 3 |
| EmbeddingService | DeleteSource | delete_source | deleteEmbeddingSource | destructive | OK | 37.71 | 37.71 | 37.71 | 1 |
| EmbeddingService | ListSources | list_sources | listEmbeddingSources | read_only | OK | 19.37 | 22.81 | 19.14 | 10 |
| EmbeddingService | RegisterSource | register_source | registerEmbeddingSource | mutation | OK | 39.64 | 39.64 | 38.17 | 3 |
| EmbeddingService | ReportEmbedding | report_embedding | reportEmbedding | mutation | OK | 39.35 | 39.35 | 46.26 | 3 |
| EmbeddingService | Retrieve | retrieve | retrieveEmbedding | read_only | OK | 25.80 | 39.12 | 30.18 | 10 |
| IdentityProviderService | CreateProvider | create_provider | createProvider | mutation | OK | 32.67 | 32.67 | 36.52 | 3 |
| IdentityProviderService | DisableProvider | disable_provider | disableProvider | mutation | OK | 40.18 | 40.18 | 37.78 | 3 |
| IdentityProviderService | ForceJwksRefresh | force_jwks_refresh | forceJwksRefresh | mutation | OK | 58.43 | 58.43 | 72.28 | 3 |
| IdentityProviderService | GetProvider | get_provider | getProvider | read_only | OK | 9.56 | 13.26 | 10.74 | 10 |
| IdentityProviderService | ImportSamlMetadata | import_saml_metadata | importSamlMetadata | mutation | OK | 19.92 | 19.92 | 28.27 | 3 |
| IdentityProviderService | LinkIdentity | link_identity | linkIdentity | mutation | OK | 33.45 | 33.45 | 35.94 | 3 |
| IdentityProviderService | ListExternalIdentities | list_external_identities | listExternalIdentities | read_only | OK | 14.28 | 16.45 | 14.81 | 10 |
| IdentityProviderService | ListProviders | list_providers | listProviders | read_only | OK | 17.20 | 22.05 | 18.03 | 10 |
| IdentityProviderService | PreviewClaimMapping | preview_claim_mapping | previewClaimMapping | read_only | OK | 7.78 | 8.84 | 8.07 | 10 |
| IdentityProviderService | PreviewGroupMapping | preview_group_mapping | previewGroupMapping | read_only | OK | 8.37 | 11.19 | 8.59 | 10 |
| IdentityProviderService | ResolveExternalIdentity | resolve_external_identity | resolveExternalIdentity | mutation | OK | 14.52 | 14.52 | 22.13 | 3 |
| IdentityProviderService | SamlAcs | saml_acs | samlAcs | mutation | OK | 89.29 | 89.29 | 95.79 | 3 |
| IdentityProviderService | ScimCreateGroup | scim_create_group | scimCreateGroup | mutation | OK | 7.37 | 7.37 | 7.75 | 3 |
| IdentityProviderService | ScimCreateUser | scim_create_user | scimCreateUser | mutation | OK | 41.81 | 41.81 | 50.49 | 3 |
| IdentityProviderService | ScimDeleteGroup | scim_delete_group | scimDeleteGroup | mutation | OK | 8.49 | 8.49 | 10.32 | 3 |
| IdentityProviderService | ScimDeleteUser | scim_delete_user | scimDeleteUser | mutation | OK | 122.19 | 122.19 | 122.19 | 3 |
| IdentityProviderService | ScimGetGroup | scim_get_group | scimGetGroup | mutation | OK | 11.59 | 11.59 | 11.29 | 3 |
| IdentityProviderService | ScimGetUser | scim_get_user | scimGetUser | mutation | OK | 12.10 | 12.10 | 12.19 | 3 |
| IdentityProviderService | ScimListGroups | scim_list_groups | scimListGroups | mutation | OK | 8.76 | 8.76 | 8.24 | 3 |
| IdentityProviderService | ScimListUsers | scim_list_users | scimListUsers | mutation | OK | 14.69 | 14.69 | 14.89 | 3 |
| IdentityProviderService | ScimPatchGroup | scim_patch_group | scimPatchGroup | mutation | OK | 21.33 | 21.33 | 25.36 | 3 |
| IdentityProviderService | ScimPatchUser | scim_patch_user | scimPatchUser | mutation | OK | 57.94 | 57.94 | 60.95 | 3 |
| IdentityProviderService | ScimReplaceUser | scim_replace_user | scimReplaceUser | mutation | OK | 37.89 | 37.89 | 45.54 | 3 |
| IdentityProviderService | StartSamlLogin | start_saml_login | startSamlLogin | mutation | OK | 6.97 | 6.97 | 6.81 | 3 |
| IdentityProviderService | TestProviderDiscovery | test_provider_discovery | testProviderDiscovery | read_only | OK | 8.56 | 10.24 | 8.83 | 10 |
| IdentityProviderService | UnlinkIdentity | unlink_identity | unlinkIdentity | mutation | OK | 8.52 | 8.52 | 12.48 | 3 |
| IdentityProviderService | UpdateProvider | update_provider | updateProvider | mutation | OK | 31.49 | 31.49 | 30.18 | 3 |
| LiveQueryService | Subscribe | subscribe | liveQuerySubscribe | stream_first_recv | OK | 24.21 | 24.21 | 22.78 | 3 |
| LockService | AcquireLock | acquire_lock | acquireLock | mutation | OK | 93.60 | 93.60 | 87.76 | 3 |
| LockService | ReleaseLock | release_lock | releaseLock | mutation | OK | 27.98 | 27.98 | 38.70 | 3 |
| LockService | RenewLock | renew_lock | renewLock | mutation | OK | 85.38 | 85.38 | 78.48 | 3 |
| MeteringService | CheckQuota | check_quota | checkQuota | read_only | OK | 28.01 | 33.39 | 29.12 | 10 |
| MeteringService | GetQuota | get_quota | getQuota | read_only | OK | 19.35 | 21.73 | 20.04 | 10 |
| MeteringService | ListQuotas | list_quotas | listQuotas | read_only | OK | 18.42 | 28.46 | 20.62 | 10 |
| MeteringService | PutQuota | put_quota | putQuota | mutation | OK | 49.81 | 49.81 | 49.04 | 3 |
| MeteringService | QueryUsage | query_usage | queryUsage | read_only | OK | 18.70 | 23.74 | 19.86 | 10 |
| MeteringService | RecordUsage | record_usage | recordUsage | mutation | OK | 20.19 | 20.19 | 21.83 | 3 |
| NotificationService | GetDeliveryStats | get_delivery_stats | getDeliveryStats | read_only | OK | 18.31 | 31.01 | 20.50 | 10 |
| NotificationService | GetNotification | get_notification | getNotification | read_only | OK | 35.55 | 44.78 | 38.36 | 10 |
| NotificationService | GetPreference | get_preference | getPreference | read_only | OK | 22.02 | 27.13 | 23.27 | 10 |
| NotificationService | GetTemplate | get_template | getTemplate | read_only | OK | 31.76 | 39.99 | 34.88 | 10 |
| NotificationService | ListNotifications | list_notifications | listNotifications | read_only | OK | 56.94 | 87.30 | 63.24 | 10 |
| NotificationService | ListPreferences | list_preferences | listPreferences | read_only | OK | 30.51 | 39.24 | 32.41 | 10 |
| NotificationService | ListTemplates | list_templates | listTemplates | read_only | OK | 37.50 | 55.26 | 44.21 | 10 |
| NotificationService | ReportDelivery | report_delivery | reportDelivery | mutation | OK | 36.49 | 36.49 | 34.28 | 3 |
| NotificationService | RetryNotification | retry_notification | retryNotification | mutation | OK | 25.40 | 25.40 | 25.40 | 3 |
| NotificationService | SendNotification | send_notification | sendNotification | mutation | OK | 65.25 | 65.25 | 68.48 | 3 |
| NotificationService | SetPreference | set_preference | setPreference | mutation | OK | 24.19 | 24.19 | 28.64 | 3 |
| NotificationService | UpsertTemplate | upsert_template | upsertTemplate | mutation | OK | 20.26 | 20.26 | 24.24 | 3 |
| PeerService | GetPeer | get_peer | getPeer | read_only | OK | 20.13 | 23.07 | 21.12 | 10 |
| PeerService | JoinRoom | join_room | joinRoom | mutation | OK | 50.81 | 50.81 | 52.56 | 3 |
| PeerService | JoinSession | join_session | joinSession | mutation | OK | 59.40 | 59.40 | 57.86 | 3 |
| PeerService | LeaveRoom | leave_room | leaveRoom | mutation | OK | 10.38 | 10.38 | 14.29 | 3 |
| PeerService | ListPeers | list_peers | listPeers | read_only | OK | 16.38 | 26.90 | 18.66 | 10 |
| RoomService | CloseRoom | close_room | closeRoom | mutation | OK | 50.50 | 50.50 | 51.84 | 3 |
| RoomService | CreateRoom | create_room | createRoom | mutation | OK | 44.81 | 44.81 | 42.56 | 3 |
| RoomService | GetRoom | get_room | getRoom | read_only | OK | 14.55 | 17.90 | 16.27 | 10 |
| RoomService | ListEgress | list_egress | listEgress | read_only | CAPABILITY_SKIPPED | 8.22 | 8.22 | 8.96 | 10 |
| RoomService | ListRooms | list_rooms | listRooms | read_only | OK | 17.04 | 27.33 | 18.79 | 10 |
| RoomService | StartRoomComposite | start_room_composite | startRoomComposite | mutation | CAPABILITY_SKIPPED | 9.78 | 9.78 | 9.78 | 3 |
| RoomService | StartTrackEgress | start_track_egress | startTrackEgress | mutation | CAPABILITY_SKIPPED | 6.72 | 6.72 | 6.72 | 3 |
| RoomService | StopEgress | stop_egress | stopEgress | mutation | CAPABILITY_SKIPPED | 12.41 | 12.41 | 12.41 | 3 |
| RoomService | UpdateRoom | update_room | updateRoom | mutation | OK | 18.05 | 18.05 | 18.82 | 3 |
| SchedulerService | CreateJob | create_job | createJob | mutation | OK | 27.60 | 27.60 | 31.05 | 3 |
| SchedulerService | DeleteJob | delete_job | deleteJob | destructive | OK | 17.64 | 17.64 | 17.64 | 1 |
| SchedulerService | GetJob | get_job | getJob | read_only | OK | 13.44 | 17.91 | 14.12 | 10 |
| SchedulerService | ListJobs | list_jobs | listJobs | read_only | OK | 22.38 | 28.39 | 23.23 | 10 |
| SchedulerService | PauseJob | pause_job | pauseJob | mutation | OK | 40.07 | 40.07 | 40.07 | 3 |
| SchedulerService | ResumeJob | resume_job | resumeJob | mutation | OK | 40.32 | 40.32 | 40.32 | 3 |
| SearchService | CreateIndex | create_index | createSearchIndex | mutation | OK | 53.72 | 53.72 | 61.11 | 3 |
| SearchService | DeleteIndex | delete_index | deleteSearchIndex | destructive | OK | 33.55 | 33.55 | 33.55 | 1 |
| SearchService | ListIndexes | list_indexes | listSearchIndexes | read_only | OK | 17.98 | 21.73 | 18.22 | 10 |
| SearchService | Reindex | reindex | reindexSearchIndex | mutation | OK | 36.79 | 36.79 | 40.82 | 3 |
| SearchService | Search | search | search | read_only | OK | 20.63 | 23.76 | 20.78 | 10 |
| SignalingService | Signal | signal | signal | stream_first_recv | OK | 22.68 | 22.68 | 22.68 | 3 |
| StorageService | DeleteFile | delete_file | deleteFile | mutation | OK | 47.81 | 47.81 | 47.81 | 3 |
| StorageService | DownloadFile | download_file | downloadFile | stream_first_recv | OK | 34.08 | 34.08 | 33.97 | 3 |
| StorageService | FinalizeUpload | finalize_upload | finalizeUpload | mutation | OK | 94.16 | 94.16 | 94.16 | 3 |
| StorageService | GetDownloadUrl | get_download_url | getDownloadUrl | read_only | OK | 20.44 | 24.31 | 21.35 | 10 |
| StorageService | GetFile | get_file | getFile | read_only | OK | 17.81 | 21.60 | 18.25 | 10 |
| StorageService | ListFiles | list_files | listFiles | read_only | OK | 23.87 | 31.80 | 25.32 | 10 |
| StorageService | RegisterUpload | register_upload | registerUpload | mutation | OK | 30.76 | 30.76 | 34.72 | 3 |
| StorageService | UpdateFile | update_file | updateFile | mutation | OK | 35.92 | 35.92 | 40.36 | 3 |
| TenantService | CreateTenant | create_tenant | createTenant | mutation | OK | 12.75 | 12.75 | 15.74 | 3 |
| TenantService | GetTenant | get_tenant | getTenant | read_only | OK | 18.19 | 29.67 | 20.56 | 10 |
| TenantService | GetTenantConfig | get_tenant_config | getTenantConfig | read_only | OK | 17.78 | 20.56 | 19.29 | 10 |
| TenantService | ListTenants | list_tenants | listTenants | read_only | OK | 14.29 | 17.03 | 14.89 | 10 |
| TenantService | PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 255.53 | 255.53 | 255.53 | 1 |
| TenantService | UpdateTenant | update_tenant | updateTenant | mutation | OK | 17.44 | 17.44 | 22.32 | 3 |
| TenantService | UpdateTenantConfig | update_tenant_config | updateTenantConfig | mutation | OK | 32.69 | 32.69 | 33.04 | 3 |
| TrackService | ListTracks | list_tracks | listTracks | read_only | OK | 17.13 | 19.85 | 17.88 | 10 |
| TrackService | MuteTrack | mute_track | muteTrack | mutation | OK | 13.79 | 13.79 | 18.61 | 3 |
| TrackService | PublishTrack | publish_track | publishTrack | mutation | OK | 35.32 | 35.32 | 35.11 | 3 |
| TrackService | UnpublishTrack | unpublish_track | unpublishTrack | mutation | OK | 14.17 | 14.17 | 13.96 | 3 |
| TurnService | IssueCredentials | issue_credentials | issueCredentials | mutation | OK | 22.44 | 22.44 | 25.94 | 3 |
| VaultService | CreateTransitKey | create_transit_key | createTransitKey | mutation | OK | 50.77 | 50.77 | 50.77 | 3 |
| VaultService | Decrypt | decrypt | vaultDecrypt | read_only | OK | 25.78 | 37.25 | 28.86 | 10 |
| VaultService | DeleteSecret | delete_secret | deleteSecret | mutation | OK | 31.63 | 31.63 | 28.24 | 3 |
| VaultService | DestroySecret | destroy_secret | destroySecret | destructive | OK | 26.82 | 26.82 | 26.82 | 1 |
| VaultService | Encrypt | encrypt | vaultEncrypt | mutation | OK | 14.12 | 14.12 | 14.11 | 3 |
| VaultService | GenerateDatabaseCredentials | generate_database_credentials | generateDatabaseCredentials | mutation | OK | 56.83 | 56.83 | 54.85 | 3 |
| VaultService | GetSecret | get_secret | getSecret | read_only | OK | 22.74 | 34.73 | 27.09 | 10 |
| VaultService | Hmac | hmac | vaultHmac | mutation | OK | 23.20 | 23.20 | 22.65 | 3 |
| VaultService | ListSecrets | list_secrets | listSecrets | read_only | OK | 16.13 | 31.17 | 21.62 | 10 |
| VaultService | PutSecret | put_secret | putSecret | mutation | OK | 51.65 | 51.65 | 51.65 | 3 |
| VaultService | RotateTransitKey | rotate_transit_key | rotateTransitKey | mutation | OK | 61.44 | 61.44 | 61.66 | 3 |
| VaultService | SealStatus | seal_status | vaultSealStatus | read_only | OK | 3.82 | 4.93 | 4.06 | 10 |
| VaultService | Sign | sign | vaultSign | mutation | OK | 18.96 | 18.96 | 22.78 | 3 |
| VaultService | Verify | verify | vaultVerify | read_only | OK | 20.06 | 31.13 | 23.21 | 10 |
| WebhookService | CreateEndpoint | create_endpoint | createWebhookEndpoint | mutation | OK | 24.19 | 24.19 | 29.48 | 3 |
| WebhookService | DeleteEndpoint | delete_endpoint | deleteWebhookEndpoint | destructive | OK | 33.35 | 33.35 | 33.35 | 1 |
| WebhookService | GetEndpoint | get_endpoint | getWebhookEndpoint | read_only | OK | 13.65 | 30.54 | 17.09 | 10 |
| WebhookService | ListDeliveries | list_deliveries | listWebhookDeliveries | read_only | OK | 17.09 | 17.88 | 16.78 | 10 |
| WebhookService | ListEndpoints | list_endpoints | listWebhookEndpoints | read_only | OK | 18.41 | 32.27 | 25.15 | 10 |
| WebhookService | UpdateEndpoint | update_endpoint | updateWebhookEndpoint | mutation | OK | 26.77 | 26.77 | 30.84 | 3 |
| WorkflowService | CancelWorkflow | cancel_workflow | cancelWorkflow | destructive | OK | 30.96 | 30.96 | 30.96 | 1 |
| WorkflowService | GetWorkflow | get_workflow | getWorkflow | read_only | OK | 14.57 | 24.85 | 16.54 | 10 |
| WorkflowService | ListWorkflows | list_workflows | listWorkflows | read_only | OK | 15.96 | 20.09 | 16.87 | 10 |
| WorkflowService | SignalWorkflow | signal_workflow | signalWorkflow | mutation | OK | 24.87 | 24.87 | 32.95 | 3 |
| WorkflowService | StartWorkflow | start_workflow | startWorkflow | mutation | OK | 33.38 | 33.38 | 32.53 | 3 |
