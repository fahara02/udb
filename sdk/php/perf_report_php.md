# UDB SDK Live Perf — PHP (Docker → host)

RPCs measured: 344   tenant=cc286635-5627-4cb6-ba4d-ef4b7f0dd718

Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities (a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request's reference/ID fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection latency. The TARGET is zero failures; any residual non-OK RPC is listed under Failures for the maintainer to finish.

Unary = full request/response round-trip. Streaming rows (kind=stream_open) report stream-open latency (initiate + cancel, no response drain), NOT first-message latency.

## Seeded fixtures

Captured semantic field -> seeded value keys used to resolve request fields: action, apply_run_id, approval_token, approve_draft_id, approve_run_id, approved_by, asset_id, assigned_by, auth_challenge_id, backup_id, bucket, canary_id, canary_version_id, cancel_workflow_id, catalog_manifest, catalog_manifest_b64, challenge_id, close_room_id, code, content_type, created_by, csrf_token, definition_id, delete_endpoint_id, delete_file_id, delete_policy_id, delete_role_id, delete_scim_user_id, deleted_by, device_id, disable_provider_id, dismiss_dlq_id, dlq_id, document_id, domain, ds_policy_id, egress_id, endpoint_id, event_type, external_identity_id, file_id, file_type, filename, finalize_file_id, gov_exp, instance_id, job_id, join_session_room_id, key_id, key_prefix, kind, leave_peer_id, locale, log_id, mark_saga_id, message_type, migration_id, mongo_collection, name, node_id, notification_id, object, object_key, otp_code, otp_id, owner_id, peer_id, plain_key, policy_draft_id, policy_id, policy_version_id, project, project_id, provider_id, quarantine_dlq_id, recipient_id, record_id, recovery_code, refresh_session_id, refresh_token, refresh_token_session_id, reg_challenge_id, reject_draft_id, rejected_by, relation, release_fencing_token, renew_fencing_token, replay_dlq_id, reset_otp_code, reset_otp_id, resource, resource_name, restore_tenant_id, retry_saga_id, revoke_key_id, revoke_key_prefix, revoked_by, role, role_code, role_id, rollback_policy_set_id, rollback_resource_version, rollback_target_version_id, room_id, saga_id, saml_provider_id, scim_group_id, scim_user_id, session_id, stage_name, step_id, subject, tenant, tenant_code, tenant_id, token, topic_pattern, track_id, ts_table, unpublish_track_id, update_draft_id, update_key_id, update_key_prefix, updated_by, user_id, username, vault_ciphertext, vault_create_key_name, vault_db_role, vault_delete_secret_path, vault_destroy_secret_path, vault_key_name, vault_put_secret_path, vault_secret_path, vault_signature, workflow_id

## Per-service mean latency

| Service | RPCs | mean ms |
|---|--:|--:|
| BackupService | 8 | 1220.38 |
| LockService | 3 | 362.93 |
| TenantService | 7 | 312.82 |
| AuthnService | 50 | 239.91 |
| CacheService | 7 | 170.30 |
| AuthzService | 41 | 151.57 |
| DataBroker | 77 | 149.69 |
| IdentityProviderService | 27 | 146.06 |
| EmbeddingService | 6 | 141.25 |
| ConfigService | 5 | 124.59 |
| SchedulerService | 6 | 123.06 |
| PeerService | 5 | 112.41 |
| WorkflowService | 5 | 110.81 |
| ApiKeyService | 9 | 108.45 |
| VaultService | 14 | 106.22 |
| StorageService | 8 | 104.24 |
| NotificationService | 12 | 103.03 |
| ControlPlaneService | 6 | 100.94 |
| AssetService | 8 | 82.41 |
| SearchService | 5 | 82.12 |
| TurnService | 1 | 76.31 |
| MeteringService | 6 | 71.96 |
| WebhookService | 6 | 65.85 |
| TrackService | 4 | 65.20 |
| RoomService | 9 | 49.63 |
| AnalyticsService | 7 | 28.69 |
| LiveQueryService | 1 | 0.87 |
| SignalingService | 1 | 0.34 |

## Capability skips (4)

| RPC | api_alias | operation_id | kind | p99 ms |
|---|---|---|---|--:|
| RoomService/ListEgress | list_egress | listEgress | read_only | 23.47 |
| RoomService/StartRoomComposite | start_room_composite | startRoomComposite | mutation | 15.14 |
| RoomService/StartTrackEgress | start_track_egress | startTrackEgress | mutation | 10.48 |
| RoomService/StopEgress | stop_egress | stopEgress | mutation | 14.91 |

## Failures (0)

No RPC returned a non-OK gRPC status.

## Slowest 20 by p99

| RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms |
|---|---|---|---|---|--:|--:|--:|
| BackupService/RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 4670.56 | 4670.56 | 4670.56 |
| BackupService/StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 4501.35 | 4535.19 | 4470.57 |
| AuthnService/ChangePassword | change_password | changePassword | mutation | OK | 2294.14 | 2294.14 | 2294.14 |
| DataBroker/StageCatalog | stage_catalog | stageCatalog | destructive | OK | 1915.09 | 1915.09 | 1915.09 |
| TenantService/PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 1838.14 | 1838.14 | 1838.14 |
| DataBroker/ApplyMigration | apply_migration | applyMigration | mutation | OK | 1480.60 | 1480.60 | 1480.60 |
| AuthnService/ResetPassword | reset_password | resetPassword | mutation | OK | 1364.05 | 1364.05 | 1364.05 |
| LockService/RenewLock | renew_lock | renewLock | mutation | OK | 285.24 | 1183.42 | 673.50 |
| DataBroker/Upsert | upsert | upsert | mutation | OK | 842.89 | 1040.62 | 875.04 |
| AuthnService/CreateUser | create_user | createUser | mutation | OK | 883.25 | 1002.75 | 922.35 |
| CacheService/GetNamespaceStats | get_cache_namespace_stats | getCacheNamespaceStats | read_only | OK | 376.38 | 946.92 | 434.92 |
| AuthnService/AdminResetMfa | admin_reset_mfa | adminResetMfa | destructive | OK | 937.05 | 937.05 | 937.05 |
| DataBroker/Delete | delete | delete | mutation | OK | 811.19 | 822.04 | 812.79 |
| DataBroker/GetCatalogManifest | get_catalog_manifest | getCatalogManifest | read_only | OK | 360.96 | 815.07 | 446.73 |
| AuthzService/MigrateLegacyPolicies | migrate_legacy_policies | migrateLegacyPolicies | destructive | OK | 793.05 | 793.05 | 793.05 |
| AuthzService/InvalidatePolicyBundles | invalidate_policy_bundles | invalidatePolicyBundles | destructive | OK | 750.57 | 750.57 | 750.57 |
| AuthzService/PromoteCanary | promote_canary | promoteCanary | destructive | OK | 736.46 | 736.46 | 736.46 |
| AuthnService/Login | login | login | mutation | OK | 636.42 | 638.35 | 647.17 |
| DataBroker/ValidateCatalog | validate_catalog | validateCatalog | destructive | OK | 552.83 | 552.83 | 552.83 |
| DataBroker/ActivateCatalog | activate_catalog | activateCatalog | destructive | OK | 531.28 | 531.28 | 531.28 |

## Full per-RPC table (sorted by service, then RPC)

| Service | RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms | iters |
|---|---|---|---|---|---|--:|--:|--:|--:|
| AnalyticsService | GetExecutorPerformance | get_executor_performance | getExecutorPerformance | read_only | OK | 31.82 | 50.97 | 30.64 | 25 |
| AnalyticsService | GetPipelineSummary | get_pipeline_summary | getPipelineSummary | read_only | OK | 12.49 | 14.11 | 11.74 | 25 |
| AnalyticsService | GetReconciliationAnalytics | get_reconciliation_analytics | getReconciliationAnalytics | read_only | OK | 28.81 | 55.81 | 30.43 | 25 |
| AnalyticsService | GetSlaCompliance | get_sla_compliance | getSlaCompliance | read_only | OK | 21.12 | 31.97 | 19.79 | 25 |
| AnalyticsService | GetThroughput | get_throughput | getThroughput | read_only | OK | 9.03 | 13.17 | 9.65 | 25 |
| AnalyticsService | RecordPipelineMetric | record_pipeline_metric | recordPipelineMetric | mutation | OK | 32.26 | 60.23 | 40.46 | 5 |
| AnalyticsService | TriggerSnapshot | trigger_snapshot | triggerSnapshot | mutation | OK | 36.73 | 52.87 | 58.13 | 5 |
| ApiKeyService | CreateApiKey | create_api_key | createApiKey | mutation | OK | 114.35 | 115.62 | 99.49 | 5 |
| ApiKeyService | EmergencyRevokeApiKeys | emergency_revoke_api_keys | emergencyRevokeApiKeys | destructive | OK | 408.54 | 408.54 | 408.54 | 1 |
| ApiKeyService | GetApiKey | get_api_key | getApiKey | read_only | OK | 41.01 | 50.64 | 37.07 | 25 |
| ApiKeyService | GetApiKeyUsageStats | get_api_key_usage_stats | getApiKeyUsageStats | read_only | OK | 13.16 | 46.11 | 19.53 | 25 |
| ApiKeyService | ListApiKeys | list_api_keys | listApiKeys | read_only | OK | 8.49 | 11.64 | 9.09 | 25 |
| ApiKeyService | RevokeApiKey | revoke_api_key | revokeApiKey | mutation | OK | 151.18 | 151.18 | 151.18 | 1 |
| ApiKeyService | RotateApiKey | rotate_api_key | rotateApiKey | mutation | OK | 112.31 | 112.31 | 112.31 | 1 |
| ApiKeyService | UpdateApiKey | update_api_key | updateApiKey | mutation | OK | 91.55 | 107.74 | 94.37 | 5 |
| ApiKeyService | ValidateApiKey | validate_api_key | validateApiKey | read_only | OK | 45.63 | 64.01 | 44.52 | 25 |
| AssetService | CompleteStep | complete_step | completeStep | mutation | OK | 158.95 | 164.36 | 152.35 | 5 |
| AssetService | CreatePipelineDefinition | create_pipeline_definition | createPipelineDefinition | mutation | OK | 66.58 | 79.69 | 87.97 | 5 |
| AssetService | GetAsset | get_asset | getAsset | read_only | OK | 27.19 | 56.70 | 32.14 | 25 |
| AssetService | GetPipeline | get_pipeline | getPipeline | read_only | OK | 40.93 | 60.77 | 38.12 | 25 |
| AssetService | GetPipelineDefinition | get_pipeline_definition | getPipelineDefinition | read_only | OK | 35.23 | 58.48 | 34.48 | 25 |
| AssetService | ListAssets | list_assets | listAssets | read_only | OK | 52.30 | 64.57 | 45.51 | 25 |
| AssetService | RegisterAsset | register_asset | registerAsset | mutation | OK | 164.00 | 192.25 | 162.00 | 5 |
| AssetService | StartPipeline | start_pipeline | startPipeline | mutation | OK | 41.96 | 129.13 | 106.67 | 5 |
| AuthnService | AdminResetMfa | admin_reset_mfa | adminResetMfa | destructive | OK | 937.05 | 937.05 | 937.05 | 1 |
| AuthnService | AdminResetPassword | admin_reset_password | adminResetPassword | destructive | OK | 204.17 | 204.17 | 204.17 | 1 |
| AuthnService | AdminRevokeAllTenantSessions | admin_revoke_all_tenant_sessions | adminRevokeAllTenantSessions | destructive | OK | 134.07 | 134.07 | 134.07 | 1 |
| AuthnService | AdminRevokeAllUserSessions | admin_revoke_all_user_sessions | adminRevokeAllUserSessions | destructive | OK | 298.03 | 298.03 | 298.03 | 1 |
| AuthnService | AdminRevokeSession | admin_revoke_session | adminRevokeSession | destructive | OK | 468.10 | 468.10 | 468.10 | 1 |
| AuthnService | Authenticate | authenticate | authenticate | read_only | OK | 127.99 | 187.18 | 125.60 | 25 |
| AuthnService | ChangePassword | change_password | changePassword | mutation | OK | 2294.14 | 2294.14 | 2294.14 | 1 |
| AuthnService | ChangeUserStatus | change_user_status | changeUserStatus | destructive | OK | 342.04 | 342.04 | 342.04 | 1 |
| AuthnService | ConfirmMFAEnrollment | confirm_mfaenrollment | confirmMfaenrollment | mutation | OK | 17.83 | 40.13 | 48.93 | 5 |
| AuthnService | CreateSession | create_session | createSession | mutation | OK | 53.77 | 67.89 | 91.67 | 5 |
| AuthnService | CreateUser | create_user | createUser | mutation | OK | 883.25 | 1002.75 | 922.35 | 5 |
| AuthnService | DeleteWebAuthnCredential | delete_web_authn_credential | deleteWebAuthnCredential | mutation | OK | 56.21 | 60.10 | 103.47 | 5 |
| AuthnService | DisableMfaFactor | disable_mfa_factor | disableMfaFactor | mutation | OK | 152.33 | 179.85 | 157.27 | 5 |
| AuthnService | EmergencyRevoke | emergency_revoke | emergencyRevoke | destructive | OK | 136.59 | 136.59 | 136.59 | 1 |
| AuthnService | EnrollMFA | enroll_mfa | enrollMfa | mutation | OK | 73.53 | 78.64 | 60.06 | 5 |
| AuthnService | FinishWebAuthnAuthentication | finish_web_authn_authentication | finishWebAuthnAuthentication | mutation | OK | 430.35 | 430.35 | 430.35 | 1 |
| AuthnService | FinishWebAuthnRegistration | finish_web_authn_registration | finishWebAuthnRegistration | mutation | OK | 190.93 | 190.93 | 190.93 | 1 |
| AuthnService | ForgotPassword | forgot_password | forgotPassword | mutation | OK | 37.00 | 37.00 | 49.05 | 5 |
| AuthnService | GenerateRecoveryCodes | generate_recovery_codes | generateRecoveryCodes | mutation | OK | 257.12 | 289.89 | 251.37 | 5 |
| AuthnService | GetJwks | get_jwks | getJwks | read_only | OK | 21.85 | 62.77 | 27.26 | 25 |
| AuthnService | GetMfaPolicy | get_mfa_policy | getMfaPolicy | read_only | OK | 64.07 | 107.83 | 69.54 | 25 |
| AuthnService | GetSession | get_session | getSession | read_only | OK | 18.63 | 54.56 | 23.22 | 25 |
| AuthnService | GetUser | get_user | getUser | read_only | OK | 13.57 | 28.44 | 15.77 | 25 |
| AuthnService | IntrospectToken | introspect_token | introspectToken | read_only | OK | 136.77 | 221.15 | 144.30 | 25 |
| AuthnService | IssueMfaChallenge | issue_mfa_challenge | issueMfaChallenge | mutation | OK | 50.77 | 126.25 | 82.60 | 5 |
| AuthnService | ListDevices | list_devices | listDevices | read_only | OK | 53.48 | 73.71 | 53.40 | 25 |
| AuthnService | ListMfaFactors | list_mfa_factors | listMfaFactors | read_only | OK | 39.01 | 109.75 | 47.81 | 25 |
| AuthnService | ListSessions | list_sessions | listSessions | read_only | OK | 49.22 | 102.07 | 46.75 | 25 |
| AuthnService | ListUsers | list_users | listUsers | read_only | OK | 18.00 | 26.33 | 19.94 | 25 |
| AuthnService | ListWebAuthnCredentials | list_web_authn_credentials | listWebAuthnCredentials | read_only | OK | 12.21 | 25.05 | 14.28 | 25 |
| AuthnService | Login | login | login | mutation | OK | 636.42 | 638.35 | 647.17 | 5 |
| AuthnService | Logout | logout | logout | mutation | OK | 33.65 | 48.57 | 90.28 | 5 |
| AuthnService | PutMfaPolicy | put_mfa_policy | putMfaPolicy | mutation | OK | 34.55 | 41.93 | 87.86 | 5 |
| AuthnService | RefreshSession | refresh_session | refreshSession | mutation | OK | 145.41 | 191.44 | 164.30 | 5 |
| AuthnService | RefreshToken | refresh_token | refreshToken | mutation | OK | 378.04 | 378.04 | 378.04 | 1 |
| AuthnService | RenamePasskey | rename_passkey | renamePasskey | mutation | OK | 20.75 | 26.04 | 33.55 | 5 |
| AuthnService | ResendOTP | resend_otp | resendOtp | mutation | OK | 114.97 | 118.23 | 124.66 | 5 |
| AuthnService | ResetPassword | reset_password | resetPassword | mutation | OK | 1364.05 | 1364.05 | 1364.05 | 1 |
| AuthnService | RevokeDevice | revoke_device | revokeDevice | mutation | OK | 409.44 | 409.44 | 409.44 | 1 |
| AuthnService | RevokeRecoveryCodes | revoke_recovery_codes | revokeRecoveryCodes | mutation | OK | 89.17 | 124.06 | 131.34 | 5 |
| AuthnService | RevokeSession | revoke_session | revokeSession | mutation | OK | 48.53 | 58.67 | 92.24 | 5 |
| AuthnService | SendOTP | send_otp | sendOtp | mutation | OK | 75.30 | 85.56 | 65.98 | 5 |
| AuthnService | SendPhoneVerification | send_phone_verification | sendPhoneVerification | mutation | OK | 67.47 | 69.80 | 73.61 | 5 |
| AuthnService | StartWebAuthnAuthentication | start_web_authn_authentication | startWebAuthnAuthentication | mutation | OK | 28.34 | 49.69 | 40.42 | 5 |
| AuthnService | StartWebAuthnRegistration | start_web_authn_registration | startWebAuthnRegistration | mutation | OK | 86.98 | 87.09 | 92.83 | 5 |
| AuthnService | UpdateUser | update_user | updateUser | mutation | OK | 77.97 | 80.58 | 158.65 | 5 |
| AuthnService | ValidateCSRF | validate_csrf | validateCsrf | read_only | OK | 14.58 | 71.08 | 25.82 | 25 |
| AuthnService | ValidateToken | validate_token | validateToken | read_only | OK | 106.92 | 176.05 | 117.81 | 25 |
| AuthnService | VerifyMfaChallenge | verify_mfa_challenge | verifyMfaChallenge | read_only | OK | 45.48 | 111.96 | 61.99 | 25 |
| AuthnService | VerifyOTP | verify_otp | verifyOtp | read_only | OK | 40.56 | 70.52 | 45.23 | 25 |
| AuthzService | ActivateCanary | activate_canary | activateCanary | destructive | OK | 201.18 | 201.18 | 201.18 | 1 |
| AuthzService | ActivatePolicyVersion | activate_policy_version | activatePolicyVersion | destructive | OK | 230.69 | 230.69 | 230.69 | 1 |
| AuthzService | ApprovePolicyDraft | approve_policy_draft | approvePolicyDraft | mutation | OK | 184.08 | 184.08 | 184.08 | 1 |
| AuthzService | AssignRole | assign_role | assignRole | mutation | OK | 180.93 | 207.98 | 176.41 | 5 |
| AuthzService | Authorize | authorize | authorize | read_only | OK | 42.22 | 93.92 | 50.82 | 25 |
| AuthzService | BatchCheckPermissions | batch_check_permissions | batchCheckPermissions | read_only | OK | 36.12 | 58.45 | 36.68 | 25 |
| AuthzService | CheckAccess | check_access | checkAccess | read_only | OK | 66.95 | 127.03 | 71.42 | 25 |
| AuthzService | CreatePolicyDraft | create_policy_draft | createPolicyDraft | mutation | OK | 309.09 | 327.49 | 297.36 | 5 |
| AuthzService | CreatePolicyRule | create_policy_rule | createPolicyRule | mutation | OK | 90.03 | 118.81 | 93.00 | 5 |
| AuthzService | CreateRole | create_role | createRole | mutation | OK | 102.00 | 103.30 | 107.36 | 5 |
| AuthzService | DeletePolicyRule | delete_policy_rule | deletePolicyRule | mutation | OK | 38.13 | 66.84 | 45.85 | 5 |
| AuthzService | DeleteRole | delete_role | deleteRole | mutation | OK | 46.24 | 73.76 | 76.38 | 5 |
| AuthzService | DiffPolicyDraft | diff_policy_draft | diffPolicyDraft | read_only | OK | 36.57 | 122.49 | 52.29 | 25 |
| AuthzService | ExplainPolicy | explain_policy | explainPolicy | read_only | OK | 27.68 | 47.20 | 29.34 | 25 |
| AuthzService | GetAuthzRevision | get_authz_revision | getAuthzRevision | read_only | OK | 22.07 | 62.88 | 27.95 | 25 |
| AuthzService | GetCanaryStatus | get_canary_status | getCanaryStatus | read_only | OK | 41.92 | 77.56 | 45.22 | 25 |
| AuthzService | GetNativeAccess | get_native_access | getNativeAccess | read_only | OK | 45.01 | 81.30 | 49.08 | 25 |
| AuthzService | GetPolicyBundle | get_policy_bundle | getPolicyBundle | read_only | OK | 33.64 | 64.11 | 34.30 | 25 |
| AuthzService | GetPolicyRule | get_policy_rule | getPolicyRule | read_only | OK | 28.36 | 47.24 | 27.43 | 25 |
| AuthzService | GetRole | get_role | getRole | read_only | OK | 43.69 | 67.06 | 41.91 | 25 |
| AuthzService | InvalidatePolicyBundles | invalidate_policy_bundles | invalidatePolicyBundles | destructive | OK | 750.57 | 750.57 | 750.57 | 1 |
| AuthzService | LintAuthzPolicies | lint_authz_policies | lintAuthzPolicies | read_only | OK | 3.82 | 13.02 | 5.30 | 25 |
| AuthzService | ListAccessDecisionAudits | list_access_decision_audits | listAccessDecisionAudits | read_only | OK | 38.60 | 75.48 | 44.02 | 25 |
| AuthzService | ListPolicyRules | list_policy_rules | listPolicyRules | read_only | OK | 12.92 | 43.92 | 17.98 | 25 |
| AuthzService | ListPolicyVersions | list_policy_versions | listPolicyVersions | read_only | OK | 40.81 | 105.68 | 50.63 | 25 |
| AuthzService | ListRoles | list_roles | listRoles | read_only | OK | 34.71 | 79.78 | 37.28 | 25 |
| AuthzService | ListUserPermissions | list_user_permissions | listUserPermissions | read_only | OK | 4.37 | 6.38 | 4.62 | 25 |
| AuthzService | ListUserRoles | list_user_roles | listUserRoles | read_only | OK | 43.54 | 126.33 | 51.38 | 25 |
| AuthzService | MigrateLegacyPolicies | migrate_legacy_policies | migrateLegacyPolicies | destructive | OK | 793.05 | 793.05 | 793.05 | 1 |
| AuthzService | PromoteCanary | promote_canary | promoteCanary | destructive | OK | 736.46 | 736.46 | 736.46 | 1 |
| AuthzService | PutAuthzPolicy | put_authz_policy | putAuthzPolicy | mutation | OK | 54.23 | 76.11 | 62.44 | 5 |
| AuthzService | PutRelationship | put_relationship | putRelationship | mutation | OK | 105.25 | 151.82 | 131.83 | 5 |
| AuthzService | PutRoleBinding | put_role_binding | putRoleBinding | mutation | OK | 79.75 | 101.93 | 100.49 | 5 |
| AuthzService | RejectPolicyDraft | reject_policy_draft | rejectPolicyDraft | mutation | OK | 184.30 | 184.30 | 184.30 | 1 |
| AuthzService | RevokeRole | revoke_role | revokeRole | mutation | OK | 74.94 | 82.01 | 110.29 | 5 |
| AuthzService | RollbackPolicyVersion | rollback_policy_version | rollbackPolicyVersion | destructive | OK | 371.71 | 371.71 | 371.71 | 1 |
| AuthzService | SeedBuiltinRoles | seed_builtin_roles | seedBuiltinRoles | mutation | OK | 311.84 | 332.91 | 305.29 | 5 |
| AuthzService | SimulatePolicy | simulate_policy | simulatePolicy | mutation | OK | 44.45 | 46.98 | 103.58 | 5 |
| AuthzService | SubmitPolicyDraft | submit_policy_draft | submitPolicyDraft | mutation | OK | 154.35 | 154.35 | 154.35 | 1 |
| AuthzService | UpdatePolicyDraft | update_policy_draft | updatePolicyDraft | mutation | OK | 246.48 | 253.62 | 215.49 | 5 |
| AuthzService | UpdateRole | update_role | updateRole | mutation | OK | 112.15 | 126.43 | 104.36 | 5 |
| BackupService | DeleteBackupPolicy | delete_backup_policy | deleteBackupPolicy | mutation | OK | 118.63 | 129.56 | 160.80 | 5 |
| BackupService | GetBackup | get_backup | getBackup | read_only | OK | 64.38 | 137.43 | 68.70 | 25 |
| BackupService | GetBackupPolicy | get_backup_policy | getBackupPolicy | read_only | OK | 97.10 | 207.52 | 112.19 | 25 |
| BackupService | ListBackupPolicies | list_backup_policies | listBackupPolicies | read_only | OK | 57.14 | 193.04 | 75.64 | 25 |
| BackupService | ListBackups | list_backups | listBackups | read_only | OK | 51.31 | 89.93 | 48.96 | 25 |
| BackupService | PutBackupPolicy | put_backup_policy | putBackupPolicy | mutation | OK | 158.55 | 168.06 | 155.61 | 5 |
| BackupService | RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 4670.56 | 4670.56 | 4670.56 | 1 |
| BackupService | StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 4501.35 | 4535.19 | 4470.57 | 5 |
| CacheService | CreateNamespace | create_cache_namespace | createCacheNamespace | mutation | OK | 49.28 | 55.04 | 65.01 | 5 |
| CacheService | Delete | cache_delete | cacheNamespaceDelete | mutation | OK | 30.60 | 40.17 | 38.08 | 5 |
| CacheService | DeleteNamespace | delete_cache_namespace | deleteCacheNamespace | destructive | OK | 469.20 | 469.20 | 469.20 | 1 |
| CacheService | Get | cache_get | cacheNamespaceGet | read_only | OK | 67.63 | 105.95 | 64.40 | 25 |
| CacheService | GetNamespaceStats | get_cache_namespace_stats | getCacheNamespaceStats | read_only | OK | 376.38 | 946.92 | 434.92 | 25 |
| CacheService | Scan | cache_scan | cacheNamespaceScan | read_only | OK | 41.42 | 131.26 | 53.45 | 25 |
| CacheService | Set | cache_set | cacheNamespaceSet | mutation | OK | 31.98 | 36.38 | 67.03 | 5 |
| ConfigService | DeleteFlag | delete_flag | deleteFlag | destructive | OK | 433.30 | 433.30 | 433.30 | 1 |
| ConfigService | EvaluateFlags | evaluate_flags | evaluateFlags | read_only | OK | 35.16 | 81.32 | 39.88 | 25 |
| ConfigService | GetFlag | get_flag | getFlag | read_only | OK | 23.74 | 54.75 | 28.86 | 25 |
| ConfigService | ListFlags | list_flags | listFlags | read_only | OK | 44.85 | 127.11 | 53.10 | 25 |
| ConfigService | PutFlag | put_flag | putFlag | mutation | OK | 60.35 | 88.63 | 67.82 | 5 |
| ControlPlaneService | AckStatus | ack_status | ackStatus | mutation | OK | 24.28 | 31.10 | 39.64 | 5 |
| ControlPlaneService | DeltaResources | delta_resources | deltaResources | stream_open | OK | 0.30 | 0.30 | 0.30 | 1 |
| ControlPlaneService | GetResources | get_resources | getResources | read_only | OK | 11.74 | 21.53 | 12.49 | 25 |
| ControlPlaneService | ListNodeStates | list_node_states | listNodeStates | read_only | OK | 96.42 | 159.49 | 102.82 | 25 |
| ControlPlaneService | RollbackResources | rollback_resources | rollbackResources | mutation | OK | 502.18 | 507.26 | 448.90 | 5 |
| ControlPlaneService | StreamResources | stream_resources | streamResources | stream_open | OK | 1.52 | 1.52 | 1.52 | 1 |
| DataBroker | ActivateCatalog | activate_catalog | activateCatalog | destructive | OK | 531.28 | 531.28 | 531.28 | 1 |
| DataBroker | AnalyticalQuery | analytical_query | analyticalQuery | read_only | OK | 23.21 | 35.49 | 24.02 | 25 |
| DataBroker | ApplyMigration | apply_migration | applyMigration | mutation | OK | 1480.60 | 1480.60 | 1480.60 | 1 |
| DataBroker | ApproveMigrationPlan | approve_migration_plan | approveMigrationPlan | mutation | OK | 144.32 | 144.32 | 144.32 | 1 |
| DataBroker | BatchSelect | batch_select | batchSelect | stream_open | OK | 0.75 | 0.75 | 0.75 | 1 |
| DataBroker | BatchUpsert | batch_upsert | batchUpsert | stream_open | OK | 0.49 | 0.49 | 0.49 | 1 |
| DataBroker | BeginTx | begin_tx | beginTx | stream_open | OK | 0.72 | 0.72 | 0.72 | 1 |
| DataBroker | CacheDelete | cache_delete | cacheDelete | mutation | OK | 60.44 | 81.04 | 65.10 | 5 |
| DataBroker | CacheGet | cache_get | cacheGet | read_only | OK | 69.27 | 90.12 | 64.51 | 25 |
| DataBroker | CacheScan | cache_scan | cacheScan | read_only | OK | 83.47 | 102.48 | 82.04 | 25 |
| DataBroker | CacheSet | cache_set | cacheSet | mutation | OK | 104.63 | 126.33 | 100.27 | 5 |
| DataBroker | CreateMaterializedView | create_materialized_view | createMaterializedView | mutation | OK | 15.99 | 18.45 | 19.98 | 5 |
| DataBroker | Delete | delete | delete | mutation | OK | 811.19 | 822.04 | 812.79 | 5 |
| DataBroker | DeletePolicy | delete_policy | deletePolicy | mutation | OK | 396.10 | 396.10 | 396.10 | 1 |
| DataBroker | DismissDlqEvent | dismiss_dlq_event | dismissDlqEvent | mutation | OK | 70.91 | 77.37 | 73.43 | 5 |
| DataBroker | DocumentDelete | document_delete | documentDelete | mutation | OK | 35.95 | 36.74 | 35.59 | 5 |
| DataBroker | DocumentFind | document_find | documentFind | read_only | OK | 13.25 | 18.35 | 13.34 | 25 |
| DataBroker | DocumentGet | document_get | documentGet | read_only | OK | 28.65 | 63.96 | 36.41 | 25 |
| DataBroker | DocumentUpsert | document_upsert | documentUpsert | mutation | OK | 32.36 | 37.73 | 35.23 | 5 |
| DataBroker | DropResource | drop_resource | dropResource | destructive | OK | 107.86 | 107.86 | 107.86 | 1 |
| DataBroker | EnqueueOutboxEvent | enqueue_outbox_event | enqueueOutboxEvent | mutation | OK | 75.86 | 75.86 | 75.86 | 1 |
| DataBroker | EnsureBaseline | ensure_baseline | ensureBaseline | mutation | OK | 277.21 | 475.02 | 387.51 | 5 |
| DataBroker | EnsureProject | ensure_project | ensureProject | mutation | OK | 43.18 | 52.01 | 91.23 | 5 |
| DataBroker | EnsureResource | ensure_resource | ensureResource | mutation | OK | 91.19 | 121.51 | 95.01 | 5 |
| DataBroker | GeneratePresignedUrl | generate_presigned_url | generatePresignedUrl | mutation | OK | 11.77 | 20.97 | 30.49 | 5 |
| DataBroker | GenericDispatch | generic_dispatch | genericDispatch | mutation | OK | 25.93 | 30.76 | 64.58 | 5 |
| DataBroker | GetAdminSummary | get_admin_summary | getAdminSummary | read_only | OK | 217.95 | 486.87 | 257.01 | 25 |
| DataBroker | GetCapabilities | get_capabilities | getCapabilities | read_only | OK | 35.31 | 145.32 | 54.67 | 25 |
| DataBroker | GetCatalogManifest | get_catalog_manifest | getCatalogManifest | read_only | OK | 360.96 | 815.07 | 446.73 | 25 |
| DataBroker | GetCatalogVersion | get_catalog_version | getCatalogVersion | read_only | OK | 28.94 | 43.70 | 29.46 | 25 |
| DataBroker | GetCatalogVersions | get_catalog_versions | getCatalogVersions | read_only | OK | 15.49 | 37.17 | 18.07 | 25 |
| DataBroker | GetCdcStatus | get_cdc_status | getCdcStatus | read_only | OK | 30.77 | 41.34 | 27.72 | 25 |
| DataBroker | GetDlqEvent | get_dlq_event | getDlqEvent | read_only | OK | 42.37 | 64.02 | 42.44 | 25 |
| DataBroker | GetHealthReport | get_health_report | getHealthReport | read_only | OK | 17.91 | 35.21 | 22.31 | 25 |
| DataBroker | GetMigrationStatus | get_migration_status | getMigrationStatus | read_only | OK | 34.28 | 67.05 | 38.66 | 25 |
| DataBroker | GetObject | get_object | getObject | stream_open | OK | 0.36 | 0.36 | 0.36 | 1 |
| DataBroker | GetSaga | get_saga | getSaga | read_only | OK | 34.71 | 75.79 | 34.92 | 25 |
| DataBroker | GraphMutate | graph_mutate | graphMutate | mutation | OK | 56.25 | 61.39 | 344.79 | 5 |
| DataBroker | GraphQuery | graph_query | graphQuery | read_only | OK | 45.69 | 60.43 | 44.59 | 25 |
| DataBroker | InitiateMultipartUpload | initiate_multipart_upload | initiateMultipartUpload | mutation | OK | 71.72 | 90.50 | 104.64 | 5 |
| DataBroker | LintPolicies | lint_policies | lintPolicies | read_only | OK | 47.11 | 95.96 | 51.91 | 25 |
| DataBroker | ListAdminAuditLogs | list_admin_audit_logs | listAdminAuditLogs | read_only | OK | 42.63 | 117.67 | 52.03 | 25 |
| DataBroker | ListDlqEvents | list_dlq_events | listDlqEvents | read_only | OK | 22.85 | 52.13 | 26.68 | 25 |
| DataBroker | ListMessageSchemas | list_message_schemas | listMessageSchemas | read_only | OK | 6.91 | 13.72 | 7.82 | 25 |
| DataBroker | ListMigrationRuns | list_migration_runs | listMigrationRuns | read_only | OK | 23.10 | 47.72 | 23.87 | 25 |
| DataBroker | ListPolicies | list_policies | listPolicies | read_only | OK | 24.29 | 53.32 | 30.40 | 25 |
| DataBroker | ListProjects | list_projects | listProjects | read_only | OK | 17.35 | 73.67 | 26.39 | 25 |
| DataBroker | ListResources | list_resources | listResources | read_only | OK | 33.33 | 46.05 | 32.21 | 25 |
| DataBroker | ListSagas | list_sagas | listSagas | read_only | OK | 34.98 | 47.80 | 35.69 | 25 |
| DataBroker | LookupMessageSchema | lookup_message_schema | lookupMessageSchema | read_only | OK | 6.44 | 20.28 | 8.45 | 25 |
| DataBroker | MarkSagaReviewed | mark_saga_reviewed | markSagaReviewed | mutation | OK | 137.16 | 144.38 | 134.83 | 5 |
| DataBroker | PauseCdc | pause_cdc | pauseCdc | mutation | OK | 34.69 | 61.52 | 52.88 | 5 |
| DataBroker | PlanMigration | plan_migration | planMigration | mutation | OK | 61.33 | 70.74 | 69.58 | 5 |
| DataBroker | PreviewCdcRedaction | preview_cdc_redaction | previewCdcRedaction | read_only | OK | 71.76 | 111.98 | 67.68 | 25 |
| DataBroker | PublishCDC | publish_cdc | publishCdc | stream_open | OK | 0.81 | 0.81 | 0.81 | 1 |
| DataBroker | PutObject | put_object | putObject | mutation | OK | 60.16 | 60.16 | 63.04 | 3 |
| DataBroker | PutPolicy | put_policy | putPolicy | destructive | OK | 290.79 | 290.79 | 290.79 | 1 |
| DataBroker | QuarantineDlqEvent | quarantine_dlq_event | quarantineDlqEvent | mutation | OK | 126.52 | 168.14 | 133.11 | 5 |
| DataBroker | ReloadPolicies | reload_policies | reloadPolicies | destructive | OK | 100.59 | 100.59 | 100.59 | 1 |
| DataBroker | ReplayDlqEvent | replay_dlq_event | replayDlqEvent | mutation | OK | 141.30 | 141.30 | 141.30 | 1 |
| DataBroker | ResumeCdc | resume_cdc | resumeCdc | mutation | OK | 34.31 | 35.61 | 34.81 | 5 |
| DataBroker | RetrySagaCompensation | retry_saga_compensation | retrySagaCompensation | mutation | OK | 55.91 | 55.91 | 55.91 | 1 |
| DataBroker | RollbackCatalog | rollback_catalog | rollbackCatalog | destructive | OK | 11.81 | 11.81 | 11.81 | 1 |
| DataBroker | ScanProjectionDrift | scan_projection_drift | scanProjectionDrift | read_only | OK | 76.11 | 119.65 | 72.81 | 25 |
| DataBroker | Select | select | select | read_only | OK | 73.94 | 116.66 | 74.94 | 25 |
| DataBroker | SelectV2 | select_v_2 | selectV2 | stream_open | OK | 0.48 | 0.48 | 0.48 | 1 |
| DataBroker | StageCatalog | stage_catalog | stageCatalog | destructive | OK | 1915.09 | 1915.09 | 1915.09 | 1 |
| DataBroker | StepDownCdcLeader | step_down_cdc_leader | stepDownCdcLeader | mutation | OK | 35.94 | 36.86 | 52.63 | 5 |
| DataBroker | TimeSeriesQuery | time_series_query | timeSeriesQuery | read_only | OK | 38.25 | 52.05 | 36.58 | 25 |
| DataBroker | TimeSeriesWrite | time_series_write | timeSeriesWrite | mutation | OK | 20.47 | 24.25 | 30.99 | 5 |
| DataBroker | Upsert | upsert | upsert | mutation | OK | 842.89 | 1040.62 | 875.04 | 5 |
| DataBroker | ValidateCatalog | validate_catalog | validateCatalog | destructive | OK | 552.83 | 552.83 | 552.83 | 1 |
| DataBroker | VectorBatchUpsert | vector_batch_upsert | vectorBatchUpsert | stream_open | OK | 0.59 | 0.59 | 0.59 | 1 |
| DataBroker | VectorHybridSearch | vector_hybrid_search | vectorHybridSearch | read_only | OK | 43.98 | 55.63 | 42.73 | 25 |
| DataBroker | VectorSearch | vector_search | vectorSearch | read_only | OK | 50.09 | 66.20 | 53.03 | 25 |
| DataBroker | VectorUpsert | vector_upsert | vectorUpsert | mutation | OK | 65.50 | 79.71 | 70.03 | 5 |
| DataBroker | VerifyAdminAuditLog | verify_admin_audit_log | verifyAdminAuditLog | read_only | OK | 48.32 | 113.78 | 59.50 | 25 |
| EmbeddingService | Backfill | backfill | backfillEmbeddingSource | mutation | OK | 117.82 | 203.38 | 142.11 | 5 |
| EmbeddingService | DeleteSource | delete_source | deleteEmbeddingSource | destructive | OK | 395.86 | 395.86 | 395.86 | 1 |
| EmbeddingService | ListSources | list_sources | listEmbeddingSources | read_only | OK | 35.63 | 56.00 | 37.39 | 25 |
| EmbeddingService | RegisterSource | register_source | registerEmbeddingSource | mutation | OK | 64.01 | 65.94 | 86.50 | 5 |
| EmbeddingService | ReportEmbedding | report_embedding | reportEmbedding | mutation | OK | 153.56 | 166.67 | 141.13 | 5 |
| EmbeddingService | Retrieve | retrieve | retrieveEmbedding | read_only | OK | 42.68 | 71.51 | 44.48 | 25 |
| IdentityProviderService | CreateProvider | create_provider | createProvider | mutation | OK | 76.09 | 76.09 | 76.09 | 1 |
| IdentityProviderService | DisableProvider | disable_provider | disableProvider | mutation | OK | 184.12 | 197.71 | 291.32 | 5 |
| IdentityProviderService | ForceJwksRefresh | force_jwks_refresh | forceJwksRefresh | mutation | OK | 138.50 | 256.96 | 294.30 | 5 |
| IdentityProviderService | GetProvider | get_provider | getProvider | read_only | OK | 16.23 | 29.38 | 16.76 | 25 |
| IdentityProviderService | ImportSamlMetadata | import_saml_metadata | importSamlMetadata | mutation | OK | 183.88 | 192.67 | 181.44 | 5 |
| IdentityProviderService | LinkIdentity | link_identity | linkIdentity | mutation | OK | 112.40 | 131.37 | 124.73 | 5 |
| IdentityProviderService | ListExternalIdentities | list_external_identities | listExternalIdentities | read_only | OK | 22.08 | 81.50 | 38.94 | 25 |
| IdentityProviderService | ListProviders | list_providers | listProviders | read_only | OK | 33.59 | 80.58 | 41.35 | 25 |
| IdentityProviderService | PreviewClaimMapping | preview_claim_mapping | previewClaimMapping | read_only | OK | 22.87 | 42.11 | 22.37 | 25 |
| IdentityProviderService | PreviewGroupMapping | preview_group_mapping | previewGroupMapping | read_only | OK | 10.38 | 20.05 | 12.17 | 25 |
| IdentityProviderService | ResolveExternalIdentity | resolve_external_identity | resolveExternalIdentity | mutation | OK | 46.54 | 49.58 | 99.53 | 5 |
| IdentityProviderService | SamlAcs | saml_acs | samlAcs | mutation | OK | 206.00 | 239.53 | 250.62 | 5 |
| IdentityProviderService | ScimCreateGroup | scim_create_group | scimCreateGroup | mutation | OK | 16.55 | 16.90 | 16.71 | 5 |
| IdentityProviderService | ScimCreateUser | scim_create_user | scimCreateUser | mutation | OK | 159.63 | 164.17 | 141.79 | 5 |
| IdentityProviderService | ScimDeleteGroup | scim_delete_group | scimDeleteGroup | mutation | OK | 20.58 | 32.62 | 120.62 | 5 |
| IdentityProviderService | ScimDeleteUser | scim_delete_user | scimDeleteUser | mutation | OK | 477.08 | 477.08 | 477.08 | 1 |
| IdentityProviderService | ScimGetGroup | scim_get_group | scimGetGroup | mutation | OK | 194.78 | 242.76 | 200.67 | 5 |
| IdentityProviderService | ScimGetUser | scim_get_user | scimGetUser | mutation | OK | 84.41 | 90.08 | 77.31 | 5 |
| IdentityProviderService | ScimListGroups | scim_list_groups | scimListGroups | mutation | OK | 21.84 | 46.65 | 191.51 | 5 |
| IdentityProviderService | ScimListUsers | scim_list_users | scimListUsers | mutation | OK | 72.51 | 120.23 | 238.75 | 5 |
| IdentityProviderService | ScimPatchGroup | scim_patch_group | scimPatchGroup | mutation | OK | 18.48 | 29.34 | 117.45 | 5 |
| IdentityProviderService | ScimPatchUser | scim_patch_user | scimPatchUser | mutation | OK | 93.18 | 140.05 | 228.55 | 5 |
| IdentityProviderService | ScimReplaceUser | scim_replace_user | scimReplaceUser | mutation | OK | 227.59 | 272.07 | 276.77 | 5 |
| IdentityProviderService | StartSamlLogin | start_saml_login | startSamlLogin | mutation | OK | 23.58 | 30.62 | 51.36 | 5 |
| IdentityProviderService | TestProviderDiscovery | test_provider_discovery | testProviderDiscovery | read_only | OK | 11.47 | 17.61 | 12.58 | 25 |
| IdentityProviderService | UnlinkIdentity | unlink_identity | unlinkIdentity | mutation | OK | 49.89 | 106.20 | 107.72 | 5 |
| IdentityProviderService | UpdateProvider | update_provider | updateProvider | mutation | OK | 118.85 | 173.30 | 235.18 | 5 |
| LiveQueryService | Subscribe | subscribe | liveQuerySubscribe | stream_open | OK | 0.87 | 0.87 | 0.87 | 1 |
| LockService | AcquireLock | acquire_lock | acquireLock | mutation | OK | 353.50 | 354.65 | 289.16 | 5 |
| LockService | ReleaseLock | release_lock | releaseLock | mutation | OK | 78.16 | 114.29 | 126.14 | 5 |
| LockService | RenewLock | renew_lock | renewLock | mutation | OK | 285.24 | 1183.42 | 673.50 | 5 |
| MeteringService | CheckQuota | check_quota | checkQuota | read_only | OK | 93.78 | 157.72 | 83.53 | 25 |
| MeteringService | GetQuota | get_quota | getQuota | read_only | OK | 25.78 | 60.22 | 31.36 | 25 |
| MeteringService | ListQuotas | list_quotas | listQuotas | read_only | OK | 42.03 | 85.38 | 42.01 | 25 |
| MeteringService | PutQuota | put_quota | putQuota | mutation | OK | 119.70 | 119.83 | 105.88 | 5 |
| MeteringService | QueryUsage | query_usage | queryUsage | read_only | OK | 57.84 | 89.17 | 61.08 | 25 |
| MeteringService | RecordUsage | record_usage | recordUsage | mutation | OK | 67.26 | 75.49 | 107.87 | 5 |
| NotificationService | GetDeliveryStats | get_delivery_stats | getDeliveryStats | read_only | OK | 44.24 | 59.45 | 40.32 | 25 |
| NotificationService | GetNotification | get_notification | getNotification | read_only | OK | 46.48 | 117.17 | 56.13 | 25 |
| NotificationService | GetPreference | get_preference | getPreference | read_only | OK | 49.51 | 74.58 | 44.61 | 25 |
| NotificationService | GetTemplate | get_template | getTemplate | read_only | OK | 43.20 | 75.26 | 42.70 | 25 |
| NotificationService | ListNotifications | list_notifications | listNotifications | read_only | OK | 80.16 | 140.96 | 76.33 | 25 |
| NotificationService | ListPreferences | list_preferences | listPreferences | read_only | OK | 80.39 | 133.97 | 80.42 | 25 |
| NotificationService | ListTemplates | list_templates | listTemplates | read_only | OK | 72.06 | 153.70 | 72.11 | 25 |
| NotificationService | ReportDelivery | report_delivery | reportDelivery | mutation | OK | 56.29 | 75.70 | 115.15 | 5 |
| NotificationService | RetryNotification | retry_notification | retryNotification | mutation | OK | 274.59 | 274.59 | 274.59 | 1 |
| NotificationService | SendNotification | send_notification | sendNotification | mutation | OK | 275.81 | 369.66 | 317.96 | 5 |
| NotificationService | SetPreference | set_preference | setPreference | mutation | OK | 88.32 | 101.49 | 87.83 | 5 |
| NotificationService | UpsertTemplate | upsert_template | upsertTemplate | mutation | OK | 29.87 | 31.87 | 28.25 | 5 |
| PeerService | GetPeer | get_peer | getPeer | read_only | OK | 77.07 | 121.28 | 78.76 | 25 |
| PeerService | JoinRoom | join_room | joinRoom | mutation | OK | 137.27 | 158.94 | 140.29 | 5 |
| PeerService | JoinSession | join_session | joinSession | mutation | OK | 181.13 | 181.59 | 214.05 | 5 |
| PeerService | LeaveRoom | leave_room | leaveRoom | mutation | OK | 27.71 | 34.88 | 65.65 | 5 |
| PeerService | ListPeers | list_peers | listPeers | read_only | OK | 61.19 | 102.54 | 63.30 | 25 |
| RoomService | CloseRoom | close_room | closeRoom | mutation | OK | 169.16 | 187.39 | 187.87 | 5 |
| RoomService | CreateRoom | create_room | createRoom | mutation | OK | 32.40 | 48.95 | 38.48 | 5 |
| RoomService | GetRoom | get_room | getRoom | read_only | OK | 36.13 | 66.24 | 41.98 | 25 |
| RoomService | ListEgress | list_egress | listEgress | read_only | CAPABILITY_SKIPPED | 12.62 | 23.47 | 14.90 | 25 |
| RoomService | ListRooms | list_rooms | listRooms | read_only | OK | 41.58 | 67.89 | 47.39 | 25 |
| RoomService | StartRoomComposite | start_room_composite | startRoomComposite | mutation | CAPABILITY_SKIPPED | 14.59 | 15.14 | 14.15 | 5 |
| RoomService | StartTrackEgress | start_track_egress | startTrackEgress | mutation | CAPABILITY_SKIPPED | 9.29 | 10.48 | 9.65 | 5 |
| RoomService | StopEgress | stop_egress | stopEgress | mutation | CAPABILITY_SKIPPED | 13.60 | 14.91 | 15.02 | 5 |
| RoomService | UpdateRoom | update_room | updateRoom | mutation | OK | 56.30 | 61.50 | 77.20 | 5 |
| SchedulerService | CreateJob | create_job | createJob | mutation | OK | 52.59 | 63.96 | 61.08 | 5 |
| SchedulerService | DeleteJob | delete_job | deleteJob | destructive | OK | 289.54 | 289.54 | 289.54 | 1 |
| SchedulerService | GetJob | get_job | getJob | read_only | OK | 31.62 | 73.90 | 34.23 | 25 |
| SchedulerService | ListJobs | list_jobs | listJobs | read_only | OK | 29.90 | 92.10 | 39.98 | 25 |
| SchedulerService | PauseJob | pause_job | pauseJob | mutation | OK | 102.28 | 102.28 | 102.28 | 1 |
| SchedulerService | ResumeJob | resume_job | resumeJob | mutation | OK | 211.27 | 211.27 | 211.27 | 1 |
| SearchService | CreateIndex | create_index | createSearchIndex | mutation | OK | 82.65 | 83.74 | 76.57 | 5 |
| SearchService | DeleteIndex | delete_index | deleteSearchIndex | destructive | OK | 113.47 | 113.47 | 113.47 | 1 |
| SearchService | ListIndexes | list_indexes | listSearchIndexes | read_only | OK | 25.48 | 83.30 | 38.42 | 25 |
| SearchService | Reindex | reindex | reindexSearchIndex | mutation | OK | 89.69 | 143.34 | 131.10 | 5 |
| SearchService | Search | search | search | read_only | OK | 57.74 | 95.98 | 51.05 | 25 |
| SignalingService | Signal | signal | signal | stream_open | OK | 0.34 | 0.34 | 0.34 | 1 |
| StorageService | DeleteFile | delete_file | deleteFile | mutation | OK | 204.26 | 204.26 | 204.26 | 1 |
| StorageService | DownloadFile | download_file | downloadFile | stream_open | OK | 0.26 | 0.26 | 0.26 | 1 |
| StorageService | FinalizeUpload | finalize_upload | finalizeUpload | mutation | OK | 290.12 | 290.12 | 290.12 | 1 |
| StorageService | GetDownloadUrl | get_download_url | getDownloadUrl | read_only | OK | 60.73 | 84.00 | 52.56 | 25 |
| StorageService | GetFile | get_file | getFile | read_only | OK | 57.13 | 68.94 | 50.14 | 25 |
| StorageService | ListFiles | list_files | listFiles | read_only | OK | 57.12 | 94.40 | 55.19 | 25 |
| StorageService | RegisterUpload | register_upload | registerUpload | mutation | OK | 89.32 | 122.07 | 87.66 | 5 |
| StorageService | UpdateFile | update_file | updateFile | mutation | OK | 58.58 | 103.39 | 93.77 | 5 |
| TenantService | CreateTenant | create_tenant | createTenant | mutation | OK | 35.56 | 71.49 | 97.63 | 5 |
| TenantService | GetTenant | get_tenant | getTenant | read_only | OK | 48.39 | 67.69 | 47.23 | 25 |
| TenantService | GetTenantConfig | get_tenant_config | getTenantConfig | read_only | OK | 50.27 | 93.82 | 50.76 | 25 |
| TenantService | ListTenants | list_tenants | listTenants | read_only | OK | 11.68 | 24.05 | 12.46 | 25 |
| TenantService | PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 1838.14 | 1838.14 | 1838.14 | 1 |
| TenantService | UpdateTenant | update_tenant | updateTenant | mutation | OK | 44.13 | 49.48 | 51.95 | 5 |
| TenantService | UpdateTenantConfig | update_tenant_config | updateTenantConfig | mutation | OK | 94.92 | 97.52 | 91.55 | 5 |
| TrackService | ListTracks | list_tracks | listTracks | read_only | OK | 54.92 | 103.25 | 59.70 | 25 |
| TrackService | MuteTrack | mute_track | muteTrack | mutation | OK | 86.84 | 153.87 | 101.11 | 5 |
| TrackService | PublishTrack | publish_track | publishTrack | mutation | OK | 48.85 | 53.97 | 49.40 | 5 |
| TrackService | UnpublishTrack | unpublish_track | unpublishTrack | mutation | OK | 39.64 | 53.25 | 50.62 | 5 |
| TurnService | IssueCredentials | issue_credentials | issueCredentials | mutation | OK | 65.37 | 105.39 | 76.31 | 5 |
| VaultService | CreateTransitKey | create_transit_key | createTransitKey | mutation | OK | 219.67 | 219.67 | 219.67 | 1 |
| VaultService | Decrypt | decrypt | vaultDecrypt | read_only | OK | 30.02 | 88.09 | 38.88 | 25 |
| VaultService | DeleteSecret | delete_secret | deleteSecret | mutation | OK | 64.52 | 112.25 | 135.67 | 5 |
| VaultService | DestroySecret | destroy_secret | destroySecret | destructive | OK | 75.56 | 75.56 | 75.56 | 1 |
| VaultService | Encrypt | encrypt | vaultEncrypt | mutation | OK | 39.82 | 74.66 | 59.17 | 5 |
| VaultService | GenerateDatabaseCredentials | generate_database_credentials | generateDatabaseCredentials | mutation | OK | 98.33 | 125.90 | 119.78 | 5 |
| VaultService | GetSecret | get_secret | getSecret | read_only | OK | 68.71 | 102.52 | 61.91 | 25 |
| VaultService | Hmac | hmac | vaultHmac | mutation | OK | 90.12 | 131.19 | 96.14 | 5 |
| VaultService | ListSecrets | list_secrets | listSecrets | read_only | OK | 74.47 | 99.35 | 66.86 | 25 |
| VaultService | PutSecret | put_secret | putSecret | mutation | OK | 121.76 | 121.76 | 121.76 | 1 |
| VaultService | RotateTransitKey | rotate_transit_key | rotateTransitKey | mutation | OK | 341.96 | 508.98 | 352.52 | 5 |
| VaultService | SealStatus | seal_status | vaultSealStatus | read_only | OK | 4.94 | 7.08 | 5.25 | 25 |
| VaultService | Sign | sign | vaultSign | mutation | OK | 52.95 | 61.03 | 60.98 | 5 |
| VaultService | Verify | verify | vaultVerify | read_only | OK | 73.75 | 99.20 | 72.95 | 25 |
| WebhookService | CreateEndpoint | create_endpoint | createWebhookEndpoint | mutation | OK | 30.32 | 42.67 | 35.53 | 5 |
| WebhookService | DeleteEndpoint | delete_endpoint | deleteWebhookEndpoint | destructive | OK | 87.43 | 87.43 | 87.43 | 1 |
| WebhookService | GetEndpoint | get_endpoint | getWebhookEndpoint | read_only | OK | 17.55 | 33.54 | 19.60 | 25 |
| WebhookService | ListDeliveries | list_deliveries | listWebhookDeliveries | read_only | OK | 60.88 | 138.16 | 69.93 | 25 |
| WebhookService | ListEndpoints | list_endpoints | listWebhookEndpoints | read_only | OK | 55.00 | 71.66 | 48.57 | 25 |
| WebhookService | UpdateEndpoint | update_endpoint | updateWebhookEndpoint | mutation | OK | 126.85 | 143.52 | 134.03 | 5 |
| WorkflowService | CancelWorkflow | cancel_workflow | cancelWorkflow | destructive | OK | 168.50 | 168.50 | 168.50 | 1 |
| WorkflowService | GetWorkflow | get_workflow | getWorkflow | read_only | OK | 100.07 | 200.09 | 108.72 | 25 |
| WorkflowService | ListWorkflows | list_workflows | listWorkflows | read_only | OK | 121.60 | 161.37 | 113.05 | 25 |
| WorkflowService | SignalWorkflow | signal_workflow | signalWorkflow | mutation | OK | 33.95 | 154.14 | 85.03 | 5 |
| WorkflowService | StartWorkflow | start_workflow | startWorkflow | mutation | OK | 75.68 | 80.90 | 78.75 | 5 |
