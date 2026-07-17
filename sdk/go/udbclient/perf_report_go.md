# UDB SDK Live Perf — Go (localhost)

RPCs measured: 353   tenant=f471e3c0-8c5b-438b-9be7-f2878157b30b

Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities (a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request's reference/ID fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection latency. The TARGET is zero failures; any residual non-OK RPC is listed under Failures for the maintainer to finish.

Unary RPCs = full request→response round-trip. Non-CDC streaming RPCs report time-to-FIRST-RESPONSE with seeded inputs. CDC subscription (PublishCDC) reports time-to-FIRST-EVENT: the harness subscribes, fires a real Upsert that flows outbox→CDC→Kafka, and times the first delivered event. Streaming rows are marked in the note column.

## Seeded fixtures

Captured semantic field → seeded value keys used to resolve request fields: action, admin_reset_mfa_user_id, admin_reset_password_user_id, apply_run_id, approval_token, approve_draft_id, approve_run_id, approved_by, asset_id, assigned_by, auth_challenge_id, auth_token, backup_id, bucket, canary_id, canary_version_id, cancel_workflow_id, catalog_manifest, catalog_manifest_b64, challenge_id, change_password_user_id, change_status_user_id, close_room_id, code, collection, content_type, created_by, csrf_token, definition_id, delete_endpoint_id, delete_file_id, delete_policy_id, delete_role_id, delete_scim_user_id, deleted_by, device_id, disable_mfa_user_id, disable_provider_id, dismiss_dlq_id, dlq_id, document_id, domain, ds_policy_id, egress_id, endpoint_id, event_type, external_identity_id, file_id, file_type, filename, finalize_file_id, gov_exp, instance_id, job_id, join_session_room_id, key_id, kind, leave_peer_id, locale, log_id, mark_saga_id, message_type, migration_id, mongo_collection, name, node_id, notification_id, object, object_key, otp_code, otp_id, owner_id, peer_id, plain_key, policy_draft_id, policy_id, policy_version_id, project, project_id, provider_id, purge_tenant_id, quarantine_dlq_id, recipient_id, record_id, recovery_code, refresh_session_id, refresh_token, reg_challenge_id, reissue_file_id, reject_draft_id, rejected_by, relation, release_fencing_token, renew_fencing_token, replay_dlq_id, reset_otp_code, reset_otp_id, resource, resource_name, restore_tenant_id, retry_saga_id, revoke_device_id, revoke_device_user_id, revoke_key_id, revoke_recovery_user_id, revoked_by, role, role_code, role_id, rollback_policy_set_id, rollback_resource_version, rollback_target_version_id, room_id, saga_id, saml_provider_id, scim_group_id, scim_user_id, session_id, signal_peer_id, stage_name, step_id, subject, tenant, tenant_code, tenant_id, token, topic_pattern, track_id, ts_table, unpublish_track_id, update_draft_id, update_key_id, updated_by, user_id, user_role_id, username, vault_ciphertext, vault_create_key_name, vault_db_role, vault_delete_secret_path, vault_destroy_secret_path, vault_key_name, vault_put_secret_path, vault_secret_path, vault_signature, vault_signing_key_name, workflow_id

## Per-service mean latency (mean of per-RPC means)

| Service | RPCs | mean |
|---|---:|---:|
| AuthnService | 50 | 87.254ms |
| BackupService | 8 | 381.832ms |
| DataBroker | 77 | 33.778ms |
| AuthzService | 41 | 29.471ms |
| IdentityProviderService | 27 | 18.289ms |
| VaultService | 20 | 19.614ms |
| ControlPlaneService | 6 | 63.813ms |
| TenantService | 7 | 51.684ms |
| NotificationService | 12 | 18.898ms |
| AnalyticsService | 7 | 29.393ms |
| StorageService | 9 | 22.837ms |
| PeerService | 5 | 34.776ms |
| ApiKeyService | 9 | 18.79ms |
| AssetService | 8 | 20.37ms |
| RoomService | 9 | 17.669ms |
| EmbeddingService | 6 | 23.012ms |
| LockService | 5 | 25.455ms |
| CacheService | 7 | 18.049ms |
| WorkflowService | 5 | 21.908ms |
| SearchService | 5 | 19.908ms |
| SchedulerService | 6 | 15.964ms |
| ConfigService | 5 | 17.921ms |
| WebhookService | 6 | 14.835ms |
| MeteringService | 6 | 14.548ms |
| TrackService | 4 | 21.227ms |
| SignalingService | 1 | 16.296ms |
| LiveQueryService | 1 | 15.321ms |
| TurnService | 1 | 11.583ms |

## Failures — still to fix (0)

No RPC returned a non-OK gRPC status — every RPC ran its success path.

## Slowest 25 RPCs by p99

| RPC | api_alias | operation_id | kind | err | p50 | p99 | mean | iters | note |
|---|---|---|---|---|---:|---:|---:|---:|---|
| BackupService/StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 1.628222s | 1.685154s | 1.615326s | 5 | mutation (seeded success path) |
| BackupService/RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 1.318203s | 1.318203s | 1.318203s | 1 | destructive: 1 real call against a seeded disposable target |
| AuthnService/ChangePassword | change_password | changePassword | mutation | OK | 1.299088s | 1.299088s | 1.299088s | 5 | mutation (seeded success path) |
| DataBroker/StageCatalog | stage_catalog | stageCatalog | destructive | OK | 811.079ms | 811.079ms | 811.079ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthnService/CreateUser | create_user | createUser | mutation | OK | 786.801ms | 786.801ms | 786.801ms | 5 | mutation (seeded success path) |
| AuthnService/ResetPassword | reset_password | resetPassword | mutation | OK | 780.655ms | 780.655ms | 780.655ms | 5 | mutation (seeded success path) |
| AuthnService/Login | login | login | mutation | OK | 748.084ms | 757.25ms | 702.423ms | 5 | mutation (seeded success path) |
| DataBroker/ApplyMigration | apply_migration | applyMigration | mutation | OK | 264.077ms | 264.077ms | 264.077ms | 5 | mutation (seeded success path) |
| DataBroker/PublishCDC | publish_cdc | publishCdc | mutation | OK | 259.262ms | 259.262ms | 263.517ms | 3 | cdc subscription: time-to-first-event (real mutation produced) |
| TenantService/PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 255.008ms | 255.008ms | 255.008ms | 1 | destructive: 1 real call against a seeded disposable target |
| AnalyticsService/GetReconciliationAnalytics | get_reconciliation_analytics | getReconciliationAnalytics | read_only | OK | 120.778ms | 167.557ms | 107.194ms | 25 | read_only (seeded success path) |
| ControlPlaneService/StreamResources | stream_resources | streamResources | mutation | OK | 107.885ms | 139.319ms | 117.769ms | 5 | streaming: time-to-first-response (seeded; bidi) |
| PeerService/GetPeer | get_peer | getPeer | read_only | OK | 40.509ms | 138.958ms | 64.61ms | 25 | read_only (seeded success path) |
| ControlPlaneService/DeltaResources | delta_resources | deltaResources | mutation | OK | 106.648ms | 125.462ms | 114.4ms | 5 | streaming: time-to-first-response (seeded; bidi) |
| IdentityProviderService/SamlAcs | saml_acs | samlAcs | mutation | OK | 101.328ms | 107.251ms | 100.969ms | 5 | mutation (seeded success path) |
| DataBroker/GetAdminSummary | get_admin_summary | getAdminSummary | read_only | OK | 49.246ms | 101.119ms | 56.296ms | 25 | read_only (seeded success path) |
| AuthzService/PromoteCanary | promote_canary | promoteCanary | destructive | OK | 96.293ms | 96.293ms | 96.293ms | 1 | destructive: 1 real call against a seeded disposable target |
| DataBroker/ValidateCatalog | validate_catalog | validateCatalog | destructive | OK | 94.061ms | 94.061ms | 94.061ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthzService/RollbackPolicyVersion | rollback_policy_version | rollbackPolicyVersion | destructive | OK | 88.081ms | 88.081ms | 88.081ms | 1 | destructive: 1 real call against a seeded disposable target |
| TrackService/ListTracks | list_tracks | listTracks | read_only | OK | 35.103ms | 84.456ms | 43.06ms | 25 | read_only (seeded success path) |
| AuthzService/ActivatePolicyVersion | activate_policy_version | activatePolicyVersion | destructive | OK | 83.801ms | 83.801ms | 83.801ms | 1 | destructive: 1 real call against a seeded disposable target |
| ControlPlaneService/ListNodeStates | list_node_states | listNodeStates | read_only | OK | 50.737ms | 82.739ms | 57.342ms | 25 | read_only (seeded success path) |
| ControlPlaneService/RollbackResources | rollback_resources | rollbackResources | mutation | OK | 72.16ms | 75.795ms | 78.311ms | 5 | mutation (seeded success path) |
| RoomService/GetRoom | get_room | getRoom | read_only | OK | 32.265ms | 73.98ms | 39.46ms | 25 | read_only (seeded success path) |
| PeerService/ListPeers | list_peers | listPeers | read_only | OK | 29.858ms | 70.074ms | 34.98ms | 25 | read_only (seeded success path) |

## Full per-RPC table (sorted by service, then name)

| Service | RPC | api_alias | operation_id | kind | err | p50 | p99 | mean | min | max | iters |
|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|
| AnalyticsService | GetExecutorPerformance | get_executor_performance | getExecutorPerformance | read_only | OK | 13.474ms | 20.473ms | 13.856ms | 9.018ms | 28.262ms | 25 |
| AnalyticsService | GetPipelineSummary | get_pipeline_summary | getPipelineSummary | read_only | OK | 12.912ms | 55.191ms | 17.836ms | 7.224ms | 57.159ms | 25 |
| AnalyticsService | GetReconciliationAnalytics | get_reconciliation_analytics | getReconciliationAnalytics | read_only | OK | 120.778ms | 167.557ms | 107.194ms | 10.771ms | 167.708ms | 25 |
| AnalyticsService | GetSlaCompliance | get_sla_compliance | getSlaCompliance | read_only | OK | 18.432ms | 50.051ms | 24.426ms | 7.61ms | 52.864ms | 25 |
| AnalyticsService | GetThroughput | get_throughput | getThroughput | read_only | OK | 8.784ms | 12.246ms | 9.234ms | 6.533ms | 20.346ms | 25 |
| AnalyticsService | RecordPipelineMetric | record_pipeline_metric | recordPipelineMetric | mutation | OK | 14.991ms | 15.131ms | 15.586ms | 13.666ms | 19.793ms | 5 |
| AnalyticsService | TriggerSnapshot | trigger_snapshot | triggerSnapshot | mutation | OK | 17.685ms | 18.563ms | 17.617ms | 14.497ms | 20.535ms | 5 |
| ApiKeyService | CreateApiKey | create_api_key | createApiKey | mutation | OK | 15.104ms | 18.761ms | 17.219ms | 11.641ms | 28.734ms | 5 |
| ApiKeyService | EmergencyRevokeApiKeys | emergency_revoke_api_keys | emergencyRevokeApiKeys | destructive | OK | 49.331ms | 49.331ms | 49.331ms | 49.331ms | 49.331ms | 1 |
| ApiKeyService | GetApiKey | get_api_key | getApiKey | read_only | OK | 7.018ms | 14.575ms | 7.481ms | 3.817ms | 15.63ms | 25 |
| ApiKeyService | GetApiKeyUsageStats | get_api_key_usage_stats | getApiKeyUsageStats | read_only | OK | 8.491ms | 12.995ms | 8.821ms | 4.355ms | 13.877ms | 25 |
| ApiKeyService | ListApiKeys | list_api_keys | listApiKeys | read_only | OK | 9.211ms | 12.862ms | 9.041ms | 5.819ms | 15.283ms | 25 |
| ApiKeyService | RevokeApiKey | revoke_api_key | revokeApiKey | mutation | OK | 18.682ms | 18.682ms | 18.682ms | 18.682ms | 18.682ms | 5 |
| ApiKeyService | RotateApiKey | rotate_api_key | rotateApiKey | mutation | OK | 24.268ms | 24.268ms | 24.268ms | 24.268ms | 24.268ms | 5 |
| ApiKeyService | UpdateApiKey | update_api_key | updateApiKey | mutation | OK | 16.461ms | 17.202ms | 19.68ms | 15.37ms | 33.391ms | 5 |
| ApiKeyService | ValidateApiKey | validate_api_key | validateApiKey | read_only | OK | 14.242ms | 19.19ms | 14.59ms | 10.512ms | 22.511ms | 25 |
| AssetService | CompleteStep | complete_step | completeStep | mutation | OK | 28.567ms | 34.214ms | 31.584ms | 26.819ms | 41.41ms | 5 |
| AssetService | CreatePipelineDefinition | create_pipeline_definition | createPipelineDefinition | mutation | OK | 13.217ms | 13.217ms | 13.217ms | 13.217ms | 13.217ms | 5 |
| AssetService | GetAsset | get_asset | getAsset | read_only | OK | 21.042ms | 32.189ms | 22.441ms | 16.326ms | 39.574ms | 25 |
| AssetService | GetPipeline | get_pipeline | getPipeline | read_only | OK | 21.341ms | 25.781ms | 21.069ms | 15.665ms | 27.64ms | 25 |
| AssetService | GetPipelineDefinition | get_pipeline_definition | getPipelineDefinition | read_only | OK | 13.63ms | 23.669ms | 15.604ms | 10.245ms | 27.74ms | 25 |
| AssetService | ListAssets | list_assets | listAssets | read_only | OK | 15.826ms | 19.993ms | 15.882ms | 11.9ms | 22.003ms | 25 |
| AssetService | RegisterAsset | register_asset | registerAsset | mutation | OK | 20.021ms | 24.59ms | 20.306ms | 15.589ms | 25.054ms | 5 |
| AssetService | StartPipeline | start_pipeline | startPipeline | mutation | OK | 10.175ms | 12.646ms | 22.859ms | 8.358ms | 73.617ms | 5 |
| AuthnService | AdminResetMfa | admin_reset_mfa | adminResetMfa | destructive | OK | 36.383ms | 36.383ms | 36.383ms | 36.383ms | 36.383ms | 1 |
| AuthnService | AdminResetPassword | admin_reset_password | adminResetPassword | destructive | OK | 9.964ms | 9.964ms | 9.964ms | 9.964ms | 9.964ms | 1 |
| AuthnService | AdminRevokeAllTenantSessions | admin_revoke_all_tenant_sessions | adminRevokeAllTenantSessions | destructive | OK | 20.406ms | 20.406ms | 20.406ms | 20.406ms | 20.406ms | 1 |
| AuthnService | AdminRevokeAllUserSessions | admin_revoke_all_user_sessions | adminRevokeAllUserSessions | destructive | OK | 14.34ms | 14.34ms | 14.34ms | 14.34ms | 14.34ms | 1 |
| AuthnService | AdminRevokeSession | admin_revoke_session | adminRevokeSession | destructive | OK | 12.532ms | 12.532ms | 12.532ms | 12.532ms | 12.532ms | 1 |
| AuthnService | Authenticate | authenticate | authenticate | read_only | OK | 26.232ms | 33.193ms | 26.713ms | 19.919ms | 33.847ms | 25 |
| AuthnService | ChangePassword | change_password | changePassword | mutation | OK | 1.299088s | 1.299088s | 1.299088s | 1.299088s | 1.299088s | 5 |
| AuthnService | ChangeUserStatus | change_user_status | changeUserStatus | destructive | OK | 23.962ms | 23.962ms | 23.962ms | 23.962ms | 23.962ms | 1 |
| AuthnService | ConfirmMFAEnrollment | confirm_mfaenrollment | confirmMfaenrollment | mutation | OK | 4.303ms | 4.321ms | 4.234ms | 3.33ms | 5.158ms | 5 |
| AuthnService | CreateSession | create_session | createSession | mutation | OK | 7.368ms | 7.475ms | 6.984ms | 4.713ms | 8.554ms | 5 |
| AuthnService | CreateUser | create_user | createUser | mutation | OK | 786.801ms | 786.801ms | 786.801ms | 786.801ms | 786.801ms | 5 |
| AuthnService | DeleteWebAuthnCredential | delete_web_authn_credential | deleteWebAuthnCredential | mutation | OK | 8.93ms | 9.463ms | 8.907ms | 7.325ms | 10.432ms | 5 |
| AuthnService | DisableMfaFactor | disable_mfa_factor | disableMfaFactor | mutation | OK | 16.181ms | 16.737ms | 16.053ms | 12.635ms | 21.608ms | 5 |
| AuthnService | EmergencyRevoke | emergency_revoke | emergencyRevoke | destructive | OK | 14.933ms | 14.933ms | 14.933ms | 14.933ms | 14.933ms | 1 |
| AuthnService | EnrollMFA | enroll_mfa | enrollMfa | mutation | OK | 16.902ms | 17.262ms | 17.153ms | 15.585ms | 19.65ms | 5 |
| AuthnService | FinishWebAuthnAuthentication | finish_web_authn_authentication | finishWebAuthnAuthentication | mutation | OK | 56.092ms | 56.092ms | 56.092ms | 56.092ms | 56.092ms | 5 |
| AuthnService | FinishWebAuthnRegistration | finish_web_authn_registration | finishWebAuthnRegistration | mutation | OK | 48.894ms | 48.894ms | 48.894ms | 48.894ms | 48.894ms | 5 |
| AuthnService | ForgotPassword | forgot_password | forgotPassword | mutation | OK | 21.555ms | 21.966ms | 20.051ms | 16.74ms | 22.159ms | 5 |
| AuthnService | GenerateRecoveryCodes | generate_recovery_codes | generateRecoveryCodes | mutation | OK | 34.377ms | 34.585ms | 34.142ms | 31.863ms | 36.567ms | 5 |
| AuthnService | GetJwks | get_jwks | getJwks | read_only | OK | 6.477ms | 9.607ms | 6.808ms | 4.763ms | 9.804ms | 25 |
| AuthnService | GetMfaPolicy | get_mfa_policy | getMfaPolicy | read_only | OK | 5.873ms | 6.994ms | 5.717ms | 2.735ms | 7.662ms | 25 |
| AuthnService | GetSession | get_session | getSession | read_only | OK | 6.108ms | 7.948ms | 6.249ms | 4.109ms | 10.486ms | 25 |
| AuthnService | GetUser | get_user | getUser | read_only | OK | 4.36ms | 6.112ms | 4.441ms | 2.913ms | 7.338ms | 25 |
| AuthnService | IntrospectToken | introspect_token | introspectToken | read_only | OK | 42.77ms | 59.728ms | 43.372ms | 29.997ms | 71.294ms | 25 |
| AuthnService | IssueMfaChallenge | issue_mfa_challenge | issueMfaChallenge | mutation | OK | 14.453ms | 16.075ms | 14.433ms | 12.311ms | 16.08ms | 5 |
| AuthnService | ListDevices | list_devices | listDevices | read_only | OK | 5.793ms | 7.772ms | 5.871ms | 3.793ms | 10.057ms | 25 |
| AuthnService | ListMfaFactors | list_mfa_factors | listMfaFactors | read_only | OK | 7.336ms | 12.272ms | 7.578ms | 5.088ms | 12.362ms | 25 |
| AuthnService | ListSessions | list_sessions | listSessions | read_only | OK | 8.401ms | 12.775ms | 8.871ms | 5.53ms | 14.428ms | 25 |
| AuthnService | ListUsers | list_users | listUsers | read_only | OK | 9.985ms | 13.366ms | 10.582ms | 7.539ms | 14.554ms | 25 |
| AuthnService | ListWebAuthnCredentials | list_web_authn_credentials | listWebAuthnCredentials | read_only | OK | 5.695ms | 8.365ms | 5.897ms | 3.912ms | 12.757ms | 25 |
| AuthnService | Login | login | login | mutation | OK | 748.084ms | 757.25ms | 702.423ms | 607.103ms | 760.166ms | 5 |
| AuthnService | Logout | logout | logout | mutation | OK | 7.532ms | 8.169ms | 7.616ms | 5.842ms | 9.862ms | 5 |
| AuthnService | PutMfaPolicy | put_mfa_policy | putMfaPolicy | mutation | OK | 7.052ms | 7.753ms | 6.787ms | 5.549ms | 8.017ms | 5 |
| AuthnService | RefreshSession | refresh_session | refreshSession | mutation | OK | 18.098ms | 18.173ms | 18.078ms | 17.679ms | 18.366ms | 5 |
| AuthnService | RefreshToken | refresh_token | refreshToken | mutation | OK | 13.346ms | 13.346ms | 13.346ms | 13.346ms | 13.346ms | 5 |
| AuthnService | RenamePasskey | rename_passkey | renamePasskey | mutation | OK | 9.504ms | 9.978ms | 10.264ms | 7.72ms | 15.211ms | 5 |
| AuthnService | ResendOTP | resend_otp | resendOtp | mutation | OK | 32.478ms | 33.649ms | 30.748ms | 22.933ms | 34.401ms | 5 |
| AuthnService | ResetPassword | reset_password | resetPassword | mutation | OK | 780.655ms | 780.655ms | 780.655ms | 780.655ms | 780.655ms | 5 |
| AuthnService | RevokeDevice | revoke_device | revokeDevice | mutation | OK | 20.058ms | 20.058ms | 20.058ms | 20.058ms | 20.058ms | 5 |
| AuthnService | RevokeRecoveryCodes | revoke_recovery_codes | revokeRecoveryCodes | mutation | OK | 19.356ms | 21.25ms | 19.468ms | 14.787ms | 26.287ms | 5 |
| AuthnService | RevokeSession | revoke_session | revokeSession | mutation | OK | 9.569ms | 10.22ms | 9.914ms | 7.436ms | 14.401ms | 5 |
| AuthnService | SendOTP | send_otp | sendOtp | mutation | OK | 20.753ms | 21.144ms | 21.513ms | 17.201ms | 28.591ms | 5 |
| AuthnService | SendPhoneVerification | send_phone_verification | sendPhoneVerification | mutation | OK | 16.974ms | 18.655ms | 17.011ms | 13.639ms | 21.368ms | 5 |
| AuthnService | StartWebAuthnAuthentication | start_web_authn_authentication | startWebAuthnAuthentication | mutation | OK | 22.863ms | 35.258ms | 28.801ms | 17.311ms | 49.435ms | 5 |
| AuthnService | StartWebAuthnRegistration | start_web_authn_registration | startWebAuthnRegistration | mutation | OK | 20.446ms | 25.871ms | 21.762ms | 16.642ms | 27.429ms | 5 |
| AuthnService | UpdateUser | update_user | updateUser | mutation | OK | 11.622ms | 14.776ms | 13.444ms | 10.825ms | 19ms | 5 |
| AuthnService | ValidateCSRF | validate_csrf | validateCsrf | read_only | OK | 5.21ms | 8.185ms | 5.569ms | 3.646ms | 9.983ms | 25 |
| AuthnService | ValidateToken | validate_token | validateToken | read_only | OK | 23.027ms | 29.693ms | 23.9ms | 18.196ms | 42.064ms | 25 |
| AuthnService | VerifyMfaChallenge | verify_mfa_challenge | verifyMfaChallenge | read_only | OK | 9.902ms | 12.763ms | 10.019ms | 6.824ms | 13.274ms | 25 |
| AuthnService | VerifyOTP | verify_otp | verifyOtp | read_only | OK | 22.557ms | 39.546ms | 23.87ms | 15.197ms | 41.344ms | 25 |
| AuthzService | ActivateCanary | activate_canary | activateCanary | destructive | OK | 50.794ms | 50.794ms | 50.794ms | 50.794ms | 50.794ms | 1 |
| AuthzService | ActivatePolicyVersion | activate_policy_version | activatePolicyVersion | destructive | OK | 83.801ms | 83.801ms | 83.801ms | 83.801ms | 83.801ms | 1 |
| AuthzService | ApprovePolicyDraft | approve_policy_draft | approvePolicyDraft | mutation | OK | 55.951ms | 55.951ms | 55.951ms | 55.951ms | 55.951ms | 5 |
| AuthzService | AssignRole | assign_role | assignRole | mutation | OK | 38.777ms | 45.634ms | 56.006ms | 25.037ms | 143.233ms | 5 |
| AuthzService | Authorize | authorize | authorize | read_only | OK | 29.304ms | 40.031ms | 29.908ms | 22.685ms | 40.052ms | 25 |
| AuthzService | BatchCheckPermissions | batch_check_permissions | batchCheckPermissions | read_only | OK | 12.676ms | 16.992ms | 12.449ms | 8.51ms | 21.903ms | 25 |
| AuthzService | CheckAccess | check_access | checkAccess | read_only | OK | 10.604ms | 14.249ms | 10.817ms | 7.928ms | 15.603ms | 25 |
| AuthzService | CreatePolicyDraft | create_policy_draft | createPolicyDraft | mutation | OK | 50.539ms | 50.64ms | 48.575ms | 40.406ms | 57.233ms | 5 |
| AuthzService | CreatePolicyRule | create_policy_rule | createPolicyRule | mutation | OK | 17.859ms | 20.666ms | 19.485ms | 16.524ms | 24.754ms | 5 |
| AuthzService | CreateRole | create_role | createRole | mutation | OK | 38.605ms | 38.605ms | 38.605ms | 38.605ms | 38.605ms | 5 |
| AuthzService | DeletePolicyRule | delete_policy_rule | deletePolicyRule | mutation | OK | 10.656ms | 12.382ms | 10.429ms | 7.892ms | 13.14ms | 5 |
| AuthzService | DeleteRole | delete_role | deleteRole | mutation | OK | 10.941ms | 14.129ms | 17.257ms | 10.559ms | 39.928ms | 5 |
| AuthzService | DiffPolicyDraft | diff_policy_draft | diffPolicyDraft | read_only | OK | 13.924ms | 26.176ms | 16.71ms | 11.061ms | 68.881ms | 25 |
| AuthzService | ExplainPolicy | explain_policy | explainPolicy | read_only | OK | 8.008ms | 12.982ms | 9.176ms | 6.174ms | 24.163ms | 25 |
| AuthzService | GetAuthzRevision | get_authz_revision | getAuthzRevision | read_only | OK | 5.723ms | 6.698ms | 5.583ms | 3.938ms | 7.099ms | 25 |
| AuthzService | GetCanaryStatus | get_canary_status | getCanaryStatus | read_only | OK | 11.334ms | 27.18ms | 13.582ms | 8.696ms | 27.292ms | 25 |
| AuthzService | GetNativeAccess | get_native_access | getNativeAccess | read_only | OK | 37.174ms | 61.474ms | 39.438ms | 23.114ms | 70.359ms | 25 |
| AuthzService | GetPolicyBundle | get_policy_bundle | getPolicyBundle | read_only | OK | 12.766ms | 22.065ms | 14.165ms | 8.203ms | 34.136ms | 25 |
| AuthzService | GetPolicyRule | get_policy_rule | getPolicyRule | read_only | OK | 7.691ms | 13.067ms | 8.218ms | 5.05ms | 14.819ms | 25 |
| AuthzService | GetRole | get_role | getRole | read_only | OK | 6.803ms | 9.689ms | 7.223ms | 4.344ms | 11.929ms | 25 |
| AuthzService | InvalidatePolicyBundles | invalidate_policy_bundles | invalidatePolicyBundles | destructive | OK | 35.38ms | 35.38ms | 35.38ms | 35.38ms | 35.38ms | 1 |
| AuthzService | LintAuthzPolicies | lint_authz_policies | lintAuthzPolicies | read_only | OK | 2.756ms | 4.459ms | 2.842ms | 1.604ms | 4.954ms | 25 |
| AuthzService | ListAccessDecisionAudits | list_access_decision_audits | listAccessDecisionAudits | read_only | OK | 14.083ms | 23.32ms | 15.778ms | 9.803ms | 41.667ms | 25 |
| AuthzService | ListPolicyRules | list_policy_rules | listPolicyRules | read_only | OK | 7.197ms | 12.605ms | 7.809ms | 3.799ms | 16.265ms | 25 |
| AuthzService | ListPolicyVersions | list_policy_versions | listPolicyVersions | read_only | OK | 15.603ms | 20.471ms | 15.848ms | 10.981ms | 20.738ms | 25 |
| AuthzService | ListRoles | list_roles | listRoles | read_only | OK | 8.471ms | 11.771ms | 8.363ms | 4.864ms | 11.878ms | 25 |
| AuthzService | ListUserPermissions | list_user_permissions | listUserPermissions | read_only | OK | 2.796ms | 3.497ms | 2.652ms | 508µs | 3.806ms | 25 |
| AuthzService | ListUserRoles | list_user_roles | listUserRoles | read_only | OK | 6.343ms | 16.958ms | 7.285ms | 3.796ms | 17.149ms | 25 |
| AuthzService | MigrateLegacyPolicies | migrate_legacy_policies | migrateLegacyPolicies | destructive | OK | 50.928ms | 50.928ms | 50.928ms | 50.928ms | 50.928ms | 1 |
| AuthzService | PromoteCanary | promote_canary | promoteCanary | destructive | OK | 96.293ms | 96.293ms | 96.293ms | 96.293ms | 96.293ms | 1 |
| AuthzService | PutAuthzPolicy | put_authz_policy | putAuthzPolicy | mutation | OK | 21.725ms | 23.39ms | 22.028ms | 16.549ms | 27.602ms | 5 |
| AuthzService | PutRelationship | put_relationship | putRelationship | mutation | OK | 24.128ms | 24.999ms | 31.287ms | 21.887ms | 62.577ms | 5 |
| AuthzService | PutRoleBinding | put_role_binding | putRoleBinding | mutation | OK | 20.485ms | 21.363ms | 19.958ms | 16.825ms | 22.285ms | 5 |
| AuthzService | RejectPolicyDraft | reject_policy_draft | rejectPolicyDraft | mutation | OK | 29.113ms | 29.113ms | 29.113ms | 29.113ms | 29.113ms | 5 |
| AuthzService | RevokeRole | revoke_role | revokeRole | mutation | OK | 10.956ms | 14.885ms | 14.118ms | 10.327ms | 23.758ms | 5 |
| AuthzService | RollbackPolicyVersion | rollback_policy_version | rollbackPolicyVersion | destructive | OK | 88.081ms | 88.081ms | 88.081ms | 88.081ms | 88.081ms | 1 |
| AuthzService | SeedBuiltinRoles | seed_builtin_roles | seedBuiltinRoles | mutation | OK | 65.028ms | 65.918ms | 61.118ms | 49.97ms | 68.352ms | 5 |
| AuthzService | SimulatePolicy | simulate_policy | simulatePolicy | mutation | OK | 38.52ms | 48.478ms | 39.259ms | 25.106ms | 55.258ms | 5 |
| AuthzService | SubmitPolicyDraft | submit_policy_draft | submitPolicyDraft | mutation | OK | 29.638ms | 29.638ms | 29.638ms | 29.638ms | 29.638ms | 5 |
| AuthzService | UpdatePolicyDraft | update_policy_draft | updatePolicyDraft | mutation | OK | 46.449ms | 48.926ms | 47.887ms | 42.212ms | 58.397ms | 5 |
| AuthzService | UpdateRole | update_role | updateRole | mutation | OK | 34.33ms | 38.812ms | 34.458ms | 28.855ms | 40.811ms | 5 |
| BackupService | DeleteBackupPolicy | delete_backup_policy | deleteBackupPolicy | mutation | OK | 22.87ms | 33.105ms | 27.292ms | 20.809ms | 37.718ms | 5 |
| BackupService | GetBackup | get_backup | getBackup | read_only | OK | 21.156ms | 25.57ms | 21.973ms | 16.516ms | 31.569ms | 25 |
| BackupService | GetBackupPolicy | get_backup_policy | getBackupPolicy | read_only | OK | 12.35ms | 21.891ms | 13.656ms | 8.71ms | 25.02ms | 25 |
| BackupService | ListBackupPolicies | list_backup_policies | listBackupPolicies | read_only | OK | 12.426ms | 24.818ms | 13.578ms | 10.127ms | 25.92ms | 25 |
| BackupService | ListBackups | list_backups | listBackups | read_only | OK | 12.73ms | 16.392ms | 12.867ms | 9.366ms | 21.299ms | 25 |
| BackupService | PutBackupPolicy | put_backup_policy | putBackupPolicy | mutation | OK | 31.434ms | 33.09ms | 31.76ms | 30.33ms | 33.228ms | 5 |
| BackupService | RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 1.318203s | 1.318203s | 1.318203s | 1.318203s | 1.318203s | 1 |
| BackupService | StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 1.628222s | 1.685154s | 1.615326s | 1.467805s | 1.791749s | 5 |
| CacheService | CreateNamespace | create_cache_namespace | createCacheNamespace | mutation | OK | 19.762ms | 21.196ms | 22.315ms | 13.759ms | 37.266ms | 5 |
| CacheService | Delete | cache_delete | cacheNamespaceDelete | mutation | OK | 13.582ms | 19.23ms | 19.083ms | 10.439ms | 41.595ms | 5 |
| CacheService | DeleteNamespace | delete_cache_namespace | deleteCacheNamespace | destructive | OK | 33.063ms | 33.063ms | 33.063ms | 33.063ms | 33.063ms | 1 |
| CacheService | Get | cache_get | cacheNamespaceGet | read_only | OK | 8.7ms | 12.705ms | 9.68ms | 6.246ms | 22.728ms | 25 |
| CacheService | GetNamespaceStats | get_cache_namespace_stats | getCacheNamespaceStats | read_only | OK | 14.464ms | 28.029ms | 15.86ms | 10.075ms | 38.578ms | 25 |
| CacheService | Scan | cache_scan | cacheNamespaceScan | read_only | OK | 8.614ms | 11.654ms | 9.31ms | 6.368ms | 21.654ms | 25 |
| CacheService | Set | cache_set | cacheNamespaceSet | mutation | OK | 17.28ms | 18.092ms | 17.029ms | 14.45ms | 19.937ms | 5 |
| ConfigService | DeleteFlag | delete_flag | deleteFlag | destructive | OK | 19.1ms | 19.1ms | 19.1ms | 19.1ms | 19.1ms | 1 |
| ConfigService | EvaluateFlags | evaluate_flags | evaluateFlags | read_only | OK | 11.912ms | 16.542ms | 12.658ms | 8.752ms | 19.229ms | 25 |
| ConfigService | GetFlag | get_flag | getFlag | read_only | OK | 13.228ms | 26.511ms | 14.533ms | 9.855ms | 26.574ms | 25 |
| ConfigService | ListFlags | list_flags | listFlags | read_only | OK | 13.919ms | 20.867ms | 17.797ms | 11.735ms | 83.021ms | 25 |
| ConfigService | PutFlag | put_flag | putFlag | mutation | OK | 23.94ms | 27.025ms | 25.519ms | 20.369ms | 34.1ms | 5 |
| ControlPlaneService | AckStatus | ack_status | ackStatus | mutation | OK | 8.673ms | 9.683ms | 9.097ms | 8.067ms | 10.886ms | 5 |
| ControlPlaneService | DeltaResources | delta_resources | deltaResources | mutation | OK | 106.648ms | 125.462ms | 114.4ms | 95.579ms | 137.826ms | 5 |
| ControlPlaneService | GetResources | get_resources | getResources | read_only | OK | 5.936ms | 8.152ms | 5.958ms | 4.087ms | 8.564ms | 25 |
| ControlPlaneService | ListNodeStates | list_node_states | listNodeStates | read_only | OK | 50.737ms | 82.739ms | 57.342ms | 46.245ms | 98.179ms | 25 |
| ControlPlaneService | RollbackResources | rollback_resources | rollbackResources | mutation | OK | 72.16ms | 75.795ms | 78.311ms | 67.52ms | 106.207ms | 5 |
| ControlPlaneService | StreamResources | stream_resources | streamResources | mutation | OK | 107.885ms | 139.319ms | 117.769ms | 95.312ms | 143.193ms | 5 |
| DataBroker | ActivateCatalog | activate_catalog | activateCatalog | destructive | OK | 5.571ms | 5.571ms | 5.571ms | 5.571ms | 5.571ms | 1 |
| DataBroker | AnalyticalQuery | analytical_query | analyticalQuery | read_only | OK | 15.783ms | 27.078ms | 16.777ms | 10.947ms | 35.316ms | 25 |
| DataBroker | ApplyMigration | apply_migration | applyMigration | mutation | OK | 264.077ms | 264.077ms | 264.077ms | 264.077ms | 264.077ms | 5 |
| DataBroker | ApproveMigrationPlan | approve_migration_plan | approveMigrationPlan | mutation | OK | 27.172ms | 27.172ms | 27.172ms | 27.172ms | 27.172ms | 1 |
| DataBroker | BatchSelect | batch_select | batchSelect | mutation | OK | 6.846ms | 7.492ms | 9.561ms | 5.659ms | 21.843ms | 5 |
| DataBroker | BatchUpsert | batch_upsert | batchUpsert | mutation | OK | 31.002ms | 31.833ms | 35.916ms | 26.819ms | 62.271ms | 5 |
| DataBroker | BeginTx | begin_tx | beginTx | mutation | OK | 20.891ms | 21.608ms | 20.962ms | 19.02ms | 24.108ms | 5 |
| DataBroker | CacheDelete | cache_delete | cacheDelete | mutation | OK | 13.841ms | 14.655ms | 12.878ms | 7.119ms | 15.799ms | 5 |
| DataBroker | CacheGet | cache_get | cacheGet | read_only | OK | 10.057ms | 14.423ms | 10.421ms | 7.261ms | 18.335ms | 25 |
| DataBroker | CacheScan | cache_scan | cacheScan | read_only | OK | 17.006ms | 21.752ms | 16.483ms | 11.312ms | 24.951ms | 25 |
| DataBroker | CacheSet | cache_set | cacheSet | mutation | OK | 6.302ms | 11.168ms | 8.059ms | 5.693ms | 11.191ms | 5 |
| DataBroker | CreateMaterializedView | create_materialized_view | createMaterializedView | mutation | OK | 6.674ms | 6.697ms | 7.515ms | 6.071ms | 11.875ms | 5 |
| DataBroker | Delete | delete | delete | mutation | OK | 28.91ms | 30.998ms | 27.494ms | 19.148ms | 31.39ms | 5 |
| DataBroker | DeletePolicy | delete_policy | deletePolicy | mutation | OK | 34.312ms | 34.312ms | 34.312ms | 34.312ms | 34.312ms | 5 |
| DataBroker | DismissDlqEvent | dismiss_dlq_event | dismissDlqEvent | mutation | OK | 15.576ms | 16.176ms | 14.618ms | 11.137ms | 18.318ms | 5 |
| DataBroker | DocumentDelete | document_delete | documentDelete | mutation | OK | 7.014ms | 8.209ms | 7.466ms | 5.037ms | 11.196ms | 5 |
| DataBroker | DocumentFind | document_find | documentFind | read_only | OK | 9.053ms | 13.991ms | 9.751ms | 7.37ms | 13.991ms | 25 |
| DataBroker | DocumentGet | document_get | documentGet | read_only | OK | 9.461ms | 12.537ms | 9.847ms | 7.308ms | 16.448ms | 25 |
| DataBroker | DocumentUpsert | document_upsert | documentUpsert | mutation | OK | 6.669ms | 6.973ms | 6.882ms | 6.04ms | 8.389ms | 5 |
| DataBroker | DropResource | drop_resource | dropResource | destructive | OK | 21.56ms | 21.56ms | 21.56ms | 21.56ms | 21.56ms | 1 |
| DataBroker | EnqueueOutboxEvent | enqueue_outbox_event | enqueueOutboxEvent | mutation | OK | 16.924ms | 16.924ms | 16.924ms | 16.924ms | 16.924ms | 5 |
| DataBroker | EnsureBaseline | ensure_baseline | ensureBaseline | mutation | OK | 17.73ms | 20.989ms | 20.666ms | 17.32ms | 29.728ms | 5 |
| DataBroker | EnsureProject | ensure_project | ensureProject | mutation | OK | 12.647ms | 19.294ms | 14.782ms | 10.748ms | 19.433ms | 5 |
| DataBroker | EnsureResource | ensure_resource | ensureResource | mutation | OK | 23.384ms | 23.698ms | 25.278ms | 15.467ms | 43.497ms | 5 |
| DataBroker | GeneratePresignedUrl | generate_presigned_url | generatePresignedUrl | mutation | OK | 5.525ms | 7.259ms | 6.294ms | 4.66ms | 8.836ms | 5 |
| DataBroker | GenericDispatch | generic_dispatch | genericDispatch | mutation | OK | 7.68ms | 7.93ms | 7.613ms | 6.435ms | 8.882ms | 5 |
| DataBroker | GetAdminSummary | get_admin_summary | getAdminSummary | read_only | OK | 49.246ms | 101.119ms | 56.296ms | 35.696ms | 108.112ms | 25 |
| DataBroker | GetCapabilities | get_capabilities | getCapabilities | read_only | OK | 14.06ms | 18.359ms | 14.042ms | 9.378ms | 24.922ms | 25 |
| DataBroker | GetCatalogManifest | get_catalog_manifest | getCatalogManifest | read_only | OK | 25.178ms | 33.24ms | 26.043ms | 19.102ms | 38.653ms | 25 |
| DataBroker | GetCatalogVersion | get_catalog_version | getCatalogVersion | read_only | OK | 10.959ms | 14.514ms | 10.839ms | 6.577ms | 14.845ms | 25 |
| DataBroker | GetCatalogVersions | get_catalog_versions | getCatalogVersions | read_only | OK | 9.469ms | 15.965ms | 10.064ms | 5.929ms | 18.775ms | 25 |
| DataBroker | GetCdcStatus | get_cdc_status | getCdcStatus | read_only | OK | 6.42ms | 11.433ms | 6.768ms | 3.306ms | 15.37ms | 25 |
| DataBroker | GetDlqEvent | get_dlq_event | getDlqEvent | read_only | OK | 5.465ms | 7.697ms | 5.567ms | 3.242ms | 8.123ms | 25 |
| DataBroker | GetHealthReport | get_health_report | getHealthReport | read_only | OK | 2.708ms | 4.984ms | 2.91ms | 1.549ms | 5.095ms | 25 |
| DataBroker | GetMigrationStatus | get_migration_status | getMigrationStatus | read_only | OK | 6.16ms | 11.011ms | 6.817ms | 3.916ms | 15.258ms | 25 |
| DataBroker | GetObject | get_object | getObject | read_only | OK | 9.407ms | 13.421ms | 9.571ms | 6.299ms | 14.071ms | 25 |
| DataBroker | GetSaga | get_saga | getSaga | read_only | OK | 5.448ms | 7.669ms | 5.364ms | 3.23ms | 7.921ms | 25 |
| DataBroker | GraphMutate | graph_mutate | graphMutate | mutation | OK | 32.123ms | 35.915ms | 40.422ms | 22.534ms | 88.235ms | 5 |
| DataBroker | GraphQuery | graph_query | graphQuery | read_only | OK | 23.765ms | 43.463ms | 26.38ms | 17.692ms | 48.653ms | 25 |
| DataBroker | InitiateMultipartUpload | initiate_multipart_upload | initiateMultipartUpload | mutation | OK | 14.05ms | 17.181ms | 14.304ms | 11.321ms | 17.182ms | 5 |
| DataBroker | LintPolicies | lint_policies | lintPolicies | read_only | OK | 12.366ms | 22.243ms | 13.574ms | 6.863ms | 29.937ms | 25 |
| DataBroker | ListAdminAuditLogs | list_admin_audit_logs | listAdminAuditLogs | read_only | OK | 11.423ms | 15.065ms | 10.924ms | 7.681ms | 15.862ms | 25 |
| DataBroker | ListDlqEvents | list_dlq_events | listDlqEvents | read_only | OK | 8.265ms | 12.106ms | 8.607ms | 5.77ms | 15.538ms | 25 |
| DataBroker | ListMessageSchemas | list_message_schemas | listMessageSchemas | read_only | OK | 3.735ms | 4.613ms | 3.455ms | 1.882ms | 4.749ms | 25 |
| DataBroker | ListMigrationRuns | list_migration_runs | listMigrationRuns | read_only | OK | 7.655ms | 9.889ms | 7.646ms | 3.505ms | 10.471ms | 25 |
| DataBroker | ListPolicies | list_policies | listPolicies | read_only | OK | 6.72ms | 12.057ms | 7.458ms | 4.339ms | 12.397ms | 25 |
| DataBroker | ListProjects | list_projects | listProjects | read_only | OK | 7.146ms | 9.419ms | 7.509ms | 5.805ms | 10.26ms | 25 |
| DataBroker | ListResources | list_resources | listResources | read_only | OK | 7.154ms | 10.131ms | 7.259ms | 4.577ms | 10.424ms | 25 |
| DataBroker | ListSagas | list_sagas | listSagas | read_only | OK | 7.913ms | 10.232ms | 7.88ms | 4.838ms | 10.714ms | 25 |
| DataBroker | LookupMessageSchema | lookup_message_schema | lookupMessageSchema | read_only | OK | 3.296ms | 4.861ms | 3.397ms | 2.228ms | 5.81ms | 25 |
| DataBroker | MarkSagaReviewed | mark_saga_reviewed | markSagaReviewed | mutation | OK | 17.248ms | 19.051ms | 17.436ms | 14.122ms | 20.11ms | 5 |
| DataBroker | PauseCdc | pause_cdc | pauseCdc | mutation | OK | 20.345ms | 26.743ms | 24.037ms | 16.122ms | 39.081ms | 5 |
| DataBroker | PlanMigration | plan_migration | planMigration | mutation | OK | 18.275ms | 23.436ms | 20.513ms | 15.246ms | 28.547ms | 5 |
| DataBroker | PreviewCdcRedaction | preview_cdc_redaction | previewCdcRedaction | read_only | OK | 16.879ms | 26.197ms | 17.627ms | 10.739ms | 27.083ms | 25 |
| DataBroker | PublishCDC | publish_cdc | publishCdc | mutation | OK | 259.262ms | 259.262ms | 263.517ms | 244.073ms | 287.214ms | 3 |
| DataBroker | PutObject | put_object | putObject | mutation | OK | 20.144ms | 20.192ms | 19.467ms | 17.162ms | 20.233ms | 5 |
| DataBroker | PutPolicy | put_policy | putPolicy | destructive | OK | 18.94ms | 18.94ms | 18.94ms | 18.94ms | 18.94ms | 1 |
| DataBroker | QuarantineDlqEvent | quarantine_dlq_event | quarantineDlqEvent | mutation | OK | 14.52ms | 18.154ms | 17.175ms | 12.179ms | 27.588ms | 5 |
| DataBroker | ReloadPolicies | reload_policies | reloadPolicies | destructive | OK | 13.957ms | 13.957ms | 13.957ms | 13.957ms | 13.957ms | 1 |
| DataBroker | ReplayDlqEvent | replay_dlq_event | replayDlqEvent | mutation | OK | 46.985ms | 46.985ms | 46.985ms | 46.985ms | 46.985ms | 5 |
| DataBroker | ResumeCdc | resume_cdc | resumeCdc | mutation | OK | 13.989ms | 14.645ms | 14.108ms | 10.988ms | 17.202ms | 5 |
| DataBroker | RetrySagaCompensation | retry_saga_compensation | retrySagaCompensation | mutation | OK | 18.02ms | 18.02ms | 18.02ms | 18.02ms | 18.02ms | 5 |
| DataBroker | RollbackCatalog | rollback_catalog | rollbackCatalog | destructive | OK | 6.751ms | 6.751ms | 6.751ms | 6.751ms | 6.751ms | 1 |
| DataBroker | ScanProjectionDrift | scan_projection_drift | scanProjectionDrift | read_only | OK | 22.859ms | 33.819ms | 23.755ms | 12.353ms | 40.199ms | 25 |
| DataBroker | Select | select | select | read_only | OK | 6.82ms | 8.784ms | 7.042ms | 5.429ms | 8.803ms | 25 |
| DataBroker | SelectV2 | select_v_2 | selectV2 | read_only | OK | 7.585ms | 14.137ms | 8.64ms | 5.855ms | 23.335ms | 25 |
| DataBroker | StageCatalog | stage_catalog | stageCatalog | destructive | OK | 811.079ms | 811.079ms | 811.079ms | 811.079ms | 811.079ms | 1 |
| DataBroker | StepDownCdcLeader | step_down_cdc_leader | stepDownCdcLeader | mutation | OK | 13.363ms | 15.072ms | 14.468ms | 12.206ms | 19.044ms | 5 |
| DataBroker | TimeSeriesQuery | time_series_query | timeSeriesQuery | read_only | OK | 9.256ms | 11.32ms | 9.401ms | 6.398ms | 16.372ms | 25 |
| DataBroker | TimeSeriesWrite | time_series_write | timeSeriesWrite | mutation | OK | 64.578ms | 68.946ms | 74.342ms | 58.007ms | 118.464ms | 5 |
| DataBroker | Upsert | upsert | upsert | mutation | OK | 37.318ms | 42.414ms | 38.577ms | 33.087ms | 45.591ms | 5 |
| DataBroker | ValidateCatalog | validate_catalog | validateCatalog | destructive | OK | 94.061ms | 94.061ms | 94.061ms | 94.061ms | 94.061ms | 1 |
| DataBroker | VectorBatchUpsert | vector_batch_upsert | vectorBatchUpsert | mutation | OK | 8.425ms | 9.791ms | 10.345ms | 8.008ms | 17.213ms | 5 |
| DataBroker | VectorHybridSearch | vector_hybrid_search | vectorHybridSearch | read_only | OK | 6.445ms | 8.002ms | 6.374ms | 4.637ms | 8.747ms | 25 |
| DataBroker | VectorSearch | vector_search | vectorSearch | read_only | OK | 6.244ms | 8.825ms | 6.465ms | 4.635ms | 8.895ms | 25 |
| DataBroker | VectorUpsert | vector_upsert | vectorUpsert | mutation | OK | 14.371ms | 15.35ms | 14.621ms | 12.41ms | 17.776ms | 5 |
| DataBroker | VerifyAdminAuditLog | verify_admin_audit_log | verifyAdminAuditLog | read_only | OK | 10.313ms | 17.112ms | 11.207ms | 6.411ms | 19.631ms | 25 |
| EmbeddingService | Backfill | backfill | backfillEmbeddingSource | mutation | OK | 15.306ms | 15.427ms | 14.893ms | 13.889ms | 15.629ms | 5 |
| EmbeddingService | DeleteSource | delete_source | deleteEmbeddingSource | destructive | OK | 30.034ms | 30.034ms | 30.034ms | 30.034ms | 30.034ms | 1 |
| EmbeddingService | ListSources | list_sources | listEmbeddingSources | read_only | OK | 12.067ms | 19.181ms | 13.288ms | 10.025ms | 30.358ms | 25 |
| EmbeddingService | RegisterSource | register_source | registerEmbeddingSource | mutation | OK | 31.353ms | 32.436ms | 45.042ms | 26.767ms | 103.562ms | 5 |
| EmbeddingService | ReportEmbedding | report_embedding | reportEmbedding | mutation | OK | 16.883ms | 20.78ms | 19.092ms | 14.533ms | 26.938ms | 5 |
| EmbeddingService | Retrieve | retrieve | retrieveEmbedding | read_only | OK | 15.171ms | 20.385ms | 15.72ms | 10.502ms | 22.767ms | 25 |
| IdentityProviderService | CreateProvider | create_provider | createProvider | mutation | OK | 16.678ms | 16.678ms | 16.678ms | 16.678ms | 16.678ms | 5 |
| IdentityProviderService | DisableProvider | disable_provider | disableProvider | mutation | OK | 20.145ms | 24.132ms | 21.478ms | 16.53ms | 27.422ms | 5 |
| IdentityProviderService | ForceJwksRefresh | force_jwks_refresh | forceJwksRefresh | mutation | OK | 22.601ms | 33.153ms | 27.248ms | 19.858ms | 38.378ms | 5 |
| IdentityProviderService | GetProvider | get_provider | getProvider | read_only | OK | 5.331ms | 7.695ms | 5.615ms | 4.19ms | 9.332ms | 25 |
| IdentityProviderService | ImportSamlMetadata | import_saml_metadata | importSamlMetadata | mutation | OK | 24.015ms | 26.543ms | 24.791ms | 20.88ms | 29.372ms | 5 |
| IdentityProviderService | LinkIdentity | link_identity | linkIdentity | mutation | OK | 22.733ms | 26.612ms | 24.505ms | 19.49ms | 32.1ms | 5 |
| IdentityProviderService | ListExternalIdentities | list_external_identities | listExternalIdentities | read_only | OK | 8.638ms | 11.52ms | 9.057ms | 6.106ms | 14.193ms | 25 |
| IdentityProviderService | ListProviders | list_providers | listProviders | read_only | OK | 8.704ms | 11.983ms | 9.01ms | 5.943ms | 14.397ms | 25 |
| IdentityProviderService | PreviewClaimMapping | preview_claim_mapping | previewClaimMapping | read_only | OK | 6.561ms | 14.457ms | 7.635ms | 3.62ms | 16.115ms | 25 |
| IdentityProviderService | PreviewGroupMapping | preview_group_mapping | previewGroupMapping | read_only | OK | 7.333ms | 10.644ms | 7.157ms | 3.858ms | 11.752ms | 25 |
| IdentityProviderService | ResolveExternalIdentity | resolve_external_identity | resolveExternalIdentity | mutation | OK | 7.488ms | 8.084ms | 12.457ms | 5.862ms | 33.852ms | 5 |
| IdentityProviderService | SamlAcs | saml_acs | samlAcs | mutation | OK | 101.328ms | 107.251ms | 100.969ms | 87.576ms | 110.584ms | 5 |
| IdentityProviderService | ScimCreateGroup | scim_create_group | scimCreateGroup | mutation | OK | 4.463ms | 4.64ms | 4.418ms | 4.009ms | 4.697ms | 5 |
| IdentityProviderService | ScimCreateUser | scim_create_user | scimCreateUser | mutation | OK | 25.269ms | 29.554ms | 26.304ms | 21.845ms | 29.963ms | 5 |
| IdentityProviderService | ScimDeleteGroup | scim_delete_group | scimDeleteGroup | mutation | OK | 6.139ms | 6.19ms | 6.341ms | 4.511ms | 9.801ms | 5 |
| IdentityProviderService | ScimDeleteUser | scim_delete_user | scimDeleteUser | mutation | OK | 58.089ms | 58.089ms | 58.089ms | 58.089ms | 58.089ms | 5 |
| IdentityProviderService | ScimGetGroup | scim_get_group | scimGetGroup | mutation | OK | 7.547ms | 8.988ms | 7.85ms | 5.72ms | 9.866ms | 5 |
| IdentityProviderService | ScimGetUser | scim_get_user | scimGetUser | mutation | OK | 7.933ms | 8.426ms | 8.306ms | 6.975ms | 10.839ms | 5 |
| IdentityProviderService | ScimListGroups | scim_list_groups | scimListGroups | mutation | OK | 4.058ms | 4.816ms | 4.433ms | 3.63ms | 5.698ms | 5 |
| IdentityProviderService | ScimListUsers | scim_list_users | scimListUsers | mutation | OK | 9.688ms | 10.15ms | 9.688ms | 6.77ms | 12.759ms | 5 |
| IdentityProviderService | ScimPatchGroup | scim_patch_group | scimPatchGroup | mutation | OK | 10.913ms | 14.21ms | 11.775ms | 8.878ms | 15.861ms | 5 |
| IdentityProviderService | ScimPatchUser | scim_patch_user | scimPatchUser | mutation | OK | 29.106ms | 33.498ms | 27.082ms | 18.715ms | 34.136ms | 5 |
| IdentityProviderService | ScimReplaceUser | scim_replace_user | scimReplaceUser | mutation | OK | 20.84ms | 20.87ms | 20.559ms | 19.073ms | 21.939ms | 5 |
| IdentityProviderService | StartSamlLogin | start_saml_login | startSamlLogin | mutation | OK | 4.669ms | 4.676ms | 4.637ms | 3.853ms | 5.722ms | 5 |
| IdentityProviderService | TestProviderDiscovery | test_provider_discovery | testProviderDiscovery | read_only | OK | 6.833ms | 9.965ms | 7.505ms | 4.796ms | 25.125ms | 25 |
| IdentityProviderService | UnlinkIdentity | unlink_identity | unlinkIdentity | mutation | OK | 6.503ms | 9.832ms | 8.969ms | 6.023ms | 16.442ms | 5 |
| IdentityProviderService | UpdateProvider | update_provider | updateProvider | mutation | OK | 17.653ms | 21.489ms | 21.256ms | 16.537ms | 33.707ms | 5 |
| LiveQueryService | Subscribe | subscribe | liveQuerySubscribe | read_only | OK | 14.168ms | 21.829ms | 15.321ms | 9.628ms | 38.337ms | 25 |
| LockService | AcquireLock | acquire_lock | acquireLock | mutation | OK | 35.007ms | 42.065ms | 38.173ms | 33.028ms | 46.519ms | 5 |
| LockService | GetLock | get_lock | getLock | read_only | OK | 19.322ms | 36.781ms | 19.983ms | 11.159ms | 37.781ms | 25 |
| LockService | ListLocks | list_locks | listLocks | read_only | OK | 14.964ms | 27.795ms | 16.261ms | 10.953ms | 30.496ms | 25 |
| LockService | ReleaseLock | release_lock | releaseLock | mutation | OK | 11.99ms | 14.334ms | 16.341ms | 10.347ms | 33.587ms | 5 |
| LockService | RenewLock | renew_lock | renewLock | mutation | OK | 37.047ms | 37.481ms | 36.515ms | 29.496ms | 41.703ms | 5 |
| MeteringService | CheckQuota | check_quota | checkQuota | read_only | OK | 11.233ms | 18.949ms | 12.446ms | 9.79ms | 23.033ms | 25 |
| MeteringService | GetQuota | get_quota | getQuota | read_only | OK | 12.448ms | 17.951ms | 13.012ms | 10.242ms | 22.088ms | 25 |
| MeteringService | ListQuotas | list_quotas | listQuotas | read_only | OK | 14.734ms | 30.807ms | 19.231ms | 11.196ms | 78.324ms | 25 |
| MeteringService | PutQuota | put_quota | putQuota | mutation | OK | 21.903ms | 22.088ms | 21.6ms | 18.883ms | 25.624ms | 5 |
| MeteringService | QueryUsage | query_usage | queryUsage | read_only | OK | 12.697ms | 18.583ms | 13.224ms | 9.498ms | 21.868ms | 25 |
| MeteringService | RecordUsage | record_usage | recordUsage | mutation | OK | 7.297ms | 8.061ms | 7.773ms | 7.231ms | 9.002ms | 5 |
| NotificationService | GetDeliveryStats | get_delivery_stats | getDeliveryStats | read_only | OK | 10.212ms | 13.663ms | 10.27ms | 7.081ms | 21.999ms | 25 |
| NotificationService | GetNotification | get_notification | getNotification | read_only | OK | 17.752ms | 31.49ms | 19.213ms | 10.187ms | 50.93ms | 25 |
| NotificationService | GetPreference | get_preference | getPreference | read_only | OK | 15.934ms | 30.479ms | 17.64ms | 9.808ms | 32.486ms | 25 |
| NotificationService | GetTemplate | get_template | getTemplate | read_only | OK | 14.921ms | 25.36ms | 15.861ms | 9.591ms | 27.749ms | 25 |
| NotificationService | ListNotifications | list_notifications | listNotifications | read_only | OK | 19.223ms | 35.874ms | 21.418ms | 15.765ms | 42.257ms | 25 |
| NotificationService | ListPreferences | list_preferences | listPreferences | read_only | OK | 19.287ms | 29.586ms | 20.422ms | 13.805ms | 37.306ms | 25 |
| NotificationService | ListTemplates | list_templates | listTemplates | read_only | OK | 19.248ms | 28.775ms | 20.303ms | 13.297ms | 34.501ms | 25 |
| NotificationService | ReportDelivery | report_delivery | reportDelivery | mutation | OK | 13.933ms | 15.149ms | 15.646ms | 11.546ms | 23.686ms | 5 |
| NotificationService | RetryNotification | retry_notification | retryNotification | mutation | OK | 14.079ms | 14.079ms | 14.079ms | 14.079ms | 14.079ms | 5 |
| NotificationService | SendNotification | send_notification | sendNotification | mutation | OK | 38ms | 63.6ms | 49.728ms | 37.02ms | 72.815ms | 5 |
| NotificationService | SetPreference | set_preference | setPreference | mutation | OK | 10.491ms | 12.294ms | 11.573ms | 8.67ms | 17.109ms | 5 |
| NotificationService | UpsertTemplate | upsert_template | upsertTemplate | mutation | OK | 8.265ms | 10.474ms | 10.616ms | 6.76ms | 19.722ms | 5 |
| PeerService | GetPeer | get_peer | getPeer | read_only | OK | 40.509ms | 138.958ms | 64.61ms | 22.804ms | 174.462ms | 25 |
| PeerService | JoinRoom | join_room | joinRoom | mutation | OK | 30.818ms | 38.83ms | 35.497ms | 24.627ms | 56.367ms | 5 |
| PeerService | JoinSession | join_session | joinSession | mutation | OK | 23.512ms | 26.043ms | 26.594ms | 22.296ms | 37.778ms | 5 |
| PeerService | LeaveRoom | leave_room | leaveRoom | mutation | OK | 11.843ms | 16.903ms | 12.197ms | 7.15ms | 17.228ms | 5 |
| PeerService | ListPeers | list_peers | listPeers | read_only | OK | 29.858ms | 70.074ms | 34.98ms | 15.715ms | 121.075ms | 25 |
| RoomService | CloseRoom | close_room | closeRoom | mutation | OK | 24.25ms | 31.411ms | 26.701ms | 21.892ms | 31.926ms | 5 |
| RoomService | CreateRoom | create_room | createRoom | mutation | OK | 19.983ms | 20.697ms | 20.028ms | 18.776ms | 21.311ms | 5 |
| RoomService | GetRoom | get_room | getRoom | read_only | OK | 32.265ms | 73.98ms | 39.46ms | 18.559ms | 86.866ms | 25 |
| RoomService | ListEgress | list_egress | listEgress | read_only | CAPABILITY_SKIPPED | 10.477ms | 25.499ms | 12.283ms | 6.765ms | 33.739ms | 25 |
| RoomService | ListRooms | list_rooms | listRooms | read_only | OK | 22.666ms | 36.061ms | 24.315ms | 13.799ms | 45.523ms | 25 |
| RoomService | StartRoomComposite | start_room_composite | startRoomComposite | mutation | CAPABILITY_SKIPPED | 5.031ms | 5.672ms | 5.369ms | 4.804ms | 6.347ms | 5 |
| RoomService | StartTrackEgress | start_track_egress | startTrackEgress | mutation | CAPABILITY_SKIPPED | 6.996ms | 9.271ms | 8.637ms | 6.574ms | 13.471ms | 5 |
| RoomService | StopEgress | stop_egress | stopEgress | mutation | CAPABILITY_SKIPPED | 8.632ms | 8.85ms | 8.669ms | 7.123ms | 11.082ms | 5 |
| RoomService | UpdateRoom | update_room | updateRoom | mutation | OK | 12.3ms | 16.717ms | 13.56ms | 8.057ms | 20.844ms | 5 |
| SchedulerService | CreateJob | create_job | createJob | mutation | OK | 18.55ms | 19.948ms | 17.793ms | 13.848ms | 20.31ms | 5 |
| SchedulerService | DeleteJob | delete_job | deleteJob | destructive | OK | 25.461ms | 25.461ms | 25.461ms | 25.461ms | 25.461ms | 1 |
| SchedulerService | GetJob | get_job | getJob | read_only | OK | 8.817ms | 13.162ms | 9.764ms | 7.293ms | 24.11ms | 25 |
| SchedulerService | ListJobs | list_jobs | listJobs | read_only | OK | 11.355ms | 18.575ms | 12.51ms | 9.305ms | 26.652ms | 25 |
| SchedulerService | PauseJob | pause_job | pauseJob | mutation | OK | 14.049ms | 14.049ms | 14.049ms | 14.049ms | 14.049ms | 5 |
| SchedulerService | ResumeJob | resume_job | resumeJob | mutation | OK | 16.205ms | 16.205ms | 16.205ms | 16.205ms | 16.205ms | 5 |
| SearchService | CreateIndex | create_index | createSearchIndex | mutation | OK | 26.609ms | 27.27ms | 27.668ms | 20.594ms | 37.585ms | 5 |
| SearchService | DeleteIndex | delete_index | deleteSearchIndex | destructive | OK | 19.555ms | 19.555ms | 19.555ms | 19.555ms | 19.555ms | 1 |
| SearchService | ListIndexes | list_indexes | listSearchIndexes | read_only | OK | 11.987ms | 27.886ms | 14.141ms | 9.618ms | 28.618ms | 25 |
| SearchService | Reindex | reindex | reindexSearchIndex | mutation | OK | 24.428ms | 24.525ms | 25.272ms | 20.871ms | 35.229ms | 5 |
| SearchService | Search | search | search | read_only | OK | 12.308ms | 16.161ms | 12.906ms | 9.606ms | 26.403ms | 25 |
| SignalingService | Signal | signal | signal | mutation | OK | 16.296ms | 16.296ms | 16.296ms | 16.296ms | 16.296ms | 5 |
| StorageService | DeleteFile | delete_file | deleteFile | mutation | OK | 27.988ms | 27.988ms | 27.988ms | 27.988ms | 27.988ms | 5 |
| StorageService | DownloadFile | download_file | downloadFile | read_only | OK | 23.352ms | 31.478ms | 23.799ms | 18.646ms | 31.795ms | 25 |
| StorageService | FinalizeUpload | finalize_upload | finalizeUpload | mutation | OK | 35.457ms | 35.457ms | 35.457ms | 35.457ms | 35.457ms | 5 |
| StorageService | GetDownloadUrl | get_download_url | getDownloadUrl | read_only | OK | 15.67ms | 24.336ms | 16.795ms | 12.22ms | 25.798ms | 25 |
| StorageService | GetFile | get_file | getFile | read_only | OK | 13.105ms | 20.012ms | 13.791ms | 9.11ms | 28.504ms | 25 |
| StorageService | ListFiles | list_files | listFiles | read_only | OK | 19.821ms | 33.917ms | 21.709ms | 15.855ms | 34.637ms | 25 |
| StorageService | RegisterUpload | register_upload | registerUpload | mutation | OK | 27.614ms | 29.844ms | 26.499ms | 17.781ms | 36.151ms | 5 |
| StorageService | ReissueUploadUrl | reissue_upload_url | reissueUploadUrl | read_only | OK | 14.249ms | 25.168ms | 15.748ms | 11.333ms | 27.047ms | 25 |
| StorageService | UpdateFile | update_file | updateFile | mutation | OK | 22.968ms | 23.886ms | 23.746ms | 20.822ms | 28.186ms | 5 |
| TenantService | CreateTenant | create_tenant | createTenant | mutation | OK | 14.726ms | 14.979ms | 14.2ms | 11.778ms | 16.163ms | 5 |
| TenantService | GetTenant | get_tenant | getTenant | read_only | OK | 12.357ms | 18.256ms | 12.74ms | 9.103ms | 21.81ms | 25 |
| TenantService | GetTenantConfig | get_tenant_config | getTenantConfig | read_only | OK | 12.54ms | 17.623ms | 13.456ms | 9.523ms | 31.388ms | 25 |
| TenantService | ListTenants | list_tenants | listTenants | read_only | OK | 14.283ms | 25.585ms | 14.941ms | 9.031ms | 26.599ms | 25 |
| TenantService | PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 255.008ms | 255.008ms | 255.008ms | 255.008ms | 255.008ms | 1 |
| TenantService | UpdateTenant | update_tenant | updateTenant | mutation | OK | 16.549ms | 17.282ms | 17.627ms | 13.511ms | 25.743ms | 5 |
| TenantService | UpdateTenantConfig | update_tenant_config | updateTenantConfig | mutation | OK | 23.964ms | 37.834ms | 33.814ms | 22.602ms | 61.869ms | 5 |
| TrackService | ListTracks | list_tracks | listTracks | read_only | OK | 35.103ms | 84.456ms | 43.06ms | 12.98ms | 231.899ms | 25 |
| TrackService | MuteTrack | mute_track | muteTrack | mutation | OK | 9.827ms | 11.024ms | 10.392ms | 8.801ms | 13.373ms | 5 |
| TrackService | PublishTrack | publish_track | publishTrack | mutation | OK | 16.464ms | 20.128ms | 17.897ms | 16.092ms | 20.416ms | 5 |
| TrackService | UnpublishTrack | unpublish_track | unpublishTrack | mutation | OK | 11.013ms | 14.648ms | 13.56ms | 8.523ms | 22.694ms | 5 |
| TurnService | IssueCredentials | issue_credentials | issueCredentials | mutation | OK | 10.319ms | 11.744ms | 11.583ms | 8.871ms | 16.714ms | 5 |
| VaultService | BatchDecrypt | batch_decrypt | vaultBatchDecrypt | mutation | OK | 18.732ms | 19.965ms | 20.458ms | 14.858ms | 31.271ms | 5 |
| VaultService | BatchEncrypt | batch_encrypt | vaultBatchEncrypt | mutation | OK | 19.977ms | 20.34ms | 20.091ms | 14.887ms | 27.029ms | 5 |
| VaultService | CreateTransitKey | create_transit_key | createTransitKey | mutation | OK | 22.482ms | 22.482ms | 22.482ms | 22.482ms | 22.482ms | 5 |
| VaultService | Decrypt | decrypt | vaultDecrypt | read_only | OK | 20.269ms | 28.672ms | 20.42ms | 13.647ms | 31.261ms | 25 |
| VaultService | DeleteSecret | delete_secret | deleteSecret | mutation | OK | 14.575ms | 15.709ms | 15.259ms | 10.922ms | 23.506ms | 5 |
| VaultService | DestroySecret | destroy_secret | destroySecret | destructive | OK | 22.313ms | 22.313ms | 22.313ms | 22.313ms | 22.313ms | 1 |
| VaultService | Encrypt | encrypt | vaultEncrypt | mutation | OK | 15.301ms | 16.768ms | 15.37ms | 13.07ms | 16.988ms | 5 |
| VaultService | GenerateDataKey | generate_data_key | vaultGenerateDataKey | mutation | OK | 18.991ms | 19.333ms | 18.574ms | 15.624ms | 22.067ms | 5 |
| VaultService | GenerateDatabaseCredentials | generate_database_credentials | generateDatabaseCredentials | mutation | OK | 24.486ms | 25.378ms | 25.936ms | 18.836ms | 37.229ms | 5 |
| VaultService | GetSecret | get_secret | getSecret | read_only | OK | 19.727ms | 31.603ms | 20.642ms | 13.198ms | 33.89ms | 25 |
| VaultService | GetTransitPublicKey | get_transit_public_key | vaultGetTransitPublicKey | read_only | OK | 12.525ms | 15.798ms | 12.593ms | 9.681ms | 16.158ms | 25 |
| VaultService | Hmac | hmac | vaultHmac | mutation | OK | 19.838ms | 19.865ms | 19.921ms | 13.523ms | 29.981ms | 5 |
| VaultService | ListSecrets | list_secrets | listSecrets | read_only | OK | 17.288ms | 22.983ms | 18.43ms | 14.639ms | 35.33ms | 25 |
| VaultService | PutSecret | put_secret | putSecret | mutation | OK | 27.407ms | 27.407ms | 27.407ms | 27.407ms | 27.407ms | 5 |
| VaultService | Rewrap | rewrap | vaultRewrap | mutation | OK | 15.706ms | 18.025ms | 16.566ms | 13.313ms | 22.19ms | 5 |
| VaultService | RotateTransitKey | rotate_transit_key | rotateTransitKey | mutation | OK | 30.667ms | 44.214ms | 36.714ms | 28.86ms | 49.836ms | 5 |
| VaultService | SealStatus | seal_status | vaultSealStatus | read_only | OK | 2.992ms | 3.918ms | 3.096ms | 1.759ms | 4.649ms | 25 |
| VaultService | Sign | sign | vaultSign | mutation | OK | 15.871ms | 16.23ms | 16.17ms | 15.454ms | 17.546ms | 5 |
| VaultService | UndeleteSecret | undelete_secret | undeleteSecret | mutation | OK | 22.637ms | 22.637ms | 22.637ms | 22.637ms | 22.637ms | 5 |
| VaultService | Verify | verify | vaultVerify | read_only | OK | 16.313ms | 24.052ms | 17.21ms | 13.441ms | 24.621ms | 25 |
| WebhookService | CreateEndpoint | create_endpoint | createWebhookEndpoint | mutation | OK | 13.577ms | 14.468ms | 13.655ms | 10.399ms | 18.232ms | 5 |
| WebhookService | DeleteEndpoint | delete_endpoint | deleteWebhookEndpoint | destructive | OK | 12.688ms | 12.688ms | 12.688ms | 12.688ms | 12.688ms | 1 |
| WebhookService | GetEndpoint | get_endpoint | getWebhookEndpoint | read_only | OK | 8.623ms | 11.751ms | 9.132ms | 6.434ms | 20.532ms | 25 |
| WebhookService | ListDeliveries | list_deliveries | listWebhookDeliveries | read_only | OK | 17.978ms | 32.621ms | 19.597ms | 10.809ms | 33.637ms | 25 |
| WebhookService | ListEndpoints | list_endpoints | listWebhookEndpoints | read_only | OK | 16.176ms | 28.897ms | 17.557ms | 9.211ms | 35.617ms | 25 |
| WebhookService | UpdateEndpoint | update_endpoint | updateWebhookEndpoint | mutation | OK | 14.094ms | 15.349ms | 16.38ms | 12.332ms | 27.09ms | 5 |
| WorkflowService | CancelWorkflow | cancel_workflow | cancelWorkflow | destructive | OK | 19.047ms | 19.047ms | 19.047ms | 19.047ms | 19.047ms | 1 |
| WorkflowService | GetWorkflow | get_workflow | getWorkflow | read_only | OK | 19.445ms | 26.924ms | 20.264ms | 12.097ms | 44.519ms | 25 |
| WorkflowService | ListWorkflows | list_workflows | listWorkflows | read_only | OK | 32.813ms | 54.372ms | 34.469ms | 23.451ms | 56.096ms | 25 |
| WorkflowService | SignalWorkflow | signal_workflow | signalWorkflow | mutation | OK | 19.324ms | 20.4ms | 18.802ms | 14.99ms | 23.868ms | 5 |
| WorkflowService | StartWorkflow | start_workflow | startWorkflow | mutation | OK | 16.677ms | 18.526ms | 16.958ms | 14.238ms | 20.512ms | 5 |
