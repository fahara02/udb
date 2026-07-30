# UDB SDK Live Perf — Go (localhost)

RPCs measured: 376   tenant=ffb87952-295b-40a1-8a8e-e2de2a7867fd

Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities (a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request's reference/ID fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection latency. The TARGET is zero failures; any residual non-OK RPC is listed under Failures for the maintainer to finish.

Unary RPCs = full request→response round-trip. Non-CDC streaming RPCs report time-to-FIRST-RESPONSE with seeded inputs. CDC subscription (PublishCDC) reports time-to-FIRST-EVENT: the harness subscribes, fires a real Upsert that flows outbox→CDC→Kafka, and times the first delivered event. Streaming rows are marked in the note column.

## Seeded fixtures

Captured semantic field → seeded value keys used to resolve request fields: action, admin_reset_mfa_user_id, admin_reset_password_user_id, apply_run_id, approval_token, approve_draft_id, approve_run_id, approved_by, asset_id, assigned_by, auth_token, backup_id, bucket, canary_id, canary_version_id, cancel_workflow_id, catalog_manifest, catalog_manifest_b64, challenge_id, change_password_user_id, change_status_user_id, close_room_id, code, collection, content_type, created_by, csrf_token, definition_id, delete_endpoint_id, delete_file_id, delete_policy_id, delete_role_id, delete_scim_user_id, deleted_by, device_id, disable_mfa_user_id, disable_provider_id, dismiss_dlq_id, dlq_id, document_id, domain, ds_policy_id, egress_id, embedding_delete_model_id, embedding_document_id, embedding_document_job_id, embedding_job_id, embedding_work_item_id, endpoint_id, event_type, external_identity_id, file_id, file_type, filename, finalize_file_id, gov_exp, grant_binding_id, grant_create_user_id, instance_id, job_id, join_session_room_id, key_id, kind, leave_peer_id, locale, log_id, mark_saga_id, message_type, migration_id, mongo_collection, name, node_id, notification_id, object, object_key, otp_code, otp_id, owner_id, peer_id, plain_key, policy_draft_id, policy_id, policy_version_id, project, project_id, provider_id, purge_tenant_id, quarantine_dlq_id, recipient_id, record_id, recovery_code, refresh_session_id, refresh_token, reissue_file_id, reject_draft_id, rejected_by, relation, release_fencing_token, renew_fencing_token, replay_dlq_id, reset_otp_code, reset_otp_id, resource, resource_name, restore_tenant_id, retry_saga_id, revoke_device_id, revoke_device_user_id, revoke_key_id, revoke_recovery_user_id, revoked_by, role, role_code, role_id, rollback_policy_set_id, rollback_resource_version, rollback_target_version_id, room_id, saga_id, saml_provider_id, scim_group_id, scim_user_id, session_id, signal_peer_id, stage_name, step_id, subject, tenant, tenant_code, tenant_id, token, topic_pattern, track_id, ts_table, unpublish_track_id, update_draft_id, update_key_id, updated_by, user_id, user_role_id, username, vault_ciphertext, vault_create_key_name, vault_db_role, vault_delete_secret_path, vault_destroy_secret_path, vault_hmac_key_name, vault_key_name, vault_put_secret_path, vault_secret_path, vault_signature, vault_signing_key_name, workflow_id

## Per-service mean latency (mean of per-RPC means)

| Service | RPCs | mean |
|---|---:|---:|
| AuthnService | 59 | 56.149ms |
| BackupService | 8 | 277.356ms |
| DataBroker | 78 | 22.653ms |
| AuthzService | 41 | 21.281ms |
| EmbeddingService | 19 | 27.505ms |
| IdentityProviderService | 27 | 14.521ms |
| VaultService | 20 | 17.295ms |
| ControlPlaneService | 6 | 47.24ms |
| ConfigService | 5 | 55.265ms |
| TenantService | 7 | 38.778ms |
| StorageService | 9 | 25.032ms |
| NotificationService | 12 | 16.111ms |
| AssetService | 8 | 19.077ms |
| ApiKeyService | 9 | 15.229ms |
| SearchService | 5 | 21.417ms |
| CacheService | 7 | 14.15ms |
| RoomService | 9 | 10.948ms |
| LockService | 5 | 17.145ms |
| PeerService | 5 | 16.905ms |
| AnalyticsService | 7 | 11.936ms |
| SchedulerService | 6 | 13.7ms |
| MeteringService | 6 | 12.796ms |
| WebhookService | 6 | 12.738ms |
| WorkflowService | 5 | 15.148ms |
| TrackService | 4 | 12.451ms |
| SignalingService | 1 | 16.439ms |
| LiveQueryService | 1 | 14.774ms |
| TurnService | 1 | 8.43ms |

## Failures — still to fix (5)

These RPCs still returned a non-OK gRPC status on their last iteration: the seed phase could not construct a fully-valid request for them. They are reported (not silently sampled) so the maintainer can finish their seeding/fixtures.

| RPC | api_alias | operation_id | kind | err | detail | p99 | mean | iters |
|---|---|---|---|---|---|---:|---:|---:|
| AuthnService/FinishWebAuthnAuthentication | finish_web_authn_authentication | finishWebAuthnAuthentication | mutation | NO-BODY | NO-BODY | 0s | 0s | 5 |
| AuthnService/FinishWebAuthnRegistration | finish_web_authn_registration | finishWebAuthnRegistration | mutation | NO-BODY | NO-BODY | 0s | 101µs | 5 |
| AuthnService/StartWebAuthnAuthentication | start_web_authn_authentication | startWebAuthnAuthentication | mutation | FailedPrecondition | udb /udb.core.authn.services.v1.AuthnService/StartWebAuthnAuthentication: WebAuthn requires building UDB with the `webauthn` feature (FailedPrecondition) [+error-detail 41B] | 1.617ms | 1.39ms | 5 |
| AuthnService/StartWebAuthnRegistration | start_web_authn_registration | startWebAuthnRegistration | mutation | FailedPrecondition | udb /udb.core.authn.services.v1.AuthnService/StartWebAuthnRegistration: WebAuthn requires building UDB with the `webauthn` feature (FailedPrecondition) [+error-detail 41B] | 1.619ms | 1.611ms | 5 |
| LockService/RenewLock | renew_lock | renewLock | mutation | NotFound | udb /udb.core.lock.services.v1.LockService/RenewLock: lock not held (NotFound) [+error-detail 35B] | 10.256ms | 10.205ms | 5 |

## Slowest 25 RPCs by p99

| RPC | api_alias | operation_id | kind | err | p50 | p99 | mean | iters | note |
|---|---|---|---|---|---:|---:|---:|---:|---|
| BackupService/StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 1.115287s | 1.169338s | 1.13336s | 5 | mutation (seeded success path) |
| BackupService/RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 993.734ms | 993.734ms | 993.734ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthnService/Login | login | login | mutation | OK | 833.385ms | 868.756ms | 842.375ms | 5 | mutation (seeded success path) |
| AuthnService/ChangePassword | change_password | changePassword | mutation | OK | 827.609ms | 827.609ms | 827.609ms | 5 | mutation (seeded success path) |
| DataBroker/StageCatalog | stage_catalog | stageCatalog | destructive | OK | 430.782ms | 430.782ms | 430.782ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthnService/CreateUser | create_user | createUser | mutation | OK | 430.02ms | 430.02ms | 430.02ms | 5 | mutation (seeded success path) |
| AuthnService/ResetPassword | reset_password | resetPassword | mutation | OK | 428.169ms | 428.169ms | 428.169ms | 5 | mutation (seeded success path) |
| DataBroker/PublishCDC | publish_cdc | publishCdc | mutation | OK | 246.855ms | 246.855ms | 234.386ms | 3 | cdc subscription: time-to-first-event (real mutation produced) |
| ConfigService/GetFlag | get_flag | getFlag | read_only | OK | 112.661ms | 238.893ms | 136.571ms | 25 | read_only (seeded success path) |
| DataBroker/ApplyMigration | apply_migration | applyMigration | mutation | OK | 210.654ms | 210.654ms | 210.654ms | 5 | mutation (seeded success path) |
| TenantService/PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 183.764ms | 183.764ms | 183.764ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthnService/Authenticate | authenticate | authenticate | read_only | OK | 56.134ms | 97.563ms | 63.493ms | 25 | read_only (seeded success path) |
| ConfigService/EvaluateFlags | evaluate_flags | evaluateFlags | read_only | OK | 63.753ms | 95.507ms | 65.917ms | 25 | read_only (seeded success path) |
| ControlPlaneService/DeltaResources | delta_resources | deltaResources | mutation | OK | 84.298ms | 90.948ms | 90.663ms | 5 | streaming: time-to-first-response (seeded; bidi) |
| EmbeddingService/IngestDocument | ingest_document | ingestEmbeddingDocument | mutation | OK | 73.192ms | 76.655ms | 74.247ms | 5 | mutation (seeded success path) |
| AuthnService/RefreshSession | refresh_session | refreshSession | mutation | OK | 55.93ms | 75.815ms | 61.532ms | 5 | mutation (seeded success path) |
| EmbeddingService/IngestDocumentBatch | ingest_document_batch | ingestEmbeddingDocumentBatch | mutation | OK | 69.216ms | 73.722ms | 68.204ms | 5 | mutation (seeded success path) |
| AuthnService/ValidateToken | validate_token | validateToken | read_only | OK | 37.489ms | 73.357ms | 42.954ms | 25 | read_only (seeded success path) |
| ControlPlaneService/StreamResources | stream_resources | streamResources | mutation | OK | 71.778ms | 72.896ms | 73.627ms | 5 | streaming: time-to-first-response (seeded; bidi) |
| ControlPlaneService/RollbackResources | rollback_resources | rollbackResources | mutation | OK | 64.243ms | 70.281ms | 65.051ms | 5 | mutation (seeded success path) |
| AuthnService/IntrospectToken | introspect_token | introspectToken | read_only | OK | 43.812ms | 68.997ms | 46.53ms | 25 | read_only (seeded success path) |
| AuthzService/PromoteCanary | promote_canary | promoteCanary | destructive | OK | 66.022ms | 66.022ms | 66.022ms | 1 | destructive: 1 real call against a seeded disposable target |
| IdentityProviderService/SamlAcs | saml_acs | samlAcs | mutation | OK | 59.468ms | 62.537ms | 63.765ms | 5 | mutation (seeded success path) |
| AuthzService/RollbackPolicyVersion | rollback_policy_version | rollbackPolicyVersion | destructive | OK | 61.933ms | 61.933ms | 61.933ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthzService/ActivatePolicyVersion | activate_policy_version | activatePolicyVersion | destructive | OK | 61.837ms | 61.837ms | 61.837ms | 1 | destructive: 1 real call against a seeded disposable target |

## Full per-RPC table (sorted by service, then name)

| Service | RPC | api_alias | operation_id | kind | err | p50 | p99 | mean | min | max | iters |
|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|
| AnalyticsService | GetExecutorPerformance | get_executor_performance | getExecutorPerformance | read_only | OK | 11.21ms | 13.636ms | 11.195ms | 7.475ms | 14.306ms | 25 |
| AnalyticsService | GetPipelineSummary | get_pipeline_summary | getPipelineSummary | read_only | OK | 11.574ms | 14.94ms | 11.16ms | 8.174ms | 16.011ms | 25 |
| AnalyticsService | GetReconciliationAnalytics | get_reconciliation_analytics | getReconciliationAnalytics | read_only | OK | 9.027ms | 13.102ms | 9.463ms | 6.492ms | 13.449ms | 25 |
| AnalyticsService | GetSlaCompliance | get_sla_compliance | getSlaCompliance | read_only | OK | 13.442ms | 19.072ms | 14.043ms | 9.829ms | 25.347ms | 25 |
| AnalyticsService | GetThroughput | get_throughput | getThroughput | read_only | OK | 9.59ms | 16.757ms | 10.663ms | 6.189ms | 21.051ms | 25 |
| AnalyticsService | RecordPipelineMetric | record_pipeline_metric | recordPipelineMetric | mutation | OK | 12.325ms | 12.618ms | 12.006ms | 10.57ms | 13.172ms | 5 |
| AnalyticsService | TriggerSnapshot | trigger_snapshot | triggerSnapshot | mutation | OK | 14.554ms | 15.289ms | 15.021ms | 14.118ms | 16.919ms | 5 |
| ApiKeyService | CreateApiKey | create_api_key | createApiKey | mutation | OK | 20.695ms | 20.695ms | 20.695ms | 20.695ms | 20.695ms | 5 |
| ApiKeyService | EmergencyRevokeApiKeys | emergency_revoke_api_keys | emergencyRevokeApiKeys | destructive | OK | 17.914ms | 17.914ms | 17.914ms | 17.914ms | 17.914ms | 1 |
| ApiKeyService | GetApiKey | get_api_key | getApiKey | read_only | OK | 6.439ms | 10.827ms | 7.005ms | 4.472ms | 10.967ms | 25 |
| ApiKeyService | GetApiKeyUsageStats | get_api_key_usage_stats | getApiKeyUsageStats | read_only | OK | 6.896ms | 11.573ms | 7.626ms | 4.483ms | 19.514ms | 25 |
| ApiKeyService | ListApiKeys | list_api_keys | listApiKeys | read_only | OK | 8.313ms | 11.8ms | 8.556ms | 4.378ms | 13.629ms | 25 |
| ApiKeyService | RevokeApiKey | revoke_api_key | revokeApiKey | mutation | OK | 14.469ms | 14.469ms | 14.469ms | 14.469ms | 14.469ms | 5 |
| ApiKeyService | RotateApiKey | rotate_api_key | rotateApiKey | mutation | OK | 20.724ms | 20.724ms | 20.724ms | 20.724ms | 20.724ms | 5 |
| ApiKeyService | UpdateApiKey | update_api_key | updateApiKey | mutation | OK | 17.59ms | 19.376ms | 18.871ms | 16.848ms | 23.447ms | 5 |
| ApiKeyService | ValidateApiKey | validate_api_key | validateApiKey | read_only | OK | 19.738ms | 30.378ms | 21.205ms | 13.988ms | 37.367ms | 25 |
| AssetService | CompleteStep | complete_step | completeStep | mutation | OK | 25.662ms | 26.413ms | 25.938ms | 23.681ms | 28.529ms | 5 |
| AssetService | CreatePipelineDefinition | create_pipeline_definition | createPipelineDefinition | mutation | OK | 10.907ms | 10.907ms | 10.907ms | 10.907ms | 10.907ms | 5 |
| AssetService | GetAsset | get_asset | getAsset | read_only | OK | 15.253ms | 22.327ms | 16.469ms | 9.857ms | 26.402ms | 25 |
| AssetService | GetPipeline | get_pipeline | getPipeline | read_only | OK | 13.644ms | 32.317ms | 16.4ms | 10.75ms | 47.777ms | 25 |
| AssetService | GetPipelineDefinition | get_pipeline_definition | getPipelineDefinition | read_only | OK | 15.612ms | 24.012ms | 16.196ms | 10.684ms | 24.987ms | 25 |
| AssetService | ListAssets | list_assets | listAssets | read_only | OK | 24.03ms | 31.008ms | 24.676ms | 16.802ms | 33.728ms | 25 |
| AssetService | RegisterAsset | register_asset | registerAsset | mutation | OK | 19.724ms | 27.868ms | 24.414ms | 16.309ms | 40.252ms | 5 |
| AssetService | StartPipeline | start_pipeline | startPipeline | mutation | OK | 9.648ms | 11.464ms | 17.613ms | 8.924ms | 48.56ms | 5 |
| AuthnService | AdminResetMfa | admin_reset_mfa | adminResetMfa | destructive | OK | 24.442ms | 24.442ms | 24.442ms | 24.442ms | 24.442ms | 1 |
| AuthnService | AdminResetPassword | admin_reset_password | adminResetPassword | destructive | OK | 8.136ms | 8.136ms | 8.136ms | 8.136ms | 8.136ms | 1 |
| AuthnService | AdminRevokeAllTenantSessions | admin_revoke_all_tenant_sessions | adminRevokeAllTenantSessions | destructive | OK | 14.785ms | 14.785ms | 14.785ms | 14.785ms | 14.785ms | 1 |
| AuthnService | AdminRevokeAllUserSessions | admin_revoke_all_user_sessions | adminRevokeAllUserSessions | destructive | OK | 10.859ms | 10.859ms | 10.859ms | 10.859ms | 10.859ms | 1 |
| AuthnService | AdminRevokeSession | admin_revoke_session | adminRevokeSession | destructive | OK | 10.982ms | 10.982ms | 10.982ms | 10.982ms | 10.982ms | 1 |
| AuthnService | Authenticate | authenticate | authenticate | read_only | OK | 56.134ms | 97.563ms | 63.493ms | 44.132ms | 108.058ms | 25 |
| AuthnService | ChangePassword | change_password | changePassword | mutation | OK | 827.609ms | 827.609ms | 827.609ms | 827.609ms | 827.609ms | 5 |
| AuthnService | ChangeUserStatus | change_user_status | changeUserStatus | destructive | OK | 13.878ms | 13.878ms | 13.878ms | 13.878ms | 13.878ms | 1 |
| AuthnService | ConfirmMFAEnrollment | confirm_mfaenrollment | confirmMfaenrollment | mutation | OK | 3.926ms | 3.947ms | 3.961ms | 3.725ms | 4.376ms | 5 |
| AuthnService | CreateCertificateBinding | create_certificate_binding | createCertificateBinding | mutation | OK | 18.72ms | 18.72ms | 18.72ms | 18.72ms | 18.72ms | 5 |
| AuthnService | CreateServiceAccountGrant | create_service_account_grant | createServiceAccountGrant | mutation | OK | 13.593ms | 13.593ms | 13.593ms | 13.593ms | 13.593ms | 5 |
| AuthnService | CreateSession | create_session | createSession | mutation | OK | 6.659ms | 7.671ms | 6.824ms | 6.007ms | 7.717ms | 5 |
| AuthnService | CreateUser | create_user | createUser | mutation | OK | 430.02ms | 430.02ms | 430.02ms | 430.02ms | 430.02ms | 5 |
| AuthnService | DeleteWebAuthnCredential | delete_web_authn_credential | deleteWebAuthnCredential | mutation | OK | 9.959ms | 10.116ms | 9.844ms | 7.643ms | 11.77ms | 5 |
| AuthnService | DisableMfaFactor | disable_mfa_factor | disableMfaFactor | mutation | OK | 14.462ms | 14.622ms | 14.048ms | 12.666ms | 15.223ms | 5 |
| AuthnService | EmergencyRevoke | emergency_revoke | emergencyRevoke | destructive | OK | 10.971ms | 10.971ms | 10.971ms | 10.971ms | 10.971ms | 1 |
| AuthnService | EnrollMFA | enroll_mfa | enrollMfa | mutation | OK | 13.989ms | 14.204ms | 13.365ms | 11.479ms | 14.528ms | 5 |
| AuthnService | FinishWebAuthnAuthentication | finish_web_authn_authentication | finishWebAuthnAuthentication | mutation | NO-BODY | 0s | 0s | 0s | 0s | 0s | 5 |
| AuthnService | FinishWebAuthnRegistration | finish_web_authn_registration | finishWebAuthnRegistration | mutation | NO-BODY | 0s | 0s | 101µs | 0s | 507µs | 5 |
| AuthnService | ForgotPassword | forgot_password | forgotPassword | mutation | OK | 17.606ms | 18.748ms | 17.031ms | 13.936ms | 18.887ms | 5 |
| AuthnService | GenerateRecoveryCodes | generate_recovery_codes | generateRecoveryCodes | mutation | OK | 29.365ms | 29.434ms | 29.043ms | 25.285ms | 32.52ms | 5 |
| AuthnService | GetJwks | get_jwks | getJwks | read_only | OK | 5.831ms | 9.064ms | 6.235ms | 3.759ms | 12.235ms | 25 |
| AuthnService | GetMfaPolicy | get_mfa_policy | getMfaPolicy | read_only | OK | 6.678ms | 15.601ms | 8.096ms | 4.441ms | 21.325ms | 25 |
| AuthnService | GetServiceAccountGrant | get_service_account_grant | getServiceAccountGrant | read_only | OK | 6.42ms | 10.618ms | 6.675ms | 4.411ms | 10.856ms | 25 |
| AuthnService | GetSession | get_session | getSession | read_only | OK | 6.985ms | 12.99ms | 7.483ms | 4.301ms | 13.063ms | 25 |
| AuthnService | GetUser | get_user | getUser | read_only | OK | 5.353ms | 8.9ms | 5.987ms | 4.28ms | 11.009ms | 25 |
| AuthnService | IntrospectToken | introspect_token | introspectToken | read_only | OK | 43.812ms | 68.997ms | 46.53ms | 29.081ms | 99.728ms | 25 |
| AuthnService | IssueMfaChallenge | issue_mfa_challenge | issueMfaChallenge | mutation | OK | 10.979ms | 11.484ms | 11.373ms | 10.896ms | 12.571ms | 5 |
| AuthnService | ListCertificateBindings | list_certificate_bindings | listCertificateBindings | read_only | OK | 8.058ms | 15.092ms | 8.83ms | 4.034ms | 17.779ms | 25 |
| AuthnService | ListDevices | list_devices | listDevices | read_only | OK | 7.074ms | 15.48ms | 8.457ms | 3.98ms | 20.612ms | 25 |
| AuthnService | ListMfaFactors | list_mfa_factors | listMfaFactors | read_only | OK | 10.494ms | 20.349ms | 11.441ms | 6.001ms | 22.946ms | 25 |
| AuthnService | ListServiceAccountGrants | list_service_account_grants | listServiceAccountGrants | read_only | OK | 8.928ms | 24.985ms | 11.935ms | 5.165ms | 29.19ms | 25 |
| AuthnService | ListSessions | list_sessions | listSessions | read_only | OK | 16.248ms | 25.955ms | 16.168ms | 9.399ms | 26.973ms | 25 |
| AuthnService | ListUsers | list_users | listUsers | read_only | OK | 17.875ms | 28.865ms | 19.125ms | 9.943ms | 48.738ms | 25 |
| AuthnService | ListWebAuthnCredentials | list_web_authn_credentials | listWebAuthnCredentials | read_only | OK | 10.639ms | 16.987ms | 11.545ms | 6.871ms | 28.625ms | 25 |
| AuthnService | Login | login | login | mutation | OK | 833.385ms | 868.756ms | 842.375ms | 741.66ms | 997.331ms | 5 |
| AuthnService | Logout | logout | logout | mutation | OK | 4.914ms | 5.553ms | 5.254ms | 4.866ms | 6.039ms | 5 |
| AuthnService | PutMfaPolicy | put_mfa_policy | putMfaPolicy | mutation | OK | 6.545ms | 6.616ms | 6.831ms | 5.722ms | 9.375ms | 5 |
| AuthnService | RefreshSession | refresh_session | refreshSession | mutation | OK | 55.93ms | 75.815ms | 61.532ms | 28.196ms | 92.493ms | 5 |
| AuthnService | RefreshToken | refresh_token | refreshToken | mutation | OK | 11.621ms | 11.621ms | 11.621ms | 11.621ms | 11.621ms | 5 |
| AuthnService | RenamePasskey | rename_passkey | renamePasskey | mutation | OK | 8.384ms | 8.807ms | 8.632ms | 7.627ms | 10.169ms | 5 |
| AuthnService | ReplaceServiceAccountGrant | replace_service_account_grant | replaceServiceAccountGrant | mutation | OK | 11.467ms | 11.467ms | 11.467ms | 11.467ms | 11.467ms | 5 |
| AuthnService | ResendOTP | resend_otp | resendOtp | mutation | OK | 15.85ms | 15.863ms | 15.86ms | 12.504ms | 21.164ms | 5 |
| AuthnService | ResetPassword | reset_password | resetPassword | mutation | OK | 428.169ms | 428.169ms | 428.169ms | 428.169ms | 428.169ms | 5 |
| AuthnService | RevokeCertificateBinding | revoke_certificate_binding | revokeCertificateBinding | destructive | OK | 12.382ms | 12.382ms | 12.382ms | 12.382ms | 12.382ms | 1 |
| AuthnService | RevokeDevice | revoke_device | revokeDevice | mutation | OK | 11.927ms | 11.927ms | 11.927ms | 11.927ms | 11.927ms | 5 |
| AuthnService | RevokeRecoveryCodes | revoke_recovery_codes | revokeRecoveryCodes | mutation | OK | 8.962ms | 9.383ms | 8.857ms | 7.605ms | 9.674ms | 5 |
| AuthnService | RevokeServiceAccountGrant | revoke_service_account_grant | revokeServiceAccountGrant | destructive | OK | 11.292ms | 11.292ms | 11.292ms | 11.292ms | 11.292ms | 1 |
| AuthnService | RevokeSession | revoke_session | revokeSession | mutation | OK | 6.77ms | 7.748ms | 7.938ms | 5.761ms | 13.257ms | 5 |
| AuthnService | RotateServiceAccountIdentity | rotate_service_account_identity | rotateServiceAccountIdentity | destructive | OK | 16.323ms | 16.323ms | 16.323ms | 16.323ms | 16.323ms | 1 |
| AuthnService | SendOTP | send_otp | sendOtp | mutation | OK | 12.781ms | 14.022ms | 13.232ms | 12.122ms | 14.727ms | 5 |
| AuthnService | SendPhoneVerification | send_phone_verification | sendPhoneVerification | mutation | OK | 13.242ms | 15.179ms | 14.632ms | 10.216ms | 21.495ms | 5 |
| AuthnService | StartWebAuthnAuthentication | start_web_authn_authentication | startWebAuthnAuthentication | mutation | FailedPrecondition | 1.585ms | 1.617ms | 1.39ms | 1.058ms | 1.622ms | 5 |
| AuthnService | StartWebAuthnRegistration | start_web_authn_registration | startWebAuthnRegistration | mutation | FailedPrecondition | 1.593ms | 1.619ms | 1.611ms | 1.568ms | 1.686ms | 5 |
| AuthnService | UpdateUser | update_user | updateUser | mutation | OK | 9.483ms | 9.492ms | 10.047ms | 8.144ms | 14.506ms | 5 |
| AuthnService | ValidateCSRF | validate_csrf | validateCsrf | read_only | OK | 8.234ms | 13.392ms | 8.99ms | 4.984ms | 17.852ms | 25 |
| AuthnService | ValidateToken | validate_token | validateToken | read_only | OK | 37.489ms | 73.357ms | 42.954ms | 21.58ms | 98.987ms | 25 |
| AuthnService | VerifyMfaChallenge | verify_mfa_challenge | verifyMfaChallenge | read_only | OK | 12.891ms | 25.777ms | 14.325ms | 9.286ms | 25.786ms | 25 |
| AuthnService | VerifyOTP | verify_otp | verifyOtp | read_only | OK | 19.469ms | 42.929ms | 23.54ms | 13.125ms | 51.78ms | 25 |
| AuthzService | ActivateCanary | activate_canary | activateCanary | destructive | OK | 30.38ms | 30.38ms | 30.38ms | 30.38ms | 30.38ms | 1 |
| AuthzService | ActivatePolicyVersion | activate_policy_version | activatePolicyVersion | destructive | OK | 61.837ms | 61.837ms | 61.837ms | 61.837ms | 61.837ms | 1 |
| AuthzService | ApprovePolicyDraft | approve_policy_draft | approvePolicyDraft | mutation | OK | 35.823ms | 35.823ms | 35.823ms | 35.823ms | 35.823ms | 5 |
| AuthzService | AssignRole | assign_role | assignRole | mutation | OK | 23.719ms | 24.068ms | 23.541ms | 20.764ms | 25.686ms | 5 |
| AuthzService | Authorize | authorize | authorize | read_only | OK | 19.895ms | 22.572ms | 20.019ms | 15.829ms | 26.114ms | 25 |
| AuthzService | BatchCheckPermissions | batch_check_permissions | batchCheckPermissions | read_only | OK | 9.649ms | 13.556ms | 9.862ms | 7.065ms | 15.425ms | 25 |
| AuthzService | CheckAccess | check_access | checkAccess | read_only | OK | 9.07ms | 10.184ms | 8.892ms | 7.553ms | 10.604ms | 25 |
| AuthzService | CreatePolicyDraft | create_policy_draft | createPolicyDraft | mutation | OK | 35.01ms | 37.021ms | 37.078ms | 34.518ms | 43.907ms | 5 |
| AuthzService | CreatePolicyRule | create_policy_rule | createPolicyRule | mutation | OK | 24.871ms | 25.836ms | 26.573ms | 21.179ms | 37.496ms | 5 |
| AuthzService | CreateRole | create_role | createRole | mutation | OK | 33.261ms | 33.261ms | 33.261ms | 33.261ms | 33.261ms | 5 |
| AuthzService | DeletePolicyRule | delete_policy_rule | deletePolicyRule | mutation | OK | 11.64ms | 12.151ms | 11.118ms | 8.321ms | 13.796ms | 5 |
| AuthzService | DeleteRole | delete_role | deleteRole | mutation | OK | 13.582ms | 31.164ms | 21.151ms | 12.298ms | 35.916ms | 5 |
| AuthzService | DiffPolicyDraft | diff_policy_draft | diffPolicyDraft | read_only | OK | 12.016ms | 14.151ms | 12.153ms | 9.28ms | 14.365ms | 25 |
| AuthzService | ExplainPolicy | explain_policy | explainPolicy | read_only | OK | 7.603ms | 10.311ms | 7.663ms | 4.229ms | 10.431ms | 25 |
| AuthzService | GetAuthzRevision | get_authz_revision | getAuthzRevision | read_only | OK | 4.949ms | 8.282ms | 5.439ms | 2.622ms | 18.672ms | 25 |
| AuthzService | GetCanaryStatus | get_canary_status | getCanaryStatus | read_only | OK | 9.887ms | 12.187ms | 10.08ms | 7.707ms | 12.623ms | 25 |
| AuthzService | GetNativeAccess | get_native_access | getNativeAccess | read_only | OK | 17.098ms | 22.914ms | 17.955ms | 14.603ms | 23.68ms | 25 |
| AuthzService | GetPolicyBundle | get_policy_bundle | getPolicyBundle | read_only | OK | 7.479ms | 11.453ms | 8.083ms | 6.527ms | 14.226ms | 25 |
| AuthzService | GetPolicyRule | get_policy_rule | getPolicyRule | read_only | OK | 4.894ms | 7.02ms | 5.028ms | 3.344ms | 7.138ms | 25 |
| AuthzService | GetRole | get_role | getRole | read_only | OK | 4.81ms | 7.584ms | 4.655ms | 510µs | 8.339ms | 25 |
| AuthzService | InvalidatePolicyBundles | invalidate_policy_bundles | invalidatePolicyBundles | destructive | OK | 25.467ms | 25.467ms | 25.467ms | 25.467ms | 25.467ms | 1 |
| AuthzService | LintAuthzPolicies | lint_authz_policies | lintAuthzPolicies | read_only | OK | 1.834ms | 2.781ms | 1.926ms | 720µs | 3.15ms | 25 |
| AuthzService | ListAccessDecisionAudits | list_access_decision_audits | listAccessDecisionAudits | read_only | OK | 11.826ms | 17.192ms | 12.723ms | 10.175ms | 24.088ms | 25 |
| AuthzService | ListPolicyRules | list_policy_rules | listPolicyRules | read_only | OK | 5.489ms | 6.596ms | 5.268ms | 3.236ms | 8.673ms | 25 |
| AuthzService | ListPolicyVersions | list_policy_versions | listPolicyVersions | read_only | OK | 10.845ms | 14.746ms | 10.979ms | 8.239ms | 17.01ms | 25 |
| AuthzService | ListRoles | list_roles | listRoles | read_only | OK | 5.542ms | 8.194ms | 5.689ms | 3.776ms | 8.776ms | 25 |
| AuthzService | ListUserPermissions | list_user_permissions | listUserPermissions | read_only | OK | 2.142ms | 2.696ms | 2.124ms | 1.519ms | 2.915ms | 25 |
| AuthzService | ListUserRoles | list_user_roles | listUserRoles | read_only | OK | 5.37ms | 8.523ms | 5.214ms | 0s | 11.953ms | 25 |
| AuthzService | MigrateLegacyPolicies | migrate_legacy_policies | migrateLegacyPolicies | destructive | OK | 25.803ms | 25.803ms | 25.803ms | 25.803ms | 25.803ms | 1 |
| AuthzService | PromoteCanary | promote_canary | promoteCanary | destructive | OK | 66.022ms | 66.022ms | 66.022ms | 66.022ms | 66.022ms | 1 |
| AuthzService | PutAuthzPolicy | put_authz_policy | putAuthzPolicy | mutation | OK | 23.181ms | 25.865ms | 24.2ms | 21.042ms | 28.273ms | 5 |
| AuthzService | PutRelationship | put_relationship | putRelationship | mutation | OK | 28.627ms | 28.895ms | 29.039ms | 26.78ms | 32.703ms | 5 |
| AuthzService | PutRoleBinding | put_role_binding | putRoleBinding | mutation | OK | 18.825ms | 19.688ms | 19.104ms | 16.713ms | 21.893ms | 5 |
| AuthzService | RejectPolicyDraft | reject_policy_draft | rejectPolicyDraft | mutation | OK | 31.862ms | 31.862ms | 31.862ms | 31.862ms | 31.862ms | 5 |
| AuthzService | RevokeRole | revoke_role | revokeRole | mutation | OK | 8.287ms | 8.778ms | 10.699ms | 7.512ms | 21.376ms | 5 |
| AuthzService | RollbackPolicyVersion | rollback_policy_version | rollbackPolicyVersion | destructive | OK | 61.933ms | 61.933ms | 61.933ms | 61.933ms | 61.933ms | 1 |
| AuthzService | SeedBuiltinRoles | seed_builtin_roles | seedBuiltinRoles | mutation | OK | 50.58ms | 52.386ms | 50.33ms | 45.044ms | 53.609ms | 5 |
| AuthzService | SimulatePolicy | simulate_policy | simulatePolicy | mutation | OK | 15.653ms | 16.748ms | 18.357ms | 13.602ms | 30.571ms | 5 |
| AuthzService | SubmitPolicyDraft | submit_policy_draft | submitPolicyDraft | mutation | OK | 22.033ms | 22.033ms | 22.033ms | 22.033ms | 22.033ms | 5 |
| AuthzService | UpdatePolicyDraft | update_policy_draft | updatePolicyDraft | mutation | OK | 28.003ms | 28.95ms | 28.161ms | 26.236ms | 29.651ms | 5 |
| AuthzService | UpdateRole | update_role | updateRole | mutation | OK | 23.754ms | 25.455ms | 25.005ms | 17.714ms | 37.73ms | 5 |
| BackupService | DeleteBackupPolicy | delete_backup_policy | deleteBackupPolicy | mutation | OK | 13.967ms | 14.003ms | 13.851ms | 12.023ms | 16.7ms | 5 |
| BackupService | GetBackup | get_backup | getBackup | read_only | OK | 21.301ms | 25.648ms | 21.648ms | 17.853ms | 32.37ms | 25 |
| BackupService | GetBackupPolicy | get_backup_policy | getBackupPolicy | read_only | OK | 12.02ms | 14.051ms | 12.132ms | 9.797ms | 17.418ms | 25 |
| BackupService | ListBackupPolicies | list_backup_policies | listBackupPolicies | read_only | OK | 11.262ms | 13.116ms | 11.353ms | 9.422ms | 15.453ms | 25 |
| BackupService | ListBackups | list_backups | listBackups | read_only | OK | 11.09ms | 13.429ms | 11.394ms | 9.438ms | 14.199ms | 25 |
| BackupService | PutBackupPolicy | put_backup_policy | putBackupPolicy | mutation | OK | 20.32ms | 20.966ms | 21.372ms | 18.914ms | 26.575ms | 5 |
| BackupService | RestoreTenant | restore_tenant | restoreTenant | destructive | OK | 993.734ms | 993.734ms | 993.734ms | 993.734ms | 993.734ms | 1 |
| BackupService | StartTenantBackup | start_tenant_backup | startTenantBackup | mutation | OK | 1.115287s | 1.169338s | 1.13336s | 1.095407s | 1.186284s | 5 |
| CacheService | CreateNamespace | create_cache_namespace | createCacheNamespace | mutation | OK | 13.027ms | 16.695ms | 14.578ms | 11.527ms | 19.384ms | 5 |
| CacheService | Delete | cache_delete | cacheNamespaceDelete | mutation | OK | 10.3ms | 10.757ms | 10.372ms | 9.599ms | 11.128ms | 5 |
| CacheService | DeleteNamespace | delete_cache_namespace | deleteCacheNamespace | destructive | OK | 20.056ms | 20.056ms | 20.056ms | 20.056ms | 20.056ms | 1 |
| CacheService | Get | cache_get | cacheNamespaceGet | read_only | OK | 9.01ms | 13.774ms | 10.024ms | 7.673ms | 21.731ms | 25 |
| CacheService | GetNamespaceStats | get_cache_namespace_stats | getCacheNamespaceStats | read_only | OK | 12.486ms | 14.637ms | 12.477ms | 10.175ms | 14.866ms | 25 |
| CacheService | Scan | cache_scan | cacheNamespaceScan | read_only | OK | 7.565ms | 40.393ms | 15.388ms | 5.983ms | 41.675ms | 25 |
| CacheService | Set | cache_set | cacheNamespaceSet | mutation | OK | 16.626ms | 16.771ms | 16.154ms | 13.663ms | 17.754ms | 5 |
| ConfigService | DeleteFlag | delete_flag | deleteFlag | destructive | OK | 19.723ms | 19.723ms | 19.723ms | 19.723ms | 19.723ms | 1 |
| ConfigService | EvaluateFlags | evaluate_flags | evaluateFlags | read_only | OK | 63.753ms | 95.507ms | 65.917ms | 39.268ms | 110.167ms | 25 |
| ConfigService | GetFlag | get_flag | getFlag | read_only | OK | 112.661ms | 238.893ms | 136.571ms | 36.114ms | 585.219ms | 25 |
| ConfigService | ListFlags | list_flags | listFlags | read_only | OK | 28.318ms | 48.212ms | 30.121ms | 16.329ms | 65.318ms | 25 |
| ConfigService | PutFlag | put_flag | putFlag | mutation | OK | 24.245ms | 24.971ms | 23.992ms | 21.021ms | 25.504ms | 5 |
| ControlPlaneService | AckStatus | ack_status | ackStatus | mutation | OK | 8.22ms | 9.347ms | 7.843ms | 5.87ms | 9.35ms | 5 |
| ControlPlaneService | DeltaResources | delta_resources | deltaResources | mutation | OK | 84.298ms | 90.948ms | 90.663ms | 82.934ms | 111.807ms | 5 |
| ControlPlaneService | GetResources | get_resources | getResources | read_only | OK | 6.425ms | 15.347ms | 7.598ms | 4.472ms | 15.701ms | 25 |
| ControlPlaneService | ListNodeStates | list_node_states | listNodeStates | read_only | OK | 36.905ms | 52.769ms | 38.654ms | 26.96ms | 65.773ms | 25 |
| ControlPlaneService | RollbackResources | rollback_resources | rollbackResources | mutation | OK | 64.243ms | 70.281ms | 65.051ms | 58.375ms | 71.046ms | 5 |
| ControlPlaneService | StreamResources | stream_resources | streamResources | mutation | OK | 71.778ms | 72.896ms | 73.627ms | 66.385ms | 88.748ms | 5 |
| DataBroker | ActivateCatalog | activate_catalog | activateCatalog | destructive | OK | 5.481ms | 5.481ms | 5.481ms | 5.481ms | 5.481ms | 1 |
| DataBroker | AnalyticalQuery | analytical_query | analyticalQuery | read_only | OK | 8.522ms | 10.869ms | 8.46ms | 6.029ms | 11.006ms | 25 |
| DataBroker | ApplyMigration | apply_migration | applyMigration | mutation | OK | 210.654ms | 210.654ms | 210.654ms | 210.654ms | 210.654ms | 5 |
| DataBroker | ApproveMigrationPlan | approve_migration_plan | approveMigrationPlan | mutation | OK | 26.942ms | 26.942ms | 26.942ms | 26.942ms | 26.942ms | 1 |
| DataBroker | BatchSelect | batch_select | batchSelect | mutation | OK | 10.457ms | 12.165ms | 11.027ms | 9.274ms | 13.52ms | 5 |
| DataBroker | BatchUpsert | batch_upsert | batchUpsert | mutation | OK | 32.857ms | 34.81ms | 32.079ms | 25.759ms | 34.885ms | 5 |
| DataBroker | BeginTx | begin_tx | beginTx | mutation | OK | 17.901ms | 22.131ms | 21.887ms | 17.661ms | 33.925ms | 5 |
| DataBroker | CacheDelete | cache_delete | cacheDelete | mutation | OK | 6.094ms | 6.528ms | 6.028ms | 4.792ms | 7.662ms | 5 |
| DataBroker | CacheGet | cache_get | cacheGet | read_only | OK | 7.059ms | 8.798ms | 7.158ms | 5.092ms | 10.015ms | 25 |
| DataBroker | CacheScan | cache_scan | cacheScan | read_only | OK | 9.609ms | 11.543ms | 10.083ms | 7.574ms | 20.937ms | 25 |
| DataBroker | CacheSet | cache_set | cacheSet | mutation | OK | 5.648ms | 6.019ms | 5.91ms | 5.381ms | 7.097ms | 5 |
| DataBroker | CreateMaterializedView | create_materialized_view | createMaterializedView | mutation | OK | 6.499ms | 7.143ms | 6.397ms | 5.427ms | 7.446ms | 5 |
| DataBroker | Delete | delete | delete | mutation | OK | 23.594ms | 24.028ms | 26.549ms | 20.23ms | 44.546ms | 5 |
| DataBroker | DeletePolicy | delete_policy | deletePolicy | mutation | OK | 14.14ms | 14.14ms | 14.14ms | 14.14ms | 14.14ms | 5 |
| DataBroker | DismissDlqEvent | dismiss_dlq_event | dismissDlqEvent | mutation | OK | 15.017ms | 17.565ms | 16.183ms | 13.187ms | 21.009ms | 5 |
| DataBroker | DocumentDelete | document_delete | documentDelete | mutation | OK | 6.529ms | 6.554ms | 7.011ms | 5.435ms | 10.409ms | 5 |
| DataBroker | DocumentFind | document_find | documentFind | read_only | OK | 5.42ms | 8.776ms | 5.584ms | 523µs | 10.3ms | 25 |
| DataBroker | DocumentGet | document_get | documentGet | read_only | OK | 6.075ms | 7.895ms | 6.388ms | 4.966ms | 8.012ms | 25 |
| DataBroker | DocumentUpsert | document_upsert | documentUpsert | mutation | OK | 5.967ms | 6.667ms | 6.386ms | 4.981ms | 8.952ms | 5 |
| DataBroker | DropResource | drop_resource | dropResource | destructive | OK | 19.685ms | 19.685ms | 19.685ms | 19.685ms | 19.685ms | 1 |
| DataBroker | EnqueueOutboxEvent | enqueue_outbox_event | enqueueOutboxEvent | mutation | OK | 16.089ms | 16.089ms | 16.089ms | 16.089ms | 16.089ms | 5 |
| DataBroker | EnsureBaseline | ensure_baseline | ensureBaseline | mutation | OK | 17.457ms | 19.368ms | 18.332ms | 15.868ms | 22.701ms | 5 |
| DataBroker | EnsureProject | ensure_project | ensureProject | mutation | OK | 14.858ms | 19.123ms | 16.414ms | 13.211ms | 20.335ms | 5 |
| DataBroker | EnsureResource | ensure_resource | ensureResource | mutation | OK | 17.611ms | 21.231ms | 18.521ms | 13.633ms | 23.739ms | 5 |
| DataBroker | GeneratePresignedUrl | generate_presigned_url | generatePresignedUrl | mutation | OK | 4.115ms | 4.906ms | 4.25ms | 3.305ms | 4.969ms | 5 |
| DataBroker | GenericDispatch | generic_dispatch | genericDispatch | mutation | OK | 3.854ms | 4.663ms | 4.204ms | 3.722ms | 4.943ms | 5 |
| DataBroker | GetAdminSummary | get_admin_summary | getAdminSummary | read_only | OK | 25.666ms | 40.294ms | 27.501ms | 19.19ms | 53.012ms | 25 |
| DataBroker | GetCapabilities | get_capabilities | getCapabilities | read_only | OK | 6.486ms | 8.57ms | 6.624ms | 4.806ms | 8.867ms | 25 |
| DataBroker | GetCatalogManifest | get_catalog_manifest | getCatalogManifest | read_only | OK | 13.298ms | 19.05ms | 13.858ms | 9.823ms | 22.757ms | 25 |
| DataBroker | GetCatalogVersion | get_catalog_version | getCatalogVersion | read_only | OK | 4.406ms | 6.044ms | 4.889ms | 3.205ms | 15.798ms | 25 |
| DataBroker | GetCatalogVersions | get_catalog_versions | getCatalogVersions | read_only | OK | 3.95ms | 6.389ms | 4.426ms | 3.225ms | 9.615ms | 25 |
| DataBroker | GetCdcStatus | get_cdc_status | getCdcStatus | read_only | OK | 4.978ms | 11.992ms | 5.807ms | 3.24ms | 13.635ms | 25 |
| DataBroker | GetDlqEvent | get_dlq_event | getDlqEvent | read_only | OK | 4.885ms | 6.87ms | 4.711ms | 514µs | 8.126ms | 25 |
| DataBroker | GetHealthReport | get_health_report | getHealthReport | read_only | OK | 2.795ms | 3.407ms | 2.899ms | 2.03ms | 3.987ms | 25 |
| DataBroker | GetMigrationStatus | get_migration_status | getMigrationStatus | read_only | OK | 4.995ms | 6.411ms | 5.042ms | 3.291ms | 7.14ms | 25 |
| DataBroker | GetObject | get_object | getObject | read_only | OK | 8.02ms | 10.444ms | 8.219ms | 6.318ms | 11.411ms | 25 |
| DataBroker | GetSaga | get_saga | getSaga | read_only | OK | 4.872ms | 6.461ms | 4.872ms | 3.803ms | 7.043ms | 25 |
| DataBroker | GraphMutate | graph_mutate | graphMutate | mutation | OK | 18.605ms | 19.694ms | 24.406ms | 17.56ms | 47.828ms | 5 |
| DataBroker | GraphQuery | graph_query | graphQuery | read_only | OK | 19.587ms | 33.222ms | 21.324ms | 12.193ms | 38.057ms | 25 |
| DataBroker | InitiateMultipartUpload | initiate_multipart_upload | initiateMultipartUpload | mutation | OK | 9.653ms | 10.107ms | 9.737ms | 8.748ms | 10.966ms | 5 |
| DataBroker | LintPolicies | lint_policies | lintPolicies | read_only | OK | 5.884ms | 8.15ms | 6.05ms | 3.745ms | 9.479ms | 25 |
| DataBroker | ListAdminAuditLogs | list_admin_audit_logs | listAdminAuditLogs | read_only | OK | 6.158ms | 9.703ms | 6.48ms | 506µs | 12.532ms | 25 |
| DataBroker | ListDlqEvents | list_dlq_events | listDlqEvents | read_only | OK | 5.208ms | 6.518ms | 5.403ms | 4.009ms | 7.241ms | 25 |
| DataBroker | ListMessageSchemas | list_message_schemas | listMessageSchemas | read_only | OK | 2.195ms | 2.765ms | 2.31ms | 1.6ms | 3.979ms | 25 |
| DataBroker | ListMigrationRuns | list_migration_runs | listMigrationRuns | read_only | OK | 4.892ms | 6.667ms | 5.017ms | 3.731ms | 8.213ms | 25 |
| DataBroker | ListPolicies | list_policies | listPolicies | read_only | OK | 5.371ms | 7.616ms | 5.64ms | 3.716ms | 8.597ms | 25 |
| DataBroker | ListProjects | list_projects | listProjects | read_only | OK | 5.366ms | 6.51ms | 5.262ms | 3.737ms | 7.066ms | 25 |
| DataBroker | ListResources | list_resources | listResources | read_only | OK | 4.488ms | 5.85ms | 4.761ms | 3.311ms | 6.038ms | 25 |
| DataBroker | ListSagas | list_sagas | listSagas | read_only | OK | 4.845ms | 8.381ms | 5.704ms | 3.247ms | 21.001ms | 25 |
| DataBroker | LookupMessageSchema | lookup_message_schema | lookupMessageSchema | read_only | OK | 2.176ms | 3.365ms | 2.349ms | 1.596ms | 4.033ms | 25 |
| DataBroker | MarkSagaReviewed | mark_saga_reviewed | markSagaReviewed | mutation | OK | 14.058ms | 14.455ms | 13.702ms | 10.424ms | 16.411ms | 5 |
| DataBroker | PauseCdc | pause_cdc | pauseCdc | mutation | OK | 12.119ms | 13.025ms | 12.787ms | 10.836ms | 16.442ms | 5 |
| DataBroker | PlanMigration | plan_migration | planMigration | mutation | OK | 21.3ms | 21.634ms | 19.011ms | 14.485ms | 22.44ms | 5 |
| DataBroker | PreviewCdcRedaction | preview_cdc_redaction | previewCdcRedaction | read_only | OK | 12.17ms | 15.593ms | 12.29ms | 8.702ms | 22.657ms | 25 |
| DataBroker | PublishCDC | publish_cdc | publishCdc | mutation | OK | 246.855ms | 246.855ms | 234.386ms | 196.065ms | 260.238ms | 3 |
| DataBroker | PutObject | put_object | putObject | mutation | OK | 18.897ms | 18.913ms | 18.35ms | 16.698ms | 19.197ms | 5 |
| DataBroker | PutPolicy | put_policy | putPolicy | destructive | OK | 13.661ms | 13.661ms | 13.661ms | 13.661ms | 13.661ms | 1 |
| DataBroker | QuarantineDlqEvent | quarantine_dlq_event | quarantineDlqEvent | mutation | OK | 13.108ms | 14.458ms | 13.211ms | 11.365ms | 15.704ms | 5 |
| DataBroker | ReloadPolicies | reload_policies | reloadPolicies | destructive | OK | 8.127ms | 8.127ms | 8.127ms | 8.127ms | 8.127ms | 1 |
| DataBroker | ReplayDlqEvent | replay_dlq_event | replayDlqEvent | mutation | OK | 18.386ms | 18.386ms | 18.386ms | 18.386ms | 18.386ms | 5 |
| DataBroker | ResumeCdc | resume_cdc | resumeCdc | mutation | OK | 13.684ms | 14.912ms | 13.929ms | 12.948ms | 14.937ms | 5 |
| DataBroker | RetrySagaCompensation | retry_saga_compensation | retrySagaCompensation | mutation | OK | 14.916ms | 14.916ms | 14.916ms | 14.916ms | 14.916ms | 5 |
| DataBroker | RollbackCatalog | rollback_catalog | rollbackCatalog | destructive | OK | 4.266ms | 4.266ms | 4.266ms | 4.266ms | 4.266ms | 1 |
| DataBroker | ScanProjectionDrift | scan_projection_drift | scanProjectionDrift | read_only | OK | 13.625ms | 16.064ms | 13.6ms | 10.38ms | 18.244ms | 25 |
| DataBroker | Select | select | select | read_only | OK | 9.885ms | 17.664ms | 10.871ms | 6.088ms | 17.841ms | 25 |
| DataBroker | SelectV2 | select_v_2 | selectV2 | read_only | OK | 10.19ms | 14.094ms | 10.3ms | 7.506ms | 14.568ms | 25 |
| DataBroker | StageCatalog | stage_catalog | stageCatalog | destructive | OK | 430.782ms | 430.782ms | 430.782ms | 430.782ms | 430.782ms | 1 |
| DataBroker | StepDownCdcLeader | step_down_cdc_leader | stepDownCdcLeader | mutation | OK | 11.487ms | 13.669ms | 12.528ms | 10.905ms | 15.188ms | 5 |
| DataBroker | TimeSeriesQuery | time_series_query | timeSeriesQuery | read_only | OK | 8.279ms | 12.922ms | 8.675ms | 6.212ms | 12.96ms | 25 |
| DataBroker | TimeSeriesWrite | time_series_write | timeSeriesWrite | mutation | OK | 8.248ms | 8.338ms | 8.906ms | 7.028ms | 13.283ms | 5 |
| DataBroker | Update | update | update | mutation | OK | 27.483ms | 28.902ms | 26.846ms | 21.799ms | 32.586ms | 5 |
| DataBroker | Upsert | upsert | upsert | mutation | OK | 25.652ms | 25.702ms | 26.914ms | 23.431ms | 34.313ms | 5 |
| DataBroker | ValidateCatalog | validate_catalog | validateCatalog | destructive | OK | 54.185ms | 54.185ms | 54.185ms | 54.185ms | 54.185ms | 1 |
| DataBroker | VectorBatchUpsert | vector_batch_upsert | vectorBatchUpsert | mutation | OK | 6.578ms | 8.595ms | 7.396ms | 5.402ms | 10.42ms | 5 |
| DataBroker | VectorHybridSearch | vector_hybrid_search | vectorHybridSearch | read_only | OK | 6.28ms | 8.275ms | 6.484ms | 5.498ms | 8.741ms | 25 |
| DataBroker | VectorSearch | vector_search | vectorSearch | read_only | OK | 5.955ms | 7.846ms | 6.074ms | 4.927ms | 8.236ms | 25 |
| DataBroker | VectorUpsert | vector_upsert | vectorUpsert | mutation | OK | 9.912ms | 13.013ms | 11.715ms | 9.248ms | 16.558ms | 5 |
| DataBroker | VerifyAdminAuditLog | verify_admin_audit_log | verifyAdminAuditLog | read_only | OK | 9.194ms | 12.963ms | 9.478ms | 5.446ms | 13.747ms | 25 |
| EmbeddingService | Backfill | backfill | backfillEmbeddingSource | mutation | OK | 21.38ms | 22.876ms | 21.905ms | 20.392ms | 24.04ms | 5 |
| EmbeddingService | CutoverModelAlias | cutover_model_alias | cutoverEmbeddingModelAlias | mutation | OK | 29.578ms | 33.54ms | 31.239ms | 25.138ms | 39.737ms | 5 |
| EmbeddingService | DeleteModel | delete_model | deleteEmbeddingModel | destructive | OK | 9.08ms | 9.08ms | 9.08ms | 9.08ms | 9.08ms | 1 |
| EmbeddingService | DeleteSource | delete_source | deleteEmbeddingSource | destructive | OK | 25.79ms | 25.79ms | 25.79ms | 25.79ms | 25.79ms | 1 |
| EmbeddingService | GetEmbeddingJobStatus | get_job_status | getEmbeddingJobStatus | read_only | OK | 10.203ms | 12.309ms | 10.277ms | 6.867ms | 12.442ms | 25 |
| EmbeddingService | IngestDocument | ingest_document | ingestEmbeddingDocument | mutation | OK | 73.192ms | 76.655ms | 74.247ms | 68.656ms | 81.405ms | 5 |
| EmbeddingService | IngestDocumentBatch | ingest_document_batch | ingestEmbeddingDocumentBatch | mutation | OK | 69.216ms | 73.722ms | 68.204ms | 60.718ms | 73.745ms | 5 |
| EmbeddingService | ListEmbeddingWorkItems | list_work_items | listEmbeddingWorkItems | read_only | OK | 10.214ms | 12.606ms | 10.669ms | 8.15ms | 17.917ms | 25 |
| EmbeddingService | ListModels | list_models | listEmbeddingModels | read_only | OK | 10.494ms | 13.044ms | 10.854ms | 8.575ms | 19.37ms | 25 |
| EmbeddingService | ListSources | list_sources | listEmbeddingSources | read_only | OK | 13.928ms | 20.608ms | 14.582ms | 10.373ms | 20.85ms | 25 |
| EmbeddingService | RegisterModel | register_model | registerEmbeddingModel | mutation | OK | 45.829ms | 54.723ms | 50.356ms | 43.135ms | 63.762ms | 5 |
| EmbeddingService | RegisterSource | register_source | registerEmbeddingSource | mutation | OK | 29.175ms | 32.623ms | 29.471ms | 24.408ms | 33.336ms | 5 |
| EmbeddingService | ReportEmbedding | report_embedding | reportEmbedding | mutation | OK | 23.395ms | 23.781ms | 24.617ms | 22.5ms | 30.901ms | 5 |
| EmbeddingService | ReportEmbeddingBatch | report_embedding_batch | reportEmbeddingBatch | mutation | OK | 22.867ms | 24.552ms | 24.894ms | 20.342ms | 34.543ms | 5 |
| EmbeddingService | ReportEmbeddingFailure | report_embedding_failure | reportEmbeddingFailure | mutation | OK | 7.441ms | 8.145ms | 7.368ms | 5.949ms | 8.152ms | 5 |
| EmbeddingService | ReportParsedDocument | report_parsed_document | reportParsedDocument | mutation | OK | 55.18ms | 58.777ms | 56.554ms | 52.166ms | 62.901ms | 5 |
| EmbeddingService | ReportRetrievalEvaluation | report_retrieval_evaluation | reportRetrievalEvaluation | mutation | OK | 7.664ms | 8.252ms | 7.97ms | 5.709ms | 10.698ms | 5 |
| EmbeddingService | Retrieve | retrieve | retrieveEmbedding | read_only | OK | 23.125ms | 29.661ms | 23.981ms | 18.537ms | 43.048ms | 25 |
| EmbeddingService | SetModelStatus | set_model_status | setEmbeddingModelStatus | mutation | OK | 20.418ms | 20.669ms | 20.547ms | 19.679ms | 21.692ms | 5 |
| IdentityProviderService | CreateProvider | create_provider | createProvider | mutation | OK | 13.908ms | 13.908ms | 13.908ms | 13.908ms | 13.908ms | 5 |
| IdentityProviderService | DisableProvider | disable_provider | disableProvider | mutation | OK | 17.251ms | 17.582ms | 17.071ms | 16.068ms | 18.172ms | 5 |
| IdentityProviderService | ForceJwksRefresh | force_jwks_refresh | forceJwksRefresh | mutation | OK | 19.138ms | 20.876ms | 21.143ms | 17.837ms | 29.818ms | 5 |
| IdentityProviderService | GetProvider | get_provider | getProvider | read_only | OK | 5.432ms | 7.655ms | 5.356ms | 3.552ms | 8.644ms | 25 |
| IdentityProviderService | ImportSamlMetadata | import_saml_metadata | importSamlMetadata | mutation | OK | 16.654ms | 18.259ms | 16.913ms | 14.392ms | 18.79ms | 5 |
| IdentityProviderService | LinkIdentity | link_identity | linkIdentity | mutation | OK | 19.411ms | 20.521ms | 19.546ms | 15.09ms | 23.599ms | 5 |
| IdentityProviderService | ListExternalIdentities | list_external_identities | listExternalIdentities | read_only | OK | 7.572ms | 9.138ms | 7.585ms | 5.889ms | 10.989ms | 25 |
| IdentityProviderService | ListProviders | list_providers | listProviders | read_only | OK | 10.454ms | 14.129ms | 10.721ms | 6.898ms | 18.525ms | 25 |
| IdentityProviderService | PreviewClaimMapping | preview_claim_mapping | previewClaimMapping | read_only | OK | 5.036ms | 6.362ms | 5.042ms | 3.247ms | 7.374ms | 25 |
| IdentityProviderService | PreviewGroupMapping | preview_group_mapping | previewGroupMapping | read_only | OK | 4.902ms | 6.071ms | 4.873ms | 3.788ms | 6.865ms | 25 |
| IdentityProviderService | ResolveExternalIdentity | resolve_external_identity | resolveExternalIdentity | mutation | OK | 8.704ms | 23.531ms | 15.115ms | 5.415ms | 31.12ms | 5 |
| IdentityProviderService | SamlAcs | saml_acs | samlAcs | mutation | OK | 59.468ms | 62.537ms | 63.765ms | 52.33ms | 88.295ms | 5 |
| IdentityProviderService | ScimCreateGroup | scim_create_group | scimCreateGroup | mutation | OK | 4.341ms | 4.348ms | 4.108ms | 3.743ms | 4.356ms | 5 |
| IdentityProviderService | ScimCreateUser | scim_create_user | scimCreateUser | mutation | OK | 22.604ms | 23.43ms | 23.025ms | 21.158ms | 26.077ms | 5 |
| IdentityProviderService | ScimDeleteGroup | scim_delete_group | scimDeleteGroup | mutation | OK | 3.851ms | 3.859ms | 3.828ms | 3.758ms | 3.862ms | 5 |
| IdentityProviderService | ScimDeleteUser | scim_delete_user | scimDeleteUser | mutation | OK | 44.978ms | 44.978ms | 44.978ms | 44.978ms | 44.978ms | 5 |
| IdentityProviderService | ScimGetGroup | scim_get_group | scimGetGroup | mutation | OK | 6.149ms | 6.524ms | 6.121ms | 5.414ms | 6.624ms | 5 |
| IdentityProviderService | ScimGetUser | scim_get_user | scimGetUser | mutation | OK | 7.491ms | 7.933ms | 7.061ms | 5.026ms | 8.294ms | 5 |
| IdentityProviderService | ScimListGroups | scim_list_groups | scimListGroups | mutation | OK | 3.757ms | 4.91ms | 4.483ms | 3.228ms | 7.254ms | 5 |
| IdentityProviderService | ScimListUsers | scim_list_users | scimListUsers | mutation | OK | 9.33ms | 10.185ms | 9.288ms | 8.277ms | 10.321ms | 5 |
| IdentityProviderService | ScimPatchGroup | scim_patch_group | scimPatchGroup | mutation | OK | 9.704ms | 9.888ms | 9.056ms | 7.592ms | 9.917ms | 5 |
| IdentityProviderService | ScimPatchUser | scim_patch_user | scimPatchUser | mutation | OK | 20.465ms | 20.76ms | 19.463ms | 16.683ms | 22.318ms | 5 |
| IdentityProviderService | ScimReplaceUser | scim_replace_user | scimReplaceUser | mutation | OK | 23.368ms | 28.087ms | 23.41ms | 18.269ms | 28.104ms | 5 |
| IdentityProviderService | StartSamlLogin | start_saml_login | startSamlLogin | mutation | OK | 5.461ms | 7.009ms | 5.769ms | 4.062ms | 7.075ms | 5 |
| IdentityProviderService | TestProviderDiscovery | test_provider_discovery | testProviderDiscovery | read_only | OK | 6.415ms | 9.792ms | 6.731ms | 4.861ms | 11.907ms | 25 |
| IdentityProviderService | UnlinkIdentity | unlink_identity | unlinkIdentity | mutation | OK | 4.969ms | 5.886ms | 6.502ms | 4.321ms | 12.449ms | 5 |
| IdentityProviderService | UpdateProvider | update_provider | updateProvider | mutation | OK | 16.388ms | 17.533ms | 17.21ms | 15.576ms | 20.277ms | 5 |
| LiveQueryService | Subscribe | subscribe | liveQuerySubscribe | read_only | OK | 14.073ms | 17.247ms | 14.774ms | 11.83ms | 31.816ms | 25 |
| LockService | AcquireLock | acquire_lock | acquireLock | mutation | OK | 36.657ms | 41.557ms | 38.579ms | 31.918ms | 49.843ms | 5 |
| LockService | GetLock | get_lock | getLock | read_only | OK | 12.148ms | 19.055ms | 12.852ms | 9.463ms | 25.462ms | 25 |
| LockService | ListLocks | list_locks | listLocks | read_only | OK | 13.523ms | 18.557ms | 13.658ms | 7.942ms | 19.123ms | 25 |
| LockService | ReleaseLock | release_lock | releaseLock | mutation | OK | 10.752ms | 10.94ms | 10.434ms | 8.078ms | 12.583ms | 5 |
| LockService | RenewLock | renew_lock | renewLock | mutation | NotFound | 9.913ms | 10.256ms | 10.205ms | 9.485ms | 11.501ms | 5 |
| MeteringService | CheckQuota | check_quota | checkQuota | read_only | OK | 12.028ms | 13.482ms | 11.844ms | 9.996ms | 14.021ms | 25 |
| MeteringService | GetQuota | get_quota | getQuota | read_only | OK | 11.187ms | 13.52ms | 11.393ms | 9.473ms | 13.741ms | 25 |
| MeteringService | ListQuotas | list_quotas | listQuotas | read_only | OK | 13.027ms | 19.268ms | 13.886ms | 8.708ms | 30.248ms | 25 |
| MeteringService | PutQuota | put_quota | putQuota | mutation | OK | 19.079ms | 20.833ms | 19.547ms | 17.981ms | 21.383ms | 5 |
| MeteringService | QueryUsage | query_usage | queryUsage | read_only | OK | 12.04ms | 14.498ms | 11.943ms | 8.793ms | 17.854ms | 25 |
| MeteringService | RecordUsage | record_usage | recordUsage | mutation | OK | 8.182ms | 8.627ms | 8.16ms | 6.974ms | 9.387ms | 5 |
| NotificationService | GetDeliveryStats | get_delivery_stats | getDeliveryStats | read_only | OK | 10.217ms | 17.764ms | 11.25ms | 7.647ms | 20.134ms | 25 |
| NotificationService | GetNotification | get_notification | getNotification | read_only | OK | 15.495ms | 24.896ms | 17.012ms | 11.832ms | 28.953ms | 25 |
| NotificationService | GetPreference | get_preference | getPreference | read_only | OK | 13.871ms | 16.589ms | 14.062ms | 12.066ms | 17.353ms | 25 |
| NotificationService | GetTemplate | get_template | getTemplate | read_only | OK | 14.31ms | 18.616ms | 14.858ms | 11.467ms | 19.357ms | 25 |
| NotificationService | ListNotifications | list_notifications | listNotifications | read_only | OK | 20.928ms | 26.473ms | 21.359ms | 16.788ms | 27.004ms | 25 |
| NotificationService | ListPreferences | list_preferences | listPreferences | read_only | OK | 19.492ms | 24.383ms | 19.658ms | 15.865ms | 28.048ms | 25 |
| NotificationService | ListTemplates | list_templates | listTemplates | read_only | OK | 20.982ms | 31.326ms | 22.628ms | 15.937ms | 45.078ms | 25 |
| NotificationService | ReportDelivery | report_delivery | reportDelivery | mutation | OK | 12.453ms | 12.956ms | 12.636ms | 11.615ms | 14.495ms | 5 |
| NotificationService | RetryNotification | retry_notification | retryNotification | mutation | OK | 11.895ms | 11.895ms | 11.895ms | 11.895ms | 11.895ms | 5 |
| NotificationService | SendNotification | send_notification | sendNotification | mutation | OK | 31.77ms | 33.666ms | 31.876ms | 28.216ms | 34.184ms | 5 |
| NotificationService | SetPreference | set_preference | setPreference | mutation | OK | 9.257ms | 9.955ms | 9.027ms | 6.535ms | 11.733ms | 5 |
| NotificationService | UpsertTemplate | upsert_template | upsertTemplate | mutation | OK | 7.19ms | 7.214ms | 7.071ms | 6.125ms | 7.794ms | 5 |
| PeerService | GetPeer | get_peer | getPeer | read_only | OK | 12.225ms | 15.426ms | 12.52ms | 10.42ms | 15.555ms | 25 |
| PeerService | JoinRoom | join_room | joinRoom | mutation | OK | 21.923ms | 23.146ms | 21.368ms | 17.713ms | 25.011ms | 5 |
| PeerService | JoinSession | join_session | joinSession | mutation | OK | 23.139ms | 23.353ms | 26.568ms | 20.877ms | 43.53ms | 5 |
| PeerService | LeaveRoom | leave_room | leaveRoom | mutation | OK | 9.89ms | 11.05ms | 10.745ms | 6.973ms | 18.15ms | 5 |
| PeerService | ListPeers | list_peers | listPeers | read_only | OK | 13.792ms | 16.702ms | 13.322ms | 7.617ms | 16.788ms | 25 |
| RoomService | CloseRoom | close_room | closeRoom | mutation | OK | 24.748ms | 26.059ms | 24.701ms | 21.904ms | 26.127ms | 5 |
| RoomService | CreateRoom | create_room | createRoom | mutation | OK | 16.536ms | 16.593ms | 16.974ms | 14.634ms | 21.769ms | 5 |
| RoomService | GetRoom | get_room | getRoom | read_only | OK | 11.369ms | 14.656ms | 11.761ms | 8.084ms | 19.897ms | 25 |
| RoomService | ListEgress | list_egress | listEgress | read_only | CAPABILITY_SKIPPED | 4.943ms | 13.779ms | 5.883ms | 3.825ms | 17.536ms | 25 |
| RoomService | ListRooms | list_rooms | listRooms | read_only | OK | 10.325ms | 12.872ms | 10.494ms | 7.928ms | 13.082ms | 25 |
| RoomService | StartRoomComposite | start_room_composite | startRoomComposite | mutation | CAPABILITY_SKIPPED | 5.858ms | 7.536ms | 7.237ms | 5.596ms | 11.349ms | 5 |
| RoomService | StartTrackEgress | start_track_egress | startTrackEgress | mutation | CAPABILITY_SKIPPED | 5.471ms | 6.063ms | 5.575ms | 4.932ms | 6.454ms | 5 |
| RoomService | StopEgress | stop_egress | stopEgress | mutation | CAPABILITY_SKIPPED | 6.022ms | 6.482ms | 6.354ms | 4.863ms | 8.882ms | 5 |
| RoomService | UpdateRoom | update_room | updateRoom | mutation | OK | 9.363ms | 9.836ms | 9.551ms | 8.682ms | 10.922ms | 5 |
| SchedulerService | CreateJob | create_job | createJob | mutation | OK | 15.671ms | 15.681ms | 14.822ms | 12.994ms | 15.701ms | 5 |
| SchedulerService | DeleteJob | delete_job | deleteJob | destructive | OK | 11.055ms | 11.055ms | 11.055ms | 11.055ms | 11.055ms | 1 |
| SchedulerService | GetJob | get_job | getJob | read_only | OK | 11.356ms | 15.972ms | 11.726ms | 8.525ms | 19.445ms | 25 |
| SchedulerService | ListJobs | list_jobs | listJobs | read_only | OK | 17.517ms | 24.794ms | 18.656ms | 12.499ms | 27.273ms | 25 |
| SchedulerService | PauseJob | pause_job | pauseJob | mutation | OK | 10.878ms | 10.878ms | 10.878ms | 10.878ms | 10.878ms | 5 |
| SchedulerService | ResumeJob | resume_job | resumeJob | mutation | OK | 15.066ms | 15.066ms | 15.066ms | 15.066ms | 15.066ms | 5 |
| SearchService | CreateIndex | create_index | createSearchIndex | mutation | OK | 20.366ms | 22.603ms | 26.725ms | 19.982ms | 50.537ms | 5 |
| SearchService | DeleteIndex | delete_index | deleteSearchIndex | destructive | OK | 18.461ms | 18.461ms | 18.461ms | 18.461ms | 18.461ms | 1 |
| SearchService | ListIndexes | list_indexes | listSearchIndexes | read_only | OK | 19.396ms | 30.379ms | 20.651ms | 12.066ms | 40.053ms | 25 |
| SearchService | Reindex | reindex | reindexSearchIndex | mutation | OK | 21.588ms | 24.19ms | 21.693ms | 18.886ms | 24.75ms | 5 |
| SearchService | Search | search | search | read_only | OK | 17.797ms | 30.75ms | 19.557ms | 10.927ms | 31.978ms | 25 |
| SignalingService | Signal | signal | signal | mutation | OK | 16.439ms | 16.439ms | 16.439ms | 16.439ms | 16.439ms | 5 |
| StorageService | DeleteFile | delete_file | deleteFile | mutation | OK | 28.628ms | 28.628ms | 28.628ms | 28.628ms | 28.628ms | 5 |
| StorageService | DownloadFile | download_file | downloadFile | read_only | OK | 35.452ms | 53.775ms | 37.225ms | 24.189ms | 66.083ms | 25 |
| StorageService | FinalizeUpload | finalize_upload | finalizeUpload | mutation | OK | 33.161ms | 33.161ms | 33.161ms | 33.161ms | 33.161ms | 5 |
| StorageService | GetDownloadUrl | get_download_url | getDownloadUrl | read_only | OK | 18.261ms | 30.249ms | 19.642ms | 12.86ms | 30.306ms | 25 |
| StorageService | GetFile | get_file | getFile | read_only | OK | 16.146ms | 31.333ms | 18.634ms | 11.396ms | 38.874ms | 25 |
| StorageService | ListFiles | list_files | listFiles | read_only | OK | 27.357ms | 43.743ms | 30.433ms | 21.69ms | 49.629ms | 25 |
| StorageService | RegisterUpload | register_upload | registerUpload | mutation | OK | 20.527ms | 20.706ms | 20.385ms | 19.168ms | 22.277ms | 5 |
| StorageService | ReissueUploadUrl | reissue_upload_url | reissueUploadUrl | read_only | OK | 11.283ms | 22.464ms | 13.151ms | 10.172ms | 37.754ms | 25 |
| StorageService | UpdateFile | update_file | updateFile | mutation | OK | 23.364ms | 24.228ms | 24.026ms | 20.622ms | 28.647ms | 5 |
| TenantService | CreateTenant | create_tenant | createTenant | mutation | OK | 14.522ms | 15.401ms | 14.725ms | 10.978ms | 19.335ms | 5 |
| TenantService | GetTenant | get_tenant | getTenant | read_only | OK | 11.329ms | 18.272ms | 12.385ms | 9.357ms | 18.796ms | 25 |
| TenantService | GetTenantConfig | get_tenant_config | getTenantConfig | read_only | OK | 10.164ms | 12.946ms | 11.025ms | 8.088ms | 29.63ms | 25 |
| TenantService | ListTenants | list_tenants | listTenants | read_only | OK | 11.418ms | 18.036ms | 11.959ms | 7.265ms | 18.18ms | 25 |
| TenantService | PurgeTenant | purge_tenant | purgeTenant | destructive | OK | 183.764ms | 183.764ms | 183.764ms | 183.764ms | 183.764ms | 1 |
| TenantService | UpdateTenant | update_tenant | updateTenant | mutation | OK | 12.088ms | 12.759ms | 16.275ms | 10.634ms | 34.176ms | 5 |
| TenantService | UpdateTenantConfig | update_tenant_config | updateTenantConfig | mutation | OK | 20.208ms | 23.527ms | 21.313ms | 19.113ms | 23.669ms | 5 |
| TrackService | ListTracks | list_tracks | listTracks | read_only | OK | 10.799ms | 12.318ms | 10.797ms | 9.285ms | 13.983ms | 25 |
| TrackService | MuteTrack | mute_track | muteTrack | mutation | OK | 9.454ms | 10.481ms | 9.906ms | 8.204ms | 12.187ms | 5 |
| TrackService | PublishTrack | publish_track | publishTrack | mutation | OK | 17.985ms | 20.304ms | 18.919ms | 17.079ms | 21.395ms | 5 |
| TrackService | UnpublishTrack | unpublish_track | unpublishTrack | mutation | OK | 9.594ms | 11.401ms | 10.184ms | 9.19ms | 11.488ms | 5 |
| TurnService | IssueCredentials | issue_credentials | issueCredentials | mutation | OK | 9.207ms | 9.259ms | 8.43ms | 6.353ms | 9.705ms | 5 |
| VaultService | BatchDecrypt | batch_decrypt | vaultBatchDecrypt | mutation | OK | 12.651ms | 14.227ms | 13.083ms | 11.864ms | 14.291ms | 5 |
| VaultService | BatchEncrypt | batch_encrypt | vaultBatchEncrypt | mutation | OK | 17.619ms | 21.258ms | 18.813ms | 15.724ms | 21.958ms | 5 |
| VaultService | CreateTransitKey | create_transit_key | createTransitKey | mutation | OK | 26.335ms | 26.335ms | 26.335ms | 26.335ms | 26.335ms | 5 |
| VaultService | Decrypt | decrypt | vaultDecrypt | read_only | OK | 17.046ms | 19.727ms | 16.558ms | 11.967ms | 20.934ms | 25 |
| VaultService | DeleteSecret | delete_secret | deleteSecret | mutation | OK | 9.924ms | 11.865ms | 11.842ms | 9.521ms | 18.087ms | 5 |
| VaultService | DestroySecret | destroy_secret | destroySecret | destructive | OK | 22.659ms | 22.659ms | 22.659ms | 22.659ms | 22.659ms | 1 |
| VaultService | Encrypt | encrypt | vaultEncrypt | mutation | OK | 14.309ms | 14.508ms | 14.889ms | 13.487ms | 18.006ms | 5 |
| VaultService | GenerateDataKey | generate_data_key | vaultGenerateDataKey | mutation | OK | 14.331ms | 14.465ms | 13.823ms | 12.016ms | 14.744ms | 5 |
| VaultService | GenerateDatabaseCredentials | generate_database_credentials | generateDatabaseCredentials | mutation | OK | 22.837ms | 24.353ms | 23.56ms | 22.301ms | 25.626ms | 5 |
| VaultService | GetSecret | get_secret | getSecret | read_only | OK | 15.268ms | 21.892ms | 15.929ms | 10.842ms | 30.081ms | 25 |
| VaultService | GetTransitPublicKey | get_transit_public_key | vaultGetTransitPublicKey | read_only | OK | 11.927ms | 14.217ms | 12.002ms | 10.369ms | 15.126ms | 25 |
| VaultService | Hmac | hmac | vaultHmac | mutation | OK | 12.185ms | 15.977ms | 14.099ms | 11.379ms | 18.956ms | 5 |
| VaultService | ListSecrets | list_secrets | listSecrets | read_only | OK | 15.507ms | 24.037ms | 16.743ms | 12.379ms | 28.186ms | 25 |
| VaultService | PutSecret | put_secret | putSecret | mutation | OK | 23.108ms | 23.108ms | 23.108ms | 23.108ms | 23.108ms | 5 |
| VaultService | Rewrap | rewrap | vaultRewrap | mutation | OK | 13.802ms | 14.968ms | 14.359ms | 13.013ms | 16.796ms | 5 |
| VaultService | RotateTransitKey | rotate_transit_key | rotateTransitKey | mutation | OK | 29.626ms | 30.721ms | 29.239ms | 26.623ms | 31.708ms | 5 |
| VaultService | SealStatus | seal_status | vaultSealStatus | read_only | OK | 2.151ms | 2.734ms | 2.095ms | 1.6ms | 3.436ms | 25 |
| VaultService | Sign | sign | vaultSign | mutation | OK | 16.152ms | 16.631ms | 16.262ms | 14.56ms | 18.22ms | 5 |
| VaultService | UndeleteSecret | undelete_secret | undeleteSecret | mutation | OK | 25.096ms | 25.096ms | 25.096ms | 25.096ms | 25.096ms | 5 |
| VaultService | Verify | verify | vaultVerify | read_only | OK | 14.261ms | 23.572ms | 15.4ms | 11.274ms | 28.075ms | 25 |
| WebhookService | CreateEndpoint | create_endpoint | createWebhookEndpoint | mutation | OK | 15.599ms | 16.576ms | 15.148ms | 12.696ms | 16.744ms | 5 |
| WebhookService | DeleteEndpoint | delete_endpoint | deleteWebhookEndpoint | destructive | OK | 14.411ms | 14.411ms | 14.411ms | 14.411ms | 14.411ms | 1 |
| WebhookService | GetEndpoint | get_endpoint | getWebhookEndpoint | read_only | OK | 8.335ms | 11.362ms | 8.893ms | 5.422ms | 21.613ms | 25 |
| WebhookService | ListDeliveries | list_deliveries | listWebhookDeliveries | read_only | OK | 12.559ms | 16.242ms | 12.445ms | 9.246ms | 16.937ms | 25 |
| WebhookService | ListEndpoints | list_endpoints | listWebhookEndpoints | read_only | OK | 11.273ms | 16.416ms | 11.708ms | 8.785ms | 17.126ms | 25 |
| WebhookService | UpdateEndpoint | update_endpoint | updateWebhookEndpoint | mutation | OK | 13.34ms | 14.672ms | 13.825ms | 13.075ms | 14.909ms | 5 |
| WorkflowService | CancelWorkflow | cancel_workflow | cancelWorkflow | destructive | OK | 23.026ms | 23.026ms | 23.026ms | 23.026ms | 23.026ms | 1 |
| WorkflowService | GetWorkflow | get_workflow | getWorkflow | read_only | OK | 8.087ms | 10.816ms | 7.937ms | 5.413ms | 10.934ms | 25 |
| WorkflowService | ListWorkflows | list_workflows | listWorkflows | read_only | OK | 10.543ms | 12.77ms | 10.598ms | 7.616ms | 12.835ms | 25 |
| WorkflowService | SignalWorkflow | signal_workflow | signalWorkflow | mutation | OK | 17.671ms | 19.779ms | 17.729ms | 14.766ms | 20.058ms | 5 |
| WorkflowService | StartWorkflow | start_workflow | startWorkflow | mutation | OK | 16.278ms | 16.818ms | 16.451ms | 14.159ms | 19.058ms | 5 |
