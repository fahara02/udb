# UDB SDK Live Perf — Go (localhost)

RPCs measured: 265   tenant=ccbc3a9e-95c7-409b-8091-edac1f3806a1

Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities (a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request's reference/ID fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection latency. The TARGET is zero failures; any residual non-OK RPC is listed under Failures for the maintainer to finish.

Unary RPCs = full request→response round-trip. Non-CDC streaming RPCs report time-to-FIRST-RESPONSE with seeded inputs. CDC subscription (PublishCDC) reports time-to-FIRST-EVENT: the harness subscribes, fires a real Upsert that flows outbox→CDC→Kafka, and times the first delivered event. Streaming rows are marked in the note column.

## Seeded fixtures

Captured semantic field → seeded value keys used to resolve request fields: action, apply_run_id, approval_token, approve_draft_id, approve_run_id, approved_by, asset_id, assigned_by, auth_challenge_id, auth_token, bucket, canary_id, canary_version_id, catalog_manifest, challenge_id, close_room_id, code, collection, content_type, created_by, csrf_token, definition_id, delete_file_id, delete_policy_id, delete_role_id, delete_scim_user_id, deleted_by, device_id, disable_provider_id, dismiss_dlq_id, dlq_id, document_id, domain, ds_policy_id, event_type, external_identity_id, file_id, file_type, filename, finalize_file_id, gov_exp, instance_id, join_session_room_id, key_id, kind, leave_peer_id, locale, log_id, mark_saga_id, message_type, migration_id, mongo_collection, name, node_id, notification_id, object, object_key, otp_code, otp_id, owner_id, peer_id, plain_key, policy_draft_id, policy_id, policy_version_id, project, project_id, provider_id, quarantine_dlq_id, recipient_id, record_id, recovery_code, refresh_session_id, refresh_token, reg_challenge_id, reject_draft_id, rejected_by, relation, replay_dlq_id, reset_otp_code, reset_otp_id, resource, retry_saga_id, revoke_key_id, revoked_by, role, role_code, role_id, rollback_policy_set_id, rollback_target_version_id, room_id, saga_id, saml_provider_id, scim_group_id, scim_user_id, session_id, signal_peer_id, stage_name, step_id, subject, tenant, tenant_id, token, topic_pattern, track_id, ts_table, unpublish_track_id, update_draft_id, update_key_id, updated_by, user_id, user_role_id, username

## Per-service mean latency (mean of per-RPC means)

| Service | RPCs | mean |
|---|---:|---:|
| AuthnService | 50 | 58.753ms |
| DataBroker | 77 | 23.592ms |
| AuthzService | 41 | 26.834ms |
| IdentityProviderService | 27 | 18.144ms |
| ControlPlaneService | 5 | 35.706ms |
| StorageService | 8 | 20.864ms |
| NotificationService | 11 | 14.861ms |
| ApiKeyService | 9 | 15.168ms |
| AssetService | 8 | 10.938ms |
| PeerService | 5 | 15.506ms |
| TenantService | 6 | 11.478ms |
| RoomService | 5 | 13.697ms |
| AnalyticsService | 7 | 5.552ms |
| TrackService | 4 | 9.707ms |
| SignalingService | 1 | 22.865ms |
| TurnService | 1 | 3.683ms |

## Failures — still to fix (0)

No RPC returned a non-OK gRPC status — every RPC ran its success path.

## Slowest 25 RPCs by p99

| RPC | api_alias | operation_id | kind | err | p50 | p99 | mean | iters | note |
|---|---|---|---|---|---:|---:|---:|---:|---|
| AuthnService/ChangePassword | change_password | changePassword | mutation | OK | 784.914ms | 784.914ms | 784.914ms | 5 | mutation (seeded success path) |
| AuthnService/ResetPassword | reset_password | resetPassword | mutation | OK | 461.394ms | 461.394ms | 461.394ms | 5 | mutation (seeded success path) |
| AuthnService/CreateUser | create_user | createUser | mutation | OK | 425.802ms | 425.802ms | 425.802ms | 5 | mutation (seeded success path) |
| AuthnService/Login | login | login | mutation | OK | 388.766ms | 404.135ms | 395.011ms | 5 | mutation (seeded success path) |
| DataBroker/StageCatalog | stage_catalog | stageCatalog | destructive | OK | 342.164ms | 342.164ms | 342.164ms | 1 | destructive: 1 real call against a seeded disposable target |
| DataBroker/ApplyMigration | apply_migration | applyMigration | mutation | OK | 249.206ms | 249.206ms | 249.206ms | 5 | mutation (seeded success path) |
| DataBroker/PublishCDC | publish_cdc | publishCdc | mutation | OK | 245.768ms | 245.768ms | 195.141ms | 3 | cdc subscription: time-to-first-event (real mutation produced) |
| AuthnService/GenerateRecoveryCodes | generate_recovery_codes | generateRecoveryCodes | mutation | OK | 158.001ms | 176.29ms | 151.216ms | 5 | mutation (seeded success path) |
| AuthnService/ForgotPassword | forgot_password | forgotPassword | mutation | OK | 109.658ms | 167.952ms | 107.719ms | 5 | mutation (seeded success path) |
| AuthzService/PromoteCanary | promote_canary | promoteCanary | destructive | OK | 120.089ms | 120.089ms | 120.089ms | 1 | destructive: 1 real call against a seeded disposable target |
| ControlPlaneService/DeltaResources | delta_resources | deltaResources | mutation | OK | 84.606ms | 96.166ms | 86.217ms | 5 | streaming: time-to-first-response (seeded; bidi) |
| AuthzService/RollbackPolicyVersion | rollback_policy_version | rollbackPolicyVersion | destructive | OK | 91.646ms | 91.646ms | 91.646ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthzService/SeedBuiltinRoles | seed_builtin_roles | seedBuiltinRoles | mutation | OK | 87.592ms | 88.074ms | 82.842ms | 5 | mutation (seeded success path) |
| DataBroker/StepDownCdcLeader | step_down_cdc_leader | stepDownCdcLeader | mutation | OK | 77.834ms | 80.231ms | 61.772ms | 5 | mutation (seeded success path) |
| IdentityProviderService/SamlAcs | saml_acs | samlAcs | mutation | OK | 65.535ms | 75.355ms | 78.669ms | 5 | mutation (seeded success path) |
| AuthzService/ActivatePolicyVersion | activate_policy_version | activatePolicyVersion | destructive | OK | 72.549ms | 72.549ms | 72.549ms | 1 | destructive: 1 real call against a seeded disposable target |
| ControlPlaneService/StreamResources | stream_resources | streamResources | mutation | OK | 37.58ms | 65.417ms | 52.45ms | 5 | streaming: time-to-first-response (seeded; bidi) |
| AuthzService/CreatePolicyDraft | create_policy_draft | createPolicyDraft | mutation | OK | 57.921ms | 63.879ms | 61.058ms | 5 | mutation (seeded success path) |
| IdentityProviderService/CreateProvider | create_provider | createProvider | mutation | OK | 56.933ms | 56.933ms | 56.933ms | 5 | mutation (seeded success path) |
| ApiKeyService/EmergencyRevokeApiKeys | emergency_revoke_api_keys | emergencyRevokeApiKeys | destructive | OK | 56.745ms | 56.745ms | 56.745ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthzService/ApprovePolicyDraft | approve_policy_draft | approvePolicyDraft | mutation | OK | 56.21ms | 56.21ms | 56.21ms | 5 | mutation (seeded success path) |
| NotificationService/SendNotification | send_notification | sendNotification | mutation | OK | 43.121ms | 55.793ms | 47.416ms | 5 | mutation (seeded success path) |
| AuthnService/AdminResetMfa | admin_reset_mfa | adminResetMfa | destructive | OK | 50.95ms | 50.95ms | 50.95ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthzService/InvalidatePolicyBundles | invalidate_policy_bundles | invalidatePolicyBundles | destructive | OK | 47.727ms | 47.727ms | 47.727ms | 1 | destructive: 1 real call against a seeded disposable target |
| AuthzService/MigrateLegacyPolicies | migrate_legacy_policies | migrateLegacyPolicies | destructive | OK | 46.413ms | 46.413ms | 46.413ms | 1 | destructive: 1 real call against a seeded disposable target |

## Full per-RPC table (sorted by service, then name)

| Service | RPC | api_alias | operation_id | kind | err | p50 | p99 | mean | min | max | iters |
|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|
| AnalyticsService | GetExecutorPerformance | get_executor_performance | getExecutorPerformance | read_only | OK | 4.772ms | 13.531ms | 6.436ms | 3.33ms | 13.605ms | 25 |
| AnalyticsService | GetPipelineSummary | get_pipeline_summary | getPipelineSummary | read_only | OK | 5.762ms | 9.551ms | 6.688ms | 4.128ms | 9.935ms | 25 |
| AnalyticsService | GetReconciliationAnalytics | get_reconciliation_analytics | getReconciliationAnalytics | read_only | OK | 5.872ms | 10.34ms | 6.084ms | 3.239ms | 12.188ms | 25 |
| AnalyticsService | GetSlaCompliance | get_sla_compliance | getSlaCompliance | read_only | OK | 4.939ms | 7.933ms | 5.245ms | 2.841ms | 10.424ms | 25 |
| AnalyticsService | GetThroughput | get_throughput | getThroughput | read_only | OK | 5.019ms | 6.583ms | 4.832ms | 3.078ms | 6.772ms | 25 |
| AnalyticsService | RecordPipelineMetric | record_pipeline_metric | recordPipelineMetric | mutation | OK | 5.154ms | 5.518ms | 5.324ms | 4.866ms | 5.977ms | 5 |
| AnalyticsService | TriggerSnapshot | trigger_snapshot | triggerSnapshot | mutation | OK | 4.417ms | 4.524ms | 4.256ms | 2.81ms | 5.128ms | 5 |
| ApiKeyService | CreateApiKey | create_api_key | createApiKey | mutation | OK | 11.259ms | 11.445ms | 11.207ms | 10.489ms | 11.956ms | 5 |
| ApiKeyService | EmergencyRevokeApiKeys | emergency_revoke_api_keys | emergencyRevokeApiKeys | destructive | OK | 56.745ms | 56.745ms | 56.745ms | 56.745ms | 56.745ms | 1 |
| ApiKeyService | GetApiKey | get_api_key | getApiKey | read_only | OK | 4.298ms | 7.284ms | 4.921ms | 2.703ms | 9.567ms | 25 |
| ApiKeyService | GetApiKeyUsageStats | get_api_key_usage_stats | getApiKeyUsageStats | read_only | OK | 4.324ms | 14.58ms | 6.515ms | 3.472ms | 19.111ms | 25 |
| ApiKeyService | ListApiKeys | list_api_keys | listApiKeys | read_only | OK | 4.044ms | 6.755ms | 4.318ms | 3.085ms | 7.382ms | 25 |
| ApiKeyService | RevokeApiKey | revoke_api_key | revokeApiKey | mutation | OK | 12.201ms | 12.201ms | 12.201ms | 12.201ms | 12.201ms | 5 |
| ApiKeyService | RotateApiKey | rotate_api_key | rotateApiKey | mutation | OK | 20.237ms | 20.237ms | 20.237ms | 20.237ms | 20.237ms | 5 |
| ApiKeyService | UpdateApiKey | update_api_key | updateApiKey | mutation | OK | 11.257ms | 14.993ms | 12.485ms | 9.953ms | 15.865ms | 5 |
| ApiKeyService | ValidateApiKey | validate_api_key | validateApiKey | read_only | OK | 7.766ms | 11.188ms | 7.879ms | 5.69ms | 15.923ms | 25 |
| AssetService | CompleteStep | complete_step | completeStep | mutation | OK | 20.972ms | 21.3ms | 20.39ms | 17.833ms | 21.814ms | 5 |
| AssetService | CreatePipelineDefinition | create_pipeline_definition | createPipelineDefinition | mutation | OK | 8.234ms | 8.234ms | 8.234ms | 8.234ms | 8.234ms | 5 |
| AssetService | GetAsset | get_asset | getAsset | read_only | OK | 8.229ms | 11.103ms | 8.454ms | 6.019ms | 11.92ms | 25 |
| AssetService | GetPipeline | get_pipeline | getPipeline | read_only | OK | 7.335ms | 11.36ms | 8.122ms | 5.227ms | 14.121ms | 25 |
| AssetService | GetPipelineDefinition | get_pipeline_definition | getPipelineDefinition | read_only | OK | 8.02ms | 11.91ms | 8.333ms | 5.905ms | 14.682ms | 25 |
| AssetService | ListAssets | list_assets | listAssets | read_only | OK | 9.793ms | 14.09ms | 10.134ms | 6.323ms | 16.516ms | 25 |
| AssetService | RegisterAsset | register_asset | registerAsset | mutation | OK | 14.375ms | 15.184ms | 13.826ms | 11.001ms | 15.896ms | 5 |
| AssetService | StartPipeline | start_pipeline | startPipeline | mutation | OK | 4.459ms | 4.682ms | 10.015ms | 3.384ms | 33.673ms | 5 |
| AuthnService | AdminResetMfa | admin_reset_mfa | adminResetMfa | destructive | OK | 50.95ms | 50.95ms | 50.95ms | 50.95ms | 50.95ms | 1 |
| AuthnService | AdminResetPassword | admin_reset_password | adminResetPassword | destructive | OK | 13.989ms | 13.989ms | 13.989ms | 13.989ms | 13.989ms | 1 |
| AuthnService | AdminRevokeAllTenantSessions | admin_revoke_all_tenant_sessions | adminRevokeAllTenantSessions | destructive | OK | 20.341ms | 20.341ms | 20.341ms | 20.341ms | 20.341ms | 1 |
| AuthnService | AdminRevokeAllUserSessions | admin_revoke_all_user_sessions | adminRevokeAllUserSessions | destructive | OK | 18.87ms | 18.87ms | 18.87ms | 18.87ms | 18.87ms | 1 |
| AuthnService | AdminRevokeSession | admin_revoke_session | adminRevokeSession | destructive | OK | 33.536ms | 33.536ms | 33.536ms | 33.536ms | 33.536ms | 1 |
| AuthnService | Authenticate | authenticate | authenticate | read_only | OK | 14.58ms | 21.209ms | 15.091ms | 11.182ms | 22.002ms | 25 |
| AuthnService | ChangePassword | change_password | changePassword | mutation | OK | 784.914ms | 784.914ms | 784.914ms | 784.914ms | 784.914ms | 5 |
| AuthnService | ChangeUserStatus | change_user_status | changeUserStatus | destructive | OK | 12.477ms | 12.477ms | 12.477ms | 12.477ms | 12.477ms | 1 |
| AuthnService | ConfirmMFAEnrollment | confirm_mfaenrollment | confirmMfaenrollment | mutation | OK | 4.413ms | 4.523ms | 3.946ms | 2.772ms | 4.588ms | 5 |
| AuthnService | CreateSession | create_session | createSession | mutation | OK | 6.605ms | 9.237ms | 10.325ms | 5.256ms | 24.351ms | 5 |
| AuthnService | CreateUser | create_user | createUser | mutation | OK | 425.802ms | 425.802ms | 425.802ms | 425.802ms | 425.802ms | 5 |
| AuthnService | DeleteWebAuthnCredential | delete_web_authn_credential | deleteWebAuthnCredential | mutation | OK | 7.418ms | 8.207ms | 7.306ms | 5.839ms | 8.324ms | 5 |
| AuthnService | DisableMfaFactor | disable_mfa_factor | disableMfaFactor | mutation | OK | 13.431ms | 14.173ms | 16.541ms | 12.184ms | 29.76ms | 5 |
| AuthnService | EmergencyRevoke | emergency_revoke | emergencyRevoke | destructive | OK | 27.55ms | 27.55ms | 27.55ms | 27.55ms | 27.55ms | 1 |
| AuthnService | EnrollMFA | enroll_mfa | enrollMfa | mutation | OK | 12.536ms | 13.441ms | 12.099ms | 9.972ms | 14.113ms | 5 |
| AuthnService | FinishWebAuthnAuthentication | finish_web_authn_authentication | finishWebAuthnAuthentication | mutation | OK | 34.175ms | 34.175ms | 34.175ms | 34.175ms | 34.175ms | 5 |
| AuthnService | FinishWebAuthnRegistration | finish_web_authn_registration | finishWebAuthnRegistration | mutation | OK | 26.164ms | 26.164ms | 26.164ms | 26.164ms | 26.164ms | 5 |
| AuthnService | ForgotPassword | forgot_password | forgotPassword | mutation | OK | 109.658ms | 167.952ms | 107.719ms | 25.059ms | 205.457ms | 5 |
| AuthnService | GenerateRecoveryCodes | generate_recovery_codes | generateRecoveryCodes | mutation | OK | 158.001ms | 176.29ms | 151.216ms | 65.754ms | 227.784ms | 5 |
| AuthnService | GetJwks | get_jwks | getJwks | read_only | OK | 4.394ms | 6.864ms | 4.548ms | 2.709ms | 7.398ms | 25 |
| AuthnService | GetMfaPolicy | get_mfa_policy | getMfaPolicy | read_only | OK | 3.787ms | 7.023ms | 4.037ms | 2.691ms | 9.139ms | 25 |
| AuthnService | GetSession | get_session | getSession | read_only | OK | 3.422ms | 4.853ms | 3.63ms | 2.643ms | 6.294ms | 25 |
| AuthnService | GetUser | get_user | getUser | read_only | OK | 3.753ms | 6.549ms | 4.034ms | 2.701ms | 6.567ms | 25 |
| AuthnService | IntrospectToken | introspect_token | introspectToken | read_only | OK | 21.774ms | 24.522ms | 21.816ms | 17.355ms | 25.604ms | 25 |
| AuthnService | IssueMfaChallenge | issue_mfa_challenge | issueMfaChallenge | mutation | OK | 13.944ms | 23.53ms | 18.689ms | 12.879ms | 29.711ms | 5 |
| AuthnService | ListDevices | list_devices | listDevices | read_only | OK | 3.977ms | 6.009ms | 4.256ms | 2.978ms | 6.418ms | 25 |
| AuthnService | ListMfaFactors | list_mfa_factors | listMfaFactors | read_only | OK | 4.947ms | 8.35ms | 5.964ms | 4.351ms | 15.133ms | 25 |
| AuthnService | ListSessions | list_sessions | listSessions | read_only | OK | 6.786ms | 8.935ms | 6.712ms | 4.596ms | 9.369ms | 25 |
| AuthnService | ListUsers | list_users | listUsers | read_only | OK | 6.017ms | 8.909ms | 6.419ms | 4.506ms | 10.323ms | 25 |
| AuthnService | ListWebAuthnCredentials | list_web_authn_credentials | listWebAuthnCredentials | read_only | OK | 3.385ms | 4.944ms | 3.542ms | 2.578ms | 5.193ms | 25 |
| AuthnService | Login | login | login | mutation | OK | 388.766ms | 404.135ms | 395.011ms | 372.189ms | 435.333ms | 5 |
| AuthnService | Logout | logout | logout | mutation | OK | 4.593ms | 6.365ms | 5.215ms | 3.994ms | 6.617ms | 5 |
| AuthnService | PutMfaPolicy | put_mfa_policy | putMfaPolicy | mutation | OK | 6.006ms | 6.866ms | 7.994ms | 3.258ms | 18.232ms | 5 |
| AuthnService | RefreshSession | refresh_session | refreshSession | mutation | OK | 11.946ms | 12.92ms | 14.844ms | 11.576ms | 25.868ms | 5 |
| AuthnService | RefreshToken | refresh_token | refreshToken | mutation | OK | 11.412ms | 11.412ms | 11.412ms | 11.412ms | 11.412ms | 5 |
| AuthnService | RenamePasskey | rename_passkey | renamePasskey | mutation | OK | 9.638ms | 10.934ms | 9.403ms | 6.615ms | 11.792ms | 5 |
| AuthnService | ResendOTP | resend_otp | resendOtp | mutation | OK | 16.159ms | 16.253ms | 16.918ms | 10.937ms | 26.859ms | 5 |
| AuthnService | ResetPassword | reset_password | resetPassword | mutation | OK | 461.394ms | 461.394ms | 461.394ms | 461.394ms | 461.394ms | 5 |
| AuthnService | RevokeDevice | revoke_device | revokeDevice | mutation | OK | 24.675ms | 24.675ms | 24.675ms | 24.675ms | 24.675ms | 5 |
| AuthnService | RevokeRecoveryCodes | revoke_recovery_codes | revokeRecoveryCodes | mutation | OK | 10.322ms | 10.468ms | 9.735ms | 7.307ms | 12.827ms | 5 |
| AuthnService | RevokeSession | revoke_session | revokeSession | mutation | OK | 3.927ms | 4.224ms | 4.028ms | 3.758ms | 4.405ms | 5 |
| AuthnService | SendOTP | send_otp | sendOtp | mutation | OK | 13.619ms | 13.809ms | 15.475ms | 10.873ms | 26.375ms | 5 |
| AuthnService | SendPhoneVerification | send_phone_verification | sendPhoneVerification | mutation | OK | 16.392ms | 27.237ms | 20.13ms | 12.431ms | 30.802ms | 5 |
| AuthnService | StartWebAuthnAuthentication | start_web_authn_authentication | startWebAuthnAuthentication | mutation | OK | 14.579ms | 27.381ms | 19.36ms | 11.751ms | 30.227ms | 5 |
| AuthnService | StartWebAuthnRegistration | start_web_authn_registration | startWebAuthnRegistration | mutation | OK | 13.535ms | 15.013ms | 15.35ms | 13.076ms | 21.63ms | 5 |
| AuthnService | UpdateUser | update_user | updateUser | mutation | OK | 10.663ms | 10.741ms | 11.72ms | 6.546ms | 23.046ms | 5 |
| AuthnService | ValidateCSRF | validate_csrf | validateCsrf | read_only | OK | 3.242ms | 4.04ms | 3.297ms | 2.452ms | 4.501ms | 25 |
| AuthnService | ValidateToken | validate_token | validateToken | read_only | OK | 13.822ms | 17.178ms | 14.461ms | 11.272ms | 24.324ms | 25 |
| AuthnService | VerifyMfaChallenge | verify_mfa_challenge | verifyMfaChallenge | read_only | OK | 6.604ms | 8.508ms | 6.76ms | 4.846ms | 12.83ms | 25 |
| AuthnService | VerifyOTP | verify_otp | verifyOtp | read_only | OK | 13.646ms | 17.027ms | 13.803ms | 10.939ms | 17.511ms | 25 |
| AuthzService | ActivateCanary | activate_canary | activateCanary | destructive | OK | 35.623ms | 35.623ms | 35.623ms | 35.623ms | 35.623ms | 1 |
| AuthzService | ActivatePolicyVersion | activate_policy_version | activatePolicyVersion | destructive | OK | 72.549ms | 72.549ms | 72.549ms | 72.549ms | 72.549ms | 1 |
| AuthzService | ApprovePolicyDraft | approve_policy_draft | approvePolicyDraft | mutation | OK | 56.21ms | 56.21ms | 56.21ms | 56.21ms | 56.21ms | 5 |
| AuthzService | AssignRole | assign_role | assignRole | mutation | OK | 28.864ms | 42.068ms | 45.296ms | 24.885ms | 102.287ms | 5 |
| AuthzService | Authorize | authorize | authorize | read_only | OK | 17.8ms | 20.292ms | 17.685ms | 13.862ms | 20.966ms | 25 |
| AuthzService | BatchCheckPermissions | batch_check_permissions | batchCheckPermissions | read_only | OK | 7.951ms | 12.625ms | 8.328ms | 5.976ms | 17.188ms | 25 |
| AuthzService | CheckAccess | check_access | checkAccess | read_only | OK | 8.104ms | 17.212ms | 8.943ms | 6.491ms | 21.029ms | 25 |
| AuthzService | CreatePolicyDraft | create_policy_draft | createPolicyDraft | mutation | OK | 57.921ms | 63.879ms | 61.058ms | 55.788ms | 70.636ms | 5 |
| AuthzService | CreatePolicyRule | create_policy_rule | createPolicyRule | mutation | OK | 21.22ms | 33.706ms | 27.132ms | 19.467ms | 40.385ms | 5 |
| AuthzService | CreateRole | create_role | createRole | mutation | OK | 22.969ms | 22.969ms | 22.969ms | 22.969ms | 22.969ms | 5 |
| AuthzService | DeletePolicyRule | delete_policy_rule | deletePolicyRule | mutation | OK | 7.589ms | 9.343ms | 7.845ms | 4.804ms | 9.964ms | 5 |
| AuthzService | DeleteRole | delete_role | deleteRole | mutation | OK | 11.546ms | 11.7ms | 16.619ms | 8.241ms | 41.23ms | 5 |
| AuthzService | DiffPolicyDraft | diff_policy_draft | diffPolicyDraft | read_only | OK | 10.286ms | 13.454ms | 10.995ms | 8.183ms | 24.906ms | 25 |
| AuthzService | ExplainPolicy | explain_policy | explainPolicy | read_only | OK | 6.659ms | 7.954ms | 6.666ms | 5.488ms | 8.185ms | 25 |
| AuthzService | GetAuthzRevision | get_authz_revision | getAuthzRevision | read_only | OK | 3.539ms | 4.81ms | 3.641ms | 2.689ms | 4.979ms | 25 |
| AuthzService | GetCanaryStatus | get_canary_status | getCanaryStatus | read_only | OK | 8.469ms | 11.192ms | 8.626ms | 6.6ms | 12.938ms | 25 |
| AuthzService | GetNativeAccess | get_native_access | getNativeAccess | read_only | OK | 16.283ms | 21.338ms | 17.114ms | 14.424ms | 26.322ms | 25 |
| AuthzService | GetPolicyBundle | get_policy_bundle | getPolicyBundle | read_only | OK | 6.246ms | 7.73ms | 6.404ms | 4.877ms | 7.88ms | 25 |
| AuthzService | GetPolicyRule | get_policy_rule | getPolicyRule | read_only | OK | 3.245ms | 5.239ms | 3.55ms | 2.384ms | 5.376ms | 25 |
| AuthzService | GetRole | get_role | getRole | read_only | OK | 3.876ms | 5.008ms | 3.973ms | 2.681ms | 5.766ms | 25 |
| AuthzService | InvalidatePolicyBundles | invalidate_policy_bundles | invalidatePolicyBundles | destructive | OK | 47.727ms | 47.727ms | 47.727ms | 47.727ms | 47.727ms | 1 |
| AuthzService | LintAuthzPolicies | lint_authz_policies | lintAuthzPolicies | read_only | OK | 1.296ms | 1.803ms | 1.399ms | 518µs | 3.143ms | 25 |
| AuthzService | ListAccessDecisionAudits | list_access_decision_audits | listAccessDecisionAudits | read_only | OK | 9.452ms | 18.84ms | 10.813ms | 5.942ms | 26.437ms | 25 |
| AuthzService | ListPolicyRules | list_policy_rules | listPolicyRules | read_only | OK | 3.757ms | 5.513ms | 3.916ms | 2.704ms | 6.266ms | 25 |
| AuthzService | ListPolicyVersions | list_policy_versions | listPolicyVersions | read_only | OK | 8.92ms | 12.052ms | 9.331ms | 6.74ms | 20.142ms | 25 |
| AuthzService | ListRoles | list_roles | listRoles | read_only | OK | 4.377ms | 5.803ms | 4.412ms | 2.797ms | 5.96ms | 25 |
| AuthzService | ListUserPermissions | list_user_permissions | listUserPermissions | read_only | OK | 1.62ms | 2.272ms | 1.661ms | 1.065ms | 4.02ms | 25 |
| AuthzService | ListUserRoles | list_user_roles | listUserRoles | read_only | OK | 4.686ms | 6.308ms | 4.698ms | 3.227ms | 6.526ms | 25 |
| AuthzService | MigrateLegacyPolicies | migrate_legacy_policies | migrateLegacyPolicies | destructive | OK | 46.413ms | 46.413ms | 46.413ms | 46.413ms | 46.413ms | 1 |
| AuthzService | PromoteCanary | promote_canary | promoteCanary | destructive | OK | 120.089ms | 120.089ms | 120.089ms | 120.089ms | 120.089ms | 1 |
| AuthzService | PutAuthzPolicy | put_authz_policy | putAuthzPolicy | mutation | OK | 20.181ms | 29.802ms | 27.855ms | 14.471ms | 56.24ms | 5 |
| AuthzService | PutRelationship | put_relationship | putRelationship | mutation | OK | 24.46ms | 40.017ms | 29.698ms | 19.055ms | 42.45ms | 5 |
| AuthzService | PutRoleBinding | put_role_binding | putRoleBinding | mutation | OK | 30.002ms | 31.387ms | 25.734ms | 13.159ms | 36.198ms | 5 |
| AuthzService | RejectPolicyDraft | reject_policy_draft | rejectPolicyDraft | mutation | OK | 26.073ms | 26.073ms | 26.073ms | 26.073ms | 26.073ms | 5 |
| AuthzService | RevokeRole | revoke_role | revokeRole | mutation | OK | 8.014ms | 8.912ms | 10.228ms | 7.002ms | 19.376ms | 5 |
| AuthzService | RollbackPolicyVersion | rollback_policy_version | rollbackPolicyVersion | destructive | OK | 91.646ms | 91.646ms | 91.646ms | 91.646ms | 91.646ms | 1 |
| AuthzService | SeedBuiltinRoles | seed_builtin_roles | seedBuiltinRoles | mutation | OK | 87.592ms | 88.074ms | 82.842ms | 59.785ms | 104.167ms | 5 |
| AuthzService | SimulatePolicy | simulate_policy | simulatePolicy | mutation | OK | 14.555ms | 25.389ms | 19.674ms | 10.752ms | 34.085ms | 5 |
| AuthzService | SubmitPolicyDraft | submit_policy_draft | submitPolicyDraft | mutation | OK | 17.407ms | 17.407ms | 17.407ms | 17.407ms | 17.407ms | 5 |
| AuthzService | UpdatePolicyDraft | update_policy_draft | updatePolicyDraft | mutation | OK | 44.573ms | 46.076ms | 41.955ms | 21.263ms | 54.405ms | 5 |
| AuthzService | UpdateRole | update_role | updateRole | mutation | OK | 36.747ms | 39.149ms | 35.386ms | 21.949ms | 42.416ms | 5 |
| ControlPlaneService | AckStatus | ack_status | ackStatus | mutation | OK | 6.813ms | 6.927ms | 6.561ms | 5.392ms | 7.525ms | 5 |
| ControlPlaneService | DeltaResources | delta_resources | deltaResources | mutation | OK | 84.606ms | 96.166ms | 86.217ms | 54.692ms | 134.169ms | 5 |
| ControlPlaneService | GetResources | get_resources | getResources | read_only | OK | 4.741ms | 6.269ms | 4.825ms | 3.394ms | 6.638ms | 25 |
| ControlPlaneService | ListNodeStates | list_node_states | listNodeStates | read_only | OK | 26.584ms | 40.872ms | 28.477ms | 21.709ms | 45.9ms | 25 |
| ControlPlaneService | StreamResources | stream_resources | streamResources | mutation | OK | 37.58ms | 65.417ms | 52.45ms | 36.74ms | 84.986ms | 5 |
| DataBroker | ActivateCatalog | activate_catalog | activateCatalog | destructive | OK | 6.172ms | 6.172ms | 6.172ms | 6.172ms | 6.172ms | 1 |
| DataBroker | AnalyticalQuery | analytical_query | analyticalQuery | read_only | OK | 8.874ms | 11.768ms | 8.965ms | 7.145ms | 12.045ms | 25 |
| DataBroker | ApplyMigration | apply_migration | applyMigration | mutation | OK | 249.206ms | 249.206ms | 249.206ms | 249.206ms | 249.206ms | 5 |
| DataBroker | ApproveMigrationPlan | approve_migration_plan | approveMigrationPlan | mutation | OK | 44.702ms | 44.702ms | 44.702ms | 44.702ms | 44.702ms | 1 |
| DataBroker | BatchSelect | batch_select | batchSelect | mutation | OK | 5.879ms | 5.968ms | 5.777ms | 5.066ms | 6.098ms | 5 |
| DataBroker | BatchUpsert | batch_upsert | batchUpsert | mutation | OK | 25.557ms | 25.697ms | 25.858ms | 22.608ms | 31.451ms | 5 |
| DataBroker | BeginTx | begin_tx | beginTx | mutation | OK | 20.238ms | 23.532ms | 22.245ms | 16.896ms | 32.399ms | 5 |
| DataBroker | CacheDelete | cache_delete | cacheDelete | mutation | OK | 7.711ms | 7.881ms | 7.287ms | 5.648ms | 9.339ms | 5 |
| DataBroker | CacheGet | cache_get | cacheGet | read_only | OK | 6.262ms | 8.153ms | 6.3ms | 4.34ms | 9.516ms | 25 |
| DataBroker | CacheScan | cache_scan | cacheScan | read_only | OK | 7.477ms | 9.505ms | 7.865ms | 6.714ms | 9.592ms | 25 |
| DataBroker | CacheSet | cache_set | cacheSet | mutation | OK | 6.394ms | 6.917ms | 6.229ms | 4.825ms | 8.118ms | 5 |
| DataBroker | CreateMaterializedView | create_materialized_view | createMaterializedView | mutation | OK | 5.541ms | 5.609ms | 5.606ms | 4.853ms | 6.859ms | 5 |
| DataBroker | Delete | delete | delete | mutation | OK | 18.944ms | 19.828ms | 18.843ms | 14.12ms | 26.019ms | 5 |
| DataBroker | DeletePolicy | delete_policy | deletePolicy | mutation | OK | 19.385ms | 19.385ms | 19.385ms | 19.385ms | 19.385ms | 5 |
| DataBroker | DismissDlqEvent | dismiss_dlq_event | dismissDlqEvent | mutation | OK | 13.239ms | 14.674ms | 13.685ms | 11.002ms | 17.516ms | 5 |
| DataBroker | DocumentDelete | document_delete | documentDelete | mutation | OK | 6.021ms | 6.665ms | 9.644ms | 5.682ms | 23.893ms | 5 |
| DataBroker | DocumentFind | document_find | documentFind | read_only | OK | 5.418ms | 6.511ms | 5.565ms | 4.115ms | 12.622ms | 25 |
| DataBroker | DocumentGet | document_get | documentGet | read_only | OK | 5.284ms | 7.114ms | 5.429ms | 3.794ms | 7.835ms | 25 |
| DataBroker | DocumentUpsert | document_upsert | documentUpsert | mutation | OK | 4.922ms | 5.085ms | 5.236ms | 4.4ms | 7.201ms | 5 |
| DataBroker | DropResource | drop_resource | dropResource | destructive | OK | 32.147ms | 32.147ms | 32.147ms | 32.147ms | 32.147ms | 1 |
| DataBroker | EnqueueOutboxEvent | enqueue_outbox_event | enqueueOutboxEvent | mutation | OK | 10.029ms | 10.029ms | 10.029ms | 10.029ms | 10.029ms | 5 |
| DataBroker | EnsureBaseline | ensure_baseline | ensureBaseline | mutation | OK | 16.84ms | 20.982ms | 18.879ms | 12.843ms | 29.381ms | 5 |
| DataBroker | EnsureProject | ensure_project | ensureProject | mutation | OK | 12.262ms | 13.933ms | 14.602ms | 8.786ms | 25.85ms | 5 |
| DataBroker | EnsureResource | ensure_resource | ensureResource | mutation | OK | 13.671ms | 13.948ms | 12.88ms | 11.299ms | 14.103ms | 5 |
| DataBroker | GeneratePresignedUrl | generate_presigned_url | generatePresignedUrl | mutation | OK | 3.317ms | 3.896ms | 3.47ms | 2.807ms | 4.077ms | 5 |
| DataBroker | GenericDispatch | generic_dispatch | genericDispatch | mutation | OK | 7.005ms | 7.138ms | 6.915ms | 6.3ms | 7.69ms | 5 |
| DataBroker | GetAdminSummary | get_admin_summary | getAdminSummary | read_only | OK | 21.404ms | 26.784ms | 21.517ms | 15.919ms | 30.077ms | 25 |
| DataBroker | GetCapabilities | get_capabilities | getCapabilities | read_only | OK | 5.428ms | 7.403ms | 5.931ms | 4.21ms | 17.683ms | 25 |
| DataBroker | GetCatalogManifest | get_catalog_manifest | getCatalogManifest | read_only | OK | 8.932ms | 11.848ms | 9.464ms | 7.57ms | 13.331ms | 25 |
| DataBroker | GetCatalogVersion | get_catalog_version | getCatalogVersion | read_only | OK | 3.845ms | 5.644ms | 3.99ms | 3.248ms | 6.039ms | 25 |
| DataBroker | GetCatalogVersions | get_catalog_versions | getCatalogVersions | read_only | OK | 3.961ms | 5.485ms | 4.186ms | 3.253ms | 6.385ms | 25 |
| DataBroker | GetCdcStatus | get_cdc_status | getCdcStatus | read_only | OK | 3.624ms | 4.538ms | 3.735ms | 2.871ms | 5.216ms | 25 |
| DataBroker | GetDlqEvent | get_dlq_event | getDlqEvent | read_only | OK | 4.177ms | 5.502ms | 4.196ms | 3.181ms | 6.301ms | 25 |
| DataBroker | GetHealthReport | get_health_report | getHealthReport | read_only | OK | 2.384ms | 3.419ms | 2.512ms | 1.623ms | 4.795ms | 25 |
| DataBroker | GetMigrationStatus | get_migration_status | getMigrationStatus | read_only | OK | 5.035ms | 5.17ms | 4.879ms | 4.355ms | 5.305ms | 25 |
| DataBroker | GetObject | get_object | getObject | read_only | OK | 6.961ms | 9.617ms | 7.324ms | 5.62ms | 9.931ms | 25 |
| DataBroker | GetSaga | get_saga | getSaga | read_only | OK | 4.327ms | 6.049ms | 4.44ms | 2.766ms | 9.188ms | 25 |
| DataBroker | GraphMutate | graph_mutate | graphMutate | mutation | OK | 21.074ms | 33.245ms | 136.9ms | 16.01ms | 594.252ms | 5 |
| DataBroker | GraphQuery | graph_query | graphQuery | read_only | OK | 13.304ms | 19.029ms | 14.446ms | 11.145ms | 24.16ms | 25 |
| DataBroker | InitiateMultipartUpload | initiate_multipart_upload | initiateMultipartUpload | mutation | OK | 15.852ms | 28.975ms | 19.374ms | 9.113ms | 30.828ms | 5 |
| DataBroker | LintPolicies | lint_policies | lintPolicies | read_only | OK | 4.286ms | 6.235ms | 4.533ms | 2.824ms | 6.606ms | 25 |
| DataBroker | ListAdminAuditLogs | list_admin_audit_logs | listAdminAuditLogs | read_only | OK | 5.347ms | 8.202ms | 5.828ms | 4.456ms | 8.946ms | 25 |
| DataBroker | ListDlqEvents | list_dlq_events | listDlqEvents | read_only | OK | 5.057ms | 6.725ms | 5.196ms | 3.999ms | 7.581ms | 25 |
| DataBroker | ListMessageSchemas | list_message_schemas | listMessageSchemas | read_only | OK | 2.244ms | 2.881ms | 2.287ms | 1.72ms | 2.961ms | 25 |
| DataBroker | ListMigrationRuns | list_migration_runs | listMigrationRuns | read_only | OK | 4.996ms | 6.492ms | 5.106ms | 3.706ms | 7.501ms | 25 |
| DataBroker | ListPolicies | list_policies | listPolicies | read_only | OK | 5.162ms | 6.142ms | 5.078ms | 3.374ms | 6.767ms | 25 |
| DataBroker | ListProjects | list_projects | listProjects | read_only | OK | 5.036ms | 7.072ms | 5.532ms | 3.788ms | 18.345ms | 25 |
| DataBroker | ListResources | list_resources | listResources | read_only | OK | 4.08ms | 5.04ms | 4.16ms | 3.29ms | 5.144ms | 25 |
| DataBroker | ListSagas | list_sagas | listSagas | read_only | OK | 4.41ms | 6.213ms | 4.688ms | 3.407ms | 7.118ms | 25 |
| DataBroker | LookupMessageSchema | lookup_message_schema | lookupMessageSchema | read_only | OK | 2.827ms | 3.581ms | 2.879ms | 2.165ms | 3.678ms | 25 |
| DataBroker | MarkSagaReviewed | mark_saga_reviewed | markSagaReviewed | mutation | OK | 12.062ms | 17.932ms | 14.967ms | 9.621ms | 25.141ms | 5 |
| DataBroker | PauseCdc | pause_cdc | pauseCdc | mutation | OK | 12.814ms | 13.502ms | 14.493ms | 9.918ms | 26.018ms | 5 |
| DataBroker | PlanMigration | plan_migration | planMigration | mutation | OK | 13.498ms | 17.211ms | 16.753ms | 13.313ms | 26.253ms | 5 |
| DataBroker | PreviewCdcRedaction | preview_cdc_redaction | previewCdcRedaction | read_only | OK | 10.554ms | 22.685ms | 11.52ms | 8.303ms | 22.758ms | 25 |
| DataBroker | PublishCDC | publish_cdc | publishCdc | mutation | OK | 245.768ms | 245.768ms | 195.141ms | 90.415ms | 249.24ms | 3 |
| DataBroker | PutObject | put_object | putObject | mutation | OK | 32.37ms | 33.653ms | 29.253ms | 13.234ms | 35.251ms | 5 |
| DataBroker | PutPolicy | put_policy | putPolicy | destructive | OK | 24.993ms | 24.993ms | 24.993ms | 24.993ms | 24.993ms | 1 |
| DataBroker | QuarantineDlqEvent | quarantine_dlq_event | quarantineDlqEvent | mutation | OK | 14.813ms | 15.017ms | 14.168ms | 12.799ms | 15.299ms | 5 |
| DataBroker | ReloadPolicies | reload_policies | reloadPolicies | destructive | OK | 15.862ms | 15.862ms | 15.862ms | 15.862ms | 15.862ms | 1 |
| DataBroker | ReplayDlqEvent | replay_dlq_event | replayDlqEvent | mutation | OK | 31.675ms | 31.675ms | 31.675ms | 31.675ms | 31.675ms | 5 |
| DataBroker | ResumeCdc | resume_cdc | resumeCdc | mutation | OK | 28.435ms | 29.573ms | 27.739ms | 15.643ms | 40.002ms | 5 |
| DataBroker | RetrySagaCompensation | retry_saga_compensation | retrySagaCompensation | mutation | OK | 14.633ms | 14.633ms | 14.633ms | 14.633ms | 14.633ms | 5 |
| DataBroker | RollbackCatalog | rollback_catalog | rollbackCatalog | destructive | OK | 15.757ms | 15.757ms | 15.757ms | 15.757ms | 15.757ms | 1 |
| DataBroker | ScanProjectionDrift | scan_projection_drift | scanProjectionDrift | read_only | OK | 14.978ms | 18.691ms | 14.955ms | 11.666ms | 18.87ms | 25 |
| DataBroker | Select | select | select | read_only | OK | 6.652ms | 8.457ms | 6.713ms | 5.458ms | 8.792ms | 25 |
| DataBroker | SelectV2 | select_v_2 | selectV2 | read_only | OK | 7.017ms | 8.608ms | 7.759ms | 4.547ms | 27.101ms | 25 |
| DataBroker | StageCatalog | stage_catalog | stageCatalog | destructive | OK | 342.164ms | 342.164ms | 342.164ms | 342.164ms | 342.164ms | 1 |
| DataBroker | StepDownCdcLeader | step_down_cdc_leader | stepDownCdcLeader | mutation | OK | 77.834ms | 80.231ms | 61.772ms | 23.479ms | 101.747ms | 5 |
| DataBroker | TimeSeriesQuery | time_series_query | timeSeriesQuery | read_only | OK | 9.36ms | 12.056ms | 9.425ms | 7.585ms | 13.231ms | 25 |
| DataBroker | TimeSeriesWrite | time_series_write | timeSeriesWrite | mutation | OK | 3.302ms | 3.332ms | 3.035ms | 2.219ms | 3.469ms | 5 |
| DataBroker | Upsert | upsert | upsert | mutation | OK | 25.587ms | 29ms | 26.338ms | 23.205ms | 29.341ms | 5 |
| DataBroker | ValidateCatalog | validate_catalog | validateCatalog | destructive | OK | 1.661ms | 1.661ms | 1.661ms | 1.661ms | 1.661ms | 1 |
| DataBroker | VectorBatchUpsert | vector_batch_upsert | vectorBatchUpsert | mutation | OK | 8.104ms | 10.764ms | 22.878ms | 6.483ms | 81.118ms | 5 |
| DataBroker | VectorHybridSearch | vector_hybrid_search | vectorHybridSearch | read_only | OK | 5.428ms | 7.548ms | 5.685ms | 4.45ms | 8.817ms | 25 |
| DataBroker | VectorSearch | vector_search | vectorSearch | read_only | OK | 5.139ms | 7.087ms | 5.364ms | 4.227ms | 7.545ms | 25 |
| DataBroker | VectorUpsert | vector_upsert | vectorUpsert | mutation | OK | 12.872ms | 15.056ms | 13.131ms | 9.242ms | 16.327ms | 5 |
| DataBroker | VerifyAdminAuditLog | verify_admin_audit_log | verifyAdminAuditLog | read_only | OK | 6.953ms | 12.507ms | 7.639ms | 5.038ms | 12.945ms | 25 |
| IdentityProviderService | CreateProvider | create_provider | createProvider | mutation | OK | 56.933ms | 56.933ms | 56.933ms | 56.933ms | 56.933ms | 5 |
| IdentityProviderService | DisableProvider | disable_provider | disableProvider | mutation | OK | 31.18ms | 35.652ms | 29.598ms | 17.436ms | 42.255ms | 5 |
| IdentityProviderService | ForceJwksRefresh | force_jwks_refresh | forceJwksRefresh | mutation | OK | 35.197ms | 38.627ms | 31.608ms | 17.4ms | 47.176ms | 5 |
| IdentityProviderService | GetProvider | get_provider | getProvider | read_only | OK | 3.522ms | 5.977ms | 3.774ms | 2.388ms | 6.049ms | 25 |
| IdentityProviderService | ImportSamlMetadata | import_saml_metadata | importSamlMetadata | mutation | OK | 16.897ms | 18.691ms | 20.495ms | 16.341ms | 34.128ms | 5 |
| IdentityProviderService | LinkIdentity | link_identity | linkIdentity | mutation | OK | 24.05ms | 29.254ms | 23.408ms | 15.459ms | 32.459ms | 5 |
| IdentityProviderService | ListExternalIdentities | list_external_identities | listExternalIdentities | read_only | OK | 5.976ms | 8.971ms | 6.292ms | 4.382ms | 11.556ms | 25 |
| IdentityProviderService | ListProviders | list_providers | listProviders | read_only | OK | 6.1ms | 8.572ms | 6.484ms | 4.337ms | 10.945ms | 25 |
| IdentityProviderService | PreviewClaimMapping | preview_claim_mapping | previewClaimMapping | read_only | OK | 3.409ms | 5.016ms | 3.545ms | 2.749ms | 5.168ms | 25 |
| IdentityProviderService | PreviewGroupMapping | preview_group_mapping | previewGroupMapping | read_only | OK | 3.405ms | 5.101ms | 3.67ms | 2.655ms | 5.114ms | 25 |
| IdentityProviderService | ResolveExternalIdentity | resolve_external_identity | resolveExternalIdentity | mutation | OK | 8.454ms | 11.647ms | 15.628ms | 5.614ms | 44.208ms | 5 |
| IdentityProviderService | SamlAcs | saml_acs | samlAcs | mutation | OK | 65.535ms | 75.355ms | 78.669ms | 55.188ms | 132.213ms | 5 |
| IdentityProviderService | ScimCreateGroup | scim_create_group | scimCreateGroup | mutation | OK | 4.434ms | 5.711ms | 4.931ms | 3.883ms | 6.271ms | 5 |
| IdentityProviderService | ScimCreateUser | scim_create_user | scimCreateUser | mutation | OK | 39.879ms | 41.441ms | 32.897ms | 19.154ms | 43.35ms | 5 |
| IdentityProviderService | ScimDeleteGroup | scim_delete_group | scimDeleteGroup | mutation | OK | 5.033ms | 5.042ms | 4.808ms | 4.249ms | 5.211ms | 5 |
| IdentityProviderService | ScimDeleteUser | scim_delete_user | scimDeleteUser | mutation | OK | 35.449ms | 35.449ms | 35.449ms | 35.449ms | 35.449ms | 5 |
| IdentityProviderService | ScimGetGroup | scim_get_group | scimGetGroup | mutation | OK | 6.127ms | 6.383ms | 6.211ms | 5.173ms | 7.969ms | 5 |
| IdentityProviderService | ScimGetUser | scim_get_user | scimGetUser | mutation | OK | 5.604ms | 5.942ms | 5.607ms | 4.977ms | 6.225ms | 5 |
| IdentityProviderService | ScimListGroups | scim_list_groups | scimListGroups | mutation | OK | 3.471ms | 3.971ms | 3.78ms | 3.341ms | 4.69ms | 5 |
| IdentityProviderService | ScimListUsers | scim_list_users | scimListUsers | mutation | OK | 9.865ms | 10.854ms | 11.602ms | 7.029ms | 21.082ms | 5 |
| IdentityProviderService | ScimPatchGroup | scim_patch_group | scimPatchGroup | mutation | OK | 7.846ms | 9.619ms | 11.698ms | 7.031ms | 26.358ms | 5 |
| IdentityProviderService | ScimPatchUser | scim_patch_user | scimPatchUser | mutation | OK | 23.633ms | 45.995ms | 31.833ms | 20.304ms | 46.437ms | 5 |
| IdentityProviderService | ScimReplaceUser | scim_replace_user | scimReplaceUser | mutation | OK | 17.222ms | 17.408ms | 19.284ms | 14.538ms | 32.6ms | 5 |
| IdentityProviderService | StartSamlLogin | start_saml_login | startSamlLogin | mutation | OK | 3.703ms | 4.291ms | 3.709ms | 2.678ms | 4.472ms | 5 |
| IdentityProviderService | TestProviderDiscovery | test_provider_discovery | testProviderDiscovery | read_only | OK | 4.344ms | 5.007ms | 4.369ms | 3.789ms | 6.347ms | 25 |
| IdentityProviderService | UnlinkIdentity | unlink_identity | unlinkIdentity | mutation | OK | 4.433ms | 5.134ms | 9.418ms | 3.381ms | 30.01ms | 5 |
| IdentityProviderService | UpdateProvider | update_provider | updateProvider | mutation | OK | 25.509ms | 30.713ms | 24.198ms | 13.558ms | 36.413ms | 5 |
| NotificationService | GetDeliveryStats | get_delivery_stats | getDeliveryStats | read_only | OK | 4.546ms | 12.263ms | 6.646ms | 3.061ms | 12.69ms | 25 |
| NotificationService | GetNotification | get_notification | getNotification | read_only | OK | 6.674ms | 8.682ms | 6.877ms | 5.504ms | 9.201ms | 25 |
| NotificationService | GetPreference | get_preference | getPreference | read_only | OK | 6.152ms | 7.489ms | 6.149ms | 4.792ms | 8.021ms | 25 |
| NotificationService | GetTemplate | get_template | getTemplate | read_only | OK | 7.046ms | 13.18ms | 7.958ms | 5.45ms | 15.827ms | 25 |
| NotificationService | ListNotifications | list_notifications | listNotifications | read_only | OK | 13.211ms | 16.263ms | 13.33ms | 9.314ms | 17.131ms | 25 |
| NotificationService | ListPreferences | list_preferences | listPreferences | read_only | OK | 10.949ms | 14.427ms | 11.315ms | 9.15ms | 14.434ms | 25 |
| NotificationService | ListTemplates | list_templates | listTemplates | read_only | OK | 11.981ms | 16.416ms | 11.944ms | 8.734ms | 16.573ms | 25 |
| NotificationService | RetryNotification | retry_notification | retryNotification | mutation | OK | 23.283ms | 23.283ms | 23.283ms | 23.283ms | 23.283ms | 5 |
| NotificationService | SendNotification | send_notification | sendNotification | mutation | OK | 43.121ms | 55.793ms | 47.416ms | 39.795ms | 58.18ms | 5 |
| NotificationService | SetPreference | set_preference | setPreference | mutation | OK | 7.136ms | 7.422ms | 6.859ms | 4.883ms | 9.32ms | 5 |
| NotificationService | UpsertTemplate | upsert_template | upsertTemplate | mutation | OK | 8.162ms | 25.88ms | 21.697ms | 6.201ms | 61.39ms | 5 |
| PeerService | GetPeer | get_peer | getPeer | read_only | OK | 7.708ms | 11.106ms | 8.4ms | 6.07ms | 13.283ms | 25 |
| PeerService | JoinRoom | join_room | joinRoom | mutation | OK | 35.045ms | 37.172ms | 30.964ms | 19.99ms | 40.317ms | 5 |
| PeerService | JoinSession | join_session | joinSession | mutation | OK | 19.49ms | 21.002ms | 22.412ms | 16.999ms | 36.267ms | 5 |
| PeerService | LeaveRoom | leave_room | leaveRoom | mutation | OK | 7.068ms | 7.249ms | 7.839ms | 5.279ms | 13.687ms | 5 |
| PeerService | ListPeers | list_peers | listPeers | read_only | OK | 8.162ms | 9.058ms | 7.913ms | 6.053ms | 9.391ms | 25 |
| RoomService | CloseRoom | close_room | closeRoom | mutation | OK | 26.027ms | 27.928ms | 26.182ms | 17.119ms | 34.541ms | 5 |
| RoomService | CreateRoom | create_room | createRoom | mutation | OK | 12.793ms | 27.704ms | 19.832ms | 8.868ms | 40.473ms | 5 |
| RoomService | GetRoom | get_room | getRoom | read_only | OK | 7.975ms | 11.551ms | 8.049ms | 5.827ms | 12.635ms | 25 |
| RoomService | ListRooms | list_rooms | listRooms | read_only | OK | 5.965ms | 8.609ms | 6.334ms | 4.578ms | 10.004ms | 25 |
| RoomService | UpdateRoom | update_room | updateRoom | mutation | OK | 5.253ms | 5.331ms | 8.09ms | 4.535ms | 20.129ms | 5 |
| SignalingService | Signal | signal | signal | mutation | OK | 22.865ms | 22.865ms | 22.865ms | 22.865ms | 22.865ms | 5 |
| StorageService | DeleteFile | delete_file | deleteFile | mutation | OK | 29.376ms | 29.376ms | 29.376ms | 29.376ms | 29.376ms | 5 |
| StorageService | DownloadFile | download_file | downloadFile | read_only | OK | 18.254ms | 22.535ms | 18.319ms | 14.203ms | 23.724ms | 25 |
| StorageService | FinalizeUpload | finalize_upload | finalizeUpload | mutation | OK | 37.112ms | 37.112ms | 37.112ms | 37.112ms | 37.112ms | 5 |
| StorageService | GetDownloadUrl | get_download_url | getDownloadUrl | read_only | OK | 9.962ms | 12.72ms | 10.244ms | 8.189ms | 12.759ms | 25 |
| StorageService | GetFile | get_file | getFile | read_only | OK | 8.654ms | 11.148ms | 8.723ms | 6.357ms | 11.769ms | 25 |
| StorageService | ListFiles | list_files | listFiles | read_only | OK | 16.869ms | 24.356ms | 17.423ms | 12.262ms | 26.169ms | 25 |
| StorageService | RegisterUpload | register_upload | registerUpload | mutation | OK | 18.272ms | 19.99ms | 18.29ms | 15.89ms | 20.093ms | 5 |
| StorageService | UpdateFile | update_file | updateFile | mutation | OK | 25.468ms | 26.604ms | 27.423ms | 20.482ms | 40.532ms | 5 |
| TenantService | CreateTenant | create_tenant | createTenant | mutation | OK | 10.171ms | 10.562ms | 10.066ms | 8.669ms | 11.945ms | 5 |
| TenantService | GetTenant | get_tenant | getTenant | read_only | OK | 6.979ms | 8.886ms | 7.47ms | 5.08ms | 11.337ms | 25 |
| TenantService | GetTenantConfig | get_tenant_config | getTenantConfig | read_only | OK | 7.513ms | 10.364ms | 7.568ms | 5.292ms | 10.664ms | 25 |
| TenantService | ListTenants | list_tenants | listTenants | read_only | OK | 7.203ms | 10.128ms | 7.38ms | 4.261ms | 10.273ms | 25 |
| TenantService | UpdateTenant | update_tenant | updateTenant | mutation | OK | 8.114ms | 8.763ms | 10.85ms | 7.294ms | 22.572ms | 5 |
| TenantService | UpdateTenantConfig | update_tenant_config | updateTenantConfig | mutation | OK | 26.05ms | 28.792ms | 25.533ms | 19.03ms | 33.518ms | 5 |
| TrackService | ListTracks | list_tracks | listTracks | read_only | OK | 7.821ms | 13.755ms | 8.319ms | 5.399ms | 22.635ms | 25 |
| TrackService | MuteTrack | mute_track | muteTrack | mutation | OK | 4.956ms | 6.004ms | 6.903ms | 3.986ms | 15.017ms | 5 |
| TrackService | PublishTrack | publish_track | publishTrack | mutation | OK | 13.133ms | 13.544ms | 15.292ms | 10.646ms | 27.574ms | 5 |
| TrackService | UnpublishTrack | unpublish_track | unpublishTrack | mutation | OK | 5.221ms | 5.624ms | 8.313ms | 4.32ms | 21.308ms | 5 |
| TurnService | IssueCredentials | issue_credentials | issueCredentials | mutation | OK | 3.834ms | 3.865ms | 3.683ms | 3.021ms | 4.268ms | 5 |
