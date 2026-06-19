# UDB SDK Live Perf — PHP (Docker → host)

RPCs measured: 265   tenant=60bf53e2-977e-4797-830d-a91a87853714

Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities (a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request's reference/ID fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection latency. The TARGET is zero failures; any residual non-OK RPC is listed under Failures for the maintainer to finish.

Unary = full request/response round-trip. Streaming rows (kind=stream_open) report stream-open latency (initiate + cancel, no response drain), NOT first-message latency.

## Seeded fixtures

Captured semantic field -> seeded value keys used to resolve request fields: action, apply_run_id, approval_token, approve_draft_id, approve_run_id, approved_by, asset_id, assigned_by, auth_challenge_id, canary_id, canary_version_id, catalog_manifest, challenge_id, close_room_id, code, content_type, created_by, csrf_token, definition_id, delete_file_id, delete_policy_id, delete_role_id, delete_scim_user_id, deleted_by, device_id, disable_provider_id, dismiss_dlq_id, dlq_id, domain, ds_policy_id, event_type, external_identity_id, file_id, file_type, filename, finalize_file_id, gov_exp, instance_id, join_session_room_id, key_id, key_prefix, kind, leave_peer_id, locale, log_id, mark_saga_id, message_type, migration_id, name, node_id, notification_id, object, otp_code, otp_id, owner_id, peer_id, plain_key, policy_draft_id, policy_id, policy_version_id, project, project_id, provider_id, quarantine_dlq_id, recipient_id, record_id, recovery_code, refresh_token, reg_challenge_id, reject_draft_id, rejected_by, relation, replay_dlq_id, reset_otp_code, reset_otp_id, resource, retry_saga_id, revoke_key_id, revoke_key_prefix, revoked_by, role, role_code, role_id, rollback_policy_set_id, rollback_target_version_id, room_id, saga_id, saml_provider_id, scim_group_id, scim_user_id, session_id, stage_name, step_id, subject, tenant, tenant_id, token, track_id, ts_table, unpublish_track_id, update_draft_id, update_key_id, update_key_prefix, updated_by, user_id

## Per-service mean latency

| Service | RPCs | mean ms |
|---|--:|--:|
| AuthnService | 50 | 111.70 |
| AuthzService | 41 | 46.74 |
| StorageService | 8 | 40.76 |
| DataBroker | 77 | 34.26 |
| ApiKeyService | 9 | 30.14 |
| PeerService | 5 | 29.41 |
| IdentityProviderService | 27 | 29.16 |
| NotificationService | 11 | 26.35 |
| RoomService | 5 | 23.07 |
| AssetService | 8 | 22.37 |
| TenantService | 6 | 20.29 |
| TrackService | 4 | 17.30 |
| ControlPlaneService | 5 | 14.73 |
| AnalyticsService | 7 | 11.27 |
| TurnService | 1 | 8.21 |
| SignalingService | 1 | 0.47 |

## Failures (0)

No RPC returned a non-OK gRPC status.

## Slowest 20 by p99

| RPC | kind | err | p50 ms | p99 ms | mean ms |
|---|---|---|--:|--:|--:|
| AuthnService/ChangePassword | mutation | OK | 1361.18 | 1361.18 | 1361.18 |
| AuthnService/Login | mutation | OK | 889.81 | 974.88 | 876.67 |
| AuthnService/CreateUser | mutation | OK | 803.66 | 957.96 | 868.82 |
| AuthnService/ResetPassword | mutation | OK | 865.50 | 865.50 | 865.50 |
| DataBroker/StageCatalog | destructive | OK | 606.08 | 606.08 | 606.08 |
| DataBroker/ApplyMigration | mutation | OK | 320.78 | 320.78 | 320.78 |
| DataBroker/ValidateCatalog | destructive | OK | 313.15 | 313.15 | 313.15 |
| AuthzService/PromoteCanary | destructive | OK | 267.30 | 267.30 | 267.30 |
| AuthnService/FinishWebAuthnAuthentication | mutation | OK | 166.39 | 166.39 | 166.39 |
| DataBroker/ActivateCatalog | destructive | OK | 161.14 | 161.14 | 161.14 |
| StorageService/FinalizeUpload | mutation | OK | 147.63 | 147.63 | 147.63 |
| AuthzService/RollbackPolicyVersion | destructive | OK | 141.43 | 141.43 | 141.43 |
| AuthzService/MigrateLegacyPolicies | destructive | OK | 130.01 | 130.01 | 130.01 |
| AuthnService/EmergencyRevoke | destructive | OK | 127.69 | 127.69 | 127.69 |
| AuthnService/GenerateRecoveryCodes | mutation | OK | 107.40 | 126.28 | 130.61 |
| DataBroker/Upsert | mutation | OK | 56.62 | 120.90 | 78.62 |
| NotificationService/SendNotification | mutation | OK | 115.78 | 116.34 | 90.24 |
| IdentityProviderService/SamlAcs | mutation | OK | 108.65 | 114.77 | 129.51 |
| AuthzService/Authorize | read_only | OK | 36.63 | 109.52 | 45.08 |
| AuthzService/RejectPolicyDraft | mutation | OK | 108.77 | 108.77 | 108.77 |

## Full per-RPC table (sorted by service, then RPC)

| Service | RPC | kind | err | p50 ms | p99 ms | mean ms | iters |
|---|---|---|---|--:|--:|--:|--:|
| AnalyticsService | GetExecutorPerformance | read_only | OK | 9.20 | 13.88 | 9.53 | 25 |
| AnalyticsService | GetPipelineSummary | read_only | OK | 8.62 | 15.47 | 9.67 | 25 |
| AnalyticsService | GetReconciliationAnalytics | read_only | OK | 7.26 | 15.53 | 8.39 | 25 |
| AnalyticsService | GetSlaCompliance | read_only | OK | 13.90 | 29.69 | 14.97 | 25 |
| AnalyticsService | GetThroughput | read_only | OK | 7.97 | 67.08 | 13.56 | 25 |
| AnalyticsService | RecordPipelineMetric | mutation | OK | 12.01 | 12.32 | 13.44 | 5 |
| AnalyticsService | TriggerSnapshot | mutation | OK | 8.24 | 11.36 | 9.34 | 5 |
| ApiKeyService | CreateApiKey | mutation | OK | 24.31 | 32.68 | 33.86 | 5 |
| ApiKeyService | EmergencyRevokeApiKeys | destructive | OK | 73.10 | 73.10 | 73.10 | 1 |
| ApiKeyService | GetApiKey | read_only | OK | 8.31 | 15.54 | 9.91 | 25 |
| ApiKeyService | GetApiKeyUsageStats | read_only | OK | 8.40 | 11.64 | 8.84 | 25 |
| ApiKeyService | ListApiKeys | read_only | OK | 7.33 | 15.60 | 8.33 | 25 |
| ApiKeyService | RevokeApiKey | mutation | OK | 37.31 | 37.31 | 37.31 | 1 |
| ApiKeyService | RotateApiKey | mutation | OK | 34.24 | 34.24 | 34.24 | 1 |
| ApiKeyService | UpdateApiKey | mutation | OK | 30.23 | 30.81 | 40.45 | 5 |
| ApiKeyService | ValidateApiKey | read_only | OK | 15.12 | 79.42 | 25.26 | 25 |
| AssetService | CompleteStep | mutation | OK | 38.28 | 45.89 | 50.26 | 5 |
| AssetService | CreatePipelineDefinition | mutation | OK | 23.05 | 23.05 | 23.05 | 1 |
| AssetService | GetAsset | read_only | OK | 12.44 | 31.73 | 15.20 | 25 |
| AssetService | GetPipeline | read_only | OK | 10.51 | 15.19 | 11.41 | 25 |
| AssetService | GetPipelineDefinition | read_only | OK | 11.31 | 15.07 | 11.97 | 25 |
| AssetService | ListAssets | read_only | OK | 14.68 | 23.72 | 16.42 | 25 |
| AssetService | RegisterAsset | mutation | OK | 22.37 | 27.98 | 26.23 | 5 |
| AssetService | StartPipeline | mutation | OK | 7.11 | 10.13 | 24.38 | 5 |
| AuthnService | AdminResetMfa | destructive | OK | 61.76 | 61.76 | 61.76 | 1 |
| AuthnService | AdminResetPassword | destructive | OK | 35.43 | 35.43 | 35.43 | 1 |
| AuthnService | AdminRevokeAllTenantSessions | destructive | OK | 87.27 | 87.27 | 87.27 | 1 |
| AuthnService | AdminRevokeAllUserSessions | destructive | OK | 78.12 | 78.12 | 78.12 | 1 |
| AuthnService | AdminRevokeSession | destructive | OK | 20.41 | 20.41 | 20.41 | 1 |
| AuthnService | Authenticate | read_only | OK | 23.00 | 36.97 | 25.79 | 25 |
| AuthnService | ChangePassword | mutation | OK | 1361.18 | 1361.18 | 1361.18 | 1 |
| AuthnService | ChangeUserStatus | destructive | OK | 54.47 | 54.47 | 54.47 | 1 |
| AuthnService | ConfirmMFAEnrollment | mutation | OK | 11.58 | 13.55 | 11.53 | 5 |
| AuthnService | CreateSession | mutation | OK | 21.88 | 22.32 | 20.55 | 5 |
| AuthnService | CreateUser | mutation | OK | 803.66 | 957.96 | 868.82 | 5 |
| AuthnService | DeleteWebAuthnCredential | mutation | OK | 19.25 | 20.32 | 27.05 | 5 |
| AuthnService | DisableMfaFactor | mutation | OK | 21.85 | 28.23 | 24.96 | 5 |
| AuthnService | EmergencyRevoke | destructive | OK | 127.69 | 127.69 | 127.69 | 1 |
| AuthnService | EnrollMFA | mutation | OK | 30.08 | 34.43 | 31.09 | 5 |
| AuthnService | FinishWebAuthnAuthentication | mutation | OK | 166.39 | 166.39 | 166.39 | 1 |
| AuthnService | FinishWebAuthnRegistration | mutation | OK | 50.49 | 50.49 | 50.49 | 1 |
| AuthnService | ForgotPassword | mutation | OK | 11.34 | 11.70 | 11.34 | 5 |
| AuthnService | GenerateRecoveryCodes | mutation | OK | 107.40 | 126.28 | 130.61 | 5 |
| AuthnService | GetJwks | read_only | OK | 7.26 | 9.33 | 7.05 | 25 |
| AuthnService | GetMfaPolicy | read_only | OK | 6.17 | 8.80 | 6.48 | 25 |
| AuthnService | GetSession | read_only | OK | 8.21 | 18.74 | 10.22 | 25 |
| AuthnService | GetUser | read_only | OK | 7.85 | 11.92 | 7.67 | 25 |
| AuthnService | IntrospectToken | read_only | OK | 27.31 | 40.59 | 29.34 | 25 |
| AuthnService | IssueMfaChallenge | mutation | OK | 20.89 | 22.32 | 19.92 | 5 |
| AuthnService | ListDevices | read_only | OK | 6.89 | 11.42 | 7.70 | 25 |
| AuthnService | ListMfaFactors | read_only | OK | 9.77 | 19.95 | 13.18 | 25 |
| AuthnService | ListSessions | read_only | OK | 13.27 | 21.66 | 14.11 | 25 |
| AuthnService | ListUsers | read_only | OK | 10.18 | 14.13 | 10.69 | 25 |
| AuthnService | ListWebAuthnCredentials | read_only | OK | 7.59 | 9.70 | 9.39 | 25 |
| AuthnService | Login | mutation | OK | 889.81 | 974.88 | 876.67 | 5 |
| AuthnService | Logout | mutation | OK | 24.77 | 28.61 | 21.78 | 5 |
| AuthnService | PutMfaPolicy | mutation | OK | 11.14 | 11.40 | 10.18 | 5 |
| AuthnService | RefreshSession | mutation | OK | 23.43 | 33.40 | 28.05 | 5 |
| AuthnService | RefreshToken | mutation | OK | 16.56 | 16.56 | 16.56 | 1 |
| AuthnService | RenamePasskey | mutation | OK | 13.30 | 17.80 | 27.45 | 5 |
| AuthnService | ResendOTP | mutation | OK | 33.87 | 38.56 | 44.85 | 5 |
| AuthnService | ResetPassword | mutation | OK | 865.50 | 865.50 | 865.50 | 1 |
| AuthnService | RevokeDevice | mutation | OK | 34.29 | 34.29 | 34.29 | 1 |
| AuthnService | RevokeRecoveryCodes | mutation | OK | 17.31 | 25.12 | 22.24 | 5 |
| AuthnService | RevokeSession | mutation | OK | 22.57 | 26.90 | 34.10 | 5 |
| AuthnService | SendOTP | mutation | OK | 28.20 | 30.45 | 51.12 | 5 |
| AuthnService | SendPhoneVerification | mutation | OK | 20.50 | 35.19 | 36.53 | 5 |
| AuthnService | StartWebAuthnAuthentication | mutation | OK | 24.55 | 87.03 | 48.61 | 5 |
| AuthnService | StartWebAuthnRegistration | mutation | OK | 27.21 | 32.98 | 37.95 | 5 |
| AuthnService | UpdateUser | mutation | OK | 18.16 | 20.43 | 31.65 | 5 |
| AuthnService | ValidateCSRF | read_only | OK | 7.81 | 9.43 | 7.62 | 25 |
| AuthnService | ValidateToken | read_only | OK | 22.31 | 32.45 | 22.82 | 25 |
| AuthnService | VerifyMfaChallenge | read_only | OK | 10.17 | 14.54 | 10.79 | 25 |
| AuthnService | VerifyOTP | read_only | OK | 22.06 | 41.42 | 25.39 | 25 |
| AuthzService | ActivateCanary | destructive | OK | 42.89 | 42.89 | 42.89 | 1 |
| AuthzService | ActivatePolicyVersion | destructive | OK | 100.89 | 100.89 | 100.89 | 1 |
| AuthzService | ApprovePolicyDraft | mutation | OK | 53.75 | 53.75 | 53.75 | 1 |
| AuthzService | AssignRole | mutation | OK | 48.93 | 55.52 | 61.49 | 5 |
| AuthzService | Authorize | read_only | OK | 36.63 | 109.52 | 45.08 | 25 |
| AuthzService | BatchCheckPermissions | read_only | OK | 16.55 | 28.33 | 17.60 | 25 |
| AuthzService | CheckAccess | read_only | OK | 17.17 | 24.97 | 18.31 | 25 |
| AuthzService | CreatePolicyDraft | mutation | OK | 57.00 | 64.87 | 89.52 | 5 |
| AuthzService | CreatePolicyRule | mutation | OK | 25.43 | 29.33 | 34.91 | 5 |
| AuthzService | CreateRole | mutation | OK | 50.52 | 66.53 | 73.07 | 5 |
| AuthzService | DeletePolicyRule | mutation | OK | 13.39 | 14.41 | 14.36 | 5 |
| AuthzService | DeleteRole | mutation | OK | 14.80 | 15.94 | 35.51 | 5 |
| AuthzService | DiffPolicyDraft | read_only | OK | 17.55 | 32.39 | 22.46 | 25 |
| AuthzService | ExplainPolicy | read_only | OK | 13.12 | 16.46 | 13.53 | 25 |
| AuthzService | GetAuthzRevision | read_only | OK | 7.94 | 13.80 | 8.69 | 25 |
| AuthzService | GetCanaryStatus | read_only | OK | 15.08 | 19.64 | 15.19 | 25 |
| AuthzService | GetNativeAccess | read_only | OK | 33.06 | 44.97 | 34.42 | 25 |
| AuthzService | GetPolicyBundle | read_only | OK | 14.12 | 54.07 | 20.32 | 25 |
| AuthzService | GetPolicyRule | read_only | OK | 7.07 | 10.96 | 7.63 | 25 |
| AuthzService | GetRole | read_only | OK | 7.17 | 16.18 | 9.40 | 25 |
| AuthzService | InvalidatePolicyBundles | destructive | OK | 50.89 | 50.89 | 50.89 | 1 |
| AuthzService | LintAuthzPolicies | read_only | OK | 4.50 | 6.44 | 4.50 | 25 |
| AuthzService | ListAccessDecisionAudits | read_only | OK | 13.97 | 22.19 | 18.27 | 25 |
| AuthzService | ListPolicyRules | read_only | OK | 8.31 | 11.05 | 8.19 | 25 |
| AuthzService | ListPolicyVersions | read_only | OK | 15.29 | 28.25 | 16.81 | 25 |
| AuthzService | ListRoles | read_only | OK | 8.64 | 75.83 | 17.96 | 25 |
| AuthzService | ListUserPermissions | read_only | OK | 4.22 | 5.64 | 4.38 | 25 |
| AuthzService | ListUserRoles | read_only | OK | 7.26 | 14.00 | 8.41 | 25 |
| AuthzService | MigrateLegacyPolicies | destructive | OK | 130.01 | 130.01 | 130.01 | 1 |
| AuthzService | PromoteCanary | destructive | OK | 267.30 | 267.30 | 267.30 | 1 |
| AuthzService | PutAuthzPolicy | mutation | OK | 35.27 | 35.73 | 44.39 | 5 |
| AuthzService | PutRelationship | mutation | OK | 36.13 | 44.39 | 50.71 | 5 |
| AuthzService | PutRoleBinding | mutation | OK | 29.68 | 30.08 | 27.76 | 5 |
| AuthzService | RejectPolicyDraft | mutation | OK | 108.77 | 108.77 | 108.77 | 1 |
| AuthzService | RevokeRole | mutation | OK | 11.28 | 11.48 | 10.60 | 5 |
| AuthzService | RollbackPolicyVersion | destructive | OK | 141.43 | 141.43 | 141.43 | 1 |
| AuthzService | SeedBuiltinRoles | mutation | OK | 82.91 | 84.24 | 95.84 | 5 |
| AuthzService | SimulatePolicy | mutation | OK | 50.89 | 53.39 | 62.77 | 5 |
| AuthzService | SubmitPolicyDraft | mutation | OK | 30.58 | 30.58 | 30.58 | 1 |
| AuthzService | UpdatePolicyDraft | mutation | OK | 44.72 | 50.50 | 44.08 | 5 |
| AuthzService | UpdateRole | mutation | OK | 47.42 | 62.65 | 53.49 | 5 |
| ControlPlaneService | AckStatus | mutation | OK | 14.94 | 15.43 | 14.60 | 5 |
| ControlPlaneService | DeltaResources | stream_open | OK | 0.19 | 0.19 | 0.19 | 1 |
| ControlPlaneService | GetResources | read_only | OK | 9.41 | 19.55 | 11.96 | 25 |
| ControlPlaneService | ListNodeStates | read_only | OK | 46.16 | 57.53 | 46.44 | 25 |
| ControlPlaneService | StreamResources | stream_open | OK | 0.45 | 0.45 | 0.45 | 1 |
| DataBroker | ActivateCatalog | destructive | OK | 161.14 | 161.14 | 161.14 | 1 |
| DataBroker | AnalyticalQuery | read_only | OK | 12.33 | 16.10 | 12.01 | 25 |
| DataBroker | ApplyMigration | mutation | OK | 320.78 | 320.78 | 320.78 | 1 |
| DataBroker | ApproveMigrationPlan | mutation | OK | 34.95 | 34.95 | 34.95 | 1 |
| DataBroker | BatchSelect | stream_open | OK | 0.21 | 0.21 | 0.21 | 1 |
| DataBroker | BatchUpsert | stream_open | OK | 0.21 | 0.21 | 0.21 | 1 |
| DataBroker | BeginTx | stream_open | OK | 0.51 | 0.51 | 0.51 | 1 |
| DataBroker | CacheDelete | mutation | OK | 9.12 | 9.89 | 9.78 | 5 |
| DataBroker | CacheGet | read_only | OK | 9.58 | 33.48 | 13.60 | 25 |
| DataBroker | CacheScan | read_only | OK | 16.98 | 32.09 | 18.45 | 25 |
| DataBroker | CacheSet | mutation | OK | 9.40 | 11.20 | 10.27 | 5 |
| DataBroker | CreateMaterializedView | mutation | OK | 11.11 | 11.38 | 10.81 | 5 |
| DataBroker | Delete | mutation | OK | 36.56 | 41.69 | 55.24 | 5 |
| DataBroker | DeletePolicy | mutation | OK | 13.46 | 13.46 | 13.46 | 1 |
| DataBroker | DismissDlqEvent | mutation | OK | 23.70 | 26.10 | 25.30 | 5 |
| DataBroker | DocumentDelete | mutation | OK | 11.33 | 12.08 | 11.78 | 5 |
| DataBroker | DocumentFind | read_only | OK | 8.56 | 11.03 | 8.78 | 25 |
| DataBroker | DocumentGet | read_only | OK | 9.41 | 11.43 | 9.19 | 25 |
| DataBroker | DocumentUpsert | mutation | OK | 10.40 | 13.16 | 17.11 | 5 |
| DataBroker | DropResource | destructive | OK | 33.73 | 33.73 | 33.73 | 1 |
| DataBroker | EnqueueOutboxEvent | mutation | OK | 14.06 | 16.67 | 15.50 | 5 |
| DataBroker | EnsureBaseline | mutation | OK | 17.18 | 17.64 | 20.10 | 5 |
| DataBroker | EnsureProject | mutation | OK | 18.56 | 19.23 | 16.97 | 5 |
| DataBroker | EnsureResource | mutation | OK | 33.90 | 37.84 | 33.12 | 5 |
| DataBroker | GeneratePresignedUrl | mutation | OK | 7.90 | 8.33 | 8.37 | 5 |
| DataBroker | GenericDispatch | mutation | OK | 14.98 | 16.71 | 16.03 | 5 |
| DataBroker | GetAdminSummary | read_only | OK | 30.91 | 55.31 | 34.89 | 25 |
| DataBroker | GetCapabilities | read_only | OK | 11.95 | 14.23 | 11.93 | 25 |
| DataBroker | GetCatalogManifest | read_only | OK | 69.87 | 90.37 | 70.13 | 25 |
| DataBroker | GetCatalogVersion | read_only | OK | 7.53 | 10.32 | 7.66 | 25 |
| DataBroker | GetCatalogVersions | read_only | OK | 7.30 | 9.03 | 7.33 | 25 |
| DataBroker | GetCdcStatus | read_only | OK | 9.34 | 11.66 | 9.41 | 25 |
| DataBroker | GetDlqEvent | read_only | OK | 9.82 | 18.04 | 10.70 | 25 |
| DataBroker | GetHealthReport | read_only | OK | 8.58 | 54.06 | 14.55 | 25 |
| DataBroker | GetMigrationStatus | read_only | OK | 7.87 | 17.43 | 10.11 | 9 |
| DataBroker | GetObject | stream_open | OK | 0.46 | 0.46 | 0.46 | 1 |
| DataBroker | GetSaga | read_only | OK | 8.52 | 18.03 | 9.51 | 25 |
| DataBroker | GraphMutate | mutation | OK | 30.08 | 37.68 | 36.55 | 5 |
| DataBroker | GraphQuery | read_only | OK | 22.31 | 32.27 | 24.40 | 25 |
| DataBroker | InitiateMultipartUpload | mutation | OK | 16.21 | 21.27 | 19.59 | 5 |
| DataBroker | LintPolicies | read_only | OK | 7.73 | 10.64 | 8.11 | 25 |
| DataBroker | ListAdminAuditLogs | read_only | OK | 19.21 | 21.78 | 19.36 | 25 |
| DataBroker | ListDlqEvents | read_only | OK | 10.28 | 16.30 | 10.97 | 25 |
| DataBroker | ListMessageSchemas | read_only | OK | 5.02 | 7.04 | 5.23 | 25 |
| DataBroker | ListMigrationRuns | read_only | OK | 8.20 | 10.22 | 8.17 | 25 |
| DataBroker | ListPolicies | read_only | OK | 8.38 | 11.86 | 8.53 | 25 |
| DataBroker | ListProjects | read_only | OK | 6.77 | 9.53 | 7.11 | 25 |
| DataBroker | ListResources | read_only | OK | 8.36 | 10.37 | 8.45 | 25 |
| DataBroker | ListSagas | read_only | OK | 8.12 | 10.83 | 8.01 | 25 |
| DataBroker | LookupMessageSchema | read_only | OK | 4.86 | 6.48 | 5.03 | 25 |
| DataBroker | MarkSagaReviewed | mutation | OK | 13.36 | 15.44 | 14.98 | 5 |
| DataBroker | PauseCdc | mutation | OK | 25.25 | 26.64 | 23.28 | 5 |
| DataBroker | PlanMigration | mutation | OK | 30.82 | 32.79 | 30.54 | 5 |
| DataBroker | PreviewCdcRedaction | read_only | OK | 17.17 | 21.36 | 17.50 | 25 |
| DataBroker | PublishCDC | stream_open | OK | 0.75 | 0.75 | 0.75 | 1 |
| DataBroker | PutObject | mutation | OK | 23.99 | 23.99 | 28.32 | 3 |
| DataBroker | PutPolicy | destructive | OK | 29.69 | 29.69 | 29.69 | 1 |
| DataBroker | QuarantineDlqEvent | mutation | OK | 24.22 | 24.50 | 23.28 | 5 |
| DataBroker | ReloadPolicies | destructive | OK | 35.00 | 35.00 | 35.00 | 1 |
| DataBroker | ReplayDlqEvent | mutation | OK | 33.66 | 33.66 | 33.66 | 1 |
| DataBroker | ResumeCdc | mutation | OK | 15.15 | 17.35 | 16.90 | 5 |
| DataBroker | RetrySagaCompensation | mutation | OK | 15.20 | 15.20 | 15.20 | 1 |
| DataBroker | RollbackCatalog | destructive | OK | 11.83 | 11.83 | 11.83 | 1 |
| DataBroker | ScanProjectionDrift | read_only | OK | 19.22 | 37.33 | 22.75 | 25 |
| DataBroker | Select | read_only | OK | 11.54 | 15.40 | 11.71 | 25 |
| DataBroker | SelectV2 | stream_open | OK | 0.47 | 0.47 | 0.47 | 1 |
| DataBroker | StageCatalog | destructive | OK | 606.08 | 606.08 | 606.08 | 1 |
| DataBroker | StepDownCdcLeader | mutation | OK | 15.48 | 17.29 | 17.03 | 5 |
| DataBroker | TimeSeriesQuery | read_only | OK | 14.89 | 21.56 | 15.27 | 25 |
| DataBroker | TimeSeriesWrite | mutation | OK | 6.25 | 7.24 | 6.41 | 5 |
| DataBroker | Upsert | mutation | OK | 56.62 | 120.90 | 78.62 | 5 |
| DataBroker | ValidateCatalog | destructive | OK | 313.15 | 313.15 | 313.15 | 1 |
| DataBroker | VectorBatchUpsert | stream_open | OK | 0.32 | 0.32 | 0.32 | 1 |
| DataBroker | VectorHybridSearch | read_only | OK | 9.86 | 12.38 | 10.07 | 25 |
| DataBroker | VectorSearch | read_only | OK | 10.56 | 13.06 | 10.73 | 25 |
| DataBroker | VectorUpsert | mutation | OK | 18.36 | 19.99 | 19.23 | 5 |
| DataBroker | VerifyAdminAuditLog | read_only | OK | 10.88 | 18.20 | 11.75 | 25 |
| IdentityProviderService | CreateProvider | mutation | OK | 23.29 | 24.33 | 24.06 | 5 |
| IdentityProviderService | DisableProvider | mutation | OK | 27.79 | 32.99 | 40.31 | 5 |
| IdentityProviderService | ForceJwksRefresh | mutation | OK | 32.22 | 36.76 | 38.89 | 5 |
| IdentityProviderService | GetProvider | read_only | OK | 7.60 | 11.54 | 8.19 | 25 |
| IdentityProviderService | ImportSamlMetadata | mutation | OK | 29.13 | 101.16 | 56.17 | 5 |
| IdentityProviderService | LinkIdentity | mutation | OK | 35.89 | 37.26 | 41.46 | 5 |
| IdentityProviderService | ListExternalIdentities | read_only | OK | 11.56 | 19.08 | 13.77 | 25 |
| IdentityProviderService | ListProviders | read_only | OK | 14.73 | 27.32 | 17.58 | 25 |
| IdentityProviderService | PreviewClaimMapping | read_only | OK | 8.26 | 9.71 | 8.17 | 25 |
| IdentityProviderService | PreviewGroupMapping | read_only | OK | 6.78 | 9.79 | 7.19 | 25 |
| IdentityProviderService | ResolveExternalIdentity | mutation | OK | 10.84 | 11.26 | 27.72 | 5 |
| IdentityProviderService | SamlAcs | mutation | OK | 108.65 | 114.77 | 129.51 | 5 |
| IdentityProviderService | ScimCreateGroup | mutation | OK | 7.42 | 9.96 | 14.93 | 5 |
| IdentityProviderService | ScimCreateUser | mutation | OK | 52.87 | 65.47 | 59.50 | 5 |
| IdentityProviderService | ScimDeleteGroup | mutation | OK | 6.61 | 7.68 | 6.81 | 5 |
| IdentityProviderService | ScimDeleteUser | mutation | OK | 55.52 | 55.52 | 55.52 | 1 |
| IdentityProviderService | ScimGetGroup | mutation | OK | 10.09 | 10.57 | 10.78 | 5 |
| IdentityProviderService | ScimGetUser | mutation | OK | 9.72 | 9.83 | 10.01 | 5 |
| IdentityProviderService | ScimListGroups | mutation | OK | 8.49 | 9.33 | 8.59 | 5 |
| IdentityProviderService | ScimListUsers | mutation | OK | 16.75 | 17.23 | 17.23 | 5 |
| IdentityProviderService | ScimPatchGroup | mutation | OK | 18.73 | 21.32 | 28.24 | 5 |
| IdentityProviderService | ScimPatchUser | mutation | OK | 45.06 | 49.29 | 55.35 | 5 |
| IdentityProviderService | ScimReplaceUser | mutation | OK | 26.68 | 35.49 | 40.11 | 5 |
| IdentityProviderService | StartSamlLogin | mutation | OK | 7.36 | 7.37 | 7.72 | 5 |
| IdentityProviderService | TestProviderDiscovery | read_only | OK | 8.91 | 10.86 | 9.01 | 25 |
| IdentityProviderService | UnlinkIdentity | mutation | OK | 9.16 | 9.45 | 10.20 | 5 |
| IdentityProviderService | UpdateProvider | mutation | OK | 30.96 | 30.98 | 40.18 | 5 |
| NotificationService | GetDeliveryStats | read_only | OK | 11.05 | 16.60 | 11.85 | 25 |
| NotificationService | GetNotification | read_only | OK | 16.20 | 73.66 | 23.99 | 25 |
| NotificationService | GetPreference | read_only | OK | 12.78 | 26.42 | 15.89 | 25 |
| NotificationService | GetTemplate | read_only | OK | 16.13 | 65.28 | 20.82 | 25 |
| NotificationService | ListNotifications | read_only | OK | 24.68 | 38.99 | 24.89 | 25 |
| NotificationService | ListPreferences | read_only | OK | 22.85 | 34.40 | 26.34 | 25 |
| NotificationService | ListTemplates | read_only | OK | 24.01 | 30.91 | 24.39 | 25 |
| NotificationService | RetryNotification | mutation | OK | 30.65 | 30.65 | 30.65 | 1 |
| NotificationService | SendNotification | mutation | OK | 115.78 | 116.34 | 90.24 | 5 |
| NotificationService | SetPreference | mutation | OK | 9.75 | 9.88 | 9.71 | 5 |
| NotificationService | UpsertTemplate | mutation | OK | 11.39 | 12.57 | 11.02 | 5 |
| PeerService | GetPeer | read_only | OK | 10.57 | 15.52 | 11.27 | 25 |
| PeerService | JoinRoom | mutation | OK | 39.99 | 46.14 | 61.32 | 5 |
| PeerService | JoinSession | mutation | OK | 32.34 | 34.00 | 47.98 | 5 |
| PeerService | LeaveRoom | mutation | OK | 10.16 | 11.59 | 14.95 | 5 |
| PeerService | ListPeers | read_only | OK | 11.67 | 13.53 | 11.50 | 25 |
| RoomService | CloseRoom | mutation | OK | 35.42 | 36.16 | 44.58 | 5 |
| RoomService | CreateRoom | mutation | OK | 23.52 | 24.19 | 33.91 | 5 |
| RoomService | GetRoom | read_only | OK | 12.66 | 22.98 | 13.25 | 25 |
| RoomService | ListRooms | read_only | OK | 10.11 | 14.08 | 13.17 | 25 |
| RoomService | UpdateRoom | mutation | OK | 9.49 | 12.49 | 10.44 | 5 |
| SignalingService | Signal | stream_open | OK | 0.47 | 0.47 | 0.47 | 1 |
| StorageService | DeleteFile | mutation | OK | 34.04 | 34.04 | 34.04 | 1 |
| StorageService | DownloadFile | stream_open | OK | 0.99 | 0.99 | 0.99 | 1 |
| StorageService | FinalizeUpload | mutation | OK | 147.63 | 147.63 | 147.63 | 1 |
| StorageService | GetDownloadUrl | read_only | OK | 17.15 | 22.27 | 16.94 | 25 |
| StorageService | GetFile | read_only | OK | 12.03 | 18.80 | 14.48 | 25 |
| StorageService | ListFiles | read_only | OK | 20.80 | 28.71 | 21.60 | 25 |
| StorageService | RegisterUpload | mutation | OK | 20.95 | 27.95 | 45.71 | 5 |
| StorageService | UpdateFile | mutation | OK | 32.37 | 35.03 | 44.72 | 5 |
| TenantService | CreateTenant | mutation | OK | 19.41 | 22.33 | 20.14 | 5 |
| TenantService | GetTenant | read_only | OK | 11.56 | 28.71 | 16.64 | 25 |
| TenantService | GetTenantConfig | read_only | OK | 11.04 | 72.90 | 16.85 | 25 |
| TenantService | ListTenants | read_only | OK | 10.06 | 16.21 | 13.69 | 25 |
| TenantService | UpdateTenant | mutation | OK | 11.19 | 11.79 | 11.04 | 5 |
| TenantService | UpdateTenantConfig | mutation | OK | 32.88 | 39.91 | 43.37 | 5 |
| TrackService | ListTracks | read_only | OK | 10.52 | 13.35 | 10.95 | 25 |
| TrackService | MuteTrack | mutation | OK | 10.20 | 11.72 | 9.86 | 5 |
| TrackService | PublishTrack | mutation | OK | 30.01 | 30.01 | 36.99 | 5 |
| TrackService | UnpublishTrack | mutation | OK | 11.22 | 11.63 | 11.39 | 5 |
| TurnService | IssueCredentials | mutation | OK | 7.84 | 9.66 | 8.21 | 5 |
