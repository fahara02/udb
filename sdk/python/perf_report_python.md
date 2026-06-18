# UDB SDK Live Perf — Python (localhost)

RPCs measured: 265   tenant=c96679ef-9b6d-4e57-9883-fb6b692442ce

Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities (a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request's reference/ID fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection latency. The TARGET is zero failures; any residual non-OK RPC is listed under Failures for the maintainer to finish.

Unary = full request/response round-trip. Non-CDC streaming RPCs (kind=stream_first_recv) report time-to-FIRST-RESPONSE with seeded inputs. CDC subscription (kind=cdc_first_event, PublishCDC) reports time-to-FIRST-EVENT: the harness subscribes, fires a real Upsert that flows outbox->CDC->Kafka, and times the first delivered event.

## Seeded fixtures

Captured semantic field -> seeded value keys used to resolve request fields: access_token, action, apply_run_id, approval_token, approve_draft_id, approve_run_id, approved_by, asset_id, assigned_by, auth_challenge_id, bucket, canary_id, canary_version_id, catalog_manifest, challenge_id, close_room_id, code, collection, content_type, created_by, csrf_token, current_password, definition_id, delete_file_id, delete_policy_id, delete_scim_user_id, deleted_by, device_id, dismiss_dlq_id, dlq_id, document_id, domain, ds_policy_id, email, event_type, external_identity_id, file_id, file_size_bytes, file_type, filename, finalize_file_id, gov_exp, identifier, instance_id, join_session_room_id, key_id, kind, leave_peer_id, locale, log_id, mark_saga_id, message_type, migration_id, mongo_collection, name, new_password, notification_id, object, object_key, otp_code, otp_id, owner_id, password, peer_id, plain_key, policy_draft_id, policy_id, policy_version_id, project, project_id, provider_id, quarantine_dlq_id, recipient_id, record_id, recovery_code, refresh_token, reg_challenge_id, reject_draft_id, rejected_by, relation, replay_dlq_id, reset_otp_code, reset_otp_id, resource, retry_saga_id, revoke_key_id, revoked_by, role, role_code, role_id, rollback_policy_set_id, rollback_target_version_id, room_id, saga_id, saml_provider_id, scim_group_id, scim_user_id, send_otp_user_id, session_id, signal_peer_id, stage_name, step_id, subject, tenant, tenant_id, token, topic_pattern, track_id, ts_table, unpublish_track_id, update_draft_id, update_draft_updated_at_unix, update_key_id, updated_by, user_id, user_role_id, username, vector_collection

## Per-service mean latency

| Service | RPCs | mean ms |
|---|--:|--:|
| AuthnService | 50 | 88.87 |
| ControlPlaneService | 5 | 46.33 |
| DataBroker | 77 | 46.05 |
| AuthzService | 41 | 32.30 |
| StorageService | 8 | 26.86 |
| ApiKeyService | 9 | 24.00 |
| NotificationService | 11 | 20.35 |
| AssetService | 8 | 19.56 |
| PeerService | 5 | 18.63 |
| TenantService | 6 | 18.56 |
| IdentityProviderService | 27 | 15.83 |
| RoomService | 5 | 15.30 |
| SignalingService | 1 | 12.77 |
| TrackService | 4 | 12.39 |
| AnalyticsService | 7 | 10.91 |
| TurnService | 1 | 7.03 |

## Failures (0)

No RPC returned a non-OK gRPC status.

## Slowest 20 by p99

| RPC | kind | err | p50 ms | p99 ms | mean ms |
|---|---|---|--:|--:|--:|
| AuthnService/ChangePassword | mutation | OK | 1266.59 | 1266.59 | 1266.59 |
| DataBroker/StageCatalog | destructive | OK | 909.31 | 909.31 | 909.31 |
| AuthnService/ResetPassword | mutation | OK | 757.92 | 757.92 | 757.92 |
| AuthnService/CreateUser | mutation | OK | 712.53 | 712.53 | 726.87 |
| AuthnService/Login | mutation | OK | 677.62 | 677.62 | 691.48 |
| DataBroker/PreviewCdcRedaction | read_only | OK | 206.53 | 527.21 | 316.30 |
| DataBroker/ApplyMigration | mutation | OK | 292.43 | 292.43 | 292.43 |
| DataBroker/ScanProjectionDrift | read_only | OK | 220.20 | 270.34 | 187.66 |
| DataBroker/ActivateCatalog | destructive | OK | 178.11 | 178.11 | 178.11 |
| ControlPlaneService/DeltaResources | stream_first_recv | OK | 131.55 | 131.55 | 123.63 |
| AuthzService/ActivatePolicyVersion | destructive | OK | 128.18 | 128.18 | 128.18 |
| DataBroker/PublishCDC | cdc_first_event | OK | 113.34 | 113.34 | 113.34 |
| AuthzService/PromoteCanary | destructive | OK | 109.68 | 109.68 | 109.68 |
| AuthzService/RollbackPolicyVersion | destructive | OK | 91.13 | 91.13 | 91.13 |
| ApiKeyService/EmergencyRevokeApiKeys | destructive | OK | 84.28 | 84.28 | 84.28 |
| AuthzService/SeedBuiltinRoles | mutation | OK | 83.30 | 83.30 | 91.91 |
| ControlPlaneService/StreamResources | stream_first_recv | OK | 81.96 | 81.96 | 82.09 |
| AuthnService/FinishWebAuthnAuthentication | mutation | OK | 76.90 | 76.90 | 76.90 |
| AuthzService/ApprovePolicyDraft | mutation | OK | 71.71 | 71.71 | 71.71 |
| DataBroker/RetrySagaCompensation | mutation | OK | 70.82 | 70.82 | 70.82 |

## Full per-RPC table (sorted by service, then RPC)

| Service | RPC | kind | err | p50 ms | p99 ms | mean ms | iters |
|---|---|---|---|--:|--:|--:|--:|
| AnalyticsService | GetExecutorPerformance | read_only | OK | 14.87 | 17.16 | 14.64 | 10 |
| AnalyticsService | GetPipelineSummary | read_only | OK | 10.40 | 16.93 | 12.45 | 10 |
| AnalyticsService | GetReconciliationAnalytics | read_only | OK | 12.90 | 15.10 | 11.94 | 10 |
| AnalyticsService | GetSlaCompliance | read_only | OK | 9.72 | 12.03 | 10.10 | 10 |
| AnalyticsService | GetThroughput | read_only | OK | 9.70 | 13.56 | 10.86 | 10 |
| AnalyticsService | RecordPipelineMetric | mutation | OK | 9.44 | 9.44 | 8.66 | 3 |
| AnalyticsService | TriggerSnapshot | mutation | OK | 8.02 | 8.02 | 7.75 | 3 |
| ApiKeyService | CreateApiKey | mutation | OK | 15.11 | 15.11 | 16.39 | 3 |
| ApiKeyService | EmergencyRevokeApiKeys | destructive | OK | 84.28 | 84.28 | 84.28 | 1 |
| ApiKeyService | GetApiKey | read_only | OK | 10.51 | 12.67 | 10.58 | 10 |
| ApiKeyService | GetApiKeyUsageStats | read_only | OK | 12.61 | 16.74 | 15.17 | 10 |
| ApiKeyService | ListApiKeys | read_only | OK | 8.90 | 12.48 | 9.38 | 10 |
| ApiKeyService | RevokeApiKey | mutation | OK | 19.50 | 19.50 | 19.50 | 3 |
| ApiKeyService | RotateApiKey | mutation | OK | 26.04 | 26.04 | 26.04 | 3 |
| ApiKeyService | UpdateApiKey | mutation | OK | 16.17 | 16.17 | 16.68 | 3 |
| ApiKeyService | ValidateApiKey | read_only | OK | 17.75 | 20.29 | 18.03 | 10 |
| AssetService | CompleteStep | mutation | OK | 30.00 | 30.00 | 28.08 | 3 |
| AssetService | CreatePipelineDefinition | mutation | OK | 12.98 | 12.98 | 12.98 | 3 |
| AssetService | GetAsset | read_only | OK | 7.85 | 10.95 | 8.52 | 10 |
| AssetService | GetPipeline | read_only | OK | 9.90 | 11.73 | 9.70 | 10 |
| AssetService | GetPipelineDefinition | read_only | OK | 16.59 | 23.66 | 17.31 | 10 |
| AssetService | ListAssets | read_only | OK | 11.02 | 14.35 | 11.80 | 10 |
| AssetService | RegisterAsset | mutation | OK | 21.14 | 21.14 | 21.87 | 3 |
| AssetService | StartPipeline | mutation | OK | 49.20 | 49.20 | 46.19 | 3 |
| AuthnService | AdminResetMfa | destructive | OK | 30.02 | 30.02 | 30.02 | 1 |
| AuthnService | AdminResetPassword | destructive | OK | 13.17 | 13.17 | 13.17 | 1 |
| AuthnService | AdminRevokeAllTenantSessions | destructive | OK | 13.58 | 13.58 | 13.58 | 1 |
| AuthnService | AdminRevokeAllUserSessions | destructive | OK | 15.54 | 15.54 | 15.54 | 1 |
| AuthnService | AdminRevokeSession | destructive | OK | 16.23 | 16.23 | 16.23 | 1 |
| AuthnService | Authenticate | read_only | OK | 39.21 | 39.21 | 39.21 | 1 |
| AuthnService | ChangePassword | mutation | OK | 1266.59 | 1266.59 | 1266.59 | 1 |
| AuthnService | ChangeUserStatus | destructive | OK | 20.40 | 20.40 | 20.40 | 1 |
| AuthnService | ConfirmMFAEnrollment | mutation | OK | 5.67 | 5.67 | 6.05 | 3 |
| AuthnService | CreateSession | mutation | OK | 9.13 | 9.13 | 9.93 | 3 |
| AuthnService | CreateUser | mutation | OK | 712.53 | 712.53 | 726.87 | 3 |
| AuthnService | DeleteWebAuthnCredential | mutation | OK | 10.50 | 10.50 | 11.00 | 3 |
| AuthnService | DisableMfaFactor | mutation | OK | 18.98 | 18.98 | 19.15 | 3 |
| AuthnService | EmergencyRevoke | destructive | OK | 21.75 | 21.75 | 21.75 | 1 |
| AuthnService | EnrollMFA | mutation | OK | 20.01 | 20.01 | 19.70 | 3 |
| AuthnService | FinishWebAuthnAuthentication | mutation | OK | 76.90 | 76.90 | 76.90 | 3 |
| AuthnService | FinishWebAuthnRegistration | mutation | OK | 45.90 | 45.90 | 45.90 | 3 |
| AuthnService | ForgotPassword | mutation | OK | 29.39 | 29.39 | 29.92 | 3 |
| AuthnService | GenerateRecoveryCodes | mutation | OK | 47.56 | 47.56 | 46.09 | 3 |
| AuthnService | GetJwks | read_only | OK | 8.57 | 11.34 | 8.69 | 10 |
| AuthnService | GetMfaPolicy | read_only | OK | 5.73 | 6.80 | 6.05 | 10 |
| AuthnService | GetSession | read_only | OK | 6.43 | 13.28 | 8.30 | 10 |
| AuthnService | GetUser | read_only | OK | 4.97 | 6.89 | 5.43 | 10 |
| AuthnService | IntrospectToken | read_only | OK | 47.80 | 59.20 | 49.16 | 10 |
| AuthnService | IssueMfaChallenge | mutation | OK | 20.72 | 20.72 | 19.81 | 3 |
| AuthnService | ListDevices | read_only | OK | 7.23 | 7.99 | 7.52 | 10 |
| AuthnService | ListMfaFactors | read_only | OK | 8.06 | 10.84 | 8.72 | 10 |
| AuthnService | ListSessions | read_only | OK | 9.52 | 11.78 | 9.82 | 10 |
| AuthnService | ListUsers | read_only | OK | 7.73 | 10.15 | 8.09 | 10 |
| AuthnService | ListWebAuthnCredentials | read_only | OK | 5.75 | 7.30 | 6.11 | 10 |
| AuthnService | Login | mutation | OK | 677.62 | 677.62 | 691.48 | 3 |
| AuthnService | Logout | mutation | OK | 22.84 | 22.84 | 32.90 | 3 |
| AuthnService | PutMfaPolicy | mutation | OK | 20.81 | 20.81 | 23.92 | 3 |
| AuthnService | RefreshSession | mutation | OK | 47.62 | 47.62 | 47.62 | 1 |
| AuthnService | RefreshToken | mutation | OK | 21.36 | 21.36 | 21.36 | 1 |
| AuthnService | RenamePasskey | mutation | OK | 22.77 | 22.77 | 29.48 | 3 |
| AuthnService | ResendOTP | mutation | OK | 27.45 | 27.45 | 27.45 | 1 |
| AuthnService | ResetPassword | mutation | OK | 757.92 | 757.92 | 757.92 | 1 |
| AuthnService | RevokeDevice | mutation | OK | 25.13 | 25.13 | 25.13 | 3 |
| AuthnService | RevokeRecoveryCodes | mutation | OK | 12.98 | 12.98 | 13.97 | 3 |
| AuthnService | RevokeSession | mutation | OK | 9.84 | 9.84 | 11.28 | 3 |
| AuthnService | SendOTP | mutation | OK | 21.40 | 21.40 | 21.40 | 1 |
| AuthnService | SendPhoneVerification | mutation | OK | 19.57 | 19.57 | 20.48 | 3 |
| AuthnService | StartWebAuthnAuthentication | mutation | OK | 34.58 | 34.58 | 30.55 | 3 |
| AuthnService | StartWebAuthnRegistration | mutation | OK | 16.32 | 16.32 | 17.41 | 3 |
| AuthnService | UpdateUser | mutation | OK | 17.89 | 17.89 | 17.81 | 3 |
| AuthnService | ValidateCSRF | read_only | OK | 9.64 | 14.06 | 11.26 | 10 |
| AuthnService | ValidateToken | read_only | OK | 35.46 | 63.62 | 43.01 | 10 |
| AuthnService | VerifyMfaChallenge | read_only | OK | 13.27 | 13.27 | 13.27 | 1 |
| AuthnService | VerifyOTP | read_only | OK | 20.07 | 20.07 | 20.07 | 1 |
| AuthzService | ActivateCanary | destructive | OK | 69.35 | 69.35 | 69.35 | 1 |
| AuthzService | ActivatePolicyVersion | destructive | OK | 128.18 | 128.18 | 128.18 | 1 |
| AuthzService | ApprovePolicyDraft | mutation | OK | 71.71 | 71.71 | 71.71 | 3 |
| AuthzService | AssignRole | mutation | OK | 27.72 | 27.72 | 30.68 | 3 |
| AuthzService | Authorize | read_only | OK | 29.13 | 32.59 | 29.05 | 10 |
| AuthzService | BatchCheckPermissions | read_only | OK | 11.56 | 13.41 | 12.21 | 10 |
| AuthzService | CheckAccess | read_only | OK | 11.87 | 16.19 | 13.22 | 10 |
| AuthzService | CreatePolicyDraft | mutation | OK | 40.50 | 40.50 | 41.45 | 3 |
| AuthzService | CreatePolicyRule | mutation | OK | 21.95 | 21.95 | 23.41 | 3 |
| AuthzService | CreateRole | mutation | OK | 32.16 | 32.16 | 38.69 | 3 |
| AuthzService | DeletePolicyRule | mutation | OK | 14.17 | 14.17 | 14.05 | 3 |
| AuthzService | DeleteRole | mutation | OK | 15.41 | 15.41 | 21.87 | 3 |
| AuthzService | DiffPolicyDraft | read_only | OK | 14.51 | 17.64 | 17.67 | 10 |
| AuthzService | ExplainPolicy | read_only | OK | 11.17 | 14.79 | 12.44 | 10 |
| AuthzService | GetAuthzRevision | read_only | OK | 4.12 | 5.33 | 4.64 | 10 |
| AuthzService | GetCanaryStatus | read_only | OK | 11.37 | 14.99 | 11.87 | 10 |
| AuthzService | GetNativeAccess | read_only | OK | 27.42 | 31.81 | 28.82 | 10 |
| AuthzService | GetPolicyBundle | read_only | OK | 8.44 | 9.92 | 8.98 | 10 |
| AuthzService | GetPolicyRule | read_only | OK | 6.25 | 13.64 | 8.47 | 10 |
| AuthzService | GetRole | read_only | OK | 6.19 | 9.65 | 7.41 | 10 |
| AuthzService | InvalidatePolicyBundles | destructive | OK | 44.27 | 44.27 | 44.27 | 1 |
| AuthzService | LintAuthzPolicies | read_only | OK | 2.43 | 2.92 | 2.64 | 10 |
| AuthzService | ListAccessDecisionAudits | read_only | OK | 9.78 | 20.06 | 12.82 | 10 |
| AuthzService | ListPolicyRules | read_only | OK | 6.00 | 6.92 | 6.17 | 10 |
| AuthzService | ListPolicyVersions | read_only | OK | 11.89 | 20.49 | 13.99 | 10 |
| AuthzService | ListRoles | read_only | OK | 5.16 | 7.19 | 5.87 | 10 |
| AuthzService | ListUserPermissions | read_only | OK | 2.20 | 2.42 | 2.30 | 10 |
| AuthzService | ListUserRoles | read_only | OK | 7.13 | 8.62 | 7.16 | 10 |
| AuthzService | MigrateLegacyPolicies | destructive | OK | 61.62 | 61.62 | 61.62 | 1 |
| AuthzService | PromoteCanary | destructive | OK | 109.68 | 109.68 | 109.68 | 1 |
| AuthzService | PutAuthzPolicy | mutation | OK | 23.20 | 23.20 | 23.43 | 3 |
| AuthzService | PutRelationship | mutation | OK | 25.85 | 25.85 | 28.29 | 3 |
| AuthzService | PutRoleBinding | mutation | OK | 24.39 | 24.39 | 23.33 | 3 |
| AuthzService | RejectPolicyDraft | mutation | OK | 42.92 | 42.92 | 42.92 | 3 |
| AuthzService | RevokeRole | mutation | OK | 13.67 | 13.67 | 22.56 | 3 |
| AuthzService | RollbackPolicyVersion | destructive | OK | 91.13 | 91.13 | 91.13 | 1 |
| AuthzService | SeedBuiltinRoles | mutation | OK | 83.30 | 83.30 | 91.91 | 3 |
| AuthzService | SimulatePolicy | mutation | OK | 56.72 | 56.72 | 54.70 | 3 |
| AuthzService | SubmitPolicyDraft | mutation | OK | 27.06 | 27.06 | 27.06 | 3 |
| AuthzService | UpdatePolicyDraft | mutation | OK | 31.98 | 31.98 | 33.02 | 3 |
| AuthzService | UpdateRole | mutation | OK | 26.25 | 26.25 | 25.13 | 3 |
| ControlPlaneService | AckStatus | mutation | OK | 11.32 | 11.32 | 10.78 | 3 |
| ControlPlaneService | DeltaResources | stream_first_recv | OK | 131.55 | 131.55 | 123.63 | 3 |
| ControlPlaneService | GetResources | read_only | OK | 6.06 | 6.52 | 5.83 | 10 |
| ControlPlaneService | ListNodeStates | read_only | OK | 8.20 | 11.89 | 9.31 | 10 |
| ControlPlaneService | StreamResources | stream_first_recv | OK | 81.96 | 81.96 | 82.09 | 3 |
| DataBroker | ActivateCatalog | destructive | OK | 178.11 | 178.11 | 178.11 | 1 |
| DataBroker | AnalyticalQuery | read_only | OK | 12.04 | 15.18 | 12.99 | 10 |
| DataBroker | ApplyMigration | mutation | OK | 292.43 | 292.43 | 292.43 | 3 |
| DataBroker | ApproveMigrationPlan | mutation | OK | 36.03 | 36.03 | 36.03 | 3 |
| DataBroker | BatchSelect | stream_first_recv | OK | 9.31 | 9.31 | 9.88 | 3 |
| DataBroker | BatchUpsert | stream_first_recv | OK | 41.49 | 41.49 | 41.65 | 3 |
| DataBroker | BeginTx | stream_first_recv | OK | 19.45 | 19.45 | 20.15 | 3 |
| DataBroker | CacheDelete | mutation | OK | 7.58 | 7.58 | 8.64 | 3 |
| DataBroker | CacheGet | read_only | OK | 10.45 | 13.08 | 11.54 | 10 |
| DataBroker | CacheScan | read_only | OK | 15.24 | 18.24 | 15.96 | 10 |
| DataBroker | CacheSet | mutation | OK | 10.56 | 10.56 | 9.75 | 3 |
| DataBroker | CreateMaterializedView | mutation | OK | 14.56 | 14.56 | 13.40 | 3 |
| DataBroker | Delete | mutation | OK | 25.34 | 25.34 | 28.13 | 3 |
| DataBroker | DeletePolicy | mutation | OK | 22.53 | 22.53 | 22.53 | 3 |
| DataBroker | DismissDlqEvent | mutation | OK | 18.81 | 18.81 | 18.19 | 3 |
| DataBroker | DocumentDelete | mutation | OK | 14.88 | 14.88 | 13.39 | 3 |
| DataBroker | DocumentFind | read_only | OK | 9.72 | 10.66 | 9.71 | 10 |
| DataBroker | DocumentGet | read_only | OK | 8.88 | 10.50 | 9.36 | 10 |
| DataBroker | DocumentUpsert | mutation | OK | 10.89 | 10.89 | 10.70 | 3 |
| DataBroker | DropResource | destructive | OK | 38.42 | 38.42 | 38.42 | 1 |
| DataBroker | EnqueueOutboxEvent | mutation | OK | 14.05 | 14.05 | 16.98 | 3 |
| DataBroker | EnsureBaseline | mutation | OK | 47.47 | 47.47 | 47.22 | 3 |
| DataBroker | EnsureProject | mutation | OK | 30.98 | 30.98 | 30.80 | 3 |
| DataBroker | EnsureResource | mutation | OK | 20.96 | 20.96 | 21.17 | 3 |
| DataBroker | GeneratePresignedUrl | mutation | OK | 6.44 | 6.44 | 6.74 | 3 |
| DataBroker | GenericDispatch | mutation | OK | 7.67 | 7.67 | 7.99 | 3 |
| DataBroker | GetAdminSummary | read_only | OK | 41.01 | 52.71 | 42.11 | 10 |
| DataBroker | GetCapabilities | read_only | OK | 9.50 | 13.37 | 10.14 | 10 |
| DataBroker | GetCatalogManifest | read_only | OK | 14.00 | 19.59 | 15.49 | 10 |
| DataBroker | GetCatalogVersion | read_only | OK | 9.07 | 12.90 | 10.34 | 10 |
| DataBroker | GetCatalogVersions | read_only | OK | 9.99 | 12.37 | 10.54 | 10 |
| DataBroker | GetCdcStatus | read_only | OK | 7.90 | 12.37 | 8.99 | 10 |
| DataBroker | GetDlqEvent | read_only | OK | 9.36 | 10.69 | 9.62 | 10 |
| DataBroker | GetHealthReport | read_only | OK | 4.07 | 5.08 | 4.37 | 10 |
| DataBroker | GetMigrationStatus | read_only | OK | 12.34 | 14.40 | 12.71 | 10 |
| DataBroker | GetObject | stream_first_recv | OK | 16.26 | 16.26 | 17.02 | 3 |
| DataBroker | GetSaga | read_only | OK | 7.99 | 10.07 | 8.11 | 10 |
| DataBroker | GraphMutate | mutation | OK | 40.07 | 40.07 | 223.23 | 3 |
| DataBroker | GraphQuery | read_only | OK | 29.05 | 37.17 | 32.77 | 10 |
| DataBroker | InitiateMultipartUpload | mutation | OK | 16.01 | 16.01 | 16.73 | 3 |
| DataBroker | LintPolicies | read_only | OK | 8.90 | 14.94 | 9.58 | 10 |
| DataBroker | ListAdminAuditLogs | read_only | OK | 11.29 | 14.15 | 12.01 | 10 |
| DataBroker | ListDlqEvents | read_only | OK | 10.69 | 13.77 | 11.01 | 10 |
| DataBroker | ListMessageSchemas | read_only | OK | 3.79 | 4.56 | 3.70 | 10 |
| DataBroker | ListMigrationRuns | read_only | OK | 9.52 | 11.08 | 9.92 | 10 |
| DataBroker | ListPolicies | read_only | OK | 7.97 | 11.65 | 9.22 | 10 |
| DataBroker | ListProjects | read_only | OK | 7.73 | 11.05 | 9.14 | 10 |
| DataBroker | ListResources | read_only | OK | 8.80 | 9.99 | 9.01 | 10 |
| DataBroker | ListSagas | read_only | OK | 7.48 | 15.91 | 10.20 | 10 |
| DataBroker | LookupMessageSchema | read_only | OK | 3.71 | 5.57 | 4.12 | 10 |
| DataBroker | MarkSagaReviewed | mutation | OK | 28.64 | 28.64 | 29.78 | 3 |
| DataBroker | PauseCdc | mutation | OK | 17.37 | 17.37 | 17.40 | 3 |
| DataBroker | PlanMigration | mutation | OK | 40.36 | 40.36 | 35.37 | 3 |
| DataBroker | PreviewCdcRedaction | read_only | OK | 206.53 | 527.21 | 316.30 | 10 |
| DataBroker | PublishCDC | cdc_first_event | OK | 113.34 | 113.34 | 113.34 | 1 |
| DataBroker | PutObject | stream_first_recv | OK | 22.79 | 22.79 | 23.01 | 3 |
| DataBroker | PutPolicy | destructive | OK | 21.08 | 21.08 | 21.08 | 1 |
| DataBroker | QuarantineDlqEvent | mutation | OK | 16.09 | 16.09 | 16.50 | 3 |
| DataBroker | ReloadPolicies | destructive | OK | 18.49 | 18.49 | 18.49 | 1 |
| DataBroker | ReplayDlqEvent | mutation | OK | 31.10 | 31.10 | 31.10 | 3 |
| DataBroker | ResumeCdc | mutation | OK | 21.98 | 21.98 | 22.21 | 3 |
| DataBroker | RetrySagaCompensation | mutation | OK | 70.82 | 70.82 | 70.82 | 3 |
| DataBroker | RollbackCatalog | destructive | OK | 15.89 | 15.89 | 15.89 | 1 |
| DataBroker | ScanProjectionDrift | read_only | OK | 220.20 | 270.34 | 187.66 | 10 |
| DataBroker | Select | read_only | OK | 12.25 | 16.68 | 13.85 | 10 |
| DataBroker | SelectV2 | stream_first_recv | OK | 18.90 | 18.90 | 18.65 | 3 |
| DataBroker | StageCatalog | destructive | OK | 909.31 | 909.31 | 909.31 | 1 |
| DataBroker | StepDownCdcLeader | mutation | OK | 29.53 | 29.53 | 28.20 | 3 |
| DataBroker | TimeSeriesQuery | read_only | OK | 10.91 | 12.54 | 11.32 | 10 |
| DataBroker | TimeSeriesWrite | mutation | OK | 17.66 | 17.66 | 21.65 | 3 |
| DataBroker | Upsert | mutation | OK | 46.76 | 46.76 | 46.36 | 3 |
| DataBroker | ValidateCatalog | destructive | OK | 64.03 | 64.03 | 64.03 | 1 |
| DataBroker | VectorBatchUpsert | stream_first_recv | OK | 10.32 | 10.32 | 10.30 | 3 |
| DataBroker | VectorHybridSearch | read_only | OK | 9.62 | 12.25 | 10.38 | 10 |
| DataBroker | VectorSearch | read_only | OK | 11.81 | 19.04 | 13.71 | 10 |
| DataBroker | VectorUpsert | mutation | OK | 14.58 | 14.58 | 26.47 | 3 |
| DataBroker | VerifyAdminAuditLog | read_only | OK | 10.94 | 12.58 | 11.07 | 10 |
| IdentityProviderService | CreateProvider | mutation | OK | 29.62 | 29.62 | 31.11 | 3 |
| IdentityProviderService | DisableProvider | mutation | OK | 21.40 | 21.40 | 22.26 | 3 |
| IdentityProviderService | ForceJwksRefresh | mutation | OK | 30.05 | 30.05 | 33.46 | 3 |
| IdentityProviderService | GetProvider | read_only | OK | 5.59 | 7.31 | 6.10 | 10 |
| IdentityProviderService | ImportSamlMetadata | mutation | OK | 23.03 | 23.03 | 22.23 | 3 |
| IdentityProviderService | LinkIdentity | mutation | OK | 21.60 | 21.60 | 19.69 | 3 |
| IdentityProviderService | ListExternalIdentities | read_only | OK | 8.45 | 12.72 | 9.99 | 10 |
| IdentityProviderService | ListProviders | read_only | OK | 10.14 | 17.17 | 11.34 | 10 |
| IdentityProviderService | PreviewClaimMapping | read_only | OK | 6.49 | 7.39 | 6.46 | 10 |
| IdentityProviderService | PreviewGroupMapping | read_only | OK | 6.20 | 10.06 | 7.35 | 10 |
| IdentityProviderService | ResolveExternalIdentity | mutation | OK | 7.90 | 7.90 | 15.54 | 3 |
| IdentityProviderService | SamlAcs | mutation | OK | 12.44 | 12.44 | 11.72 | 3 |
| IdentityProviderService | ScimCreateGroup | mutation | OK | 6.38 | 6.38 | 6.58 | 3 |
| IdentityProviderService | ScimCreateUser | mutation | OK | 24.18 | 24.18 | 24.83 | 3 |
| IdentityProviderService | ScimDeleteGroup | mutation | OK | 5.30 | 5.30 | 5.60 | 3 |
| IdentityProviderService | ScimDeleteUser | mutation | OK | 32.04 | 32.04 | 32.04 | 3 |
| IdentityProviderService | ScimGetGroup | mutation | OK | 16.74 | 16.74 | 15.84 | 3 |
| IdentityProviderService | ScimGetUser | mutation | OK | 9.07 | 9.07 | 9.37 | 3 |
| IdentityProviderService | ScimListGroups | mutation | OK | 6.69 | 6.69 | 6.58 | 3 |
| IdentityProviderService | ScimListUsers | mutation | OK | 17.10 | 17.10 | 16.56 | 3 |
| IdentityProviderService | ScimPatchGroup | mutation | OK | 12.50 | 12.50 | 12.57 | 3 |
| IdentityProviderService | ScimPatchUser | mutation | OK | 26.79 | 26.79 | 26.71 | 3 |
| IdentityProviderService | ScimReplaceUser | mutation | OK | 28.75 | 28.75 | 25.58 | 3 |
| IdentityProviderService | StartSamlLogin | mutation | OK | 7.12 | 7.12 | 7.98 | 3 |
| IdentityProviderService | TestProviderDiscovery | read_only | OK | 7.88 | 9.60 | 8.32 | 10 |
| IdentityProviderService | UnlinkIdentity | mutation | OK | 5.82 | 5.82 | 5.98 | 3 |
| IdentityProviderService | UpdateProvider | mutation | OK | 24.59 | 24.59 | 25.56 | 3 |
| NotificationService | GetDeliveryStats | read_only | OK | 9.11 | 10.05 | 9.27 | 10 |
| NotificationService | GetNotification | read_only | OK | 16.73 | 22.43 | 16.06 | 10 |
| NotificationService | GetPreference | read_only | OK | 10.39 | 13.72 | 11.64 | 10 |
| NotificationService | GetTemplate | read_only | OK | 17.52 | 21.93 | 18.05 | 10 |
| NotificationService | ListNotifications | read_only | OK | 34.50 | 49.16 | 35.33 | 10 |
| NotificationService | ListPreferences | read_only | OK | 18.87 | 23.34 | 19.20 | 10 |
| NotificationService | ListTemplates | read_only | OK | 23.72 | 27.77 | 23.63 | 10 |
| NotificationService | RetryNotification | mutation | OK | 21.47 | 21.47 | 21.47 | 3 |
| NotificationService | SendNotification | mutation | OK | 41.79 | 41.79 | 46.11 | 3 |
| NotificationService | SetPreference | mutation | OK | 10.31 | 10.31 | 9.73 | 3 |
| NotificationService | UpsertTemplate | mutation | OK | 12.74 | 12.74 | 13.39 | 3 |
| PeerService | GetPeer | read_only | OK | 16.67 | 23.19 | 17.32 | 10 |
| PeerService | JoinRoom | mutation | OK | 27.47 | 27.47 | 26.62 | 3 |
| PeerService | JoinSession | mutation | OK | 25.72 | 25.72 | 26.19 | 3 |
| PeerService | LeaveRoom | mutation | OK | 7.69 | 7.69 | 10.80 | 3 |
| PeerService | ListPeers | read_only | OK | 10.80 | 16.04 | 12.21 | 10 |
| RoomService | CloseRoom | mutation | OK | 23.71 | 23.71 | 22.23 | 3 |
| RoomService | CreateRoom | mutation | OK | 19.53 | 19.53 | 19.15 | 3 |
| RoomService | GetRoom | read_only | OK | 12.42 | 15.22 | 12.88 | 10 |
| RoomService | ListRooms | read_only | OK | 12.48 | 15.31 | 13.44 | 10 |
| RoomService | UpdateRoom | mutation | OK | 9.18 | 9.18 | 8.80 | 3 |
| SignalingService | Signal | stream_first_recv | OK | 12.77 | 12.77 | 12.77 | 3 |
| StorageService | DeleteFile | mutation | OK | 31.13 | 31.13 | 31.13 | 3 |
| StorageService | DownloadFile | stream_first_recv | OK | 26.57 | 26.57 | 29.29 | 3 |
| StorageService | FinalizeUpload | mutation | OK | 47.45 | 47.45 | 47.45 | 3 |
| StorageService | GetDownloadUrl | read_only | OK | 15.10 | 22.82 | 16.28 | 10 |
| StorageService | GetFile | read_only | OK | 11.42 | 13.17 | 11.63 | 10 |
| StorageService | ListFiles | read_only | OK | 21.34 | 25.04 | 22.81 | 10 |
| StorageService | RegisterUpload | mutation | OK | 26.12 | 26.12 | 23.62 | 3 |
| StorageService | UpdateFile | mutation | OK | 31.00 | 31.00 | 32.69 | 3 |
| TenantService | CreateTenant | mutation | OK | 13.57 | 13.57 | 17.42 | 3 |
| TenantService | GetTenant | read_only | OK | 15.21 | 16.19 | 15.30 | 10 |
| TenantService | GetTenantConfig | read_only | OK | 14.60 | 21.42 | 17.36 | 10 |
| TenantService | ListTenants | read_only | OK | 14.63 | 19.27 | 17.14 | 10 |
| TenantService | UpdateTenant | mutation | OK | 11.34 | 11.34 | 11.98 | 3 |
| TenantService | UpdateTenantConfig | mutation | OK | 31.60 | 31.60 | 32.15 | 3 |
| TrackService | ListTracks | read_only | OK | 14.17 | 19.11 | 15.18 | 10 |
| TrackService | MuteTrack | mutation | OK | 7.41 | 7.41 | 7.31 | 3 |
| TrackService | PublishTrack | mutation | OK | 16.70 | 16.70 | 17.62 | 3 |
| TrackService | UnpublishTrack | mutation | OK | 7.74 | 7.74 | 9.45 | 3 |
| TurnService | IssueCredentials | mutation | OK | 6.62 | 6.62 | 7.03 | 3 |
