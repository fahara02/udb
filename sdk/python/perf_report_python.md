# UDB SDK Live Perf — Python (localhost)

RPCs measured: 265   tenant=732b88a9-378d-4ee2-98e0-ebc03a629cd3

Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities (a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request's reference/ID fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection latency. The TARGET is zero failures; any residual non-OK RPC is listed under Failures for the maintainer to finish.

Unary = full request/response round-trip. Non-CDC streaming RPCs (kind=stream_first_recv) report time-to-FIRST-RESPONSE with seeded inputs. CDC subscription (kind=cdc_first_event, PublishCDC) reports time-to-FIRST-EVENT: the harness subscribes, fires a real Upsert that flows outbox->CDC->Kafka, and times the first delivered event.

## Seeded fixtures

Captured semantic field -> seeded value keys used to resolve request fields: access_token, action, apply_run_id, approval_token, approve_draft_id, approve_run_id, approved_by, asset_id, assigned_by, auth_challenge_id, bucket, canary_id, canary_version_id, catalog_manifest, challenge_id, close_room_id, code, collection, content_type, created_by, csrf_token, current_password, definition_id, delete_file_id, delete_policy_id, delete_scim_user_id, deleted_by, device_id, dismiss_dlq_id, dlq_id, document_id, domain, ds_policy_id, email, event_type, external_identity_id, file_id, file_size_bytes, file_type, filename, finalize_file_id, gov_exp, identifier, instance_id, join_session_room_id, key_id, kind, leave_peer_id, locale, log_id, mark_saga_id, message_type, migration_id, mongo_collection, name, new_password, notification_id, object, object_key, otp_code, otp_id, owner_id, password, peer_id, plain_key, policy_draft_id, policy_id, policy_version_id, project, project_id, provider_id, quarantine_dlq_id, recipient_id, record_id, recovery_code, refresh_token, reg_challenge_id, reject_draft_id, rejected_by, relation, replay_dlq_id, reset_otp_code, reset_otp_id, resource, retry_saga_id, revoke_key_id, revoked_by, role, role_code, role_id, rollback_policy_set_id, rollback_target_version_id, room_id, saga_id, saml_provider_id, scim_group_id, scim_user_id, send_otp_user_id, session_id, signal_peer_id, stage_name, step_id, subject, tenant, tenant_id, token, topic_pattern, track_id, ts_table, unpublish_track_id, update_draft_id, update_draft_updated_at_unix, update_key_id, updated_by, user_id, user_role_id, username, vector_collection

## Per-service mean latency

| Service | RPCs | mean ms |
|---|--:|--:|
| AuthnService | 50 | 85.88 |
| DataBroker | 77 | 27.10 |
| ControlPlaneService | 5 | 24.02 |
| AuthzService | 41 | 20.78 |
| StorageService | 8 | 19.18 |
| AssetService | 8 | 14.49 |
| SignalingService | 1 | 14.03 |
| ApiKeyService | 9 | 12.97 |
| NotificationService | 11 | 12.78 |
| PeerService | 5 | 12.67 |
| RoomService | 5 | 12.45 |
| IdentityProviderService | 27 | 11.22 |
| TenantService | 6 | 9.78 |
| TrackService | 4 | 8.77 |
| AnalyticsService | 7 | 7.67 |
| TurnService | 1 | 5.31 |

## Failures (0)

No RPC returned a non-OK gRPC status.

## Slowest 20 by p99

| RPC | kind | err | p50 ms | p99 ms | mean ms |
|---|---|---|--:|--:|--:|
| AuthnService/ChangePassword | mutation | OK | 1477.56 | 1477.56 | 1477.56 |
| AuthnService/Login | mutation | OK | 761.64 | 761.64 | 722.51 |
| AuthnService/ResetPassword | mutation | OK | 659.46 | 659.46 | 659.46 |
| AuthnService/CreateUser | mutation | OK | 629.01 | 629.01 | 679.95 |
| DataBroker/StageCatalog | destructive | OK | 590.39 | 590.39 | 590.39 |
| DataBroker/PublishCDC | cdc_first_event | OK | 248.40 | 248.40 | 248.40 |
| DataBroker/ApplyMigration | mutation | OK | 169.17 | 169.17 | 169.17 |
| DataBroker/ActivateCatalog | destructive | OK | 104.29 | 104.29 | 104.29 |
| DataBroker/ValidateCatalog | destructive | OK | 75.04 | 75.04 | 75.04 |
| AuthnService/IntrospectToken | read_only | OK | 19.05 | 69.28 | 32.82 |
| AuthnService/FinishWebAuthnAuthentication | mutation | OK | 65.06 | 65.06 | 65.06 |
| AuthzService/PromoteCanary | destructive | OK | 55.39 | 55.39 | 55.39 |
| AuthzService/ActivatePolicyVersion | destructive | OK | 54.31 | 54.31 | 54.31 |
| AuthzService/RollbackPolicyVersion | destructive | OK | 51.12 | 51.12 | 51.12 |
| ControlPlaneService/StreamResources | stream_first_recv | OK | 48.87 | 48.87 | 51.37 |
| ControlPlaneService/DeltaResources | stream_first_recv | OK | 47.76 | 47.76 | 47.01 |
| AuthzService/CreatePolicyDraft | mutation | OK | 43.61 | 43.61 | 41.24 |
| StorageService/DeleteFile | mutation | OK | 40.07 | 40.07 | 40.07 |
| AuthnService/FinishWebAuthnRegistration | mutation | OK | 39.86 | 39.86 | 39.86 |
| AuthnService/RefreshSession | mutation | OK | 39.64 | 39.64 | 39.64 |

## Full per-RPC table (sorted by service, then RPC)

| Service | RPC | kind | err | p50 ms | p99 ms | mean ms | iters |
|---|---|---|---|--:|--:|--:|--:|
| AnalyticsService | GetExecutorPerformance | read_only | OK | 8.05 | 11.90 | 8.43 | 10 |
| AnalyticsService | GetPipelineSummary | read_only | OK | 9.28 | 11.67 | 8.66 | 10 |
| AnalyticsService | GetReconciliationAnalytics | read_only | OK | 8.10 | 10.45 | 8.12 | 10 |
| AnalyticsService | GetSlaCompliance | read_only | OK | 6.12 | 7.17 | 6.33 | 10 |
| AnalyticsService | GetThroughput | read_only | OK | 8.29 | 11.84 | 8.47 | 10 |
| AnalyticsService | RecordPipelineMetric | mutation | OK | 5.86 | 5.86 | 6.30 | 3 |
| AnalyticsService | TriggerSnapshot | mutation | OK | 8.10 | 8.10 | 7.40 | 3 |
| ApiKeyService | CreateApiKey | mutation | OK | 12.09 | 12.09 | 14.19 | 3 |
| ApiKeyService | EmergencyRevokeApiKeys | destructive | OK | 27.07 | 27.07 | 27.07 | 1 |
| ApiKeyService | GetApiKey | read_only | OK | 5.78 | 7.15 | 6.12 | 10 |
| ApiKeyService | GetApiKeyUsageStats | read_only | OK | 5.57 | 9.47 | 6.17 | 10 |
| ApiKeyService | ListApiKeys | read_only | OK | 5.36 | 6.88 | 5.57 | 10 |
| ApiKeyService | RevokeApiKey | mutation | OK | 14.55 | 14.55 | 14.55 | 3 |
| ApiKeyService | RotateApiKey | mutation | OK | 19.67 | 19.67 | 19.67 | 3 |
| ApiKeyService | UpdateApiKey | mutation | OK | 13.27 | 13.27 | 13.33 | 3 |
| ApiKeyService | ValidateApiKey | read_only | OK | 9.98 | 11.90 | 10.09 | 10 |
| AssetService | CompleteStep | mutation | OK | 20.04 | 20.04 | 20.48 | 3 |
| AssetService | CreatePipelineDefinition | mutation | OK | 12.20 | 12.20 | 12.20 | 3 |
| AssetService | GetAsset | read_only | OK | 8.84 | 10.37 | 8.86 | 10 |
| AssetService | GetPipeline | read_only | OK | 8.40 | 10.07 | 8.32 | 10 |
| AssetService | GetPipelineDefinition | read_only | OK | 8.41 | 8.88 | 8.21 | 10 |
| AssetService | ListAssets | read_only | OK | 9.85 | 24.30 | 12.90 | 10 |
| AssetService | RegisterAsset | mutation | OK | 14.44 | 14.44 | 14.83 | 3 |
| AssetService | StartPipeline | mutation | OK | 29.94 | 29.94 | 30.10 | 3 |
| AuthnService | AdminResetMfa | destructive | OK | 32.73 | 32.73 | 32.73 | 1 |
| AuthnService | AdminResetPassword | destructive | OK | 8.79 | 8.79 | 8.79 | 1 |
| AuthnService | AdminRevokeAllTenantSessions | destructive | OK | 12.32 | 12.32 | 12.32 | 1 |
| AuthnService | AdminRevokeAllUserSessions | destructive | OK | 10.31 | 10.31 | 10.31 | 1 |
| AuthnService | AdminRevokeSession | destructive | OK | 12.30 | 12.30 | 12.30 | 1 |
| AuthnService | Authenticate | read_only | OK | 27.26 | 27.26 | 27.26 | 1 |
| AuthnService | ChangePassword | mutation | OK | 1477.56 | 1477.56 | 1477.56 | 1 |
| AuthnService | ChangeUserStatus | destructive | OK | 12.26 | 12.26 | 12.26 | 1 |
| AuthnService | ConfirmMFAEnrollment | mutation | OK | 4.99 | 4.99 | 5.24 | 3 |
| AuthnService | CreateSession | mutation | OK | 9.09 | 9.09 | 9.17 | 3 |
| AuthnService | CreateUser | mutation | OK | 629.01 | 629.01 | 679.95 | 3 |
| AuthnService | DeleteWebAuthnCredential | mutation | OK | 9.17 | 9.17 | 9.40 | 3 |
| AuthnService | DisableMfaFactor | mutation | OK | 13.94 | 13.94 | 13.87 | 3 |
| AuthnService | EmergencyRevoke | destructive | OK | 17.12 | 17.12 | 17.12 | 1 |
| AuthnService | EnrollMFA | mutation | OK | 13.88 | 13.88 | 14.16 | 3 |
| AuthnService | FinishWebAuthnAuthentication | mutation | OK | 65.06 | 65.06 | 65.06 | 3 |
| AuthnService | FinishWebAuthnRegistration | mutation | OK | 39.86 | 39.86 | 39.86 | 3 |
| AuthnService | ForgotPassword | mutation | OK | 18.94 | 18.94 | 20.58 | 3 |
| AuthnService | GenerateRecoveryCodes | mutation | OK | 31.14 | 31.14 | 29.71 | 3 |
| AuthnService | GetJwks | read_only | OK | 4.70 | 7.97 | 5.81 | 10 |
| AuthnService | GetMfaPolicy | read_only | OK | 4.66 | 5.65 | 4.83 | 10 |
| AuthnService | GetSession | read_only | OK | 5.63 | 5.89 | 5.54 | 10 |
| AuthnService | GetUser | read_only | OK | 4.37 | 4.68 | 4.44 | 10 |
| AuthnService | IntrospectToken | read_only | OK | 19.05 | 69.28 | 32.82 | 10 |
| AuthnService | IssueMfaChallenge | mutation | OK | 13.73 | 13.73 | 13.50 | 3 |
| AuthnService | ListDevices | read_only | OK | 4.68 | 6.94 | 5.60 | 10 |
| AuthnService | ListMfaFactors | read_only | OK | 8.10 | 10.10 | 8.31 | 10 |
| AuthnService | ListSessions | read_only | OK | 11.37 | 13.46 | 11.43 | 10 |
| AuthnService | ListUsers | read_only | OK | 8.58 | 10.36 | 9.46 | 10 |
| AuthnService | ListWebAuthnCredentials | read_only | OK | 5.60 | 6.81 | 5.80 | 10 |
| AuthnService | Login | mutation | OK | 761.64 | 761.64 | 722.51 | 3 |
| AuthnService | Logout | mutation | OK | 5.40 | 5.40 | 11.64 | 3 |
| AuthnService | PutMfaPolicy | mutation | OK | 11.84 | 11.84 | 12.79 | 3 |
| AuthnService | RefreshSession | mutation | OK | 39.64 | 39.64 | 39.64 | 1 |
| AuthnService | RefreshToken | mutation | OK | 14.87 | 14.87 | 14.87 | 1 |
| AuthnService | RenamePasskey | mutation | OK | 11.27 | 11.27 | 10.78 | 3 |
| AuthnService | ResendOTP | mutation | OK | 23.90 | 23.90 | 23.90 | 1 |
| AuthnService | ResetPassword | mutation | OK | 659.46 | 659.46 | 659.46 | 1 |
| AuthnService | RevokeDevice | mutation | OK | 11.73 | 11.73 | 11.73 | 3 |
| AuthnService | RevokeRecoveryCodes | mutation | OK | 10.11 | 10.11 | 9.92 | 3 |
| AuthnService | RevokeSession | mutation | OK | 5.79 | 5.79 | 6.17 | 3 |
| AuthnService | SendOTP | mutation | OK | 19.23 | 19.23 | 19.23 | 1 |
| AuthnService | SendPhoneVerification | mutation | OK | 16.24 | 16.24 | 16.33 | 3 |
| AuthnService | StartWebAuthnAuthentication | mutation | OK | 17.43 | 17.43 | 17.64 | 3 |
| AuthnService | StartWebAuthnRegistration | mutation | OK | 16.65 | 16.65 | 18.87 | 3 |
| AuthnService | UpdateUser | mutation | OK | 12.54 | 12.54 | 13.49 | 3 |
| AuthnService | ValidateCSRF | read_only | OK | 9.83 | 11.15 | 10.34 | 10 |
| AuthnService | ValidateToken | read_only | OK | 18.77 | 25.81 | 20.51 | 10 |
| AuthnService | VerifyMfaChallenge | read_only | OK | 25.65 | 25.65 | 25.65 | 1 |
| AuthnService | VerifyOTP | read_only | OK | 23.32 | 23.32 | 23.32 | 1 |
| AuthzService | ActivateCanary | destructive | OK | 27.74 | 27.74 | 27.74 | 1 |
| AuthzService | ActivatePolicyVersion | destructive | OK | 54.31 | 54.31 | 54.31 | 1 |
| AuthzService | ApprovePolicyDraft | mutation | OK | 31.90 | 31.90 | 31.90 | 3 |
| AuthzService | AssignRole | mutation | OK | 23.95 | 23.95 | 24.21 | 3 |
| AuthzService | Authorize | read_only | OK | 25.22 | 28.67 | 26.10 | 10 |
| AuthzService | BatchCheckPermissions | read_only | OK | 10.11 | 12.20 | 10.69 | 10 |
| AuthzService | CheckAccess | read_only | OK | 10.31 | 10.91 | 10.32 | 10 |
| AuthzService | CreatePolicyDraft | mutation | OK | 43.61 | 43.61 | 41.24 | 3 |
| AuthzService | CreatePolicyRule | mutation | OK | 16.43 | 16.43 | 16.87 | 3 |
| AuthzService | CreateRole | mutation | OK | 26.24 | 26.24 | 27.08 | 3 |
| AuthzService | DeletePolicyRule | mutation | OK | 8.30 | 8.30 | 8.33 | 3 |
| AuthzService | DeleteRole | mutation | OK | 11.43 | 11.43 | 16.43 | 3 |
| AuthzService | DiffPolicyDraft | read_only | OK | 18.45 | 26.40 | 21.72 | 10 |
| AuthzService | ExplainPolicy | read_only | OK | 15.51 | 18.34 | 17.47 | 10 |
| AuthzService | GetAuthzRevision | read_only | OK | 7.43 | 10.53 | 8.12 | 10 |
| AuthzService | GetCanaryStatus | read_only | OK | 13.74 | 16.96 | 14.70 | 10 |
| AuthzService | GetNativeAccess | read_only | OK | 25.18 | 30.75 | 26.49 | 10 |
| AuthzService | GetPolicyBundle | read_only | OK | 12.33 | 13.93 | 12.77 | 10 |
| AuthzService | GetPolicyRule | read_only | OK | 5.57 | 6.96 | 5.98 | 10 |
| AuthzService | GetRole | read_only | OK | 5.46 | 5.68 | 5.31 | 10 |
| AuthzService | InvalidatePolicyBundles | destructive | OK | 23.78 | 23.78 | 23.78 | 1 |
| AuthzService | LintAuthzPolicies | read_only | OK | 1.98 | 2.74 | 2.19 | 10 |
| AuthzService | ListAccessDecisionAudits | read_only | OK | 14.21 | 20.92 | 15.32 | 10 |
| AuthzService | ListPolicyRules | read_only | OK | 5.16 | 5.93 | 5.25 | 10 |
| AuthzService | ListPolicyVersions | read_only | OK | 15.44 | 18.30 | 16.34 | 10 |
| AuthzService | ListRoles | read_only | OK | 5.67 | 6.15 | 5.49 | 10 |
| AuthzService | ListUserPermissions | read_only | OK | 2.34 | 2.81 | 2.51 | 10 |
| AuthzService | ListUserRoles | read_only | OK | 4.78 | 7.10 | 5.27 | 10 |
| AuthzService | MigrateLegacyPolicies | destructive | OK | 34.89 | 34.89 | 34.89 | 1 |
| AuthzService | PromoteCanary | destructive | OK | 55.39 | 55.39 | 55.39 | 1 |
| AuthzService | PutAuthzPolicy | mutation | OK | 14.74 | 14.74 | 16.26 | 3 |
| AuthzService | PutRelationship | mutation | OK | 23.49 | 23.49 | 23.22 | 3 |
| AuthzService | PutRoleBinding | mutation | OK | 21.15 | 21.15 | 20.77 | 3 |
| AuthzService | RejectPolicyDraft | mutation | OK | 24.63 | 24.63 | 24.63 | 3 |
| AuthzService | RevokeRole | mutation | OK | 8.23 | 8.23 | 12.36 | 3 |
| AuthzService | RollbackPolicyVersion | destructive | OK | 51.12 | 51.12 | 51.12 | 1 |
| AuthzService | SeedBuiltinRoles | mutation | OK | 37.36 | 37.36 | 42.84 | 3 |
| AuthzService | SimulatePolicy | mutation | OK | 21.87 | 21.87 | 25.91 | 3 |
| AuthzService | SubmitPolicyDraft | mutation | OK | 17.17 | 17.17 | 17.17 | 3 |
| AuthzService | UpdatePolicyDraft | mutation | OK | 22.54 | 22.54 | 23.29 | 3 |
| AuthzService | UpdateRole | mutation | OK | 19.35 | 19.35 | 20.04 | 3 |
| ControlPlaneService | AckStatus | mutation | OK | 6.55 | 6.55 | 6.76 | 3 |
| ControlPlaneService | DeltaResources | stream_first_recv | OK | 47.76 | 47.76 | 47.01 | 3 |
| ControlPlaneService | GetResources | read_only | OK | 6.39 | 6.65 | 6.42 | 10 |
| ControlPlaneService | ListNodeStates | read_only | OK | 8.55 | 9.66 | 8.54 | 10 |
| ControlPlaneService | StreamResources | stream_first_recv | OK | 48.87 | 48.87 | 51.37 | 3 |
| DataBroker | ActivateCatalog | destructive | OK | 104.29 | 104.29 | 104.29 | 1 |
| DataBroker | AnalyticalQuery | read_only | OK | 7.82 | 8.90 | 8.52 | 10 |
| DataBroker | ApplyMigration | mutation | OK | 169.17 | 169.17 | 169.17 | 3 |
| DataBroker | ApproveMigrationPlan | mutation | OK | 18.48 | 18.48 | 18.48 | 3 |
| DataBroker | BatchSelect | stream_first_recv | OK | 7.35 | 7.35 | 8.67 | 3 |
| DataBroker | BatchUpsert | stream_first_recv | OK | 34.74 | 34.74 | 34.40 | 3 |
| DataBroker | BeginTx | stream_first_recv | OK | 13.06 | 13.06 | 16.08 | 3 |
| DataBroker | CacheDelete | mutation | OK | 6.21 | 6.21 | 6.21 | 3 |
| DataBroker | CacheGet | read_only | OK | 6.30 | 7.66 | 6.66 | 10 |
| DataBroker | CacheScan | read_only | OK | 8.83 | 10.27 | 8.88 | 10 |
| DataBroker | CacheSet | mutation | OK | 9.83 | 9.83 | 9.31 | 3 |
| DataBroker | CreateMaterializedView | mutation | OK | 8.24 | 8.24 | 8.23 | 3 |
| DataBroker | Delete | mutation | OK | 22.80 | 22.80 | 24.39 | 3 |
| DataBroker | DeletePolicy | mutation | OK | 20.51 | 20.51 | 20.51 | 3 |
| DataBroker | DismissDlqEvent | mutation | OK | 10.76 | 10.76 | 10.46 | 3 |
| DataBroker | DocumentDelete | mutation | OK | 7.73 | 7.73 | 8.43 | 3 |
| DataBroker | DocumentFind | read_only | OK | 6.45 | 6.86 | 6.43 | 10 |
| DataBroker | DocumentGet | read_only | OK | 6.08 | 6.97 | 6.06 | 10 |
| DataBroker | DocumentUpsert | mutation | OK | 8.07 | 8.07 | 8.92 | 3 |
| DataBroker | DropResource | destructive | OK | 21.58 | 21.58 | 21.58 | 1 |
| DataBroker | EnqueueOutboxEvent | mutation | OK | 12.66 | 12.66 | 13.64 | 3 |
| DataBroker | EnsureBaseline | mutation | OK | 16.78 | 16.78 | 16.78 | 3 |
| DataBroker | EnsureProject | mutation | OK | 10.21 | 10.21 | 10.44 | 3 |
| DataBroker | EnsureResource | mutation | OK | 19.74 | 19.74 | 20.04 | 3 |
| DataBroker | GeneratePresignedUrl | mutation | OK | 5.03 | 5.03 | 5.78 | 3 |
| DataBroker | GenericDispatch | mutation | OK | 6.41 | 6.41 | 8.00 | 3 |
| DataBroker | GetAdminSummary | read_only | OK | 17.78 | 27.82 | 20.80 | 10 |
| DataBroker | GetCapabilities | read_only | OK | 5.64 | 6.95 | 6.02 | 10 |
| DataBroker | GetCatalogManifest | read_only | OK | 9.21 | 12.47 | 10.28 | 10 |
| DataBroker | GetCatalogVersion | read_only | OK | 4.37 | 5.35 | 4.44 | 10 |
| DataBroker | GetCatalogVersions | read_only | OK | 5.82 | 7.66 | 6.11 | 10 |
| DataBroker | GetCdcStatus | read_only | OK | 4.92 | 6.01 | 5.28 | 10 |
| DataBroker | GetDlqEvent | read_only | OK | 4.15 | 5.81 | 4.75 | 10 |
| DataBroker | GetHealthReport | read_only | OK | 3.19 | 4.06 | 3.87 | 10 |
| DataBroker | GetMigrationStatus | read_only | OK | 5.20 | 5.78 | 5.68 | 10 |
| DataBroker | GetObject | stream_first_recv | OK | 8.60 | 8.60 | 10.00 | 3 |
| DataBroker | GetSaga | read_only | OK | 5.79 | 6.31 | 5.80 | 10 |
| DataBroker | GraphMutate | mutation | OK | 18.79 | 18.79 | 105.44 | 3 |
| DataBroker | GraphQuery | read_only | OK | 20.66 | 24.42 | 20.70 | 10 |
| DataBroker | InitiateMultipartUpload | mutation | OK | 13.68 | 13.68 | 15.62 | 3 |
| DataBroker | LintPolicies | read_only | OK | 6.32 | 11.91 | 7.61 | 10 |
| DataBroker | ListAdminAuditLogs | read_only | OK | 9.18 | 11.57 | 9.69 | 10 |
| DataBroker | ListDlqEvents | read_only | OK | 5.36 | 8.21 | 5.96 | 10 |
| DataBroker | ListMessageSchemas | read_only | OK | 2.77 | 3.57 | 3.03 | 10 |
| DataBroker | ListMigrationRuns | read_only | OK | 5.12 | 7.66 | 5.85 | 10 |
| DataBroker | ListPolicies | read_only | OK | 5.40 | 6.29 | 5.33 | 10 |
| DataBroker | ListProjects | read_only | OK | 6.32 | 7.98 | 6.94 | 10 |
| DataBroker | ListResources | read_only | OK | 6.38 | 8.94 | 7.07 | 10 |
| DataBroker | ListSagas | read_only | OK | 7.28 | 12.17 | 8.68 | 10 |
| DataBroker | LookupMessageSchema | read_only | OK | 2.44 | 3.04 | 2.60 | 10 |
| DataBroker | MarkSagaReviewed | mutation | OK | 12.56 | 12.56 | 12.02 | 3 |
| DataBroker | PauseCdc | mutation | OK | 10.84 | 10.84 | 11.19 | 3 |
| DataBroker | PlanMigration | mutation | OK | 24.81 | 24.81 | 23.86 | 3 |
| DataBroker | PreviewCdcRedaction | read_only | OK | 9.11 | 10.50 | 9.65 | 10 |
| DataBroker | PublishCDC | cdc_first_event | OK | 248.40 | 248.40 | 248.40 | 1 |
| DataBroker | PutObject | stream_first_recv | OK | 18.18 | 18.18 | 18.50 | 3 |
| DataBroker | PutPolicy | destructive | OK | 19.21 | 19.21 | 19.21 | 1 |
| DataBroker | QuarantineDlqEvent | mutation | OK | 11.16 | 11.16 | 10.81 | 3 |
| DataBroker | ReloadPolicies | destructive | OK | 13.87 | 13.87 | 13.87 | 1 |
| DataBroker | ReplayDlqEvent | mutation | OK | 19.62 | 19.62 | 19.62 | 3 |
| DataBroker | ResumeCdc | mutation | OK | 12.27 | 12.27 | 11.84 | 3 |
| DataBroker | RetrySagaCompensation | mutation | OK | 14.81 | 14.81 | 14.81 | 3 |
| DataBroker | RollbackCatalog | destructive | OK | 6.46 | 6.46 | 6.46 | 1 |
| DataBroker | ScanProjectionDrift | read_only | OK | 12.62 | 13.72 | 12.50 | 10 |
| DataBroker | Select | read_only | OK | 5.80 | 8.95 | 6.45 | 10 |
| DataBroker | SelectV2 | stream_first_recv | OK | 6.81 | 6.81 | 6.87 | 3 |
| DataBroker | StageCatalog | destructive | OK | 590.39 | 590.39 | 590.39 | 1 |
| DataBroker | StepDownCdcLeader | mutation | OK | 11.54 | 11.54 | 11.12 | 3 |
| DataBroker | TimeSeriesQuery | read_only | OK | 10.47 | 13.11 | 12.50 | 10 |
| DataBroker | TimeSeriesWrite | mutation | OK | 8.97 | 8.97 | 13.04 | 3 |
| DataBroker | Upsert | mutation | OK | 26.15 | 26.15 | 26.49 | 3 |
| DataBroker | ValidateCatalog | destructive | OK | 75.04 | 75.04 | 75.04 | 1 |
| DataBroker | VectorBatchUpsert | stream_first_recv | OK | 5.96 | 5.96 | 6.53 | 3 |
| DataBroker | VectorHybridSearch | read_only | OK | 6.21 | 7.58 | 6.48 | 10 |
| DataBroker | VectorSearch | read_only | OK | 5.76 | 7.62 | 6.16 | 10 |
| DataBroker | VectorUpsert | mutation | OK | 12.42 | 12.42 | 16.46 | 3 |
| DataBroker | VerifyAdminAuditLog | read_only | OK | 8.55 | 8.99 | 8.34 | 10 |
| IdentityProviderService | CreateProvider | mutation | OK | 16.68 | 16.68 | 18.17 | 3 |
| IdentityProviderService | DisableProvider | mutation | OK | 14.42 | 14.42 | 14.40 | 3 |
| IdentityProviderService | ForceJwksRefresh | mutation | OK | 19.77 | 19.77 | 25.23 | 3 |
| IdentityProviderService | GetProvider | read_only | OK | 4.98 | 8.07 | 5.62 | 10 |
| IdentityProviderService | ImportSamlMetadata | mutation | OK | 16.34 | 16.34 | 15.82 | 3 |
| IdentityProviderService | LinkIdentity | mutation | OK | 13.35 | 13.35 | 13.52 | 3 |
| IdentityProviderService | ListExternalIdentities | read_only | OK | 7.57 | 8.62 | 7.76 | 10 |
| IdentityProviderService | ListProviders | read_only | OK | 7.11 | 9.59 | 7.61 | 10 |
| IdentityProviderService | PreviewClaimMapping | read_only | OK | 4.46 | 5.49 | 4.68 | 10 |
| IdentityProviderService | PreviewGroupMapping | read_only | OK | 4.63 | 6.01 | 5.02 | 10 |
| IdentityProviderService | ResolveExternalIdentity | mutation | OK | 7.37 | 7.37 | 12.78 | 3 |
| IdentityProviderService | SamlAcs | mutation | OK | 11.04 | 11.04 | 11.03 | 3 |
| IdentityProviderService | ScimCreateGroup | mutation | OK | 4.75 | 4.75 | 5.21 | 3 |
| IdentityProviderService | ScimCreateUser | mutation | OK | 20.08 | 20.08 | 20.03 | 3 |
| IdentityProviderService | ScimDeleteGroup | mutation | OK | 4.31 | 4.31 | 4.60 | 3 |
| IdentityProviderService | ScimDeleteUser | mutation | OK | 21.07 | 21.07 | 21.07 | 3 |
| IdentityProviderService | ScimGetGroup | mutation | OK | 7.80 | 7.80 | 8.10 | 3 |
| IdentityProviderService | ScimGetUser | mutation | OK | 6.78 | 6.78 | 6.74 | 3 |
| IdentityProviderService | ScimListGroups | mutation | OK | 6.06 | 6.06 | 5.51 | 3 |
| IdentityProviderService | ScimListUsers | mutation | OK | 8.83 | 8.83 | 8.76 | 3 |
| IdentityProviderService | ScimPatchGroup | mutation | OK | 9.57 | 9.57 | 9.74 | 3 |
| IdentityProviderService | ScimPatchUser | mutation | OK | 18.46 | 18.46 | 18.88 | 3 |
| IdentityProviderService | ScimReplaceUser | mutation | OK | 17.42 | 17.42 | 18.37 | 3 |
| IdentityProviderService | StartSamlLogin | mutation | OK | 5.75 | 5.75 | 6.26 | 3 |
| IdentityProviderService | TestProviderDiscovery | read_only | OK | 5.42 | 9.64 | 6.41 | 10 |
| IdentityProviderService | UnlinkIdentity | mutation | OK | 5.07 | 5.07 | 5.32 | 3 |
| IdentityProviderService | UpdateProvider | mutation | OK | 16.12 | 16.12 | 16.22 | 3 |
| NotificationService | GetDeliveryStats | read_only | OK | 7.50 | 10.38 | 8.07 | 10 |
| NotificationService | GetNotification | read_only | OK | 9.03 | 14.44 | 10.67 | 10 |
| NotificationService | GetPreference | read_only | OK | 8.86 | 11.16 | 9.22 | 10 |
| NotificationService | GetTemplate | read_only | OK | 7.67 | 10.31 | 8.28 | 10 |
| NotificationService | ListNotifications | read_only | OK | 14.53 | 21.78 | 17.56 | 10 |
| NotificationService | ListPreferences | read_only | OK | 13.00 | 14.66 | 13.46 | 10 |
| NotificationService | ListTemplates | read_only | OK | 12.37 | 24.37 | 14.71 | 10 |
| NotificationService | RetryNotification | mutation | OK | 13.06 | 13.06 | 13.06 | 3 |
| NotificationService | SendNotification | mutation | OK | 30.84 | 30.84 | 31.59 | 3 |
| NotificationService | SetPreference | mutation | OK | 7.02 | 7.02 | 6.61 | 3 |
| NotificationService | UpsertTemplate | mutation | OK | 7.17 | 7.17 | 7.31 | 3 |
| PeerService | GetPeer | read_only | OK | 7.84 | 9.24 | 7.96 | 10 |
| PeerService | JoinRoom | mutation | OK | 16.25 | 16.25 | 15.84 | 3 |
| PeerService | JoinSession | mutation | OK | 14.76 | 14.76 | 14.90 | 3 |
| PeerService | LeaveRoom | mutation | OK | 5.90 | 5.90 | 15.10 | 3 |
| PeerService | ListPeers | read_only | OK | 8.90 | 11.27 | 9.54 | 10 |
| RoomService | CloseRoom | mutation | OK | 20.78 | 20.78 | 19.66 | 3 |
| RoomService | CreateRoom | mutation | OK | 11.77 | 11.77 | 11.66 | 3 |
| RoomService | GetRoom | read_only | OK | 8.29 | 12.41 | 9.72 | 10 |
| RoomService | ListRooms | read_only | OK | 7.91 | 10.38 | 9.28 | 10 |
| RoomService | UpdateRoom | mutation | OK | 9.43 | 9.43 | 11.93 | 3 |
| SignalingService | Signal | stream_first_recv | OK | 14.03 | 14.03 | 14.03 | 3 |
| StorageService | DeleteFile | mutation | OK | 40.07 | 40.07 | 40.07 | 3 |
| StorageService | DownloadFile | stream_first_recv | OK | 19.91 | 19.91 | 20.47 | 3 |
| StorageService | FinalizeUpload | mutation | OK | 29.86 | 29.86 | 29.86 | 3 |
| StorageService | GetDownloadUrl | read_only | OK | 9.89 | 12.62 | 10.65 | 10 |
| StorageService | GetFile | read_only | OK | 7.17 | 8.43 | 7.47 | 10 |
| StorageService | ListFiles | read_only | OK | 13.10 | 16.47 | 13.50 | 10 |
| StorageService | RegisterUpload | mutation | OK | 13.45 | 13.45 | 14.59 | 3 |
| StorageService | UpdateFile | mutation | OK | 16.82 | 16.82 | 16.80 | 3 |
| TenantService | CreateTenant | mutation | OK | 7.28 | 7.28 | 9.28 | 3 |
| TenantService | GetTenant | read_only | OK | 7.57 | 10.07 | 8.70 | 10 |
| TenantService | GetTenantConfig | read_only | OK | 7.47 | 10.67 | 8.22 | 10 |
| TenantService | ListTenants | read_only | OK | 7.91 | 8.69 | 8.14 | 10 |
| TenantService | UpdateTenant | mutation | OK | 7.83 | 7.83 | 7.61 | 3 |
| TenantService | UpdateTenantConfig | mutation | OK | 16.70 | 16.70 | 16.72 | 3 |
| TrackService | ListTracks | read_only | OK | 7.78 | 8.90 | 8.07 | 10 |
| TrackService | MuteTrack | mutation | OK | 8.36 | 8.36 | 8.17 | 3 |
| TrackService | PublishTrack | mutation | OK | 11.16 | 11.16 | 11.67 | 3 |
| TrackService | UnpublishTrack | mutation | OK | 7.13 | 7.13 | 7.18 | 3 |
| TurnService | IssueCredentials | mutation | OK | 5.37 | 5.37 | 5.31 | 3 |
