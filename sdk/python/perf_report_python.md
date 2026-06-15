# UDB SDK Live Perf — Python (localhost)

RPCs measured: 262   tenant=6b943bac-248c-4766-91d5-aa278e86fbd3

Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities (a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request's reference/ID fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection latency. The TARGET is zero failures; any residual non-OK RPC is listed under Failures for the maintainer to finish.

Unary = full request/response round-trip. Non-CDC streaming RPCs (kind=stream_first_recv) report time-to-FIRST-RESPONSE with seeded inputs. CDC subscription (kind=cdc_first_event, PublishCDC) reports time-to-FIRST-EVENT: the harness subscribes, fires a real Upsert that flows outbox->CDC->Kafka, and times the first delivered event.

## Seeded fixtures

Captured semantic field -> seeded value keys used to resolve request fields: access_token, action, apply_run_id, approval_token, approve_draft_id, approve_run_id, approved_by, asset_id, assigned_by, auth_challenge_id, bucket, canary_id, canary_version_id, catalog_manifest, challenge_id, close_room_id, code, collection, content_type, created_by, csrf_token, current_password, definition_id, delete_file_id, delete_policy_id, delete_scim_user_id, deleted_by, device_id, dismiss_dlq_id, dlq_id, document_id, domain, ds_policy_id, email, event_type, external_identity_id, file_id, file_size_bytes, file_type, filename, identifier, instance_id, key_id, kind, leave_peer_id, locale, log_id, mark_saga_id, message_type, migration_id, mongo_collection, name, new_password, notification_id, object, object_key, otp_code, otp_id, owner_id, password, peer_id, plain_key, policy_draft_id, policy_id, policy_version_id, project, project_id, provider_id, quarantine_dlq_id, recipient_id, record_id, recovery_code, refresh_token, reg_challenge_id, reject_draft_id, rejected_by, relation, replay_dlq_id, reset_otp_code, reset_otp_id, resource, retry_saga_id, revoke_key_id, revoked_by, role, role_code, role_id, rollback_policy_set_id, rollback_target_version_id, room_id, saga_id, saml_provider_id, scim_group_id, scim_user_id, send_otp_user_id, session_id, signal_peer_id, stage_name, step_id, subject, tenant, tenant_id, token, topic_pattern, track_id, ts_table, unpublish_track_id, update_draft_id, update_draft_updated_at_unix, update_key_id, updated_by, user_id, user_role_id, username, vector_collection

## Per-service mean latency

| Service | RPCs | mean ms |
|---|--:|--:|
| AuthnService | 50 | 122.76 |
| PeerService | 4 | 64.59 |
| ControlPlaneService | 5 | 57.86 |
| SignalingService | 1 | 51.22 |
| AuthzService | 41 | 46.22 |
| AssetService | 8 | 43.67 |
| DataBroker | 76 | 42.62 |
| StorageService | 7 | 36.34 |
| ApiKeyService | 9 | 29.81 |
| IdentityProviderService | 27 | 27.33 |
| NotificationService | 11 | 27.03 |
| RoomService | 5 | 26.91 |
| TrackService | 4 | 26.18 |
| TenantService | 6 | 21.01 |
| AnalyticsService | 7 | 16.51 |
| TurnService | 1 | 11.23 |

## Failures (0)

No RPC returned a non-OK gRPC status.

## Slowest 20 by p99

| RPC | kind | err | p50 ms | p99 ms | mean ms |
|---|---|---|--:|--:|--:|
| AuthnService/ChangePassword | mutation | OK | 1696.37 | 1696.37 | 1696.37 |
| AuthnService/CreateUser | mutation | OK | 1029.48 | 1029.48 | 1036.50 |
| AuthnService/ResetPassword | mutation | OK | 910.69 | 910.69 | 910.69 |
| AuthnService/Login | mutation | OK | 866.33 | 866.33 | 853.24 |
| DataBroker/StageCatalog | destructive | OK | 526.39 | 526.39 | 526.39 |
| DataBroker/ApplyMigration | mutation | OK | 443.20 | 443.20 | 443.20 |
| DataBroker/PublishCDC | cdc_first_event | OK | 252.95 | 252.95 | 252.95 |
| AuthzService/PromoteCanary | destructive | OK | 221.05 | 221.05 | 221.05 |
| DataBroker/ActivateCatalog | destructive | OK | 144.43 | 144.43 | 144.43 |
| AuthnService/FinishWebAuthnAuthentication | mutation | OK | 130.67 | 130.67 | 130.67 |
| AuthzService/SeedBuiltinRoles | mutation | OK | 128.05 | 128.05 | 133.84 |
| ControlPlaneService/StreamResources | stream_first_recv | OK | 127.77 | 127.77 | 125.24 |
| DataBroker/GetAdminSummary | read_only | OK | 92.36 | 126.43 | 94.51 |
| AuthzService/GetPolicyBundle | read_only | OK | 50.70 | 119.34 | 71.35 |
| AuthzService/RollbackPolicyVersion | destructive | OK | 113.47 | 113.47 | 113.47 |
| AssetService/StartPipeline | mutation | OK | 112.46 | 112.46 | 112.57 |
| ControlPlaneService/DeltaResources | stream_first_recv | OK | 111.80 | 111.80 | 125.94 |
| DataBroker/Upsert | mutation | OK | 108.99 | 108.99 | 104.96 |
| DataBroker/BatchUpsert | stream_first_recv | OK | 100.73 | 100.73 | 101.83 |
| AuthzService/CreatePolicyDraft | mutation | OK | 87.85 | 87.85 | 80.31 |
