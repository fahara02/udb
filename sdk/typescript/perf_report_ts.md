# UDB SDK Live Perf — TypeScript (localhost)

RPCs measured: 265   tenant=4a24f274-5925-49f7-a02f-43f8a6299392

Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities (a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request's reference/ID fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection latency. Any residual non-OK RPC is listed under Failures for the maintainer to finish.

Unary = full request/response round-trip. Non-CDC server-streaming RPCs (kind=stream) report time-to-FIRST-RESPONSE with seeded inputs; client-streaming/bidi RPCs (kind=stream_open) report stream-open latency. CDC subscription (publish_cdc, kind=stream) reports time-to-FIRST-EVENT: the harness subscribes, fires a real seeded Upsert that flows outbox→CDC→Kafka, and times the first delivered event.

RPCs run on the AUTH ROUTE in three phases (BENCH_RPC_BODIES.md "Execution order"): Phase 1 establishes the session (AuthnService login → refresh_token → refresh_session → authenticate → validate_token → introspect_token → get_jwks), then the seed phase; Phase 2 measures everything else; Phase 3 LAST runs the session/credential-teardown AuthnService RPCs (logout, revoke_*, change/reset password, admin_reset_mfa, disable_mfa_factor, …) against the seeded DISPOSABLE user/session so the admin's own session is never killed mid-run.

## Seeded fixtures

Captured semantic field → seeded value keys used to resolve request fields: action, apply_run_id, approval_token, approve_draft_id, approve_run_id, approved_by, asset_id, assigned_by, auth_challenge_id, bucket, canary_id, canary_version_id, challenge_id, close_room_id, code, collection, content_type, created_by, csrf_token, definition_id, delete_file_id, delete_policy_id, delete_role_id, delete_scim_user_id, deleted_by, device_id, disable_provider_id, dismiss_dlq_id, dlq_id, domain, ds_policy_id, event_type, external_identity_id, file_id, file_type, filename, finalize_file_id, gov_exp, instance_id, join_session_room_id, key_id, key_prefix, kind, leave_peer_id, locale, log_id, mark_saga_id, message_type, migration_id, mongo_collection, name, node_id, notification_id, object, object_key, otp_code, otp_id, owner_id, peer_id, plain_key, policy_draft_id, policy_id, policy_version_id, project, project_id, provider_id, quarantine_dlq_id, recipient_id, record_id, recovery_code, refresh_token, reg_challenge_id, reject_draft_id, rejected_by, relation, replay_dlq_id, reset_otp_code, reset_otp_id, resource, retry_saga_id, revoke_key_id, revoke_key_prefix, revoked_by, role, role_code, role_id, rollback_policy_set_id, rollback_target_version_id, room_id, saga_id, saml_provider_id, scim_group_id, scim_user_id, session_id, stage_name, step_id, subject, tenant, tenant_id, token, track_id, ts_table, unpublish_track_id, update_draft_id, update_key_id, update_key_prefix, updated_by, user_id, user_role_id, username

## Per-service mean latency

| Service | RPCs | mean ms |
|---|--:|--:|
| AuthnService | 50 | 53.37 |
| AuthzService | 41 | 23.02 |
| DataBroker | 77 | 18.31 |
| StorageService | 8 | 18.24 |
| AssetService | 8 | 18.07 |
| IdentityProviderService | 27 | 17.48 |
| ApiKeyService | 9 | 16.43 |
| PeerService | 5 | 14.71 |
| NotificationService | 11 | 13.44 |
| RoomService | 5 | 12.04 |
| TenantService | 6 | 11.87 |
| ControlPlaneService | 5 | 9.50 |
| TrackService | 4 | 9.23 |
| AnalyticsService | 7 | 8.66 |
| TurnService | 1 | 6.48 |
| SignalingService | 1 | 5.85 |

## Failures (0)

No RPC returned a non-OK gRPC status.

## Slowest 20 by p99

| RPC | kind | err | p50 ms | p99 ms | mean ms | note |
|---|---|---|--:|--:|--:|---|
| AuthnService/change_password | mutation | OK | 773.52 | 773.52 | 773.52 | mutation (seeded success path) |
| AuthnService/login | mutation | OK | 400.07 | 403.38 | 403.33 | mutation (seeded success path) |
| AuthnService/reset_password | mutation | OK | 401.11 | 401.11 | 401.11 | mutation (seeded success path) |
| AuthnService/create_user | mutation | OK | 358.21 | 358.83 | 366.88 | mutation (seeded success path) |
| DataBroker/stage_catalog | destructive | OK | 240.91 | 240.91 | 240.91 | destructive: 1 real call against a seeded disposable target |
| DataBroker/apply_migration | mutation | OK | 194.49 | 194.49 | 194.49 | mutation (seeded success path) |
| AuthnService/authenticate | read_only | OK | 30.13 | 133.73 | 44.38 | read_only (seeded success path) |
| IdentityProviderService/saml_acs | mutation | OK | 81.81 | 98.07 | 85.89 | mutation (seeded success path) |
| AuthzService/promote_canary | destructive | OK | 73.27 | 73.27 | 73.27 | destructive: 1 real call against a seeded disposable target |
| AuthnService/finish_web_authn_authentication | mutation | OK | 59.64 | 59.64 | 59.64 | mutation (seeded success path) |
| AuthzService/activate_policy_version | destructive | OK | 59.05 | 59.05 | 59.05 | destructive: 1 real call against a seeded disposable target |
| AuthzService/approve_policy_draft | mutation | OK | 58.73 | 58.73 | 58.73 | mutation (seeded success path) |
| AuthzService/create_policy_draft | mutation | OK | 51.82 | 53.69 | 49.46 | mutation (seeded success path) |
| AuthzService/seed_builtin_roles | mutation | OK | 45.59 | 48.96 | 45.90 | mutation (seeded success path) |
| DataBroker/validate_catalog | destructive | OK | 45.98 | 45.98 | 45.98 | destructive: 1 real call against a seeded disposable target |
| AuthzService/rollback_policy_version | destructive | OK | 44.74 | 44.74 | 44.74 | destructive: 1 real call against a seeded disposable target |
| ControlPlaneService/list_node_states | read_only | OK | 31.02 | 44.51 | 32.53 | read_only (seeded success path) |
| AuthnService/introspect_token | read_only | OK | 22.56 | 43.50 | 30.59 | read_only (seeded success path) |
| AuthzService/migrate_legacy_policies | destructive | OK | 43.19 | 43.19 | 43.19 | destructive: 1 real call against a seeded disposable target |
| AssetService/start_pipeline | mutation | OK | 40.52 | 43.06 | 40.65 | mutation (seeded success path) |

## Full per-RPC table (sorted by service, then RPC)

| Service | RPC | kind | err | p50 ms | p99 ms | mean ms | note |
|---|---|---|---|--:|--:|--:|---|
| AnalyticsService | get_executor_performance | read_only | OK | 7.86 | 14.48 | 9.04 | read_only (seeded success path) |
| AnalyticsService | get_pipeline_summary | read_only | OK | 9.21 | 15.58 | 9.62 | read_only (seeded success path) |
| AnalyticsService | get_reconciliation_analytics | read_only | OK | 8.19 | 15.58 | 8.81 | read_only (seeded success path) |
| AnalyticsService | get_sla_compliance | read_only | OK | 6.58 | 9.39 | 6.91 | read_only (seeded success path) |
| AnalyticsService | get_throughput | read_only | OK | 7.38 | 10.06 | 7.30 | read_only (seeded success path) |
| AnalyticsService | record_pipeline_metric | mutation | OK | 10.85 | 11.24 | 11.15 | mutation (seeded success path) |
| AnalyticsService | trigger_snapshot | mutation | OK | 7.39 | 8.61 | 7.81 | mutation (seeded success path) |
| ApiKeyService | create_api_key | mutation | OK | 12.01 | 13.12 | 11.91 | mutation (seeded success path) |
| ApiKeyService | emergency_revoke_api_keys | destructive | OK | 40.16 | 40.16 | 40.16 | destructive: 1 real call against a seeded disposable target |
| ApiKeyService | get_api_key | read_only | OK | 5.14 | 9.76 | 5.66 | read_only (seeded success path) |
| ApiKeyService | get_api_key_usage_stats | read_only | OK | 7.42 | 13.82 | 8.43 | read_only (seeded success path) |
| ApiKeyService | list_api_keys | read_only | OK | 6.63 | 9.95 | 6.75 | read_only (seeded success path) |
| ApiKeyService | revoke_api_key | mutation | OK | 19.20 | 19.20 | 19.20 | mutation (seeded success path) |
| ApiKeyService | rotate_api_key | mutation | OK | 27.07 | 27.07 | 27.07 | mutation (seeded success path) |
| ApiKeyService | update_api_key | mutation | OK | 18.70 | 20.92 | 19.31 | mutation (seeded success path) |
| ApiKeyService | validate_api_key | read_only | OK | 8.69 | 13.67 | 9.39 | read_only (seeded success path) |
| AssetService | complete_step | mutation | OK | 29.45 | 31.45 | 30.13 | mutation (seeded success path) |
| AssetService | create_pipeline_definition | mutation | OK | 14.67 | 14.82 | 14.42 | mutation (seeded success path) |
| AssetService | get_asset | read_only | OK | 8.27 | 10.47 | 8.46 | read_only (seeded success path) |
| AssetService | get_pipeline | read_only | OK | 9.46 | 12.30 | 9.58 | read_only (seeded success path) |
| AssetService | get_pipeline_definition | read_only | OK | 8.46 | 13.09 | 8.94 | read_only (seeded success path) |
| AssetService | list_assets | read_only | OK | 11.24 | 17.32 | 12.14 | read_only (seeded success path) |
| AssetService | register_asset | mutation | OK | 19.58 | 20.66 | 20.24 | mutation (seeded success path) |
| AssetService | start_pipeline | mutation | OK | 40.52 | 43.06 | 40.65 | mutation (seeded success path) |
| AuthnService | admin_reset_mfa | destructive | OK | 26.54 | 26.54 | 26.54 | destructive: 1 real call against a seeded disposable target |
| AuthnService | admin_reset_password | destructive | OK | 12.74 | 12.74 | 12.74 | destructive: 1 real call against a seeded disposable target |
| AuthnService | admin_revoke_all_tenant_sessions | destructive | OK | 21.17 | 21.17 | 21.17 | destructive: 1 real call against a seeded disposable target |
| AuthnService | admin_revoke_all_user_sessions | destructive | OK | 19.38 | 19.38 | 19.38 | destructive: 1 real call against a seeded disposable target |
| AuthnService | admin_revoke_session | destructive | OK | 16.23 | 16.23 | 16.23 | destructive: 1 real call against a seeded disposable target |
| AuthnService | authenticate | read_only | OK | 30.13 | 133.73 | 44.38 | read_only (seeded success path) |
| AuthnService | change_password | mutation | OK | 773.52 | 773.52 | 773.52 | mutation (seeded success path) |
| AuthnService | change_user_status | destructive | OK | 30.14 | 30.14 | 30.14 | destructive: 1 real call against a seeded disposable target |
| AuthnService | confirm_mfaenrollment | mutation | OK | 4.56 | 4.75 | 4.74 | mutation (seeded success path) |
| AuthnService | create_session | mutation | OK | 6.56 | 7.69 | 7.38 | mutation (seeded success path) |
| AuthnService | create_user | mutation | OK | 358.21 | 358.83 | 366.88 | mutation (seeded success path) |
| AuthnService | delete_web_authn_credential | mutation | OK | 12.34 | 14.73 | 12.59 | mutation (seeded success path) |
| AuthnService | disable_mfa_factor | mutation | OK | 13.29 | 14.04 | 13.98 | mutation (seeded success path) |
| AuthnService | emergency_revoke | destructive | OK | 13.15 | 13.15 | 13.15 | destructive: 1 real call against a seeded disposable target |
| AuthnService | enroll_mfa | mutation | OK | 17.67 | 21.43 | 21.40 | mutation (seeded success path) |
| AuthnService | finish_web_authn_authentication | mutation | OK | 59.64 | 59.64 | 59.64 | mutation (seeded success path) |
| AuthnService | finish_web_authn_registration | mutation | OK | 35.69 | 35.69 | 35.69 | mutation (seeded success path) |
| AuthnService | forgot_password | mutation | OK | 8.21 | 10.01 | 8.61 | mutation (seeded success path) |
| AuthnService | generate_recovery_codes | mutation | OK | 28.74 | 35.61 | 32.11 | mutation (seeded success path) |
| AuthnService | get_jwks | read_only | OK | 4.81 | 6.61 | 5.02 | read_only (seeded success path) |
| AuthnService | get_mfa_policy | read_only | OK | 4.47 | 6.59 | 4.88 | read_only (seeded success path) |
| AuthnService | get_session | read_only | OK | 5.28 | 11.08 | 6.21 | read_only (seeded success path) |
| AuthnService | get_user | read_only | OK | 5.19 | 8.38 | 5.78 | read_only (seeded success path) |
| AuthnService | introspect_token | read_only | OK | 22.56 | 43.50 | 30.59 | read_only (seeded success path) |
| AuthnService | issue_mfa_challenge | mutation | OK | 13.66 | 13.78 | 12.90 | mutation (seeded success path) |
| AuthnService | list_devices | read_only | OK | 6.50 | 11.61 | 7.15 | read_only (seeded success path) |
| AuthnService | list_mfa_factors | read_only | OK | 7.66 | 13.35 | 8.35 | read_only (seeded success path) |
| AuthnService | list_sessions | read_only | OK | 11.89 | 17.53 | 12.70 | read_only (seeded success path) |
| AuthnService | list_users | read_only | OK | 10.44 | 17.71 | 11.40 | read_only (seeded success path) |
| AuthnService | list_web_authn_credentials | read_only | OK | 6.24 | 10.91 | 7.04 | read_only (seeded success path) |
| AuthnService | login | mutation | OK | 400.07 | 403.38 | 403.33 | mutation (seeded success path) |
| AuthnService | logout | mutation | OK | 5.95 | 6.40 | 6.20 | mutation (seeded success path) |
| AuthnService | put_mfa_policy | mutation | OK | 7.71 | 8.57 | 7.37 | mutation (seeded success path) |
| AuthnService | refresh_session | mutation | OK | 19.79 | 20.07 | 19.88 | mutation (seeded success path) |
| AuthnService | refresh_token | mutation | OK | 8.88 | 8.88 | 8.88 | mutation (seeded success path) |
| AuthnService | rename_passkey | mutation | OK | 8.77 | 9.74 | 9.53 | mutation (seeded success path) |
| AuthnService | resend_otp | mutation | OK | 16.17 | 17.48 | 16.46 | mutation (seeded success path) |
| AuthnService | reset_password | mutation | OK | 401.11 | 401.11 | 401.11 | mutation (seeded success path) |
| AuthnService | revoke_device | mutation | OK | 14.31 | 14.31 | 14.31 | mutation (seeded success path) |
| AuthnService | revoke_recovery_codes | mutation | OK | 10.78 | 11.40 | 10.25 | mutation (seeded success path) |
| AuthnService | revoke_session | mutation | OK | 6.45 | 6.65 | 6.68 | mutation (seeded success path) |
| AuthnService | send_otp | mutation | OK | 14.17 | 15.70 | 14.94 | mutation (seeded success path) |
| AuthnService | send_phone_verification | mutation | OK | 15.81 | 20.70 | 18.74 | mutation (seeded success path) |
| AuthnService | start_web_authn_authentication | mutation | OK | 14.30 | 14.81 | 15.11 | mutation (seeded success path) |
| AuthnService | start_web_authn_registration | mutation | OK | 14.72 | 16.30 | 15.20 | mutation (seeded success path) |
| AuthnService | update_user | mutation | OK | 11.05 | 11.37 | 11.23 | mutation (seeded success path) |
| AuthnService | validate_csrf | read_only | OK | 6.35 | 12.21 | 6.97 | read_only (seeded success path) |
| AuthnService | validate_token | read_only | OK | 19.08 | 38.34 | 21.98 | read_only (seeded success path) |
| AuthnService | verify_mfa_challenge | read_only | OK | 8.05 | 15.91 | 8.87 | read_only (seeded success path) |
| AuthnService | verify_otp | read_only | OK | 18.90 | 27.51 | 19.37 | read_only (seeded success path) |
| AuthzService | activate_canary | destructive | OK | 39.78 | 39.78 | 39.78 | destructive: 1 real call against a seeded disposable target |
| AuthzService | activate_policy_version | destructive | OK | 59.05 | 59.05 | 59.05 | destructive: 1 real call against a seeded disposable target |
| AuthzService | approve_policy_draft | mutation | OK | 58.73 | 58.73 | 58.73 | mutation (seeded success path) |
| AuthzService | assign_role | mutation | OK | 29.91 | 30.16 | 29.16 | mutation (seeded success path) |
| AuthzService | authorize | read_only | OK | 20.08 | 25.03 | 20.36 | read_only (seeded success path) |
| AuthzService | batch_check_permissions | read_only | OK | 11.02 | 17.40 | 11.95 | read_only (seeded success path) |
| AuthzService | check_access | read_only | OK | 9.80 | 12.34 | 10.34 | read_only (seeded success path) |
| AuthzService | create_policy_draft | mutation | OK | 51.82 | 53.69 | 49.46 | mutation (seeded success path) |
| AuthzService | create_policy_rule | mutation | OK | 25.13 | 25.53 | 24.23 | mutation (seeded success path) |
| AuthzService | create_role | mutation | OK | 29.10 | 29.54 | 28.21 | mutation (seeded success path) |
| AuthzService | delete_policy_rule | mutation | OK | 12.12 | 13.16 | 12.10 | mutation (seeded success path) |
| AuthzService | delete_role | mutation | OK | 10.68 | 11.81 | 15.18 | mutation (seeded success path) |
| AuthzService | diff_policy_draft | read_only | OK | 12.31 | 16.90 | 13.01 | read_only (seeded success path) |
| AuthzService | explain_policy | read_only | OK | 8.26 | 10.79 | 8.41 | read_only (seeded success path) |
| AuthzService | get_authz_revision | read_only | OK | 4.79 | 7.60 | 5.31 | read_only (seeded success path) |
| AuthzService | get_canary_status | read_only | OK | 11.23 | 15.52 | 11.73 | read_only (seeded success path) |
| AuthzService | get_native_access | read_only | OK | 17.45 | 19.58 | 17.56 | read_only (seeded success path) |
| AuthzService | get_policy_bundle | read_only | OK | 9.69 | 13.45 | 9.54 | read_only (seeded success path) |
| AuthzService | get_policy_rule | read_only | OK | 6.45 | 8.82 | 6.52 | read_only (seeded success path) |
| AuthzService | get_role | read_only | OK | 4.92 | 6.75 | 5.10 | read_only (seeded success path) |
| AuthzService | invalidate_policy_bundles | destructive | OK | 33.89 | 33.89 | 33.89 | destructive: 1 real call against a seeded disposable target |
| AuthzService | lint_authz_policies | read_only | OK | 1.91 | 2.37 | 1.93 | read_only (seeded success path) |
| AuthzService | list_access_decision_audits | read_only | OK | 16.56 | 28.19 | 18.95 | read_only (seeded success path) |
| AuthzService | list_policy_rules | read_only | OK | 6.75 | 10.30 | 7.20 | read_only (seeded success path) |
| AuthzService | list_policy_versions | read_only | OK | 10.94 | 18.20 | 12.36 | read_only (seeded success path) |
| AuthzService | list_roles | read_only | OK | 5.07 | 7.34 | 5.22 | read_only (seeded success path) |
| AuthzService | list_user_permissions | read_only | OK | 1.81 | 2.50 | 1.87 | read_only (seeded success path) |
| AuthzService | list_user_roles | read_only | OK | 5.89 | 10.03 | 6.24 | read_only (seeded success path) |
| AuthzService | migrate_legacy_policies | destructive | OK | 43.19 | 43.19 | 43.19 | destructive: 1 real call against a seeded disposable target |
| AuthzService | promote_canary | destructive | OK | 73.27 | 73.27 | 73.27 | destructive: 1 real call against a seeded disposable target |
| AuthzService | put_authz_policy | mutation | OK | 19.49 | 21.11 | 19.32 | mutation (seeded success path) |
| AuthzService | put_relationship | mutation | OK | 25.96 | 26.00 | 25.80 | mutation (seeded success path) |
| AuthzService | put_role_binding | mutation | OK | 19.59 | 20.18 | 18.69 | mutation (seeded success path) |
| AuthzService | reject_policy_draft | mutation | OK | 31.26 | 31.26 | 31.26 | mutation (seeded success path) |
| AuthzService | revoke_role | mutation | OK | 10.98 | 11.47 | 14.18 | mutation (seeded success path) |
| AuthzService | rollback_policy_version | destructive | OK | 44.74 | 44.74 | 44.74 | destructive: 1 real call against a seeded disposable target |
| AuthzService | seed_builtin_roles | mutation | OK | 45.59 | 48.96 | 45.90 | mutation (seeded success path) |
| AuthzService | simulate_policy | mutation | OK | 18.89 | 20.79 | 21.36 | mutation (seeded success path) |
| AuthzService | submit_policy_draft | mutation | OK | 29.36 | 29.36 | 29.36 | mutation (seeded success path) |
| AuthzService | update_policy_draft | mutation | OK | 31.37 | 31.70 | 33.78 | mutation (seeded success path) |
| AuthzService | update_role | mutation | OK | 20.01 | 20.63 | 19.58 | mutation (seeded success path) |
| ControlPlaneService | ack_status | mutation | OK | 8.66 | 8.78 | 8.28 | mutation (seeded success path) |
| ControlPlaneService | delta_resources | stream_open | OK | 0.32 | 0.32 | 0.32 | streaming: stream-open latency |
| ControlPlaneService | get_resources | read_only | OK | 5.98 | 9.64 | 6.23 | read_only (seeded success path) |
| ControlPlaneService | list_node_states | read_only | OK | 31.02 | 44.51 | 32.53 | read_only (seeded success path) |
| ControlPlaneService | stream_resources | stream_open | OK | 0.11 | 0.11 | 0.11 | streaming: stream-open latency |
| DataBroker | activate_catalog | destructive | OK | 6.33 | 6.33 | 6.33 | destructive: 1 real call against a seeded disposable target |
| DataBroker | analytical_query | read_only | OK | 8.64 | 12.35 | 8.98 | read_only (seeded success path) |
| DataBroker | apply_migration | mutation | OK | 194.49 | 194.49 | 194.49 | mutation (seeded success path) |
| DataBroker | approve_migration_plan | mutation | OK | 32.06 | 32.06 | 32.06 | mutation (seeded success path) |
| DataBroker | batch_select | stream_open | OK | 0.16 | 0.16 | 0.16 | streaming: stream-open latency |
| DataBroker | batch_upsert | stream_open | OK | 0.10 | 0.10 | 0.10 | streaming: stream-open latency |
| DataBroker | begin_tx | stream_open | OK | 0.10 | 0.10 | 0.10 | streaming: stream-open latency |
| DataBroker | cache_delete | mutation | OK | 6.09 | 6.96 | 6.36 | mutation (seeded success path) |
| DataBroker | cache_get | read_only | OK | 6.68 | 8.92 | 6.97 | read_only (seeded success path) |
| DataBroker | cache_scan | read_only | OK | 9.25 | 14.77 | 9.96 | read_only (seeded success path) |
| DataBroker | cache_set | mutation | OK | 6.77 | 9.11 | 9.78 | mutation (seeded success path) |
| DataBroker | create_materialized_view | mutation | OK | 7.07 | 7.74 | 7.24 | mutation (seeded success path) |
| DataBroker | delete | mutation | OK | 26.87 | 30.11 | 28.03 | mutation (seeded success path) |
| DataBroker | delete_policy | mutation | OK | 14.78 | 14.78 | 14.78 | mutation (seeded success path) |
| DataBroker | dismiss_dlq_event | mutation | OK | 13.93 | 14.26 | 13.48 | mutation (seeded success path) |
| DataBroker | document_delete | mutation | OK | 5.43 | 6.07 | 5.89 | mutation (seeded success path) |
| DataBroker | document_find | read_only | OK | 6.67 | 10.67 | 7.27 | read_only (seeded success path) |
| DataBroker | document_get | read_only | OK | 7.52 | 12.12 | 7.65 | read_only (seeded success path) |
| DataBroker | document_upsert | mutation | OK | 6.17 | 6.18 | 6.59 | mutation (seeded success path) |
| DataBroker | drop_resource | destructive | OK | 19.55 | 19.55 | 19.55 | destructive: 1 real call against a seeded disposable target |
| DataBroker | enqueue_outbox_event | mutation | OK | 13.89 | 14.92 | 13.50 | mutation (seeded success path) |
| DataBroker | ensure_baseline | mutation | OK | 16.58 | 16.58 | 18.70 | mutation (seeded success path) |
| DataBroker | ensure_project | mutation | OK | 15.96 | 16.27 | 16.03 | mutation (seeded success path) |
| DataBroker | ensure_resource | mutation | OK | 18.24 | 21.05 | 19.10 | mutation (seeded success path) |
| DataBroker | generate_presigned_url | mutation | OK | 6.54 | 6.58 | 6.56 | mutation (seeded success path) |
| DataBroker | generic_dispatch | mutation | OK | 12.40 | 13.42 | 11.70 | mutation (seeded success path) |
| DataBroker | get_admin_summary | read_only | OK | 26.15 | 36.44 | 27.17 | read_only (seeded success path) |
| DataBroker | get_capabilities | read_only | OK | 7.48 | 12.61 | 8.00 | read_only (seeded success path) |
| DataBroker | get_catalog_manifest | read_only | OK | 12.94 | 26.16 | 14.36 | read_only (seeded success path) |
| DataBroker | get_catalog_version | read_only | OK | 6.40 | 9.01 | 6.38 | read_only (seeded success path) |
| DataBroker | get_catalog_versions | read_only | OK | 5.23 | 8.22 | 5.47 | read_only (seeded success path) |
| DataBroker | get_cdc_status | read_only | OK | 6.38 | 11.04 | 6.48 | read_only (seeded success path) |
| DataBroker | get_dlq_event | read_only | OK | 5.79 | 7.67 | 5.94 | read_only (seeded success path) |
| DataBroker | get_health_report | read_only | OK | 3.23 | 4.52 | 3.38 | read_only (seeded success path) |
| DataBroker | get_migration_status | read_only | OK | 7.54 | 10.30 | 8.36 | read_only (seeded success path) |
| DataBroker | get_object | stream | OK | 7.63 | 17.27 | 11.72 | streaming: time-to-first-response (seeded) |
| DataBroker | get_saga | read_only | OK | 6.38 | 13.04 | 7.35 | read_only (seeded success path) |
| DataBroker | graph_mutate | mutation | OK | 25.13 | 30.57 | 54.08 | mutation (seeded success path) |
| DataBroker | graph_query | read_only | OK | 18.69 | 23.67 | 18.96 | read_only (seeded success path) |
| DataBroker | initiate_multipart_upload | mutation | OK | 10.29 | 12.88 | 12.08 | mutation (seeded success path) |
| DataBroker | lint_policies | read_only | OK | 7.54 | 13.07 | 8.04 | read_only (seeded success path) |
| DataBroker | list_admin_audit_logs | read_only | OK | 8.29 | 14.07 | 9.31 | read_only (seeded success path) |
| DataBroker | list_dlq_events | read_only | OK | 6.10 | 8.18 | 6.38 | read_only (seeded success path) |
| DataBroker | list_message_schemas | read_only | OK | 3.21 | 4.42 | 3.23 | read_only (seeded success path) |
| DataBroker | list_migration_runs | read_only | OK | 7.18 | 9.79 | 7.55 | read_only (seeded success path) |
| DataBroker | list_policies | read_only | OK | 6.17 | 11.72 | 6.69 | read_only (seeded success path) |
| DataBroker | list_projects | read_only | OK | 7.46 | 12.20 | 7.86 | read_only (seeded success path) |
| DataBroker | list_resources | read_only | OK | 6.12 | 7.63 | 6.12 | read_only (seeded success path) |
| DataBroker | list_sagas | read_only | OK | 5.52 | 8.37 | 6.01 | read_only (seeded success path) |
| DataBroker | lookup_message_schema | read_only | OK | 3.58 | 4.36 | 3.63 | read_only (seeded success path) |
| DataBroker | mark_saga_reviewed | mutation | OK | 17.62 | 17.68 | 17.17 | mutation (seeded success path) |
| DataBroker | pause_cdc | mutation | OK | 15.30 | 16.45 | 15.87 | mutation (seeded success path) |
| DataBroker | plan_migration | mutation | OK | 20.67 | 21.90 | 20.22 | mutation (seeded success path) |
| DataBroker | preview_cdc_redaction | read_only | OK | 10.64 | 15.53 | 11.33 | read_only (seeded success path) |
| DataBroker | publish_cdc | stream | OK | 22.35 | 22.35 | 88.74 | cdc: time-to-first-event (real seeded Upsert produced) |
| DataBroker | put_object | stream_open | OK | 0.87 | 0.87 | 0.87 | streaming: stream-open latency |
| DataBroker | put_policy | destructive | OK | 15.03 | 15.03 | 15.03 | destructive: 1 real call against a seeded disposable target |
| DataBroker | quarantine_dlq_event | mutation | OK | 16.44 | 23.22 | 18.71 | mutation (seeded success path) |
| DataBroker | reload_policies | destructive | OK | 12.73 | 12.73 | 12.73 | destructive: 1 real call against a seeded disposable target |
| DataBroker | replay_dlq_event | mutation | OK | 16.39 | 16.39 | 16.39 | mutation (seeded success path) |
| DataBroker | resume_cdc | mutation | OK | 14.10 | 14.66 | 14.51 | mutation (seeded success path) |
| DataBroker | retry_saga_compensation | mutation | OK | 22.56 | 22.56 | 22.56 | mutation (seeded success path) |
| DataBroker | rollback_catalog | destructive | OK | 5.37 | 5.37 | 5.37 | destructive: 1 real call against a seeded disposable target |
| DataBroker | scan_projection_drift | read_only | OK | 14.36 | 23.76 | 15.64 | read_only (seeded success path) |
| DataBroker | select | read_only | OK | 8.50 | 15.95 | 9.29 | read_only (seeded success path) |
| DataBroker | select_v_2 | stream | OK | 8.19 | 9.30 | 8.33 | streaming: time-to-first-response (seeded) |
| DataBroker | stage_catalog | destructive | OK | 240.91 | 240.91 | 240.91 | destructive: 1 real call against a seeded disposable target |
| DataBroker | step_down_cdc_leader | mutation | OK | 14.64 | 15.76 | 15.22 | mutation (seeded success path) |
| DataBroker | time_series_query | read_only | OK | 9.28 | 13.25 | 9.72 | read_only (seeded success path) |
| DataBroker | time_series_write | mutation | OK | 4.40 | 4.63 | 4.76 | mutation (seeded success path) |
| DataBroker | upsert | mutation | OK | 35.11 | 36.18 | 33.90 | mutation (seeded success path) |
| DataBroker | validate_catalog | destructive | OK | 45.98 | 45.98 | 45.98 | destructive: 1 real call against a seeded disposable target |
| DataBroker | vector_batch_upsert | stream_open | OK | 0.07 | 0.07 | 0.07 | streaming: stream-open latency |
| DataBroker | vector_hybrid_search | read_only | OK | 7.49 | 12.59 | 7.85 | read_only (seeded success path) |
| DataBroker | vector_search | read_only | OK | 7.30 | 10.31 | 7.33 | read_only (seeded success path) |
| DataBroker | vector_upsert | mutation | OK | 11.99 | 12.84 | 13.67 | mutation (seeded success path) |
| DataBroker | verify_admin_audit_log | read_only | OK | 11.35 | 18.68 | 11.58 | read_only (seeded success path) |
| IdentityProviderService | create_provider | mutation | OK | 17.13 | 18.78 | 17.63 | mutation (seeded success path) |
| IdentityProviderService | disable_provider | mutation | OK | 17.86 | 18.40 | 16.64 | mutation (seeded success path) |
| IdentityProviderService | force_jwks_refresh | mutation | OK | 29.14 | 29.44 | 28.41 | mutation (seeded success path) |
| IdentityProviderService | get_provider | read_only | OK | 6.82 | 10.77 | 7.15 | read_only (seeded success path) |
| IdentityProviderService | import_saml_metadata | mutation | OK | 14.66 | 16.62 | 15.41 | mutation (seeded success path) |
| IdentityProviderService | link_identity | mutation | OK | 15.74 | 20.61 | 17.53 | mutation (seeded success path) |
| IdentityProviderService | list_external_identities | read_only | OK | 9.45 | 13.73 | 9.58 | read_only (seeded success path) |
| IdentityProviderService | list_providers | read_only | OK | 9.03 | 12.69 | 9.25 | read_only (seeded success path) |
| IdentityProviderService | preview_claim_mapping | read_only | OK | 4.95 | 9.59 | 5.71 | read_only (seeded success path) |
| IdentityProviderService | preview_group_mapping | read_only | OK | 6.37 | 9.34 | 6.60 | read_only (seeded success path) |
| IdentityProviderService | resolve_external_identity | mutation | OK | 34.16 | 35.70 | 32.76 | mutation (seeded success path) |
| IdentityProviderService | saml_acs | mutation | OK | 81.81 | 98.07 | 85.89 | mutation (seeded success path) |
| IdentityProviderService | scim_create_group | mutation | OK | 7.70 | 7.83 | 7.42 | mutation (seeded success path) |
| IdentityProviderService | scim_create_user | mutation | OK | 25.25 | 35.71 | 28.33 | mutation (seeded success path) |
| IdentityProviderService | scim_delete_group | mutation | OK | 5.66 | 8.31 | 6.94 | mutation (seeded success path) |
| IdentityProviderService | scim_delete_user | mutation | OK | 37.81 | 37.81 | 37.81 | mutation (seeded success path) |
| IdentityProviderService | scim_get_group | mutation | OK | 7.04 | 7.20 | 7.13 | mutation (seeded success path) |
| IdentityProviderService | scim_get_user | mutation | OK | 8.04 | 8.99 | 8.36 | mutation (seeded success path) |
| IdentityProviderService | scim_list_groups | mutation | OK | 5.87 | 6.34 | 6.37 | mutation (seeded success path) |
| IdentityProviderService | scim_list_users | mutation | OK | 16.94 | 20.32 | 17.27 | mutation (seeded success path) |
| IdentityProviderService | scim_patch_group | mutation | OK | 14.53 | 15.27 | 14.33 | mutation (seeded success path) |
| IdentityProviderService | scim_patch_user | mutation | OK | 24.81 | 25.62 | 23.40 | mutation (seeded success path) |
| IdentityProviderService | scim_replace_user | mutation | OK | 16.69 | 23.07 | 20.33 | mutation (seeded success path) |
| IdentityProviderService | start_saml_login | mutation | OK | 5.24 | 7.01 | 5.78 | mutation (seeded success path) |
| IdentityProviderService | test_provider_discovery | read_only | OK | 6.32 | 11.75 | 7.01 | read_only (seeded success path) |
| IdentityProviderService | unlink_identity | mutation | OK | 5.75 | 6.54 | 8.40 | mutation (seeded success path) |
| IdentityProviderService | update_provider | mutation | OK | 18.92 | 22.41 | 20.51 | mutation (seeded success path) |
| NotificationService | get_delivery_stats | read_only | OK | 8.80 | 18.68 | 10.67 | read_only (seeded success path) |
| NotificationService | get_notification | read_only | OK | 8.72 | 14.97 | 9.46 | read_only (seeded success path) |
| NotificationService | get_preference | read_only | OK | 8.59 | 13.38 | 9.52 | read_only (seeded success path) |
| NotificationService | get_template | read_only | OK | 12.32 | 16.95 | 12.14 | read_only (seeded success path) |
| NotificationService | list_notifications | read_only | OK | 14.91 | 22.90 | 15.70 | read_only (seeded success path) |
| NotificationService | list_preferences | read_only | OK | 15.92 | 26.24 | 17.78 | read_only (seeded success path) |
| NotificationService | list_templates | read_only | OK | 16.42 | 20.92 | 16.67 | read_only (seeded success path) |
| NotificationService | retry_notification | mutation | OK | 14.40 | 14.40 | 14.40 | mutation (seeded success path) |
| NotificationService | send_notification | mutation | OK | 28.38 | 31.17 | 27.92 | mutation (seeded success path) |
| NotificationService | set_preference | mutation | OK | 7.03 | 7.52 | 7.06 | mutation (seeded success path) |
| NotificationService | upsert_template | mutation | OK | 6.28 | 6.56 | 6.52 | mutation (seeded success path) |
| PeerService | get_peer | read_only | OK | 12.13 | 19.85 | 12.30 | read_only (seeded success path) |
| PeerService | join_room | mutation | OK | 18.94 | 21.65 | 20.85 | mutation (seeded success path) |
| PeerService | join_session | mutation | OK | 22.33 | 23.80 | 20.68 | mutation (seeded success path) |
| PeerService | leave_room | mutation | OK | 7.89 | 11.31 | 9.03 | mutation (seeded success path) |
| PeerService | list_peers | read_only | OK | 10.44 | 13.10 | 10.68 | read_only (seeded success path) |
| RoomService | close_room | mutation | OK | 21.13 | 23.57 | 23.62 | mutation (seeded success path) |
| RoomService | create_room | mutation | OK | 11.28 | 11.56 | 11.00 | mutation (seeded success path) |
| RoomService | get_room | read_only | OK | 9.42 | 14.35 | 9.78 | read_only (seeded success path) |
| RoomService | list_rooms | read_only | OK | 8.35 | 11.39 | 8.66 | read_only (seeded success path) |
| RoomService | update_room | mutation | OK | 7.40 | 7.45 | 7.13 | mutation (seeded success path) |
| SignalingService | signal | stream_open | OK | 5.85 | 5.85 | 5.85 | streaming: stream-open latency |
| StorageService | delete_file | mutation | OK | 22.20 | 22.20 | 22.20 | mutation (seeded success path) |
| StorageService | download_file | stream | OK | 18.81 | 23.48 | 21.15 | streaming: time-to-first-response (seeded) |
| StorageService | finalize_upload | mutation | OK | 27.28 | 27.28 | 27.28 | mutation (seeded success path) |
| StorageService | get_download_url | read_only | OK | 9.07 | 11.09 | 9.54 | read_only (seeded success path) |
| StorageService | get_file | read_only | OK | 8.32 | 16.67 | 10.12 | read_only (seeded success path) |
| StorageService | list_files | read_only | OK | 17.14 | 26.31 | 17.93 | read_only (seeded success path) |
| StorageService | register_upload | mutation | OK | 12.73 | 16.91 | 13.94 | mutation (seeded success path) |
| StorageService | update_file | mutation | OK | 23.36 | 25.79 | 23.72 | mutation (seeded success path) |
| TenantService | create_tenant | mutation | OK | 10.45 | 11.98 | 10.89 | mutation (seeded success path) |
| TenantService | get_tenant | read_only | OK | 12.42 | 16.51 | 12.08 | read_only (seeded success path) |
| TenantService | get_tenant_config | read_only | OK | 11.70 | 18.41 | 11.99 | read_only (seeded success path) |
| TenantService | list_tenants | read_only | OK | 8.87 | 12.72 | 9.31 | read_only (seeded success path) |
| TenantService | update_tenant | mutation | OK | 7.43 | 7.64 | 7.20 | mutation (seeded success path) |
| TenantService | update_tenant_config | mutation | OK | 18.79 | 20.49 | 19.74 | mutation (seeded success path) |
| TrackService | list_tracks | read_only | OK | 10.34 | 13.77 | 10.84 | read_only (seeded success path) |
| TrackService | mute_track | mutation | OK | 6.47 | 8.45 | 7.16 | mutation (seeded success path) |
| TrackService | publish_track | mutation | OK | 12.27 | 14.76 | 13.10 | mutation (seeded success path) |
| TrackService | unpublish_track | mutation | OK | 5.57 | 6.26 | 5.83 | mutation (seeded success path) |
| TurnService | issue_credentials | mutation | OK | 4.75 | 6.90 | 6.48 | mutation (seeded success path) |
