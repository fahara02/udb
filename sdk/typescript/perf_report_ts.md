# UDB SDK Live Perf — TypeScript (localhost)

RPCs measured: 262   tenant=c370aba0-93ac-44b4-95eb-9d9077c6be39

Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities (a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request's reference/ID fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection latency. Any residual non-OK RPC is listed under Failures for the maintainer to finish.

Unary = full request/response round-trip. Non-CDC server-streaming RPCs (kind=stream) report time-to-FIRST-RESPONSE with seeded inputs; client-streaming/bidi RPCs (kind=stream_open) report stream-open latency. CDC subscription (publish_c_d_c, kind=stream) reports time-to-FIRST-EVENT: the harness subscribes, fires a real seeded Upsert that flows outbox→CDC→Kafka, and times the first delivered event.

RPCs run on the AUTH ROUTE in three phases (BENCH_RPC_BODIES.md "Execution order"): Phase 1 establishes the session (AuthnService login → refresh_token → refresh_session → authenticate → validate_token → introspect_token → get_jwks), then the seed phase; Phase 2 measures everything else; Phase 3 LAST runs the session/credential-teardown AuthnService RPCs (logout, revoke_*, change/reset password, admin_reset_mfa, disable_mfa_factor, …) against the seeded DISPOSABLE user/session so the admin's own session is never killed mid-run.

## Seeded fixtures

Captured semantic field → seeded value keys used to resolve request fields: action, apply_run_id, approval_token, approve_draft_id, approve_run_id, approved_by, asset_id, assigned_by, auth_challenge_id, bucket, canary_id, canary_version_id, challenge_id, close_room_id, code, collection, content_type, created_by, csrf_token, definition_id, delete_file_id, delete_policy_id, delete_role_id, delete_scim_user_id, deleted_by, device_id, disable_provider_id, dismiss_dlq_id, dlq_id, domain, ds_policy_id, event_type, external_identity_id, file_id, file_type, filename, instance_id, key_id, key_prefix, kind, leave_peer_id, locale, log_id, mark_saga_id, message_type, migration_id, mongo_collection, name, node_id, notification_id, object, object_key, otp_code, otp_id, owner_id, peer_id, plain_key, policy_draft_id, policy_id, policy_version_id, project, project_id, provider_id, quarantine_dlq_id, recipient_id, record_id, recovery_code, refresh_token, reg_challenge_id, reject_draft_id, rejected_by, relation, replay_dlq_id, reset_otp_code, reset_otp_id, resource, retry_saga_id, revoke_key_id, revoke_key_prefix, revoked_by, role, role_code, role_id, rollback_policy_set_id, rollback_target_version_id, room_id, saga_id, saml_provider_id, scim_group_id, scim_user_id, session_id, stage_name, step_id, subject, tenant, tenant_id, token, track_id, ts_table, unpublish_track_id, update_draft_id, update_key_id, update_key_prefix, updated_by, user_id, user_role_id, username

## Per-service mean latency

| Service | RPCs | mean ms |
|---|--:|--:|
| AuthnService | 50 | 60.23 |
| StorageService | 7 | 23.22 |
| AuthzService | 41 | 22.79 |
| DataBroker | 76 | 21.97 |
| IdentityProviderService | 27 | 21.17 |
| AssetService | 8 | 19.16 |
| ApiKeyService | 9 | 16.03 |
| NotificationService | 11 | 14.79 |
| RoomService | 5 | 14.25 |
| TenantService | 6 | 14.23 |
| PeerService | 4 | 13.25 |
| TrackService | 4 | 10.74 |
| ControlPlaneService | 5 | 9.38 |
| TurnService | 1 | 7.85 |
| AnalyticsService | 7 | 6.95 |
| SignalingService | 1 | 6.63 |

## Failures (0)

No RPC returned a non-OK gRPC status.

## Slowest 20 by p99

| RPC | kind | err | p50 ms | p99 ms | mean ms | note |
|---|---|---|--:|--:|--:|---|
| AuthnService/change_password | mutation | OK | 846.13 | 846.13 | 846.13 | mutation (seeded success path) |
| AuthnService/reset_password | mutation | OK | 494.40 | 494.40 | 494.40 | mutation (seeded success path) |
| AuthnService/create_user | mutation | OK | 481.37 | 488.75 | 481.49 | mutation (seeded success path) |
| AuthnService/login | mutation | OK | 415.42 | 451.90 | 430.45 | mutation (seeded success path) |
| DataBroker/stage_catalog | destructive | OK | 328.40 | 328.40 | 328.40 | destructive: 1 real call against a seeded disposable target |
| DataBroker/apply_migration | mutation | OK | 222.85 | 222.85 | 222.85 | mutation (seeded success path) |
| IdentityProviderService/saml_acs | mutation | OK | 113.01 | 114.79 | 108.35 | mutation (seeded success path) |
| AuthzService/promote_canary | destructive | OK | 76.69 | 76.69 | 76.69 | destructive: 1 real call against a seeded disposable target |
| AuthnService/finish_web_authn_authentication | mutation | OK | 69.38 | 69.38 | 69.38 | mutation (seeded success path) |
| DataBroker/validate_catalog | destructive | OK | 68.21 | 68.21 | 68.21 | destructive: 1 real call against a seeded disposable target |
| AuthzService/seed_builtin_roles | mutation | OK | 66.56 | 68.10 | 66.15 | mutation (seeded success path) |
| DataBroker/approve_migration_plan | mutation | OK | 63.73 | 63.73 | 63.73 | mutation (seeded success path) |
| DataBroker/upsert | mutation | OK | 61.96 | 62.71 | 61.91 | mutation (seeded success path) |
| AuthnService/generate_recovery_codes | mutation | OK | 57.13 | 62.57 | 54.68 | mutation (seeded success path) |
| AuthnService/finish_web_authn_registration | mutation | OK | 60.97 | 60.97 | 60.97 | mutation (seeded success path) |
| AuthzService/rollback_policy_version | destructive | OK | 57.13 | 57.13 | 57.13 | destructive: 1 real call against a seeded disposable target |
| IdentityProviderService/scim_delete_user | mutation | OK | 55.89 | 55.89 | 55.89 | mutation (seeded success path) |
| AssetService/start_pipeline | mutation | OK | 50.42 | 52.68 | 47.39 | mutation (seeded success path) |
| AuthzService/activate_policy_version | destructive | OK | 49.28 | 49.28 | 49.28 | destructive: 1 real call against a seeded disposable target |
| StorageService/delete_file | mutation | OK | 48.27 | 48.27 | 48.27 | mutation (seeded success path) |

## Full per-RPC table (sorted by service, then RPC)

| Service | RPC | kind | err | p50 ms | p99 ms | mean ms | note |
|---|---|---|---|--:|--:|--:|---|
| AnalyticsService | get_executor_performance | read_only | OK | 5.86 | 12.64 | 7.51 | read_only (seeded success path) |
| AnalyticsService | get_pipeline_summary | read_only | OK | 7.58 | 13.13 | 8.74 | read_only (seeded success path) |
| AnalyticsService | get_reconciliation_analytics | read_only | OK | 6.58 | 10.26 | 7.04 | read_only (seeded success path) |
| AnalyticsService | get_sla_compliance | read_only | OK | 5.01 | 8.42 | 5.52 | read_only (seeded success path) |
| AnalyticsService | get_throughput | read_only | OK | 5.96 | 8.46 | 5.96 | read_only (seeded success path) |
| AnalyticsService | record_pipeline_metric | mutation | OK | 7.40 | 7.77 | 7.24 | mutation (seeded success path) |
| AnalyticsService | trigger_snapshot | mutation | OK | 6.65 | 7.05 | 6.66 | mutation (seeded success path) |
| ApiKeyService | create_api_key | mutation | OK | 14.25 | 18.09 | 15.64 | mutation (seeded success path) |
| ApiKeyService | emergency_revoke_api_keys | destructive | OK | 43.09 | 43.09 | 43.09 | destructive: 1 real call against a seeded disposable target |
| ApiKeyService | get_api_key | read_only | OK | 4.84 | 8.48 | 5.54 | read_only (seeded success path) |
| ApiKeyService | get_api_key_usage_stats | read_only | OK | 7.89 | 16.49 | 9.42 | read_only (seeded success path) |
| ApiKeyService | list_api_keys | read_only | OK | 5.43 | 6.75 | 5.29 | read_only (seeded success path) |
| ApiKeyService | revoke_api_key | mutation | OK | 14.18 | 14.18 | 14.18 | mutation (seeded success path) |
| ApiKeyService | rotate_api_key | mutation | OK | 28.23 | 28.23 | 28.23 | mutation (seeded success path) |
| ApiKeyService | update_api_key | mutation | OK | 15.27 | 15.28 | 14.65 | mutation (seeded success path) |
| ApiKeyService | validate_api_key | read_only | OK | 8.11 | 10.44 | 8.24 | read_only (seeded success path) |
| AssetService | complete_step | mutation | OK | 22.60 | 37.64 | 28.46 | mutation (seeded success path) |
| AssetService | create_pipeline_definition | mutation | OK | 10.77 | 11.98 | 13.54 | mutation (seeded success path) |
| AssetService | get_asset | read_only | OK | 9.15 | 14.05 | 9.78 | read_only (seeded success path) |
| AssetService | get_pipeline | read_only | OK | 7.63 | 11.31 | 7.97 | read_only (seeded success path) |
| AssetService | get_pipeline_definition | read_only | OK | 9.36 | 11.95 | 9.46 | read_only (seeded success path) |
| AssetService | list_assets | read_only | OK | 13.68 | 17.05 | 13.52 | read_only (seeded success path) |
| AssetService | register_asset | mutation | OK | 16.57 | 19.25 | 23.17 | mutation (seeded success path) |
| AssetService | start_pipeline | mutation | OK | 50.42 | 52.68 | 47.39 | mutation (seeded success path) |
| AuthnService | admin_reset_mfa | destructive | OK | 43.57 | 43.57 | 43.57 | destructive: 1 real call against a seeded disposable target |
| AuthnService | admin_reset_password | destructive | OK | 17.73 | 17.73 | 17.73 | destructive: 1 real call against a seeded disposable target |
| AuthnService | admin_revoke_all_tenant_sessions | destructive | OK | 14.03 | 14.03 | 14.03 | destructive: 1 real call against a seeded disposable target |
| AuthnService | admin_revoke_all_user_sessions | destructive | OK | 15.15 | 15.15 | 15.15 | destructive: 1 real call against a seeded disposable target |
| AuthnService | admin_revoke_session | destructive | OK | 15.18 | 15.18 | 15.18 | destructive: 1 real call against a seeded disposable target |
| AuthnService | authenticate | read_only | OK | 16.05 | 23.44 | 17.47 | read_only (seeded success path) |
| AuthnService | change_password | mutation | OK | 846.13 | 846.13 | 846.13 | mutation (seeded success path) |
| AuthnService | change_user_status | destructive | OK | 16.47 | 16.47 | 16.47 | destructive: 1 real call against a seeded disposable target |
| AuthnService | confirm_m_f_a_enrollment | mutation | OK | 5.99 | 6.74 | 6.28 | mutation (seeded success path) |
| AuthnService | create_session | mutation | OK | 9.82 | 10.24 | 12.47 | mutation (seeded success path) |
| AuthnService | create_user | mutation | OK | 481.37 | 488.75 | 481.49 | mutation (seeded success path) |
| AuthnService | delete_web_authn_credential | mutation | OK | 9.13 | 9.40 | 8.99 | mutation (seeded success path) |
| AuthnService | disable_mfa_factor | mutation | OK | 14.91 | 17.11 | 17.23 | mutation (seeded success path) |
| AuthnService | emergency_revoke | destructive | OK | 14.04 | 14.04 | 14.04 | destructive: 1 real call against a seeded disposable target |
| AuthnService | enroll_m_f_a | mutation | OK | 19.91 | 23.55 | 21.98 | mutation (seeded success path) |
| AuthnService | finish_web_authn_authentication | mutation | OK | 69.38 | 69.38 | 69.38 | mutation (seeded success path) |
| AuthnService | finish_web_authn_registration | mutation | OK | 60.97 | 60.97 | 60.97 | mutation (seeded success path) |
| AuthnService | forgot_password | mutation | OK | 14.32 | 14.61 | 13.86 | mutation (seeded success path) |
| AuthnService | generate_recovery_codes | mutation | OK | 57.13 | 62.57 | 54.68 | mutation (seeded success path) |
| AuthnService | get_jwks | read_only | OK | 4.58 | 7.03 | 4.92 | read_only (seeded success path) |
| AuthnService | get_mfa_policy | read_only | OK | 6.16 | 8.18 | 6.16 | read_only (seeded success path) |
| AuthnService | get_session | read_only | OK | 7.25 | 8.76 | 7.45 | read_only (seeded success path) |
| AuthnService | get_user | read_only | OK | 4.70 | 6.29 | 5.11 | read_only (seeded success path) |
| AuthnService | introspect_token | read_only | OK | 19.88 | 22.82 | 19.80 | read_only (seeded success path) |
| AuthnService | issue_mfa_challenge | mutation | OK | 17.14 | 19.02 | 18.37 | mutation (seeded success path) |
| AuthnService | list_devices | read_only | OK | 5.38 | 9.51 | 6.08 | read_only (seeded success path) |
| AuthnService | list_mfa_factors | read_only | OK | 7.17 | 10.32 | 7.44 | read_only (seeded success path) |
| AuthnService | list_sessions | read_only | OK | 8.25 | 9.86 | 8.18 | read_only (seeded success path) |
| AuthnService | list_users | read_only | OK | 7.59 | 10.38 | 7.99 | read_only (seeded success path) |
| AuthnService | list_web_authn_credentials | read_only | OK | 4.91 | 5.99 | 4.90 | read_only (seeded success path) |
| AuthnService | login | mutation | OK | 415.42 | 451.90 | 430.45 | mutation (seeded success path) |
| AuthnService | logout | mutation | OK | 6.64 | 6.88 | 7.12 | mutation (seeded success path) |
| AuthnService | put_mfa_policy | mutation | OK | 8.47 | 8.88 | 8.97 | mutation (seeded success path) |
| AuthnService | refresh_session | mutation | OK | 18.21 | 20.18 | 20.59 | mutation (seeded success path) |
| AuthnService | refresh_token | mutation | OK | 9.00 | 9.00 | 9.00 | mutation (seeded success path) |
| AuthnService | rename_passkey | mutation | OK | 12.50 | 12.55 | 11.98 | mutation (seeded success path) |
| AuthnService | resend_o_t_p | mutation | OK | 16.34 | 17.26 | 16.09 | mutation (seeded success path) |
| AuthnService | reset_password | mutation | OK | 494.40 | 494.40 | 494.40 | mutation (seeded success path) |
| AuthnService | revoke_device | mutation | OK | 28.02 | 28.02 | 28.02 | mutation (seeded success path) |
| AuthnService | revoke_recovery_codes | mutation | OK | 9.91 | 10.00 | 9.28 | mutation (seeded success path) |
| AuthnService | revoke_session | mutation | OK | 5.88 | 6.11 | 6.09 | mutation (seeded success path) |
| AuthnService | send_o_t_p | mutation | OK | 15.73 | 16.69 | 18.41 | mutation (seeded success path) |
| AuthnService | send_phone_verification | mutation | OK | 13.25 | 13.83 | 15.72 | mutation (seeded success path) |
| AuthnService | start_web_authn_authentication | mutation | OK | 15.31 | 15.67 | 17.82 | mutation (seeded success path) |
| AuthnService | start_web_authn_registration | mutation | OK | 14.63 | 16.13 | 15.01 | mutation (seeded success path) |
| AuthnService | update_user | mutation | OK | 12.85 | 13.87 | 13.15 | mutation (seeded success path) |
| AuthnService | validate_c_s_r_f | read_only | OK | 4.98 | 6.52 | 5.16 | read_only (seeded success path) |
| AuthnService | validate_token | read_only | OK | 15.01 | 17.63 | 15.30 | read_only (seeded success path) |
| AuthnService | verify_mfa_challenge | read_only | OK | 8.76 | 10.73 | 8.85 | read_only (seeded success path) |
| AuthnService | verify_o_t_p | read_only | OK | 15.27 | 19.35 | 16.55 | read_only (seeded success path) |
| AuthzService | activate_canary | destructive | OK | 38.19 | 38.19 | 38.19 | destructive: 1 real call against a seeded disposable target |
| AuthzService | activate_policy_version | destructive | OK | 49.28 | 49.28 | 49.28 | destructive: 1 real call against a seeded disposable target |
| AuthzService | approve_policy_draft | mutation | OK | 43.14 | 43.14 | 43.14 | mutation (seeded success path) |
| AuthzService | assign_role | mutation | OK | 25.02 | 36.34 | 29.69 | mutation (seeded success path) |
| AuthzService | authorize | read_only | OK | 21.96 | 26.29 | 22.32 | read_only (seeded success path) |
| AuthzService | batch_check_permissions | read_only | OK | 9.79 | 11.52 | 9.75 | read_only (seeded success path) |
| AuthzService | check_access | read_only | OK | 11.34 | 16.52 | 11.93 | read_only (seeded success path) |
| AuthzService | create_policy_draft | mutation | OK | 30.80 | 36.97 | 35.55 | mutation (seeded success path) |
| AuthzService | create_policy_rule | mutation | OK | 22.31 | 25.12 | 23.34 | mutation (seeded success path) |
| AuthzService | create_role | mutation | OK | 28.74 | 37.94 | 32.10 | mutation (seeded success path) |
| AuthzService | delete_policy_rule | mutation | OK | 11.57 | 11.83 | 11.54 | mutation (seeded success path) |
| AuthzService | delete_role | mutation | OK | 12.08 | 12.85 | 15.49 | mutation (seeded success path) |
| AuthzService | diff_policy_draft | read_only | OK | 7.67 | 8.99 | 7.57 | read_only (seeded success path) |
| AuthzService | explain_policy | read_only | OK | 3.51 | 4.31 | 3.48 | read_only (seeded success path) |
| AuthzService | get_authz_revision | read_only | OK | 5.03 | 5.93 | 5.20 | read_only (seeded success path) |
| AuthzService | get_canary_status | read_only | OK | 5.49 | 7.01 | 5.66 | read_only (seeded success path) |
| AuthzService | get_native_access | read_only | OK | 21.78 | 25.72 | 21.93 | read_only (seeded success path) |
| AuthzService | get_policy_bundle | read_only | OK | 10.38 | 13.59 | 10.80 | read_only (seeded success path) |
| AuthzService | get_policy_rule | read_only | OK | 6.37 | 7.79 | 6.53 | read_only (seeded success path) |
| AuthzService | get_role | read_only | OK | 6.92 | 8.67 | 7.05 | read_only (seeded success path) |
| AuthzService | invalidate_policy_bundles | destructive | OK | 32.62 | 32.62 | 32.62 | destructive: 1 real call against a seeded disposable target |
| AuthzService | lint_authz_policies | read_only | OK | 3.68 | 4.12 | 3.63 | read_only (seeded success path) |
| AuthzService | list_access_decision_audits | read_only | OK | 14.84 | 36.07 | 18.97 | read_only (seeded success path) |
| AuthzService | list_policy_rules | read_only | OK | 6.96 | 8.90 | 7.28 | read_only (seeded success path) |
| AuthzService | list_policy_versions | read_only | OK | 6.58 | 10.60 | 7.43 | read_only (seeded success path) |
| AuthzService | list_roles | read_only | OK | 6.69 | 10.66 | 7.15 | read_only (seeded success path) |
| AuthzService | list_user_permissions | read_only | OK | 3.07 | 3.97 | 3.15 | read_only (seeded success path) |
| AuthzService | list_user_roles | read_only | OK | 5.23 | 8.07 | 5.75 | read_only (seeded success path) |
| AuthzService | migrate_legacy_policies | destructive | OK | 29.13 | 29.13 | 29.13 | destructive: 1 real call against a seeded disposable target |
| AuthzService | promote_canary | destructive | OK | 76.69 | 76.69 | 76.69 | destructive: 1 real call against a seeded disposable target |
| AuthzService | put_authz_policy | mutation | OK | 19.77 | 23.63 | 22.46 | mutation (seeded success path) |
| AuthzService | put_relationship | mutation | OK | 27.50 | 28.55 | 29.05 | mutation (seeded success path) |
| AuthzService | put_role_binding | mutation | OK | 19.15 | 20.89 | 19.71 | mutation (seeded success path) |
| AuthzService | reject_policy_draft | mutation | OK | 48.01 | 48.01 | 48.01 | mutation (seeded success path) |
| AuthzService | revoke_role | mutation | OK | 9.74 | 15.14 | 13.27 | mutation (seeded success path) |
| AuthzService | rollback_policy_version | destructive | OK | 57.13 | 57.13 | 57.13 | destructive: 1 real call against a seeded disposable target |
| AuthzService | seed_builtin_roles | mutation | OK | 66.56 | 68.10 | 66.15 | mutation (seeded success path) |
| AuthzService | simulate_policy | mutation | OK | 11.48 | 12.02 | 17.72 | mutation (seeded success path) |
| AuthzService | submit_policy_draft | mutation | OK | 19.85 | 19.85 | 19.85 | mutation (seeded success path) |
| AuthzService | update_policy_draft | mutation | OK | 25.61 | 26.37 | 27.95 | mutation (seeded success path) |
| AuthzService | update_role | mutation | OK | 28.09 | 39.01 | 30.72 | mutation (seeded success path) |
| ControlPlaneService | ack_status | mutation | OK | 9.82 | 10.27 | 10.83 | mutation (seeded success path) |
| ControlPlaneService | delta_resources | stream_open | OK | 1.04 | 1.04 | 1.04 | streaming: stream-open latency |
| ControlPlaneService | get_resources | read_only | OK | 6.17 | 11.00 | 6.45 | read_only (seeded success path) |
| ControlPlaneService | list_node_states | read_only | OK | 28.53 | 32.02 | 28.29 | read_only (seeded success path) |
| ControlPlaneService | stream_resources | stream_open | OK | 0.28 | 0.28 | 0.28 | streaming: stream-open latency |
| DataBroker | activate_catalog | destructive | OK | 7.74 | 7.74 | 7.74 | destructive: 1 real call against a seeded disposable target |
| DataBroker | analytical_query | read_only | OK | 8.48 | 11.53 | 8.76 | read_only (seeded success path) |
| DataBroker | apply_migration | mutation | OK | 222.85 | 222.85 | 222.85 | mutation (seeded success path) |
| DataBroker | approve_migration_plan | mutation | OK | 63.73 | 63.73 | 63.73 | mutation (seeded success path) |
| DataBroker | batch_select | stream_open | OK | 0.23 | 0.23 | 0.23 | streaming: stream-open latency |
| DataBroker | batch_upsert | stream_open | OK | 0.20 | 0.20 | 0.20 | streaming: stream-open latency |
| DataBroker | begin_tx | stream_open | OK | 0.24 | 0.24 | 0.24 | streaming: stream-open latency |
| DataBroker | cache_delete | mutation | OK | 7.37 | 7.58 | 7.43 | mutation (seeded success path) |
| DataBroker | cache_get | read_only | OK | 6.61 | 8.73 | 6.84 | read_only (seeded success path) |
| DataBroker | cache_scan | read_only | OK | 9.01 | 14.14 | 9.58 | read_only (seeded success path) |
| DataBroker | cache_set | mutation | OK | 7.87 | 8.45 | 7.91 | mutation (seeded success path) |
| DataBroker | create_materialized_view | mutation | OK | 7.09 | 7.71 | 7.75 | mutation (seeded success path) |
| DataBroker | delete | mutation | OK | 38.81 | 39.25 | 39.17 | mutation (seeded success path) |
| DataBroker | delete_policy | mutation | OK | 14.12 | 14.12 | 14.12 | mutation (seeded success path) |
| DataBroker | dismiss_dlq_event | mutation | OK | 13.21 | 16.44 | 16.15 | mutation (seeded success path) |
| DataBroker | document_delete | mutation | OK | 7.76 | 8.82 | 8.34 | mutation (seeded success path) |
| DataBroker | document_find | read_only | OK | 7.69 | 12.66 | 8.53 | read_only (seeded success path) |
| DataBroker | document_get | read_only | OK | 6.56 | 8.61 | 6.91 | read_only (seeded success path) |
| DataBroker | document_upsert | mutation | OK | 7.11 | 7.65 | 7.26 | mutation (seeded success path) |
| DataBroker | drop_resource | destructive | OK | 21.76 | 21.76 | 21.76 | destructive: 1 real call against a seeded disposable target |
| DataBroker | enqueue_outbox_event | mutation | OK | 12.91 | 15.23 | 13.91 | mutation (seeded success path) |
| DataBroker | ensure_project | mutation | OK | 15.80 | 20.98 | 17.99 | mutation (seeded success path) |
| DataBroker | ensure_resource | mutation | OK | 22.80 | 24.92 | 23.38 | mutation (seeded success path) |
| DataBroker | generate_presigned_url | mutation | OK | 6.33 | 6.74 | 6.77 | mutation (seeded success path) |
| DataBroker | generic_dispatch | mutation | OK | 11.51 | 11.58 | 11.51 | mutation (seeded success path) |
| DataBroker | get_admin_summary | read_only | OK | 24.66 | 30.43 | 25.00 | read_only (seeded success path) |
| DataBroker | get_capabilities | read_only | OK | 8.16 | 11.19 | 8.46 | read_only (seeded success path) |
| DataBroker | get_catalog_manifest | read_only | OK | 16.15 | 24.17 | 17.29 | read_only (seeded success path) |
| DataBroker | get_catalog_version | read_only | OK | 7.69 | 9.41 | 7.60 | read_only (seeded success path) |
| DataBroker | get_catalog_versions | read_only | OK | 6.53 | 8.39 | 6.78 | read_only (seeded success path) |
| DataBroker | get_cdc_status | read_only | OK | 6.38 | 7.34 | 6.35 | read_only (seeded success path) |
| DataBroker | get_dlq_event | read_only | OK | 7.88 | 9.88 | 7.90 | read_only (seeded success path) |
| DataBroker | get_health_report | read_only | OK | 5.08 | 6.33 | 5.12 | read_only (seeded success path) |
| DataBroker | get_migration_status | read_only | OK | 7.90 | 9.37 | 8.27 | read_only (seeded success path) |
| DataBroker | get_object | stream | OK | 11.74 | 12.51 | 12.21 | streaming: time-to-first-response (seeded) |
| DataBroker | get_saga | read_only | OK | 7.62 | 13.03 | 8.31 | read_only (seeded success path) |
| DataBroker | graph_mutate | mutation | OK | 29.32 | 30.87 | 50.69 | mutation (seeded success path) |
| DataBroker | graph_query | read_only | OK | 21.72 | 27.49 | 22.64 | read_only (seeded success path) |
| DataBroker | initiate_multipart_upload | mutation | OK | 15.36 | 25.99 | 19.18 | mutation (seeded success path) |
| DataBroker | lint_policies | read_only | OK | 7.09 | 10.10 | 7.72 | read_only (seeded success path) |
| DataBroker | list_admin_audit_logs | read_only | OK | 8.69 | 12.09 | 9.28 | read_only (seeded success path) |
| DataBroker | list_dlq_events | read_only | OK | 8.74 | 12.93 | 9.18 | read_only (seeded success path) |
| DataBroker | list_message_schemas | read_only | OK | 4.68 | 5.24 | 4.75 | read_only (seeded success path) |
| DataBroker | list_migration_runs | read_only | OK | 8.83 | 11.69 | 8.97 | read_only (seeded success path) |
| DataBroker | list_policies | read_only | OK | 7.18 | 13.11 | 7.68 | read_only (seeded success path) |
| DataBroker | list_projects | read_only | OK | 7.09 | 8.85 | 7.26 | read_only (seeded success path) |
| DataBroker | list_resources | read_only | OK | 6.15 | 8.77 | 6.46 | read_only (seeded success path) |
| DataBroker | list_sagas | read_only | OK | 6.85 | 9.10 | 7.18 | read_only (seeded success path) |
| DataBroker | lookup_message_schema | read_only | OK | 4.06 | 4.81 | 4.16 | read_only (seeded success path) |
| DataBroker | mark_saga_reviewed | mutation | OK | 14.15 | 16.50 | 15.31 | mutation (seeded success path) |
| DataBroker | pause_cdc | mutation | OK | 16.20 | 16.29 | 17.58 | mutation (seeded success path) |
| DataBroker | plan_migration | mutation | OK | 15.74 | 17.79 | 18.63 | mutation (seeded success path) |
| DataBroker | preview_cdc_redaction | read_only | OK | 14.83 | 23.92 | 15.68 | read_only (seeded success path) |
| DataBroker | publish_c_d_c | stream | OK | 22.01 | 22.01 | 89.97 | cdc: time-to-first-event (real seeded Upsert produced) |
| DataBroker | put_object | stream_open | OK | 0.97 | 0.97 | 0.97 | streaming: stream-open latency |
| DataBroker | put_policy | destructive | OK | 15.55 | 15.55 | 15.55 | destructive: 1 real call against a seeded disposable target |
| DataBroker | quarantine_dlq_event | mutation | OK | 14.86 | 15.26 | 16.44 | mutation (seeded success path) |
| DataBroker | reload_policies | destructive | OK | 28.62 | 28.62 | 28.62 | destructive: 1 real call against a seeded disposable target |
| DataBroker | replay_dlq_event | mutation | OK | 21.98 | 21.98 | 21.98 | mutation (seeded success path) |
| DataBroker | resume_cdc | mutation | OK | 17.15 | 19.27 | 20.33 | mutation (seeded success path) |
| DataBroker | retry_saga_compensation | mutation | OK | 16.99 | 16.99 | 16.99 | mutation (seeded success path) |
| DataBroker | rollback_catalog | destructive | OK | 7.35 | 7.35 | 7.35 | destructive: 1 real call against a seeded disposable target |
| DataBroker | scan_projection_drift | read_only | OK | 15.41 | 29.35 | 16.68 | read_only (seeded success path) |
| DataBroker | select | read_only | OK | 7.63 | 10.05 | 8.04 | read_only (seeded success path) |
| DataBroker | select_v2 | stream | OK | 7.47 | 7.76 | 7.37 | streaming: time-to-first-response (seeded) |
| DataBroker | stage_catalog | destructive | OK | 328.40 | 328.40 | 328.40 | destructive: 1 real call against a seeded disposable target |
| DataBroker | step_down_cdc_leader | mutation | OK | 15.56 | 17.38 | 18.30 | mutation (seeded success path) |
| DataBroker | time_series_query | read_only | OK | 9.54 | 10.64 | 9.49 | read_only (seeded success path) |
| DataBroker | time_series_write | mutation | OK | 6.19 | 6.39 | 6.07 | mutation (seeded success path) |
| DataBroker | upsert | mutation | OK | 61.96 | 62.71 | 61.91 | mutation (seeded success path) |
| DataBroker | validate_catalog | destructive | OK | 68.21 | 68.21 | 68.21 | destructive: 1 real call against a seeded disposable target |
| DataBroker | vector_batch_upsert | stream_open | OK | 0.13 | 0.13 | 0.13 | streaming: stream-open latency |
| DataBroker | vector_hybrid_search | read_only | OK | 7.04 | 8.12 | 7.05 | read_only (seeded success path) |
| DataBroker | vector_search | read_only | OK | 7.20 | 8.20 | 7.23 | read_only (seeded success path) |
| DataBroker | vector_upsert | mutation | OK | 11.96 | 12.35 | 17.96 | mutation (seeded success path) |
| DataBroker | verify_admin_audit_log | read_only | OK | 10.00 | 13.09 | 9.97 | read_only (seeded success path) |
| IdentityProviderService | create_provider | mutation | OK | 18.48 | 20.08 | 18.89 | mutation (seeded success path) |
| IdentityProviderService | disable_provider | mutation | OK | 28.66 | 31.23 | 25.97 | mutation (seeded success path) |
| IdentityProviderService | force_jwks_refresh | mutation | OK | 21.24 | 27.77 | 23.30 | mutation (seeded success path) |
| IdentityProviderService | get_provider | read_only | OK | 5.73 | 8.58 | 6.20 | read_only (seeded success path) |
| IdentityProviderService | import_saml_metadata | mutation | OK | 20.85 | 26.82 | 22.64 | mutation (seeded success path) |
| IdentityProviderService | link_identity | mutation | OK | 19.51 | 29.55 | 22.72 | mutation (seeded success path) |
| IdentityProviderService | list_external_identities | read_only | OK | 8.34 | 13.01 | 8.83 | read_only (seeded success path) |
| IdentityProviderService | list_providers | read_only | OK | 8.43 | 11.15 | 8.59 | read_only (seeded success path) |
| IdentityProviderService | preview_claim_mapping | read_only | OK | 5.55 | 6.44 | 5.51 | read_only (seeded success path) |
| IdentityProviderService | preview_group_mapping | read_only | OK | 5.01 | 6.07 | 5.11 | read_only (seeded success path) |
| IdentityProviderService | resolve_external_identity | mutation | OK | 31.67 | 39.66 | 34.55 | mutation (seeded success path) |
| IdentityProviderService | saml_acs | mutation | OK | 113.01 | 114.79 | 108.35 | mutation (seeded success path) |
| IdentityProviderService | scim_create_group | mutation | OK | 10.12 | 10.25 | 11.18 | mutation (seeded success path) |
| IdentityProviderService | scim_create_user | mutation | OK | 43.31 | 47.83 | 42.93 | mutation (seeded success path) |
| IdentityProviderService | scim_delete_group | mutation | OK | 8.47 | 8.81 | 8.43 | mutation (seeded success path) |
| IdentityProviderService | scim_delete_user | mutation | OK | 55.89 | 55.89 | 55.89 | mutation (seeded success path) |
| IdentityProviderService | scim_get_group | mutation | OK | 10.31 | 10.37 | 10.38 | mutation (seeded success path) |
| IdentityProviderService | scim_get_user | mutation | OK | 11.71 | 12.10 | 11.74 | mutation (seeded success path) |
| IdentityProviderService | scim_list_groups | mutation | OK | 7.70 | 9.21 | 7.96 | mutation (seeded success path) |
| IdentityProviderService | scim_list_users | mutation | OK | 14.27 | 14.30 | 14.82 | mutation (seeded success path) |
| IdentityProviderService | scim_patch_group | mutation | OK | 11.77 | 15.68 | 15.40 | mutation (seeded success path) |
| IdentityProviderService | scim_patch_user | mutation | OK | 28.86 | 33.67 | 32.25 | mutation (seeded success path) |
| IdentityProviderService | scim_replace_user | mutation | OK | 22.82 | 23.78 | 27.58 | mutation (seeded success path) |
| IdentityProviderService | start_saml_login | mutation | OK | 6.60 | 7.17 | 7.08 | mutation (seeded success path) |
| IdentityProviderService | test_provider_discovery | read_only | OK | 5.67 | 6.77 | 5.60 | read_only (seeded success path) |
| IdentityProviderService | unlink_identity | mutation | OK | 7.02 | 7.28 | 11.13 | mutation (seeded success path) |
| IdentityProviderService | update_provider | mutation | OK | 18.34 | 18.63 | 18.57 | mutation (seeded success path) |
| NotificationService | get_delivery_stats | read_only | OK | 6.77 | 14.97 | 8.73 | read_only (seeded success path) |
| NotificationService | get_notification | read_only | OK | 8.59 | 15.48 | 9.17 | read_only (seeded success path) |
| NotificationService | get_preference | read_only | OK | 8.21 | 10.60 | 8.42 | read_only (seeded success path) |
| NotificationService | get_template | read_only | OK | 8.51 | 10.84 | 8.63 | read_only (seeded success path) |
| NotificationService | list_notifications | read_only | OK | 14.45 | 16.66 | 14.63 | read_only (seeded success path) |
| NotificationService | list_preferences | read_only | OK | 14.67 | 29.50 | 16.77 | read_only (seeded success path) |
| NotificationService | list_templates | read_only | OK | 14.82 | 16.13 | 14.38 | read_only (seeded success path) |
| NotificationService | retry_notification | mutation | OK | 27.06 | 27.06 | 27.06 | mutation (seeded success path) |
| NotificationService | send_notification | mutation | OK | 29.17 | 31.81 | 29.56 | mutation (seeded success path) |
| NotificationService | set_preference | mutation | OK | 8.44 | 9.28 | 15.07 | mutation (seeded success path) |
| NotificationService | upsert_template | mutation | OK | 7.72 | 10.20 | 10.31 | mutation (seeded success path) |
| PeerService | get_peer | read_only | OK | 9.44 | 10.79 | 9.48 | read_only (seeded success path) |
| PeerService | join_room | mutation | OK | 22.12 | 34.36 | 26.43 | mutation (seeded success path) |
| PeerService | leave_room | mutation | OK | 6.47 | 6.72 | 8.38 | mutation (seeded success path) |
| PeerService | list_peers | read_only | OK | 8.77 | 10.72 | 8.72 | read_only (seeded success path) |
| RoomService | close_room | mutation | OK | 23.80 | 24.37 | 25.74 | mutation (seeded success path) |
| RoomService | create_room | mutation | OK | 15.44 | 26.76 | 19.02 | mutation (seeded success path) |
| RoomService | get_room | read_only | OK | 8.53 | 11.02 | 8.70 | read_only (seeded success path) |
| RoomService | list_rooms | read_only | OK | 7.52 | 10.82 | 7.90 | read_only (seeded success path) |
| RoomService | update_room | mutation | OK | 7.23 | 10.36 | 9.91 | mutation (seeded success path) |
| SignalingService | signal | stream_open | OK | 6.63 | 6.63 | 6.63 | streaming: stream-open latency |
| StorageService | delete_file | mutation | OK | 48.27 | 48.27 | 48.27 | mutation (seeded success path) |
| StorageService | finalize_upload | mutation | OK | 29.16 | 32.21 | 30.35 | mutation (seeded success path) |
| StorageService | get_download_url | read_only | OK | 10.93 | 13.69 | 11.15 | read_only (seeded success path) |
| StorageService | get_file | read_only | OK | 11.26 | 15.50 | 11.35 | read_only (seeded success path) |
| StorageService | list_files | read_only | OK | 19.86 | 28.75 | 20.30 | read_only (seeded success path) |
| StorageService | register_upload | mutation | OK | 16.59 | 16.97 | 18.59 | mutation (seeded success path) |
| StorageService | update_file | mutation | OK | 20.70 | 20.93 | 22.57 | mutation (seeded success path) |
| TenantService | create_tenant | mutation | OK | 11.76 | 15.57 | 13.85 | mutation (seeded success path) |
| TenantService | get_tenant | read_only | OK | 8.24 | 15.61 | 9.19 | read_only (seeded success path) |
| TenantService | get_tenant_config | read_only | OK | 7.95 | 9.70 | 8.01 | read_only (seeded success path) |
| TenantService | list_tenants | read_only | OK | 7.34 | 9.97 | 7.63 | read_only (seeded success path) |
| TenantService | update_tenant | mutation | OK | 11.19 | 11.22 | 11.88 | mutation (seeded success path) |
| TenantService | update_tenant_config | mutation | OK | 21.42 | 21.92 | 34.83 | mutation (seeded success path) |
| TrackService | list_tracks | read_only | OK | 9.28 | 12.14 | 9.51 | read_only (seeded success path) |
| TrackService | mute_track | mutation | OK | 7.04 | 7.84 | 7.37 | mutation (seeded success path) |
| TrackService | publish_track | mutation | OK | 13.44 | 15.91 | 16.45 | mutation (seeded success path) |
| TrackService | unpublish_track | mutation | OK | 9.06 | 9.16 | 9.65 | mutation (seeded success path) |
| TurnService | issue_credentials | mutation | OK | 5.87 | 7.08 | 7.85 | mutation (seeded success path) |
