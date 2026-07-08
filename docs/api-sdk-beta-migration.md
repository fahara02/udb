# API/SDK Beta Migration Fixture

Generated clients and REST/OpenAPI artifacts for UDB `0.3.7` use the current
beta API/SDK names below. This file is the explicit migration fixture for the
Chapter 14 route and SDK alias cleanup; old names are allowed here and in release
notes only.

Public docs, SDK docs/examples, Pages content, and benchmark dashboards should use the new route, SDK alias, and benchmark identity. Raw wire RPC names remain
diagnostic dispatch metadata.

| Domain | Old beta HTTP route or label | Current HTTP route | Old SDK/public method shape | Current SDK alias / operationId | Benchmark label | Test or guard owner |
|---|---|---|---|---|---|---|
| API keys collection | `/v1/api_keys...` | `/v1/api-keys...` | raw `ApiKeyService/*` fallback | `create_api_key` / `createApiKey`, `list_api_keys` / `listApiKeys`, `rotate_api_key` / `rotateApiKey`, `validate_api_key` / `validateApiKey` | `operation_id || api_alias || wire_api` | `scripts/check-http-api-style.mjs --source-only`; `scripts/check-openapi-operationid-posture.py`; `scripts/check-api-sdk-alias-posture.py` |
| Analytics resources | `/v1/analytics/pipeline_metrics`, `/pipeline_summaries`, `/executor_performance`, `/reconciliation_stats`, `/sla_compliance` | `/v1/analytics/pipeline-metrics`, `/pipeline-summaries`, `/executor-performance`, `/reconciliation-stats`, `/sla-compliance` | raw `AnalyticsService/*` fallback | `record_pipeline_metric` / `recordPipelineMetric`, `get_pipeline_summary` / `getPipelineSummary`, `get_executor_performance` / `getExecutorPerformance` | `operation_id || api_alias || wire_api` | `scripts/check-http-api-style.mjs --source-only`; `scripts/gen-sdk-benchmark-docs.mjs --check` |
| Asset namespace | `/v1/asset/assets`, `/v1/asset/pipelines...` | `/v1/assets`, `/v1/assets/pipeline-definitions`, `/v1/assets/pipelines`, `/v1/assets/steps/{step_id}:complete` | raw `AssetService/*` fallback | `register_asset` / `registerAsset`, `start_pipeline` / `startPipeline`, `complete_step` / `completeStep` | `operation_id || api_alias || wire_api` | `scripts/check-http-api-style.mjs --source-only`; `sdk-conformance/run.mjs metadata` |
| Storage upload finalize | `/v1/storage/uploads/{file_id}/finalize` | `/v1/storage/uploads/{file_id}:finalize` | raw `StorageService/FinalizeUpload` fallback | `finalize_upload` / `finalizeUpload` | `finalizeUpload` | `scripts/rest_route_gateway_smoke.py --check-openapi`; SDK live upload sequence gates |
| Storage download URL | `/v1/storage/files/{file_id}/download-url` | `/v1/storage/files/{file_id}:getDownloadUrl` | raw `GetDownloadUrl` fallback / generated acronym-free method varies by language | `get_download_url` / `getDownloadUrl` | `getDownloadUrl` | `scripts/rest_route_gateway_smoke.py --check-openapi`; SDK download helper tests |
| Storage download bytes | `/v1/storage/files/{file_id}/download` | `/v1/storage/files/{file_id}:download` | raw `DownloadFile` fallback | `download_file` / `downloadFile` | `downloadFile` | `scripts/rest_route_gateway_smoke.py --check-openapi`; SDK download stream tests |
| WebRTC room close | `/v1/webrtc/rooms/{room_id}/close` | `/v1/webrtc/rooms/{room_id}:close` | raw `RoomService/CloseRoom` fallback | `close_room` / `closeRoom` | `closeRoom` | `scripts/rest_route_gateway_smoke.py --check-openapi`; WebRTC live conformance |
| WebRTC peer leave | `/v1/webrtc/rooms/{room_id}/peers/{peer_id}/leave` | `/v1/webrtc/rooms/{room_id}/peers/{peer_id}:leave` | raw `PeerService/LeaveRoom` fallback | `leave_room` / `leaveRoom` | `leaveRoom` | `scripts/rest_route_gateway_smoke.py --check-openapi`; WebRTC helper sequence tests |
| WebRTC track mute | `/v1/webrtc/tracks/{track_id}/mute` | `/v1/webrtc/tracks/{track_id}:mute` | raw `TrackService/MuteTrack` fallback | `mute_track` / `muteTrack` | `muteTrack` | `scripts/rest_route_gateway_smoke.py --check-openapi`; WebRTC live conformance |
| WebRTC track unpublish | `/v1/webrtc/tracks/{track_id}/unpublish` | `/v1/webrtc/tracks/{track_id}:unpublish` | raw `TrackService/UnpublishTrack` fallback | `unpublish_track` / `unpublishTrack` | `unpublishTrack` | `scripts/rest_route_gateway_smoke.py --check-openapi`; WebRTC live conformance |
| Auth OTP actions | `/v1/auth/otp:send`, `/verify`, `/resend` | `/v1/auth/otps:send`, `/v1/auth/otps:verify`, `/v1/auth/otps:resend` | acronym-split fallbacks such as `send_o_t_p` / raw `SendOTP` | `send_otp` / `sendOtp`, `verify_otp` / `verifyOtp`, `resend_otp` / `resendOtp` | `sendOtp`, `verifyOtp`, `resendOtp` | `sdk-conformance/run.mjs metadata` rejects acronym-split public methods |
| Auth token actions | `/v1/auth/token:refresh`, `/validate`, `/introspect` | `/v1/auth/tokens:refresh`, `/v1/auth/tokens:validate`, `/v1/auth/tokens:introspect` | raw `RefreshToken` / `ValidateToken` / `IntrospectToken` fallback | `refresh_token` / `refreshToken`, `validate_token` / `validateToken`, `introspect_token` / `introspectToken` | `refreshToken`, `validateToken`, `introspectToken` | `scripts/check-openapi-operationid-posture.py`; `sdk-conformance/run.mjs metadata` |
| Auth password actions | `/v1/auth/password:change`, `/forgot`, `/reset` | `/v1/auth/passwords:change`, `/v1/auth/passwords:forgot`, `/v1/auth/passwords:reset` | raw `ChangePassword` / `ForgotPassword` / `ResetPassword` fallback | `change_password` / `changePassword`, `forgot_password` / `forgotPassword`, `reset_password` / `resetPassword` | `changePassword`, `forgotPassword`, `resetPassword` | `scripts/rest_route_gateway_smoke.py --check-openapi`; live auth conformance |
| Auth CSRF validation | `/v1/auth/csrf:validate` | `/v1/auth/csrf-tokens:validate` | raw `ValidateCSRF` fallback | `validate_csrf` / `validateCsrf` | `validateCsrf` | `sdk-conformance/run.mjs metadata`; route-style guard |
| Authz governance version list | `/v1/authz/governance/versions:list` | `GET /v1/authz/governance/versions` | raw `ListPolicyVersions` fallback | `list_policy_versions` / `listPolicyVersions` | `listPolicyVersions` | `scripts/check-http-api-style.mjs --source-only`; API-rule posture |
| Authz governance current revision | `/v1/authz/governance/revision` | `/v1/authz/governance/revisions/current` | raw `GetAuthzRevision` fallback | `get_authz_revision` / `getAuthzRevision` | `getAuthzRevision` | `scripts/rest_route_gateway_smoke.py --check-openapi`; Authz live conformance |
| Authz simulate/explain | `/v1/authz/governance/simulate`, `/explain` | `/v1/authz/governance/policy-simulations`, `/policy-explanations` | raw `SimulatePolicy` / `ExplainPolicy` fallback | `simulate_policy` / `simulatePolicy`, `explain_policy` / `explainPolicy` | `simulatePolicy`, `explainPolicy` | `scripts/rest_route_gateway_smoke.py --check-openapi`; SDK facade sequence gates |
| IdP provider refresh/test/preview actions | `/v1/idp/providers/{provider_id}/refresh_jwks`, `/test_discovery`, `/preview_claim_mapping` | `/v1/idp/providers/{provider_id}:refreshJwks`, `:testDiscovery`, `:previewClaimMapping` | raw `ForceJwksRefresh`, `TestProviderDiscovery`, `PreviewClaimMapping` fallback | `force_jwks_refresh` / `forceJwksRefresh`, `test_provider_discovery` / `testProviderDiscovery`, `preview_claim_mapping` / `previewClaimMapping` | `forceJwksRefresh`, `testProviderDiscovery`, `previewClaimMapping` | `scripts/rest_route_gateway_smoke.py --check-openapi`; `scripts/check-openapi-operationid-posture.py` |
| SCIM protocol exception | SCIM path spelling stays protocol-owned | `/v1/idp/scim/v2/Users`, `/Groups` | unchanged protocol method aliases | `scim_*` aliases | `scim_*` aliases | `scripts/check-http-api-style.mjs --source-only` documents SCIM/JWKS protocol exception |

## Search Contract

Old beta route literals and acronym-split public SDK method names are valid only
in this fixture, release notes, archived plans, generated/protobuf internals, and
tests that explicitly prove migration behavior. Use:

```bash
python scripts/check-beta-versioning-posture.py --selftest
python scripts/check-beta-versioning-posture.py
```

The guard fails if public docs, SDK docs/examples, Pages content, Swagger
artifacts, or benchmark-facing docs reintroduce the retired route or SDK method
shapes outside this fixture.
