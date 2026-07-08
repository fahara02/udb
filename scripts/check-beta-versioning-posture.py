#!/usr/bin/env python3
"""Source guard for pre-1.0 beta API/SDK compatibility posture."""

from __future__ import annotations

import argparse
import ast
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class RequiredDoc:
    label: str
    path: str
    tokens: tuple[str, ...]


@dataclass(frozen=True)
class MigrationRow:
    domain: str
    old_tokens: tuple[str, ...]
    current_tokens: tuple[str, ...]
    old_sdk_tokens: tuple[str, ...]
    alias_tokens: tuple[str, ...]
    benchmark_tokens: tuple[str, ...]


REQUIRED_DOCS: tuple[RequiredDoc, ...] = (
    RequiredDoc(
        "versioning beta compatibility",
        "VERSIONING.md",
        (
            "## Pre-1.0 Beta Compatibility",
            "UDB `0.x` releases are beta",
            "Before `1.0.0`, HTTP routes, OpenAPI",
            "SDK public method names",
            "wire protocol version remains factual metadata",
            "not a promise that the",
            "product API and SDK surface are stable during `0.x`",
            "Breaking `0.x` changes must be documented with migration notes",
            "not a backward-compatibility guarantee",
            "[docs/api-rules.md](docs/api-rules.md)",
            "[docs/api-sdk-beta-migration.md](docs/api-sdk-beta-migration.md)",
            "### Beta Breaking-Change Note Template",
            "- Product version: `0.x.y`",
            "- Old HTTP route or SDK method:",
            "- New HTTP route or SDK method:",
            "- Reason:",
            "- Affected SDK languages:",
            "- Migration snippet:",
            "- Removal/deprecation posture:",
            "- Related API rule:",
        ),
    ),
    RequiredDoc(
        "API rules beta compatibility",
        "docs/api-rules.md",
        (
            "is beta and pre-1.0",
            "Until `1.0.0`, UDB may make breaking API and SDK changes",
            "Do not claim stable backward",
            "Breaking `0.x` changes must have migration notes",
            "Do not use the protocol version to imply product API stability",
        ),
    ),
    RequiredDoc(
        "API/SDK beta migration fixture",
        "docs/api-sdk-beta-migration.md",
        (
            "# API/SDK Beta Migration Fixture",
            "Old beta route literals and acronym-split public SDK method names are valid only",
            "| API keys collection | `/v1/api_keys...` | `/v1/api-keys...`",
            "| Storage download URL | `/v1/storage/files/{file_id}/download-url` | `/v1/storage/files/{file_id}:getDownloadUrl`",
            "| Auth OTP actions | `/v1/auth/otp:send`, `/verify`, `/resend` | `/v1/auth/otps:send`",
            "| IdP provider refresh/test/preview actions | `/v1/idp/providers/{provider_id}/refresh_jwks`",
            "Public docs, SDK docs/examples, Pages content, and benchmark dashboards should use",
            "Old beta route literals and acronym-split public SDK method names are valid only",
            "python scripts/check-beta-versioning-posture.py --selftest",
            "python scripts/check-beta-versioning-posture.py",
        ),
    ),
)

EXPECTED_MIGRATION_ROWS: tuple[MigrationRow, ...] = (
    MigrationRow(
        "API keys collection",
        ("/v1/api_keys",),
        ("/v1/api-keys",),
        ("ApiKeyService/*",),
        ("create_api_key", "createApiKey", "list_api_keys", "listApiKeys", "rotate_api_key", "rotateApiKey", "validate_api_key", "validateApiKey"),
        ("operation_id || api_alias || wire_api",),
    ),
    MigrationRow(
        "Analytics resources",
        ("/v1/analytics/pipeline_metrics", "/pipeline_summaries", "/executor_performance", "/reconciliation_stats", "/sla_compliance"),
        ("/v1/analytics/pipeline-metrics", "/pipeline-summaries", "/executor-performance", "/reconciliation-stats", "/sla-compliance"),
        ("AnalyticsService/*",),
        ("record_pipeline_metric", "recordPipelineMetric", "get_pipeline_summary", "getPipelineSummary", "get_executor_performance", "getExecutorPerformance"),
        ("operation_id || api_alias || wire_api",),
    ),
    MigrationRow(
        "Asset namespace",
        ("/v1/asset/assets", "/v1/asset/pipelines"),
        ("/v1/assets", "/v1/assets/pipeline-definitions", "/v1/assets/pipelines", "/v1/assets/steps/{step_id}:complete"),
        ("AssetService/*",),
        ("register_asset", "registerAsset", "start_pipeline", "startPipeline", "complete_step", "completeStep"),
        ("operation_id || api_alias || wire_api",),
    ),
    MigrationRow(
        "Storage upload finalize",
        ("/v1/storage/uploads/{file_id}/finalize",),
        ("/v1/storage/uploads/{file_id}:finalize",),
        ("StorageService/FinalizeUpload",),
        ("finalize_upload", "finalizeUpload"),
        ("finalizeUpload",),
    ),
    MigrationRow(
        "Storage download URL",
        ("/v1/storage/files/{file_id}/download-url",),
        ("/v1/storage/files/{file_id}:getDownloadUrl",),
        ("GetDownloadUrl",),
        ("get_download_url", "getDownloadUrl"),
        ("getDownloadUrl",),
    ),
    MigrationRow(
        "Storage download bytes",
        ("/v1/storage/files/{file_id}/download",),
        ("/v1/storage/files/{file_id}:download",),
        ("DownloadFile",),
        ("download_file", "downloadFile"),
        ("downloadFile",),
    ),
    MigrationRow(
        "WebRTC room close",
        ("/v1/webrtc/rooms/{room_id}/close",),
        ("/v1/webrtc/rooms/{room_id}:close",),
        ("RoomService/CloseRoom",),
        ("close_room", "closeRoom"),
        ("closeRoom",),
    ),
    MigrationRow(
        "WebRTC peer leave",
        ("/v1/webrtc/rooms/{room_id}/peers/{peer_id}/leave",),
        ("/v1/webrtc/rooms/{room_id}/peers/{peer_id}:leave",),
        ("PeerService/LeaveRoom",),
        ("leave_room", "leaveRoom"),
        ("leaveRoom",),
    ),
    MigrationRow(
        "WebRTC track mute",
        ("/v1/webrtc/tracks/{track_id}/mute",),
        ("/v1/webrtc/tracks/{track_id}:mute",),
        ("TrackService/MuteTrack",),
        ("mute_track", "muteTrack"),
        ("muteTrack",),
    ),
    MigrationRow(
        "WebRTC track unpublish",
        ("/v1/webrtc/tracks/{track_id}/unpublish",),
        ("/v1/webrtc/tracks/{track_id}:unpublish",),
        ("TrackService/UnpublishTrack",),
        ("unpublish_track", "unpublishTrack"),
        ("unpublishTrack",),
    ),
    MigrationRow(
        "Auth OTP actions",
        ("/v1/auth/otp:send", "/verify", "/resend"),
        ("/v1/auth/otps:send", "/v1/auth/otps:verify", "/v1/auth/otps:resend"),
        ("send_o_t_p", "SendOTP"),
        ("send_otp", "sendOtp", "verify_otp", "verifyOtp", "resend_otp", "resendOtp"),
        ("sendOtp", "verifyOtp", "resendOtp"),
    ),
    MigrationRow(
        "Auth token actions",
        ("/v1/auth/token:refresh", "/validate", "/introspect"),
        ("/v1/auth/tokens:refresh", "/v1/auth/tokens:validate", "/v1/auth/tokens:introspect"),
        ("RefreshToken", "ValidateToken", "IntrospectToken"),
        ("refresh_token", "refreshToken", "validate_token", "validateToken", "introspect_token", "introspectToken"),
        ("refreshToken", "validateToken", "introspectToken"),
    ),
    MigrationRow(
        "Auth password actions",
        ("/v1/auth/password:change", "/forgot", "/reset"),
        ("/v1/auth/passwords:change", "/v1/auth/passwords:forgot", "/v1/auth/passwords:reset"),
        ("ChangePassword", "ForgotPassword", "ResetPassword"),
        ("change_password", "changePassword", "forgot_password", "forgotPassword", "reset_password", "resetPassword"),
        ("changePassword", "forgotPassword", "resetPassword"),
    ),
    MigrationRow(
        "Auth CSRF validation",
        ("/v1/auth/csrf:validate",),
        ("/v1/auth/csrf-tokens:validate",),
        ("ValidateCSRF",),
        ("validate_csrf", "validateCsrf"),
        ("validateCsrf",),
    ),
    MigrationRow(
        "Authz governance version list",
        ("/v1/authz/governance/versions:list",),
        ("GET /v1/authz/governance/versions",),
        ("ListPolicyVersions",),
        ("list_policy_versions", "listPolicyVersions"),
        ("listPolicyVersions",),
    ),
    MigrationRow(
        "Authz governance current revision",
        ("/v1/authz/governance/revision",),
        ("/v1/authz/governance/revisions/current",),
        ("GetAuthzRevision",),
        ("get_authz_revision", "getAuthzRevision"),
        ("getAuthzRevision",),
    ),
    MigrationRow(
        "Authz simulate/explain",
        ("/v1/authz/governance/simulate", "/explain"),
        ("/v1/authz/governance/policy-simulations", "/policy-explanations"),
        ("SimulatePolicy", "ExplainPolicy"),
        ("simulate_policy", "simulatePolicy", "explain_policy", "explainPolicy"),
        ("simulatePolicy", "explainPolicy"),
    ),
    MigrationRow(
        "IdP provider refresh/test/preview actions",
        ("/v1/idp/providers/{provider_id}/refresh_jwks", "/test_discovery", "/preview_claim_mapping"),
        ("/v1/idp/providers/{provider_id}:refreshJwks", ":testDiscovery", ":previewClaimMapping"),
        ("ForceJwksRefresh", "TestProviderDiscovery", "PreviewClaimMapping"),
        ("force_jwks_refresh", "forceJwksRefresh", "test_provider_discovery", "testProviderDiscovery", "preview_claim_mapping", "previewClaimMapping"),
        ("forceJwksRefresh", "testProviderDiscovery", "previewClaimMapping"),
    ),
    MigrationRow(
        "SCIM protocol exception",
        ("SCIM path spelling stays protocol-owned",),
        ("/v1/idp/scim/v2/Users", "/Groups"),
        ("unchanged protocol method aliases",),
        ("scim_*",),
        ("scim_*",),
    ),
)

EXPECTED_MIGRATION_HEADER: tuple[str, ...] = (
    "Domain",
    "Old beta HTTP route or label",
    "Current HTTP route",
    "Old SDK/public method shape",
    "Current SDK alias / operationId",
    "Benchmark label",
    "Test or guard owner",
)

MIGRATION_TOKEN_COLUMNS: tuple[tuple[str, int, str, str], ...] = (
    ("old", 1, "old", "in Old beta HTTP route or label"),
    ("current", 2, "current", "in Current HTTP route"),
    ("old_sdk", 3, "old SDK/public method", "in Old SDK/public method shape"),
    ("alias", 4, "alias", "in Current SDK alias / operationId"),
)

EXPECTED_SERVED_ROUTE_PROOF_TOKENS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("API keys collection", ("/v1/api-keys", "/v1/api_keys")),
    (
        "Analytics resources",
        (
            "/v1/analytics/pipeline-metrics",
            "/v1/analytics/pipeline-summaries",
            "/v1/analytics/executor-performance",
            "/v1/analytics/reconciliation-stats",
            "/v1/analytics/sla-compliance",
            "/v1/analytics/pipeline_metrics",
            "/v1/analytics/pipeline_summaries",
            "/v1/analytics/executor_performance",
            "/v1/analytics/reconciliation_stats",
            "/v1/analytics/sla_compliance",
        ),
    ),
    (
        "Asset namespace",
        (
            "/v1/assets",
            "/v1/assets/pipeline-definitions",
            "/v1/assets/pipelines",
            "/v1/assets/steps/{stepId}:complete",
            "/v1/asset/assets",
            "/v1/asset/pipelines",
        ),
    ),
    (
        "Storage upload finalize",
        ("/v1/storage/uploads/{fileId}:finalize", "/v1/storage/uploads/{fileId}/finalize"),
    ),
    (
        "Storage download URL",
        ("/v1/storage/files/{fileId}:getDownloadUrl", "/v1/storage/files/{fileId}/download-url"),
    ),
    (
        "Storage download bytes",
        ("/v1/storage/files/{fileId}:download", "/v1/storage/files/{fileId}/download"),
    ),
    ("WebRTC room close", ("/v1/webrtc/rooms/{roomId}:close", "/v1/webrtc/rooms/{roomId}/close")),
    (
        "WebRTC peer leave",
        ("/v1/webrtc/rooms/{roomId}/peers/{peerId}:leave", "/v1/webrtc/rooms/{roomId}/peers/{peerId}/leave"),
    ),
    ("WebRTC track mute", ("/v1/webrtc/tracks/{trackId}:mute", "/v1/webrtc/tracks/{trackId}/mute")),
    (
        "WebRTC track unpublish",
        ("/v1/webrtc/tracks/{trackId}:unpublish", "/v1/webrtc/tracks/{trackId}/unpublish"),
    ),
    (
        "Auth OTP actions",
        (
            "/v1/auth/otps:send",
            "/v1/auth/otps:verify",
            "/v1/auth/otps:resend",
            "/v1/auth/otp:send",
            "/v1/auth/otp:verify",
            "/v1/auth/otp:resend",
        ),
    ),
    (
        "Auth token actions",
        (
            "/v1/auth/tokens:refresh",
            "/v1/auth/tokens:validate",
            "/v1/auth/tokens:introspect",
            "/v1/auth/token:refresh",
            "/v1/auth/token:validate",
            "/v1/auth/token:introspect",
        ),
    ),
    (
        "Auth password actions",
        (
            "/v1/auth/passwords:change",
            "/v1/auth/passwords:forgot",
            "/v1/auth/passwords:reset",
            "/v1/auth/password:change",
            "/v1/auth/password:forgot",
            "/v1/auth/password:reset",
        ),
    ),
    ("Auth CSRF validation", ("/v1/auth/csrf-tokens:validate", "/v1/auth/csrf:validate")),
    (
        "Authz governance version list",
        ("/v1/authz/governance/versions", "/v1/authz/governance/versions:list"),
    ),
    (
        "Authz governance current revision",
        ("/v1/authz/governance/revisions/current", "/v1/authz/governance/revision"),
    ),
    (
        "Authz simulate/explain",
        (
            "/v1/authz/governance/policy-simulations",
            "/v1/authz/governance/policy-explanations",
            "/v1/authz/governance/simulate",
            "/v1/authz/governance/explain",
        ),
    ),
    (
        "IdP provider refresh/test/preview actions",
        (
            "/v1/idp/providers/{providerId}:refreshJwks",
            "/v1/idp/providers/{providerId}:testDiscovery",
            "/v1/idp/providers/{providerId}:previewClaimMapping",
            "/v1/idp/providers/{providerId}/refresh_jwks",
            "/v1/idp/providers/{providerId}/test_discovery",
            "/v1/idp/providers/{providerId}/preview_claim_mapping",
        ),
    ),
)

EXPECTED_OWNER_TOKENS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("API keys collection", ("scripts/check-http-api-style.mjs", "scripts/check-api-sdk-alias-posture.py")),
    ("Analytics resources", ("scripts/check-http-api-style.mjs", "scripts/gen-sdk-benchmark-docs.mjs")),
    ("Asset namespace", ("scripts/check-http-api-style.mjs", "sdk-conformance/run.mjs")),
    ("Storage upload finalize", ("scripts/rest_route_gateway_smoke.py",)),
    ("Storage download URL", ("scripts/rest_route_gateway_smoke.py",)),
    ("Storage download bytes", ("scripts/rest_route_gateway_smoke.py",)),
    ("WebRTC room close", ("scripts/rest_route_gateway_smoke.py",)),
    ("WebRTC peer leave", ("scripts/rest_route_gateway_smoke.py",)),
    ("WebRTC track mute", ("scripts/rest_route_gateway_smoke.py",)),
    ("WebRTC track unpublish", ("scripts/rest_route_gateway_smoke.py",)),
    ("Auth OTP actions", ("sdk-conformance/run.mjs",)),
    ("Auth token actions", ("scripts/check-openapi-operationid-posture.py", "sdk-conformance/run.mjs")),
    ("Auth password actions", ("scripts/rest_route_gateway_smoke.py",)),
    ("Auth CSRF validation", ("sdk-conformance/run.mjs",)),
    ("Authz governance version list", ("scripts/check-http-api-style.mjs",)),
    ("Authz governance current revision", ("scripts/rest_route_gateway_smoke.py",)),
    ("Authz simulate/explain", ("scripts/rest_route_gateway_smoke.py",)),
    ("IdP provider refresh/test/preview actions", ("scripts/check-openapi-operationid-posture.py", "scripts/rest_route_gateway_smoke.py")),
    ("SCIM protocol exception", ("scripts/check-http-api-style.mjs",)),
)

EXPECTED_OPERATION_ID_ROUTE_TOKENS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Auth token actions", ("refreshToken", "validateToken", "introspectToken")),
    ("Auth password actions", ("changePassword", "forgotPassword", "resetPassword")),
    ("Authz simulate/explain", ("simulatePolicy", "explainPolicy")),
    (
        "IdP provider refresh/test/preview actions",
        ("forceJwksRefresh", "testProviderDiscovery", "previewClaimMapping"),
    ),
)

SERVED_ROUTE_PROOF_EXCEPTIONS = frozenset({"SCIM protocol exception"})

MISLEADING_STABILITY = re.compile(
    r"\bstable\s+(?:API|SDK)\b|\bbackward[- ]compat(?:ible|ibility)\b|\bGA\b",
    re.IGNORECASE,
)

RETIRED_ROUTE_LITERALS: tuple[str, ...] = (
    "/v1/api_keys",
    "/v1/analytics/pipeline_metrics",
    "/v1/analytics/pipeline_summaries",
    "/v1/analytics/executor_performance",
    "/v1/analytics/reconciliation_stats",
    "/v1/analytics/sla_compliance",
    "/v1/asset/",
    "/v1/storage/uploads/{file_id}/finalize",
    "/v1/storage/uploads/{fileId}/finalize",
    "/v1/storage/files/{file_id}/download-url",
    "/v1/storage/files/{fileId}/download-url",
    "/v1/storage/files/{file_id}/download\"",
    "/v1/storage/files/{fileId}/download\"",
    "/v1/webrtc/rooms/{room_id}/close",
    "/v1/webrtc/rooms/{roomId}/close",
    "/v1/webrtc/rooms/{room_id}/peers/{peer_id}/leave",
    "/v1/webrtc/rooms/{roomId}/peers/{peerId}/leave",
    "/v1/webrtc/tracks/{track_id}/mute",
    "/v1/webrtc/tracks/{trackId}/mute",
    "/v1/webrtc/tracks/{track_id}/unpublish",
    "/v1/webrtc/tracks/{trackId}/unpublish",
    "/v1/auth/otp",
    "/v1/auth/token",
    "/v1/auth/password:",
    "/v1/auth/csrf",
    "/v1/authz/governance/versions:list",
    "/v1/authz/governance/revision\"",
    "/v1/authz/governance/simulate",
    "/v1/authz/governance/explain",
    "/v1/idp/providers/{provider_id}/refresh_jwks",
    "/v1/idp/providers/{providerId}/refresh_jwks",
    "/v1/idp/providers/{provider_id}/test_discovery",
    "/v1/idp/providers/{providerId}/test_discovery",
    "/v1/idp/providers/{provider_id}/preview_claim_mapping",
    "/v1/idp/providers/{providerId}/preview_claim_mapping",
)

RETIRED_ROUTE_REGEXES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("/v1/auth/otp", re.compile(r"/v1/auth/otp(?=[:/\"}])")),
    ("/v1/auth/token", re.compile(r"/v1/auth/token(?=[:/\"}])")),
    ("/v1/auth/csrf", re.compile(r"/v1/auth/csrf(?=[:/\"}])")),
)

RETIRED_ROUTE_LITERAL_EXCEPTIONS = {label for label, _pattern in RETIRED_ROUTE_REGEXES}

RETIRED_SDK_METHOD_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("send_o_t_p", re.compile(r"\bsend_o_t_p\b")),
    ("resend_o_t_p", re.compile(r"\bresend_o_t_p\b")),
    ("verify_o_t_p", re.compile(r"\bverify_o_t_p\b")),
    ("confirm_m_f_a_enrollment", re.compile(r"\bconfirm_m_f_a_enrollment\b")),
    ("raw SendOTP() public method", re.compile(r"\bSendOTP\s*\(")),
    ("raw ResendOTP() public method", re.compile(r"\bResendOTP\s*\(")),
    ("raw VerifyOTP() public method", re.compile(r"\bVerifyOTP\s*\(")),
    ("raw ValidateCSRF() public method", re.compile(r"\bValidateCSRF\s*\(")),
    ("raw ForceJwksRefresh() public method", re.compile(r"\bForceJwksRefresh\s*\(")),
    ("raw TestProviderDiscovery() public method", re.compile(r"\bTestProviderDiscovery\s*\(")),
    ("raw PreviewClaimMapping() public method", re.compile(r"\bPreviewClaimMapping\s*\(")),
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def _public_doc_paths(root: Path) -> list[Path]:
    paths = [root / "README.md", root / "docs" / "README.md"]
    site = root / "docs" / "site"
    if site.is_dir():
        paths.extend(sorted(site.rglob("*.html")))
    return [path for path in paths if path.is_file()]


def _published_api_paths(root: Path) -> list[Path]:
    return [
        path
        for path in (
            root / "api" / "udb-broker.swagger.json",
            root / "docs" / "site" / "api" / "udb-broker.swagger.json",
            root / "docs" / "generated" / "udb-native-contract.json",
        )
        if path.is_file()
    ]


def _sdk_doc_example_paths(root: Path) -> list[Path]:
    paths: list[Path] = []
    sdk = root / "sdk"
    if not sdk.is_dir():
        return paths
    for path in sdk.rglob("*"):
        if not path.is_file():
            continue
        rel_parts = path.relative_to(sdk).parts
        if any(part in {"gen", "proto", "node_modules", "vendor", "dist-test", "bin", "obj", "target"} for part in rel_parts):
            continue
        if path.name == "README.md" or "examples" in rel_parts:
            paths.append(path)
    return sorted(paths)


def _check_benchmark_identity(root: Path) -> list[str]:
    failures: list[str] = []
    required_tokens = (
        (
            "benchmark collector canonical API identity",
            root / "scripts" / "collect_sdk_bench_results.py",
            "return operation_id or api_alias or wire_api",
        ),
        (
            "benchmark dashboard public identity fallback",
            root / "docs" / "site" / "benchmarks.js",
            "r.api || r.operation_id || r.api_alias || r.wire_api",
        ),
        (
            "benchmark generated-doc source public identity prose",
            root / "scripts" / "gen-sdk-benchmark-docs.mjs",
            "operation_id || api_alias || wire_api",
        ),
        (
            "benchmark generated listing public identity prose",
            root / "sdk" / "SDK_PERF_LISTING.md",
            "operation_id || api_alias || wire_api",
        ),
    )
    for label, path, token in required_tokens:
        if not path.is_file():
            failures.append(f"{label}: missing file {path.relative_to(root).as_posix()}")
            continue
        if token not in _read(path):
            failures.append(
                f"{label}: missing benchmark identity token {token!r} in {path.relative_to(root).as_posix()}"
        )
    return failures


def _split_markdown_row(line: str) -> list[str]:
    text = line.strip().strip("|")
    cells: list[str] = []
    current: list[str] = []
    in_code = False
    for char in text:
        if char == "`":
            in_code = not in_code
            current.append(char)
            continue
        if char == "|" and not in_code:
            cells.append("".join(current).strip())
            current = []
            continue
        current.append(char)
    cells.append("".join(current).strip())
    return cells


def _contains_control_character(value: str) -> bool:
    return any(ord(char) < 32 or ord(char) == 127 for char in value)


def _migration_fixture_rows(
    text: str,
) -> tuple[dict[str, tuple[str, tuple[str, ...]]], list[str], list[str]]:
    rows: dict[str, tuple[str, tuple[str, ...]]] = {}
    duplicates: list[str] = []
    shape_failures: list[str] = []
    header_seen = False
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        cells = _split_markdown_row(stripped)
        if not cells:
            continue
        domain = cells[0]
        for index, cell in enumerate(cells, start=1):
            if _contains_control_character(cell):
                shape_failures.append(
                    f"migration fixture row {domain or '<blank>'} column {index} contains control characters"
                )
        if domain == "Domain":
            header_seen = True
            if tuple(cells) != EXPECTED_MIGRATION_HEADER:
                shape_failures.append(
                    "migration fixture header must be "
                    + " | ".join(EXPECTED_MIGRATION_HEADER)
                )
            continue
        if set(domain) <= {"-"}:
            continue
        if len(cells) != len(EXPECTED_MIGRATION_HEADER):
            shape_failures.append(
                f"migration fixture row {domain or '<blank>'} has {len(cells)} columns, "
                f"expected {len(EXPECTED_MIGRATION_HEADER)}"
            )
            continue
        if domain in rows:
            duplicates.append(domain)
        rows[domain] = (stripped, tuple(cells))
    if not header_seen:
        shape_failures.append("migration fixture missing expected table header")
    return rows, duplicates, shape_failures


def _check_migration_fixture_coverage(root: Path) -> list[str]:
    path = root / "docs" / "api-sdk-beta-migration.md"
    if not path.is_file():
        return ["API/SDK beta migration fixture: missing file docs/api-sdk-beta-migration.md"]

    rows, duplicates, shape_failures = _migration_fixture_rows(_read(path))
    failures: list[str] = []
    failures.extend(shape_failures)
    for domain in sorted(set(duplicates)):
        failures.append(f"migration fixture duplicate row for {domain}")
    for expected in EXPECTED_MIGRATION_ROWS:
        row_entry = rows.get(expected.domain)
        if row_entry is None:
            failures.append(f"migration fixture missing row for {expected.domain}")
            continue
        row, cells = row_entry
        for kind, column_index, token_label, column_suffix in MIGRATION_TOKEN_COLUMNS:
            tokens = getattr(expected, f"{kind}_tokens")
            cell = cells[column_index] if len(cells) > column_index else ""
            for token in tokens:
                if token not in cell:
                    failures.append(
                        f"migration fixture row {expected.domain} missing {token_label} token "
                        f"{token!r} {column_suffix}"
                    )
        benchmark_cell = cells[5] if len(cells) > 5 else ""
        if "current operationId" in benchmark_cell:
            failures.append(
                f"migration fixture row {expected.domain} uses generic benchmark label "
                "'current operationId'; list concrete operationId or SDK alias tokens"
            )
        for token in expected.benchmark_tokens:
            if token not in benchmark_cell:
                failures.append(
                    f"migration fixture row {expected.domain} missing benchmark label token {token!r}"
                )
    expected_owner_domains = {row.domain for row in EXPECTED_MIGRATION_ROWS}
    owner_domains = {domain for domain, _tokens in EXPECTED_OWNER_TOKENS}
    if owner_domains != expected_owner_domains:
        missing = sorted(expected_owner_domains - owner_domains)
        extra = sorted(owner_domains - expected_owner_domains)
        failures.append(
            "migration fixture owner inventory drifted; "
            f"missing {missing or 'none'}, extra {extra or 'none'}"
        )
    for domain, tokens in EXPECTED_OWNER_TOKENS:
        row_entry = rows.get(domain)
        if row_entry is None:
            continue
        _row, cells = row_entry
        owner_cell = cells[6] if len(cells) > 6 else ""
        for token in tokens:
            if token not in owner_cell:
                failures.append(
                    f"migration fixture row {domain} missing test/guard owner token {token!r}"
                )
    return failures


def _check_served_route_proof_coverage(root: Path) -> list[str]:
    migration_path = root / "docs" / "api-sdk-beta-migration.md"
    route_smoke_path = root / "scripts" / "rest_route_gateway_smoke.py"
    if not migration_path.is_file() or not route_smoke_path.is_file():
        return []

    rows, _duplicates, _shape_failures = _migration_fixture_rows(_read(migration_path))
    route_smoke = _read(route_smoke_path)
    route_inventory = _served_route_inventory_text(route_smoke)
    operation_ids = set(_served_route_operation_id_strings(route_smoke))
    failures: list[str] = []
    if not route_inventory:
        failures.append("served route proof inventory block ROUTE_CASES is missing")

    expected_domains = {
        row.domain
        for row in EXPECTED_MIGRATION_ROWS
        if row.domain not in SERVED_ROUTE_PROOF_EXCEPTIONS
    }
    proof_domains = {domain for domain, _tokens in EXPECTED_SERVED_ROUTE_PROOF_TOKENS}
    if proof_domains != expected_domains:
        missing = sorted(expected_domains - proof_domains)
        extra = sorted(proof_domains - expected_domains)
        failures.append(
            "served route proof domain inventory drifted; "
            f"missing {missing or 'none'}, extra {extra or 'none'}"
        )

    for domain, tokens in EXPECTED_SERVED_ROUTE_PROOF_TOKENS:
        if domain not in rows:
            continue
        for token in tokens:
            if token not in route_inventory:
                failures.append(f"served route proof for {domain} missing token {token!r}")
    for domain, tokens in EXPECTED_OPERATION_ID_ROUTE_TOKENS:
        if domain not in rows:
            continue
        for token in tokens:
            if token not in operation_ids:
                failures.append(f"served route proof for {domain} missing operationId token {token!r}")
    return failures


def _served_route_inventory_text(route_smoke: str) -> str:
    return "\n".join(_served_route_inventory_strings(route_smoke))


def _served_route_inventory_strings(route_smoke: str) -> tuple[str, ...]:
    value = _served_route_cases_ast(route_smoke)
    if value is None:
        return ()
    return tuple(
        child.value
        for child in ast.walk(value)
        if isinstance(child, ast.Constant) and isinstance(child.value, str)
    )


def _served_route_operation_id_strings(route_smoke: str) -> tuple[str, ...]:
    value = _served_route_cases_ast(route_smoke)
    if value is None:
        return ()
    operation_ids: list[str] = []
    for node in ast.walk(value):
        if not isinstance(node, ast.Call):
            continue
        for keyword in node.keywords:
            if (
                keyword.arg == "operation_id"
                and isinstance(keyword.value, ast.Constant)
                and isinstance(keyword.value.value, str)
            ):
                operation_ids.append(keyword.value.value)
    return tuple(operation_ids)


def _served_route_cases_ast(route_smoke: str):
    try:
        module = ast.parse(route_smoke)
    except SyntaxError:
        return None
    for node in module.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == "ROUTE_CASES":
            return node.value
        elif isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "ROUTE_CASES" for target in node.targets):
            return node.value
    return None


def _served_route_proof_fixture_text() -> str:
    tokens = [token for _domain, domain_tokens in EXPECTED_SERVED_ROUTE_PROOF_TOKENS for token in domain_tokens]
    operation_ids = [
        token
        for _domain, domain_tokens in EXPECTED_OPERATION_ID_ROUTE_TOKENS
        for token in domain_tokens
    ]
    route_entries = "\n".join(
        f'        HttpRoute("GET", "{token}", "{token}"),'
        for token in tokens
    )
    operation_entries = "\n".join(
        f'        HttpRoute("POST", "/proof/{token}", "/proof/{token}", operation_id="{token}"),'
        for token in operation_ids
    )
    return (
        "ROUTE_CASES: tuple[RouteCase, ...] = (\n"
        "    RouteCase(\n"
        '        "fixture",\n'
        "        (\n"
        f"{route_entries}\n"
        f"{operation_entries}\n"
        "        ),\n"
        "        (),\n"
        "    ),\n"
        ")\n\n"
        "def validate_route_inventory(): pass\n"
    )


def _migration_fixture_text() -> str:
    return """# API/SDK Beta Migration Fixture

Public docs, SDK docs/examples, Pages content, and benchmark dashboards should use the new route, SDK alias, and benchmark identity.

| Domain | Old beta HTTP route or label | Current HTTP route | Old SDK/public method shape | Current SDK alias / operationId | Benchmark label | Test or guard owner |
|---|---|---|---|---|---|---|
| API keys collection | `/v1/api_keys...` | `/v1/api-keys...` | raw `ApiKeyService/*` fallback | `create_api_key` / `createApiKey`, `list_api_keys` / `listApiKeys`, `rotate_api_key` / `rotateApiKey`, `validate_api_key` / `validateApiKey` | `operation_id || api_alias || wire_api` | `scripts/check-http-api-style.mjs`; `scripts/check-api-sdk-alias-posture.py` |
| Analytics resources | `/v1/analytics/pipeline_metrics`, `/pipeline_summaries`, `/executor_performance`, `/reconciliation_stats`, `/sla_compliance` | `/v1/analytics/pipeline-metrics`, `/pipeline-summaries`, `/executor-performance`, `/reconciliation-stats`, `/sla-compliance` | raw `AnalyticsService/*` fallback | `record_pipeline_metric` / `recordPipelineMetric`, `get_pipeline_summary` / `getPipelineSummary`, `get_executor_performance` / `getExecutorPerformance` | `operation_id || api_alias || wire_api` | `scripts/check-http-api-style.mjs`; `scripts/gen-sdk-benchmark-docs.mjs` |
| Asset namespace | `/v1/asset/assets`, `/v1/asset/pipelines...` | `/v1/assets`, `/v1/assets/pipeline-definitions`, `/v1/assets/pipelines`, `/v1/assets/steps/{step_id}:complete` | raw `AssetService/*` fallback | `register_asset` / `registerAsset`, `start_pipeline` / `startPipeline`, `complete_step` / `completeStep` | `operation_id || api_alias || wire_api` | `scripts/check-http-api-style.mjs`; `sdk-conformance/run.mjs` |
| Storage upload finalize | `/v1/storage/uploads/{file_id}/finalize` | `/v1/storage/uploads/{file_id}:finalize` | raw `StorageService/FinalizeUpload` fallback | `finalize_upload` / `finalizeUpload` | `finalizeUpload` | `scripts/rest_route_gateway_smoke.py` |
| Storage download URL | `/v1/storage/files/{file_id}/download-url` | `/v1/storage/files/{file_id}:getDownloadUrl` | raw `GetDownloadUrl` fallback | `get_download_url` / `getDownloadUrl` | `getDownloadUrl` | `scripts/rest_route_gateway_smoke.py` |
| Storage download bytes | `/v1/storage/files/{file_id}/download` | `/v1/storage/files/{file_id}:download` | raw `DownloadFile` fallback | `download_file` / `downloadFile` | `downloadFile` | `scripts/rest_route_gateway_smoke.py` |
| WebRTC room close | `/v1/webrtc/rooms/{room_id}/close` | `/v1/webrtc/rooms/{room_id}:close` | raw `RoomService/CloseRoom` fallback | `close_room` / `closeRoom` | `closeRoom` | `scripts/rest_route_gateway_smoke.py` |
| WebRTC peer leave | `/v1/webrtc/rooms/{room_id}/peers/{peer_id}/leave` | `/v1/webrtc/rooms/{room_id}/peers/{peer_id}:leave` | raw `PeerService/LeaveRoom` fallback | `leave_room` / `leaveRoom` | `leaveRoom` | `scripts/rest_route_gateway_smoke.py` |
| WebRTC track mute | `/v1/webrtc/tracks/{track_id}/mute` | `/v1/webrtc/tracks/{track_id}:mute` | raw `TrackService/MuteTrack` fallback | `mute_track` / `muteTrack` | `muteTrack` | `scripts/rest_route_gateway_smoke.py` |
| WebRTC track unpublish | `/v1/webrtc/tracks/{track_id}/unpublish` | `/v1/webrtc/tracks/{track_id}:unpublish` | raw `TrackService/UnpublishTrack` fallback | `unpublish_track` / `unpublishTrack` | `unpublishTrack` | `scripts/rest_route_gateway_smoke.py` |
| Auth OTP actions | `/v1/auth/otp:send`, `/verify`, `/resend` | `/v1/auth/otps:send`, `/v1/auth/otps:verify`, `/v1/auth/otps:resend` | acronym-split fallbacks such as `send_o_t_p` / raw `SendOTP` | `send_otp` / `sendOtp`, `verify_otp` / `verifyOtp`, `resend_otp` / `resendOtp` | `sendOtp`, `verifyOtp`, `resendOtp` | `sdk-conformance/run.mjs` |
| Auth token actions | `/v1/auth/token:refresh`, `/validate`, `/introspect` | `/v1/auth/tokens:refresh`, `/v1/auth/tokens:validate`, `/v1/auth/tokens:introspect` | raw `RefreshToken` / `ValidateToken` / `IntrospectToken` fallback | `refresh_token` / `refreshToken`, `validate_token` / `validateToken`, `introspect_token` / `introspectToken` | `refreshToken`, `validateToken`, `introspectToken` | `scripts/check-openapi-operationid-posture.py`; `sdk-conformance/run.mjs` |
| Auth password actions | `/v1/auth/password:change`, `/forgot`, `/reset` | `/v1/auth/passwords:change`, `/v1/auth/passwords:forgot`, `/v1/auth/passwords:reset` | raw `ChangePassword` / `ForgotPassword` / `ResetPassword` fallback | `change_password` / `changePassword`, `forgot_password` / `forgotPassword`, `reset_password` / `resetPassword` | `changePassword`, `forgotPassword`, `resetPassword` | `scripts/rest_route_gateway_smoke.py` |
| Auth CSRF validation | `/v1/auth/csrf:validate` | `/v1/auth/csrf-tokens:validate` | raw `ValidateCSRF` fallback | `validate_csrf` / `validateCsrf` | `validateCsrf` | `sdk-conformance/run.mjs` |
| Authz governance version list | `/v1/authz/governance/versions:list` | `GET /v1/authz/governance/versions` | raw `ListPolicyVersions` fallback | `list_policy_versions` / `listPolicyVersions` | `listPolicyVersions` | `scripts/check-http-api-style.mjs` |
| Authz governance current revision | `/v1/authz/governance/revision` | `/v1/authz/governance/revisions/current` | raw `GetAuthzRevision` fallback | `get_authz_revision` / `getAuthzRevision` | `getAuthzRevision` | `scripts/rest_route_gateway_smoke.py` |
| Authz simulate/explain | `/v1/authz/governance/simulate`, `/explain` | `/v1/authz/governance/policy-simulations`, `/policy-explanations` | raw `SimulatePolicy` / `ExplainPolicy` fallback | `simulate_policy` / `simulatePolicy`, `explain_policy` / `explainPolicy` | `simulatePolicy`, `explainPolicy` | `scripts/rest_route_gateway_smoke.py` |
| IdP provider refresh/test/preview actions | `/v1/idp/providers/{provider_id}/refresh_jwks`, `/test_discovery`, `/preview_claim_mapping` | `/v1/idp/providers/{provider_id}:refreshJwks`, `:testDiscovery`, `:previewClaimMapping` | raw `ForceJwksRefresh`, `TestProviderDiscovery`, `PreviewClaimMapping` fallback | `force_jwks_refresh` / `forceJwksRefresh`, `test_provider_discovery` / `testProviderDiscovery`, `preview_claim_mapping` / `previewClaimMapping` | `forceJwksRefresh`, `testProviderDiscovery`, `previewClaimMapping` | `scripts/check-openapi-operationid-posture.py`; `scripts/rest_route_gateway_smoke.py` |
| SCIM protocol exception | SCIM path spelling stays protocol-owned | `/v1/idp/scim/v2/Users`, `/Groups` | unchanged protocol method aliases | `scim_*` aliases | `scim_*` aliases | `scripts/check-http-api-style.mjs` |

## Search Contract

Old beta route literals and acronym-split public SDK method names are valid only
in this fixture, release notes, archived plans, generated/protobuf internals, and
tests that explicitly prove migration behavior. Use:

```bash
python scripts/check-beta-versioning-posture.py --selftest
python scripts/check-beta-versioning-posture.py
```
"""


def check_source(root: Path = ROOT) -> list[str]:
    failures: list[str] = []

    for doc in REQUIRED_DOCS:
        path = root / doc.path
        if not path.is_file():
            failures.append(f"{doc.label}: missing file {doc.path}")
            continue
        text = _read(path)
        for token in doc.tokens:
            if token not in text:
                failures.append(f"{doc.label}: missing token {token!r} in {doc.path}")

    for path in _public_doc_paths(root):
        rel = path.relative_to(root).as_posix()
        for line_no, line in enumerate(_read(path).splitlines(), start=1):
            match = MISLEADING_STABILITY.search(line)
            if match:
                failures.append(
                    f"{rel}:{line_no}: misleading pre-1.0 stability wording {match.group(0)!r}"
                )

    for path in [*_public_doc_paths(root), *_published_api_paths(root)]:
        rel = path.relative_to(root).as_posix()
        text = _read(path)
        for literal in RETIRED_ROUTE_LITERALS:
            if literal in RETIRED_ROUTE_LITERAL_EXCEPTIONS:
                continue
            if literal in text:
                failures.append(f"{rel}: retired beta route literal leaked outside migration fixture: {literal}")
        for label, pattern in RETIRED_ROUTE_REGEXES:
            if pattern.search(text):
                failures.append(f"{rel}: retired beta route literal leaked outside migration fixture: {label}")

    for path in [*_public_doc_paths(root), *_sdk_doc_example_paths(root)]:
        rel = path.relative_to(root).as_posix()
        text = _read(path)
        for label, pattern in RETIRED_SDK_METHOD_PATTERNS:
            if pattern.search(text):
                failures.append(f"{rel}: retired beta SDK method leaked outside migration fixture: {label}")

    failures.extend(_check_benchmark_identity(root))
    failures.extend(_check_migration_fixture_coverage(root))
    failures.extend(_check_served_route_proof_coverage(root))

    return failures


def _write_required_fixture(root: Path) -> None:
    for doc in REQUIRED_DOCS:
        path = root / doc.path
        path.parent.mkdir(parents=True, exist_ok=True)
        text = _migration_fixture_text() if doc.path == "docs/api-sdk-beta-migration.md" else "\n".join(doc.tokens) + "\n"
        path.write_text(text, encoding="utf-8")
    (root / "README.md").write_text("# UDB\n\nBeta product docs.\n", encoding="utf-8")
    docs = root / "docs"
    docs.mkdir(parents=True, exist_ok=True)
    (docs / "README.md").write_text("# Docs\n\nCurrent beta docs.\n", encoding="utf-8")
    site = docs / "site"
    site.mkdir(parents=True, exist_ok=True)
    (site / "index.html").write_text("<p>UDB beta API guide.</p>\n", encoding="utf-8")
    (site / "benchmarks.js").write_text(
        "function fullRows(r) { return r.api || r.operation_id || r.api_alias || r.wire_api; }\n",
        encoding="utf-8",
    )
    scripts = root / "scripts"
    scripts.mkdir(parents=True, exist_ok=True)
    (scripts / "collect_sdk_bench_results.py").write_text(
        "def _identity(operation_id, api_alias, wire_api):\n    return operation_id or api_alias or wire_api\n",
        encoding="utf-8",
    )
    (scripts / "gen-sdk-benchmark-docs.mjs").write_text(
        "const prose = `operation_id || api_alias || wire_api`;\n",
        encoding="utf-8",
    )
    (scripts / "rest_route_gateway_smoke.py").write_text(_served_route_proof_fixture_text(), encoding="utf-8")
    sdk = root / "sdk"
    sdk.mkdir(parents=True, exist_ok=True)
    (sdk / "SDK_PERF_LISTING.md").write_text(
        "The dashboard groups by `operation_id || api_alias || wire_api`.\n",
        encoding="utf-8",
    )


def run_selftest() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_required_fixture(root)

        failures = check_source(root)
        if failures:
            raise AssertionError(f"expected clean fixture, got {failures}")

        versioning = root / "VERSIONING.md"
        versioning.write_text(
            versioning.read_text(encoding="utf-8").replace("### Beta Breaking-Change Note Template\n", ""),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("Beta Breaking-Change Note Template" in failure for failure in failures):
            raise AssertionError(f"expected missing template failure, got {failures}")

        _write_required_fixture(root)
        readme = root / "README.md"
        readme.write_text("# UDB\n\nThe stable API is ready.\n", encoding="utf-8")
        failures = check_source(root)
        if not any("README.md:3" in failure and "stable API" in failure for failure in failures):
            raise AssertionError(f"expected public-doc stability failure, got {failures}")

        _write_required_fixture(root)
        site_page = root / "docs" / "site" / "sdks.html"
        site_page.write_text("<p>SDK surface is backward compatible.</p>\n", encoding="utf-8")
        failures = check_source(root)
        if not any("docs/site/sdks.html:1" in failure and "backward compatible" in failure for failure in failures):
            raise AssertionError(f"expected site stability failure, got {failures}")

        _write_required_fixture(root)
        api = root / "api" / "udb-broker.swagger.json"
        api.parent.mkdir(parents=True, exist_ok=True)
        api.write_text('{"paths": {"/v1/api_keys": {}}}\n', encoding="utf-8")
        failures = check_source(root)
        if not any("/v1/api_keys" in failure for failure in failures):
            raise AssertionError(f"expected retired route leakage failure, got {failures}")

        _write_required_fixture(root)
        sdk_readme = root / "sdk" / "typescript" / "README.md"
        sdk_readme.parent.mkdir(parents=True, exist_ok=True)
        sdk_readme.write_text("Call client.send_o_t_p(...) in beta examples.\n", encoding="utf-8")
        failures = check_source(root)
        if not any("retired beta SDK method" in failure and "send_o_t_p" in failure for failure in failures):
            raise AssertionError(f"expected retired SDK method leakage failure, got {failures}")

        _write_required_fixture(root)
        sdk_example = root / "sdk" / "java" / "examples" / "Legacy.java"
        sdk_example.parent.mkdir(parents=True, exist_ok=True)
        sdk_example.write_text("client.SendOTP(request);\n", encoding="utf-8")
        failures = check_source(root)
        if not any("retired beta SDK method" in failure and "SendOTP" in failure for failure in failures):
            raise AssertionError(f"expected raw SDK method leakage failure, got {failures}")

        _write_required_fixture(root)
        collector = root / "scripts" / "collect_sdk_bench_results.py"
        collector.write_text(
            "def _identity(operation_id, api_alias, wire_api):\n    return wire_api\n",
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("benchmark collector canonical API identity" in failure for failure in failures):
            raise AssertionError(f"expected benchmark collector identity failure, got {failures}")

        _write_required_fixture(root)
        dashboard = root / "docs" / "site" / "benchmarks.js"
        dashboard.write_text("function fullRows(r) { return r.wire_api || r.operation_id; }\n", encoding="utf-8")
        failures = check_source(root)
        if not any("benchmark dashboard public identity fallback" in failure for failure in failures):
            raise AssertionError(f"expected benchmark dashboard identity failure, got {failures}")

        _write_required_fixture(root)
        migration = root / "docs" / "api-sdk-beta-migration.md"
        migration.write_text(
            migration.read_text(encoding="utf-8").replace(
                "`get_download_url` / `getDownloadUrl`",
                "`GetDownloadUrl` fallback only",
            ),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("migration fixture row Storage download URL missing alias token" in failure for failure in failures):
            raise AssertionError(f"expected migration fixture coverage failure, got {failures}")

        _write_required_fixture(root)
        migration = root / "docs" / "api-sdk-beta-migration.md"
        text = migration.read_text(encoding="utf-8")
        row = next(line for line in text.splitlines() if line.startswith("| Storage download URL |"))
        migration.write_text(text.replace(row, f"{row}\n{row}"), encoding="utf-8")
        failures = check_source(root)
        if not any("migration fixture duplicate row for Storage download URL" in failure for failure in failures):
            raise AssertionError(f"expected migration fixture duplicate-row failure, got {failures}")

        _write_required_fixture(root)
        migration = root / "docs" / "api-sdk-beta-migration.md"
        text = migration.read_text(encoding="utf-8")
        row = next(line for line in text.splitlines() if line.startswith("| Storage download URL |"))
        malformed = row[:-1] + " | extra evidence |"
        migration.write_text(text.replace(row, malformed), encoding="utf-8")
        failures = check_source(root)
        if not any("migration fixture row Storage download URL has 8 columns, expected 7" in failure for failure in failures):
            raise AssertionError(f"expected migration fixture row-shape failure, got {failures}")

        _write_required_fixture(root)
        migration = root / "docs" / "api-sdk-beta-migration.md"
        text = migration.read_text(encoding="utf-8")
        row = next(line for line in text.splitlines() if line.startswith("| Storage download URL |"))
        poisoned = row.replace(" | `getDownloadUrl` | `scripts", " | `getDownloadUrl`\x00 | `scripts", 1)
        migration.write_text(text.replace(row, poisoned), encoding="utf-8")
        failures = check_source(root)
        if not any(
            "migration fixture row Storage download URL column 6 contains control characters" in failure
            for failure in failures
        ):
            raise AssertionError(f"expected migration fixture control-character failure, got {failures}")

        _write_required_fixture(root)
        migration = root / "docs" / "api-sdk-beta-migration.md"
        migration.write_text(
            migration.read_text(encoding="utf-8").replace(
                "| Storage download URL | `/v1/storage/files/{file_id}/download-url` | `/v1/storage/files/{file_id}:getDownloadUrl` | raw `GetDownloadUrl` fallback | `get_download_url` / `getDownloadUrl` | `getDownloadUrl` | `scripts/rest_route_gateway_smoke.py` |",
                "| Storage download URL | `/v1/storage/files/{file_id}/download-url` | `/v1/storage/files/{file_id}:getDownloadUrl` | raw `GetDownloadUrl` fallback | `get_download_url` / `getDownloadUrl` | raw `StorageService/GetDownloadUrl` | `scripts/rest_route_gateway_smoke.py` |",
            ),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("migration fixture row Storage download URL missing benchmark label token" in failure for failure in failures):
            raise AssertionError(f"expected migration fixture benchmark-label failure, got {failures}")

        _write_required_fixture(root)
        migration = root / "docs" / "api-sdk-beta-migration.md"
        migration.write_text(
            migration.read_text(encoding="utf-8").replace(
                "| Auth token actions | `/v1/auth/token:refresh`, `/validate`, `/introspect` | `/v1/auth/tokens:refresh`, `/v1/auth/tokens:validate`, `/v1/auth/tokens:introspect` | raw `RefreshToken` / `ValidateToken` / `IntrospectToken` fallback | `refresh_token` / `refreshToken`, `validate_token` / `validateToken`, `introspect_token` / `introspectToken` | `refreshToken`, `validateToken`, `introspectToken` | `scripts/check-openapi-operationid-posture.py`; `sdk-conformance/run.mjs` |",
                "| Auth token actions | `/v1/auth/token:refresh`, `/validate`, `/introspect` | `/v1/auth/tokens:refresh`, `/v1/auth/tokens:validate`, `/v1/auth/tokens:introspect` | raw `RefreshToken` / `ValidateToken` / `IntrospectToken` fallback | `refresh_token` / `refreshToken`, `validate_token` / `validateToken`, `introspect_token` / `introspectToken` | current operationId | `scripts/check-openapi-operationid-posture.py`; `sdk-conformance/run.mjs` |",
            ),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("uses generic benchmark label" in failure for failure in failures):
            raise AssertionError(f"expected generic benchmark-label failure, got {failures}")

        _write_required_fixture(root)
        migration = root / "docs" / "api-sdk-beta-migration.md"
        migration.write_text(
            migration.read_text(encoding="utf-8").replace(
                "| Storage download URL | `/v1/storage/files/{file_id}/download-url` | `/v1/storage/files/{file_id}:getDownloadUrl` | raw `GetDownloadUrl` fallback | `get_download_url` / `getDownloadUrl` | `getDownloadUrl` | `scripts/rest_route_gateway_smoke.py` |",
                "| Storage download URL | `/v1/storage/files/{file_id}/download-url`, `/v1/storage/files/{file_id}:getDownloadUrl` | current route moved to old column | raw `GetDownloadUrl` fallback | `get_download_url` / `getDownloadUrl` | `getDownloadUrl` | `scripts/rest_route_gateway_smoke.py` |",
            ),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("missing current token" in failure and "Current HTTP route" in failure for failure in failures):
            raise AssertionError(f"expected migration fixture column-specific route failure, got {failures}")

        _write_required_fixture(root)
        migration = root / "docs" / "api-sdk-beta-migration.md"
        migration.write_text(
            migration.read_text(encoding="utf-8").replace(
                "| Storage download URL | `/v1/storage/files/{file_id}/download-url` | `/v1/storage/files/{file_id}:getDownloadUrl` | raw `GetDownloadUrl` fallback | `get_download_url` / `getDownloadUrl` | `getDownloadUrl` | `scripts/rest_route_gateway_smoke.py` |",
                "| Storage download URL | `/v1/storage/files/{file_id}/download-url` | `/v1/storage/files/{file_id}:getDownloadUrl` | raw `GetDownloadUrl` fallback / `getDownloadUrl` | `get_download_url` moved to old method notes | `getDownloadUrl` | `scripts/rest_route_gateway_smoke.py` |",
            ),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("missing alias token" in failure and "Current SDK alias / operationId" in failure for failure in failures):
            raise AssertionError(f"expected migration fixture column-specific alias failure, got {failures}")

        _write_required_fixture(root)
        migration = root / "docs" / "api-sdk-beta-migration.md"
        migration.write_text(
            migration.read_text(encoding="utf-8").replace(
                "| Storage download URL | `/v1/storage/files/{file_id}/download-url` | `/v1/storage/files/{file_id}:getDownloadUrl` | raw `GetDownloadUrl` fallback | `get_download_url` / `getDownloadUrl` | `getDownloadUrl` | `scripts/rest_route_gateway_smoke.py` |",
                "| Storage download URL | `/v1/storage/files/{file_id}/download-url` | `/v1/storage/files/{file_id}:getDownloadUrl` | raw download URL fallback | `get_download_url` / `getDownloadUrl` | `getDownloadUrl` | `scripts/rest_route_gateway_smoke.py` |",
            ),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any(
            "missing old SDK/public method token" in failure and "Old SDK/public method shape" in failure
            for failure in failures
        ):
            raise AssertionError(f"expected migration fixture old-SDK column failure, got {failures}")

        _write_required_fixture(root)
        migration = root / "docs" / "api-sdk-beta-migration.md"
        migration.write_text(
            migration.read_text(encoding="utf-8").replace(
                "| Storage download URL | `/v1/storage/files/{file_id}/download-url` | `/v1/storage/files/{file_id}:getDownloadUrl` | raw `GetDownloadUrl` fallback | `get_download_url` / `getDownloadUrl` | `getDownloadUrl` | `scripts/rest_route_gateway_smoke.py` |",
                "| Storage download URL | `/v1/storage/files/{file_id}/download-url` | `/v1/storage/files/{file_id}:getDownloadUrl` | raw `GetDownloadUrl` fallback | `get_download_url` / `getDownloadUrl` | `getDownloadUrl` | spreadsheet owner |",
            ),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("migration fixture row Storage download URL missing test/guard owner token" in failure for failure in failures):
            raise AssertionError(f"expected migration fixture owner-token failure, got {failures}")

        _write_required_fixture(root)
        route_smoke = root / "scripts" / "rest_route_gateway_smoke.py"
        route_smoke.write_text(
            route_smoke.read_text(encoding="utf-8").replace(
                "/v1/storage/files/{fileId}:getDownloadUrl",
                "/v1/storage/files/{fileId}:legacyDownloadUrl",
            ),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("served route proof for Storage download URL missing token" in failure for failure in failures):
            raise AssertionError(f"expected served route proof coverage regression, got {failures}")

        _write_required_fixture(root)
        route_smoke = root / "scripts" / "rest_route_gateway_smoke.py"
        route_smoke.write_text(
            route_smoke.read_text(encoding="utf-8").replace(
                "/v1/storage/files/{fileId}:getDownloadUrl",
                "/v1/storage/files/{fileId}:legacyDownloadUrl",
            )
            + "\n# Non-inventory prose must not satisfy served route proof: /v1/storage/files/{fileId}:getDownloadUrl\n",
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("served route proof for Storage download URL missing token" in failure for failure in failures):
            raise AssertionError(f"expected served route inventory-only coverage regression, got {failures}")

        _write_required_fixture(root)
        route_smoke = root / "scripts" / "rest_route_gateway_smoke.py"
        route_smoke.write_text(
            route_smoke.read_text(encoding="utf-8").replace(
                'HttpRoute("GET", "/v1/storage/files/{fileId}:getDownloadUrl", "/v1/storage/files/{fileId}:getDownloadUrl"),',
                '# Commented route token must not satisfy served route proof: /v1/storage/files/{fileId}:getDownloadUrl',
            ),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("served route proof for Storage download URL missing token" in failure for failure in failures):
            raise AssertionError(f"expected served route AST inventory coverage regression, got {failures}")

        _write_required_fixture(root)
        route_smoke = root / "scripts" / "rest_route_gateway_smoke.py"
        route_smoke.write_text(
            route_smoke.read_text(encoding="utf-8").replace(
                'operation_id="refreshToken"',
                'operation_id="legacyRefreshToken"',
            ),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("served route proof for Auth token actions missing operationId token" in failure for failure in failures):
            raise AssertionError(f"expected served route operationId proof regression, got {failures}")

    print("beta versioning posture selftest passed")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selftest", action="store_true", help="run no-repo fixture assertions")
    args = parser.parse_args(argv)
    if args.selftest:
        return run_selftest()

    failures = check_source()
    if failures:
        for failure in failures:
            print(f"::error::{failure}")
        return 1
    print("beta versioning posture guard passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
