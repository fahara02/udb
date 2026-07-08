#!/usr/bin/env python3
"""REST/gateway route smoke for Chapter 14 API route migrations.

The committed OpenAPI artifact proves the generated route table. Live mode is
stricter: canonical routes may fail with auth or validation errors, but must not
look missing; retired beta routes must look absent.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import socketserver
import tempfile
import threading
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SWAGGER = ROOT / "api" / "udb-broker.swagger.json"
ROUTE_MISSING_STATUSES = {404, 405}
ROUTE_REDIRECT_STATUSES = set(range(300, 400))
ROUTE_SERVER_ERROR_STATUSES = set(range(500, 600))
ROUTE_NEGOTIATION_FAILURE_STATUSES = {406, 415}
ROUTE_NO_BODY_SUCCESS_STATUSES = {204, 205}
DEFAULT_TIMEOUT_SECONDS = 5.0
MAX_LIVE_TIMEOUT_SECONDS = 120.0
MAX_REST_RESPONSE_BYTES = 1_048_576
MAX_LIVE_HEADER_COUNT = 32
MAX_LIVE_HEADER_VALUE_BYTES = 8_192
MAX_API_ERROR_STRING_BYTES = 8_192
HTTP_HEADER_NAME_CHARS = frozenset("!#$%&'*+-.^_`|~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
PROOF_MANAGED_REQUEST_HEADERS = {"accept", "content-type"}
JSON_MEDIA_TYPE = "application/json"
REST_TIMEOUT_DECIMAL_PATTERN = re.compile(r"^(?:[1-9]\d*(?:\.\d+)?|0\.\d*[1-9]\d*)$")
API_ERROR_FIELDS = ("code", "message", "httpStatusCode", "retryable", "fieldViolations")
API_ERROR_ALLOWED_FIELDS = frozenset(API_ERROR_FIELDS)
API_ERROR_FIELD_VIOLATION_FIELDS = frozenset(("field", "description"))
LIVE_BOUNDARY_HTTP_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}
CANONICAL_GRPC_ERROR_CODES = {
    "CANCELLED",
    "UNKNOWN",
    "INVALID_ARGUMENT",
    "DEADLINE_EXCEEDED",
    "NOT_FOUND",
    "ALREADY_EXISTS",
    "PERMISSION_DENIED",
    "RESOURCE_EXHAUSTED",
    "FAILED_PRECONDITION",
    "ABORTED",
    "OUT_OF_RANGE",
    "UNIMPLEMENTED",
    "INTERNAL",
    "UNAVAILABLE",
    "DATA_LOSS",
    "UNAUTHENTICATED",
}
GRPC_HTTP_STATUS_FOR_CODE = {
    "INVALID_ARGUMENT": 400,
    "FAILED_PRECONDITION": 400,
    "UNAUTHENTICATED": 401,
    "PERMISSION_DENIED": 403,
    "NOT_FOUND": 404,
    "ALREADY_EXISTS": 409,
    "ABORTED": 409,
    "RESOURCE_EXHAUSTED": 429,
    "UNKNOWN": 500,
    "INTERNAL": 500,
    "DATA_LOSS": 500,
    "UNIMPLEMENTED": 501,
    "UNAVAILABLE": 503,
    "DEADLINE_EXCEEDED": 504,
}
EXPECTED_ROUTE_FAMILY_NAMES = (
    "api keys",
    "analytics",
    "assets",
    "storage",
    "webrtc",
    "auth",
    "authz governance",
    "identity providers",
    "config flags",
    "embedding sources",
    "metering quotas",
    "search indexes",
    "vault secrets",
)
EXPECTED_CANONICAL_ROUTE_COUNT = 46
EXPECTED_RETIRED_ROUTE_COUNT = 44
EVIDENCE_SCHEMA_VERSION = 1


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401, ANN001
        return None


NO_REDIRECT_OPENER = urllib.request.build_opener(_NoRedirectHandler)


@dataclass(frozen=True)
class HttpRoute:
    method: str
    openapi_path: str
    live_path: str
    operation_id: str | None = None
    sdk_alias: str | None = None


@dataclass(frozen=True)
class RouteCase:
    family: str
    canonical: tuple[HttpRoute, ...]
    retired: tuple[HttpRoute, ...]


@dataclass(frozen=True)
class HttpResponse:
    status: int
    content_type: str
    body: bytes
    extra_content_types: tuple[str, ...] = ()


ROUTE_CASES: tuple[RouteCase, ...] = (
    RouteCase(
        "api keys",
        (
            HttpRoute("GET", "/v1/api-keys", "/v1/api-keys"),
            HttpRoute("POST", "/v1/api-keys:validate", "/v1/api-keys:validate"),
        ),
        (HttpRoute("GET", "/v1/api_keys", "/v1/api_keys"),),
    ),
    RouteCase(
        "analytics",
        (
            HttpRoute("POST", "/v1/analytics/pipeline-metrics", "/v1/analytics/pipeline-metrics"),
            HttpRoute("GET", "/v1/analytics/pipeline-summaries", "/v1/analytics/pipeline-summaries"),
            HttpRoute("GET", "/v1/analytics/executor-performance", "/v1/analytics/executor-performance"),
            HttpRoute("GET", "/v1/analytics/reconciliation-stats", "/v1/analytics/reconciliation-stats"),
            HttpRoute("GET", "/v1/analytics/sla-compliance", "/v1/analytics/sla-compliance"),
        ),
        (
            HttpRoute("POST", "/v1/analytics/pipeline_metrics", "/v1/analytics/pipeline_metrics"),
            HttpRoute("GET", "/v1/analytics/pipeline_summaries", "/v1/analytics/pipeline_summaries"),
            HttpRoute("GET", "/v1/analytics/executor_performance", "/v1/analytics/executor_performance"),
            HttpRoute("GET", "/v1/analytics/reconciliation_stats", "/v1/analytics/reconciliation_stats"),
            HttpRoute("GET", "/v1/analytics/sla_compliance", "/v1/analytics/sla_compliance"),
        ),
    ),
    RouteCase(
        "assets",
        (
            HttpRoute("GET", "/v1/assets", "/v1/assets"),
            HttpRoute("POST", "/v1/assets", "/v1/assets"),
            HttpRoute("POST", "/v1/assets/pipeline-definitions", "/v1/assets/pipeline-definitions"),
            HttpRoute("POST", "/v1/assets/pipelines", "/v1/assets/pipelines"),
            HttpRoute("POST", "/v1/assets/steps/{stepId}:complete", "/v1/assets/steps/step-smoke:complete"),
            HttpRoute("GET", "/v1/assets/{assetId}", "/v1/assets/asset-smoke"),
        ),
        (
            HttpRoute("GET", "/v1/asset/assets", "/v1/asset/assets"),
            HttpRoute("POST", "/v1/asset/assets", "/v1/asset/assets"),
            HttpRoute("POST", "/v1/asset/pipeline-definitions", "/v1/asset/pipeline-definitions"),
            HttpRoute("POST", "/v1/asset/pipelines", "/v1/asset/pipelines"),
            HttpRoute("POST", "/v1/asset/steps/{stepId}:complete", "/v1/asset/steps/step-smoke:complete"),
            HttpRoute("GET", "/v1/asset/{assetId}", "/v1/asset/asset-smoke"),
        ),
    ),
    RouteCase(
        "storage",
        (
            HttpRoute(
                "GET",
                "/v1/storage/files/{fileId}:getDownloadUrl",
                "/v1/storage/files/file-smoke:getDownloadUrl",
                operation_id="getDownloadUrl",
                sdk_alias="get_download_url",
            ),
            HttpRoute(
                "GET",
                "/v1/storage/files/{fileId}:download",
                "/v1/storage/files/file-smoke:download",
                operation_id="downloadFile",
                sdk_alias="download_file",
            ),
            HttpRoute(
                "POST",
                "/v1/storage/uploads/{fileId}:finalize",
                "/v1/storage/uploads/file-smoke:finalize",
                operation_id="finalizeUpload",
                sdk_alias="finalize_upload",
            ),
        ),
        (
            HttpRoute("GET", "/v1/storage/files/{fileId}/download-url", "/v1/storage/files/file-smoke/download-url"),
            HttpRoute("GET", "/v1/storage/files/{fileId}/download", "/v1/storage/files/file-smoke/download"),
            HttpRoute("POST", "/v1/storage/uploads/{fileId}/finalize", "/v1/storage/uploads/file-smoke/finalize"),
        ),
    ),
    RouteCase(
        "webrtc",
        (
            HttpRoute("POST", "/v1/webrtc/rooms/{roomId}:close", "/v1/webrtc/rooms/room-smoke:close"),
            HttpRoute("POST", "/v1/webrtc/rooms/{roomId}/peers/{peerId}:leave", "/v1/webrtc/rooms/room-smoke/peers/peer-smoke:leave"),
            HttpRoute("POST", "/v1/webrtc/tracks/{trackId}:startEgress", "/v1/webrtc/tracks/track-smoke:startEgress"),
            HttpRoute("POST", "/v1/webrtc/tracks/{trackId}:mute", "/v1/webrtc/tracks/track-smoke:mute"),
            HttpRoute("POST", "/v1/webrtc/tracks/{trackId}:unpublish", "/v1/webrtc/tracks/track-smoke:unpublish"),
        ),
        (
            HttpRoute("POST", "/v1/webrtc/rooms/{roomId}/close", "/v1/webrtc/rooms/room-smoke/close"),
            HttpRoute("POST", "/v1/webrtc/rooms/{roomId}/peers/{peerId}/leave", "/v1/webrtc/rooms/room-smoke/peers/peer-smoke/leave"),
            HttpRoute("POST", "/v1/webrtc/rooms/{roomId}/tracks/{trackId}/egress", "/v1/webrtc/rooms/room-smoke/tracks/track-smoke/egress"),
            HttpRoute("POST", "/v1/webrtc/tracks/{trackId}/mute", "/v1/webrtc/tracks/track-smoke/mute"),
            HttpRoute("POST", "/v1/webrtc/tracks/{trackId}/unpublish", "/v1/webrtc/tracks/track-smoke/unpublish"),
        ),
    ),
    RouteCase(
        "auth",
        (
            HttpRoute("POST", "/v1/auth/otps:send", "/v1/auth/otps:send", operation_id="sendOtp", sdk_alias="send_otp"),
            HttpRoute("POST", "/v1/auth/otps:verify", "/v1/auth/otps:verify", operation_id="verifyOtp", sdk_alias="verify_otp"),
            HttpRoute("POST", "/v1/auth/otps:resend", "/v1/auth/otps:resend", operation_id="resendOtp", sdk_alias="resend_otp"),
            HttpRoute("POST", "/v1/auth/tokens:refresh", "/v1/auth/tokens:refresh", operation_id="refreshToken", sdk_alias="refresh_token"),
            HttpRoute("POST", "/v1/auth/tokens:validate", "/v1/auth/tokens:validate", operation_id="validateToken", sdk_alias="validate_token"),
            HttpRoute(
                "POST",
                "/v1/auth/tokens:introspect",
                "/v1/auth/tokens:introspect",
                operation_id="introspectToken",
                sdk_alias="introspect_token",
            ),
            HttpRoute("POST", "/v1/auth/passwords:change", "/v1/auth/passwords:change", operation_id="changePassword", sdk_alias="change_password"),
            HttpRoute("POST", "/v1/auth/passwords:forgot", "/v1/auth/passwords:forgot", operation_id="forgotPassword", sdk_alias="forgot_password"),
            HttpRoute("POST", "/v1/auth/passwords:reset", "/v1/auth/passwords:reset", operation_id="resetPassword", sdk_alias="reset_password"),
            HttpRoute("POST", "/v1/auth/csrf-tokens:validate", "/v1/auth/csrf-tokens:validate"),
        ),
        (
            HttpRoute("POST", "/v1/auth/otp:send", "/v1/auth/otp:send"),
            HttpRoute("POST", "/v1/auth/otp:verify", "/v1/auth/otp:verify"),
            HttpRoute("POST", "/v1/auth/otp:resend", "/v1/auth/otp:resend"),
            HttpRoute("POST", "/v1/auth/token:refresh", "/v1/auth/token:refresh"),
            HttpRoute("POST", "/v1/auth/token:validate", "/v1/auth/token:validate"),
            HttpRoute("POST", "/v1/auth/token:introspect", "/v1/auth/token:introspect"),
            HttpRoute("POST", "/v1/auth/password:change", "/v1/auth/password:change"),
            HttpRoute("POST", "/v1/auth/password:forgot", "/v1/auth/password:forgot"),
            HttpRoute("POST", "/v1/auth/password:reset", "/v1/auth/password:reset"),
            HttpRoute("POST", "/v1/auth/csrf:validate", "/v1/auth/csrf:validate"),
        ),
    ),
    RouteCase(
        "authz governance",
        (
            HttpRoute("GET", "/v1/authz/governance/versions", "/v1/authz/governance/versions"),
            HttpRoute("GET", "/v1/authz/governance/revisions/current", "/v1/authz/governance/revisions/current"),
            HttpRoute(
                "POST",
                "/v1/authz/governance/policy-simulations",
                "/v1/authz/governance/policy-simulations",
                operation_id="simulatePolicy",
                sdk_alias="simulate_policy",
            ),
            HttpRoute(
                "POST",
                "/v1/authz/governance/policy-explanations",
                "/v1/authz/governance/policy-explanations",
                operation_id="explainPolicy",
                sdk_alias="explain_policy",
            ),
        ),
        (
            HttpRoute("POST", "/v1/authz/governance/versions:list", "/v1/authz/governance/versions:list"),
            HttpRoute("GET", "/v1/authz/governance/revision", "/v1/authz/governance/revision"),
            HttpRoute("POST", "/v1/authz/governance/simulate", "/v1/authz/governance/simulate"),
            HttpRoute("POST", "/v1/authz/governance/explain", "/v1/authz/governance/explain"),
        ),
    ),
    RouteCase(
        "identity providers",
        (
            HttpRoute(
                "POST",
                "/v1/idp/providers/{providerId}:refreshJwks",
                "/v1/idp/providers/provider-smoke:refreshJwks",
                operation_id="forceJwksRefresh",
                sdk_alias="force_jwks_refresh",
            ),
            HttpRoute(
                "POST",
                "/v1/idp/providers/{providerId}:testDiscovery",
                "/v1/idp/providers/provider-smoke:testDiscovery",
                operation_id="testProviderDiscovery",
                sdk_alias="test_provider_discovery",
            ),
            HttpRoute(
                "POST",
                "/v1/idp/providers/{providerId}:previewClaimMapping",
                "/v1/idp/providers/provider-smoke:previewClaimMapping",
                operation_id="previewClaimMapping",
                sdk_alias="preview_claim_mapping",
            ),
        ),
        (
            HttpRoute("POST", "/v1/idp/providers/{providerId}/refresh_jwks", "/v1/idp/providers/provider-smoke/refresh_jwks"),
            HttpRoute("POST", "/v1/idp/providers/{providerId}/test_discovery", "/v1/idp/providers/provider-smoke/test_discovery"),
            HttpRoute("POST", "/v1/idp/providers/{providerId}/preview_claim_mapping", "/v1/idp/providers/provider-smoke/preview_claim_mapping"),
        ),
    ),
    RouteCase(
        "config flags",
        (
            HttpRoute("GET", "/v1/config/flags", "/v1/config/flags"),
            HttpRoute("GET", "/v1/config/flags/{flagKey}", "/v1/config/flags/flag-smoke"),
        ),
        (
            HttpRoute("POST", "/v1/config/flags:list", "/v1/config/flags:list"),
            HttpRoute("POST", "/v1/config/flags:get", "/v1/config/flags:get"),
        ),
    ),
    RouteCase(
        "embedding sources",
        (HttpRoute("GET", "/v1/embedding/sources", "/v1/embedding/sources"),),
        (HttpRoute("POST", "/v1/embedding/sources:list", "/v1/embedding/sources:list"),),
    ),
    RouteCase(
        "metering quotas",
        (
            HttpRoute("GET", "/v1/metering/quotas", "/v1/metering/quotas"),
            HttpRoute("GET", "/v1/metering/quotas/{metric}", "/v1/metering/quotas/messages"),
        ),
        (
            HttpRoute("POST", "/v1/metering/quotas:list", "/v1/metering/quotas:list"),
            HttpRoute("POST", "/v1/metering/quotas:get", "/v1/metering/quotas:get"),
        ),
    ),
    RouteCase(
        "search indexes",
        (HttpRoute("GET", "/v1/search/indexes", "/v1/search/indexes"),),
        (HttpRoute("POST", "/v1/search/indexes:list", "/v1/search/indexes:list"),),
    ),
    RouteCase(
        "vault secrets",
        (
            HttpRoute("GET", "/v1/vault/secrets", "/v1/vault/secrets"),
            HttpRoute("GET", "/v1/vault/secrets/{secretPath}", "/v1/vault/secrets/app/smoke"),
        ),
        (HttpRoute("POST", "/v1/vault/secrets:get", "/v1/vault/secrets:get"),),
    ),
)


def validate_route_inventory(route_cases: tuple[RouteCase, ...] = ROUTE_CASES) -> list[str]:
    failures: list[str] = []
    family_names = tuple(case.family for case in route_cases)
    if family_names != EXPECTED_ROUTE_FAMILY_NAMES:
        failures.append(
            "REST route migration inventory family list drifted; "
            f"expected {EXPECTED_ROUTE_FAMILY_NAMES!r}, got {family_names!r}"
        )
    canonical_count = sum(len(case.canonical) for case in route_cases)
    retired_count = sum(len(case.retired) for case in route_cases)
    if canonical_count != EXPECTED_CANONICAL_ROUTE_COUNT or retired_count != EXPECTED_RETIRED_ROUTE_COUNT:
        failures.append(
            "REST route migration inventory must cover "
            f"{EXPECTED_CANONICAL_ROUTE_COUNT} canonical routes and "
            f"{EXPECTED_RETIRED_ROUTE_COUNT} retired routes; got "
            f"{canonical_count} canonical and {retired_count} retired"
        )
    for case in route_cases:
        if not case.canonical:
            failures.append(f"{case.family}: REST route migration inventory missing canonical routes")
        if not case.retired:
            failures.append(f"{case.family}: REST route migration inventory missing retired beta routes")
    seen_openapi: dict[tuple[str, str], str] = {}
    seen_live: dict[tuple[str, str], str] = {}
    for case in route_cases:
        for kind, routes in (("canonical", case.canonical), ("retired", case.retired)):
            for route in routes:
                openapi_key = (route.method.upper(), route.openapi_path)
                live_key = (route.method.upper(), route.live_path)
                label = f"{case.family} {kind} {route.method} {route.openapi_path}"
                method = route.method
                stripped_method = method.strip()
                if method != stripped_method:
                    failures.append(f"{label}: REST route method must not include surrounding whitespace")
                elif _contains_control_character(method):
                    failures.append(f"{label}: REST route method must not contain control characters")
                elif any(char.isspace() for char in method):
                    failures.append(f"{label}: REST route method must not include whitespace")
                elif method != method.upper():
                    failures.append(f"{label}: REST route method must be uppercase")
                elif method not in LIVE_BOUNDARY_HTTP_METHODS:
                    allowed = ", ".join(sorted(LIVE_BOUNDARY_HTTP_METHODS))
                    failures.append(f"{label}: REST route method must be one of {allowed}")
                for path_field, path_value in (
                    ("openapi_path", route.openapi_path),
                    ("live_path", route.live_path),
                ):
                    stripped_path = path_value.strip()
                    if path_value != stripped_path:
                        failures.append(f"{label}: REST route {path_field} must not include surrounding whitespace")
                    elif not path_value.startswith("/"):
                        failures.append(f"{label}: REST route {path_field} must start with /")
                    elif _contains_control_character(path_value):
                        failures.append(f"{label}: REST route {path_field} must not contain control characters")
                    elif any(char.isspace() for char in path_value):
                        failures.append(f"{label}: REST route {path_field} must not contain whitespace")
                    else:
                        parsed_path = urllib.parse.urlsplit(path_value)
                        if parsed_path.scheme or parsed_path.netloc or parsed_path.query or parsed_path.fragment:
                            failures.append(
                                f"{label}: REST route {path_field} must be a path without authority, query, or fragment"
                            )
                    if _path_contains_dot_segment(path_value):
                        failures.append(f"{label}: REST route {path_field} must not contain dot-segments")
                for field_name, field_value in (
                    ("operation_id", route.operation_id),
                    ("sdk_alias", route.sdk_alias),
                ):
                    if field_value is None:
                        continue
                    stripped = field_value.strip()
                    if not stripped:
                        failures.append(f"{label}: REST route {field_name} must be non-empty")
                    elif field_value != stripped:
                        failures.append(f"{label}: REST route {field_name} must not include surrounding whitespace")
                    elif any(char.isspace() for char in stripped):
                        failures.append(f"{label}: REST route {field_name} must not include whitespace")
                previous = seen_openapi.get(openapi_key)
                if previous is not None:
                    failures.append(
                        f"duplicate REST route migration OpenAPI inventory entry: "
                        f"{route.method} {route.openapi_path} in {label}; already used by {previous}"
                    )
                else:
                    seen_openapi[openapi_key] = label
                previous = seen_live.get(live_key)
                if previous is not None:
                    failures.append(
                        f"duplicate REST route migration live inventory entry: "
                        f"{route.method} {route.live_path} in {label}; already used by {previous}"
                    )
                else:
                    seen_live[live_key] = label
    return failures


def _path_contains_dot_segment(path: str) -> bool:
    return any(urllib.parse.unquote(segment) in {".", ".."} for segment in path.split("/"))


def _contains_control_character(value: str) -> bool:
    return any(ord(char) < 0x20 or ord(char) == 0x7F for char in value)


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def check_openapi(swagger_path: Path = DEFAULT_SWAGGER) -> list[str]:
    swagger = _read_json(swagger_path)
    paths = swagger.get("paths") or {}
    failures: list[str] = []
    for case in ROUTE_CASES:
        for route in case.canonical:
            methods = paths.get(route.openapi_path)
            if not isinstance(methods, dict):
                failures.append(f"{case.family}: canonical route missing from OpenAPI: {route.openapi_path}")
                continue
            if route.method.lower() not in methods:
                failures.append(
                    f"{case.family}: canonical route method missing from OpenAPI: "
                    f"{route.method} {route.openapi_path}"
                )
                continue
            operation = methods.get(route.method.lower())
            if not isinstance(operation, dict):
                failures.append(
                    f"{case.family}: canonical route method is not an OpenAPI operation object: "
                    f"{route.method} {route.openapi_path}"
                )
                continue
            if route.operation_id and operation.get("operationId") != route.operation_id:
                failures.append(
                    f"{case.family}: canonical route operationId drift: "
                    f"{route.method} {route.openapi_path} expected {route.operation_id!r}, "
                    f"got {operation.get('operationId')!r}"
                )
            if route.sdk_alias and operation.get("x-udb-sdk-alias") != route.sdk_alias:
                failures.append(
                    f"{case.family}: canonical route x-udb-sdk-alias drift: "
                    f"{route.method} {route.openapi_path} expected {route.sdk_alias!r}, "
                    f"got {operation.get('x-udb-sdk-alias')!r}"
                )
        for route in case.retired:
            methods = paths.get(route.openapi_path)
            if isinstance(methods, dict) and route.method.lower() in methods:
                failures.append(f"{case.family}: retired beta route still present in OpenAPI: {route.method} {route.openapi_path}")
    return failures


def _parse_header(header: str) -> tuple[str, str]:
    if ":" not in header:
        raise ValueError(f"header must use Name: Value form, got {header!r}")
    raw_name, raw_value = header.split(":", 1)
    name = raw_name.strip()
    if raw_name != name:
        raise ValueError(f"header name must not include surrounding whitespace in {header!r}")
    value = raw_value[1:] if raw_value.startswith(" ") else raw_value
    if value != value.strip():
        raise ValueError(f"header value must not include surrounding whitespace in {header!r}")
    if not name:
        raise ValueError(f"header name is empty in {header!r}")
    if any(char not in HTTP_HEADER_NAME_CHARS for char in name):
        raise ValueError(f"header name must be an HTTP token in {header!r}")
    if not value:
        raise ValueError(f"header value is empty in {header!r}")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f"header value must not contain control characters in {header!r}")
    if len(value.encode("utf-8")) > MAX_LIVE_HEADER_VALUE_BYTES:
        raise ValueError(f"header value must be <= {MAX_LIVE_HEADER_VALUE_BYTES} bytes in {header!r}")
    return name, value


def parse_headers(header_values: list[str]) -> tuple[dict[str, str], list[str]]:
    headers: dict[str, str] = {}
    seen: set[str] = set()
    failures: list[str] = []
    if len(header_values) > MAX_LIVE_HEADER_COUNT:
        failures.append(f"live headers must be <= {MAX_LIVE_HEADER_COUNT} entries")
        return headers, failures
    for header in header_values:
        try:
            name, value = _parse_header(header)
        except ValueError as error:
            failures.append(str(error))
            continue
        canonical_name = name.lower()
        if canonical_name in seen:
            failures.append(f"duplicate live header {name!r}")
            continue
        if canonical_name in PROOF_MANAGED_REQUEST_HEADERS:
            failures.append(f"live header {name!r} is managed by the REST proof harness")
            continue
        seen.add(canonical_name)
        headers[name] = value
    return headers, failures


def _parse_live_route(value: str) -> HttpRoute:
    text = str(value)
    if text != text.strip():
        raise ValueError(f"live route must not include surrounding whitespace, got {value!r}")
    method, sep, path = text.partition(" ")
    if not sep:
        method, sep, path = text.partition(":")
    if not sep or not method.strip() or not path.strip().startswith("/"):
        raise ValueError(f"route must use 'METHOD /path' or 'METHOD:/path', got {value!r}")
    method_token = method.strip()
    if method != method_token:
        raise ValueError(f"live route method token must not include surrounding whitespace, got {value!r}")
    if method_token != method_token.upper():
        raise ValueError(f"live route method token must be uppercase, got {method_token!r}")
    if path != path.strip():
        raise ValueError(f"live route path token must not include surrounding whitespace, got {value!r}")
    normalized_method = method_token
    if normalized_method not in LIVE_BOUNDARY_HTTP_METHODS:
        allowed = ", ".join(sorted(LIVE_BOUNDARY_HTTP_METHODS))
        raise ValueError(f"live route method must be one of {allowed}, got {normalized_method!r}")
    normalized_path = path.strip()
    if _contains_control_character(normalized_path):
        raise ValueError(f"live route path must not contain control characters, got {normalized_path!r}")
    if any(char.isspace() for char in normalized_path):
        raise ValueError(f"live route path must not contain whitespace, got {normalized_path!r}")
    parsed_path = urllib.parse.urlsplit(normalized_path)
    if parsed_path.scheme or parsed_path.netloc or parsed_path.query or parsed_path.fragment:
        raise ValueError(f"live route path must be a path without authority, query, or fragment, got {normalized_path!r}")
    if _path_contains_dot_segment(normalized_path):
        raise ValueError(f"live route path must not contain dot-segments, got {normalized_path!r}")
    return HttpRoute(normalized_method, normalized_path, normalized_path)


def validate_base_url(base_url: str | None) -> list[str]:
    if not base_url:
        return []
    value = str(base_url).strip()
    failures: list[str] = []
    if value != base_url or any(char.isspace() for char in value):
        failures.append("REST live base URL must not contain whitespace")
        return failures
    if _contains_control_character(value):
        failures.append("REST live base URL must not contain control characters")
        return failures
    try:
        parsed = urllib.parse.urlparse(value)
    except ValueError as error:
        failures.append(f"REST live base URL authority is malformed: {error}")
        return failures
    if parsed.scheme not in {"http", "https"}:
        failures.append("REST live base URL must use http or https")
    try:
        hostname = parsed.hostname
        username = parsed.username
        password = parsed.password
    except ValueError as error:
        failures.append(f"REST live base URL authority is malformed: {error}")
        hostname = None
        username = None
        password = None
    if not parsed.netloc or not hostname:
        failures.append("REST live base URL must include a host")
    if username is not None or password is not None:
        failures.append("REST live base URL must not include userinfo")
    if parsed.path not in {"", "/"}:
        failures.append("REST live base URL must not include a path")
    try:
        port = parsed.port
    except ValueError as error:
        failures.append(f"REST live base URL port must be an integer from 1 to 65535: {error}")
    else:
        if port is not None and not (1 <= port <= 65535):
            failures.append("REST live base URL port must be an integer from 1 to 65535")
    if parsed.query or parsed.fragment:
        failures.append("REST live base URL must not include query or fragment")
    return failures


def normalize_timeout_seconds(timeout: float | str) -> float:
    if isinstance(timeout, str):
        raw = timeout
        stripped = raw.strip()
        if raw != stripped:
            raise ValueError("REST live timeout must not include surrounding whitespace")
        if not REST_TIMEOUT_DECIMAL_PATTERN.fullmatch(stripped):
            raise ValueError("REST live timeout must be a positive decimal number of seconds")
        parsed = float(stripped)
    else:
        parsed = float(timeout)
    if not math.isfinite(parsed):
        raise ValueError("REST live timeout must be a finite number of seconds")
    if parsed <= 0:
        raise ValueError("REST live timeout must be greater than 0 seconds")
    if parsed > MAX_LIVE_TIMEOUT_SECONDS:
        raise ValueError("REST live timeout must be <= 120 seconds")
    return parsed


def validate_timeout_seconds(timeout: float | str) -> list[str]:
    try:
        normalize_timeout_seconds(timeout)
    except (TypeError, ValueError) as error:
        return [str(error)]
    return []


def _status_for(base_url: str, route: HttpRoute, headers: dict[str, str], timeout: float) -> int:
    url = urllib.parse.urljoin(base_url.rstrip("/") + "/", route.live_path.lstrip("/"))
    body = b"{}" if route.method.upper() in {"POST", "PUT", "PATCH"} else None
    request_headers = {"Accept": "application/json", **headers}
    if body is not None:
        request_headers.setdefault("Content-Type", "application/json")
    request = urllib.request.Request(url, data=body, headers=request_headers, method=route.method.upper())
    try:
        with NO_REDIRECT_OPENER.open(request, timeout=timeout) as response:
            return int(response.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)


def _read_limited_response_body(response) -> bytes:
    body = response.read(MAX_REST_RESPONSE_BYTES + 1)
    if not isinstance(body, (bytes, bytearray)):
        raise ValueError(f"REST response body must be bytes, got {type(body).__name__}")
    if len(body) > MAX_REST_RESPONSE_BYTES:
        raise ValueError(f"REST response body must be <= {MAX_REST_RESPONSE_BYTES} bytes")
    return bytes(body)


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in pairs:
        if key in out:
            raise ValueError(f"response JSON must not contain duplicate key {key!r}")
        out[key] = value
    return out


def _reject_non_finite_json_constant(value: str) -> None:
    raise ValueError(f"response JSON must not contain non-standard constant {value}")


def _response_content_type(headers) -> str:
    try:
        values = headers.get_all("Content-Type") if hasattr(headers, "get_all") else None
    except Exception as error:
        raise ValueError(f"REST response Content-Type header could not be read: {error}") from error
    if values is None:
        try:
            value = headers.get("Content-Type", "")
        except Exception as error:
            raise ValueError(f"REST response Content-Type header could not be read: {error}") from error
        values = [value] if value else []
    if len(values) > 1:
        raise ValueError("REST response must include exactly one Content-Type header")
    if not values:
        return ""
    value = values[0]
    if not isinstance(value, str):
        raise ValueError(f"REST response Content-Type header must be a string, got {type(value).__name__}")
    return value


def _response_for(base_url: str, route: HttpRoute, headers: dict[str, str], timeout: float) -> HttpResponse:
    url = urllib.parse.urljoin(base_url.rstrip("/") + "/", route.live_path.lstrip("/"))
    body = b"{}" if route.method.upper() in {"POST", "PUT", "PATCH"} else None
    request_headers = {"Accept": JSON_MEDIA_TYPE, **headers}
    if body is not None:
        request_headers.setdefault("Content-Type", JSON_MEDIA_TYPE)
    request = urllib.request.Request(url, data=body, headers=request_headers, method=route.method.upper())
    try:
        with NO_REDIRECT_OPENER.open(request, timeout=timeout) as response:
            return HttpResponse(
                status=int(response.status),
                content_type=_response_content_type(response.headers),
                body=_read_limited_response_body(response),
            )
    except urllib.error.HTTPError as exc:
        return HttpResponse(
            status=int(exc.code),
            content_type=_response_content_type(exc.headers),
            body=_read_limited_response_body(exc),
        )


def _decode_json_response(response: HttpResponse, where: str, failures: list[str]) -> Any | None:
    if not isinstance(response.content_type, str):
        failures.append(f"{where}: Content-Type header must be a string, got {type(response.content_type).__name__}")
        return None
    if response.content_type != response.content_type.strip():
        failures.append(f"{where}: Content-Type header must not include surrounding whitespace")
        return None
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in response.content_type):
        failures.append(f"{where}: Content-Type header must not contain control characters")
        return None
    if not isinstance(response.body, (bytes, bytearray)):
        failures.append(f"{where}: response body must be bytes, got {type(response.body).__name__}")
        return None
    media_type = response.content_type.split(";", 1)[0].strip().lower()
    if media_type != JSON_MEDIA_TYPE:
        failures.append(f"{where}: Content-Type media type must be {JSON_MEDIA_TYPE}, got {response.content_type!r}")
        return None
    try:
        return json.loads(
            bytes(response.body).decode("utf-8"),
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_non_finite_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        failures.append(f"{where}: response body must be valid JSON: {exc}")
        return None


def _looks_like_success_envelope(value: Any) -> bool:
    return isinstance(value, dict) and "success" in value and ("data" in value or "error" in value)


def _is_api_error(value: Any) -> bool:
    return isinstance(value, dict) and all(field in value for field in API_ERROR_FIELDS)


def _check_api_error_public_shape(where: str, body: dict[str, Any], failures: list[str], code: str | None = None) -> None:
    extra_fields = sorted(set(body) - API_ERROR_ALLOWED_FIELDS)
    if extra_fields:
        failures.append(f"{where}: ApiError must not expose undocumented fields: {', '.join(extra_fields)}")
    message = body.get("message")
    if not isinstance(message, str) or not message.strip():
        failures.append(f"{where}: ApiError.message must be a non-empty string")
    elif message != message.strip():
        failures.append(f"{where}: ApiError.message must not include surrounding whitespace")
    elif any(ord(char) < 0x20 or ord(char) == 0x7F for char in message):
        failures.append(f"{where}: ApiError.message must not contain control characters")
    elif len(message.encode("utf-8")) > MAX_API_ERROR_STRING_BYTES:
        failures.append(f"{where}: ApiError.message must be <= {MAX_API_ERROR_STRING_BYTES} bytes")
    if not isinstance(body.get("retryable"), bool):
        failures.append(f"{where}: ApiError.retryable must be a boolean")
    field_violations = body.get("fieldViolations")
    if not isinstance(field_violations, list):
        failures.append(f"{where}: ApiError.fieldViolations must be an array")
    else:
        if code == "INVALID_ARGUMENT" and not field_violations:
            failures.append(f"{where}: ApiError.fieldViolations must be non-empty for INVALID_ARGUMENT")
        if code and code != "INVALID_ARGUMENT" and field_violations:
            failures.append(f"{where}: ApiError.fieldViolations must be empty unless ApiError.code is INVALID_ARGUMENT")
        for index, violation in enumerate(field_violations):
            if not isinstance(violation, dict):
                failures.append(f"{where}: ApiError.fieldViolations[{index}] must be an object")
                continue
            extra_violation_fields = sorted(set(violation) - API_ERROR_FIELD_VIOLATION_FIELDS)
            if extra_violation_fields:
                failures.append(
                    f"{where}: ApiError.fieldViolations[{index}] must not expose undocumented fields: "
                    f"{', '.join(extra_violation_fields)}"
                )
            field = violation.get("field")
            if not isinstance(field, str) or not field:
                failures.append(f"{where}: ApiError.fieldViolations[{index}].field must be a non-empty string")
            elif field != field.strip():
                failures.append(f"{where}: ApiError.fieldViolations[{index}].field must not include surrounding whitespace")
            elif any(char.isspace() for char in field):
                failures.append(f"{where}: ApiError.fieldViolations[{index}].field must not include whitespace")
            elif any(ord(char) < 0x20 or ord(char) == 0x7F for char in field):
                failures.append(f"{where}: ApiError.fieldViolations[{index}].field must not contain control characters")
            elif len(field.encode("utf-8")) > MAX_API_ERROR_STRING_BYTES:
                failures.append(
                    f"{where}: ApiError.fieldViolations[{index}].field must be "
                    f"<= {MAX_API_ERROR_STRING_BYTES} bytes"
                )
            description = violation.get("description")
            if not isinstance(description, str) or not description:
                failures.append(f"{where}: ApiError.fieldViolations[{index}].description must be a non-empty string")
            elif description != description.strip():
                failures.append(f"{where}: ApiError.fieldViolations[{index}].description must not include surrounding whitespace")
            elif any(ord(char) < 0x20 or ord(char) == 0x7F for char in description):
                failures.append(f"{where}: ApiError.fieldViolations[{index}].description must not contain control characters")
            elif len(description.encode("utf-8")) > MAX_API_ERROR_STRING_BYTES:
                failures.append(
                    f"{where}: ApiError.fieldViolations[{index}].description must be "
                    f"<= {MAX_API_ERROR_STRING_BYTES} bytes"
                )


def _check_route_family_error_shape(where: str, response: HttpResponse, failures: list[str]) -> None:
    body = _decode_json_response(response, where, failures)
    if not _is_api_error(body):
        failures.append(f"{where}: canonical route client error body must expose ApiError fields {', '.join(API_ERROR_FIELDS)}")
        return
    if not isinstance(body, dict):
        return
    code = body.get("code")
    if not isinstance(code, str) or not code:
        failures.append(f"{where}: ApiError.code must be a non-empty canonical error code")
    elif code != code.strip():
        failures.append(f"{where}: ApiError.code must not include surrounding whitespace")
    elif any(ord(char) < 0x20 or ord(char) == 0x7F for char in code):
        failures.append(f"{where}: ApiError.code must not contain control characters")
    elif any(char.isspace() for char in code):
        failures.append(f"{where}: ApiError.code must not include whitespace")
    elif code not in CANONICAL_GRPC_ERROR_CODES:
        failures.append(f"{where}: ApiError.code must be a canonical gRPC error code, got {code!r}")
    elif code in GRPC_HTTP_STATUS_FOR_CODE and response.status != GRPC_HTTP_STATUS_FOR_CODE[code]:
        failures.append(
            f"{where}: ApiError.code {code!r} maps to HTTP "
            f"{GRPC_HTTP_STATUS_FOR_CODE[code]}, got {response.status}"
        )
    http_status_code = body.get("httpStatusCode")
    if not isinstance(http_status_code, int) or isinstance(http_status_code, bool):
        failures.append(f"{where}: ApiError.httpStatusCode must be an integer")
    elif http_status_code != response.status:
        failures.append(
            f"{where}: ApiError.httpStatusCode {http_status_code!r} "
            f"does not match HTTP status {response.status}"
        )
    _check_api_error_public_shape(where, body, failures, code if isinstance(code, str) else None)


def check_live_boundary(
    base_url: str,
    success_route: HttpRoute | None,
    error_route: HttpRoute | None,
    expected_error_code: str | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> list[str]:
    request_headers = headers or {}
    failures: list[str] = []
    if success_route is not None:
        where = f"success boundary {success_route.method} {success_route.live_path}"
        try:
            response = _response_for(base_url, success_route, request_headers, timeout)
        except (OSError, ValueError) as exc:
            failures.append(f"{where}: request failed: {exc}")
        else:
            if response.status < 200 or response.status >= 300:
                failures.append(f"{where}: expected 2xx success, got {response.status}")
            body = _decode_json_response(response, where, failures)
            if body is not None and not isinstance(body, dict):
                failures.append(f"{where}: success body must be a bare typed JSON object")
            if isinstance(body, dict) and not body:
                failures.append(f"{where}: success body must be a non-empty typed JSON object")
            if isinstance(body, dict) and "success" in body:
                failures.append(f"{where}: success body must not expose a top-level success flag")
            if _looks_like_success_envelope(body):
                failures.append(f"{where}: success body must be the bare typed JSON body, not a success/data/error envelope")
            if _is_api_error(body):
                failures.append(f"{where}: success body must not be the ApiError shape")

    if error_route is not None:
        where = f"error boundary {error_route.method} {error_route.live_path}"
        try:
            response = _response_for(base_url, error_route, request_headers, timeout)
        except (OSError, ValueError) as exc:
            failures.append(f"{where}: request failed: {exc}")
        else:
            if response.status < 400:
                failures.append(f"{where}: expected non-2xx error, got {response.status}")
            body = _decode_json_response(response, where, failures)
            if _looks_like_success_envelope(body):
                failures.append(f"{where}: error body must be a bare ApiError, not a success/data/error envelope")
            if not _is_api_error(body):
                failures.append(f"{where}: error body must expose ApiError fields {', '.join(API_ERROR_FIELDS)}")
            elif isinstance(body, dict):
                code = body.get("code")
                if not isinstance(code, str) or not code:
                    failures.append(f"{where}: ApiError.code must be a non-empty canonical error code")
                elif code != code.strip():
                    failures.append(f"{where}: ApiError.code must not include surrounding whitespace")
                elif any(ord(char) < 0x20 or ord(char) == 0x7F for char in code):
                    failures.append(f"{where}: ApiError.code must not contain control characters")
                elif any(char.isspace() for char in code):
                    failures.append(f"{where}: ApiError.code must not include whitespace")
                elif code not in CANONICAL_GRPC_ERROR_CODES:
                    failures.append(f"{where}: ApiError.code must be a canonical gRPC error code, got {code!r}")
                elif expected_error_code and code != expected_error_code:
                    failures.append(
                        f"{where}: ApiError.code {code!r} does not match expected "
                        f"canonical code {expected_error_code!r}"
                    )
                elif code in GRPC_HTTP_STATUS_FOR_CODE and response.status != GRPC_HTTP_STATUS_FOR_CODE[code]:
                    failures.append(
                        f"{where}: ApiError.code {code!r} maps to HTTP "
                        f"{GRPC_HTTP_STATUS_FOR_CODE[code]}, got {response.status}"
                    )
                http_status_code = body.get("httpStatusCode")
                if not isinstance(http_status_code, int) or isinstance(http_status_code, bool):
                    failures.append(f"{where}: ApiError.httpStatusCode must be an integer")
                elif http_status_code != response.status:
                    failures.append(
                        f"{where}: ApiError.httpStatusCode {http_status_code!r} "
                        f"does not match HTTP status {response.status}"
                    )
                _check_api_error_public_shape(where, body, failures, code)
    return failures


def validate_boundary_inputs(success_route: str | None, error_route: str | None, expected_error_code: str | None) -> list[str]:
    failures: list[str] = []
    if bool(success_route) != bool(error_route):
        failures.append("REST boundary proof requires both --boundary-success and --boundary-error")
    parsed_success: HttpRoute | None = None
    parsed_error: HttpRoute | None = None
    if success_route:
        try:
            parsed_success = _parse_live_route(success_route)
        except ValueError as error:
            failures.append(str(error))
    if error_route:
        try:
            parsed_error = _parse_live_route(error_route)
        except ValueError as error:
            failures.append(str(error))
    if parsed_success is not None and parsed_error is not None:
        if (parsed_success.method, parsed_success.live_path) == (parsed_error.method, parsed_error.live_path):
            failures.append("REST boundary proof requires distinct success and error routes")
    if error_route:
        error_code = str(expected_error_code or "")
        stripped_error_code = error_code.strip()
        if not stripped_error_code:
            failures.append("REST boundary proof requires --boundary-error-code")
        elif error_code != stripped_error_code:
            failures.append("REST boundary proof --boundary-error-code must not include surrounding whitespace")
        elif any(ord(char) < 0x20 or ord(char) == 0x7F for char in stripped_error_code):
            failures.append("REST boundary proof --boundary-error-code must not contain control characters")
        elif any(char.isspace() for char in stripped_error_code):
            failures.append("REST boundary proof --boundary-error-code must not include whitespace")
        elif error_code not in CANONICAL_GRPC_ERROR_CODES:
            failures.append("REST boundary proof requires --boundary-error-code to be a canonical gRPC error code")
    return failures


def parse_validated_boundary_routes(
    success_route: str | None,
    error_route: str | None,
    validation_failures: list[str],
) -> tuple[HttpRoute | None, HttpRoute | None]:
    if validation_failures:
        return None, None
    parsed_success = _parse_live_route(success_route) if success_route else None
    parsed_error = _parse_live_route(error_route) if error_route else None
    return parsed_success, parsed_error


def validate_required_live_proofs(
    require_route_family_proof: bool,
    route_family_checked: bool,
    require_boundary_proof: bool,
    boundary_checked: bool,
) -> list[str]:
    failures: list[str] = []
    if require_route_family_proof and not route_family_checked:
        failures.append("--require-route-family-proof requires --base-url so canonical/retired routes are probed")
    if require_boundary_proof and not boundary_checked:
        failures.append("--require-boundary-proof requires --base-url plus --boundary-success/--boundary-error/--boundary-error-code")
    return failures


def route_inventory_counts() -> dict[str, int]:
    return {
        "route_families": len(ROUTE_CASES),
        "canonical_routes": sum(len(case.canonical) for case in ROUTE_CASES),
        "retired_routes": sum(len(case.retired) for case in ROUTE_CASES),
    }


def _base_url_evidence(base_url: str | None) -> dict[str, str] | None:
    if not base_url:
        return None
    parsed = urllib.parse.urlparse(base_url)
    return {
        "scheme": parsed.scheme,
        "authority": parsed.netloc,
    }


def _route_evidence(route: HttpRoute | None) -> dict[str, str] | None:
    if route is None:
        return None
    return {
        "method": route.method,
        "path": route.live_path,
    }


def write_evidence(
    path: Path,
    *,
    base_url: str | None,
    route_family_checked: bool,
    boundary_checked: bool,
    success_route: HttpRoute | None,
    error_route: HttpRoute | None,
    expected_error_code: str | None,
    timeout_seconds: float,
) -> None:
    route_counts = route_inventory_counts()
    evidence = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "script": "scripts/rest_route_gateway_smoke.py",
        "base_url": _base_url_evidence(base_url),
        "route_family_proof": {
            "checked": route_family_checked,
            "families_probed": route_counts["route_families"] if route_family_checked else 0,
            "canonical_routes_probed": route_counts["canonical_routes"] if route_family_checked else 0,
            "retired_routes_probed": route_counts["retired_routes"] if route_family_checked else 0,
        },
        "boundary_proof": {
            "checked": boundary_checked,
            "success_route": _route_evidence(success_route),
            "error_route": _route_evidence(error_route),
            "expected_error_code": expected_error_code if boundary_checked else None,
        },
        "timeout_seconds": timeout_seconds,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def check_live_gateway(base_url: str, headers: dict[str, str] | None = None, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> list[str]:
    request_headers = headers or {}
    failures: list[str] = []
    for case in ROUTE_CASES:
        for route in case.canonical:
            where = f"{case.family}: canonical route {route.method} {route.live_path}"
            try:
                response = _response_for(base_url, route, request_headers, timeout)
            except (OSError, ValueError) as exc:
                failures.append(f"{case.family}: canonical route request failed: {route.method} {route.live_path}: {exc}")
                continue
            status = response.status
            if status in ROUTE_MISSING_STATUSES:
                failures.append(
                    f"{case.family}: canonical route returned route-missing status "
                    f"{status}: {route.method} {route.live_path}"
                )
            if status in ROUTE_REDIRECT_STATUSES:
                failures.append(
                    f"{case.family}: canonical route returned redirect status "
                    f"{status}: {route.method} {route.live_path}"
                )
            if status in ROUTE_SERVER_ERROR_STATUSES:
                failures.append(
                    f"{case.family}: canonical route returned server-error status "
                    f"{status}: {route.method} {route.live_path}"
                )
            if status in ROUTE_NEGOTIATION_FAILURE_STATUSES:
                failures.append(
                    f"{case.family}: canonical route rejected JSON negotiation with status "
                    f"{status}: {route.method} {route.live_path}"
                )
            if status in ROUTE_NO_BODY_SUCCESS_STATUSES:
                failures.append(
                    f"{case.family}: canonical route returned no-body success status "
                    f"{status}: {route.method} {route.live_path}"
                )
            if 400 <= status < 500 and status not in ROUTE_MISSING_STATUSES and status not in ROUTE_NEGOTIATION_FAILURE_STATUSES:
                _check_route_family_error_shape(where, response, failures)
        for route in case.retired:
            try:
                status = _status_for(base_url, route, request_headers, timeout)
            except OSError as exc:
                failures.append(f"{case.family}: retired route request failed: {route.method} {route.live_path}: {exc}")
                continue
            if status not in ROUTE_MISSING_STATUSES:
                failures.append(
                    f"{case.family}: retired route is still served with status "
                    f"{status}: {route.method} {route.live_path}"
                )
    return failures


def _minimal_swagger(
    include_retired: bool = False,
    omit_first_canonical: bool = False,
    corrupt_first_identity: bool = False,
) -> dict:
    paths: dict[str, dict[str, object]] = {}
    first_canonical = True
    first_identity = True
    for case in ROUTE_CASES:
        for route in case.canonical:
            if omit_first_canonical and first_canonical:
                first_canonical = False
                continue
            first_canonical = False
            operation: dict[str, object] = {"operationId": route.operation_id or route.openapi_path}
            if route.sdk_alias:
                operation["x-udb-sdk-alias"] = route.sdk_alias
            if corrupt_first_identity and first_identity and route.operation_id and route.sdk_alias:
                operation["operationId"] = "staleOperationId"
                operation["x-udb-sdk-alias"] = "stale_alias"
                first_identity = False
            paths.setdefault(route.openapi_path, {})[route.method.lower()] = operation
        if include_retired:
            for route in case.retired:
                paths.setdefault(route.openapi_path, {})[route.method.lower()] = {"operationId": route.openapi_path}
    return {"swagger": "2.0", "paths": paths}


class _RouteTestServer(socketserver.TCPServer):
    allow_reuse_address = True


def _api_error_body(status: int, code: str | None = None) -> bytes:
    code_for_status = {
        400: "INVALID_ARGUMENT",
        401: "UNAUTHENTICATED",
        403: "PERMISSION_DENIED",
        404: "NOT_FOUND",
        409: "ALREADY_EXISTS",
        429: "RESOURCE_EXHAUSTED",
        500: "INTERNAL",
        503: "UNAVAILABLE",
        504: "DEADLINE_EXCEEDED",
    }
    resolved_code = code or code_for_status.get(status, "UNKNOWN")
    return json.dumps(
        {
            "code": resolved_code,
            "message": "route fixture",
            "httpStatusCode": status,
            "retryable": False,
            "fieldViolations": (
                [{"field": "request", "description": "invalid"}]
                if resolved_code == "INVALID_ARGUMENT"
                else []
            ),
        }
    ).encode("utf-8")


def _serve_statuses(statuses: dict[tuple[str, str], int]):
    responses = {
        key: HttpResponse(
            status=status,
            content_type=JSON_MEDIA_TYPE,
            body=(_api_error_body(status) if status >= 400 else b"{}"),
        )
        for key, status in statuses.items()
    }
    return _serve_responses(responses)


def _serve_responses(responses: dict[tuple[str, str], HttpResponse]):
    class Handler(BaseHTTPRequestHandler):
        def _send(self) -> None:
            length = int(self.headers.get("Content-Length") or "0")
            if length:
                self.rfile.read(length)
            key = (self.command.upper(), urllib.parse.urlsplit(self.path).path)
            response = responses.get(
                key,
                HttpResponse(
                    status=404,
                    content_type=JSON_MEDIA_TYPE,
                    body=json.dumps(
                        {
                            "code": "NOT_FOUND",
                            "message": "not found",
                            "httpStatusCode": 404,
                            "retryable": False,
                            "fieldViolations": [],
                        }
                    ).encode("utf-8"),
                ),
            )
            self.send_response(response.status)
            self.send_header("Content-Type", response.content_type)
            for content_type in response.extra_content_types:
                self.send_header("Content-Type", content_type)
            self.end_headers()
            self.wfile.write(response.body)

        def do_GET(self) -> None:  # noqa: N802
            self._send()

        def do_POST(self) -> None:  # noqa: N802
            self._send()

        def log_message(self, _format: str, *_args: object) -> None:
            return

    server = _RouteTestServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def _status_fixture(canonical_status: int = 401, retired_status: int = 404) -> dict[tuple[str, str], int]:
    statuses: dict[tuple[str, str], int] = {}
    for case in ROUTE_CASES:
        for route in case.canonical:
            statuses[(route.method, route.live_path)] = canonical_status
        for route in case.retired:
            statuses[(route.method, route.live_path)] = retired_status
    return statuses


def _check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def run_selftest() -> int:
    _check(not validate_route_inventory(), "current route inventory drifted")
    failures = validate_route_inventory(ROUTE_CASES[1:])
    _check(
        any("family list drifted" in failure for failure in failures),
        f"route inventory family drift was not caught: {failures}",
    )
    failures = validate_route_inventory(
        (
            RouteCase(
                ROUTE_CASES[0].family,
                ROUTE_CASES[0].canonical[:-1],
                ROUTE_CASES[0].retired,
            ),
            *ROUTE_CASES[1:],
        )
    )
    _check(
        any("REST route migration inventory must cover" in failure for failure in failures),
        f"route inventory count drift was not caught: {failures}",
    )
    duplicate_canonical = (ROUTE_CASES[0].canonical[0], *ROUTE_CASES[0].canonical[:-1])
    failures = validate_route_inventory(
        (
            RouteCase(
                ROUTE_CASES[0].family,
                duplicate_canonical,
                ROUTE_CASES[0].retired,
            ),
            *ROUTE_CASES[1:],
        )
    )
    _check(
        any("duplicate REST route migration OpenAPI inventory entry" in failure for failure in failures)
        and any("duplicate REST route migration live inventory entry" in failure for failure in failures),
        f"route inventory duplicate was not caught: {failures}",
    )
    dot_segment_route = HttpRoute(
        ROUTE_CASES[0].canonical[0].method,
        "/v1/api-keys/../users",
        "/v1/api-keys/%2e%2e/users",
    )
    failures = validate_route_inventory(
        (
            RouteCase(
                ROUTE_CASES[0].family,
                (dot_segment_route, *ROUTE_CASES[0].canonical[1:]),
                ROUTE_CASES[0].retired,
            ),
            *ROUTE_CASES[1:],
        )
    )
    _check(
        len([failure for failure in failures if "REST route " in failure and "must not contain dot-segments" in failure]) == 2,
        f"route inventory dot-segment was not caught: {failures}",
    )
    malformed_path_route = HttpRoute(
        ROUTE_CASES[0].canonical[0].method,
        "/v1/api-keys?debug=true",
        "//example.com/v1/api-keys",
    )
    failures = validate_route_inventory(
        (
            RouteCase(
                ROUTE_CASES[0].family,
                (malformed_path_route, *ROUTE_CASES[0].canonical[1:]),
                ROUTE_CASES[0].retired,
            ),
            *ROUTE_CASES[1:],
        )
    )
    _check(
        len(
            [
                failure
                for failure in failures
                if "REST route " in failure and "without authority, query, or fragment" in failure
            ]
        )
        == 2,
        f"route inventory query/authority path was not caught: {failures}",
    )
    whitespace_path_route = HttpRoute(
        ROUTE_CASES[0].canonical[0].method,
        " /v1/api-keys",
        "/v1/api keys",
    )
    failures = validate_route_inventory(
        (
            RouteCase(
                ROUTE_CASES[0].family,
                (whitespace_path_route, *ROUTE_CASES[0].canonical[1:]),
                ROUTE_CASES[0].retired,
            ),
            *ROUTE_CASES[1:],
        )
    )
    _check(
        any("REST route openapi_path must not include surrounding whitespace" in failure for failure in failures)
        and any("REST route live_path must not contain whitespace" in failure for failure in failures),
        f"route inventory whitespace path was not caught: {failures}",
    )
    lowercase_method_route = HttpRoute(
        "get",
        ROUTE_CASES[0].canonical[0].openapi_path,
        ROUTE_CASES[0].canonical[0].live_path,
    )
    failures = validate_route_inventory(
        (
            RouteCase(
                ROUTE_CASES[0].family,
                (lowercase_method_route, *ROUTE_CASES[0].canonical[1:]),
                ROUTE_CASES[0].retired,
            ),
            *ROUTE_CASES[1:],
        )
    )
    _check(
        any("REST route method must be uppercase" in failure for failure in failures),
        f"route inventory lowercase method was not caught: {failures}",
    )
    unsupported_method_route = HttpRoute(
        "TRACE",
        ROUTE_CASES[0].canonical[0].openapi_path,
        ROUTE_CASES[0].canonical[0].live_path,
    )
    failures = validate_route_inventory(
        (
            RouteCase(
                ROUTE_CASES[0].family,
                (unsupported_method_route, *ROUTE_CASES[0].canonical[1:]),
                ROUTE_CASES[0].retired,
            ),
            *ROUTE_CASES[1:],
        )
    )
    _check(
        any("REST route method must be one of" in failure for failure in failures),
        f"route inventory unsupported method was not caught: {failures}",
    )
    bad_operation_route = HttpRoute(
        ROUTE_CASES[0].canonical[0].method,
        ROUTE_CASES[0].canonical[0].openapi_path,
        ROUTE_CASES[0].canonical[0].live_path,
        operation_id=" downloadFile ",
    )
    failures = validate_route_inventory(
        (
            RouteCase(
                ROUTE_CASES[0].family,
                (bad_operation_route, *ROUTE_CASES[0].canonical[1:]),
                ROUTE_CASES[0].retired,
            ),
            *ROUTE_CASES[1:],
        )
    )
    _check(
        any("REST route operation_id must not include surrounding whitespace" in failure for failure in failures),
        f"route inventory spaced operation_id was not caught: {failures}",
    )
    bad_alias_route = HttpRoute(
        ROUTE_CASES[0].canonical[0].method,
        ROUTE_CASES[0].canonical[0].openapi_path,
        ROUTE_CASES[0].canonical[0].live_path,
        sdk_alias="download file",
    )
    failures = validate_route_inventory(
        (
            RouteCase(
                ROUTE_CASES[0].family,
                (bad_alias_route, *ROUTE_CASES[0].canonical[1:]),
                ROUTE_CASES[0].retired,
            ),
            *ROUTE_CASES[1:],
        )
    )
    _check(
        any("REST route sdk_alias must not include whitespace" in failure for failure in failures),
        f"route inventory embedded-whitespace sdk_alias was not caught: {failures}",
    )

    with tempfile.TemporaryDirectory() as tmp:
        clean_swagger = Path(tmp) / "clean.json"
        clean_swagger.write_text(json.dumps(_minimal_swagger()), encoding="utf-8")
        _check(not check_openapi(clean_swagger), "clean OpenAPI route fixture failed")

        missing_swagger = Path(tmp) / "missing.json"
        missing_swagger.write_text(json.dumps(_minimal_swagger(omit_first_canonical=True)), encoding="utf-8")
        failures = check_openapi(missing_swagger)
        _check(any("canonical route missing from OpenAPI" in failure for failure in failures), f"missing canonical route was not caught: {failures}")

        retired_swagger = Path(tmp) / "retired.json"
        retired_swagger.write_text(json.dumps(_minimal_swagger(include_retired=True)), encoding="utf-8")
        failures = check_openapi(retired_swagger)
        _check(any("retired beta route still present in OpenAPI" in failure for failure in failures), f"retired OpenAPI route was not caught: {failures}")

        stale_identity_swagger = Path(tmp) / "stale-identity.json"
        stale_identity_swagger.write_text(json.dumps(_minimal_swagger(corrupt_first_identity=True)), encoding="utf-8")
        failures = check_openapi(stale_identity_swagger)
        _check(any("canonical route operationId drift" in failure for failure in failures), f"stale operationId was not caught: {failures}")
        _check(any("canonical route x-udb-sdk-alias drift" in failure for failure in failures), f"stale SDK alias was not caught: {failures}")

    server = _serve_statuses(_status_fixture())
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        _check(not check_live_gateway(base_url), "clean live route fixture failed")
    finally:
        server.shutdown()
        server.server_close()

    statuses = _status_fixture()
    first = ROUTE_CASES[0].canonical[0]
    responses = {
        key: HttpResponse(
            status=status,
            content_type=JSON_MEDIA_TYPE,
            body=(_api_error_body(status) if status >= 400 else b"{}"),
        )
        for key, status in statuses.items()
    }
    responses[(first.method, first.live_path)] = HttpResponse(
        status=401,
        content_type="text/plain",
        body=b"unauthenticated",
    )
    server = _serve_responses(responses)
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_gateway(base_url)
        _check(
            any("Content-Type media type must be application/json" in failure for failure in failures),
            f"route-family non-JSON client error was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    statuses = _status_fixture()
    responses = {
        key: HttpResponse(
            status=status,
            content_type=JSON_MEDIA_TYPE,
            body=(_api_error_body(status) if status >= 400 else b"{}"),
        )
        for key, status in statuses.items()
    }
    responses[(first.method, first.live_path)] = HttpResponse(
        status=401,
        content_type=JSON_MEDIA_TYPE,
        body=json.dumps({"code": "UNAUTHENTICATED"}).encode("utf-8"),
    )
    server = _serve_responses(responses)
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_gateway(base_url)
        _check(
            any("canonical route client error body must expose ApiError fields" in failure for failure in failures),
            f"route-family incomplete ApiError body was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    statuses = _status_fixture()
    first = ROUTE_CASES[0].canonical[0]
    statuses[(first.method, first.live_path)] = 404
    server = _serve_statuses(statuses)
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_gateway(base_url)
        _check(any("canonical route returned route-missing status" in failure for failure in failures), f"missing live route was not caught: {failures}")
    finally:
        server.shutdown()
        server.server_close()

    statuses = _status_fixture()
    first = ROUTE_CASES[0].canonical[0]
    statuses[(first.method, first.live_path)] = 302
    server = _serve_statuses(statuses)
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_gateway(base_url)
        _check(
            any("canonical route returned redirect status" in failure for failure in failures),
            f"redirect live route was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    statuses = _status_fixture()
    first = ROUTE_CASES[0].canonical[0]
    statuses[(first.method, first.live_path)] = 500
    server = _serve_statuses(statuses)
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_gateway(base_url)
        _check(
            any("canonical route returned server-error status" in failure for failure in failures),
            f"server-error live route was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    statuses = _status_fixture()
    first = ROUTE_CASES[0].canonical[0]
    statuses[(first.method, first.live_path)] = 415
    server = _serve_statuses(statuses)
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_gateway(base_url)
        _check(
            any("canonical route rejected JSON negotiation" in failure for failure in failures),
            f"negotiation-failure live route was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    statuses = _status_fixture()
    first = ROUTE_CASES[0].canonical[0]
    statuses[(first.method, first.live_path)] = 204
    server = _serve_statuses(statuses)
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_gateway(base_url)
        _check(
            any("canonical route returned no-body success status" in failure for failure in failures),
            f"no-body success live route was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    statuses = _status_fixture()
    first_retired = ROUTE_CASES[0].retired[0]
    statuses[(first_retired.method, first_retired.live_path)] = 401
    server = _serve_statuses(statuses)
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_gateway(base_url)
        _check(any("retired route is still served" in failure for failure in failures), f"served retired route was not caught: {failures}")
    finally:
        server.shutdown()
        server.server_close()

    success = HttpRoute("GET", "/v1/smoke/success", "/v1/smoke/success")
    error = HttpRoute("GET", "/v1/smoke/error", "/v1/smoke/error")
    api_error_body = json.dumps(
        {
            "code": "NOT_FOUND",
            "message": "missing smoke resource",
            "httpStatusCode": 404,
            "retryable": False,
            "fieldViolations": [],
        }
    ).encode("utf-8")
    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=api_error_body,
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        _check(
            not check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND"),
            "clean REST boundary fixture failed",
        )
    finally:
        server.shutdown()
        server.server_close()

    oversized_body = b" " * (MAX_REST_RESPONSE_BYTES + 1)
    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=oversized_body,
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=api_error_body,
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND")
        _check(
            any("REST response body must be <=" in failure for failure in failures),
            f"oversized success response body was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    class _NonBytesResponseBody:
        def read(self, _limit: int):
            return "not-bytes"

    try:
        _read_limited_response_body(_NonBytesResponseBody())
    except ValueError as exc:
        _check(
            "REST response body must be bytes" in str(exc),
            f"non-bytes response body failure was wrong: {exc}",
        )
    else:
        raise AssertionError("non-bytes response body was not caught")

    failures = []
    _decode_json_response(HttpResponse(status=200, content_type=object(), body=b"{}"), "direct decoder", failures)  # type: ignore[arg-type]
    _check(
        any("Content-Type header must be a string" in failure for failure in failures),
        f"direct decoder non-string Content-Type was not caught: {failures}",
    )
    failures = []
    _decode_json_response(HttpResponse(status=200, content_type=f" {JSON_MEDIA_TYPE} ", body=b"{}"), "direct decoder", failures)
    _check(
        any("Content-Type header must not include surrounding whitespace" in failure for failure in failures),
        f"direct decoder padded Content-Type was not caught: {failures}",
    )
    failures = []
    _decode_json_response(HttpResponse(status=200, content_type=f"{JSON_MEDIA_TYPE}; charset=utf-8\x01", body=b"{}"), "direct decoder", failures)
    _check(
        any("Content-Type header must not contain control characters" in failure for failure in failures),
        f"direct decoder control-character Content-Type was not caught: {failures}",
    )
    failures = []
    _decode_json_response(HttpResponse(status=200, content_type=JSON_MEDIA_TYPE, body="{}"), "direct decoder", failures)  # type: ignore[arg-type]
    _check(
        any("response body must be bytes" in failure for failure in failures),
        f"direct decoder non-bytes response body was not caught: {failures}",
    )

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=b'{"resourceId": NaN}',
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=api_error_body,
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error)
        _check(
            any("response JSON must not contain non-standard constant NaN" in failure for failure in failures),
            f"non-standard JSON constant success response body was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": "PERMISSION_DENIED",
                        "message": "forbidden smoke resource",
                        "httpStatusCode": 404,
                        "retryable": False,
                        "fieldViolations": [],
                    }
                ).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="PERMISSION_DENIED")
        _check(
            any("ApiError.code 'PERMISSION_DENIED' maps to HTTP 403, got 404" in failure for failure in failures),
            f"wrong ApiError code/status mapping was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"success": True}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=api_error_body,
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND")
        _check(
            any("success body must not expose a top-level success flag" in failure for failure in failures),
            f"success flag body was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": "NOT_FOUND",
                        "message": "missing smoke resource",
                        "httpStatusCode": True,
                        "retryable": False,
                        "fieldViolations": [],
                    }
                ).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND")
        _check(
            any("ApiError.httpStatusCode must be an integer" in failure for failure in failures),
            f"boolean ApiError httpStatusCode was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=400,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": "INVALID_ARGUMENT",
                        "message": "invalid smoke request",
                        "httpStatusCode": 400,
                        "retryable": False,
                        "fieldViolations": [
                            {"field": "tenant\u0000id", "description": "required"},
                            {"field": "f" * (MAX_API_ERROR_STRING_BYTES + 1), "description": "required"},
                        ],
                    }
                ).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="INVALID_ARGUMENT")
        _check(
            any("ApiError.fieldViolations[0].field must not contain control characters" in failure for failure in failures)
            and any("ApiError.fieldViolations[1].field must be <= 8192 bytes" in failure for failure in failures),
            f"invalid ApiError fieldViolations field tokens were not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": "NOT_FOUND",
                        "message": "missing smoke resource\nx-injected: yes",
                        "httpStatusCode": 404,
                        "retryable": False,
                        "fieldViolations": [],
                    }
                ).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND")
        _check(
            any("ApiError.message must not contain control characters" in failure for failure in failures),
            f"control-character ApiError message was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": "NOT_FOUND",
                        "message": "missing smoke resource",
                        "httpStatusCode": 404,
                        "retryable": False,
                        "fieldViolations": [],
                        "backend": "postgres",
                    }
                ).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND")
        _check(
            any("ApiError must not expose undocumented fields" in failure for failure in failures),
            f"undocumented ApiError field was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=400,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": "INVALID_ARGUMENT",
                        "message": "invalid smoke request",
                        "httpStatusCode": 400,
                        "retryable": False,
                        "fieldViolations": [
                            {
                                "field": "tenant_id",
                                "description": "a" * (MAX_API_ERROR_STRING_BYTES + 1),
                            }
                        ],
                    }
                ).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="INVALID_ARGUMENT")
        _check(
            any(
                "ApiError.fieldViolations[0].description must be <= 8192 bytes" in failure
                for failure in failures
            ),
            f"oversized ApiError fieldViolations description was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": " NOT_FOUND ",
                        "message": "missing smoke resource",
                        "httpStatusCode": 404,
                        "retryable": False,
                        "fieldViolations": [],
                    }
                ).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND")
        _check(
            any("ApiError.code must not include surrounding whitespace" in failure for failure in failures),
            f"spaced ApiError code was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=400,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": "INVALID_ARGUMENT",
                        "message": "invalid smoke request",
                        "httpStatusCode": 400,
                        "retryable": False,
                        "fieldViolations": [
                            {"field": "tenant_id", "description": "required", "reason": "missing"}
                        ],
                    }
                ).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="INVALID_ARGUMENT")
        _check(
            any("ApiError.fieldViolations[0] must not expose undocumented fields" in failure for failure in failures),
            f"undocumented ApiError fieldViolations entry field was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": "NOT_FOUND",
                        "message": "missing smoke resource",
                        "httpStatusCode": 404.0,
                        "retryable": False,
                        "fieldViolations": [],
                    }
                ).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND")
        _check(
            any("ApiError.httpStatusCode must be an integer" in failure for failure in failures),
            f"non-integer ApiError httpStatusCode was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": "NOT_FOUND",
                        "message": " missing smoke resource ",
                        "httpStatusCode": 404,
                        "retryable": False,
                        "fieldViolations": [],
                    }
                ).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND")
        _check(
            any("ApiError.message must not include surrounding whitespace" in failure for failure in failures),
            f"spaced ApiError message was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=400,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": "INVALID_ARGUMENT",
                        "message": "invalid smoke request",
                        "httpStatusCode": 400,
                        "retryable": False,
                        "fieldViolations": [
                            {"field": " tenant_id ", "description": "required"},
                            {"field": "tenant id", "description": "required"},
                            {"field": "project_id", "description": " required "},
                        ],
                    }
                ).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="INVALID_ARGUMENT")
        _check(
            any("ApiError.fieldViolations[0].field must not include surrounding whitespace" in failure for failure in failures)
            and any("ApiError.fieldViolations[1].field must not include whitespace" in failure for failure in failures)
            and any("ApiError.fieldViolations[2].description must not include surrounding whitespace" in failure for failure in failures),
            f"whitespace ApiError fieldViolations entries were not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=b'{"resourceId":"one","resourceId":"two"}',
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=api_error_body,
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND")
        _check(
            any("response JSON must not contain duplicate key" in failure for failure in failures),
            f"duplicate-key success response body was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=(
                    b'{"code":"UNKNOWN","code":"NOT_FOUND","message":"missing smoke resource",'
                    b'"httpStatusCode":404,"retryable":false,"fieldViolations":[]}'
                ),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND")
        _check(
            any("response JSON must not contain duplicate key" in failure for failure in failures),
            f"duplicate-key error response body was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=oversized_body,
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND")
        _check(
            any("REST response body must be <=" in failure for failure in failures),
            f"oversized error response body was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": "not_found",
                        "message": "missing smoke resource",
                        "httpStatusCode": 404,
                        "retryable": False,
                        "fieldViolations": [],
                    }
                ).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error)
        _check(
            any("ApiError.code must be a canonical gRPC error code" in failure for failure in failures),
            f"non-canonical ApiError code was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": "NOT_FOUND\0",
                        "message": "missing smoke resource",
                        "httpStatusCode": 404,
                        "retryable": False,
                        "fieldViolations": [],
                    }
                ).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error)
        _check(
            any("ApiError.code must not contain control characters" in failure for failure in failures),
            f"control-character ApiError code was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=api_error_body,
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND")
        _check(
            any("success body must be a non-empty typed JSON object" in failure for failure in failures),
            f"empty success object body was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(["not", "a", "typed", "object"]).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=api_error_body,
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND")
        _check(
            any("success body must be a bare typed JSON object" in failure for failure in failures),
            f"success non-object JSON body was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": "OK_SHAPED_AS_ERROR",
                        "message": "success leaked an error envelope",
                        "httpStatusCode": 200,
                        "retryable": False,
                        "fieldViolations": [],
                    }
                ).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=api_error_body,
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND")
        _check(
            any("success body must not be the ApiError shape" in failure for failure in failures),
            f"success ApiError-shaped body was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=400,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": "INVALID_ARGUMENT",
                        "message": "invalid smoke request",
                        "httpStatusCode": 400,
                        "retryable": False,
                        "fieldViolations": [],
                    }
                ).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="INVALID_ARGUMENT")
        _check(
            any("ApiError.fieldViolations must be non-empty for INVALID_ARGUMENT" in failure for failure in failures),
            f"empty INVALID_ARGUMENT fieldViolations were not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": "NOT_FOUND",
                        "message": "missing smoke resource",
                        "httpStatusCode": 404,
                        "retryable": False,
                        "fieldViolations": [{"field": "tenant_id", "description": "should be validation-only"}],
                    }
                ).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND")
        _check(
            any("ApiError.fieldViolations must be empty unless ApiError.code is INVALID_ARGUMENT" in failure for failure in failures),
            f"non-validation ApiError fieldViolations were not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": "NOT_FOUND",
                        "message": "",
                        "httpStatusCode": 404,
                        "retryable": "false",
                        "fieldViolations": {},
                    }
                ).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND")
        _check(
            any("ApiError.retryable must be a boolean" in failure for failure in failures)
            and any("ApiError.fieldViolations must be an array" in failure for failure in failures)
            and any("ApiError.message must be a non-empty string" in failure for failure in failures),
            f"malformed ApiError public fields were not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=400,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": "INVALID_ARGUMENT",
                        "message": "invalid smoke request",
                        "httpStatusCode": 400,
                        "retryable": False,
                        "fieldViolations": [
                            {"field": "tenant_id", "description": "required"},
                            {"field": "", "description": "malformed"},
                            "not-an-object",
                        ],
                    }
                ).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="INVALID_ARGUMENT")
        _check(
            any("ApiError.fieldViolations[1].field must be a non-empty string" in failure for failure in failures)
            and any("ApiError.fieldViolations[2] must be an object" in failure for failure in failures),
            f"malformed ApiError fieldViolations entries were not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": "NOT_FOUND",
                        "message": "missing smoke resource",
                        "httpStatusCode": 500,
                        "retryable": False,
                        "fieldViolations": [],
                    }
                ).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND")
        _check(
            any("does not match HTTP status" in failure for failure in failures),
            f"wrong ApiError httpStatusCode was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps(
                    {
                        "code": "PERMISSION_DENIED",
                        "message": "missing smoke resource",
                        "httpStatusCode": 404,
                        "retryable": False,
                        "fieldViolations": [],
                    }
                ).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND")
        _check(
            any("does not match expected canonical code" in failure for failure in failures),
            f"wrong ApiError code was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"success": True, "data": {"resourceId": "smoke"}}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=api_error_body,
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error)
        _check(
            any("bare typed JSON body" in failure for failure in failures),
            f"success envelope response was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=json.dumps({"code": "NOT_FOUND", "message": "missing"}).encode("utf-8"),
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error)
        _check(
            any("error body must expose ApiError fields" in failure for failure in failures),
            f"incomplete ApiError body was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=f"text/plain; note={JSON_MEDIA_TYPE}",
                body=b"ok",
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=api_error_body,
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error)
        _check(
            any("Content-Type media type must be application/json" in failure for failure in failures),
            f"misleading non-JSON success Content-Type was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    server = _serve_responses(
        {
            ("GET", success.live_path): HttpResponse(
                status=200,
                content_type=JSON_MEDIA_TYPE,
                extra_content_types=("text/plain",),
                body=json.dumps({"resourceId": "smoke"}).encode("utf-8"),
            ),
            ("GET", error.live_path): HttpResponse(
                status=404,
                content_type=JSON_MEDIA_TYPE,
                body=api_error_body,
            ),
        }
    )
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        failures = check_live_boundary(base_url, success, error, expected_error_code="NOT_FOUND")
        _check(
            any("REST response must include exactly one Content-Type header" in failure for failure in failures),
            f"duplicate Content-Type response was not caught: {failures}",
        )
    finally:
        server.shutdown()
        server.server_close()

    class _NonStringContentTypeHeaders:
        def get_all(self, _name: str):
            return [object()]

    try:
        _response_content_type(_NonStringContentTypeHeaders())
    except ValueError as exc:
        _check(
            "REST response Content-Type header must be a string" in str(exc),
            f"non-string Content-Type response failure was wrong: {exc}",
        )
    else:
        raise AssertionError("non-string Content-Type response was not caught")

    class _ThrowingContentTypeHeaders:
        def get_all(self, _name: str):
            raise RuntimeError("headers unavailable")

    try:
        _response_content_type(_ThrowingContentTypeHeaders())
    except ValueError as exc:
        _check(
            "REST response Content-Type header could not be read" in str(exc),
            f"unreadable Content-Type response failure was wrong: {exc}",
        )
    else:
        raise AssertionError("unreadable Content-Type response was not caught")

    failures = validate_boundary_inputs("GET /v1/smoke/success", None, "NOT_FOUND")
    _check(
        any("requires both --boundary-success and --boundary-error" in failure for failure in failures),
        f"partial REST boundary proof was not caught: {failures}",
    )
    failures = validate_required_live_proofs(False, False, True, False)
    _check(
        any("--require-boundary-proof requires --base-url" in failure for failure in failures),
        f"missing required REST boundary proof was not caught: {failures}",
    )
    failures = validate_required_live_proofs(True, False, True, True)
    _check(
        any("--require-route-family-proof requires --base-url" in failure for failure in failures),
        f"missing required REST route-family proof was not caught: {failures}",
    )
    _check(
        not validate_required_live_proofs(True, True, True, True),
        "complete required REST live proof was rejected",
    )
    failures = validate_boundary_inputs("GET /v1/smoke/success", "GET /v1/smoke/error", "")
    _check(
        any("requires --boundary-error-code" in failure for failure in failures),
        f"missing REST boundary error-code proof was not caught: {failures}",
    )
    failures = validate_boundary_inputs("GET /v1/smoke/success", "GET /v1/smoke/error", "not_found")
    _check(
        any("canonical gRPC error code" in failure for failure in failures),
        f"non-canonical REST boundary error-code proof was not caught: {failures}",
    )
    failures = validate_boundary_inputs("GET /v1/smoke/success", "GET /v1/smoke/error", " NOT_FOUND ")
    _check(
        any("must not include surrounding whitespace" in failure for failure in failures),
        f"spaced REST boundary error-code proof was not caught: {failures}",
    )
    failures = validate_boundary_inputs("GET /v1/smoke/success", "GET /v1/smoke/error", "NOT FOUND")
    _check(
        any("must not include whitespace" in failure for failure in failures),
        f"embedded-whitespace REST boundary error-code proof was not caught: {failures}",
    )
    failures = validate_boundary_inputs("GET /v1/smoke/success", "GET /v1/smoke/error", "NOT_FOUND\0")
    _check(
        any("must not contain control characters" in failure for failure in failures),
        f"control-character REST boundary error-code proof was not caught: {failures}",
    )
    failures = validate_boundary_inputs("GET /v1/smoke/same", "GET /v1/smoke/same", "NOT_FOUND")
    _check(
        any("requires distinct success and error routes" in failure for failure in failures),
        f"same REST boundary route proof was not caught: {failures}",
    )
    malformed_boundary_failures = validate_boundary_inputs(" GET /v1/smoke/success", "GET /v1/smoke/error", "NOT_FOUND")
    try:
        parsed_success, parsed_error = parse_validated_boundary_routes(
            " GET /v1/smoke/success",
            "GET /v1/smoke/error",
            malformed_boundary_failures,
        )
    except ValueError as error:
        raise AssertionError(f"invalid REST boundary proof reparsed after validation failure: {error}") from error
    _check(
        parsed_success is None and parsed_error is None,
        "invalid REST boundary proof should not be reparsed after validation failure",
    )
    try:
        _parse_live_route(" GET /v1/smoke/success")
    except ValueError as error:
        _check(
            "live route must not include surrounding whitespace" in str(error),
            f"surrounding-whitespace REST boundary route error was wrong: {error}",
        )
    else:
        raise AssertionError("surrounding-whitespace REST boundary route was not caught")
    try:
        _parse_live_route("GET  /v1/smoke/success")
    except ValueError as error:
        _check(
            "live route path token must not include surrounding whitespace" in str(error),
            f"extra-separator REST boundary route error was wrong: {error}",
        )
    else:
        raise AssertionError("extra-separator REST boundary route was not caught")
    try:
        _parse_live_route("GET\t:/v1/smoke/success")
    except ValueError as error:
        _check(
            "live route method token must not include surrounding whitespace" in str(error),
            f"spaced-method REST boundary route error was wrong: {error}",
        )
    else:
        raise AssertionError("spaced-method REST boundary route was not caught")
    try:
        _parse_live_route("get /v1/smoke/success")
    except ValueError as error:
        _check(
            "live route method token must be uppercase" in str(error),
            f"lowercase-method REST boundary route error was wrong: {error}",
        )
    else:
        raise AssertionError("lowercase-method REST boundary route was not caught")
    try:
        _parse_live_route("GET /v1/smoke/bad path")
    except ValueError as error:
        _check(
            "live route path must not contain whitespace" in str(error),
            f"whitespace REST boundary path error was wrong: {error}",
        )
    else:
        raise AssertionError("whitespace REST boundary path was not caught")
    try:
        _parse_live_route("GET /v1/smoke/\0success")
    except ValueError as error:
        _check(
            "live route path must not contain control characters" in str(error),
            f"control-character REST boundary path error was wrong: {error}",
        )
    else:
        raise AssertionError("control-character REST boundary path was not caught")
    try:
        _parse_live_route("GET /v1/smoke/success?debug=true")
    except ValueError as error:
        _check(
            "without authority, query, or fragment" in str(error),
            f"query REST boundary path error was wrong: {error}",
        )
    else:
        raise AssertionError("query REST boundary path was not caught")
    try:
        _parse_live_route("GET //example.com/v1/smoke/success")
    except ValueError as error:
        _check(
            "without authority, query, or fragment" in str(error),
            f"authority REST boundary path error was wrong: {error}",
        )
    else:
        raise AssertionError("authority REST boundary path was not caught")
    try:
        _parse_live_route("GET /v1/smoke/../admin")
    except ValueError as error:
        _check(
            "live route path must not contain dot-segments" in str(error),
            f"dot-segment REST boundary path error was wrong: {error}",
        )
    else:
        raise AssertionError("dot-segment REST boundary path was not caught")
    try:
        _parse_live_route("GET /v1/smoke/%2e%2e/admin")
    except ValueError as error:
        _check(
            "live route path must not contain dot-segments" in str(error),
            f"encoded dot-segment REST boundary path error was wrong: {error}",
        )
    else:
        raise AssertionError("encoded dot-segment REST boundary path was not caught")
    failures = validate_base_url("ftp://127.0.0.1:8080")
    _check(any("must use http or https" in failure for failure in failures), f"non-HTTP REST base URL was not caught: {failures}")
    failures = validate_base_url("http://127.0.0.1:8080?probe=1")
    _check(any("must not include query or fragment" in failure for failure in failures), f"query REST base URL was not caught: {failures}")
    failures = validate_base_url(" http://127.0.0.1:8080 ")
    _check(any("must not contain whitespace" in failure for failure in failures), f"whitespace REST base URL was not caught: {failures}")
    failures = validate_base_url("http://127.0.0.1:8080\0")
    _check(
        any("must not contain control characters" in failure for failure in failures),
        f"control-character REST base URL was not caught: {failures}",
    )
    failures = validate_base_url("http:///missing-host")
    _check(any("must include a host" in failure for failure in failures), f"hostless REST base URL was not caught: {failures}")
    failures = validate_base_url("http://:8080")
    _check(any("must include a host" in failure for failure in failures), f"empty-host REST base URL was not caught: {failures}")
    failures = validate_base_url("http://[::1")
    _check(
        any("authority is malformed" in failure for failure in failures),
        f"malformed REST base URL authority was not caught: {failures}",
    )
    failures = validate_base_url("http://127.0.0.1:not-a-port")
    _check(any("port must be an integer from 1 to 65535" in failure for failure in failures), f"non-integer REST base URL port was not caught: {failures}")
    failures = validate_base_url("http://127.0.0.1:70000")
    _check(any("port must be an integer from 1 to 65535" in failure for failure in failures), f"out-of-range REST base URL port was not caught: {failures}")
    failures = validate_base_url("http://user:pass@127.0.0.1:8080")
    _check(any("must not include userinfo" in failure for failure in failures), f"userinfo REST base URL was not caught: {failures}")
    failures = validate_base_url("http://127.0.0.1:8080/api")
    _check(any("must not include a path" in failure for failure in failures), f"path-prefixed REST base URL was not caught: {failures}")
    _check(not validate_timeout_seconds(DEFAULT_TIMEOUT_SECONDS), "valid REST timeout was rejected")
    _check(normalize_timeout_seconds(str(DEFAULT_TIMEOUT_SECONDS)) == DEFAULT_TIMEOUT_SECONDS, "canonical REST timeout string was rejected")
    failures = validate_timeout_seconds(" 5 ")
    _check(
        any("must not include surrounding whitespace" in failure for failure in failures),
        f"padded REST timeout was not caught: {failures}",
    )
    failures = validate_timeout_seconds("1e2")
    _check(
        any("positive decimal number" in failure for failure in failures),
        f"non-decimal REST timeout was not caught: {failures}",
    )
    failures = validate_timeout_seconds(0.0)
    _check(any("greater than 0" in failure for failure in failures), f"non-positive REST timeout was not caught: {failures}")
    failures = validate_timeout_seconds(float("inf"))
    _check(any("finite" in failure for failure in failures), f"infinite REST timeout was not caught: {failures}")
    failures = validate_timeout_seconds(MAX_LIVE_TIMEOUT_SECONDS + 1)
    _check(any("<= 120 seconds" in failure for failure in failures), f"excessive REST timeout was not caught: {failures}")
    try:
        _parse_header("Authorization:")
    except ValueError as error:
        _check("header value is empty" in str(error), f"empty REST header value error was wrong: {error}")
    else:
        raise AssertionError("empty REST header value was not caught")
    try:
        _parse_header("Bad Header: value")
    except ValueError as error:
        _check("header name must be an HTTP token" in str(error), f"malformed REST header name error was wrong: {error}")
    else:
        raise AssertionError("malformed REST header name was not caught")
    try:
        _parse_header(" Authorization: Bearer one")
    except ValueError as error:
        _check(
            "header name must not include surrounding whitespace" in str(error),
            f"spaced REST header name error was wrong: {error}",
        )
    else:
        raise AssertionError("spaced REST header name was not caught")
    try:
        _parse_header("Authorization:  Bearer one ")
    except ValueError as error:
        _check(
            "header value must not include surrounding whitespace" in str(error),
            f"spaced REST header value error was wrong: {error}",
        )
    else:
        raise AssertionError("spaced REST header value was not caught")
    try:
        _parse_header("Authorization: Bearer one\r\nX-Injected: yes")
    except ValueError as error:
        _check(
            "header value must not contain control characters" in str(error),
            f"control-character REST header value error was wrong: {error}",
        )
    else:
        raise AssertionError("control-character REST header value was not caught")
    try:
        _parse_header("Authorization: " + ("a" * (MAX_LIVE_HEADER_VALUE_BYTES + 1)))
    except ValueError as error:
        _check("header value must be <=" in str(error), f"oversized REST header value error was wrong: {error}")
    else:
        raise AssertionError("oversized REST header value was not caught")
    _, failures = parse_headers([f"X-Proof-{index}: value" for index in range(MAX_LIVE_HEADER_COUNT + 1)])
    _check(any("live headers must be <=" in failure for failure in failures), f"excessive REST header count was not caught: {failures}")
    _, failures = parse_headers(["Authorization: Bearer one", "authorization: Bearer two"])
    _check(any("duplicate live header" in failure for failure in failures), f"duplicate REST header was not caught: {failures}")
    _, failures = parse_headers(["Accept: text/plain"])
    _check(any("managed by the REST proof harness" in failure for failure in failures), f"Accept override REST header was not caught: {failures}")
    _, failures = parse_headers(["Content-Type: text/plain"])
    _check(any("managed by the REST proof harness" in failure for failure in failures), f"Content-Type override REST header was not caught: {failures}")
    try:
        _parse_live_route("TRACE /v1/smoke/success")
    except ValueError as error:
        _check(
            "live route method must be one of" in str(error),
            f"unsupported REST boundary method error was wrong: {error}",
        )
    else:
        raise AssertionError("unsupported REST boundary method was not caught")

    with tempfile.TemporaryDirectory() as tmp:
        evidence_path = Path(tmp) / "rest-evidence.json"
        success_route = _parse_live_route("GET /v1/smoke/success")
        error_route = _parse_live_route("POST /v1/smoke/error")
        write_evidence(
            evidence_path,
            base_url="http://127.0.0.1:8080",
            route_family_checked=True,
            boundary_checked=True,
            success_route=success_route,
            error_route=error_route,
            expected_error_code="INVALID_ARGUMENT",
            timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
        )
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
        _check(evidence["schema_version"] == EVIDENCE_SCHEMA_VERSION, "REST evidence schema version drifted")
        _check(evidence["base_url"] == {"scheme": "http", "authority": "127.0.0.1:8080"}, "REST evidence base URL was not redacted to scheme/authority")
        _check(evidence["route_family_proof"]["canonical_routes_probed"] == EXPECTED_CANONICAL_ROUTE_COUNT, "REST evidence canonical route count drifted")
        _check(evidence["route_family_proof"]["retired_routes_probed"] == EXPECTED_RETIRED_ROUTE_COUNT, "REST evidence retired route count drifted")
        _check(evidence["boundary_proof"]["success_route"] == {"method": "GET", "path": "/v1/smoke/success"}, "REST evidence success route drifted")
        _check(evidence["boundary_proof"]["expected_error_code"] == "INVALID_ARGUMENT", "REST evidence error code drifted")

    print("rest-route-gateway-smoke selftest passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Check canonical REST routes and retired beta routes.")
    parser.add_argument("--selftest", action="store_true", help="run local positive and negative fixtures")
    parser.add_argument("--check-openapi", action="store_true", help="check committed/generated Swagger route shape")
    parser.add_argument("--swagger", type=Path, default=DEFAULT_SWAGGER, help="Swagger JSON path for --check-openapi")
    parser.add_argument("--base-url", help="live REST gateway base URL")
    parser.add_argument(
        "--require-route-family-proof",
        action="store_true",
        help="fail unless the live canonical/retired route-family proof runs",
    )
    parser.add_argument(
        "--require-boundary-proof",
        action="store_true",
        help="fail unless the live success/error REST boundary proof runs",
    )
    parser.add_argument("--header", action="append", default=[], help="extra live request header, as 'Name: Value'")
    parser.add_argument(
        "--boundary-success",
        help="live success boundary route as 'METHOD /path' or 'METHOD:/path'; requires --base-url",
    )
    parser.add_argument(
        "--boundary-error",
        help="live error boundary route as 'METHOD /path' or 'METHOD:/path'; requires --base-url",
    )
    parser.add_argument(
        "--boundary-error-code",
        help="expected ApiError.code for --boundary-error, for example NOT_FOUND or INVALID_ARGUMENT",
    )
    parser.add_argument("--timeout", default=str(DEFAULT_TIMEOUT_SECONDS), help="live request timeout in seconds")
    parser.add_argument("--evidence-out", type=Path, help="write structured JSON proof for a successful run")
    args = parser.parse_args()

    if args.selftest:
        return run_selftest()

    failures: list[str] = []
    route_family_checked = False
    boundary_checked = False
    success_route: HttpRoute | None = None
    error_route: HttpRoute | None = None
    failures.extend(validate_route_inventory())
    boundary_failures = validate_boundary_inputs(args.boundary_success, args.boundary_error, args.boundary_error_code)
    failures.extend(boundary_failures)
    base_url_failures = validate_base_url(args.base_url)
    base_url_failures.extend(validate_timeout_seconds(args.timeout))
    failures.extend(base_url_failures)
    timeout_seconds = normalize_timeout_seconds(args.timeout) if not base_url_failures else DEFAULT_TIMEOUT_SECONDS
    headers, header_failures = parse_headers(args.header)
    failures.extend(header_failures)
    if args.check_openapi or not args.base_url:
        failures.extend(check_openapi(args.swagger))
    if args.base_url and not base_url_failures and not header_failures:
        failures.extend(check_live_gateway(args.base_url, headers=headers, timeout=timeout_seconds))
        route_family_checked = True
        success_route, error_route = parse_validated_boundary_routes(
            args.boundary_success,
            args.boundary_error,
            boundary_failures,
        )
        if success_route or error_route:
            failures.extend(
                check_live_boundary(
                    args.base_url,
                    success_route,
                    error_route,
                    expected_error_code=args.boundary_error_code,
                    headers=headers,
                    timeout=timeout_seconds,
                )
            )
            boundary_checked = True
    elif args.boundary_success or args.boundary_error:
        failures.append("--boundary-success/--boundary-error require --base-url")
    failures.extend(
        validate_required_live_proofs(
            args.require_route_family_proof,
            route_family_checked,
            args.require_boundary_proof,
            boundary_checked,
        )
    )

    if failures:
        for failure in failures:
            print(failure)
        return 1

    if args.evidence_out is not None:
        write_evidence(
            args.evidence_out,
            base_url=args.base_url,
            route_family_checked=route_family_checked,
            boundary_checked=boundary_checked,
            success_route=success_route,
            error_route=error_route,
            expected_error_code=args.boundary_error_code,
            timeout_seconds=timeout_seconds,
        )

    checked = []
    if args.check_openapi or not args.base_url:
        checked.append(f"OpenAPI {args.swagger}")
    if args.base_url:
        checked.append(f"live gateway {args.base_url}")
        if route_family_checked:
            checked.append("live canonical/retired route families")
        if args.boundary_success or args.boundary_error:
            checked.append("live REST boundary")
    print(f"rest-route-gateway-smoke passed: {', '.join(checked)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
