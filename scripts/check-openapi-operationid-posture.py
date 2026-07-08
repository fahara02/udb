#!/usr/bin/env python3
"""Source guard for Chapter 14.3 descriptor-owned OpenAPI operation IDs."""

from __future__ import annotations

import argparse
import json
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class SourceCheck:
    label: str
    path: str
    tokens: tuple[str, ...]


SOURCE_CHECKS: tuple[SourceCheck, ...] = (
    SourceCheck(
        "OpenAPI inventory count guard",
        "scripts/check-openapi-operationid-posture.py",
        (
            "def openapi_operation_count(root: Path) -> int:",
            "def native_contract_http_count(root: Path) -> int:",
            "def status_constructor_count(root: Path) -> int:",
            "API inventory mismatch: proto HTTP RPCs=",
            "status_constructor_sites=",
            "expected API inventory mismatch failure",
        ),
    ),
    SourceCheck(
        "native manifest operation id JSON",
        "src/cli/mod.rs",
        (
            '"rest_operation_id": rpc.sdk_surface.as_ref().map(|s| s.rest_operation_id.clone()).unwrap_or_default()',
            '"sdk_surface": rpc.sdk_surface.as_ref().map(sdk_surface_json)',
            '"rest_operation_id": surface.rest_operation_id',
        ),
    ),
    SourceCheck(
        "OpenAPI postprocess descriptor mapping",
        "scripts/openapi-postprocess.mjs",
        (
            "const nativeContractPath = resolve(repoRoot, 'docs/generated/udb-native-contract.json');",
            "const sdkSurface = rpc.sdk_surface || {};",
            "const restOperationId = sdkSurface.rest_operation_id || toLowerCamel(annotatedAlias) || toLowerCamel(rpc.method);",
            "byGeneratedId.set(metadata.generatedId, metadata);",
            "byRoute.set(metadata.routeKey, metadata);",
            "operation.operationId = metadata.operationId;",
            "operation['x-udb-sdk-alias'] = metadata.sdkAlias;",
            "operation['x-udb-operation-kind'] = metadata.operationKind;",
        ),
    ),
    SourceCheck(
        "OpenAPI API-rule guard",
        "scripts/check-openapi-api-rules.mjs",
        (
            "retiredBetaRoutes",
            "'/v1/auth/login'",
            "'/v1/storage/uploads/{file_id}/finalize'",
            "'/v1/webrtc/rooms/{room_id}/close'",
            "descriptorOwnedExtensions",
            "Service_RpcName shape",
            "operationId is required",
            "retired route regression was not caught",
            "missing descriptor extension was not caught",
            "SDK-normalized operationId collision was not caught",
            "betaStabilityClaim",
            "function scanBetaStabilityClaim(errors, where, value)",
            "beta stability wording was not caught",
        ),
    ),
    SourceCheck(
        "CI generated Swagger smoke gate",
        ".github/workflows/ci.yml",
        (
            "node scripts/openapi-postprocess.mjs",
            "node --check scripts/check-openapi-api-rules.mjs",
            "node scripts/check-openapi-api-rules.mjs --selftest",
            "node scripts/check-openapi-api-rules.mjs",
            "git diff --quiet -- sdk/php/gen sdk/go/gen sdk/typescript/gen sdk/python/gen sdk/java/gen sdk/csharp/gen api",
            "Committed SDK stubs are stale. Run 'buf generate --include-imports' and commit the result.",
        ),
    ),
)

RPC_BLOCK = re.compile(r"^  rpc\s+(?P<name>[A-Za-z0-9_]+)\(.*?^  \}", re.MULTILINE | re.DOTALL)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def check_source(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    for check in SOURCE_CHECKS:
        path = root / check.path
        if not path.is_file():
            failures.append(f"{check.label}: missing file {check.path}")
            continue
        text = _read(path)
        for token in check.tokens:
            if token not in text:
                failures.append(f"{check.label}: missing token {token!r} in {check.path}")

    failures.extend(check_proto_http_operation_ids(root))
    failures.extend(check_committed_swagger_operation_ids(root))
    failures.extend(check_api_inventory_counts(root))
    return failures


def proto_http_rpc_count(root: Path) -> tuple[int, list[str]]:
    failures: list[str] = []
    proto_root = root / "proto" / "udb" / "core"
    if not proto_root.is_dir():
        return 0, ["proto HTTP operation IDs: missing proto/udb/core"]

    http_count = 0
    for path in sorted(proto_root.rglob("*.proto")):
        rel = path.relative_to(root).as_posix()
        text = _read(path)
        for block in RPC_BLOCK.finditer(text):
            body = block.group(0)
            if "google.api.http" not in body:
                continue
            http_count += 1
            name = block.group("name")
            if not re.search(r"\brest_operation_id\s*:\s*\"[^\"]+\"", body):
                failures.append(f"{rel}::{name}: HTTP RPC missing sdk_surface.rest_operation_id")

    if http_count == 0:
        failures.append("proto HTTP operation IDs: found no google.api.http RPCs")
    return http_count, failures


def check_proto_http_operation_ids(root: Path) -> list[str]:
    return proto_http_rpc_count(root)[1]


def openapi_operation_count(root: Path) -> int:
    path = root / "api" / "udb-broker.swagger.json"
    if not path.is_file():
        return 0
    swagger = json.loads(_read(path))
    count = 0
    for methods in (swagger.get("paths") or {}).values():
        for method, operation in (methods or {}).items():
            if method.lower() in {"get", "put", "post", "patch", "delete"} and isinstance(operation, dict):
                count += 1
    return count


def native_contract_http_count(root: Path) -> int:
    path = root / "docs" / "generated" / "udb-native-contract.json"
    if not path.is_file():
        return 0
    contract = json.loads(_read(path))
    count = 0
    for service in contract.get("services") or []:
        for rpc in service.get("rpcs") or []:
            if rpc.get("http"):
                count += 1
    return count


def status_constructor_count(root: Path) -> int:
    src = root / "src"
    if not src.is_dir():
        return 0
    total = 0
    for path in src.rglob("*.rs"):
        total += len(re.findall(r"\bStatus::[A-Za-z_][A-Za-z0-9_]*", _read(path)))
    return total


def check_api_inventory_counts(root: Path) -> list[str]:
    proto_count, proto_failures = proto_http_rpc_count(root)
    failures = [failure for failure in proto_failures if "missing sdk_surface.rest_operation_id" not in failure]
    native_count = native_contract_http_count(root)
    openapi_count = openapi_operation_count(root)
    if native_count and proto_count and native_count != proto_count:
        failures.append(
            f"API inventory mismatch: proto HTTP RPCs={proto_count}, native-contract HTTP RPCs={native_count}"
        )
    if openapi_count and proto_count and openapi_count != proto_count:
        failures.append(
            f"API inventory mismatch: proto HTTP RPCs={proto_count}, OpenAPI operations={openapi_count}"
        )
    return failures


def check_committed_swagger_operation_ids(root: Path) -> list[str]:
    path = root / "api" / "udb-broker.swagger.json"
    if not path.is_file():
        return []
    text = _read(path)
    failures: list[str] = []
    if re.search(r'"operationId"\s*:\s*"[A-Za-z0-9]+Service_[A-Za-z0-9]+"', text):
        failures.append("committed Swagger still contains generated Service_RpcName operationId")
    for route in (
        '"/v1/auth/login"',
        '"/v1/storage/uploads/{file_id}/finalize"',
        '"/v1/storage/uploads/{fileId}/finalize"',
        '"/v1/webrtc/rooms/{room_id}/close"',
        '"/v1/webrtc/rooms/{roomId}/close"',
    ):
        if route in text:
            failures.append(f"committed Swagger still contains retired beta route {route}")
    for token in ('"operationId": "sendOtp"', '"x-udb-sdk-alias": "send_otp"', '"operationId": "scimPatchUser"'):
        if token not in text:
            failures.append(f"committed Swagger missing descriptor-owned token {token}")
    return failures


def _write_fixture(
    root: Path,
    missing_rest_id: bool = False,
    generated_operation_id: bool = False,
    extra_swagger_operation: bool = False,
) -> None:
    for check in SOURCE_CHECKS:
        path = root / check.path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(check.tokens) + "\n", encoding="utf-8")

    proto = root / "proto/udb/core/authn/services/v1/authn_service.proto"
    proto.parent.mkdir(parents=True, exist_ok=True)
    rest = "" if missing_rest_id else ' rest_operation_id: "sendOtp"'
    proto.write_text(
        f"""
syntax = "proto3";
package udb.core.authn.services.v1;
service AuthnService {{
  rpc SendOTP(SendOTPRequest) returns (SendOTPResponse) {{
    option (udb.core.common.v1.sdk_surface) = {{ method_alias: "send_otp"{rest} }};
    option (google.api.http) = {{ post: "/v1/auth/otps:send" body: "*" }};
  }}
  rpc ScimPatchUser(ScimPatchUserRequest) returns (ScimPatchUserResponse) {{
    option (udb.core.common.v1.sdk_surface) = {{ method_alias: "scim_patch_user" rest_operation_id: "scimPatchUser" }};
    option (google.api.http) = {{ patch: "/v1/idp/scim/{{provider_id}}/Users/{{scim_user_id}}" body: "*" }};
  }}
}}
""".lstrip(),
        encoding="utf-8",
    )

    api = root / "api" / "udb-broker.swagger.json"
    api.parent.mkdir(parents=True, exist_ok=True)
    operation_id = "AuthnService_SendOTP" if generated_operation_id else "sendOtp"
    extra = """
    "/v1/auth/otps:verify": {
      "post": {
        "operationId": "verifyOtp",
        "x-udb-sdk-alias": "verify_otp"
      }
    },
""".rstrip() if extra_swagger_operation else ""
    api.write_text(
        f"""
{{
  "paths": {{
    {extra}
    "/v1/auth/otps:send": {{
      "post": {{
        "operationId": "{operation_id}",
        "x-udb-sdk-alias": "send_otp"
      }}
    }},
    "/v1/idp/scim/{{provider_id}}/Users/{{scim_user_id}}": {{
      "patch": {{
        "operationId": "scimPatchUser"
      }}
    }}
  }}
}}
""".lstrip(),
        encoding="utf-8",
    )

    contract = root / "docs" / "generated" / "udb-native-contract.json"
    contract.parent.mkdir(parents=True, exist_ok=True)
    contract.write_text(
        json.dumps(
            {
                "services": [
                    {
                        "service": "udb.core.authn.services.v1.AuthnService",
                        "rpcs": [
                            {
                                "method": "SendOTP",
                                "http": {"verb": "post", "path": "/v1/auth/otps:send"},
                            },
                            {
                                "method": "ScimPatchUser",
                                "http": {
                                    "verb": "patch",
                                    "path": "/v1/idp/scim/{provider_id}/Users/{scim_user_id}",
                                },
                            }
                        ],
                    }
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    status_file = root / "src" / "runtime" / "service" / "tests.rs"
    status_file.parent.mkdir(parents=True, exist_ok=True)
    status_file.write_text("fn sample() { let _ = Status::invalid_argument(\"bad\"); }\n", encoding="utf-8")


def _write_retired_route_fixture(root: Path) -> None:
    _write_fixture(root)
    api = root / "api" / "udb-broker.swagger.json"
    api.write_text(
        api.read_text(encoding="utf-8").replace(
            '"/v1/auth/otps:send"',
            '"/v1/storage/uploads/{file_id}/finalize"',
        ),
        encoding="utf-8",
    )


def run_selftest() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_fixture(root)
        failures = check_source(root)
        if failures:
            raise AssertionError(f"expected clean fixture, got {failures}")

        _write_fixture(root, missing_rest_id=True)
        failures = check_source(root)
        if not any("HTTP RPC missing sdk_surface.rest_operation_id" in failure for failure in failures):
            raise AssertionError(f"expected missing rest_operation_id failure, got {failures}")

        _write_fixture(root, generated_operation_id=True)
        failures = check_source(root)
        if not any("Service_RpcName operationId" in failure for failure in failures):
            raise AssertionError(f"expected generated operationId failure, got {failures}")

        _write_retired_route_fixture(root)
        failures = check_source(root)
        if not any("retired beta route" in failure for failure in failures):
            raise AssertionError(f"expected retired beta route failure, got {failures}")

        _write_fixture(root, extra_swagger_operation=True)
        failures = check_source(root)
        if not any("API inventory mismatch" in failure for failure in failures):
            raise AssertionError(f"expected API inventory mismatch failure, got {failures}")

    print("OpenAPI operation-id posture selftest passed")
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
    proto_count, _ = proto_http_rpc_count(ROOT)
    print(
        "OpenAPI operation-id posture guard passed "
        f"(proto_http={proto_count}, native_contract_http={native_contract_http_count(ROOT)}, "
        f"openapi_operations={openapi_operation_count(ROOT)}, status_constructor_sites={status_constructor_count(ROOT)})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
