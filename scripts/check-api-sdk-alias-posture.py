#!/usr/bin/env python3
"""Source guard for Chapter 14.2 API/SDK alias posture."""

from __future__ import annotations

import argparse
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
        "sdk surface proto options",
        "proto/udb/core/common/v1/security.proto",
        (
            "string method_alias = 2;",
            "bool generate_minimal_example = 10;",
            "string rest_operation_id = 11;",
        ),
    ),
    SourceCheck(
        "descriptor manifest rest operation decode",
        "src/runtime/descriptor_manifest.rs",
        (
            "pub struct SdkSurfaceContract",
            "pub method_alias: String,",
            "pub rest_operation_id: String,",
            "rest_operation_id: value.rest_operation_id.clone()",
            "#[prost(string, tag = \"11\")]",
            "fn sdk_surface_contract_decodes_rest_operation_id()",
        ),
    ),
    SourceCheck(
        "runtime SDK manifest aliases",
        "src/runtime/sdk_manifest.rs",
        (
            "pub method_alias: String,",
            "pub method_alias_snake: String,",
            "pub method_alias_camel: String,",
            "pub method_alias_pascal: String,",
            "pub rest_operation_id: String,",
            "let method_alias_snake = alias_snake_case(&method_alias);",
            "let method_alias_camel = alias_camel_case(&method_alias);",
            "let method_alias_pascal = alias_pascal_case(&method_alias);",
            "fn sdk_alias_identity_is_acronym_safe()",
        ),
    ),
    SourceCheck(
        "DataBroker explicit SDK aliases",
        "proto/udb/services/v1/data_broker.proto",
        (
            'method_alias: "select_v2" rest_operation_id: "selectV2"',
            'method_alias: "publish_cdc" rest_operation_id: "publishCdc"',
            'method_alias: "list_dlq_events" rest_operation_id: "listDlqEvents"',
            'method_alias: "get_cdc_status" rest_operation_id: "getCdcStatus"',
            'method_alias: "approve_migration_plan" rest_operation_id: "approveMigrationPlan"',
        ),
    ),
    SourceCheck(
        "SDK generator placeholders",
        "src/cli/sdk_gen.rs",
        (
            "\"method_alias\".to_string()",
            "\"method_alias_snake\".to_string()",
            "\"method_alias_camel\".to_string()",
            "\"method_alias_pascal\".to_string()",
            "\"rest_operation_id\".to_string()",
            "\"http_verb\".to_string()",
            "\"http_path\".to_string()",
            "(\"{{RPC_WIRE_NAME}}\", rpc.method.clone())",
            "(\"{{RPC_ALIAS_SNAKE}}\", alias_snake.clone())",
            "(\"{{RPC_ALIAS_CAMEL}}\", alias_camel.clone())",
            "(\"{{RPC_ALIAS_PASCAL}}\", alias_pascal.clone())",
            "(\"{{REST_OPERATION_ID}}\", rest_operation_id.clone())",
            "(\"{{RPC_HTTP_METHOD}}\", rpc.http_verb.clone())",
            "(\"{{RPC_HTTP_PATH}}\", rpc.http_path.clone())",
            "fn alias_placeholders_are_acronym_safe_and_wire_name_stays_compatible()",
            "fn sdk_manifest_json_exposes_template_token_fields()",
        ),
    ),
    SourceCheck(
        "native lint alias checks",
        "src/cli/native_lint.rs",
        (
            "findings.extend(sdk_alias_findings(manifest));",
            "\"code\": \"sdk_method_alias_missing\"",
            "\"code\": \"sdk_method_alias_collision\"",
            "let key = (rpc.service_full(), namespace.to_string(), rendered.clone());",
            "fn sdk_alias_namespaces(alias: &str)",
            "fn public_sdk_rpc_missing_method_alias_is_error()",
            "fn public_sdk_alias_collision_after_language_normalization_is_error()",
        ),
    ),
    SourceCheck(
        "TypeScript generated alias template",
        "sdk-templates/typescript/generatedClient.ts.tmpl",
        (
            "{{RPC_ALIAS_SNAKE}}<TRes = RpcOutput<\"{{RPC_OUTPUT}}\">>(request: RpcInput<\"{{RPC_INPUT}}\">, call?: CallOptions): Promise<TRes>;",
            "{{RPC_ALIAS_CAMEL}}<TRes = RpcOutput<\"{{RPC_OUTPUT}}\">>(request: RpcInput<\"{{RPC_INPUT}}\">, call?: CallOptions): Promise<TRes>;",
            "api.{{RPC_ALIAS_SNAKE}} = call;",
            "api.{{RPC_ALIAS_CAMEL}} = call;",
            "RPC_HTTP_METHOD",
            "RPC_HTTP_PATH",
        ),
    ),
    SourceCheck(
        "Python generated alias template",
        "sdk-templates/python/udb_client/generated_client.py.tmpl",
        (
            "def {{RPC_ALIAS_SNAKE}}(",
            "wire RPC ``{{RPC_PATH}}``",
            "RPC_HTTP_METHOD",
            "RPC_HTTP_PATH",
        ),
    ),
    SourceCheck(
        "PHP generated alias template",
        "sdk-templates/php/src/Generated/GeneratedClient.php.tmpl",
        (
            "public alias {{RPC_ALIAS_SNAKE}}",
            "METHOD_ALIASES",
            "public function {{PHP_RPC_METHOD_CAMEL}}",
            "{{PHP_METHOD_ALIAS_ENTRIES}}",
            "OPERATION_KIND_BY_RPC",
            "HTTP_METHOD",
            "HTTP_PATH",
        ),
    ),
    SourceCheck(
        "Java generated alias template",
        "sdk-templates/java/src/main/java/dev/udb/client/generated/GeneratedUdbClient.java.tmpl",
        (
            "public alias {@code {{RPC_ALIAS_SNAKE}}}",
            "{{RPC_ALIAS_PASCAL}}(",
            "operationKind",
            "httpMethod",
            "httpPath",
        ),
    ),
    SourceCheck(
        "C# generated alias template",
        "sdk-templates/csharp/Udb.Client/GeneratedClient.cs.tmpl",
        (
            "<c>{{RPC_ALIAS_SNAKE}}</c>",
            "{{RPC_ALIAS_PASCAL}}Async(",
            "OperationKind",
            "HttpMethod",
            "HttpPath",
        ),
    ),
    SourceCheck(
        "Go generated alias metadata",
        "sdk-templates/go/udbclient/generated_client.go.tmpl",
        (
            'APIAlias: "{{RPC_ALIAS_SNAKE}}"',
            'OperationID: "{{REST_OPERATION_ID}}"',
            'HTTPMethod: "{{RPC_HTTP_METHOD}}"',
            'HTTPPath: "{{RPC_HTTP_PATH}}"',
            'OperationKind: "{{RPC_OPERATION_KIND}}"',
            'FullMethod: "{{RPC_PATH}}"',
            'Name: "{{RPC_WIRE_NAME}}"',
        ),
    ),
    SourceCheck(
        "SDK conformance Swagger route metadata gate",
        "sdk-conformance/run.mjs",
        (
            "function normalizeHttpPath(path)",
            "function compareSwaggerRoutes(failures, expected, swagger)",
            "Swagger: missing route for",
            "Swagger: duplicate operationId",
            "Swagger: ${info.operationId} alias mismatch",
            "Swagger: ${info.operationId} route mismatch",
            "compareSwaggerRoutes(failures, expected, swagger);",
        ),
    ),
)

SDK_SURFACE_BLOCK = re.compile(
    r"option\s+\(udb\.core\.common\.v1\.sdk_surface\)\s*=\s*\{(?P<body>.*?)\};",
    re.DOTALL,
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

    proto_root = root / "proto" / "udb"
    if proto_root.is_dir():
        for path in sorted(proto_root.rglob("*.proto")):
            rel = path.relative_to(root).as_posix()
            text = _read(path)
            service_surface = re.search(
                r"option\s+\(udb\.core\.common\.v1\.service_sdk_surface\)\s*=\s*\{(?P<body>.*?)\};",
                text,
                re.DOTALL,
            )
            service_default_facade = bool(
                service_surface and re.search(r"\binclude_in_facade\s*:\s*true\b", service_surface.group("body"))
            )
            for idx, match in enumerate(SDK_SURFACE_BLOCK.finditer(text), start=1):
                body = match.group("body")
                public_facade = re.search(r"\binclude_in_facade\s*:\s*true\b", body) or (
                    service_default_facade and re.search(r"\brest_operation_id\s*:\s*\"[^\"]+\"", body)
                )
                if public_facade and not re.search(
                    r"\bmethod_alias\s*:\s*\"[^\"]+\"",
                    body,
                ):
                    failures.append(f"{rel}: public sdk_surface block {idx} has no method_alias")
    failures.extend(check_data_broker_aliases(root))
    return failures


def check_data_broker_aliases(root: Path) -> list[str]:
    failures: list[str] = []
    path = root / "proto" / "udb" / "services" / "v1" / "data_broker.proto"
    if not path.is_file():
        return ["DataBroker explicit SDK aliases: missing proto/udb/services/v1/data_broker.proto"]

    text = _read(path)
    blocks = list(RPC_BLOCK.finditer(text))
    if (root / ".git").exists() and len(blocks) != 78:
        failures.append(f"DataBroker explicit SDK aliases: expected 78 RPC blocks, found {len(blocks)}")

    for block in blocks:
        name = block.group("name")
        body = block.group(0)
        if "option (udb.core.common.v1.sdk_surface)" not in body:
            failures.append(f"DataBroker/{name}: missing sdk_surface option")
            continue
        if not re.search(r"\binclude_in_facade\s*:\s*true\b", body):
            failures.append(f"DataBroker/{name}: sdk_surface must set include_in_facade=true")
        if not re.search(r"\bmethod_alias\s*:\s*\"[^\"]+\"", body):
            failures.append(f"DataBroker/{name}: sdk_surface missing method_alias")
        if not re.search(r"\brest_operation_id\s*:\s*\"[^\"]+\"", body):
            failures.append(f"DataBroker/{name}: sdk_surface missing rest_operation_id")
    return failures


def _write_fixture(root: Path, missing_alias: bool = False, missing_data_broker_alias: bool = False) -> None:
    for check in SOURCE_CHECKS:
        path = root / check.path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(check.tokens) + "\n", encoding="utf-8")

    proto = root / "proto/udb/core/authn/services/v1/authn_service.proto"
    proto.parent.mkdir(parents=True, exist_ok=True)
    alias = "" if missing_alias else ' method_alias: "send_otp"'
    proto.write_text(
        f"""
syntax = "proto3";
package udb.core.authn.services.v1;
service AuthnService {{
  option (udb.core.common.v1.service_sdk_surface) = {{ include_in_facade: true }};
  rpc SendOTP(SendOTPRequest) returns (SendOTPResponse) {{
    option (udb.core.common.v1.sdk_surface) = {{ {alias} rest_operation_id: "sendOtp" }};
  }}
  rpc InternalProbe(ProbeRequest) returns (ProbeResponse) {{
    option (udb.core.common.v1.sdk_surface) = {{ method_alias: "internal_probe" rest_operation_id: "internalProbe" }};
  }}
}}
""".lstrip(),
        encoding="utf-8",
    )
    data_broker = root / "proto/udb/services/v1/data_broker.proto"
    data_broker.parent.mkdir(parents=True, exist_ok=True)
    data_broker_alias = "" if missing_data_broker_alias else ' method_alias: "select_v2"'
    data_broker.write_text(
        f"""
syntax = "proto3";
package udb.services.v1;
service DataBroker {{
  rpc SelectV2(SelectRequest) returns (RecordBatchV2) {{
    option (udb.core.common.v1.operation_kind) = OPERATION_KIND_READ_ONLY;
    option (udb.core.common.v1.sdk_surface) = {{ include_in_facade: true{data_broker_alias} rest_operation_id: "selectV2" }};
  }}
  rpc PublishCDC(CDCSubscriptionRequest) returns (CDCEnvelope) {{
    option (udb.core.common.v1.operation_kind) = OPERATION_KIND_MUTATION;
    option (udb.core.common.v1.sdk_surface) = {{ include_in_facade: true method_alias: "publish_cdc" rest_operation_id: "publishCdc" }};
  }}
  rpc ListDlqEvents(DlqListRequest) returns (DlqListResponse) {{
    option (udb.core.common.v1.operation_kind) = OPERATION_KIND_READ_ONLY;
    option (udb.core.common.v1.sdk_surface) = {{ include_in_facade: true method_alias: "list_dlq_events" rest_operation_id: "listDlqEvents" }};
  }}
  rpc GetCdcStatus(CdcControlRequest) returns (CdcStatusResponse) {{
    option (udb.core.common.v1.operation_kind) = OPERATION_KIND_READ_ONLY;
    option (udb.core.common.v1.sdk_surface) = {{ include_in_facade: true method_alias: "get_cdc_status" rest_operation_id: "getCdcStatus" }};
  }}
  rpc ApproveMigrationPlan(MigrationRunRequest) returns (MigrationStatusResponse) {{
    option (udb.core.common.v1.operation_kind) = OPERATION_KIND_MUTATION;
    option (udb.core.common.v1.sdk_surface) = {{ include_in_facade: true method_alias: "approve_migration_plan" rest_operation_id: "approveMigrationPlan" }};
  }}
}}
""".lstrip(),
        encoding="utf-8",
    )


def run_selftest() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write_fixture(root)
        failures = check_source(root)
        if failures:
            raise AssertionError(f"expected clean fixture, got {failures}")

        _write_fixture(root, missing_alias=True)
        failures = check_source(root)
        if not any("public sdk_surface block" in failure and "method_alias" in failure for failure in failures):
            raise AssertionError(f"expected missing service-default facade alias failure, got {failures}")

        _write_fixture(root, missing_data_broker_alias=True)
        failures = check_source(root)
        if not any("DataBroker/SelectV2" in failure and "method_alias" in failure for failure in failures):
            raise AssertionError(f"expected missing DataBroker alias failure, got {failures}")

        _write_fixture(root)
        native_lint = root / "src/cli/native_lint.rs"
        native_lint.write_text(
            native_lint.read_text(encoding="utf-8").replace('"code": "sdk_method_alias_collision"\n', ""),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("sdk_method_alias_collision" in failure for failure in failures):
            raise AssertionError(f"expected native-lint collision token failure, got {failures}")

    print("API/SDK alias posture selftest passed")
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
    print("API/SDK alias posture guard passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
