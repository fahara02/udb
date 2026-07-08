#!/usr/bin/env python3
"""Fail CI if replay-safe mutation retry metadata drifts from proto contracts."""

from __future__ import annotations

import argparse
import re
import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class ProtoRpc:
    full_method: str
    operation_kind: str
    request_key_field: str
    duplicate_response_field: str
    replay_safe: bool
    block: str
    path: Path


@dataclass(frozen=True)
class GeneratedRpc:
    full_method: str
    operation_kind: str
    replay_safe: bool


@dataclass(frozen=True)
class TokenCheck:
    label: str
    path: str
    tokens: tuple[str, ...]


@dataclass(frozen=True)
class ForbiddenTokenCheck:
    label: str
    path: str
    tokens: tuple[str, ...]


TOKEN_CHECKS: tuple[TokenCheck, ...] = (
    TokenCheck(
        "SDK manifest derives replay-safe from proto idempotency contract",
        "src/runtime/sdk_manifest.rs",
        (
            "replay_safe: method",
            ".idempotency_contract",
            ".map(|c| c.replay_safe)",
            ".unwrap_or(false)",
        ),
    ),
    TokenCheck(
        "SDK generator emits replay-safe placeholders and manifest JSON",
        "src/cli/sdk_gen.rs",
        (
            '"replay_safe".to_string()',
            "serde_json::json!(rpc.replay_safe)",
            '("{{RPC_REPLAY_SAFE}}", rpc.replay_safe.to_string())',
            "replay_safe_placeholder_reflects_idempotency_contract",
        ),
    ),
    TokenCheck(
        "Go retry template gates mutations on replay-safe plus idempotency key",
        "sdk-templates/go/udbclient/generated_client.go.tmpl",
        (
            "func (rc RetryConfig) retryableForRPC(code codes.Code, readOnly, replaySafe, hasIdempotencyKey bool) bool",
            "if !replaySafe || !hasIdempotencyKey",
            "ReplaySafe: {{RPC_REPLAY_SAFE}}",
            "func isReplaySafeRPC(method string) bool",
            "func hasIdempotencyKey(req any) bool",
            "strings.TrimSpace(k.GetIdempotencyKey()) != \"\"",
        ),
    ),
    TokenCheck(
        "Generated Go retry surface gates mutations on replay-safe plus idempotency key",
        "sdk/go/udbclient/generated_client.go",
        (
            "func (rc RetryConfig) retryableForRPC(code codes.Code, readOnly, replaySafe, hasIdempotencyKey bool) bool",
            "if !replaySafe || !hasIdempotencyKey",
            "ReplaySafe: true",
            "ReplaySafe: false",
            "func isReplaySafeRPC(method string) bool",
            "func hasIdempotencyKey(req any) bool",
            "strings.TrimSpace(k.GetIdempotencyKey()) != \"\"",
        ),
    ),
    TokenCheck(
        "TypeScript retry template gates mutations on replay-safe plus idempotency key",
        "sdk-templates/typescript/generatedClient.ts.tmpl",
        (
            "export const RPC_REPLAY_SAFE",
            "RPC_REPLAY_SAFE[path] === true && UdbCore.hasIdempotencyKey(request)",
            "private static hasIdempotencyKey(request: any): boolean",
            "top-level idempotency key",
            "request.idempotency_key ??",
            "request.idempotencyKey;",
        ),
    ),
    TokenCheck(
        "TypeScript dist-test retry surface gates mutations on replay-safe plus idempotency key",
        "sdk/typescript/dist-test/generatedClient.js",
        (
            "exports.RPC_REPLAY_SAFE",
            "exports.RPC_REPLAY_SAFE[path] === true && UdbCore.hasIdempotencyKey(request)",
            "static hasIdempotencyKey(request)",
            "top-level idempotency key",
            "request.idempotency_key ??",
            "request.idempotencyKey;",
        ),
    ),
    TokenCheck(
        "Python retry template gates mutations on replay-safe plus idempotency key",
        "sdk-templates/python/udb_client/generated_client.py.tmpl",
        (
            "RPC_REPLAY_SAFE: dict[str, str]",
            "if not (replay_safe and has_idempotency_key):",
            "def _request_has_idempotency_key(request: Any) -> bool:",
            "top-level ``idempotency_key``",
            "nested only under request context",
            "key.strip()",
        ),
    ),
    TokenCheck(
        "Generated Python retry surface gates mutations on replay-safe plus nonblank idempotency key",
        "sdk/python/udb_client/generated_client.py",
        (
            "RPC_REPLAY_SAFE: dict[str, str]",
            "if not (replay_safe and has_idempotency_key):",
            "def _request_has_idempotency_key(request: Any) -> bool:",
            "top-level ``idempotency_key``",
            "nested only under request context",
            "key.strip()",
        ),
    ),
    TokenCheck(
        "PHP retry template gates mutations on replay-safe plus idempotency key",
        "sdk-templates/php/src/Generated/GeneratedClient.php.tmpl",
        (
            "private function isRetryable(int $code, bool $readOnly, bool $replaySafe, bool $hasIdempotencyKey): bool",
            "if (! $replaySafe || ! $hasIdempotencyKey) {",
            "private function hasIdempotencyKey(mixed $request): bool",
            "trim((string) $request->getIdempotencyKey()) !== ''",
        ),
    ),
    TokenCheck(
        "Generated PHP retry surface gates mutations on replay-safe plus nonblank idempotency key",
        "sdk/php/src/Generated/GeneratedClient.php",
        (
            "private function isRetryable(int $code, bool $readOnly, bool $replaySafe, bool $hasIdempotencyKey): bool",
            "if (! $replaySafe || ! $hasIdempotencyKey) {",
            "private function hasIdempotencyKey(mixed $request): bool",
            "trim((string) $request->getIdempotencyKey()) !== ''",
        ),
    ),
    TokenCheck(
        "Java retry template gates mutations on replay-safe plus idempotency key",
        "sdk-templates/java/src/main/java/dev/udb/client/generated/GeneratedClientSupport.java",
        (
            "Status.Code code, boolean readOnly, boolean replaySafe, boolean hasIdempotencyKey",
            "if (!replaySafe || !hasIdempotencyKey)",
            "return code != Status.Code.DEADLINE_EXCEEDED && RETRYABLE_CODES.contains(code);",
            "private static boolean hasIdempotencyKey(Object request)",
            'getMethod("getIdempotencyKey")',
        ),
    ),
    TokenCheck(
        "Generated Java retry support gates mutations on replay-safe plus idempotency key",
        "sdk/java/src/main/java/dev/udb/client/generated/GeneratedClientSupport.java",
        (
            "Status.Code code, boolean readOnly, boolean replaySafe, boolean hasIdempotencyKey",
            "if (!replaySafe || !hasIdempotencyKey)",
            "return code != Status.Code.DEADLINE_EXCEEDED && RETRYABLE_CODES.contains(code);",
            "private static boolean hasIdempotencyKey(Object request)",
            'getMethod("getIdempotencyKey")',
        ),
    ),
    TokenCheck(
        "Java generated wrappers pass replay-safe placeholder metadata",
        "sdk-templates/java/src/main/java/dev/udb/client/generated/GeneratedUdbClient.java.tmpl",
        (
            "GeneratedClientSupport.unary(",
            '"{{RPC_OPERATION_KIND}}".equals("read_only")',
            '"{{RPC_REPLAY_SAFE}}".equals("true")',
        ),
    ),
    TokenCheck(
        "Generated Java wrappers pass replay-safe metadata",
        "sdk/java/src/main/java/dev/udb/client/generated/GeneratedUdbClient.java",
        (
            "GeneratedClientSupport.unary(",
            '"true".equals("true")',
            '"false".equals("true")',
        ),
    ),
    TokenCheck(
        "C# retry template gates mutations on replay-safe plus idempotency key",
        "sdk-templates/csharp/Udb.Client/GeneratedClientRuntime.cs",
        (
            "private bool IsRetryable(StatusCode code, bool readOnly, bool replaySafe, bool hasIdempotencyKey)",
            "if (!replaySafe || !hasIdempotencyKey)",
            "return code != StatusCode.DeadlineExceeded && Options.RetryableCodes.Contains(code);",
            "private static bool HasIdempotencyKey(object? request)",
            'GetProperty("IdempotencyKey")',
        ),
    ),
    TokenCheck(
        "Generated C# retry support gates mutations on replay-safe plus idempotency key",
        "sdk/csharp/Udb.Client/GeneratedClientRuntime.cs",
        (
            "private bool IsRetryable(StatusCode code, bool readOnly, bool replaySafe, bool hasIdempotencyKey)",
            "if (!replaySafe || !hasIdempotencyKey)",
            "return code != StatusCode.DeadlineExceeded && Options.RetryableCodes.Contains(code);",
            "private static bool HasIdempotencyKey(object? request)",
            'GetProperty("IdempotencyKey")',
        ),
    ),
    TokenCheck(
        "C# generated wrappers pass replay-safe placeholder metadata",
        "sdk-templates/csharp/Udb.Client/GeneratedClient.cs.tmpl",
        (
            "InvokeUnaryAsync(",
            '"{{RPC_OPERATION_KIND}}" == "read_only"',
            '"{{RPC_REPLAY_SAFE}}" == "true"',
            "(object)request);",
        ),
    ),
    TokenCheck(
        "Generated C# wrappers pass replay-safe metadata",
        "sdk/csharp/Udb.Client/GeneratedClient.cs",
        (
            "InvokeUnaryAsync(",
            '"true" == "true"',
            '"false" == "true"',
            "(object)request);",
        ),
    ),
    TokenCheck(
        "Java SDK retry tests cover replay-safe mutation key gates",
        "sdk/java/src/test/java/dev/udb/client/UdbGeneratedRetryTest.java",
        (
            "replaySafeMutationWithKeyRetries",
            "replaySafeMutationWithoutKeyDoesNotRetry",
            "nonReplaySafeMutationWithKeyDoesNotRetry",
            "deadlineExceededMutationDoesNotRetry",
            "blankIdempotencyKeyDoesNotSatisfyMutationRetry",
        ),
    ),
    TokenCheck(
        "C# SDK retry tests cover replay-safe mutation key gates",
        "sdk/csharp/Udb.Client.Tests/UdbGeneratedRetryTests.cs",
        (
            "ReplaySafeMutationWithKeyRetries",
            "ReplaySafeMutationWithoutKeyDoesNotRetry",
            "NonReplaySafeMutationWithKeyDoesNotRetry",
            "DeadlineExceededMutationDoesNotRetry",
            "BlankIdempotencyKeyDoesNotSatisfyMutationRetry",
        ),
    ),
    TokenCheck(
        "Go SDK retry tests cover replay-safe mutation key gates",
        "sdk/go/udbclient/generated_retry_test.go",
        (
            "TestRetryConfigReplaySafeMutationRequiresKey",
            "TestRetryReplaySafeMutationWithKeyRetriesThenSucceeds",
            "TestRetryReplaySafeMutationWithoutKeyDoesNotRetry",
            "TestRetryReplaySafeMutationWithBlankKeyDoesNotRetry",
            "TestRetryNonReplaySafeMutationDoesNotRetry",
            "whitespace-only key",
        ),
    ),
    TokenCheck(
        "Python SDK retry tests reject blank idempotency keys",
        "sdk/python/tests/test_retry_policy.py",
        (
            "test_invoke_replay_safe_mutation_with_blank_key_not_retried",
            "test_invoke_replay_safe_mutation_with_context_only_key_not_retried",
            "_request_has_idempotency_key",
            "idempotency_key=\"   \"",
            "req.context.idempotency_key = \"ctx-key\"",
            "assert fake.calls == 1",
        ),
    ),
    TokenCheck(
        "TypeScript SDK retry tests cover replay-safe mutation key gates",
        "sdk/typescript/retry.test.ts",
        (
            "replay-safe mutation WITH idempotency key retries then succeeds",
            "replay-safe mutation WITHOUT idempotency key is not retried",
            "replay-safe mutation with CONTEXT-ONLY idempotency key is not retried",
            "context: { idempotency_key: \"ctx-key\" }",
            "non-replay-safe mutation is not retried even WITH an idempotency key",
        ),
    ),
    TokenCheck(
        "TypeScript dist-test retry tests cover replay-safe mutation key gates",
        "sdk/typescript/dist-test/retry.test.js",
        (
            "replay-safe mutation WITH idempotency key retries then succeeds",
            "replay-safe mutation WITHOUT idempotency key is not retried",
            "replay-safe mutation with BLANK idempotency key is not retried",
            "replay-safe mutation with CONTEXT-ONLY idempotency key is not retried",
            "context: { idempotency_key: \"ctx-key\" }",
            "non-replay-safe mutation is not retried even WITH an idempotency key",
        ),
    ),
    TokenCheck(
        "PHP SDK retry tests cover replay-safe mutation key gates",
        "sdk/php/tests/Unit/GeneratedRetryTest.php",
        (
            "replay-safe mutation + non-empty idempotency key",
            "gates mutation retry on replay-safe AND idempotency key",
            "detects a non-empty idempotency key on the request proto",
            "setIdempotencyKey('   ')",
            "replay-safe mutation with blank key does not retry",
        ),
    ),
    TokenCheck(
        "retry-safe served smoke ties generated metadata to broker replay",
        "scripts/retry_safe_served_smoke.py",
        (
            "UPSERT_METHOD = \"/udb.services.v1.DataBroker/Upsert\"",
            "DELETE_METHOD = \"/udb.services.v1.DataBroker/Delete\"",
            "DOCUMENT_UPSERT_METHOD = \"/udb.services.v1.DataBroker/DocumentUpsert\"",
            "RPC_REPLAY_SAFE",
            "RetryPolicy",
            "DeleteRequest",
            "from udb.services.v1 import data_broker_pb2",
            "_is_replay_safe",
            "_request_has_idempotency_key",
            "def assert_retry_metadata_gate(",
            "def assert_databroker_method_request(",
            "data_broker_pb2.DESCRIPTOR.services_by_name.get(\"DataBroker\")",
            "request_descriptor = getattr(request, \"DESCRIPTOR\", None)",
            "method_descriptor.input_type.full_name",
            "does not match RPC input",
            "DataBroker generated descriptor has no method",
            "def validate_replay_request(",
            "def validate_message_type_token(",
            "def validate_upsert_payload(",
            "def validate_delete_filter(",
            "object_pairs_hook=_reject_duplicate_json_keys",
            "parse_constant=_reject_non_finite_json_constant",
            "proof JSON must not contain non-standard constant",
            "def validate_shared_replay_scope(",
            "GRPC_METADATA_NAME_CHARS",
            "gRPC metadata header name must contain only lowercase letters",
            "gRPC metadata header name must not start with grpc-",
            "def _contains_control_character(",
            "def validate_grpc_target(",
            "gRPC target must be a host:port authority, not a URL or path",
            "gRPC target must not include control characters",
            "gRPC target port must be an integer from 1 to 65535",
            "MAX_LIVE_TIMEOUT_SECONDS = 120.0",
            "TIMEOUT_DECIMAL_PATTERN",
            "def normalize_timeout_seconds(",
            "def validate_timeout_seconds(",
            "def validate_runtime_metadata(",
            "def validate_runtime_timeout_seconds(",
            "def validate_runtime_transport_inputs(",
            "def validate_runtime_upsert_request(",
            "def validate_runtime_delete_request(",
            "def validate_runtime_stub_method(",
            "runtime stub must expose callable",
            "def validate_runtime_mutation_response(",
            "runtime response must be a MutationResponse",
            "def call_runtime_mutation(",
            "runtime call raised unexpected gRPC error",
            "runtime call raised error",
            "timeout must not include surrounding whitespace",
            "timeout must be a positive decimal number of seconds",
            "timeout must be a finite number of seconds",
            "timeout must be greater than 0 seconds",
            "timeout must be <= 120 seconds",
            "MAX_PROOF_INPUT_BYTES = 1_048_576",
            "def _read_proof_text(",
            "proof file must exist and be a regular file",
            "proof file must be <=",
            "def _assert_restored_summary(",
            "def _assert_typed_write_receipt_lockstep(",
            "def check_served_replay(",
            "def check_served_delete_replay(",
            'validate_message_type_token(f"{label} proof message_type"',
            "message_type must not include surrounding whitespace",
            "message_type must not include whitespace",
            "must not contain control characters",
            "Upsert proof requires non-empty record_json",
            "Upsert proof record_json must be a valid JSON object",
            "Upsert proof record_json must not contain non-standard JSON constants",
            "Upsert proof record_json must be a JSON object",
            "Upsert proof record_json must be a non-empty JSON object",
            "must use only one of record_json, record_json_object, or record_json_text",
            "proof JSON must not contain duplicate key",
            "Delete proof requires a non-empty filter",
            "Delete proof filter field names must be non-empty",
            "Delete proof filter field names must not contain control characters",
            "Delete proof filter values must not be null",
            "Upsert/Delete replay proofs must share",
            "idempotency_key",
            "DataBroker.Delete must be generated as replay-safe",
            "replay-safe keyed mutation should retry UNAVAILABLE",
            "replay-safe mutation without idempotency key must not retry",
            "non-replay-safe mutation must not retry even with idempotency key",
            "mutation DEADLINE_EXCEEDED must not be auto-retried",
            "second replay-safe Upsert did not return was_duplicate=true",
            "second replay-safe Delete did not return was_duplicate=true",
            "first replay-safe Upsert affected_rows must be positive",
            "first replay-safe Delete affected_rows must be positive",
            "duplicate replay mutation_id differs from first response",
            "duplicate Delete replay mutation_id differs from first response",
            "second.mutation_id != first.mutation_id",
            "MUTATION_ID_PATTERN = re.compile",
            "mutation_id must be non-empty",
            "mutation_id must be a canonical lowercase UUID",
            "duplicate replay affected_rows differs from first response",
            "duplicate Delete replay affected_rows differs from first response",
            "first response must include at least one replay summary field",
            "first response record_json must include request field/value",
            "first response {field} must not contain non-standard JSON constants",
            "first response resource_uri authority must equal request tenant_id",
            "first response resource_uri path must start with request message_type",
            "first response resource_uri path must include request message_type and resource id",
            "first response resource_uri id must match an identity request field value",
            "resource_uri id proof requires at least one scalar identity request field",
            "resource_uri id proof identity field value must not include surrounding whitespace",
            "resource_uri id proof identity field value must not include whitespace",
            "first response checksum_sha256 must be sha256:<64 lowercase hex>",
            "duplicate replay checksum_sha256 differs from first response",
            "MANIFEST_CHECKSUM_PATTERN = re.compile",
            "first response write_receipt_json unexpected fields",
            "first response write_receipt_json source_lsn must be non-empty",
            "first response write_receipt_json source_lsn must not include whitespace",
            "first response write_receipt_json source_lsn must not contain control characters",
            "first response write_receipt_json projection_task_ids[{index}] must not include whitespace",
            "first response write_receipt_json projection_task_ids[{index}] must not contain control characters",
            "first response write_receipt_json manifest_checksum must be sha256:<64 lowercase hex>",
            "duplicate replay write_receipt_json differs from first response",
            "typed write_receipt must be present when write_receipt_json is present",
            "typed write_receipt must match write_receipt_json",
            "Upsert affected_rows replay regression was not caught",
            "Delete affected_rows replay regression was not caught",
            "Upsert mutation_id replay regression was not caught",
            "Upsert added mutation_id replay regression was not caught",
            "Upsert invalid mutation_id shape regression was not caught",
            "Delete mutation_id replay regression was not caught",
            "Delete added mutation_id replay regression was not caught",
            "Delete invalid mutation_id shape regression was not caught",
            "Upsert fresh affected_rows regression was not caught",
            "Delete fresh affected_rows regression was not caught",
            "Upsert empty replay summary regression was not caught",
            "Delete empty replay summary regression was not caught",
            "Upsert wrong-tenant resource_uri replay summary regression was not caught",
            "Delete wrong-tenant resource_uri replay summary regression was not caught",
            "Upsert wrong-message resource_uri replay summary regression was not caught",
            "Delete wrong-message resource_uri replay summary regression was not caught",
            "Upsert short-path resource_uri replay summary regression was not caught",
            "Delete short-path resource_uri replay summary regression was not caught",
            "Upsert wrong-id resource_uri replay summary regression was not caught",
            "Delete wrong-id resource_uri replay summary regression was not caught",
            "Upsert non-identity scalar resource_uri replay summary regression was not caught",
            "Delete non-identity scalar resource_uri replay summary regression was not caught",
            "Upsert missing identity resource_uri replay summary regression was not caught",
            "Delete missing identity resource_uri replay summary regression was not caught",
            "Upsert padded identity resource_uri replay summary regression was not caught",
            "Upsert embedded-space identity resource_uri replay summary regression was not caught",
            "Delete padded identity resource_uri replay summary regression was not caught",
            "Delete embedded-space identity resource_uri replay summary regression was not caught",
            "control-character Upsert idempotency_key regression was not caught",
            "Upsert non-finite record_json replay summary regression was not caught",
            "Upsert mismatched record_json replay summary regression was not caught",
            "Upsert invalid checksum_sha256 replay summary regression was not caught",
            "Upsert checksum replay regression was not caught",
            "Upsert unexpected-field write_receipt_json replay summary regression was not caught",
            "Upsert empty source_lsn write_receipt_json replay summary regression was not caught",
            "Upsert control-character source_lsn write_receipt_json replay summary regression was not caught",
            "Upsert whitespace projection_task_ids write_receipt_json replay summary regression was not caught",
            "Upsert control-character projection_task_ids write_receipt_json replay summary regression was not caught",
            "Upsert invalid manifest_checksum write_receipt_json replay summary regression was not caught",
            "Upsert missing typed write_receipt replay summary regression was not caught",
            "Upsert mismatched typed write_receipt replay summary regression was not caught",
            "Delete missing typed write_receipt replay summary regression was not caught",
            "Delete mismatched typed write_receipt replay summary regression was not caught",
            "Upsert dropped replay summary regression was not caught",
            "Delete dropped replay summary regression was not caught",
            "retry-safe runtime Upsert request-message validation regression was not caught",
            "retry-safe runtime Delete request-message validation regression was not caught",
            "retry-safe runtime metadata validation regression was not caught",
            "retry-safe runtime timeout validation regression was not caught",
            "retry-safe runtime Upsert stub validation regression was not caught",
            "retry-safe runtime Delete stub validation regression was not caught",
            "retry-safe runtime Upsert response-message validation regression was not caught",
            "retry-safe runtime Delete response-message validation regression was not caught",
            "retry-safe runtime Upsert call-error validation regression was not caught",
            "retry-safe runtime Upsert unexpected-RpcError validation regression was not caught",
            "retry-safe runtime Delete call-error validation regression was not caught",
            "retry-safe runtime Delete unexpected-RpcError validation regression was not caught",
            "retry-safe method/request descriptor mismatch regression was not caught",
            "retry-safe missing method descriptor regression was not caught",
            "missing Upsert proof file regression was not caught",
            "missing Delete proof file regression was not caught",
            "oversized Upsert proof file regression was not caught",
            "oversized Delete proof file regression was not caught",
            "missing Upsert message_type regression was not caught",
            "missing Delete message_type regression was not caught",
            "spaced Upsert message_type regression was not caught",
            "embedded-space Delete message_type regression was not caught",
            "missing Upsert record_json regression was not caught",
            "malformed Upsert record_json regression was not caught",
            "array Upsert record_json regression was not caught",
            "empty-object Upsert record_json regression was not caught",
            "ambiguous Upsert record_json encoding regression was not caught",
            "non-finite Upsert record_json regression was not caught",
            "non-finite Upsert proof JSON regression was not caught",
            "duplicate-key Upsert proof JSON regression was not caught",
            "duplicate-key Delete proof JSON regression was not caught",
            "malformed gRPC header name regression was not caught",
            "reserved gRPC header name regression was not caught",
            "URL-shaped gRPC target regression was not caught",
            "whitespace gRPC target regression was not caught",
            "control-character gRPC target regression was not caught",
            "missing-port gRPC target regression was not caught",
            "canonical timeout string was rejected",
            "padded timeout regression was not caught",
            "non-decimal timeout regression was not caught",
            "non-positive timeout regression was not caught",
            "infinite timeout regression was not caught",
            "excessive timeout regression was not caught",
            "missing Delete filter regression was not caught",
            "empty Delete filter field regression was not caught",
            "control-character Delete filter field regression was not caught",
            "null Delete filter value regression was not caught",
            "mismatched Upsert/Delete replay scope regression was not caught",
            "mismatched Upsert/Delete idempotency key regression was not caught",
            "--upsert-json",
            "--delete-json",
            "--require-all-proofs",
            "--require-all-proofs is required for retry-safe live served proof",
            "missing retry-safe complete-proof flag regression was not caught",
            "served keyed Upsert/Delete replay",
            "retry-safe served smoke selftest passed",
        ),
    ),
    TokenCheck(
        "retry-safe served workflow exposes the live replay proof",
        ".github/workflows/retry-safe-served-smoke.yml",
        (
            "retry-safe-served:",
            "target:",
            "upsert_json:",
            "delete_json:",
            "python -m pip install -e sdk/python",
            "python scripts/retry_safe_served_smoke.py --selftest",
            'printf \'%s\' "$UPSERT_JSON" > smoke-input/upsert.json',
            'printf \'%s\' "$DELETE_JSON" > smoke-input/delete.json',
            "--require-all-proofs",
            "--upsert-json smoke-input/upsert.json",
            "--delete-json smoke-input/delete.json",
            "Retry-safe mutation metadata served proof",
        ),
    ),
)


FORBIDDEN_TOKEN_CHECKS: tuple[ForbiddenTokenCheck, ...] = (
    ForbiddenTokenCheck(
        "TypeScript retry gate must not accept context-only idempotency keys",
        "sdk-templates/typescript/generatedClient.ts.tmpl",
        ("fromCtx", "request.request_context", "request.requestContext"),
    ),
    ForbiddenTokenCheck(
        "Generated TypeScript retry gate must not accept context-only idempotency keys",
        "sdk/typescript/generatedClient.ts",
        ("fromCtx", "request.request_context", "request.requestContext"),
    ),
    ForbiddenTokenCheck(
        "TypeScript dist-test retry gate must not accept context-only idempotency keys",
        "sdk/typescript/dist-test/generatedClient.js",
        ("fromCtx", "request.request_context", "request.requestContext"),
    ),
    ForbiddenTokenCheck(
        "Python retry gate must not accept context-only idempotency keys",
        "sdk-templates/python/udb_client/generated_client.py.tmpl",
        ("ctx_key", "getattr(context, \"idempotency_key\""),
    ),
    ForbiddenTokenCheck(
        "Generated Python retry gate must not accept context-only idempotency keys",
        "sdk/python/udb_client/generated_client.py",
        ("ctx_key", "getattr(context, \"idempotency_key\""),
    ),
)


def read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


def iter_proto_rpcs(root: Path) -> dict[str, ProtoRpc]:
    out: dict[str, ProtoRpc] = {}
    for path in (root / "proto" / "udb").rglob("*.proto"):
        text = read(path)
        package = re.search(r"^package\s+([\w.]+);", text, re.MULTILINE)
        if not package:
            continue
        lines = text.splitlines()
        service = ""
        service_depth = 0
        pending_comments: list[str] = []
        index = 0
        while index < len(lines):
            line = lines[index]
            service_match = re.match(r"\s*service\s+(\w+)\s*\{", line)
            if service_match:
                service = service_match.group(1)
                service_depth = line.count("{") - line.count("}")
                pending_comments = []
                index += 1
                continue
            if not service:
                index += 1
                continue

            service_depth += line.count("{") - line.count("}")
            if re.match(r"\s*//", line) or not line.strip():
                pending_comments.append(line)
                pending_comments = pending_comments[-24:]
                index += 1
                continue

            rpc_match = re.match(r"\s*rpc\s+(\w+)\s*\(", line)
            if rpc_match:
                block_lines = [line]
                depth = line.count("{") - line.count("}")
                cursor = index + 1
                while cursor < len(lines) and depth > 0:
                    block_lines.append(lines[cursor])
                    depth += lines[cursor].count("{") - lines[cursor].count("}")
                    cursor += 1
                block = "\n".join(pending_comments + block_lines)
                full_method = f"/{package.group(1)}.{service}/{rpc_match.group(1)}"
                out[full_method] = ProtoRpc(
                    full_method=full_method,
                    operation_kind=operation_kind(block),
                    request_key_field=field_value(block, "request_key_field"),
                    duplicate_response_field=field_value(block, "duplicate_response_field"),
                    replay_safe=bool(re.search(r"\breplay_safe:\s*true\b", block)),
                    block=block,
                    path=path,
                )
                pending_comments = []
                index = cursor
                continue

            pending_comments = []
            if service_depth <= 0:
                service = ""
            index += 1
    return out


def operation_kind(block: str) -> str:
    if "OPERATION_KIND_READ_ONLY" in block:
        return "read_only"
    if "OPERATION_KIND_DESTRUCTIVE" in block:
        return "destructive"
    if "OPERATION_KIND_MUTATION" in block:
        return "mutation"
    return ""


def field_value(block: str, field: str) -> str:
    match = re.search(rf"\b{re.escape(field)}:\s*\"([^\"]+)\"", block)
    return match.group(1) if match else ""


def parse_generated_go_rpcs(root: Path) -> dict[str, GeneratedRpc]:
    text = read(root / "sdk" / "go" / "udbclient" / "generated_client.go")
    row_re = re.compile(
        r"\{Service: \"[^\"]+\", ServicePkg: \"[^\"]+\", "
        r"FullMethod: \"([^\"]+)\", Name: \"[^\"]+\".*?"
        r"OperationKind: \"([^\"]+)\", ReplaySafe: (true|false)\}",
        re.DOTALL,
    )
    return {
        full_method: GeneratedRpc(
            full_method=full_method,
            operation_kind=kind,
            replay_safe=replay_safe == "true",
        )
        for full_method, kind, replay_safe in row_re.findall(text)
    }


def has_proof_terms(rpc: ProtoRpc) -> bool:
    lower = rpc.block.lower()
    key = rpc.request_key_field.lower()
    required_terms = ("tenant", "dedup")
    if not all(term in lower for term in required_terms):
        return False
    if key and key not in lower:
        return False
    if "project" not in lower and "correlation_id" not in lower:
        return False
    if "tx" not in lower and "transaction" not in lower and "unique-index" not in lower:
        return False
    return True


def check_tokens(root: Path) -> list[str]:
    errors: list[str] = []
    for check in TOKEN_CHECKS:
        path = root / check.path
        text = read(path)
        if not text:
            errors.append(f"{check.label}: missing {check.path}")
            continue
        for token in check.tokens:
            if token not in text:
                errors.append(f"{check.label}: missing token {token!r} in {check.path}")
    for check in FORBIDDEN_TOKEN_CHECKS:
        path = root / check.path
        text = read(path)
        if not text:
            errors.append(f"{check.label}: missing {check.path}")
            continue
        for token in check.tokens:
            if token in text:
                errors.append(f"{check.label}: forbidden token {token!r} in {check.path}")
    return errors


def check_retry_safe_contracts(root: Path) -> list[str]:
    errors: list[str] = []
    proto_rpcs = iter_proto_rpcs(root)
    generated_rpcs = parse_generated_go_rpcs(root)
    if not generated_rpcs:
        errors.append("generated Go AllRPCs metadata is missing or unparsable")
        return errors

    for full_method, generated in sorted(generated_rpcs.items()):
        proto = proto_rpcs.get(full_method)
        if generated.replay_safe:
            if proto is None:
                errors.append(f"{full_method}: generated ReplaySafe=true but no matching proto RPC was found")
                continue
            if generated.operation_kind not in {"mutation", "destructive"}:
                errors.append(f"{full_method}: generated ReplaySafe=true on non-mutating operation_kind={generated.operation_kind!r}")
            if not proto.replay_safe:
                errors.append(f"{full_method}: generated ReplaySafe=true but proto idempotency replay_safe=true is absent")
            if not proto.request_key_field:
                errors.append(f"{full_method}: replay-safe proto contract is missing request_key_field")
            if not has_proof_terms(proto):
                errors.append(f"{full_method}: replay-safe proto comments lack tenant/project/caller scoping and durable dedup proof terms")
            if proto.full_method.startswith("/udb.services.v1.DataBroker/") and not proto.duplicate_response_field:
                errors.append(f"{full_method}: DataBroker replay-safe mutation is missing duplicate_response_field")

    for full_method, proto in sorted(proto_rpcs.items()):
        generated = generated_rpcs.get(full_method)
        if proto.replay_safe:
            if proto.operation_kind not in {"mutation", "destructive"}:
                errors.append(f"{full_method}: proto replay_safe=true is not a mutating operation")
            if generated is None:
                errors.append(f"{full_method}: proto replay_safe=true but generated Go metadata is missing the RPC")
            elif not generated.replay_safe:
                errors.append(f"{full_method}: proto replay_safe=true but generated Go metadata has ReplaySafe=false")
            if not proto.request_key_field:
                errors.append(f"{full_method}: proto replay_safe=true but request_key_field is empty")
    return errors


def check_repo(root: Path) -> list[str]:
    return check_tokens(root) + check_retry_safe_contracts(root)


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def make_good_fixture(root: Path) -> None:
    write(
        root / "proto/udb/services/v1/data_broker.proto",
        """syntax = "proto3";
package udb.services.v1;
import "udb/core/common/v1/security.proto";

service DataBroker {
  // Verified: keyed dedup in the write tx, scoped to tenant+project+caller.
  // The retry returns was_duplicate=true for the original body.
  rpc Upsert(Req) returns (Resp) {
    option (udb.core.common.v1.operation_kind) = OPERATION_KIND_MUTATION;
    option (udb.core.common.v1.method_idempotency_contract) = {
      request_key_field: "idempotency_key"
      duplicate_response_field: "was_duplicate"
      replay_safe: true
    };
  }

  rpc Select(Req) returns (Resp) {
    option (udb.core.common.v1.operation_kind) = OPERATION_KIND_READ_ONLY;
  }
}
""",
    )
    write(
        root / "sdk/go/udbclient/generated_client.go",
        """package udbclient
func (rc RetryConfig) retryableForRPC(code codes.Code, readOnly, replaySafe, hasIdempotencyKey bool) bool { if !replaySafe || !hasIdempotencyKey { return false }; return true }
type RPCInfo struct { ReplaySafe bool }
var AllRPCs = []RPCInfo{
    {Service: "DataBroker", ServicePkg: "udb.services.v1", FullMethod: "/udb.services.v1.DataBroker/Upsert", Name: "Upsert", APIAlias: "upsert", OperationID: "upsert", HTTPMethod: "", HTTPPath: "", Kind: RPCKind("unary"), ReadOnly: false, OperationKind: "mutation", ReplaySafe: true},
    {Service: "DataBroker", ServicePkg: "udb.services.v1", FullMethod: "/udb.services.v1.DataBroker/Select", Name: "Select", APIAlias: "select", OperationID: "select", HTTPMethod: "", HTTPPath: "", Kind: RPCKind("unary"), ReadOnly: true, OperationKind: "read_only", ReplaySafe: false},
}
func isReplaySafeRPC(method string) bool { return false }
func hasIdempotencyKey(req any) bool { return strings.TrimSpace(k.GetIdempotencyKey()) != "" }
""",
    )
    token_files = {
        "src/runtime/sdk_manifest.rs": "replay_safe: method .idempotency_contract .map(|c| c.replay_safe) .unwrap_or(false)",
        "src/cli/sdk_gen.rs": '"replay_safe".to_string() serde_json::json!(rpc.replay_safe) ("{{RPC_REPLAY_SAFE}}", rpc.replay_safe.to_string()) replay_safe_placeholder_reflects_idempotency_contract',
        "sdk-templates/go/udbclient/generated_client.go.tmpl": "func (rc RetryConfig) retryableForRPC(code codes.Code, readOnly, replaySafe, hasIdempotencyKey bool) bool\nif !replaySafe || !hasIdempotencyKey\nReplaySafe: {{RPC_REPLAY_SAFE}}\nfunc isReplaySafeRPC(method string) bool\nfunc hasIdempotencyKey(req any) bool\nstrings.TrimSpace(k.GetIdempotencyKey()) != \"\"",
        "sdk-templates/typescript/generatedClient.ts.tmpl": "export const RPC_REPLAY_SAFE\nRPC_REPLAY_SAFE[path] === true && UdbCore.hasIdempotencyKey(request)\nprivate static hasIdempotencyKey(request: any): boolean\ntop-level idempotency key\nrequest.idempotency_key ??\nrequest.idempotencyKey;",
        "sdk/typescript/generatedClient.ts": "top-level idempotency key\nrequest.idempotency_key ??\nrequest.idempotencyKey;",
        "sdk/typescript/dist-test/generatedClient.js": "exports.RPC_REPLAY_SAFE\nexports.RPC_REPLAY_SAFE[path] === true && UdbCore.hasIdempotencyKey(request)\nstatic hasIdempotencyKey(request)\ntop-level idempotency key\nrequest.idempotency_key ??\nrequest.idempotencyKey;",
        "sdk-templates/python/udb_client/generated_client.py.tmpl": "RPC_REPLAY_SAFE: dict[str, str]\nif not (replay_safe and has_idempotency_key):\ndef _request_has_idempotency_key(request: Any) -> bool:\ntop-level ``idempotency_key``\nnested only under request context\nkey.strip()",
        "sdk/python/udb_client/generated_client.py": "RPC_REPLAY_SAFE: dict[str, str]\nif not (replay_safe and has_idempotency_key):\ndef _request_has_idempotency_key(request: Any) -> bool:\ntop-level ``idempotency_key``\nnested only under request context\nkey.strip()",
        "sdk-templates/php/src/Generated/GeneratedClient.php.tmpl": "private function isRetryable(int $code, bool $readOnly, bool $replaySafe, bool $hasIdempotencyKey): bool\nif (! $replaySafe || ! $hasIdempotencyKey) {\nprivate function hasIdempotencyKey(mixed $request): bool\ntrim((string) $request->getIdempotencyKey()) !== ''",
        "sdk/php/src/Generated/GeneratedClient.php": "private function isRetryable(int $code, bool $readOnly, bool $replaySafe, bool $hasIdempotencyKey): bool\nif (! $replaySafe || ! $hasIdempotencyKey) {\nprivate function hasIdempotencyKey(mixed $request): bool\ntrim((string) $request->getIdempotencyKey()) !== ''",
        "sdk-templates/java/src/main/java/dev/udb/client/generated/GeneratedClientSupport.java": "Status.Code code, boolean readOnly, boolean replaySafe, boolean hasIdempotencyKey\nif (!replaySafe || !hasIdempotencyKey)\nreturn code != Status.Code.DEADLINE_EXCEEDED && RETRYABLE_CODES.contains(code);\nprivate static boolean hasIdempotencyKey(Object request)\ngetMethod(\"getIdempotencyKey\")",
        "sdk/java/src/main/java/dev/udb/client/generated/GeneratedClientSupport.java": "Status.Code code, boolean readOnly, boolean replaySafe, boolean hasIdempotencyKey\nif (!replaySafe || !hasIdempotencyKey)\nreturn code != Status.Code.DEADLINE_EXCEEDED && RETRYABLE_CODES.contains(code);\nprivate static boolean hasIdempotencyKey(Object request)\ngetMethod(\"getIdempotencyKey\")",
        "sdk-templates/java/src/main/java/dev/udb/client/generated/GeneratedUdbClient.java.tmpl": "GeneratedClientSupport.unary(\n\"{{RPC_OPERATION_KIND}}\".equals(\"read_only\")\n\"{{RPC_REPLAY_SAFE}}\".equals(\"true\")",
        "sdk/java/src/main/java/dev/udb/client/generated/GeneratedUdbClient.java": "GeneratedClientSupport.unary(\n\"true\".equals(\"true\")\n\"false\".equals(\"true\")",
        "sdk-templates/csharp/Udb.Client/GeneratedClientRuntime.cs": "private bool IsRetryable(StatusCode code, bool readOnly, bool replaySafe, bool hasIdempotencyKey)\nif (!replaySafe || !hasIdempotencyKey)\nreturn code != StatusCode.DeadlineExceeded && Options.RetryableCodes.Contains(code);\nprivate static bool HasIdempotencyKey(object? request)\nGetProperty(\"IdempotencyKey\")",
        "sdk/csharp/Udb.Client/GeneratedClientRuntime.cs": "private bool IsRetryable(StatusCode code, bool readOnly, bool replaySafe, bool hasIdempotencyKey)\nif (!replaySafe || !hasIdempotencyKey)\nreturn code != StatusCode.DeadlineExceeded && Options.RetryableCodes.Contains(code);\nprivate static bool HasIdempotencyKey(object? request)\nGetProperty(\"IdempotencyKey\")",
        "sdk-templates/csharp/Udb.Client/GeneratedClient.cs.tmpl": "InvokeUnaryAsync(\n\"{{RPC_OPERATION_KIND}}\" == \"read_only\"\n\"{{RPC_REPLAY_SAFE}}\" == \"true\"\n(object)request);",
        "sdk/csharp/Udb.Client/GeneratedClient.cs": "InvokeUnaryAsync(\n\"true\" == \"true\"\n\"false\" == \"true\"\n(object)request);",
        "sdk/java/src/test/java/dev/udb/client/UdbGeneratedRetryTest.java": "replaySafeMutationWithKeyRetries\nreplaySafeMutationWithoutKeyDoesNotRetry\nnonReplaySafeMutationWithKeyDoesNotRetry\ndeadlineExceededMutationDoesNotRetry\nblankIdempotencyKeyDoesNotSatisfyMutationRetry",
        "sdk/csharp/Udb.Client.Tests/UdbGeneratedRetryTests.cs": "ReplaySafeMutationWithKeyRetries\nReplaySafeMutationWithoutKeyDoesNotRetry\nNonReplaySafeMutationWithKeyDoesNotRetry\nDeadlineExceededMutationDoesNotRetry\nBlankIdempotencyKeyDoesNotSatisfyMutationRetry",
        "sdk/go/udbclient/generated_retry_test.go": "TestRetryConfigReplaySafeMutationRequiresKey\nTestRetryReplaySafeMutationWithKeyRetriesThenSucceeds\nTestRetryReplaySafeMutationWithoutKeyDoesNotRetry\nTestRetryReplaySafeMutationWithBlankKeyDoesNotRetry\nTestRetryNonReplaySafeMutationDoesNotRetry\nwhitespace-only key",
        "sdk/python/tests/test_retry_policy.py": "test_invoke_replay_safe_mutation_with_blank_key_not_retried\ntest_invoke_replay_safe_mutation_with_context_only_key_not_retried\n_request_has_idempotency_key\nidempotency_key=\"   \"\nreq.context.idempotency_key = \"ctx-key\"\nassert fake.calls == 1",
        "sdk/typescript/retry.test.ts": "replay-safe mutation WITH idempotency key retries then succeeds\nreplay-safe mutation WITHOUT idempotency key is not retried\nreplay-safe mutation with CONTEXT-ONLY idempotency key is not retried\ncontext: { idempotency_key: \"ctx-key\" }\nnon-replay-safe mutation is not retried even WITH an idempotency key",
        "sdk/typescript/dist-test/retry.test.js": "replay-safe mutation WITH idempotency key retries then succeeds\nreplay-safe mutation WITHOUT idempotency key is not retried\nreplay-safe mutation with BLANK idempotency key is not retried\nreplay-safe mutation with CONTEXT-ONLY idempotency key is not retried\ncontext: { idempotency_key: \"ctx-key\" }\nnon-replay-safe mutation is not retried even WITH an idempotency key",
        "sdk/php/tests/Unit/GeneratedRetryTest.php": "replay-safe mutation + non-empty idempotency key\ngates mutation retry on replay-safe AND idempotency key\ndetects a non-empty idempotency key on the request proto\nsetIdempotencyKey('   ')\nreplay-safe mutation with blank key does not retry",
        "scripts/retry_safe_served_smoke.py": "UPSERT_METHOD = \"/udb.services.v1.DataBroker/Upsert\"\nDELETE_METHOD = \"/udb.services.v1.DataBroker/Delete\"\nDOCUMENT_UPSERT_METHOD = \"/udb.services.v1.DataBroker/DocumentUpsert\"\nRPC_REPLAY_SAFE\nRetryPolicy\nDeleteRequest\n_is_replay_safe\n_request_has_idempotency_key\ndef assert_retry_metadata_gate(\ndef validate_replay_request(\nobject_pairs_hook=_reject_duplicate_json_keys\ndef validate_shared_replay_scope(\nGRPC_METADATA_NAME_CHARS\ngRPC metadata header name must contain only lowercase letters\ngRPC metadata header name must not start with grpc-\ndef _contains_control_character(\ndef validate_grpc_target(\ngRPC target must be a host:port authority, not a URL or path\ngRPC target must not include control characters\ngRPC target port must be an integer from 1 to 65535\nMAX_LIVE_TIMEOUT_SECONDS = 120.0\nTIMEOUT_DECIMAL_PATTERN\ndef normalize_timeout_seconds(\ndef validate_timeout_seconds(\ndef validate_runtime_metadata(\ndef validate_runtime_timeout_seconds(\ndef validate_runtime_transport_inputs(\ndef validate_runtime_upsert_request(\ndef validate_runtime_delete_request(\ntimeout must not include surrounding whitespace\ntimeout must be a positive decimal number of seconds\ntimeout must be a finite number of seconds\ntimeout must be greater than 0 seconds\ntimeout must be <= 120 seconds\ndef validate_complete_proof_mode(\n--require-all-proofs is required for retry-safe live served proof\nmissing retry-safe complete-proof flag regression was not caught\ndef _assert_restored_summary(\ndef _assert_typed_write_receipt_lockstep(\ndef check_served_replay(\ndef check_served_delete_replay(\nUpsert proof requires non-empty record_json\nmust use only one of record_json, record_json_object, or record_json_text\nproof JSON must not contain duplicate key\nDelete proof requires a non-empty filter\nUpsert/Delete replay proofs must share\nidempotency_key\nmust not contain control characters\nDataBroker.Delete must be generated as replay-safe\nreplay-safe keyed mutation should retry UNAVAILABLE\nreplay-safe mutation without idempotency key must not retry\nnon-replay-safe mutation must not retry even with idempotency key\nmutation DEADLINE_EXCEEDED must not be auto-retried\nsecond replay-safe Upsert did not return was_duplicate=true\nsecond replay-safe Delete did not return was_duplicate=true\nfirst replay-safe Upsert affected_rows must be positive\nfirst replay-safe Delete affected_rows must be positive\nduplicate replay mutation_id differs from first response\nduplicate Delete replay mutation_id differs from first response\nsecond.mutation_id != first.mutation_id\nMUTATION_ID_PATTERN = re.compile\nmutation_id must be non-empty\nmutation_id must be a canonical lowercase UUID\nduplicate replay affected_rows differs from first response\nduplicate Delete replay affected_rows differs from first response\nfirst response must include at least one replay summary field\nfirst response record_json must include request field/value\nfirst response resource_uri authority must equal request tenant_id\nfirst response resource_uri path must start with request message_type\nfirst response resource_uri path must include request message_type and resource id\nMANIFEST_CHECKSUM_PATTERN = re.compile\nfirst response checksum_sha256 must be sha256:<64 lowercase hex>\nfirst response write_receipt_json unexpected fields\nfirst response write_receipt_json source_lsn must be non-empty\nfirst response write_receipt_json source_lsn must not include whitespace\nfirst response write_receipt_json source_lsn must not contain control characters\nfirst response write_receipt_json projection_task_ids[{index}] must not include whitespace\nfirst response write_receipt_json projection_task_ids[{index}] must not contain control characters\nfirst response write_receipt_json manifest_checksum must be sha256:<64 lowercase hex>\nduplicate replay write_receipt_json differs from first response\ntyped write_receipt must be present when write_receipt_json is present\ntyped write_receipt must match write_receipt_json\nUpsert affected_rows replay regression was not caught\nDelete affected_rows replay regression was not caught\nUpsert mutation_id replay regression was not caught\nUpsert added mutation_id replay regression was not caught\nUpsert invalid mutation_id shape regression was not caught\nDelete mutation_id replay regression was not caught\nDelete added mutation_id replay regression was not caught\nDelete invalid mutation_id shape regression was not caught\nUpsert fresh affected_rows regression was not caught\nDelete fresh affected_rows regression was not caught\nUpsert empty replay summary regression was not caught\nDelete empty replay summary regression was not caught\nUpsert wrong-tenant resource_uri replay summary regression was not caught\nDelete wrong-tenant resource_uri replay summary regression was not caught\nUpsert wrong-message resource_uri replay summary regression was not caught\nDelete wrong-message resource_uri replay summary regression was not caught\nUpsert short-path resource_uri replay summary regression was not caught\nDelete short-path resource_uri replay summary regression was not caught\nUpsert mismatched record_json replay summary regression was not caught\nUpsert invalid checksum_sha256 replay summary regression was not caught\nUpsert unexpected-field write_receipt_json replay summary regression was not caught\nUpsert empty source_lsn write_receipt_json replay summary regression was not caught\nUpsert control-character source_lsn write_receipt_json replay summary regression was not caught\nUpsert whitespace projection_task_ids write_receipt_json replay summary regression was not caught\nUpsert control-character projection_task_ids write_receipt_json replay summary regression was not caught\nUpsert invalid manifest_checksum write_receipt_json replay summary regression was not caught\nUpsert missing typed write_receipt replay summary regression was not caught\nUpsert mismatched typed write_receipt replay summary regression was not caught\nDelete missing typed write_receipt replay summary regression was not caught\nDelete mismatched typed write_receipt replay summary regression was not caught\nUpsert dropped replay summary regression was not caught\nDelete dropped replay summary regression was not caught\nretry-safe runtime Upsert request-message validation regression was not caught\nretry-safe runtime Delete request-message validation regression was not caught\nretry-safe runtime metadata validation regression was not caught\nretry-safe runtime timeout validation regression was not caught\nambiguous Upsert record_json encoding regression was not caught\nduplicate-key Upsert proof JSON regression was not caught\nduplicate-key Delete proof JSON regression was not caught\nmalformed gRPC header name regression was not caught\nreserved gRPC header name regression was not caught\nURL-shaped gRPC target regression was not caught\nwhitespace gRPC target regression was not caught\ncontrol-character gRPC target regression was not caught\nmissing-port gRPC target regression was not caught\ncanonical timeout string was rejected\npadded timeout regression was not caught\nnon-decimal timeout regression was not caught\nnon-positive timeout regression was not caught\ninfinite timeout regression was not caught\nexcessive timeout regression was not caught\nmissing Upsert record_json regression was not caught\nmissing Delete filter regression was not caught\nmismatched Upsert/Delete replay scope regression was not caught\nmismatched Upsert/Delete idempotency key regression was not caught\ncontrol-character Upsert idempotency_key regression was not caught\n--upsert-json\n--delete-json\n--require-all-proofs\nserved keyed Upsert/Delete replay\nretry-safe served smoke selftest passed",
        ".github/workflows/retry-safe-served-smoke.yml": "retry-safe-served:\ntarget:\nupsert_json:\ndelete_json:\npython -m pip install -e sdk/python\npython scripts/retry_safe_served_smoke.py --selftest\nprintf '%s' \"$UPSERT_JSON\" > smoke-input/upsert.json\nprintf '%s' \"$DELETE_JSON\" > smoke-input/delete.json\n--require-all-proofs\n--upsert-json smoke-input/upsert.json\n--delete-json smoke-input/delete.json\nRetry-safe mutation metadata served proof",
    }
    token_files["scripts/retry_safe_served_smoke.py"] = (
        token_files["scripts/retry_safe_served_smoke.py"]
        .replace("def validate_replay_request(\n", "def validate_replay_request(\ndef validate_message_type_token(\ndef validate_upsert_payload(\n")
        .replace(
            "object_pairs_hook=_reject_duplicate_json_keys\n",
            "object_pairs_hook=_reject_duplicate_json_keys\n"
            "parse_constant=_reject_non_finite_json_constant\n"
            "proof JSON must not contain non-standard constant\n",
        )
        .replace(
            "_request_has_idempotency_key\n",
            "_request_has_idempotency_key\n"
            "from udb.services.v1 import data_broker_pb2\n"
            "def assert_databroker_method_request(\n"
            'data_broker_pb2.DESCRIPTOR.services_by_name.get("DataBroker")\n'
            'request_descriptor = getattr(request, "DESCRIPTOR", None)\n'
            "method_descriptor.input_type.full_name\n"
            "does not match RPC input\n"
            "DataBroker generated descriptor has no method\n",
        )
        .replace("def validate_upsert_payload(\n", "def validate_upsert_payload(\ndef validate_delete_filter(\n")
        .replace(
            "def validate_delete_filter(\n",
            "def validate_delete_filter(\n"
            'validate_message_type_token(f"{label} proof message_type"\n'
            "message_type must not include surrounding whitespace\n"
            "message_type must not include whitespace\n"
            "must not contain control characters\n",
        )
        .replace(
            "timeout must be <= 120 seconds\n",
            "timeout must be <= 120 seconds\n"
            "MAX_PROOF_INPUT_BYTES = 1_048_576\n"
            "def _read_proof_text(\n"
            "proof file must exist and be a regular file\n"
            "proof file must be <=\n",
        )
        .replace(
            "def validate_runtime_delete_request(\n",
            "def validate_runtime_delete_request(\n"
            "def validate_runtime_stub_method(\n"
            "runtime stub must expose callable\n",
        )
        .replace(
            "runtime stub must expose callable\n",
            "runtime stub must expose callable\n"
            "def validate_runtime_mutation_response(\n"
            "runtime response must be a MutationResponse\n",
        )
        .replace(
            "runtime response must be a MutationResponse\n",
            "runtime response must be a MutationResponse\n"
            "def call_runtime_mutation(\n"
            "runtime call raised unexpected gRPC error\n"
            "runtime call raised error\n",
        )
        .replace(
            "Upsert proof requires non-empty record_json\n",
            "Upsert proof requires non-empty record_json\n"
            "Upsert proof record_json must be a valid JSON object\n"
            "Upsert proof record_json must not contain non-standard JSON constants\n"
            "Upsert proof record_json must be a JSON object\n",
        )
        .replace(
            "Upsert proof record_json must be a JSON object\n",
            "Upsert proof record_json must be a JSON object\n"
            "Upsert proof record_json must be a non-empty JSON object\n",
        )
        .replace(
            "missing Upsert record_json regression was not caught\n",
            "retry-safe runtime Upsert stub validation regression was not caught\n"
            "retry-safe runtime Delete stub validation regression was not caught\n"
            "retry-safe runtime Upsert response-message validation regression was not caught\n"
            "retry-safe runtime Delete response-message validation regression was not caught\n"
            "retry-safe runtime Upsert call-error validation regression was not caught\n"
            "retry-safe runtime Upsert unexpected-RpcError validation regression was not caught\n"
            "retry-safe runtime Delete call-error validation regression was not caught\n"
            "retry-safe runtime Delete unexpected-RpcError validation regression was not caught\n"
            "retry-safe method/request descriptor mismatch regression was not caught\n"
            "retry-safe missing method descriptor regression was not caught\n"
            "missing Upsert proof file regression was not caught\n"
            "missing Delete proof file regression was not caught\n"
            "oversized Upsert proof file regression was not caught\n"
            "oversized Delete proof file regression was not caught\n"
            "missing Upsert message_type regression was not caught\n"
            "missing Delete message_type regression was not caught\n"
            "spaced Upsert message_type regression was not caught\n"
            "embedded-space Delete message_type regression was not caught\n"
            "missing Upsert record_json regression was not caught\n"
            "malformed Upsert record_json regression was not caught\n"
            "array Upsert record_json regression was not caught\n",
        )
        .replace(
            "array Upsert record_json regression was not caught\n",
            "array Upsert record_json regression was not caught\n"
            "empty-object Upsert record_json regression was not caught\n",
        )
        .replace(
            "empty-object Upsert record_json regression was not caught\n",
            "empty-object Upsert record_json regression was not caught\n"
            "non-finite Upsert record_json regression was not caught\n"
            "non-finite Upsert proof JSON regression was not caught\n",
        )
        .replace(
            "Delete proof requires a non-empty filter\n",
            "Delete proof requires a non-empty filter\n"
            "Delete proof filter field names must be non-empty\n"
            "Delete proof filter field names must not contain control characters\n"
            "Delete proof filter values must not be null\n",
        )
        .replace(
            "first response resource_uri path must include request message_type and resource id\n",
            "first response resource_uri path must include request message_type and resource id\n"
            "first response resource_uri id must match an identity request field value\n"
            "resource_uri id proof requires at least one scalar identity request field\n"
            "resource_uri id proof identity field value must not include surrounding whitespace\n"
            "resource_uri id proof identity field value must not include whitespace\n",
        )
        .replace(
            "resource_uri id proof identity field value must not include whitespace\n",
            "resource_uri id proof identity field value must not include whitespace\n"
            "first response {field} must not contain non-standard JSON constants\n"
            "first response record_json must not contain non-standard JSON constants\n"
            "first response checksum_sha256 must be sha256:<64 lowercase hex>\n"
            "duplicate replay checksum_sha256 differs from first response\n",
        )
        .replace(
            "Delete short-path resource_uri replay summary regression was not caught\n",
            "Delete short-path resource_uri replay summary regression was not caught\n"
            "Upsert wrong-id resource_uri replay summary regression was not caught\n"
            "Delete wrong-id resource_uri replay summary regression was not caught\n"
            "Upsert non-identity scalar resource_uri replay summary regression was not caught\n"
            "Delete non-identity scalar resource_uri replay summary regression was not caught\n"
            "Upsert missing identity resource_uri replay summary regression was not caught\n"
            "Delete missing identity resource_uri replay summary regression was not caught\n"
            "Upsert padded identity resource_uri replay summary regression was not caught\n"
            "Upsert embedded-space identity resource_uri replay summary regression was not caught\n"
            "Delete padded identity resource_uri replay summary regression was not caught\n"
            "Delete embedded-space identity resource_uri replay summary regression was not caught\n",
        )
        .replace(
            "Upsert mismatched record_json replay summary regression was not caught\n",
            "Upsert non-finite record_json replay summary regression was not caught\n"
            "Upsert mismatched record_json replay summary regression was not caught\n",
        )
        .replace(
            "Upsert mismatched record_json replay summary regression was not caught\n",
            "Upsert mismatched record_json replay summary regression was not caught\n"
            "Upsert invalid checksum_sha256 replay summary regression was not caught\n"
            "Upsert checksum replay regression was not caught\n",
        )
        .replace(
            "missing Delete filter regression was not caught\n",
            "missing Delete filter regression was not caught\n"
            "empty Delete filter field regression was not caught\n"
            "control-character Delete filter field regression was not caught\n"
            "null Delete filter value regression was not caught\n",
        )
    )
    for path, text in token_files.items():
        write(root / path, text)


def expect_failure(root: Path, label: str, mutator) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        fixture = Path(tmp) / "repo"
        shutil.copytree(root, fixture)
        mutator(fixture)
        errors = check_repo(fixture)
        if not errors:
            raise AssertionError(f"selftest regression was not caught: {label}")


def run_selftest() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "repo"
        make_good_fixture(root)
        errors = check_repo(root)
        if errors:
            raise AssertionError("good fixture failed:\n" + "\n".join(errors))

        expect_failure(
            root,
            "generated replay-safe mutation without proto idempotency contract",
            lambda r: write(
                r / "proto/udb/services/v1/data_broker.proto",
                read(r / "proto/udb/services/v1/data_broker.proto").replace(
                    '    option (udb.core.common.v1.method_idempotency_contract) = {\n'
                    '      request_key_field: "idempotency_key"\n'
                    '      duplicate_response_field: "was_duplicate"\n'
                    '      replay_safe: true\n'
                    "    };\n",
                    "",
                ),
            ),
        )
        expect_failure(
            root,
            "DataBroker replay-safe mutation without duplicate response field",
            lambda r: write(
                r / "proto/udb/services/v1/data_broker.proto",
                read(r / "proto/udb/services/v1/data_broker.proto").replace(
                    '      duplicate_response_field: "was_duplicate"\n',
                    "",
                ),
            ),
        )
        expect_failure(
            root,
            "generated replay-safe mutation missing proto RPC",
            lambda r: write(
                r / "sdk/go/udbclient/generated_client.go",
                read(r / "sdk/go/udbclient/generated_client.go").replace(
                    'FullMethod: "/udb.services.v1.DataBroker/Upsert", Name: "Upsert"',
                    'FullMethod: "/udb.services.v1.DataBroker/Delete", Name: "Delete"',
                ),
            ),
        )
        expect_failure(
            root,
            "SDK retry gate missing idempotency key check",
            lambda r: write(
                r / "sdk/go/udbclient/generated_client.go",
                read(r / "sdk/go/udbclient/generated_client.go").replace(
                    "if !replaySafe || !hasIdempotencyKey",
                    "if !replaySafe",
                ),
            ),
        )
        expect_failure(
            root,
            "TypeScript retry gate accepted context-only idempotency key",
            lambda r: write(
                r / "sdk-templates/typescript/generatedClient.ts.tmpl",
                read(r / "sdk-templates/typescript/generatedClient.ts.tmpl")
                + "\nconst fromCtx = () => undefined;\nrequest.request_context;\n",
            ),
        )
        expect_failure(
            root,
            "TypeScript dist-test retry gate lost replay-safe idempotency-key check",
            lambda r: write(
                r / "sdk/typescript/dist-test/generatedClient.js",
                read(r / "sdk/typescript/dist-test/generatedClient.js").replace(
                    "exports.RPC_REPLAY_SAFE[path] === true && UdbCore.hasIdempotencyKey(request)",
                    "exports.RPC_REPLAY_SAFE[path] === true",
                ),
            ),
        )
        expect_failure(
            root,
            "Python retry gate accepted context-only idempotency key",
            lambda r: write(
                r / "sdk/python/udb_client/generated_client.py",
                read(r / "sdk/python/udb_client/generated_client.py")
                + '\nctx_key = getattr(context, "idempotency_key", "")\n',
            ),
        )
        expect_failure(
            root,
            "Java retry support lost replay-safe idempotency-key gate",
            lambda r: write(
                r / "sdk/java/src/main/java/dev/udb/client/generated/GeneratedClientSupport.java",
                read(r / "sdk/java/src/main/java/dev/udb/client/generated/GeneratedClientSupport.java").replace(
                    "if (!replaySafe || !hasIdempotencyKey)",
                    "if (!replaySafe)",
                ),
            ),
        )
        expect_failure(
            root,
            "Java generated wrappers lost replay-safe placeholder",
            lambda r: write(
                r / "sdk-templates/java/src/main/java/dev/udb/client/generated/GeneratedUdbClient.java.tmpl",
                read(r / "sdk-templates/java/src/main/java/dev/udb/client/generated/GeneratedUdbClient.java.tmpl").replace(
                    '"{{RPC_REPLAY_SAFE}}".equals("true")',
                    "false",
                ),
            ),
        )
        expect_failure(
            root,
            "C# retry support lost replay-safe idempotency-key gate",
            lambda r: write(
                r / "sdk/csharp/Udb.Client/GeneratedClientRuntime.cs",
                read(r / "sdk/csharp/Udb.Client/GeneratedClientRuntime.cs").replace(
                    "if (!replaySafe || !hasIdempotencyKey)",
                    "if (!replaySafe)",
                ),
            ),
        )
        expect_failure(
            root,
            "C# generated wrappers lost replay-safe placeholder",
            lambda r: write(
                r / "sdk-templates/csharp/Udb.Client/GeneratedClient.cs.tmpl",
                read(r / "sdk-templates/csharp/Udb.Client/GeneratedClient.cs.tmpl").replace(
                    '"{{RPC_REPLAY_SAFE}}" == "true"',
                    "false",
                ),
            ),
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selftest", action="store_true", help="run built-in regression fixtures")
    parser.add_argument("--root", type=Path, default=ROOT, help="repository root")
    args = parser.parse_args(argv)

    if args.selftest:
        run_selftest()
        print("retry-safe posture selftest passed")
        return 0

    errors = check_repo(args.root.resolve())
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("retry-safe posture OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
